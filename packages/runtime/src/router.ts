import type { Operation } from "@gateway/kernel";

export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

export type ChannelHealthSnapshot = {
  channel_id: string;
  state: CircuitState;
  consecutive_failures: number;
  ewma_latency_ms: number;
  open_until_ms: number;
  cooldown_until_ms: number;
  probe_in_flight: boolean;
  backoff_level: number;
};

export type RoutableCandidate = {
  id: string;
  channel_id: string;
  account_id: string;
  priority: number;
  weight: number;
  operations: readonly Operation[];
  enabled: boolean;
  channel_enabled: boolean;
  account_active: boolean;
};

export type SmartRouterOptions = {
  failureThreshold?: number;
  windowSize?: number;
  errorRateThreshold?: number;
  baseOpenMs?: number;
  maxOpenMs?: number;
  defaultCooldownMs?: number;
  maxCooldownMs?: number;
  probeTimeoutMs?: number;
  now?: () => number;
  random?: () => number;
  onStateChange?: (event: {
    channel_id: string;
    state: CircuitState;
    open_until_ms: number;
    cooldown_until_ms: number;
    open_duration_ms: number;
    cooldown_duration_ms: number;
  }) => void;
};

type InternalChannelState = {
  state: CircuitState;
  consecutiveFailures: number;
  window: boolean[];
  windowCursor: number;
  ewmaLatencyMs: number;
  openUntilMs: number;
  cooldownUntilMs: number;
  probeInFlight: boolean;
  probeStartedAtMs: number;
  backoffLevel: number;
};

export class SmartRouter {
  private readonly channels = new Map<string, InternalChannelState>();
  private readonly rrCursors = new Map<string, number>();
  private readonly failureThreshold: number;
  private readonly windowSize: number;
  private readonly errorRateThreshold: number;
  private readonly baseOpenMs: number;
  private readonly maxOpenMs: number;
  private readonly defaultCooldownMs: number;
  private readonly maxCooldownMs: number;
  private readonly probeTimeoutMs: number;
  private readonly now: () => number;
  private readonly random: () => number;
  private readonly onStateChange: SmartRouterOptions["onStateChange"];

  constructor(options: SmartRouterOptions = {}) {
    this.failureThreshold = options.failureThreshold ?? 3;
    this.windowSize = options.windowSize ?? 20;
    this.errorRateThreshold = options.errorRateThreshold ?? 0.5;
    this.baseOpenMs = options.baseOpenMs ?? 15_000;
    this.maxOpenMs = options.maxOpenMs ?? 120_000;
    this.defaultCooldownMs = options.defaultCooldownMs ?? 5_000;
    this.maxCooldownMs = options.maxCooldownMs ?? 60_000;
    this.probeTimeoutMs = options.probeTimeoutMs ?? 30_000;
    this.now = options.now ?? (() => performance.now());
    this.random = options.random ?? Math.random;
    this.onStateChange = options.onStateChange;
  }

  private getOrCreate(channelId: string): InternalChannelState {
    let st = this.channels.get(channelId);
    if (!st) {
      st = {
        state: "CLOSED",
        consecutiveFailures: 0,
        window: [],
        windowCursor: 0,
        ewmaLatencyMs: 100,
        openUntilMs: 0,
        cooldownUntilMs: 0,
        probeInFlight: false,
        probeStartedAtMs: 0,
        backoffLevel: 0,
      };
      this.channels.set(channelId, st);
    }
    return st;
  }

  private pushWindow(st: InternalChannelState, success: boolean): void {
    if (st.window.length < this.windowSize) {
      st.window.push(success);
    } else {
      st.window[st.windowCursor] = success;
      st.windowCursor = (st.windowCursor + 1) % this.windowSize;
    }
  }

  private healthFactor(st: InternalChannelState): number {
    if (st.window.length === 0) return 1;
    let ok = 0;
    for (const v of st.window) if (v) ok++;
    return Math.max(0.1, ok / st.window.length);
  }

  isAvailable(channelId: string): { available: boolean; isProbe: boolean } {
    const st = this.channels.get(channelId);
    if (!st) return { available: true, isProbe: false };
    const t = this.now();
    if (st.cooldownUntilMs > t) return { available: false, isProbe: false };
    if (st.probeInFlight && t - st.probeStartedAtMs >= this.probeTimeoutMs) {
      st.probeInFlight = false;
      st.probeStartedAtMs = 0;
    }
    if (st.state === "OPEN") {
      if (st.openUntilMs > t) return { available: false, isProbe: false };
      if (st.probeInFlight) return { available: false, isProbe: false };
      st.state = "HALF_OPEN";
      st.probeInFlight = true;
      st.probeStartedAtMs = t;
      return { available: true, isProbe: true };
    }
    if (st.state === "HALF_OPEN") {
      if (st.probeInFlight) return { available: false, isProbe: false };
      st.probeInFlight = true;
      st.probeStartedAtMs = t;
      return { available: true, isProbe: true };
    }
    return { available: true, isProbe: false };
  }

  releaseProbe(channelId: string): void {
    const st = this.channels.get(channelId);
    if (st && st.probeInFlight) {
      st.probeInFlight = false;
      st.probeStartedAtMs = 0;
    }
  }

  recordSuccess(channelId: string, latencyMs: number): void {
    const st = this.getOrCreate(channelId);
    const prevState = st.state;
    st.state = "CLOSED";
    st.consecutiveFailures = 0;
    st.backoffLevel = 0;
    st.openUntilMs = 0;
    st.probeInFlight = false;
    st.probeStartedAtMs = 0;
    st.ewmaLatencyMs = st.ewmaLatencyMs * 0.8 + Math.max(1, latencyMs) * 0.2;
    this.pushWindow(st, true);
    if (prevState !== "CLOSED") {
      const t = this.now();
      this.onStateChange?.({
        channel_id: channelId,
        state: "CLOSED",
        open_until_ms: 0,
        cooldown_until_ms: st.cooldownUntilMs,
        open_duration_ms: 0,
        cooldown_duration_ms: Math.max(0, st.cooldownUntilMs - t),
      });
    }
  }

  recordFailure(
    channelId: string,
    kind: "retryable" | "rate_limit" | "auth_error",
    retryAfterMs?: number,
    latencyMs?: number,
  ): void {
    const st = this.getOrCreate(channelId);
    const t = this.now();
    st.probeInFlight = false;
    if (latencyMs !== undefined && latencyMs > 0) {
      st.ewmaLatencyMs = st.ewmaLatencyMs * 0.8 + latencyMs * 0.2;
    }
    if (kind === "rate_limit") {
      const cd = Math.min(
        this.maxCooldownMs,
        Math.max(1000, retryAfterMs ?? this.defaultCooldownMs),
      );
      st.cooldownUntilMs = t + cd;
      this.onStateChange?.({
        channel_id: channelId,
        state: st.state,
        open_until_ms: st.openUntilMs,
        cooldown_until_ms: st.cooldownUntilMs,
        open_duration_ms: Math.max(0, st.openUntilMs - t),
        cooldown_duration_ms: cd,
      });
      return;
    }
    if (kind === "auth_error") {
      const dur = Math.min(this.maxOpenMs, 60_000);
      st.state = "OPEN";
      st.consecutiveFailures = this.failureThreshold;
      st.openUntilMs = t + dur;
      this.onStateChange?.({
        channel_id: channelId,
        state: "OPEN",
        open_until_ms: st.openUntilMs,
        cooldown_until_ms: st.cooldownUntilMs,
        open_duration_ms: dur,
        cooldown_duration_ms: Math.max(0, st.cooldownUntilMs - t),
      });
      return;
    }

    st.consecutiveFailures++;
    this.pushWindow(st, false);
    const failCount = st.window.filter((v) => !v).length;
    const rateTripped =
      st.window.length >= Math.min(10, this.windowSize) &&
      failCount / st.window.length > this.errorRateThreshold;

    if (
      st.state === "HALF_OPEN" ||
      st.consecutiveFailures >= this.failureThreshold ||
      rateTripped
    ) {
      const duration = Math.min(this.maxOpenMs, this.baseOpenMs * 2 ** st.backoffLevel);
      st.backoffLevel = Math.min(st.backoffLevel + 1, 5);
      st.state = "OPEN";
      st.openUntilMs = t + duration;
      this.onStateChange?.({
        channel_id: channelId,
        state: "OPEN",
        open_until_ms: st.openUntilMs,
        cooldown_until_ms: st.cooldownUntilMs,
        open_duration_ms: duration,
        cooldown_duration_ms: Math.max(0, st.cooldownUntilMs - t),
      });
    }
  }

  applyRemoteEvent(event: {
    channel_id: string;
    state: CircuitState;
    open_until_ms?: number;
    cooldown_until_ms?: number;
    open_duration_ms?: number;
    cooldown_duration_ms?: number;
  }): void {
    const st = this.getOrCreate(event.channel_id);
    const t = this.now();
    const targetOpen =
      event.open_duration_ms !== undefined
        ? t + event.open_duration_ms
        : (event.open_until_ms ?? 0);
    const targetCooldown =
      event.cooldown_duration_ms !== undefined
        ? t + event.cooldown_duration_ms
        : (event.cooldown_until_ms ?? 0);
    if (event.state === "OPEN" && targetOpen > st.openUntilMs) {
      st.state = "OPEN";
      st.openUntilMs = targetOpen;
      st.probeInFlight = false;
      st.probeStartedAtMs = 0;
    } else if (event.state === "CLOSED") {
      st.state = "CLOSED";
      st.openUntilMs = 0;
      st.consecutiveFailures = 0;
      st.backoffLevel = 0;
      st.probeInFlight = false;
      st.probeStartedAtMs = 0;
    }
    if (targetCooldown > st.cooldownUntilMs) {
      st.cooldownUntilMs = targetCooldown;
    }
  }

  selectCandidates<T extends RoutableCandidate>(
    modelKey: string,
    candidates: readonly T[],
    strategy: "WEIGHTED" | "ROUND_ROBIN" | "LEAST_LATENCY",
    operation: Operation,
  ): { route: T; isProbe: boolean }[] {
    const eligible: { route: T; isProbe: boolean; st: InternalChannelState | undefined }[] = [];
    for (const c of candidates) {
      if (!c.enabled || !c.channel_enabled || !c.account_active) continue;
      if (!c.operations.includes(operation)) continue;
      const avail = this.isAvailable(c.channel_id);
      if (!avail.available) continue;
      eligible.push({ route: c, isProbe: avail.isProbe, st: this.channels.get(c.channel_id) });
    }
    if (eligible.length <= 1) {
      return eligible.map((e) => ({ route: e.route, isProbe: e.isProbe }));
    }

    const byPriority = new Map<number, typeof eligible>();
    for (const e of eligible) {
      const group = byPriority.get(e.route.priority);
      if (group) group.push(e);
      else byPriority.set(e.route.priority, [e]);
    }
    const priorities = [...byPriority.keys()].sort((a, b) => b - a);
    const ordered: { route: T; isProbe: boolean }[] = [];

    for (const p of priorities) {
      const group = byPriority.get(p)!;
      if (group.length === 1) {
        ordered.push({ route: group[0]!.route, isProbe: group[0]!.isProbe });
        continue;
      }
      if (strategy === "LEAST_LATENCY") {
        group.sort((a, b) => (a.st?.ewmaLatencyMs ?? 100) - (b.st?.ewmaLatencyMs ?? 100));
        for (const item of group) ordered.push({ route: item.route, isProbe: item.isProbe });
      } else if (strategy === "ROUND_ROBIN") {
        const cursorKey = `${modelKey}:${p}`;
        const cursor = this.rrCursors.get(cursorKey) ?? 0;
        this.rrCursors.set(cursorKey, (cursor + 1) % group.length);
        for (let i = 0; i < group.length; i++) {
          const item = group[(cursor + i) % group.length]!;
          ordered.push({ route: item.route, isProbe: item.isProbe });
        }
      } else {
        const remaining = [...group];
        while (remaining.length > 0) {
          let totalWeight = 0;
          const weights = remaining.map((item) => {
            const w = item.route.weight * (item.st ? this.healthFactor(item.st) : 1);
            totalWeight += w;
            return w;
          });
          let pick = this.random() * totalWeight;
          let chosenIdx = remaining.length - 1;
          for (let i = 0; i < weights.length; i++) {
            pick -= weights[i]!;
            if (pick <= 0) {
              chosenIdx = i;
              break;
            }
          }
          const [chosen] = remaining.splice(chosenIdx, 1);
          ordered.push({ route: chosen!.route, isProbe: chosen!.isProbe });
        }
      }
    }
    return ordered;
  }

  snapshot(): ChannelHealthSnapshot[] {
    const out: ChannelHealthSnapshot[] = [];
    for (const [id, st] of this.channels) {
      out.push({
        channel_id: id,
        state: st.state,
        consecutive_failures: st.consecutiveFailures,
        ewma_latency_ms: st.ewmaLatencyMs,
        open_until_ms: st.openUntilMs,
        cooldown_until_ms: st.cooldownUntilMs,
        probe_in_flight: st.probeInFlight,
        backoff_level: st.backoffLevel,
      });
    }
    return out;
  }
}

type BucketState = {
  windowStartMs: number;
  reqCount: number;
  tokenCount: number;
  active: number;
};

export class LocalRateLimiter {
  private readonly buckets = new Map<string, BucketState>();
  private readonly now: () => number;

  constructor(now: () => number = () => performance.now()) {
    this.now = now;
  }

  acquire(
    key: string,
    limits: { rpm: number; tpm: number; concurrency: number },
    estimatedTokens: number,
  ): { ok: true; release: () => void } | { ok: false; reason: "rpm" | "tpm" | "concurrency" } {
    const t = this.now();
    let b = this.buckets.get(key);
    if (!b) {
      b = { windowStartMs: t, reqCount: 0, tokenCount: 0, active: 0 };
      this.buckets.set(key, b);
    }
    if (t - b.windowStartMs >= 60_000) {
      b.windowStartMs = t;
      b.reqCount = 0;
      b.tokenCount = 0;
    }
    if (b.active >= limits.concurrency) return { ok: false, reason: "concurrency" };
    if (b.reqCount >= limits.rpm) return { ok: false, reason: "rpm" };
    if (b.tokenCount + estimatedTokens > limits.tpm) return { ok: false, reason: "tpm" };
    b.reqCount++;
    b.tokenCount += estimatedTokens;
    b.active++;
    let released = false;
    return {
      ok: true,
      release: () => {
        if (released) return;
        released = true;
        if (b.active > 0) b.active--;
      },
    };
  }
}
