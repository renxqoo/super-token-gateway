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
  same_protocol?: boolean;
};

type AdaptiveOrdering<T extends RoutableCandidate> = {
  mode: "observe" | "active";
  factors: (group: readonly T[]) => ReadonlyMap<string, number> | undefined;
  compare: (changed: boolean) => void;
};

function pickWeighted(weights: readonly number[], total: number, draw: number): number {
  let pick = draw * total;
  for (let i = 0; i < weights.length; i++) {
    pick -= weights[i]!;
    if (pick <= 0) return i;
  }
  return weights.length - 1;
}

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
  onStateChange?: (event: CircuitTransition) => void;
};

export type CircuitTransition = {
  channel_id: string;
  state: CircuitState;
  circuit_id: string | null;
  issued_at_ms: number;
  open_duration_ms: number;
  cooldown_duration_ms: number;
};

export type RoutingAttempt = { generation: number; probe: symbol | undefined };

type InternalChannelState = {
  state: CircuitState;
  consecutiveFailures: number;
  window: boolean[];
  windowCursor: number;
  ewmaLatencyMs: number;
  openUntilMs: number;
  cooldownUntilMs: number;
  probe: symbol | undefined;
  generation: number;
  circuitId: string | null;
  circuitAt: number;
  closedCircuits?: Set<string>;
  dialFailures: number;
  dialUntilMs: number;
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
        probe: undefined,
        generation: 0,
        circuitId: null,
        circuitAt: 0,
        dialFailures: 0,
        dialUntilMs: 0,
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

  private probeIsCurrent(st: InternalChannelState, probe: symbol | undefined): boolean {
    return Boolean(
      probe && st.probe === probe && this.now() - st.probeStartedAtMs < this.probeTimeoutMs,
    );
  }

  isAvailable(channelId: string): { available: boolean; probe?: symbol } {
    const st = this.channels.get(channelId);
    if (!st) return { available: true };
    if (!this.peekAvailable(channelId)) return { available: false };
    if (st.state === "CLOSED") return { available: true };
    st.state = "HALF_OPEN";
    st.probe = Symbol();
    st.probeStartedAtMs = this.now();
    return { available: true, probe: st.probe };
  }

  peekAvailable(channelId: string, probe?: symbol): boolean {
    const st = this.channels.get(channelId);
    if (!st) return true;
    if (st.state === "CLOSED" && st.cooldownUntilMs === 0 && st.dialUntilMs === 0) {
      return !probe;
    }
    const t = this.now();
    if (st.cooldownUntilMs > t || st.dialUntilMs > t) return false;
    if (probe) return st.state === "HALF_OPEN" && this.probeIsCurrent(st, probe);
    if (st.state === "OPEN") return st.openUntilMs <= t;
    return st.state === "CLOSED" || !this.probeIsCurrent(st, st.probe);
  }

  releaseProbe(channelId: string, probe: symbol): void {
    const st = this.channels.get(channelId);
    if (st?.probe === probe) st.probe = undefined;
  }

  beginAttempt(channelId: string, probe?: symbol): RoutingAttempt {
    return { generation: this.channels.get(channelId)?.generation ?? 0, probe };
  }

  private accepts(st: InternalChannelState, attempt?: RoutingAttempt): boolean {
    if (attempt && attempt.generation !== st.generation) return false;
    if (st.state === "CLOSED") return !attempt?.probe;
    return st.state === "HALF_OPEN" && this.probeIsCurrent(st, attempt?.probe);
  }

  private closeCircuit(st: InternalChannelState) {
    st.generation++;
    st.state = "CLOSED";
    st.openUntilMs = 0;
    st.consecutiveFailures = 0;
    st.backoffLevel = 0;
    st.probe = undefined;
    st.window.length = 0;
    st.windowCursor = 0;
  }

  private rememberClosed(st: InternalChannelState, circuitId: string) {
    const closed = (st.closedCircuits ??= new Set());
    closed.add(circuitId);
    if (closed.size > 16) closed.delete(closed.values().next().value!);
  }

  private openCircuit(channelId: string, st: InternalChannelState, duration: number) {
    st.generation++;
    st.state = "OPEN";
    st.openUntilMs = this.now() + duration;
    st.probe = undefined;
    st.circuitId = crypto.randomUUID();
    st.circuitAt = Date.now();
    this.onStateChange?.({
      channel_id: channelId,
      state: "OPEN",
      circuit_id: st.circuitId,
      issued_at_ms: st.circuitAt,
      open_duration_ms: duration,
      cooldown_duration_ms: 0,
    });
  }

  recordSuccess(channelId: string, latencyMs: number, attempt?: RoutingAttempt): boolean {
    const st = this.getOrCreate(channelId);
    if (!this.accepts(st, attempt)) return false;
    if (st.state === "HALF_OPEN") {
      this.closeCircuit(st);
      if (st.circuitId) this.rememberClosed(st, st.circuitId);
      this.onStateChange?.({
        channel_id: channelId,
        state: "CLOSED",
        circuit_id: st.circuitId,
        issued_at_ms: Date.now(),
        open_duration_ms: 0,
        cooldown_duration_ms: 0,
      });
    }
    st.consecutiveFailures = 0;
    st.dialFailures = 0;
    st.ewmaLatencyMs = st.ewmaLatencyMs * 0.8 + Math.max(1, latencyMs) * 0.2;
    this.pushWindow(st, true);
    return true;
  }

  recordDialFailure(channelId: string, attempt?: RoutingAttempt): void {
    const st = this.getOrCreate(channelId);
    if (!this.accepts(st, attempt)) return;
    if (attempt?.probe) this.releaseProbe(channelId, attempt.probe);
    const now = this.now();
    if (st.dialUntilMs > now) return;
    if (++st.dialFailures >= this.failureThreshold) {
      st.dialFailures = 0;
      st.dialUntilMs = now + 250;
    }
  }

  recordFailure(
    channelId: string,
    kind: "retryable" | "rate_limit" | "auth_error",
    retryAfterMs?: number,
    latencyMs?: number,
    attempt?: RoutingAttempt,
  ): void {
    const st = this.getOrCreate(channelId);
    if (!this.accepts(st, attempt)) return;
    const t = this.now();
    st.probe = undefined;
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
        state: "HALF_OPEN",
        circuit_id: null,
        issued_at_ms: Date.now(),
        open_duration_ms: 0,
        cooldown_duration_ms: cd,
      });
      return;
    }
    if (kind === "auth_error") {
      st.consecutiveFailures = this.failureThreshold;
      this.openCircuit(channelId, st, Math.min(this.maxOpenMs, 60_000));
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
      this.openCircuit(channelId, st, duration);
    }
  }

  applyRemoteEvent(event: CircuitTransition): void {
    const st = this.getOrCreate(event.channel_id);
    const elapsed = Number.isFinite(event.issued_at_ms)
      ? Math.max(0, Date.now() - event.issued_at_ms)
      : 0;
    const now = this.now();
    if (event.cooldown_duration_ms > elapsed) {
      st.cooldownUntilMs = Math.max(st.cooldownUntilMs, now + event.cooldown_duration_ms - elapsed);
    }
    if (!event.circuit_id || !Number.isFinite(event.issued_at_ms)) return;
    if (event.state === "CLOSED") {
      this.rememberClosed(st, event.circuit_id);
      if (event.circuit_id === st.circuitId && st.state !== "CLOSED") this.closeCircuit(st);
    } else if (
      event.state === "OPEN" &&
      event.circuit_id !== st.circuitId &&
      !st.closedCircuits?.has(event.circuit_id) &&
      (event.issued_at_ms > st.circuitAt ||
        (event.issued_at_ms === st.circuitAt && event.circuit_id > (st.circuitId ?? ""))) &&
      event.open_duration_ms > elapsed
    ) {
      st.generation++;
      st.state = "OPEN";
      st.openUntilMs = now + event.open_duration_ms - elapsed;
      st.probe = undefined;
      st.circuitId = event.circuit_id;
      st.circuitAt = event.issued_at_ms;
    }
  }

  selectCandidates<T extends RoutableCandidate>(
    modelKey: string,
    candidates: readonly T[],
    strategy: "WEIGHTED" | "ROUND_ROBIN" | "LEAST_LATENCY",
    operation: Operation,
    adaptive?: AdaptiveOrdering<T>,
  ): { route: T; probe?: symbol | undefined }[] {
    const eligible: {
      route: T;
      probe?: symbol | undefined;
      st: InternalChannelState | undefined;
    }[] = [];
    let firstPriority: number | undefined;
    let samePriority = true;
    let hasSameProto = false;
    let hasCrossProto = false;
    for (let i = 0; i < candidates.length; i++) {
      const c = candidates[i]!;
      if (!c.enabled || !c.channel_enabled || !c.account_active) continue;
      if (!c.operations.includes(operation)) continue;
      const avail = this.isAvailable(c.channel_id);
      if (!avail.available) continue;
      if (firstPriority === undefined) firstPriority = c.priority;
      else if (c.priority !== firstPriority) samePriority = false;
      if (c.same_protocol === true) hasSameProto = true;
      else hasCrossProto = true;
      eligible.push({ route: c, probe: avail.probe, st: this.channels.get(c.channel_id) });
    }
    if (eligible.length === 0) return [];
    if (eligible.length === 1) {
      return [{ route: eligible[0]!.route, probe: eligible[0]!.probe }];
    }

    const ordered: { route: T; probe?: symbol | undefined }[] = [];

    const orderSubgroup = (
      p: number,
      suffix: string,
      group: { route: T; probe?: symbol | undefined; st: InternalChannelState | undefined }[],
    ) => {
      if (group.length === 0) return;
      if (group.length === 1) {
        ordered.push({ route: group[0]!.route, probe: group[0]!.probe });
        return;
      }
      if (strategy === "LEAST_LATENCY") {
        group.sort((a, b) => (a.st?.ewmaLatencyMs ?? 100) - (b.st?.ewmaLatencyMs ?? 100));
        for (let i = 0; i < group.length; i++) {
          const item = group[i]!;
          ordered.push({ route: item.route, probe: item.probe });
        }
      } else if (strategy === "ROUND_ROBIN") {
        const cursorKey = `${modelKey}:${p}${suffix}`;
        const cursor = this.rrCursors.get(cursorKey) ?? 0;
        this.rrCursors.set(cursorKey, (cursor + 1) % group.length);
        for (let i = 0; i < group.length; i++) {
          const item = group[(cursor + i) % group.length]!;
          ordered.push({ route: item.route, probe: item.probe });
        }
      } else {
        const remaining = [...group];
        const factors = adaptive?.factors(group.map((item) => item.route));
        const scoredWeights =
          factors && group.map((item) => item.route.weight * (factors.get(item.route.id) ?? 0));
        const adaptiveWeights = scoredWeights?.every(
          (weight) => Number.isFinite(weight) && weight > 0,
        )
          ? scoredWeights
          : undefined;
        const activeWeights = adaptive?.mode === "active" ? adaptiveWeights : undefined;
        while (remaining.length > 0) {
          let totalWeight = 0;
          const weights =
            activeWeights ??
            remaining.map((item) => {
              const w = item.route.weight * (item.st ? this.healthFactor(item.st) : 1);
              totalWeight += w;
              return w;
            });
          if (activeWeights) totalWeight = activeWeights.reduce((sum, weight) => sum + weight, 0);
          const draw = this.random();
          const chosenIdx = pickWeighted(weights, totalWeight, draw);
          if (
            adaptive?.mode === "observe" &&
            adaptiveWeights &&
            remaining.length === group.length
          ) {
            const adaptiveTotal = adaptiveWeights.reduce((sum, weight) => sum + weight, 0);
            adaptive.compare(chosenIdx !== pickWeighted(adaptiveWeights, adaptiveTotal, draw));
          }
          activeWeights?.splice(chosenIdx, 1);
          const [chosen] = remaining.splice(chosenIdx, 1);
          ordered.push({ route: chosen!.route, probe: chosen!.probe });
        }
      }
    };

    if (samePriority) {
      if (hasSameProto && hasCrossProto) {
        const sameProto: typeof eligible = [];
        const crossProto: typeof eligible = [];
        for (let i = 0; i < eligible.length; i++) {
          const item = eligible[i]!;
          if (item.route.same_protocol === true) sameProto.push(item);
          else crossProto.push(item);
        }
        orderSubgroup(firstPriority!, ":same", sameProto);
        orderSubgroup(firstPriority!, ":cross", crossProto);
      } else {
        orderSubgroup(firstPriority!, "", eligible);
      }
      return ordered;
    }

    const byPriority = new Map<number, typeof eligible>();
    for (const e of eligible) {
      const group = byPriority.get(e.route.priority);
      if (group) group.push(e);
      else byPriority.set(e.route.priority, [e]);
    }
    const priorities = [...byPriority.keys()].sort((a, b) => b - a);

    for (const p of priorities) {
      const group = byPriority.get(p)!;
      const sameProto = group.filter((item) => item.route.same_protocol === true);
      if (sameProto.length > 0 && sameProto.length < group.length) {
        const crossProto = group.filter((item) => item.route.same_protocol !== true);
        orderSubgroup(p, ":same", sameProto);
        orderSubgroup(p, ":cross", crossProto);
      } else {
        orderSubgroup(p, "", group);
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
        probe_in_flight: this.probeIsCurrent(st, st.probe),
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
