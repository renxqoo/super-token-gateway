import { RedisClient } from "bun";
import { LocalRateLimiter, type CircuitState } from "./router.ts";

export type ClusterCircuitEvent = {
  node_id: string;
  channel_id: string;
  state: CircuitState;
  open_duration_ms: number;
  cooldown_duration_ms: number;
};

export type ClusterEpochEvent = {
  node_id: string;
  epoch: number;
};

export type ClusterCoordinatorOptions = {
  redisUrl?: string;
  sentinel?: { urls: readonly string[]; master: string };
  namespace?: string;
  nodeId?: string;
  onCircuitEvent?: (event: ClusterCircuitEvent) => void;
  onConfigEpoch?: (event: ClusterEpochEvent) => void;
};

const BATCH_ACQUIRE_LUA = `
local n = #KEYS / 3
local time = redis.call('TIME')
local nowMs = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local out = {}
for i = 1, n do
  local ki = (i - 1) * 3
  local ai = (i - 1) * 6
  local rpmKey = KEYS[ki + 1]
  local tpmKey = KEYS[ki + 2]
  local concKey = KEYS[ki + 3]
  local rpmLimit = tonumber(ARGV[ai + 1])
  local tpmLimit = tonumber(ARGV[ai + 2])
  local concLimit = tonumber(ARGV[ai + 3])
  local estTokens = tonumber(ARGV[ai + 4])
  local leaseTtlMs = tonumber(ARGV[ai + 5])
  local member = ARGV[ai + 6]

  redis.call('ZREMRANGEBYSCORE', concKey, '-inf', nowMs)
  local active = redis.call('ZCARD', concKey)
  if active >= concLimit then
    out[i] = 3
  else
    local curRpm = tonumber(redis.call('GET', rpmKey) or '0')
    if curRpm >= rpmLimit then
      out[i] = 1
    else
      local curTpm = tonumber(redis.call('GET', tpmKey) or '0')
      if curTpm + estTokens > tpmLimit then
        out[i] = 2
      else
        local nextRpm = redis.call('INCR', rpmKey)
        if nextRpm == 1 then
          redis.call('PEXPIRE', rpmKey, 60000)
        end
        local nextTpm = redis.call('INCRBY', tpmKey, estTokens)
        if nextTpm == estTokens then
          redis.call('PEXPIRE', tpmKey, 60000)
        end
        redis.call('ZADD', concKey, nowMs + leaseTtlMs, member)
        local latest = redis.call('ZREVRANGE', concKey, 0, 0, 'WITHSCORES')
        redis.call('PEXPIREAT', concKey, math.ceil(tonumber(latest[2])))
        out[i] = 0
      end
    end
  end
end
return out
`;

const RENEW_LUA = `
local key = KEYS[1]
local member = ARGV[1]
local ttlMs = tonumber(ARGV[2])
local time = redis.call('TIME')
local nowMs = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local score = redis.call('ZSCORE', key, member)
if not score or tonumber(score) <= nowMs then return 0 end
redis.call('ZADD', key, 'XX', nowMs + ttlMs, member)
local latest = redis.call('ZREVRANGE', key, 0, 0, 'WITHSCORES')
redis.call('PEXPIREAT', key, math.ceil(tonumber(latest[2])))
return 1
`;

const batchAcquireSha = new Bun.CryptoHasher("sha1").update(BATCH_ACQUIRE_LUA).digest("hex");
const renewSha = new Bun.CryptoHasher("sha1").update(RENEW_LUA).digest("hex");

async function evalPermit(
  redis: RedisClient,
  script: string,
  sha: string,
  args: string[],
): Promise<unknown> {
  try {
    return await redis.send("EVALSHA", [sha, ...args]);
  } catch (error) {
    if (!(error instanceof Error) || !/^NOSCRIPT\b/.test(error.message)) throw error;
    return redis.send("EVAL", [script, ...args]);
  }
}

async function withRedisDeadline<T>(work: Promise<T>, timeoutMs: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error("Redis request timed out")), timeoutMs);
    timer.unref();
  });
  try {
    return await Promise.race([work, deadline]);
  } finally {
    clearTimeout(timer);
  }
}

type PendingAcquire = {
  redis: RedisClient;
  rpmKey: string;
  tpmKey: string;
  concKey: string;
  rpm: string;
  tpm: string;
  concurrency: string;
  estimatedTokens: string;
  leaseTtlMs: number;
  leaseTtlStr: string;
  member: string;
  onRevoke?: () => void;
  resolve: (result: RateLimitAcquireResult) => void;
};

type PendingRelease = {
  redis: RedisClient;
  concKey: string;
  member: string;
};

type SweepPermit = {
  redis: RedisClient;
  concKey: string;
  member: string;
  leaseTtlStr: string;
  renewEveryMs: number;
  nextRenewAtMs: number;
  renewing: boolean;
  released: boolean;
  revoke: () => void;
};

export type RateLimitAcquireResult =
  | { ok: true; release: () => void; signal?: AbortSignal }
  | { ok: false; reason: "rpm" | "tpm" | "concurrency" | "unavailable" };

const ACQUIRE_BATCH_MAX = 64;
const RELEASE_BATCH_MAX = 128;

export class ClusterCoordinator {
  readonly nodeId: string;
  private readonly instanceTag = crypto.randomUUID().slice(0, 8);
  private readonly ns: string;
  private readonly localLimiter = new LocalRateLimiter();
  private readonly redisUrl: string | undefined;
  private readonly onCircuitEvent: ClusterCoordinatorOptions["onCircuitEvent"];
  private readonly onConfigEpoch: ClusterCoordinatorOptions["onConfigEpoch"];
  private client: RedisClient | null = null;
  private subscriber: RedisClient | null = null;
  private connected = false;
  private closed = false;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private sentinelTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly sentinel: ClusterCoordinatorOptions["sentinel"];
  private readonly permits = new Set<() => void>();
  private readonly sweepPermits = new Set<SweepPermit>();
  private sweepTimer: ReturnType<typeof setInterval> | undefined;
  private permitSeq = 0;
  private pendingAcquires: PendingAcquire[] = [];
  private acquireTimer: ReturnType<typeof setTimeout> | undefined;
  private acquireMicrotask = false;
  private pendingReleases: PendingRelease[] = [];
  private releaseTimer: ReturnType<typeof setTimeout> | undefined;
  private releaseMicrotask = false;

  constructor(options: ClusterCoordinatorOptions = {}) {
    this.redisUrl = options.redisUrl;
    this.sentinel = options.sentinel;
    this.ns = options.namespace ?? "stg";
    this.nodeId = options.nodeId ?? crypto.randomUUID();
    this.onCircuitEvent = options.onCircuitEvent;
    this.onConfigEpoch = options.onConfigEpoch;
  }

  isRedisConnected(): boolean {
    return this.connected && Boolean(this.client?.connected);
  }

  private circuitTopic(): string {
    return `${this.ns}:circuit`;
  }

  private epochTopic(): string {
    return `${this.ns}:epoch`;
  }

  async start(): Promise<void> {
    this.closed = false;
    await this.connect();
  }

  private scheduleReconnect(): void {
    if (this.closed || this.retryTimer) return;
    this.retryTimer = setTimeout(
      () => {
        this.retryTimer = undefined;
        void this.connect();
      },
      1000 + Math.random() * 500,
    );
    this.retryTimer.unref();
  }

  private failPendingFor(client: RedisClient): void {
    if (this.pendingAcquires.length > 0) {
      const remaining: PendingAcquire[] = [];
      for (const item of this.pendingAcquires) {
        if (item.redis === client) item.resolve({ ok: false, reason: "unavailable" });
        else remaining.push(item);
      }
      this.pendingAcquires = remaining;
    }
    if (this.pendingReleases.length > 0) {
      this.pendingReleases = this.pendingReleases.filter((item) => item.redis !== client);
    }
  }

  private stopSweep(): void {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = undefined;
    }
  }

  private ensureSweepTimer(): void {
    if (this.sweepTimer) return;
    this.sweepTimer = setInterval(() => {
      if (this.sweepPermits.size === 0) return;
      const now = performance.now();
      for (const permit of this.sweepPermits) {
        if (permit.released || permit.renewing || now < permit.nextRenewAtMs) continue;
        permit.renewing = true;
        void withRedisDeadline(
          evalPermit(permit.redis, RENEW_LUA, renewSha, [
            "1",
            permit.concKey,
            permit.member,
            permit.leaseTtlStr,
          ]),
          Math.min(1000, permit.renewEveryMs),
        )
          .then((renewed) => {
            permit.renewing = false;
            if (permit.released) return;
            if (Number(renewed) !== 1) {
              permit.revoke();
            } else {
              permit.nextRenewAtMs = performance.now() + permit.renewEveryMs;
            }
          })
          .catch(() => {
            permit.renewing = false;
            if (!permit.released) permit.revoke();
          });
      }
    }, 500);
    this.sweepTimer.unref();
  }

  private disconnect(client: RedisClient): void {
    if (this.client !== client) return;
    this.connected = false;
    clearTimeout(this.sentinelTimer);
    if (this.acquireTimer) {
      clearTimeout(this.acquireTimer);
      this.acquireTimer = undefined;
    }
    if (this.releaseTimer) {
      clearTimeout(this.releaseTimer);
      this.releaseTimer = undefined;
    }
    const subscriber = this.subscriber;
    this.subscriber = null;
    this.client = null;
    this.failPendingFor(client);
    this.stopSweep();
    try {
      subscriber?.close();
    } catch {
      void 0;
    }
    try {
      client.close();
    } catch {
      void 0;
    }
    for (const revoke of this.permits) revoke();
    this.sweepPermits.clear();
    this.scheduleReconnect();
  }

  private async discoverRedis(template: string): Promise<string> {
    if (!this.sentinel) return template;
    const { urls, master } = this.sentinel;
    const replies = await Promise.allSettled(
      urls.map(async (url) => {
        const sentinel = new RedisClient(url, {
          connectionTimeout: 500,
          autoReconnect: false,
          enableOfflineQueue: false,
        });
        try {
          await withRedisDeadline(sentinel.connect(), 500);
          const address: unknown = await withRedisDeadline(
            sentinel.send("SENTINEL", ["get-master-addr-by-name", master]),
            500,
          );
          if (
            !Array.isArray(address) ||
            typeof address[0] !== "string" ||
            !/^[A-Za-z0-9.:-]+$/.test(address[0]) ||
            !/^\d+$/.test(String(address[1]))
          ) {
            throw new Error("Redis Sentinel returned an invalid primary address");
          }
          const primary = new URL(template);
          primary.hostname = address[0].includes(":") ? `[${address[0]}]` : address[0];
          primary.port = String(address[1]);
          if (
            primary.port !== String(address[1]) ||
            Number(primary.port) > 65535 ||
            Number(primary.port) < 1
          ) {
            throw new Error("Redis Sentinel returned an invalid primary port");
          }
          return primary.toString();
        } finally {
          sentinel.close();
        }
      }),
    );
    const votes = new Map<string, number>();
    for (const reply of replies) {
      if (reply.status !== "fulfilled") continue;
      const count = (votes.get(reply.value) ?? 0) + 1;
      if (count > urls.length / 2) return reply.value;
      votes.set(reply.value, count);
    }
    throw new Error("Configured Redis Sentinels disagree on the primary");
  }

  private monitorPrimary(client: RedisClient, primary: string): void {
    if (!this.sentinel || !this.redisUrl) return;
    const template = this.redisUrl;
    this.sentinelTimer = setTimeout(async () => {
      if (this.closed || this.client !== client) return;
      try {
        const address = await this.discoverRedis(template);
        if (address !== primary) return this.disconnect(client);
      } catch {
        return this.disconnect(client);
      }
      if (!this.closed && this.client === client) this.monitorPrimary(client, primary);
    }, 2000);
    this.sentinelTimer.unref();
  }

  private async connect(): Promise<void> {
    if (!this.redisUrl || this.closed) return;
    let client: RedisClient | undefined;
    let sub: RedisClient | null = null;
    try {
      const primary = await this.discoverRedis(this.redisUrl);
      if (this.closed) return;
      const connecting = new RedisClient(primary, {
        connectionTimeout: 2000,
        autoReconnect: false,
        enableOfflineQueue: false,
      });
      client = connecting;
      await withRedisDeadline(connecting.connect(), 2000);
      if (this.sentinel) {
        const role: unknown = await withRedisDeadline(connecting.send("ROLE", []), 1000);
        if (!Array.isArray(role) || role[0] !== "master") {
          throw new Error("Discovered Redis node is not writable");
        }
      }
      sub = await withRedisDeadline(connecting.duplicate(), 2000);
      await withRedisDeadline(
        sub.subscribe(this.circuitTopic(), (message) => {
          try {
            const parsed = JSON.parse(message) as ClusterCircuitEvent;
            if (parsed.node_id && parsed.node_id !== this.nodeId) {
              this.onCircuitEvent?.(parsed);
            }
          } catch {
            void 0;
          }
        }),
        2000,
      );
      await withRedisDeadline(
        sub.subscribe(this.epochTopic(), (message) => {
          try {
            const parsed = JSON.parse(message) as ClusterEpochEvent;
            if (parsed.node_id && parsed.node_id !== this.nodeId) {
              this.onConfigEpoch?.(parsed);
            }
          } catch {
            void 0;
          }
        }),
        2000,
      );
      if (this.closed) {
        sub.close();
        client.close();
        return;
      }
      this.client = client;
      this.subscriber = sub;
      this.connected = true;
      connecting.onclose = () => this.disconnect(connecting);
      sub.onclose = () => this.disconnect(connecting);
      if (!connecting.connected || !sub.connected) {
        this.disconnect(connecting);
        return;
      }
      this.monitorPrimary(connecting, primary);
    } catch {
      sub?.close();
      client?.close();
      this.scheduleReconnect();
    }
  }

  private scheduleReleaseFlush(immediate: boolean): void {
    if (immediate) {
      if (this.releaseTimer) {
        clearTimeout(this.releaseTimer);
        this.releaseTimer = undefined;
      }
      if (!this.releaseMicrotask) {
        this.releaseMicrotask = true;
        queueMicrotask(() => {
          this.releaseMicrotask = false;
          this.flushReleases();
        });
      }
      return;
    }
    if (this.releaseTimer || this.releaseMicrotask) return;
    this.releaseTimer = setTimeout(() => {
      this.releaseTimer = undefined;
      this.flushReleases();
    }, 1);
    this.releaseTimer.unref();
  }

  private enqueueRelease(
    redis: RedisClient,
    concKey: string,
    member: string,
    immediate: boolean,
  ): void {
    if (!redis.connected || this.closed || this.client !== redis) return;
    this.pendingReleases.push({ redis, concKey, member });
    if (this.pendingReleases.length >= RELEASE_BATCH_MAX) {
      if (this.releaseTimer) {
        clearTimeout(this.releaseTimer);
        this.releaseTimer = undefined;
      }
      this.flushReleases();
      return;
    }
    this.scheduleReleaseFlush(immediate || this.permits.size < 4);
  }

  private flushReleases(): void {
    if (this.pendingReleases.length === 0) return;
    const batch = this.pendingReleases.splice(0, RELEASE_BATCH_MAX);
    if (this.pendingReleases.length > 0) {
      this.scheduleReleaseFlush(true);
    }
    if (batch.length === 1) {
      const only = batch[0]!;
      if (only.redis.connected) {
        void only.redis.send("ZREM", [only.concKey, only.member]).catch(() => undefined);
      }
      return;
    }
    const byClient = new Map<RedisClient, Map<string, string[]>>();
    for (const item of batch) {
      if (!item.redis.connected) continue;
      let byKey = byClient.get(item.redis);
      if (!byKey) {
        byKey = new Map<string, string[]>();
        byClient.set(item.redis, byKey);
      }
      let args = byKey.get(item.concKey);
      if (!args) {
        args = [item.concKey];
        byKey.set(item.concKey, args);
      }
      args.push(item.member);
    }
    for (const [redis, byKey] of byClient) {
      for (const args of byKey.values()) {
        void redis.send("ZREM", args).catch(() => undefined);
      }
    }
  }

  private registerPermit(
    redis: RedisClient,
    concKey: string,
    member: string,
    leaseTtlMs: number,
    leaseTtlStr: string,
    onRevoke?: () => void,
  ): RateLimitAcquireResult {
    const revoked = onRevoke ? null : new AbortController();
    const renewEveryMs = Math.max(1, Math.floor(leaseTtlMs / 3));
    if (leaseTtlMs >= 3000) {
      let revokeFn: () => void = () => {};
      const entry: SweepPermit = {
        redis,
        concKey,
        member,
        leaseTtlStr,
        renewEveryMs,
        nextRenewAtMs: performance.now() + renewEveryMs,
        renewing: false,
        released: false,
        revoke: () => revokeFn(),
      };
      const release = () => {
        if (entry.released) return;
        entry.released = true;
        this.permits.delete(revokeFn);
        this.sweepPermits.delete(entry);
        this.enqueueRelease(redis, concKey, member, false);
      };
      revokeFn = () => {
        if (entry.released) return;
        release();
        if (onRevoke) onRevoke();
        else revoked!.abort(new Error("Shared concurrency permit could not be renewed"));
      };
      this.permits.add(revokeFn);
      this.sweepPermits.add(entry);
      this.ensureSweepTimer();
      return revoked ? { ok: true, release, signal: revoked.signal } : { ok: true, release };
    }
    let released = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const release = () => {
      if (released) return;
      released = true;
      clearTimeout(timer);
      this.permits.delete(revoke);
      this.enqueueRelease(redis, concKey, member, true);
    };
    const revoke = () => {
      if (released) return;
      release();
      if (onRevoke) onRevoke();
      else revoked!.abort(new Error("Shared concurrency permit could not be renewed"));
    };
    const renew = async () => {
      if (released) return;
      try {
        const renewed = await withRedisDeadline(
          evalPermit(redis, RENEW_LUA, renewSha, ["1", concKey, member, leaseTtlStr]),
          Math.min(1000, renewEveryMs),
        );
        if (Number(renewed) !== 1) {
          revoke();
        } else if (!released) {
          timer = setTimeout(renew, renewEveryMs);
          timer.unref();
        }
      } catch {
        revoke();
      }
    };
    this.permits.add(revoke);
    timer = setTimeout(renew, renewEveryMs);
    timer.unref();
    return revoked ? { ok: true, release, signal: revoked.signal } : { ok: true, release };
  }

  private scheduleAcquireFlush(immediate: boolean): void {
    if (immediate) {
      if (this.acquireTimer) {
        clearTimeout(this.acquireTimer);
        this.acquireTimer = undefined;
      }
      if (!this.acquireMicrotask) {
        this.acquireMicrotask = true;
        queueMicrotask(() => {
          this.acquireMicrotask = false;
          this.flushAcquires();
        });
      }
      return;
    }
    if (this.acquireTimer || this.acquireMicrotask) return;
    this.acquireTimer = setTimeout(() => {
      this.acquireTimer = undefined;
      this.flushAcquires();
    }, 1);
    this.acquireTimer.unref();
  }

  private flushAcquires(): void {
    if (this.pendingAcquires.length === 0) return;
    if (this.pendingReleases.length > 0) {
      if (this.releaseTimer) {
        clearTimeout(this.releaseTimer);
        this.releaseTimer = undefined;
      }
      while (this.pendingReleases.length > 0) {
        this.flushReleases();
      }
    }
    const batch = this.pendingAcquires.splice(0, ACQUIRE_BATCH_MAX);
    if (this.pendingAcquires.length > 0) {
      this.scheduleAcquireFlush(true);
    }
    const groups = new Map<RedisClient, PendingAcquire[]>();
    for (const item of batch) {
      if (this.closed || this.client !== item.redis || !this.connected || !item.redis.connected) {
        item.resolve({ ok: false, reason: "unavailable" });
        continue;
      }
      let list = groups.get(item.redis);
      if (!list) {
        list = [];
        groups.set(item.redis, list);
      }
      list.push(item);
    }
    for (const [redis, items] of groups) {
      void this.runAcquireBatch(redis, items);
    }
  }

  private async runAcquireBatch(redis: RedisClient, items: PendingAcquire[]): Promise<void> {
    const n = items.length;
    const args: string[] = [String(n * 3)];
    for (let i = 0; i < n; i++) {
      const it = items[i]!;
      args.push(it.rpmKey, it.tpmKey, it.concKey);
    }
    for (let i = 0; i < n; i++) {
      const it = items[i]!;
      args.push(it.rpm, it.tpm, it.concurrency, it.estimatedTokens, it.leaseTtlStr, it.member);
    }
    try {
      const raw = await withRedisDeadline(
        evalPermit(redis, BATCH_ACQUIRE_LUA, batchAcquireSha, args),
        1000,
      );
      if (!Array.isArray(raw) || raw.length !== n) {
        throw new Error("Invalid Redis rate limit reply");
      }
      const disconnected = this.closed || this.client !== redis || !this.connected;
      for (let i = 0; i < n; i++) {
        const it = items[i]!;
        const code = Number(raw[i]);
        if (code === 1) {
          it.resolve({ ok: false, reason: "rpm" });
        } else if (code === 2) {
          it.resolve({ ok: false, reason: "tpm" });
        } else if (code === 3) {
          it.resolve({ ok: false, reason: "concurrency" });
        } else if (code !== 0) {
          it.resolve({ ok: false, reason: "unavailable" });
        } else if (disconnected) {
          void redis.send("ZREM", [it.concKey, it.member]).catch(() => undefined);
          it.resolve({ ok: false, reason: "unavailable" });
        } else {
          it.resolve(
            this.registerPermit(
              redis,
              it.concKey,
              it.member,
              it.leaseTtlMs,
              it.leaseTtlStr,
              it.onRevoke,
            ),
          );
        }
      }
    } catch {
      this.disconnect(redis);
      for (const it of items) it.resolve({ ok: false, reason: "unavailable" });
    }
  }

  acquireRateLimit(
    key: string,
    limits: { rpm: number; tpm: number; concurrency: number },
    estimatedTokens: number,
    leaseTtlMs = 120_000,
    onRevoke?: () => void,
  ): Promise<RateLimitAcquireResult> {
    if (!this.redisUrl) {
      return Promise.resolve(this.localLimiter.acquire(key, limits, estimatedTokens));
    }
    const redis = this.client;
    if (!redis?.connected || !this.connected || this.closed) {
      return Promise.resolve({ ok: false, reason: "unavailable" });
    }
    const rpmKey = `${this.ns}:rl:rpm:${key}`;
    const tpmKey = `${this.ns}:rl:tpm:${key}`;
    const concKey = `${this.ns}:rl:conc:${key}`;
    const member = `${this.nodeId}:${this.instanceTag}:${++this.permitSeq}`;
    const safeTpmLimit = Math.min(limits.tpm, Number.MAX_SAFE_INTEGER);
    return new Promise<RateLimitAcquireResult>((resolve) => {
      this.pendingAcquires.push({
        redis,
        rpmKey,
        tpmKey,
        concKey,
        rpm: String(limits.rpm),
        tpm: String(safeTpmLimit),
        concurrency: String(limits.concurrency),
        estimatedTokens: String(estimatedTokens),
        leaseTtlMs,
        leaseTtlStr: String(leaseTtlMs),
        member,
        ...(onRevoke ? { onRevoke } : {}),
        resolve,
      });
      if (this.pendingAcquires.length >= ACQUIRE_BATCH_MAX) {
        if (this.acquireTimer) {
          clearTimeout(this.acquireTimer);
          this.acquireTimer = undefined;
        }
        this.flushAcquires();
        return;
      }
      this.scheduleAcquireFlush(leaseTtlMs < 3000 || this.permits.size < 4);
    });
  }

  publishCircuitEvent(event: Omit<ClusterCircuitEvent, "node_id">): void {
    if (!this.client || !this.connected) return;
    const payload: ClusterCircuitEvent = { node_id: this.nodeId, ...event };
    void this.client.publish(this.circuitTopic(), JSON.stringify(payload)).catch(() => undefined);
  }

  publishConfigEpoch(epoch: number): void {
    if (!this.client || !this.connected) return;
    const payload: ClusterEpochEvent = { node_id: this.nodeId, epoch };
    void this.client.publish(this.epochTopic(), JSON.stringify(payload)).catch(() => undefined);
  }

  async close(): Promise<void> {
    this.connected = false;
    this.closed = true;
    clearTimeout(this.retryTimer);
    clearTimeout(this.sentinelTimer);
    if (this.acquireTimer) {
      clearTimeout(this.acquireTimer);
      this.acquireTimer = undefined;
    }
    if (this.releaseTimer) {
      clearTimeout(this.releaseTimer);
      this.releaseTimer = undefined;
    }
    this.stopSweep();
    for (const item of this.pendingAcquires) item.resolve({ ok: false, reason: "unavailable" });
    this.pendingAcquires = [];
    this.pendingReleases = [];
    for (const revoke of this.permits) revoke();
    this.sweepPermits.clear();
    const sub = this.subscriber;
    const cli = this.client;
    this.subscriber = null;
    this.client = null;
    try {
      sub?.close();
    } catch {
      void 0;
    }
    try {
      cli?.close();
    } catch {
      void 0;
    }
  }
}
