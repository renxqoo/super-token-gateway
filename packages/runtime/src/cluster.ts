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
local concCount = {}
local concAdds = {}
local rpmCur = {}
local rpmBase = {}
local rpmDelta = {}
local tpmCur = {}
local tpmBase = {}
local tpmDelta = {}
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

  local active = concCount[concKey]
  if active == nil then
    redis.call('ZREMRANGEBYSCORE', concKey, '-inf', nowMs)
    active = redis.call('ZCARD', concKey)
    concCount[concKey] = active
  end
  if active >= concLimit then
    out[i] = 3
  else
    local curRpm = rpmCur[rpmKey]
    if curRpm == nil then
      curRpm = tonumber(redis.call('GET', rpmKey) or '0')
      rpmCur[rpmKey] = curRpm
      rpmBase[rpmKey] = curRpm
    end
    if curRpm >= rpmLimit then
      out[i] = 1
    else
      local curTpm = tpmCur[tpmKey]
      if curTpm == nil then
        curTpm = tonumber(redis.call('GET', tpmKey) or '0')
        tpmCur[tpmKey] = curTpm
        tpmBase[tpmKey] = curTpm
      end
      if curTpm + estTokens > tpmLimit then
        out[i] = 2
      else
        rpmCur[rpmKey] = curRpm + 1
        rpmDelta[rpmKey] = (rpmDelta[rpmKey] or 0) + 1
        tpmCur[tpmKey] = curTpm + estTokens
        tpmDelta[tpmKey] = (tpmDelta[tpmKey] or 0) + estTokens
        concCount[concKey] = active + 1
        local exp = nowMs + leaseTtlMs
        local adds = concAdds[concKey]
        if adds == nil then
          concAdds[concKey] = { exp, member }
        else
          local len = #adds
          adds[len + 1] = exp
          adds[len + 2] = member
        end
        out[i] = 0
      end
    end
  end
end
for k, delta in pairs(rpmDelta) do
  local nextVal = redis.call('INCRBY', k, delta)
  if nextVal == delta or rpmBase[k] == 0 then
    redis.call('PEXPIRE', k, 60000)
  end
end
for k, delta in pairs(tpmDelta) do
  if delta > 0 then
    local nextVal = redis.call('INCRBY', k, delta)
    if nextVal == delta or tpmBase[k] == 0 then
      redis.call('PEXPIRE', k, 60000)
    end
  end
end
for k, adds in pairs(concAdds) do
  redis.call('ZADD', k, unpack(adds))
  local latest = redis.call('ZREVRANGE', k, 0, 0, 'WITHSCORES')
  if latest[2] then
    redis.call('PEXPIREAT', k, math.ceil(tonumber(latest[2])))
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

async function evalPermitPrepared(
  redis: RedisClient,
  script: string,
  shaArgs: string[],
): Promise<unknown> {
  try {
    return await redis.send("EVALSHA", shaArgs);
  } catch (error) {
    if (!(error instanceof Error) || !/^NOSCRIPT\b/.test(error.message)) throw error;
    const evalArgs = shaArgs.slice();
    evalArgs[0] = script;
    return redis.send("EVAL", evalArgs);
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

type KeyTriple = {
  rpmKey: string;
  tpmKey: string;
  concKey: string;
};

type PendingAcquire = {
  redis: RedisClient;
  keys: KeyTriple;
  rpm: string;
  tpm: string;
  concurrency: string;
  estimatedTokens: string;
  leaseTtlMs: number;
  leaseTtlStr: string;
  member: string;
  onRevoke: (() => void) | undefined;
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
  onRevoke: (() => void) | undefined;
  revoked: AbortController | null;
};

export type RateLimitAcquireResult =
  | { ok: true; release: () => void; signal?: AbortSignal }
  | { ok: false; reason: "rpm" | "tpm" | "concurrency" | "unavailable" };

const ACQUIRE_BATCH_MAX = 96;
const RELEASE_BATCH_MAX = 512;
const KEY_CACHE_MAX = 8192;

export class ClusterCoordinator {
  readonly nodeId: string;
  private readonly memberPrefix: string;
  private readonly ns: string;
  private readonly localLimiter = new LocalRateLimiter();
  private readonly redisUrl: string | undefined;
  private readonly onCircuitEvent: ClusterCoordinatorOptions["onCircuitEvent"];
  private readonly onConfigEpoch: ClusterCoordinatorOptions["onConfigEpoch"];
  private readonly keyCache = new Map<string, KeyTriple>();
  private client: RedisClient | null = null;
  private subscriber: RedisClient | null = null;
  private connected = false;
  private closed = false;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private sentinelTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly sentinel: ClusterCoordinatorOptions["sentinel"];
  private readonly shortPermits = new Set<() => void>();
  private readonly sweepPermits = new Set<SweepPermit>();
  private sweepTimer: ReturnType<typeof setInterval> | undefined;
  private permitSeq = 0;
  private pendingAcquires: PendingAcquire[] = [];
  private acquireTimer: ReturnType<typeof setTimeout> | undefined;
  private acquireMicrotask = false;
  private acquireInFlight = 0;
  private pendingReleases: PendingRelease[] = [];
  private releaseTimer: ReturnType<typeof setTimeout> | undefined;
  private releaseMicrotask = false;

  constructor(options: ClusterCoordinatorOptions = {}) {
    this.redisUrl = options.redisUrl;
    this.sentinel = options.sentinel;
    this.ns = options.namespace ?? "stg";
    this.nodeId = options.nodeId ?? crypto.randomUUID();
    this.memberPrefix = `${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}:`;
    this.onCircuitEvent = options.onCircuitEvent;
    this.onConfigEpoch = options.onConfigEpoch;
  }

  private getKeyTriple(key: string): KeyTriple {
    let entry = this.keyCache.get(key);
    if (!entry) {
      if (this.keyCache.size >= KEY_CACHE_MAX) this.keyCache.clear();
      entry = {
        rpmKey: `${this.ns}:rl:rpm:${key}`,
        tpmKey: `${this.ns}:rl:tpm:${key}`,
        concKey: `${this.ns}:rl:conc:${key}`,
      };
      this.keyCache.set(key, entry);
    }
    return entry;
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

  private releaseSweepPermit(entry: SweepPermit): void {
    if (entry.released) return;
    entry.released = true;
    this.sweepPermits.delete(entry);
    this.enqueueRelease(entry.redis, entry.concKey, entry.member, false);
  }

  private revokeSweepPermit(entry: SweepPermit): void {
    if (entry.released) return;
    this.releaseSweepPermit(entry);
    if (entry.onRevoke) entry.onRevoke();
    else entry.revoked?.abort(new Error("Shared concurrency permit could not be renewed"));
  }

  private ensureSweepTimer(): void {
    if (this.sweepTimer) return;
    this.sweepTimer = setInterval(() => {
      if (this.sweepPermits.size === 0) return;
      const now = Date.now();
      for (const permit of this.sweepPermits) {
        if (permit.released || permit.renewing || now < permit.nextRenewAtMs) continue;
        permit.renewing = true;
        void withRedisDeadline(
          evalPermitPrepared(permit.redis, RENEW_LUA, [
            renewSha,
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
              this.revokeSweepPermit(permit);
            } else {
              permit.nextRenewAtMs = Date.now() + permit.renewEveryMs;
            }
          })
          .catch(() => {
            permit.renewing = false;
            if (!permit.released) this.revokeSweepPermit(permit);
          });
      }
    }, 500);
    this.sweepTimer.unref();
  }

  private disconnect(client: RedisClient): void {
    if (this.client !== client) return;
    this.connected = false;
    clearTimeout(this.sentinelTimer);
    this.sentinelTimer = undefined;
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
    for (const revoke of this.shortPermits) revoke();
    this.shortPermits.clear();
    const activeSweep = [...this.sweepPermits];
    this.sweepPermits.clear();
    for (const entry of activeSweep) this.revokeSweepPermit(entry);
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
      await withRedisDeadline(
        Promise.all([
          connecting.send("SCRIPT", ["LOAD", BATCH_ACQUIRE_LUA]),
          connecting.send("SCRIPT", ["LOAD", RENEW_LUA]),
        ]),
        2000,
      );
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
    this.scheduleReleaseFlush(immediate || this.sweepPermits.size + this.shortPermits.size < 4);
  }

  private flushReleases(): void {
    const total = this.pendingReleases.length;
    if (total === 0) return;
    const batch =
      total <= RELEASE_BATCH_MAX
        ? this.pendingReleases
        : this.pendingReleases.splice(0, RELEASE_BATCH_MAX);
    if (total <= RELEASE_BATCH_MAX) this.pendingReleases = [];
    else this.scheduleReleaseFlush(true);

    if (batch.length === 1) {
      const only = batch[0]!;
      if (only.redis.connected) {
        void only.redis.send("ZREM", [only.concKey, only.member]).catch(() => undefined);
      }
      return;
    }

    const firstRedis = batch[0]!.redis;
    let sameClient = true;
    for (let i = 1; i < batch.length; i++) {
      if (batch[i]!.redis !== firstRedis) {
        sameClient = false;
        break;
      }
    }
    if (sameClient) {
      if (!firstRedis.connected) return;
      const byKey = new Map<string, string[]>();
      for (let i = 0; i < batch.length; i++) {
        const item = batch[i]!;
        let args = byKey.get(item.concKey);
        if (!args) {
          args = [item.concKey];
          byKey.set(item.concKey, args);
        }
        args.push(item.member);
      }
      for (const args of byKey.values()) {
        void firstRedis.send("ZREM", args).catch(() => undefined);
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
    onRevoke: (() => void) | undefined,
    batchNowMs: number,
  ): RateLimitAcquireResult {
    const revoked = onRevoke ? null : new AbortController();
    const renewEveryMs = Math.max(1, Math.floor(leaseTtlMs / 3));
    if (leaseTtlMs >= 3000) {
      const entry: SweepPermit = {
        redis,
        concKey,
        member,
        leaseTtlStr,
        renewEveryMs,
        nextRenewAtMs: batchNowMs + renewEveryMs,
        renewing: false,
        released: false,
        onRevoke,
        revoked,
      };
      this.sweepPermits.add(entry);
      if (!this.sweepTimer) this.ensureSweepTimer();
      const release = () => this.releaseSweepPermit(entry);
      return revoked ? { ok: true, release, signal: revoked.signal } : { ok: true, release };
    }
    let released = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const release = () => {
      if (released) return;
      released = true;
      clearTimeout(timer);
      this.shortPermits.delete(revoke);
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
          evalPermitPrepared(redis, RENEW_LUA, [renewSha, "1", concKey, member, leaseTtlStr]),
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
    this.shortPermits.add(revoke);
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
    const total = this.pendingAcquires.length;
    if (total === 0) return;
    if (this.pendingReleases.length > 0) {
      if (this.releaseTimer) {
        clearTimeout(this.releaseTimer);
        this.releaseTimer = undefined;
      }
      while (this.pendingReleases.length > 0) {
        this.flushReleases();
      }
    }
    const batch =
      total <= ACQUIRE_BATCH_MAX
        ? this.pendingAcquires
        : this.pendingAcquires.splice(0, ACQUIRE_BATCH_MAX);
    if (total <= ACQUIRE_BATCH_MAX) this.pendingAcquires = [];
    else this.scheduleAcquireFlush(true);

    const activeClient = this.client;
    if (activeClient && !this.closed && this.connected && activeClient.connected) {
      let allMatch = true;
      for (let i = 0; i < batch.length; i++) {
        if (batch[i]!.redis !== activeClient) {
          allMatch = false;
          break;
        }
      }
      if (allMatch) {
        void this.runAcquireBatch(activeClient, batch);
        return;
      }
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
    this.acquireInFlight++;
    const n = items.length;
    const shaArgs: string[] = [batchAcquireSha, String(n * 3)];
    for (let i = 0; i < n; i++) {
      const k = items[i]!.keys;
      shaArgs.push(k.rpmKey, k.tpmKey, k.concKey);
    }
    for (let i = 0; i < n; i++) {
      const it = items[i]!;
      shaArgs.push(it.rpm, it.tpm, it.concurrency, it.estimatedTokens, it.leaseTtlStr, it.member);
    }
    try {
      const raw = await withRedisDeadline(
        evalPermitPrepared(redis, BATCH_ACQUIRE_LUA, shaArgs),
        1000,
      );
      if (!Array.isArray(raw) || raw.length !== n) {
        throw new Error("Invalid Redis rate limit reply");
      }
      const disconnected = this.closed || this.client !== redis || !this.connected;
      const batchNowMs = Date.now();
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
          void redis.send("ZREM", [it.keys.concKey, it.member]).catch(() => undefined);
          it.resolve({ ok: false, reason: "unavailable" });
        } else {
          it.resolve(
            this.registerPermit(
              redis,
              it.keys.concKey,
              it.member,
              it.leaseTtlMs,
              it.leaseTtlStr,
              it.onRevoke,
              batchNowMs,
            ),
          );
        }
      }
    } catch {
      this.disconnect(redis);
      for (const it of items) it.resolve({ ok: false, reason: "unavailable" });
    } finally {
      this.acquireInFlight--;
      if (this.pendingAcquires.length > 0 && !this.closed) {
        this.scheduleAcquireFlush(true);
      }
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
    const keys = this.getKeyTriple(key);
    const member = `${this.memberPrefix}${++this.permitSeq}`;
    const safeTpmLimit = Math.min(limits.tpm, Number.MAX_SAFE_INTEGER);
    return new Promise<RateLimitAcquireResult>((resolve) => {
      this.pendingAcquires.push({
        redis,
        keys,
        rpm: String(limits.rpm),
        tpm: String(safeTpmLimit),
        concurrency: String(limits.concurrency),
        estimatedTokens: String(estimatedTokens),
        leaseTtlMs,
        leaseTtlStr: String(leaseTtlMs),
        member,
        onRevoke,
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
      this.scheduleAcquireFlush(
        this.acquireInFlight < 2 ||
          leaseTtlMs < 3000 ||
          this.sweepPermits.size + this.shortPermits.size < 4,
      );
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
    this.retryTimer = undefined;
    clearTimeout(this.sentinelTimer);
    this.sentinelTimer = undefined;
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
    for (const revoke of this.shortPermits) revoke();
    this.shortPermits.clear();
    const activeSweep = [...this.sweepPermits];
    this.sweepPermits.clear();
    for (const entry of activeSweep) this.revokeSweepPermit(entry);
    const sub = this.subscriber;
    const client = this.client;
    this.subscriber = null;
    this.client = null;
    try {
      sub?.close();
    } catch {
      void 0;
    }
    try {
      client?.close();
    } catch {
      void 0;
    }
  }
}
