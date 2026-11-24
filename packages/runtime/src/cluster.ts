import { RedisClient } from "bun";
import { LocalRateLimiter, type CircuitTransition } from "./router.ts";

export type ClusterCircuitEvent = CircuitTransition & { node_id: string };

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
if latest[2] then
  redis.call('PEXPIREAT', key, math.ceil(tonumber(latest[2])))
end
return 1
`;

const BATCH_RENEW_LUA = `
local n = #KEYS
local time = redis.call('TIME')
local nowMs = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local out = {}
local touched = {}
for i = 1, n do
  local key = KEYS[i]
  local ai = (i - 1) * 2
  local member = ARGV[ai + 1]
  local ttlMs = tonumber(ARGV[ai + 2])
  local score = redis.call('ZSCORE', key, member)
  if not score or tonumber(score) <= nowMs then
    out[i] = 0
  else
    redis.call('ZADD', key, 'XX', nowMs + ttlMs, member)
    touched[key] = true
    out[i] = 1
  end
end
for key, _ in pairs(touched) do
  local latest = redis.call('ZREVRANGE', key, 0, 0, 'WITHSCORES')
  if latest[2] then
    redis.call('PEXPIREAT', key, math.ceil(tonumber(latest[2])))
  end
end
return out
`;

const batchAcquireSha = new Bun.CryptoHasher("sha1").update(BATCH_ACQUIRE_LUA).digest("hex");
const renewSha = new Bun.CryptoHasher("sha1").update(RENEW_LUA).digest("hex");
const batchRenewSha = new Bun.CryptoHasher("sha1").update(BATCH_RENEW_LUA).digest("hex");

const INGRESS_WINDOW_LUA = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local count = tonumber(redis.call('GET', key) or '0')
if not count or count < 0 or count ~= math.floor(count) then return -1 end
if count == 0 then
  redis.call('SET', key, 1, 'PX', window)
  return 0
end
local ttl = redis.call('PTTL', key)
if ttl < 0 then
  redis.call('PEXPIRE', key, window)
  ttl = window
end
if count >= limit then return math.max(1, ttl) end
redis.call('INCR', key)
return 0
`;
const ingressWindowSha = new Bun.CryptoHasher("sha1").update(INGRESS_WINDOW_LUA).digest("hex");

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
  expiresAtMs: number;
};

type OrphanedRelease = {
  concKey: string;
  member: string;
  expiresAtMs: number;
};

type ActivePermit = {
  concKey: string;
  member: string;
  leaseTtlMs: number;
  leaseTtlStr: string;
  renewEveryMs: number;
  safetyMarginMs: number;
  confirmedExpiresAtMs: number;
  nextRenewAtMs: number;
  renewing: boolean;
  released: boolean;
  onRevoke: (() => void) | undefined;
  revoked: AbortController | null;
  shortTimer?: ReturnType<typeof setTimeout> | undefined;
};

export type RateLimitAcquireResult =
  | { ok: true; release: () => void; signal?: AbortSignal }
  | { ok: false; reason: "rpm" | "tpm" | "concurrency" | "unavailable" };

const ACQUIRE_BATCH_MAX = 96;
const MAX_ACQUIRE_IN_FLIGHT = 8;
const ACQUIRE_TIMEOUT_MS = 2000;
const RENEW_BATCH_MAX = 256;
const RENEW_TIMEOUT_MS = 2000;
const RELEASE_BATCH_MAX = 512;
const ORPHAN_RELEASE_MAX = 100_000;
const KEY_CACHE_MAX = 8192;

function safetyMarginFor(leaseTtlMs: number): number {
  return Math.min(1000, Math.max(15, Math.floor(leaseTtlMs / 6)));
}

function jitteredRenewDelay(renewEveryMs: number): number {
  if (renewEveryMs < 200) return renewEveryMs;
  const factor = 0.85 + Math.random() * 0.3;
  return Math.max(50, Math.floor(renewEveryMs * factor));
}

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
  private maintClient: RedisClient | null = null;
  private subscriber: RedisClient | null = null;
  private primary: string | undefined;
  private ingressConnection: { client: RedisClient; ready: Promise<unknown> } | undefined;
  private connected = false;
  private closed = false;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private sentinelTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly sentinel: ClusterCoordinatorOptions["sentinel"];
  private readonly shortPermits = new Set<ActivePermit>();
  private readonly sweepPermits = new Set<ActivePermit>();
  private sweepTimer: ReturnType<typeof setInterval> | undefined;
  private permitSeq = 0;
  private pendingAcquires: PendingAcquire[] = [];
  private acquireTimer: ReturnType<typeof setTimeout> | undefined;
  private acquireMicrotask = false;
  private acquireInFlight = 0;
  private consecutiveAcquireErrors = 0;
  private probingHealth = false;
  private reconnectAttempt = 0;
  private pendingReleases: PendingRelease[] = [];
  private orphanedReleases: OrphanedRelease[] = [];
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
    return (
      this.connected && Boolean(this.client?.connected) && Boolean(this.maintClient?.connected)
    );
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
    const base = Math.min(1500, 150 * 2 ** Math.min(this.reconnectAttempt, 4));
    this.reconnectAttempt++;
    this.retryTimer = setTimeout(
      () => {
        this.retryTimer = undefined;
        void this.connect();
      },
      base + Math.random() * 100,
    );
    this.retryTimer.unref();
  }

  private recordOrphanedRelease(concKey: string, member: string, expiresAtMs: number): void {
    const now = Date.now();
    if (expiresAtMs <= now) return;
    if (this.orphanedReleases.length >= ORPHAN_RELEASE_MAX) {
      this.orphanedReleases = this.orphanedReleases.filter((item) => item.expiresAtMs > now);
      if (this.orphanedReleases.length >= ORPHAN_RELEASE_MAX) {
        this.orphanedReleases.splice(0, this.orphanedReleases.length - ORPHAN_RELEASE_MAX + 1);
      }
    }
    this.orphanedReleases.push({ concKey, member, expiresAtMs });
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
      const remaining: PendingRelease[] = [];
      for (const item of this.pendingReleases) {
        if (item.redis === client) {
          this.recordOrphanedRelease(item.concKey, item.member, item.expiresAtMs);
        } else {
          remaining.push(item);
        }
      }
      this.pendingReleases = remaining;
    }
  }

  private stopSweep(): void {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = undefined;
    }
  }

  private releasePermit(entry: ActivePermit, isShort: boolean, sendToRedis = true): void {
    if (entry.released) return;
    entry.released = true;
    if (entry.shortTimer) {
      clearTimeout(entry.shortTimer);
      entry.shortTimer = undefined;
    }
    if (isShort) this.shortPermits.delete(entry);
    else this.sweepPermits.delete(entry);
    if (!sendToRedis) return;
    const activeClient = this.client;
    if (activeClient && this.connected && activeClient.connected && !this.closed) {
      this.enqueueRelease(
        activeClient,
        entry.concKey,
        entry.member,
        entry.confirmedExpiresAtMs,
        isShort,
      );
    } else if (!this.closed) {
      this.recordOrphanedRelease(entry.concKey, entry.member, entry.confirmedExpiresAtMs);
    }
  }

  private revokePermit(entry: ActivePermit, isShort: boolean, stillInRedis = true): void {
    if (entry.released) return;
    this.releasePermit(entry, isShort, stillInRedis);
    if (entry.onRevoke) entry.onRevoke();
    else entry.revoked?.abort(new Error("Shared concurrency permit could not be renewed"));
  }

  private async runRenewBatch(
    redis: RedisClient,
    batch: ActivePermit[],
    isShortBatch: boolean,
  ): Promise<void> {
    const n = batch.length;
    if (n === 0) return;
    const shaArgs: string[] = [batchRenewSha, String(n)];
    for (let i = 0; i < n; i++) shaArgs.push(batch[i]!.concKey);
    for (let i = 0; i < n; i++) {
      const p = batch[i]!;
      shaArgs.push(p.member, p.leaseTtlStr);
    }
    const sentAt = Date.now();
    try {
      const raw = await withRedisDeadline(
        evalPermitPrepared(redis, BATCH_RENEW_LUA, shaArgs),
        RENEW_TIMEOUT_MS,
      );
      if (!Array.isArray(raw) || raw.length !== n) {
        throw new Error("Invalid Redis batch renew reply");
      }
      const postNow = Date.now();
      for (let i = 0; i < n; i++) {
        const permit = batch[i]!;
        permit.renewing = false;
        if (permit.released) continue;
        if (Number(raw[i]) === 1) {
          permit.confirmedExpiresAtMs = sentAt + permit.leaseTtlMs;
          permit.nextRenewAtMs = postNow + jitteredRenewDelay(permit.renewEveryMs);
          if (isShortBatch) this.scheduleShortRenew(permit);
        } else {
          this.revokePermit(permit, isShortBatch, false);
        }
      }
    } catch {
      const postNow = Date.now();
      for (let i = 0; i < n; i++) {
        const permit = batch[i]!;
        permit.renewing = false;
        if (permit.released) continue;
        if (postNow >= permit.confirmedExpiresAtMs - permit.safetyMarginMs) {
          this.revokePermit(permit, isShortBatch);
        } else {
          permit.nextRenewAtMs = postNow + Math.min(1000, permit.renewEveryMs);
          if (isShortBatch) this.scheduleShortRenew(permit);
        }
      }
    }
  }

  private scheduleShortRenew(permit: ActivePermit): void {
    if (permit.released) return;
    if (permit.shortTimer) {
      clearTimeout(permit.shortTimer);
      permit.shortTimer = undefined;
    }
    const now = Date.now();
    const cutoff = permit.confirmedExpiresAtMs - permit.safetyMarginMs;
    if (now >= cutoff) {
      this.revokePermit(permit, true);
      return;
    }
    const target =
      this.connected && this.maintClient?.connected
        ? Math.min(permit.nextRenewAtMs, cutoff)
        : cutoff;
    const delay = Math.max(1, target - now);
    permit.shortTimer = setTimeout(() => {
      permit.shortTimer = undefined;
      if (permit.released || permit.renewing) return;
      const tickNow = Date.now();
      if (tickNow >= permit.confirmedExpiresAtMs - permit.safetyMarginMs) {
        this.revokePermit(permit, true);
        return;
      }
      const maint = this.maintClient;
      if (!this.connected || !maint?.connected || this.closed) {
        this.scheduleShortRenew(permit);
        return;
      }
      permit.renewing = true;
      const sentAt = Date.now();
      void withRedisDeadline(
        evalPermitPrepared(maint, RENEW_LUA, [
          renewSha,
          "1",
          permit.concKey,
          permit.member,
          permit.leaseTtlStr,
        ]),
        Math.min(1000, Math.max(100, permit.renewEveryMs)),
      )
        .then((renewed) => {
          permit.renewing = false;
          if (permit.released) return;
          if (Number(renewed) === 1) {
            permit.confirmedExpiresAtMs = sentAt + permit.leaseTtlMs;
            permit.nextRenewAtMs = Date.now() + permit.renewEveryMs;
            this.scheduleShortRenew(permit);
          } else {
            this.revokePermit(permit, true, false);
          }
        })
        .catch(() => {
          permit.renewing = false;
          if (permit.released) return;
          const failNow = Date.now();
          if (failNow >= permit.confirmedExpiresAtMs - permit.safetyMarginMs) {
            this.revokePermit(permit, true);
          } else {
            permit.nextRenewAtMs = failNow + Math.min(200, permit.renewEveryMs);
            this.scheduleShortRenew(permit);
          }
        });
    }, delay);
    permit.shortTimer.unref();
  }

  private ensureSweepTimer(): void {
    if (this.sweepTimer) return;
    this.sweepTimer = setInterval(() => {
      if (this.sweepPermits.size === 0) return;
      const now = Date.now();
      const maint = this.maintClient;
      const canRenew = this.connected && Boolean(maint?.connected) && !this.closed;
      const due: ActivePermit[] = [];
      for (const permit of this.sweepPermits) {
        if (permit.released) continue;
        if (now >= permit.confirmedExpiresAtMs - permit.safetyMarginMs) {
          this.revokePermit(permit, false);
          continue;
        }
        if (canRenew && !permit.renewing && now >= permit.nextRenewAtMs) {
          permit.renewing = true;
          due.push(permit);
        }
      }
      if (due.length === 0 || !maint) return;
      for (let i = 0; i < due.length; i += RENEW_BATCH_MAX) {
        void this.runRenewBatch(maint, due.slice(i, i + RENEW_BATCH_MAX), false);
      }
    }, 250);
    this.sweepTimer.unref();
  }

  private async flushOrphanedReleases(maint: RedisClient): Promise<void> {
    if (this.orphanedReleases.length === 0) return;
    const now = Date.now();
    const pending = this.orphanedReleases;
    this.orphanedReleases = [];
    const byKey = new Map<string, OrphanedRelease[]>();
    for (let i = 0; i < pending.length; i++) {
      const item = pending[i]!;
      if (item.expiresAtMs <= now) continue;
      let list = byKey.get(item.concKey);
      if (!list) {
        list = [];
        byKey.set(item.concKey, list);
      }
      list.push(item);
    }
    const entries = [...byKey.entries()];
    for (let e = 0; e < entries.length; e++) {
      const [concKey, items] = entries[e]!;
      for (let i = 0; i < items.length; i += RELEASE_BATCH_MAX) {
        const chunk = items.slice(i, i + RELEASE_BATCH_MAX);
        try {
          await withRedisDeadline(
            maint.send("ZREM", [concKey, ...chunk.map((item) => item.member)]),
            RENEW_TIMEOUT_MS,
          );
        } catch (error) {
          for (let r = i; r < items.length; r++) {
            const rem = items[r]!;
            this.recordOrphanedRelease(rem.concKey, rem.member, rem.expiresAtMs);
          }
          for (let nextE = e + 1; nextE < entries.length; nextE++) {
            const nextItems = entries[nextE]![1];
            for (let r = 0; r < nextItems.length; r++) {
              const rem = nextItems[r]!;
              this.recordOrphanedRelease(rem.concKey, rem.member, rem.expiresAtMs);
            }
          }
          throw error;
        }
      }
    }
  }

  private async reconcileAfterReconnect(maint: RedisClient): Promise<void> {
    await this.flushOrphanedReleases(maint);
    const activeSweep: ActivePermit[] = [];
    for (const p of this.sweepPermits) {
      if (!p.released) {
        p.renewing = true;
        activeSweep.push(p);
      }
    }
    const activeShort: ActivePermit[] = [];
    for (const p of this.shortPermits) {
      if (!p.released) {
        if (p.shortTimer) {
          clearTimeout(p.shortTimer);
          p.shortTimer = undefined;
        }
        p.renewing = true;
        activeShort.push(p);
      }
    }
    const verifyGroup = async (group: ActivePermit[], isShort: boolean) => {
      for (let offset = 0; offset < group.length; offset += RENEW_BATCH_MAX) {
        const slice = group.slice(offset, offset + RENEW_BATCH_MAX);
        const shaArgs: string[] = [batchRenewSha, String(slice.length)];
        for (let i = 0; i < slice.length; i++) shaArgs.push(slice[i]!.concKey);
        for (let i = 0; i < slice.length; i++) {
          const p = slice[i]!;
          shaArgs.push(p.member, p.leaseTtlStr);
        }
        const sentAt = Date.now();
        const raw = await withRedisDeadline(
          evalPermitPrepared(maint, BATCH_RENEW_LUA, shaArgs),
          RENEW_TIMEOUT_MS,
        );
        if (!Array.isArray(raw) || raw.length !== slice.length) {
          throw new Error("Invalid Redis reconcile renew reply");
        }
        const postNow = Date.now();
        for (let i = 0; i < slice.length; i++) {
          const permit = slice[i]!;
          permit.renewing = false;
          if (permit.released) continue;
          if (Number(raw[i]) === 1) {
            permit.confirmedExpiresAtMs = sentAt + permit.leaseTtlMs;
            permit.nextRenewAtMs =
              postNow + (isShort ? permit.renewEveryMs : jitteredRenewDelay(permit.renewEveryMs));
            if (isShort) this.scheduleShortRenew(permit);
          } else {
            this.revokePermit(permit, isShort, false);
          }
        }
      }
    };
    try {
      await verifyGroup(activeSweep, false);
      await verifyGroup(activeShort, true);
      if (this.orphanedReleases.length > 0) {
        await this.flushOrphanedReleases(maint);
      }
    } catch (error) {
      for (const p of activeSweep) p.renewing = false;
      for (const p of activeShort) {
        p.renewing = false;
        if (!p.released) this.scheduleShortRenew(p);
      }
      throw error;
    }
  }

  private probeConnectionHealth(client: RedisClient): void {
    if (this.probingHealth || this.closed || this.client !== client) return;
    this.probingHealth = true;
    const target = this.maintClient ?? client;
    void withRedisDeadline(target.send("PING", []), 500)
      .then((reply) => {
        if (reply !== "PONG") this.disconnect(client);
        else this.consecutiveAcquireErrors = 0;
      })
      .catch(() => {
        this.disconnect(client);
      })
      .finally(() => {
        this.probingHealth = false;
      });
  }

  private disconnect(client: RedisClient): void {
    if (this.client !== client) return;
    this.connected = false;
    this.consecutiveAcquireErrors = 0;
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
    const maint = this.maintClient;
    this.subscriber = null;
    this.maintClient = null;
    this.client = null;
    this.primary = undefined;
    this.closeIngressConnection();
    this.failPendingFor(client);
    try {
      subscriber?.close();
    } catch {
      void 0;
    }
    try {
      maint?.close();
    } catch {
      void 0;
    }
    try {
      client.close();
    } catch {
      void 0;
    }
    const now = Date.now();
    for (const entry of this.sweepPermits) {
      entry.renewing = false;
      if (now >= entry.confirmedExpiresAtMs - entry.safetyMarginMs) {
        this.revokePermit(entry, false);
      }
    }
    for (const entry of this.shortPermits) {
      entry.renewing = false;
      if (now >= entry.confirmedExpiresAtMs - entry.safetyMarginMs) {
        this.revokePermit(entry, true);
      } else {
        this.scheduleShortRenew(entry);
      }
    }
    if (this.sweepPermits.size > 0) this.ensureSweepTimer();
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
    let maint: RedisClient | null = null;
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
          connecting.send("SCRIPT", ["LOAD", BATCH_RENEW_LUA]),
        ]),
        2000,
      );
      maint = await withRedisDeadline(connecting.duplicate(), 2000);
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
        maint.close();
        client.close();
        return;
      }
      await this.reconcileAfterReconnect(maint);
      if (this.closed) {
        sub.close();
        maint.close();
        client.close();
        return;
      }
      this.client = client;
      this.maintClient = maint;
      this.subscriber = sub;
      this.primary = primary;
      this.connected = true;
      this.reconnectAttempt = 0;
      this.consecutiveAcquireErrors = 0;
      connecting.onclose = () => this.disconnect(connecting);
      maint.onclose = () => this.disconnect(connecting);
      sub.onclose = () => this.disconnect(connecting);
      if (!connecting.connected || !maint.connected || !sub.connected) {
        this.disconnect(connecting);
        return;
      }
      if (this.sweepPermits.size > 0) this.ensureSweepTimer();
      this.monitorPrimary(connecting, primary);
    } catch {
      sub?.close();
      maint?.close();
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
    expiresAtMs: number,
    immediate: boolean,
  ): void {
    if (!redis.connected || this.closed || this.client !== redis) {
      if (!this.closed) this.recordOrphanedRelease(concKey, member, expiresAtMs);
      return;
    }
    this.pendingReleases.push({ redis, concKey, member, expiresAtMs });
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
        void only.redis.send("ZREM", [only.concKey, only.member]).catch(() => {
          if (!this.closed) {
            this.recordOrphanedRelease(only.concKey, only.member, only.expiresAtMs);
          }
        });
      } else if (!this.closed) {
        this.recordOrphanedRelease(only.concKey, only.member, only.expiresAtMs);
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
      if (!firstRedis.connected) {
        if (!this.closed) {
          for (const item of batch) {
            this.recordOrphanedRelease(item.concKey, item.member, item.expiresAtMs);
          }
        }
        return;
      }
      const byKey = new Map<string, { args: string[]; items: PendingRelease[] }>();
      for (let i = 0; i < batch.length; i++) {
        const item = batch[i]!;
        let group = byKey.get(item.concKey);
        if (!group) {
          group = { args: [item.concKey], items: [] };
          byKey.set(item.concKey, group);
        }
        group.args.push(item.member);
        group.items.push(item);
      }
      for (const group of byKey.values()) {
        void firstRedis.send("ZREM", group.args).catch(() => {
          if (!this.closed) {
            for (const item of group.items) {
              this.recordOrphanedRelease(item.concKey, item.member, item.expiresAtMs);
            }
          }
        });
      }
      return;
    }

    const byClient = new Map<
      RedisClient,
      Map<string, { args: string[]; items: PendingRelease[] }>
    >();
    for (const item of batch) {
      if (!item.redis.connected) {
        if (!this.closed) {
          this.recordOrphanedRelease(item.concKey, item.member, item.expiresAtMs);
        }
        continue;
      }
      let byKey = byClient.get(item.redis);
      if (!byKey) {
        byKey = new Map();
        byClient.set(item.redis, byKey);
      }
      let group = byKey.get(item.concKey);
      if (!group) {
        group = { args: [item.concKey], items: [] };
        byKey.set(item.concKey, group);
      }
      group.args.push(item.member);
      group.items.push(item);
    }
    for (const [redis, byKey] of byClient) {
      for (const group of byKey.values()) {
        void redis.send("ZREM", group.args).catch(() => {
          if (!this.closed) {
            for (const item of group.items) {
              this.recordOrphanedRelease(item.concKey, item.member, item.expiresAtMs);
            }
          }
        });
      }
    }
  }

  private registerPermit(
    concKey: string,
    member: string,
    leaseTtlMs: number,
    leaseTtlStr: string,
    onRevoke: (() => void) | undefined,
    batchNowMs: number,
  ): RateLimitAcquireResult {
    const revoked = onRevoke ? null : new AbortController();
    const renewEveryMs = Math.max(1, Math.floor(leaseTtlMs / 3));
    const safetyMarginMs = safetyMarginFor(leaseTtlMs);
    const isShort = leaseTtlMs < 3000;
    const entry: ActivePermit = {
      concKey,
      member,
      leaseTtlMs,
      leaseTtlStr,
      renewEveryMs,
      safetyMarginMs,
      confirmedExpiresAtMs: batchNowMs + leaseTtlMs,
      nextRenewAtMs: batchNowMs + (isShort ? renewEveryMs : jitteredRenewDelay(renewEveryMs)),
      renewing: false,
      released: false,
      onRevoke,
      revoked,
    };
    if (isShort) {
      this.shortPermits.add(entry);
      this.scheduleShortRenew(entry);
    } else {
      this.sweepPermits.add(entry);
      if (!this.sweepTimer) this.ensureSweepTimer();
    }
    const release = () => this.releasePermit(entry, isShort);
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
    if (this.acquireInFlight >= MAX_ACQUIRE_IN_FLIGHT) return;
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
    else if (this.acquireInFlight + 1 < MAX_ACQUIRE_IN_FLIGHT) this.scheduleAcquireFlush(true);

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
    const sentAtMs = Date.now();
    try {
      const raw = await withRedisDeadline(
        evalPermitPrepared(redis, BATCH_ACQUIRE_LUA, shaArgs),
        ACQUIRE_TIMEOUT_MS,
      );
      if (!Array.isArray(raw) || raw.length !== n) {
        throw new Error("Invalid Redis rate limit reply");
      }
      this.consecutiveAcquireErrors = 0;
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
          if (redis.connected) {
            void redis.send("ZREM", [it.keys.concKey, it.member]).catch(() => {
              if (!this.closed) {
                this.recordOrphanedRelease(it.keys.concKey, it.member, sentAtMs + it.leaseTtlMs);
              }
            });
          } else if (!this.closed) {
            this.recordOrphanedRelease(it.keys.concKey, it.member, sentAtMs + it.leaseTtlMs);
          }
          it.resolve({ ok: false, reason: "unavailable" });
        } else {
          it.resolve(
            this.registerPermit(
              it.keys.concKey,
              it.member,
              it.leaseTtlMs,
              it.leaseTtlStr,
              it.onRevoke,
              sentAtMs,
            ),
          );
        }
      }
    } catch {
      for (const it of items) it.resolve({ ok: false, reason: "unavailable" });
      if (!redis.connected || this.closed || this.client !== redis) {
        this.disconnect(redis);
      } else {
        this.consecutiveAcquireErrors++;
        if (this.consecutiveAcquireErrors >= 3) {
          this.probeConnectionHealth(redis);
        }
      }
    } finally {
      this.acquireInFlight--;
      if (
        this.pendingAcquires.length > 0 &&
        !this.closed &&
        this.acquireInFlight < MAX_ACQUIRE_IN_FLIGHT
      ) {
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
      if (
        this.pendingAcquires.length >= ACQUIRE_BATCH_MAX &&
        this.acquireInFlight < MAX_ACQUIRE_IN_FLIGHT
      ) {
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

  private closeIngressConnection(): void {
    const connection = this.ingressConnection;
    this.ingressConnection = undefined;
    connection?.client.close();
  }

  async takeIngressWindow(
    ip: string,
    limit: number,
    windowMs: number,
    timeoutMs: number,
  ): Promise<number | null> {
    if (!this.primary || !this.isRedisConnected() || this.closed) return null;
    if (!this.ingressConnection) {
      const client = new RedisClient(this.primary, {
        connectionTimeout: timeoutMs,
        autoReconnect: false,
        enableOfflineQueue: false,
      });
      this.ingressConnection = { client, ready: client.connect() };
    }
    const connection = this.ingressConnection;
    try {
      const wait = await withRedisDeadline(
        connection.ready.then(() =>
          evalPermitPrepared(connection.client, INGRESS_WINDOW_LUA, [
            ingressWindowSha,
            "1",
            `${this.ns}:ingress:ip:${ip}`,
            String(limit),
            String(windowMs),
          ]),
        ),
        timeoutMs,
      );
      return this.ingressConnection === connection &&
        this.connected &&
        typeof wait === "number" &&
        Number.isSafeInteger(wait) &&
        wait >= 0
        ? wait
        : null;
    } catch {
      if (this.ingressConnection === connection) this.closeIngressConnection();
      return null;
    }
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
    this.orphanedReleases = [];
    const activeShort = [...this.shortPermits];
    this.shortPermits.clear();
    for (const entry of activeShort) this.revokePermit(entry, true);
    const activeSweep = [...this.sweepPermits];
    this.sweepPermits.clear();
    for (const entry of activeSweep) this.revokePermit(entry, false);
    const sub = this.subscriber;
    const maint = this.maintClient;
    const client = this.client;
    this.subscriber = null;
    this.maintClient = null;
    this.client = null;
    this.primary = undefined;
    this.closeIngressConnection();
    try {
      sub?.close();
    } catch {
      void 0;
    }
    try {
      maint?.close();
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
