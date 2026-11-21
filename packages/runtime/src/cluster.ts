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

const ACQUIRE_LUA = `
local rpmKey = KEYS[1]
local tpmKey = KEYS[2]
local concKey = KEYS[3]
local rpmLimit = tonumber(ARGV[1])
local tpmLimit = tonumber(ARGV[2])
local concLimit = tonumber(ARGV[3])
local estTokens = tonumber(ARGV[4])
local leaseTtlMs = tonumber(ARGV[5])
local member = ARGV[6]
local time = redis.call('TIME')
local nowMs = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

redis.call('ZREMRANGEBYSCORE', concKey, '-inf', nowMs)
local active = redis.call('ZCARD', concKey)
if active >= concLimit then
  return 3
end

local curRpm = tonumber(redis.call('GET', rpmKey) or '0')
if curRpm >= rpmLimit then
  return 1
end

local curTpm = tonumber(redis.call('GET', tpmKey) or '0')
if curTpm + estTokens > tpmLimit then
  return 2
end

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
return 0
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

export type RateLimitAcquireResult =
  | { ok: true; release: () => void; signal?: AbortSignal }
  | { ok: false; reason: "rpm" | "tpm" | "concurrency" | "unavailable" };

export class ClusterCoordinator {
  readonly nodeId: string;
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

  private disconnect(client: RedisClient): void {
    if (this.client !== client) return;
    this.connected = false;
    clearTimeout(this.sentinelTimer);
    const subscriber = this.subscriber;
    this.subscriber = null;
    this.client = null;
    for (const revoke of this.permits) revoke();
    subscriber?.close();
    client.close();
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

  async acquireRateLimit(
    key: string,
    limits: { rpm: number; tpm: number; concurrency: number },
    estimatedTokens: number,
    leaseTtlMs = 120_000,
  ): Promise<RateLimitAcquireResult> {
    if (!this.redisUrl) return this.localLimiter.acquire(key, limits, estimatedTokens);
    const redis = this.client;
    if (!redis?.connected || this.closed) return { ok: false, reason: "unavailable" };
    const rpmKey = `${this.ns}:rl:rpm:${key}`;
    const tpmKey = `${this.ns}:rl:tpm:${key}`;
    const concKey = `${this.ns}:rl:conc:${key}`;
    const member = `${this.nodeId}:${crypto.randomUUID()}`;
    const safeTpmLimit = Math.min(limits.tpm, Number.MAX_SAFE_INTEGER);
    try {
      const code = Number(
        await withRedisDeadline(
          redis.send("EVAL", [
            ACQUIRE_LUA,
            "3",
            rpmKey,
            tpmKey,
            concKey,
            String(limits.rpm),
            String(safeTpmLimit),
            String(limits.concurrency),
            String(estimatedTokens),
            String(leaseTtlMs),
            member,
          ]),
          1000,
        ),
      );
      if (code === 1) return { ok: false, reason: "rpm" };
      if (code === 2) return { ok: false, reason: "tpm" };
      if (code === 3) return { ok: false, reason: "concurrency" };
      if (code !== 0) return { ok: false, reason: "unavailable" };
      if (this.closed || this.client !== redis || !this.connected) {
        void redis.send("ZREM", [concKey, member]).catch(() => undefined);
        return { ok: false, reason: "unavailable" };
      }
      const revoked = new AbortController();
      let released = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const release = () => {
        if (released) return;
        released = true;
        clearTimeout(timer);
        this.permits.delete(revoke);
        void redis.send("ZREM", [concKey, member]).catch(() => undefined);
      };
      const revoke = () => {
        if (released) return;
        release();
        revoked.abort(new Error("Shared concurrency permit could not be renewed"));
      };
      const renewEveryMs = Math.max(1, Math.floor(leaseTtlMs / 3));
      const renew = async () => {
        if (released) return;
        try {
          const renewed = await withRedisDeadline(
            redis.send("EVAL", [RENEW_LUA, "1", concKey, member, String(leaseTtlMs)]),
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
      return { ok: true, release, signal: revoked.signal };
    } catch {
      this.disconnect(redis);
      return { ok: false, reason: "unavailable" };
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
    clearTimeout(this.sentinelTimer);
    for (const revoke of this.permits) revoke();
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
