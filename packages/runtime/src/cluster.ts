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
local nowMs = tonumber(ARGV[5])
local leaseTtlMs = tonumber(ARGV[6])
local member = ARGV[7]

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
redis.call('PEXPIRE', concKey, math.ceil(leaseTtlMs / 1000) + 10)
return 0
`;

export type RateLimitAcquireResult =
  { ok: true; release: () => void } | { ok: false; reason: "rpm" | "tpm" | "concurrency" };

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

  constructor(options: ClusterCoordinatorOptions = {}) {
    this.redisUrl = options.redisUrl;
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
    if (!this.redisUrl) return;
    const client = new RedisClient(this.redisUrl, {
      connectionTimeout: 2000,
      autoReconnect: true,
    });
    await client.connect();
    const sub = await client.duplicate();
    await sub.subscribe(this.circuitTopic(), (message) => {
      try {
        const parsed = JSON.parse(message) as ClusterCircuitEvent;
        if (parsed.node_id && parsed.node_id !== this.nodeId) {
          this.onCircuitEvent?.(parsed);
        }
      } catch {
        void 0;
      }
    });
    await sub.subscribe(this.epochTopic(), (message) => {
      try {
        const parsed = JSON.parse(message) as ClusterEpochEvent;
        if (parsed.node_id && parsed.node_id !== this.nodeId) {
          this.onConfigEpoch?.(parsed);
        }
      } catch {
        void 0;
      }
    });
    this.client = client;
    this.subscriber = sub;
    this.connected = true;
  }

  async acquireRateLimit(
    key: string,
    limits: { rpm: number; tpm: number; concurrency: number },
    estimatedTokens: number,
    leaseTtlMs = 120_000,
  ): Promise<RateLimitAcquireResult> {
    if (!this.client || !this.connected) {
      return this.localLimiter.acquire(key, limits, estimatedTokens);
    }
    const minuteBucket = Math.floor(Date.now() / 60_000);
    const rpmKey = `${this.ns}:rl:rpm:${key}:${minuteBucket}`;
    const tpmKey = `${this.ns}:rl:tpm:${key}:${minuteBucket}`;
    const concKey = `${this.ns}:rl:conc:${key}`;
    const member = `${this.nodeId}:${crypto.randomUUID()}`;
    const safeTpmLimit = Math.min(limits.tpm, Number.MAX_SAFE_INTEGER);
    try {
      const code = Number(
        await this.client.send("EVAL", [
          ACQUIRE_LUA,
          "3",
          rpmKey,
          tpmKey,
          concKey,
          String(limits.rpm),
          String(safeTpmLimit),
          String(limits.concurrency),
          String(estimatedTokens),
          String(Date.now()),
          String(leaseTtlMs),
          member,
        ]),
      );
      if (code === 1) return { ok: false, reason: "rpm" };
      if (code === 2) return { ok: false, reason: "tpm" };
      if (code === 3) return { ok: false, reason: "concurrency" };
      let released = false;
      const redis = this.client;
      return {
        ok: true,
        release: () => {
          if (released) return;
          released = true;
          void redis.send("ZREM", [concKey, member]).catch(() => undefined);
        },
      };
    } catch {
      return this.localLimiter.acquire(key, limits, estimatedTokens);
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
