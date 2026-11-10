import { RedisClient } from "bun";
import { GatewayError } from "@gateway/kernel";

const enterScript = `
local now_parts=redis.call('TIME')
local now=tonumber(now_parts[1])*1000+math.floor(tonumber(now_parts[2])/1000)
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now-60000)
redis.call('ZREMRANGEBYSCORE',KEYS[2],'-inf',now)
local rpm=redis.call('ZCARD',KEYS[1])
local concurrent=redis.call('ZCARD',KEYS[2])
local tokenKey=KEYS[3]..':'..math.floor(now/60000)
local tokens=tonumber(redis.call('GET',tokenKey) or '0')
if rpm>=tonumber(ARGV[2]) or concurrent>=tonumber(ARGV[3]) or tokens+tonumber(ARGV[5])>tonumber(ARGV[6]) then return 0 end
redis.call('ZADD',KEYS[1],now,ARGV[1])
redis.call('ZADD',KEYS[2],now+tonumber(ARGV[4]),ARGV[1])
redis.call('PEXPIRE',KEYS[1],60000)
redis.call('PEXPIRE',KEYS[2],tonumber(ARGV[4])+60000)
redis.call('INCRBY',tokenKey,ARGV[5])
redis.call('PEXPIREAT',tokenKey,(math.floor(now/60000)+2)*60000)
return tokenKey`;

export type RatePermit = (() => Promise<void>) & {
  adjustTokens: (actual: number) => Promise<void>;
};

export class Limits {
  redis: RedisClient;
  private readonly url: string;
  private connecting: { client: RedisClient; promise: Promise<unknown> } | undefined;
  private closed = false;
  constructor(
    url: string,
    readonly namespace: string,
    readonly commandTimeoutMs = 1000,
  ) {
    this.url = url;
    this.redis = this.client();
  }
  private client(): RedisClient {
    return new RedisClient(this.url, {
      connectionTimeout: 1000,
      idleTimeout: 0,
      maxRetries: 3,
      enableOfflineQueue: false,
    });
  }
  async command(command: string, args: string[] = []): Promise<unknown> {
    if (this.closed)
      throw new GatewayError(503, "rate_limit_unavailable", "Admission is shutting down");
    const client = this.redis;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        (async () => {
          if (!client.connected) {
            if (this.connecting?.client !== client)
              this.connecting = { client, promise: client.connect() };
            await this.connecting.promise;
          }
          return client.send(command, args);
        })(),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            if (this.redis === client && !this.closed) {
              client.close();
              this.redis = this.client();
              this.connecting = undefined;
            }
            reject(
              new GatewayError(
                503,
                "rate_limit_unavailable",
                "Distributed admission deadline reached",
              ),
            );
          }, this.commandTimeoutMs);
        }),
      ]);
    } catch {
      if (this.redis === client && !this.closed) {
        client.close();
        this.redis = this.client();
        this.connecting = undefined;
      }
      throw new GatewayError(503, "rate_limit_unavailable", "Distributed admission is unavailable");
    } finally {
      clearTimeout(timer);
    }
  }
  async roundRobin(scope: string, size: number): Promise<number> {
    const value = await this.command("EVAL", [
      "local n=redis.call('INCR',KEYS[1]);redis.call('EXPIRE',KEYS[1],86400);return (n-1)%tonumber(ARGV[1])",
      "1",
      `${this.namespace}:{routing:${scope}}:rr`,
      String(size),
    ]);
    return Number(value);
  }
  close(): void {
    this.closed = true;
    this.redis.close();
  }
  // Only one instance may probe an expired circuit. Normal closed circuits incur no lease.
  async probe(providerId: string, leaseMs: number): Promise<boolean> {
    return (
      Number(
        await this.command("EVAL", [
          `local t=redis.call('TIME')
local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
local opened=tonumber(redis.call('HGET',KEYS[1],'open_until') or '0')
if opened==0 then return 1 end
if opened>now or tonumber(redis.call('HGET',KEYS[1],'probe_until') or '0')>now then return 0 end
redis.call('HSET',KEYS[1],'probe_until',now+tonumber(ARGV[1])) return 1`,
          "1",
          `${this.namespace}:health:${providerId}`,
          String(leaseMs),
        ]),
      ) === 1
    );
  }
  async enter(
    scope: string,
    id: string,
    rpm: number,
    concurrency: number,
    leaseMs: number,
    tokens = 0,
    tpm = 1_000_000_000,
  ): Promise<RatePermit> {
    const prefix = `${this.namespace}:{${scope}}`;
    let result: unknown;
    try {
      result = await this.command("EVAL", [
        enterScript,
        "3",
        `${prefix}:rpm`,
        `${prefix}:active`,
        `${prefix}:tpm`,
        id,
        String(rpm),
        String(concurrency),
        String(leaseMs),
        String(tokens),
        String(tpm),
      ]);
    } catch {
      throw new GatewayError(503, "rate_limit_unavailable", "Distributed admission is unavailable");
    }
    if (typeof result !== "string")
      throw new GatewayError(
        429,
        "rate_limited",
        "Request rate, token budget or concurrency limit reached",
      );
    let released = false;
    const release = async () => {
      if (released) return;
      released = true;
      try {
        await this.command("ZREM", [`${prefix}:active`, id]);
      } catch {
        /* 有界租约兜底，不放开新请求。 */
      }
    };
    let reconciled = false;
    return Object.assign(release, {
      adjustTokens: async (actual: number) => {
        if (reconciled || !Number.isSafeInteger(actual) || actual < 0) return;
        reconciled = true;
        const script =
          "if redis.call('EXISTS',KEYS[1])==1 then return redis.call('INCRBY',KEYS[1],ARGV[1]) else return 0 end";
        await this.command("EVAL", [script, "1", String(result), String(actual - tokens)]);
      },
    });
  }
  async health(providerId: string): Promise<{ open: boolean; latency: number }> {
    const values = (await this.command("HMGET", [
      `${this.namespace}:health:${providerId}`,
      "open_until",
      "latency",
    ])) as (string | null)[];
    return { open: Number(values[0] ?? 0) > Date.now(), latency: Number(values[1] ?? 100) };
  }
  async observe(
    providerId: string,
    failed: boolean,
    latency: number,
    cooldownMs = 10_000,
  ): Promise<void> {
    const key = `${this.namespace}:health:${providerId}`;
    const script = `local fails=tonumber(redis.call('HGET',KEYS[1],'fails') or '0')
local prior=tonumber(redis.call('HGET',KEYS[1],'latency') or ARGV[2])
if ARGV[1]=='1' then fails=fails+1 else fails=0 redis.call('HSET',KEYS[1],'open_until',0) end
redis.call('HDEL',KEYS[1],'probe_until')
redis.call('HSET',KEYS[1],'fails',fails,'latency',prior*0.8+tonumber(ARGV[2])*0.2)
if fails>=3 then redis.call('HSET',KEYS[1],'open_until',tonumber(ARGV[3])+tonumber(ARGV[4])) end
redis.call('EXPIRE',KEYS[1],600) return 1`;
    await this.command("EVAL", [
      script,
      "1",
      key,
      failed ? "1" : "0",
      String(latency),
      String(Date.now()),
      String(cooldownMs),
    ]);
  }
}
