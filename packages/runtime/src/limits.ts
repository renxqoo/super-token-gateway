import { RedisClient } from "bun";
import { GatewayError } from "@gateway/contracts";
import { sha256 } from "./crypto.ts";

const enterScript = `
local now_parts=redis.call('TIME')
local now=tonumber(now_parts[1])*1000+math.floor(tonumber(now_parts[2])/1000)
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now-60000)
redis.call('ZREMRANGEBYSCORE',KEYS[2],'-inf',now)
local rpm=redis.call('ZCARD',KEYS[1])
local concurrent=redis.call('ZCARD',KEYS[2])
local tokenKey=KEYS[3]..':'..math.floor(now/60000)
local tokenValue=redis.call('GET',tokenKey)
local tokens=tonumber(tokenValue or '0')
if rpm>=tonumber(ARGV[2]) or concurrent>=tonumber(ARGV[3]) or tokens+tonumber(ARGV[5])>tonumber(ARGV[6]) then return 0 end
redis.call('ZADD',KEYS[1],now,ARGV[1])
redis.call('ZADD',KEYS[2],now+tonumber(ARGV[4]),ARGV[1])
if redis.call('PTTL',KEYS[1])<60000 then redis.call('PEXPIRE',KEYS[1],90000) end
if redis.call('PTTL',KEYS[2])<tonumber(ARGV[4])+60000 then redis.call('PEXPIRE',KEYS[2],tonumber(ARGV[4])+90000) end
if not tokenValue or tonumber(ARGV[5])~=0 then redis.call('INCRBY',tokenKey,ARGV[5]) end
if not tokenValue then redis.call('PEXPIREAT',tokenKey,(math.floor(now/60000)+2)*60000) end
return tokenKey`;

export type RatePermit = (() => Promise<void>) & {
  adjustTokens: (actual: number) => Promise<void>;
};

type ProbePermit = (() => Promise<void>) & {
  observe: (failed: boolean, latency: number) => Promise<void>;
};

export class Limits {
  redis: RedisClient;
  private readonly url: string;
  private readonly shards: readonly Limits[] | undefined;
  private readonly shardCache = new Map<string, Limits>();
  private connecting: { client: RedisClient; promise: Promise<unknown> } | undefined;
  private closed = false;
  constructor(
    url: string | readonly string[],
    readonly namespace: string,
    readonly commandTimeoutMs = 1000,
  ) {
    const urls = typeof url === "string" ? [url] : url;
    if (!urls.length || urls.length > 8)
      throw new RangeError("Admission requires 1 to 8 Redis endpoints");
    for (const endpoint of urls) {
      let parsed: URL;
      try {
        parsed = new URL(endpoint);
      } catch {
        throw new TypeError("Invalid admission Redis endpoint");
      }
      if (!["redis:", "rediss:"].includes(parsed.protocol))
        throw new TypeError("Invalid admission Redis endpoint protocol");
    }
    this.url = urls[0]!;
    this.redis = this.client();
    this.shards =
      urls.length > 1
        ? [
            this,
            ...urls.slice(1).map((endpoint) => new Limits(endpoint, namespace, commandTimeoutMs)),
          ]
        : undefined;
  }
  private shard(scope: string): Limits {
    if (!this.shards) return this;
    const cached = this.shardCache.get(scope);
    if (cached) return cached;
    const shard = this.shards[Number.parseInt(sha256(scope).slice(0, 8), 16) % this.shards.length]!;
    if (this.shardCache.size >= 10000) this.shardCache.clear();
    this.shardCache.set(scope, shard);
    return shard;
  }
  async ping(): Promise<void> {
    await Promise.all((this.shards ?? [this]).map((shard) => shard.command("PING")));
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
          if (command !== "EVAL") return client.send(command, args);
          const digest = new Bun.CryptoHasher("sha1").update(args[0]!).digest("hex");
          try {
            return await client.send("EVALSHA", [digest, ...args.slice(1)]);
          } catch (error) {
            if (!(error instanceof Error) || !/^NOSCRIPT\b/.test(error.message)) throw error;
            return client.send(command, args);
          }
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
    const shard = this.shard(`routing:${scope}`);
    if (shard !== this) return shard.roundRobin(scope, size);
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
    this.shardCache.clear();
    this.redis.close();
    for (const shard of this.shards?.slice(1) ?? []) shard.close();
  }
  async probe(
    providerId: string,
    leaseMs: number,
    identity: string,
  ): Promise<ProbePermit | undefined> {
    const shard = this.shard(`provider:${providerId}`);
    if (shard !== this) return shard.probe(providerId, leaseMs, identity);
    const key = `${this.namespace}:health:${providerId}`;
    const outcome = (await this.command("EVAL", [
      `local t=redis.call('TIME')
local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
local health=redis.call('HMGET',KEYS[1],'open_until','epoch')
local opened=tonumber(health[1] or '0')
local epoch=health[2]
if not epoch then epoch=ARGV[2] redis.call('HSET',KEYS[1],'epoch',epoch) redis.call('EXPIRE',KEYS[1],600) end
if opened==0 then return {1,epoch} end
if opened>now or tonumber(redis.call('HGET',KEYS[1],'probe_until') or '0')>now then return {0,epoch} end
redis.call('HSET',KEYS[1],'probe_until',now+tonumber(ARGV[1]),'probe_token',ARGV[2])
local retention=tonumber(ARGV[1])+600000
if redis.call('PTTL',KEYS[1])<retention then redis.call('PEXPIRE',KEYS[1],retention) end
return {2,epoch}`,
      "1",
      key,
      String(leaseMs),
      identity,
    ])) as [number, string];
    const result = Number(outcome[0]);
    const epoch = outcome[1];
    if (result === 0) return undefined;
    return Object.assign(
      async () => {
        if (result === 2)
          await this.command("EVAL", [
            "if redis.call('HGET',KEYS[1],'probe_token')==ARGV[1] then redis.call('HDEL',KEYS[1],'probe_until','probe_token') end return 1",
            "1",
            key,
            identity,
          ]);
      },
      {
        observe: (failed: boolean, latency: number) =>
          this.observe(providerId, failed, latency, 10000, {
            identity,
            halfOpen: result === 2,
            epoch,
          }),
      },
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
    const shard = this.shard(scope);
    if (shard !== this) return shard.enter(scope, id, rpm, concurrency, leaseMs, tokens, tpm);
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
      } catch {}
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
    const shard = this.shard(`provider:${providerId}`);
    if (shard !== this) return shard.health(providerId);
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
    permit?: { identity: string; halfOpen: boolean; epoch: string },
  ): Promise<void> {
    const shard = this.shard(`provider:${providerId}`);
    if (shard !== this) return shard.observe(providerId, failed, latency, cooldownMs, permit);
    const key = `${this.namespace}:health:${providerId}`;
    const script = `local health=redis.call('HMGET',KEYS[1],'epoch','open_until','fails','latency')
if ARGV[6]~='' and health[1]~=ARGV[7] then return 0 end
if ARGV[6]=='probe' then
  local t=redis.call('TIME')
  local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
  local probe=redis.call('HMGET',KEYS[1],'probe_token','probe_until')
  if probe[1]~=ARGV[5] or tonumber(probe[2] or '0')<=now then return 0 end
elseif ARGV[6]=='closed' and tonumber(health[2] or '0')~=0 then return 0 end
local fails=tonumber(health[3] or '0')
local prior=tonumber(health[4] or ARGV[2])
if ARGV[1]=='1' then
  fails=fails+1
  redis.call('HSET',KEYS[1],'fails',fails,'latency',prior*0.8+tonumber(ARGV[2])*0.2)
else
  fails=0
  redis.call('HSET',KEYS[1],'fails',0,'latency',prior*0.8+tonumber(ARGV[2])*0.2,'open_until',0)
end
if fails>=3 then redis.call('HSET',KEYS[1],'open_until',tonumber(ARGV[3])+tonumber(ARGV[4])) end
if fails>=3 or ARGV[6]=='' then redis.call('HSET',KEYS[1],'epoch',ARGV[5]) end
if ARGV[6]=='probe' then redis.call('HDEL',KEYS[1],'probe_until','probe_token') end
redis.call('EXPIRE',KEYS[1],600) return 1`;
    await this.command("EVAL", [
      script,
      "1",
      key,
      failed ? "1" : "0",
      String(latency),
      String(Date.now()),
      String(cooldownMs),
      permit?.identity ?? crypto.randomUUID(),
      permit ? (permit.halfOpen ? "probe" : "closed") : "",
      permit?.epoch ?? "",
    ]);
  }
}
