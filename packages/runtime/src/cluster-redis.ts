import type { RedisClient } from "bun";

export type IngressIpCheck = {
  ip: string;
  limit: number;
  windowMs: number;
  timeoutMs: number;
  limitStr?: string;
  windowMsStr?: string;
};

export type PendingAcquire = {
  redis: RedisClient;
  keys: KeyTriple;
  rpm: string;
  tpm: string;
  concurrency: string;
  estimatedTokens: string;
  leaseTtlMs: number;
  leaseTtlStr: string;
  member: string;
  ingressIp: IngressIpCheck | undefined;
  onRevoke: (() => void) | undefined;
  resolve: (result: RateLimitAcquireResult) => void;
};

export type ActivePermit = {
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

export const BATCH_ACQUIRE_LUA = `
local n = tonumber(ARGV[1])
local scopeCount = tonumber(ARGV[2])
local ipCount = tonumber(ARGV[3])
local argStride = ipCount > 0 and 8 or 7
local ipKeyBase = scopeCount * 3
local rkStart = ipKeyBase + ipCount + 1
local ipMetaBase = 4 + n * argStride
if rkStart <= #KEYS then
  local ridx = ipMetaBase + ipCount * 2
  for ri = rkStart, #KEYS do
    local cnt = tonumber(ARGV[ridx])
    ridx = ridx + 1
    if cnt == 1 then
      redis.call('ZREM', KEYS[ri], ARGV[ridx])
      ridx = ridx + 1
    elseif cnt > 1 then
      local rEnd = ridx + cnt - 1
      local p = ridx
      while p <= rEnd do
        local chunkEnd = p + 999
        if chunkEnd > rEnd then chunkEnd = rEnd end
        redis.call('ZREM', KEYS[ri], unpack(ARGV, p, chunkEnd))
        p = chunkEnd + 1
      end
      ridx = rEnd + 1
    end
  end
end
local time = redis.call('TIME')
local nowMs = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local out = {}
local concCount = {}
local concInitial = {}
local concAdds = {}
local concMaxExp = {}
local rpmCur = {}
local rpmBase = {}
local rpmDelta = {}
local tpmCur = {}
local tpmBase = {}
local tpmDelta = {}
local ipCur, ipBase, ipDelta, ipWin, ipTtl, ipBad, ipLimits
if ipCount > 0 then
  ipCur = {}
  ipBase = {}
  ipDelta = {}
  ipWin = {}
  ipTtl = {}
  ipBad = {}
  ipLimits = {}
  for g = 1, ipCount do
    local ipKey = KEYS[ipKeyBase + g]
    local ipLimit = tonumber(ARGV[ipMetaBase + (g - 1) * 2])
    local ipWindow = tonumber(ARGV[ipMetaBase + (g - 1) * 2 + 1])
    ipLimits[g] = ipLimit
    local count = tonumber(redis.call('GET', ipKey) or '0')
    if not count or count < 0 or count ~= math.floor(count) then
      ipBad[g] = true
    elseif count == 0 then
      ipCur[g] = 0
      ipBase[g] = 0
      ipWin[g] = ipWindow
      ipTtl[g] = math.max(1, ipWindow)
    else
      local ttl = redis.call('PTTL', ipKey)
      if ttl < 0 then
        redis.call('PEXPIRE', ipKey, ipWindow)
        ttl = ipWindow
      end
      ipCur[g] = count
      ipBase[g] = count
      ipWin[g] = ipWindow
      ipTtl[g] = math.max(1, ttl)
    end
  end
end
for i = 1, n do
  local ai = 3 + (i - 1) * argStride
  local ipDenied = false
  if ipCount > 0 then
    local g = tonumber(ARGV[ai + 8])
    if g > 0 then
      if ipBad[g] then
        out[i] = 4
        ipDenied = true
      elseif ipCur[g] >= ipLimits[g] then
        out[i] = -ipTtl[g]
        ipDenied = true
      else
        ipCur[g] = ipCur[g] + 1
        ipDelta[g] = (ipDelta[g] or 0) + 1
      end
    end
  end
  if not ipDenied then
    local s = tonumber(ARGV[ai + 7])
    local ki = (s - 1) * 3
    local rpmLimit = tonumber(ARGV[ai + 1])
    local tpmLimit = tonumber(ARGV[ai + 2])
    local concLimit = tonumber(ARGV[ai + 3])
    local estTokens = tonumber(ARGV[ai + 4])
    local leaseTtlMs = tonumber(ARGV[ai + 5])
    local member = ARGV[ai + 6]

    local active = concCount[s]
    if active == nil then
      local concKey = KEYS[ki + 3]
      redis.call('ZREMRANGEBYSCORE', concKey, '-inf', nowMs)
      active = redis.call('ZCARD', concKey)
      concCount[s] = active
      concInitial[s] = active
    end
    if active >= concLimit then
      out[i] = 3
    else
      local curRpm = rpmCur[s]
      if curRpm == nil then
        curRpm = tonumber(redis.call('GET', KEYS[ki + 1]) or '0')
        rpmCur[s] = curRpm
        rpmBase[s] = curRpm
      end
      if curRpm >= rpmLimit then
        out[i] = 1
      else
        local curTpm = tpmCur[s]
        if curTpm == nil then
          curTpm = tonumber(redis.call('GET', KEYS[ki + 2]) or '0')
          tpmCur[s] = curTpm
          tpmBase[s] = curTpm
        end
        if curTpm + estTokens > tpmLimit then
          out[i] = 2
        else
          rpmCur[s] = curRpm + 1
          rpmDelta[s] = (rpmDelta[s] or 0) + 1
          tpmCur[s] = curTpm + estTokens
          tpmDelta[s] = (tpmDelta[s] or 0) + estTokens
          concCount[s] = active + 1
          local exp = nowMs + leaseTtlMs
          local prevMax = concMaxExp[s]
          if not prevMax or exp > prevMax then
            concMaxExp[s] = exp
          end
          local adds = concAdds[s]
          if adds == nil then
            concAdds[s] = { exp, member }
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
end
if ipCount > 0 then
  for g = 1, ipCount do
    local delta = ipDelta[g]
    if delta then
      local k = KEYS[ipKeyBase + g]
      if ipBase[g] == 0 then
        redis.call('SET', k, delta, 'PX', ipWin[g])
      else
        redis.call('INCRBY', k, delta)
      end
    end
  end
end
for s = 1, scopeCount do
  local delta = rpmDelta[s]
  if delta then
    local k = KEYS[(s - 1) * 3 + 1]
    local nextVal = redis.call('INCRBY', k, delta)
    if nextVal == delta or rpmBase[s] == 0 then
      redis.call('PEXPIRE', k, 60000)
    end
  end
end
for s = 1, scopeCount do
  local delta = tpmDelta[s]
  if delta and delta > 0 then
    local k = KEYS[(s - 1) * 3 + 2]
    local nextVal = redis.call('INCRBY', k, delta)
    if nextVal == delta or tpmBase[s] == 0 then
      redis.call('PEXPIRE', k, 60000)
    end
  end
end
for s = 1, scopeCount do
  local adds = concAdds[s]
  if adds then
    local k = KEYS[(s - 1) * 3 + 3]
    local total = #adds
    if total <= 1000 then
      redis.call('ZADD', k, unpack(adds))
    else
      local p = 1
      while p <= total do
        local chunkEnd = p + 999
        if chunkEnd > total then chunkEnd = total end
        redis.call('ZADD', k, unpack(adds, p, chunkEnd))
        p = chunkEnd + 1
      end
    end
    if concInitial[s] > 0 then
      local latest = redis.call('ZREVRANGE', k, 0, 0, 'WITHSCORES')
      if latest[2] then
        redis.call('PEXPIREAT', k, math.ceil(tonumber(latest[2])))
      end
    else
      redis.call('PEXPIREAT', k, math.ceil(concMaxExp[s]))
    end
  end
end
return out
`;

export const RENEW_LUA = `
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

export const BATCH_RENEW_LUA = `
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

export const batchAcquireSha = new Bun.CryptoHasher("sha1").update(BATCH_ACQUIRE_LUA).digest("hex");
export const renewSha = new Bun.CryptoHasher("sha1").update(RENEW_LUA).digest("hex");
export const batchRenewSha = new Bun.CryptoHasher("sha1").update(BATCH_RENEW_LUA).digest("hex");

export const BATCH_INGRESS_WINDOW_LUA = `
local n = #KEYS
local out = {}
for i = 1, n do
  local key = KEYS[i]
  local ai = (i - 1) * 3
  local limit = tonumber(ARGV[ai + 1])
  local window = tonumber(ARGV[ai + 2])
  local want = tonumber(ARGV[ai + 3])
  local count = tonumber(redis.call('GET', key) or '0')
  if not count or count < 0 or count ~= math.floor(count) then
    out[(i - 1) * 2 + 1] = -1
    out[(i - 1) * 2 + 2] = 0
  elseif count == 0 then
    local take = math.min(limit, want)
    if take > 0 then
      redis.call('SET', key, take, 'PX', window)
    end
    out[(i - 1) * 2 + 1] = take
    out[(i - 1) * 2 + 2] = math.max(1, window)
  else
    local ttl = redis.call('PTTL', key)
    if ttl < 0 then
      redis.call('PEXPIRE', key, window)
      ttl = window
    end
    local rem = limit - count
    if rem <= 0 then
      out[(i - 1) * 2 + 1] = 0
      out[(i - 1) * 2 + 2] = math.max(1, ttl)
    else
      local take = math.min(rem, want)
      redis.call('INCRBY', key, take)
      out[(i - 1) * 2 + 1] = take
      out[(i - 1) * 2 + 2] = math.max(1, ttl)
    end
  end
end
return out
`;
export const batchIngressWindowSha = new Bun.CryptoHasher("sha1")
  .update(BATCH_INGRESS_WINDOW_LUA)
  .digest("hex");

export const BATCH_RELEASE_LUA = `
local idx = 1
for i = 1, #KEYS do
  local cnt = tonumber(ARGV[idx])
  idx = idx + 1
  if cnt == 1 then
    redis.call('ZREM', KEYS[i], ARGV[idx])
    idx = idx + 1
  elseif cnt > 1 then
    redis.call('ZREM', KEYS[i], unpack(ARGV, idx, idx + cnt - 1))
    idx = idx + cnt
  end
end
return 1
`;
export const batchReleaseSha = new Bun.CryptoHasher("sha1").update(BATCH_RELEASE_LUA).digest("hex");

const SMALL_INTS = Array.from({ length: 257 }, (_, i) => String(i));

function smallIntStr(v: number): string {
  return v >= 0 && v < SMALL_INTS.length ? SMALL_INTS[v]! : String(v);
}

export function buildBatchAcquireArgs(
  items: {
    keys: KeyTriple;
    rpm: string;
    tpm: string;
    concurrency: string;
    estimatedTokens: string;
    leaseTtlStr: string;
    member: string;
    ingressIp?: IngressIpCheck | undefined;
  }[],
  relGroups: Map<string, string[]> | undefined,
  ingressKeyFor?: (ip: string) => string,
): { shaArgs: string[]; minIpTimeoutMs: number | undefined } {
  const n = items.length;
  const scopeKeys: KeyTriple[] = [];
  let itemScopeIndices: Int32Array | undefined;
  let scopeRefIndex: Map<KeyTriple, number> | undefined;
  let scopeStrIndex: Map<string, number> | undefined;
  let s0: KeyTriple | undefined;
  let s1: KeyTriple | undefined;
  if (n > 0) {
    s0 = items[0]!.keys;
    scopeKeys.push(s0);
  }
  for (let i = 1; i < n; i++) {
    const k = items[i]!.keys;
    if (k === s0) {
      if (itemScopeIndices) itemScopeIndices[i] = 1;
      continue;
    }
    if (s1 !== undefined && k === s1) {
      itemScopeIndices![i] = 2;
      continue;
    }
    if (scopeRefIndex) {
      let idx = scopeRefIndex.get(k);
      if (idx === undefined) {
        const sk = `${k.rpmKey}\0${k.tpmKey}\0${k.concKey}`;
        idx = scopeStrIndex!.get(sk);
        if (idx === undefined) {
          scopeKeys.push(k);
          idx = scopeKeys.length;
          scopeStrIndex!.set(sk, idx);
        }
        scopeRefIndex.set(k, idx);
      }
      itemScopeIndices![i] = idx;
      continue;
    }
    if (k.rpmKey === s0!.rpmKey && k.tpmKey === s0!.tpmKey && k.concKey === s0!.concKey) {
      if (itemScopeIndices) itemScopeIndices[i] = 1;
      continue;
    }
    if (s1 === undefined) {
      s1 = k;
      scopeKeys.push(k);
      itemScopeIndices = new Int32Array(n);
      itemScopeIndices.fill(1, 0, i);
      itemScopeIndices[i] = 2;
      continue;
    }
    if (k.rpmKey === s1.rpmKey && k.tpmKey === s1.tpmKey && k.concKey === s1.concKey) {
      itemScopeIndices![i] = 2;
      continue;
    }
    scopeRefIndex = new Map([
      [s0!, 1],
      [s1, 2],
    ]);
    scopeStrIndex = new Map([
      [`${s0!.rpmKey}\0${s0!.tpmKey}\0${s0!.concKey}`, 1],
      [`${s1.rpmKey}\0${s1.tpmKey}\0${s1.concKey}`, 2],
    ]);
    const sk = `${k.rpmKey}\0${k.tpmKey}\0${k.concKey}`;
    let idx = scopeStrIndex.get(sk);
    if (idx === undefined) {
      scopeKeys.push(k);
      idx = scopeKeys.length;
      scopeStrIndex.set(sk, idx);
    }
    scopeRefIndex.set(k, idx);
    itemScopeIndices![i] = idx;
  }

  let ipKeys: string[] | undefined;
  let ipLimits: string[] | undefined;
  let ipWindows: string[] | undefined;
  let ipKeyIndex: Map<string, number> | undefined;
  let itemIpIndices: Int32Array | undefined;
  let minIpTimeoutMs: number | undefined;
  let singleIpKey: string | undefined;
  for (let i = 0; i < n; i++) {
    const ipCheck = items[i]!.ingressIp;
    if (!ipCheck) continue;
    if (minIpTimeoutMs === undefined || ipCheck.timeoutMs < minIpTimeoutMs) {
      minIpTimeoutMs = ipCheck.timeoutMs;
    }
    const key = ingressKeyFor!(ipCheck.ip);
    if (!ipKeys) {
      ipKeys = [key];
      ipLimits = [ipCheck.limitStr ?? String(ipCheck.limit)];
      ipWindows = [ipCheck.windowMsStr ?? String(ipCheck.windowMs)];
      itemIpIndices = new Int32Array(n);
      singleIpKey = key;
      itemIpIndices[i] = 1;
      continue;
    }
    if (singleIpKey !== undefined && key === singleIpKey) {
      itemIpIndices![i] = 1;
      continue;
    }
    if (!ipKeyIndex) {
      ipKeyIndex = new Map([[singleIpKey!, 1]]);
      singleIpKey = undefined;
    }
    let idx = ipKeyIndex.get(key);
    if (idx === undefined) {
      ipKeys.push(key);
      ipLimits!.push(ipCheck.limitStr ?? String(ipCheck.limit));
      ipWindows!.push(ipCheck.windowMsStr ?? String(ipCheck.windowMs));
      idx = ipKeys.length;
      ipKeyIndex.set(key, idx);
    }
    itemIpIndices![i] = idx;
  }
  const scopeCount = scopeKeys.length;
  const ipCount = ipKeys ? ipKeys.length : 0;
  const shaArgs: string[] = [
    batchAcquireSha,
    smallIntStr(scopeCount * 3 + ipCount + (relGroups ? relGroups.size : 0)),
  ];
  for (let s = 0; s < scopeCount; s++) {
    const k = scopeKeys[s]!;
    shaArgs.push(k.rpmKey, k.tpmKey, k.concKey);
  }
  if (ipKeys) {
    for (let i = 0; i < ipKeys.length; i++) shaArgs.push(ipKeys[i]!);
  }
  if (relGroups) {
    for (const key of relGroups.keys()) shaArgs.push(key);
  }
  shaArgs.push(smallIntStr(n), smallIntStr(scopeCount), smallIntStr(ipCount));
  if (ipCount === 0) {
    for (let i = 0; i < n; i++) {
      const it = items[i]!;
      const scopeStr = itemScopeIndices ? smallIntStr(itemScopeIndices[i]!) : "1";
      shaArgs.push(
        it.rpm,
        it.tpm,
        it.concurrency,
        it.estimatedTokens,
        it.leaseTtlStr,
        it.member,
        scopeStr,
      );
    }
  } else {
    for (let i = 0; i < n; i++) {
      const it = items[i]!;
      const scopeStr = itemScopeIndices ? smallIntStr(itemScopeIndices[i]!) : "1";
      const ipIdx = itemIpIndices![i]!;
      shaArgs.push(
        it.rpm,
        it.tpm,
        it.concurrency,
        it.estimatedTokens,
        it.leaseTtlStr,
        it.member,
        scopeStr,
        smallIntStr(ipIdx),
      );
    }
    for (let g = 0; g < ipCount; g++) {
      shaArgs.push(ipLimits![g]!, ipWindows![g]!);
    }
  }
  if (relGroups) {
    for (const members of relGroups.values()) {
      shaArgs.push(smallIntStr(members.length));
      for (let i = 0; i < members.length; i++) shaArgs.push(members[i]!);
    }
  }
  return { shaArgs, minIpTimeoutMs };
}

export async function evalPermitPrepared(
  redis: RedisClient,
  script: string,
  shaArgs: string[],
  mayRetry?: () => boolean,
): Promise<unknown> {
  try {
    return await redis.send("EVALSHA", shaArgs);
  } catch (error) {
    if (!(error instanceof Error) || !/^NOSCRIPT\b/.test(error.message) || mayRetry?.() === false)
      throw error;
    const evalArgs = shaArgs.slice();
    evalArgs[0] = script;
    return redis.send("EVAL", evalArgs);
  }
}

export async function withRedisDeadline<T>(
  work: Promise<T>,
  timeoutMs: number,
  onTimeout?: () => void,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      onTimeout?.();
      reject(new Error("Redis request timed out"));
    }, timeoutMs);
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
  | {
      ok: false;
      reason: "rpm" | "tpm" | "concurrency" | "unavailable" | "ingress_quota_unavailable";
    }
  | { ok: false; reason: "ip_rate_limited"; retryAfterMs: number };

export function failAcquire(ip?: IngressIpCheck): RateLimitAcquireResult {
  return { ok: false, reason: ip ? "ingress_quota_unavailable" : "unavailable" };
}

export function decodeAcquireFailure(
  code: number,
  ip?: IngressIpCheck,
): RateLimitAcquireResult | null {
  if (code === 0) return null;
  if (code < 0 && Number.isSafeInteger(code)) {
    return { ok: false, reason: "ip_rate_limited", retryAfterMs: -code };
  }
  if (code === 4) return { ok: false, reason: "ingress_quota_unavailable" };
  if (code === 1) return { ok: false, reason: "rpm" };
  if (code === 2) return { ok: false, reason: "tpm" };
  if (code === 3) return { ok: false, reason: "concurrency" };
  return failAcquire(ip);
}

const KEY_CACHE_MAX = 8192;

export type KeyTriple = {
  rpmKey: string;
  tpmKey: string;
  concKey: string;
};

export function createQuotaKeyCache(namespace: string) {
  const cached = new Map<string, KeyTriple>();
  return (key: string): KeyTriple => {
    let entry = cached.get(key);
    if (!entry) {
      if (cached.size >= KEY_CACHE_MAX) cached.clear();
      entry = {
        rpmKey: `${namespace}:rl:rpm:${key}`,
        tpmKey: `${namespace}:rl:tpm:${key}`,
        concKey: `${namespace}:rl:conc:${key}`,
      };
      cached.set(key, entry);
    }
    return entry;
  };
}
