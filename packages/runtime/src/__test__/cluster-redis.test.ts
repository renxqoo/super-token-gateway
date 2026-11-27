import { expect, test } from "bun:test";
import type { RedisClient } from "bun";
import {
  BATCH_ACQUIRE_LUA,
  batchAcquireSha,
  buildBatchAcquireArgs,
  createQuotaKeyCache,
  decodeAcquireFailure,
  evalPermitPrepared,
  type IngressIpCheck,
  type KeyTriple,
} from "../cluster-redis.ts";

function makeItem(
  keys: KeyTriple,
  member: string,
  overrides: Partial<{
    rpm: string;
    tpm: string;
    concurrency: string;
    estimatedTokens: string;
    leaseTtlStr: string;
    ingressIp: IngressIpCheck;
  }> = {},
) {
  return {
    keys,
    rpm: overrides.rpm ?? "1000",
    tpm: overrides.tpm ?? "100000",
    concurrency: overrides.concurrency ?? "64",
    estimatedTokens: overrides.estimatedTokens ?? "50",
    leaseTtlStr: overrides.leaseTtlStr ?? "120000",
    member,
    ...(overrides.ingressIp ? { ingressIp: overrides.ingressIp } : {}),
  };
}

test("buildBatchAcquireArgs deduplicates identical scopes across cached references and distinct triples", () => {
  const keyCache = createQuotaKeyCache("ns");
  const shared = keyCache("acct-1");
  const cloned: KeyTriple = {
    rpmKey: shared.rpmKey,
    tpmKey: shared.tpmKey,
    concKey: shared.concKey,
  };
  const items = [
    makeItem(shared, "m1"),
    makeItem(shared, "m2"),
    makeItem(cloned, "m3"),
    makeItem(shared, "m4"),
  ];

  const { shaArgs, minIpTimeoutMs } = buildBatchAcquireArgs(items, undefined);

  expect(minIpTimeoutMs).toBeUndefined();
  expect(shaArgs[0]).toBe(batchAcquireSha);
  expect(shaArgs[1]).toBe("3");
  expect(shaArgs.slice(2, 5)).toEqual([shared.rpmKey, shared.tpmKey, shared.concKey]);

  const argv = shaArgs.slice(5);
  expect(argv.slice(0, 3)).toEqual(["4", "1", "0"]);
  for (let i = 0; i < 4; i++) {
    const base = 3 + i * 7;
    expect(argv[base + 5]).toBe(`m${i + 1}`);
    expect(argv[base + 6]).toBe("1");
  }
  expect(argv.length).toBe(3 + 4 * 7);
});

test("buildBatchAcquireArgs deduplicates two interleaved scopes and appends piggyback releases at exact offsets", () => {
  const keyCache = createQuotaKeyCache("stg");
  const acct = keyCache("billing_account:a1");
  const chan = keyCache("channel:c1");
  const items = [
    makeItem(acct, "m1"),
    makeItem(chan, "m2"),
    makeItem(acct, "m3"),
    makeItem(chan, "m4"),
    makeItem({ rpmKey: chan.rpmKey, tpmKey: chan.tpmKey, concKey: chan.concKey }, "m5"),
  ];
  const relGroups = new Map<string, string[]>([
    [acct.concKey, ["old-a1", "old-a2"]],
    [chan.concKey, ["old-c1"]],
  ]);

  const { shaArgs, minIpTimeoutMs } = buildBatchAcquireArgs(items, relGroups);

  expect(minIpTimeoutMs).toBeUndefined();
  expect(shaArgs[1]).toBe("8");
  expect(shaArgs.slice(2, 10)).toEqual([
    acct.rpmKey,
    acct.tpmKey,
    acct.concKey,
    chan.rpmKey,
    chan.tpmKey,
    chan.concKey,
    acct.concKey,
    chan.concKey,
  ]);

  const argv = shaArgs.slice(10);
  expect(argv.slice(0, 3)).toEqual(["5", "2", "0"]);
  const scopeIndices = [0, 1, 2, 3, 4].map((i) => argv[3 + i * 7 + 6]);
  expect(scopeIndices).toEqual(["1", "2", "1", "2", "2"]);
  expect(argv.slice(3 + 5 * 7)).toEqual(["2", "old-a1", "old-a2", "1", "old-c1"]);
});

test("buildBatchAcquireArgs encodes multi-scope, multi-IP, mixed non-IP, and piggyback releases with exact layout", () => {
  const keyCache = createQuotaKeyCache("prod");
  const s1 = keyCache("acct-1");
  const s2 = keyCache("acct-2");
  const s3 = keyCache("acct-3");
  const ipA: IngressIpCheck = { ip: "198.51.100.1", limit: 100, windowMs: 60_000, timeoutMs: 250 };
  const ipB: IngressIpCheck = {
    ip: "198.51.100.2",
    limit: 50,
    windowMs: 30_000,
    timeoutMs: 120,
    limitStr: "50",
    windowMsStr: "30000",
  };
  const items = [
    makeItem(s1, "m1", { ingressIp: ipA }),
    makeItem(s2, "m2"),
    makeItem(s3, "m3", { ingressIp: ipB }),
    makeItem(s1, "m4", { ingressIp: ipA }),
    makeItem(s3, "m5", { ingressIp: ipB }),
  ];
  const relGroups = new Map<string, string[]>([[s2.concKey, ["rel-1"]]]);

  const { shaArgs, minIpTimeoutMs } = buildBatchAcquireArgs(
    items,
    relGroups,
    (ip) => `prod:ingress:ip:${ip}`,
  );

  expect(minIpTimeoutMs).toBe(120);
  const numKeys = Number(shaArgs[1]);
  expect(numKeys).toBe(3 * 3 + 2 + 1);
  const keys = shaArgs.slice(2, 2 + numKeys);
  expect(keys).toEqual([
    s1.rpmKey,
    s1.tpmKey,
    s1.concKey,
    s2.rpmKey,
    s2.tpmKey,
    s2.concKey,
    s3.rpmKey,
    s3.tpmKey,
    s3.concKey,
    "prod:ingress:ip:198.51.100.1",
    "prod:ingress:ip:198.51.100.2",
    s2.concKey,
  ]);

  const argv = shaArgs.slice(2 + numKeys);
  expect(argv.slice(0, 3)).toEqual(["5", "3", "2"]);
  const itemRefs = [0, 1, 2, 3, 4].map((i) => ({
    member: argv[3 + i * 8 + 5],
    scopeIdx: argv[3 + i * 8 + 6],
    ipIdx: argv[3 + i * 8 + 7],
  }));
  expect(itemRefs).toEqual([
    { member: "m1", scopeIdx: "1", ipIdx: "1" },
    { member: "m2", scopeIdx: "2", ipIdx: "0" },
    { member: "m3", scopeIdx: "3", ipIdx: "2" },
    { member: "m4", scopeIdx: "1", ipIdx: "1" },
    { member: "m5", scopeIdx: "3", ipIdx: "2" },
  ]);
  const ipMetaStart = 3 + 5 * 8;
  expect(argv.slice(ipMetaStart, ipMetaStart + 4)).toEqual(["100", "60000", "50", "30000"]);
  expect(argv.slice(ipMetaStart + 4)).toEqual(["1", "rel-1"]);
});

test("buildBatchAcquireArgs keeps KEYS bounded to 3 on large single-scope batches and encodes >1000 piggyback releases with deduplicated KEYS", () => {
  const keyCache = createQuotaKeyCache("large");
  const scope = keyCache("hot-account");
  const items = Array.from({ length: 600 }, (_, i) => makeItem(scope, `m-${i}`));

  const { shaArgs, minIpTimeoutMs } = buildBatchAcquireArgs(items, undefined);

  expect(minIpTimeoutMs).toBeUndefined();
  expect(shaArgs[0]).toBe(batchAcquireSha);
  expect(new Bun.CryptoHasher("sha1").update(BATCH_ACQUIRE_LUA).digest("hex")).toBe(
    batchAcquireSha,
  );
  expect(shaArgs[1]).toBe("3");
  expect(shaArgs.slice(2, 5)).toEqual([scope.rpmKey, scope.tpmKey, scope.concKey]);
  const argv = shaArgs.slice(5);
  expect(argv.slice(0, 3)).toEqual(["600", "1", "0"]);
  expect(argv.length).toBe(3 + 600 * 7);
  expect(argv[3 + 5]).toBe("m-0");
  expect(argv[3 + 6]).toBe("1");
  expect(argv[3 + 599 * 7 + 5]).toBe("m-599");
  expect(argv[3 + 599 * 7 + 6]).toBe("1");

  const releases = Array.from({ length: 1_250 }, (_, i) => `rel-${i}`);
  const relGroups = new Map<string, string[]>([[scope.concKey, releases]]);
  const withReleases = buildBatchAcquireArgs(items, relGroups);

  expect(withReleases.minIpTimeoutMs).toBeUndefined();
  expect(withReleases.shaArgs[0]).toBe(batchAcquireSha);
  expect(withReleases.shaArgs[1]).toBe("4");
  expect(withReleases.shaArgs.slice(2, 6)).toEqual([
    scope.rpmKey,
    scope.tpmKey,
    scope.concKey,
    scope.concKey,
  ]);
  const relArgv = withReleases.shaArgs.slice(6);
  expect(relArgv.slice(0, 3)).toEqual(["600", "1", "0"]);
  const relOffset = 3 + 600 * 7;
  expect(relArgv[relOffset]).toBe("1250");
  expect(relArgv[relOffset + 1]).toBe("rel-0");
  expect(relArgv[relOffset + 1000]).toBe("rel-999");
  expect(relArgv[relOffset + 1250]).toBe("rel-1249");
  expect(relArgv.length).toBe(relOffset + 1 + 1_250);
});

test("decodeAcquireFailure maps every Redis Lua status code accurately", () => {
  const ip: IngressIpCheck = { ip: "198.51.100.9", limit: 10, windowMs: 1000, timeoutMs: 200 };
  expect(decodeAcquireFailure(0, ip)).toBeNull();
  expect(decodeAcquireFailure(1, ip)).toEqual({ ok: false, reason: "rpm" });
  expect(decodeAcquireFailure(2, ip)).toEqual({ ok: false, reason: "tpm" });
  expect(decodeAcquireFailure(3, ip)).toEqual({ ok: false, reason: "concurrency" });
  expect(decodeAcquireFailure(4, ip)).toEqual({ ok: false, reason: "ingress_quota_unavailable" });
  expect(decodeAcquireFailure(-750, ip)).toEqual({
    ok: false,
    reason: "ip_rate_limited",
    retryAfterMs: 750,
  });
  expect(decodeAcquireFailure(99)).toEqual({ ok: false, reason: "unavailable" });
  expect(decodeAcquireFailure(99, ip)).toEqual({ ok: false, reason: "ingress_quota_unavailable" });
});

test("evalPermitPrepared retries NOSCRIPT with EVAL unless retry is disallowed", async () => {
  const calls: { cmd: string; args: string[] }[] = [];
  const fakeRedis = {
    async send(cmd: string, args: string[]) {
      calls.push({ cmd, args });
      if (cmd === "EVALSHA") throw new Error("NOSCRIPT No matching script");
      return [0];
    },
  } as unknown as RedisClient;

  const res = await evalPermitPrepared(fakeRedis, "return {0}", ["sha1", "0"]);
  expect(res).toEqual([0]);
  expect(calls).toEqual([
    { cmd: "EVALSHA", args: ["sha1", "0"] },
    { cmd: "EVAL", args: ["return {0}", "0"] },
  ]);

  await expect(
    evalPermitPrepared(fakeRedis, "return {0}", ["sha1", "0"], () => false),
  ).rejects.toThrow("NOSCRIPT");
});
