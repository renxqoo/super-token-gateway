import { expect, test } from "bun:test";
import { RedisClient } from "bun";
import { createQuotaLeases } from "@gateway/runtime/quota-leases";
import { sha256 } from "@gateway/runtime/crypto";
import { appendFile } from "node:fs/promises";
async function withQuota(
  run: (
    quota: ReturnType<typeof createQuotaLeases>,
    redis: RedisClient,
    namespace: string,
  ) => Promise<void>,
) {
  const namespace = `financial_quota_${crypto.randomUUID().replaceAll("-", "")}`,
    quota = createQuotaLeases({ urls: [process.env.REDIS_URL!], namespace, generation: "g1" }),
    redis = new RedisClient(process.env.REDIS_URL!);
  await appendFile(
    ".local/T04b2a-resources.jsonl",
    JSON.stringify({ event: "redis_namespace_created", namespace }) + "\n",
    { mode: 0o600 },
  );
  try {
    await redis.connect();
    await run(quota, redis, namespace);
  } finally {
    quota.close();
    const keys = (await redis.send("KEYS", [`${namespace}:*`])) as string[];
    for (const key of keys) await redis.send("DEL", [key]);
    redis.close();
    await appendFile(
      ".local/T04b2a-resources.jsonl",
      JSON.stringify({ event: "redis_namespace_removed", namespace }) + "\n",
      { mode: 0o600 },
    );
  }
}
function input() {
  return {
    scope: {
      kind: "project" as const,
      id: crypto.randomUUID(),
      rpm: 10,
      tpm: 1000,
      concurrency: 10,
    },
    caller: "gateway:synthetic",
    kind: "execution_admit_v1",
    command_id: crypto.randomUUID(),
    body_digest: sha256("command"),
    owner_session_id: crypto.randomUUID(),
    deadline_ms: Date.now() + 90000,
    tokens: 90,
  };
}
async function keyFor(redis: RedisClient, namespace: string, suffix: string) {
  return ((await redis.send("KEYS", [`${namespace}:*${suffix}`])) as string[])[0]!;
}
test(
  "shared acquire never double charges RPM or TPM and rejected cleanup cannot delete an adopted winner",
  () =>
    withQuota(async (q, redis, namespace) => {
      const command = input(),
        a = await q.acquire(command),
        b = await q.acquire(command);
      expect(a.descriptor.lease_id).toBe(b.descriptor.lease_id);
      const rpm = await keyFor(redis, namespace, ":rpm"),
        bucket = await keyFor(redis, namespace, `:tokens:${a.descriptor.bucket}`),
        active = await keyFor(redis, namespace, ":active");
      expect(Number(await redis.send("ZCARD", [rpm]))).toBe(1);
      expect(await redis.send("GET", [bucket])).toBe("90");
      await expect(q.acquire({ ...command, tokens: 91 })).rejects.toThrow();
      const attempt = crypto.randomUUID(),
        adopted = await q.adopt(a.descriptor, attempt, "1", "adopt");
      await expect(
        q.release(b.descriptor, {
          state: "REJECTED",
          command_id: command.command_id,
          body_digest: command.body_digest,
        }),
      ).rejects.toThrow();
      expect(Number(await redis.send("ZCARD", [active]))).toBe(1);
      const renewed = await q.renew(adopted.descriptor, attempt, "1", "renew"),
        oldReplay = await q.adopt(a.descriptor, attempt, "1", "adopt");
      expect(oldReplay.descriptor.revision).toBe(adopted.descriptor.revision);
      expect(renewed.descriptor.revision).toBe("2");
      const released = await q.release(renewed.descriptor, {
        state: "RELEASE_PENDING",
        attempt_id: attempt,
        permit_epoch: "1",
      });
      expect(released.descriptor.state).toBe("RELEASED");
      await expect(q.renew(renewed.descriptor, attempt, "1", "late-renew")).rejects.toThrow();
      expect(Number(await redis.send("ZCARD", [active]))).toBe(0);
      expect(Number(await redis.send("ZCARD", [rpm]))).toBe(1);
      await q.reconcile(released.descriptor, attempt, "1", 40);
      await q.reconcile(released.descriptor, attempt, "1", 40);
      expect(await redis.send("GET", [bucket])).toBe("40");
      await redis.send("PEXPIRE", [bucket, "1"]);
      await Bun.sleep(5);
      expect(await redis.send("GET", [bucket])).toBeNull();
      const sentinel = `${bucket}next`;
      await redis.send("SET", [sentinel, "70", "PX", "5000"]);
      await q.reconcile(released.descriptor, attempt, "1", 10);
      expect(await redis.send("GET", [bucket])).toBeNull();
      expect(await redis.send("GET", [sentinel])).toBe("70");
    }),
  10000,
);

test(
  "DRAFT expires after real 30 seconds and its finite tombstone prevents a second charge",
  () =>
    withQuota(async (q, redis, namespace) => {
      const command = input(),
        a = await q.acquire(command),
        lease = await keyFor(redis, namespace, `:lease:${a.descriptor.lease_id}`),
        rpm = await keyFor(redis, namespace, ":rpm");
      const first = Number(await redis.send("PTTL", [lease]));
      expect(first).toBeGreaterThan(100000);
      expect(first).toBeLessThanOrEqual(120000);
      await Bun.sleep(100);
      const b = await q.acquire(command);
      expect(b.descriptor.remaining_ms).toBeLessThan(a.descriptor.remaining_ms);
      expect(b.descriptor.remaining_ms).toBeLessThan(30000);
      await Bun.sleep(30020);
      await expect(q.acquire(command)).rejects.toThrow();
      expect(Number(await redis.send("ZCARD", [rpm]))).toBe(1);
      expect(Number(await redis.send("PTTL", [lease]))).toBeGreaterThan(0);
    }),
  40000,
);
