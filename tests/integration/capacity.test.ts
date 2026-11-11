import { expect, test } from "bun:test";
import { RedisClient } from "bun";
import { cleanupCapacityNamespaces } from "../../scripts/capacity-cleanup.ts";

test("an unavailable cleanup shard cannot leave another shard's fixture keys or delete unrelated data", async () => {
  const url = process.env.REDIS_URL;
  if (!url) throw new Error("REDIS_URL is required");
  const redis = new RedisClient(url);
  const namespace = `bench:${crypto.randomUUID().replaceAll("-", "")}`;
  const marker = `${namespace}:marker`,
    unrelated = `other:${crypto.randomUUID()}`;
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response() });
  const unavailable = `redis://127.0.0.1:${server.port}`;
  await server.stop(true);
  try {
    await redis.send("SETEX", [marker, "60", "fixture"]);
    await redis.send("SETEX", [unrelated, "60", "preserve"]);
    expect(await cleanupCapacityNamespaces([unavailable, url], namespace)).toEqual({
      cleaned: 1,
      failed: 1,
    });
    expect(await redis.get(marker)).toBeNull();
    expect(await redis.get(unrelated)).toBe("preserve");
  } finally {
    await redis.send("DEL", [marker, unrelated]);
    redis.close();
  }
});

test("cleanup keeps neighboring namespaces even when their names contain the fixture identity", async () => {
  const url = process.env.REDIS_URL;
  if (!url) throw new Error("REDIS_URL is required");
  const redis = new RedisClient(url);
  const namespace = `bench:${crypto.randomUUID().replaceAll("-", "")}`;
  const owned = [`${namespace}:marker`, `{${namespace}:usage}:marker`];
  const neighbors = [
    `other:${namespace}:marker`,
    `${namespace}suffix:marker`,
    `{${namespace}:other}:marker`,
  ];
  try {
    for (const key of [...owned, ...neighbors]) await redis.send("SETEX", [key, "60", "preserve"]);
    expect(await cleanupCapacityNamespaces([url], namespace)).toEqual({ cleaned: 1, failed: 0 });
    for (const key of owned) expect(await redis.get(key)).toBeNull();
    for (const key of neighbors) expect(await redis.get(key)).toBe("preserve");
  } finally {
    await redis.send("DEL", [...owned, ...neighbors]);
    redis.close();
  }
});
