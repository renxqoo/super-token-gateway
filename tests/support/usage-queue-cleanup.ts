import { RedisClient } from "bun";

export function createUsageQueueCleanup(namespace: string) {
  if (
    !/^test-usage:[a-z][a-z0-9-]{0,31}:[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(
      namespace,
    )
  )
    throw new Error("Usage queue cleanup requires an owned test namespace");
  const prefix = `{${namespace}:usage}:usage-v1:`;
  async function withClient<T>(url: string, run: (redis: RedisClient) => Promise<T>): Promise<T> {
    const redis = new RedisClient(url, {
      connectionTimeout: 1000,
      maxRetries: 0,
      autoReconnect: false,
      enableOfflineQueue: false,
    });
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        (async () => {
          await redis.connect();
          return run(redis);
        })(),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error("Usage queue cleanup deadline")), 5000);
        }),
      ]);
    } finally {
      clearTimeout(timer);
      redis.close();
    }
  }
  async function keys(redis: RedisClient): Promise<string[]> {
    const found = new Set<string>();
    let cursor = "0";
    for (let page = 0; page < 100; page++) {
      const [next, batch] = (await redis.send("SCAN", [
        cursor,
        "MATCH",
        `${prefix}*`,
        "COUNT",
        "100",
      ])) as [string, string[]];
      for (const key of batch) {
        if (!key.startsWith(prefix)) throw new Error("Usage queue cleanup escaped its namespace");
        found.add(key);
      }
      if (found.size > 10000) throw new Error("Usage queue cleanup key limit exceeded");
      if (next === "0") return [...found].sort();
      cursor = next;
    }
    throw new Error("Usage queue cleanup scan limit exceeded");
  }
  return {
    keys: (url: string) => withClient(url, keys),
    remove: (url: string) =>
      withClient(url, async (redis) => {
        const owned = await keys(redis);
        for (let offset = 0; offset < owned.length; offset += 100)
          await redis.send("UNLINK", owned.slice(offset, offset + 100));
      }),
  };
}
