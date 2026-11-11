import { RedisClient } from "bun";

export async function cleanupCapacityNamespaces(urls: string[], namespace: string) {
  if (!/^bench:[a-f0-9]{32}$/.test(namespace)) throw new Error("Invalid capacity namespace");
  let cleaned = 0,
    failed = 0;
  for (const url of urls) {
    let redis: RedisClient | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      redis = new RedisClient(url, {
        connectionTimeout: 1000,
        maxRetries: 0,
        autoReconnect: false,
        enableOfflineQueue: false,
      });
      const client = redis;
      await Promise.race([
        (async () => {
          await client.connect();
          for (const pattern of [`${namespace}:*`, `{${namespace}:usage}:*`]) {
            let cursor = "0";
            do {
              const result = (await client.send("SCAN", [
                cursor,
                "MATCH",
                pattern,
                "COUNT",
                "1000",
              ])) as [string, string[]];
              cursor = result[0];
              if (result[1].length) await client.send("UNLINK", result[1]);
            } while (cursor !== "0");
          }
        })(),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error("Capacity cleanup deadline")), 2000);
        }),
      ]);
      cleaned++;
    } catch {
      failed++;
    } finally {
      clearTimeout(timer);
      redis?.close();
    }
  }
  return { cleaned, failed };
}
