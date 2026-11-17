import { expect, test } from "bun:test";
import { createIdentityReadCache } from "../read-cache.ts";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((accept) => (resolve = accept));
  return { promise, resolve };
}

test("concurrent identity reads share one load and refresh exactly at the TTL boundary", async () => {
  let now = 0;
  let reads = 0;
  const loading = deferred<string>();
  const cache = createIdentityReadCache({
    capacity: 2,
    maxPending: 1,
    ttlMs: 1000,
    now: () => now,
    load: async () => {
      reads++;
      return reads === 1 ? loading.promise : "revision-two";
    },
  });
  const first = cache.get("session");
  const second = cache.get("session");
  loading.resolve("revision-one");
  expect(await Promise.all([first, second])).toEqual(["revision-one", "revision-one"]);
  expect(reads).toBe(1);
  now = 999;
  expect(await cache.get("session")).toBe("revision-one");
  now = 1000;
  expect(await cache.get("session")).toBe("revision-two");
  expect(reads).toBe(2);
  cache.close();
});

test("identity LRU evicts the least recently used entry within its memory bound", async () => {
  const reads: string[] = [];
  const cache = createIdentityReadCache({
    capacity: 2,
    maxPending: 2,
    ttlMs: 1000,
    load: async (key) => {
      reads.push(key);
      return key;
    },
  });
  for (const key of ["A", "B", "A", "C", "B"]) expect(await cache.get(key)).toBe(key);
  expect(reads).toEqual(["A", "B", "C", "B"]);
  expect(cache.stats()).toEqual({ entries: 2, pending: 0 });
  cache.close();
});

test("cold identity load limit rejects overflow without rejecting a shared-key waiter", async () => {
  const loading = deferred<string>();
  const reads: string[] = [];
  const cache = createIdentityReadCache({
    capacity: 2,
    maxPending: 1,
    ttlMs: 1000,
    load: async (key) => {
      reads.push(key);
      return key === "A" ? loading.promise : key;
    },
  });
  const first = cache.get("A"),
    second = cache.get("A");
  await expect(cache.get("B")).rejects.toThrow("internal_capacity");
  loading.resolve("A");
  expect(await Promise.all([first, second])).toEqual(["A", "A"]);
  expect(await cache.get("B")).toBe("B");
  expect(reads).toEqual(["A", "B"]);
  cache.close();
});

test("failed and missing identity reads do not poison later attempts", async () => {
  let reads = 0;
  const cache = createIdentityReadCache({
    capacity: 2,
    maxPending: 1,
    ttlMs: 1000,
    load: async () => {
      reads++;
      if (reads === 1) throw new Error("offline");
      return reads === 2 ? undefined : "registered";
    },
  });
  await expect(cache.get("session")).rejects.toThrow("offline");
  expect(await cache.get("session")).toBeUndefined();
  expect(await cache.get("session")).toBe("registered");
  expect(reads).toBe(3);
  cache.close();
});

test("invalidation and close prevent in-flight identity reads from repopulating old state", async () => {
  let reads = 0;
  const firstLoad = deferred<string>(),
    lastLoad = deferred<string>();
  const cache = createIdentityReadCache({
    capacity: 2,
    maxPending: 1,
    ttlMs: 1000,
    load: async () => {
      reads++;
      return reads === 1 ? firstLoad.promise : lastLoad.promise;
    },
  });
  const first = cache.get("session");
  cache.invalidate();
  firstLoad.resolve("stale");
  expect(await first).toBe("stale");
  expect(cache.stats()).toEqual({ entries: 0, pending: 0 });
  const last = cache.get("session").catch((error: Error) => error.message);
  cache.close();
  lastLoad.resolve("closed");
  expect(await last).toBe("identity_store_unavailable");
  await Bun.sleep(0);
  expect(cache.stats()).toEqual({ entries: 0, pending: 0 });
  await expect(cache.get("session")).rejects.toThrow("identity_store_unavailable");
});

test("a slow identity read cannot extend its cache lifetime past the original TTL", async () => {
  let now = 0,
    reads = 0;
  const loading = deferred<string>();
  const cache = createIdentityReadCache({
    capacity: 2,
    maxPending: 1,
    ttlMs: 1000,
    now: () => now,
    load: async () => {
      reads++;
      return reads === 1 ? loading.promise : "fresh";
    },
  });
  const first = cache.get("session");
  now = 1001;
  loading.resolve("old");
  expect(await first).toBe("old");
  expect(await cache.get("session")).toBe("fresh");
  expect(reads).toBe(2);
  cache.close();
});

test("a cancelled identity waiter releases immediately without cancelling another shared waiter", async () => {
  const loading = deferred<string>();
  const controller = new AbortController();
  const cache = createIdentityReadCache({
    capacity: 2,
    maxPending: 1,
    ttlMs: 1000,
    load: async () => loading.promise,
  });
  const cancelled = cache.get("session", controller.signal).then(
    () => "resolved",
    () => "cancelled",
  );
  const survivor = cache.get("session");
  controller.abort(new Error("client gone"));
  try {
    const outcome = await Promise.race([cancelled, Bun.sleep(20).then(() => "still waiting")]);
    expect(outcome).toBe("cancelled");
    loading.resolve("registered");
    expect(await survivor).toBe("registered");
  } finally {
    loading.resolve("registered");
    await Promise.allSettled([cancelled, survivor]);
    cache.close();
  }
});

test("closing identity reads aborts the owner and releases waiters even if its loader has not finished", async () => {
  const loading = deferred<string>();
  let owner: AbortSignal | undefined;
  const cache = createIdentityReadCache({
    capacity: 2,
    maxPending: 1,
    ttlMs: 1000,
    load: async (_key, signal) => {
      owner = signal;
      return loading.promise;
    },
  });
  const reading = cache.get("session").then(
    () => "resolved",
    () => "closed",
  );
  await Promise.resolve();
  cache.close();
  try {
    expect(owner?.aborted ?? false).toBe(true);
    const outcome = await Promise.race([reading, Bun.sleep(20).then(() => "still waiting")]);
    expect(outcome).toBe("closed");
  } finally {
    loading.resolve("late");
    await reading;
  }
});

test("identity loading timeout includes queue time and keeps unfinished cancelled loads inside the pending limit", async () => {
  const loading = deferred<string>();
  let owner: AbortSignal | undefined;
  const cache = createIdentityReadCache({
    capacity: 2,
    maxPending: 1,
    ttlMs: 1000,
    loadTimeoutMs: 10,
    load: async (_key, signal) => {
      owner = signal;
      return loading.promise;
    },
  });
  try {
    await expect(cache.get("session")).rejects.toThrow("identity_store_unavailable");
    expect(owner?.aborted).toBe(true);
    await expect(cache.get("other")).rejects.toThrow("internal_capacity");
    expect(cache.stats()).toEqual({ entries: 0, pending: 1 });
  } finally {
    cache.close();
    loading.resolve("too late");
    await Bun.sleep(0);
  }
  expect(cache.stats()).toEqual({ entries: 0, pending: 0 });
});
