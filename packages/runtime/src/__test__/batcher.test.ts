import { expect, test } from "bun:test";
import { Batcher } from "../batcher.ts";
test("an idle batch starts without waiting its collection window and still waits for commit", async () => {
  const submitted = Promise.withResolvers<number[]>();
  const commit = Promise.withResolvers<number[]>();
  const batch = new Batcher<number, number>(
    async (values) => {
      submitted.resolve(values);
      return commit.promise;
    },
    8,
    250,
  );
  let acknowledged = 0;
  const track = (value: number) => {
    acknowledged++;
    return value;
  };
  const first = batch.add(1).then(track),
    second = batch.add(2).then(track);
  try {
    expect(await Promise.race([submitted.promise, Bun.sleep(100).then(() => null)])).toEqual([
      1, 2,
    ]);
    expect(acknowledged).toBe(0);
  } finally {
    commit.resolve([100, 200]);
    await Promise.all([first, second]);
  }
  expect(await Promise.all([first, second])).toEqual([100, 200]);
});

test("batch acknowledgments wait for commit and preserve request/result association", async () => {
  const commit = Promise.withResolvers<number[]>();
  const submitted = Promise.withResolvers<number[]>();
  const batch = new Batcher<number, number>(
    async (values) => {
      submitted.resolve(values);
      return commit.promise;
    },
    8,
    1,
  );
  let settled = 0;
  const one = batch.add(1).then((v) => {
    settled++;
    return v;
  });
  const two = batch.add(2).then((v) => {
    settled++;
    return v;
  });
  try {
    expect(await submitted.promise).toEqual([1, 2]);
    expect(settled).toBe(0);
  } finally {
    commit.resolve([100, 200]);
  }
  expect(await Promise.all([one, two])).toEqual([100, 200]);
});
test("busy batches retain collection time and queued work starts promptly when the database becomes idle", async () => {
  const commits = [Promise.withResolvers<number[]>(), Promise.withResolvers<number[]>()];
  const submissions = [Promise.withResolvers<number[]>(), Promise.withResolvers<number[]>()];
  let calls = 0;
  const batch = new Batcher<number, number>(
    async (values) => {
      const index = calls++;
      submissions[index]!.resolve(values);
      return commits[index]!.promise;
    },
    8,
    250,
    2,
  );
  const first = batch.add(1);
  await submissions[0]!.promise;
  const second = batch.add(2),
    third = batch.add(3);
  try {
    await Bun.sleep(20);
    expect(calls).toBe(1);
    commits[0]!.resolve([100]);
    expect(await Promise.race([submissions[1]!.promise, Bun.sleep(100).then(() => null)])).toEqual([
      2, 3,
    ]);
  } finally {
    commits[0]!.resolve([100]);
    commits[1]!.resolve([200, 300]);
    await Promise.all([first, second, third]);
  }
  expect(await Promise.all([first, second, third])).toEqual([100, 200, 300]);
});
test("batch commit failure rejects every caller without manufacturing partial success", async () => {
  const batch = new Batcher<number, number>(
    async () => {
      throw new Error("commit failed");
    },
    8,
    1,
  );
  const results = await Promise.allSettled([batch.add(1), batch.add(2)]);
  expect(results.every((result) => result.status === "rejected")).toBe(true);
});

test("bounded batching reports overload while admitted work remains commit-gated", async () => {
  const release = Promise.withResolvers<void>();
  const acknowledged: number[] = [];
  const batch = new Batcher<number, number>(
    async (values) => {
      await release.promise;
      return values;
    },
    1,
    0,
    1,
    1,
  );
  const track = (value: number) => {
    acknowledged.push(value);
    return value;
  };
  const first = batch.add(1).then(track),
    second = batch.add(2).then(track);
  try {
    await expect(batch.add(3)).rejects.toMatchObject({ status: 503, code: "overloaded" });
    expect(acknowledged).toEqual([]);
  } finally {
    release.resolve();
  }
  expect(await Promise.all([first, second])).toEqual([1, 2]);
});
