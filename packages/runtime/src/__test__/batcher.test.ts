import { expect, test } from "bun:test";
import { Batcher } from "../batcher.ts";
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
