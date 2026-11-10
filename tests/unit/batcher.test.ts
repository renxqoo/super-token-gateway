import { expect, test } from "bun:test";
import { Batcher } from "../../packages/kernel/src/batcher.ts";
test("batch acknowledgments wait for commit and preserve request/result association", async () => {
  let release!: (value: number[]) => void;
  let submitted: number[] = [];
  const batch = new Batcher<number, number>(
    async (values) => {
      submitted = values;
      return new Promise((r) => {
        release = r;
      });
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
  await Bun.sleep(5);
  expect(submitted).toEqual([1, 2]);
  expect(settled).toBe(0);
  release([100, 200]);
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
