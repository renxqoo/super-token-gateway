import { expect, test } from "bun:test";
import { startPeriodic } from "../maintenance.ts";

test("a failing maintenance task is reported once and waits for its interval instead of spinning, and close interrupts the wait", async () => {
  const calls: number[] = [];
  let failures = 0;
  const loop = startPeriodic(
    async () => {
      calls.push(Date.now());
      if (calls.length === 1) throw new Error("transient");
      return calls.length < 4;
    },
    60000,
    () => {
      failures++;
    },
  );
  const deadline = Date.now() + 2000;
  while (calls.length < 2 && Date.now() < deadline) await Bun.sleep(5);
  expect(calls.length).toBe(1);
  expect(failures).toBe(1);
  const started = Date.now();
  await loop.close();
  expect(Date.now() - started).toBeLessThan(1000);
  expect(calls.length).toBe(1);
});

test("a task that reports more work is called again immediately until it reports none", async () => {
  let calls = 0;
  const loop = startPeriodic(
    async () => {
      calls++;
      return calls < 3;
    },
    60000,
    () => {},
  );
  const deadline = Date.now() + 2000;
  while (calls < 3 && Date.now() < deadline) await Bun.sleep(5);
  await loop.close();
  expect(calls).toBe(3);
});
