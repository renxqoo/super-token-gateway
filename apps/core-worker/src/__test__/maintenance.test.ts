import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { recoverExecutions, startPeriodic } from "../maintenance.ts";

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
  for (let i = 0; i < 100; i++) loop.trigger();
  await Bun.sleep(5);
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

test("commit notifications coalesce into one follow-up while a recovery is running and wake an idle recovery", async () => {
  let release!: () => void;
  const blocked = new Promise<void>((resolve) => (release = resolve));
  let calls = 0;
  const loop = startPeriodic(
    async () => {
      calls++;
      if (calls === 1) await blocked;
      return false;
    },
    60000,
    () => {},
  );
  try {
    for (let i = 0; i < 100; i++) loop.trigger();
    expect(calls).toBe(1);
    release();
    let until = Date.now() + 2000;
    while (calls < 2 && Date.now() < until) await Bun.sleep(1);
    await Bun.sleep(5);
    expect(calls).toBe(2);
    loop.trigger();
    until = Date.now() + 2000;
    while (calls < 3 && Date.now() < until) await Bun.sleep(1);
    expect(calls).toBe(3);
  } finally {
    release();
    await loop.close();
  }
  loop.trigger();
  expect(calls).toBe(3);
});

test("a full release batch schedules more work without expired permits while a short or failed batch stops", async () => {
  const db = (async () => []) as unknown as SQL;
  const core = {
    execution: async () => ({ released: 2, pending: 2, failed: 0, expired: 0, expiry_more: false }),
  } as Parameters<typeof recoverExecutions>[1];
  expect(await recoverExecutions(db, core, 2)).toEqual({
    released: 2,
    aborted: 0,
    candidates: 0,
    blocked: false,
    expired: 0,
    more: true,
  });
  core.execution = async () => ({
    released: 1,
    pending: 1,
    failed: 0,
    expired: 0,
    expiry_more: false,
  });
  expect((await recoverExecutions(db, core, 2)).more).toBe(false);
  core.execution = async () => ({
    released: 0,
    pending: 2,
    failed: 2,
    expired: 0,
    expiry_more: false,
  });
  expect(await recoverExecutions(db, core, 2)).toEqual({
    released: 0,
    aborted: 0,
    candidates: 0,
    blocked: true,
    expired: 0,
    more: false,
  });
});
