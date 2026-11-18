import { expect, spyOn, test } from "bun:test";
import type { SQL } from "bun";
import type { CoreVerifiedCallerProjection } from "@gateway/contracts";
import { createExecutionStore } from "../index.ts";

test("a rolled back quota lock conflict can complete without replaying an unknown database outcome", async () => {
  const busy = Object.assign(new Error("permit_busy"), { errno: "55P03" });
  let calls = 0;
  const db = (() =>
    Promise.resolve().then(() => {
      if (++calls === 1) throw busy;
      return [{ value: { synchronized: 1 } }];
    })) as unknown as SQL;
  const caller = {
    deadline_at: new Date(Date.now() + 5000).toISOString(),
  } as CoreVerifiedCallerProjection;
  expect(await createExecutionStore(db).quota(caller, "attempt", "ACTIVE", [])).toEqual({
    synchronized: 1,
  });
  expect(calls).toBe(2);
});

test("quota synchronization never retries an unknown commit or connection error", async () => {
  const failure = Object.assign(new Error("connection_lost"), { errno: "08006" });
  let calls = 0;
  const db = (() =>
    Promise.resolve().then(() => {
      calls++;
      throw failure;
    })) as unknown as SQL;
  const caller = {
    deadline_at: new Date(Date.now() + 5000).toISOString(),
  } as CoreVerifiedCallerProjection;
  expect(
    await createExecutionStore(db)
      .quota(caller, "attempt", "ACTIVE", [])
      .catch((error: unknown) => error),
  ).toBe(failure);
  expect(calls).toBe(1);
});

test("a late quota retry timer and a backward wall clock cannot extend the one second budget", async () => {
  const failure = Object.assign(new Error("permit_busy"), { errno: "55P03" });
  let calls = 0,
    elapsed = 0,
    wall = 0;
  const db = (() =>
    Promise.resolve().then(() => {
      calls++;
      throw failure;
    })) as unknown as SQL;
  const caller = { deadline_at: new Date(5000).toISOString() } as CoreVerifiedCallerProjection;
  const clock = spyOn(Date, "now").mockImplementation(() => wall);
  const monotonic = spyOn(performance, "now").mockImplementation(() => elapsed);
  const sleep = spyOn(Bun, "sleep").mockImplementation(async () => {
    elapsed = 1100;
    wall = -5000;
  });
  try {
    expect(
      await createExecutionStore(db)
        .quota(caller, "attempt", "ACTIVE", [])
        .catch((error: unknown) => error),
    ).toBe(failure);
    expect(calls).toBe(1);
  } finally {
    clock.mockRestore();
    monotonic.mockRestore();
    sleep.mockRestore();
  }
});
