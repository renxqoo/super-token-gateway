import { expect, spyOn, test } from "bun:test";
import type { SQL } from "bun";
import type { CoreVerifiedCallerProjection } from "@gateway/contracts";
import { createExecutionStore } from "../index.ts";

const caller = { deadline_at: new Date(5000).toISOString() } as CoreVerifiedCallerProjection;
const busy = Object.assign(new Error("core_busy"), { errno: "P0503" });
function database() {
  let attempts = 0;
  return (() =>
    Object.assign(
      Promise.resolve().then(() => {
        if (attempts++ === 0) throw busy;
        return [{ value: { context_id: "late-context" } }];
      }),
      { cancel() {} },
    )) as unknown as SQL;
}

test("a delayed retry timer cannot create a context after the one second budget", async () => {
  let now = 0;
  const clock = spyOn(Date, "now").mockImplementation(() => now);
  const monotonic = spyOn(performance, "now").mockImplementation(() => now);
  const sleep = spyOn(Bun, "sleep").mockImplementation(async () => {
    now = 1100;
  });
  try {
    await expect(createExecutionStore(database()).context(caller, "id", {})).rejects.toBe(busy);
  } finally {
    sleep.mockRestore();
    monotonic.mockRestore();
    clock.mockRestore();
  }
});

test("cancelling during a context retry cannot start a later context write", async () => {
  const controller = new AbortController(),
    reason = new Error("request cancelled");
  const clock = spyOn(Date, "now").mockReturnValue(0);
  const sleep = spyOn(Bun, "sleep").mockImplementation(async () => {
    controller.abort(reason);
  });
  try {
    await expect(
      createExecutionStore(database()).context(caller, "id", {}, controller.signal),
    ).rejects.toBe(reason);
  } finally {
    sleep.mockRestore();
    clock.mockRestore();
  }
});

test("a backward wall clock step cannot extend the context retry budget", async () => {
  let now = 0,
    elapsed = 0;
  const wall = spyOn(Date, "now").mockImplementation(() => now);
  const monotonic = spyOn(performance, "now").mockImplementation(() => elapsed);
  const sleep = spyOn(Bun, "sleep").mockImplementation(async () => {
    now = -5000;
    elapsed = 1100;
  });
  try {
    await expect(createExecutionStore(database()).context(caller, "id", {})).rejects.toBe(busy);
  } finally {
    sleep.mockRestore();
    monotonic.mockRestore();
    wall.mockRestore();
  }
});
