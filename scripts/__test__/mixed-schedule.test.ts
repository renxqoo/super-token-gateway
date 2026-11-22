import { expect, test } from "bun:test";
import { createMixedSchedule } from "../mixed-schedule.ts";

test("paused and full mixed load retains due requests and dispatches each exactly once", () => {
  const schedule = createMixedSchedule(1, 2000, 256);
  const sent: number[] = [];
  let peakBurst = 0;
  for (let time = 0; time <= 1002; time += 2) {
    if (time > 100 && time < 290) continue;
    const fullAfterPause = time >= 290 && time < 530;
    const { first, end } = schedule.take(time, fullAfterPause ? 256 : 0);
    peakBurst = Math.max(peakBurst, end - first);
    for (let index = first; index < end; index++) sent.push(index);
  }
  expect(sent).toEqual(Array.from({ length: 2000 }, (_, index) => index));
  expect(peakBurst).toBeLessThanOrEqual(30);
  expect(schedule.status()).toMatchObject({ total: 2000, started: 2000 });
  expect(schedule.done(1002)).toBe(true);
});

test("expired mixed workload exposes unsent slots instead of pushing them into drain", () => {
  const schedule = createMixedSchedule(1, 2000, 256);
  schedule.take(999, 256);
  expect(schedule.take(1051, 0)).toEqual({ first: 0, end: 0 });
  expect(schedule.done(1051)).toBe(true);
  expect(schedule.status()).toMatchObject({ total: 2000, started: 0 });
});
