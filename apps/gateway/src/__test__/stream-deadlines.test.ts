import { expect, test } from "bun:test";
import { idleGuard, stallGuard } from "../stream-deadlines.ts";

const bytes = (text: string) => new TextEncoder().encode(text);
const text = (value: Uint8Array | undefined) => new TextDecoder().decode(value);

test("the idle deadline only runs while a read waits on the upstream, so a slow reader of ready data never expires it", async () => {
  let expired = 0;
  const guarded = idleGuard(
    new ReadableStream({
      start(controller) {
        controller.enqueue(bytes("a"));
        controller.enqueue(bytes("b"));
        controller.close();
      },
    }),
    30,
    () => expired++,
  );
  const reader = guarded.getReader();
  expect(text((await reader.read()).value)).toBe("a");
  await Bun.sleep(100);
  expect(text((await reader.read()).value)).toBe("b");
  await Bun.sleep(100);
  expect((await reader.read()).done).toBe(true);
  expect(expired).toBe(0);
});

test("a silent upstream expires the idle deadline once and cancelling the guarded stream releases the upstream", async () => {
  let expired = 0,
    cancelled = false;
  const guarded = idleGuard(
    new ReadableStream({
      start(controller) {
        controller.enqueue(bytes("a"));
      },
      cancel() {
        cancelled = true;
      },
    }),
    30,
    () => expired++,
  );
  const reader = guarded.getReader();
  expect(text((await reader.read()).value)).toBe("a");
  const pending = reader.read();
  await Bun.sleep(100);
  expect(expired).toBe(1);
  await reader.cancel();
  expect((await pending).done).toBe(true);
  expect(cancelled).toBe(true);
});

test("an upstream answer without a body reads as an ended stream instead of failing the relay", async () => {
  let expired = 0;
  const reader = idleGuard(null, 30, () => expired++).getReader();
  expect((await reader.read()).done).toBe(true);
  expect(expired).toBe(0);
});

test("a downstream that keeps reading receives the first chunk then the rest without a stall", async () => {
  let stalled = 0;
  async function* rest() {
    yield bytes("b");
    await Bun.sleep(100);
    yield bytes("c");
  }
  const reader = stallGuard(rest(), bytes("a"), 30, () => stalled++).getReader();
  const seen: string[] = [];
  for (;;) {
    const next = await reader.read();
    if (next.done) break;
    seen.push(text(next.value));
  }
  expect(seen).toEqual(["a", "b", "c"]);
  expect(stalled).toBe(0);
});

test("a downstream that stops reading is cut at the stall deadline: the source is finished, the stream errors and the stall is reported once", async () => {
  let stalled = 0,
    finished = false;
  async function* source() {
    try {
      for (;;) yield bytes("x");
    } finally {
      finished = true;
    }
  }
  const reader = stallGuard(source(), undefined, 30, () => stalled++).getReader();
  expect(text((await reader.read()).value)).toBe("x");
  await Bun.sleep(150);
  expect(stalled).toBe(1);
  expect(finished).toBe(true);
  const drained: unknown[] = [];
  const failure = await (async () => {
    try {
      for (;;) {
        const next = await reader.read();
        if (next.done) return undefined;
        drained.push(next.value);
      }
    } catch (error) {
      return error;
    }
  })();
  expect(failure).toBeInstanceOf(Error);
});

test("cancelling the delivered stream finishes its source without reporting a stall", async () => {
  let stalled = 0,
    finished = false;
  async function* source() {
    try {
      for (;;) yield bytes("x");
    } finally {
      finished = true;
    }
  }
  const reader = stallGuard(source(), undefined, 30, () => stalled++).getReader();
  await reader.read();
  await reader.cancel();
  await Bun.sleep(100);
  expect(finished).toBe(true);
  expect(stalled).toBe(0);
});
