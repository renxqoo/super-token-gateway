import { expect, test } from "bun:test";
import { Lifecycle, Metrics } from "@gateway/runtime";
import { relayBinary } from "../relay-binary.ts";
import type { Dependencies } from "../context.ts";

test("graceful drain waits for delayed binary usage commit before closing dependencies", async () => {
  const lifecycle = new Lifecycle(2);
  const active = lifecycle.enter();
  let commit!: () => void;
  const gate = new Promise<void>((resolve) => {
    commit = resolve;
  });
  let committing = false;
  let cleaned = false;
  const deps = {
    lifecycle,
    metrics: new Metrics(),
    config: { DOWNSTREAM_STALL_MS: 500, TOTAL_TIMEOUT_MS: 1000 },
    usage: {
      async publish() {
        committing = true;
        await gate;
      },
    },
  } as unknown as Dependencies;
  relayBinary(
    deps,
    new ReadableStream<Uint8Array>({
      pull(c) {
        c.enqueue(new Uint8Array(65536));
      },
    }),
    "attempt",
    "request",
    active.controller,
    async () => {
      cleaned = true;
      active.release();
    },
  );
  let drained = false;
  const pending = lifecycle.drain(1).then(() => {
    drained = true;
  });
  await Bun.sleep(50);
  expect(committing).toBe(true);
  expect(drained).toBe(false);
  expect(cleaned).toBe(false);
  commit();
  await pending;
  expect(cleaned).toBe(true);
  expect(lifecycle.size).toBe(0);
});
