import { expect, test } from "bun:test";
import { boundedRelay, MemoryBudget, readBounded, safeFetch } from "../index.ts";

test("global memory reservations cannot oversubscribe and leases release once", () => {
  const budget = new MemoryBudget(1024);
  const a = budget.lease();
  const b = budget.lease();
  a.update(700);
  expect(() => b.update(400)).toThrow("capacity");
  expect(budget.bufferedBytes).toBe(700);
  a.update(-300);
  b.update(624);
  expect(budget.bufferedBytes).toBe(1024);
  a.close();
  a.close();
  a.update(500);
  b.close();
  expect(budget.bufferedBytes).toBe(0);
});
test("idle unconsumed relay cancels source and completes cleanup once", async () => {
  let cancelled = 0;
  let cleaned = 0;
  const source = new ReadableStream<Uint8Array>({
    pull(c) {
      c.enqueue(new Uint8Array(65536));
    },
    cancel() {
      cancelled++;
    },
  });
  const relay = boundedRelay(source, new AbortController().signal, 30, 300, () => {
    cleaned++;
  });
  await Bun.sleep(80);
  expect(cancelled).toBe(1);
  expect(cleaned).toBe(1);
  await relay.cancel().catch(() => undefined);
  expect(cleaned).toBe(1);
});
test("aborting a pending source read releases bounded reader", async () => {
  let cleanup = 0;
  const controller = new AbortController();
  const relay = boundedRelay(
    new ReadableStream<Uint8Array>({
      pull() {
        return new Promise(() => {});
      },
    }),
    controller.signal,
    200,
    500,
    () => {
      cleanup++;
    },
  );
  const reader = relay.getReader();
  const pending = reader.read();
  controller.abort();
  await expect(pending).rejects.toThrow();
  await Bun.sleep(10);
  expect(cleanup).toBe(1);
  reader.releaseLock();
});
test("body budgets cancel chunked overflow and timeout instead of buffering forever", async () => {
  let cancels = 0;
  const stream = () =>
    new ReadableStream<Uint8Array>({
      pull(c) {
        c.enqueue(new Uint8Array(700));
      },
      cancel() {
        cancels++;
      },
    });
  await expect(readBounded(stream(), 1024)).rejects.toThrow("limit");
  expect(cancels).toBe(1);
  const controller = new AbortController();
  const pending = readBounded(new ReadableStream<Uint8Array>(), 1024, controller.signal);
  controller.abort();
  await expect(pending).rejects.toThrow();
});
test("outbound fetch rejects private address before connecting or following redirects", async () => {
  await expect(safeFetch(new URL("https://127.0.0.1:1/"), {}, false)).rejects.toThrow(
    "not permitted",
  );
});

test("CIDR matching and proxy resolution trust only the verified transport peer", async () => {
  const { inCidrs, clientAddress, validateCidr } = await import("../index.ts");
  expect(inCidrs("::ffff:127.0.0.1", ["127.0.0.0/8"])).toBe(true);
  expect(inCidrs("2001:db8::1234", ["2001:db8::/32"])).toBe(true);
  expect(inCidrs("10.1.1.1", ["10.2.0.0/16"])).toBe(false);
  expect(() => validateCidr("10.0.0.1/33")).toThrow();
  expect(clientAddress("127.0.0.1", "203.0.113.2", [])).toBe("127.0.0.1");
  expect(clientAddress("10.0.0.1", "203.0.113.2, 192.0.2.1, 10.0.0.2", ["10.0.0.0/8"])).toBe(
    "192.0.2.1",
  );
  expect(() => clientAddress("10.0.0.1", "bad", ["10.0.0.0/8"])).toThrow();
});
