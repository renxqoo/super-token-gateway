import { expect, test } from "bun:test";
import { LatencyHistogram, validChat, passesCapacityTarget } from "../../scripts/benchmark-lib.ts";
test("benchmark rejects empty/error/malformed Chat successes and invalid SSE completion", () => {
  expect(validChat('{"choices":[]}', false)).toBe(false);
  expect(validChat("not json", false)).toBe(false);
  expect(validChat('{"error":{},"choices":[{}]}', false)).toBe(false);
  const chunk = 'data: {"choices":[{"delta":{"content":"hello"},"finish_reason":"stop"}]}\n\n';
  expect(validChat(chunk + "data: [DONE]\n\n", true, "hello")).toBe(true);
  expect(validChat(chunk + 'data: {"error":{"message":"fail"}}\n\ndata: [DONE]\n\n', true)).toBe(
    false,
  );
  expect(validChat(chunk + "data: [DONE]\n\ndata: [DONE]\n\n", true)).toBe(false);
  expect(validChat("data: [DONE]\n\n", true)).toBe(false);
  expect(validChat(chunk, true)).toBe(false);
});
test("bounded histogram quantiles retain millisecond accuracy across many observations", () => {
  const h = new LatencyHistogram();
  for (let i = 0; i < 100000; i++) h.add((i % 100) + 1);
  expect(h.count).toBe(100000);
  expect(h.quantile(0.5)).toBeGreaterThanOrEqual(50);
  expect(h.quantile(0.5)).toBeLessThan(51);
  expect(h.quantile(0.99)).toBeGreaterThanOrEqual(99);
  expect(h.quantile(0.99)).toBeLessThan(100);
});

test("rounded 6000 RPS cannot pass when the unrounded completed rate is below target", () => {
  expect(passesCapacityTarget(true, { succeeded: 5999996, duration_seconds: 1000 }, 6000)).toBe(
    false,
  );
  expect(passesCapacityTarget(true, { succeeded: 6000000, duration_seconds: 1000 }, 6000)).toBe(
    true,
  );
  expect(passesCapacityTarget(false, { succeeded: 7000000, duration_seconds: 1000 }, 6000)).toBe(
    false,
  );
  expect(passesCapacityTarget(true, { succeeded: 0, duration_seconds: 0 }, 6000)).toBe(false);
});
