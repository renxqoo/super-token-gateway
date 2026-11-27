import { expect, test } from "bun:test";
import { AdaptiveRoutingStats, routingLatencySample } from "../adaptive-routing-stats.ts";

test("local waits that can overlap arrival do not manufacture a fast or slow latency sample", () => {
  expect(routingLatencySample(108, 100, 5, 50)).toBeUndefined();
  expect(routingLatencySample(48, 38, 5, 50)).toBe(true);
  expect(routingLatencySample(108, 25, 5, 50)).toBe(false);
  expect(routingLatencySample(108, 52, 80, 70)).toBe(false);
});

test("route quality needs recent route and mode samples and distinguishes slow and incomplete responses", () => {
  let now = 0;
  const stats = new AdaptiveRoutingStats(() => now);
  try {
    const record = (key: string, complete: boolean, latencyMs: number, outputTokens?: number) =>
      stats.observe({
        routeKey: key,
        modelKey: "same-model-stream",
        complete,
        onTime: latencyMs <= 120,
        ...(outputTokens === undefined ? {} : { outputTokens }),
      });
    for (let i = 0; i < 11; i++) record("healthy-stream-version-1", true, 30, 20);
    expect(stats.quality("healthy-stream-version-1")).toBeUndefined();
    record("healthy-stream-version-1", true, 30, 20);
    for (let i = 0; i < 12; i++) record("slow-stream-version-1", true, 150);
    for (let i = 0; i < 12; i++) record("partial-stream-version-1", i < 6, 30);
    expect(stats.quality("healthy-stream-version-1")).toBe(1);
    expect(stats.quality("slow-stream-version-1")).toBe(0.4);
    expect(stats.quality("partial-stream-version-1")).toBe(0.2);
    expect(stats.quality("healthy-unary-version-1")).toBeUndefined();
    expect(stats.quality("healthy-stream-version-2")).toBeUndefined();
    expect(stats.outputEstimate("same-model-stream", 15)).toBe(15);
    expect(stats.outputEstimate("same-model-unary", 25)).toBeUndefined();
    now = 60_000;
    expect(stats.quality("healthy-stream-version-1")).toBeUndefined();
    expect(stats.outputEstimate("same-model-stream", 15)).toBeUndefined();
  } finally {
    stats.close();
  }
});
