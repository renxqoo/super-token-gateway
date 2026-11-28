import { expect, test } from "bun:test";
import { bumpLabeled, escapeLabel, Histogram } from "../prometheus.ts";

test("symptom: escapeLabel leaves CR and Unicode line separators unescaped in Prometheus exposition", () => {
  expect(escapeLabel('a\\b"c\nd')).toBe('a\\\\b\\"c d');
  expect(escapeLabel("line1\r\nline2")).toBe("line1 line2");
  expect(escapeLabel("line1\rline2")).toBe("line1 line2");
  expect(escapeLabel("line1\u2028line2\u2029line3")).toBe("line1 line2 line3");

  const injected = escapeLabel('bad"chan\r\nfake_metric 1\u2028\u2029\\end');
  expect(injected).not.toMatch(/[\r\n\u2028\u2029]/);
  expect(injected).toBe('bad\\"chan fake_metric 1  \\\\end');

  const hist = new Histogram([10, 50]);
  hist.observe(5);
  const lines = hist.render("gw_test", "help", `channel="${injected}"`);
  for (const line of lines) {
    expect(line).not.toMatch(/[\r\n\u2028\u2029]/);
  }
});

test("Histogram clamps non-finite or negative observations and preserves monotonic cumulative buckets", () => {
  const hist = new Histogram([10, 50, 100]);
  hist.observe(-5);
  hist.observe(Number.NaN);
  hist.observe(Number.POSITIVE_INFINITY);
  hist.observe(Number.NEGATIVE_INFINITY);
  hist.observe(10);
  hist.observe(25);
  hist.observe(150);

  const lines = hist.render("gw_latency_ms", "Latency", 'op="chat"');
  expect(lines).toEqual([
    "# HELP gw_latency_ms Latency",
    "# TYPE gw_latency_ms histogram",
    'gw_latency_ms_bucket{op="chat",le="10"} 5',
    'gw_latency_ms_bucket{op="chat",le="50"} 6',
    'gw_latency_ms_bucket{op="chat",le="100"} 6',
    'gw_latency_ms_bucket{op="chat",le="+Inf"} 7',
    'gw_latency_ms_sum{op="chat"} 185.000000000',
    'gw_latency_ms_count{op="chat"} 7',
  ]);
});

test("bumpLabeled caps distinct series at maxEntries while continuing to increment existing keys", () => {
  const map = new Map<string, { labels: readonly [string]; count: number }>();
  expect(bumpLabeled(map, "k1", () => ["a"] as const, 2)).toBe(true);
  expect(bumpLabeled(map, "k2", () => ["b"] as const, 2)).toBe(true);
  expect(bumpLabeled(map, "k3", () => ["c"] as const, 2)).toBe(false);
  expect(bumpLabeled(map, "k1", () => ["a"] as const, 2)).toBe(true);
  expect(map.size).toBe(2);
  expect(map.get("k1")?.count).toBe(2);
  expect(map.get("k2")?.count).toBe(1);
});
