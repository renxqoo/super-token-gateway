import { expect, test } from "bun:test";
import {
  createProcessAccumulator,
  discoverRedisProcesses,
  parseProcessRows,
  summarizeStatements,
  type StatementSnapshot,
} from "../benchmark-metrics.ts";

const row = (
  pid: number,
  cpu: number,
  started = "Tue Nov 17 10:00:00 2026",
  ppid = 1,
  command = "postgres",
) => ({ pid, ppid, cpu_seconds: cpu, started, command, rss_bytes: 1024 });

test("CPU does not become negative or lose sampled work when a PG backend exits mid-run", () => {
  const meter = createProcessAccumulator();
  meter.sample([row(10, 1), row(11, 4)], { pg: [10, 11] }, 0);
  meter.sample([row(10, 2), row(11, 7)], { pg: [10, 11] }, 500);
  meter.sample([row(10, 3), row(12, 2)], { pg: [10, 12] }, 1000);
  const report = meter.summary(100).pg!;
  expect(report.cpu_seconds).toBe(7);
  expect(report.cpu_ms_per_request).toBe(70);
  expect(report.pids.map((pid) => pid.pid)).toEqual([10, 11, 12]);
  expect(report.peak_processes).toBe(2);
});

test("CPU counter regression and PID reuse cannot subtract CPU or double count recovery", () => {
  const meter = createProcessAccumulator();
  meter.sample([row(10, 5)], { pg: [10] }, 0);
  meter.sample([row(10, 8)], { pg: [10] }, 500);
  meter.sample([row(10, 2)], { pg: [10] }, 1000);
  meter.sample([row(10, 9)], { pg: [10] }, 1500);
  meter.sample([row(10, 3, "Tue Nov 17 10:00:02 2026")], { pg: [10] }, 2000);
  expect(meter.summary(10).pg!.cpu_seconds).toBe(7);
});

test("helper descendants are counted separately without duplicating overlapping roots", () => {
  const meter = createProcessAccumulator();
  meter.sample(
    [row(1, 1), row(2, 2, undefined, 1, "plugin-host"), row(3, 3, undefined, 2, "worker")],
    { gateway: [1, 1], other: [2] },
    0,
  );
  meter.sample(
    [row(1, 2), row(2, 4, undefined, 1, "plugin-host"), row(3, 6, undefined, 2, "worker")],
    { gateway: [1], other: [2] },
    500,
  );
  expect(meter.summary(10).gateway!.cpu_seconds).toBe(1);
  expect(meter.summary(10).plugin_helper!.cpu_seconds).toBe(5);
  expect(meter.summary(10).other).toBeUndefined();
});

test("ps days and fractional seconds preserve process start identity and RSS", () => {
  expect(
    parseProcessRows(
      " 42 1 128 1-02:03:04.50 Tue Nov 17 09:00:00 2026 /bin/redis-server 127.0.0.1:56400",
    )[0],
  ).toMatchObject({
    pid: 42,
    ppid: 1,
    rss_bytes: 131072,
    cpu_seconds: 93784.5,
    started: "Tue Nov 17 09:00:00 2026",
  });
  expect(() => parseProcessRows("invalid")).toThrow("Malformed");
});

const statement = (calls: number, ms: number, bytes: number, toplevel = true) => ({
  userid: "10",
  queryid: toplevel ? "20" : "21",
  toplevel,
  query: "SELECT benchmark($1)",
  calls,
  total_exec_time: ms,
  wal_bytes: bytes,
  wal_records: calls * 2,
});
const snapshot = (rows: StatementSnapshot["rows"]): StatementSnapshot => ({
  stats_reset: "2026-11-17",
  dealloc: 0,
  rows,
});

test("PG deltas use a hand calculated request denominator and separate nested execution", () => {
  const result = summarizeStatements(
    snapshot([statement(10, 20, 100)]),
    snapshot([statement(30, 70, 1100), statement(100, 200, 900, false)]),
    10,
  );
  expect(result.valid).toBe(true);
  expect(result.topLevel).toMatchObject({
    calls: 20,
    exec_ms: 50,
    wal_bytes: 1000,
    calls_per_request: 2,
    exec_ms_per_request: 5,
    wal_bytes_per_request: 100,
  });
  expect(result.nested.calls_per_request).toBe(10);
  expect(result.top25[0]!.toplevel).toBe(false);
});

test("PG resets, evictions and regressed counters invalidate audit instead of reporting negative work", () => {
  const before = snapshot([statement(30, 70, 1100)]);
  const after = { ...snapshot([statement(10, 20, 100)]), stats_reset: "changed", dealloc: 1 };
  const result = summarizeStatements(before, after, 10);
  expect(result.valid).toBe(false);
  expect(result.errors).toContain("statistics_reset");
  expect(result.errors).toContain("statements_evicted");
  expect(result.topLevel.calls).toBe(0);
  expect(summarizeStatements(before, snapshot([]), 10).errors).toContain("statements_missing");
  expect(() => summarizeStatements(before, after, 0)).toThrow("denominator");
});

test("PG audit retains only the 25 most expensive statements", () => {
  const rows = Array.from({ length: 30 }, (_, i) => ({
    ...statement(1, i + 1, 0),
    queryid: String(i),
  }));
  const result = summarizeStatements(snapshot([]), snapshot(rows), 1);
  expect(result.topLevel.calls).toBe(30);
  expect(result.top25).toHaveLength(25);
  expect(result.top25[0]!.total_exec_time).toBe(30);
  expect(result.top25.at(-1)!.total_exec_time).toBe(6);
});

test("CPU discovery retains every Redis replica instead of overwriting earlier PIDs", async () => {
  const topology: Record<string, { pid: number; replicas: string[] }> = {
    "redis://127.0.0.1:6000": {
      pid: 10,
      replicas: ["redis://127.0.0.1:6001", "redis://127.0.0.1:6002"],
    },
    "redis://127.0.0.1:6001": { pid: 11, replicas: [] },
    "redis://127.0.0.1:6002": { pid: 12, replicas: [] },
  };
  expect(
    await discoverRedisProcesses(
      { evidence: "redis://127.0.0.1:6000" },
      async (url) => topology[url]!,
    ),
  ).toEqual({ evidence: [10], evidence_replica: [11, 12] });
});
