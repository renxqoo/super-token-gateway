import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { auditCapacityIdentities, createIdentityWriter } from "../capacity-evidence.ts";
import { passesLongStreamCapacity, passesOnlineCapacity } from "../benchmark-lib.ts";

test("minute capacity uses the later HTTP and receipt ACK time and excludes tail completions", async () => {
  const directory = await mkdtemp(join(tmpdir(), "gateway-identities-"));
  try {
    const http = await createIdentityWriter(join(directory, "http.tsv"));
    const ack = await createIdentityWriter(join(directory, "ack.tsv"));
    await http.write(["third"], 1000);
    await http.write(["first"], 2000);
    await http.write(["second"], 62000);
    await ack.write(["warmup"], 900);
    await ack.write(["second"], 60000);
    await ack.write(["third"], 121000);
    await ack.write(["first"], 3000);
    await http.close();
    await ack.close();
    expect(
      await auditCapacityIdentities(
        join(directory, "http.tsv"),
        join(directory, "ack.tsv"),
        1000,
        120,
      ),
    ).toEqual({
      fullyCompletedByWindow: 2,
      minuteCompletions: [1, 1],
      matchedRequests: 3,
      httpOnly: 0,
      ackOnly: 1,
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("steady stream capacity counts filled streams leaving and excludes starts and completions outside the window", async () => {
  const directory = await mkdtemp(join(tmpdir(), "gateway-long-stream-"));
  try {
    const http = join(directory, "http.tsv");
    const ack = join(directory, "ack.tsv");
    const dispatch = join(directory, "dispatch.tsv");
    await Bun.write(
      dispatch,
      "tail\t69000\nfilled\t6000\nsteady\t20000\nearly\t3100\nlate\t70000\n",
    );
    await Bun.write(http, "early\t8100\ntail\t74000\nlate\t75000\nfilled\t11000\nsteady\t25000\n");
    await Bun.write(ack, "steady\t25010\nlate\t75001\nearly\t8101\nfilled\t11200\ntail\t74100\n");
    const fill = { arrivalsStartedAt: 3000, minimumStreamMs: 5000 };
    const audit = await auditCapacityIdentities(http, ack, 10000, 60, dispatch, 1500, fill);
    expect(audit).toMatchObject({
      matchedRequests: 5,
      minuteCompletions: [2],
      longStream: { minuteStarts: [2], inflightAtWindowStart: 1, shortStreams: 0 },
    });
    await Bun.write(http, "early\t8100\ntail\t73999\nlate\t75000\nfilled\t11000\nsteady\t25000\n");
    const shortened = await auditCapacityIdentities(http, ack, 10000, 60, dispatch, 1500, fill);
    expect(shortened.longStream?.shortStreams).toBe(1);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("long-stream throughput requires enough in-flight requests and both rates in every minute", () => {
  const audit = {
    minuteCompletions: [180100, 180000],
    longStream: { minuteStarts: [180000, 180100], inflightAtWindowStart: 90000, shortStreams: 0 },
  };
  const passes = () => passesLongStreamCapacity(true, 3000, 120, 30, audit, 250);
  expect(passes()).toBe(true);
  audit.longStream.inflightAtWindowStart--;
  expect(passes()).toBe(false);
  audit.longStream.inflightAtWindowStart++;
  audit.longStream.minuteStarts[0]!--;
  expect(passes()).toBe(false);
  audit.longStream.minuteStarts[0]!++;
  audit.minuteCompletions[1]!--;
  expect(passes()).toBe(false);
  audit.minuteCompletions[1]!++;
  expect(passesLongStreamCapacity(true, 3000, 120, 30, audit, 120001)).toBe(false);
  audit.longStream.shortStreams = 1;
  expect(passes()).toBe(false);
});

test("duplicate or malformed identities cannot inflate capacity even outside the measurement window", async () => {
  const directory = await mkdtemp(join(tmpdir(), "gateway-invalid-identities-"));
  try {
    const http = join(directory, "http.tsv"),
      ack = join(directory, "ack.tsv");
    await Bun.write(http, "a\t1000\n");
    await Bun.write(ack, "z\t1\nz\t2\n");
    await expect(auditCapacityIdentities(http, ack, 1000, 60)).rejects.toThrow("Duplicate");
    await Bun.write(ack, "z\tNaN\n");
    await expect(auditCapacityIdentities(http, ack, 1000, 60)).rejects.toThrow("Malformed");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("stream completion latency includes a late external ACK and rejects an unrelated dispatch time", async () => {
  const directory = await mkdtemp(join(tmpdir(), "gateway-stream-latency-"));
  try {
    const http = join(directory, "http.tsv");
    const ack = join(directory, "ack.tsv");
    const dispatch = join(directory, "dispatch.tsv");
    await Bun.write(http, "a\t1040\nb\t1070\n");
    await Bun.write(ack, "b\t1050\na\t1500\n");
    await Bun.write(dispatch, "b\t1020\na\t1000\n");
    const result = await auditCapacityIdentities(http, ack, 1000, 60, dispatch);
    expect(result.completionLatencyMs?.samples).toBe(2);
    expect(result.completionLatencyMs!.p50!).toBeCloseTo(50, 0);
    expect(result.completionLatencyMs!.p99!).toBeGreaterThanOrEqual(500);
    expect(result.completionLatencyMs!.p99!).toBeLessThan(503);
    await Bun.write(dispatch, "a\t1000\nc\t1020\n");
    await expect(auditCapacityIdentities(http, ack, 1000, 60, dispatch)).rejects.toThrow(
      "dispatch",
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("a strong aggregate cannot hide an under-capacity minute or a P99 budget breach", () => {
  const http = {
    succeeded: 750000,
    duration_seconds: 120,
    latency_ms: { p99: 200 },
    latency_windows: {
      "0": { p99: 100 },
      "1": { p99: 200 },
    },
  };
  expect(passesOnlineCapacity(true, http, 6000, 120, [370000, 359999], 1500, 500)).toBe(false);
  expect(passesOnlineCapacity(true, http, 6000, 120, [370000, 360000], 1500, 500)).toBe(true);
  http.latency_windows["1"].p99 = 1500.01;
  expect(passesOnlineCapacity(true, http, 6000, 120, [370000, 360000], 1500, 500)).toBe(false);
  http.latency_windows["1"].p99 = 200;
  expect(passesOnlineCapacity(false, http, 6000, 120, [370000, 360000], 1500, 500)).toBe(false);
  expect(passesOnlineCapacity(true, http, 6000, 120, [730000], 1500, 500)).toBe(false);
});

test("fast HTTP cannot pass a stream minute when over one percent of its final ACKs arrive late", async () => {
  const directory = await mkdtemp(join(tmpdir(), "gateway-late-stream-ack-"));
  try {
    const dispatch = join(directory, "dispatch.tsv");
    const http = join(directory, "http.tsv");
    const ack = join(directory, "ack.tsv");
    const ids = Array.from({ length: 200 }, (_, index) => String(index));
    const start = (index: number) => (index < 100 ? 1000 : 61000);
    await Bun.write(dispatch, ids.map((id, i) => `${id}\t${start(i)}\n`).join(""));
    await Bun.write(http, ids.map((id, i) => `${id}\t${start(i) + 5}\n`).join(""));
    for (const late of [1, 2]) {
      await Bun.write(
        ack,
        ids.map((id, i) => `${id}\t${start(i) + (i < late ? 1501 : 1500)}\n`).join(""),
      );
      const audit = await auditCapacityIdentities(http, ack, 1000, 120, dispatch, 1500);
      expect(audit.minuteCompletions).toEqual([100, 100]);
      const response = {
        succeeded: ids.length,
        duration_seconds: 120,
        latency_ms: { p99: 5 },
        latency_windows: { "0": { p99: 5 }, "1": { p99: 5 } },
      };
      expect(
        passesOnlineCapacity(
          true,
          response,
          1,
          120,
          audit.minuteCompletions,
          1500,
          500,
          audit.completionLatencyMs,
        ),
      ).toBe(late === 1);
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("slow tail responses cannot pass capacity when window completions meet the throughput target", () => {
  const http = {
    succeeded: 600000,
    duration_seconds: 62,
    latency_ms: { p99: 2000 },
    latency_windows: { "0": { p99: 2000 } },
    minute_windows: {
      "0": { latency_ms: { p99: 100 } },
      "1": { latency_ms: { p99: 2000 } },
    },
  };
  expect(passesOnlineCapacity(true, http, 6000, 60, [592000], 1500, 500)).toBe(false);
});

test("fast requests dispatched after the window cannot dilute a failed stream minute", async () => {
  const dir = await mkdtemp(join(tmpdir(), "gateway-stream-overrun-"));
  try {
    const starts = Array.from({ length: 120 }, (_, index) =>
      index < 99 ? 1000 + index * 500 : 61025,
    );
    const log = (times: number[]) => times.map((time, id) => `${id}\t${time}\n`).join("");
    const http = join(dir, "http"),
      ack = join(dir, "ack"),
      dispatch = join(dir, "dispatch");
    await Bun.write(dispatch, log(starts));
    await Bun.write(http, log(starts.map((at) => at + 5)));
    await Bun.write(ack, log(starts.map((at, id) => at + (id === 0 ? 1501 : 5))));
    const result = await auditCapacityIdentities(http, ack, 1000, 60, dispatch);
    const response = {
      succeeded: 120,
      duration_seconds: 60.03,
      latency_ms: { p99: 5 },
      latency_windows: { "0": { p99: 5 } },
    };
    expect(
      passesOnlineCapacity(
        true,
        response,
        1,
        60,
        result.minuteCompletions,
        1500,
        30,
        result.completionLatencyMs,
      ),
    ).toBe(false);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("drain beyond 120 seconds after arrivals stop cannot pass despite correct final accounting", () => {
  const http = {
    succeeded: 400000,
    duration_seconds: 61,
    latency_ms: { p99: 100 },
    latency_windows: { "0": { p99: 100 } },
  };
  expect(passesOnlineCapacity(true, http, 6000, 60, [370000], 1500, 120001)).toBe(false);
});

test("a looser --max-p99-ms cannot turn a contract latency breach into a capacity pass", () => {
  const http = {
    succeeded: 750000,
    duration_seconds: 120,
    latency_ms: { p99: 1800 },
    latency_windows: { "0": { p99: 1800 }, "1": { p99: 1800 } },
  };
  expect(passesOnlineCapacity(true, http, 6000, 120, [370000, 360000], 2000, 500)).toBe(false);
  http.latency_ms.p99 = 900;
  http.latency_windows["0"].p99 = 900;
  http.latency_windows["1"].p99 = 900;
  expect(passesOnlineCapacity(true, http, 6000, 120, [370000, 360000], 1000, 500)).toBe(true);
});
