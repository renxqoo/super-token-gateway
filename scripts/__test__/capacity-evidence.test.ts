import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { auditCapacityIdentities, createIdentityWriter } from "../capacity-evidence.ts";
import { passesOnlineCapacity } from "../benchmark-lib.ts";

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

test("drain beyond 120 seconds after arrivals stop cannot pass despite correct final accounting", () => {
  const http = {
    succeeded: 400000,
    duration_seconds: 61,
    latency_ms: { p99: 100 },
    latency_windows: { "0": { p99: 100 } },
  };
  expect(passesOnlineCapacity(true, http, 6000, 60, [370000], 1500, 120001)).toBe(false);
});
