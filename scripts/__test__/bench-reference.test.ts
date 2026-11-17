import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  referenceOptions,
  auditReferencePreConsumption,
  referenceVerdict,
  type ReferenceHttp,
  type ReferenceAccounting,
} from "../bench-reference-lib.ts";

test("reference CLI accepts disabled trust and rejects malformed or unsafe workload bounds", () => {
  expect(
    referenceOptions([
      "--binary",
      "/tmp/new-api",
      "--wallets",
      "10",
      "--rate",
      "3000",
      "--trust-quota-usd",
      "0",
    ]),
  ).toMatchObject({ wallets: 10, rate: 3000, trustQuotaUsd: 0 });
  for (const args of [
    ["--rate", "0"],
    ["--rate", "NaN"],
    ["--wallets", "1001"],
    ["--seconds", "3601"],
    ["--trust-quota-usd", "-1"],
    ["--trust-quota-usd", "Infinity"],
    ["--unknown", "1"],
    ["--rate"],
    ["--rate", "1", "--rate", "2"],
  ])
    expect(() => referenceOptions(["--binary", "/tmp/new-api", ...args])).toThrow();
  expect(() => referenceOptions([])).toThrow("--binary");
});

function fixture() {
  const http: ReferenceHttp = {
    offered: 120,
    completed: 120,
    shed: 0,
    invalid: 0,
    succeeded: 120,
    warmup_requests: 10,
    warmup_concurrency: 1,
    duration_seconds: 60.1,
    latency_ms: { p99: 10 },
    latency_windows: { "0": { p99: 10 } },
    minute_windows: { "0": { succeeded: 119 } },
    wallet_calls: [130],
    measurement_started_at: "2026-11-17T00:00:00Z",
  };
  const accounting: ReferenceAccounting = {
    logs: 130,
    log_quota: 260,
    users_used_quota: 260,
    tokens_used_quota: 260,
    channel_used_quota: 260,
    user_requests: 130,
    invalid_logs: 0,
    wallet_mismatches: 0,
  };
  return { http, accounting };
}

test("reference audit includes warmup but capacity counts only completed arrival windows", () => {
  const { http, accounting } = fixture();
  const verdict = referenceVerdict(http, accounting, 2, 60, 1, 100);
  expect(verdict).toMatchObject({
    valid: true,
    capacityPassed: true,
    expected_requests: 130,
    expected_quota: 260,
    quota_equivalent_requests: { logs: 130, users: 130, tokens: 130, channel: 130 },
  });
  expect(referenceVerdict(http, accounting, 2, 60, 2, 100).capacityPassed).toBe(false);
});

test("reference rejects dropped arrivals, incorrect quota, wallet differences, slow tails and late drain", () => {
  const { http, accounting } = fixture();
  expect(referenceVerdict({ ...http, shed: 1 }, accounting, 2, 60, 1, 100).valid).toBe(false);
  expect(referenceVerdict({ ...http, completed: 119 }, accounting, 2, 60, 1, 100).valid).toBe(
    false,
  );
  for (const key of [
    "logs",
    "log_quota",
    "users_used_quota",
    "tokens_used_quota",
    "channel_used_quota",
    "user_requests",
  ] as const)
    expect(
      referenceVerdict(http, { ...accounting, [key]: accounting[key] - 1 }, 2, 60, 1, 100).valid,
    ).toBe(false);
  expect(referenceVerdict(http, { ...accounting, wallet_mismatches: 1 }, 2, 60, 1, 100).valid).toBe(
    false,
  );
  expect(referenceVerdict(http, { ...accounting, invalid_logs: 1 }, 2, 60, 1, 100).valid).toBe(
    false,
  );
  expect(
    referenceVerdict(
      { ...http, latency_windows: { "0": { p99: 1501 } } },
      accounting,
      2,
      60,
      1,
      100,
    ).capacityPassed,
  ).toBe(false);
  expect(referenceVerdict(http, accounting, 2, 60, 1, 120001).capacityPassed).toBe(false);
});

test("reference reservation multiplier rejects zero and preserves independently configured trust", () => {
  expect(
    referenceOptions([
      "--binary",
      "/tmp/new-api",
      "--trust-quota-usd",
      "0",
      "--pre-consume-multiplier",
      "100",
    ]),
  ).toMatchObject({ trustQuotaUsd: 0, preConsumeMultiplier: 100 });
  for (const value of ["0", "-1", "NaN", "Infinity", "1000001"])
    expect(() =>
      referenceOptions(["--binary", "/tmp/new-api", "--pre-consume-multiplier", value]),
    ).toThrow();
});

test("reference distinguishes a zero rounded estimate from actual positive pre-consumption", async () => {
  const directory = await mkdtemp(join(tmpdir(), "reference-billing-log-"));
  try {
    const path = join(directory, "api.log");
    await Bun.write(
      path,
      "user 1 has enough quota, trusted without pre-consume (funding=wallet)\ncharging the difference after pre-consume: ＄0.000004 (actual ＄0.000004, pre-consumed ＄0.000000)\nuser 2 needs pre-consume ＄0.000120 (funding=wallet)\nrefunding the difference after pre-consume: ＄0.000116 (actual ＄0.000004, pre-consumed ＄0.000120)\nuser 3 needs pre-consume ＄0.000120 (funding=wallet)",
    );
    expect(await auditReferencePreConsumption(path)).toMatchObject({
      trusted_requests: 1,
      positive_preconsume_requests: 2,
      zero_preconsumed_settlements: 1,
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("positive reservation acceptance fails for a missing positive branch, trusted bypass or zero settlement", () => {
  const { http, accounting } = fixture();
  const audit = {
    positive_preconsume_requests: 130,
    trusted_requests: 0,
    zero_preconsumed_settlements: 0,
  };
  expect(referenceVerdict(http, accounting, 2, 60, 1, 100, audit).capacityPassed).toBe(true);
  for (const bad of [
    { ...audit, positive_preconsume_requests: 129 },
    { ...audit, trusted_requests: 1 },
    { ...audit, zero_preconsumed_settlements: 1 },
  ]) {
    const verdict = referenceVerdict(http, accounting, 2, 60, 1, 100, bad);
    expect(verdict.capacityPassed).toBe(false);
    expect(verdict.failures).toContain("positive_preconsume_not_proven");
  }
});
