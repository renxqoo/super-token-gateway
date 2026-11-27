import { SQL, type TransactionSQL } from "bun";
import { expect, test } from "bun:test";
import {
  createConsoleDatabase,
  createConsoleRuntime,
  migrateConsoleDb,
} from "@gateway/console-api";
import migrationManifest from "../../apps/console-api/src/db/migrations/manifest.json";
import {
  parseUsageMonitorPolicy,
  versionUsageMonitorPolicy,
  type UsageMonitorScope,
} from "../../apps/console-api/src/usage-monitor/policy.ts";
import {
  claimUsageWindow,
  readUsageSamples,
  saveUsageWindow,
} from "../../apps/console-api/src/usage-monitor/store.ts";
import { waitFor } from "./adversarial-billing-fixture.ts";
import { createConsoleTestHarness } from "./helpers/console-harness.ts";

const width = 60_000;
const epoch = Math.ceil((Date.now() + 30_000) / width) * width;
const initialGrace = 1000;
const finalGrace = 5000;
const path = "/admin/v1/usage-monitor/findings";

type Harness = Awaited<ReturnType<typeof createConsoleTestHarness>>;
type Finding = {
  id: string;
  policy_version: string;
  channel_id: string;
  rule_key: string;
  matched: boolean;
  disposition: string;
  revision: number;
  evidence: {
    observed: string;
    reference: string;
    sample_count: number;
    samples: { event_id: string; request_id: string; charge_id: string; value: string }[];
  };
};

function scope(rules: Partial<UsageMonitorScope>): UsageMonitorScope {
  return {
    channel_id: crypto.randomUUID(),
    model: "monitor-chat",
    operation: "chat",
    currency: "USD",
    ...rules,
  };
}

function settlement(target: UsageMonitorScope, window: number, output?: string | null) {
  const { channel_id, model, operation, currency } = target;
  return {
    event_id: crypto.randomUUID(),
    request_id: crypto.randomUUID(),
    kind: "billing.settled",
    created_at: new Date(epoch + window * width + 1).toISOString(),
    payload: {
      channel_id,
      model,
      operation,
      currency,
      charge_id: crypto.randomUUID(),
      customer_atoms: "10",
      outcome: "SETTLED",
      usage: output === undefined ? {} : { output_tokens: output },
    },
  };
}

function policyFor(scopes: UsageMonitorScope[], maxRows = 100) {
  return parseUsageMonitorPolicy({
    effective_from: new Date(epoch).toISOString(),
    window_ms: width,
    initial_grace_ms: initialGrace,
    final_grace_ms: finalGrace,
    max_rows: maxRows,
    scopes,
  });
}

function harnessWith(policy: ReturnType<typeof policyFor>) {
  return createConsoleTestHarness({
    enableWorker: false,
    consoleConfigOverrides: { usageMonitor: { policy, notifications: true, intervalMs: 0 } },
  });
}

function push(h: Harness, events: ReturnType<typeof settlement>[]) {
  return h.adminApi("POST", "/internal/outbox/events", h.outboxToken, { events });
}

function replayFirstWindow(h: Harness, token: string, version: string) {
  return h.adminApi("POST", "/admin/v1/usage-monitor/replay", token, {
    policy_version: version,
    from: new Date(epoch).toISOString(),
    to: new Date(epoch + width).toISOString(),
  });
}

function list(h: Harness, token?: string, query = "") {
  return h.adminApi<{ data: Finding[]; next_cursor: Record<string, string> | null }>(
    "GET",
    path + query,
    token,
  );
}

test("precise settled usage creates one platform finding and late replay preserves operator disposition", async () => {
  const target = scope({ max_output_tokens: "9007199254740992" });
  const h = await harnessWith(policyFor([target]));
  try {
    const { accessToken } = await h.bootstrapAdmin();
    const tenant = await h.registerUser("monitor-tenant@example.test");
    const extreme = settlement(target, 0, "9007199254740993");
    const atLimit = settlement(target, 0, "9007199254740992");
    expect(
      await push(h, [
        extreme,
        atLimit,
        settlement(target, 0, "0"),
        settlement(target, 0),
        settlement(target, 0, null),
        extreme,
      ]),
    ).toMatchObject({ status: 200, data: { ingested: 5 } });
    expect(await h.consoleRuntime.flushUsageMonitorOnce(epoch + width + initialGrace)).toBe(
      "evaluated",
    );
    const response = await list(h, accessToken);
    expect(response.status).toBe(200);
    expect(response.data.data).toHaveLength(1);
    const [finding] = response.data.data;
    expect(finding).toMatchObject({
      rule_key: "MAX_OUTPUT",
      channel_id: target.channel_id,
      disposition: "OPEN",
      evidence: {
        observed: "9007199254740993",
        reference: "9007199254740992",
        sample_count: 1,
        samples: [
          {
            event_id: extreme.event_id,
            request_id: extreme.request_id,
            charge_id: extreme.payload.charge_id,
            value: "9007199254740993",
          },
        ],
      },
    });
    expect((await list(h)).status).toBe(401);
    expect((await list(h, tenant.accessToken)).status).toBe(401);
    expect((await h.clientApi("GET", path, accessToken)).status).toBe(404);
    const metrics = await fetch(h.adminUrlHttp + "/metrics").then((res) => res.text());
    expect(metrics).toContain('stg_console_usage_monitor_open_findings{rule="MAX_OUTPUT"} 1');
    expect(metrics).not.toContain(extreme.request_id);

    const [quality] =
      await h.conDb`SELECT evaluated->'output' AS output FROM usage_monitor_window_stats`;
    expect(quality?.output).toMatchObject({
      measured: 3,
      missing: 1,
      invalid: 1,
      max: "9007199254740993",
    });
    expect(
      await h.adminApi("PATCH", path + "/" + finding!.id, accessToken, {
        revision: finding!.revision,
        disposition: "ACKNOWLEDGED",
        reason: "Reviewing usage",
      }),
    ).toMatchObject({ status: 200, data: { disposition: "ACKNOWLEDGED" } });
    expect(
      await h.adminApi("PATCH", path + "/" + finding!.id, accessToken, {
        revision: finding!.revision,
        disposition: "ACKNOWLEDGED",
        reason: "Stale change",
      }),
    ).toMatchObject({ status: 409 });

    const later = settlement(target, 0, "9007199254740994");
    expect((await push(h, [later, extreme])).data.ingested).toBe(1);
    await h.consoleRuntime.close();
    const restarted = createConsoleRuntime({ config: h.consoleConfig, db: h.conDb });
    try {
      await restarted.start();
      expect(await restarted.flushUsageMonitorOnce(epoch + width + finalGrace)).toBe("evaluated");
      const final = await list(h, accessToken);
      expect(final.data.data).toHaveLength(1);
      const [resolved] = final.data.data;
      expect(resolved).toMatchObject({
        id: finding!.id,
        disposition: "ACKNOWLEDGED",
        evidence: { observed: "9007199254740994", sample_count: 2 },
      });
      expect(
        (
          await h.adminApi("PATCH", path + "/" + finding!.id, accessToken, {
            revision: resolved!.revision,
            disposition: "RESOLVED",
            reason: "Evidence reviewed",
          })
        ).status,
      ).toBe(200);
      const lateAfterFinal = settlement(target, 0, "9007199254740995");
      expect((await push(h, [lateAfterFinal])).data.ingested).toBe(1);
      expect(await replayFirstWindow(h, accessToken, finding!.policy_version)).toMatchObject({
        status: 202,
        data: { scheduled: 1 },
      });
      expect(await restarted.flushUsageMonitorOnce(epoch + width + finalGrace)).toBe("evaluated");
      expect((await list(h, accessToken)).data.data).toEqual([
        expect.objectContaining({
          id: finding!.id,
          disposition: "RESOLVED",
          evidence: expect.objectContaining({ observed: "9007199254740995", sample_count: 3 }),
        }),
      ]);
      const audit = await h.conDb`SELECT action FROM console_audit_logs
        WHERE action LIKE 'admin.usage_monitor.%' ORDER BY created_at`;
      expect(audit).toEqual([
        { action: "admin.usage_monitor.disposition" },
        { action: "admin.usage_monitor.disposition" },
        { action: "admin.usage_monitor.replay" },
      ]);
    } finally {
      await restarted.close();
    }
  } finally {
    await h.close();
  }
});

test("trend findings require enough samples in consecutive windows for the same scope", async () => {
  const target = scope({
    trend: {
      minimum_samples: 2,
      baseline_windows: 2,
      consecutive_windows: 2,
      increase_bps: 5000,
      minimum_delta: "20",
    },
  });
  const other = scope({});
  const h = await harnessWith(policyFor([target]));
  try {
    const { accessToken } = await h.bootstrapAdmin();
    for (const [window, value, count] of [
      [0, "100", 2],
      [1, "105", 2],
      [2, "175", 1],
      [3, "180", 2],
      [4, "100", 2],
      [5, "110", 2],
      [6, "175", 2],
      [7, "180", 2],
    ] as const) {
      const settled = Array.from({ length: count }, () => settlement(target, window, value));
      expect((await push(h, [...settled, settlement(other, window, "90000")])).status).toBe(200);
      expect(
        await h.consoleRuntime.flushUsageMonitorOnce(epoch + (window + 1) * width + finalGrace),
      ).toBe("evaluated");
    }
    const result = await list(h, accessToken);
    expect(result.data.data).toHaveLength(1);
    expect(result.data.data[0]).toMatchObject({
      channel_id: target.channel_id,
      rule_key: "TREND_OUTPUT",
      evidence: { observed: "180", reference: "100", sample_count: 2 },
    });
  } finally {
    await h.close();
  }
});

test("trend uses discrete P95 and also requires the independent absolute increase", async () => {
  const target = scope({
    trend: {
      minimum_samples: 20,
      baseline_windows: 1,
      consecutive_windows: 1,
      increase_bps: 5000,
      minimum_delta: "20",
    },
  });
  const h = await harnessWith(policyFor([target]));
  try {
    const { accessToken } = await h.bootstrapAdmin();
    const values = [
      ["1", "10", "1000"],
      ["1", "16", "1100"],
      ["2", "45", "900"],
    ] as const;
    for (const [window, [low, p95, max]] of values.entries()) {
      const samples = [...Array<string>(18).fill(low), p95, max];
      expect(
        await push(
          h,
          samples.map((value) => settlement(target, window, value)),
        ),
      ).toMatchObject({
        status: 200,
        data: { ingested: 20 },
      });
      expect(
        await h.consoleRuntime.flushUsageMonitorOnce(epoch + (window + 1) * width + finalGrace),
      ).toBe("evaluated");
    }
    expect((await list(h, accessToken)).data.data).toEqual([
      expect.objectContaining({
        rule_key: "TREND_OUTPUT",
        evidence: expect.objectContaining({ observed: "45", reference: "16", sample_count: 20 }),
      }),
    ]);
  } finally {
    await h.close();
  }
});

test("range replay waits for an earlier baseline window that is in retry backoff", async () => {
  const target = scope({
    trend: {
      minimum_samples: 1,
      baseline_windows: 1,
      consecutive_windows: 1,
      increase_bps: 5000,
      minimum_delta: "20",
    },
  });
  const policy = policyFor([target]);
  const h = await harnessWith(policy);
  try {
    const { accessToken } = await h.bootstrapAdmin();
    expect(
      (await push(h, [settlement(target, 0, "100"), settlement(target, 1, "50")])).status,
    ).toBe(200);
    const trigger = epoch + 2 * width + finalGrace;
    expect(await h.consoleRuntime.flushUsageMonitorOnce(epoch + width + finalGrace)).toBe(
      "evaluated",
    );
    expect(await h.consoleRuntime.flushUsageMonitorOnce(trigger)).toBe("evaluated");
    expect((await list(h, accessToken)).data.data).toEqual([]);
    expect(
      (
        await push(
          h,
          Array.from({ length: 20 }, () => settlement(target, 0, "10")),
        )
      ).status,
    ).toBe(200);
    expect(
      await h.adminApi("POST", "/admin/v1/usage-monitor/replay", accessToken, {
        policy_version: versionUsageMonitorPolicy(policy),
        from: new Date(epoch).toISOString(),
        to: new Date(epoch + 2 * width).toISOString(),
      }),
    ).toMatchObject({ status: 202, data: { scheduled: 2 } });
    const retry = trigger + 1000;
    await h.conDb`UPDATE usage_monitor_windows SET next_attempt_at = ${new Date(retry)}
      WHERE window_start = ${new Date(epoch)}`;
    expect(await h.consoleRuntime.flushUsageMonitorOnce(trigger)).toBe("idle");
    expect(await h.consoleRuntime.flushUsageMonitorOnce(retry)).toBe("evaluated");
    expect(await h.consoleRuntime.flushUsageMonitorOnce(retry)).toBe("evaluated");
    expect((await list(h, accessToken)).data.data).toEqual([
      expect.objectContaining({
        rule_key: "TREND_OUTPUT",
        evidence: expect.objectContaining({ observed: "50", reference: "10" }),
      }),
    ]);
  } finally {
    await h.close();
  }
});

test("a newer ordinary window waits for a valid trend baseline and is refreshed after replay", async () => {
  const target = scope({
    max_output_tokens: "40",
    trend: {
      minimum_samples: 1,
      baseline_windows: 1,
      consecutive_windows: 1,
      increase_bps: 5000,
      minimum_delta: "20",
    },
  });
  const policy = policyFor([target]);
  const h = await harnessWith(policy);
  try {
    const { accessToken } = await h.bootstrapAdmin();
    const version = versionUsageMonitorPolicy(policy);
    const firstFinal = epoch + width + finalGrace;
    const secondFinal = epoch + 2 * width + finalGrace;
    expect(
      (await push(h, [settlement(target, 0, "100"), settlement(target, 1, "80")])).status,
    ).toBe(200);
    expect(await h.consoleRuntime.flushUsageMonitorOnce(firstFinal)).toBe("evaluated");
    expect(
      (
        await push(
          h,
          Array.from({ length: 20 }, () => settlement(target, 0, "10")),
        )
      ).status,
    ).toBe(200);
    const replay = () => replayFirstWindow(h, accessToken, version);
    expect(await replay()).toMatchObject({ status: 202, data: { scheduled: 1 } });
    const resumedAt = secondFinal + 1_000;
    await h.conDb`UPDATE usage_monitor_windows SET next_attempt_at = ${new Date(resumedAt)} WHERE window_start = ${new Date(epoch)}`;
    expect(await h.consoleRuntime.flushUsageMonitorOnce(secondFinal)).toBe("evaluated");
    const trends = async () =>
      (await list(h, accessToken)).data.data.filter(({ rule_key }) => rule_key === "TREND_OUTPUT");
    expect(await trends()).toEqual([]);
    expect((await list(h, accessToken)).data.data).toContainEqual(
      expect.objectContaining({
        rule_key: "MAX_OUTPUT",
        matched: true,
        evidence: expect.objectContaining({ observed: "80" }),
      }),
    );
    expect(await h.consoleRuntime.flushUsageMonitorOnce(resumedAt)).toBe("evaluated");
    await h.consoleRuntime.flushUsageMonitorOnce(resumedAt);
    expect(await trends()).toEqual([
      expect.objectContaining({
        matched: true,
        evidence: expect.objectContaining({ observed: "80", reference: "10" }),
      }),
    ]);
    expect(
      (
        await push(
          h,
          Array.from({ length: 20 }, () => settlement(target, 0, "100")),
        )
      ).status,
    ).toBe(200);
    expect(await replay()).toMatchObject({ status: 202, data: { scheduled: 1 } });
    expect(await trends()).toEqual([expect.objectContaining({ matched: false })]);
    expect(await h.consoleRuntime.flushUsageMonitorOnce(resumedAt)).toBe("evaluated");
    await h.consoleRuntime.flushUsageMonitorOnce(resumedAt);
    expect(await trends()).toEqual([expect.objectContaining({ matched: false })]);
  } finally {
    await h.close();
  }
});

test("a superseded replay lets another channel run and releases its window on completion", async () => {
  const target = scope({ max_output_tokens: "5" });
  const other = scope({ max_output_tokens: "10" });
  const policy = policyFor([target, other]);
  const h = await harnessWith(policy);
  try {
    const { accessToken } = await h.bootstrapAdmin();
    const version = versionUsageMonitorPolicy(policy);
    const now = epoch + width + finalGrace;
    const replay = () => replayFirstWindow(h, accessToken, version);
    expect(await push(h, [settlement(target, 0, "10")])).toMatchObject({ status: 200 });
    expect(await h.consoleRuntime.flushUsageMonitorOnce(now)).toBe("evaluated");
    expect(await replay()).toMatchObject({ status: 202, data: { scheduled: 1 } });
    const old = await claimUsageWindow(h.conDb, version, policy, now);
    expect(old).toBeDefined();
    const oldSamples = await readUsageSamples(h.conDb, old!.lease, policy);
    expect(oldSamples?.map(({ output_tokens }) => output_tokens)).toEqual(["10"]);
    expect(await push(h, [settlement(target, 0, "25")])).toMatchObject({ status: 200 });
    expect(await replay()).toMatchObject({ status: 202, data: { scheduled: 1 } });
    expect(await push(h, [settlement(other, 1, "12")])).toMatchObject({ status: 200 });
    expect(await h.consoleRuntime.flushUsageMonitorOnce(now)).toBe("idle");
    expect(await h.consoleRuntime.flushUsageMonitorOnce(now + width)).toBe("evaluated");
    expect((await list(h, accessToken)).data.data).toContainEqual(
      expect.objectContaining({
        channel_id: other.channel_id,
        rule_key: "MAX_OUTPUT",
        evidence: expect.objectContaining({ observed: "12" }),
      }),
    );
    expect(await saveUsageWindow(h.conDb, old!.lease, policy, oldSamples!)).toBe(false);
    expect(await h.consoleRuntime.flushUsageMonitorOnce(now)).toBe("evaluated");
    expect(await saveUsageWindow(h.conDb, old!.lease, policy, oldSamples!)).toBe(false);
    expect((await list(h, accessToken, `?channel_id=${target.channel_id}`)).data.data).toEqual([
      expect.objectContaining({
        rule_key: "MAX_OUTPUT",
        evidence: expect.objectContaining({ observed: "25", sample_count: 2 }),
      }),
    ]);
  } finally {
    await h.close();
  }
});

async function holdTransaction(h: Harness, acquire: (tx: TransactionSQL) => PromiseLike<unknown>) {
  const locked = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const lock = h.conDb.begin(async (tx) => {
    await acquire(tx);
    locked.resolve();
    await release.promise;
  });
  await Promise.race([locked.promise, lock]);
  return async () => {
    release.resolve();
    await lock;
  };
}

async function waitingUsageReads(h: Harness) {
  return h.conDb<{ pid: number }[]>`
    SELECT pid FROM pg_locks
    WHERE relation = 'public.usage_events'::regclass AND mode = 'AccessShareLock' AND NOT granted
      AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
  `;
}

test.each([
  { scenario: "ordinary cleanup", interruptRetry: false },
  { scenario: "interrupted cleanup transaction", interruptRetry: true },
])("a failed superseded scan releases the window after $scenario", async ({ interruptRetry }) => {
  const target = scope({ max_output_tokens: "5" });
  const policy = policyFor([target]);
  const h = await harnessWith(policy);
  let releaseSource: undefined | (() => Promise<void>);
  let releaseWriter: undefined | (() => Promise<void>);
  let old: Promise<{ outcome: string | undefined } | { error: unknown }> | undefined;
  try {
    const { accessToken } = await h.bootstrapAdmin();
    const version = versionUsageMonitorPolicy(policy);
    const now = epoch + width + finalGrace;
    const replay = () => replayFirstWindow(h, accessToken, version);
    expect(await push(h, [settlement(target, 0, "10")])).toMatchObject({ status: 200 });
    expect(await h.consoleRuntime.flushUsageMonitorOnce(now)).toBe("evaluated");
    expect(await replay()).toMatchObject({ status: 202, data: { scheduled: 1 } });
    expect(await push(h, [settlement(target, 0, "25")])).toMatchObject({ status: 200 });
    releaseSource = await holdTransaction(
      h,
      (tx) => tx`LOCK TABLE public.usage_events IN ACCESS EXCLUSIVE MODE`,
    );
    old = h.consoleRuntime.flushUsageMonitorOnce(now).then(
      (outcome) => ({ outcome }),
      (error: unknown) => ({ error }),
    );
    await waitFor(async () => (await waitingUsageReads(h)).length === 1, 2000);
    const [reader] = await waitingUsageReads(h);
    expect(reader?.pid).toBeGreaterThan(0);
    expect(await replay()).toMatchObject({ status: 202, data: { scheduled: 1 } });
    if (interruptRetry) {
      releaseWriter = await holdTransaction(h, (tx) => tx`SELECT pg_advisory_xact_lock(72194032)`);
    }
    const stopStatement = async () => {
      const [row] = await h.conDb`SELECT pg_cancel_backend(${reader!.pid}::int) AS canceled`;
      expect(row?.canceled).toBe(true);
    };
    await stopStatement();
    if (interruptRetry) {
      await waitFor(
        async () =>
          (
            await h.conDb`SELECT 1 FROM pg_locks WHERE pid = ${reader!.pid} AND locktype = 'advisory' AND NOT granted`
          ).length > 0,
        2000,
      );
      await stopStatement();
      expect(await old).toMatchObject({ error: expect.objectContaining({ errno: "57014" }) });
      await releaseWriter?.();
    } else {
      expect(await old).toEqual({ outcome: "stale" });
    }
    await releaseSource();
    expect(await h.consoleRuntime.flushUsageMonitorOnce(now)).toBe("evaluated");
    expect((await list(h, accessToken)).data.data).toEqual([
      expect.objectContaining({
        rule_key: "MAX_OUTPUT",
        evidence: expect.objectContaining({ observed: "25", sample_count: 2 }),
      }),
    ]);
  } finally {
    await Promise.allSettled([releaseSource?.(), releaseWriter?.(), old]);
    await h.close();
  }
});

test("an expired lease cannot start a second scan while the old database read is active", async () => {
  const target = scope({ max_output_tokens: "5" });
  const policy = policyFor([target]);
  const h = await harnessWith(policy);
  let releaseSource: undefined | (() => Promise<void>);
  let oldRead: ReturnType<typeof readUsageSamples> | undefined;
  try {
    const version = versionUsageMonitorPolicy(policy);
    const now = epoch + width + finalGrace;
    releaseSource = await holdTransaction(
      h,
      (tx) => tx`LOCK TABLE public.usage_events IN ACCESS EXCLUSIVE MODE`,
    );
    const old = await claimUsageWindow(h.conDb, version, policy, now);
    expect(old).toBeDefined();
    oldRead = readUsageSamples(h.conDb, old!.lease, policy);
    await Promise.race([
      waitFor(async () => (await waitingUsageReads(h)).length === 1, 2000),
      oldRead.then(() => {
        throw new Error("Old scan exited before it reached the lock");
      }),
    ]);
    await h.conDb`UPDATE usage_monitor_windows SET leased_until = clock_timestamp() - interval '1 second'
      WHERE window_start = ${new Date(epoch)}`;
    expect(await h.consoleRuntime.flushUsageMonitorOnce(now)).toBe("stale");
    expect(await waitingUsageReads(h)).toHaveLength(1);
    await releaseSource();
    expect(await oldRead).toEqual([]);
    expect(await h.consoleRuntime.flushUsageMonitorOnce(now)).toBe("evaluated");
  } finally {
    await Promise.allSettled([releaseSource?.(), oldRead]);
    await h.close();
  }
});

test("an initial trend stops paging when final evaluation cannot cover the window", async () => {
  const target = scope({
    trend: {
      minimum_samples: 1,
      baseline_windows: 1,
      consecutive_windows: 1,
      increase_bps: 5000,
      minimum_delta: "20",
    },
  });
  const h = await harnessWith(policyFor([target], 2));
  try {
    const { accessToken } = await h.bootstrapAdmin();
    expect((await push(h, [settlement(target, 0, "10"), settlement(target, 1, "80")])).status).toBe(
      200,
    );
    expect(await h.consoleRuntime.flushUsageMonitorOnce(epoch + width + finalGrace)).toBe(
      "evaluated",
    );
    expect(await h.consoleRuntime.flushUsageMonitorOnce(epoch + 2 * width + initialGrace)).toBe(
      "evaluated",
    );
    expect((await list(h, accessToken)).data.data).toEqual([
      expect.objectContaining({ rule_key: "TREND_OUTPUT", matched: true }),
    ]);
    expect((await push(h, [settlement(target, 1, "90"), settlement(target, 1, "90")])).status).toBe(
      200,
    );
    expect(await h.consoleRuntime.flushUsageMonitorOnce(epoch + 2 * width + finalGrace)).toBe(
      "limited",
    );
    const metrics = await fetch(h.adminUrlHttp + "/metrics").then((res) => res.text());
    expect(metrics).toContain("stg_console_usage_monitor_coverage_gaps 1");
    expect(metrics).toContain('stg_console_usage_monitor_open_findings{rule="TREND_OUTPUT"} 0');
    expect((await list(h, accessToken)).data.data).toEqual([
      expect.objectContaining({ rule_key: "TREND_OUTPUT", matched: false }),
    ]);
  } finally {
    await h.close();
  }
});

test("window scans over budget record a coverage gap without partial findings", async () => {
  const primary = scope({ max_output_tokens: "5" });
  const secondary = scope({ max_output_tokens: "5" });
  const h = await harnessWith(policyFor([primary, secondary], 2));
  try {
    const { accessToken } = await h.bootstrapAdmin();
    expect(
      await push(h, [settlement(primary, 0, "9"), settlement(secondary, 0, "8")]),
    ).toMatchObject({ status: 200, data: { ingested: 2 } });
    expect(await push(h, [settlement(primary, 0, "10")])).toMatchObject({
      status: 200,
      data: { ingested: 1 },
    });
    expect(await h.consoleRuntime.flushUsageMonitorOnce(epoch + width + initialGrace)).toBe(
      "limited",
    );
    expect((await list(h, accessToken)).data.data).toEqual([]);
    const metrics = await fetch(h.adminUrlHttp + "/metrics").then((res) => res.text());
    expect(metrics).toContain("stg_console_usage_monitor_coverage_gaps 1");
    expect((await h.conDb`SELECT count(*)::int AS count FROM usage_events`)[0]?.count).toBe(3);
  } finally {
    await h.close();
  }
});

test("scheduling failure does not publish a fresh monitor heartbeat", async () => {
  const target = scope({ max_output_tokens: "5" });
  const h = await harnessWith(policyFor([target]));
  try {
    await h.conDb`ALTER TABLE usage_monitor_windows ADD CONSTRAINT simulated_insert_failure CHECK (false)`;
    expect(await push(h, [settlement(target, 0, "8")])).toMatchObject({ status: 200 });
    const now = epoch + width + initialGrace;
    await expect(h.consoleRuntime.flushUsageMonitorOnce(now)).rejects.toThrow(
      /simulated_insert_failure/,
    );
    const metrics = () => fetch(h.adminUrlHttp + "/metrics").then((res) => res.text());
    expect(await metrics()).toContain("stg_console_usage_monitor_last_tick_timestamp_seconds 0");
    await h.conDb`ALTER TABLE usage_monitor_windows DROP CONSTRAINT simulated_insert_failure`;
    expect(await h.consoleRuntime.flushUsageMonitorOnce(now)).toBe("evaluated");
    expect(await metrics()).toContain(
      `stg_console_usage_monitor_last_tick_timestamp_seconds ${now / 1000}`,
    );
  } finally {
    await h.close();
  }
});

test("two readers share one window lease and a stale reader cannot overwrite a final result", async () => {
  const target = scope({ max_output_tokens: "5" });
  const policy = policyFor([target]);
  const version = versionUsageMonitorPolicy(policy);
  const h = await harnessWith(policy);
  try {
    expect(await push(h, [settlement(target, 0, "8")])).toMatchObject({ status: 200 });
    const attempts = await Promise.all([
      claimUsageWindow(h.conDb, version, policy, epoch + width + initialGrace),
      claimUsageWindow(h.conDb, version, policy, epoch + width + initialGrace),
    ]);
    const leases = attempts.filter((claim) => claim !== undefined);
    expect(leases).toHaveLength(1);
    await h.conDb`UPDATE usage_monitor_windows SET leased_until=clock_timestamp() - interval '1 second'`;
    expect(await h.consoleRuntime.flushUsageMonitorOnce(epoch + width + finalGrace)).toBe(
      "evaluated",
    );
    expect(await saveUsageWindow(h.conDb, leases[0]!.lease, policy, [])).toBe(false);
    const { accessToken } = await h.bootstrapAdmin();
    expect((await list(h, accessToken)).data.data).toEqual([
      expect.objectContaining({
        matched: true,
        evidence: expect.objectContaining({ observed: "8" }),
      }),
    ]);
  } finally {
    await h.close();
  }
});

test("Console schema upgrade adds monitoring while preserving already projected usage", async () => {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL is required");
  const name = `console_monitor_upgrade_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
  const baseUrl = new URL(base);
  baseUrl.pathname = "/postgres";
  const admin = createConsoleDatabase(baseUrl.toString(), 1);
  const target = new URL(base);
  target.pathname = `/${name}`;
  const db = createConsoleDatabase(target.toString(), 1);
  const [baseline] = migrationManifest.migrations;
  const sql = await Bun.file(
    new URL(`../../apps/console-api/src/db/${baseline!.path}`, import.meta.url),
  ).text();
  const old = settlement(scope({ max_output_tokens: "50" }), 0, "75");
  try {
    await admin.unsafe(`CREATE DATABASE ${name}`);
    await db.begin(async (tx) => {
      await tx`CREATE TABLE console_schema_migrations (
        version integer PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now()
      )`;
      await tx.unsafe(sql);
      await tx`INSERT INTO console_schema_migrations (version, checksum)
        VALUES (${baseline!.version}, ${baseline!.checksum})`;
      await tx`INSERT INTO usage_events (event_id, request_id, kind, payload)
        VALUES (${old.event_id}, ${old.request_id}, 'billing.settled', ${JSON.stringify(old.payload)}::text::jsonb)`;
      await tx`UPDATE usage_events SET channel_id = ${old.payload.channel_id}, operation = 'chat',
        model = repeat('歴史', 500), outcome = 'SETTLED' WHERE event_id = ${old.event_id}`;
      await tx.unsafe(`
        CREATE FUNCTION hold_monitor_create_index() RETURNS event_trigger LANGUAGE plpgsql
        AS $fn$ BEGIN PERFORM pg_sleep(0.3); END; $fn$;
        CREATE EVENT TRIGGER hold_monitor_create_index ON ddl_command_start
          WHEN TAG IN ('CREATE INDEX') EXECUTE FUNCTION hold_monitor_create_index();
      `);
    });
    const writer = new SQL(target.toString(), { max: 1, connection: { statement_timeout: 150 } });
    try {
      await migrateConsoleDb(writer);
      const [timeout] = await writer`SHOW statement_timeout`;
      expect(timeout?.statement_timeout).toBe("150ms");
    } finally {
      await writer.close();
    }
    await migrateConsoleDb(db);
    const preserved =
      await db`SELECT event_id::text, request_id::text, char_length(model) AS model_chars, payload->'usage'->>'output_tokens' AS output
      FROM usage_events`;
    expect(preserved).toEqual([
      { event_id: old.event_id, request_id: old.request_id, model_chars: 1000, output: "75" },
    ]);
    const [index] = await db`SELECT pg_get_expr(indpred, indrelid) AS predicate FROM pg_index
      WHERE indexrelid = 'usage_events_monitor_scope_time_idx'::regclass`;
    const expression = await db.unsafe(
      `SELECT (${index?.predicate}) IS TRUE AS indexed FROM usage_events`,
    );
    expect(expression).toEqual([{ indexed: false }]);
    expect(await db`SELECT id FROM usage_monitor_findings`).toEqual([]);
  } finally {
    await db.close();
    await admin.unsafe(`DROP DATABASE IF EXISTS ${name}`);
    await admin.close();
  }
});
