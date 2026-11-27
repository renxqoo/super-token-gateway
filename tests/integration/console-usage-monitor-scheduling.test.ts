import { expect, test } from "bun:test";
import {
  parseUsageMonitorPolicy,
  versionUsageMonitorPolicy,
} from "../../apps/console-api/src/usage-monitor/policy.ts";
import { createConsoleTestHarness } from "./helpers/console-harness.ts";

const width = 60_000;
const windowStart = Math.ceil((Date.now() + 30_000) / width) * width;

test.each([
  { name: "a fresh replay yields to the due ordinary window", aged: false },
  { name: "an aged replay regains its due-time priority", aged: true },
])("$name", async ({ aged }) => {
  const target = {
    channel_id: crypto.randomUUID(),
    model: "replay-target",
    operation: "chat",
    currency: "USD",
    max_output_tokens: "5",
  } as const;
  const ordinary = { ...target, channel_id: crypto.randomUUID(), model: "ordinary" };
  const first = new Date(windowStart);
  const second = new Date(windowStart + width);
  const policy = parseUsageMonitorPolicy({
    effective_from: first.toISOString(),
    window_ms: width,
    initial_grace_ms: 1000,
    final_grace_ms: 5000,
    scopes: [target, ordinary],
  });
  const h = await createConsoleTestHarness({
    enableWorker: false,
    consoleConfigOverrides: { usageMonitor: { policy, intervalMs: 0, notifications: false } },
  });
  try {
    function event(scope: typeof target | typeof ordinary, at: Date, output: string) {
      return {
        event_id: crypto.randomUUID(),
        request_id: crypto.randomUUID(),
        kind: "billing.settled",
        created_at: at.toISOString(),
        payload: {
          ...scope,
          charge_id: crypto.randomUUID(),
          customer_atoms: "10",
          outcome: "SETTLED",
          usage: { output_tokens: output },
        },
      };
    }
    const push = (events: ReturnType<typeof event>[]) =>
      h.adminApi("POST", "/internal/outbox/events", h.outboxToken, { events });
    const { accessToken } = await h.bootstrapAdmin();
    expect(await push([event(target, first, "10"), event(ordinary, second, "30")])).toMatchObject({
      status: 200,
    });
    expect(await h.consoleRuntime.flushUsageMonitorOnce(+second + 5000)).toBe("evaluated");
    expect(await push([event(target, first, "20")])).toMatchObject({ status: 200 });
    expect(
      await h.adminApi("POST", "/admin/v1/usage-monitor/replay", accessToken, {
        policy_version: versionUsageMonitorPolicy(policy),
        from: first.toISOString(),
        to: second.toISOString(),
      }),
    ).toMatchObject({ status: 202, data: { scheduled: 1 } });
    if (aged) {
      await h.conDb`UPDATE usage_monitor_windows
        SET next_attempt_at = statement_timestamp() - interval '65 seconds'
        WHERE window_start = ${first}`;
    }
    const due = +second + width + 5000;
    const findings = () => h.conDb`
      SELECT model, evidence->>'observed' AS value
      FROM usage_monitor_findings ORDER BY model
    `;
    expect(await h.consoleRuntime.flushUsageMonitorOnce(due)).toBe("evaluated");
    expect(await findings()).toEqual(
      aged
        ? [{ model: "replay-target", value: "20" }]
        : [
            { model: "ordinary", value: "30" },
            { model: "replay-target", value: "10" },
          ],
    );
    expect(await h.consoleRuntime.flushUsageMonitorOnce(due)).toBe("evaluated");
    expect(await findings()).toEqual([
      { model: "ordinary", value: "30" },
      { model: "replay-target", value: "20" },
    ]);
  } finally {
    await h.close();
  }
});
