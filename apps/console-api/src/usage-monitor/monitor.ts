import type { SQL } from "bun";
import { versionUsageMonitorPolicy, type UsageMonitorConfig } from "./policy.ts";
import { usageMonitorRuleKeys } from "./rules.ts";
import {
  claimUsageWindow,
  readUsageSamples,
  releaseUsageWindow,
  retryUsageWindow,
  saveUsageWindow,
} from "./store.ts";

export function createUsageMonitor(db: SQL, config: UsageMonitorConfig) {
  const version = versionUsageMonitorPolicy(config.policy);
  let running = false;
  let snapshot = {
    open: new Map<string, number>(),
    oldestDueTimestamp: 0,
    coverageGaps: 0,
    skippedWindows: "0",
    lastSkippedTimestamp: 0,
    evaluatedAt: 0,
  };

  async function refreshMetrics(now: number, healthy: boolean) {
    const open = await db<{ rule_key: string; total: number }[]>`
      SELECT rule_key, count(*)::int AS total FROM public.usage_monitor_findings
      WHERE matched AND disposition = 'OPEN'
      GROUP BY rule_key
    `;
    const [work] = await db<{ oldest: string | null; gaps: number }[]>`
      SELECT min(next_attempt_at) FILTER (WHERE next_attempt_at <= ${new Date(now)})::text AS oldest,
        count(*) FILTER (WHERE last_error IS NOT NULL)::int AS gaps
      FROM public.usage_monitor_windows
      WHERE next_attempt_at IS NOT NULL AND (policy_version = ${version} OR replay_requested OR trend_refresh)
    `;
    const [schedule] = await db<{ skipped: string; last_skipped_at: string | null }[]>`
      SELECT skipped_windows::text AS skipped, last_skipped_at::text FROM public.usage_monitor_windows
      WHERE policy_version = ${version} ORDER BY window_start DESC LIMIT 1
    `;
    const oldestAt = work?.oldest ? Date.parse(work.oldest) : 0;
    snapshot = {
      open: new Map(open.map((row) => [row.rule_key, row.total])),
      oldestDueTimestamp: Math.floor(oldestAt / 1000),
      coverageGaps: work?.gaps ?? 0,
      skippedWindows: schedule?.skipped ?? "0",
      lastSkippedTimestamp: schedule?.last_skipped_at
        ? Math.floor(Date.parse(schedule.last_skipped_at) / 1000)
        : 0,
      evaluatedAt: healthy ? Math.floor(now / 1000) : snapshot.evaluatedAt,
    };
  }

  return {
    async runOnce(now = Date.now()) {
      if (running) return "busy";
      running = true;
      let healthy = true;
      try {
        const claim = await claimUsageWindow(db, version, config.policy, now);
        if (!claim) return "idle";
        const { lease, policy } = claim;
        try {
          const samples = await readUsageSamples(db, lease, policy);
          if (!samples) {
            await releaseUsageWindow(db, lease);
            return "stale";
          }
          if (samples.length > policy.max_rows) {
            const retrying = await retryUsageWindow(db, lease, policy, "SCAN_LIMIT", now);
            return retrying ? "limited" : "stale";
          }
          const saved = await saveUsageWindow(db, lease, policy, samples);
          return saved ? "evaluated" : "stale";
        } catch (error) {
          const retrying = await retryUsageWindow(
            db,
            lease,
            policy,
            "EVALUATION_FAILED",
            now,
          ).catch(() => undefined);
          if (retrying === false) return "stale";
          if (retrying === undefined) await releaseUsageWindow(db, lease).catch(() => undefined);
          throw error;
        }
      } catch (error) {
        healthy = false;
        throw error;
      } finally {
        try {
          await refreshMetrics(now, healthy);
        } finally {
          running = false;
        }
      }
    },

    metrics() {
      return [
        "stg_console_usage_monitor_enabled 1",
        `stg_console_usage_monitor_notifications_enabled ${Number(config.notifications)}`,
        `stg_console_usage_monitor_last_tick_timestamp_seconds ${snapshot.evaluatedAt}`,
        `stg_console_usage_monitor_oldest_due_timestamp_seconds ${snapshot.oldestDueTimestamp}`,
        `stg_console_usage_monitor_coverage_gaps ${snapshot.coverageGaps}`,
        `stg_console_usage_monitor_skipped_windows_total ${snapshot.skippedWindows}`,
        `stg_console_usage_monitor_last_skip_timestamp_seconds ${snapshot.lastSkippedTimestamp}`,
        ...usageMonitorRuleKeys.map(
          (rule) =>
            `stg_console_usage_monitor_open_findings{rule="${rule}"} ${snapshot.open.get(rule) ?? 0}`,
        ),
        "",
      ].join("\n");
    },
  };
}
