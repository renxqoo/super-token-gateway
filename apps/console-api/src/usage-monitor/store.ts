import type { SQL, TransactionSQL } from "bun";
import { parseUsageMonitorPolicy, scopeKey, type UsageMonitorPolicy } from "./policy.ts";
import { evaluateUsageWindow, type HistoricalWindow, type UsageMonitorSample } from "./rules.ts";

type Phase = "INITIAL" | "FINAL";
type Lease = {
  policy_version: string;
  window_start: string;
  phase: Phase;
  lease_token: string;
};
type Claim = { lease: Lease; policy: UsageMonitorPolicy };

const retryDelayMs = 60_000;
const usageMonitorLock = 72194032;
const usageMonitorScanLock = 72194033;

function trendDependencies(policy: UsageMonitorPolicy) {
  return policy.scopes.flatMap((scope) =>
    scope.trend
      ? [
          {
            scope_key: scopeKey(scope),
            lookback_ms:
              (scope.trend.baseline_windows + scope.trend.consecutive_windows - 1) *
              policy.window_ms,
          },
        ]
      : [],
  );
}

export async function lockUsageMonitorWrites(tx: TransactionSQL) {
  await tx`SELECT pg_advisory_xact_lock(${usageMonitorLock})`;
}

export async function suspendUsageTrends(
  tx: TransactionSQL,
  version: string,
  starts: string[],
  policy: UsageMonitorPolicy,
  includeSource = true,
) {
  const dependencies = trendDependencies(policy);
  if (!dependencies.length) return;
  const horizon = Math.max(...dependencies.map(({ lookback_ms }) => lookback_ms));
  await tx`
    WITH sources AS (SELECT start FROM unnest(${starts}::timestamptz[]) AS s(start))
    UPDATE public.usage_monitor_findings f
    SET matched = false, revision = f.revision + 1, observed_at = clock_timestamp()
    WHERE f.policy_version = ${version} AND f.matched AND f.rule_key IN ('TREND_INPUT', 'TREND_OUTPUT')
      AND f.window_start BETWEEN
        (SELECT min(start) FROM sources)
        AND (SELECT max(start) FROM sources) + ${horizon}::int * interval '1 ms'
      AND EXISTS (
        SELECT 1
        FROM sources s
        CROSS JOIN jsonb_to_recordset(${JSON.stringify(dependencies)}::text::jsonb)
          AS d(scope_key text, lookback_ms integer)
        WHERE d.scope_key = f.scope_key
          AND f.window_start BETWEEN s.start AND s.start + d.lookback_ms * interval '1 ms'
          AND (${includeSource} OR f.window_start > s.start)
      )
  `;
}

export async function claimUsageWindow(
  db: SQL,
  activeVersion: string,
  policy: UsageMonitorPolicy,
  now: number,
): Promise<Claim | undefined> {
  const width = policy.window_ms;
  const newest = Math.floor((now - policy.initial_grace_ms) / width) * width - width;
  const oldest = Math.max(
    Date.parse(policy.effective_from),
    newest - (policy.catchup_windows - 1) * width,
  );
  return db.begin(async (tx) => {
    const [lock] = await tx`SELECT pg_try_advisory_xact_lock(${usageMonitorLock}) AS held`;
    if (!lock?.held) return;
    if (oldest <= newest) {
      const [latest] = await tx<
        { window_start: string; skipped_windows: string; last_skipped_at: string | null }[]
      >`
        SELECT window_start::text, skipped_windows::text, last_skipped_at::text FROM public.usage_monitor_windows
        WHERE policy_version = ${activeVersion}
        ORDER BY window_start DESC LIMIT 1
      `;
      const expected = latest
        ? Date.parse(latest.window_start) + width
        : Date.parse(policy.effective_from);
      const start = Math.max(oldest, expected);
      const skipped = BigInt(latest?.skipped_windows ?? "0") + BigInt((start - expected) / width);
      const lastSkipped =
        start > expected ? new Date(now).toISOString() : (latest?.last_skipped_at ?? null);
      if (start <= newest) {
        await tx`
          INSERT INTO public.usage_monitor_windows
            (policy_version, window_start, policy, phase, final_at, next_attempt_at, skipped_windows, last_skipped_at)
          SELECT ${activeVersion}, starts.at, ${JSON.stringify(policy)}::text::jsonb, 'INITIAL',
            starts.at + ${width + policy.final_grace_ms}::int * interval '1 ms',
            starts.at + ${width + policy.initial_grace_ms}::int * interval '1 ms',
            ${skipped.toString()}::bigint, ${lastSkipped}::timestamptz
          FROM generate_series(${new Date(start)}::timestamptz, ${new Date(newest)}::timestamptz,
            ${width}::int * interval '1 ms') AS starts(at)
          ON CONFLICT DO NOTHING
        `;
      }
    }
    const token = crypto.randomUUID();
    const [row] = await tx<(Lease & { policy: unknown })[]>`
      WITH active_leases AS (
        SELECT lease_superseded FROM public.usage_monitor_windows
        WHERE next_attempt_at IS NOT NULL AND leased_until > clock_timestamp()
      ), candidate AS (
        SELECT due.policy_version, due.window_start FROM public.usage_monitor_windows due
        WHERE (due.policy_version = ${activeVersion} OR due.replay_requested OR due.trend_refresh)
          AND due.next_attempt_at <= ${new Date(now)}
          AND (due.leased_until IS NULL OR due.leased_until <= clock_timestamp())
          AND NOT EXISTS (SELECT 1 FROM active_leases WHERE NOT lease_superseded)
          AND (SELECT count(*) FROM active_leases) < 2
          AND (NOT due.replay_requested OR NOT EXISTS (
            SELECT 1 FROM public.usage_monitor_windows earlier
            WHERE earlier.policy_version = due.policy_version AND earlier.window_start < due.window_start
              AND earlier.next_attempt_at IS NOT NULL AND earlier.replay_requested
          ))
        ORDER BY (due.replay_requested OR due.trend_refresh)
            AND due.next_attempt_at > statement_timestamp() - interval '60 seconds',
          due.next_attempt_at, due.window_start, due.policy_version
        LIMIT 1 FOR UPDATE SKIP LOCKED
      )
      UPDATE public.usage_monitor_windows w
      SET phase = CASE WHEN w.final_at <= ${new Date(now)} THEN 'FINAL' ELSE w.phase END,
        lease_token = ${token}, leased_until = clock_timestamp() + interval '30 seconds', lease_superseded = false
      FROM candidate c WHERE w.policy_version = c.policy_version AND w.window_start = c.window_start
      RETURNING w.policy_version, w.window_start::text, w.phase, w.lease_token::text, w.policy
    `;
    if (!row) return;
    const { policy: snapshot, ...lease } = row;
    return { lease, policy: parseUsageMonitorPolicy(snapshot) };
  });
}

export async function readUsageSamples(
  db: SQL,
  lease: Lease,
  policy: UsageMonitorPolicy,
): Promise<UsageMonitorSample[] | undefined> {
  const start = new Date(lease.window_start);
  const end = new Date(start.getTime() + policy.window_ms);
  const scopes = policy.scopes.map(({ channel_id, model, operation, currency }) => ({
    channel_id,
    model,
    operation,
    currency,
    scope_key: scopeKey({ channel_id, model, operation, currency }),
  }));
  return db.begin("ISOLATION LEVEL REPEATABLE READ READ ONLY", async (tx) => {
    await tx`
      SELECT set_config('statement_timeout', '3s', true), set_config('transaction_timeout', '5s', true)
    `;
    const [scan] = await tx<{ held: boolean }[]>`
      SELECT pg_try_advisory_xact_lock(
        ${usageMonitorScanLock}::int, hashtext(${`${lease.policy_version}:${start.getTime()}`})
      ) AS held
      FROM public.usage_monitor_windows
      WHERE policy_version = ${lease.policy_version} AND window_start = ${start}
        AND phase = ${lease.phase} AND lease_token = ${lease.lease_token}::uuid
        AND leased_until > clock_timestamp() AND NOT lease_superseded
    `;
    if (!scan?.held) return;
    return tx<UsageMonitorSample[]>`
      WITH source AS MATERIALIZED (
        SELECT s.scope_key, e.event_id, e.request_id, e.charge_id, e.customer_atoms, e.payload
        FROM jsonb_to_recordset(${JSON.stringify(scopes)}::text::jsonb)
          AS s(channel_id uuid, model text, operation text, currency text, scope_key text)
        CROSS JOIN LATERAL (
          SELECT event_id, request_id, charge_id, customer_atoms,
            CASE WHEN octet_length(payload::text) <= 65536 THEN payload END AS payload
          FROM public.usage_events
          WHERE kind = 'billing.settled' AND outcome IN ('SETTLED', 'DEBT')
            AND channel_id = s.channel_id AND model = s.model
            AND octet_length(model) <= 640 AND octet_length(operation) <= 32 AND octet_length(currency) <= 8
            AND operation = s.operation AND currency = s.currency
            AND occurred_at >= ${start} AND occurred_at < ${end}
          ORDER BY occurred_at, event_id
          LIMIT ${policy.max_rows + 1}
        ) e
        LIMIT ${policy.max_rows + 1}
      ),
      decoded AS (
        SELECT *, CASE WHEN jsonb_typeof(payload) = 'object' THEN payload
          WHEN jsonb_typeof(payload) = 'string' AND pg_input_is_valid(payload #>> '{}', 'jsonb')
          THEN (payload #>> '{}')::jsonb END AS document
        FROM source
      ),
      dimensions AS (
        SELECT *,
          document -> 'usage' AS usage,
          jsonb_typeof(document) IS DISTINCT FROM 'object' AS invalid_document,
          document #> '{usage,input_tokens}' AS input_value,
          document #> '{usage,output_tokens}' AS output_value
        FROM decoded
      ),
      quantities AS (
        SELECT *,
          CASE WHEN jsonb_typeof(input_value) = 'string' AND input_value #>> '{}' ~ '^(0|[1-9][0-9]{0,37})$'
            THEN input_value #>> '{}' END AS input_tokens,
          CASE WHEN jsonb_typeof(output_value) = 'string' AND output_value #>> '{}' ~ '^(0|[1-9][0-9]{0,37})$'
            THEN output_value #>> '{}' END AS output_tokens
        FROM dimensions
      )
      SELECT scope_key, event_id::text, request_id::text, charge_id::text, customer_atoms::text,
        input_tokens, output_tokens,
        invalid_document OR (usage IS NOT NULL AND jsonb_typeof(usage) <> 'object')
          OR (input_value IS NOT NULL AND input_tokens IS NULL) AS invalid_input,
        invalid_document OR (usage IS NOT NULL AND jsonb_typeof(usage) <> 'object')
          OR (output_value IS NOT NULL AND output_tokens IS NULL) AS invalid_output
      FROM quantities
    `;
  });
}

export async function releaseUsageWindow(db: SQL | TransactionSQL, lease: Lease) {
  await db`
    UPDATE public.usage_monitor_windows
    SET lease_token = NULL, leased_until = NULL, lease_superseded = false
    WHERE policy_version = ${lease.policy_version} AND window_start = ${lease.window_start}::timestamptz
      AND phase = ${lease.phase} AND lease_token = ${lease.lease_token}::uuid
  `;
}

export async function saveUsageWindow(
  db: SQL,
  lease: Lease,
  policy: UsageMonitorPolicy,
  samples: UsageMonitorSample[],
): Promise<boolean> {
  const start = Date.parse(lease.window_start);
  const horizon = Math.max(0, ...trendDependencies(policy).map(({ lookback_ms }) => lookback_ms));
  return db.begin(async (tx) => {
    await lockUsageMonitorWrites(tx);
    const [owned] = await tx<{ available: boolean }[]>`
      SELECT finalized_at IS NOT NULL AND NOT replay_requested AND last_error IS NULL AS available
      FROM public.usage_monitor_windows
      WHERE policy_version = ${lease.policy_version} AND window_start = ${lease.window_start}::timestamptz
        AND phase = ${lease.phase} AND lease_token = ${lease.lease_token}::uuid
        AND leased_until > clock_timestamp() AND NOT lease_superseded FOR UPDATE
    `;
    if (!owned) {
      await releaseUsageWindow(tx, lease);
      return false;
    }
    const history =
      horizon === 0
        ? []
        : await tx<HistoricalWindow[]>`
      SELECT w.window_start::text, true AS final, jsonb_agg(s.evaluated) AS stats
      FROM public.usage_monitor_windows w
      JOIN public.usage_monitor_window_stats s USING (policy_version, window_start)
      WHERE w.policy_version = ${lease.policy_version} AND w.finalized_at IS NOT NULL
        AND NOT w.replay_requested AND w.last_error IS NULL
        AND w.window_start >= ${new Date(start - horizon)} AND w.window_start < ${new Date(start)}
      GROUP BY w.window_start
    `;
    const result = evaluateUsageWindow(policy, start, samples, history);
    const changedStats = await tx`
      INSERT INTO public.usage_monitor_window_stats AS stored (policy_version, window_start, scope_key, evaluated)
      SELECT ${lease.policy_version}, ${new Date(start)}, value->>'scope_key', value
      FROM jsonb_array_elements(${JSON.stringify(result.stats)}::text::jsonb)
      ON CONFLICT (policy_version, window_start, scope_key)
      DO UPDATE SET evaluated = EXCLUDED.evaluated
      WHERE stored.evaluated IS DISTINCT FROM EXCLUDED.evaluated
      RETURNING 1
    `;
    const final = lease.phase === "FINAL";
    if (final && horizon > 0 && (!owned.available || changedStats.length > 0)) {
      if (owned.available) {
        await suspendUsageTrends(tx, lease.policy_version, [lease.window_start], policy, false);
      }
      await tx`
        UPDATE public.usage_monitor_windows
        SET trend_refresh = true, next_attempt_at = statement_timestamp()
        WHERE policy_version = ${lease.policy_version} AND finalized_at IS NOT NULL
          AND next_attempt_at IS NULL AND window_start > ${new Date(start)}
          AND window_start <= ${new Date(start + horizon)}
      `;
    }
    const matches = JSON.stringify(
      result.findings.map((finding) => ({ ...finding, id: crypto.randomUUID() })),
    );
    await tx`
      UPDATE public.usage_monitor_findings f
      SET matched = false, revision = f.revision + 1, observed_at = clock_timestamp()
      WHERE f.policy_version = ${lease.policy_version} AND f.window_start = ${lease.window_start}::timestamptz
        AND f.matched AND NOT EXISTS (
          SELECT 1 FROM jsonb_to_recordset(${matches}::text::jsonb) AS m(scope_key text, rule_key text)
          WHERE m.scope_key = f.scope_key AND m.rule_key = f.rule_key
        )
    `;
    await tx`
      INSERT INTO public.usage_monitor_findings AS old
        (id, policy_version, window_start, scope_key, rule_key, channel_id, model, operation, currency, evidence)
      SELECT m.id, ${lease.policy_version}, ${lease.window_start}::timestamptz, m.scope_key, m.rule_key,
        m.channel_id, m.model, m.operation, m.currency, m.evidence
      FROM jsonb_to_recordset(${matches}::text::jsonb)
        AS m(id uuid, scope_key text, rule_key text, channel_id uuid, model text, operation text, currency text, evidence jsonb)
      ON CONFLICT (policy_version, window_start, scope_key, rule_key) DO UPDATE
      SET evidence = EXCLUDED.evidence, matched = true, revision = old.revision + 1, observed_at = clock_timestamp()
      WHERE NOT old.matched OR old.evidence IS DISTINCT FROM EXCLUDED.evidence
    `;
    await tx`
      UPDATE public.usage_monitor_windows
      SET phase = 'FINAL', next_attempt_at = CASE WHEN ${final} THEN NULL ELSE final_at END,
        finalized_at = CASE WHEN ${final} THEN clock_timestamp() ELSE finalized_at END,
        replay_requested = false, trend_refresh = false, leased_until = NULL, lease_token = NULL, last_error = NULL, failed_attempts = 0,
        coverage = ${JSON.stringify({ samples: result.stats.reduce((count, entry) => count + entry.samples, 0) })}::text::jsonb
      WHERE policy_version = ${lease.policy_version} AND window_start = ${lease.window_start}::timestamptz
    `;
    return true;
  });
}

export async function retryUsageWindow(
  db: SQL,
  lease: Lease,
  policy: UsageMonitorPolicy,
  reason: "SCAN_LIMIT" | "EVALUATION_FAILED",
  now: number,
) {
  return db.begin(async (tx) => {
    await lockUsageMonitorWrites(tx);
    const updated = await tx`
      UPDATE public.usage_monitor_windows
      SET lease_token = NULL, leased_until = NULL, last_error = ${reason},
        failed_attempts = failed_attempts + 1, next_attempt_at = ${new Date(now + retryDelayMs)}
      WHERE policy_version = ${lease.policy_version} AND window_start = ${lease.window_start}::timestamptz
        AND phase = ${lease.phase} AND lease_token = ${lease.lease_token}::uuid
        AND leased_until > clock_timestamp() AND NOT lease_superseded
      RETURNING 1
    `;
    if (!updated.length) {
      await releaseUsageWindow(tx, lease);
      return false;
    }
    await suspendUsageTrends(tx, lease.policy_version, [lease.window_start], policy);
    return true;
  });
}
