import { scopeKey, type UsageMonitorPolicy, type UsageMonitorScope } from "./policy.ts";

export const usageMonitorRuleKeys = [
  "MAX_INPUT",
  "MAX_OUTPUT",
  "MAX_CHARGE",
  "TREND_INPUT",
  "TREND_OUTPUT",
] as const;
type RuleKey = (typeof usageMonitorRuleKeys)[number];

export type UsageMonitorSample = {
  scope_key: string;
  event_id: string;
  request_id: string | null;
  charge_id: string | null;
  customer_atoms: string;
  input_tokens: string | null;
  output_tokens: string | null;
  invalid_input: boolean;
  invalid_output: boolean;
};

type SampleReference = Pick<UsageMonitorSample, "event_id" | "request_id" | "charge_id"> & {
  value: string;
};
export type TokenStats = {
  measured: number;
  missing: number;
  invalid: number;
  p95: string | null;
  max: string | null;
};
export type ScopeStats = {
  scope_key: string;
  samples: number;
  customer_atoms: string;
  input: TokenStats;
  output: TokenStats;
};

export type Finding = {
  scope_key: string;
  rule_key: RuleKey;
  channel_id: string;
  model: string;
  operation: string;
  currency: string;
  evidence: {
    observed: string;
    reference: string;
    sample_count: number;
    samples: SampleReference[];
  };
};

export type HistoricalWindow = {
  window_start: string;
  final: boolean;
  stats: ScopeStats[];
};

const sampleLimit = 5;

function compareInt(left: bigint, right: bigint) {
  return left < right ? -1 : Number(left > right);
}

function metricSamples(
  samples: UsageMonitorSample[],
  field: "input_tokens" | "output_tokens" | "customer_atoms",
) {
  return samples.flatMap((sample) => {
    const value = sample[field];
    return value === null ? [] : [{ sample, value: BigInt(value) }];
  });
}

function summarizeTokens(
  samples: UsageMonitorSample[],
  field: "input_tokens" | "output_tokens",
  invalidField: "invalid_input" | "invalid_output",
): TokenStats {
  const sorted = metricSamples(samples, field)
    .map((record) => record.value)
    .sort(compareInt);
  const invalid = samples.filter((sample) => sample[invalidField]).length;
  const measured = sorted.length;
  return {
    measured,
    invalid,
    missing: samples.length - measured - invalid,
    p95: sorted[Math.ceil(measured * 0.95) - 1]?.toString() ?? null,
    max: sorted.at(-1)?.toString() ?? null,
  };
}

function evidenceSamples(sorted: ReturnType<typeof metricSamples>): SampleReference[] {
  return sorted.slice(0, sampleLimit).map(({ sample, value }) => ({
    event_id: sample.event_id,
    request_id: sample.request_id,
    charge_id: sample.charge_id,
    value: value.toString(),
  }));
}

function largest(records: ReturnType<typeof metricSamples>) {
  return records.sort(
    (a, b) => compareInt(b.value, a.value) || a.sample.event_id.localeCompare(b.sample.event_id),
  );
}

function finding(
  scope: UsageMonitorScope,
  key: string,
  rule: RuleKey,
  evidence: Finding["evidence"],
): Finding {
  return {
    scope_key: key,
    rule_key: rule,
    channel_id: scope.channel_id,
    model: scope.model,
    operation: scope.operation,
    currency: scope.currency,
    evidence,
  };
}

function tokenTrend(
  config: NonNullable<UsageMonitorScope["trend"]>,
  current: ScopeStats,
  key: "input" | "output",
  recent: HistoricalWindow[],
  from: number,
  width: number,
): bigint | undefined {
  const history = new Map(recent.map((row) => [Date.parse(row.window_start), row]));
  const value = (stats: ScopeStats | undefined) => {
    const metric = stats?.[key];
    return metric && metric.p95 !== null && metric.measured >= config.minimum_samples
      ? BigInt(metric.p95)
      : undefined;
  };
  const streak = [value(current)];
  const base: bigint[] = [];
  for (let i = 1; i < config.consecutive_windows + config.baseline_windows; i++) {
    const previous = history.get(from - i * width);
    const measured = previous?.final
      ? value(previous.stats.find((entry) => entry.scope_key === current.scope_key))
      : undefined;
    if (measured === undefined) return;
    if (i < config.consecutive_windows) streak.push(measured);
    else base.push(measured);
  }
  base.sort(compareInt);
  const median = base[Math.ceil(base.length / 2) - 1]!;
  if (median === 0n) return;
  const pass = streak.every(
    (v) =>
      v !== undefined &&
      v * 10_000n >= median * BigInt(10_000 + config.increase_bps) &&
      v - median >= BigInt(config.minimum_delta),
  );
  return pass ? median : undefined;
}

export function evaluateUsageWindow(
  policy: UsageMonitorPolicy,
  windowStart: number,
  samples: UsageMonitorSample[],
  history: HistoricalWindow[],
) {
  const grouped = new Map<string, UsageMonitorSample[]>();
  for (const sample of samples) {
    const group = grouped.get(sample.scope_key);
    if (group) group.push(sample);
    else grouped.set(sample.scope_key, [sample]);
  }
  const stats: ScopeStats[] = [];
  const findings: Finding[] = [];
  for (const scope of policy.scopes) {
    const key = scopeKey(scope);
    const group = grouped.get(key) ?? [];
    const entry: ScopeStats = {
      scope_key: key,
      samples: group.length,
      customer_atoms: group
        .reduce((value, sample) => value + BigInt(sample.customer_atoms), 0n)
        .toString(),
      input: summarizeTokens(group, "input_tokens", "invalid_input"),
      output: summarizeTokens(group, "output_tokens", "invalid_output"),
    };
    stats.push(entry);
    for (const [rule, field, limit] of [
      ["MAX_INPUT", "input_tokens", scope.max_input_tokens],
      ["MAX_OUTPUT", "output_tokens", scope.max_output_tokens],
      ["MAX_CHARGE", "customer_atoms", scope.max_customer_atoms],
    ] as const) {
      if (limit === undefined) continue;
      const bound = BigInt(limit);
      const matches = largest(metricSamples(group, field).filter(({ value }) => value > bound));
      if (!matches.length) continue;
      findings.push(
        finding(scope, key, rule, {
          observed: matches[0]!.value.toString(),
          reference: limit,
          sample_count: matches.length,
          samples: evidenceSamples(matches),
        }),
      );
    }
    if (!scope.trend) continue;
    for (const [dimension, field, rule] of [
      ["input", "input_tokens", "TREND_INPUT"],
      ["output", "output_tokens", "TREND_OUTPUT"],
    ] as const) {
      const baseline = tokenTrend(
        scope.trend,
        entry,
        dimension,
        history,
        windowStart,
        policy.window_ms,
      );
      if (baseline === undefined) continue;
      findings.push(
        finding(scope, key, rule, {
          observed: entry[dimension].p95!,
          reference: baseline.toString(),
          sample_count: entry[dimension].measured,
          samples: evidenceSamples(largest(metricSamples(group, field))),
        }),
      );
    }
  }
  return { stats, findings };
}
