import { CONTRACT_MAX_P99_MS, passesOnlineCapacity } from "./benchmark-lib.ts";

export function referenceOptions(args: string[]) {
  const allowed = new Set([
    "--binary",
    "--wallets",
    "--rate",
    "--seconds",
    "--trust-quota-usd",
    "--pre-consume-multiplier",
    "--target-rps",
    "--concurrency",
    "--redis-server",
    "--output",
  ]);
  const values = new Map<string, string>();
  for (let i = 0; i < args.length; i += 2) {
    const key = args[i]!,
      value = args[i + 1];
    if (!allowed.has(key) || value === undefined || value.startsWith("--") || values.has(key))
      throw new Error(`Invalid reference argument: ${key}`);
    values.set(key, value);
  }
  const integer = (key: string, fallback: number, max: number) => {
    const value = Number(values.get(key) ?? fallback);
    if (!Number.isSafeInteger(value) || value < 1 || value > max) throw new Error(`Invalid ${key}`);
    return value;
  };
  const binary = values.get("--binary");
  if (!binary) throw new Error("--binary is required");
  const rate = integer("--rate", 3000, 100000);
  const trust = values.has("--trust-quota-usd")
    ? Number(values.get("--trust-quota-usd"))
    : undefined;
  if (trust !== undefined && (!Number.isFinite(trust) || trust < 0 || trust > 1e9))
    throw new Error("Invalid --trust-quota-usd");
  const multiplier = values.has("--pre-consume-multiplier")
    ? Number(values.get("--pre-consume-multiplier"))
    : undefined;
  if (
    multiplier !== undefined &&
    (!Number.isFinite(multiplier) || multiplier <= 0 || multiplier > 1e6)
  )
    throw new Error("Invalid --pre-consume-multiplier");
  return {
    binary,
    wallets: integer("--wallets", 1000, 1000),
    rate,
    seconds: integer("--seconds", 60, 3600),
    targetRps: integer("--target-rps", rate, 100000),
    concurrency: integer("--concurrency", 2048, 8192),
    trustQuotaUsd: trust,
    preConsumeMultiplier: multiplier,
    redisServer: values.get("--redis-server") ?? ".local/toolchain/redis74/bin/redis-server",
    output: values.get("--output"),
  };
}

export type ReferenceAccounting = {
  logs: number;
  log_quota: number;
  users_used_quota: number;
  tokens_used_quota: number;
  channel_used_quota: number;
  user_requests: number;
  invalid_logs: number;
  wallet_mismatches: number;
};
export type ReferenceHttp = {
  succeeded: number;
  completed: number;
  offered: number;
  shed: number;
  invalid: number;
  warmup_requests: number;
  warmup_concurrency: number;
  duration_seconds: number;
  latency_ms: { p99: number };
  latency_windows: Record<string, { p99: number }>;
  minute_windows: Record<string, { succeeded: number }>;
  wallet_calls: number[];
  measurement_started_at: string;
};

export function referenceVerdict(
  http: ReferenceHttp,
  accounting: ReferenceAccounting,
  rate: number,
  seconds: number,
  target: number,
  drainMs: number,
  positivePreConsumption?: {
    trusted_requests: number;
    positive_preconsume_requests: number;
    zero_preconsumed_settlements: number;
  },
) {
  const requests = http.succeeded + http.warmup_requests;
  const quota = requests * 2;
  const failures: string[] = [];
  if (
    http.offered !== rate * seconds ||
    http.completed !== rate * seconds ||
    http.shed !== 0 ||
    http.invalid !== 0 ||
    http.succeeded !== http.completed
  )
    failures.push("http_or_planned_arrivals");
  if (
    accounting.logs !== requests ||
    accounting.user_requests !== requests ||
    accounting.invalid_logs !== 0 ||
    accounting.wallet_mismatches !== 0 ||
    accounting.log_quota !== quota ||
    accounting.users_used_quota !== quota ||
    accounting.tokens_used_quota !== quota ||
    accounting.channel_used_quota !== quota
  )
    failures.push("accounting");
  if (
    positivePreConsumption &&
    (positivePreConsumption.positive_preconsume_requests !== requests ||
      positivePreConsumption.trusted_requests !== 0 ||
      positivePreConsumption.zero_preconsumed_settlements !== 0)
  )
    failures.push("positive_preconsume_not_proven");
  if (drainMs > 120000) failures.push("drain_deadline");
  const valid = failures.length === 0;
  const minutes = Array.from(
    { length: Math.floor(seconds / 60) },
    (_, i) => http.minute_windows[String(i)]?.succeeded ?? 0,
  );
  return {
    valid,
    failures,
    expected_requests: requests,
    expected_quota: quota,
    quota_per_request: 2,
    quota_equivalent_requests: {
      logs: accounting.log_quota / 2,
      users: accounting.users_used_quota / 2,
      tokens: accounting.tokens_used_quota / 2,
      channel: accounting.channel_used_quota / 2,
    },
    capacityPassed: passesOnlineCapacity(
      valid,
      http,
      target,
      seconds,
      minutes,
      CONTRACT_MAX_P99_MS,
      drainMs,
    ),
  };
}

export async function auditReferencePreConsumption(path: string) {
  let trusted_requests = 0,
    positive_preconsume_requests = 0,
    zero_preconsumed_settlements = 0;
  let buffered = "";
  function line(value: string) {
    if (value.includes("trusted without pre-consume")) trusted_requests++;
    if (value.includes("needs pre-consume")) positive_preconsume_requests++;
    if (value.includes("pre-consumed ＄0.000000)")) zero_preconsumed_settlements++;
  }
  for await (const chunk of Bun.file(path).stream().pipeThrough(new TextDecoderStream())) {
    buffered += chunk;
    const lines = buffered.split("\n");
    buffered = lines.pop()!;
    for (const value of lines) line(value);
    if (buffered.length > 1048576) throw new Error("Reference log line exceeds audit budget");
  }
  if (buffered) line(buffered);
  return {
    trusted_requests,
    positive_preconsume_requests,
    zero_preconsumed_settlements,
    source: "new-api billing_session.go preConsume and billing.go SettleBilling log branches",
  };
}
