import { normalizeAndValidateCidr } from "./auth.ts";
import {
  parseUsageMonitorPolicy,
  usageMonitorIntervalMs,
  type UsageMonitorConfig,
} from "./usage-monitor/policy.ts";

export type GatewayRoleTokens = {
  catalog?: string;
  security?: string;
  finance?: string;
  auditor?: string;
  ops?: string;
  events?: string;
};

export type ConsoleConfig = {
  host: string;
  port: number;
  adminHost: string;
  adminPort: number;
  databaseUrl: string;
  dbMaxConnections: number;
  gatewayInternalUrl: string;
  gatewayPublicUrl: string;
  gatewayTokens: GatewayRoleTokens;
  outboxWebhookToken: string;
  outboxConsumerName: string;
  bootstrapAdminEmail?: string;
  bootstrapAdminToken?: string;
  trustedProxyCidrs: string[];
  sessionTtlMs: number;
  refreshTtlMs: number;
  reconcileIntervalMs: number;
  outboxPullIntervalMs: number;
  alertEvaluationIntervalMs?: number;
  alertDeliveryIntervalMs: number;
  allowPrivateAlertWebhooks?: boolean;
  usageMonitor?: UsageMonitorConfig;
  clientCorsOrigins: string[];
  adminCorsOrigins: string[];
};

function intEnv(
  env: Record<string, string | undefined>,
  name: string,
  fallback: number,
  min = 0,
): number {
  const raw = env[name];
  if (raw === undefined || raw === "") return fallback;
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed < min) {
    throw new Error(`${name} must be an integer >= ${min}`);
  }
  return parsed;
}

function parseList(raw: string | undefined): string[] {
  return (raw ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function loadUsageMonitor(env: Record<string, string | undefined>): UsageMonitorConfig | undefined {
  const raw = env.CONSOLE_USAGE_MONITOR_POLICY;
  const notifications = env.CONSOLE_USAGE_MONITOR_NOTIFICATIONS || "false";
  if (notifications !== "true" && notifications !== "false") {
    throw new Error("CONSOLE_USAGE_MONITOR_NOTIFICATIONS must be true or false");
  }
  if (!raw) {
    if (notifications === "true") {
      throw new Error("CONSOLE_USAGE_MONITOR_NOTIFICATIONS requires CONSOLE_USAGE_MONITOR_POLICY");
    }
    return;
  }
  try {
    return {
      policy: parseUsageMonitorPolicy(JSON.parse(raw)),
      notifications: notifications === "true",
      intervalMs: usageMonitorIntervalMs,
    };
  } catch (error) {
    throw new Error("Invalid CONSOLE_USAGE_MONITOR_POLICY", { cause: error });
  }
}

export function loadConsoleConfigFromEnv(
  env: Record<string, string | undefined> = process.env,
): ConsoleConfig {
  const databaseUrl = env.CONSOLE_DATABASE_URL;
  if (!databaseUrl) throw new Error("CONSOLE_DATABASE_URL is required");
  const gatewayInternalUrl = (env.GATEWAY_INTERNAL_URL ?? "http://127.0.0.1:8081").replace(
    /\/+$/,
    "",
  );
  const gatewayPublicUrl = (env.GATEWAY_PUBLIC_URL ?? "http://127.0.0.1:8080").replace(/\/+$/, "");
  const outboxPullIntervalMs = intEnv(env, "CONSOLE_OUTBOX_PULL_INTERVAL_MS", 0, 0);
  const alertDeliveryIntervalMs = intEnv(env, "CONSOLE_ALERT_DELIVERY_INTERVAL_MS", 5_000, 0);

  const gatewayTokens: GatewayRoleTokens = {
    ...(env.GATEWAY_CATALOG_TOKEN ? { catalog: env.GATEWAY_CATALOG_TOKEN } : {}),
    ...(env.GATEWAY_SECURITY_TOKEN ? { security: env.GATEWAY_SECURITY_TOKEN } : {}),
    ...(env.GATEWAY_FINANCE_TOKEN ? { finance: env.GATEWAY_FINANCE_TOKEN } : {}),
    ...(env.GATEWAY_AUDITOR_TOKEN ? { auditor: env.GATEWAY_AUDITOR_TOKEN } : {}),
    ...(env.GATEWAY_OPS_TOKEN ? { ops: env.GATEWAY_OPS_TOKEN } : {}),
    ...(env.GATEWAY_EVENTS_TOKEN ? { events: env.GATEWAY_EVENTS_TOKEN } : {}),
  };
  if (
    !gatewayTokens.catalog ||
    !gatewayTokens.security ||
    !gatewayTokens.finance ||
    !gatewayTokens.auditor ||
    !gatewayTokens.ops
  ) {
    throw new Error("Provide GATEWAY_{CATALOG,SECURITY,FINANCE,AUDITOR,OPS}_TOKEN credentials");
  }
  if (outboxPullIntervalMs > 0 && !gatewayTokens.events) {
    throw new Error("GATEWAY_EVENTS_TOKEN is required when CONSOLE_OUTBOX_PULL_INTERVAL_MS > 0");
  }
  const outboxWebhookToken = env.OUTBOX_WEBHOOK_TOKEN;
  if (!outboxWebhookToken || outboxWebhookToken.length < 16) {
    throw new Error("OUTBOX_WEBHOOK_TOKEN must be at least 16 characters");
  }
  const bootstrapAdminEmail = env.BOOTSTRAP_ADMIN_EMAIL?.trim().toLowerCase() || undefined;
  const bootstrapAdminToken = env.BOOTSTRAP_ADMIN_TOKEN || undefined;
  if (bootstrapAdminEmail && (!bootstrapAdminToken || bootstrapAdminToken.length < 16)) {
    throw new Error(
      "BOOTSTRAP_ADMIN_TOKEN of at least 16 characters is required when BOOTSTRAP_ADMIN_EMAIL is set",
    );
  }

  const trustedProxyCidrs: string[] = [];
  for (const rawCidr of parseList(env.CONSOLE_TRUSTED_PROXY_CIDRS)) {
    const normalized = normalizeAndValidateCidr(rawCidr);
    if (!normalized) {
      throw new Error(`Invalid CIDR in CONSOLE_TRUSTED_PROXY_CIDRS: ${rawCidr}`);
    }
    trustedProxyCidrs.push(normalized);
  }

  const usageMonitor = loadUsageMonitor(env);
  return {
    host: env.CONSOLE_HOST ?? "0.0.0.0",
    port: intEnv(env, "CONSOLE_PORT", 8092, 0),
    adminHost: env.CONSOLE_ADMIN_HOST ?? "127.0.0.1",
    adminPort: intEnv(env, "CONSOLE_ADMIN_PORT", 8093, 0),
    databaseUrl,
    dbMaxConnections: intEnv(env, "CONSOLE_DB_MAX_CONNECTIONS", 16, 2),
    gatewayInternalUrl,
    gatewayPublicUrl,
    gatewayTokens,
    outboxWebhookToken,
    outboxConsumerName: env.OUTBOX_CONSUMER_NAME ?? "console_api",
    ...(bootstrapAdminEmail ? { bootstrapAdminEmail } : {}),
    ...(bootstrapAdminToken ? { bootstrapAdminToken } : {}),
    trustedProxyCidrs,
    sessionTtlMs: intEnv(env, "CONSOLE_SESSION_TTL_MS", 24 * 3_600_000, 60_000),
    refreshTtlMs: intEnv(env, "CONSOLE_REFRESH_TTL_MS", 30 * 24 * 3_600_000, 300_000),
    reconcileIntervalMs: intEnv(env, "CONSOLE_RECONCILE_INTERVAL_MS", 10_000, 0),
    outboxPullIntervalMs,
    alertEvaluationIntervalMs: intEnv(
      env,
      "CONSOLE_ALERT_EVALUATION_INTERVAL_MS",
      alertDeliveryIntervalMs || 5_000,
    ),
    alertDeliveryIntervalMs,
    ...(usageMonitor ? { usageMonitor } : {}),
    clientCorsOrigins: parseList(env.CONSOLE_CLIENT_CORS_ORIGINS),
    adminCorsOrigins: parseList(env.CONSOLE_ADMIN_CORS_ORIGINS),
  };
}
