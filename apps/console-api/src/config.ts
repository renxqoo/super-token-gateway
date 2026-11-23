export type GatewayRoleTokens = {
  admin?: string;
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
  trustProxyHeaders?: boolean;
  sessionTtlMs: number;
  refreshTtlMs: number;
  reconcileIntervalMs: number;
  outboxPullIntervalMs: number;
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

function parseOriginList(raw: string | undefined): string[] {
  return (raw ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
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

  const gatewayTokens: GatewayRoleTokens = {
    ...(env.GATEWAY_ADMIN_TOKEN ? { admin: env.GATEWAY_ADMIN_TOKEN } : {}),
    ...(env.GATEWAY_CATALOG_TOKEN ? { catalog: env.GATEWAY_CATALOG_TOKEN } : {}),
    ...(env.GATEWAY_SECURITY_TOKEN ? { security: env.GATEWAY_SECURITY_TOKEN } : {}),
    ...(env.GATEWAY_FINANCE_TOKEN ? { finance: env.GATEWAY_FINANCE_TOKEN } : {}),
    ...(env.GATEWAY_AUDITOR_TOKEN ? { auditor: env.GATEWAY_AUDITOR_TOKEN } : {}),
    ...(env.GATEWAY_OPS_TOKEN ? { ops: env.GATEWAY_OPS_TOKEN } : {}),
    ...(env.GATEWAY_EVENTS_TOKEN ? { events: env.GATEWAY_EVENTS_TOKEN } : {}),
  };
  if (
    !gatewayTokens.admin &&
    (!gatewayTokens.catalog ||
      !gatewayTokens.security ||
      !gatewayTokens.finance ||
      !gatewayTokens.auditor)
  ) {
    throw new Error(
      "Provide GATEWAY_ADMIN_TOKEN or role-scoped GATEWAY_*_TOKEN credentials for Gateway internal API",
    );
  }
  const outboxWebhookToken = env.OUTBOX_WEBHOOK_TOKEN;
  if (!outboxWebhookToken || outboxWebhookToken.length < 16) {
    throw new Error("OUTBOX_WEBHOOK_TOKEN must be at least 16 characters");
  }
  return {
    host: env.CONSOLE_HOST ?? "0.0.0.0",
    port: intEnv(env, "CONSOLE_PORT", 8090, 0),
    adminHost: env.CONSOLE_ADMIN_HOST ?? "127.0.0.1",
    adminPort: intEnv(env, "CONSOLE_ADMIN_PORT", 8091, 0),
    databaseUrl,
    dbMaxConnections: intEnv(env, "CONSOLE_DB_MAX_CONNECTIONS", 16, 2),
    gatewayInternalUrl,
    gatewayPublicUrl,
    gatewayTokens,
    outboxWebhookToken,
    outboxConsumerName: env.OUTBOX_CONSUMER_NAME ?? "console_api",
    ...(env.BOOTSTRAP_ADMIN_EMAIL
      ? { bootstrapAdminEmail: env.BOOTSTRAP_ADMIN_EMAIL.trim().toLowerCase() }
      : {}),
    ...(env.BOOTSTRAP_ADMIN_TOKEN ? { bootstrapAdminToken: env.BOOTSTRAP_ADMIN_TOKEN } : {}),
    trustProxyHeaders: env.CONSOLE_TRUST_PROXY_HEADERS === "true",
    sessionTtlMs: intEnv(env, "CONSOLE_SESSION_TTL_MS", 24 * 3_600_000, 60_000),
    refreshTtlMs: intEnv(env, "CONSOLE_REFRESH_TTL_MS", 30 * 24 * 3_600_000, 300_000),
    reconcileIntervalMs: intEnv(env, "CONSOLE_RECONCILE_INTERVAL_MS", 10_000, 0),
    outboxPullIntervalMs: intEnv(env, "CONSOLE_OUTBOX_PULL_INTERVAL_MS", 0, 0),
    clientCorsOrigins: parseOriginList(env.CONSOLE_CLIENT_CORS_ORIGINS ?? env.CONSOLE_CORS_ORIGINS),
    adminCorsOrigins: parseOriginList(env.CONSOLE_ADMIN_CORS_ORIGINS),
  };
}
