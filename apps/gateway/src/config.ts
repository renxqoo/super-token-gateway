import { validateCidr } from "@gateway/runtime";

export type GatewayConfig = {
  mode: "all-in-one" | "gateway";
  host: string;
  port: number;
  internalHost: string;
  internalPort: number;
  databaseUrl: string;
  dbMaxConnections: number;
  redisUrl?: string;
  keyPepper: string;
  encryptionKey: string;
  adminToken: string;
  allowPrivateUpstreams: boolean;
  privateUpstreamCidrs: string[];
  trustedProxyCidrs: string[];
  maxInFlight: number;
  maxBodyBytes: number;
  holdTtlMs: number;
  attemptTimeoutMs: number;
  snapshotPollMs: number;
  microBatchWindowMs: number;
  microBatchMaxSize: number;
  wasmLibPath?: string;
  pluginSigningPublicKeyPem?: string;
  businessWebhookUrl?: string;
  businessWebhookToken?: string;
};

function parseCsv(value: string | undefined): string[] {
  if (!value) return [];
  return value
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean)
    .map((v) => validateCidr(v));
}

function parseMode(
  env: Record<string, string | undefined>,
  argv: readonly string[],
): "all-in-one" | "gateway" {
  const flag = argv.find((a) => a.startsWith("--mode="));
  const raw = flag ? flag.slice("--mode=".length) : (env.GATEWAY_MODE ?? "all-in-one");
  if (raw === "all-in-one" || raw === "gateway") return raw;
  throw new Error(`Invalid GATEWAY_MODE: ${raw}`);
}

export function loadConfigFromEnv(
  env: Record<string, string | undefined> = process.env,
  argv: readonly string[] = process.argv.slice(2),
): GatewayConfig {
  const databaseUrl = env.DATABASE_URL;
  if (!databaseUrl) throw new Error("DATABASE_URL is required");
  const keyPepper = env.KEY_PEPPER;
  if (!keyPepper || keyPepper.length < 16) {
    throw new Error("KEY_PEPPER must be at least 16 characters");
  }
  const encryptionKey = env.ENCRYPTION_KEY;
  if (!encryptionKey || !/^[0-9a-fA-F]{64}$/.test(encryptionKey)) {
    throw new Error("ENCRYPTION_KEY must be a 64-character hex string");
  }
  const adminToken = env.ADMIN_TOKEN;
  if (!adminToken || adminToken.length < 16) {
    throw new Error("ADMIN_TOKEN must be at least 16 characters");
  }
  const mode = parseMode(env, argv);
  return {
    mode,
    host: env.GATEWAY_HOST ?? "0.0.0.0",
    port: Number(env.GATEWAY_PORT ?? 8080),
    internalHost: env.INTERNAL_HOST ?? "127.0.0.1",
    internalPort: Number(env.INTERNAL_PORT ?? 8081),
    databaseUrl,
    dbMaxConnections: Number(env.DB_MAX_CONNECTIONS ?? 32),
    ...(env.REDIS_URL ? { redisUrl: env.REDIS_URL } : {}),
    keyPepper,
    encryptionKey,
    adminToken,
    allowPrivateUpstreams: env.ALLOW_PRIVATE_UPSTREAMS === "true",
    privateUpstreamCidrs: parseCsv(env.PRIVATE_UPSTREAM_CIDRS),
    trustedProxyCidrs: parseCsv(env.TRUSTED_PROXY_CIDRS),
    maxInFlight: Number(env.MAX_IN_FLIGHT ?? 2000),
    maxBodyBytes: Number(env.MAX_BODY_BYTES ?? 10 * 1024 * 1024),
    holdTtlMs: Number(env.HOLD_TTL_MS ?? 60_000),
    attemptTimeoutMs: Number(env.ATTEMPT_TIMEOUT_MS ?? 30_000),
    snapshotPollMs: Number(env.SNAPSHOT_POLL_MS ?? 2000),
    microBatchWindowMs: Number(env.MICRO_BATCH_WINDOW_MS ?? 3),
    microBatchMaxSize: Number(env.MICRO_BATCH_MAX_SIZE ?? 64),
    ...(env.STG_WASMTIME_LIB ? { wasmLibPath: env.STG_WASMTIME_LIB } : {}),
    ...(env.PLUGIN_SIGNING_PUBLIC_KEY_PEM
      ? { pluginSigningPublicKeyPem: env.PLUGIN_SIGNING_PUBLIC_KEY_PEM }
      : {}),
    ...(env.BUSINESS_WEBHOOK_URL ? { businessWebhookUrl: env.BUSINESS_WEBHOOK_URL } : {}),
    ...(env.BUSINESS_WEBHOOK_TOKEN ? { businessWebhookToken: env.BUSINESS_WEBHOOK_TOKEN } : {}),
  };
}
