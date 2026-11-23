import { createPublicKey } from "node:crypto";
import { validateCidr } from "@gateway/runtime";
import { loadInternalCredentials, type InternalCredentials } from "./internal-auth.ts";

export type GatewayConfig = {
  mode: "all-in-one" | "gateway";
  host: string;
  port: number;
  internalHost: string;
  internalPort: number;
  databaseUrl: string;
  dbMaxConnections: number;
  snapshotDbMaxConnections?: number;
  redisUrl?: string;
  redisNamespace?: string;
  redisSentinel?: { urls: string[]; master: string };
  accessLog?: boolean;
  privateMetrics?: boolean;
  requireDbRoles?: boolean;
  keyPepper: string;
  encryptionKey: string;
  adminToken?: string;
  internalCredentials?: InternalCredentials;
  internalDatabaseUrl?: string;
  internalDbMaxConnections?: number;
  allowPrivateUpstreams: boolean;
  privateUpstreamCidrs: string[];
  trustedProxyCidrs: string[];
  maxInFlight: number;
  maxBodyBytes: number;
  bodyReadTimeoutMs?: number;
  reusePort?: boolean;
  holdTtlMs: number;
  attemptTimeoutMs: number;
  firstByteTimeoutMs?: number;
  maxAmbiguousRetriesPerMinute?: number;
  streamIdleTimeoutMs?: number;
  streamMaxDurationMs?: number;
  snapshotPollMs: number;
  microBatchWindowMs: number;
  microBatchMaxSize: number;
  wasmLibPath?: string;
  contentWasmPoolSize?: number;
  providerWasmPoolSize?: number;
  pluginSigningPublicKeyPem?: string;
  platformAbsorbMaxCountPerMinute?: number;
  platformAbsorbMaxAtomsPerMinute?: string;
  businessWebhookUrl?: string;
  businessWebhookToken?: string;
  maxActiveAsyncTasksPerProject?: number;
  maxAutoRefundsPerProject24h?: number;
  asyncTaskCancelCooldownMs?: number;
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

function intFromEnv(
  env: Record<string, string | undefined>,
  name: string,
  fallback: number,
  min = 1,
  max = 2_147_483_647,
): number {
  const raw = env[name];
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!/^\d+$/.test(raw) || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }
  return value;
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
  const internalCredentials = env.INTERNAL_AUTH_FILE
    ? loadInternalCredentials(env.INTERNAL_AUTH_FILE)
    : undefined;
  const adminToken = internalCredentials ? undefined : env.ADMIN_TOKEN;
  if (!internalCredentials && (!adminToken || adminToken.length < 16)) {
    throw new Error("ADMIN_TOKEN must be at least 16 characters");
  }
  const mode = parseMode(env, argv);
  const production = env.NODE_ENV === "production";
  const sentinelUrls = env.REDIS_SENTINEL_URLS?.split(",").map((url) => url.trim());
  const sentinelMaster = env.REDIS_SENTINEL_MASTER;
  if (sentinelUrls || sentinelMaster !== undefined) {
    if (
      !env.REDIS_URL ||
      !sentinelUrls?.length ||
      sentinelUrls.some((url) => !/^rediss?:\/\//.test(url)) ||
      new Set(sentinelUrls.map((url) => new URL(url).host)).size !== sentinelUrls.length ||
      (production && sentinelUrls.length < 3) ||
      !sentinelMaster ||
      !/^[a-zA-Z0-9_.-]{1,128}$/.test(sentinelMaster)
    ) {
      throw new Error(
        "Redis Sentinel requires REDIS_URL, REDIS_SENTINEL_URLS and REDIS_SENTINEL_MASTER",
      );
    }
  }
  if (
    production &&
    (!internalCredentials || !env.INTERNAL_DATABASE_URL || mode !== "gateway" || !env.REDIS_URL)
  ) {
    throw new Error(
      "Production requires standalone Gateway, INTERNAL_AUTH_FILE, INTERNAL_DATABASE_URL and REDIS_URL",
    );
  }
  const signingKeyPem = env.PLUGIN_SIGNING_PUBLIC_KEY_PEM?.trim();
  if (production && !signingKeyPem) {
    throw new Error("Production requires PLUGIN_SIGNING_PUBLIC_KEY_PEM");
  }
  if (signingKeyPem) {
    try {
      const pub = createPublicKey(signingKeyPem);
      if (pub.asymmetricKeyType !== "ed25519") {
        throw new Error("PLUGIN_SIGNING_PUBLIC_KEY_PEM must be an Ed25519 public key");
      }
    } catch (error) {
      if (error instanceof Error && error.message.includes("Ed25519")) throw error;
      throw new Error("PLUGIN_SIGNING_PUBLIC_KEY_PEM is not a valid public key");
    }
  }
  if (env.INTERNAL_DATABASE_URL) {
    const dataUrl = new URL(databaseUrl);
    const controlUrl = new URL(env.INTERNAL_DATABASE_URL);
    if (
      dataUrl.host !== controlUrl.host ||
      dataUrl.pathname !== controlUrl.pathname ||
      dataUrl.username === controlUrl.username
    ) {
      throw new Error(
        "Use distinct data and control PostgreSQL logins for the same ledger database",
      );
    }
  }
  const absorbAtoms = env.PLATFORM_ABSORB_MAX_ATOMS_PER_MINUTE?.trim();
  if (absorbAtoms !== undefined && !/^[1-9][0-9]{0,37}$/.test(absorbAtoms)) {
    throw new Error("PLATFORM_ABSORB_MAX_ATOMS_PER_MINUTE must be a positive integer");
  }
  return {
    mode,
    host: env.GATEWAY_HOST ?? "0.0.0.0",
    port: intFromEnv(env, "GATEWAY_PORT", 8080, 0, 65535),
    internalHost: env.INTERNAL_HOST ?? "127.0.0.1",
    internalPort: intFromEnv(env, "INTERNAL_PORT", 8081, 0, 65535),
    databaseUrl,
    dbMaxConnections: intFromEnv(env, "DB_MAX_CONNECTIONS", 32, mode === "all-in-one" ? 2 : 1),
    snapshotDbMaxConnections: intFromEnv(env, "SNAPSHOT_DB_MAX_CONNECTIONS", 2, 1, 16),
    ...(env.REDIS_URL ? { redisUrl: env.REDIS_URL } : {}),
    ...(env.REDIS_NAMESPACE ? { redisNamespace: env.REDIS_NAMESPACE } : {}),
    ...(sentinelUrls && sentinelMaster
      ? { redisSentinel: { urls: sentinelUrls, master: sentinelMaster } }
      : {}),
    accessLog: env.ACCESS_LOG !== "false",
    privateMetrics: production || env.PRIVATE_METRICS === "true",
    requireDbRoles: production,
    keyPepper,
    encryptionKey,
    ...(adminToken ? { adminToken } : {}),
    ...(internalCredentials ? { internalCredentials } : {}),
    ...(env.INTERNAL_DATABASE_URL
      ? {
          internalDatabaseUrl: env.INTERNAL_DATABASE_URL,
          internalDbMaxConnections: intFromEnv(env, "INTERNAL_DB_MAX_CONNECTIONS", 4),
        }
      : {}),
    allowPrivateUpstreams: env.ALLOW_PRIVATE_UPSTREAMS === "true",
    privateUpstreamCidrs: parseCsv(env.PRIVATE_UPSTREAM_CIDRS),
    trustedProxyCidrs: parseCsv(env.TRUSTED_PROXY_CIDRS),
    maxInFlight: intFromEnv(env, "MAX_IN_FLIGHT", 2000),
    maxBodyBytes: intFromEnv(env, "MAX_BODY_BYTES", 10 * 1024 * 1024),
    ...(env.BODY_READ_TIMEOUT_MS !== undefined
      ? { bodyReadTimeoutMs: intFromEnv(env, "BODY_READ_TIMEOUT_MS", 15_000) }
      : {}),
    ...(env.GATEWAY_REUSE_PORT === "true" ? { reusePort: true } : {}),
    holdTtlMs: intFromEnv(env, "HOLD_TTL_MS", 60_000),
    attemptTimeoutMs: intFromEnv(env, "ATTEMPT_TIMEOUT_MS", 30_000),
    ...(env.FIRST_BYTE_TIMEOUT_MS !== undefined
      ? { firstByteTimeoutMs: intFromEnv(env, "FIRST_BYTE_TIMEOUT_MS", 15_000) }
      : {}),
    ...(env.MAX_AMBIGUOUS_RETRIES_PER_MINUTE !== undefined
      ? {
          maxAmbiguousRetriesPerMinute: intFromEnv(env, "MAX_AMBIGUOUS_RETRIES_PER_MINUTE", 60, 0),
        }
      : {}),
    ...(env.STREAM_IDLE_TIMEOUT_MS !== undefined
      ? { streamIdleTimeoutMs: intFromEnv(env, "STREAM_IDLE_TIMEOUT_MS", 30_000) }
      : {}),
    streamMaxDurationMs: intFromEnv(env, "STREAM_MAX_DURATION_MS", 300_000),
    snapshotPollMs: intFromEnv(env, "SNAPSHOT_POLL_MS", 2000, production ? 1 : 0),
    microBatchWindowMs: intFromEnv(env, "MICRO_BATCH_WINDOW_MS", 3, 0),
    microBatchMaxSize: intFromEnv(env, "MICRO_BATCH_MAX_SIZE", 256),
    ...(env.STG_WASMTIME_LIB ? { wasmLibPath: env.STG_WASMTIME_LIB } : {}),
    contentWasmPoolSize: intFromEnv(env, "CONTENT_WASM_POOL_SIZE", 4, 1, 16),
    providerWasmPoolSize: intFromEnv(env, "PROVIDER_WASM_POOL_SIZE", 4, 1, 16),
    ...(signingKeyPem ? { pluginSigningPublicKeyPem: signingKeyPem } : {}),
    platformAbsorbMaxCountPerMinute: intFromEnv(
      env,
      "PLATFORM_ABSORB_MAX_COUNT_PER_MINUTE",
      100,
      1,
    ),
    ...(absorbAtoms ? { platformAbsorbMaxAtomsPerMinute: absorbAtoms } : {}),
    ...(env.BUSINESS_WEBHOOK_URL ? { businessWebhookUrl: env.BUSINESS_WEBHOOK_URL } : {}),
    ...(env.BUSINESS_WEBHOOK_TOKEN ? { businessWebhookToken: env.BUSINESS_WEBHOOK_TOKEN } : {}),
    maxActiveAsyncTasksPerProject: intFromEnv(
      env,
      "MAX_ACTIVE_ASYNC_TASKS_PER_PROJECT",
      10,
      1,
      1000,
    ),
    maxAutoRefundsPerProject24h: intFromEnv(env, "MAX_AUTO_REFUNDS_PER_PROJECT_24H", 5, 0, 1000),
    asyncTaskCancelCooldownMs: intFromEnv(
      env,
      "ASYNC_TASK_CANCEL_COOLDOWN_MS",
      60_000,
      0,
      86_400_000,
    ),
  };
}
