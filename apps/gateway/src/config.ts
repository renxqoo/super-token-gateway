import { createPublicKey } from "node:crypto";
import { assertProductionPostgresTransport } from "@gateway/ledger";
import { validateCidr, type TransportOptions } from "@gateway/runtime";
import { loadInternalCredentials, type InternalCredentials } from "./internal-auth.ts";
import type { TraceExportConfig } from "./trace-queue.ts";

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
  traceMetrics?: boolean;
  traceExport?: TraceExportConfig;
  routingObservationModels?: ReadonlySet<string>;
  privateMetrics?: boolean;
  requireDbRoles?: boolean;
  keyPepper: string;
  encryptionKey: string;
  internalCredentials?: InternalCredentials;
  internalDatabaseUrl?: string;
  internalDbMaxConnections?: number;
  allowPrivateUpstreams: boolean;
  privateUpstreamCidrs: string[];
  trustedProxyCidrs: string[];
  maxInFlight: number;
  ingressIp?: {
    mode: "observe" | "enforce";
    windowMs: number;
    maxRequests: number;
    nodeMaxRequests: number;
    maxPending: number;
    timeoutMs: number;
  };
  ingressPressure?: {
    mode: "observe" | "enforce";
    sampleMs: number;
    staleMs: number;
    highSamples: number;
    recoverySamples: number;
    cpuHigh: number;
    cpuRecovery: number;
    memoryHigh: number;
    memoryRecovery: number;
    memoryCritical: number;
  };
  ingressBodyBufferBytes?: number;
  upstreamDial?: TransportOptions["dial"];
  maxBodyBytes: number;
  bodyReadTimeoutMs?: number;
  reusePort?: boolean;
  holdTtlMs: number;
  attemptTimeoutMs: number;
  firstByteTimeoutMs?: number;
  unaryClientAbortGraceMs?: number;
  streamEstimatedSettlement?: boolean;
  maxAmbiguousRetriesPerMinute?: number;
  streamIdleTimeoutMs?: number;
  streamMaxDurationMs?: number;
  snapshotPollMs: number;
  microBatchWindowMs: number;
  microBatchMaxSize: number;
  walletLeaseIdleReleaseMs?: number;
  wasmLibPath?: string;
  contentWasmPoolSize?: number;
  providerWasmPoolSize?: number;
  pluginSigningPublicKeyPem?: string;
  platformAbsorbMaxCountPerMinute?: number;
  platformAbsorbMaxAtomsPerMinute?: string;
  businessWebhookUrl?: string;
  businessWebhookToken?: string;
  maxActiveAsyncTasksPerBillingAccount?: number;
  maxAutoRefundsPerBillingAccount24h?: number;
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

function loadIngressConfig(env: Record<string, string | undefined>, maxBodyBytes: number) {
  const required = (name: string, max = 2_147_483_647) => {
    if (env[name] === undefined)
      throw new Error(`${name} is required when ingress protection is enabled`);
    return intFromEnv(env, name, 0, 1, max);
  };
  function mode(name: string): "observe" | "enforce" | undefined {
    const value = env[name];
    if (value === undefined || value === "off") return undefined;
    if (value === "observe" || value === "enforce") return value;
    throw new Error(`${name} must be off, observe or enforce`);
  }
  const ipMode = mode("INGRESS_IP_MODE");
  const pressureMode = mode("INGRESS_PRESSURE_MODE");
  if (ipMode && !env.REDIS_URL) throw new Error("Ingress IP quota requires REDIS_URL");
  const ingressIp = ipMode && {
    mode: ipMode,
    windowMs: required("INGRESS_IP_WINDOW_MS"),
    maxRequests: required("INGRESS_IP_MAX_REQUESTS"),
    nodeMaxRequests: required("INGRESS_IP_NODE_MAX_REQUESTS"),
    maxPending: required("INGRESS_IP_MAX_PENDING"),
    timeoutMs: required("INGRESS_IP_TIMEOUT_MS"),
  };
  const ingressPressure = pressureMode && {
    mode: pressureMode,
    sampleMs: required("INGRESS_PRESSURE_SAMPLE_MS"),
    staleMs: required("INGRESS_PRESSURE_STALE_MS"),
    highSamples: required("INGRESS_PRESSURE_HIGH_SAMPLES"),
    recoverySamples: required("INGRESS_PRESSURE_RECOVERY_SAMPLES"),
    cpuHigh: required("INGRESS_CPU_HIGH_PERCENT", 100),
    cpuRecovery: required("INGRESS_CPU_RECOVERY_PERCENT", 100),
    memoryHigh: required("INGRESS_MEMORY_HIGH_PERCENT", 100),
    memoryRecovery: required("INGRESS_MEMORY_RECOVERY_PERCENT", 100),
    memoryCritical: required("INGRESS_MEMORY_CRITICAL_PERCENT", 100),
  };
  if (
    ingressPressure &&
    (ingressPressure.staleMs <= ingressPressure.sampleMs ||
      ingressPressure.cpuRecovery >= ingressPressure.cpuHigh ||
      ingressPressure.memoryRecovery >= ingressPressure.memoryHigh ||
      ingressPressure.memoryHigh >= ingressPressure.memoryCritical)
  ) {
    throw new Error("Ingress pressure thresholds or sampling intervals are out of order");
  }
  return {
    ...(ingressIp ? { ingressIp } : {}),
    ...(ingressPressure ? { ingressPressure } : {}),
    ...(env.INGRESS_BODY_BUFFER_BYTES !== undefined
      ? {
          ingressBodyBufferBytes: intFromEnv(
            env,
            "INGRESS_BODY_BUFFER_BYTES",
            0,
            maxBodyBytes,
            Number.MAX_SAFE_INTEGER,
          ),
        }
      : {}),
  };
}

function loadTraceExport(
  env: Record<string, string | undefined>,
  ledgerUrl: string,
): TraceExportConfig | undefined {
  if (!env.TRACE_DATABASE_URL) return undefined;
  const url = new URL(env.TRACE_DATABASE_URL);
  if (
    !/^postgres(?:ql)?:$/.test(url.protocol) ||
    !url.hostname ||
    url.pathname.length < 2 ||
    decodeURIComponent(url.pathname) === decodeURIComponent(new URL(ledgerUrl).pathname)
  ) {
    throw new Error("TRACE_DATABASE_URL must reference a separate PostgreSQL database");
  }
  const required = (name: string, min = 1, max = 2_147_483_647) => {
    if (env[name] === undefined) throw new Error(`${name} is required with TRACE_DATABASE_URL`);
    return intFromEnv(env, name, 0, min, max);
  };
  const maxRecords = required("TRACE_QUEUE_RECORDS", 2);
  return {
    databaseUrl: url.toString(),
    sampleBasisPoints: required("TRACE_SAMPLE_BPS", 0, 10_000),
    slowFirstTextMs: required("TRACE_SLOW_FIRST_TEXT_MS"),
    maxAttempts: required("TRACE_ATTEMPTS_PER_REQUEST", 2, maxRecords),
    errorBodyMaxBytes: intFromEnv(env, "TRACE_ERROR_BODY_BYTES", 8192, 0, 65_536),
    maxRecords,
    maxBytes: required("TRACE_QUEUE_BYTES"),
    maxPerBillingAccount: required("TRACE_QUEUE_PER_BILLING_ACCOUNT", 1, maxRecords),
    priorityReserve: required("TRACE_QUEUE_PRIORITY_RESERVE", 1, maxRecords - 1),
    batchSize: required("TRACE_BATCH_RECORDS", 1, maxRecords),
    batchDeadlineMs: required("TRACE_BATCH_DEADLINE_MS"),
    flushMs: required("TRACE_FLUSH_MS"),
    timeoutMs: required("TRACE_DB_TIMEOUT_MS"),
    retryLimit: required("TRACE_RETRY_LIMIT", 0),
    retentionSeconds: required("TRACE_RETENTION_SECONDS"),
    cleanupIntervalMs: required("TRACE_CLEANUP_INTERVAL_MS"),
    cleanupBatch: required("TRACE_CLEANUP_BATCH_SPANS"),
  };
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
  if (!env.INTERNAL_AUTH_FILE) throw new Error("INTERNAL_AUTH_FILE is required");
  const internalCredentials = loadInternalCredentials(env.INTERNAL_AUTH_FILE);
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
  if (production && (!env.INTERNAL_DATABASE_URL || mode !== "gateway" || !env.REDIS_URL)) {
    throw new Error(
      "Production requires standalone Gateway, INTERNAL_AUTH_FILE, INTERNAL_DATABASE_URL and REDIS_URL",
    );
  }
  if (production) {
    assertProductionPostgresTransport(databaseUrl, env.NODE_TLS_REJECT_UNAUTHORIZED);
    if (env.INTERNAL_DATABASE_URL) {
      assertProductionPostgresTransport(
        env.INTERNAL_DATABASE_URL,
        env.NODE_TLS_REJECT_UNAUTHORIZED,
      );
    }
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
  const maxBodyBytes = intFromEnv(env, "MAX_BODY_BYTES", 10 * 1024 * 1024);
  const traceExport = loadTraceExport(env, databaseUrl);
  const routingObservationModels = new Set(
    env.ROUTING_OBSERVATION_MODELS?.split(",")
      .map((name) => name.trim())
      .filter(Boolean),
  );
  if (
    routingObservationModels.size > 16 ||
    [...routingObservationModels].some((name) => !/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,149}$/.test(name))
  ) {
    throw new Error("ROUTING_OBSERVATION_MODELS must list at most 16 valid model names");
  }
  if (
    env.TRACE_METRICS !== undefined &&
    env.TRACE_METRICS !== "true" &&
    env.TRACE_METRICS !== "false"
  )
    throw new Error("TRACE_METRICS must be true or false");
  if (
    env.STREAM_ESTIMATED_SETTLEMENT !== undefined &&
    env.STREAM_ESTIMATED_SETTLEMENT !== "true" &&
    env.STREAM_ESTIMATED_SETTLEMENT !== "false"
  )
    throw new Error("STREAM_ESTIMATED_SETTLEMENT must be true or false");
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
    traceMetrics: env.TRACE_METRICS !== "false",
    ...(traceExport ? { traceExport } : {}),
    ...(routingObservationModels.size ? { routingObservationModels } : {}),
    privateMetrics: production || env.PRIVATE_METRICS === "true",
    requireDbRoles: production,
    keyPepper,
    encryptionKey,
    internalCredentials,
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
    upstreamDial: {
      concurrency: intFromEnv(env, "UPSTREAM_DIAL_CONCURRENCY", 16, 1, 65_535),
      startsPerSecond: intFromEnv(env, "UPSTREAM_DIAL_RPS", 2000, 1, 65_535),
      maxQueueWaitMs: intFromEnv(env, "UPSTREAM_DIAL_QUEUE_WAIT_MS", 10_000),
      maxQueueBytes: intFromEnv(env, "UPSTREAM_DIAL_QUEUE_BYTES", 64 * 1024 * 1024),
    },
    maxBodyBytes,
    ...loadIngressConfig(env, maxBodyBytes),
    ...(env.BODY_READ_TIMEOUT_MS !== undefined
      ? { bodyReadTimeoutMs: intFromEnv(env, "BODY_READ_TIMEOUT_MS", 15_000) }
      : {}),
    ...(env.GATEWAY_REUSE_PORT === "true" ? { reusePort: true } : {}),
    holdTtlMs: intFromEnv(env, "HOLD_TTL_MS", 60_000),
    attemptTimeoutMs: intFromEnv(env, "ATTEMPT_TIMEOUT_MS", 30_000),
    ...(env.FIRST_BYTE_TIMEOUT_MS !== undefined
      ? { firstByteTimeoutMs: intFromEnv(env, "FIRST_BYTE_TIMEOUT_MS", 15_000) }
      : {}),
    unaryClientAbortGraceMs: intFromEnv(env, "UNARY_CLIENT_ABORT_GRACE_MS", 600_000, 0),
    streamEstimatedSettlement: env.STREAM_ESTIMATED_SETTLEMENT !== "false",
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
    walletLeaseIdleReleaseMs: intFromEnv(env, "WALLET_LEASE_IDLE_RELEASE_MS", 2500, 0),
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
    maxActiveAsyncTasksPerBillingAccount: intFromEnv(
      env,
      "MAX_ACTIVE_ASYNC_TASKS_PER_BILLING_ACCOUNT",
      10,
      1,
      1000,
    ),
    maxAutoRefundsPerBillingAccount24h: intFromEnv(
      env,
      "MAX_AUTO_REFUNDS_PER_BILLING_ACCOUNT_24H",
      5,
      0,
      1000,
    ),
    asyncTaskCancelCooldownMs: intFromEnv(
      env,
      "ASYNC_TASK_CANCEL_COOLDOWN_MS",
      60_000,
      0,
      86_400_000,
    ),
  };
}
