import { validateCidr } from "@gateway/runtime";
import { z } from "zod";

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  DATABASE_URL: z.string().startsWith("postgres"),
  REDIS_URL: z.string().startsWith("redis"),
  QUEUE_REDIS_URL: z.string().startsWith("redis"),
  QUEUE_REQUIRED_REPLICAS: z.coerce.number().int().min(0).max(5).default(0),
  QUEUE_ACK_TIMEOUT_MS: z.coerce.number().int().min(100).max(10000).default(2000),
  QUEUE_BATCH_SIZE: z.coerce.number().int().min(1).max(128).default(64),
  QUEUE_BATCH_DELAY_MS: z.coerce.number().int().min(0).max(10).default(1),
  QUEUE_MAX_PENDING_JOBS: z.coerce.number().int().min(1).max(100000).default(1000),
  ADMIN_KEY: z.string().min(32),
  KEY_PEPPER: z.string().min(32),
  ENCRYPTION_KEY: z.string().regex(/^[a-fA-F0-9]{64}$/),
  PORT: z.coerce.number().int().min(1).max(65535).default(8088),
  HOST: z.string().default("127.0.0.1"),
  WORKER_PORT: z.coerce.number().int().min(1).max(65535).default(8099),
  REDIS_NAMESPACE: z
    .string()
    .regex(/^[a-zA-Z0-9:_-]{1,80}$/)
    .default("stg"),
  ALLOW_PRIVATE_UPSTREAM: z
    .enum(["true", "false"])
    .default("false")
    .transform((v) => v === "true"),
  TRUSTED_PROXY_CIDRS: z
    .string()
    .default("")
    .transform((v) =>
      v
        .split(",")
        .map((v) => v.trim())
        .filter(Boolean),
    ),
  PRIVATE_UPSTREAM_CIDRS: z
    .string()
    .default("")
    .transform((v) =>
      v
        .split(",")
        .map((v) => v.trim())
        .filter(Boolean),
    ),
  MAX_INFLIGHT: z.coerce.number().int().min(1).max(100000).default(10000),
  MAX_BUFFERED_BYTES: z.coerce.number().int().min(1048576).default(134217728),
  MAX_ATTEMPTS: z.coerce.number().int().min(1).max(5).default(3),
  TOTAL_TIMEOUT_MS: z.coerce.number().int().min(100).max(3_600_000).default(180000),
  HEADERS_TIMEOUT_MS: z.coerce.number().int().min(100).default(30000),
  STREAM_IDLE_MS: z.coerce.number().int().min(100).default(30000),
  DOWNSTREAM_STALL_MS: z.coerce.number().int().min(100).default(30000),
  MAX_BODY_BYTES: z.coerce.number().int().min(1024).default(16_777_216),
  MAX_RESPONSE_BYTES: z.coerce.number().int().min(1024).default(33_554_432),
  MAX_EVENT_BYTES: z.coerce.number().int().min(1024).default(1_048_576),
  SHUTDOWN_GRACE_MS: z.coerce.number().int().min(100).default(20000),
  BILLING_CONCURRENCY: z.coerce.number().int().min(1).max(64).default(8),
  DB_POOL_SIZE: z.coerce.number().int().min(1).max(200).default(24),
});
export type Config = z.infer<typeof envSchema>;
export function loadConfig(env: Record<string, string | undefined> = process.env): Config {
  const result = envSchema.safeParse(env);
  if (!result.success)
    throw new Error(
      `Invalid configuration fields: ${result.error.issues.map((i) => i.path.join(".")).join(", ")}`,
    );
  if (result.data.NODE_ENV === "production" && result.data.ALLOW_PRIVATE_UPSTREAM)
    throw new Error("Production forbids private upstream bypass");
  for (const cidr of [...result.data.TRUSTED_PROXY_CIDRS, ...result.data.PRIVATE_UPSTREAM_CIDRS])
    validateCidr(cidr);
  if (result.data.NODE_ENV === "production" && result.data.QUEUE_REQUIRED_REPLICAS < 1)
    throw new Error("Production requires an acknowledged AOF replica for the usage queue");
  if (result.data.REDIS_URL === result.data.QUEUE_REDIS_URL)
    throw new Error("Queue Redis and admission Redis must be separate");
  return result.data;
}
