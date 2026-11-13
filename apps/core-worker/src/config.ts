import { z } from "zod";

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  DATABASE_URL: z.string().startsWith("postgres"),
  DB_POOL_SIZE: z.coerce.number().int().min(1).max(200).default(8),
  QUEUE_REDIS_URL: z.string().startsWith("redis"),
  QUEUE_ACK_TIMEOUT_MS: z.coerce.number().int().min(100).max(10000).default(2000),
  REDIS_NAMESPACE: z
    .string()
    .regex(/^[a-zA-Z0-9:_-]{1,80}$/)
    .default("stg"),
  SETTLEMENT_CONCURRENCY: z.coerce.number().int().min(1).max(64).default(8),
  HOST: z.string().default("127.0.0.1"),
  WORKER_PORT: z.coerce.number().int().min(0).max(65535).default(8099),
  SHUTDOWN_GRACE_MS: z.coerce.number().int().min(100).default(20000),
});

export type CoreWorkerConfig = z.infer<typeof envSchema>;

export function loadCoreWorkerConfig(
  env: Record<string, string | undefined> = process.env,
): CoreWorkerConfig {
  const result = envSchema.safeParse(env);
  if (!result.success)
    throw new Error(
      `Invalid configuration fields: ${result.error.issues.map((i) => i.path.join(".")).join(", ")}`,
    );
  return result.data;
}
