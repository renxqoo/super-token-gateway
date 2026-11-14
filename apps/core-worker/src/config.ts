import { z } from "zod";
import { coreAudiencePattern } from "@gateway/core-client";

const coreFields = [
  "CORE_ORIGIN",
  "CORE_SERVERNAME",
  "CORE_AUDIENCE",
  "CORE_PEER_SHA256",
  "CORE_SUBJECT_ALTNAME",
  "CORE_KEY_FILE",
  "CORE_CERT_FILE",
  "CORE_CA_FILE",
] as const;

const envSchema = z
  .object({
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
    RECONCILE_INTERVAL_MS: z.coerce.number().int().min(50).max(60000).default(1000),
    RECONCILE_BATCH: z.coerce.number().int().min(1).max(256).default(32),
    RECOVERY_INTERVAL_MS: z.coerce.number().int().min(50).max(60000).default(5000),
    RECOVERY_BATCH: z.coerce.number().int().min(1).max(100).default(32),
    CORE_ORIGIN: z.string().url().optional(),
    CORE_SERVERNAME: z.string().min(1).optional(),
    CORE_AUDIENCE: z.string().regex(coreAudiencePattern).optional(),
    CORE_PEER_SHA256: z
      .string()
      .regex(/^[a-f0-9]{64}$/)
      .optional(),
    CORE_SUBJECT_ALTNAME: z.string().min(1).max(2000).optional(),
    CORE_KEY_FILE: z.string().min(1).optional(),
    CORE_CERT_FILE: z.string().min(1).optional(),
    CORE_CA_FILE: z.string().min(1).optional(),
  })
  .superRefine((value, context) => {
    const present = coreFields.filter((field) => value[field] !== undefined);
    if (present.length === coreFields.length) return;
    if (present.length > 0 || value.NODE_ENV === "production")
      for (const field of coreFields)
        if (value[field] === undefined)
          context.addIssue({
            code: "custom",
            path: [field],
            message: "Core recovery client field required",
          });
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
