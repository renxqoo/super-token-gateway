import { z } from "zod";
import { validateCidr } from "@gateway/runtime";
const configSchema = z.strictObject({
  database_url: z.string().url(),
  execution: z
    .strictObject({
      pepper: z.string().min(1),
      encryptionKey: z.string().regex(/^[0-9a-f]{64}$/),
      redisUrls: z.array(z.string().url()).min(1).max(8),
      quotaNamespace: z.string(),
      quotaGeneration: z.string(),
      maxAttempts: z.number().int().min(1).max(5).optional(),
    })
    .optional(),
  terminal_settlement: z
    .strictObject({
      database_url: z.url({ protocol: /^postgres(ql)?$/ }),
      standby_url: z.url({ protocol: /^postgres(ql)?$/ }),
      commit_short_chat_sse: z.boolean().optional(),
      short_chat_funding_epoch: z.number().int().positive().max(Number.MAX_SAFE_INTEGER).optional(),
      recovery_queue: z.strictObject({
        url: z.url({ protocol: /^rediss?$/ }),
        namespace: z.string().regex(/^[a-zA-Z0-9:_-]{1,80}$/),
        required_replicas: z.number().int().min(1).max(5),
        ack_timeout_ms: z.number().int().min(100).max(1000),
      }),
    })
    .optional(),
  audience: z.string(),
  hostname: z.string(),
  servername: z.string(),
  port: z.number().int().min(0).max(65535),
  key_file: z.string(),
  cert_file: z.string(),
  ca_file: z.string(),
  client_key_file: z.string(),
  client_cert_file: z.string(),
  challenge_ttl_seconds: z.number().int().min(1).max(300).optional(),
  session_ttl_seconds: z.number().int().min(1).max(86400).optional(),
  limits: z
    .strictObject({
      bodyBytes: z.number().int().positive().max(8192),
      responseBytes: z.number().int().min(512).max(8192),
      headerBytes: z.number().int().positive().max(16384),
      headerCount: z.number().int().positive().max(32),
      connections: z.number().int().positive().max(128),
      connectionsPerPeer: z.number().int().positive().max(4),
      streams: z.number().int().positive().max(256),
      streamsPerSession: z.number().int().positive().max(32),
      receiveBytes: z.number().int().positive().max(16777216),
      headersMs: z.number().int().positive().max(1000),
      bodyMs: z.number().int().positive().max(2000),
      progressMs: z.number().int().positive().max(1000),
      processingMs: z.number().int().positive().max(5000),
      idleMs: z.number().int().positive().max(30000),
      lifetimeMs: z.number().int().positive().max(300000),
      drainMs: z.number().int().positive().max(1000),
    })
    .partial()
    .transform((value) =>
      Object.fromEntries(Object.entries(value).filter(([, item]) => item !== undefined)),
    )
    .optional(),
  admin: z
    .strictObject({
      database_url: z.string().url(),
      hostname: z.string(),
      port: z.number().int().min(0).max(65535),
      admin_key: z.string().min(32),
      max_body_bytes: z.number().int().positive().max(67108864).optional(),
      shutdown_grace_ms: z.number().int().min(100).max(120000).optional(),
      queue: z
        .strictObject({
          url: z.string().startsWith("redis"),
          namespace: z.string().regex(/^[a-zA-Z0-9:_-]{1,80}$/),
          required_replicas: z.number().int().min(0).max(5),
          ack_timeout_ms: z.number().int().min(100).max(10000).default(2000),
        })
        .optional(),
      plugins: z
        .strictObject({
          release_policy: z.string().min(1),
          release_directory: z.string().min(1),
          helper_executable: z.string().min(1),
        })
        .optional(),
    })
    .optional(),
  public: z
    .strictObject({
      database_url: z.string().url(),
      hostname: z.string(),
      port: z.number().int().min(0).max(65535),
      trusted_proxy_cidrs: z.array(z.string().max(64)).max(64).default([]),
      shutdown_grace_ms: z.number().int().min(100).max(120000).optional(),
    })
    .optional(),
});

export type CoreApiConfig = z.infer<typeof configSchema>;

export async function loadCoreApiConfig(
  path = process.env.CORE_API_CONFIG_FILE,
): Promise<CoreApiConfig> {
  if (!path) throw new Error("CORE_API_CONFIG_FILE required");
  const config = configSchema.parse(await Bun.file(path).json());
  if (config.admin && !config.execution)
    throw new Error("Core API admin listener requires the execution configuration");
  if (config.public && !config.execution)
    throw new Error("Core API public listener requires the execution configuration");
  if (config.terminal_settlement && !config.execution)
    throw new Error("Core API terminal settlement requires the execution configuration");
  for (const cidr of config.public?.trusted_proxy_cidrs ?? []) validateCidr(cidr);
  return config;
}
