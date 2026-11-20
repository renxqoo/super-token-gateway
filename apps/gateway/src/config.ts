import { z } from "zod";
import { coreAudiencePattern } from "@gateway/core-client";
import { validateCidr } from "@gateway/runtime";

const cidrs = z.array(z.string().max(64)).max(64).default([]);
const queueSchema = z.strictObject({
  url: z.string().startsWith("redis"),
  namespace: z.string().regex(/^[a-zA-Z0-9:_-]{1,80}$/),
  required_replicas: z.number().int().min(0).max(5),
  ack_timeout_ms: z.number().int().min(100).max(10000).default(2000),
});
const coreTargetSchema = z.strictObject({
  origin: z.string().url(),
  servername: z.string().min(1),
  peer_sha256: z.string().regex(/^[a-f0-9]{64}$/),
  subject_altname: z.string().min(1).max(2000),
});
const configSchema = z.strictObject({
  hostname: z.string().min(1),
  port: z.number().int().min(0).max(65535),
  max_body_bytes: z
    .number()
    .int()
    .positive()
    .max(16 << 20)
    .default(16 << 20),
  max_response_bytes: z
    .number()
    .int()
    .positive()
    .max(64 << 20)
    .default(32 << 20),
  total_timeout_ms: z.number().int().min(1000).max(600000).default(180000),
  upstream_header_timeout_ms: z.number().int().min(100).max(600000).default(30000),
  upstream_idle_timeout_ms: z.number().int().min(100).max(600000).default(30000),
  downstream_stall_timeout_ms: z.number().int().min(100).max(600000).default(30000),
  max_inflight: z.number().int().positive().max(100000).default(1024),
  quota_renew_ms: z.number().int().min(100).max(20000).default(10000),
  resource_poll_ms: z.number().int().min(100).max(600000).default(5000),
  shutdown_grace_ms: z.number().int().min(100).max(120000).default(20000),
  trusted_proxy_cidrs: cidrs,
  allow_private_upstream: z.boolean().default(false),
  private_upstream_cidrs: cidrs,
  upstream_pool_size: z.number().int().positive().max(65536).default(1024),
  commit_short_chat: z.boolean().default(false),
  commit_short_chat_sse: z.boolean().optional(),
  core: coreTargetSchema.extend({
    audience: z.string().regex(coreAudiencePattern),
    short_chat_peer: coreTargetSchema
      .extend({
        funding_group_parity: z.union([z.literal(0), z.literal(1)]),
      })
      .optional(),
    key_file: z.string().min(1),
    cert_file: z.string().min(1),
    ca_file: z.string().min(1),
    connections: z.number().int().min(1).max(4).default(1),
  }),
  queue: queueSchema,
  terminal_recovery_queue: queueSchema
    .extend({
      required_replicas: z.number().int().min(1).max(5),
      ack_timeout_ms: z.number().int().min(100).max(1000).default(1000),
    })
    .optional(),
  plugins: z.strictObject({
    helper_executable: z.string().min(1),
    trust_policy: z.string().min(1),
    max_concurrent_attempts: z.number().int().min(1).max(1024).default(128),
  }),
  production: z.boolean().default(false),
});

export type GatewayConfig = z.infer<typeof configSchema>;

export async function loadGatewayConfig(
  path = process.env.GATEWAY_CONFIG_FILE,
): Promise<GatewayConfig> {
  if (!path) throw new Error("GATEWAY_CONFIG_FILE required");
  const config = configSchema.parse(await Bun.file(path).json());
  if (config.core.short_chat_peer && (!config.commit_short_chat || config.core.connections !== 4))
    throw new Error("Project Core routing requires short Chat and four connections");
  if (config.commit_short_chat_sse && !config.commit_short_chat)
    throw new Error("SSE terminal settlement requires committed short Chat");
  if (config.commit_short_chat && !config.terminal_recovery_queue)
    throw new Error("Short Chat requires terminal_recovery_queue");
  if (config.commit_short_chat && config.queue.required_replicas < 1)
    throw new Error("Short Chat requires an acknowledged evidence queue replica");
  for (const cidr of [...config.trusted_proxy_cidrs, ...config.private_upstream_cidrs])
    validateCidr(cidr);
  if (config.production && config.allow_private_upstream)
    throw new Error("Production forbids private upstream bypass");
  if (config.production && config.queue.required_replicas < 1)
    throw new Error("Production requires an acknowledged AOF replica for evidence");
  return config;
}
