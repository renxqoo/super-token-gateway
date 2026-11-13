import { z } from "zod";
import { coreAudiencePattern } from "@gateway/core-client";
import { validateCidr } from "@gateway/runtime";

const cidrs = z.array(z.string().max(64)).max(64).default([]);
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
  max_inflight: z.number().int().positive().max(100000).default(1024),
  quota_renew_ms: z.number().int().min(100).max(20000).default(10000),
  shutdown_grace_ms: z.number().int().min(100).max(120000).default(20000),
  trusted_proxy_cidrs: cidrs,
  allow_private_upstream: z.boolean().default(false),
  private_upstream_cidrs: cidrs,
  upstream_pool_size: z.number().int().positive().max(65536).default(1024),
  core: z.strictObject({
    origin: z.string().url(),
    servername: z.string().min(1),
    audience: z.string().regex(coreAudiencePattern),
    peer_sha256: z.string().regex(/^[a-f0-9]{64}$/),
    subject_altname: z.string().min(1).max(2000),
    key_file: z.string().min(1),
    cert_file: z.string().min(1),
    ca_file: z.string().min(1),
  }),
  queue: z.strictObject({
    url: z.string().startsWith("redis"),
    namespace: z.string().regex(/^[a-zA-Z0-9:_-]{1,80}$/),
    required_replicas: z.number().int().min(0).max(5),
    ack_timeout_ms: z.number().int().min(100).max(10000).default(2000),
  }),
  plugins: z.strictObject({
    helper_executable: z.string().min(1),
    trust_policy: z.string().min(1),
  }),
  production: z.boolean().default(false),
});

export type GatewayConfig = z.infer<typeof configSchema>;

export async function loadGatewayConfig(
  path = process.env.GATEWAY_CONFIG_FILE,
): Promise<GatewayConfig> {
  if (!path) throw new Error("GATEWAY_CONFIG_FILE required");
  const config = configSchema.parse(await Bun.file(path).json());
  for (const cidr of [...config.trusted_proxy_cidrs, ...config.private_upstream_cidrs])
    validateCidr(cidr);
  if (config.production && config.allow_private_upstream)
    throw new Error("Production forbids private upstream bypass");
  if (config.production && config.queue.required_replicas < 1)
    throw new Error("Production requires an acknowledged AOF replica for evidence");
  return config;
}
