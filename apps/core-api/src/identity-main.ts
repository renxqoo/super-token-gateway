import { z } from "zod";
import { startCoreInternalListener } from "./identity/internal-listener.ts";
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
      streams: z.number().int().positive().max(16),
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
});
const path = process.env.INTERNAL_IDENTITY_CONFIG_FILE;
if (!path) throw new Error("INTERNAL_IDENTITY_CONFIG_FILE required");
const c = configSchema.parse(await Bun.file(path).json());
const listener = await startCoreInternalListener({
  databaseUrl: c.database_url,
  ...(c.execution ? { execution: c.execution } : {}),
  audience: c.audience,
  hostname: c.hostname,
  servername: c.servername,
  port: c.port,
  key: await Bun.file(c.key_file).text(),
  cert: await Bun.file(c.cert_file).text(),
  ca: await Bun.file(c.ca_file).text(),
  clientKey: await Bun.file(c.client_key_file).text(),
  clientCert: await Bun.file(c.client_cert_file).text(),
  ...(c.limits ? { limits: c.limits } : {}),
  ...(c.challenge_ttl_seconds ? { challengeTtlSeconds: c.challenge_ttl_seconds } : {}),
  ...(c.session_ttl_seconds ? { sessionTtlSeconds: c.session_ttl_seconds } : {}),
});
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  await listener.stop();
  console.log(JSON.stringify({ event: "stopped", resources: listener.stats() }));
}
process.on("SIGTERM", () => {
  void stop();
});
process.on("SIGINT", () => {
  void stop();
});
console.log(JSON.stringify({ event: "ready", port: listener.port, session: listener.session }));
