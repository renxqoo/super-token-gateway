import type { SQL } from "bun";
import { Hono } from "hono";
import { createDatabase } from "@gateway/storage";
import { UsageProducer, evidenceChannel } from "@gateway/runtime";
import { loadCoreApiConfig, type CoreApiConfig } from "./config.ts";
import { startCoreInternalListener } from "./identity/internal-listener.ts";
import { createCoreManagementApplication } from "./platform/management.ts";
import { createCatalogApplication, type CatalogApplication } from "./plugins/application.ts";
import { catalogRoutes } from "./plugins/http.ts";
import { createCoreAccountApplication } from "./public/account.ts";

type AdminConfig = NonNullable<CoreApiConfig["admin"]>;
type PublicConfig = NonNullable<CoreApiConfig["public"]>;
type Execution = NonNullable<CoreApiConfig["execution"]>;

function serveHttp(options: {
  app: Hono<{ Bindings: { peerAddress?: string } }>;
  hostname: string;
  port: number;
  maxBodyBytes: number;
  shutdownGraceMs: number;
  stopping: () => void;
  close: () => Promise<void>;
}) {
  let server: ReturnType<typeof Bun.serve>;
  try {
    server = Bun.serve({
      hostname: options.hostname,
      port: options.port,
      fetch: (request, server) =>
        options.app.fetch(request, { peerAddress: server.requestIP(request)?.address }),
      maxRequestBodySize: options.maxBodyBytes,
      error: () =>
        Response.json(
          { error: { code: "server_error", message: "Core API server error" } },
          { status: 500 },
        ),
    });
  } catch (error) {
    void options.close().catch(() => {});
    throw error;
  }
  return {
    port: server.port,
    async stop() {
      options.stopping();
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          server.stop(false),
          new Promise<void>((resolve) => {
            timer = setTimeout(resolve, options.shutdownGraceMs);
          }),
        ]);
      } finally {
        clearTimeout(timer);
        await server.stop(true);
        await options.close();
      }
    },
  };
}

function health(
  app: Hono<{ Bindings: { peerAddress?: string } }>,
  db: SQL,
  stopping: () => boolean,
) {
  app.get("/livez", (c) => c.json({ status: "alive" }));
  app.get("/readyz", async (c) => {
    if (stopping()) return c.json({ status: "unavailable" }, 503);
    try {
      await db`SELECT 1`;
      return c.json({ status: "ready" });
    } catch {
      return c.json({ status: "unavailable" }, 503);
    }
  });
}

function startPublicListener(config: PublicConfig, execution: Execution) {
  const db = createDatabase(config.database_url, 8);
  let stopping = false;
  const app = new Hono<{ Bindings: { peerAddress?: string } }>();
  health(app, db, () => stopping);
  app.route(
    "/",
    createCoreAccountApplication({
      db,
      pepper: execution.pepper,
      trustedProxyCidrs: config.trusted_proxy_cidrs,
    }),
  );
  return serveHttp({
    app,
    hostname: config.hostname,
    port: config.port,
    maxBodyBytes: 64 << 10,
    shutdownGraceMs: config.shutdown_grace_ms ?? 10000,
    stopping: () => {
      stopping = true;
    },
    close: () => db.close({ timeout: 1 }),
  });
}

function startAdminListener(admin: AdminConfig, execution: Execution, eventStandbyUrl?: string) {
  const db = createDatabase(admin.database_url, 8);
  let catalog: CatalogApplication | undefined;
  try {
    catalog = admin.plugins
      ? createCatalogApplication({
          db,
          adminKey: admin.admin_key,
          pepper: execution.pepper,
          policyFile: admin.plugins.release_policy,
          releaseDirectory: admin.plugins.release_directory,
          helperExecutable: admin.plugins.helper_executable,
        })
      : undefined;
  } catch (error) {
    void db.close({ timeout: 1 });
    throw error;
  }
  const queue = admin.queue
    ? new UsageProducer({
        url: admin.queue.url,
        namespace: admin.queue.namespace,
        replicas: admin.queue.required_replicas,
        timeoutMs: admin.queue.ack_timeout_ms,
        channel: evidenceChannel,
      })
    : undefined;
  let stopping = false;
  const eventStandby = eventStandbyUrl ? createDatabase(eventStandbyUrl, 4) : undefined;
  const app = new Hono<{ Bindings: { peerAddress?: string } }>();
  health(app, db, () => stopping);
  if (catalog) app.route("/admin", catalogRoutes(catalog));
  app.route(
    "/admin",
    createCoreManagementApplication({
      db,
      adminKey: admin.admin_key,
      pepper: execution.pepper,
      encryptionKey: execution.encryptionKey,
      ...(queue ? { queue } : {}),
      ...(eventStandby ? { eventStandby } : {}),
    }),
  );
  return serveHttp({
    app,
    hostname: admin.hostname,
    port: admin.port,
    maxBodyBytes: admin.max_body_bytes ?? 8 << 20,
    shutdownGraceMs: admin.shutdown_grace_ms ?? 10000,
    stopping: () => {
      stopping = true;
    },
    close: async () => {
      queue?.close();
      try {
        await catalog?.close();
      } finally {
        await Promise.all([db.close({ timeout: 1 }), eventStandby?.close({ timeout: 1 })]);
      }
    },
  });
}

const c = await loadCoreApiConfig();
const listener = await startCoreInternalListener({
  databaseUrl: c.database_url,
  ...(c.execution ? { execution: c.execution } : {}),
  ...(c.terminal_settlement ? { terminalSettlement: c.terminal_settlement } : {}),
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
let admin: ReturnType<typeof startAdminListener> | undefined;
let account: ReturnType<typeof startPublicListener> | undefined;
try {
  admin =
    c.admin && c.execution
      ? startAdminListener(c.admin, c.execution, c.terminal_settlement?.standby_url)
      : undefined;
  account = c.public && c.execution ? startPublicListener(c.public, c.execution) : undefined;
} catch (error) {
  try {
    await admin?.stop();
  } finally {
    await listener.stop();
  }
  throw error;
}
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  try {
    await Promise.all([admin?.stop(), account?.stop()]);
  } finally {
    await listener.stop();
    console.log(JSON.stringify({ event: "stopped", resources: listener.stats() }));
  }
}
process.on("SIGTERM", () => {
  void stop();
});
process.on("SIGINT", () => {
  void stop();
});
console.log(
  JSON.stringify({
    event: "ready",
    port: listener.port,
    session: listener.session,
    ...(admin ? { admin_port: admin.port } : {}),
    ...(account ? { public_port: account.port } : {}),
  }),
);
