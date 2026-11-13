import { Hono } from "hono";
import { createDatabase } from "@gateway/storage";
import { loadCoreApiConfig, type CoreApiConfig } from "./config.ts";
import { startCoreInternalListener } from "./identity/internal-listener.ts";
import { createCoreManagementApplication } from "./platform/management.ts";
import { createCatalogApplication, type CatalogApplication } from "./plugins/application.ts";
import { catalogRoutes } from "./plugins/http.ts";

type AdminConfig = NonNullable<CoreApiConfig["admin"]>;
type Execution = NonNullable<CoreApiConfig["execution"]>;

function startAdminListener(admin: AdminConfig, execution: Execution) {
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
  let stopping = false;
  const app = new Hono();
  app.get("/livez", (c) => c.json({ status: "alive" }));
  app.get("/readyz", async (c) => {
    if (stopping) return c.json({ status: "unavailable" }, 503);
    try {
      await db`SELECT 1`;
      return c.json({ status: "ready" });
    } catch {
      return c.json({ status: "unavailable" }, 503);
    }
  });
  if (catalog) app.route("/admin", catalogRoutes(catalog));
  app.route(
    "/admin",
    createCoreManagementApplication({
      db,
      adminKey: admin.admin_key,
      pepper: execution.pepper,
      encryptionKey: execution.encryptionKey,
    }),
  );
  const server = Bun.serve({
    hostname: admin.hostname,
    port: admin.port,
    fetch: app.fetch,
    maxRequestBodySize: admin.max_body_bytes ?? 8 << 20,
    error: () =>
      Response.json(
        { error: { code: "server_error", message: "Core API server error" } },
        { status: 500 },
      ),
  });
  return {
    port: server.port,
    async stop() {
      stopping = true;
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          server.stop(false),
          new Promise<void>((resolve) => {
            timer = setTimeout(resolve, admin.shutdown_grace_ms ?? 10000);
          }),
        ]);
      } finally {
        clearTimeout(timer);
        await server.stop(true);
        await catalog?.close();
        await db.close({ timeout: 1 });
      }
    },
  };
}

const c = await loadCoreApiConfig();
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
let admin: ReturnType<typeof startAdminListener> | undefined;
try {
  admin = c.admin && c.execution ? startAdminListener(c.admin, c.execution) : undefined;
} catch (error) {
  await listener.stop();
  throw error;
}
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  try {
    await admin?.stop();
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
  }),
);
