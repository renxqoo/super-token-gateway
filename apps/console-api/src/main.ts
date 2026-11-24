import { createConsoleRuntime } from "./app.ts";
import { loadConsoleConfigFromEnv } from "./config.ts";

const config = loadConsoleConfigFromEnv();
const runtime = createConsoleRuntime({ config });

let clientServer: ReturnType<typeof Bun.serve> | null = null;
let adminServer: ReturnType<typeof Bun.serve> | null = null;

try {
  await runtime.start();

  clientServer = Bun.serve({
    hostname: config.host,
    port: config.port,
    idleTimeout: 60,
    fetch: (req, server) => runtime.fetchClient(req, server.requestIP(req)?.address),
  });

  adminServer = Bun.serve({
    hostname: config.adminHost,
    port: config.adminPort,
    idleTimeout: 60,
    fetch: (req, server) => runtime.fetchAdmin(req, server.requestIP(req)?.address),
  });
} catch (err) {
  await Promise.allSettled([
    clientServer ? clientServer.stop() : Promise.resolve(),
    adminServer ? adminServer.stop() : Promise.resolve(),
  ]);
  await runtime.close(5_000);
  throw err;
}

console.log(
  JSON.stringify({
    service: "console-api",
    client_port: clientServer.port,
    admin_port: adminServer.port,
  }),
);

let shuttingDown = false;
async function shutdown(): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  const servers = [clientServer, adminServer];
  const deadline = setTimeout(() => process.exit(1), 10_000);
  const grace = Promise.withResolvers<void>();
  const drainTimer = setTimeout(grace.resolve, 4_000);
  try {
    await Promise.race([
      Promise.allSettled(servers.map((server) => server?.stop())),
      grace.promise,
    ]);
    await Promise.allSettled(servers.map((server) => server?.stop(true)));
    await runtime.close(5_000);
  } finally {
    clearTimeout(drainTimer);
    clearTimeout(deadline);
  }
  process.exit(0);
}

process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());
