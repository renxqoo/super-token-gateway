import { createConsoleRuntime } from "./app.ts";
import { loadConsoleConfigFromEnv } from "./config.ts";

const config = loadConsoleConfigFromEnv();
const runtime = createConsoleRuntime({ config });
await runtime.start();

const clientServer = Bun.serve({
  hostname: config.host,
  port: config.port,
  idleTimeout: 60,
  fetch: runtime.clientApp.fetch,
});

const adminServer = Bun.serve({
  hostname: config.adminHost,
  port: config.adminPort,
  idleTimeout: 60,
  fetch: runtime.adminApp.fetch,
});

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
  await Promise.all([clientServer.stop(), adminServer.stop()]);
  await runtime.close();
  process.exit(0);
}

process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());
