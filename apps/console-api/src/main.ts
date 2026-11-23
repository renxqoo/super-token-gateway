import { createConsoleRuntime } from "./app.ts";
import { loadConsoleConfigFromEnv } from "./config.ts";

const config = loadConsoleConfigFromEnv();
const runtime = createConsoleRuntime({ config });
await runtime.start();

const server = Bun.serve({
  hostname: config.host,
  port: config.port,
  idleTimeout: 60,
  fetch: runtime.app.fetch,
});

console.log(
  JSON.stringify({
    service: "console-api",
    port: server.port,
  }),
);

let shuttingDown = false;
async function shutdown(): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  await server.stop();
  await runtime.close();
  process.exit(0);
}

process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());
