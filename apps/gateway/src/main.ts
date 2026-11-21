import { loadConfigFromEnv } from "./config.ts";
import { createGatewayRuntime } from "./app.ts";

const config = loadConfigFromEnv();
const runtime = createGatewayRuntime({ config });
await runtime.start();

const publicServer = Bun.serve({
  hostname: config.host,
  port: config.port,
  idleTimeout: 120,
  fetch(req, server) {
    const peerAddress = server.requestIP(req)?.address;
    return runtime.publicApp.fetch(req, { peerAddress });
  },
});

const internalServer = Bun.serve({
  hostname: config.internalHost,
  port: config.internalPort,
  idleTimeout: 60,
  fetch: runtime.internalApp.fetch,
});

console.log(
  JSON.stringify({
    service: "gateway",
    mode: config.mode,
    public_port: publicServer.port,
    internal_port: internalServer.port,
    epoch: runtime.snapshot.get().epoch,
  }),
);

let shuttingDown = false;
async function shutdown(): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  await runtime.close(10_000);
  await publicServer.stop();
  await internalServer.stop();
  process.exit(0);
}

process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());
