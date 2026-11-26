import { loadConfigFromEnv } from "./config.ts";
import { createGatewayRuntime } from "./app.ts";

const config = loadConfigFromEnv();
const runtime = createGatewayRuntime({ config });
await runtime.start();

const publicServer = Bun.serve({
  hostname: config.host,
  port: config.port,
  idleTimeout: 120,
  ...(config.reusePort ? { reusePort: true } : {}),
  fetch(req, server) {
    if (config.ingressIp) {
      return runtime.fetchPublic(req, server.requestIP(req)?.address, true);
    }
    return runtime.fetchPublic(
      req,
      {
        getPeerAddress: () => server.requestIP(req)?.address,
      },
      true,
    );
  },
});

const internalServer = Bun.serve({
  hostname: config.internalHost,
  port: config.internalPort,
  idleTimeout: 60,
  fetch(req) {
    if (req.method === "GET" && new URL(req.url).pathname === "/metrics") {
      return runtime.metricsResponse();
    }
    if (!runtime.lifecycle.accepting) {
      return Response.json(
        { error: { code: "overloaded", message: "Gateway is draining" } },
        { status: 503 },
      );
    }
    return runtime.lifecycle.track(Promise.resolve(runtime.internalApp.fetch(req)));
  },
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
  runtime.lifecycle.accepting = false;
  const listeners = [publicServer.stop(), internalServer.stop()];
  await runtime.close(10_000);
  await Promise.all(listeners);
  process.exit(0);
}

process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());
