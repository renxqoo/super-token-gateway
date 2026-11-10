import { loadConfig } from "./config.ts";
import { assemble } from "./assembly.ts";
import { createApp } from "./app.ts";
import { recoveryTick } from "./recovery.ts";

const config = loadConfig();
const deps = await assemble(config);
const app = createApp(deps);
const server = Bun.serve({
  hostname: config.HOST,
  port: config.PORT,
  fetch: async (request, server) => {
    const response = await deps.lifecycle.track(
      Promise.resolve(app.fetch(request, { peerAddress: server.requestIP(request)?.address })),
    );
    if (response.headers.get("content-type")?.includes("text/event-stream"))
      server.timeout(request, 0);
    return response;
  },
  idleTimeout: 30,
  maxRequestBodySize: config.MAX_BODY_BYTES,
  error: () =>
    Response.json(
      { error: { code: "server_error", message: "Gateway server error" } },
      { status: 500 },
    ),
});
console.log(JSON.stringify({ service: "llm-gateway", port: server.port, runtime: Bun.version }));
let stopping = false;
let recovering = false;
const recovery = setInterval(() => {
  if (recovering) return;
  recovering = true;
  deps.lifecycle
    .track(recoveryTick(deps))
    .catch(() => deps.metrics.increment("recovery_failures_total"))
    .finally(() => {
      recovering = false;
    });
}, 10000);
recovery.unref();
const shutdown = async () => {
  if (stopping) return;
  stopping = true;
  deps.lifecycle.accepting = false;
  clearInterval(recovery);
  const stopped = server.stop(false);
  let networkTimer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.all([
      deps.lifecycle.drain(config.SHUTDOWN_GRACE_MS),
      Promise.race([
        stopped,
        new Promise<void>((resolve) => {
          networkTimer = setTimeout(resolve, config.SHUTDOWN_GRACE_MS);
        }),
      ]),
    ]);
  } finally {
    clearTimeout(networkTimer);
  }
  await server.stop(true);
  await stopped;
  deps.usage.close();
  deps.limits.close();
  await deps.store.db.close({ timeout: 5 });
};
process.on("SIGTERM", () => {
  void shutdown();
});
process.on("SIGINT", () => {
  void shutdown();
});
