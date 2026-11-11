import { secureEqual } from "@gateway/kernel";
import { loadConfig } from "./config.ts";
import { assemble } from "./assembly.ts";
import { recoveryTick } from "./recovery.ts";
import { BillingWorker } from "./billing-worker.ts";
const deps = await assemble(loadConfig());
const billing = new BillingWorker(deps);
let stopping = false;
process.on("SIGTERM", () => {
  stopping = true;
});
process.on("SIGINT", () => {
  stopping = true;
});
await billing.start();
const health = Bun.serve({
  hostname: deps.config.HOST,
  port: deps.config.WORKER_PORT,
  fetch: async (request) => {
    const path = new URL(request.url).pathname;
    if (request.method !== "GET") return new Response(null, { status: 405 });
    if (path === "/livez") return Response.json({ status: "alive" });
    if (path === "/readyz") {
      if (stopping || !billing.consumer.healthy())
        return Response.json({ status: "unavailable" }, { status: 503 });
      try {
        await deps.database.ping();
        await deps.usage.producer.ready();
        await deps.limits.ping();
        return Response.json({ status: "ready" });
      } catch {
        return Response.json({ status: "unavailable" }, { status: 503 });
      }
    }
    if (path === "/metrics") {
      if (
        !secureEqual(
          request.headers.get("authorization")?.replace(/^Bearer /, "") ?? "",
          deps.config.ADMIN_KEY,
        )
      )
        return new Response(null, { status: 401 });
      return new Response(deps.metrics.render(0), {
        headers: { "content-type": "text/plain; version=0.0.4" },
      });
    }
    return new Response(null, { status: 404 });
  },
});

try {
  while (!stopping) {
    try {
      await deps.funds.reconcileKnownHolds();
      await recoveryTick(deps);
    } catch {
      deps.metrics.increment("worker_failures_total");
    }
    if (!stopping) await Bun.sleep(250);
  }
} finally {
  await health.stop(true);
  await billing.close();
  deps.usage.close();
  deps.transport.close();
  deps.limits.close();
  await deps.database.close();
}
