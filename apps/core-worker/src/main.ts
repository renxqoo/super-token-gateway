import { createDatabase, SCHEMA_VERSION } from "@gateway/storage";
import { loadCoreWorkerConfig } from "./config.ts";
import { createSettlementWorker } from "./settlement/worker.ts";

const config = loadCoreWorkerConfig();
const db = createDatabase(config.DATABASE_URL, config.DB_POOL_SIZE);
const schema = await db<
  { version: number }[]
>`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`;
if (schema[0]?.version !== SCHEMA_VERSION) {
  await db.close({ timeout: 1 });
  throw new Error("Incompatible database schema for the settlement worker");
}
const counters = { committed: 0, rejected: 0, quarantined: 0, failures: 0 };
const settlement = createSettlementWorker({
  db,
  url: config.QUEUE_REDIS_URL,
  namespace: config.REDIS_NAMESPACE,
  replicas: 0,
  timeoutMs: config.QUEUE_ACK_TIMEOUT_MS,
  concurrency: config.SETTLEMENT_CONCURRENCY,
  onCommitted: (groups) => {
    counters.committed += groups;
  },
  onRejected: (count) => {
    counters.rejected += count;
  },
  onQuarantined: (count) => {
    counters.quarantined += count;
  },
  onError: () => {
    counters.failures++;
  },
});
await settlement.ready();
let stopping = false;
const health = Bun.serve({
  hostname: config.HOST,
  port: config.WORKER_PORT,
  fetch: async (request) => {
    const path = new URL(request.url).pathname;
    if (request.method !== "GET") return new Response(null, { status: 405 });
    if (path === "/livez") return Response.json({ status: "alive" });
    if (path === "/readyz") {
      if (stopping || !settlement.healthy())
        return Response.json({ status: "unavailable" }, { status: 503 });
      try {
        await db`SELECT 1`;
        return Response.json({ status: "ready", ...counters });
      } catch {
        return Response.json({ status: "unavailable" }, { status: 503 });
      }
    }
    return new Response(null, { status: 404 });
  },
});
console.log(JSON.stringify({ service: "core-worker", event: "ready", port: health.port }));
async function shutdown() {
  if (stopping) return;
  stopping = true;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      settlement.close(),
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, config.SHUTDOWN_GRACE_MS);
      }),
    ]);
  } finally {
    clearTimeout(timer);
    await health.stop(true);
    await db.close({ timeout: 1 });
    console.log(JSON.stringify({ service: "core-worker", event: "stopped", ...counters }));
    process.exit(0);
  }
}
process.on("SIGTERM", () => void shutdown());
process.on("SIGINT", () => void shutdown());
