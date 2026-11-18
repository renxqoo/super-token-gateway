import { createCoreRpcClient } from "@gateway/core-client";
import { createCustomerFundsStore, createDatabase, SCHEMA_VERSION } from "@gateway/storage";
import { loadCoreWorkerConfig } from "./config.ts";
import { recoverExecutions, startPeriodic } from "./maintenance.ts";
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
const counters = {
  committed: 0,
  rejected: 0,
  quarantined: 0,
  failures: 0,
  reconciled: 0,
  aborted: 0,
  released: 0,
  quota_expired: 0,
  maintenance_failures: 0,
};
let recovery: ReturnType<typeof startPeriodic> | undefined;
const settlement = createSettlementWorker({
  db,
  url: config.QUEUE_REDIS_URL,
  namespace: config.REDIS_NAMESPACE,
  replicas: 0,
  timeoutMs: config.QUEUE_ACK_TIMEOUT_MS,
  concurrency: config.SETTLEMENT_CONCURRENCY,
  onCommitted: (groups) => {
    counters.committed += groups;
    recovery?.trigger();
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
const core = config.CORE_ORIGIN
  ? createCoreRpcClient({
      origin: config.CORE_ORIGIN,
      servername: config.CORE_SERVERNAME!,
      audience: config.CORE_AUDIENCE!,
      expectedPeerSha256: config.CORE_PEER_SHA256!,
      expectedSubjectAltname: config.CORE_SUBJECT_ALTNAME!,
      key: await Bun.file(config.CORE_KEY_FILE!).text(),
      cert: await Bun.file(config.CORE_CERT_FILE!).text(),
      ca: await Bun.file(config.CORE_CA_FILE!).text(),
    })
  : undefined;
const funds = createCustomerFundsStore(db);
const maintenanceFailed = () => {
  counters.maintenance_failures++;
};
const reconciliation = startPeriodic(
  async () => {
    const settled = await funds.reconcileKnownHolds(config.RECONCILE_BATCH);
    counters.reconciled += settled;
    return settled === config.RECONCILE_BATCH;
  },
  config.RECONCILE_INTERVAL_MS,
  maintenanceFailed,
);
recovery = core
  ? startPeriodic(
      async () => {
        await core.startup();
        const result = await recoverExecutions(db, core, config.RECOVERY_BATCH);
        counters.aborted += result.aborted;
        counters.released += result.released;
        counters.quota_expired += result.expired;
        if (result.blocked) throw new Error("quota_recovery_incomplete");
        return result.more;
      },
      config.RECOVERY_INTERVAL_MS,
      maintenanceFailed,
    )
  : undefined;
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
        return Response.json({ status: "ready", ...counters, persistence: settlement.stats() });
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
      Promise.all([settlement.close(), reconciliation.close(), recovery?.close()]),
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, config.SHUTDOWN_GRACE_MS);
      }),
    ]);
  } finally {
    clearTimeout(timer);
    await health.stop(true);
    await core?.close();
    await db.close({ timeout: 1 });
    console.log(JSON.stringify({ service: "core-worker", event: "stopped", ...counters }));
    process.exit(0);
  }
}
process.on("SIGTERM", () => void shutdown());
process.on("SIGINT", () => void shutdown());
