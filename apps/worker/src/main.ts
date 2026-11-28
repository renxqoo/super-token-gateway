import {
  assertProductionPostgresTransport,
  assertSchemaVersion,
  assertServiceRole,
  createDatabase,
} from "@gateway/ledger";
import { createWorkerRunner } from "./runner.ts";

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) {
  throw new Error("DATABASE_URL is required");
}

const production = process.env.NODE_ENV === "production";
if (production) {
  assertProductionPostgresTransport(databaseUrl, process.env.NODE_TLS_REJECT_UNAUTHORIZED);
}

const dbPoolSize = Number(process.env.WORKER_DB_MAX ?? 8);
if (!Number.isSafeInteger(dbPoolSize) || dbPoolSize < 2) {
  throw new Error("WORKER_DB_MAX must allow both leader and work connections");
}
const db = createDatabase(databaseUrl, dbPoolSize);
const currentVersion = await assertSchemaVersion(db);
if (production) await assertServiceRole(db, "stg_worker");

const worker = createWorkerRunner({
  db,
  sweepIntervalMs: Number(process.env.WORKER_SWEEP_MS ?? 2000),
  maintenanceIntervalMs: Number(process.env.WORKER_MAINTENANCE_MS ?? 15_000),
  outboxIntervalMs: Number(process.env.WORKER_OUTBOX_MS ?? 1000),
  monitorIntervalMs: Number(process.env.WORKER_MONITOR_MS ?? 15_000),
  ...(process.env.BUSINESS_WEBHOOK_URL
    ? { businessWebhookUrl: process.env.BUSINESS_WEBHOOK_URL }
    : {}),
  ...(process.env.BUSINESS_WEBHOOK_TOKEN
    ? { businessWebhookToken: process.env.BUSINESS_WEBHOOK_TOKEN }
    : {}),
});

await worker.start();

const healthPort = Number(process.env.WORKER_HEALTH_PORT ?? 0);
const server =
  healthPort > 0
    ? Bun.serve({
        hostname: process.env.WORKER_HEALTH_HOST ?? "127.0.0.1",
        port: healthPort,
        async fetch(req) {
          const path = new URL(req.url).pathname;
          if (path === "/livez") return Response.json({ status: "ok" });
          if (path === "/metrics")
            return new Response(worker.metrics(), {
              headers: { "content-type": "text/plain; version=0.0.4; charset=utf-8" },
            });
          if (path === "/readyz") {
            try {
              const [database] = await db`SELECT NOT pg_is_in_recovery()
                AND current_setting('transaction_read_only') = 'off' AS writable`;
              if (database?.writable) return Response.json({ status: "ready", ...worker.status() });
            } catch {
              void 0;
            }
            return Response.json({ status: "db_unavailable", ...worker.status() }, { status: 503 });
          }
          return new Response("Not Found", { status: 404 });
        },
      })
    : null;

console.log(
  JSON.stringify({
    service: "worker",
    leader: worker.isLeader(),
    health_port: server?.port ?? null,
    schema_version: currentVersion,
  }),
);

let shuttingDown = false;
async function shutdown(): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  await worker.stop();
  if (server) await server.stop();
  await db.close();
  process.exit(0);
}

process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());
