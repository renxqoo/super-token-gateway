import { createDatabase, SCHEMA_VERSION } from "@gateway/ledger";
import { createWorkerRunner } from "./runner.ts";

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) {
  throw new Error("DATABASE_URL is required");
}

const db = createDatabase(databaseUrl, Number(process.env.WORKER_DB_MAX ?? 8));
const versionRows = await db`SELECT max(version) AS version FROM public.schema_migrations`;
const currentVersion = Number(versionRows[0]?.version ?? 0);
if (currentVersion !== SCHEMA_VERSION) {
  throw new Error(`Schema version mismatch: expected ${SCHEMA_VERSION}, got ${currentVersion}`);
}

const worker = createWorkerRunner({
  db,
  sweepIntervalMs: Number(process.env.WORKER_SWEEP_MS ?? 2000),
  maintenanceIntervalMs: Number(process.env.WORKER_MAINTENANCE_MS ?? 15_000),
  outboxIntervalMs: Number(process.env.WORKER_OUTBOX_MS ?? 1000),
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
        fetch(req) {
          const path = new URL(req.url).pathname;
          if (path === "/livez") return Response.json({ status: "ok" });
          if (path === "/readyz") {
            return Response.json({ status: "ready", leader: worker.isLeader() });
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
