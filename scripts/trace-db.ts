import { SQL } from "bun";
import { createTraceStore } from "../apps/gateway/src/trace-storage.ts";

const databaseUrl = process.env.TRACE_DATABASE_URL;
if (!databaseUrl) throw new Error("TRACE_DATABASE_URL is required");
const target = new URL(databaseUrl);
const ledger = process.env.DATABASE_URL;
const targetDatabase = decodeURIComponent(target.pathname.slice(1));
if (
  !targetDatabase ||
  (ledger && decodeURIComponent(new URL(ledger).pathname.slice(1)) === targetDatabase)
)
  throw new Error("Use a database dedicated to traces");

const [action, identifier] = process.argv.slice(2);
if (action === "migrate") {
  const sql = new SQL({ url: target.toString(), max: 1 });
  try {
    await sql.begin(async (tx) => {
      const [guard] = await tx`SELECT current_database()=${targetDatabase} AS expected,
        to_regclass('public.billing_holds') IS NULL
          AND to_regclass('public.console_schema_migrations') IS NULL AS isolated`;
      if (!guard?.expected || !guard.isolated)
        throw new Error("Use a database dedicated to traces");
      const path = import.meta.path.endsWith(".ts")
        ? "../infra/postgres/trace-schema.sql"
        : "./trace-schema.sql";
      await tx.unsafe(await Bun.file(new URL(path, import.meta.url)).text());
    });
    console.log(JSON.stringify({ migrated: true }));
  } finally {
    await sql.close();
  }
} else if (
  action === "read" &&
  identifier &&
  (/^[0-9a-f]{32}$/.test(identifier) ||
    /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(identifier))
) {
  const store = createTraceStore(databaseUrl, Number(process.env.TRACE_DB_TIMEOUT_MS ?? 2500));
  try {
    console.log(JSON.stringify(await store.read(identifier)));
  } finally {
    await store.close();
  }
} else throw new Error("Usage: trace-db.ts migrate | read TRACE_ID_OR_REQUEST_ID");
