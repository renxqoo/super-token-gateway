import { SQL } from "bun";
import manifest from "./migrations/manifest.json";

export const CONSOLE_SCHEMA_VERSION = manifest.migrations.at(-1)!.version;
const CONSOLE_MIGRATE_LOCK_KEY = 72194031;

const databases = new WeakMap<SQL, string>();

export function createConsoleDatabase(url: string, maxConnections = 16): SQL {
  const name = decodeURIComponent(new URL(url).pathname.slice(1));
  if (!name) throw new Error("An explicit PostgreSQL database name is required");
  const db = new SQL(url, {
    max: maxConnections,
    idleTimeout: 30,
    maxLifetime: 1800,
    connectionTimeout: 5,
    connection: {
      statement_timeout: 10_000,
      lock_timeout: 5_000,
      application_name: "super-token-gateway-console",
    },
  });
  databases.set(db, name);
  return db;
}

async function readMigrationSql(relativePath: string): Promise<string> {
  const path = import.meta.path.endsWith(".ts")
    ? relativePath
    : relativePath.replace("./migrations/", "./console-migrations/");
  return Bun.file(new URL(path, import.meta.url)).text();
}

type AppliedRow = { version: number; checksum: string };

function validateAppliedHistory(rows: AppliedRow[]): number {
  if (!rows.length || rows.length > manifest.migrations.length) {
    throw new Error("Incompatible console database schema migration history");
  }
  for (let i = 0; i < rows.length; i++) {
    const expected = manifest.migrations[i]!;
    const actual = rows[i]!;
    if (
      Number(actual.version) !== expected.version ||
      String(actual.checksum) !== expected.checksum
    ) {
      throw new Error(
        `Console migration checksum mismatch for version ${actual.version} (expected ${expected.version})`,
      );
    }
  }
  return rows.length;
}

export async function migrateConsoleDb(db: SQL): Promise<number> {
  const prepared: { version: number; sqlText: string; checksum: string }[] = [];
  for (const mig of manifest.migrations) {
    const sqlText = await readMigrationSql(mig.path);
    const checksum = new Bun.CryptoHasher("sha256").update(sqlText).digest("hex");
    if (checksum !== mig.checksum) {
      throw new Error(`Console migration file checksum mismatch: ${mig.path}`);
    }
    prepared.push({ version: mig.version, sqlText, checksum });
  }
  await db.begin(async (tx) => {
    await tx`SELECT pg_advisory_xact_lock(${CONSOLE_MIGRATE_LOCK_KEY})`;
    const expectedDbName = databases.get(db);
    if (expectedDbName) {
      const identity = (await tx`SELECT current_database() AS name`)[0]?.name;
      if (identity !== expectedDbName) {
        throw new Error("Console migration target database identity mismatch");
      }
    }
    const existing = await tx`SELECT to_regclass('public.console_schema_migrations') AS relation`;
    let appliedCount = 0;
    if (existing[0]?.relation) {
      await tx`LOCK TABLE public.console_schema_migrations IN ACCESS EXCLUSIVE MODE`;
      const rows = await tx<AppliedRow[]>`
        SELECT version::int AS version, checksum
        FROM public.console_schema_migrations
        ORDER BY version ASC
      `;
      appliedCount = validateAppliedHistory(rows);
    } else {
      const objects = await tx`
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
        UNION ALL
        SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public'
        UNION ALL
        SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = 'public'
        UNION ALL
        SELECT 1 FROM pg_extension WHERE extname <> 'plpgsql'
        UNION ALL
        SELECT 1 FROM pg_namespace WHERE nspname NOT IN ('public', 'information_schema') AND nspname !~ '^pg_'
        LIMIT 1
      `;
      if (objects.length > 0) {
        throw new Error("Refusing to initialize console schema in a nonempty database");
      }
      await tx`
        CREATE TABLE IF NOT EXISTS public.console_schema_migrations (
          version INTEGER PRIMARY KEY,
          checksum TEXT NOT NULL,
          applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `;
    }
    if (appliedCount < prepared.length) await tx`SET LOCAL statement_timeout = '15min'`;
    for (const mig of prepared.slice(appliedCount)) {
      await tx.unsafe(mig.sqlText);
      await tx`
        INSERT INTO public.console_schema_migrations (version, checksum)
        VALUES (${mig.version}, ${mig.checksum})
      `;
    }
  });
  return CONSOLE_SCHEMA_VERSION;
}

export async function assertConsoleSchemaVersion(db: SQL): Promise<number> {
  const rows = await db<AppliedRow[]>`
    SELECT version::int AS version, checksum
    FROM public.console_schema_migrations
    ORDER BY version ASC
  `;
  const applied = validateAppliedHistory(rows);
  const current = applied > 0 ? Number(rows[applied - 1]!.version) : 0;
  if (applied !== manifest.migrations.length || current !== CONSOLE_SCHEMA_VERSION) {
    throw new Error(
      `Console schema version mismatch: expected ${CONSOLE_SCHEMA_VERSION}, got ${current}`,
    );
  }
  return current;
}
