import { SQL } from "bun";

export const CONSOLE_SCHEMA_VERSION = 2026112301;
const CONSOLE_MIGRATE_LOCK_KEY = 72194031;

const MIGRATION_FILES: { version: number; relativePath: string }[] = [
  { version: 2026112301, relativePath: "./migrations/0000_console_baseline.sql" },
];

export function createConsoleDatabase(url: string, maxConnections = 16): SQL {
  return new SQL(url, {
    max: maxConnections,
    idleTimeout: 30,
    maxLifetime: 1800,
    connectionTimeout: 10,
  });
}

async function readMigrationSql(relativePath: string): Promise<string> {
  const direct = Bun.file(new URL(relativePath, import.meta.url));
  if (await direct.exists()) return direct.text();
  const bundled = Bun.file(
    new URL(relativePath.replace("./migrations/", "./console-migrations/"), import.meta.url),
  );
  if (await bundled.exists()) return bundled.text();
  throw new Error(`Console migration file not found: ${relativePath}`);
}

export async function migrateConsoleDb(db: SQL): Promise<number> {
  const prepared: { version: number; sqlText: string; checksum: string }[] = [];
  for (const mig of MIGRATION_FILES) {
    const sqlText = await readMigrationSql(mig.relativePath);
    const checksum = new Bun.CryptoHasher("sha256").update(sqlText).digest("hex");
    prepared.push({ version: mig.version, sqlText, checksum });
  }
  await db.begin(async (tx) => {
    await tx`SELECT pg_advisory_xact_lock(${CONSOLE_MIGRATE_LOCK_KEY})`;
    await tx`
      CREATE TABLE IF NOT EXISTS public.console_schema_migrations (
        version INTEGER PRIMARY KEY,
        checksum TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `;
    for (const mig of prepared) {
      const existing = await tx`
        SELECT version, checksum FROM public.console_schema_migrations WHERE version = ${mig.version}
      `;
      if (existing.length > 0) {
        if (String(existing[0]!.checksum) !== mig.checksum) {
          throw new Error(`Console migration checksum mismatch for version ${mig.version}`);
        }
        continue;
      }
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
  const rows = await db`
    SELECT version::int AS version, checksum
    FROM public.console_schema_migrations
    ORDER BY version DESC
    LIMIT 1
  `;
  const latest = rows[0];
  const current = Number(latest?.version ?? 0);
  if (current !== CONSOLE_SCHEMA_VERSION) {
    throw new Error(
      `Console schema version mismatch: expected ${CONSOLE_SCHEMA_VERSION}, got ${current}`,
    );
  }
  const expectedMig = MIGRATION_FILES.at(-1)!;
  const sqlText = await readMigrationSql(expectedMig.relativePath);
  const expectedChecksum = new Bun.CryptoHasher("sha256").update(sqlText).digest("hex");
  if (String(latest?.checksum ?? "") !== expectedChecksum) {
    throw new Error(`Console schema checksum mismatch for version ${current}`);
  }
  return current;
}
