import { SQL } from "bun";

export function createDatabase(url: string, max = 16): SQL {
  return new SQL(url, {
    max,
    connectionTimeout: 5,
    idleTimeout: 30,
    maxLifetime: 1800,
    connection: {
      statement_timeout: 8000,
      lock_timeout: 2000,
      application_name: "super-token-gateway",
    },
  });
}

export const SCHEMA_VERSION = 2026111005;
export async function migrate(db: SQL): Promise<void> {
  const source = await Bun.file(new URL("./schema.sql", import.meta.url)).text();
  await db.begin(async (tx) => {
    await tx`SELECT pg_advisory_xact_lock(81442591)`;
    const existing = await tx`SELECT to_regclass('public.schema_migrations') AS relation`;
    if (existing[0]?.relation) {
      const versions = await tx<{ version: number }[]>`SELECT version FROM schema_migrations`;
      if (versions.length === 1 && versions[0]?.version === SCHEMA_VERSION) return;
      throw new Error(
        "Incompatible database schema; initialize a new dedicated database. Previous runtime contracts are not supported.",
      );
    }
    const objects =
      await tx`SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S') LIMIT 1`;
    if (objects.length) throw new Error("Refusing to initialize a nonempty database");
    await tx.unsafe(source);
    await tx`INSERT INTO schema_migrations(version) VALUES(${SCHEMA_VERSION})`;
  });
}
