import { SQL } from "bun";
import { drizzle } from "drizzle-orm/bun-sql";
import * as schema from "./schema/index.ts";
import manifest from "./migrations/manifest.json";

const databases = new WeakMap<SQL, string>();
export function createDatabase(url: string, max = 16): SQL {
  const name = decodeURIComponent(new URL(url).pathname.slice(1));
  if (!name) throw new Error("An explicit PostgreSQL database name is required");
  const db = new SQL(url, {
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
  databases.set(db, name);
  return db;
}
export function createRepositories(db: SQL) {
  return drizzle({ client: db, schema });
}
export const SCHEMA_VERSION = manifest.migrations.at(-1)!.version;
export async function migrate(db: SQL): Promise<void> {
  const plan = await Promise.all(
    manifest.migrations.map(async (migration) => ({
      migration,
      checksum: new Bun.CryptoHasher("sha256").update(JSON.stringify(migration)).digest("hex"),
      files: await Promise.all(
        migration.files.map(async (entry) => {
          const source = await Bun.file(new URL(entry.path, import.meta.url)).text();
          const checksum = new Bun.CryptoHasher("sha256").update(source).digest("hex");
          if (checksum !== entry.checksum)
            throw new Error(`Migration checksum mismatch: ${entry.path}`);
          return { ...entry, source };
        }),
      ),
    })),
  );
  await db.begin(async (tx) => {
    await tx`SELECT pg_advisory_xact_lock(81442591)`;
    const identity = (await tx`SELECT current_database() AS name`)[0]?.name;
    if (!databases.get(db) || identity !== databases.get(db))
      throw new Error("Migration target database identity mismatch");
    const existing = await tx`SELECT to_regclass('public.schema_migrations') AS relation`;
    let applied = 0;
    if (existing[0]?.relation) {
      await tx`LOCK TABLE schema_migrations IN ACCESS EXCLUSIVE MODE`;
      const columns =
        await tx`SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='schema_migrations'`;
      if (!columns.some((row: { column_name: string }) => row.column_name === "checksum"))
        throw new Error("Incompatible database schema; initialize a new empty v2 database");
      const versions =
        await tx`SELECT version,checksum,manifest FROM schema_migrations ORDER BY version`;
      if (!versions.length || versions.length > plan.length)
        throw new Error("Incompatible database schema; initialize a new empty v2 database");
      for (const [index, row] of versions.entries()) {
        const expected = plan[index];
        if (!expected || row.version !== expected.migration.version)
          throw new Error("Incompatible database schema; initialize a new empty v2 database");
        const stored = row.manifest;
        if (
          row.checksum !== expected.checksum ||
          stored?.version !== expected.migration.version ||
          !Array.isArray(stored?.files) ||
          stored.files.length !== expected.files.length ||
          expected.files.some(
            (entry, i) =>
              stored.files[i]?.path !== entry.path || stored.files[i]?.checksum !== entry.checksum,
          )
        )
          throw new Error("Applied migration manifest checksum mismatch");
      }
      applied = versions.length;
    } else {
      const objects =
        await tx`SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S','f') UNION ALL SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' UNION ALL SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public' UNION ALL SELECT 1 FROM pg_extension WHERE extname<>'plpgsql' UNION ALL SELECT 1 FROM pg_namespace WHERE nspname NOT IN ('public','information_schema') AND nspname !~ '^pg_' LIMIT 1`;
      if (objects.length) throw new Error("Refusing to initialize a nonempty database");
    }
    for (const entry of plan.slice(applied)) {
      for (const file of entry.files) await tx.unsafe(file.source);
      await tx`INSERT INTO schema_migrations(version,checksum,manifest) VALUES(${entry.migration.version},${entry.checksum},${entry.migration}::jsonb)`;
    }
  });
}
