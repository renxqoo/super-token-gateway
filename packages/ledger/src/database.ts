import { SQL } from "bun";
import { and, asc, getTableColumns, gte, lt, sql } from "drizzle-orm";
import { createHash } from "node:crypto";
import { createGzip, createGunzip } from "node:zlib";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, rename, rm } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createInterface } from "node:readline";
import manifest from "./migrations/manifest.json";
import { billingCharges, createDrizzle, ledgerEntries } from "./schema.ts";

const databases = new WeakMap<SQL, string>();

export const SCHEMA_VERSION = manifest.migrations.at(-1)!.version;

export function createDatabase(url: string, max = 32): SQL {
  const name = decodeURIComponent(new URL(url).pathname.slice(1));
  if (!name) throw new Error("An explicit PostgreSQL database name is required");
  const db = new SQL(url, {
    max,
    connectionTimeout: 5,
    idleTimeout: 30,
    maxLifetime: 1800,
    connection: {
      statement_timeout: 8000,
      lock_timeout: 5000,
      application_name: "super-token-gateway",
    },
  });
  databases.set(db, name);
  return db;
}

type Migration = (typeof manifest.migrations)[number];
type AppliedMigration = { version: number; checksum: string; manifest: Migration | null };

function migrationDigest(migration: Migration): string {
  return new Bun.CryptoHasher("sha256").update(JSON.stringify(migration)).digest("hex");
}

function validateAppliedMigrations(rows: AppliedMigration[]): number {
  if (!rows.length || rows.length > manifest.migrations.length) {
    throw new Error("Incompatible database schema migration history");
  }
  for (const [index, row] of rows.entries()) {
    const canonical = manifest.migrations[index]!;
    const candidates = index === 0 ? [canonical, ...manifest.legacyBaselines] : [canonical];
    const expected = candidates.find(
      (candidate) =>
        candidate.version === row.version && migrationDigest(candidate) === row.checksum,
    );
    const stored = row.manifest;
    if (
      !expected ||
      stored?.version !== expected.version ||
      !Array.isArray(stored.files) ||
      stored.files.length !== expected.files.length ||
      expected.files.some(
        (entry, i) =>
          stored.files[i]?.path !== entry.path || stored.files[i]?.checksum !== entry.checksum,
      )
    ) {
      throw new Error("Schema migration manifest checksum mismatch");
    }
  }
  return rows.length;
}

export async function assertSchemaVersion(db: SQL): Promise<number> {
  const rows = await db<AppliedMigration[]>`
    SELECT version, checksum, manifest FROM public.schema_migrations ORDER BY version
  `;
  const applied = validateAppliedMigrations(rows);
  const current = rows[applied - 1]?.version ?? 0;
  if (applied !== manifest.migrations.length || current !== SCHEMA_VERSION) {
    throw new Error(`Schema version mismatch: expected ${SCHEMA_VERSION}, got ${current}`);
  }
  return current;
}

export async function assertServiceRole(
  db: SQL,
  role: "stg_data" | "stg_control" | "stg_worker" | "stg_audit",
): Promise<void> {
  const [record] = await db`SELECT
    current_user = session_user
    AND NOT (u.rolsuper OR u.rolcreaterole OR u.rolcreatedb OR u.rolreplication OR u.rolbypassrls)
    AND NOT has_schema_privilege(current_user, 'public', 'CREATE')
    AND NOT EXISTS (SELECT 1 FROM pg_database d WHERE d.datname = current_database()
      AND pg_has_role(current_user, d.datdba, 'MEMBER'))
    AND EXISTS (SELECT 1 FROM pg_roles r WHERE r.rolname = ${role}
      AND pg_has_role(current_user, r.oid, 'USAGE'))
    AND NOT EXISTS (SELECT 1 FROM pg_roles r
      WHERE r.rolname NOT IN (${role}, current_user) AND pg_has_role(current_user, r.oid, 'MEMBER'))
    AND NOT EXISTS (SELECT 1 FROM pg_class o JOIN pg_namespace n ON n.oid = o.relnamespace
      WHERE n.nspname = 'public' AND (pg_has_role(current_user, o.relowner, 'MEMBER') OR EXISTS (
        SELECT FROM aclexplode(o.relacl) acl WHERE acl.grantee = u.oid
      ) OR EXISTS (
        SELECT FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) acl
        WHERE a.attrelid = o.oid AND acl.grantee = u.oid
      )))
    AS allowed FROM pg_roles u WHERE u.rolname = current_user`;
  if (!record?.allowed) throw new Error(`PostgreSQL login must have isolated ${role} privileges`);
}

export async function migrate(db: SQL): Promise<void> {
  const plan = await Promise.all(
    manifest.migrations.map(async (migration) => ({
      migration,
      checksum: migrationDigest(migration),
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
        throw new Error("Incompatible database schema; initialize a new empty v4 database");
      const versions = await tx<AppliedMigration[]>`
        SELECT version, checksum, manifest FROM public.schema_migrations ORDER BY version
      `;
      applied = validateAppliedMigrations(versions);
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

export async function prepareLedgerMonth(db: SQL, month: string): Promise<void> {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(month)) throw new Error("Expected YYYY-MM month");
  await db`SELECT public.prepare_ledger_month(${month + "-01"}::date)`;
}

export type ArchiveManifest = {
  month: string;
  schema_version: number;
  exported_at: string;
  files: { kind: string; file: string; rows: number; sha256: string }[];
};

const archivePageSize = 1000;

async function* archivePages<T extends { cursorTime: string }>(
  load: (cursor?: T) => Promise<T[]>,
): AsyncGenerator<Omit<T, "cursorTime">> {
  let cursor: T | undefined;
  for (;;) {
    const rows = await load(cursor);
    for (const { cursorTime, ...row } of rows) {
      void cursorTime;
      yield row;
    }
    if (rows.length < archivePageSize) return;
    cursor = rows.at(-1);
  }
}

async function fileDigest(path: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

export async function exportLedgerArchive(
  db: SQL,
  month: string,
  directory: string,
): Promise<string> {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(month)) throw new Error("Expected YYYY-MM month");
  const startIso = `${month}-01T00:00:00.000Z`;
  const next = new Date(startIso);
  next.setUTCMonth(next.getUTCMonth() + 1);
  const endIso = next.toISOString();
  await mkdir(directory, { recursive: true });
  const generation = crypto.randomUUID();
  const generated: string[] = [];
  const writeGzip = async (
    kind: string,
    rows: AsyncIterable<Record<string, unknown>>,
  ): Promise<ArchiveManifest["files"][number]> => {
    const file = `${kind}-${generation}.ndjson.gz`;
    const path = join(directory, file);
    generated.push(path);
    let count = 0;
    async function* ndjson() {
      for await (const row of rows) {
        count++;
        yield `${JSON.stringify(row)}\n`;
      }
    }
    await pipeline(Readable.from(ndjson()), createGzip(), createWriteStream(path, { flags: "wx" }));
    return { kind, file, rows: count, sha256: await fileDigest(path) };
  };

  const manifestPath = join(directory, "manifest.json");
  const temporaryManifest = join(directory, `.manifest-${generation}.json`);
  generated.push(temporaryManifest);
  try {
    const doc = await db.begin("isolation level repeatable read read only", async (tx) => {
      const version = await assertSchemaVersion(tx);
      const orm = createDrizzle(tx);
      const charges = archivePages(
        (cursor?: typeof billingCharges.$inferSelect & { cursorTime: string }) =>
          orm
            .select({
              ...getTableColumns(billingCharges),
              cursorTime: sql<string>`${billingCharges.createdAt}::text`,
            })
            .from(billingCharges)
            .where(
              and(
                gte(billingCharges.createdAt, startIso),
                lt(billingCharges.createdAt, endIso),
                cursor
                  ? sql`(${billingCharges.createdAt}, ${billingCharges.id})
              > (${cursor.cursorTime}::timestamptz, ${cursor.id}::uuid)`
                  : undefined,
              ),
            )
            .orderBy(asc(billingCharges.createdAt), asc(billingCharges.id))
            .limit(archivePageSize),
      );
      const entries = archivePages(
        (cursor?: typeof ledgerEntries.$inferSelect & { cursorTime: string }) =>
          orm
            .select({
              ...getTableColumns(ledgerEntries),
              cursorTime: sql<string>`${ledgerEntries.createdAt}::text`,
            })
            .from(ledgerEntries)
            .where(
              and(
                gte(ledgerEntries.createdAt, startIso),
                lt(ledgerEntries.createdAt, endIso),
                cursor
                  ? sql`(${ledgerEntries.createdAt}, ${ledgerEntries.journalId}, ${ledgerEntries.ordinal})
              > (${cursor.cursorTime}::timestamptz, ${cursor.journalId}::uuid, ${cursor.ordinal}::int)`
                  : undefined,
              ),
            )
            .orderBy(
              asc(ledgerEntries.createdAt),
              asc(ledgerEntries.journalId),
              asc(ledgerEntries.ordinal),
            )
            .limit(archivePageSize),
      );
      return {
        month,
        schema_version: version,
        exported_at: new Date().toISOString(),
        files: [await writeGzip("charges", charges), await writeGzip("entries", entries)],
      } satisfies ArchiveManifest;
    });
    await Bun.write(temporaryManifest, `${JSON.stringify(doc, null, 2)}\n`);
    await rename(temporaryManifest, manifestPath);
    return manifestPath;
  } catch (error) {
    await Promise.allSettled(generated.map((path) => rm(path, { force: true })));
    throw error;
  }
}

export async function verifyLedgerArchive(manifestPath: string): Promise<ArchiveManifest> {
  const doc = (await Bun.file(manifestPath).json()) as ArchiveManifest;
  const directory = dirname(manifestPath);
  for (const entry of doc.files) {
    if (entry.file !== basename(entry.file) || !entry.file.endsWith(".ndjson.gz")) {
      throw new Error("Invalid archive file path");
    }
    const filePath = join(directory, entry.file);
    if ((await fileDigest(filePath)) !== entry.sha256) {
      throw new Error(`Archive checksum mismatch: ${entry.file}`);
    }
    let count = 0;
    await pipeline(createReadStream(filePath), createGunzip(), async (input) => {
      const lines = createInterface({ input: Readable.from(input), crlfDelay: Infinity });
      for await (const line of lines) {
        if (!line.trim()) continue;
        JSON.parse(line);
        count++;
      }
    });
    if (count !== entry.rows) throw new Error(`Archive row count mismatch: ${entry.file}`);
  }
  return doc;
}
