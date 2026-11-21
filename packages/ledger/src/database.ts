import { SQL } from "bun";
import { and, asc, gte, lt, sql } from "drizzle-orm";
import { createHash } from "node:crypto";
import { createGzip, createGunzip } from "node:zlib";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, rename } from "node:fs/promises";
import { join } from "node:path";
import manifest from "./migrations/manifest.json";
import { billingCharges, createDrizzle, ledgerEntries, schemaMigrations } from "./schema.ts";

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
      lock_timeout: 2000,
      application_name: "super-token-gateway",
    },
  });
  databases.set(db, name);
  return db;
}

export async function assertSchemaVersion(db: SQL): Promise<number> {
  const orm = createDrizzle(db);
  const rows = await orm
    .select({ version: sql<number>`coalesce(max(${schemaMigrations.version}), 0)::int` })
    .from(schemaMigrations);
  const current = Number(rows[0]?.version ?? 0);
  if (current !== SCHEMA_VERSION) {
    throw new Error(`Schema version mismatch: expected ${SCHEMA_VERSION}, got ${current}`);
  }
  return current;
}

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
        throw new Error("Incompatible database schema; initialize a new empty v4 database");
      const versions =
        await tx`SELECT version,checksum,manifest FROM schema_migrations ORDER BY version`;
      if (!versions.length || versions.length > plan.length)
        throw new Error("Incompatible database schema; initialize a new empty v4 database");
      for (const [index, row] of versions.entries()) {
        const expected = plan[index];
        if (!expected || row.version !== expected.migration.version)
          throw new Error("Incompatible database schema; initialize a new empty v4 database");
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

export async function prepareLedgerMonth(db: SQL, month: string): Promise<void> {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(month)) throw new Error("Expected YYYY-MM month");
  const start = `${month}-01`;
  const nextMonth = new Date(`${start}T00:00:00.000Z`);
  nextMonth.setUTCMonth(nextMonth.getUTCMonth() + 1);
  const endIso = nextMonth.toISOString();
  const startIso = `${start}T00:00:00.000Z`;
  const tableName = `ledger_entries_${month.replace("-", "")}`;
  await db.begin(async (tx) => {
    const exists = (await tx`SELECT to_regclass(${`public.${tableName}`}) AS rel`)[0]?.rel;
    if (exists) return;
    await tx`LOCK TABLE public.ledger_entries IN ACCESS EXCLUSIVE MODE`;
    const moved = await tx`
      SELECT journal_id, ordinal, created_at, side, entity_id, request_id, reference, kind, currency, account, atoms::text
      FROM public.ledger_entries_default
      WHERE created_at >= ${startIso}::timestamptz AND created_at < ${endIso}::timestamptz
    `;
    if (moved.length > 0) {
      await tx`ALTER TABLE public.ledger_entries_default DISABLE TRIGGER immutable_ledger_entries`;
      await tx`
        DELETE FROM public.ledger_entries_default
        WHERE created_at >= ${startIso}::timestamptz AND created_at < ${endIso}::timestamptz
      `;
      await tx`ALTER TABLE public.ledger_entries_default ENABLE TRIGGER immutable_ledger_entries`;
    }
    await tx`SELECT public.prepare_ledger_month(${start}::date)`;
    if (moved.length > 0) {
      await tx.unsafe(`ALTER TABLE public.${tableName} DISABLE TRIGGER balanced_ledger_journal`);
      for (const row of moved) {
        await tx`
          INSERT INTO public.ledger_entries (journal_id, ordinal, created_at, side, entity_id, request_id, reference, kind, currency, account, atoms)
          VALUES (${row.journal_id}, ${row.ordinal}, ${row.created_at}, ${row.side}, ${row.entity_id}, ${row.request_id}, ${row.reference}, ${row.kind}, ${row.currency}, ${row.account}, ${row.atoms}::numeric)
        `;
      }
      await tx.unsafe(`ALTER TABLE public.${tableName} ENABLE TRIGGER balanced_ledger_journal`);
    }
  });
}

export type ArchiveManifest = {
  month: string;
  schema_version: number;
  exported_at: string;
  files: { kind: string; file: string; rows: number; sha256: string }[];
};

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
  const writeGzip = async (
    kind: string,
    file: string,
    rows: Record<string, unknown>[],
  ): Promise<{ kind: string; file: string; rows: number; sha256: string }> => {
    const target = join(directory, file);
    const temp = `${target}.tmp`;
    const gzip = createGzip();
    const out = createWriteStream(temp);
    gzip.pipe(out);
    for (const row of rows) gzip.write(`${JSON.stringify(row)}\n`);
    gzip.end();
    await new Promise<void>((resolve, reject) => {
      out.on("finish", resolve);
      out.on("error", reject);
      gzip.on("error", reject);
    });
    await rename(temp, target);
    const hash = createHash("sha256");
    for await (const chunk of createReadStream(target)) hash.update(chunk);
    return { kind, file, rows: rows.length, sha256: hash.digest("hex") };
  };

  const orm = createDrizzle(db);
  const [charges, entries, versionRows] = await Promise.all([
    orm
      .select()
      .from(billingCharges)
      .where(and(gte(billingCharges.createdAt, startIso), lt(billingCharges.createdAt, endIso)))
      .orderBy(asc(billingCharges.createdAt), asc(billingCharges.id)),
    orm
      .select()
      .from(ledgerEntries)
      .where(and(gte(ledgerEntries.createdAt, startIso), lt(ledgerEntries.createdAt, endIso)))
      .orderBy(
        asc(ledgerEntries.createdAt),
        asc(ledgerEntries.journalId),
        asc(ledgerEntries.ordinal),
      ),
    orm
      .select({ version: sql<number>`coalesce(max(${schemaMigrations.version}), 0)::int` })
      .from(schemaMigrations),
  ]);

  const files = await Promise.all([
    writeGzip("charges", "charges.ndjson.gz", charges),
    writeGzip("entries", "entries.ndjson.gz", entries),
  ]);
  const manifestPath = join(directory, "manifest.json");
  const doc: ArchiveManifest = {
    month,
    schema_version: Number(versionRows[0]?.version || SCHEMA_VERSION),
    exported_at: new Date().toISOString(),
    files,
  };
  await Bun.write(manifestPath, `${JSON.stringify(doc, null, 2)}\n`);
  return manifestPath;
}

export async function verifyLedgerArchive(manifestPath: string): Promise<ArchiveManifest> {
  const doc = (await Bun.file(manifestPath).json()) as ArchiveManifest;
  const directory = new URL("./", `file://${manifestPath}`).pathname;
  for (const entry of doc.files) {
    const filePath = join(directory, entry.file);
    const hash = createHash("sha256");
    for await (const chunk of createReadStream(filePath)) hash.update(chunk);
    const actual = hash.digest("hex");
    if (actual !== entry.sha256) throw new Error(`Archive checksum mismatch: ${entry.file}`);
    let count = 0;
    let text = "";
    for await (const chunk of createReadStream(filePath).pipe(createGunzip()))
      text += String(chunk);
    for (const line of text.split("\n")) {
      if (!line.trim()) continue;
      JSON.parse(line);
      count++;
    }
    if (count !== entry.rows) throw new Error(`Archive row count mismatch: ${entry.file}`);
  }
  return doc;
}
