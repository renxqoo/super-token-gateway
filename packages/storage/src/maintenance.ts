import type { SQL } from "bun";
import { SCHEMA_VERSION } from "./database.ts";
export function evidenceMonth(month: string): { start: string; end: string; suffix: string } {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(month) || Number(month.slice(0, 4)) < 2000)
    throw new Error("Expected a UTC month YYYY-MM from year 2000");
  const start = `${month}-01T00:00:00.000Z`;
  const next = new Date(start);
  next.setUTCMonth(next.getUTCMonth() + 1);
  return { start, end: next.toISOString(), suffix: month.replace("-", "") };
}
export async function assertOffline(tx: SQL): Promise<void> {
  const clients =
    await tx`SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND backend_type='client backend'`;
  if (clients.length)
    throw new Error("Maintenance requires all other database clients to disconnect");
}
export async function prepareEvidenceMonth(
  db: SQL,
  month: string,
  quiesced = false,
): Promise<void> {
  const range = evidenceMonth(month);
  await db.begin(async (tx) => {
    await tx`SELECT pg_advisory_xact_lock(81442591)`;
    await tx`LOCK TABLE schema_migrations IN ACCESS EXCLUSIVE MODE`;
    const version = await tx`SELECT version FROM schema_migrations`;
    if (version.length !== 1 || version[0].version !== SCHEMA_VERSION)
      throw new Error("Maintenance schema mismatch");
    await tx`SET LOCAL statement_timeout=0`;
    if (quiesced) await assertOffline(tx as unknown as SQL);
    await tx`LOCK TABLE usage_event_index,money_entries,usage_payloads,journal_payloads IN ACCESS EXCLUSIVE MODE`;
    for (const [parent, index, constraint, trigger] of [
      ["usage_payloads", "usage_event_index", "usage_index_payload", "immutable_usage_payload"],
      ["journal_payloads", "money_entries", "journal_index_payload", "immutable_journal_payload"],
    ] as const) {
      const name = `${parent}_${range.suffix}`;
      if ((await tx`SELECT to_regclass(${`public.${name}`}) AS name`)[0]?.name) continue;
      const count = (
        await tx.unsafe(
          `SELECT EXISTS(SELECT 1 FROM public.${parent}_default WHERE created_at>=$1::timestamptz AND created_at<$2::timestamptz) AS n`,
          [range.start, range.end],
        )
      )[0].n;
      if (count) {
        if (!quiesced)
          throw new Error("The month has fallback evidence; retry offline with --quiesced");
        await tx.unsafe(
          `CREATE TEMP TABLE evidence_move ON COMMIT DROP AS SELECT * FROM public.${parent}_default WHERE created_at>=$1::timestamptz AND created_at<$2::timestamptz`,
          [range.start, range.end],
        );
        await tx.unsafe(`ALTER TABLE public.${index} DROP CONSTRAINT ${constraint}`);
        await tx.unsafe(`ALTER TABLE public.${parent} DISABLE TRIGGER ${trigger}`);
        await tx.unsafe(
          `DELETE FROM public.${parent}_default WHERE created_at>=$1::timestamptz AND created_at<$2::timestamptz`,
          [range.start, range.end],
        );
      }
      await tx.unsafe(
        `CREATE TABLE public.${name} PARTITION OF public.${parent} FOR VALUES FROM ('${range.start}') TO ('${range.end}')`,
      );
      if (count) {
        await tx.unsafe(`INSERT INTO public.${parent} SELECT * FROM pg_temp.evidence_move`);
        const mismatch = await tx.unsafe(
          `SELECT 1 FROM ((SELECT * FROM pg_temp.evidence_move EXCEPT SELECT * FROM public.${name}) UNION ALL (SELECT * FROM public.${name} EXCEPT SELECT * FROM pg_temp.evidence_move)) diff LIMIT 1`,
        );
        if (mismatch.length) throw new Error("Fallback evidence movement mismatch");
        await tx.unsafe(`ALTER TABLE public.${parent} ENABLE TRIGGER ${trigger}`);
        await tx.unsafe(
          `ALTER TABLE public.${index} ADD CONSTRAINT ${constraint} FOREIGN KEY(id,created_at) REFERENCES public.${parent}(id,created_at) DEFERRABLE INITIALLY DEFERRED`,
        );
        await tx`DROP TABLE pg_temp.evidence_move`;
      }
    }
  });
}
export async function moveEvidenceMonth(db: SQL, month: string, tablespace: string): Promise<void> {
  const range = evidenceMonth(month);
  if (!/^[a-z_][a-z0-9_]{0,62}$/.test(tablespace)) throw new Error("Invalid tablespace identifier");
  await db.begin(async (tx) => {
    await tx`SELECT pg_advisory_xact_lock(81442591)`;
    await tx`LOCK TABLE schema_migrations IN ACCESS EXCLUSIVE MODE`;
    const version = await tx`SELECT version FROM schema_migrations`;
    if (version.length !== 1 || version[0].version !== SCHEMA_VERSION)
      throw new Error("Maintenance schema mismatch");
    await assertOffline(tx as unknown as SQL);
    await tx`SET LOCAL statement_timeout=0`;
    if (!(await tx`SELECT 1 FROM pg_tablespace WHERE spcname=${tablespace}`).length)
      throw new Error("Configured tablespace does not exist");
    for (const parent of ["usage_payloads", "journal_payloads"]) {
      const name = `${parent}_${range.suffix}`;
      if (!(await tx`SELECT to_regclass(${`public.${name}`}) AS name`)[0]?.name)
        throw new Error("Prepare this month before moving its partitions");
      await tx.unsafe(`ALTER TABLE public.${name} SET TABLESPACE ${tablespace}`);
      const indexes = await tx<
        { name: string }[]
      >`SELECT c.relname AS name FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=${`public.${name}`}::regclass`;
      for (const index of indexes) {
        if (!/^[a-z_][a-z0-9_]*$/.test(index.name)) throw new Error("Unexpected index identity");
        await tx.unsafe(`ALTER INDEX public.${index.name} SET TABLESPACE ${tablespace}`);
      }
    }
  });
}
