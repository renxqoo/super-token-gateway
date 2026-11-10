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

export const SCHEMA_VERSION = 2026111008;
export async function migrate(
  db: SQL,
  options: { upgrade?: boolean; quiesced?: boolean } = {},
): Promise<void> {
  const source = await Bun.file(new URL("./schema.sql", import.meta.url)).text();
  const lifecycle = await Bun.file(new URL("./lifecycle.sql", import.meta.url)).text();
  await db.begin(async (tx) => {
    await tx`SELECT pg_advisory_xact_lock(81442591)`;
    const existing = await tx`SELECT to_regclass('public.schema_migrations') AS relation`;
    if (existing[0]?.relation) {
      await tx`LOCK TABLE schema_migrations IN ACCESS EXCLUSIVE MODE`;
      const versions = await tx<{ version: number }[]>`SELECT version FROM schema_migrations`;
      if (versions.length === 1 && versions[0]?.version === SCHEMA_VERSION) return;
      if (
        versions.length !== 1 ||
        ![2026111006, 2026111007].includes(versions[0]?.version ?? 0) ||
        !options.upgrade ||
        !options.quiesced
      )
        throw new Error(
          "Incompatible database schema; supported upgrades require --upgrade --quiesced",
        );
      const active =
        await tx`SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND backend_type='client backend'`;
      if (active.length)
        throw new Error("Offline upgrade requires all other database clients to disconnect");
      await tx`SET LOCAL statement_timeout=0`;
      const before = (
        await tx`SELECT (SELECT count(*)::text FROM usage_events) AS usage,(SELECT count(*)::text FROM money_journal) AS journals,(SELECT count(*)::text FROM gateway_requests) AS requests,(SELECT sum(available_atoms+reserved_atoms+spent_atoms+expired_atoms+debt_atoms)::text FROM wallets) AS balances`
      )[0];
      if (versions[0]?.version === 2026111006) await tx.unsafe(lifecycle);
      const functions = `${source}\n${lifecycle}`.match(
        /CREATE FUNCTION [\s\S]*?\bAS (\$[A-Za-z_]*\$)[\s\S]*?\1;/g,
      );
      if (!functions?.length) throw new Error("Current schema function manifest is empty");
      for (const definition of functions)
        await tx.unsafe(definition.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION"));
      await tx`REVOKE ALL ON FUNCTION public.ingest_initial_usage(jsonb,uuid[]) FROM PUBLIC`;
      await tx`DROP INDEX IF EXISTS public.requests_stale`;
      await tx`DROP INDEX IF EXISTS public.usage_attempt`;
      await tx`ALTER TABLE public.gateway_requests SET (fillfactor=70)`;
      const after = (
        await tx`SELECT (SELECT count(*)::text FROM usage_events) AS usage,(SELECT count(*)::text FROM money_journal) AS journals,(SELECT count(*)::text FROM gateway_requests) AS requests,(SELECT sum(available_atoms+reserved_atoms+spent_atoms+expired_atoms+debt_atoms)::text FROM wallets) AS balances`
      )[0];
      if (JSON.stringify(before) !== JSON.stringify(after))
        throw new Error("Upgrade accounting snapshot mismatch");
      const invalid =
        await tx`SELECT 1 FROM pg_constraint WHERE connamespace='public'::regnamespace AND NOT convalidated LIMIT 1`;
      if (invalid.length) throw new Error("Upgrade left unvalidated constraints");
      for (const role of ["gateway_runtime", "gateway_worker"]) {
        if ((await tx`SELECT 1 FROM pg_roles WHERE rolname=${role}`).length)
          await tx.unsafe(`GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${role}`);
      }
      await tx`UPDATE schema_migrations SET version=${SCHEMA_VERSION},applied_at=now()`;
      return;
    }
    const objects =
      await tx`SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S') LIMIT 1`;
    if (objects.length) throw new Error("Refusing to initialize a nonempty database");
    await tx.unsafe(source);
    await tx.unsafe(lifecycle);
    await tx`INSERT INTO schema_migrations(version) VALUES(${SCHEMA_VERSION})`;
  });
}
