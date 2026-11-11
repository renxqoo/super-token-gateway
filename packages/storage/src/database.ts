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

export const SCHEMA_VERSION = 2026111102;
export async function migrate(
  db: SQL,
  options: { upgrade?: boolean; quiesced?: boolean } = {},
): Promise<void> {
  const source = await Bun.file(new URL("./schema.sql", import.meta.url)).text();
  const lifecycle = await Bun.file(new URL("./lifecycle.sql", import.meta.url)).text();
  const credit = await Bun.file(new URL("./credit.sql", import.meta.url)).text();
  await db.begin(async (tx) => {
    await tx`SELECT pg_advisory_xact_lock(81442591)`;
    const existing = await tx`SELECT to_regclass('public.schema_migrations') AS relation`;
    if (existing[0]?.relation) {
      await tx`LOCK TABLE schema_migrations IN ACCESS EXCLUSIVE MODE`;
      const versions = await tx<{ version: number }[]>`SELECT version FROM schema_migrations`;
      if (versions.length === 1 && versions[0]?.version === SCHEMA_VERSION) return;
      if (
        versions.length !== 1 ||
        ![2026111006, 2026111007, 2026111008, 2026111009, 2026111010, 2026111101].includes(
          versions[0]?.version ?? 0,
        ) ||
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
      if ((versions[0]?.version ?? 0) < 2026111009) {
        await tx`ALTER TABLE public.resources ADD COLUMN IF NOT EXISTS batch_manifest jsonb`;
        await tx`UPDATE public.gateway_requests q SET billing_outcome='PENDING'
        FROM public.billing_holds h, public.resources r WHERE h.request_id=q.id AND r.attempt_id=q.selected_attempt_id
        AND r.kind='batch' AND h.state IN ('HELD','RECONCILING') AND q.billing_outcome IN ('SUCCESS','CANCELLED')`;
        await tx`UPDATE public.resources r SET state='PENDING_USAGE',next_poll_at=now(),poll_until=NULL,poll_token=NULL
        FROM public.attempts a JOIN public.billing_holds h ON h.request_id=a.request_id
        WHERE r.attempt_id=a.id AND r.kind='batch' AND r.state IN ('COMPLETED','CANCELLED','FAILED')
        AND h.state IN ('HELD','RECONCILING')`;
      }
      if ((versions[0]?.version ?? 0) < 2026111101) await tx.unsafe(credit);
      const functions = `${source}\n${lifecycle}\n${credit}`.match(
        /CREATE FUNCTION [\s\S]*?\bAS (\$[A-Za-z_]*\$)[\s\S]*?\1;/g,
      );
      if (!functions?.length) throw new Error("Current schema function manifest is empty");
      for (const definition of functions)
        await tx.unsafe(definition.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION"));
      await tx`REVOKE ALL ON FUNCTION public.ingest_initial_usage(jsonb,uuid[]) FROM PUBLIC`;
      await tx`REVOKE ALL ON FUNCTION public.ledger_uuid() FROM PUBLIC`;
      await tx`REVOKE ALL ON FUNCTION public.settle_covered_requests(uuid[]) FROM PUBLIC`;
      await tx`REVOKE ALL ON FUNCTION public.wallet_freeze_reason(boolean,boolean,numeric,numeric,numeric) FROM PUBLIC`;
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
      if ((await tx`SELECT 1 FROM pg_roles WHERE rolname='gateway_runtime'`).length) {
        await tx`GRANT EXECUTE ON FUNCTION public.configure_wallet_credit(uuid,text,text,numeric,numeric) TO gateway_runtime`;
        await tx`GRANT EXECUTE ON FUNCTION public.wallet_freeze_reason(boolean,boolean,numeric,numeric,numeric) TO gateway_runtime`;
        await tx`REVOKE UPDATE ON public.gateway_requests FROM gateway_runtime`;
        await tx`GRANT UPDATE(state,updated_at) ON public.gateway_requests TO gateway_runtime`;
      }
      await tx`UPDATE schema_migrations SET version=${SCHEMA_VERSION},applied_at=now()`;
      return;
    }
    const objects =
      await tx`SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S') LIMIT 1`;
    if (objects.length) throw new Error("Refusing to initialize a nonempty database");
    await tx.unsafe(source);
    await tx.unsafe(lifecycle);
    await tx.unsafe(credit);
    await tx`INSERT INTO schema_migrations(version) VALUES(${SCHEMA_VERSION})`;
  });
}
