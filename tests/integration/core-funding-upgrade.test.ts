import { expect, test } from "bun:test";
import { migrate } from "@gateway/storage";
import manifest from "../../packages/storage/src/migrations/manifest.json";
import { withDatabase } from "../support/database.ts";
import type { SQL } from "bun";

async function installPrefix(db: SQL, through: number) {
  await db.begin(async (tx) => {
    for (const entry of manifest.migrations.filter((m) => m.version <= through)) {
      for (const file of entry.files)
        await tx.unsafe(
          await Bun.file(
            new URL(`../../packages/storage/src/${file.path}`, import.meta.url),
          ).text(),
        );
      const digest = new Bun.CryptoHasher("sha256").update(JSON.stringify(entry)).digest("hex");
      await tx`INSERT INTO schema_migrations(version,checksum,manifest) VALUES(${entry.version},${digest},${entry}::jsonb)`;
    }
  });
}

test("funding upgrade cannot turn an account-id reader or revision-only writer into a balance reader or money writer", () =>
  withDatabase(async (db) => {
    await installPrefix(db, 2026111803);
    const suffix = crypto.randomUUID().replaceAll("-", "");
    const limited = `funding_limited_${suffix}`;
    const writer = `funding_writer_${suffix}`;
    const created: string[] = [];
    try {
      for (const role of [limited, writer]) {
        await db.unsafe(`CREATE ROLE ${role} NOLOGIN`);
        created.push(role);
        await db.unsafe(`GRANT USAGE ON SCHEMA public TO ${role}`);
      }
      await db.unsafe(
        `GRANT SELECT(account_id),UPDATE(revision) ON upstream_account_balances TO ${limited}`,
      );
      await db.unsafe(`GRANT SELECT,INSERT,UPDATE ON upstream_account_balances TO ${writer}`);
      await db.unsafe(
        `GRANT EXECUTE ON FUNCTION core_platform_authorize_v1(jsonb,text) TO ${writer}`,
      );
      await migrate(db);
      expect(
        (
          await db`SELECT has_function_privilege(${limited},'public.core_upstream_fund_v1(uuid,text,numeric,boolean)','EXECUTE') allowed`
        )[0].allowed,
      ).toBe(false);
      expect(
        (
          await db`SELECT has_function_privilege(${writer},'public.core_upstream_fund_v1(uuid,text,numeric,boolean)','EXECUTE') allowed`
        )[0].allowed,
      ).toBe(true);
      expect(
        (
          await db`SELECT has_table_privilege(${writer},'upstream_account_balances','UPDATE') allowed`
        )[0].allowed,
      ).toBe(false);
      await db.begin(async (tx) => {
        await tx.unsafe(`SET LOCAL ROLE ${limited}`);
        expect(await tx`SELECT account_id FROM core_upstream_balances_v1`).toEqual([]);
        await expect(
          tx.savepoint((tx) => tx`SELECT balance_atoms FROM core_upstream_balances_v1`),
        ).rejects.toMatchObject({ errno: "42501" });
        await expect(
          tx.savepoint(
            (tx) =>
              tx`SELECT core_upstream_fund_v1('10000000-0000-4000-8000-000000000000'::uuid,'USD',1,false)`,
          ),
        ).rejects.toMatchObject({ errno: "42501" });
      });
    } finally {
      for (const role of created.reverse()) {
        await db.unsafe(`DROP OWNED BY ${role}`);
        await db.unsafe(`DROP ROLE ${role}`);
      }
    }
  }));

test("funding batch upgrade preserves a narrow slow-owner delegation without granting it ledger writes", () =>
  withDatabase(async (db) => {
    await installPrefix(db, 2026111804);
    const suffix = crypto.randomUUID().replaceAll("-", "");
    const batch = `funding_batch_owner_${suffix}`;
    const slow = `funding_slow_owner_${suffix}`;
    const created: string[] = [];
    try {
      for (const role of [batch, slow]) {
        await db.unsafe(`CREATE ROLE ${role} NOLOGIN`);
        created.push(role);
        await db.unsafe(`GRANT USAGE,CREATE ON SCHEMA public TO ${role}`);
      }
      await db.unsafe(
        `ALTER FUNCTION public.persist_core_evidence_groups_v1(jsonb) OWNER TO ${batch}`,
      );
      await db.unsafe(
        `ALTER FUNCTION public.persist_core_evidence_slow_v1(jsonb) OWNER TO ${slow}`,
      );
      await db.unsafe(`REVOKE CREATE ON SCHEMA public FROM ${batch},${slow}`);
      await db.unsafe(`GRANT INSERT ON public.core_evidence_rejections TO ${batch}`);
      await db.unsafe(`GRANT SELECT ON public.upstream_holds TO ${slow}`);
      await db.unsafe(`GRANT USAGE ON SCHEMA core_funding TO ${slow}`);
      await db.unsafe(
        `GRANT EXECUTE ON FUNCTION core_funding.exclusive_v1(uuid[]),public.core_catalog_write_v1(),public.persist_core_evidence_groups_v1(jsonb) TO ${slow}`,
      );
      const group = {
        rejected: "evidence_signature_invalid",
        events: [
          {
            origin_identity: "gateway:historical",
            origin_session_id: crypto.randomUUID(),
            event_id: crypto.randomUUID(),
            attempt_id: crypto.randomUUID(),
          },
        ],
      };
      async function invoke() {
        return db.begin(async (tx) => {
          await tx.unsafe(`SET LOCAL ROLE ${slow}`);
          return (await tx`SELECT public.persist_core_evidence_slow_v1(${group}::jsonb) value`)[0]
            .value;
        });
      }
      expect(await invoke()).toEqual({ status: "REJECTED", error: "evidence_signature_invalid" });
      await migrate(db);
      expect(await invoke()).toEqual({ status: "REJECTED", error: "evidence_signature_invalid" });
      expect(
        (
          await db`SELECT has_table_privilege(${slow},'public.core_evidence_rejections','INSERT') allowed`
        )[0].allowed,
      ).toBe(false);
      expect(
        (await db`SELECT has_schema_privilege(${batch},'core_funding','CREATE') allowed`)[0]
          .allowed,
      ).toBe(false);
      expect(
        (
          await db`SELECT has_function_privilege(${slow},'core_funding.flush_v1(jsonb)','EXECUTE') allowed`
        )[0].allowed,
      ).toBe(false);
      expect(await db`SELECT count(*)::integer n FROM core_evidence_rejections`).toEqual([
        { n: 2 },
      ]);
    } finally {
      for (const role of created.reverse()) {
        await db.unsafe(`DROP OWNED BY ${role}`);
        await db.unsafe(`DROP ROLE ${role}`);
      }
    }
  }));
