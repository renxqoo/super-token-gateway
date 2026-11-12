import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { migrate, Store, FundsStore, createDatabase } from "@gateway/storage";
import manifest from "../../packages/storage/src/migrations/manifest.json";
import { withDatabase } from "../support/database.ts";
import { withFinancial } from "../support/financial-execution.ts";
async function installV5(db: SQL) {
  for (const m of manifest.migrations.slice(0, 4)) {
    for (const f of m.files)
      await db.unsafe(
        await Bun.file(new URL(`../../packages/storage/src/${f.path}`, import.meta.url)).text(),
      );
    const hash = new Bun.CryptoHasher("sha256").update(JSON.stringify(m)).digest("hex");
    await db`INSERT INTO schema_migrations(version,checksum,manifest) VALUES(${m.version},${hash},${m}::jsonb)`;
  }
}
async function historical(db: SQL) {
  const store = new Store(db),
    project = await store.createProject({
      name: "historical",
      rpm: 100,
      tpm: 100000,
      concurrency: 10,
    }),
    key = await store.createCredential(
      project,
      { name: "historical", models: [], operations: ["chat"] },
      crypto.randomUUID(),
      "old",
    ),
    provider = await store.createProvider(
      crypto.randomUUID(),
      {
        name: "historical-provider",
        kind: "openai",
        base_url: "https://historical.test",
        rpm: 100,
        concurrency: 10,
      },
      "historical-ciphertext",
    );
  await store.createModel({
    name: "historical-model",
    operations: ["chat"],
    max_output_tokens: 100,
  });
  const route = await store.createRoute("historical-model", {
      provider_id: provider.id,
      upstream_model: "old",
      operations: ["chat"],
      priority: 0,
      weight: 1,
      pricing: { currency: "USD", rates: { requests: { numerator: "1", denominator: "1" } } },
      ceilings: { requests: "1" },
    }),
    tariff = await new FundsStore(db).setTariff({
      model: "historical-model",
      operation: "chat",
      provider_id: provider.id,
      pricing: { currency: "USD", rates: { requests: { numerator: "1", denominator: "1" } } },
      ceilings: { requests: "1" },
      basis: "REQUEST_COMPLETION",
    });
  await new FundsStore(db).credit(project, {
    currency: "USD",
    external_ref: "historical-credit",
    amount_atoms: "100",
    kind: "CASH",
    priority: 100,
    models: [],
    operations: [],
  });
  const cost = (await db`SELECT pricing_id FROM routes WHERE id=${route}`)[0].pricing_id,
    request = crypto.randomUUID(),
    result = (
      await db`SELECT admit_gateway_batch(${[{ id: request, key_id: key, project_id: project, operation: "chat", model: "historical-model", digest: "historical-intent", fingerprint: "old-fingerprint", route_id: route, cost_pricing_id: cost, quote: { currency: "USD", amount_atoms: "1", input_characters: "0", prices: { [provider.id]: tariff.pricing_id } } }]}::jsonb) result`
    )[0].result["0"];
  return { store, project, key, provider, request, attempt: result.attempt_id as string };
}

test("v5 to financial append preserves historical wallets requests and null catalog owners without fabricated captures", () =>
  withDatabase(
    async (db) => {
      await installV5(db);
      await historical(db);
      const command = crypto.randomUUID();
      await db`INSERT INTO execution_commands(id,command_id,kind,caller_identity,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state) VALUES(${crypto.randomUUID()},${command},'plugin_register_v1','historical:catalog',${crypto.randomUUID()},repeat('a',64),clock_timestamp()+interval '1 hour','historical',1,'COMMITTED')`;
      const before = {
        wallet: await db`SELECT * FROM wallets`,
        request: await db`SELECT * FROM gateway_requests`,
        attempt: await db`SELECT * FROM attempts`,
        commands: await db`SELECT * FROM execution_commands`,
      };
      await migrate(db);
      expect(
        (
          await db`SELECT count(*)::integer n FROM information_schema.columns WHERE table_schema='public' AND table_name='upstream_holds' AND column_name='remaining_reserved_atoms'`
        )[0].n,
      ).toBe(0);
      expect(await db`SELECT * FROM wallets`).toEqual(before.wallet);
      expect(
        (await db`SELECT * FROM gateway_requests`).map(
          ({ execution_budget, execution_closed_at, ...row }: Record<string, unknown>) => {
            expect(execution_budget).toBeNull();
            expect(execution_closed_at).toBeNull();
            return row;
          },
        ),
      ).toEqual(before.request);
      expect(await db`SELECT * FROM attempts`).toEqual(before.attempt);
      expect(
        (await db`SELECT * FROM execution_commands`).map(
          ({ receipt, context_id, ...row }: Record<string, unknown>) => {
            expect(receipt).toEqual({});
            expect(context_id).toBeNull();
            return row;
          },
        ),
      ).toEqual(before.commands);
      expect((await db`SELECT count(*)::integer n FROM upstream_holds`)[0].n).toBe(0);
      expect((await db`SELECT count(*)::integer n FROM schema_migrations`)[0].n).toBe(5);
    },
    { max: 4, registry: ".local/T04b2a-resources.jsonl" },
  ));

test("append refuses unmappable active supplier holds and rolls all new schema back", () =>
  withDatabase(
    async (db) => {
      await installV5(db);
      const old = await historical(db);
      await db`INSERT INTO upstream_account_balances(account_id,currency) VALUES(${old.provider.account_id},'USD')`;
      await db`INSERT INTO upstream_holds(attempt_id,account_id,channel_id,currency,estimated_atoms,cost_version_id,state) SELECT ${old.attempt},${old.provider.account_id},${old.provider.id},'USD',1,pricing_id,'UNKNOWN' FROM attempts WHERE id=${old.attempt}`;
      const before = await db`SELECT * FROM upstream_holds`;
      const failure = await migrate(db).catch((error) => error as Error);
      expect(failure).toBeInstanceOf(Error);
      expect((failure as Error).message).toContain("Unmappable active upstream holds");
      expect(await db`SELECT * FROM upstream_holds`).toEqual(before);
      expect((await db`SELECT to_regclass('execution_contexts') relation`)[0].relation).toBeNull();
      expect((await db`SELECT count(*)::integer n FROM schema_migrations`)[0].n).toBe(4);
    },
    { max: 4, registry: ".local/T04b2a-resources.jsonl" },
  ));

test("append rejects cross-key duplicate project intents instead of selecting a historical winner", () =>
  withDatabase(
    async (db) => {
      await installV5(db);
      const old = await historical(db),
        key = await old.store.createCredential(
          old.project,
          { name: "second", models: [], operations: ["chat"] },
          crypto.randomUUID(),
          "old-second",
        );
      await db`INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state) VALUES(${crypto.randomUUID()},${old.project},${key},'chat','historical-model','historical-intent','different','UNKNOWN')`;
      const before = await db`SELECT * FROM gateway_requests ORDER BY id`;
      const failure = await migrate(db).catch((error) => error);
      expect(failure).toMatchObject({ errno: "23505" });
      expect(await db`SELECT * FROM gateway_requests ORDER BY id`).toEqual(before);
      expect((await db`SELECT count(*)::integer n FROM schema_migrations`)[0].n).toBe(4);
    },
    { max: 4, registry: ".local/T04b2a-resources.jsonl" },
  ));

test(
  "Core execution runtime role cannot read money or invoke private or legacy allocation kernels",
  () =>
    withFinancial(async (f) => {
      const role = createDatabase(f.identity.apiUrl);
      try {
        for (const query of [
          () => role`SELECT * FROM wallets`,
          () => role`SELECT * FROM channels`,
          () => role`SELECT allocate_customer_batch_v1('[]'::jsonb)`,
          () => role`SELECT admit_gateway_batch('[]'::jsonb)`,
          () => role`SELECT core_execution_v1('{}'::jsonb,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb)`,
        ]) {
          const failure = await query().catch((error) => error);
          expect(failure).toMatchObject({ errno: "42501" });
        }
      } finally {
        await role.close();
      }
    }),
  30000,
);
