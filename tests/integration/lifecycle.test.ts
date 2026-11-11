import { test, expect } from "bun:test";
import {
  createDatabase,
  migrate,
  Store,
  FundsStore,
  prepareEvidenceMonth,
  SCHEMA_VERSION,
  moveEvidenceMonth,
} from "@gateway/storage";
import { exportEvidenceArchive, verifyEvidenceArchive } from "../../scripts/evidence-archive.ts";
import { mkdir, rm, appendFile } from "node:fs/promises";

for (const previousVersion of [2026111008, 2026111009, 2026111010, 2026111101, 2026111102])
  test(`schema${previousVersion} upgrade preserves settled receipts, ACKs and an open hold before completing it once`, async () => {
    const base = process.env.DATABASE_URL;
    if (!base) throw new Error("DATABASE_URL required");
    const name = `gateway_test_upgrade_${crypto.randomUUID().replaceAll("-", "")}`;
    const owner = createDatabase(base, 1);
    await owner.unsafe(`CREATE DATABASE ${name}`);
    const url = new URL(base);
    url.pathname = `/${name}`;
    const db = createDatabase(url.toString(), 1);
    try {
      await db.begin(async (tx) => {
        const source = Bun.gunzipSync(
          new Uint8Array(
            await Bun.file(
              new URL(`../fixtures/schema-${previousVersion}.sql.gz`, import.meta.url),
            ).arrayBuffer(),
          ),
        );
        await tx.unsafe(new TextDecoder().decode(source));
        await tx`INSERT INTO schema_migrations(version) VALUES(${previousVersion})`;
      });
      const store = new Store(db);
      const funds = new FundsStore(db);
      const tenant = await store.createProject({
        name: "schema09-upgrade",
        rpm: 1000,
        tpm: 10000,
        concurrency: 100,
      });
      const key = await store.createCredential(
        tenant,
        { name: "fixture", models: [], operations: ["chat"] },
        crypto.randomUUID(),
        "fixture",
      );
      const provider = crypto.randomUUID();
      await store.createProvider(
        provider,
        {
          name: "fixture",
          kind: "openai",
          base_url: "http://fixture.invalid/v1",
          concurrency: 100,
          rpm: 1000,
        },
        "encrypted-fixture",
      );
      await store.createModel({ name: "fixture", operations: ["chat"], max_output_tokens: 100 });
      await store.createRoute("fixture", {
        provider_id: provider,
        upstream_model: "fixture",
        operations: ["chat"],
        priority: 1,
        weight: 1,
        pricing: { currency: "USD", rates: { requests: { numerator: "1", denominator: "1" } } },
      });
      const route = (await store.routes("fixture", "chat"))[0]!;
      const tariff = await funds.setTariff({
        model: "fixture",
        operation: "chat",
        pricing: { currency: "USD", rates: { requests: { numerator: "3", denominator: "1" } } },
        ceilings: { requests: "1" },
        basis: "PROVIDER_USAGE",
      });
      await funds.credit(tenant, {
        currency: "USD",
        external_ref: "opening",
        amount_atoms: "1000",
        kind: "CASH",
        priority: 100,
        models: [],
        operations: [],
      });
      const credential = {
        id: key,
        project_id: tenant,
        models: [],
        operations: ["chat"] as ["chat"],
      };
      const quote = {
        currency: "USD",
        amount_atoms: "10",
        input_characters: "0",
        prices: { [provider]: tariff.pricing_id },
      };
      const first = await store.admitInitial(
        crypto.randomUUID(),
        credential,
        "chat",
        "fixture",
        "settled",
        "fixture",
        route,
        quote,
      );
      const pending = await store.admitInitial(
        crypto.randomUUID(),
        credential,
        "chat",
        "fixture",
        "pending",
        "fixture",
        route,
        quote,
      );
      const consumer = await store.createConsumer("fixture", false);
      const fact = {
        attemptId: first.attempt_id!,
        source: "execution",
        state: "SUCCEEDED" as const,
        finality: "FINAL" as const,
        requestState: "SUCCEEDED" as const,
        usage: { requests: "1" },
      };
      await store.persistUsage([fact]);
      const page = await store.claimEvents(consumer, 10);
      expect(
        await store.ackEvents(
          consumer,
          page.lease_token,
          page.events.map((event) => event.id),
        ),
      ).toBe(1);
      const tables = [
        "wallets",
        "wallet_lots",
        "gateway_requests",
        "attempts",
        "billing_holds",
        "billing_charges",
        "charge_allocations",
        "hold_allocations",
        "usage_events",
        "money_journal",
        "event_deliveries",
        "event_consumers",
      ];
      const snapshot = async () => {
        const values: unknown[] = [];
        for (const table of tables)
          values.push(
            (
              await db.unsafe(
                `SELECT jsonb_agg(to_jsonb(t)-ARRAY['credit_limit_atoms','credit_reserved_atoms','credit_policy_version','credit_atoms'] ORDER BY to_jsonb(t)-ARRAY['credit_limit_atoms','credit_reserved_atoms','credit_policy_version','credit_atoms']) AS rows FROM ${table} t`,
              )
            )[0].rows,
          );
        return values;
      };
      const before = await snapshot();
      await db`DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='gateway_runtime') THEN CREATE ROLE gateway_runtime NOLOGIN;END IF;END $$`;
      await db`GRANT USAGE ON SCHEMA public TO gateway_runtime`;
      await db`GRANT UPDATE ON gateway_requests TO gateway_runtime`;
      await expect(migrate(db)).rejects.toThrow("--upgrade --quiesced");
      await migrate(db, { upgrade: true, quiesced: true });
      expect(await snapshot()).toEqual(before);
      await expect(
        db.begin(async (tx) => {
          await tx`SET LOCAL ROLE gateway_runtime`;
          await tx`UPDATE gateway_requests SET billing_outcome='FAILED' WHERE id=${pending.record.id}`;
        }),
      ).rejects.toMatchObject({ errno: "42501" });
      await db.begin(async (tx) => {
        await tx`SET LOCAL ROLE gateway_runtime`;
        await tx`UPDATE gateway_requests SET updated_at=updated_at WHERE id=${pending.record.id}`;
      });
      expect(
        (
          await db`SELECT has_function_privilege('public','public.wallet_freeze_reason(boolean,boolean,numeric,numeric,numeric)','EXECUTE') AS public_execute,
          has_function_privilege('gateway_runtime','public.wallet_freeze_reason(boolean,boolean,numeric,numeric,numeric)','EXECUTE') AS runtime_execute,
          (SELECT proacl IS NOT NULL FROM pg_proc WHERE proname='wallet_freeze_reason') AS explicit_acl`
        )[0],
      ).toEqual({ public_execute: false, runtime_execute: true, explicit_acl: true });
      expect((await funds.wallets(tenant))[0]).toMatchObject({
        credit_limit_atoms: "0",
        credit_reserved_atoms: "0",
        credit_policy_version: "0",
      });
      await db.begin(async (tx) => {
        await tx`SET LOCAL ROLE gateway_runtime`;
        const configured =
          await tx`SELECT configure_wallet_credit(${tenant}::uuid,'USD','upgraded-runtime',0,1) AS result`;
        expect(configured[0].result).toMatchObject({
          credit_limit_atoms: "1",
          credit_policy_version: "1",
        });
      });
      await expect(
        db.begin(async (tx) => {
          await tx`SET LOCAL ROLE gateway_runtime`;
          await tx`UPDATE wallets SET credit_limit_atoms=100 WHERE project_id=${tenant}`;
        }),
      ).rejects.toMatchObject({ errno: "42501" });
      expect(await store.persistUsage([fact])).toEqual({ REPLAYED: 1 });
      expect(await store.persistUsage([{ ...fact, attemptId: pending.attempt_id! }])).toEqual({
        SETTLED: 1,
      });
      expect((await funds.wallets(tenant))[0]).toMatchObject({
        available_atoms: "994",
        reserved_atoms: "0",
        spent_atoms: "6",
      });
      const next = await store.claimEvents(consumer, 10);
      expect(next.events).toHaveLength(1);
      expect(next.events[0]!.request_id).toBe(pending.record.id);
      expect(
        await store.ackEvents(
          consumer,
          next.lease_token,
          next.events.map((event) => event.id),
        ),
      ).toBe(1);
    } finally {
      await db.close();
      await owner.unsafe(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
      await owner.close();
    }
  });

test("schema07 function upgrade preserves funded wallets and permits concurrent foreign-key readers", async () => {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL required");
  const name = `gateway_test_upgrade_${crypto.randomUUID().replaceAll("-", "")}`;
  const owner = createDatabase(base, 1);
  await owner.unsafe(`CREATE DATABASE ${name}`);
  const url = new URL(base);
  url.pathname = `/${name}`;
  let db = createDatabase(url.toString(), 1);
  try {
    await db.begin(async (tx) => {
      const source = Bun.gunzipSync(
        new Uint8Array(
          await Bun.file(
            new URL("../fixtures/schema-2026111007.sql.gz", import.meta.url),
          ).arrayBuffer(),
        ),
      );
      await tx.unsafe(new TextDecoder().decode(source));
      await tx`INSERT INTO schema_migrations(version) VALUES(2026111007)`;
    });
    const store = new Store(db);
    let funds = new FundsStore(db);
    const project = await store.createProject({
      name: "function-upgrade",
      rpm: 1000,
      tpm: 10000,
      concurrency: 100,
    });
    const input = {
      currency: "USD",
      external_ref: "original",
      amount_atoms: "1000",
      kind: "CASH" as const,
      priority: 100,
      models: [],
      operations: [],
    };
    await funds.credit(project, input);
    const snapshot = () =>
      db`SELECT (to_jsonb(w)-ARRAY['credit_limit_atoms','credit_reserved_atoms','credit_policy_version'])::text AS row FROM wallets w WHERE project_id=${project}`;
    const before = await snapshot();
    await expect(migrate(db)).rejects.toThrow("--upgrade --quiesced");
    await migrate(db, { upgrade: true, quiesced: true });
    expect((await db`SELECT version FROM schema_migrations`)[0].version).toBe(SCHEMA_VERSION);
    expect(await snapshot()).toEqual(before);
    await db.close();
    db = createDatabase(url.toString(), 2);
    funds = new FundsStore(db);
    await db.begin(async (tx) => {
      await tx`SELECT id FROM wallets WHERE project_id=${project} FOR KEY SHARE`;
      await funds.credit(project, { ...input, external_ref: "after-upgrade", amount_atoms: "1" });
    });
    expect((await funds.wallets(project))[0]).toMatchObject({
      available_atoms: "1001",
      reserved_atoms: "0",
      debt_atoms: "0",
    });
    expect(await funds.journal(project, "USD")).toHaveLength(2);
  } finally {
    await db.close();
    await owner.unsafe(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await owner.close();
  }
});

test("schema06 upgrade preserves exact evidence and funds, rolls back failures, and archives verified monthly snapshots", async () => {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL required");
  const name = `gateway_test_lifecycle_${crypto.randomUUID().replaceAll("-", "")}`;
  const owner = createDatabase(base, 1);
  await owner.unsafe(`CREATE DATABASE ${name}`);
  const url = new URL(base);
  url.pathname = `/${name}`;
  let db = createDatabase(url.toString(), 1);
  const directory = new URL(`../../.local/${name}/`, import.meta.url);
  try {
    await db.begin(async (tx) => {
      const source = Bun.gunzipSync(
        new Uint8Array(
          await Bun.file(
            new URL("../fixtures/schema-2026111006.sql.gz", import.meta.url),
          ).arrayBuffer(),
        ),
      );
      await tx.unsafe(new TextDecoder().decode(source));
      await tx`INSERT INTO schema_migrations(version) VALUES(2026111006)`;
    });
    const store = new Store(db),
      funds = new FundsStore(db);
    const project = await store.createProject({
      name: "upgrade",
      rpm: 1000,
      tpm: 100000,
      concurrency: 100,
    });
    const key = await store.createCredential(
      project,
      { name: "fixture", models: [], operations: ["chat"] },
      crypto.randomUUID(),
      "fixture",
    );
    const provider = crypto.randomUUID();
    await store.createProvider(
      provider,
      {
        name: "fixture",
        kind: "openai",
        base_url: "http://fixture.invalid/v1",
        concurrency: 100,
        rpm: 1000,
      },
      "encrypted-fixture",
    );
    await store.createModel({ name: "fixture", operations: ["chat"], max_output_tokens: 100 });
    await store.createRoute("fixture", {
      provider_id: provider,
      upstream_model: "fixture",
      operations: ["chat"],
      priority: 1,
      weight: 1,
      pricing: { currency: "USD", rates: { requests: { numerator: "1", denominator: "1" } } },
    });
    const route = (await store.routes("fixture", "chat"))[0]!;
    const tariff = await funds.setTariff({
      model: "fixture",
      operation: "chat",
      pricing: { currency: "USD", rates: { requests: { numerator: "3", denominator: "1" } } },
      ceilings: { requests: "1" },
      basis: "REQUEST_COMPLETION",
    });
    await funds.credit(project, {
      currency: "USD",
      external_ref: "credit",
      amount_atoms: "1000",
      kind: "CASH",
      priority: 100,
      models: [],
      operations: [],
    });
    const credential = {
      id: key,
      project_id: project,
      models: [],
      operations: ["chat"] as ["chat"],
    };
    const quote = {
      currency: "USD",
      amount_atoms: "10",
      input_characters: "0",
      prices: { [provider]: tariff.pricing_id },
    };
    const first = await store.admitInitial(
      crypto.randomUUID(),
      credential,
      "chat",
      "fixture",
      "same-intent",
      "fixture",
      route,
      quote,
    );
    await store.persistUsage([
      {
        attemptId: first.attempt_id!,
        source: "execution",
        state: "SUCCEEDED",
        finality: "FINAL",
        usage: { requests: "1" },
        requestState: "SUCCEEDED",
      },
    ]);
    const second = await store.admitInitial(
      crypto.randomUUID(),
      credential,
      "chat",
      "fixture",
      null,
      "unknown",
      route,
      quote,
    );
    await store.persistUsage([
      {
        attemptId: second.attempt_id!,
        source: "execution",
        state: "UNKNOWN",
        finality: "UNKNOWN",
        usage: {},
        requestState: "UNKNOWN",
      },
    ]);
    await db`ALTER TABLE usage_events DISABLE TRIGGER immutable_usage`;
    await db`WITH ordered AS (SELECT id,row_number() OVER(ORDER BY id DESC) AS n FROM usage_events) UPDATE usage_events e SET created_at='2025-01-15T00:00:00Z'::timestamptz+ordered.n*interval '1 microsecond' FROM ordered WHERE e.id=ordered.id`;
    await db`ALTER TABLE usage_events ENABLE TRIGGER immutable_usage`;
    await db`ALTER TABLE money_journal DISABLE TRIGGER immutable_journal`;
    await db`UPDATE money_journal SET created_at='2025-01-15T00:00:00Z'`;
    await db`ALTER TABLE money_journal ENABLE TRIGGER immutable_journal`;
    const snapshot = async () => ({
      usage: await db`SELECT to_jsonb(e)::text AS row FROM usage_events e ORDER BY id`,
      journal: await db`SELECT to_jsonb(e)::text AS row FROM money_journal e ORDER BY id`,
      wallets:
        await db`SELECT (to_jsonb(e)-ARRAY['credit_limit_atoms','credit_reserved_atoms','credit_policy_version'])::text AS row FROM wallets e ORDER BY id`,
      holds:
        await db`SELECT (to_jsonb(e)-'credit_atoms')::text AS row FROM billing_holds e ORDER BY id`,
    });
    const before = await snapshot();
    await expect(migrate(db)).rejects.toThrow("--upgrade --quiesced");
    const blocker = createDatabase(url.toString(), 1);
    await blocker`SELECT 1`;
    await expect(migrate(db, { upgrade: true, quiesced: true })).rejects.toThrow("disconnect");
    await blocker.close();
    await db.unsafe(
      "CREATE FUNCTION reject_upgrade() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'upgrade fixture failure';END $$",
    );
    await db`CREATE TRIGGER reject_upgrade BEFORE UPDATE ON schema_migrations FOR EACH ROW EXECUTE FUNCTION reject_upgrade()`;
    await expect(migrate(db, { upgrade: true, quiesced: true })).rejects.toThrow(
      "upgrade fixture failure",
    );
    expect((await db`SELECT to_regclass('public.usage_payloads') AS id`)[0].id).toBeNull();
    expect(await snapshot()).toEqual(before);
    await db`DROP TRIGGER reject_upgrade ON schema_migrations`;
    await db`DROP FUNCTION reject_upgrade()`;
    await migrate(db, { upgrade: true, quiesced: true });
    await migrate(db);
    expect((await db`SELECT version FROM schema_migrations`)[0].version).toBe(SCHEMA_VERSION);
    expect(await snapshot()).toEqual(before);
    const upgraded = new Store(db);
    const replay = await upgraded.admitInitial(
      crypto.randomUUID(),
      credential,
      "chat",
      "fixture",
      "same-intent",
      "fixture",
      route,
      quote,
    );
    expect(replay.created).toBe(false);
    expect(replay.record.id).toBe(first.record.id);
    await upgraded.persistUsage([
      {
        attemptId: second.attempt_id!,
        source: "execution",
        state: "SUCCEEDED",
        finality: "FINAL",
        usage: { requests: "1" },
        requestState: "SUCCEEDED",
      },
    ]);
    expect(
      (await db`SELECT state FROM billing_holds WHERE request_id=${second.record.id}`)[0].state,
    ).toBe("SETTLED");
    await moveEvidenceMonth(db, "2025-01", "pg_default");
    await expect(
      db`INSERT INTO usage_payloads SELECT id,created_at+interval '1 day',usage FROM usage_events LIMIT 1`,
    ).rejects.toThrow();
    await expect(db`UPDATE journal_payloads SET lines='[]'`).rejects.toThrow();
    await expect(
      db`INSERT INTO usage_events(id,seq,project_id,request_id,attempt_id,source,finality,usage,cost_atoms,currency) SELECT ${crypto.randomUUID()},1,project_id,request_id,attempt_id,'bad_seq','UNKNOWN','{}',0,currency FROM usage_events LIMIT 1`,
    ).rejects.toThrow("generated always");
    await db`INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines,created_at) SELECT ${crypto.randomUUID()},id,'fallback','ADJUSTMENT',currency,'[{"account":"system:funding","atoms":"0"},{"account":"system:revenue","atoms":"0"}]','2024-01-02T00:00:00Z' FROM wallets LIMIT 1`;
    expect((await db`SELECT count(*)::int AS n FROM journal_payloads_default`)[0].n).toBe(1);
    await expect(prepareEvidenceMonth(db, "2024-01")).rejects.toThrow("--quiesced");
    await prepareEvidenceMonth(db, "2024-01", true);
    expect((await db`SELECT count(*)::int AS n FROM journal_payloads_default`)[0].n).toBe(0);
    expect((await db`SELECT count(*)::int AS n FROM journal_payloads_202401`)[0].n).toBe(1);
    await expect(db`DELETE FROM journal_payloads_202401`).rejects.toThrow("immutable");
    await mkdir(directory, { recursive: true });
    const manifest = await exportEvidenceArchive(db, "2025-01", directory.pathname);
    const verified = await verifyEvidenceArchive(manifest);
    expect(verified.files.find((file) => file.kind === "usage")?.rows).toBe(2);
    expect(verified.files.find((file) => file.kind === "journal")?.rows).toBe(
      before.journal.length,
    );
    await appendFile(new URL("usage.ndjson.gz", directory), "corrupt");
    await expect(verifyEvidenceArchive(manifest)).rejects.toThrow("checksum");
  } finally {
    await db.close();
    await owner.unsafe(`DROP DATABASE ${name} WITH (FORCE)`);
    await owner.close();
    await rm(directory, { recursive: true, force: true });
  }
}, 30000);
