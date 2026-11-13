import { test, expect } from "bun:test";
import {
  createDatabase,
  createRepositories,
  migrate,
  Store,
  FundsStore,
  createUpstreamFundingStore,
  prepareEvidenceMonth,
  SCHEMA_VERSION,
  moveEvidenceMonth,
} from "@gateway/storage";
import { exportEvidenceArchive, verifyEvidenceArchive } from "../../scripts/evidence-archive.ts";
import { mkdir, rm, appendFile } from "node:fs/promises";
import { installV2Baseline } from "../support/schema-baseline.ts";
import { pluginVersions } from "@gateway/storage/schema";

test("v2 through financial upgrade preserves settled receipts ACKs open hold and immutable plugin package before replay", async () => {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL required");
  const name = `gateway_test_upgrade_${crypto.randomUUID().replaceAll("-", "")}`;
  const owner = createDatabase(base, 1);
  await owner.unsafe(`CREATE DATABASE ${name}`);
  const url = new URL(base);
  url.pathname = `/${name}`;
  const db = createDatabase(url.toString(), 1);
  try {
    await installV2Baseline(db);
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
      ceilings: { requests: "1" },
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
    expect(
      (await db`SELECT binding_id FROM attempts WHERE id=${first.attempt_id}`)[0].binding_id,
    ).toBeNull();
    await expect(
      db`UPDATE attempts SET binding_id=${crypto.randomUUID()} WHERE id=${first.attempt_id}`,
    ).rejects.toMatchObject({ errno: "23503" });
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
    const leaseToken = crypto.randomUUID();
    const leased =
      await db`UPDATE event_deliveries SET state='LEASED',lease_token=${leaseToken},lease_until=now()+interval '30 seconds',attempts=attempts+1 WHERE consumer_id=${consumer} AND state<>'ACKED' RETURNING event_id`;
    expect(leased).toHaveLength(1);
    expect(
      await store.ackEvents(
        consumer,
        leaseToken,
        leased.map((row: { event_id: string }) => row.event_id),
      ),
    ).toBe(1);
    const packageBytes = Buffer.from("synthetic-upgrade-package-fact");
    const packageDigest = new Bun.CryptoHasher("sha256").update(packageBytes).digest("hex");
    const repo = createRepositories(db);
    await db`INSERT INTO plugin_packages(digest,package_bytes,manifest,signature,signing_key_id,uploaded_by) VALUES(${packageDigest},${packageBytes},${{ abi_version: 1 }}::jsonb,'synthetic-unpublished','synthetic','fixture')`;
    await repo.insert(pluginVersions).values({
      id: crypto.randomUUID(),
      name: "synthetic-upgrade",
      version: "1",
      packageDigest,
      abiVersion: 1,
      configSchemaDigest: "b".repeat(64),
    });
    const upstream = createUpstreamFundingStore(db);
    const financialBytes = Buffer.from("%PDF-1.7\nSynthetic upgrade financial evidence\n%%EOF");
    const voucher = await upstream.registerVoucher(
      {
        content: financialBytes,
        content_type: "application/pdf",
        digest: new Bun.CryptoHasher("sha256").update(financialBytes).digest("hex"),
        expires_at: new Date(Date.now() + 3600000).toISOString(),
      },
      "synthetic-upgrade-actor",
    );
    const upstreamAccount = (await db`SELECT account_id FROM channels WHERE id=${provider}`)[0]
      .account_id;
    const rechargeInput = {
      external_ref: "upgrade-real-recharge",
      payment_currency: "CNY",
      payment_atoms: "700000000000000000000",
      credited_currency: "USD",
      credited_atoms: "100000000000000000000",
      voucher_id: voucher.id,
      voucher_digest: voucher.digest,
      completed_at: "2026-01-01T00:00:00.123456Z",
      reason: "Synthetic completed recharge before schema upgrade",
    };
    const fundingReceipt = await upstream.recharge(
      upstreamAccount,
      rechargeInput,
      "synthetic-upgrade-actor",
    );
    const oldMigration =
      await db`SELECT version,checksum,manifest FROM schema_migrations ORDER BY version`;
    const tables = [
      "plugin_packages",
      "plugin_versions",
      "financial_vouchers",
      "upstream_accounts",
      "upstream_account_balances",
      "upstream_funding_operations",
      "upstream_journals",
      "upstream_entries",
      "upstream_audit_events",
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
      for (const table of tables) {
        const projection =
          table === "plugin_packages"
            ? "to_jsonb(t)-ARRAY['release_descriptor','release_signature','release_key_id']"
            : table === "gateway_requests"
              ? "to_jsonb(t)-ARRAY['execution_budget','execution_closed_at']"
              : table === "usage_events"
                ? "to_jsonb(t)-ARRAY['record_kind','snapshot_revision','final_dimensions']"
                : "to_jsonb(t)";
        values.push(
          (
            await db.unsafe(
              `SELECT jsonb_agg(${projection} ORDER BY ${projection}) AS rows FROM ${table} t`,
            )
          )[0].rows,
        );
      }
      return values;
    };
    const before = await snapshot();

    await db.unsafe(
      await Bun.file(new URL("../fixtures/runtime-role-2026111203.sql", import.meta.url)).text(),
    );
    for (const table of ["plugin_packages", "financial_vouchers", "administrator_credentials"]) {
      await expect(
        db.begin(async (tx) => {
          await tx`SET LOCAL ROLE gateway_runtime`;
          await tx.unsafe(`SELECT * FROM ${table}`);
        }),
      ).rejects.toMatchObject({ errno: "42501" });
    }
    await migrate(db);
    await db.unsafe(
      await Bun.file(new URL("../../infra/runtime-role.sql", import.meta.url)).text(),
    );
    for (const table of ["plugin_packages", "financial_vouchers", "administrator_credentials"]) {
      await expect(
        db.begin(async (tx) => {
          await tx`SET LOCAL ROLE gateway_runtime`;
          await tx.unsafe(`SELECT * FROM ${table}`);
        }),
      ).rejects.toMatchObject({ errno: "42501" });
    }

    expect(await db`SELECT version FROM schema_migrations ORDER BY version`).toEqual([
      { version: 2026111202 },
      { version: 2026111203 },
      { version: 2026111204 },
      { version: 2026111205 },
      { version: 2026111206 },
      { version: SCHEMA_VERSION },
    ]);
    expect(
      await db`SELECT version,checksum,manifest FROM schema_migrations WHERE version=2026111202`,
    ).toEqual(oldMigration);
    expect(
      (
        await db`SELECT release_descriptor,release_signature,release_key_id FROM plugin_packages`
      )[0],
    ).toEqual({ release_descriptor: null, release_signature: null, release_key_id: null });
    expect(
      (
        await db`SELECT count(*)::integer n FROM gateway_requests WHERE execution_budget IS NOT NULL OR execution_closed_at IS NOT NULL`
      )[0].n,
    ).toBe(0);
    expect(
      await db`SELECT record_kind,count(*)::integer n FROM usage_event_index WHERE snapshot_revision IS NULL AND final_dimensions IS NULL GROUP BY record_kind`,
    ).toEqual([
      {
        record_kind: "GATEWAY",
        n: (await db`SELECT count(*)::integer n FROM usage_event_index`)[0].n,
      },
    ]);
    expect(await snapshot()).toEqual(before);
    await migrate(db);
    expect(await snapshot()).toEqual(before);
    expect(
      await upstream.recharge(upstreamAccount, rechargeInput, "synthetic-retry-actor"),
    ).toEqual(fundingReceipt);
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

test("baseline replay preserves funded wallets and permits concurrent foreign-key readers", async () => {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL required");
  const name = `gateway_test_upgrade_${crypto.randomUUID().replaceAll("-", "")}`;
  const owner = createDatabase(base, 1);
  await owner.unsafe(`CREATE DATABASE ${name}`);
  const url = new URL(base);
  url.pathname = `/${name}`;
  let db = createDatabase(url.toString(), 1);
  try {
    await migrate(db);
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
      db`SELECT to_jsonb(w)::text AS row FROM wallets w WHERE project_id=${project}`;
    const before = await snapshot();
    await migrate(db);
    expect(
      (await db`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`)[0].version,
    ).toBe(SCHEMA_VERSION);
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

test("baseline preserves exact evidence and funds and archives verified monthly snapshots", async () => {
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
    await migrate(db);
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
      ceilings: { requests: "1" },
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
    await prepareEvidenceMonth(db, "2025-01");
    await db.begin(async (tx) => {
      await tx`ALTER TABLE usage_payloads ALTER CONSTRAINT usage_payloads_id_created_at_fkey DEFERRABLE INITIALLY DEFERRED`;
      await tx`ALTER TABLE journal_payloads ALTER CONSTRAINT journal_payloads_id_created_at_fkey DEFERRABLE INITIALLY DEFERRED`;
      await tx`ALTER TABLE usage_event_index DISABLE TRIGGER immutable_usage`;
      await tx`ALTER TABLE usage_payloads DISABLE TRIGGER immutable_usage_payload`;
      await tx`CREATE TEMP TABLE usage_dates ON COMMIT DROP AS SELECT id,'2025-01-15T00:00:00Z'::timestamptz+row_number() OVER(ORDER BY id DESC)*interval '1 microsecond' AS at FROM usage_event_index`;
      await tx`UPDATE usage_event_index e SET created_at=d.at FROM usage_dates d WHERE e.id=d.id`;
      await tx`UPDATE usage_payloads e SET created_at=d.at FROM usage_dates d WHERE e.id=d.id`;
      await tx`SET CONSTRAINTS ALL IMMEDIATE`;
      await tx`ALTER TABLE usage_event_index ENABLE TRIGGER immutable_usage`;
      await tx`ALTER TABLE usage_payloads ENABLE TRIGGER immutable_usage_payload`;
      await tx`ALTER TABLE money_entries DISABLE TRIGGER immutable_journal`;
      await tx`ALTER TABLE journal_payloads DISABLE TRIGGER immutable_journal_payload`;
      await tx`SET CONSTRAINTS ALL DEFERRED`;
      await tx`UPDATE money_entries SET created_at='2025-01-15T00:00:00Z'`;
      await tx`UPDATE journal_payloads SET created_at='2025-01-15T00:00:00Z'`;
      await tx`SET CONSTRAINTS ALL IMMEDIATE`;
      await tx`ALTER TABLE money_entries ENABLE TRIGGER immutable_journal`;
      await tx`ALTER TABLE journal_payloads ENABLE TRIGGER immutable_journal_payload`;
      await tx`SET CONSTRAINTS ALL IMMEDIATE`;
      await tx`ALTER TABLE usage_payloads ALTER CONSTRAINT usage_payloads_id_created_at_fkey NOT DEFERRABLE`;
      await tx`ALTER TABLE journal_payloads ALTER CONSTRAINT journal_payloads_id_created_at_fkey NOT DEFERRABLE`;
    });
    const snapshot = async () => ({
      usage: await db`SELECT to_jsonb(e)::text AS row FROM usage_events e ORDER BY id`,
      journal: await db`SELECT to_jsonb(e)::text AS row FROM money_journal e ORDER BY id`,
      wallets: await db`SELECT to_jsonb(e)::text AS row FROM wallets e ORDER BY id`,
      holds: await db`SELECT to_jsonb(e)::text AS row FROM billing_holds e ORDER BY id`,
    });
    const before = await snapshot();
    await migrate(db);
    expect(
      (await db`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`)[0].version,
    ).toBe(SCHEMA_VERSION);
    expect(await snapshot()).toEqual(before);
    expect(
      (
        await db`SELECT to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS.US') AS instant FROM usage_events ORDER BY created_at`
      ).map((row: { instant: string }) => row.instant),
    ).toEqual(["2025-01-15 00:00:00.000001", "2025-01-15 00:00:00.000002"]);
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
