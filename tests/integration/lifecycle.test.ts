import { test, expect } from "bun:test";
import {
  createCatalogStore,
  createDatabase,
  createEventStore,
  createRepositories,
  migrate,
  recordEventConsumer,
  createCustomerFundsStore,
  createUpstreamFundingStore,
  prepareEvidenceMonth,
  SCHEMA_VERSION,
  moveEvidenceMonth,
} from "@gateway/storage";
import { exportEvidenceArchive, verifyEvidenceArchive } from "../../scripts/evidence-archive.ts";
import { mkdir, rm, appendFile } from "node:fs/promises";
import { installV2Baseline } from "../support/schema-baseline.ts";
import {
  baselineAdmission,
  baselineChatFixture,
  baselineEvidence,
} from "../support/baseline-ledger.ts";
import { pluginVersions } from "@gateway/storage/schema";
import { seedHistoricalRecharge } from "../support/historical-funding.ts";

test("v2 through financial upgrade preserves settled receipts, ACKs and immutable plugin package, and an open baseline hold settles by explicit resolution", async () => {
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
    const funds = createCustomerFundsStore(db);
    const tenant = await createCatalogStore(db).createProject({
      name: "schema09-upgrade",
      rpm: 1000,
      tpm: 10000,
      concurrency: 100,
    });
    const fixture = await baselineChatFixture(db, tenant);
    const provider = fixture.provider;
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
    const admission = { ...fixture, project: tenant, tariffPricing: tariff.pricing_id };
    const first = await baselineAdmission(db, {
      ...admission,
      digest: "settled",
      fingerprint: "fixture",
    });
    const pending = await baselineAdmission(db, {
      ...admission,
      digest: "pending",
      fingerprint: "fixture",
    });
    expect(
      (await db`SELECT binding_id FROM attempts WHERE id=${first.attempt}`)[0].binding_id,
    ).toBeNull();
    await expect(
      db`UPDATE attempts SET binding_id=${crypto.randomUUID()} WHERE id=${first.attempt}`,
    ).rejects.toMatchObject({ errno: "23503" });
    const consumer = await db.begin((tx) => recordEventConsumer(tx, "fixture", false));
    const events = createEventStore(db);
    await baselineEvidence(db, first.attempt, "SUCCEEDED");
    const leaseToken = crypto.randomUUID();
    const leased =
      await db`UPDATE event_deliveries SET state='LEASED',lease_token=${leaseToken},lease_until=now()+interval '30 seconds',attempts=attempts+1 WHERE consumer_id=${consumer} AND state<>'ACKED' RETURNING event_id`;
    expect(leased).toHaveLength(1);
    expect(
      await events.ack(
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
    const fundingReceipt = await seedHistoricalRecharge(
      db,
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
    for (const role of ["gateway_runtime", "gateway_worker"])
      for (const table of [
        "wallets",
        "gateway_requests",
        "channels",
        "plugin_packages",
        "financial_vouchers",
        "administrator_credentials",
        "internal_service_instances",
      ])
        await expect(
          db.begin(async (tx) => {
            await tx.unsafe(`SET LOCAL ROLE ${role}`);
            await tx.unsafe(`SELECT * FROM ${table}`);
          }),
        ).rejects.toMatchObject({ errno: "42501" });
    expect(
      (
        await db`SELECT count(*)::integer n FROM pg_proc WHERE pronamespace='public'::regnamespace AND proname IN ('admit_gateway_batch','persist_usage_batch','ingest_initial_usage','commit_usage_evidence','record_usage_evidence','gateway_token_summary')`
      )[0].n,
    ).toBe(0);

    expect(await db`SELECT version FROM schema_migrations ORDER BY version`).toEqual([
      { version: 2026111202 },
      { version: 2026111203 },
      { version: 2026111204 },
      { version: 2026111205 },
      { version: 2026111206 },
      { version: 2026111207 },
      { version: 2026111208 },
      { version: 2026111209 },
      { version: 2026111210 },
      { version: 2026111211 },
      { version: 2026111212 },
      { version: 2026111213 },
      { version: 2026111214 },
      { version: 2026111215 },
      { version: 2026111216 },
      { version: 2026111217 },
      { version: 2026111218 },
      { version: 2026111219 },
      { version: 2026111220 },
      { version: 2026111221 },
      { version: 2026111222 },
      { version: 2026111223 },
      { version: 2026111224 },
      { version: 2026111225 },
      { version: 2026111226 },
      { version: 2026111701 },
      { version: 2026111702 },
      { version: 2026111703 },
      { version: 2026111704 },
      { version: 2026111705 },
      { version: 2026111706 },
      { version: 2026111707 },
      { version: 2026111801 },
      { version: 2026111802 },
      { version: 2026111803 },
      { version: 2026111804 },
      { version: 2026111805 },
      { version: 2026111806 },
      { version: 2026111807 },
      { version: 2026111808 },
      { version: 2026111809 },
      { version: 2026111810 },
      { version: 2026111811 },
      { version: 2026111812 },
      { version: 2026111813 },
      { version: 2026111814 },
      { version: 2026111815 },
      { version: 2026111816 },
      { version: 2026111817 },
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
        await tx`UPDATE gateway_requests SET billing_outcome='FAILED' WHERE id=${pending.request}`;
      }),
    ).rejects.toMatchObject({ errno: "42501" });
    await expect(
      db.begin(async (tx) => {
        await tx`SET LOCAL ROLE gateway_runtime`;
        await tx`UPDATE gateway_requests SET updated_at=updated_at WHERE id=${pending.request}`;
      }),
    ).rejects.toMatchObject({ errno: "42501" });
    expect(
      (
        await db`SELECT has_function_privilege('public','public.wallet_freeze_reason(boolean,boolean,numeric,numeric,numeric)','EXECUTE') AS public_execute,
          has_function_privilege('gateway_runtime','public.wallet_freeze_reason(boolean,boolean,numeric,numeric,numeric)','EXECUTE') AS runtime_execute,
          (SELECT proacl IS NOT NULL FROM pg_proc WHERE proname='wallet_freeze_reason') AS explicit_acl`
      )[0],
    ).toEqual({ public_execute: false, runtime_execute: false, explicit_acl: true });
    expect((await funds.wallets(tenant))[0]).toMatchObject({
      credit_limit_atoms: "0",
      credit_reserved_atoms: "0",
      credit_policy_version: "0",
    });
    await expect(
      db.begin(async (tx) => {
        await tx`SET LOCAL ROLE gateway_runtime`;
        await tx`SELECT configure_wallet_credit(${tenant}::uuid,'USD','upgraded-runtime',0,1)`;
      }),
    ).rejects.toMatchObject({ errno: "42501" });
    expect(
      await funds.configureCredit(tenant, "USD", {
        external_ref: "upgraded-management",
        expected_version: "0",
        credit_limit_atoms: "1",
      }),
    ).toMatchObject({ credit_limit_atoms: "1", credit_policy_version: "1" });
    await expect(
      db.begin(async (tx) => {
        await tx`SET LOCAL ROLE gateway_runtime`;
        await tx`UPDATE wallets SET credit_limit_atoms=100 WHERE project_id=${tenant}`;
      }),
    ).rejects.toMatchObject({ errno: "42501" });
    expect(
      await funds.resolve(pending.request, {
        external_ref: "upgraded-settlement",
        action: "SETTLE",
        usage: { requests: "1" },
        pricing_id: tariff.pricing_id,
        reason: "EVIDENCE_RECONCILIATION",
      }),
    ).toMatchObject({ replayed: false });
    expect(await funds.reconcileKnownHolds()).toBe(1);
    expect((await funds.wallets(tenant))[0]).toMatchObject({
      available_atoms: "994",
      reserved_atoms: "0",
      spent_atoms: "6",
    });
    const next = await events.claim(consumer, 10);
    expect(next.events).toEqual([]);
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
    let funds = createCustomerFundsStore(db);
    const project = await createCatalogStore(db).createProject({
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
    funds = createCustomerFundsStore(db);
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

test("evidence written at the v2 baseline survives the upgrade, closes by explicit resolution and archives verified monthly snapshots", async () => {
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
    await installV2Baseline(db);
    const funds = createCustomerFundsStore(db);
    const project = await createCatalogStore(db).createProject({
      name: "upgrade",
      rpm: 1000,
      tpm: 100000,
      concurrency: 100,
    });
    const fixture = await baselineChatFixture(db, project);
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
    const admission = { ...fixture, project, tariffPricing: tariff.pricing_id };
    const first = await baselineAdmission(db, {
      ...admission,
      digest: "same-intent",
      fingerprint: "fixture",
    });
    await baselineEvidence(db, first.attempt, "SUCCEEDED");
    const second = await baselineAdmission(db, {
      ...admission,
      digest: null,
      fingerprint: "unknown",
    });
    await baselineEvidence(db, second.attempt, "UNKNOWN");
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
      usage:
        await db`SELECT (to_jsonb(e)-ARRAY['record_kind','snapshot_revision','final_dimensions'])::text AS row FROM usage_events e ORDER BY id`,
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
    await funds.resolve(second.request, {
      external_ref: "upgraded-settlement",
      action: "SETTLE",
      usage: { requests: "1" },
      pricing_id: tariff.pricing_id,
      reason: "EVIDENCE_RECONCILIATION",
    });
    expect(await funds.reconcileKnownHolds()).toBe(1);
    expect(
      (await db`SELECT state FROM billing_holds WHERE request_id=${second.request}`)[0].state,
    ).toBe("SETTLED");
    await expect(prepareEvidenceMonth(db, "2025-01")).rejects.toThrow("--quiesced");
    await prepareEvidenceMonth(db, "2025-01", true);
    expect((await db`SELECT count(*)::int AS n FROM usage_payloads_202501`)[0].n).toBe(2);
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
