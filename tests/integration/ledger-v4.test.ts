import { expect, test } from "bun:test";
import { appendFile, rm } from "node:fs/promises";
import type { SQL } from "bun";
import { getTableConfig } from "drizzle-orm/pg-core";
import {
  assertSchemaVersion,
  createCatalogRepository,
  createDatabase,
  createLedgerEngine,
  createLedgerQueries,
  exportLedgerArchive,
  ledgerSchema,
  migrate,
  prepareLedgerMonth,
  SCHEMA_VERSION,
  verifyLedgerArchive,
} from "@gateway/ledger";

async function withLedgerDb(run: (db: SQL) => Promise<void>): Promise<void> {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL required");
  const name = `gateway_v4_ledger_${crypto.randomUUID().replaceAll("-", "")}`;
  const owner = createDatabase(base, 1);
  await owner.unsafe(`CREATE DATABASE ${name}`);
  const url = new URL(base);
  url.pathname = `/${name}`;
  const db = createDatabase(url.toString(), 16);
  try {
    await migrate(db);
    await run(db);
  } finally {
    await db.close({ timeout: 1 });
    await owner.unsafe(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await owner.close({ timeout: 1 });
  }
}

async function seedFixture(db: SQL) {
  const catalog = createCatalogRepository(db);
  const ledger = createLedgerEngine(db, { batchWindowMs: 2, maxBatchSize: 64 });
  const project = await catalog.upsertProject({
    name: "ledger-fixture",
    currency: "PTS",
    rpm: 10000,
    tpm: 10000000,
    concurrency: 1000,
  });
  const accountA = await catalog.upsertUpstreamAccount({ name: `acct-a-${crypto.randomUUID()}` });
  const accountB = await catalog.upsertUpstreamAccount({ name: `acct-b-${crypto.randomUUID()}` });
  const accountC = await catalog.upsertUpstreamAccount({ name: `acct-c-${crypto.randomUUID()}` });
  const channelA = await catalog.upsertChannel({
    account_id: accountA.id,
    name: "chan-a",
    kind: "openai",
    base_url: "https://a.invalid/v1",
    secret_ciphertext: "enc-a",
    rpm: 10000,
    concurrency: 1000,
  });
  const channelB = await catalog.upsertChannel({
    account_id: accountB.id,
    name: "chan-b",
    kind: "openai",
    base_url: "https://b.invalid/v1",
    secret_ciphertext: "enc-b",
    rpm: 10000,
    concurrency: 1000,
  });
  const channelC = await catalog.upsertChannel({
    account_id: accountC.id,
    name: "chan-c",
    kind: "openai",
    base_url: "https://c.invalid/v1",
    secret_ciphertext: "enc-c",
    rpm: 10000,
    concurrency: 1000,
  });
  await catalog.upsertModel({
    name: "gpt-4o",
    operations: ["chat"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 4096,
  });
  const billingPrice = await catalog.createPriceVersion({
    side: "CUSTOMER",
    currency: "PTS",
    basis: "PROVIDER_USAGE",
    rates: {
      input_tokens: { numerator: "1", denominator: "3" },
      output_tokens: { numerator: "1", denominator: "3" },
    },
  });
  const flatBillingPrice = await catalog.createPriceVersion({
    side: "CUSTOMER",
    currency: "PTS",
    basis: "PROVIDER_USAGE",
    rates: {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "1", denominator: "1" },
    },
  });
  const costPrice = await catalog.createPriceVersion({
    side: "SUPPLIER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    rates: {
      input_tokens: { numerator: "1", denominator: "2" },
      output_tokens: { numerator: "1", denominator: "2" },
    },
  });
  await ledger.rechargeSupplier({
    account_id: accountA.id,
    currency: "USD",
    external_ref: "init-a",
    amount_atoms: "1000000",
  });
  await ledger.rechargeSupplier({
    account_id: accountB.id,
    currency: "USD",
    external_ref: "init-b",
    amount_atoms: "1000000",
  });
  await ledger.rechargeSupplier({
    account_id: accountC.id,
    currency: "USD",
    external_ref: "init-c",
    amount_atoms: "1000000",
  });
  return {
    catalog,
    ledger,
    project,
    accountA,
    accountB,
    accountC,
    channelA,
    channelB,
    channelC,
    billingPrice,
    flatBillingPrice,
    costPrice,
  };
}

test("G1.1 exact rational pricing, multi-lot priority, and expired hold surplus never resurrects", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await migrate(db);
    expect((await db`SELECT max(version) AS v FROM public.schema_migrations`)[0].v).toBe(
      SCHEMA_VERSION,
    );

    const expiryIso = new Date(Date.now() + 60_000).toISOString();
    const grant = await f.ledger.creditWallet({
      project_id: f.project.id,
      currency: "PTS",
      external_ref: "grant-expiring",
      amount_atoms: "50",
      kind: "GRANT",
      priority: 10,
      expires_at: expiryIso,
      models: ["gpt-4o"],
      operations: ["chat"],
    });
    await f.ledger.creditWallet({
      project_id: f.project.id,
      currency: "PTS",
      external_ref: "cash-permanent",
      amount_atoms: "100",
      kind: "CASH",
      priority: 100,
    });

    const reqId = crypto.randomUUID();
    const attId = crypto.randomUUID();
    const held = await f.ledger.acquireHold({
      request_id: reqId,
      attempt_id: attId,
      project_id: f.project.id,
      currency: "PTS",
      operation: "chat",
      model: "gpt-4o",
      idempotency_key: "idem-1",
      fingerprint: "fp-1",
      customer_atoms: "80",
      supplier_account_id: f.accountA.id,
      supplier_channel_id: f.channelA.id,
      supplier_currency: "USD",
      supplier_atoms: "40",
      expires_at: new Date(Date.now() + 30000).toISOString(),
    });
    expect(held.ok).toBe(true);

    const replay = await f.ledger.acquireHold({
      request_id: crypto.randomUUID(),
      attempt_id: crypto.randomUUID(),
      project_id: f.project.id,
      currency: "PTS",
      operation: "chat",
      model: "gpt-4o",
      idempotency_key: "idem-1",
      fingerprint: "fp-1",
      customer_atoms: "80",
      supplier_account_id: f.accountA.id,
      supplier_channel_id: f.channelA.id,
      supplier_currency: "USD",
      supplier_atoms: "40",
      expires_at: new Date(Date.now() + 30000).toISOString(),
    });
    expect(replay).toMatchObject({ ok: true, replayed: true, request_id: reqId });

    const conflict = await f.ledger.acquireHold({
      request_id: crypto.randomUUID(),
      attempt_id: crypto.randomUUID(),
      project_id: f.project.id,
      currency: "PTS",
      operation: "chat",
      model: "gpt-4o",
      idempotency_key: "idem-1",
      fingerprint: "fp-different",
      customer_atoms: "80",
      supplier_account_id: f.accountA.id,
      supplier_channel_id: f.channelA.id,
      supplier_currency: "USD",
      supplier_atoms: "40",
      expires_at: new Date(Date.now() + 30000).toISOString(),
    });
    expect(conflict).toMatchObject({ ok: false, code: "idempotency_conflict" });

    await db`UPDATE public.wallet_lots SET expires_at = now() - interval '1 second' WHERE id = ${grant.lot_id}`;

    const settled = await f.ledger.commitTerminal({
      request_id: reqId,
      selected_attempt_id: attId,
      channel_id: f.channelA.id,
      billing_price_id: f.billingPrice.id,
      cost_version_id: f.costPrice.id,
      usage: { input_tokens: "1", output_tokens: "1" },
      billing_pricing: { currency: "PTS", rates: f.billingPrice.rates },
      billing_basis: "PROVIDER_USAGE",
      supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
      supplier_currency: "USD",
    });
    expect(settled).toMatchObject({
      ok: true,
      replayed: false,
      outcome: "SETTLED",
      customer_atoms: "1",
      funded_atoms: "1",
      debt_atoms: "0",
      supplier_cost_atoms: "1",
    });

    const wallet = await f.ledger.getWallet(f.project.id, "PTS");
    expect(wallet).toMatchObject({
      available_atoms: "100",
      reserved_atoms: "0",
      spent_atoms: "1",
      expired_atoms: "49",
      debt_atoms: "0",
    });

    const audit = await f.ledger.auditConservation();
    expect(audit).toEqual({
      ok: true,
      unbalanced_journals: 0,
      broken_lots: 0,
      wallet_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
  }));

test("G1.2 reverse-order lot refund, debt waiver, auto debt collection, and NUMERIC(38,0) precision", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.configureWalletCredit({
      project_id: f.project.id,
      currency: "PTS",
      expected_version: "0",
      credit_limit_atoms: "1000",
    });
    await f.ledger.creditWallet({
      project_id: f.project.id,
      currency: "PTS",
      external_ref: "cash-1",
      amount_atoms: "9007199254740993",
      kind: "CASH",
    });
    const wBig = await f.ledger.getWallet(f.project.id, "PTS");
    expect(wBig?.available_atoms).toBe("9007199254740993");

    const proj2 = await f.catalog.upsertProject({
      name: "debt-project",
      currency: "PTS",
      rpm: 1000,
      tpm: 1000000,
      concurrency: 100,
    });
    await f.ledger.configureWalletCredit({
      project_id: proj2.id,
      currency: "PTS",
      expected_version: "0",
      credit_limit_atoms: "50",
    });
    await f.ledger.creditWallet({
      project_id: proj2.id,
      currency: "PTS",
      external_ref: "small-cash",
      amount_atoms: "100",
      kind: "CASH",
    });

    const reqId = crypto.randomUUID();
    const attId = crypto.randomUUID();
    const hold = await f.ledger.acquireHold({
      request_id: reqId,
      attempt_id: attId,
      project_id: proj2.id,
      currency: "PTS",
      operation: "chat",
      model: "gpt-4o",
      idempotency_key: null,
      fingerprint: "fp-debt",
      customer_atoms: "120",
      supplier_account_id: f.accountA.id,
      supplier_channel_id: f.channelA.id,
      supplier_currency: "USD",
      supplier_atoms: "50",
      expires_at: new Date(Date.now() + 30000).toISOString(),
    });
    expect(hold).toMatchObject({ ok: true, held_atoms: "120", credit_atoms: "20" });

    const settled = await f.ledger.commitTerminal({
      request_id: reqId,
      selected_attempt_id: attId,
      channel_id: f.channelA.id,
      billing_price_id: f.flatBillingPrice.id,
      cost_version_id: f.costPrice.id,
      usage: { input_tokens: "100", output_tokens: "80" },
      billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
      billing_basis: "PROVIDER_USAGE",
      supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
      supplier_currency: "USD",
    });
    expect(settled).toMatchObject({
      ok: true,
      outcome: "DEBT",
      customer_atoms: "180",
      funded_atoms: "100",
      debt_atoms: "80",
    });
    if (!settled.ok) throw new Error("unreachable");

    const frozenWallet = await f.ledger.getWallet(proj2.id, "PTS");
    expect(frozenWallet?.freeze_reason).toBe("DEBT_EXCEEDS_CREDIT_LIMIT");

    const refunded = await f.ledger.refundCharge({
      charge_id: settled.charge_id,
      external_ref: "ref-1",
      amount_atoms: "50",
      reason: "CUSTOMER_REFUND",
    });
    expect(refunded).toMatchObject({
      replayed: false,
      refunded_atoms: "50",
      waived_debt_atoms: "50",
      expired_atoms: "0",
    });

    const topup = await f.ledger.creditWallet({
      project_id: proj2.id,
      currency: "PTS",
      external_ref: "pay-debt",
      amount_atoms: "100",
      kind: "CASH",
    });
    expect(topup.debt_collected_charges).toBe(1);
    const afterCollect = await f.ledger.getWallet(proj2.id, "PTS");
    expect(afterCollect).toMatchObject({
      available_atoms: "70",
      debt_atoms: "0",
      spent_atoms: "130",
      freeze_reason: null,
    });
    expect((await f.ledger.auditConservation()).ok).toBe(true);
  }));

test("G1.3 100 concurrent micro-batched holds on one single hot wallet have zero 55P03 errors and exact conservation", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      project_id: f.project.id,
      currency: "PTS",
      external_ref: "hot-wallet-fund",
      amount_atoms: "5000",
      kind: "CASH",
    });

    const requests = Array.from({ length: 100 }, (_, i) => ({
      reqId: crypto.randomUUID(),
      attId: crypto.randomUUID(),
      fp: `hot-${i}`,
    }));

    const holds = await Promise.all(
      requests.map((r) =>
        f.ledger.acquireHold({
          request_id: r.reqId,
          attempt_id: r.attId,
          project_id: f.project.id,
          currency: "PTS",
          operation: "chat",
          model: "gpt-4o",
          idempotency_key: null,
          fingerprint: r.fp,
          customer_atoms: "100",
          supplier_account_id: f.accountA.id,
          supplier_channel_id: f.channelA.id,
          supplier_currency: "USD",
          supplier_atoms: "50",
          expires_at: new Date(Date.now() + 30000).toISOString(),
        }),
      ),
    );

    const succeeded = holds
      .map((res, idx) => ({ res, req: requests[idx]! }))
      .filter((x) => x.res.ok);
    const rejected = holds.filter((res) => !res.ok);
    expect(succeeded).toHaveLength(50);
    expect(rejected).toHaveLength(50);
    for (const r of rejected) expect(r).toMatchObject({ ok: false, code: "insufficient_balance" });

    const midWallet = await f.ledger.getWallet(f.project.id, "PTS");
    expect(midWallet).toMatchObject({ available_atoms: "0", reserved_atoms: "5000" });

    const commits = await Promise.all(
      succeeded.map(({ req }) =>
        f.ledger.commitTerminal({
          request_id: req.reqId,
          selected_attempt_id: req.attId,
          channel_id: f.channelA.id,
          billing_price_id: f.flatBillingPrice.id,
          cost_version_id: f.costPrice.id,
          usage: { input_tokens: "40", output_tokens: "30" },
          billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
          billing_basis: "PROVIDER_USAGE",
          supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
          supplier_currency: "USD",
        }),
      ),
    );
    expect(commits.every((c) => c.ok && !c.replayed && c.customer_atoms === "70")).toBe(true);

    const endWallet = await f.ledger.getWallet(f.project.id, "PTS");
    expect(endWallet).toMatchObject({
      available_atoms: "1500",
      reserved_atoms: "0",
      spent_atoms: "3500",
      debt_atoms: "0",
    });
    const sup = await f.ledger.getSupplierBalance(f.accountA.id, "USD");
    expect(sup).toEqual({
      balance_atoms: "998250",
      reserved_atoms: "0",
      spent_atoms: "1750",
    });

    const claimed = await f.ledger.claimOutboxEvents("business-consumer", 100);
    expect(claimed.events).toHaveLength(50);
    expect(
      await f.ledger.ackOutboxEvents(
        "business-consumer",
        claimed.lease_token,
        claimed.events.at(-1)!.seq,
      ),
    ).toBe(true);
    expect((await f.ledger.claimOutboxEvents("business-consumer", 100)).events).toHaveLength(0);

    expect(await f.ledger.auditConservation()).toEqual({
      ok: true,
      unbalanced_journals: 0,
      broken_lots: 0,
      wallet_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
  }));

test("G1.4 dynamic failover supplier switch, customer hold top-up, and UNKNOWN supplier preservation", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      project_id: f.project.id,
      currency: "PTS",
      external_ref: "failover-fund",
      amount_atoms: "500",
      kind: "CASH",
    });

    const reqId = crypto.randomUUID();
    const attA = crypto.randomUUID();
    const attB = crypto.randomUUID();
    const attC = crypto.randomUUID();

    const hold = await f.ledger.acquireHold({
      request_id: reqId,
      attempt_id: attA,
      project_id: f.project.id,
      currency: "PTS",
      operation: "chat",
      model: "gpt-4o",
      idempotency_key: null,
      fingerprint: "fp-failover",
      customer_atoms: "100",
      supplier_account_id: f.accountA.id,
      supplier_channel_id: f.channelA.id,
      supplier_currency: "USD",
      supplier_atoms: "50",
      expires_at: new Date(Date.now() + 30000).toISOString(),
    });
    expect(hold.ok).toBe(true);

    const sw1 = await f.ledger.switchSupplierAttempt({
      request_id: reqId,
      failed_attempt_id: attA,
      failed_outcome: "FAILED",
      next_attempt_id: attB,
      next_account_id: f.accountB.id,
      next_channel_id: f.channelB.id,
      next_currency: "USD",
      next_supplier_atoms: "60",
      required_customer_atoms: "150",
    });
    expect(sw1).toMatchObject({ ok: true, held_atoms: "150" });
    expect((await f.ledger.getSupplierBalance(f.accountA.id, "USD")).reserved_atoms).toBe("0");
    expect((await f.ledger.getSupplierBalance(f.accountB.id, "USD")).reserved_atoms).toBe("60");

    const sw2 = await f.ledger.switchSupplierAttempt({
      request_id: reqId,
      failed_attempt_id: attB,
      failed_outcome: "UNKNOWN",
      next_attempt_id: attC,
      next_account_id: f.accountC.id,
      next_channel_id: f.channelC.id,
      next_currency: "USD",
      next_supplier_atoms: "70",
    });
    expect(sw2.ok).toBe(true);
    expect((await f.ledger.getSupplierBalance(f.accountB.id, "USD")).reserved_atoms).toBe("60");
    expect((await f.ledger.getSupplierBalance(f.accountC.id, "USD")).reserved_atoms).toBe("70");

    const settled = await f.ledger.commitTerminal({
      request_id: reqId,
      selected_attempt_id: attC,
      channel_id: f.channelC.id,
      billing_price_id: f.flatBillingPrice.id,
      cost_version_id: f.costPrice.id,
      usage: { input_tokens: "60", output_tokens: "60" },
      billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
      billing_basis: "PROVIDER_USAGE",
      supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
      supplier_currency: "USD",
    });
    expect(settled).toMatchObject({
      ok: true,
      customer_atoms: "120",
      supplier_cost_atoms: "60",
    });

    expect(await f.ledger.getWallet(f.project.id, "PTS")).toMatchObject({
      available_atoms: "380",
      reserved_atoms: "0",
      spent_atoms: "120",
    });
    expect(await f.ledger.getSupplierBalance(f.accountA.id, "USD")).toEqual({
      balance_atoms: "1000000",
      reserved_atoms: "0",
      spent_atoms: "0",
    });
    expect(await f.ledger.getSupplierBalance(f.accountB.id, "USD")).toEqual({
      balance_atoms: "1000000",
      reserved_atoms: "60",
      spent_atoms: "0",
    });
    expect(await f.ledger.getSupplierBalance(f.accountC.id, "USD")).toEqual({
      balance_atoms: "999940",
      reserved_atoms: "0",
      spent_atoms: "60",
    });
    expect((await f.ledger.auditConservation()).ok).toBe(true);
  }));

test("G1.5 moderation PLATFORM_ABSORB, DB immutability/balance triggers, and monthly archive verification", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      project_id: f.project.id,
      currency: "PTS",
      external_ref: "mod-fund",
      amount_atoms: "300",
      kind: "CASH",
    });

    const reqId = crypto.randomUUID();
    const attId = crypto.randomUUID();
    await f.ledger.acquireHold({
      request_id: reqId,
      attempt_id: attId,
      project_id: f.project.id,
      currency: "PTS",
      operation: "chat",
      model: "gpt-4o",
      idempotency_key: null,
      fingerprint: "fp-mod",
      customer_atoms: "100",
      supplier_account_id: f.accountA.id,
      supplier_channel_id: f.channelA.id,
      supplier_currency: "USD",
      supplier_atoms: "50",
      expires_at: new Date(Date.now() + 30000).toISOString(),
    });

    const blocked = await f.ledger.commitTerminal({
      request_id: reqId,
      selected_attempt_id: attId,
      channel_id: f.channelA.id,
      billing_price_id: f.flatBillingPrice.id,
      cost_version_id: f.costPrice.id,
      usage: { input_tokens: "30", output_tokens: "30" },
      billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
      billing_basis: "PROVIDER_USAGE",
      supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
      supplier_currency: "USD",
      charge_policy: "PLATFORM_ABSORB",
      moderation_blocked: true,
      plugin_audit: [
        {
          plugin_id: "mod-1",
          version: "1.0.0",
          hook: "on_response",
          action: "BLOCK",
          rule_code: "PII_BLOCKED",
          latency_us: 45,
        },
      ],
    });
    expect(blocked).toMatchObject({
      ok: true,
      outcome: "MODERATION_BLOCKED",
      customer_atoms: "0",
      funded_atoms: "0",
      supplier_cost_atoms: "30",
    });
    expect(await f.ledger.getWallet(f.project.id, "PTS")).toMatchObject({
      available_atoms: "300",
      reserved_atoms: "0",
      spent_atoms: "0",
    });
    expect((await f.ledger.getSupplierBalance(f.accountA.id, "USD")).spent_atoms).toBe("30");

    const badJournal = crypto.randomUUID();
    await expect(
      db.begin(async (tx) => {
        await tx`
          INSERT INTO public.ledger_entries (journal_id, ordinal, side, entity_id, reference, kind, currency, account, atoms)
          VALUES (${badJournal}, 0, 'CUSTOMER', ${f.project.id}, 'bad-1', 'ADJUST', 'PTS', 'system:revenue', 100)
        `;
      }),
    ).rejects.toThrow("unbalanced ledger journal");

    await expect(db`DELETE FROM public.ledger_entries`).rejects.toThrow("immutable record");
    await expect(db`UPDATE public.price_versions SET currency = 'EUR'`).rejects.toThrow(
      "immutable record",
    );

    const month = new Date().toISOString().slice(0, 7);
    await prepareLedgerMonth(db, month);
    const archiveDir = new URL(`../../.local/v4-archive-${crypto.randomUUID()}/`, import.meta.url)
      .pathname;
    try {
      const manifestPath = await exportLedgerArchive(db, month, archiveDir);
      const verified = await verifyLedgerArchive(manifestPath);
      expect(verified.files.find((x) => x.kind === "charges")?.rows).toBe(1);
      expect(verified.files.find((x) => x.kind === "entries")?.rows).toBeGreaterThanOrEqual(4);
      await appendFile(`${archiveDir}/entries.ndjson.gz`, "tampered");
      await expect(verifyLedgerArchive(manifestPath)).rejects.toThrow("checksum");
    } finally {
      await rm(archiveDir, { recursive: true, force: true });
    }

    expect((await f.ledger.auditConservation()).ok).toBe(true);
  }));

test("G1.6 Drizzle schema declarations match migrated PostgreSQL tables and columns 1-to-1 with zero drift", () =>
  withLedgerDb(async (db) => {
    expect(await assertSchemaVersion(db)).toBe(SCHEMA_VERSION);
    const pgRows = await db`
      SELECT c.table_name, c.column_name
      FROM information_schema.columns c
      JOIN pg_class cl ON cl.relname = c.table_name
      JOIN pg_namespace n ON n.oid = cl.relnamespace AND n.nspname = c.table_schema
      WHERE c.table_schema = 'public'
        AND NOT cl.relispartition
      ORDER BY c.table_name, c.column_name
    `;
    const actualByTable = new Map<string, string[]>();
    for (const row of pgRows) {
      const t = String(row.table_name);
      const col = String(row.column_name);
      const arr = actualByTable.get(t);
      if (arr) arr.push(col);
      else actualByTable.set(t, [col]);
    }

    const declaredByTable = new Map<string, string[]>();
    for (const table of Object.values(ledgerSchema)) {
      const cfg = getTableConfig(table);
      declaredByTable.set(cfg.name, cfg.columns.map((c) => c.name).sort());
    }

    expect([...declaredByTable.keys()].sort()).toEqual([...actualByTable.keys()].sort());
    for (const [tableName, cols] of declaredByTable) {
      expect(actualByTable.get(tableName)).toEqual(cols);
    }
  }));

test("G1.7 sharded wallet & supplier leases settle 200 concurrent requests via fast path with batch journal traceability and clean drain", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const queries = createLedgerQueries(db);
    await f.ledger.creditWallet({
      project_id: f.project.id,
      currency: "PTS",
      external_ref: "lease-fund-1",
      amount_atoms: "100000000",
      kind: "CASH",
    });
    await f.ledger.rechargeSupplier({
      account_id: f.accountA.id,
      currency: "USD",
      external_ref: "lease-sup-1",
      amount_atoms: "100000000",
    });

    const totalReqs = 200;
    const reqs = Array.from({ length: totalReqs }, () => ({
      reqId: crypto.randomUUID(),
      attId: crypto.randomUUID(),
    }));

    const firstWave = reqs.slice(0, 16);
    const firstHolds = await Promise.all(
      firstWave.map((r) =>
        f.ledger.acquireHold({
          request_id: r.reqId,
          attempt_id: r.attId,
          project_id: f.project.id,
          currency: "PTS",
          operation: "chat",
          model: "gpt-4o",
          idempotency_key: null,
          fingerprint: "",
          customer_atoms: "200",
          supplier_account_id: f.accountA.id,
          supplier_channel_id: f.channelA.id,
          supplier_currency: "USD",
          supplier_atoms: "100",
          expires_at: new Date(Date.now() + 30000).toISOString(),
        }),
      ),
    );
    expect(firstHolds.every((h) => h.ok)).toBe(true);

    const activeWlCount = Number(
      (await db`SELECT count(*)::int AS n FROM public.wallet_leases WHERE state = 'ACTIVE'`)[0]!.n,
    );
    const activeSlCount = Number(
      (await db`SELECT count(*)::int AS n FROM public.supplier_leases WHERE state = 'ACTIVE'`)[0]!
        .n,
    );
    expect(activeWlCount).toBe(1);
    expect(activeSlCount).toBe(1);

    const secondWave = reqs.slice(16);
    const secondHolds = await Promise.all(
      secondWave.map((r) =>
        f.ledger.acquireHold({
          request_id: r.reqId,
          attempt_id: r.attId,
          project_id: f.project.id,
          currency: "PTS",
          operation: "chat",
          model: "gpt-4o",
          idempotency_key: null,
          fingerprint: "",
          customer_atoms: "200",
          supplier_account_id: f.accountA.id,
          supplier_channel_id: f.channelA.id,
          supplier_currency: "USD",
          supplier_atoms: "100",
          expires_at: new Date(Date.now() + 30000).toISOString(),
        }),
      ),
    );
    expect(secondHolds.every((h) => h.ok)).toBe(true);

    const leasedHoldRows = Number(
      (
        await db`SELECT count(*)::int AS n FROM public.billing_holds WHERE wallet_lease_id IS NOT NULL`
      )[0]!.n,
    );
    expect(leasedHoldRows).toBe(secondWave.length);

    const commits = await Promise.all(
      reqs.map((r) =>
        f.ledger.commitTerminal({
          request_id: r.reqId,
          selected_attempt_id: r.attId,
          channel_id: f.channelA.id,
          billing_price_id: f.flatBillingPrice.id,
          cost_version_id: f.costPrice.id,
          usage: { input_tokens: "60", output_tokens: "40" },
          billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
          billing_basis: "PROVIDER_USAGE",
          supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
          supplier_currency: "USD",
        }),
      ),
    );
    expect(
      commits.every((c) => c.ok && c.customer_atoms === "100" && c.supplier_cost_atoms === "50"),
    ).toBe(true);

    const sampleReqId = secondWave[0]!.reqId;
    const reqDetail = await queries.getRequestBilling(sampleReqId);
    expect(reqDetail.charge?.customerJournalId).toBeTruthy();
    expect(reqDetail.charge?.supplierJournalId).toBeTruthy();
    expect(reqDetail.entries.length).toBeGreaterThanOrEqual(4);

    const entryList = await queries.listLedgerEntries({ request_id: sampleReqId });
    expect(entryList.total).toBeGreaterThanOrEqual(4);

    await f.ledger.drain();

    const remainingActiveLeases = Number(
      (
        await db`
          SELECT (
            (SELECT count(*) FROM public.wallet_leases WHERE state = 'ACTIVE') +
            (SELECT count(*) FROM public.supplier_leases WHERE state = 'ACTIVE')
          )::int AS n
        `
      )[0]!.n,
    );
    expect(remainingActiveLeases).toBe(0);

    const wallet = await f.ledger.getWallet(f.project.id, "PTS");
    expect(wallet).toMatchObject({
      available_atoms: String(100_000_000 - totalReqs * 100),
      reserved_atoms: "0",
      spent_atoms: String(totalReqs * 100),
    });
    const sup = await f.ledger.getSupplierBalance(f.accountA.id, "USD");
    expect(sup).toEqual({
      balance_atoms: String(101_000_000 - totalReqs * 50),
      reserved_atoms: "0",
      spent_atoms: String(totalReqs * 50),
    });
    expect(await f.ledger.auditConservation()).toEqual({
      ok: true,
      unbalanced_journals: 0,
      broken_lots: 0,
      wallet_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
  }));

test("G1.8 expired lot inside lease never resurrects and Worker sweepExpiredLeases recovers dead node leases after SIGKILL", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const shortGrant = await f.ledger.creditWallet({
      project_id: f.project.id,
      currency: "PTS",
      external_ref: "lease-grant-expiring",
      amount_atoms: "1000000",
      kind: "GRANT",
      priority: 10,
      expires_at: new Date(Date.now() + 60_000).toISOString(),
    });
    await f.ledger.creditWallet({
      project_id: f.project.id,
      currency: "PTS",
      external_ref: "lease-cash-perm",
      amount_atoms: "50000000",
      kind: "CASH",
      priority: 100,
    });
    await f.ledger.rechargeSupplier({
      account_id: f.accountA.id,
      currency: "USD",
      external_ref: "lease-sup-crash",
      amount_atoms: "50000000",
    });

    const promoReqs = Array.from({ length: 8 }, () => ({
      reqId: crypto.randomUUID(),
      attId: crypto.randomUUID(),
    }));
    await Promise.all(
      promoReqs.map((r) =>
        f.ledger.acquireHold({
          request_id: r.reqId,
          attempt_id: r.attId,
          project_id: f.project.id,
          currency: "PTS",
          operation: "chat",
          model: "gpt-4o",
          idempotency_key: null,
          fingerprint: "",
          customer_atoms: "200",
          supplier_account_id: f.accountA.id,
          supplier_channel_id: f.channelA.id,
          supplier_currency: "USD",
          supplier_atoms: "100",
          expires_at: new Date(Date.now() + 30000).toISOString(),
        }),
      ),
    );
    await Promise.all(promoReqs.map((r) => f.ledger.releaseHold(r.reqId)));

    const crashReqs = Array.from({ length: 4 }, () => ({
      reqId: crypto.randomUUID(),
      attId: crypto.randomUUID(),
    }));
    const crashHolds = await Promise.all(
      crashReqs.map((r) =>
        f.ledger.acquireHold({
          request_id: r.reqId,
          attempt_id: r.attId,
          project_id: f.project.id,
          currency: "PTS",
          operation: "chat",
          model: "gpt-4o",
          idempotency_key: null,
          fingerprint: "",
          customer_atoms: "200",
          supplier_account_id: f.accountA.id,
          supplier_channel_id: f.channelA.id,
          supplier_currency: "USD",
          supplier_atoms: "100",
          expires_at: new Date(Date.now() + 30000).toISOString(),
        }),
      ),
    );
    expect(crashHolds.every((h) => h.ok)).toBe(true);

    await db`UPDATE public.wallet_lots SET expires_at = now() - interval '1 second' WHERE id = ${shortGrant.lot_id}`;
    await db`UPDATE public.billing_holds SET expires_at = now() - interval '1 second' WHERE state = 'HELD'`;
    await db`UPDATE public.wallet_leases SET expires_at = now() - interval '1 second' WHERE state = 'ACTIVE'`;
    await db`UPDATE public.supplier_leases SET expires_at = now() - interval '1 second' WHERE state = 'ACTIVE'`;

    const workerLedger = createLedgerEngine(db, { enableLeases: false });
    const sweptHolds = await workerLedger.sweepTimedOutHolds(100);
    const sweptLeases = await workerLedger.sweepExpiredLeases(100);
    const expiredLotWallets = await workerLedger.expireAllLots(50);
    expect(sweptHolds).toBe(4);
    expect(sweptLeases).toBe(2);
    expect(expiredLotWallets).toBe(1);

    const midWallet = await workerLedger.getWallet(f.project.id, "PTS");
    expect(midWallet?.reserved_atoms).toBe("800");
    expect(BigInt(midWallet!.expired_atoms)).toBeGreaterThan(0n);

    for (let i = 0; i < crashReqs.length; i++) {
      if (i < 2) {
        await workerLedger.resolveRequest({
          request_id: crashReqs[i]!.reqId,
          action: "RELEASE",
          reason: "verified_not_sent",
          actor: "ops",
        });
      } else {
        await workerLedger.resolveRequest({
          request_id: crashReqs[i]!.reqId,
          action: "SETTLE",
          billing_price_id: f.flatBillingPrice.id,
          usage: { input_tokens: "50", output_tokens: "50" },
          supplier_cost_atoms: "50",
          reason: "verified_sent",
          actor: "ops",
        });
      }
    }

    const endWallet = await workerLedger.getWallet(f.project.id, "PTS");
    expect(endWallet?.reserved_atoms).toBe("0");
    expect(endWallet?.spent_atoms).toBe("200");
    expect(endWallet?.expired_atoms).toBe("999800");
    expect(endWallet?.available_atoms).toBe("50000000");

    const endSup = await workerLedger.getSupplierBalance(f.accountA.id, "USD");
    expect(endSup.reserved_atoms).toBe("0");
    expect(endSup.spent_atoms).toBe("100");
    expect(await workerLedger.auditConservation()).toEqual({
      ok: true,
      unbalanced_journals: 0,
      broken_lots: 0,
      wallet_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
    await f.ledger.drain();
  }));
