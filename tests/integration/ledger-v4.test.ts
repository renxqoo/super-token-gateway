import { expect, test } from "bun:test";
import { appendFile, rm } from "node:fs/promises";
import type { SQL } from "bun";
import { getTableConfig } from "drizzle-orm/pg-core";
import { createWorkerRunner } from "@gateway/worker";
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

async function withLedgerDb(
  run: (db: SQL) => Promise<void>,
  initialize: (db: SQL) => Promise<void> = migrate,
): Promise<void> {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL required");
  const name = `gateway_v4_ledger_${crypto.randomUUID().replaceAll("-", "")}`;
  const owner = createDatabase(base, 1);
  await owner.unsafe(`CREATE DATABASE ${name}`);
  const url = new URL(base);
  url.pathname = `/${name}`;
  const db = createDatabase(url.toString(), 16);
  try {
    await initialize(db);
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
      reservation_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
  }));

test("G1.2 refund and debt collection share wallet-first lock order and preserve exact balances", () =>
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

    const refundInput = {
      charge_id: settled.charge_id,
      external_ref: "ref-1",
      amount_atoms: "50",
      reason: "CUSTOMER_REFUND",
    };
    let pendingRefund:
      Promise<Awaited<ReturnType<typeof f.ledger.refundCharge>> | Error> | undefined;
    const walletLock = await db
      .begin(async (tx) => {
        await tx`SELECT id FROM public.wallets WHERE project_id = ${proj2.id} FOR NO KEY UPDATE`;
        pendingRefund = f.ledger.refundCharge(refundInput).catch((error: Error) => error);
        await Bun.sleep(50);
        await tx`SELECT id FROM public.billing_charges
        WHERE id = ${settled.charge_id} FOR NO KEY UPDATE`;
        return true;
      })
      .catch(() => false);
    const refunded = await pendingRefund;
    expect(walletLock).toBe(true);
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
      reservation_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
  }));

test("G1.4 failed fallback cannot strand a customer top-up, and successful failover preserves UNKNOWN supplier funds", () =>
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

    const rejected = await f.ledger.switchSupplierAttempt({
      request_id: reqId,
      failed_attempt_id: attA,
      failed_outcome: "FAILED",
      next_attempt_id: crypto.randomUUID(),
      next_account_id: f.accountB.id,
      next_channel_id: f.channelB.id,
      next_currency: "USD",
      next_supplier_atoms: "1000001",
      required_customer_atoms: "150",
    });
    expect(rejected).toMatchObject({ ok: false, code: "supplier_insufficient_balance" });
    expect(await f.ledger.getWallet(f.project.id, "PTS")).toMatchObject({
      available_atoms: "400",
      reserved_atoms: "100",
    });
    expect((await f.ledger.getSupplierBalance(f.accountA.id, "USD")).reserved_atoms).toBe("50");

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

    await db.begin(async (tx) => {
      await tx`UPDATE public.wallets SET available_atoms = available_atoms - 1,
        reserved_atoms = reserved_atoms + 1 WHERE project_id = ${f.project.id}`;
      await tx`UPDATE public.wallet_lots SET available_atoms = available_atoms - 1,
        reserved_atoms = reserved_atoms + 1
        WHERE wallet_id = (SELECT id FROM public.wallets WHERE project_id = ${f.project.id})`;
    });
    expect((await f.ledger.auditConservation()).ok).toBe(false);
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
      const entriesFile = verified.files.find((file) => file.kind === "entries")!.file;
      await appendFile(`${archiveDir}/${entriesFile}`, "tampered");
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
      reservation_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
  }));

for (const action of ["RELEASE", "SETTLE"] as const) {
  test(`manual ${action} keeps the hold and balances when writing the resolution fails`, () =>
    withLedgerDb(async (db) => {
      const f = await seedFixture(db);
      try {
        await f.ledger.creditWallet({
          project_id: f.project.id,
          currency: "PTS",
          external_ref: "manual-resolution-credit",
          amount_atoms: "100",
          kind: "CASH",
        });
        const requestId = crypto.randomUUID();
        expect(
          await f.ledger.acquireHold({
            request_id: requestId,
            attempt_id: crypto.randomUUID(),
            project_id: f.project.id,
            currency: "PTS",
            operation: "chat",
            model: "gpt-4o",
            idempotency_key: "manual-resolution",
            fingerprint: "manual-resolution",
            customer_atoms: "100",
            supplier_account_id: f.accountA.id,
            supplier_channel_id: f.channelA.id,
            supplier_cost_version_id: f.costPrice.id,
            supplier_currency: "USD",
            supplier_atoms: "50",
            expires_at: new Date(Date.now() + 30_000).toISOString(),
          }),
        ).toMatchObject({ ok: true });
        expect(await f.ledger.markUnknown(requestId)).toBe(true);
        await db`ALTER TABLE public.billing_resolutions
          ADD CONSTRAINT reject_resolution_actor CHECK (actor <> 'reject-write')`;
        const input = {
          request_id: requestId,
          action,
          billing_price_id: f.flatBillingPrice.id,
          usage: { input_tokens: "30" },
          supplier_cost_atoms: "10",
          reason: "verified from supplier",
          actor: "reject-write",
        };
        await expect(f.ledger.resolveRequest(input)).rejects.toThrow("reject_resolution_actor");
        const afterFailure = await createLedgerQueries(db).getRequestBilling(requestId);
        expect(afterFailure.hold?.state).toBe("UNKNOWN");
        expect(afterFailure.charge).toBeNull();
        expect(await f.ledger.getWallet(f.project.id, "PTS")).toMatchObject({
          available_atoms: "0",
          reserved_atoms: "100",
          spent_atoms: "0",
        });
        expect((await f.ledger.getSupplierBalance(f.accountA.id, "USD")).reserved_atoms).toBe("50");
        expect(
          await db`SELECT seq FROM public.outbox_events WHERE request_id = ${requestId}`,
        ).toHaveLength(0);

        const repaired = { ...input, actor: "finance" };
        const results = await Promise.all([
          f.ledger.resolveRequest(repaired),
          f.ledger.resolveRequest(repaired),
        ]);
        expect(results.filter((result) => result.replayed)).toHaveLength(1);
        expect(
          await db`SELECT action, actor FROM public.billing_resolutions WHERE request_id = ${requestId}`,
        ).toEqual([{ action, actor: "finance" }]);
        expect(await f.ledger.getWallet(f.project.id, "PTS")).toMatchObject({
          available_atoms: action === "SETTLE" ? "70" : "100",
          reserved_atoms: "0",
          spent_atoms: action === "SETTLE" ? "30" : "0",
        });
        expect((await f.ledger.auditConservation()).ok).toBe(true);
      } finally {
        await f.ledger.drain();
      }
    }));
}

test("G1.8 expired lot inside lease never resurrects and Worker recovers dead node leases and retains the actual supplier tariff", () =>
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
        const resolution = {
          request_id: crashReqs[i]!.reqId,
          action: "SETTLE" as const,
          billing_price_id: f.flatBillingPrice.id,
          usage: { input_tokens: "50", output_tokens: "50" },
          supplier_cost_atoms: "50",
          supplier_cost_version_id: f.costPrice.id,
          reason: "verified_sent",
          actor: "ops",
        };
        await workerLedger.resolveRequest(resolution);
      }
    }

    const recovered = await db<{ cost_version_id: string }[]>`
      SELECT cost_version_id FROM public.billing_charges
      WHERE request_id = ANY(${crashReqs.slice(2).map((request) => request.reqId)}::uuid[])
    `;
    expect(recovered.map((charge) => charge.cost_version_id)).toEqual([
      f.costPrice.id,
      f.costPrice.id,
    ]);

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
      reservation_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
    await f.ledger.drain();
  }));

test.each([
  ["v4-before-fund-leases.sql", 2026112101],
  ["v4-before-provider-plugins.sql", 1],
] as const)(
  "migrates existing %s without changing wallet balances or channel identities",
  (file, version) =>
    withLedgerDb(
      async (db) => {
        const accountId = crypto.randomUUID();
        const channelId = crypto.randomUUID();
        const projectId = crypto.randomUUID();
        const walletId = crypto.randomUUID();
        await db`INSERT INTO public.upstream_accounts(id, name) VALUES(${accountId}, 'old account')`;
        await db`INSERT INTO public.channels(id, account_id, name, kind, base_url, secret_ciphertext, rpm, concurrency)
        VALUES (${channelId}, ${accountId}, 'old channel', 'openai', 'https://old.invalid', 'cipher', 50, 5)`;
        await db`INSERT INTO public.projects(id, name, currency, rpm, tpm, concurrency)
        VALUES (${projectId}, 'old project', 'USD', 10, 1000, 5)`;
        await db`INSERT INTO public.wallets(id, project_id, currency, available_atoms)
        VALUES (${walletId}, ${projectId}, 'USD', 123)`;
        await expect(assertSchemaVersion(db)).rejects.toThrow(/Schema/);
        await migrate(db);
        await migrate(db);
        expect(await assertSchemaVersion(db)).toBe(SCHEMA_VERSION);
        const snapshot = await createCatalogRepository(db).loadSnapshot();
        const channel = snapshot.channels.get(channelId);
        expect(channel).toMatchObject({
          id: channelId,
          account_id: accountId,
          kind: "openai",
          base_url: "https://old.invalid",
          secret_ciphertext: "cipher",
          adapter_config: {},
        });
        expect(snapshot.plugins.get(channel?.provider_plugin_id ?? "")).toMatchObject({
          name: "openai",
          capability: "PROVIDER",
          builtin: true,
          provider_config: { endpoints: { chat: { path: "/v1/chat/completions" } } },
        });
        expect(
          await createLedgerEngine(db, { enableLeases: false }).getWallet(projectId, "USD"),
        ).toMatchObject({ id: walletId, available_atoms: "123" });
        const versions = await db<
          { version: number }[]
        >`SELECT version FROM public.schema_migrations ORDER BY version`;
        expect(versions.map((r) => Number(r.version))).toEqual([
          version,
          2026112102,
          2026112201,
          2026112202,
          2026112203,
          2026112204,
          2026112205,
          2026112301,
          SCHEMA_VERSION,
        ]);
      },
      async (db) => {
        const sql = await Bun.file(new URL(`../fixtures/${file}`, import.meta.url)).text();
        const spec = {
          version,
          files: [
            {
              path: "./migrations/0000_v4_baseline.sql",
              checksum: new Bun.CryptoHasher("sha256").update(sql).digest("hex"),
            },
          ],
        };
        await db.begin(async (tx) => {
          await tx.unsafe(sql);
          const checksum = new Bun.CryptoHasher("sha256")
            .update(JSON.stringify(spec))
            .digest("hex");
          await tx`INSERT INTO public.schema_migrations(version,checksum,manifest)
          VALUES(${version},${checksum},${spec}::jsonb)`;
        });
      },
    ),
);

test("a live node cannot admit new holds against wallet and supplier leases reaped by a Worker", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const node = createLedgerEngine(db, { leaseTtlMs: 60_000, leaseIdleReleaseMs: 60_000 });
    const worker = createLedgerEngine(db, { enableLeases: false });
    const unlock = Promise.withResolvers<void>();
    const releaseWalletFence = Promise.withResolvers<void>();
    const releaseSupplierFence = Promise.withResolvers<void>();
    let settleLock: Promise<unknown> | undefined;
    let walletFence: Promise<unknown> | undefined;
    let supplierFence: Promise<unknown> | undefined;
    let sweep: Promise<number> | undefined;
    try {
      await f.ledger.creditWallet({
        project_id: f.project.id,
        currency: "PTS",
        external_ref: "fencing-seed",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      const acquire = () =>
        node.acquireHold({
          request_id: crypto.randomUUID(),
          attempt_id: crypto.randomUUID(),
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
          expires_at: new Date(Date.now() + 30_000).toISOString(),
        });
      const existing = await Promise.all(Array.from({ length: 16 }, acquire));
      expect(existing.every((hold) => hold.ok)).toBe(true);
      const locked = Promise.withResolvers<void>();
      settleLock = db.begin(async (tx) => {
        await tx`SELECT id FROM public.wallet_leases WHERE state = 'ACTIVE' FOR NO KEY UPDATE`;
        await tx`SELECT id FROM public.supplier_leases WHERE state = 'ACTIVE' FOR NO KEY UPDATE`;
        locked.resolve();
        await unlock.promise;
      });
      await locked.promise;
      const concurrent = acquire();
      const beforeUnlock = await Promise.race([concurrent, Bun.sleep(750).then(() => null)]);
      unlock.resolve();
      await settleLock;
      const independent = await concurrent;
      expect(independent.ok).toBe(true);
      expect(beforeUnlock).toMatchObject({ ok: true });
      await db`UPDATE public.wallet_leases SET expires_at = now() WHERE state = 'ACTIVE'`;
      await db`UPDATE public.supplier_leases SET expires_at = now() WHERE state = 'ACTIVE'`;
      const walletFenced = Promise.withResolvers<void>();
      const supplierFenced = Promise.withResolvers<void>();
      walletFence = db.begin(async (tx) => {
        await tx`SELECT id FROM public.wallet_leases WHERE state = 'ACTIVE' FOR KEY SHARE`;
        walletFenced.resolve();
        await releaseWalletFence.promise;
      });
      supplierFence = db.begin(async (tx) => {
        await tx`SELECT id FROM public.supplier_leases WHERE state = 'ACTIVE' FOR KEY SHARE`;
        supplierFenced.resolve();
        await releaseSupplierFence.promise;
      });
      await Promise.all([walletFenced.promise, supplierFenced.promise]);
      sweep = worker.sweepExpiredLeases();
      const waitForClosure = async (table: string) => {
        const deadline = performance.now() + 700;
        while (performance.now() < deadline) {
          const [activity] = await db`SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity WHERE datname = current_database()
              AND wait_event_type = 'Lock' AND query LIKE ${`%FROM public.${table}%AND state = 'ACTIVE'%`}
          ) AS waiting`;
          if (activity?.waiting === true) return true;
          await Bun.sleep(10);
        }
        return false;
      };
      const walletBlocked = await waitForClosure("wallet_leases");
      releaseWalletFence.resolve();
      await walletFence;
      const supplierBlocked = await waitForClosure("supplier_leases");
      releaseSupplierFence.resolve();
      await supplierFence;
      expect(await sweep).toBe(2);
      expect(walletBlocked).toBe(true);
      expect(supplierBlocked).toBe(true);
      const admitted = await Promise.all(Array.from({ length: 4 }, acquire));
      expect(admitted.every((hold) => hold.ok)).toBe(true);
      const [invalid] = await db`
        SELECT count(*)::int AS n
        FROM public.billing_holds h
        LEFT JOIN public.wallet_leases w ON w.id = h.wallet_lease_id
        LEFT JOIN public.supplier_leases s ON s.id = h.supplier_lease_id
        WHERE h.state = 'HELD' AND (w.state = 'CLOSED' OR s.state = 'CLOSED')
      `;
      expect(invalid!.n).toBe(0);
      for (const result of [...existing, independent, ...admitted]) {
        if (!result.ok) throw new Error(result.message);
        expect(await node.releaseHold(result.request_id)).toBe(true);
      }
      await node.releaseIdleLeases();
      expect(await node.getWallet(f.project.id, "PTS")).toMatchObject({
        available_atoms: "100000000",
        reserved_atoms: "0",
      });
    } finally {
      unlock.resolve();
      releaseWalletFence.resolve();
      releaseSupplierFence.resolve();
      await Promise.allSettled([settleLock, walletFence, supplierFence, sweep]);
      await node.drain();
      await f.ledger.drain();
      await worker.drain();
    }
  }));

test("outbox acknowledgement cannot skip a late commit or advance beyond a claimed batch", () =>
  withLedgerDb(async (db) => {
    const ledger = createLedgerEngine(db, { enableLeases: false });
    const firstId = crypto.randomUUID();
    const secondId = crypto.randomUUID();
    const inserted = Promise.withResolvers<void>();
    const commit = Promise.withResolvers<void>();
    const first = db.begin(async (tx) => {
      await tx`INSERT INTO public.outbox_events(event_id, kind, payload)
        VALUES (${firstId}, 'billing.settled', '{}'::jsonb)`;
      inserted.resolve();
      await commit.promise;
    });
    await inserted.promise;
    const second = db`INSERT INTO public.outbox_events(event_id, kind, payload)
      VALUES (${secondId}, 'billing.settled', '{}'::jsonb)`.then(() => true);
    const consumer = "ordered-test";
    const delivered: string[] = [];
    try {
      const early = await Promise.race([second, Bun.sleep(150).then(() => false)]);
      if (early) {
        const batch = await ledger.claimOutboxEvents(consumer);
        delivered.push(...batch.events.map((event) => event.event_id));
        await ledger.ackOutboxEvents(consumer, batch.lease_token, batch.events.at(-1)!.seq);
      }
    } finally {
      commit.resolve();
      await Promise.all([first, second]);
    }
    const batch = await ledger.claimOutboxEvents(consumer);
    delivered.push(...batch.events.map((event) => event.event_id));
    expect(delivered.sort()).toEqual([firstId, secondId].sort());
    expect((await ledger.claimOutboxEvents(consumer)).events).toEqual([]);
    const last = batch.events.at(-1)!.seq;
    expect(
      await ledger.ackOutboxEvents(consumer, batch.lease_token, String(BigInt(last) + 1n)),
    ).toBe(false);
    expect(await ledger.ackOutboxEvents(consumer, batch.lease_token, last)).toBe(true);
  }));

test("sharded outbox inserts proceed concurrently across distinct projects without skipping late commits", () =>
  withLedgerDb(async (db) => {
    const ledger = createLedgerEngine(db, { enableLeases: false });
    const projShardA = "00000000-0000-4000-8000-000000000001";
    const projShardB = "00000000-0000-4000-8000-000000000002";
    const eventA = crypto.randomUUID();
    const eventB = crypto.randomUUID();
    const enteredA = Promise.withResolvers<void>();
    const releaseA = Promise.withResolvers<void>();

    const txA = db.begin(async (tx) => {
      await tx`INSERT INTO public.outbox_events(event_id, kind, project_id, payload)
        VALUES (${eventA}, 'billing.settled', ${projShardA}::uuid, '{"k":"a"}'::jsonb)`;
      enteredA.resolve();
      await releaseA.promise;
    });
    await enteredA.promise;

    const txB = db`INSERT INTO public.outbox_events(event_id, kind, project_id, payload)
      VALUES (${eventB}, 'billing.settled', ${projShardB}::uuid, '{"k":"b"}'::jsonb)`.then(
      () => true,
    );
    const consumer = "sharded-concurrent-test";
    try {
      const bCommittedImmediately = await Promise.race([txB, Bun.sleep(400).then(() => false)]);
      expect(bCommittedImmediately).toBe(true);

      const batch1 = await ledger.claimOutboxEvents(consumer);
      expect(batch1.events.map((e) => e.event_id)).toEqual([eventB]);
      expect(
        await ledger.ackOutboxEvents(consumer, batch1.lease_token, batch1.events[0]!.seq),
      ).toBe(true);
    } finally {
      releaseA.resolve();
      await Promise.all([txA, txB]);
    }

    const batch2 = await ledger.claimOutboxEvents(consumer);
    expect(batch2.events.map((e) => e.event_id)).toEqual([eventA]);
    expect(await ledger.ackOutboxEvents(consumer, batch2.lease_token, batch2.events[0]!.seq)).toBe(
      true,
    );
    expect((await ledger.claimOutboxEvents(consumer)).events).toEqual([]);
  }));

test("same-shard outbox inserts assign seq after acquiring shard lock so late commits are never skipped", () =>
  withLedgerDb(async (db) => {
    const ledger = createLedgerEngine(db, { enableLeases: false });
    const sameProject = "00000000-0000-4000-8000-000000000003";
    const event1 = crypto.randomUUID();
    const event2 = crypto.randomUUID();
    const tx1Locked = Promise.withResolvers<void>();
    const tx1Commit = Promise.withResolvers<void>();

    const tx1 = db.begin(async (tx) => {
      await tx`INSERT INTO public.outbox_events(event_id, kind, project_id, payload)
        VALUES (${event1}, 'billing.settled', ${sameProject}::uuid, '{"order":1}'::jsonb)`;
      tx1Locked.resolve();
      await tx1Commit.promise;
    });
    await tx1Locked.promise;

    let tx2Finished = false;
    const tx2 = db`INSERT INTO public.outbox_events(event_id, kind, project_id, payload)
      VALUES (${event2}, 'billing.settled', ${sameProject}::uuid, '{"order":2}'::jsonb)`.then(
      () => {
        tx2Finished = true;
      },
    );
    await Bun.sleep(100);
    expect(tx2Finished).toBe(false);
    tx1Commit.resolve();
    await Promise.all([tx1, tx2]);

    const rows = await db<{ event_id: string; seq: string }[]>`
      SELECT event_id, seq::text FROM public.outbox_events
      WHERE project_id = ${sameProject}::uuid
      ORDER BY seq ASC
    `;
    expect(rows.map((r) => r.event_id)).toEqual([event1, event2]);
    expect(BigInt(rows[0]!.seq) < BigInt(rows[1]!.seq)).toBe(true);

    const claimed = await ledger.claimOutboxEvents("same-shard-order", 10);
    expect(claimed.events.map((e) => e.event_id)).toEqual([event1, event2]);
  }));

test("check_ledger_journal_balance rejects unbalanced journals even after GUC manipulation", () =>
  withLedgerDb(async (db) => {
    const journalId = crypto.randomUUID();
    const entityId = crypto.randomUUID();
    await expect(
      db.begin(async (tx) => {
        await tx`SELECT set_config('stg.last_checked_journal', ${journalId}, true)`;
        await tx`
          INSERT INTO public.ledger_entries(
            journal_id, ordinal, side, entity_id, reference, kind, currency, account, atoms
          )
          VALUES
            (${journalId}::uuid, 0, 'CUSTOMER', ${entityId}::uuid, 'tamper-1', 'ADJUST', 'USD', 'system:revenue', 100),
            (${journalId}::uuid, 1, 'CUSTOMER', ${entityId}::uuid, 'tamper-1', 'ADJUST', 'USD', 'system:funding', -50)
        `;
      }),
    ).rejects.toThrow(/unbalanced ledger journal/);
  }));

test("concurrent claimOutboxEvents and ackOutboxEvents avoid deadlocks and preserve pre-upgrade active leases", () =>
  withLedgerDb(async (db) => {
    const ledger = createLedgerEngine(db, { enableLeases: false });
    const proj = "00000000-0000-4000-8000-000000000004";
    const [ev] = await db<{ seq: string }[]>`
      INSERT INTO public.outbox_events(event_id, kind, project_id, payload)
      VALUES (${crypto.randomUUID()}::uuid, 'billing.settled', ${proj}::uuid, '{}'::jsonb)
      RETURNING seq::text
    `;
    const legacyToken = crypto.randomUUID();
    await db`
      INSERT INTO public.outbox_consumers(name, acked_seq, lease_token, leased_through_seq, leased_until)
      VALUES ('legacy-consumer', 0, ${legacyToken}::uuid, ${ev!.seq}::bigint, clock_timestamp() + interval '30 seconds')
    `;
    expect((await ledger.claimOutboxEvents("legacy-consumer")).events).toEqual([]);
    expect(await ledger.ackOutboxEvents("legacy-consumer", legacyToken, ev!.seq)).toBe(true);
    expect((await ledger.claimOutboxEvents("legacy-consumer")).events).toEqual([]);

    for (let i = 0; i < 20; i++) {
      await db`
        INSERT INTO public.outbox_events(event_id, kind, project_id, payload)
        VALUES (${crypto.randomUUID()}::uuid, 'billing.settled', ${proj}::uuid, '{}'::jsonb)
      `;
    }
    const consumer = "concurrent-ack-claim";
    for (let round = 0; round < 10; round++) {
      const batch = await ledger.claimOutboxEvents(consumer, 2);
      if (batch.events.length === 0) break;
      const lastSeq = batch.events.at(-1)!.seq;
      const [ackRes, competingClaim] = await Promise.all([
        ledger.ackOutboxEvents(consumer, batch.lease_token, lastSeq),
        ledger.claimOutboxEvents(consumer, 2),
      ]);
      expect(ackRes).toBe(true);
      expect(Array.isArray(competingClaim.events)).toBe(true);
      if (competingClaim.events.length > 0) {
        await ledger.ackOutboxEvents(
          consumer,
          competingClaim.lease_token,
          competingClaim.events.at(-1)!.seq,
        );
      }
    }
  }));

test("configuration snapshots cannot label stale project state with the new epoch", () =>
  withLedgerDb(async (db) => {
    const catalog = createCatalogRepository(db);
    const project = await catalog.upsertProject({
      name: "snapshot-project",
      currency: "USD",
      rpm: 10,
      tpm: 1000,
      concurrency: 1,
    });
    const lock = Promise.withResolvers<void>();
    const update = Promise.withResolvers<void>();
    const writer = db.begin(async (tx) => {
      await tx`LOCK TABLE public.config_epoch IN ACCESS EXCLUSIVE MODE`;
      lock.resolve();
      await update.promise;
      await tx`UPDATE public.projects SET enabled = false WHERE id = ${project.id}`;
      await tx`UPDATE public.config_epoch SET version = version + 1`;
    });
    await lock.promise;
    const reading = catalog.loadSnapshot();
    try {
      await Bun.sleep(80);
    } finally {
      update.resolve();
      await writer;
    }
    const snapshot = await reading;
    const current = await catalog.loadSnapshot();
    expect(current.projects.get(project.id)?.enabled).toBe(false);
    expect(snapshot.projects.get(project.id)?.enabled).toBe(snapshot.epoch < current.epoch);
  }));

test("the timeout sweep leaves an expired hold alone when it was released concurrently", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const requestId = crypto.randomUUID();
    await f.ledger.creditWallet({
      project_id: f.project.id,
      currency: "PTS",
      external_ref: "sweep-race-fund",
      amount_atoms: "500",
      kind: "CASH",
    });
    const hold = await f.ledger.acquireHold({
      request_id: requestId,
      attempt_id: crypto.randomUUID(),
      project_id: f.project.id,
      currency: "PTS",
      operation: "chat",
      model: "gpt-4o",
      idempotency_key: null,
      fingerprint: "",
      customer_atoms: "100",
      supplier_account_id: f.accountA.id,
      supplier_channel_id: f.channelA.id,
      supplier_currency: "USD",
      supplier_atoms: "50",
      expires_at: new Date(Date.now() - 1000).toISOString(),
    });
    expect(hold.ok).toBe(true);
    const locked = Promise.withResolvers<void>();
    const unlock = Promise.withResolvers<void>();
    const barrier = db.begin(async (tx) => {
      await tx`SELECT request_id FROM public.billing_holds
        WHERE request_id = ${requestId} FOR NO KEY UPDATE`;
      locked.resolve();
      await unlock.promise;
    });
    await locked.promise;
    const release = f.ledger.releaseHold(requestId);
    await Bun.sleep(40);
    const sweep = f.ledger.sweepTimedOutHolds();
    try {
      await Bun.sleep(50);
    } finally {
      unlock.resolve();
      await barrier;
    }
    expect(await release).toBe(true);
    expect(await sweep).toBe(0);
    expect((await createLedgerQueries(db).getRequestBilling(requestId)).hold?.state).toBe(
      "RELEASED",
    );
    await f.ledger.drain();
  }));

test("a settlement constraint error does not roll back another request in the same microbatch", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const badRequest = "00000000-0000-4000-8000-000000000012";
    await db`ALTER TABLE public.billing_charges
      ADD CONSTRAINT test_bad_charge CHECK (request_id <> '00000000-0000-4000-8000-000000000012')`;
    await f.ledger.creditWallet({
      project_id: f.project.id,
      currency: "PTS",
      external_ref: "batch-isolation-fund",
      amount_atoms: "500",
      kind: "CASH",
    });
    const items = [badRequest, crypto.randomUUID()].map((request_id) => ({
      request_id,
      attempt_id: crypto.randomUUID(),
    }));
    for (const item of items) {
      const hold = await f.ledger.acquireHold({
        ...item,
        project_id: f.project.id,
        currency: "PTS",
        operation: "chat",
        model: "gpt-4o",
        idempotency_key: null,
        fingerprint: "",
        customer_atoms: "100",
        supplier_account_id: f.accountA.id,
        supplier_channel_id: f.channelA.id,
        supplier_currency: "USD",
        supplier_atoms: "50",
        expires_at: new Date(Date.now() + 1000).toISOString(),
      });
      expect(hold.ok).toBe(true);
    }
    const [bad, good] = await Promise.allSettled(
      items.map((item) =>
        f.ledger.commitTerminal({
          request_id: item.request_id,
          selected_attempt_id: item.attempt_id,
          channel_id: f.channelA.id,
          billing_price_id: f.flatBillingPrice.id,
          cost_version_id: f.costPrice.id,
          usage: { input_tokens: "60", output_tokens: "20" },
          billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
          billing_basis: "PROVIDER_USAGE",
          supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
          supplier_currency: "USD",
        }),
      ),
    );
    expect(bad).toMatchObject({ status: "rejected" });
    expect(good).toMatchObject({
      status: "fulfilled",
      value: { ok: true, customer_atoms: "80", supplier_cost_atoms: "40" },
    });
    expect(await f.ledger.releaseHold(badRequest)).toBe(true);
    expect(await f.ledger.getWallet(f.project.id, "PTS")).toMatchObject({
      available_atoms: "420",
      reserved_atoms: "0",
      spent_atoms: "80",
    });
    await f.ledger.drain();
  }));

test("funds recovery continues while the business webhook is waiting for a response", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const entered = Promise.withResolvers<void>();
    const resume = Promise.withResolvers<void>();
    const webhook = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch() {
        entered.resolve();
        await resume.promise;
        return new Response(null, { status: 204 });
      },
    });
    const worker = createWorkerRunner({
      db,
      businessWebhookUrl: `http://127.0.0.1:${webhook.port}`,
      outboxIntervalMs: 10,
      sweepIntervalMs: 100,
    });
    try {
      await db`INSERT INTO public.outbox_events(event_id, kind, payload)
        VALUES(${crypto.randomUUID()}, 'billing.settled', '{}'::jsonb)`;
      await worker.start();
      await entered.promise;
      await f.ledger.creditWallet({
        project_id: f.project.id,
        currency: "PTS",
        external_ref: "sweep-independent-fund",
        amount_atoms: "500",
        kind: "CASH",
      });
      const requestId = crypto.randomUUID();
      const hold = await f.ledger.acquireHold({
        request_id: requestId,
        attempt_id: crypto.randomUUID(),
        project_id: f.project.id,
        currency: "PTS",
        operation: "chat",
        model: "gpt-4o",
        idempotency_key: null,
        fingerprint: "",
        customer_atoms: "100",
        supplier_account_id: f.accountA.id,
        supplier_channel_id: f.channelA.id,
        supplier_currency: "USD",
        supplier_atoms: "50",
        expires_at: new Date(Date.now() - 100).toISOString(),
      });
      expect(hold.ok).toBe(true);
      const queries = createLedgerQueries(db);
      const until = performance.now() + 1000;
      let state: string | undefined;
      while (performance.now() < until) {
        state = (await queries.getRequestBilling(requestId)).hold?.state;
        if (state === "UNKNOWN") break;
        await Bun.sleep(30);
      }
      expect(state).toBe("UNKNOWN");
      expect(await f.ledger.releaseHold(requestId)).toBe(true);
    } finally {
      resume.resolve();
      await worker.stop();
      await f.ledger.drain();
      await webhook.stop(true);
    }
  }));

test("a locked supplier lease cannot stall settlement for a wallet on another funded cell", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const ledger = createLedgerEngine(db, { leaseIdleReleaseMs: 60_000, batchWindowMs: 1 });
    const projectIds = [
      "e01e6e75-7ad7-45de-87c4-000000000001",
      "e01e6e75-7ad7-45de-87c4-000000000002",
    ];
    const locked = Promise.withResolvers<void>();
    const unlock = Promise.withResolvers<void>();
    let blocking: Promise<unknown> | undefined;
    const remaining: string[] = [];
    const expiry = new Date(Date.now() + 30_000).toISOString();

    async function acquire(projectId: string) {
      const requestId = crypto.randomUUID();
      const attemptId = crypto.randomUUID();
      const hold = await ledger.acquireHold({
        request_id: requestId,
        attempt_id: attemptId,
        project_id: projectId,
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
        expires_at: expiry,
      });
      expect(hold.ok).toBe(true);
      remaining.push(requestId);
      return { requestId, attemptId };
    }
    function settle(req: Awaited<ReturnType<typeof acquire>>) {
      return ledger.commitTerminal({
        request_id: req.requestId,
        selected_attempt_id: req.attemptId,
        channel_id: f.channelA.id,
        billing_price_id: f.flatBillingPrice.id,
        cost_version_id: f.costPrice.id,
        usage: { input_tokens: "60", output_tokens: "40" },
        billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
        billing_basis: "PROVIDER_USAGE",
        supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
        supplier_currency: "USD",
      });
    }

    try {
      await ledger.rechargeSupplier({
        account_id: f.accountA.id,
        currency: "USD",
        external_ref: "shared-supplier-cells",
        amount_atoms: "500000000",
      });
      for (const [i, id] of projectIds.entries()) {
        await f.catalog.upsertProject({
          id,
          name: `lease-lane-${i}`,
          currency: "PTS",
          rpm: 1000,
          tpm: 100000,
          concurrency: 500,
        });
        await ledger.creditWallet({
          project_id: id,
          currency: "PTS",
          kind: "CASH",
          external_ref: `lane-cash-${i}`,
          amount_atoms: "100000000",
        });
      }
      await Promise.all(projectIds.flatMap((id) => Array.from({ length: 16 }, () => acquire(id))));
      const [one, two] = await Promise.all(projectIds.map(acquire));
      const [row] =
        await db`SELECT supplier_lease_id FROM public.billing_holds WHERE request_id = ${one!.requestId}`;
      expect(row?.supplier_lease_id).toBeTruthy();
      blocking = db.begin(async (tx) => {
        await tx`SELECT id FROM public.supplier_leases WHERE id = ${row!.supplier_lease_id} FOR NO KEY UPDATE`;
        locked.resolve();
        await unlock.promise;
      });
      await locked.promise;
      const second = settle(two!);
      const first = settle(one!);
      const ahead = await Promise.race([second, Bun.sleep(750).then(() => null)]);
      unlock.resolve();
      await blocking;
      await Promise.all([first, second]);
      expect(ahead).toMatchObject({ ok: true, customer_atoms: "100", supplier_cost_atoms: "50" });
      const [balances] = await db`SELECT count(*)::int AS used, sum(spent_atoms)::text AS spent
        FROM public.supplier_cells WHERE account_id = ${f.accountA.id} AND spent_atoms > 0`;
      expect(balances).toMatchObject({ used: 2, spent: "100" });
    } finally {
      unlock.resolve();
      await blocking;
      for (const requestId of remaining) await ledger.releaseHold(requestId);
      await ledger.drain();
      await f.ledger.drain();
    }
    expect(await ledger.auditConservation()).toMatchObject({
      ok: true,
      open_holds: 0,
      unknown_holds: 0,
    });
  }));

test("a wallet row lock timeout does not lose a measured settlement", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const ledger = createLedgerEngine(db, { leaseIdleReleaseMs: 60_000, batchWindowMs: 1 });
    const requestIds: string[] = [];
    let pending: Promise<unknown> | undefined;

    async function acquire() {
      const requestId = crypto.randomUUID();
      const attemptId = crypto.randomUUID();
      const hold = await ledger.acquireHold({
        request_id: requestId,
        attempt_id: attemptId,
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
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      });
      expect(hold.ok).toBe(true);
      requestIds.push(requestId);
      return { requestId, attemptId };
    }

    try {
      await ledger.creditWallet({
        project_id: f.project.id,
        currency: "PTS",
        kind: "CASH",
        external_ref: "lock-retry-cash",
        amount_atoms: "100000000",
      });
      await ledger.rechargeSupplier({
        account_id: f.accountA.id,
        currency: "USD",
        external_ref: "lock-retry-supplier",
        amount_atoms: "500000000",
      });
      await Promise.all(Array.from({ length: 16 }, () => acquire()));
      const { requestId, attemptId } = await acquire();
      const [hold] = await db`SELECT wallet_id, wallet_lease_id, supplier_lease_id
        FROM public.billing_holds WHERE request_id = ${requestId}`;
      expect(hold?.wallet_lease_id).toBeTruthy();
      expect(hold?.supplier_lease_id).toBeTruthy();

      await db.begin(async (tx) => {
        const [blocker] = await tx`SELECT pg_backend_pid() AS pid FROM public.wallets
          WHERE id = ${hold!.wallet_id} FOR NO KEY UPDATE`;
        pending = ledger
          .commitTerminal({
            request_id: requestId,
            selected_attempt_id: attemptId,
            channel_id: f.channelA.id,
            billing_price_id: f.flatBillingPrice.id,
            cost_version_id: f.costPrice.id,
            usage: { input_tokens: "60", output_tokens: "40" },
            billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
            billing_basis: "PROVIDER_USAGE",
            supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
            supplier_currency: "USD",
          })
          .catch((error: unknown) => error);
        const deadline = Date.now() + 2_000;
        for (;;) {
          const [row] = await db`SELECT EXISTS (SELECT 1 FROM pg_stat_activity
            WHERE datname = current_database() AND wait_event_type = 'Lock'
              AND (query LIKE '%FROM public.wallets%' OR query LIKE '%commit_leased_batch%')
              AND ${blocker!.pid} = ANY(pg_blocking_pids(pid))) AS waiting`;
          if (row!.waiting) break;
          if (Date.now() >= deadline)
            throw new Error("Settlement did not wait for the wallet lock");
          await Bun.sleep(20);
        }
        await Bun.sleep(5_150);
      });

      expect(await pending).toMatchObject({
        ok: true,
        customer_atoms: "100",
        supplier_cost_atoms: "50",
      });
      expect(
        await db`SELECT customer_atoms::text, supplier_cost_atoms::text, selected_attempt_id
        FROM public.billing_charges WHERE request_id = ${requestId}`,
      ).toEqual([
        { customer_atoms: "100", supplier_cost_atoms: "50", selected_attempt_id: attemptId },
      ]);
    } finally {
      await pending;
      for (const requestId of requestIds) await ledger.releaseHold(requestId);
      await ledger.drain();
      await f.ledger.drain();
    }
    expect(await ledger.auditConservation()).toMatchObject({
      ok: true,
      open_holds: 0,
      unknown_holds: 0,
    });
  }));

test("a temporarily locked sole funded cell does not falsely reject a supplier hold", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const locked = Promise.withResolvers<void>();
    const unlock = Promise.withResolvers<void>();
    let blocking: Promise<unknown> | undefined;
    const id = "e01e6e75-7ad7-45de-87c4-000000000001";
    const requestId = crypto.randomUUID();
    try {
      await f.catalog.upsertProject({
        id,
        name: "only-funded-cell",
        currency: "PTS",
        rpm: 1000,
        tpm: 100000,
        concurrency: 500,
      });
      await f.ledger.creditWallet({
        project_id: id,
        currency: "PTS",
        kind: "CASH",
        external_ref: "only-funded-cell-cash",
        amount_atoms: "100000000",
      });
      await db`UPDATE public.supplier_cells SET balance_atoms = CASE WHEN cell = 0 THEN 1000000 ELSE 0 END
        WHERE account_id = ${f.accountA.id} AND currency = 'USD'`;
      blocking = db.begin(async (tx) => {
        await tx`SELECT cell FROM public.supplier_cells WHERE account_id = ${f.accountA.id}
          AND currency = 'USD' AND cell = 0 FOR NO KEY UPDATE`;
        locked.resolve();
        await unlock.promise;
      });
      await locked.promise;
      const pending = f.ledger.acquireHold({
        request_id: requestId,
        attempt_id: crypto.randomUUID(),
        project_id: id,
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
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      });
      await Promise.race([pending, Bun.sleep(150)]);
      unlock.resolve();
      await blocking;
      expect(await pending).toMatchObject({ ok: true, supplier_cell: 0, held_atoms: "200" });
      expect(await f.ledger.releaseHold(requestId)).toBe(true);
      expect(await f.ledger.auditConservation()).toMatchObject({
        ok: true,
        open_holds: 0,
        unknown_holds: 0,
      });
    } finally {
      unlock.resolve();
      await blocking;
      await f.ledger.drain();
    }
  }));

test("symptom: commit_leased_batch succeeds under least-privilege stg_data outbox column grants", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 12);
    const roleName = `stg_data_t_${suffix}`;
    const rolePass = `pw_${suffix}`;
    let dataDb: SQL | undefined;
    let dataLedger: ReturnType<typeof createLedgerEngine> | undefined;
    try {
      await f.ledger.creditWallet({
        project_id: f.project.id,
        currency: "PTS",
        kind: "CASH",
        external_ref: `least-priv-${suffix}`,
        amount_atoms: "100000000",
      });
      await db.unsafe(
        `CREATE ROLE ${roleName} LOGIN PASSWORD '${rolePass}' NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE`,
      );
      await db.unsafe(`GRANT USAGE ON SCHEMA public TO ${roleName}`);
      await db.unsafe(`GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${roleName}`);
      await db.unsafe(
        `GRANT INSERT ON public.billing_holds, public.billing_charges, public.wallet_leases, public.supplier_leases TO ${roleName}`,
      );
      await db.unsafe(
        `GRANT UPDATE (available_atoms, reserved_atoms, spent_atoms, expired_atoms, debt_atoms, credit_reserved_atoms, evidence_disputed, freeze_reason) ON public.wallets TO ${roleName}`,
      );
      await db.unsafe(
        `GRANT UPDATE (available_atoms, reserved_atoms, spent_atoms, expired_atoms) ON public.wallet_lots TO ${roleName}`,
      );
      await db.unsafe(
        `GRANT UPDATE (balance_atoms, reserved_atoms, spent_atoms, revision) ON public.supplier_cells TO ${roleName}`,
      );
      await db.unsafe(
        `GRANT UPDATE (state, remaining_atoms, lot_allocations, earliest_lot_expires_at, expires_at, updated_at) ON public.wallet_leases TO ${roleName}`,
      );
      await db.unsafe(
        `GRANT UPDATE (state, remaining_atoms, expires_at, updated_at) ON public.supplier_leases TO ${roleName}`,
      );
      await db.unsafe(
        `GRANT UPDATE (state, held_atoms, credit_atoms, lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, closed_at) ON public.billing_holds TO ${roleName}`,
      );
      await db.unsafe(
        `GRANT UPDATE (state, revision, updated_at) ON public.upstream_accounts TO ${roleName}`,
      );
      await db.unsafe(
        `GRANT INSERT (journal_id, ordinal, side, entity_id, request_id, reference, kind, currency, account, atoms) ON public.ledger_entries TO ${roleName}`,
      );
      await db.unsafe(
        `GRANT INSERT (event_id, kind, request_id, project_id, payload) ON public.outbox_events TO ${roleName}`,
      );
      await db.unsafe(`GRANT USAGE ON SEQUENCE public.outbox_events_seq_seq TO ${roleName}`);
      await db.unsafe(`GRANT EXECUTE ON FUNCTION public.outbox_shard(UUID) TO ${roleName}`);
      await db.unsafe(
        `GRANT EXECUTE ON FUNCTION public.commit_leased_batch(JSONB, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ) TO ${roleName}`,
      );

      const base = new URL(process.env.DATABASE_URL!);
      const [dbRow] = await db<{ dbname: string }[]>`SELECT current_database() AS dbname`;
      base.pathname = `/${dbRow!.dbname}`;
      base.username = roleName;
      base.password = rolePass;
      dataDb = createDatabase(base.toString(), 8);
      dataLedger = createLedgerEngine(dataDb, {
        enableLeases: true,
        batchWindowMs: 2,
        maxBatchSize: 64,
      });

      const expiresAt = new Date(Date.now() + 60_000).toISOString();
      const wave = async (count: number) => {
        const items = Array.from({ length: count }, () => ({
          requestId: crypto.randomUUID(),
          attemptId: crypto.randomUUID(),
        }));
        const holds = await Promise.all(
          items.map((item) =>
            dataLedger!.acquireHold({
              request_id: item.requestId,
              attempt_id: item.attemptId,
              project_id: f.project.id,
              currency: "PTS",
              operation: "chat",
              model: "gpt-4o",
              idempotency_key: null,
              fingerprint: "",
              customer_atoms: "200",
              supplier_account_id: f.accountA.id,
              supplier_cost_version_id: f.costPrice.id,
              supplier_channel_id: f.channelA.id,
              supplier_currency: "USD",
              supplier_atoms: "100",
              expires_at: expiresAt,
            }),
          ),
        );
        expect(holds.every((h) => h.ok)).toBe(true);
        return items;
      };
      const firstWave = await wave(20);
      const secondWave = await wave(20);
      const allItems = [...firstWave, ...secondWave];
      const commits = await Promise.all(
        allItems.map((item) =>
          dataLedger!.commitTerminal({
            request_id: item.requestId,
            selected_attempt_id: item.attemptId,
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
      expect(commits.every((c) => c.ok)).toBe(true);
      const [outbox] = await db<
        { count: number }[]
      >`SELECT count(*)::int AS count FROM public.outbox_events`;
      expect(outbox?.count).toBe(allItems.length);
      const [leasedCount] = await db<{ count: number }[]>`
        SELECT count(*)::int AS count FROM public.billing_holds WHERE wallet_lease_id IS NOT NULL
      `;
      expect(leasedCount?.count ?? 0).toBeGreaterThanOrEqual(secondWave.length);
      expect(await f.ledger.auditConservation()).toMatchObject({
        ok: true,
        open_holds: 0,
        unknown_holds: 0,
      });
    } finally {
      await dataLedger?.drain().catch(() => undefined);
      await dataDb?.close({ timeout: 1 }).catch(() => undefined);
      await f.ledger.drain().catch(() => undefined);
      await db
        .unsafe(`REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM ${roleName}`)
        .catch(() => undefined);
      await db
        .unsafe(`REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM ${roleName}`)
        .catch(() => undefined);
      await db
        .unsafe(`REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM ${roleName}`)
        .catch(() => undefined);
      await db.unsafe(`REVOKE USAGE ON SCHEMA public FROM ${roleName}`).catch(() => undefined);
      await db.unsafe(`DROP ROLE IF EXISTS ${roleName}`).catch(() => undefined);
    }
  }));
