import { expect, test } from "bun:test";
import { appendFile, rm } from "node:fs/promises";
import { SQL } from "bun";
import { getTableConfig } from "drizzle-orm/pg-core";
import { createWorkerRunner } from "@gateway/worker";
import {
  assertSchemaVersion,
  createAsyncTasksRepository,
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
  run: (db: SQL, dbUrl: string) => Promise<void>,
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
    await run(db, url.toString());
  } finally {
    await db.close({ timeout: 1 });
    await owner.unsafe(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await owner.close({ timeout: 1 });
  }
}

async function seedFixture(db: SQL) {
  const catalog = createCatalogRepository(db);
  const ledger = createLedgerEngine(db, { batchWindowMs: 2, maxBatchSize: 64 });
  const billingAccount = await catalog.upsertBillingAccount({
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
    billingAccount,
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
      billing_account_id: f.billingAccount.id,
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
      billing_account_id: f.billingAccount.id,
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
      billing_account_id: f.billingAccount.id,
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
      billing_account_id: f.billingAccount.id,
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
      billing_account_id: f.billingAccount.id,
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

    const wallet = await f.ledger.getWallet(f.billingAccount.id, "PTS");
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
      customer_ledger_mismatches: 0,
      supplier_ledger_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
  }));

test("G1.2 refund and debt collection share wallet-first lock order and preserve exact balances", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.configureWalletCredit({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      expected_version: "0",
      credit_limit_atoms: "1000",
    });
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "cash-1",
      amount_atoms: "9007199254740993",
      kind: "CASH",
    });
    const wBig = await f.ledger.getWallet(f.billingAccount.id, "PTS");
    expect(wBig?.available_atoms).toBe("9007199254740993");

    const proj2 = await f.catalog.upsertBillingAccount({
      name: "debt-billingAccount",
      currency: "PTS",
      rpm: 1000,
      tpm: 1000000,
      concurrency: 100,
    });
    await f.ledger.configureWalletCredit({
      billing_account_id: proj2.id,
      currency: "PTS",
      expected_version: "0",
      credit_limit_atoms: "50",
    });
    await f.ledger.creditWallet({
      billing_account_id: proj2.id,
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
      billing_account_id: proj2.id,
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
        await tx`SELECT id FROM public.wallets WHERE billing_account_id = ${proj2.id} FOR NO KEY UPDATE`;
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
      billing_account_id: proj2.id,
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

test("symptom: concurrent requests with zero settled balance can all draw new credit", () =>
  withLedgerDb(async (db) => {
    const {
      ledger,
      billingAccount,
      accountA,
      accountB,
      channelA,
      channelB,
      flatBillingPrice,
      costPrice,
    } = await seedFixture(db);
    const otherNode = createLedgerEngine(db, { batchWindowMs: 2, maxBatchSize: 64 });
    try {
      await ledger.configureWalletCredit({
        billing_account_id: billingAccount.id,
        currency: "PTS",
        expected_version: "0",
        credit_limit_atoms: "100",
      });
      const reservation = () => ({
        request_id: crypto.randomUUID(),
        attempt_id: crypto.randomUUID(),
        billing_account_id: billingAccount.id,
        currency: "PTS",
        operation: "chat" as const,
        model: "gpt-4o",
        idempotency_key: null,
        fingerprint: "credit-draw-at-zero",
        customer_atoms: "20",
        supplier_account_id: accountA.id,
        supplier_cost_version_id: costPrice.id,
        supplier_channel_id: channelA.id,
        supplier_currency: "USD",
        supplier_atoms: "10",
        expires_at: new Date(Date.now() + 30000).toISOString(),
      });
      const inputs = Array.from({ length: 4 }, reservation);
      const results = await Promise.all(
        inputs.map((input, i) => (i % 2 === 0 ? ledger : otherNode).acquireHold(input)),
      );
      expect(results.filter((result) => result.ok)).toHaveLength(1);
      expect(results.filter((result) => !result.ok)).toEqual(
        Array.from({ length: 3 }, () => expect.objectContaining({ code: "insufficient_balance" })),
      );
      const winner = inputs[results.findIndex((result) => result.ok)]!;
      expect(await ledger.getWallet(billingAccount.id, "PTS")).toMatchObject({
        available_atoms: "0",
        debt_atoms: "0",
        credit_reserved_atoms: "20",
      });
      expect(
        await ledger.switchSupplierAttempt({
          request_id: winner.request_id,
          failed_attempt_id: winner.attempt_id,
          failed_outcome: "FAILED",
          next_attempt_id: crypto.randomUUID(),
          next_account_id: accountB.id,
          next_channel_id: channelB.id,
          next_currency: "USD",
          next_supplier_atoms: "10",
          next_supplier_cost_version_id: costPrice.id,
          required_customer_atoms: "25",
        }),
      ).toMatchObject({ ok: true, held_atoms: "25" });
      expect(await ledger.markUnknown(winner.request_id)).toBe(true);
      expect(await otherNode.acquireHold(reservation())).toMatchObject({
        ok: false,
        code: "insufficient_balance",
      });
      const settled = await ledger.resolveRequest({
        request_id: winner.request_id,
        action: "SETTLE",
        billing_price_id: flatBillingPrice.id,
        usage: { input_tokens: "20" },
        supplier_cost_atoms: "10",
        actor: "credit-operator",
        reason: "Confirmed upstream usage",
      });
      expect(settled).toEqual({ replayed: false, state: "UNKNOWN_RESOLVED" });
      expect(await ledger.getWallet(billingAccount.id, "PTS")).toMatchObject({
        available_atoms: "0",
        debt_atoms: "20",
        credit_reserved_atoms: "0",
      });
      expect(await otherNode.acquireHold(reservation())).toMatchObject({
        ok: false,
        code: "insufficient_balance",
      });
      await ledger.creditWallet({
        billing_account_id: billingAccount.id,
        currency: "PTS",
        amount_atoms: "20",
        external_ref: "repay-to-zero",
        kind: "CASH",
      });
      expect(await ledger.getWallet(billingAccount.id, "PTS")).toMatchObject({
        available_atoms: "0",
        debt_atoms: "0",
      });
      const next = reservation();
      expect(await otherNode.acquireHold(next)).toMatchObject({ ok: true, credit_atoms: "20" });
      expect(await otherNode.releaseHold(next.request_id)).toBe(true);
      expect((await ledger.auditConservation()).ok).toBe(true);
    } finally {
      await Promise.all([ledger.drain(), otherNode.drain()]);
    }
  }));

test("symptom: a transient wallet lock must not discard an active funded lease", () =>
  withLedgerDb(async (db) => {
    const { ledger, billingAccount, accountA, channelA } = await seedFixture(db);
    try {
      await ledger.creditWallet({
        billing_account_id: billingAccount.id,
        currency: "PTS",
        external_ref: "lease-competition-fund",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      await ledger.rechargeSupplier({
        account_id: accountA.id,
        currency: "USD",
        external_ref: "lease-competition-fund",
        amount_atoms: "100000000",
      });
      const hold = (amount = "200", idempotencyKey: string | null = null) =>
        ledger.acquireHold({
          request_id: crypto.randomUUID(),
          attempt_id: crypto.randomUUID(),
          billing_account_id: billingAccount.id,
          currency: "PTS",
          operation: "chat",
          model: "gpt-4o",
          idempotency_key: idempotencyKey,
          fingerprint: "leased-funds",
          customer_atoms: amount,
          supplier_account_id: accountA.id,
          supplier_channel_id: channelA.id,
          supplier_currency: "USD",
          supplier_atoms: "100",
          expires_at: new Date(Date.now() + 30000).toISOString(),
        });
      const first = await Promise.all(Array.from({ length: 16 }, () => hold()));
      expect(first.every((result) => result.ok)).toBe(true);
      expect(await hold()).toMatchObject({ ok: true });
      const wallet = (await ledger.getWallet(billingAccount.id, "PTS"))!;
      expect(await hold(wallet.available_atoms, "earmark-free-funds")).toMatchObject({ ok: true });
      expect(await ledger.getWallet(billingAccount.id, "PTS")).toMatchObject({
        available_atoms: "0",
      });
      const locked = Promise.withResolvers<void>();
      const lock = db.begin(async (tx) => {
        await tx`SELECT id FROM public.wallets WHERE billing_account_id = ${billingAccount.id} FOR NO KEY UPDATE`;
        locked.resolve();
        await Bun.sleep(160);
      });
      await locked.promise;
      const [admitted] = await Promise.all([hold(), lock]);
      expect(admitted).toMatchObject({ ok: true, held_atoms: "200", credit_atoms: "0" });
      expect((await ledger.auditConservation()).ok).toBe(true);
    } finally {
      await ledger.drain();
    }
  }));

test("symptom: expired unused credit grants must not hide negative wallet balance on leased holds", () =>
  withLedgerDb(async (db) => {
    const { ledger, billingAccount, accountA, channelA, flatBillingPrice, costPrice } =
      await seedFixture(db);
    const otherNode = createLedgerEngine(db);
    try {
      await ledger.creditWallet({
        billing_account_id: billingAccount.id,
        currency: "PTS",
        external_ref: "lease-expiry-cash",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      await ledger.configureWalletCredit({
        billing_account_id: billingAccount.id,
        currency: "PTS",
        expected_version: "0",
        credit_limit_atoms: "100000000",
      });
      await ledger.rechargeSupplier({
        account_id: accountA.id,
        currency: "USD",
        external_ref: "lease-expiry-supplier",
        amount_atoms: "100000000",
      });
      const request = (amount = "200", idempotencyKey: string | null = null) => ({
        request_id: crypto.randomUUID(),
        attempt_id: crypto.randomUUID(),
        billing_account_id: billingAccount.id,
        currency: "PTS",
        operation: "chat" as const,
        model: "gpt-4o",
        idempotency_key: idempotencyKey,
        fingerprint: "lease-expiry",
        customer_atoms: amount,
        supplier_account_id: accountA.id,
        supplier_channel_id: channelA.id,
        supplier_currency: "USD",
        supplier_atoms: "100",
        expires_at: new Date(Date.now() + 30000).toISOString(),
      });
      const initial = await Promise.all(
        Array.from({ length: 16 }, () => ledger.acquireHold(request())),
      );
      expect(initial.every((result) => result.ok)).toBe(true);
      const leased = request();
      expect(await ledger.acquireHold(leased)).toMatchObject({ ok: true });
      const [hold] =
        await db`SELECT wallet_lease_id FROM public.billing_holds WHERE request_id = ${leased.request_id}`;
      expect(hold?.wallet_lease_id).toBeTruthy();
      const cash = (await ledger.getWallet(billingAccount.id, "PTS"))!.available_atoms;
      const grantExpiresAt = Date.now() + 1000;
      await otherNode.creditWallet({
        billing_account_id: billingAccount.id,
        currency: "PTS",
        external_ref: "lease-expiry-restricted-grant",
        amount_atoms: "100",
        kind: "GRANT",
        models: ["other-model"],
        expires_at: new Date(grantExpiresAt).toISOString(),
      });
      const charged = request(cash, "charge-unleased-cash");
      expect(await ledger.acquireHold(charged)).toMatchObject({ ok: true });
      await ledger.commitTerminal({
        request_id: charged.request_id,
        selected_attempt_id: charged.attempt_id,
        channel_id: channelA.id,
        billing_price_id: flatBillingPrice.id,
        cost_version_id: costPrice.id,
        usage: { input_tokens: "100000050", output_tokens: "0" },
        billing_pricing: { currency: "PTS", rates: flatBillingPrice.rates },
        billing_basis: "PROVIDER_USAGE",
        supplier_pricing: {
          currency: "USD",
          rates: { input_tokens: { numerator: "1", denominator: "1000000000000" } },
        },
        supplier_currency: "USD",
      });
      const wallet = (await ledger.getWallet(billingAccount.id, "PTS"))!;
      expect(wallet.available_atoms).toBe("100");
      expect(BigInt(wallet.reserved_atoms) - BigInt(wallet.debt_atoms)).toBe(-50n);
      expect(await ledger.acquireHold(request())).toMatchObject({ ok: true, credit_atoms: "0" });
      await Bun.sleep(Math.max(0, grantExpiresAt - Date.now() + 20));
      expect(await ledger.acquireHold(request())).toMatchObject({
        ok: false,
        code: "insufficient_balance",
      });
      expect((await ledger.auditConservation()).ok).toBe(true);
    } finally {
      await Promise.all([ledger.drain(), otherNode.drain()]);
    }
  }));

test.each(["wallet", "supplier"])(
  "symptom: a grant that expires while waiting for a %s lock cannot fund a new hold",
  (resource) =>
    withLedgerDb(async (db) => {
      const { ledger, billingAccount, accountA, channelA } = await seedFixture(db);
      const lockAcquired = Promise.withResolvers<void>();
      const unlock = Promise.withResolvers<void>();
      let lock: Promise<unknown> | undefined;
      let admission: Promise<unknown> | undefined;
      try {
        const expiry = Date.now() + 1000;
        await ledger.creditWallet({
          billing_account_id: billingAccount.id,
          currency: "PTS",
          external_ref: "grant-expires-during-lock",
          amount_atoms: "20",
          kind: "GRANT",
          expires_at: new Date(expiry).toISOString(),
        });
        lock = db.begin(async (tx) => {
          if (resource === "wallet") {
            await tx`SELECT id FROM public.wallets WHERE billing_account_id = ${billingAccount.id} FOR NO KEY UPDATE`;
          } else {
            await tx`SELECT cell FROM public.supplier_cells WHERE account_id = ${accountA.id} AND currency = 'USD' ORDER BY cell FOR NO KEY UPDATE`;
          }
          lockAcquired.resolve();
          await unlock.promise;
        });
        await lockAcquired.promise;
        const hold = ledger.acquireHold({
          request_id: crypto.randomUUID(),
          attempt_id: crypto.randomUUID(),
          billing_account_id: billingAccount.id,
          currency: "PTS",
          operation: "chat",
          model: "gpt-4o",
          idempotency_key: "grant-expires-during-lock",
          fingerprint: "grant-expires-during-lock",
          customer_atoms: "20",
          supplier_account_id: accountA.id,
          supplier_channel_id: channelA.id,
          supplier_currency: "USD",
          supplier_atoms: "10",
          expires_at: new Date(Date.now() + 30000).toISOString(),
        });
        admission = hold;
        let blocked = false;
        while (Date.now() < expiry && !blocked) {
          const [waiting] = await db`
          SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE datname = current_database() AND cardinality(pg_blocking_pids(pid)) > 0
          ) AS blocked
        `;
          blocked = waiting?.blocked === true;
          if (!blocked) await Bun.sleep(10);
        }
        expect(blocked).toBe(true);
        await Bun.sleep(Math.max(0, expiry - Date.now() + 20));
        unlock.resolve();
        await lock;
        expect(await hold).toMatchObject({ ok: false, code: "insufficient_balance" });
      } finally {
        unlock.resolve();
        await Promise.allSettled([lock, admission]);
        await ledger.drain();
      }
    }),
);

test("G1.3 100 concurrent micro-batched holds on one single hot wallet have zero 55P03 errors and exact conservation", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
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
          billing_account_id: f.billingAccount.id,
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

    const midWallet = await f.ledger.getWallet(f.billingAccount.id, "PTS");
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

    const endWallet = await f.ledger.getWallet(f.billingAccount.id, "PTS");
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
      customer_ledger_mismatches: 0,
      supplier_ledger_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
  }));

test("G1.4 failed fallback cannot strand a customer top-up, and successful failover preserves UNKNOWN supplier funds", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
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
      billing_account_id: f.billingAccount.id,
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
    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
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

    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
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
        reserved_atoms = reserved_atoms + 1 WHERE billing_account_id = ${f.billingAccount.id}`;
      await tx`UPDATE public.wallet_lots SET available_atoms = available_atoms - 1,
        reserved_atoms = reserved_atoms + 1
        WHERE wallet_id = (SELECT id FROM public.wallets WHERE billing_account_id = ${f.billingAccount.id})`;
    });
    expect((await f.ledger.auditConservation()).ok).toBe(false);
  }));

test("G1.5 moderation PLATFORM_ABSORB, DB immutability/balance triggers, and monthly archive verification", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
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
      billing_account_id: f.billingAccount.id,
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
    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
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
          VALUES (${badJournal}, 0, 'CUSTOMER', ${f.billingAccount.id}, 'bad-1', 'ADJUST', 'PTS', 'system:revenue', 100)
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
      billing_account_id: f.billingAccount.id,
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
          billing_account_id: f.billingAccount.id,
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
          billing_account_id: f.billingAccount.id,
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

    const wallet = await f.ledger.getWallet(f.billingAccount.id, "PTS");
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
      customer_ledger_mismatches: 0,
      supplier_ledger_mismatches: 0,
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
          billing_account_id: f.billingAccount.id,
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
            billing_account_id: f.billingAccount.id,
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
        expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
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
        expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
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
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "lease-grant-expiring",
      amount_atoms: "1000000",
      kind: "GRANT",
      priority: 10,
      expires_at: new Date(Date.now() + 60_000).toISOString(),
    });
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
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
          billing_account_id: f.billingAccount.id,
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
          billing_account_id: f.billingAccount.id,
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

    const midWallet = await workerLedger.getWallet(f.billingAccount.id, "PTS");
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

    const endWallet = await workerLedger.getWallet(f.billingAccount.id, "PTS");
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
      customer_ledger_mismatches: 0,
      supplier_ledger_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
    await f.ledger.drain();
  }));

test("symptom: a demoted Key can cancel a task while waiting for its row lock", () =>
  withLedgerDb(async (db) => {
    const { catalog, ledger, billingAccount, channelA } = await seedFixture(db);
    const locked = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let blocker: Promise<unknown> | undefined;
    let unauthorized: Promise<unknown> | undefined;
    let cancellation: Promise<unknown> | undefined;
    try {
      const baseKey = { billing_account_id: billingAccount.id, prefix: "sk_fixture" };
      const owner = await catalog.upsertApiKey({
        ...baseKey,
        name: "Task owner",
        verifier: "test-task-owner",
      });
      const caller = await catalog.upsertApiKey({
        ...baseKey,
        name: "Account admin",
        verifier: "test-task-admin",
        role: "ADMIN",
      });
      const sibling = await catalog.upsertApiKey({
        ...baseKey,
        name: "Another task owner",
        verifier: "test-task-sibling",
      });
      const repository = createAsyncTasksRepository(db);
      const taskId = "vid_revoked_admin_scope";
      await repository.createTask({
        id: taskId,
        request_id: crypto.randomUUID(),
        charge_id: null,
        billing_account_id: billingAccount.id,
        api_key_id: owner.id,
        channel_id: channelA.id,
        model: "video-fixture",
        operation: "videos",
        upstream_task_id: "upstream-fixture",
      });
      blocker = db.begin(async (tx) => {
        await tx`SELECT id FROM public.async_tasks WHERE id = ${taskId} FOR UPDATE`;
        locked.resolve();
        await release.promise;
      });
      await Promise.race([locked.promise, blocker]);
      unauthorized = repository.markUserCancelled(taskId, billingAccount.id, sibling.id, () => {});
      expect(await Promise.race([unauthorized, Bun.sleep(1000).then(() => "blocked")])).toBeNull();
      cancellation = repository.markUserCancelled(taskId, billingAccount.id, caller.id, () => {});
      const deadline = Date.now() + 2000;
      let waiting = false;
      while (!waiting && Date.now() < deadline) {
        const [activity] = await db`
          SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE datname = current_database() AND pid <> pg_backend_pid()
              AND wait_event_type = 'Lock' AND query LIKE '%async_tasks%'
          ) AS waiting
        `;
        waiting = activity?.waiting === true;
        if (!waiting) await Bun.sleep(10);
      }
      expect(waiting).toBe(true);
      await catalog.upsertApiKey({
        ...baseKey,
        id: caller.id,
        name: "Former admin",
        verifier: "test-task-admin",
        role: "INVOKER",
      });
      release.resolve();
      expect(await cancellation).toBeNull();
      expect(await repository.getTask(taskId)).toMatchObject({
        status: "QUEUED",
        user_cancelled: false,
      });
    } finally {
      release.resolve();
      await Promise.allSettled([blocker, unauthorized, cancellation]);
      await ledger.drain();
    }
  }));

test("Gateway accepts an empty database and preserves an unrelated database on rejection", () =>
  withLedgerDb(
    async (db) => {
      await db.unsafe("CREATE TABLE public.existing_data (value integer NOT NULL)");
      await db.unsafe("INSERT INTO public.existing_data VALUES (41)");
      await expect(migrate(db)).rejects.toThrow(/nonempty database/);
      expect(await db.unsafe("SELECT value FROM public.existing_data")).toEqual([{ value: 41 }]);
      await db.unsafe("DROP TABLE public.existing_data");
      await migrate(db);
      await migrate(db);
      expect(await assertSchemaVersion(db)).toBe(SCHEMA_VERSION);
      await db.unsafe("UPDATE public.schema_migrations SET version = 1");
      await expect(migrate(db)).rejects.toThrow(/manifest checksum mismatch/);
    },
    async () => {},
  ));

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
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        external_ref: "fencing-seed",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      const acquire = () =>
        node.acquireHold({
          request_id: crypto.randomUUID(),
          attempt_id: crypto.randomUUID(),
          billing_account_id: f.billingAccount.id,
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
      expect(await node.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
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

test("sharded outbox inserts proceed concurrently across distinct billingAccounts without skipping late commits", () =>
  withLedgerDb(async (db) => {
    const ledger = createLedgerEngine(db, { enableLeases: false });
    const projShardA = "00000000-0000-4000-8000-000000000001";
    const projShardB = "00000000-0000-4000-8000-000000000002";
    const eventA = crypto.randomUUID();
    const eventB = crypto.randomUUID();
    const enteredA = Promise.withResolvers<void>();
    const releaseA = Promise.withResolvers<void>();

    const txA = db.begin(async (tx) => {
      await tx`INSERT INTO public.outbox_events(event_id, kind, billing_account_id, payload)
        VALUES (${eventA}, 'billing.settled', ${projShardA}::uuid, '{"k":"a"}'::jsonb)`;
      enteredA.resolve();
      await releaseA.promise;
    });
    await enteredA.promise;

    const txB = db`INSERT INTO public.outbox_events(event_id, kind, billing_account_id, payload)
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
    const sameBillingAccount = "00000000-0000-4000-8000-000000000003";
    const event1 = crypto.randomUUID();
    const event2 = crypto.randomUUID();
    const tx1Locked = Promise.withResolvers<void>();
    const tx1Commit = Promise.withResolvers<void>();

    const tx1 = db.begin(async (tx) => {
      await tx`INSERT INTO public.outbox_events(event_id, kind, billing_account_id, payload)
        VALUES (${event1}, 'billing.settled', ${sameBillingAccount}::uuid, '{"order":1}'::jsonb)`;
      tx1Locked.resolve();
      await tx1Commit.promise;
    });
    await tx1Locked.promise;

    let tx2Finished = false;
    const tx2 = db`INSERT INTO public.outbox_events(event_id, kind, billing_account_id, payload)
      VALUES (${event2}, 'billing.settled', ${sameBillingAccount}::uuid, '{"order":2}'::jsonb)`.then(
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
      WHERE billing_account_id = ${sameBillingAccount}::uuid
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

test("concurrent Outbox claims and acknowledgements preserve the active shard lease", () =>
  withLedgerDb(async (db) => {
    const ledger = createLedgerEngine(db, { enableLeases: false });
    const proj = "00000000-0000-4000-8000-000000000004";
    await db.unsafe(
      "INSERT INTO public.outbox_events (event_id, kind, payload) VALUES (gen_random_uuid(), 'billing.settled', '{}')",
    );
    const active = await ledger.claimOutboxEvents("active-consumer");
    expect(active.events).toHaveLength(1);
    expect((await ledger.claimOutboxEvents("active-consumer")).events).toEqual([]);
    expect(
      await ledger.ackOutboxEvents("active-consumer", active.lease_token, active.events[0]!.seq),
    ).toBe(true);
    expect((await ledger.claimOutboxEvents("active-consumer")).events).toEqual([]);

    for (let i = 0; i < 20; i++) {
      await db`
        INSERT INTO public.outbox_events(event_id, kind, billing_account_id, payload)
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

test("configuration snapshots cannot label stale billingAccount state with the new epoch", () =>
  withLedgerDb(async (db) => {
    const catalog = createCatalogRepository(db);
    const billingAccount = await catalog.upsertBillingAccount({
      name: "snapshot-billingAccount",
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
      await tx`UPDATE public.billing_accounts SET enabled = false WHERE id = ${billingAccount.id}`;
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
    expect(current.billingAccounts.get(billingAccount.id)?.enabled).toBe(false);
    expect(snapshot.billingAccounts.get(billingAccount.id)?.enabled).toBe(
      snapshot.epoch < current.epoch,
    );
  }));

test("the timeout sweep leaves an expired hold alone when it was released concurrently", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const requestId = crypto.randomUUID();
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "sweep-race-fund",
      amount_atoms: "500",
      kind: "CASH",
    });
    const hold = await f.ledger.acquireHold({
      request_id: requestId,
      attempt_id: crypto.randomUUID(),
      billing_account_id: f.billingAccount.id,
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
      billing_account_id: f.billingAccount.id,
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
        billing_account_id: f.billingAccount.id,
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
    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
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
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        external_ref: "sweep-independent-fund",
        amount_atoms: "500",
        kind: "CASH",
      });
      const requestId = crypto.randomUUID();
      const hold = await f.ledger.acquireHold({
        request_id: requestId,
        attempt_id: crypto.randomUUID(),
        billing_account_id: f.billingAccount.id,
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
    const billingAccountIds = [
      "e01e6e75-7ad7-45de-87c4-000000000001",
      "e01e6e75-7ad7-45de-87c4-000000000002",
    ];
    const locked = Promise.withResolvers<void>();
    const unlock = Promise.withResolvers<void>();
    let blocking: Promise<unknown> | undefined;
    const remaining: string[] = [];
    const expiry = new Date(Date.now() + 30_000).toISOString();

    async function acquire(billingAccountId: string) {
      const requestId = crypto.randomUUID();
      const attemptId = crypto.randomUUID();
      const hold = await ledger.acquireHold({
        request_id: requestId,
        attempt_id: attemptId,
        billing_account_id: billingAccountId,
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
      for (const [i, id] of billingAccountIds.entries()) {
        await f.catalog.upsertBillingAccount({
          id,
          name: `lease-lane-${i}`,
          currency: "PTS",
          rpm: 1000,
          tpm: 100000,
          concurrency: 500,
        });
        await ledger.creditWallet({
          billing_account_id: id,
          currency: "PTS",
          kind: "CASH",
          external_ref: `lane-cash-${i}`,
          amount_atoms: "100000000",
        });
      }
      await Promise.all(
        billingAccountIds.flatMap((id) => Array.from({ length: 16 }, () => acquire(id))),
      );
      const [one, two] = await Promise.all(billingAccountIds.map(acquire));
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
        billing_account_id: f.billingAccount.id,
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
        billing_account_id: f.billingAccount.id,
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

test("symptom: a hold waiting for the sole funded cell blocks a settlement on another cell", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const locked = Promise.withResolvers<number>();
    const unlock = Promise.withResolvers<void>();
    let blocking: Promise<unknown> | undefined;
    const id = "e01e6e75-7ad7-45de-87c4-000000000001";
    const requestId = crypto.randomUUID();
    try {
      await f.catalog.upsertBillingAccount({
        id,
        name: "only-funded-cell",
        currency: "PTS",
        rpm: 1000,
        tpm: 100000,
        concurrency: 500,
      });
      await f.ledger.creditWallet({
        billing_account_id: id,
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
        const [backend] = await tx`SELECT pg_backend_pid() AS pid`;
        locked.resolve(Number(backend!.pid));
        await unlock.promise;
        await tx`SELECT cell FROM public.supplier_cells WHERE account_id = ${f.accountA.id}
          AND currency = 'USD' AND cell = 1 FOR NO KEY UPDATE NOWAIT`;
      });
      const blockerPid = await locked.promise;
      const pending = f.ledger.acquireHold({
        request_id: requestId,
        attempt_id: crypto.randomUUID(),
        billing_account_id: id,
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
      const waitDeadline = Date.now() + 2000;
      let waiting = false;
      while (!waiting && Date.now() < waitDeadline) {
        const [row] = await db`SELECT EXISTS (
          SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
            AND ${blockerPid} = ANY(pg_blocking_pids(pid))
        ) AS blocked`;
        waiting = row?.blocked === true;
        if (!waiting) await Bun.sleep(10);
      }
      expect(waiting).toBe(true);
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
        billing_account_id: f.billingAccount.id,
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
        `GRANT INSERT (event_id, kind, request_id, billing_account_id, payload) ON public.outbox_events TO ${roleName}`,
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
              billing_account_id: f.billingAccount.id,
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

test("same-account confirmed-FAILED switchSupplierAttempt preserves active leases for fast settle and release, and cross-account targeted cell switch conserves funds", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const channelA2 = await f.catalog.upsertChannel({
      account_id: f.accountA.id,
      name: "chan-a-2",
      kind: "openai",
      base_url: "https://a2.invalid/v1",
      secret_ciphertext: "enc-a2",
      rpm: 10000,
      concurrency: 1000,
    });
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "fast-switch-fund",
      amount_atoms: "100000",
      kind: "CASH",
    });

    const expiresAt = new Date(Date.now() + 60_000).toISOString();
    const warmup = Array.from({ length: 16 }, () => ({
      reqId: crypto.randomUUID(),
      attId: crypto.randomUUID(),
    }));
    const warmHolds = await Promise.all(
      warmup.map((item) =>
        f.ledger.acquireHold({
          request_id: item.reqId,
          attempt_id: item.attId,
          billing_account_id: f.billingAccount.id,
          currency: "PTS",
          operation: "chat",
          model: "gpt-4o",
          idempotency_key: null,
          fingerprint: "",
          customer_atoms: "100",
          supplier_account_id: f.accountA.id,
          supplier_cost_version_id: f.costPrice.id,
          supplier_channel_id: f.channelA.id,
          supplier_currency: "USD",
          supplier_atoms: "50",
          expires_at: expiresAt,
        }),
      ),
    );
    expect(warmHolds.every((h) => h.ok)).toBe(true);
    await Promise.all(
      warmup.map((item) =>
        f.ledger.commitTerminal({
          request_id: item.reqId,
          billing_account_id: f.billingAccount.id,
          selected_attempt_id: item.attId,
          channel_id: f.channelA.id,
          billing_price_id: f.flatBillingPrice.id,
          cost_version_id: f.costPrice.id,
          usage: { input_tokens: "10", output_tokens: "10" },
          billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
          billing_basis: "PROVIDER_USAGE",
          supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
          supplier_currency: "USD",
        }),
      ),
    );

    const reqCommit = crypto.randomUUID();
    const attCommit1 = crypto.randomUUID();
    const attCommit2 = crypto.randomUUID();
    const reqRelease = crypto.randomUUID();
    const attRelease1 = crypto.randomUUID();
    const attRelease2 = crypto.randomUUID();
    const reqCross = crypto.randomUUID();
    const attCross1 = crypto.randomUUID();
    const attCross2 = crypto.randomUUID();

    for (const [reqId, attId] of [
      [reqCommit, attCommit1],
      [reqRelease, attRelease1],
      [reqCross, attCross1],
    ] as const) {
      const h = await f.ledger.acquireHold({
        request_id: reqId,
        attempt_id: attId,
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        operation: "chat",
        model: "gpt-4o",
        idempotency_key: null,
        fingerprint: "",
        customer_atoms: "100",
        supplier_account_id: f.accountA.id,
        supplier_cost_version_id: f.costPrice.id,
        supplier_channel_id: f.channelA.id,
        supplier_currency: "USD",
        supplier_atoms: "50",
        expires_at: expiresAt,
      });
      expect(h.ok).toBe(true);
    }

    const beforeRows = await db<
      { request_id: string; wallet_lease_id: string | null; supplier_lease_id: string | null }[]
    >`SELECT request_id, wallet_lease_id, supplier_lease_id FROM public.billing_holds WHERE request_id IN (${reqCommit}, ${reqRelease}, ${reqCross})`;
    const beforeByReq = new Map(beforeRows.map((r) => [r.request_id, r]));
    expect(beforeByReq.get(reqCommit)?.wallet_lease_id).not.toBeNull();
    expect(beforeByReq.get(reqCommit)?.supplier_lease_id).not.toBeNull();
    expect(beforeByReq.get(reqRelease)?.wallet_lease_id).not.toBeNull();
    expect(beforeByReq.get(reqRelease)?.supplier_lease_id).not.toBeNull();

    const swCommit = await f.ledger.switchSupplierAttempt({
      request_id: reqCommit,
      failed_attempt_id: attCommit1,
      failed_outcome: "FAILED",
      next_attempt_id: attCommit2,
      next_account_id: f.accountA.id,
      next_channel_id: channelA2.id,
      next_currency: "USD",
      next_supplier_atoms: "50",
      next_supplier_cost_version_id: f.costPrice.id,
    });
    expect(swCommit).toMatchObject({ ok: true, held_atoms: "100" });

    const swRelease = await f.ledger.switchSupplierAttempt({
      request_id: reqRelease,
      failed_attempt_id: attRelease1,
      failed_outcome: "FAILED",
      next_attempt_id: attRelease2,
      next_account_id: f.accountA.id,
      next_channel_id: channelA2.id,
      next_currency: "USD",
      next_supplier_atoms: "50",
      next_supplier_cost_version_id: f.costPrice.id,
    });
    expect(swRelease).toMatchObject({ ok: true, held_atoms: "100" });

    const afterFastRows = await db<
      { request_id: string; wallet_lease_id: string | null; supplier_lease_id: string | null }[]
    >`SELECT request_id, wallet_lease_id, supplier_lease_id FROM public.billing_holds WHERE request_id IN (${reqCommit}, ${reqRelease})`;
    for (const row of afterFastRows) {
      expect(row.wallet_lease_id).toBe(beforeByReq.get(row.request_id)!.wallet_lease_id);
      expect(row.supplier_lease_id).toBe(beforeByReq.get(row.request_id)!.supplier_lease_id);
    }

    const swCross = await f.ledger.switchSupplierAttempt({
      request_id: reqCross,
      failed_attempt_id: attCross1,
      failed_outcome: "FAILED",
      next_attempt_id: attCross2,
      next_account_id: f.accountB.id,
      next_channel_id: f.channelB.id,
      next_currency: "USD",
      next_supplier_atoms: "50",
      next_supplier_cost_version_id: f.costPrice.id,
    });
    expect(swCross).toMatchObject({ ok: true, held_atoms: "100" });
    const [crossHold] = await db<
      { wallet_lease_id: string | null; supplier_lease_id: string | null }[]
    >`SELECT wallet_lease_id, supplier_lease_id FROM public.billing_holds WHERE request_id = ${reqCross}`;
    expect(crossHold?.wallet_lease_id).toBe(beforeByReq.get(reqCross)!.wallet_lease_id);
    expect(crossHold?.supplier_lease_id).toBeNull();

    const commitFast = await f.ledger.commitTerminal({
      request_id: reqCommit,
      billing_account_id: f.billingAccount.id,
      selected_attempt_id: attCommit2,
      channel_id: channelA2.id,
      billing_price_id: f.flatBillingPrice.id,
      cost_version_id: f.costPrice.id,
      usage: { input_tokens: "30", output_tokens: "20" },
      billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
      billing_basis: "PROVIDER_USAGE",
      supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
      supplier_currency: "USD",
    });
    expect(commitFast).toMatchObject({ ok: true, customer_atoms: "50", supplier_cost_atoms: "25" });

    await f.ledger.releaseHold(reqRelease);

    const commitCross = await f.ledger.commitTerminal({
      request_id: reqCross,
      billing_account_id: f.billingAccount.id,
      selected_attempt_id: attCross2,
      channel_id: f.channelB.id,
      billing_price_id: f.flatBillingPrice.id,
      cost_version_id: f.costPrice.id,
      usage: { input_tokens: "40", output_tokens: "20" },
      billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
      billing_basis: "PROVIDER_USAGE",
      supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
      supplier_currency: "USD",
    });
    expect(commitCross).toMatchObject({
      ok: true,
      customer_atoms: "60",
      supplier_cost_atoms: "30",
    });

    await f.ledger.drain();
    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
      available_atoms: "99570",
      reserved_atoms: "0",
      spent_atoms: "430",
    });
    expect(await f.ledger.getSupplierBalance(f.accountA.id, "USD")).toEqual({
      balance_atoms: "999815",
      reserved_atoms: "0",
      spent_atoms: "185",
    });
    expect(await f.ledger.getSupplierBalance(f.accountB.id, "USD")).toEqual({
      balance_atoms: "999970",
      reserved_atoms: "0",
      spent_atoms: "30",
    });
    expect(await f.ledger.auditConservation()).toEqual({
      ok: true,
      unbalanced_journals: 0,
      broken_lots: 0,
      wallet_mismatches: 0,
      reservation_mismatches: 0,
      customer_ledger_mismatches: 0,
      supplier_ledger_mismatches: 0,
      open_holds: 0,
      unknown_holds: 0,
    });
  }));

test("sweepTimedOutHolds detaches active wallet and supplier leases so swept UNKNOWN holds remain self-contained and reconcilable", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "sweep-detach-fund",
      amount_atoms: "100000",
      kind: "CASH",
    });

    const nodeLedger = createLedgerEngine(db, {
      enableLeases: true,
      leaseTtlMs: 30_000,
      leaseIdleReleaseMs: 30_000,
    });
    const workerLedger = createLedgerEngine(db, { enableLeases: false });

    try {
      const reqExpired = crypto.randomUUID();
      const attExpired = crypto.randomUUID();
      const reqLive = crypto.randomUUID();
      const attLive = crypto.randomUUID();
      const baseHold = {
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        operation: "chat" as const,
        model: "gpt-4o",
        idempotency_key: null,
        fingerprint: "",
        customer_atoms: "200",
        supplier_account_id: f.accountA.id,
        supplier_channel_id: f.channelA.id,
        supplier_currency: "USD",
        supplier_atoms: "100",
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      };
      const warmup = Array.from({ length: 8 }, () => ({
        request_id: crypto.randomUUID(),
        attempt_id: crypto.randomUUID(),
      }));
      await Promise.all(warmup.map((w) => nodeLedger.acquireHold({ ...baseHold, ...w })));
      await Promise.all(warmup.map((w) => nodeLedger.releaseHold(w.request_id)));

      const [h1, h2] = await Promise.all([
        nodeLedger.acquireHold({ ...baseHold, request_id: reqExpired, attempt_id: attExpired }),
        nodeLedger.acquireHold({ ...baseHold, request_id: reqLive, attempt_id: attLive }),
      ]);
      expect(h1.ok).toBe(true);
      expect(h2.ok).toBe(true);

      const [before] = await db`
        SELECT wallet_lease_id, supplier_lease_id, state
        FROM public.billing_holds WHERE request_id = ${reqExpired}
      `;
      expect(before?.state).toBe("HELD");
      expect(before?.wallet_lease_id).not.toBeNull();
      expect(before?.supplier_lease_id).not.toBeNull();

      await db`
        UPDATE public.billing_holds
        SET expires_at = now() - interval '1 second'
        WHERE request_id = ${reqExpired}
      `;

      const swept = await workerLedger.sweepTimedOutHolds(100);
      expect(swept).toBe(1);

      const [after] = await db`
        SELECT state, wallet_lease_id, supplier_lease_id, lot_allocations
        FROM public.billing_holds WHERE request_id = ${reqExpired}
      `;
      expect(after?.state).toBe("UNKNOWN");
      expect(after?.wallet_lease_id).toBeNull();
      expect(after?.supplier_lease_id).toBeNull();
      const allocs = (after?.lot_allocations ?? []) as { amount_atoms: string }[];
      const allocSum = allocs.reduce((s, a) => s + BigInt(a.amount_atoms), 0n);
      expect(allocSum).toBe(200n);

      const liveCommit = await nodeLedger.commitTerminal({
        request_id: reqLive,
        billing_account_id: f.billingAccount.id,
        selected_attempt_id: attLive,
        channel_id: f.channelA.id,
        billing_price_id: f.flatBillingPrice.id,
        cost_version_id: f.costPrice.id,
        usage: { input_tokens: "30", output_tokens: "20" },
        billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
        billing_basis: "PROVIDER_USAGE",
        supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
        supplier_currency: "USD",
      });
      expect(liveCommit).toMatchObject({
        ok: true,
        customer_atoms: "50",
        supplier_cost_atoms: "25",
      });

      expect(await workerLedger.auditConservation()).toMatchObject({
        ok: true,
        open_holds: 0,
        unknown_holds: 1,
      });

      const resolved = await workerLedger.resolveRequest({
        request_id: reqExpired,
        action: "RELEASE",
        reason: "verified_unused",
        actor: "ops",
      });
      expect(resolved.state).toBe("RELEASED");

      expect(await nodeLedger.markUnknown(reqExpired)).toBe(false);
      await nodeLedger.releaseIdleLeases();
      expect(await workerLedger.auditConservation()).toEqual({
        ok: true,
        unbalanced_journals: 0,
        broken_lots: 0,
        wallet_mismatches: 0,
        reservation_mismatches: 0,
        customer_ledger_mismatches: 0,
        supplier_ledger_mismatches: 0,
        open_holds: 0,
        unknown_holds: 0,
      });
    } finally {
      await nodeLedger.drain();
      await workerLedger.drain();
    }
  }));

test("worker periodic sweep drains a large expired-hold backlog in one tick instead of capping at 200 holds per interval", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "worker-drain-fund",
      amount_atoms: "1000000",
      kind: "CASH",
    });

    const total = 620;
    const base = {
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      operation: "chat" as const,
      model: "gpt-4o",
      idempotency_key: null,
      fingerprint: "",
      customer_atoms: "100",
      supplier_account_id: f.accountA.id,
      supplier_channel_id: f.channelA.id,
      supplier_currency: "USD",
      supplier_atoms: "50",
      expires_at: new Date(Date.now() - 1000).toISOString(),
    };
    const acquired = await Promise.all(
      Array.from({ length: total }, () =>
        f.ledger.acquireHold({
          ...base,
          request_id: crypto.randomUUID(),
          attempt_id: crypto.randomUUID(),
        }),
      ),
    );
    expect(acquired.every((r) => r.ok)).toBe(true);

    const bounded = await f.ledger.sweepTimedOutHolds(50);
    expect(bounded).toBe(50);
    const remaining = total - bounded;

    const worker = createWorkerRunner({
      db,
      sweepIntervalMs: 1200,
      maintenanceIntervalMs: 60_000,
      monitorIntervalMs: 60_000,
      partitionIntervalMs: 60_000,
    });
    await worker.start();
    try {
      const deadline = performance.now() + 3000;
      while (worker.status().last_successful_sweep === null && performance.now() < deadline) {
        await Bun.sleep(25);
      }
      expect(worker.status().last_successful_sweep).not.toBeNull();
      const audit = await f.ledger.auditConservation();
      expect(audit.ok).toBe(true);
      expect(audit.open_holds).toBe(0);
      expect(audit.unknown_holds).toBe(total);
      expect(worker.metrics()).toContain(`stg_worker_swept_holds_total ${remaining}\n`);
    } finally {
      await worker.stop();
      await f.ledger.drain();
    }
  }));

test("direct settlement microbatch isolates lock-timeout holds and releaseHold/markUnknown retry transient lock timeouts", () =>
  withLedgerDb(async (db, dbUrl) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "lock-retry-fund",
      amount_atoms: "100000",
      kind: "CASH",
    });

    const shortLockDb = new SQL(dbUrl, {
      max: 4,
      connection: {
        statement_timeout: 8000,
        lock_timeout: 30,
      },
    });
    const shortLedger = createLedgerEngine(shortLockDb, {
      enableLeases: false,
      batchWindowMs: 15,
      maxBatchSize: 64,
    });
    await shortLockDb`SELECT 1`;

    try {
      const reqLocked = crypto.randomUUID();
      const attLocked = crypto.randomUUID();
      const reqFree = crypto.randomUUID();
      const attFree = crypto.randomUUID();
      const reqRetry = crypto.randomUUID();
      const attRetry = crypto.randomUUID();
      const base = {
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        operation: "chat" as const,
        model: "gpt-4o",
        idempotency_key: null,
        fingerprint: "",
        customer_atoms: "200",
        supplier_account_id: f.accountA.id,
        supplier_channel_id: f.channelA.id,
        supplier_currency: "USD",
        supplier_atoms: "100",
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      };
      for (const [request_id, attempt_id] of [
        [reqLocked, attLocked],
        [reqFree, attFree],
        [reqRetry, attRetry],
      ] as const) {
        expect((await f.ledger.acquireHold({ ...base, request_id, attempt_id })).ok).toBe(true);
      }

      const hold1Locked = Promise.withResolvers<void>();
      const unlockHold1 = Promise.withResolvers<void>();
      const hold1Barrier = db.begin(async (tx) => {
        await tx`SELECT request_id FROM public.billing_holds WHERE request_id = ${reqLocked} FOR UPDATE`;
        hold1Locked.resolve();
        await unlockHold1.promise;
      });
      await hold1Locked.promise;

      const makeCommit = (request_id: string, selected_attempt_id: string) => ({
        request_id,
        billing_account_id: f.billingAccount.id,
        selected_attempt_id,
        channel_id: f.channelA.id,
        billing_price_id: f.flatBillingPrice.id,
        cost_version_id: f.costPrice.id,
        usage: { input_tokens: "10", output_tokens: "10" },
        billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
        billing_basis: "PROVIDER_USAGE" as const,
        supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
        supplier_currency: "USD",
      });

      const [resLocked, resFree] = await Promise.allSettled([
        shortLedger.commitTerminal(makeCommit(reqLocked, attLocked)),
        shortLedger.commitTerminal(makeCommit(reqFree, attFree)),
      ]);
      unlockHold1.resolve();
      await hold1Barrier;

      expect(resLocked.status).toBe("rejected");
      expect(resFree.status).toBe("fulfilled");
      if (resFree.status === "fulfilled") {
        expect(resFree.value).toMatchObject({ ok: true, customer_atoms: "20" });
      }

      const hold3Locked = Promise.withResolvers<void>();
      const hold3Barrier = db.begin(async (tx) => {
        await tx`SELECT request_id FROM public.billing_holds WHERE request_id = ${reqRetry} FOR UPDATE`;
        hold3Locked.resolve();
        await Bun.sleep(55);
      });
      await hold3Locked.promise;
      expect(await shortLedger.markUnknown(reqRetry)).toBe(true);
      await hold3Barrier;

      const hold1ReleaseLocked = Promise.withResolvers<void>();
      const hold1ReleaseBarrier = db.begin(async (tx) => {
        await tx`SELECT request_id FROM public.billing_holds WHERE request_id = ${reqLocked} FOR UPDATE`;
        hold1ReleaseLocked.resolve();
        await Bun.sleep(55);
      });
      await hold1ReleaseLocked.promise;
      expect(await shortLedger.releaseHold(reqLocked)).toBe(true);
      await hold1ReleaseBarrier;
      expect(await shortLedger.releaseHold(reqRetry)).toBe(true);

      expect(await f.ledger.auditConservation()).toMatchObject({
        ok: true,
        open_holds: 0,
        unknown_holds: 0,
      });
    } finally {
      await shortLedger.drain();
      await shortLockDb.close();
      await f.ledger.drain();
    }
  }));

test("symptom: unknown holds stay pending without evidence and duplicate receipts can settle or release only once", async () => {
  await withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "reconciliation-credit",
      amount_atoms: "1000",
      kind: "CASH",
    });
    const acquire = async (persistedPrice = true) => {
      const requestId = crypto.randomUUID();
      const attemptId = crypto.randomUUID();
      const result = await f.ledger.acquireHold({
        request_id: requestId,
        attempt_id: attemptId,
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        operation: "chat",
        model: "gpt-4o",
        idempotency_key: null,
        fingerprint: requestId,
        customer_atoms: "100",
        supplier_account_id: f.accountA.id,
        supplier_channel_id: f.channelA.id,
        supplier_currency: "USD",
        supplier_atoms: "40",
        supplier_cost_version_id: f.costPrice.id,
        ...(persistedPrice ? { customer_price_version_id: f.billingPrice.id } : {}),
        expires_at: new Date(Date.now() + 60_000).toISOString(),
      });
      expect(result.ok).toBe(true);
      return { requestId, attemptId };
    };
    const receipt = (request: Awaited<ReturnType<typeof acquire>>) => ({
      request_id: request.requestId,
      attempt_id: request.attemptId,
      provider_request_id: "vendor-" + request.attemptId,
      account_id: f.accountA.id,
      channel_id: f.channelA.id,
      currency: "USD",
      evidence_ref: "receipt-" + request.attemptId,
      actor: "finance-test",
    });
    const customer = await acquire();
    const legs = await acquire();
    const legacy = await acquire(false);
    for (const current of [customer, legs, legacy]) {
      await f.ledger.markUnknown(
        current.requestId,
        current === legacy
          ? undefined
          : new Map([[current.attemptId, "vendor-" + current.attemptId]]),
      );
    }
    expect(await f.ledger.processReconciliation(10)).toEqual({ receipts: 0, waivers: 0 });
    expect((await f.ledger.getWallet(f.billingAccount.id, "PTS"))?.reserved_atoms).toBe("300");

    expect(
      await f.ledger.submitReconciliationReceipt({ ...receipt(legacy), verdict: "NOT_EXECUTED" }),
    ).toEqual({ status: "match_evidence_required" });
    expect(
      await f.ledger.submitReconciliationReceipt({
        ...receipt(customer),
        provider_request_id: "another-request",
        verdict: "NOT_EXECUTED",
      }),
    ).toEqual({ status: "identity_conflict" });

    const verified = {
      ...receipt(customer),
      verdict: "FINAL" as const,
      supplier_cost_atoms: "19",
      customer_usage: { input_tokens: "30", output_tokens: "9" },
    };
    expect((await f.ledger.submitReconciliationReceipt(verified)).status).toBe("accepted");
    expect((await f.ledger.submitReconciliationReceipt(verified)).status).toBe("replayed");
    const noExecution = { ...receipt(legs), verdict: "NOT_EXECUTED" as const };
    expect((await f.ledger.submitReconciliationReceipt(noExecution)).status).toBe("accepted");
    expect(await f.ledger.processReconciliation(10)).toEqual({ receipts: 2, waivers: 0 });
    expect(await f.ledger.processReconciliation(10)).toEqual({ receipts: 0, waivers: 0 });
    expect(await f.ledger.submitReconciliationReceipt(verified)).toEqual({
      status: "replayed",
      state: "APPLIED",
    });
    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
      reserved_atoms: "100",
      spent_atoms: "13",
    });
    const [charge] =
      await db`SELECT count(*)::int AS n, sum(supplier_cost_atoms)::text AS supplier FROM public.billing_charges`;
    expect(charge).toEqual({ n: 1, supplier: "19" });
    expect(await f.ledger.auditConservation()).toMatchObject({
      ok: true,
      open_holds: 0,
      unknown_holds: 1,
    });

    const supplierFirst = await acquire();
    await f.ledger.rememberProviderRequestId(
      supplierFirst.requestId,
      supplierFirst.attemptId,
      "vendor-" + supplierFirst.attemptId,
    );
    await f.ledger.markUnknown(supplierFirst.requestId);
    const pollScope = { account_id: f.accountA.id, channel_id: f.channelA.id, currency: "USD" };
    expect(
      await f.ledger.claimReconciliationPoll({ ...pollScope, account_id: f.accountB.id }, 60_000),
    ).toBeNull();
    expect(await f.ledger.claimReconciliationPoll(pollScope, 60_000)).toEqual({
      request_id: supplierFirst.requestId,
      attempt_id: supplierFirst.attemptId,
      provider_request_id: "vendor-" + supplierFirst.attemptId,
    });
    expect(await f.ledger.claimReconciliationPoll(pollScope, 60_000)).toBeNull();
    const supplierReceipt = {
      ...receipt(supplierFirst),
      verdict: "FINAL" as const,
      supplier_cost_atoms: "19",
    };
    expect((await f.ledger.submitReconciliationReceipt(supplierReceipt)).status).toBe("accepted");
    expect(
      await f.ledger
        .resolveRequest({
          request_id: supplierFirst.requestId,
          action: "RELEASE",
          reason: "conflicts with final receipt",
          actor: "finance-test",
        })
        .catch((error: Error) => error.message),
    ).toBe("Request already has a reconciliation receipt");
    const worker = createWorkerRunner({ db });
    try {
      await worker.runOnce(false);
    } finally {
      await worker.stop();
    }
    expect((await f.ledger.getSupplierBalance(f.accountA.id, "USD")).spent_atoms).toBe("38");
    expect((await f.ledger.getWallet(f.billingAccount.id, "PTS"))?.spent_atoms).toBe("13");
    expect(
      (
        await f.ledger.submitReconciliationReceipt({
          ...supplierReceipt,
          customer_usage: { input_tokens: "30", output_tokens: "9" },
        })
      ).status,
    ).toBe("accepted");
    expect(await f.ledger.processReconciliation(5)).toEqual({ receipts: 1, waivers: 0 });
    expect((await f.ledger.getSupplierBalance(f.accountA.id, "USD")).spent_atoms).toBe("38");
    expect((await f.ledger.getWallet(f.billingAccount.id, "PTS"))?.spent_atoms).toBe("26");
    const [linked] = await db`SELECT c.supplier_journal_id IS NOT NULL AS linked,
      EXISTS (SELECT 1 FROM public.ledger_entries e WHERE e.journal_id=c.supplier_journal_id) AS present
      FROM public.billing_charges c WHERE c.request_id=${supplierFirst.requestId}`;
    expect(linked).toEqual({ linked: true, present: true });
    expect((await f.ledger.auditConservation()).ok).toBe(true);

    const other = await acquire();
    await f.ledger.markUnknown(
      other.requestId,
      new Map([[other.attemptId, "vendor-" + other.attemptId]]),
    );
    const old = {
      ...receipt(legacy),
      match_reason: "matched to a supplier case",
      verdict: "NOT_EXECUTED" as const,
    };
    expect((await f.ledger.submitReconciliationReceipt(old)).status).toBe("accepted");
    expect(
      (
        await f.ledger.submitReconciliationReceipt({
          ...receipt(other),
          evidence_ref: old.evidence_ref,
          verdict: "NOT_EXECUTED",
        })
      ).status,
    ).toBe("receipt_conflict");
    expect(await f.ledger.processReconciliation(5)).toEqual({ receipts: 0, waivers: 0 });
    expect(
      (await f.ledger.listReconciliationCases(10, 0)).find(
        (entry) => entry.request_id === legacy.requestId,
      )?.receipts,
    ).toMatchObject([{ state: "REVIEW", result: "conflicting_receipt" }]);
    expect(
      await f.ledger.reviewReconciliationReceipt({
        attempt_id: legacy.attemptId,
        action: "DISMISS",
        review_evidence_ref: "supplier-correction",
        reason: "original reference was duplicated",
        actor: "finance-review",
      }),
    ).toMatchObject({ state: "DISMISSED" });
    expect(
      await f.ledger.resolveRequest({
        request_id: legacy.requestId,
        action: "RELEASE",
        reason: "supplier correction confirmed no execution",
        actor: "finance-review",
      }),
    ).toMatchObject({ state: "RELEASED" });
    expect(await f.ledger.auditConservation()).toMatchObject({ ok: true, unknown_holds: 1 });
  });
});

test("symptom: a late supplier receipt after a bounded customer waiver never reopens the customer charge", async () => {
  await withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "waiver-credit",
      amount_atoms: "500",
      kind: "CASH",
    });
    const requestId = crypto.randomUUID();
    const attemptId = crypto.randomUUID();
    const held = await f.ledger.acquireHold({
      request_id: requestId,
      attempt_id: attemptId,
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      operation: "chat",
      model: "gpt-4o",
      idempotency_key: null,
      fingerprint: requestId,
      customer_atoms: "100",
      supplier_account_id: f.accountA.id,
      supplier_channel_id: f.channelA.id,
      supplier_currency: "USD",
      supplier_atoms: "40",
      supplier_cost_version_id: f.costPrice.id,
      customer_price_version_id: f.billingPrice.id,
      expires_at: new Date(Date.now() + 60_000).toISOString(),
    });
    expect(held.ok).toBe(true);
    await f.ledger.markUnknown(requestId, new Map([[attemptId, "late-vendor-id"]]));
    await db`UPDATE public.billing_holds SET reconciliation_started_at=now()-interval '1 hour' WHERE request_id=${requestId}`;
    expect(await f.ledger.processReconciliation(5)).toEqual({ receipts: 0, waivers: 0 });
    await f.ledger.putReconciliationBudget("USD", "39");
    const policy = {
      billing_account_id: f.billingAccount.id,
      channel_id: f.channelA.id,
      supplier_currency: "USD",
      enabled: true,
      include_historical: true,
      age_seconds: 1800,
      max_customer_atoms: "100",
      limit_atoms: "40",
    };
    await f.ledger.putReconciliationPolicy(policy);
    expect(await f.ledger.processReconciliation(5)).toEqual({ receipts: 0, waivers: 0 });
    await f.ledger.putReconciliationBudget("USD", "40");
    expect(await f.ledger.processReconciliation(5)).toEqual({ receipts: 0, waivers: 1 });
    expect(await f.ledger.putReconciliationBudget("USD", "39")).toBeNull();
    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
      available_atoms: "500",
      reserved_atoms: "0",
      spent_atoms: "0",
    });
    expect(await f.ledger.getSupplierBalance(f.accountA.id, "USD")).toMatchObject({
      reserved_atoms: "40",
    });
    expect(
      (await f.ledger.listReconciliationCases(10, 0)).map((entry) => entry.request_id),
    ).toEqual([requestId]);

    expect(
      (
        await f.ledger.submitReconciliationReceipt({
          request_id: requestId,
          attempt_id: attemptId,
          account_id: f.accountA.id,
          channel_id: f.channelA.id,
          currency: "USD",
          provider_request_id: "late-vendor-id",
          evidence_ref: "vendor-invoice-row-1",
          actor: "finance-test",
          verdict: "FINAL",
          supplier_cost_atoms: "16",
          customer_usage: { input_tokens: "30", output_tokens: "6" },
        })
      ).status,
    ).toBe("accepted");
    expect(await f.ledger.processReconciliation(5)).toEqual({ receipts: 1, waivers: 0 });
    expect(await f.ledger.processReconciliation(5)).toEqual({ receipts: 0, waivers: 0 });
    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
      available_atoms: "500",
      reserved_atoms: "0",
      spent_atoms: "0",
    });
    expect(await f.ledger.getSupplierBalance(f.accountA.id, "USD")).toMatchObject({
      reserved_atoms: "0",
      spent_atoms: "16",
    });
    expect(await f.ledger.auditConservation()).toMatchObject({
      ok: true,
      open_holds: 0,
      unknown_holds: 0,
    });
  });
});
