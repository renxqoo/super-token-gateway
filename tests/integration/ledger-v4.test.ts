import { expect, test } from "bun:test";
import { SQL } from "bun";
import {
  assertSchemaVersion,
  createAsyncTasksRepository,
  createLedgerEngine,
  createLedgerQueries,
  migrate,
  SCHEMA_VERSION,
} from "@gateway/ledger";
import { withLedgerDb, seedFixture } from "./ledger-fixture.ts";

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

test.each(["transient wallet lock", "wallet statement timeout"])(
  "symptom: a %s must not discard an active funded lease",
  (scenario) =>
    withLedgerDb(async (db, dbUrl) => {
      const fixture = await seedFixture(db);
      const { billingAccount, accountA, channelA } = fixture;
      const statementTimeout = scenario === "wallet statement timeout";
      const limitedDb = statementTimeout
        ? new SQL(dbUrl, { max: 8, connection: { lock_timeout: 5000, statement_timeout: 200 } })
        : undefined;
      const ledger = limitedDb ? createLedgerEngine(limitedDb) : fixture.ledger;
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
        expect(await hold(wallet.available_atoms, "earmark-free-funds")).toMatchObject({
          ok: true,
        });
        expect(await ledger.getWallet(billingAccount.id, "PTS")).toMatchObject({
          available_atoms: "0",
        });
        const locked = Promise.withResolvers<void>();
        const lock = db.begin(async (tx) => {
          await tx`SELECT id FROM public.wallets WHERE billing_account_id = ${billingAccount.id} FOR NO KEY UPDATE`;
          locked.resolve();
          await Bun.sleep(statementTimeout ? 260 : 160);
        });
        await locked.promise;
        const [admitted] = await Promise.all([hold(), lock]);
        expect(admitted).toMatchObject({ ok: true, held_atoms: "200", credit_atoms: "0" });
        expect((await ledger.auditConservation()).ok).toBe(true);
      } finally {
        await ledger.drain();
        if (limitedDb) {
          await limitedDb.close();
          await fixture.ledger.drain();
        }
      }
    }),
);

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
      await db`UPDATE public.schema_migrations SET version = 1 WHERE version = ${SCHEMA_VERSION}`;
      await expect(migrate(db)).rejects.toThrow(/manifest checksum mismatch/);
    },
    async () => {},
  ));

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
