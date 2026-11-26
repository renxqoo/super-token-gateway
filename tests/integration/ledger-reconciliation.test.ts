import { expect, test } from "bun:test";
import { SQL } from "bun";
import { createWorkerRunner } from "@gateway/worker";
import { createLedgerEngine, type CommitTerminalInput } from "@gateway/ledger";
import { withLedgerDb, seedFixture } from "./ledger-fixture.ts";

test("symptom: a settlement statement timeout discards measured usage for other requests in its batch", () =>
  withLedgerDb(async (db, dbUrl) => {
    const f = await seedFixture(db);
    const limitedDb = new SQL(dbUrl, {
      max: 2,
      connection: { statement_timeout: 80, lock_timeout: 5000 },
    });
    const limited = createLedgerEngine(limitedDb, { enableLeases: false, batchWindowMs: 20 });
    let blocker: Promise<unknown> | undefined;
    const unlock = Promise.withResolvers<void>();
    try {
      await limitedDb`SELECT 1`;
      await f.ledger.creditWallet({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        external_ref: "settlement-statement-timeout",
        amount_atoms: "1000",
        kind: "CASH",
      });
      const measured: CommitTerminalInput[] = [];
      for (let index = 0; index < 2; index++) {
        const requestId = crypto.randomUUID();
        const attemptId = crypto.randomUUID();
        expect(
          await f.ledger.acquireHold({
            request_id: requestId,
            attempt_id: attemptId,
            billing_account_id: f.billingAccount.id,
            currency: "PTS",
            operation: "chat",
            model: "gpt-4o",
            idempotency_key: null,
            fingerprint: requestId,
            customer_atoms: "200",
            supplier_account_id: f.accountA.id,
            supplier_channel_id: f.channelA.id,
            supplier_currency: "USD",
            supplier_atoms: "100",
            expires_at: new Date(Date.now() + 30_000).toISOString(),
          }),
        ).toMatchObject({ ok: true });
        measured.push({
          request_id: requestId,
          billing_account_id: f.billingAccount.id,
          selected_attempt_id: attemptId,
          channel_id: f.channelA.id,
          billing_price_id: f.flatBillingPrice.id,
          cost_version_id: f.costPrice.id,
          usage: { input_tokens: "10", output_tokens: "10" },
          billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
          billing_basis: "PROVIDER_USAGE",
          supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
          supplier_currency: "USD",
        });
      }
      const locked = Promise.withResolvers<void>();
      blocker = db.begin(async (tx) => {
        await tx`SELECT request_id FROM public.billing_holds
          WHERE request_id = ${measured[0]!.request_id} FOR UPDATE`;
        locked.resolve();
        await unlock.promise;
      });
      await locked.promise;
      const commits = measured.map((item) => limited.commitTerminal(item));
      const outcomes = Promise.allSettled(commits);
      expect(await commits[1]).toMatchObject({ ok: true, customer_atoms: "20" });
      unlock.resolve();
      await blocker;
      const [original, unrelated] = await outcomes;
      expect(unrelated).toMatchObject({
        status: "fulfilled",
        value: { ok: true, customer_atoms: "20", supplier_cost_atoms: "10" },
      });
      const recovered =
        original?.status === "fulfilled"
          ? original.value
          : await limited.commitTerminal(measured[0]!);
      expect(recovered).toMatchObject({
        ok: true,
        customer_atoms: "20",
        supplier_cost_atoms: "10",
      });
      expect(await limited.commitTerminal(measured[0]!)).toMatchObject({
        ok: true,
        replayed: true,
      });
      expect(await f.ledger.auditConservation()).toMatchObject({
        ok: true,
        open_holds: 0,
        unknown_holds: 0,
      });
    } finally {
      unlock.resolve();
      await blocker;
      await limited.drain();
      await limitedDb.close();
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
