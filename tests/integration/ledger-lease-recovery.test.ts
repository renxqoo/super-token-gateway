import { expect, test } from "bun:test";
import { createWorkerRunner } from "@gateway/worker";
import { createLedgerEngine } from "@gateway/ledger";
import { withLedgerDb, seedFixture } from "./ledger-fixture.ts";

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

test("symptom: a stale lease owner settles a supplier attempt replaced by another ledger engine", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const switchingLedger = createLedgerEngine(db, { batchWindowMs: 2, maxBatchSize: 64 });
    try {
      await f.ledger.creditWallet({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        external_ref: "stale-lease-attempt-funding",
        amount_atoms: "100000",
        kind: "CASH",
      });
      const expiresAt = new Date(Date.now() + 60_000).toISOString();
      function acquire(requestId: string, attemptId: string) {
        return f.ledger.acquireHold({
          request_id: requestId,
          attempt_id: attemptId,
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
          expires_at: expiresAt,
        });
      }
      const warmup = Array.from({ length: 16 }, () => crypto.randomUUID());
      const warmed = await Promise.all(warmup.map((id) => acquire(id, crypto.randomUUID())));
      expect(warmed.every((h) => h.ok)).toBe(true);
      await Promise.all(warmup.map((id) => f.ledger.releaseHold(id)));

      const requestId = crypto.randomUUID();
      const original = crypto.randomUUID();
      const replacement = crypto.randomUUID();
      expect(await acquire(requestId, original)).toMatchObject({ ok: true });
      const leaseIds = () => db`
      SELECT wallet_lease_id, supplier_lease_id FROM public.billing_holds WHERE request_id = ${requestId}::uuid
    `;
      const [before] = await leaseIds();
      expect(typeof before?.wallet_lease_id).toBe("string");
      expect(typeof before?.supplier_lease_id).toBe("string");
      expect(
        await switchingLedger.switchSupplierAttempt({
          request_id: requestId,
          failed_attempt_id: original,
          failed_outcome: "FAILED",
          next_attempt_id: replacement,
          next_account_id: f.accountA.id,
          next_channel_id: f.channelA.id,
          next_currency: "USD",
          next_supplier_atoms: "50",
        }),
      ).toMatchObject({ ok: true });
      expect(await leaseIds()).toEqual([before]);

      const terminal = {
        request_id: requestId,
        selected_attempt_id: original,
        channel_id: f.channelA.id,
        billing_price_id: f.flatBillingPrice.id,
        cost_version_id: f.costPrice.id,
        usage: { input_tokens: "10", output_tokens: "10" },
        billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
        billing_basis: "PROVIDER_USAGE" as const,
        supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
        supplier_currency: "USD",
      };
      expect(await f.ledger.commitTerminal(terminal)).toMatchObject({
        ok: false,
        code: "invalid_state",
      });
      expect(await leaseIds()).toEqual([before]);
      expect((await f.ledger.getWallet(f.billingAccount.id, "PTS"))?.spent_atoms).toBe("0");
      expect((await f.ledger.getSupplierBalance(f.accountA.id, "USD")).spent_atoms).toBe("0");

      expect(
        await f.ledger.commitTerminal({ ...terminal, selected_attempt_id: replacement }),
      ).toMatchObject({
        ok: true,
        replayed: false,
        customer_atoms: "20",
        supplier_cost_atoms: "10",
      });
      expect(await f.ledger.auditConservation()).toMatchObject({ ok: true, open_holds: 0 });
    } finally {
      await Promise.all([f.ledger.drain(), switchingLedger.drain()]);
    }
  }));
