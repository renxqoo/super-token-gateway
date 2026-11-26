import { expect, test } from "bun:test";
import { createLedgerEngine, createLedgerQueries, type CommitTerminalInput } from "@gateway/ledger";
import { withLedgerDb, seedFixture } from "./ledger-fixture.ts";

test("symptom: a wallet lease is recreated between bursts inside the configured retention window", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const ledger = createLedgerEngine(db, { walletLeaseIdleReleaseMs: 1500 });
    const wallet = await f.ledger.creditWallet({
      billing_account_id: f.billingAccount.id,
      currency: "PTS",
      external_ref: "retained-wallet-funding",
      amount_atoms: "100000000",
      kind: "CASH",
    });
    const hold = async () => {
      const requestId = crypto.randomUUID();
      const attemptId = crypto.randomUUID();
      expect(
        await ledger.acquireHold({
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
          expires_at: new Date(Date.now() + 30000).toISOString(),
        }),
      ).toMatchObject({ ok: true, held_atoms: "200" });
      return {
        request_id: requestId,
        selected_attempt_id: attemptId,
        billing_account_id: f.billingAccount.id,
        channel_id: f.channelA.id,
        billing_price_id: f.flatBillingPrice.id,
        cost_version_id: f.costPrice.id,
        usage: { input_tokens: "60", output_tokens: "40" },
        billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
        billing_basis: "PROVIDER_USAGE",
        supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
        supplier_currency: "USD",
      } satisfies CommitTerminalInput;
    };
    const settle = async (input: CommitTerminalInput) => {
      expect(await ledger.commitTerminal(input)).toMatchObject({
        ok: true,
        customer_atoms: "100",
        supplier_cost_atoms: "50",
      });
    };
    const walletLeaseRows = () => db<{ id: string; state: string }[]>`
      SELECT id, state FROM public.wallet_leases WHERE wallet_id = ${wallet.wallet_id}
    `;
    try {
      await Promise.all((await Promise.all(Array.from({ length: 16 }, hold))).map(settle));
      const original = await walletLeaseRows();
      expect(original).toHaveLength(1);
      expect(original[0]?.state).toBe("ACTIVE");
      await Bun.sleep(800);
      expect(await walletLeaseRows()).toEqual(original);
      await Promise.all((await Promise.all(Array.from({ length: 16 }, hold))).map(settle));
      expect(await walletLeaseRows()).toEqual(original);
    } finally {
      await ledger.drain();
      await f.ledger.drain();
    }
    expect((await walletLeaseRows()).map((row) => row.state)).toEqual(["CLOSED"]);
    expect(await f.ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
      available_atoms: "99996800",
      reserved_atoms: "0",
      spent_atoms: "3200",
    });
    expect((await f.ledger.getSupplierBalance(f.accountA.id, "USD")).spent_atoms).toBe("1600");
    expect((await ledger.auditConservation()).ok).toBe(true);
  }));

test("idle wallet lease batch returns unused funds and preserves a concurrent open hold", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const ledger = createLedgerEngine(db, { leaseIdleReleaseMs: 5000 });
    const billingAccounts = await Promise.all(
      [0, 1, 2].map((i) =>
        f.catalog.upsertBillingAccount({
          name: `batch-close-${i}`,
          currency: "PTS",
          rpm: 100,
          tpm: 10000,
          concurrency: 32,
        }),
      ),
    );
    const walletIds = [];
    for (const billingAccount of billingAccounts) {
      const wallet = await f.ledger.creditWallet({
        billing_account_id: billingAccount.id,
        currency: "PTS",
        external_ref: "batch-close-funding",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      walletIds.push(wallet.wallet_id);
    }

    async function hold(billingAccountId: string): Promise<string> {
      const requestId = crypto.randomUUID();
      expect(
        await ledger.acquireHold({
          request_id: requestId,
          attempt_id: crypto.randomUUID(),
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
          expires_at: new Date(Date.now() + 30000).toISOString(),
        }),
      ).toMatchObject({ ok: true, held_atoms: "200" });
      return requestId;
    }

    try {
      for (const billingAccount of billingAccounts) {
        const requests = await Promise.all(
          Array.from({ length: 16 }, () => hold(billingAccount.id)),
        );
        for (const requestId of requests) await ledger.releaseHold(requestId);
      }
      const open = await hold(billingAccounts[2]!.id);
      const [before] = await db`
        SELECT wallet_lease_id FROM public.billing_holds WHERE request_id = ${open}
      `;
      expect(before.wallet_lease_id).toBeString();
      expect(
        (await db`SELECT count(*)::int AS n FROM public.wallet_leases WHERE state = 'ACTIVE'`)[0].n,
      ).toBe(3);
      await ledger.releaseIdleLeases();
      const [after] = await db`
        SELECT state, wallet_lease_id, held_atoms::text AS held_atoms
        FROM public.billing_holds WHERE request_id = ${open}
      `;
      expect(after).toMatchObject({ state: "HELD", wallet_lease_id: null, held_atoms: "200" });
      const [returnBatch] = await db`
        SELECT count(*)::int AS closed, count(DISTINCT xmin::text)::int AS commits
        FROM public.wallet_leases
        WHERE wallet_id = ANY(${walletIds.slice(0, 2)}::uuid[]) AND state = 'CLOSED'
      `;
      expect(returnBatch).toMatchObject({ closed: 2, commits: 1 });
      const balances = await db`
        SELECT id, available_atoms::text AS available, reserved_atoms::text AS reserved
        FROM public.wallets WHERE id = ANY(${walletIds}::uuid[])
      `;
      expect(balances).toHaveLength(3);
      for (const balance of balances) {
        const openWallet = balance.id === walletIds[2];
        expect(balance).toMatchObject({
          available: openWallet ? "99999800" : "100000000",
          reserved: openWallet ? "200" : "0",
        });
      }
      await ledger.releaseHold(open);
      expect((await ledger.auditConservation()).ok).toBe(true);
    } finally {
      await ledger.drain();
      await f.ledger.drain();
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
