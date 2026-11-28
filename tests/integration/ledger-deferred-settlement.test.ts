import { expect, test } from "bun:test";
import {
  createLedgerEngine,
  createLedgerQueries,
  fastUuid,
  type AcquireHoldInput,
  type CommitTerminalInput,
} from "@gateway/ledger";
import { withLedgerDb, seedFixture } from "./ledger-fixture.ts";

type Fixture = Awaited<ReturnType<typeof seedFixture>>;
type LedgerEngine = ReturnType<typeof createLedgerEngine>;

const CONSERVATION_CLEAN = {
  ok: true,
  unbalanced_journals: 0,
  broken_lots: 0,
  wallet_mismatches: 0,
  reservation_mismatches: 0,
  customer_ledger_mismatches: 0,
  supplier_ledger_mismatches: 0,
  open_holds: 0,
  unknown_holds: 0,
};

function makeCommit(
  f: Fixture,
  requestId: string,
  attemptId: string,
  billingAccountId: string,
  inputTokens = "60",
  outputTokens = "40",
  extra: Partial<CommitTerminalInput> = {},
): CommitTerminalInput {
  return {
    request_id: requestId,
    billing_account_id: billingAccountId,
    selected_attempt_id: attemptId,
    channel_id: f.channelA.id,
    billing_price_id: f.flatBillingPrice.id,
    cost_version_id: f.costPrice.id,
    usage: { input_tokens: inputTokens, output_tokens: outputTokens },
    billing_pricing: { currency: "PTS", rates: f.flatBillingPrice.rates },
    billing_basis: "PROVIDER_USAGE",
    supplier_pricing: { currency: "USD", rates: f.costPrice.rates },
    supplier_currency: "USD",
    ...extra,
  };
}

async function acquireChatHold(
  ledger: LedgerEngine,
  f: Fixture,
  billingAccountId: string,
  expiresAt: string,
  overrides: Partial<AcquireHoldInput> = {},
) {
  const requestId = overrides.request_id ?? crypto.randomUUID();
  const attemptId = overrides.attempt_id ?? crypto.randomUUID();
  const res = await ledger.acquireHold({
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
    expires_at: expiresAt,
    ...overrides,
  });
  return { res, requestId, attemptId };
}

async function warmLeases(
  ledger: LedgerEngine,
  f: Fixture,
  billingAccountId: string,
  expiresAt: string,
  count = 16,
) {
  const warm = await Promise.all(
    Array.from({ length: count }, () => acquireChatHold(ledger, f, billingAccountId, expiresAt)),
  );
  expect(warm.every((w) => w.res.ok)).toBe(true);
  await Promise.all(warm.map((w) => ledger.releaseHold(w.requestId)));
}

test("deferred lease spend is visible in live reads, conservation, and refunds before drain and flushes cleanly on drain", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const ledger = createLedgerEngine(db, {
      leaseIdleReleaseMs: 60_000,
      leaseTtlMs: 60_000,
      batchWindowMs: 1,
    });
    const expiry = new Date(Date.now() + 60_000).toISOString();

    try {
      const credit = await ledger.creditWallet({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        external_ref: "deferred-live-cash",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      await ledger.rechargeSupplier({
        account_id: f.accountA.id,
        currency: "USD",
        external_ref: "deferred-live-supplier",
        amount_atoms: "500000000",
      });

      await warmLeases(ledger, f, f.billingAccount.id, expiry);

      const from = new Date(Date.now() - 1000);
      const leasedReqs = await Promise.all(
        Array.from({ length: 5 }, () =>
          acquireChatHold(ledger, f, f.billingAccount.id, expiry, { request_id: fastUuid() }),
        ),
      );
      expect(leasedReqs.every((r) => r.res.ok)).toBe(true);
      const committed = await Promise.all(
        leasedReqs.map((r) =>
          ledger.commitTerminal(makeCommit(f, r.requestId, r.attemptId, f.billingAccount.id)),
        ),
      );
      expect(
        committed.every(
          (c) => c.ok && c.customer_atoms === "100" && c.supplier_cost_atoms === "50",
        ),
      ).toBe(true);
      const [shared] = await db<{ n: number }[]>`
        SELECT count(*)::int AS n FROM public.ledger_entries
        WHERE request_id IS NULL AND reference LIKE 'supplier_cost_batch:%' AND account = 'upstream:expense'
      `;
      expect(shared!.n).toBeGreaterThan(0);
      expect(
        await createLedgerQueries(db).getRoutingCostReport({
          model: "gpt-4o",
          from,
          to: new Date(Date.now() + 1000),
        }),
      ).toEqual({
        cohort_requests: 5,
        currencies: [
          {
            currency: "USD",
            supplier_cost_atoms: "250",
            unknown_reserved_atoms: "0",
            unknown_attempts: 0,
            unknown_attempts_without_reconciliation_start: 0,
            oldest_unknown_reconciliation_age_ms: null,
          },
        ],
      });

      const [rawWallet] = await db<{ spent_atoms: string; reserved_atoms: string }[]>`
        SELECT spent_atoms::text, reserved_atoms::text FROM public.wallets WHERE id = ${credit.wallet_id}::uuid
      `;
      expect(rawWallet?.spent_atoms).toBe("0");
      const [rawLease] = await db<
        {
          id: string;
          spent_atoms: string;
          spent_lot_id: string | null;
          remaining_atoms: string;
        }[]
      >`
        SELECT id::text, spent_atoms::text, spent_lot_id::text, remaining_atoms::text
        FROM public.wallet_leases WHERE wallet_id = ${credit.wallet_id}::uuid AND state = 'ACTIVE'
      `;
      expect(rawLease).toMatchObject({ spent_atoms: "500", spent_lot_id: credit.lot_id });

      const [rawSupSpent] = await db<{ spent: string }[]>`
        SELECT coalesce(sum(spent_atoms), 0)::text AS spent
        FROM public.supplier_cells WHERE account_id = ${f.accountA.id}::uuid AND currency = 'USD'
      `;
      expect(rawSupSpent?.spent).toBe("0");

      const liveWallet = await ledger.getWallet(f.billingAccount.id, "PTS");
      expect(liveWallet).toMatchObject({
        spent_atoms: "500",
        reserved_atoms: rawLease!.remaining_atoms,
      });
      const liveWithLots = await ledger.getWalletWithLots(f.billingAccount.id, "PTS");
      expect(liveWithLots).toMatchObject({
        spent_atoms: "500",
        reserved_atoms: rawLease!.remaining_atoms,
      });
      expect(liveWithLots?.lots.find((l) => l.id === credit.lot_id)).toMatchObject({
        spent_atoms: "500",
        reserved_atoms: rawLease!.remaining_atoms,
      });

      const liveSup = await ledger.getSupplierBalance(f.accountA.id, "USD");
      expect(liveSup.spent_atoms).toBe("250");
      expect(liveSup.balance_atoms).toBe(String(501_000_000 - 250));

      expect(
        await ledger.configureWalletCredit({
          billing_account_id: f.billingAccount.id,
          currency: "PTS",
          expected_version: "0",
          credit_limit_atoms: "1000",
        }),
      ).toMatchObject({ spent_atoms: "500", reserved_atoms: rawLease!.remaining_atoms });

      expect(
        await ledger.setWalletFreeze({
          billing_account_id: f.billingAccount.id,
          currency: "PTS",
          frozen: true,
          reason: "audit-check",
        }),
      ).toMatchObject({ spent_atoms: "500", reserved_atoms: rawLease!.remaining_atoms });
      await ledger.setWalletFreeze({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        frozen: false,
        reason: "audit-clear",
      });

      expect(
        await ledger.rechargeSupplier({
          account_id: f.accountA.id,
          currency: "USD",
          external_ref: "deferred-extra-recharge",
          amount_atoms: "1000",
        }),
      ).toMatchObject({ total_balance_atoms: String(501_001_000 - 250) });

      expect(
        await ledger.adjustSupplierBalance({
          account_id: f.accountA.id,
          currency: "USD",
          external_ref: "deferred-sup-adj",
          delta_atoms: "-100",
          reason: "reconcile",
        }),
      ).toMatchObject({ total_balance_atoms: String(501_000_900 - 250) });

      expect(await ledger.auditConservation()).toEqual(CONSERVATION_CLEAN);

      const firstCharge = committed[0]!;
      expect(firstCharge.ok).toBe(true);
      if (!firstCharge.ok) throw new Error("unreachable");
      await expect(
        ledger.refundCharge({
          charge_id: firstCharge.charge_id,
          external_ref: "refund-too-large-while-active",
          amount_atoms: "101",
          reason: "exceeds charge amount",
        }),
      ).rejects.toThrow();
      const [leaseAfterRejectedRefund] = await db<
        { spent_atoms: string; spent_lot_id: string | null }[]
      >`
        SELECT spent_atoms::text, spent_lot_id::text
        FROM public.wallet_leases WHERE id = ${rawLease!.id}::uuid
      `;
      expect(leaseAfterRejectedRefund).toEqual({
        spent_atoms: "500",
        spent_lot_id: credit.lot_id,
      });

      const refund = await ledger.refundCharge({
        charge_id: firstCharge.charge_id,
        external_ref: "refund-while-lease-active",
        amount_atoms: "40",
        reason: "partial refund during active lease",
      });
      expect(refund).toMatchObject({ replayed: false, refunded_atoms: "40" });

      const [leaseAfterRefund] = await db<
        { state: string; spent_atoms: string; spent_lot_id: string | null }[]
      >`
        SELECT state, spent_atoms::text, spent_lot_id::text
        FROM public.wallet_leases WHERE id = ${rawLease!.id}::uuid
      `;
      expect(leaseAfterRefund).toEqual({ state: "ACTIVE", spent_atoms: "0", spent_lot_id: null });

      const secondCharge = committed[1]!;
      expect(secondCharge.ok).toBe(true);
      if (!secondCharge.ok) throw new Error("unreachable");
      expect(
        await ledger.adjustCharge({
          charge_id: secondCharge.charge_id,
          external_ref: "neg-adjust-while-active",
          delta_atoms: "-10",
          reason: "discount adjustment",
        }),
      ).toMatchObject({ replayed: false, delta_atoms: "-10" });

      const afterRefundReq = await acquireChatHold(ledger, f, f.billingAccount.id, expiry);
      expect(afterRefundReq.res.ok).toBe(true);
      expect(
        await ledger.commitTerminal(
          makeCommit(f, afterRefundReq.requestId, afterRefundReq.attemptId, f.billingAccount.id),
        ),
      ).toMatchObject({ ok: true, customer_atoms: "100", supplier_cost_atoms: "50" });

      expect(await ledger.getWallet(f.billingAccount.id, "PTS")).toMatchObject({
        spent_atoms: "550",
      });
      expect(await ledger.auditConservation()).toMatchObject({ ok: true, open_holds: 0 });
    } finally {
      await ledger.drain();
      await f.ledger.drain();
    }

    const [flushedWallet] = await db<
      { available_atoms: string; reserved_atoms: string; spent_atoms: string }[]
    >`
      SELECT available_atoms::text, reserved_atoms::text, spent_atoms::text
      FROM public.wallets WHERE billing_account_id = ${f.billingAccount.id}::uuid AND currency = 'PTS'
    `;
    expect(flushedWallet).toEqual({
      available_atoms: String(100_000_000 - 550),
      reserved_atoms: "0",
      spent_atoms: "550",
    });
    const [flushedSup] = await db<{ bal: string; res: string; spent: string }[]>`
      SELECT sum(balance_atoms)::text AS bal, sum(reserved_atoms)::text AS res, sum(spent_atoms)::text AS spent
      FROM public.supplier_cells WHERE account_id = ${f.accountA.id}::uuid AND currency = 'USD'
    `;
    expect(flushedSup).toEqual({
      bal: String(501_000_900 - 300),
      res: "0",
      spent: "300",
    });
    expect(await f.ledger.auditConservation()).toEqual(CONSERVATION_CLEAN);
  }));

test("SIGKILL crash path: worker sweepExpiredLeases flushes deferred wallet and supplier lease spend with exact conservation", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const crashedNode = createLedgerEngine(db, {
      leaseIdleReleaseMs: 60_000,
      leaseTtlMs: 60_000,
      batchWindowMs: 1,
    });
    const worker = createLedgerEngine(db, { enableLeases: false });
    const expiry = new Date(Date.now() + 60_000).toISOString();

    try {
      await f.ledger.creditWallet({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        external_ref: "sigkill-cash",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      await f.ledger.rechargeSupplier({
        account_id: f.accountA.id,
        currency: "USD",
        external_ref: "sigkill-supplier",
        amount_atoms: "500000000",
      });

      await warmLeases(crashedNode, f, f.billingAccount.id, expiry);

      const settled = await Promise.all(
        Array.from({ length: 4 }, () =>
          acquireChatHold(crashedNode, f, f.billingAccount.id, expiry),
        ),
      );
      expect(settled.every((s) => s.res.ok)).toBe(true);
      const inFlight = await acquireChatHold(crashedNode, f, f.billingAccount.id, expiry);
      expect(inFlight.res.ok).toBe(true);
      await Promise.all(
        settled.map((r) =>
          crashedNode.commitTerminal(makeCommit(f, r.requestId, r.attemptId, f.billingAccount.id)),
        ),
      );

      await db`UPDATE public.billing_holds SET expires_at = now() - interval '1 second' WHERE request_id = ${inFlight.requestId}::uuid`;
      await db`UPDATE public.wallet_leases SET expires_at = now() - interval '1 second' WHERE state = 'ACTIVE'`;
      await db`UPDATE public.supplier_leases SET expires_at = now() - interval '1 second' WHERE state = 'ACTIVE'`;

      expect(await worker.sweepTimedOutHolds(100)).toBe(1);
      expect(await worker.sweepExpiredLeases(100)).toBe(2);

      const [rawWallet] = await db<{ reserved_atoms: string; spent_atoms: string }[]>`
        SELECT reserved_atoms::text, spent_atoms::text
        FROM public.wallets WHERE billing_account_id = ${f.billingAccount.id}::uuid AND currency = 'PTS'
      `;
      expect(rawWallet).toEqual({ reserved_atoms: "200", spent_atoms: "400" });

      const [rawSup] = await db<{ reserved: string; spent: string }[]>`
        SELECT sum(reserved_atoms)::text AS reserved, sum(spent_atoms)::text AS spent
        FROM public.supplier_cells WHERE account_id = ${f.accountA.id}::uuid AND currency = 'USD'
      `;
      expect(rawSup).toEqual({ reserved: "100", spent: "200" });

      expect(await worker.auditConservation()).toMatchObject({
        ok: true,
        open_holds: 0,
        unknown_holds: 1,
      });

      await worker.resolveRequest({
        request_id: inFlight.requestId,
        action: "RELEASE",
        reason: "verified_not_sent",
        actor: "ops",
      });
      expect(await worker.auditConservation()).toEqual(CONSERVATION_CLEAN);
    } finally {
      await crashedNode.drain();
      await worker.drain();
      await f.ledger.drain();
    }
  }));

test("exhausted wallet and supplier leases (remaining_atoms = 0 and spent_atoms > 0) close cleanly in batch and single paths", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const ledger = createLedgerEngine(db, {
      leaseIdleReleaseMs: 60_000,
      leaseTtlMs: 60_000,
      batchWindowMs: 1,
    });
    const worker = createLedgerEngine(db, { enableLeases: false });
    const accounts = await Promise.all(
      [0, 1, 2].map((idx) =>
        f.catalog.upsertBillingAccount({
          name: `exhausted-lease-${idx}`,
          currency: "PTS",
          rpm: 1000,
          tpm: 100000,
          concurrency: 64,
        }),
      ),
    );
    const expiry = new Date(Date.now() + 60_000).toISOString();
    const splitPricing = {
      billing_pricing: {
        currency: "PTS",
        rates: {
          input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: "0", denominator: "1" },
        },
      },
      supplier_pricing: {
        currency: "USD",
        rates: {
          input_tokens: { numerator: "0", denominator: "1" },
          output_tokens: { numerator: "1", denominator: "1" },
        },
      },
    };

    try {
      await ledger.rechargeSupplier({
        account_id: f.accountA.id,
        currency: "USD",
        external_ref: "exhausted-sup-fund",
        amount_atoms: "500000000",
      });
      for (const [idx, acct] of accounts.entries()) {
        await ledger.creditWallet({
          billing_account_id: acct.id,
          currency: "PTS",
          external_ref: `exhausted-cash-${idx}`,
          amount_atoms: "100000000",
          kind: "CASH",
        });
        await warmLeases(ledger, f, acct.id, expiry);
      }

      const activeWls = await db<{ id: string }[]>`
        SELECT id::text FROM public.wallet_leases WHERE state = 'ACTIVE' ORDER BY id
      `;
      expect(activeWls).toHaveLength(3);

      const getActiveWl = async (billingAccountId: string) => {
        const [wl] = await db<{ id: string; remaining_atoms: string; spent_atoms: string }[]>`
          SELECT wl.id::text, wl.remaining_atoms::text, wl.spent_atoms::text
          FROM public.wallet_leases wl
          JOIN public.wallets w ON w.id = wl.wallet_id
          WHERE w.billing_account_id = ${billingAccountId}::uuid AND wl.state = 'ACTIVE'
        `;
        return wl!;
      };

      for (const acct of [accounts[1]!, accounts[2]!]) {
        const wl = await getActiveWl(acct.id);
        const hold = await acquireChatHold(ledger, f, acct.id, expiry, {
          customer_atoms: wl.remaining_atoms,
          supplier_atoms: "100",
        });
        expect(hold.res.ok).toBe(true);
        expect(
          await ledger.commitTerminal(
            makeCommit(
              f,
              hold.requestId,
              hold.attemptId,
              acct.id,
              wl.remaining_atoms,
              "100",
              splitPricing,
            ),
          ),
        ).toMatchObject({
          ok: true,
          customer_atoms: wl.remaining_atoms,
          supplier_cost_atoms: "100",
        });
      }

      const probe = await acquireChatHold(ledger, f, accounts[0]!.id, expiry);
      expect(probe.res.ok).toBe(true);
      expect(
        await ledger.commitTerminal(
          makeCommit(
            f,
            probe.requestId,
            probe.attemptId,
            accounts[0]!.id,
            "100",
            "100",
            splitPricing,
          ),
        ),
      ).toMatchObject({ ok: true, customer_atoms: "100", supplier_cost_atoms: "100" });

      const [probeHold] = await db<
        { wallet_lease_id: string | null; supplier_lease_id: string | null }[]
      >`
        SELECT wallet_lease_id::text, supplier_lease_id::text
        FROM public.billing_holds WHERE request_id = ${probe.requestId}::uuid
      `;
      expect(probeHold?.wallet_lease_id).toBeTruthy();
      expect(probeHold?.supplier_lease_id).toBeTruthy();
      const wl0Before = await getActiveWl(accounts[0]!.id);
      const [sl0Before] = await db<{ id: string; cell: number; remaining_atoms: string }[]>`
        SELECT id::text, cell, remaining_atoms::text
        FROM public.supplier_leases WHERE id = ${probeHold!.supplier_lease_id}::uuid
      `;
      const exhaustBoth = await acquireChatHold(ledger, f, accounts[0]!.id, expiry, {
        customer_atoms: wl0Before.remaining_atoms,
        supplier_atoms: sl0Before!.remaining_atoms,
      });
      expect(exhaustBoth.res.ok).toBe(true);
      expect(
        await ledger.commitTerminal(
          makeCommit(
            f,
            exhaustBoth.requestId,
            exhaustBoth.attemptId,
            accounts[0]!.id,
            wl0Before.remaining_atoms,
            sl0Before!.remaining_atoms,
            splitPricing,
          ),
        ),
      ).toMatchObject({
        ok: true,
        customer_atoms: wl0Before.remaining_atoms,
        supplier_cost_atoms: sl0Before!.remaining_atoms,
      });

      const exhaustedWls = await db<
        { id: string; remaining_atoms: string; spent_atoms: string; lot_len: number }[]
      >`
        SELECT id::text, remaining_atoms::text, spent_atoms::text, jsonb_array_length(lot_allocations) AS lot_len
        FROM public.wallet_leases WHERE state = 'ACTIVE'
      `;
      expect(exhaustedWls).toHaveLength(3);
      for (const row of exhaustedWls) {
        expect(row.remaining_atoms).toBe("0");
        expect(BigInt(row.spent_atoms)).toBeGreaterThan(0n);
        expect(row.lot_len).toBe(0);
      }

      const [exhaustedSl] = await db<
        { id: string; cell: number; remaining_atoms: string; spent_atoms: string }[]
      >`
        SELECT id::text, cell, remaining_atoms::text, spent_atoms::text
        FROM public.supplier_leases WHERE id = ${sl0Before!.id}::uuid AND state = 'ACTIVE'
      `;
      expect(exhaustedSl?.remaining_atoms).toBe("0");
      expect(BigInt(exhaustedSl!.spent_atoms)).toBeGreaterThan(0n);
      expect(await ledger.auditConservation()).toMatchObject({ ok: true, open_holds: 0 });

      const wl2 = await getActiveWl(accounts[2]!.id);
      await db`UPDATE public.wallet_leases SET expires_at = now() - interval '1 second' WHERE id = ${wl2.id}::uuid`;
      expect(await worker.sweepExpiredLeases(100)).toBe(1);
      const [wl2Closed] = await db<
        {
          state: string;
          remaining_atoms: string;
          spent_atoms: string;
          spent_lot_id: string | null;
        }[]
      >`
        SELECT state, remaining_atoms::text, spent_atoms::text, spent_lot_id::text
        FROM public.wallet_leases WHERE id = ${wl2.id}::uuid
      `;
      expect(wl2Closed).toEqual({
        state: "CLOSED",
        remaining_atoms: "0",
        spent_atoms: "0",
        spent_lot_id: null,
      });
      const [wallet2AfterSingleClose] = await db<{ reserved_atoms: string; spent_atoms: string }[]>`
        SELECT reserved_atoms::text, spent_atoms::text
        FROM public.wallets WHERE billing_account_id = ${accounts[2]!.id}::uuid AND currency = 'PTS'
      `;
      expect(wallet2AfterSingleClose).toEqual({
        reserved_atoms: "0",
        spent_atoms: wl2.spent_atoms,
      });

      await ledger.releaseIdleLeases();

      const [closedBatch] = await db<{ closed: number; commits: number }[]>`
        SELECT count(*)::int AS closed, count(DISTINCT xmin::text)::int AS commits
        FROM public.wallet_leases WHERE state = 'CLOSED' AND id <> ${wl2.id}::uuid
      `;
      expect(closedBatch).toEqual({ closed: 2, commits: 1 });

      const [slClosed] = await db<
        { state: string; remaining_atoms: string; spent_atoms: string }[]
      >`
        SELECT state, remaining_atoms::text, spent_atoms::text
        FROM public.supplier_leases WHERE id = ${sl0Before!.id}::uuid
      `;
      expect(slClosed).toEqual({ state: "CLOSED", remaining_atoms: "0", spent_atoms: "0" });

      const [cellAfterClose] = await db<{ reserved_atoms: string; spent_atoms: string }[]>`
        SELECT reserved_atoms::text, spent_atoms::text
        FROM public.supplier_cells
        WHERE account_id = ${f.accountA.id}::uuid AND currency = 'USD' AND cell = ${exhaustedSl!.cell}
      `;
      expect(cellAfterClose).toEqual({
        reserved_atoms: "0",
        spent_atoms: exhaustedSl!.spent_atoms,
      });
      expect(await ledger.auditConservation()).toEqual(CONSERVATION_CLEAN);
    } finally {
      await ledger.drain();
      await worker.drain();
      await f.ledger.drain();
    }
  }));

test("debt coverage guard subtracts deferred active wallet lease spend on leased and transactional holds", () =>
  withLedgerDb(async (db) => {
    const f = await seedFixture(db);
    const ledger = createLedgerEngine(db, {
      leaseIdleReleaseMs: 60_000,
      leaseTtlMs: 60_000,
      batchWindowMs: 1,
    });
    const expiry = new Date(Date.now() + 60_000).toISOString();

    try {
      await ledger.creditWallet({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        external_ref: "debt-deferred-cash",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      await ledger.configureWalletCredit({
        billing_account_id: f.billingAccount.id,
        currency: "PTS",
        expected_version: "0",
        credit_limit_atoms: "100000000",
      });
      await ledger.rechargeSupplier({
        account_id: f.accountA.id,
        currency: "USD",
        external_ref: "debt-deferred-sup",
        amount_atoms: "500000000",
      });

      await warmLeases(ledger, f, f.billingAccount.id, expiry);

      const leasedSpend = await acquireChatHold(ledger, f, f.billingAccount.id, expiry, {
        customer_atoms: "1000000",
        supplier_atoms: "500000",
        fingerprint: "debt-deferred",
      });
      expect(leasedSpend.res).toMatchObject({ ok: true });
      expect(
        await ledger.commitTerminal(
          makeCommit(
            f,
            leasedSpend.requestId,
            leasedSpend.attemptId,
            f.billingAccount.id,
            "1000000",
            "0",
          ),
        ),
      ).toMatchObject({ ok: true, customer_atoms: "1000000" });

      const [wlAfterLeased] = await db<{ spent_atoms: string }[]>`
        SELECT wl.spent_atoms::text
        FROM public.wallet_leases wl
        JOIN public.wallets w ON w.id = wl.wallet_id
        WHERE w.billing_account_id = ${f.billingAccount.id}::uuid AND wl.state = 'ACTIVE'
      `;
      expect(wlAfterLeased?.spent_atoms).toBe("1000000");

      const liveWallet = (await ledger.getWallet(f.billingAccount.id, "PTS"))!;
      const freePlusLiveReserved =
        BigInt(liveWallet.available_atoms) + BigInt(liveWallet.reserved_atoms);

      const unleasedCharge = await acquireChatHold(ledger, f, f.billingAccount.id, expiry, {
        customer_atoms: liveWallet.available_atoms,
        idempotency_key: "drain-free-and-incur-debt",
        fingerprint: "debt-deferred",
      });
      expect(unleasedCharge.res).toMatchObject({ ok: true });
      const excessUsage = (freePlusLiveReserved + 100n).toString();
      await ledger.commitTerminal(
        makeCommit(
          f,
          unleasedCharge.requestId,
          unleasedCharge.attemptId,
          f.billingAccount.id,
          excessUsage,
          "0",
          {
            supplier_pricing: {
              currency: "USD",
              rates: { input_tokens: { numerator: "1", denominator: "1000000000000" } },
            },
          },
        ),
      );

      const [rawW] = await db<
        { available_atoms: string; reserved_atoms: string; debt_atoms: string }[]
      >`
        SELECT available_atoms::text, reserved_atoms::text, debt_atoms::text
        FROM public.wallets WHERE billing_account_id = ${f.billingAccount.id}::uuid AND currency = 'PTS'
      `;
      expect(BigInt(rawW!.available_atoms) + BigInt(rawW!.reserved_atoms)).toBeGreaterThan(
        BigInt(rawW!.debt_atoms),
      );
      const liveAfterDebt = (await ledger.getWallet(f.billingAccount.id, "PTS"))!;
      expect(
        BigInt(liveAfterDebt.available_atoms) + BigInt(liveAfterDebt.reserved_atoms),
      ).toBeLessThan(BigInt(liveAfterDebt.debt_atoms));

      const rejectedLeased = await acquireChatHold(ledger, f, f.billingAccount.id, expiry, {
        fingerprint: "debt-deferred",
      });
      expect(rejectedLeased.res).toMatchObject({ ok: false, code: "insufficient_balance" });
      const rejectedTx = await acquireChatHold(ledger, f, f.billingAccount.id, expiry, {
        idempotency_key: "direct-after-deferred-debt",
        fingerprint: "debt-deferred",
      });
      expect(rejectedTx.res).toMatchObject({ ok: false, code: "insufficient_balance" });
      expect(await ledger.auditConservation()).toMatchObject({ ok: true, open_holds: 0 });
    } finally {
      await ledger.drain();
      await f.ledger.drain();
    }
  }));
