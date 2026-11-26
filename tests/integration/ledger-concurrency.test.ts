import { expect, test } from "bun:test";
import { SQL } from "bun";
import { createDatabase, createLedgerEngine } from "@gateway/ledger";
import { withLedgerDb, seedFixture } from "./ledger-fixture.ts";

test("symptom: a leased precharge succeeds after the supplier is disabled while waiting on a wallet lock", () =>
  withLedgerDb(async (db) => {
    const fixture = await seedFixture(db);
    const ledger = createLedgerEngine(db, { leaseIdleReleaseMs: 60_000, batchWindowMs: 1 });
    const held: string[] = [];
    const expiry = new Date(Date.now() + 30_000).toISOString();
    async function acquire(requestId = crypto.randomUUID()) {
      const result = await ledger.acquireHold({
        request_id: requestId,
        attempt_id: crypto.randomUUID(),
        billing_account_id: fixture.billingAccount.id,
        currency: "PTS",
        operation: "chat",
        model: "gpt-4o",
        idempotency_key: null,
        fingerprint: "",
        customer_atoms: "200",
        supplier_account_id: fixture.accountA.id,
        supplier_channel_id: fixture.channelA.id,
        supplier_currency: "USD",
        supplier_atoms: "100",
        expires_at: expiry,
      });
      if (result.ok) held.push(requestId);
      return result;
    }
    let pending: ReturnType<typeof acquire> | undefined;
    try {
      await ledger.rechargeSupplier({
        account_id: fixture.accountA.id,
        currency: "USD",
        external_ref: "supplier-disable-lease",
        amount_atoms: "500000000",
      });
      await ledger.creditWallet({
        billing_account_id: fixture.billingAccount.id,
        currency: "PTS",
        kind: "CASH",
        external_ref: "supplier-disable-cash",
        amount_atoms: "100000000",
      });
      const warmup = await Promise.all(Array.from({ length: 16 }, () => acquire()));
      expect(warmup.every((result) => result.ok)).toBe(true);
      const controlId = crypto.randomUUID();
      expect(await acquire(controlId)).toMatchObject({ ok: true });
      const [control] = await db`SELECT wallet_id, wallet_lease_id, supplier_lease_id
        FROM public.billing_holds WHERE request_id = ${controlId}`;
      expect(control?.wallet_lease_id).toBeTruthy();
      expect(control?.supplier_lease_id).toBeTruthy();
      const requestId = crypto.randomUUID();
      await db.begin(async (tx) => {
        const [blocker] = await tx`SELECT pg_backend_pid() AS pid FROM public.wallets
          WHERE id = ${control!.wallet_id} FOR NO KEY UPDATE`;
        pending = acquire(requestId);
        const deadline = performance.now() + 2_000;
        let waiting = false;
        do {
          const [row] = await db`SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE datname = current_database()
              AND ${blocker!.pid} = ANY(pg_blocking_pids(pid))
          ) AS waiting`;
          waiting = row!.waiting;
          if (!waiting) await Bun.sleep(10);
        } while (!waiting && performance.now() < deadline);
        expect(waiting).toBe(true);
        expect(
          await fixture.catalog.upsertUpstreamAccount({
            id: fixture.accountA.id,
            name: fixture.accountA.name,
            state: "DISABLED",
          }),
        ).toMatchObject({ state: "DISABLED" });
      });
      expect(await pending).toMatchObject({ ok: false, code: "supplier_unavailable" });
      expect(
        await db`SELECT request_id FROM public.billing_holds WHERE request_id = ${requestId}`,
      ).toEqual([]);
    } finally {
      await pending?.catch(() => undefined);
      for (const id of held) await ledger.releaseHold(id);
      await ledger.drain();
      await fixture.ledger.drain();
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
