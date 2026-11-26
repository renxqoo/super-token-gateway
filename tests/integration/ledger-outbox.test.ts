import { expect, test } from "bun:test";
import { SQL } from "bun";
import { createWorkerRunner } from "@gateway/worker";
import { createCatalogRepository, createLedgerEngine, createLedgerQueries } from "@gateway/ledger";
import { withLedgerDb, seedFixture } from "./ledger-fixture.ts";

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

test("symptom: an unused Outbox consumer shard lock stalls claim and acknowledgement", () =>
  withLedgerDb(async (db, dbUrl) => {
    const ledger = createLedgerEngine(db, { enableLeases: false });
    const shortDb = new SQL(dbUrl, { max: 2, connection: { lock_timeout: 100 } });
    const consumer = createLedgerEngine(shortDb, { enableLeases: false });
    const name = "unused-shard-lock";
    const unlock = Promise.withResolvers<void>();
    let blocker: Promise<unknown> | undefined;
    try {
      expect((await ledger.claimOutboxEvents(name)).events).toEqual([]);
      const eventIds: [string, string] = [crypto.randomUUID(), crypto.randomUUID()];
      const billingAccountId = crypto.randomUUID();
      const [event] = await db`
        INSERT INTO public.outbox_events(event_id, kind, billing_account_id, payload)
        VALUES (${eventIds[0]}::uuid, 'billing.settled', ${billingAccountId}::uuid, '{}'::jsonb),
               (${eventIds[1]}::uuid, 'billing.settled', ${billingAccountId}::uuid, '{}'::jsonb)
        RETURNING shard`;
      const locked = Promise.withResolvers<void>();
      blocker = db.begin(async (tx) => {
        const [row] = await tx`
          SELECT shard FROM public.outbox_consumer_shards WHERE name=${name} AND shard <> ${event!.shard}
          ORDER BY shard LIMIT 1 FOR UPDATE`;
        expect(row).toBeDefined();
        locked.resolve();
        await unlock.promise;
      });
      await Promise.race([locked.promise, blocker]);
      const batch = await consumer.claimOutboxEvents(name);
      expect(batch.events.map((item) => item.event_id)).toEqual(eventIds);
      expect(await consumer.ackOutboxEvents(name, batch.lease_token, batch.events[0]!.seq)).toBe(
        true,
      );
      const remaining = await consumer.claimOutboxEvents(name);
      expect(remaining.events.map((item) => item.event_id)).toEqual([eventIds[1]]);
      expect(
        await consumer.ackOutboxEvents(name, remaining.lease_token, remaining.events[0]!.seq),
      ).toBe(true);
      expect((await consumer.claimOutboxEvents(name)).events).toEqual([]);
    } finally {
      unlock.resolve();
      await blocker;
      await consumer.drain();
      await shortDb.close();
      await ledger.drain();
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
