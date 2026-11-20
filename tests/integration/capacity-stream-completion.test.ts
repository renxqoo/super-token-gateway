import { expect, test } from "bun:test";
import { createSettlementStore } from "@gateway/storage/settlement";
import { createEventStore, recordEventConsumer } from "@gateway/storage";
import {
  confirmedStreamRequests,
  verifiedStreamProgress,
} from "../../scripts/capacity-stream-completion.ts";
import { committed, event, granted } from "../support/core-settlement.ts";
import { withFinancial } from "../support/financial-execution.ts";

test("stream completion requires authenticated settlement and all ACKs without reading unrelated wallet journals", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client, { stream: true });
    const consumer = await db.begin((tx) => recordEventConsumer(tx, "stream-capacity", false));
    const store = createSettlementStore(db);
    const delivery = createEventStore(db);
    const usage = { input_tokens: "6", output_tokens: "3" };
    const prices = { charge: "100", cost: "3", usage };
    const subjects = [prepared.requestId];
    const complete = () => confirmedStreamRequests(db, subjects, consumer, true, prices);
    const send = async (fields: Parameters<typeof event>[2]) => {
      committed((await store.persist([{ events: [event(owner, prepared.attemptId, fields)] }]))[0]);
      return delivery.claim(consumer, 10);
    };
    const partial = await send({
      source_revision: "1",
      usage: { input_tokens: "3" },
      finality: "PARTIAL",
    });
    expect(partial.events).toHaveLength(1);
    expect(await verifiedStreamProgress(db, partial.events)).toEqual(
      new Set([partial.events[0]!.id]),
    );
    expect(
      await verifiedStreamProgress(db, [{ ...partial.events[0]!, usage: { input_tokens: "99" } }]),
    ).toEqual(new Set());
    expect(
      await delivery.ack(
        consumer,
        partial.lease_token,
        partial.events.map((e) => e.id),
      ),
    ).toBe(1);
    expect(await complete()).toEqual([]);
    const meter = await send({
      source_revision: "2",
      finality: "FINAL",
      usage,
      final_dimensions: ["input_tokens", "output_tokens"],
    });
    expect(
      await delivery.ack(
        consumer,
        meter.lease_token,
        meter.events.map((e) => e.id),
      ),
    ).toBe(1);
    expect(await complete()).toEqual([]);
    const terminal = await send({
      source_revision: "3",
      finality: "FINAL",
      usage,
      final_dimensions: ["input_tokens", "output_tokens"],
      attempt_state: "SUCCEEDED",
      request_state: "SUCCEEDED",
    });
    expect(await confirmedStreamRequests(db, subjects, consumer, false, prices)).toEqual([
      { request_id: prepared.requestId },
    ]);
    expect(await complete()).toEqual([]);
    expect(
      await delivery.ack(
        consumer,
        terminal.lease_token,
        terminal.events.map((e) => e.id),
      ),
    ).toBe(1);
    expect(await complete()).toEqual([{ request_id: prepared.requestId }]);
    await db.begin(async (tx) => {
      await tx`WITH seed AS MATERIALIZED (
          SELECT * FROM money_journal WHERE reference='settle:'||${prepared.requestId}::text
        ), copied AS (
          INSERT INTO money_entries(id,wallet_id,reference,kind,currency,created_at)
          SELECT gen_random_uuid(),s.wallet_id,'other-settlement:'||n,s.kind,s.currency,s.created_at
          FROM seed s CROSS JOIN generate_series(1,4096) n RETURNING id,created_at
        )
        INSERT INTO journal_payloads(id,created_at,lines)
        SELECT c.id,c.created_at,s.lines FROM copied c CROSS JOIN seed s`;
      const rowsRead = async () =>
        Number(
          (
            await tx`SELECT seq_tup_read+idx_tup_fetch AS n FROM pg_stat_xact_user_tables
          WHERE relid='money_entries'::regclass`
          )[0].n,
        );
      const before = await rowsRead();
      expect(
        await confirmedStreamRequests(
          tx,
          [crypto.randomUUID(), ...subjects, ...subjects],
          consumer,
          true,
          prices,
        ),
      ).toEqual([{ request_id: prepared.requestId }]);
      expect((await rowsRead()) - before).toBeLessThanOrEqual(64);
      await tx`CREATE TEMP VIEW money_journal AS SELECT * FROM public.money_journal
        WHERE reference NOT LIKE 'settle:%'`;
      expect(await confirmedStreamRequests(tx, subjects, consumer, true, prices)).toEqual([]);
      await tx`DROP VIEW pg_temp.money_journal`;
    });
    await db.begin(async (tx) => {
      await tx`CREATE TEMP VIEW core_evidence_receipts AS
        SELECT * FROM public.core_evidence_receipts WHERE request_state IS DISTINCT FROM 'SUCCEEDED'`;
      expect(await confirmedStreamRequests(tx, subjects, consumer, true, prices)).toEqual([]);
    });
    expect(
      await confirmedStreamRequests(db, subjects, consumer, true, { ...prices, cost: "2" }),
    ).toEqual([]);
  }));
