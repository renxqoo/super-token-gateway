import { expect, test } from "bun:test";
import { createSettlementStore } from "@gateway/storage/settlement";
import { withFinancial } from "../support/financial-execution.ts";
import { committed, event, granted, supplier } from "../support/core-settlement.ts";

test("receipt revision lookup stays bounded as same-origin history grows and delivery uniqueness keeps all three keys", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000");
    const client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const fact = event(owner, prepared.attemptId, {
      finality: "FINAL",
      usage: { input_tokens: "1", output_tokens: "0" },
      final_dimensions: ["input_tokens", "output_tokens"],
      attempt_state: "SUCCEEDED",
      request_state: "SUCCEEDED",
    });
    const receipt = committed((await createSettlementStore(db).persist([{ events: [fact] }]))[0])
      .receipts[0]!;
    await f.identity.a.stop();
    await f.identity.b.stop();
    const connection = await db.reserve();
    try {
      await connection.unsafe(`
        ALTER TABLE public.core_evidence_receipts SET (autovacuum_enabled=false);
        CREATE TEMP TABLE receipt_probe_input(value jsonb);
        CREATE FUNCTION public.receipt_claim_probe(ev jsonb) RETURNS uuid LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
        DECLARE prior public.core_evidence_receipts%ROWTYPE;BEGIN
        SELECT * INTO prior FROM public.core_evidence_receipts r WHERE r.attempt_id=(ev->>'attempt_id')::uuid AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric AND r.revision_claim;
        RETURN prior.id;END $$;
      `);
      const probe = {
        attempt_id: prepared.attemptId,
        origin_identity: fact.origin_identity,
        source: "execution",
        source_revision: "20000",
      };
      await connection`INSERT INTO receipt_probe_input VALUES(${probe}::jsonb)`;
      await connection.unsafe(
        `DO $$ DECLARE ev jsonb;BEGIN SELECT value INTO ev FROM receipt_probe_input;FOR i IN 1..7 LOOP PERFORM public.receipt_claim_probe(ev);END LOOP;END $$`,
      );
      await connection`WITH seed AS MATERIALIZED(SELECT to_jsonb(r) value FROM core_evidence_receipts r WHERE id=${receipt.receipt_id}),copies AS MATERIALIZED(SELECT value||jsonb_build_object('id',gen_random_uuid(),'event_id',gen_random_uuid(),'source_revision',(1000+n)::text) value FROM seed CROSS JOIN generate_series(1,10000) n) INSERT INTO core_evidence_receipts SELECT r.* FROM copies c CROSS JOIN LATERAL jsonb_populate_record(NULL::core_evidence_receipts,c.value) r`;
      await connection`SELECT pg_stat_force_next_flush()`;
      await db`SELECT pg_stat_clear_snapshot()`;
      const before = (
        await db`SELECT seq_scan::text scans,idx_scan::text indexes,seq_tup_read::text sequential,idx_tup_fetch::text indexed,n_tup_ins::text inserts FROM pg_stat_user_tables WHERE relid='core_evidence_receipts'::regclass`
      )[0];
      expect(Number(before.inserts)).toBe(10001);
      expect((await connection`SELECT receipt_claim_probe(${probe}::jsonb) id`)[0].id).toBeNull();
      await connection`SELECT pg_stat_force_next_flush()`;
      await db`SELECT pg_stat_clear_snapshot()`;
      const after = (
        await db`SELECT seq_scan::text scans,idx_scan::text indexes,seq_tup_read::text sequential,idx_tup_fetch::text indexed FROM pg_stat_user_tables WHERE relid='core_evidence_receipts'::regclass`
      )[0];
      const visited =
        BigInt(after.sequential) +
        BigInt(after.indexed) -
        BigInt(before.sequential) -
        BigInt(before.indexed);
      expect(BigInt(after.scans) + BigInt(after.indexes)).toBeGreaterThan(
        BigInt(before.scans) + BigInt(before.indexes),
      );
      expect(visited).toBeLessThanOrEqual(32n);
      expect(
        (
          await connection`SELECT receipt_claim_probe(${{ ...probe, source_revision: "1" }}::jsonb) id`
        )[0].id,
      ).toBe(receipt.receipt_id);
      const duplicate =
        await connection`INSERT INTO core_evidence_receipts SELECT r.* FROM core_evidence_receipts original CROSS JOIN LATERAL jsonb_populate_record(NULL::core_evidence_receipts,to_jsonb(original)||jsonb_build_object('id',gen_random_uuid(),'event_claim',false,'revision_claim',false)) r WHERE original.id=${receipt.receipt_id}`.catch(
          (error: unknown) => error,
        );
      expect(duplicate).toMatchObject({ errno: "23505", constraint: "core_receipt_delivery" });
      const differentOrigin = crypto.randomUUID();
      await connection`INSERT INTO core_evidence_receipts SELECT r.* FROM core_evidence_receipts original CROSS JOIN LATERAL jsonb_populate_record(NULL::core_evidence_receipts,to_jsonb(original)||jsonb_build_object('id',${differentOrigin}::uuid,'origin_identity','gateway:receipt_plan_other','event_claim',false,'revision_claim',false)) r WHERE original.id=${receipt.receipt_id}`;
      expect(
        (
          await connection`SELECT event_id FROM core_evidence_receipts WHERE id=${differentOrigin}`
        )[0].event_id,
      ).toBe(fact.event_id);
      const differentBody = crypto.randomUUID();
      await connection`INSERT INTO core_evidence_receipts SELECT r.* FROM core_evidence_receipts original CROSS JOIN LATERAL jsonb_populate_record(NULL::core_evidence_receipts,to_jsonb(original)||jsonb_build_object('id',${differentBody}::uuid,'body_digest',repeat('0',64),'event_claim',false,'revision_claim',false)) r WHERE original.id=${receipt.receipt_id}`;
      expect(
        (await connection`SELECT event_id FROM core_evidence_receipts WHERE id=${differentBody}`)[0]
          .event_id,
      ).toBe(fact.event_id);
      const history = (
        await db`SELECT * FROM core_evidence_receipts WHERE id=${receipt.receipt_id}`
      )[0];
      await db.begin(async (tx) => {
        await tx.unsafe(
          await Bun.file(
            new URL(
              "../../packages/storage/src/migrations/0013_receipt-delivery-lookup.sql",
              import.meta.url,
            ),
          ).text(),
        );
      });
      expect(
        (await db`SELECT * FROM core_evidence_receipts WHERE id=${receipt.receipt_id}`)[0],
      ).toEqual(history);
      expect(await db`SELECT count(*)::int count FROM core_evidence_receipts`).toEqual([
        { count: 10003 },
      ]);
      expect(await db`SELECT amount_atoms::text amount FROM billing_charges`).toEqual([
        { amount: "100" },
      ]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "999",
        reserved: "0",
        spent: "1",
        state: "ACTIVE",
      });
    } finally {
      await connection.release();
    }
  }));
