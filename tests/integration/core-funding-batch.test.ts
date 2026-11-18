import { expect, test } from "bun:test";
import { createSettlementStore } from "@gateway/storage/settlement";
import { withFinancial } from "../support/financial-execution.ts";
import { committed, event, granted, supplier } from "../support/core-settlement.ts";

test("a batch applies one cell transition while rounding each request independently and replaying without extra money", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const client = f.client();
    await client.startup();
    const first = await granted(db, setup, client, {
      attemptId: "10000000-0000-4000-8000-000000000000",
    });
    const second = await granted(db, setup, client, {
      attemptId: "20000000-0000-4000-8000-000000000000",
    });
    const groups = [first, second].map(({ prepared, owner }) => ({
      events: [
        event(owner, prepared.attemptId, {
          finality: "FINAL",
          usage: { input_tokens: "1", output_tokens: "0" },
          final_dimensions: ["input_tokens", "output_tokens"],
          attempt_state: "SUCCEEDED",
          request_state: "SUCCEEDED",
        }),
      ],
    }));
    await db.unsafe(`
      CREATE TABLE public.funding_test_transitions(cell smallint,spent_delta numeric);
      CREATE FUNCTION public.funding_test_trace() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
      BEGIN INSERT INTO public.funding_test_transitions VALUES(NEW.cell,NEW.spent_atoms-OLD.spent_atoms);RETURN NULL;END $$;
      CREATE TRIGGER funding_test_trace AFTER UPDATE ON core_funding.balance_cells FOR EACH ROW EXECUTE FUNCTION public.funding_test_trace();
      CREATE TABLE public.funding_test_journal_batches(rows integer);
      CREATE FUNCTION public.funding_test_journal_batch() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
      BEGIN INSERT INTO public.funding_test_journal_batches SELECT count(*) FROM new_journals;RETURN NULL;END $$;
      CREATE TRIGGER funding_test_journal_batch AFTER INSERT ON public.upstream_journals REFERENCING NEW TABLE AS new_journals FOR EACH STATEMENT EXECUTE FUNCTION public.funding_test_journal_batch();
    `);
    const store = createSettlementStore(db);
    for (const result of await store.persist(groups)) committed(result);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "99998",
      reserved: "0",
      spent: "2",
      state: "ACTIVE",
    });
    expect(await db`SELECT cost_atoms::text cost FROM upstream_costs ORDER BY attempt_id`).toEqual([
      { cost: "1" },
      { cost: "1" },
    ]);
    expect(
      await db`SELECT amount_atoms::text amount FROM billing_charges ORDER BY request_id`,
    ).toEqual([{ amount: "100" }, { amount: "100" }]);
    expect(await db`SELECT cell,spent_delta::text FROM funding_test_transitions`).toEqual([
      { cell: 0, spent_delta: "2" },
    ]);
    expect(await db`SELECT rows FROM funding_test_journal_batches`).toEqual([{ rows: 2 }]);
    for (const result of await store.persist(groups))
      expect(committed(result).receipts[0]?.duplicate).toBe(true);
    expect(await db`SELECT count(*)::integer n FROM funding_test_transitions`).toEqual([{ n: 1 }]);
    expect(await db`SELECT count(*)::integer n FROM upstream_journals WHERE kind='COST'`).toEqual([
      { n: 2 },
    ]);
  }));

test("a cell failure during the batch flush leaves only its group uncommitted and the original facts retry once", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000");
    const client = f.client();
    await client.startup();
    const first = await granted(db, setup, client, {
      attemptId: "10000000-0000-4000-8000-000000000000",
    });
    const second = await granted(db, setup, client, {
      attemptId: "10000000-0000-4000-8000-000000000001",
    });
    const groups = [first, second].map(({ prepared, owner }) => ({
      events: [
        event(owner, prepared.attemptId, {
          finality: "FINAL",
          usage: { input_tokens: "1", output_tokens: "0" },
          final_dimensions: ["input_tokens", "output_tokens"],
          attempt_state: "SUCCEEDED",
          request_state: "SUCCEEDED",
        }),
      ],
    }));
    await db.unsafe(`
      CREATE FUNCTION public.funding_test_fail() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
      BEGIN IF NEW.cell=0 AND NEW.spent_atoms>OLD.spent_atoms THEN RAISE EXCEPTION 'synthetic cell failure' USING ERRCODE='P0F99';END IF;RETURN NEW;END $$;
      CREATE TRIGGER funding_test_fail BEFORE UPDATE ON core_funding.balance_cells FOR EACH ROW EXECUTE FUNCTION public.funding_test_fail();
    `);
    const store = createSettlementStore(db);
    const results = await store.persist(groups);
    expect(results[0]).toEqual({ status: "ERROR", error: "P0F99" });
    committed(results[1]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "999",
      reserved: "30",
      spent: "1",
      state: "ACTIVE",
    });
    expect(await db`SELECT attempt_id FROM core_evidence_receipts`).toEqual([
      { attempt_id: second.prepared.attemptId },
    ]);
    expect(await db`SELECT request_id FROM billing_charges`).toEqual([
      { request_id: second.prepared.requestId },
    ]);
    await db.unsafe("DROP TRIGGER funding_test_fail ON core_funding.balance_cells");
    const retried = await store.persist(groups);
    expect(committed(retried[0]).receipts[0]?.duplicate).toBe(false);
    expect(committed(retried[1]).receipts[0]?.duplicate).toBe(true);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "998",
      reserved: "0",
      spent: "2",
      state: "ACTIVE",
    });
    expect(await db`SELECT count(*)::integer n FROM billing_charges`).toEqual([{ n: 2 }]);
    expect(await db`SELECT count(*)::integer n FROM upstream_journals WHERE kind='COST'`).toEqual([
      { n: 2 },
    ]);
  }));

test("partial and final groups share the actual reserve and a later billing failure discards its cell projection", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000");
    const client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client, {
      attemptId: "10000000-0000-4000-8000-000000000000",
    });
    const failed = await granted(db, setup, client, {
      attemptId: "20000000-0000-4000-8000-000000000000",
    });
    const partial = event(owner, prepared.attemptId, {
      usage: { input_tokens: "1", output_tokens: "0" },
      final_dimensions: ["input_tokens"],
    });
    const failing = event(failed.owner, failed.prepared.attemptId, {
      finality: "FINAL",
      usage: { input_tokens: "1", output_tokens: "0" },
      final_dimensions: ["input_tokens", "output_tokens"],
      attempt_state: "SUCCEEDED",
      request_state: "SUCCEEDED",
    });
    const final = event(owner, prepared.attemptId, {
      source_revision: "2",
      finality: "FINAL",
      usage: { input_tokens: "1", output_tokens: "1" },
      final_dimensions: ["input_tokens", "output_tokens"],
      attempt_state: "SUCCEEDED",
      request_state: "SUCCEEDED",
    });
    await db.unsafe(`
      CREATE FUNCTION public.funding_test_billing_failure() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
      BEGIN IF NEW.request_id='${failed.prepared.requestId}'::uuid THEN RAISE EXCEPTION 'synthetic billing failure' USING ERRCODE='P0F98';END IF;RETURN NEW;END $$;
      CREATE TRIGGER funding_test_billing_failure BEFORE INSERT ON public.billing_charges FOR EACH ROW EXECUTE FUNCTION public.funding_test_billing_failure();
    `);
    const results = await createSettlementStore(db).persist([
      { events: [partial] },
      { events: [failing] },
      { events: [final] },
    ]);
    committed(results[0]);
    expect(results[1]).toEqual({ status: "ERROR", error: "P0F98" });
    committed(results[2]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "999",
      reserved: "30",
      spent: "1",
      state: "ACTIVE",
    });
    expect(
      await db`SELECT current_reserved_atoms::text reserved,state FROM upstream_holds ORDER BY attempt_id`,
    ).toEqual([
      { reserved: "0", state: "SETTLED" },
      { reserved: "30", state: "HELD" },
    ]);
    expect(
      await db`SELECT revision::text,cost_atoms::text cost FROM upstream_costs ORDER BY revision`,
    ).toEqual([
      { revision: "1", cost: "1" },
      { revision: "2", cost: "1" },
    ]);
    expect(await db`SELECT count(*)::integer n FROM core_evidence_receipts`).toEqual([{ n: 2 }]);
    expect(await db`SELECT amount_atoms::text amount FROM billing_charges`).toEqual([
      { amount: "100" },
    ]);
  }));
