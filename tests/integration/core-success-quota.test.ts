import { expect, test } from "bun:test";
import { createSettlementStore } from "@gateway/storage/settlement";
import { withFinancial } from "../support/financial-execution.ts";
import { committed, event, granted, supplier } from "../support/core-settlement.ts";

test("accepted success closes execution and recovery releases its three quotas once while a partial attempt keeps its quota and responsibility", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const client = f.client();
    await client.startup();
    const success = await granted(db, setup, client);
    const partial = await granted(db, setup, client);
    const store = createSettlementStore(db);
    const fact = event(success.owner, success.prepared.attemptId, {
      finality: "FINAL",
      usage: { input_tokens: "30", output_tokens: "15" },
      final_dimensions: ["input_tokens", "output_tokens"],
      attempt_state: "SUCCEEDED",
      request_state: "SUCCEEDED",
    });
    for (const group of await store.persist([
      { events: [fact] },
      { events: [event(partial.owner, partial.prepared.attemptId, { usage: {} })] },
    ]))
      committed(group);
    expect(
      await db`SELECT id,execution_closed_at IS NOT NULL closed FROM gateway_requests ORDER BY id`,
    ).toEqual(
      [
        { id: success.prepared.requestId, closed: true },
        { id: partial.prepared.requestId, closed: false },
      ].sort((a, b) => a.id.localeCompare(b.id)),
    );
    expect(
      await db`SELECT request_id,state,count(*)::integer n FROM execution_quota_leases GROUP BY request_id,state ORDER BY request_id`,
    ).toEqual(
      [
        { request_id: success.prepared.requestId, state: "RELEASE_PENDING", n: 3 },
        { request_id: partial.prepared.requestId, state: "ACTIVE", n: 3 },
      ].sort((a, b) => a.request_id.localeCompare(b.request_id)),
    );
    const worker = f.client(f.identity.b.port, "worker");
    await worker.startup();
    expect(await worker.execution("execution_quota_recovery_v1", { limit: 100 })).toEqual({
      released: 3,
      pending: 3,
      failed: 0,
      expired: 0,
      expiry_more: false,
    });
    expect(await worker.execution("execution_quota_recovery_v1", { limit: 100 })).toEqual({
      released: 0,
      pending: 0,
      failed: 0,
      expired: 0,
      expiry_more: false,
    });
    committed((await store.persist([{ events: [fact] }]))[0]);
    expect(
      await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE request_id=${success.prepared.requestId} AND state='RELEASED'`,
    ).toEqual([{ n: 3 }]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "985",
      reserved: "30",
      spent: "15",
      state: "ACTIVE",
    });
  }));

test("a customer settlement failure rolls back automatic execution closure and quota release with every evidence and money write", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const store = createSettlementStore(db);
    const fact = event(owner, prepared.attemptId, {
      finality: "FINAL",
      usage: { input_tokens: "30", output_tokens: "15" },
      final_dimensions: ["input_tokens", "output_tokens"],
      attempt_state: "SUCCEEDED",
      request_state: "SUCCEEDED",
    });
    await db.unsafe(
      "CREATE FUNCTION success_charge_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic charge failure';END $$;CREATE TRIGGER success_charge_failure BEFORE INSERT ON billing_charges FOR EACH ROW EXECUTE FUNCTION success_charge_failure()",
    );
    expect((await store.persist([{ events: [fact] }]))[0]?.status).toBe("ERROR");
    expect(
      await db`SELECT execution_closed_at FROM gateway_requests WHERE id=${prepared.requestId}`,
    ).toEqual([{ execution_closed_at: null }]);
    expect(await db`SELECT count(*)::integer n FROM core_evidence_receipts`).toEqual([{ n: 0 }]);
    expect(await db`SELECT count(*)::integer n FROM usage_event_index`).toEqual([{ n: 0 }]);
    expect(await db`SELECT count(*)::integer n FROM upstream_costs`).toEqual([{ n: 0 }]);
    expect(
      await db`SELECT state,count(*)::integer n FROM execution_quota_leases GROUP BY state`,
    ).toEqual([{ state: "ACTIVE", n: 3 }]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "1000",
      reserved: "30",
      spent: "0",
      state: "ACTIVE",
    });
    await db.unsafe(
      "DROP TRIGGER success_charge_failure ON billing_charges;DROP FUNCTION success_charge_failure()",
    );
    committed((await store.persist([{ events: [fact] }]))[0]);
    expect(
      await db`SELECT execution_closed_at IS NOT NULL closed FROM gateway_requests WHERE id=${prepared.requestId}`,
    ).toEqual([{ closed: true }]);
    expect(
      await db`SELECT state,count(*)::integer n FROM execution_quota_leases GROUP BY state`,
    ).toEqual([{ state: "RELEASE_PENDING", n: 3 }]);
    expect(
      await db`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${prepared.requestId}`,
    ).toEqual([{ amount_atoms: "100" }]);
  }));
