import { expect, test } from "bun:test";
import type { ExecutionReply } from "@gateway/contracts";
import { Store } from "@gateway/storage";
import { createSettlementStore } from "@gateway/storage/settlement";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";
import { body, committed, event, granted, supplier } from "../support/core-settlement.ts";

test("partial then final supplier evidence posts one combined ceiling and settles the selected customer request once", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const store = createSettlementStore(db),
      account = setup.provider.account_id,
      attempt = prepared.attemptId;
    expect(await supplier(db, account)).toEqual({
      balance: "1000",
      reserved: "30",
      spent: "0",
      state: "ACTIVE",
    });

    const partial = event(owner, attempt, {
      usage: { input_tokens: "31", output_tokens: "2" },
      final_dimensions: ["input_tokens"],
    });
    const [first] = await store.persist([{ events: [partial] }]);
    expect(committed(first).receipts.map((r) => r.outcome)).toEqual(["ACCEPTED"]);
    expect(await supplier(db, account)).toEqual({
      balance: "989",
      reserved: "10",
      spent: "11",
      state: "ACTIVE",
    });
    expect(
      (
        await db`SELECT state,billing_outcome FROM gateway_requests WHERE id=${prepared.requestId}`
      )[0],
    ).toEqual({ state: expect.any(String), billing_outcome: "PENDING" });

    const finalBody = body(owner, attempt, {
      source_revision: "3",
      finality: "FINAL",
      usage: { input_tokens: "31", output_tokens: "2" },
      final_dimensions: ["input_tokens", "output_tokens"],
      attempt_state: "SUCCEEDED",
      request_state: "SUCCEEDED",
    });
    const final = client.signEvidence(finalBody);
    const [second] = await store.persist([{ events: [final] }]);
    expect(committed(second).receipts.map((r) => r.outcome)).toEqual(["ACCEPTED"]);
    expect(await supplier(db, account)).toEqual({
      balance: "989",
      reserved: "0",
      spent: "11",
      state: "ACTIVE",
    });
    expect(
      (
        await db`SELECT state,final_cost_atoms::text FROM upstream_holds WHERE attempt_id=${attempt}`
      )[0],
    ).toEqual({ state: "SETTLED", final_cost_atoms: "11" });
    const charge =
      await db`SELECT amount_atoms::text,funded_atoms::text,evidence_id FROM billing_charges WHERE request_id=${prepared.requestId}`;
    expect(charge).toHaveLength(1);
    expect(charge[0].amount_atoms).toBe("100");
    expect(
      (
        await db`SELECT selected_attempt_id,billing_outcome FROM gateway_requests WHERE id=${prepared.requestId}`
      )[0],
    ).toEqual({ selected_attempt_id: attempt, billing_outcome: "SUCCESS" });

    const late = event(owner, attempt, {
      source_revision: "2",
      usage: { input_tokens: "31", output_tokens: "40" },
      final_dimensions: ["input_tokens"],
    });
    const changedEvent = client.signEvidence({ ...finalBody, event_id: crypto.randomUUID() });
    const [replays] = await store.persist([{ events: [partial, late, changedEvent] }]);
    expect(
      Object.fromEntries(
        committed(replays).receipts.map((r) => [r.event_id, [r.outcome, r.duplicate]]),
      ),
    ).toEqual({
      [partial.event_id]: ["ACCEPTED", true],
      [late.event_id]: ["STALE", false],
      [changedEvent.event_id]: ["REVISION_REPLAY", false],
    });
    expect(await supplier(db, account)).toEqual({
      balance: "989",
      reserved: "0",
      spent: "11",
      state: "ACTIVE",
    });
    expect(
      await db`SELECT count(*)::integer n FROM billing_charges WHERE request_id=${prepared.requestId}`,
    ).toEqual([{ n: 1 }]);
    expect(
      (await db`SELECT count(*)::integer n FROM upstream_costs WHERE attempt_id=${attempt}`)[0].n,
    ).toBe(2);
    expect(
      (await new Store(db).usage(setup.project.id)).map((row) => [
        row.semantics,
        row.snapshot_revision,
        row.cost_revision,
        row.cost_atoms,
        row.finality,
      ]),
    ).toEqual([
      ["attempt_snapshot", "4", "2", "11", "FINAL"],
      ["attempt_snapshot", "2", "1", "11", "PARTIAL"],
    ]);
    expect(
      (
        await db`SELECT count(*)::integer n FROM usage_event_index WHERE attempt_id=${attempt} AND record_kind='RAW'`
      )[0].n,
    ).toBe(3);
  }));

test("final cost above the captured ceiling posts in full, goes negative and freezes the supplier account", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "50"),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const [result] = await createSettlementStore(db).persist([
      {
        events: [
          event(owner, prepared.attemptId, {
            finality: "FINAL",
            usage: { input_tokens: "120", output_tokens: "60" },
            final_dimensions: ["input_tokens", "output_tokens"],
          }),
        ],
      },
    ]);
    committed(result);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "-10",
      reserved: "0",
      spent: "60",
      state: "RECONCILE_REQUIRED",
    });
    expect(
      (
        await db`SELECT reason FROM upstream_audit_events WHERE account_id=${setup.provider.account_id} AND action='RECONCILE_REQUIRED'`
      ).map((row: { reason: string }) => row.reason),
    ).toEqual(["supplier_cost_overrun"]);
  }));

test("trusted partial above the estimate reserves conservatively and the later final releases it without refunding", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const store = createSettlementStore(db),
      account = setup.provider.account_id;
    const [risky] = await store.persist([
      { events: [event(owner, prepared.attemptId, { usage: { input_tokens: "90" } })] },
    ]);
    committed(risky);
    expect(await supplier(db, account)).toEqual({
      balance: "1000",
      reserved: "40",
      spent: "0",
      state: "RECONCILE_REQUIRED",
    });
    expect(
      (
        await db`SELECT risk_dimensions FROM core_evidence_receipts WHERE attempt_id=${prepared.attemptId}`
      )[0].risk_dimensions,
    ).toEqual(["input_tokens"]);
    const [estimate] = await store.persist([
      {
        events: [
          event(owner, prepared.attemptId, {
            source: "recovery_interruption",
            finality: "ESTIMATED",
            usage: { input_tokens: "500", output_tokens: "500" },
          }),
        ],
      },
    ]);
    expect(committed(estimate).receipts.map((r) => r.outcome)).toEqual(["ESTIMATED"]);
    expect(await supplier(db, account)).toEqual({
      balance: "1000",
      reserved: "40",
      spent: "0",
      state: "RECONCILE_REQUIRED",
    });
    const [final] = await store.persist([
      {
        events: [
          event(owner, prepared.attemptId, {
            source_revision: "2",
            finality: "FINAL",
            usage: { input_tokens: "30", output_tokens: "15" },
            final_dimensions: ["input_tokens", "output_tokens"],
          }),
        ],
      },
    ]);
    committed(final);
    expect(await supplier(db, account)).toEqual({
      balance: "985",
      reserved: "0",
      spent: "15",
      state: "RECONCILE_REQUIRED",
    });
  }));

test("contradicting finals are disputed before any charge and a later contradiction keeps the posted head", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const store = createSettlementStore(db),
      account = setup.provider.account_id;
    const same = await granted(db, setup, client);
    const a = event(same.owner, same.prepared.attemptId, {
      finality: "FINAL",
      usage: { input_tokens: "30", output_tokens: "15" },
      final_dimensions: ["input_tokens", "output_tokens"],
      request_state: "SUCCEEDED",
    });
    const b = event(same.owner, same.prepared.attemptId, {
      source: "execution_outcome",
      finality: "FINAL",
      usage: { input_tokens: "31", output_tokens: "15" },
      final_dimensions: ["input_tokens", "output_tokens"],
      request_state: "SUCCEEDED",
    });
    const [group] = await store.persist([{ events: [a, b] }]);
    expect(committed(group).receipts.map((r) => r.outcome)).toEqual(["CONFLICT", "CONFLICT"]);
    expect(await supplier(db, account)).toEqual({
      balance: "1000",
      reserved: "30",
      spent: "0",
      state: "RECONCILE_REQUIRED",
    });
    expect(
      await db`SELECT count(*)::integer n FROM billing_charges WHERE request_id=${same.prepared.requestId}`,
    ).toEqual([{ n: 0 }]);
    expect(
      (
        await db`SELECT count(*)::integer n FROM usage_conflicts WHERE attempt_id=${same.prepared.attemptId} AND incoming_receipt_id IS NOT NULL`
      )[0].n,
    ).toBe(2);
    expect(
      (await db`SELECT metering_conflict FROM attempts WHERE id=${same.prepared.attemptId}`)[0]
        .metering_conflict,
    ).toBe(true);
    expect(
      (await new Store(db).usage(setup.project.id)).map((row) => [
        row.semantics,
        row.cost_revision,
        row.cost_atoms,
      ]),
    ).toEqual([
      ["disputed_snapshot", null, "16"],
      ["disputed_snapshot", null, "15"],
    ]);

    const other = await f.bootstrap();
    const later = await granted(db, other, client);
    const [posted] = await store.persist([
      {
        events: [
          event(later.owner, later.prepared.attemptId, {
            finality: "FINAL",
            usage: { input_tokens: "30", output_tokens: "15" },
            final_dimensions: ["input_tokens", "output_tokens"],
          }),
        ],
      },
    ]);
    committed(posted);
    const [contradiction] = await store.persist([
      {
        events: [
          event(later.owner, later.prepared.attemptId, {
            source_revision: "2",
            finality: "FINAL",
            usage: { input_tokens: "33", output_tokens: "15" },
            final_dimensions: ["input_tokens", "output_tokens"],
          }),
        ],
      },
    ]);
    expect(committed(contradiction).receipts.map((r) => r.outcome)).toEqual(["CONFLICT"]);
    expect(
      (
        await db`SELECT c.cost_atoms::text,c.disputed FROM upstream_cost_heads h JOIN upstream_costs c USING(attempt_id,revision) WHERE h.attempt_id=${later.prepared.attemptId}`
      )[0],
    ).toEqual({ cost_atoms: "15", disputed: false });
    expect(await supplier(db, other.provider.account_id)).toEqual({
      balance: "985",
      reserved: "1",
      spent: "15",
      state: "RECONCILE_REQUIRED",
    });
  }));

test("a retry delivery is billed once while the earlier unknown attempt keeps and later settles its own supplier cost", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const store = createSettlementStore(db),
      account = setup.provider.account_id;
    const first = await granted(db, setup, client);
    const retry = await prepareFinancialAdmission(setup, client, {
      requestId: first.prepared.requestId,
      deadline: first.prepared.deadline,
    });
    const next = (await client.execution(
      "execution_next_v1",
      {
        context_id: retry.data.context_id,
        dispatch_projection: retry.data.dispatch_projection,
        dispatched_digest: retry.data.dispatched_digest,
        manifest: retry.data.manifest,
        prep_session_id: retry.data.prep_session_id,
        prep_binding_digest: retry.data.prep_binding_digest,
        request_id: first.prepared.requestId,
        previous_attempt_id: first.prepared.attemptId,
        expected_ordinal: 2,
        visible_output: false,
      },
      { deadlineAt: retry.deadline },
    )) as ExecutionReply;
    await client.execution("execution_begin_v1", retry.beginData(next.receipt), {
      deadlineAt: retry.deadline,
    });
    const owner = { client, permit_epoch: "1" };
    expect(await supplier(db, account)).toEqual({
      balance: "1000",
      reserved: "60",
      spent: "0",
      state: "ACTIVE",
    });
    const [delivered] = await store.persist([
      {
        events: [
          event(owner, retry.attemptId, {
            finality: "FINAL",
            usage: { input_tokens: "30", output_tokens: "15" },
            final_dimensions: ["input_tokens", "output_tokens"],
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
          }),
        ],
      },
    ]);
    committed(delivered);
    expect(await supplier(db, account)).toEqual({
      balance: "985",
      reserved: "30",
      spent: "15",
      state: "ACTIVE",
    });
    const [late] = await store.persist([
      {
        events: [
          event(first.owner, first.prepared.attemptId, {
            finality: "FINAL",
            usage: { input_tokens: "60", output_tokens: "30" },
            final_dimensions: ["input_tokens", "output_tokens"],
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
          }),
        ],
      },
    ]);
    committed(late);
    expect(await supplier(db, account)).toEqual({
      balance: "955",
      reserved: "0",
      spent: "45",
      state: "ACTIVE",
    });
    expect(
      (
        await db`SELECT selected_attempt_id FROM gateway_requests WHERE id=${first.prepared.requestId}`
      )[0].selected_attempt_id,
    ).toBe(retry.attemptId);
    expect(
      await db`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${first.prepared.requestId}`,
    ).toEqual([{ amount_atoms: "100" }]);
  }));

test("evidence from another owner session is rejected without touching a peer group", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const successor = f.client();
    await successor.startup();
    const forged = event({ ...owner, client: successor }, prepared.attemptId, {
      finality: "FINAL",
      usage: { input_tokens: "3" },
      final_dimensions: ["input_tokens"],
    });
    const valid = event(owner, prepared.attemptId, {
      finality: "NOT_EXECUTED",
      attempt_state: "NOT_EXECUTED",
      request_state: "FAILED",
    });
    const results = await createSettlementStore(db).persist([
      { events: [forged] },
      { events: [valid] },
    ]);
    expect(results[0]).toEqual({ status: "REJECTED", error: "evidence_subject_invalid" });
    expect(committed(results[1]).receipts.map((r) => r.outcome)).toEqual(["ACCEPTED"]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "1000",
      reserved: "0",
      spent: "0",
      state: "ACTIVE",
    });
    expect(
      (await db`SELECT state FROM billing_holds WHERE request_id=${prepared.requestId}`)[0].state,
    ).toBe("RELEASED");
    expect(
      (
        await db`SELECT count(*)::integer n FROM core_evidence_receipts WHERE event_id=${forged.event_id}`
      )[0].n,
    ).toBe(0);
  }));

test("final evidence with an unfinished supplier dimension is a FINAL customer snapshot under request completion pricing", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const [result] = await createSettlementStore(db).persist([
      {
        events: [
          event(owner, prepared.attemptId, {
            finality: "FINAL",
            usage: { input_tokens: "30", output_tokens: "15" },
            final_dimensions: ["input_tokens"],
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
          }),
        ],
      },
    ]);
    committed(result);
    expect(
      (await new Store(db).usage(setup.project.id)).map((row) => [row.finality, row.cost_atoms]),
    ).toEqual([["FINAL", "10"]]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "990",
      reserved: "10",
      spent: "10",
      state: "ACTIVE",
    });
    expect(
      await db`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${prepared.requestId}`,
    ).toEqual([{ amount_atoms: "100" }]);
  }));

test("provider usage pricing keeps the snapshot PARTIAL and the charge waiting until every priced dimension is final", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000", 100, "PROVIDER_USAGE"),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const store = createSettlementStore(db);
    const [first] = await store.persist([
      {
        events: [
          event(owner, prepared.attemptId, {
            finality: "FINAL",
            usage: { input_tokens: "30", output_tokens: "15" },
            final_dimensions: ["input_tokens"],
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
          }),
        ],
      },
    ]);
    committed(first);
    expect((await new Store(db).usage(setup.project.id)).map((row) => row.finality)).toEqual([
      "PARTIAL",
    ]);
    expect(
      await db`SELECT count(*)::integer n FROM billing_charges WHERE request_id=${prepared.requestId}`,
    ).toEqual([{ n: 0 }]);
    const [second] = await store.persist([
      {
        events: [
          event(owner, prepared.attemptId, {
            source_revision: "2",
            finality: "FINAL",
            usage: { input_tokens: "30", output_tokens: "15" },
            final_dimensions: ["input_tokens", "output_tokens"],
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
          }),
        ],
      },
    ]);
    committed(second);
    expect((await new Store(db).usage(setup.project.id)).map((row) => row.finality)).toEqual([
      "FINAL",
      "PARTIAL",
    ]);
    expect(
      await db`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${prepared.requestId}`,
    ).toEqual([{ amount_atoms: "60" }]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "985",
      reserved: "0",
      spent: "15",
      state: "ACTIVE",
    });
  }));
