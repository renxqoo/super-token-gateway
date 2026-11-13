import { expect, test } from "bun:test";
import type { SQL } from "bun";
import type { ExecutionReply } from "@gateway/contracts";
import { Store } from "@gateway/storage";
import { createSettlementStore } from "@gateway/storage/settlement";
import { sha256 } from "@gateway/runtime/crypto";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";
import {
  committed,
  event,
  granted,
  supplier,
  type Fixture,
  type Setup,
} from "../support/core-settlement.ts";

const thirds = { numerator: "1", denominator: "3" };
const whole = (n: string) => ({ numerator: n, denominator: "1" });

async function receipts(db: SQL, attempt: string) {
  return (
    await db`SELECT outcome,source_revision::text revision FROM core_evidence_receipts WHERE attempt_id=${attempt} ORDER BY created_at,source_revision`
  ).map((r: { outcome: string; revision: string }) => [r.revision, r.outcome]);
}

async function hold(db: SQL, attempt: string) {
  return (
    await db`SELECT state,final_cost_atoms::text final_cost FROM upstream_holds WHERE attempt_id=${attempt}`
  )[0];
}

async function recharge(f: Fixture, setup: Setup, credited: string) {
  const bytes = Buffer.from(`%PDF-synthetic-second-recharge-${crypto.randomUUID()}`);
  const voucher = await f.request("/financial-vouchers", {
    content_base64: bytes.toString("base64"),
    content_type: "application/pdf",
    digest: sha256(bytes),
    expires_at: new Date(Date.now() + 3600000).toISOString(),
  });
  expect(voucher.status).toBe(201);
  const response = await f.request(`/upstream-accounts/${setup.provider.account_id}/recharges`, {
    ...setup.rechargeInput,
    external_ref: crypto.randomUUID(),
    credited_atoms: credited,
    voucher_id: voucher.value.id,
    voucher_digest: voucher.value.digest,
  });
  expect(response.status).toBe(201);
}

test("a later unknown outcome cannot downgrade a FINAL snapshot and provider usage pricing is still charged", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000", 100, "PROVIDER_USAGE"),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const store = createSettlementStore(db);
    committed(
      (
        await store.persist([
          {
            events: [
              event(owner, prepared.attemptId, {
                finality: "FINAL",
                usage: { input_tokens: "30", output_tokens: "15" },
                final_dimensions: ["input_tokens", "output_tokens"],
              }),
            ],
          },
        ])
      )[0],
    );
    committed(
      (
        await store.persist([
          {
            events: [
              event(owner, prepared.attemptId, {
                source: "execution_outcome",
                finality: "UNKNOWN",
                attempt_state: "SUCCEEDED",
                request_state: "SUCCEEDED",
              }),
            ],
          },
        ])
      )[0],
    );
    expect((await new Store(db).usage(setup.project.id)).map((row) => row.finality)).toEqual([
      "FINAL",
      "FINAL",
    ]);
    expect(
      await db`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${prepared.requestId}`,
    ).toEqual([{ amount_atoms: "60" }]);
    expect(
      (await db`SELECT state FROM billing_holds WHERE request_id=${prepared.requestId}`)[0].state,
    ).toBe("SETTLED");
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "985",
      reserved: "0",
      spent: "15",
      state: "ACTIVE",
    });
  }));

test("a same-content revision replay after an ESTIMATED receipt is a replay instead of a constraint error", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const store = createSettlementStore(db);
    const fields = {
      finality: "ESTIMATED" as const,
      usage: { input_tokens: "10" },
    };
    committed((await store.persist([{ events: [event(owner, prepared.attemptId, fields)] }]))[0]);
    const [replay] = await store.persist([{ events: [event(owner, prepared.attemptId, fields)] }]);
    expect(committed(replay).receipts.map((r) => [r.outcome, r.duplicate])).toEqual([
      ["REVISION_REPLAY", false],
    ]);
    const rows =
      await db`SELECT outcome,raw_evidence_id,canonical_evidence_id FROM core_evidence_receipts WHERE attempt_id=${prepared.attemptId} ORDER BY created_at`;
    expect(rows.map((r: { outcome: string }) => r.outcome)).toEqual([
      "ESTIMATED",
      "REVISION_REPLAY",
    ]);
    expect(rows[1].raw_evidence_id).toBe(rows[0].raw_evidence_id);
    expect(rows[1].canonical_evidence_id).toBeNull();
    expect(
      (
        await db`SELECT count(*)::integer n FROM usage_event_index WHERE attempt_id=${prepared.attemptId}`
      )[0].n,
    ).toBe(1);
  }));

test("in-group copies of a contested revision are each recorded as conflicts instead of a constraint error", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const store = createSettlementStore(db);
    committed(
      (
        await store.persist([
          {
            events: [
              event(owner, prepared.attemptId, {
                finality: "FINAL",
                usage: { input_tokens: "30" },
                final_dimensions: ["input_tokens"],
              }),
            ],
          },
        ])
      )[0],
    );
    const copy = {
      source_revision: "2",
      finality: "FINAL" as const,
      usage: { input_tokens: "31" },
      final_dimensions: ["input_tokens"],
    };
    const [group] = await store.persist([
      {
        events: [event(owner, prepared.attemptId, copy), event(owner, prepared.attemptId, copy)],
      },
    ]);
    expect(committed(group).receipts.map((r) => r.outcome)).toEqual(["CONFLICT", "CONFLICT"]);
    expect(await receipts(db, prepared.attemptId)).toEqual([
      ["1", "ACCEPTED"],
      ["2", "CONFLICT"],
      ["2", "CONFLICT"],
    ]);
    expect((await supplier(db, setup.provider.account_id)).state).toBe("RECONCILE_REQUIRED");
  }));

test("a third in-group claim equal to the first contested value is also contested and nothing becomes canonical", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const claim = (revision: string, input: string) =>
      event(owner, prepared.attemptId, {
        source_revision: revision,
        finality: "FINAL",
        usage: { input_tokens: input },
        final_dimensions: ["input_tokens"],
      });
    const [group] = await createSettlementStore(db).persist([
      { events: [claim("1", "30"), claim("2", "31"), claim("3", "30")] },
    ]);
    expect(committed(group).receipts.map((r) => r.outcome)).toEqual([
      "CONFLICT",
      "CONFLICT",
      "CONFLICT",
    ]);
    expect(
      (
        await db`SELECT count(*)::integer n FROM usage_event_index WHERE attempt_id=${prepared.attemptId} AND record_kind='CANONICAL'`
      )[0].n,
    ).toBe(0);
    expect((await supplier(db, setup.provider.account_id)).state).toBe("RECONCILE_REQUIRED");
  }));

test("a dispute whose exposure is covered by the posted cost leaves the supplier hold unsettled without a final cost", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const store = createSettlementStore(db);
    committed(
      (
        await store.persist([
          {
            events: [
              event(owner, prepared.attemptId, {
                finality: "FINAL",
                usage: { input_tokens: "30", output_tokens: "15" },
                final_dimensions: ["input_tokens", "output_tokens"],
              }),
            ],
          },
        ])
      )[0],
    );
    expect(await hold(db, prepared.attemptId)).toEqual({ state: "SETTLED", final_cost: "15" });
    committed(
      (
        await store.persist([
          {
            events: [
              event(owner, prepared.attemptId, {
                source: "provider_invoice",
                finality: "FINAL",
                usage: { input_tokens: "29", output_tokens: "15" },
                final_dimensions: ["input_tokens", "output_tokens"],
              }),
            ],
          },
        ])
      )[0],
    );
    expect(await hold(db, prepared.attemptId)).toEqual({ state: "PARTIAL", final_cost: null });
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "985",
      reserved: "0",
      spent: "15",
      state: "RECONCILE_REQUIRED",
    });
  }));

test("an evidence group meeting a held wallet lock reports busy at once instead of waiting for the lock timeout", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
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
    let release!: () => void, locked!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const held = new Promise<void>((resolve) => (locked = resolve));
    const holder = db.begin(async (tx) => {
      await tx`SELECT 1 FROM wallets WHERE id=(SELECT wallet_id FROM billing_holds WHERE request_id=${prepared.requestId}) FOR NO KEY UPDATE`;
      locked();
      await gate;
    });
    let busy;
    let elapsed = 0;
    try {
      await held;
      const started = performance.now();
      busy = await store.persist([{ events: [fact] }]);
      elapsed = performance.now() - started;
    } finally {
      release();
      await holder;
    }
    expect(busy).toEqual([{ status: "ERROR", error: "55P03" }]);
    expect(elapsed).toBeLessThan(1000);
    expect(
      (
        await db`SELECT count(*)::integer n FROM core_evidence_receipts WHERE attempt_id=${prepared.attemptId}`
      )[0].n,
    ).toBe(0);
    expect(committed((await store.persist([{ events: [fact] }]))[0]).receipts).toHaveLength(1);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "985",
      reserved: "0",
      spent: "15",
      state: "ACTIVE",
    });
  }));

test("signature and subject rejections leave a bounded technical audit without claiming the events", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const successor = f.client();
    await successor.startup();
    const genuine = event(owner, prepared.attemptId, {
      finality: "FINAL",
      usage: { input_tokens: "3" },
      final_dimensions: ["input_tokens"],
    });
    const tampered = { ...genuine, usage: { input_tokens: "4" } };
    const forged = event({ ...owner, client: successor }, prepared.attemptId, {
      source_revision: "2",
      finality: "FINAL",
      usage: { input_tokens: "5" },
      final_dimensions: ["input_tokens"],
    });
    expect(
      await createSettlementStore(db).persist([{ events: [tampered] }, { events: [forged] }]),
    ).toEqual([
      { status: "REJECTED", error: "evidence_signature_invalid" },
      { status: "REJECTED", error: "evidence_subject_invalid" },
    ]);
    expect(
      await db`SELECT reason,origin_identity,origin_session_id::text session,event_id::text event,attempt_id::text attempt,ordinal FROM core_evidence_rejections WHERE attempt_id=${prepared.attemptId} ORDER BY ordinal`,
    ).toEqual([
      {
        reason: "evidence_signature_invalid",
        origin_identity: genuine.origin_identity,
        session: genuine.origin_session_id,
        event: genuine.event_id,
        attempt: prepared.attemptId,
        ordinal: 0,
      },
      {
        reason: "evidence_subject_invalid",
        origin_identity: forged.origin_identity,
        session: forged.origin_session_id,
        event: forged.event_id,
        attempt: prepared.attemptId,
        ordinal: 1,
      },
    ]);
    expect(
      (
        await db`SELECT count(*)::integer n FROM core_evidence_receipts WHERE attempt_id=${prepared.attemptId}`
      )[0].n,
    ).toBe(0);
  }));

test("manual example A: a known request fee posts while temporary tokens stay reserved and the final adds only the difference", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000", 100, "REQUEST_COMPLETION", {
        rates: { requests: whole("2"), input_tokens: thirds, output_tokens: thirds },
        ceilings: { requests: "1", input_tokens: "60", output_tokens: "30" },
      }),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client, {
      ceilings: { requests: "1", input_tokens: "60", output_tokens: "30" },
    });
    const account = setup.provider.account_id;
    const store = createSettlementStore(db);
    expect(await supplier(db, account)).toEqual({
      balance: "1000",
      reserved: "32",
      spent: "0",
      state: "ACTIVE",
    });
    committed(
      (
        await store.persist([
          {
            events: [
              event(owner, prepared.attemptId, {
                usage: { requests: "1", input_tokens: "30" },
                final_dimensions: ["requests"],
              }),
            ],
          },
        ])
      )[0],
    );
    expect(await supplier(db, account)).toEqual({
      balance: "998",
      reserved: "30",
      spent: "2",
      state: "ACTIVE",
    });
    committed(
      (
        await store.persist([
          {
            events: [
              event(owner, prepared.attemptId, {
                source_revision: "2",
                finality: "FINAL",
                usage: { requests: "1", input_tokens: "31", output_tokens: "2" },
                final_dimensions: ["input_tokens", "output_tokens", "requests"],
              }),
            ],
          },
        ])
      )[0],
    );
    expect(await supplier(db, account)).toEqual({
      balance: "987",
      reserved: "0",
      spent: "13",
      state: "ACTIVE",
    });
  }));

test("manual example B: an overrun with an unknown dimension freezes at once, posts in full below zero and a recharge does not revive the account", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("HKD", "50", 100, "REQUEST_COMPLETION", {
        rates: { input_tokens: whole("1"), output_tokens: whole("1") },
        ceilings: { input_tokens: "20", output_tokens: "20" },
      }),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client, {
      ceilings: { input_tokens: "20", output_tokens: "20" },
    });
    const account = setup.provider.account_id;
    const store = createSettlementStore(db);
    expect(await supplier(db, account)).toEqual({
      balance: "50",
      reserved: "40",
      spent: "0",
      state: "ACTIVE",
    });
    committed(
      (
        await store.persist([
          {
            events: [
              event(owner, prepared.attemptId, {
                usage: { input_tokens: "30" },
                final_dimensions: ["input_tokens"],
              }),
            ],
          },
        ])
      )[0],
    );
    expect(await supplier(db, account)).toEqual({
      balance: "20",
      reserved: "20",
      spent: "30",
      state: "RECONCILE_REQUIRED",
    });
    committed(
      (
        await store.persist([
          {
            events: [
              event(owner, prepared.attemptId, {
                source_revision: "2",
                finality: "FINAL",
                usage: { input_tokens: "30", output_tokens: "25" },
                final_dimensions: ["input_tokens", "output_tokens"],
              }),
            ],
          },
        ])
      )[0],
    );
    expect(await supplier(db, account)).toEqual({
      balance: "-5",
      reserved: "0",
      spent: "55",
      state: "RECONCILE_REQUIRED",
    });
    await recharge(f, setup, "100");
    expect(await supplier(db, account)).toEqual({
      balance: "95",
      reserved: "0",
      spent: "55",
      state: "RECONCILE_REQUIRED",
    });
  }));

test("manual example C: a successful retry does not release the earlier unknown attempt and total supplier cost is 80", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "500", 100, "REQUEST_COMPLETION", {
        rates: { requests: whole("20"), input_tokens: whole("0"), output_tokens: whole("1") },
        ceilings: { requests: "1", input_tokens: "100", output_tokens: "80" },
      }),
      client = f.client();
    await client.startup();
    const account = setup.provider.account_id;
    const store = createSettlementStore(db);
    const first = await granted(db, setup, client, {
      ceilings: { requests: "1", input_tokens: "10", output_tokens: "80" },
    });
    expect(await supplier(db, account)).toEqual({
      balance: "500",
      reserved: "100",
      spent: "0",
      state: "ACTIVE",
    });
    const retry = await prepareFinancialAdmission(setup, client, {
      requestId: first.prepared.requestId,
      deadline: first.prepared.deadline,
      ceilings: { requests: "1", input_tokens: "10", output_tokens: "60" },
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
    expect(await supplier(db, account)).toEqual({
      balance: "500",
      reserved: "180",
      spent: "0",
      state: "ACTIVE",
    });
    const owner = { client, permit_epoch: "1" };
    committed(
      (
        await store.persist([
          {
            events: [
              event(owner, retry.attemptId, {
                usage: { requests: "1" },
                final_dimensions: ["requests"],
              }),
            ],
          },
        ])
      )[0],
    );
    expect(await supplier(db, account)).toEqual({
      balance: "480",
      reserved: "160",
      spent: "20",
      state: "ACTIVE",
    });
    committed(
      (
        await store.persist([
          {
            events: [
              event(owner, retry.attemptId, {
                source_revision: "2",
                finality: "FINAL",
                usage: { requests: "1", input_tokens: "5", output_tokens: "30" },
                final_dimensions: ["input_tokens", "output_tokens", "requests"],
                attempt_state: "SUCCEEDED",
                request_state: "SUCCEEDED",
              }),
            ],
          },
        ])
      )[0],
    );
    expect(await supplier(db, account)).toEqual({
      balance: "450",
      reserved: "100",
      spent: "50",
      state: "ACTIVE",
    });
    committed(
      (
        await store.persist([
          {
            events: [
              event(first.owner, first.prepared.attemptId, {
                finality: "FINAL",
                usage: { requests: "1", input_tokens: "5", output_tokens: "10" },
                final_dimensions: ["input_tokens", "output_tokens", "requests"],
                attempt_state: "SUCCEEDED",
                request_state: "SUCCEEDED",
              }),
            ],
          },
        ])
      )[0],
    );
    expect(await supplier(db, account)).toEqual({
      balance: "420",
      reserved: "0",
      spent: "80",
      state: "ACTIVE",
    });
    expect(
      (
        await db`SELECT selected_attempt_id FROM gateway_requests WHERE id=${first.prepared.requestId}`
      )[0].selected_attempt_id,
    ).toBe(retry.attemptId);
    expect(
      await db`SELECT count(*)::integer n FROM billing_charges WHERE request_id=${first.prepared.requestId}`,
    ).toEqual([{ n: 1 }]);
  }));

test("a supplier cost beyond 38 digits is an OVERFLOW dispute that freezes the account instead of a failing group", () =>
  withFinancial(async (f, db) => {
    const max = "99999999999999999999999999999999999999";
    const setup = await f.bootstrap("USD", max, 100, "REQUEST_COMPLETION", {
        rates: { input_tokens: whole(max), output_tokens: whole("0") },
        ceilings: { input_tokens: "1", output_tokens: "1" },
      }),
      client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client, {
      ceilings: { input_tokens: "1", output_tokens: "1" },
    });
    const [group] = await createSettlementStore(db).persist([
      {
        events: [
          event(owner, prepared.attemptId, {
            finality: "FINAL",
            usage: { input_tokens: "2", output_tokens: "0" },
            final_dimensions: ["input_tokens", "output_tokens"],
          }),
        ],
      },
    ]);
    expect(committed(group).receipts.map((r) => r.outcome)).toEqual(["OVERFLOW"]);
    expect(
      (
        await db`SELECT numeric_fault FROM core_evidence_receipts WHERE attempt_id=${prepared.attemptId}`
      )[0].numeric_fault,
    ).toBe("COST_OVERFLOW");
    expect(
      (
        await db`SELECT array_agg(record_kind ORDER BY snapshot_revision) kinds FROM usage_event_index WHERE attempt_id=${prepared.attemptId}`
      )[0].kinds,
    ).toEqual(["RAW", "DISPUTED"]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: max,
      reserved: max,
      spent: "0",
      state: "RECONCILE_REQUIRED",
    });
  }));
