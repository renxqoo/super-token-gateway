import { expect, test } from "bun:test";
import type { ExecutionReply } from "@gateway/contracts";
import { createSettlementStore } from "@gateway/storage/settlement";
import { withFinancial, prepareFinancialAdmission } from "../support/financial-execution.ts";
import { committed, event, granted, supplier } from "../support/core-settlement.ts";

test("fragmented supplier budget admits both requests and a locked root or another cell cannot block BEGIN or independent settlement", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100");
    const client = f.client();
    await client.startup();
    const first = await prepareFinancialAdmission(setup, client, {
      attemptId: "10000000-0000-4000-8000-000000000000",
    });
    const admittedFirst = (await first.admit()) as ExecutionReply;
    const second = await prepareFinancialAdmission(setup, client, {
      attemptId: "10000000-0000-4000-8000-000000000001",
    });
    const admittedSecond = (await second.admit()) as ExecutionReply;
    expect(admittedFirst.receipt.command_state).toBe("COMMITTED");
    expect(admittedSecond.receipt.command_state).toBe("COMMITTED");
    expect(
      await db`SELECT attempt_id,funding_cell,current_reserved_atoms::text reserved FROM upstream_holds ORDER BY attempt_id`,
    ).toEqual([
      { attempt_id: first.attemptId, funding_cell: 0, reserved: "30" },
      { attempt_id: second.attemptId, funding_cell: 1, reserved: "30" },
    ]);
    const facts = [first, second].map((request) =>
      client.signEvidence({
        event_id: crypto.randomUUID(),
        attempt_id: request.attemptId,
        permit_epoch: "1",
        source: "execution",
        source_revision: "1",
        finality: "FINAL",
        usage: { input_tokens: "30", output_tokens: "15" },
        final_dimensions: ["input_tokens", "output_tokens"],
        attempt_state: "SUCCEEDED",
        request_state: "SUCCEEDED",
      }),
    );
    const store = createSettlementStore(db);
    await db.begin(async (tx) => {
      await tx`SELECT account_id FROM upstream_account_balances WHERE account_id=${setup.provider.account_id} FOR NO KEY UPDATE`;
      await tx`SELECT cell FROM core_funding.balance_cells WHERE account_id=${setup.provider.account_id} AND currency='USD' AND cell=0 FOR NO KEY UPDATE`;
      for (const [request, admission] of [
        [first, admittedFirst],
        [second, admittedSecond],
      ] as const) {
        const begun = (await client.execution(
          "execution_begin_v1",
          request.beginData(admission.receipt),
          { deadlineAt: request.deadline },
        )) as ExecutionReply;
        expect(begun.receipt.state).toBe("SEND_GRANTED");
      }
      committed((await store.persist([{ events: [facts[1]!] }]))[0]);
      expect((await store.persist([{ events: [facts[0]!] }]))[0]).toEqual({
        status: "ERROR",
        error: "55P03",
      });
      expect(
        await db`SELECT count(*)::integer n FROM core_evidence_receipts WHERE attempt_id=${first.attemptId}`,
      ).toEqual([{ n: 0 }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "85",
        reserved: "30",
        spent: "15",
        state: "ACTIVE",
      });
    });
    for (const result of await store.persist(facts.map((fact) => ({ events: [fact] }))))
      committed(result);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "70",
      reserved: "0",
      spent: "30",
      state: "ACTIVE",
    });
    expect(await db`SELECT amount_atoms::text FROM billing_charges ORDER BY request_id`).toEqual([
      { amount_atoms: "100" },
      { amount_atoms: "100" },
    ]);
    expect(await db`SELECT count(*)::integer n FROM core_evidence_receipts`).toEqual([{ n: 2 }]);
    expect(
      (await db`SELECT core_funding.audit_v1(${setup.provider.account_id}::uuid,'USD') value`)[0]
        .value,
    ).toMatchObject({
      balance_atoms: "70",
      reserved_atoms: "0",
      spent_atoms: "30",
      revision: "5",
      cells_conserved: true,
    });
  }));

test("a frozen supplier accepts an in-range negative adjustment when the root alone is at its lower bound without releasing unknown reservation", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1");
    const account = setup.provider.account_id;
    async function adjust(delta: string, revision: string) {
      return f.request(`/upstream-accounts/${account}/adjustments`, {
        external_ref: crypto.randomUUID(),
        currency: "USD",
        delta_atoms: delta,
        expected_revision: revision,
        reason: "Exact frozen representation boundary",
      });
    }
    expect((await adjust("10", "1")).status).toBe(201);
    const client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client, {
      ceilings: { input_tokens: "33", output_tokens: "0" },
    });
    committed(
      (
        await createSettlementStore(db).persist([
          { events: [event(owner, prepared.attemptId, { finality: "UNKNOWN" })] },
        ])
      )[0],
    );
    expect((await adjust("-99999999999999999999999999999999999999", "3")).status).toBe(201);
    const last = await adjust("-1", "4");
    expect(last.status).toBe(201);
    expect(last.value).toMatchObject({
      balance_atoms: "-99999999999999999999999999999999999989",
      balance_revision: "5",
      account_state: "RECONCILE_REQUIRED",
    });
    expect(await supplier(db, account)).toEqual({
      balance: "-99999999999999999999999999999999999989",
      reserved: "11",
      spent: "0",
      state: "RECONCILE_REQUIRED",
    });
    expect(
      await db`SELECT current_reserved_atoms::text reserved FROM upstream_holds WHERE attempt_id=${prepared.attemptId}`,
    ).toEqual([{ reserved: "11" }]);
    expect(
      (await db`SELECT core_funding.audit_v1(${account}::uuid,'USD') value`)[0].value,
    ).toMatchObject({
      adjustment_atoms: "-99999999999999999999999999999999999990",
      revision: "5",
      cells_conserved: true,
    });
  }));

test("a large prefunded supplier can admit on another cell while the first still has ample budget but is locked", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const client = f.client();
    await client.startup();
    const first = await prepareFinancialAdmission(setup, client, {
      attemptId: "10000000-0000-4000-8000-000000000000",
    });
    await first.admit();
    await db.begin(async (tx) => {
      const [cell] =
        await tx`SELECT balance_atoms-reserved_atoms free FROM core_funding.balance_cells WHERE account_id=${setup.provider.account_id} AND currency='USD' AND cell=0 FOR NO KEY UPDATE`;
      expect(BigInt(cell.free)).toBeGreaterThan(30n);
      const next = await prepareFinancialAdmission(setup, client, {
        attemptId: "10000000-0000-4000-8000-000000000001",
      });
      const reply = (await next.admit()) as ExecutionReply;
      expect(reply.receipt.command_state).toBe("COMMITTED");
      expect(
        await db`SELECT funding_cell,current_reserved_atoms::text reserved FROM upstream_holds WHERE attempt_id=${next.attemptId}`,
      ).toEqual([{ funding_cell: 1, reserved: "30" }]);
    });
  }));
