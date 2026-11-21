import { expect, test } from "bun:test";
import type { SQL } from "bun";
import type { CoreEvidenceGroupV1 } from "@gateway/contracts";
import { createSettlementStore } from "@gateway/storage/settlement";
import { createDatabase } from "@gateway/storage";
import { financialRejection } from "../support/financial-rejection.ts";
import { withFinancial, prepareFinancialAdmission } from "../support/financial-execution.ts";
import { committed, event, granted, supplier } from "../support/core-settlement.ts";

type Fixture = Parameters<Parameters<typeof withFinancial>[0]>[0];
type Setup = Awaited<ReturnType<Fixture["bootstrap"]>>;

async function secondWallet(f: Fixture, setup: Setup) {
  const project = await f.request("/projects", {
    name: `independent evidence wallet ${crypto.randomUUID()}`,
    billing_currency: "USD",
    rpm: 1000,
    tpm: 10000000,
    concurrency: 100,
  });
  expect(project.status).toBe(201);
  const projectId = String(project.value.id);
  const key = await f.request(`/projects/${projectId}/keys`, {
    name: "independent evidence key",
    models: [setup.model],
    operations: ["chat"],
  });
  expect(key.status).toBe(201);
  const credit = await f.request(`/projects/${projectId}/credits`, {
    external_ref: crypto.randomUUID(),
    currency: "USD",
    amount_atoms: "1000",
    kind: "CASH",
  });
  expect(credit.status).toBe(201);
  return {
    ...setup,
    project: { id: projectId },
    key: { id: String(key.value.id), key: String(key.value.key) },
  };
}

async function traceFacts(db: SQL) {
  await db.unsafe(`
    CREATE TABLE public.evidence_batch_trace(name text,operation text,rows integer);
    CREATE FUNCTION public.evidence_batch_trace() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
    BEGIN INSERT INTO public.evidence_batch_trace SELECT TG_TABLE_NAME,TG_OP,count(*) FROM new_rows;RETURN NULL;END $$;
  `);
  for (const [table, operations] of [
    ["usage_event_index", ["INSERT"]],
    ["usage_payloads", ["INSERT"]],
    ["core_evidence_receipts", ["INSERT"]],
    ["upstream_costs", ["INSERT"]],
    ["upstream_cost_heads", ["INSERT"]],
    ["upstream_holds", ["UPDATE"]],
    ["billing_charges", ["INSERT"]],
    ["wallets", ["UPDATE"]],
    ["attempts", ["UPDATE"]],
  ] as const) {
    for (const operation of operations)
      await db.unsafe(
        `CREATE TRIGGER evidence_batch_${operation} AFTER ${operation} ON public.${table} REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.evidence_batch_trace()`,
      );
  }
}

test("independent signed groups write their facts and costs together while preserving each receipt and charge", () =>
  withFinancial(async (f, db) => {
    const firstSetup = await f.bootstrap("USD", "100000");
    const secondSetup = await secondWallet(f, firstSetup);
    const client = f.client();
    await client.startup();
    const first = await granted(db, firstSetup, client, {
      attemptId: "10000000-0000-4000-8000-000000000000",
    });
    const second = await granted(db, secondSetup, client, {
      attemptId: "20000000-0000-4000-8000-000000000000",
    });
    const groups = [first, second].map(({ prepared, owner }) => ({
      events: [
        event(owner, prepared.attemptId, {
          finality: "FINAL",
          usage: { input_tokens: "1", output_tokens: "0" },
          final_dimensions: ["input_tokens", "output_tokens"],
        }),
        event(owner, prepared.attemptId, {
          source: "execution_outcome",
          finality: "UNKNOWN",
          attempt_state: "SUCCEEDED",
          request_state: "SUCCEEDED",
        }),
      ],
    }));
    await traceFacts(db);
    const store = createSettlementStore(db);
    const results = await store.persist(groups);
    for (let i = 0; i < groups.length; i++) {
      const result = committed(results[i]);
      expect(result.receipts.map((receipt) => receipt.event_id).sort()).toEqual(
        groups[i]!.events.map((event) => event.event_id).sort(),
      );
      expect(result.receipts.map((receipt) => [receipt.outcome, receipt.duplicate])).toEqual([
        ["ACCEPTED", false],
        ["ACCEPTED", false],
      ]);
      expect(Object.keys(result.billing ?? {})).toEqual([[first, second][i]!.prepared.requestId]);
    }
    expect(
      await db`SELECT name,operation,array_agg(rows ORDER BY rows) rows FROM evidence_batch_trace WHERE rows>0 GROUP BY name,operation ORDER BY name,operation`,
    ).toEqual([
      { name: "attempts", operation: "UPDATE", rows: [2] },
      { name: "billing_charges", operation: "INSERT", rows: [2] },
      { name: "core_evidence_receipts", operation: "INSERT", rows: [4] },
      { name: "upstream_cost_heads", operation: "INSERT", rows: [2] },
      { name: "upstream_costs", operation: "INSERT", rows: [2] },
      { name: "upstream_holds", operation: "UPDATE", rows: [2] },
      { name: "usage_event_index", operation: "INSERT", rows: [8] },
      { name: "usage_payloads", operation: "INSERT", rows: [8] },
      { name: "wallets", operation: "UPDATE", rows: [2] },
    ]);
    expect(
      await db`SELECT snapshot_revision::text revision,record_kind,finality FROM usage_event_index WHERE attempt_id=${first.prepared.attemptId} ORDER BY snapshot_revision`,
    ).toEqual([
      { revision: "1", record_kind: "RAW", finality: "FINAL" },
      { revision: "2", record_kind: "CANONICAL", finality: "FINAL" },
      { revision: "3", record_kind: "RAW", finality: "UNKNOWN" },
      { revision: "4", record_kind: "CANONICAL", finality: "FINAL" },
    ]);
    expect(
      await db`SELECT available_atoms::text available,reserved_atoms::text reserved,spent_atoms::text spent FROM wallets ORDER BY project_id`,
    ).toEqual([
      { available: "900", reserved: "0", spent: "100" },
      { available: "900", reserved: "0", spent: "100" },
    ]);
    expect(await supplier(db, firstSetup.provider.account_id)).toEqual({
      balance: "99998",
      reserved: "0",
      spent: "2",
      state: "ACTIVE",
    });
    const before = await db`SELECT * FROM evidence_batch_trace WHERE rows>0`;
    for (const result of await store.persist(groups))
      for (const receipt of committed(result).receipts) expect(receipt.duplicate).toBe(true);
    expect(await db`SELECT * FROM evidence_batch_trace WHERE rows>0`).toEqual(before);
    expect(await supplier(db, firstSetup.provider.account_id)).toEqual({
      balance: "99998",
      reserved: "0",
      spent: "2",
      state: "ACTIVE",
    });
  }));

test("independent evidence groups do not scan unrelated attempts as execution history grows", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000");
    const otherWallet = await secondWallet(f, setup);
    const client = f.client();
    await client.startup();
    const history = await granted(db, setup, client);
    const requests = [await granted(db, setup, client), await granted(db, otherWallet, client)];
    const groups = requests.map(({ prepared, owner }) => ({
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
    await f.identity.a.stop();
    await f.identity.b.stop();
    await db`INSERT INTO attempts(id,request_id,ordinal,provider_id,upstream_model,pricing_id,state,billing_pricing_id,binding_id)
      SELECT gen_random_uuid(),a.request_id,n,a.provider_id,a.upstream_model,a.pricing_id,'NOT_EXECUTED',a.billing_pricing_id,a.binding_id
      FROM attempts a CROSS JOIN generate_series(2,10001) n WHERE a.id=${history.prepared.attemptId}`;
    await db`ANALYZE attempts`;
    const url = new URL(process.env.DATABASE_URL!);
    url.pathname = `/${(await db`SELECT current_database() name`)[0].name}`;
    const connection = createDatabase(url.href, 1);
    const visited = async () => {
      await connection`SELECT pg_stat_force_next_flush()`;
      await connection`SELECT pg_stat_clear_snapshot()`;
      return BigInt(
        (
          await connection`SELECT (seq_tup_read+idx_tup_fetch)::text visits FROM pg_stat_user_tables WHERE relid='attempts'::regclass`
        )[0].visits,
      );
    };
    try {
      expect(await connection`SELECT count(*)::int count FROM attempts`).toEqual([
        { count: 10003 },
      ]);
      const before = await visited();
      const results = await createSettlementStore(connection).persist(groups);
      expect(results.map((result) => committed(result).receipts[0]?.event_id)).toEqual(
        groups.map(({ events }) => events[0]!.event_id),
      );
      expect(results.map((result) => Object.keys(committed(result).billing))).toEqual(
        requests.map(({ prepared }) => [prepared.requestId]),
      );
      expect((await visited()) - before).toBeLessThanOrEqual(64n);
    } finally {
      await connection.close();
    }
  }));

test("a busy supplier cell does not materialize facts that its coalesced group will roll back", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000");
    const otherWallet = await secondWallet(f, setup);
    const client = f.client();
    await client.startup();
    const first = await granted(db, setup, client, {
      attemptId: "10000000-0000-4000-8000-000000000000",
    });
    const second = await granted(db, otherWallet, client, {
      attemptId: "10000000-0000-4000-8000-000000000001",
    });
    expect(await db`SELECT funding_cell FROM upstream_holds ORDER BY attempt_id`).toEqual([
      { funding_cell: 0 },
      { funding_cell: 1 },
    ]);
    const groups = [first, second].map(({ prepared, owner }) => ({
      events: [
        event(owner, prepared.attemptId, {
          finality: "FINAL",
          usage: { input_tokens: "1", output_tokens: "0" },
          final_dimensions: ["input_tokens", "output_tokens"],
        }),
        event(owner, prepared.attemptId, {
          source: "execution_outcome",
          finality: "UNKNOWN",
          attempt_state: "SUCCEEDED",
          request_state: "SUCCEEDED",
        }),
      ],
    }));
    await db.unsafe(`
      CREATE SEQUENCE public.evidence_write_attempts MINVALUE 0 START 0;
      CREATE FUNCTION public.evidence_write_attempt() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
      BEGIN PERFORM nextval('public.evidence_write_attempts');RETURN NEW;END $$;
      CREATE TRIGGER evidence_write_attempt BEFORE INSERT ON public.usage_event_index FOR EACH ROW EXECUTE FUNCTION public.evidence_write_attempt();
    `);
    const store = createSettlementStore(db);
    await db.begin(async (tx) => {
      await tx`SELECT cell FROM core_funding.balance_cells WHERE account_id=${setup.provider.account_id} AND currency='USD' AND cell=1 FOR NO KEY UPDATE`;
      const results = await store.persist(groups);
      committed(results[0]);
      expect(results[1]).toEqual({ status: "ERROR", error: "55P03" });
      expect(
        await db`SELECT CASE WHEN is_called THEN last_value+1 ELSE 0 END::text count FROM evidence_write_attempts`,
      ).toEqual([{ count: "4" }]);
      expect(await db`SELECT count(*)::int count FROM usage_event_index`).toEqual([{ count: 4 }]);
      expect(await db`SELECT request_id FROM billing_charges`).toEqual([
        { request_id: first.prepared.requestId },
      ]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "999",
        reserved: "30",
        spent: "1",
        state: "ACTIVE",
      });
    });
    const retried = await store.persist(groups);
    expect(committed(retried[0]).receipts.every((receipt) => receipt.duplicate)).toBe(true);
    expect(committed(retried[1]).receipts.every((receipt) => !receipt.duplicate)).toBe(true);
    expect(
      await db`SELECT CASE WHEN is_called THEN last_value+1 ELSE 0 END::text count FROM evidence_write_attempts`,
    ).toEqual([{ count: "8" }]);
    expect(await db`SELECT count(*)::int count FROM billing_charges`).toEqual([{ count: 2 }]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "998",
      reserved: "0",
      spent: "2",
      state: "ACTIVE",
    });
  }));

test("a contended supplier cell no longer forces independent evidence groups into separate writes", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const wallets = [
      setup,
      await secondWallet(f, setup),
      await secondWallet(f, setup),
      await secondWallet(f, setup),
    ];
    const client = f.client();
    await client.startup();
    const facts: CoreEvidenceGroupV1["events"] = [];
    for (const [index, wallet] of wallets.entries()) {
      const { prepared, owner } = await granted(db, wallet, client, {
        attemptId: `10000000-0000-4000-8000-00000000000${index}`,
      });
      facts.push(
        event(owner, prepared.attemptId, {
          finality: "FINAL",
          usage: { input_tokens: "1", output_tokens: "0" },
          final_dimensions: ["input_tokens", "output_tokens"],
          attempt_state: "SUCCEEDED",
          request_state: "SUCCEEDED",
        }),
      );
    }
    const groups = [
      { events: [facts[0]!] },
      { events: [facts[1]!, facts[2]!] },
      { events: [facts[3]!] },
    ];
    expect(await db`SELECT funding_cell FROM upstream_holds ORDER BY attempt_id`).toEqual([
      { funding_cell: 0 },
      { funding_cell: 1 },
      { funding_cell: 2 },
      { funding_cell: 3 },
    ]);
    await traceFacts(db);
    const store = createSettlementStore(db);
    await db.begin(async (tx) => {
      await tx`SELECT cell FROM core_funding.balance_cells WHERE account_id=${setup.provider.account_id} AND currency='USD' AND cell=1 FOR NO KEY UPDATE`;
      const results = await store.persist(groups);
      expect(results[1]).toEqual({ status: "ERROR", error: "55P03" });
      expect([
        committed(results[0]).receipts[0]?.event_id,
        committed(results[2]).receipts[0]?.event_id,
      ]).toEqual([groups[0]!.events[0]!.event_id, groups[2]!.events[0]!.event_id]);
      expect(
        await db`SELECT rows FROM evidence_batch_trace WHERE name='core_evidence_receipts' AND rows>0`,
      ).toEqual([{ rows: 2 }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "99998",
        reserved: "60",
        spent: "2",
        state: "ACTIVE",
      });
    });
    const retried = await store.persist(groups);
    expect(
      retried.map((result) => committed(result).receipts.map((receipt) => receipt.duplicate)),
    ).toEqual([[true], [false, false], [true]]);
    expect(await db`SELECT count(*)::int count FROM billing_charges`).toEqual([{ count: 4 }]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "99996",
      reserved: "0",
      spent: "4",
      state: "ACTIVE",
    });
  }));

test("late disputed evidence on a charged request freezes subsequent reserved sends", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const client = f.client();
    await client.startup();
    const first = await granted(db, setup, client);
    const independent = await f.bootstrap("USD", "100000");
    const secondKey = await f.request(`/projects/${setup.project.id}/keys`, {
      name: "unrelated supplier",
      models: [independent.model],
      operations: ["chat"],
    });
    expect(secondKey.status).toBe(201);
    const pending = await prepareFinancialAdmission(
      {
        ...independent,
        project: setup.project,
        key: { id: String(secondKey.value.id), key: String(secondKey.value.key) },
      },
      client,
    );
    const admission = await pending.admit();
    const store = createSettlementStore(db);
    const signed = (tokens: string) =>
      event(first.owner, first.prepared.attemptId, {
        usage: { input_tokens: tokens, output_tokens: "0" },
        final_dimensions: ["input_tokens", "output_tokens"],
        finality: "FINAL",
        attempt_state: "SUCCEEDED",
        request_state: "SUCCEEDED",
      });
    expect(
      committed((await store.persist([{ events: [signed("1")] }]))[0]).receipts[0]?.outcome,
    ).toBe("ACCEPTED");
    expect(
      await db`SELECT amount_atoms::text amount FROM billing_charges WHERE request_id=${first.prepared.requestId}`,
    ).toEqual([{ amount: "100" }]);
    expect(
      committed((await store.persist([{ events: [signed("2")] }]))[0]).receipts[0]?.outcome,
    ).toBe("CONFLICT");
    expect(
      await db`SELECT evidence_disputed FROM wallets WHERE project_id=${setup.project.id}`,
    ).toEqual([{ evidence_disputed: true }]);
    const result = await financialRejection(
      client.execution("execution_begin_v1", pending.beginData(admission.receipt), {
        deadlineAt: pending.deadline,
      }),
      422,
    );
    expect(result.receipt.command_state).toBe("REJECTED");
    expect(
      await db`SELECT state FROM execution_permits WHERE attempt_id=${pending.attemptId}`,
    ).toEqual([{ state: "RESERVED" }]);
  }));

test("an estimated fact remains writable while its supplier cell is busy without supplier settlement", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000");
    const client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client, {
      attemptId: "10000000-0000-4000-8000-000000000000",
    });
    await db.begin(async (tx) => {
      await tx`SELECT cell FROM core_funding.balance_cells WHERE account_id=${setup.provider.account_id} AND currency='USD' AND cell=0 FOR NO KEY UPDATE`;
      const result = committed(
        (
          await createSettlementStore(db).persist([
            {
              events: [
                event(owner, prepared.attemptId, {
                  finality: "ESTIMATED",
                  usage: { input_tokens: "1", output_tokens: "0" },
                }),
              ],
            },
          ])
        )[0],
      );
      expect(result.receipts.map((receipt) => receipt.outcome)).toEqual(["ESTIMATED"]);
      expect(await db`SELECT record_kind FROM usage_event_index`).toEqual([{ record_kind: "RAW" }]);
      expect(await db`SELECT count(*)::int count FROM billing_charges`).toEqual([{ count: 0 }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "1000",
        reserved: "30",
        spent: "0",
        state: "ACTIVE",
      });
    });
  }));

test("a billing failure after coalesced facts rolls back its whole original two-request group and replays only that group", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const second = await secondWallet(f, setup);
    const third = await secondWallet(f, setup);
    const client = f.client();
    await client.startup();
    const requests = [
      await granted(db, setup, client),
      await granted(db, second, client),
      await granted(db, third, client),
    ];
    const facts = requests.map(({ prepared, owner }) =>
      event(owner, prepared.attemptId, {
        finality: "FINAL",
        usage: { input_tokens: "1", output_tokens: "0" },
        final_dimensions: ["input_tokens", "output_tokens"],
        attempt_state: "SUCCEEDED",
        request_state: "SUCCEEDED",
      }),
    );
    const groups = [{ events: [facts[0]!] }, { events: [facts[1]!, facts[2]!] }];
    await db.unsafe(`
      CREATE FUNCTION public.evidence_batch_billing_failure() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN IF NEW.request_id='${requests[2]!.prepared.requestId}'::uuid THEN RAISE EXCEPTION 'independent group billing failure' USING ERRCODE='P0E98';END IF;RETURN NEW;END $$;
      CREATE TRIGGER evidence_batch_billing_failure BEFORE INSERT ON public.billing_charges FOR EACH ROW EXECUTE FUNCTION public.evidence_batch_billing_failure();
    `);
    const store = createSettlementStore(db);
    const results = await store.persist(groups);
    expect(committed(results[0]).receipts).toHaveLength(1);
    expect(results[1]).toEqual({ status: "ERROR", error: "P0E98" });
    expect(await db`SELECT DISTINCT attempt_id FROM usage_event_index`).toEqual([
      { attempt_id: requests[0]!.prepared.attemptId },
    ]);
    expect(await db`SELECT attempt_id FROM core_evidence_receipts`).toEqual([
      { attempt_id: requests[0]!.prepared.attemptId },
    ]);
    expect(await db`SELECT request_id FROM billing_charges`).toEqual([
      { request_id: requests[0]!.prepared.requestId },
    ]);
    for (const request of requests.slice(1)) {
      expect(
        await db`SELECT state,current_reserved_atoms::text reserved FROM upstream_holds WHERE attempt_id=${request.prepared.attemptId}`,
      ).toEqual([{ state: "HELD", reserved: "30" }]);
      expect(
        await db`SELECT reserved_atoms::text reserved,spent_atoms::text spent FROM wallets WHERE project_id=(SELECT project_id FROM gateway_requests WHERE id=${request.prepared.requestId})`,
      ).toEqual([{ reserved: "100", spent: "0" }]);
    }
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "99999",
      reserved: "60",
      spent: "1",
      state: "ACTIVE",
    });
    await db`DROP TRIGGER evidence_batch_billing_failure ON billing_charges`;
    const replay = await store.persist(groups);
    expect(committed(replay[0]).receipts[0]?.duplicate).toBe(true);
    expect(committed(replay[1]).receipts.map((receipt) => receipt.duplicate)).toEqual([
      false,
      false,
    ]);
    expect(await db`SELECT count(*)::int n FROM billing_charges`).toEqual([{ n: 3 }]);
    expect(await db`SELECT count(*)::int n FROM core_evidence_receipts`).toEqual([{ n: 3 }]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "99997",
      reserved: "0",
      spent: "3",
      state: "ACTIVE",
    });
  }));

test("rejections on both sides of the event window retain one audit batch and input ordinals", () =>
  withFinancial(async (f, db) => {
    const client = f.client();
    await client.startup();
    const groups = Array.from({ length: 65 }, () => ({
      events: [event({ client, permit_epoch: "1" }, crypto.randomUUID(), {})],
    }));
    const results = await createSettlementStore(db).persist(groups);
    expect(results.every((r) => r.status === "REJECTED")).toBe(true);
    expect(await db`SELECT count(DISTINCT batch_id)::int n FROM core_evidence_rejections`).toEqual([
      { n: 1 },
    ]);
    expect(
      (
        await db<
          { id: string; ordinal: number }[]
        >`SELECT event_id::text id,ordinal FROM core_evidence_rejections ORDER BY ordinal`
      ).map((row) => [row.id, row.ordinal]),
    ).toEqual(groups.map((g, i) => [g.events[0]!.event_id, i]));
  }));

test("fully reserved short Chat pairs on one wallet post both charges in two setwise writes across the 64-event limit", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000", 100, "PROVIDER_USAGE");
    const client = f.client();
    await client.startup();
    expect(
      (await f.request(`/providers/${setup.provider.id}`, { concurrency: 100 }, undefined, "PATCH"))
        .status,
    ).toBe(200);
    const credit = await f.request(`/projects/${setup.project.id}/credits`, {
      currency: "USD",
      amount_atoms: "10000",
      priority: 0,
      kind: "CASH",
      external_ref: crypto.randomUUID(),
    });
    expect(credit.status).toBe(201);
    const requests: Awaited<ReturnType<typeof granted>>[] = [];
    for (let i = 0; i < 33; i++) requests.push(await granted(db, setup, client));
    const groups = requests.map(({ prepared, owner }, index) => {
      const usage = { input_tokens: String(1 + (index % 2)), output_tokens: "0" };
      const base = {
        finality: "FINAL" as const,
        usage,
        final_dimensions: ["input_tokens", "output_tokens"],
      };
      return {
        events: [
          event(owner, prepared.attemptId, { ...base, source_revision: "1" }),
          event(owner, prepared.attemptId, {
            ...base,
            source_revision: "2",
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
          }),
        ],
      };
    });
    await traceFacts(db);
    const store = createSettlementStore(db);
    const results = await store.persist(groups);
    for (const [index, group] of groups.entries()) {
      const result = committed(results[index]);
      expect(result.receipts.map((r) => r.event_id)).toEqual(group.events.map((e) => e.event_id));
      expect(result.billing?.[requests[index]!.prepared.requestId]).toMatchObject({
        charged_atoms: String(1 + (index % 2)),
        funded_atoms: String(1 + (index % 2)),
        debt_atoms: "0",
      });
    }
    expect(await db`SELECT rows FROM evidence_batch_trace WHERE name='wallets' AND rows>0`).toEqual(
      [{ rows: 1 }, { rows: 1 }],
    );
    expect(
      await db`SELECT available_atoms::text available,reserved_atoms::text reserved,spent_atoms::text spent FROM wallets`,
    ).toEqual([{ available: "10951", reserved: "0", spent: "49" }]);
    expect(await db`SELECT count(*)::integer n FROM usage_event_index`).toEqual([{ n: 132 }]);
    const replay = await store.persist(groups);
    expect(replay.map((r) => committed(r).receipts.map((x) => x.duplicate))).toEqual(
      requests.map(() => [true, true]),
    );
    expect(await db`SELECT count(*)::integer n FROM billing_charges`).toEqual([{ n: 33 }]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "99967",
      reserved: "0",
      spent: "33",
      state: "ACTIVE",
    });
  }));

test.each(["shared_wallet", "expiring_lot"] as const)(
  "%s groups keep their original customer settlement order instead of coalescing",
  (mode) =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap("USD", "100000");
      const second = mode === "shared_wallet" ? setup : await secondWallet(f, setup);
      if (mode === "expiring_lot") {
        const gift = await f.request(`/projects/${setup.project.id}/credits`, {
          external_ref: crypto.randomUUID(),
          currency: "USD",
          amount_atoms: "50",
          kind: "GIFT",
          expires_at: new Date(Date.now() + 3600000).toISOString(),
        });
        expect(gift.status).toBe(201);
        expect(
          await db`SELECT has_expiring_lots FROM wallets WHERE project_id=${setup.project.id}`,
        ).toEqual([{ has_expiring_lots: true }]);
      }
      const client = f.client();
      await client.startup();
      const requests = [await granted(db, setup, client), await granted(db, second, client)];
      const groups = requests.map(({ prepared, owner }) => ({
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
      await traceFacts(db);
      for (const result of await createSettlementStore(db).persist(groups)) committed(result);
      expect(
        await db`SELECT rows FROM evidence_batch_trace WHERE name='billing_charges' AND rows>0`,
      ).toEqual([{ rows: 1 }, { rows: 1 }]);
      expect(
        await db`SELECT rows FROM evidence_batch_trace WHERE name='usage_event_index' AND rows>0`,
      ).toEqual([{ rows: 2 }, { rows: 2 }]);
      expect(
        await db`SELECT sum(spent_atoms)::text spent,sum(reserved_atoms)::text reserved FROM wallets`,
      ).toEqual([{ spent: "200", reserved: "0" }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "99998",
        reserved: "0",
        spent: "2",
        state: "ACTIVE",
      });
    }),
);

test("two revisions within one signed group preserve final dimensions and round the complete supplier cost once", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000", 100, "PROVIDER_USAGE");
    const client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const facts = [
      event(owner, prepared.attemptId, {
        usage: { input_tokens: "1", output_tokens: "0" },
        final_dimensions: ["input_tokens"],
      }),
      event(owner, prepared.attemptId, {
        source_revision: "2",
        finality: "FINAL",
        usage: { input_tokens: "1", output_tokens: "1" },
        final_dimensions: ["input_tokens", "output_tokens"],
        attempt_state: "SUCCEEDED",
        request_state: "SUCCEEDED",
      }),
    ];
    await traceFacts(db);
    const result = committed((await createSettlementStore(db).persist([{ events: facts }]))[0]);
    expect(result.receipts.map((receipt) => receipt.outcome)).toEqual(["ACCEPTED", "ACCEPTED"]);
    expect(
      await db`SELECT i.snapshot_revision::text revision,i.finality,i.cost_atoms::text cost,i.final_dimensions,p.usage FROM usage_event_index i JOIN usage_payloads p ON p.id=i.id AND p.created_at=i.created_at WHERE i.record_kind='CANONICAL' ORDER BY i.snapshot_revision`,
    ).toEqual([
      {
        revision: "2",
        finality: "PARTIAL",
        cost: "1",
        final_dimensions: ["input_tokens"],
        usage: { input_tokens: "1", output_tokens: "0" },
      },
      {
        revision: "4",
        finality: "FINAL",
        cost: "1",
        final_dimensions: ["input_tokens", "output_tokens"],
        usage: { input_tokens: "1", output_tokens: "1" },
      },
    ]);
    expect(await db`SELECT revision::text,cost_atoms::text cost FROM upstream_costs`).toEqual([
      { revision: "1", cost: "1" },
    ]);
    expect(await db`SELECT amount_atoms::text amount FROM billing_charges`).toEqual([
      { amount: "3" },
    ]);
    expect(
      await db`SELECT available_atoms::text available,reserved_atoms::text reserved,spent_atoms::text spent FROM wallets`,
    ).toEqual([{ available: "997", reserved: "0", spent: "3" }]);
    expect(
      await db`SELECT rows FROM evidence_batch_trace WHERE name='usage_event_index' AND rows>0`,
    ).toEqual([{ rows: 4 }]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "99999",
      reserved: "0",
      spent: "1",
      state: "ACTIVE",
    });
  }));

test("typed settlement continues a partial snapshot without repricing final dimensions or duplicating receipts", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000");
    const client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const attempt = prepared.attemptId;
    const partial = {
      events: [
        event(owner, attempt, {
          finality: "PARTIAL",
          usage: { input_tokens: "1", output_tokens: "0" },
          final_dimensions: ["input_tokens"],
        }),
      ],
    };
    const store = createSettlementStore(db);
    const [oldReceipt] = committed((await store.persist([partial]))[0]).receipts;
    expect(oldReceipt?.outcome).toBe("ACCEPTED");
    const historical = await db<
      { id: string; snapshot_digest: string; revision: string }[]
    >`SELECT id,snapshot_digest,snapshot_revision::text revision
      FROM usage_event_index WHERE attempt_id=${attempt} ORDER BY snapshot_revision`;
    expect(historical.map((row) => row.revision)).toEqual(["1", "2"]);

    const [replayed] = committed((await store.persist([partial]))[0]).receipts;
    expect(replayed).toEqual({ ...oldReceipt!, duplicate: true });
    const final = {
      events: [
        event(owner, attempt, {
          source_revision: "2",
          finality: "FINAL",
          usage: { output_tokens: "0" },
          final_dimensions: ["output_tokens"],
          attempt_state: "SUCCEEDED",
          request_state: "SUCCEEDED",
        }),
      ],
    };
    expect(committed((await store.persist([final]))[0]).receipts[0]?.outcome).toBe("ACCEPTED");
    expect(
      await db`SELECT id,snapshot_digest,snapshot_revision::text revision
      FROM usage_event_index WHERE attempt_id=${attempt} AND snapshot_revision<3 ORDER BY snapshot_revision`,
    ).toEqual(historical);
    expect(
      await db`SELECT i.snapshot_revision::text revision,i.parent_evidence_id parent,i.cost_atoms::text cost,p.usage
      FROM usage_event_index i JOIN usage_payloads p ON p.id=i.id AND p.created_at=i.created_at
      WHERE i.attempt_id=${attempt} AND i.record_kind='CANONICAL' ORDER BY i.snapshot_revision DESC LIMIT 1`,
    ).toEqual([
      {
        revision: "4",
        parent: historical[1]!.id,
        cost: "1",
        usage: { input_tokens: "1", output_tokens: "0" },
      },
    ]);
    expect(await db`SELECT amount_atoms::text amount FROM billing_charges`).toEqual([
      { amount: "100" },
    ]);
    expect(
      await db`SELECT count(*)::int n FROM core_evidence_receipts WHERE attempt_id=${attempt}`,
    ).toEqual([{ n: 2 }]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "999",
      reserved: "0",
      spent: "1",
      state: "ACTIVE",
    });
  }));
