import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { verifier } from "@gateway/runtime/crypto";
import { createSettlementStore } from "@gateway/storage/settlement";
import { withFinancial } from "../support/financial-execution.ts";
import { syntheticPepper } from "../support/catalog.ts";
import { event, granted, type Client, type Setup } from "../support/core-settlement.ts";

async function administrator(db: SQL, permissions: string[]) {
  const id = crypto.randomUUID(),
    token = `synthetic-funds-${crypto.randomUUID()}`;
  await db`INSERT INTO technical_administrators(id,name,permissions) VALUES(${id},'Synthetic funds administrator',${permissions}::text[])`;
  await db`INSERT INTO administrator_credentials(id,administrator_id,verifier,prefix) VALUES(${crypto.randomUUID()},${id},${verifier(token, syntheticPepper)},'synthetic')`;
  return { actor: `technical-admin:${id}`, token };
}

async function charged(db: SQL, setup: Setup, client: Client) {
  const { prepared, owner } = await granted(db, setup, client);
  await createSettlementStore(db).persist([
    {
      events: [
        event(owner, prepared.attemptId, {
          finality: "FINAL",
          usage: { input_tokens: "31", output_tokens: "2" },
          final_dimensions: ["input_tokens", "output_tokens"],
          attempt_state: "SUCCEEDED",
          request_state: "SUCCEEDED",
        }),
      ],
    },
  ]);
  const rows =
    await db`SELECT id,amount_atoms::text FROM billing_charges WHERE request_id=${prepared.requestId}`;
  expect(rows).toEqual([{ id: expect.any(String), amount_atoms: "100" }]);
  return { request: prepared.requestId, charge: String(rows[0].id) };
}

async function audits(db: SQL, resource: string) {
  return (
    await db`SELECT action,actor FROM audit_events WHERE resource_id=${resource} AND actor<>'bootstrap:ADMIN_KEY' ORDER BY created_at,id`
  ).map((row: { action: string; actor: string }) => [row.action, row.actor]);
}

test("Core admin refunds and adjusts a settled v2 charge idempotently, rejects a changed payload and audits each write", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { charge } = await charged(db, setup, client),
      operator = await administrator(db, ["customer:adjust", "customer:read"]),
      wallets = `/projects/${setup.project.id}/wallets`;
    expect((await f.request(wallets, undefined, operator.token)).value.data).toMatchObject([
      { currency: "USD", available_atoms: "900", spent_atoms: "100", debt_atoms: "0" },
    ]);
    const refund = { external_ref: "refund-1", amount_atoms: "30" };
    const first = await f.request(`/charges/${charge}/refunds`, refund, operator.token);
    expect(first.status).toBe(201);
    const replay = await f.request(`/charges/${charge}/refunds`, refund, operator.token);
    expect(replay.status).toBe(201);
    expect(replay.value).toMatchObject({ replayed: true });
    const changed = await f.request(
      `/charges/${charge}/refunds`,
      { ...refund, amount_atoms: "31" },
      operator.token,
    );
    expect(changed).toMatchObject({ status: 409, value: { error: { code: "billing_conflict" } } });
    expect((await f.request(wallets, undefined, operator.token)).value.data).toMatchObject([
      { available_atoms: "930", spent_atoms: "70" },
    ]);
    const adjustment = await f.request(
      `/charges/${charge}/adjustments`,
      { external_ref: "adjust-1", target_atoms: "50", reason: "ADMIN_CORRECTION" },
      operator.token,
    );
    expect(adjustment).toMatchObject({ status: 201, value: { delta_atoms: "-20" } });
    expect((await f.request(wallets, undefined, operator.token)).value.data).toMatchObject([
      { available_atoms: "950", spent_atoms: "50" },
    ]);
    expect(
      (await f.request(`${wallets}/USD/lots`, undefined, operator.token)).value.data,
    ).toMatchObject([{ issued_atoms: "1000", available_atoms: "950", spent_atoms: "50" }]);
    const journal = (await f.request(`${wallets}/USD/journal`, undefined, operator.token)).value
      .data as { id: string; currency: string }[];
    expect(journal.length).toBeGreaterThanOrEqual(3);
    expect(journal.every((entry) => entry.currency === "USD")).toBe(true);
    const older = (
      await f.request(`${wallets}/USD/journal?before=${journal[0]!.id}`, undefined, operator.token)
    ).value.data as { id: string }[];
    expect(older.map((entry) => entry.id)).toEqual(journal.slice(1).map((entry) => entry.id));
    expect(await audits(db, charge)).toEqual([
      ["customer.refund", operator.actor],
      ["customer.refund", operator.actor],
      ["customer.adjustment", operator.actor],
    ]);
  }));

test("Core admin credit, credit policy, wallet freeze and tariffs keep the legacy contracts under customer permissions", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      funder = await administrator(db, ["customer:fund"]),
      reader = await administrator(db, ["customer:read"]),
      project = setup.project.id,
      credit = {
        currency: "USD",
        external_ref: "top-up",
        amount_atoms: "25",
        kind: "GIFT",
        priority: 10,
      };
    const credited = await f.request(`/projects/${project}/credits`, credit, funder.token);
    expect(credited.status).toBe(201);
    expect(
      (await f.request(`/projects/${project}/credits`, credit, funder.token)).value,
    ).toMatchObject({ replayed: true });
    expect(
      await f.request(
        `/projects/${project}/credits`,
        { ...credit, amount_atoms: "26" },
        funder.token,
      ),
    ).toMatchObject({ status: 409, value: { error: { code: "billing_conflict" } } });
    const policy = `/projects/${project}/wallets/USD/credit-policy`;
    expect(
      await f.request(
        policy,
        { external_ref: "limit-1", expected_version: "0", credit_limit_atoms: "100" },
        funder.token,
      ),
    ).toMatchObject({
      status: 200,
      value: { credit_policy_version: "1", credit_limit_atoms: "100" },
    });
    expect(
      (
        await f.request(
          policy,
          { external_ref: "limit-2", expected_version: "0", credit_limit_atoms: "200" },
          funder.token,
        )
      ).status,
    ).toBe(409);
    expect(
      await f.request(
        `/projects/${crypto.randomUUID()}/wallets/USD/credit-policy`,
        { external_ref: "missing", expected_version: "0", credit_limit_atoms: "1" },
        funder.token,
      ),
    ).toMatchObject({ status: 404, value: { error: { code: "not_found" } } });
    for (const malformed of [
      { external_ref: "bad", expected_version: "0", credit_limit_atoms: "-1" },
      { external_ref: "bad", expected_version: "0", credit_limit_atoms: 1 },
      { external_ref: "bad", expected_version: "0" },
    ])
      expect(await f.request(policy, malformed, funder.token)).toMatchObject({
        status: 400,
        value: { error: { code: "invalid_management_input" } },
      });
    const wallet = `/projects/${project}/wallets/USD`;
    expect(
      await f.request(wallet, { frozen: true, policy: "GIFT_FIRST" }, funder.token, "PATCH"),
    ).toMatchObject({ status: 200, value: { updated: true } });
    expect(
      (await f.request(`/projects/${project}/wallets`, undefined, reader.token)).value.data,
    ).toMatchObject([
      {
        policy: "GIFT_FIRST",
        manual_frozen: true,
        freeze_reason: "MANUAL",
        available_atoms: "1025",
        credit_limit_atoms: "100",
        available_credit_atoms: "100",
      },
    ]);
    await f.request(wallet, { frozen: false }, funder.token, "PATCH");
    expect(
      (await f.request(`/projects/${project}/wallets`, undefined, reader.token)).value.data,
    ).toMatchObject([{ manual_frozen: false, freeze_reason: null }]);
    expect(
      (
        await f.request(
          `/projects/${crypto.randomUUID()}/wallets/USD`,
          { frozen: true },
          funder.token,
          "PATCH",
        )
      ).status,
    ).toBe(404);
    const tariffs = (
      await f.request(`/tariffs?model=${encodeURIComponent(setup.model)}`, undefined, reader.token)
    ).value.data as { model: string; currency: string }[];
    expect(tariffs.map((row) => [row.model, row.currency])).toEqual([[setup.model, "USD"]]);
    expect(await audits(db, project)).toEqual([
      ["customer.credit", funder.actor],
      ["customer.credit", funder.actor],
    ]);
    expect(await audits(db, `${project}:USD`)).toEqual([
      ["customer.credit_policy", funder.actor],
      ["customer.wallet_update", funder.actor],
      ["customer.wallet_update", funder.actor],
    ]);
  }));

test("Core admin resolutions are replay-safe, reject changed identities and uncaptured prices, and schedule hold reconciliation", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { prepared } = await granted(db, setup, client),
      operator = await administrator(db, ["customer:adjust"]),
      path = `/requests/${prepared.requestId}/resolutions`,
      release = { external_ref: "write-off", action: "RELEASE", reason: "ADMIN_WRITE_OFF" };
    await db`UPDATE billing_holds SET next_reconcile_at=now()+interval '1 hour' WHERE request_id=${prepared.requestId}`;
    const first = await f.request(path, release, operator.token);
    expect(first).toMatchObject({ status: 201, value: { replayed: false } });
    expect((await f.request(path, release, operator.token)).value).toEqual({
      id: first.value.id,
      replayed: true,
    });
    expect(
      await f.request(path, { ...release, reason: "EVIDENCE_RECONCILIATION" }, operator.token),
    ).toMatchObject({ status: 409, value: { error: { code: "billing_conflict" } } });
    expect(
      await f.request(
        path,
        {
          external_ref: "uncaptured",
          action: "SETTLE",
          pricing_id: crypto.randomUUID(),
          reason: "PROVIDER_INVOICE",
        },
        operator.token,
      ),
    ).toMatchObject({ status: 422, value: { error: { code: "invalid_resolution" } } });
    expect(
      (await f.request(`/requests/${crypto.randomUUID()}/resolutions`, release, operator.token))
        .status,
    ).toBe(404);
    expect(
      (
        await db`SELECT next_reconcile_at<=now() AS due FROM billing_holds WHERE request_id=${prepared.requestId}`
      )[0].due,
    ).toBe(true);
    expect(await audits(db, prepared.requestId)).toEqual([
      ["customer.resolution", operator.actor],
      ["customer.resolution", operator.actor],
    ]);
  }));

test("customer funds endpoints require a platform credential with the matching permission and leave money and audit untouched when denied", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { charge } = await charged(db, setup, client),
      project = setup.project.id,
      reader = await administrator(db, ["customer:read"]),
      funder = await administrator(db, ["customer:fund"]),
      cataloger = await administrator(db, ["catalog:manage", "funding:recharge"]);
    const writes: [string, Record<string, unknown>, string?][] = [
      [
        `/projects/${project}/credits`,
        { currency: "USD", external_ref: "deny", amount_atoms: "1", kind: "CASH" },
      ],
      [
        `/projects/${project}/wallets/USD/credit-policy`,
        { external_ref: "deny", expected_version: "0", credit_limit_atoms: "1" },
      ],
      [`/projects/${project}/wallets/USD`, { frozen: true }, "PATCH"],
      [`/charges/${charge}/refunds`, { external_ref: "deny", amount_atoms: "1" }],
      [
        `/charges/${charge}/adjustments`,
        { external_ref: "deny", target_atoms: "1", reason: "ADMIN_CORRECTION" },
      ],
      [
        `/requests/${crypto.randomUUID()}/resolutions`,
        { external_ref: "deny", action: "RELEASE", reason: "ADMIN_WRITE_OFF" },
      ],
    ];
    for (const [path, body, method] of writes) {
      expect((await f.request(path, body, "not-a-credential", method)).status).toBe(401);
      expect(await f.request(path, body, reader.token, method)).toMatchObject({
        status: 403,
        value: { error: { code: "platform_scope_denied" } },
      });
      expect((await f.request(path, body, cataloger.token, method)).status).toBe(403);
    }
    for (const [path, body] of writes.slice(3))
      expect((await f.request(path, body, funder.token)).status).toBe(403);
    expect(
      (
        await f.request(
          "/tariffs",
          {
            model: setup.model,
            operation: "chat",
            pricing: { currency: "USD", rates: { requests: { numerator: "1", denominator: "1" } } },
            ceilings: { requests: "1" },
            basis: "REQUEST_COMPLETION",
          },
          reader.token,
        )
      ).status,
    ).toBe(403);
    for (const path of [
      `/projects/${project}/wallets`,
      `/projects/${project}/wallets/USD/lots`,
      `/projects/${project}/wallets/USD/journal`,
      "/tariffs",
    ])
      expect((await f.request(path, undefined, funder.token)).status).toBe(403);
    expect(
      await db`SELECT available_atoms::text,manual_frozen,credit_limit_atoms::text FROM wallets WHERE project_id=${project}`,
    ).toEqual([{ available_atoms: "900", manual_frozen: false, credit_limit_atoms: "0" }]);
    expect(
      await db`SELECT refunded_atoms::text,amount_atoms::text FROM billing_charges WHERE id=${charge}`,
    ).toEqual([{ refunded_atoms: "0", amount_atoms: "100" }]);
    expect(
      (
        await db`SELECT count(*)::integer n FROM audit_events WHERE action LIKE 'customer.%' AND actor<>'bootstrap:ADMIN_KEY'`
      )[0].n,
    ).toBe(0);
    const unknown = await f.request(
      `/charges/${crypto.randomUUID()}/refunds`,
      { external_ref: "missing", amount_atoms: "1" },
      (await administrator(db, ["customer:adjust"])).token,
    );
    expect(unknown).toMatchObject({ status: 404, value: { error: { code: "not_found" } } });
    expect(
      (await db`SELECT count(*)::integer n FROM audit_events WHERE action='customer.refund'`)[0].n,
    ).toBe(0);
  }));
