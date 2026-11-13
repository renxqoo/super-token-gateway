import { afterAll, beforeAll, expect, test } from "bun:test";
import { createDatabase, migrate, Store, FundsStore } from "@gateway/storage";
import type { SQL } from "bun";
import type { Route, Credential, UsageEvidence } from "@gateway/contracts";
import type { Usage } from "@gateway/kernel";
import { createUsageQueueCleanup } from "../support/usage-queue-cleanup.ts";
const database = `gateway_test_funds_${crypto.randomUUID().replaceAll("-", "")}`;
let owner: SQL;
let db: SQL;
let store: Store;
let funds: FundsStore;
let route: Route;
let pricingId: string;
async function project(currency = "USD") {
  const id = await store.createProject({
    name: crypto.randomUUID(),
    rpm: 100000,
    tpm: 100000000,
    concurrency: 1000,
  });
  await db`UPDATE projects SET billing_currency=${currency} WHERE id=${id}`;
  const key = await store.createCredential(
    id,
    { name: "test", models: [], operations: ["chat"] },
    crypto.randomUUID(),
    "test",
  );
  return {
    id,
    credential: { id: key, project_id: id, models: [], operations: ["chat"] } as Credential,
  };
}
async function credit(
  id: string,
  amount: string,
  kind: "CASH" | "GIFT" = "CASH",
  extra: Record<string, unknown> = {},
) {
  return funds.credit(id, {
    currency: "USD",
    external_ref: crypto.randomUUID(),
    amount_atoms: amount,
    kind,
    priority: 100,
    models: [],
    operations: [],
    ...extra,
  });
}
async function admit(
  credential: Credential,
  amount = "100",
  currency = "USD",
  version = pricingId,
) {
  const request = crypto.randomUUID();
  const quote = {
    currency,
    amount_atoms: amount,
    input_characters: "0",
    prices: { [route.provider_id]: version },
  };
  const payload = {
    id: request,
    key_id: credential.id,
    project_id: credential.project_id,
    operation: "chat",
    model: "funds-model",
    digest: null,
    fingerprint: "fixture",
    route_id: route.id,
    cost_pricing_id: route.pricing_id,
    quote,
  };
  let rows = await db`SELECT admit_gateway_batch(${[payload]}::jsonb)->'0' AS result`;
  const deadline = Date.now() + 2000;
  while (rows[0].result.error === "55P03" && Date.now() < deadline) {
    await Bun.sleep(5);
    rows = await db`SELECT admit_gateway_batch(${[payload]}::jsonb)->'0' AS result`;
  }
  if (rows[0].result.error) throw { errno: rows[0].result.error };
  return { request, attempt: String(rows[0].result.attempt_id) };
}
async function complete(
  request: string,
  attempt: string,
  usage: Usage = { input_tokens: "7", output_tokens: "5" },
) {
  await store.persistUsage([
    {
      source: "execution",
      attemptId: attempt,
      state: "SUCCEEDED",
      finality: "FINAL",
      usage,
      requestState: "SUCCEEDED",
    },
  ]);
  return db`SELECT (settle_requests(ARRAY[${request}::uuid])->${request}::text) AS result`;
}
async function invariant(id: string) {
  const rows =
    await db`SELECT w.available_atoms::text,w.reserved_atoms::text,w.spent_atoms::text,w.expired_atoms::text,w.debt_atoms::text,w.freeze_reason,coalesce(sum(l.available_atoms),0)::text AS lots_available,coalesce(sum(l.reserved_atoms),0)::text AS lots_reserved,coalesce(sum(l.spent_atoms),0)::text AS lots_spent,coalesce(sum(l.expired_atoms),0)::text AS lots_expired FROM wallets w LEFT JOIN wallet_lots l ON l.wallet_id=w.id WHERE w.project_id=${id} GROUP BY w.id`;
  for (const w of rows) {
    expect(w.available_atoms).toBe(w.lots_available);
    expect(w.reserved_atoms).toBe(w.lots_reserved);
    expect(w.spent_atoms).toBe(w.lots_spent);
    expect(w.expired_atoms).toBe(w.lots_expired);
  }
  const invalid =
    await db`SELECT j.id FROM money_journal j JOIN wallets w ON w.id=j.wallet_id WHERE w.project_id=${id} AND (SELECT sum((v->>'atoms')::numeric) FROM jsonb_array_elements(j.lines) v)<>0`;
  expect(invalid).toHaveLength(0);
  const divergent = await db`WITH entries AS (
    SELECT j.wallet_id,v->>'account' AS account,sum((v->>'atoms')::numeric) AS atoms
    FROM money_journal j JOIN wallets w ON w.id=j.wallet_id CROSS JOIN LATERAL jsonb_array_elements(j.lines) v
    WHERE w.project_id=${id} GROUP BY j.wallet_id,v->>'account'
  ) SELECT l.id FROM wallet_lots l JOIN wallets w ON w.id=l.wallet_id WHERE w.project_id=${id} AND (
    l.available_atoms<>coalesce((SELECT atoms FROM entries WHERE account='lot:'||l.id||':available'),0) OR
    l.reserved_atoms<>coalesce((SELECT atoms FROM entries WHERE account='lot:'||l.id||':reserved'),0) OR
    l.expired_atoms<>coalesce((SELECT atoms FROM entries WHERE account='lot:'||l.id||':expired'),0) OR
    l.issued_atoms<>coalesce((SELECT sum(amount_atoms) FROM funding_operations WHERE lot_id=l.id),0)
  )`;
  expect(divergent).toHaveLength(0);
  const unpaid = await db`SELECT w.id FROM wallets w WHERE w.project_id=${id} AND (
    w.debt_atoms<>coalesce((SELECT sum(c.debt_atoms) FROM billing_charges c JOIN billing_holds h ON h.id=c.hold_id WHERE h.wallet_id=w.id),0) OR
    w.spent_atoms<>coalesce((SELECT sum(c.funded_atoms-c.refunded_atoms+c.debt_refunded_atoms) FROM billing_charges c JOIN billing_holds h ON h.id=c.hold_id WHERE h.wallet_id=w.id),0) OR
    w.reserved_atoms<>coalesce((SELECT sum(h.amount_atoms-h.credit_atoms) FROM billing_holds h WHERE h.wallet_id=w.id AND h.state IN ('HELD','RECONCILING')),0) OR
    w.credit_reserved_atoms<>coalesce((SELECT sum(h.credit_atoms) FROM billing_holds h WHERE h.wallet_id=w.id AND h.state IN ('HELD','RECONCILING')),0)
  )`;
  expect(unpaid).toHaveLength(0);
  return rows[0];
}
test("expected admission rejections become item outcomes without re-evaluating the whole batch", async () => {
  const funded = await project();
  await credit(funded.id, "1000");
  const broke = await project();
  await credit(broke.id, "1");
  const frozen = await project();
  await credit(frozen.id, "1000");
  await funds.configure(frozen.id, "USD", { frozen: true });
  const input = (
    p: Awaited<ReturnType<typeof project>>,
    amount: string,
    digest: string | null,
  ) => ({
    id: crypto.randomUUID(),
    key_id: p.credential.id,
    project_id: p.id,
    operation: "chat",
    model: "funds-model",
    digest,
    fingerprint: "fixture",
    route_id: route.id,
    cost_pricing_id: route.pricing_id,
    quote: {
      currency: "USD",
      amount_atoms: amount,
      input_characters: "0",
      prices: { [route.provider_id]: pricingId },
    },
  });
  const brokeDigest = crypto.randomUUID();
  const inputs = [
    input(funded, "10", null),
    input(broke, "100", brokeDigest),
    input(frozen, "10", null),
    { ...input(funded, "10", null), key_id: crypto.randomUUID() },
    input(funded, "10", crypto.randomUUID()),
  ];
  const { result, calls } = await db.begin(async (tx) => {
    await tx`SET LOCAL track_functions='pl'`;
    const rows = await tx`SELECT admit_gateway_batch(${inputs}::jsonb) AS result`;
    const stats =
      await tx`SELECT calls FROM pg_stat_xact_user_functions WHERE funcname='admit_gateway_batch'`;
    return { result: rows[0].result, calls: Number(stats[0].calls) };
  });
  expect(result["0"]).toMatchObject({ created: true });
  expect(result["1"]).toEqual({ error: "P0402" });
  expect(result["2"]).toEqual({ error: "P0402" });
  expect(result["3"]).toEqual({ error: "P0401" });
  expect(result["4"]).toMatchObject({ created: true });
  expect(calls).toBe(1);
  expect((await db`SELECT 1 FROM gateway_requests WHERE intent_digest=${brokeDigest}`).length).toBe(
    0,
  );
  expect((await funds.wallets(funded.id))[0]).toMatchObject({
    available_atoms: "980",
    reserved_atoms: "20",
  });
  expect((await funds.wallets(broke.id))[0]).toMatchObject({
    available_atoms: "1",
    reserved_atoms: "0",
  });
});

test("holds that cannot progress without new evidence back off and do not delay debt collection", async () => {
  const waiting = await project();
  await credit(waiting.id, "1000");
  for (let i = 0; i < 40; i++) {
    const q = await admit(waiting.credential, "1");
    await store.persistUsage([
      {
        attemptId: q.attempt,
        source: "execution",
        state: "UNKNOWN",
        finality: "UNKNOWN",
        usage: {},
      },
    ]);
  }
  const [{ id: wallet }] = await db`SELECT id FROM wallets WHERE project_id=${waiting.id}`;
  await db`UPDATE billing_holds SET created_at=now()-interval '2 hours',next_reconcile_at=now()-interval '1 minute' WHERE wallet_id=${wallet}`;
  const indebted = await project();
  await credit(indebted.id, "1");
  const q = await admit(indebted.credential, "1");
  await complete(q.request, q.attempt);
  expect((await funds.wallets(indebted.id))[0]).toMatchObject({
    debt_atoms: "55",
    freeze_reason: "DEBT",
  });
  await credit(indebted.id, "100");
  await funds.reconcileKnownHolds(32);
  expect((await funds.wallets(indebted.id))[0]).toMatchObject({
    debt_atoms: "0",
    freeze_reason: null,
  });
  while ((await funds.reconcileKnownHolds(32)) > 0);
  const [due] =
    await db`SELECT count(*)::int AS n FROM billing_holds WHERE wallet_id=${wallet} AND next_reconcile_at<now()+interval '59 minutes'`;
  expect(due.n).toBe(0);
  await invariant(indebted.id);
});

test("a gift-funded release does not re-wake cash debt that it cannot collect", async () => {
  const p = await project();
  await credit(p.id, "1", "GIFT", { models: ["other"] });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "1000",
  });
  const indebted = await admit(p.credential, "60");
  await complete(indebted.request, indebted.attempt);
  expect((await funds.wallets(p.id))[0]).toMatchObject({ debt_atoms: "56", freeze_reason: null });
  await credit(p.id, "100", "GIFT");
  const gifted = await admit(p.credential, "60");
  await complete(gifted.request, gifted.attempt);
  const [hold] =
    await db`SELECT next_reconcile_at>now()+interval '59 minutes' AS deferred FROM billing_holds WHERE request_id=${indebted.request}`;
  expect(hold.deferred).toBe(true);
  expect((await funds.wallets(p.id))[0]).toMatchObject({ debt_atoms: "56" });
  await credit(p.id, "100");
  while ((await funds.reconcileKnownHolds(32)) > 0);
  expect((await funds.wallets(p.id))[0]).toMatchObject({ debt_atoms: "0" });
  await invariant(p.id);
});

test("concurrent requests reserve existing debt and in-flight credit before admitting more work", async () => {
  const p = await project();
  await credit(p.id, "100");
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "50",
  });
  const outcomes = await Promise.allSettled(
    Array.from({ length: 3 }, () => admit(p.credential, "60")),
  );
  const accepted = outcomes.filter((r) => r.status === "fulfilled").map((r) => r.value);
  expect(accepted).toHaveLength(2);
  expect(
    outcomes.flatMap((r) => (r.status === "rejected" ? [r.reason as { errno: string }] : [])),
  ).toEqual([{ errno: "P0402" }]);
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    available_atoms: "0",
    reserved_atoms: "100",
    credit_reserved_atoms: "20",
    available_credit_atoms: "30",
  });
  for (const q of accepted) await complete(q.request, q.attempt);
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    credit_reserved_atoms: "0",
    debt_atoms: "12",
    available_credit_atoms: "38",
    freeze_reason: null,
  });
  await invariant(p.id);
});

test("unknown credit holds remain committed until an explicit release and duplicate release is harmless", async () => {
  const p = await project();
  await credit(p.id, "1", "GIFT", { models: ["other"] });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "60",
  });
  const q = await admit(p.credential, "60");
  await store.persistUsage([
    { attemptId: q.attempt, source: "execution", state: "UNKNOWN", finality: "UNKNOWN", usage: {} },
  ]);
  await expect(admit(p.credential, "1")).rejects.toMatchObject({ errno: "P0402" });
  expect((await funds.wallets(p.id))[0]).toMatchObject({ credit_reserved_atoms: "60" });
  const resolution = {
    external_ref: "release",
    action: "RELEASE" as const,
    usage: {},
    reason: "ADMIN_WRITE_OFF" as const,
  };
  await funds.resolve(q.request, resolution);
  await db`SELECT settle_requests(ARRAY[${q.request}::uuid])`;
  await funds.resolve(q.request, resolution);
  await db`SELECT settle_requests(ARRAY[${q.request}::uuid])`;
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    reserved_atoms: "0",
    credit_reserved_atoms: "0",
    debt_atoms: "0",
    available_credit_atoms: "60",
  });
  await invariant(p.id);
});

test("credit settlement and refunds debit once, gifts do not repay old debt, and cash restores credit", async () => {
  const p = await project();
  await credit(p.id, "1", "GIFT", { models: ["other"] });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "100",
  });
  const q = await admit(p.credential, "60");
  const settled = await complete(q.request, q.attempt);
  await complete(q.request, q.attempt);
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    debt_atoms: "56",
    credit_reserved_atoms: "0",
  });
  await credit(p.id, "100", "GIFT");
  await db`SELECT settle_requests(ARRAY[${q.request}::uuid])`;
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    debt_atoms: "56",
    available_credit_atoms: "44",
  });
  const refund = { external_ref: "refund", amount_atoms: "16", reason: "CUSTOMER_REFUND" as const };
  await funds.refund(String(settled[0].result.charge_id), refund);
  await funds.refund(String(settled[0].result.charge_id), refund);
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    debt_atoms: "40",
    available_credit_atoms: "60",
  });
  await credit(p.id, "50");
  await db`SELECT settle_requests(ARRAY[${q.request}::uuid])`;
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    debt_atoms: "0",
    available_atoms: "111",
    available_credit_atoms: "100",
  });
  await invariant(p.id);
});

test("actual overrun keeps the full receivable and freezes new promises beyond the credit limit", async () => {
  const p = await project();
  await credit(p.id, "1");
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "50",
  });
  const q = await admit(p.credential, "50");
  await complete(q.request, q.attempt, { input_tokens: "50", output_tokens: "50" });
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    available_atoms: "0",
    credit_reserved_atoms: "0",
    debt_atoms: "499",
    credit_excess_atoms: "449",
    available_credit_atoms: "0",
    freeze_reason: "DEBT",
  });
  await expect(admit(p.credential, "0")).rejects.toMatchObject({ errno: "P0402" });
  await credit(p.id, "449");
  await db`SELECT settle_requests(ARRAY[${q.request}::uuid])`;
  expect((await funds.wallets(p.id))[0]).toMatchObject({ debt_atoms: "50", freeze_reason: null });
  await admit(p.credential, "0");
  await invariant(p.id);
});

test("credit policy retries preserve their original result without overwriting a newer policy", async () => {
  const p = await project();
  await credit(p.id, "1");
  const input = { external_ref: "first", expected_version: "0", credit_limit_atoms: "100" };
  const results = await Promise.all(
    Array.from({ length: 5 }, () => funds.configureCredit(p.id, "USD", input)),
  );
  expect(new Set(results.map((r) => r.id)).size).toBe(1);
  await funds.configureCredit(p.id, "USD", {
    ...input,
    external_ref: "second",
    expected_version: "1",
    credit_limit_atoms: "200",
  });
  expect(await funds.configureCredit(p.id, "USD", input)).toMatchObject({
    credit_policy_version: "1",
    replayed: true,
  });
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    credit_limit_atoms: "200",
    credit_policy_version: "2",
  });
  await expect(
    funds.configureCredit(p.id, "USD", { ...input, credit_limit_atoms: "101" }),
  ).rejects.toMatchObject({ status: 409 });
  await expect(
    funds.configureCredit(p.id, "USD", { ...input, external_ref: "stale" }),
  ).rejects.toMatchObject({ status: 409 });
  const q = await admit(p.credential, "60");
  await expect(
    funds.configureCredit(p.id, "USD", {
      external_ref: "lower",
      expected_version: "2",
      credit_limit_atoms: "58",
    }),
  ).rejects.toMatchObject({ status: 409 });
  await funds.configure(p.id, "USD", { frozen: true });
  await expect(admit(p.credential, "0")).rejects.toMatchObject({ errno: "P0402" });
  await complete(q.request, q.attempt);
  expect((await funds.wallets(p.id))[0]).toMatchObject({ freeze_reason: "MANUAL" });
  await invariant(p.id);
});

test("Responses completion racing initial evidence cannot leave final usage and credit held forever", async () => {
  const p = await project();
  await credit(p.id, "1", "GIFT", { models: ["other"] });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "credit",
    expected_version: "0",
    credit_limit_atoms: "100",
  });
  const q = await admit(p.credential, "100");
  const resource = await store.saveResource(
    p.id,
    route.provider_id,
    `response_${crypto.randomUUID()}`,
    "response",
    "funds-model",
    q.attempt,
  );
  await store.updateResource(resource, "PENDING_USAGE", "in_progress");
  let initial: Promise<unknown> | undefined;
  try {
    await db.begin(async (tx) => {
      await tx`SELECT id FROM gateway_requests WHERE id=${q.request} FOR NO KEY UPDATE`;
      initial = store.persistUsage([
        {
          attemptId: q.attempt,
          source: "execution",
          state: "SUCCEEDED",
          finality: "FINAL",
          usage: { requests: "1" },
          requestState: "SUCCEEDED",
        },
      ]);
      const end = Date.now() + 1500;
      let blocked = false;
      while (Date.now() < end) {
        blocked =
          (
            await db`SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%persist_usage_batch%'`
          ).length > 0;
        if (blocked) break;
        await Bun.sleep(5);
      }
      expect(blocked).toBe(true);
      await store.updateResource(resource, "COMPLETED", "completed");
    });
  } finally {
    await initial;
  }
  await store.persistUsage([
    {
      attemptId: q.attempt,
      source: "async_final",
      state: "SUCCEEDED",
      finality: "FINAL",
      usage: { requests: "1", input_tokens: "7", output_tokens: "5" },
    },
  ]);
  expect(await funds.receipt(q.request, p.id)).toMatchObject({
    hold_state: "SETTLED",
    charged_atoms: "56",
    debt_atoms: "56",
  });
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    credit_reserved_atoms: "0",
    debt_atoms: "56",
  });
  await invariant(p.id);
});

test("an async resource completing after backed-off evidence wakes its hold without waiting an hour", async () => {
  const p = await project();
  await credit(p.id, "100");
  const q = await admit(p.credential, "100");
  const resource = await store.saveResource(
    p.id,
    route.provider_id,
    `response_${crypto.randomUUID()}`,
    "response",
    "funds-model",
    q.attempt,
  );
  await store.updateResource(resource, "PENDING_USAGE", "in_progress");
  await db`UPDATE billing_holds SET created_at=now()-interval '2 hours' WHERE request_id=${q.request}`;
  await store.persistUsage([
    {
      attemptId: q.attempt,
      source: "execution",
      state: "SUCCEEDED",
      finality: "FINAL",
      usage: { requests: "1", input_tokens: "7", output_tokens: "5" },
      requestState: "SUCCEEDED",
    },
  ]);
  const [waiting] =
    await db`SELECT h.state,q.billing_outcome,h.next_reconcile_at>now()+interval '59 minutes' AS deferred FROM billing_holds h JOIN gateway_requests q ON q.id=h.request_id WHERE h.request_id=${q.request}`;
  expect(waiting).toMatchObject({
    state: "RECONCILING",
    billing_outcome: "PENDING",
    deferred: true,
  });
  await store.updateResource(resource, "COMPLETED", "completed");
  while ((await funds.reconcileKnownHolds(32)) > 0);
  expect(await funds.receipt(q.request, p.id)).toMatchObject({
    billing_outcome: "SUCCESS",
    hold_state: "SETTLED",
    charged_atoms: "56",
  });
  await invariant(p.id);
});

test("credit atoms above IEEE754 remain exact and cannot fund another currency", async () => {
  const p = await project();
  await credit(p.id, "1", "GIFT", { models: ["other"] });
  await funds.credit(p.id, {
    currency: "EUR",
    external_ref: "eur",
    amount_atoms: "1",
    kind: "CASH",
    priority: 100,
    models: [],
    operations: [],
  });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "exact",
    expected_version: "0",
    credit_limit_atoms: "9007199254740993",
  });
  const q = await admit(p.credential, "9007199254740993");
  await expect(admit(p.credential, "1")).rejects.toMatchObject({ errno: "P0402" });
  expect(
    (await funds.wallets(p.id)).find((w) => (w as { currency: string }).currency === "USD"),
  ).toMatchObject({ credit_reserved_atoms: "9007199254740993", available_credit_atoms: "0" });
  await complete(q.request, q.attempt);
  expect(
    (await funds.wallets(p.id)).find((w) => (w as { currency: string }).currency === "USD"),
  ).toMatchObject({
    credit_reserved_atoms: "0",
    debt_atoms: "56",
    available_credit_atoms: "9007199254740937",
  });
  expect(
    (await funds.wallets(p.id)).find((w) => (w as { currency: string }).currency === "EUR"),
  ).toMatchObject({ available_atoms: "1", credit_limit_atoms: "0", debt_atoms: "0" });
  await invariant(p.id);
});

test("concurrent credit reduction and admission cannot commit promises beyond the resulting limit", async () => {
  const p = await project();
  await credit(p.id, "1", "GIFT", { models: ["other"] });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "open",
    expected_version: "0",
    credit_limit_atoms: "1000",
  });
  const reducing = funds.configureCredit(p.id, "USD", {
    external_ref: "reduce",
    expected_version: "1",
    credit_limit_atoms: "100",
  });
  const [reduced, admitted] = await Promise.all([
    reducing.then(
      () => true,
      () => false,
    ),
    Promise.allSettled(Array.from({ length: 10 }, () => admit(p.credential, "100"))),
  ]);
  const accepted = admitted.filter((result) => result.status === "fulfilled").length;
  expect(accepted).toBeGreaterThanOrEqual(1);
  if (reduced) expect(accepted).toBe(1);
  else expect(accepted).toBeGreaterThan(1);
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    credit_limit_atoms: reduced ? "100" : "1000",
    credit_reserved_atoms: String(accepted * 100),
    available_credit_atoms: String((reduced ? 100 : 1000) - accepted * 100),
  });
  await invariant(p.id);
});

test("charge adjustments freeze and reopen the same credit boundary without turning credit into cash", async () => {
  const p = await project();
  await credit(p.id, "1", "GIFT", { models: ["other"] });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "100",
  });
  const q = await admit(p.credential, "60");
  const result = await complete(q.request, q.attempt);
  const charge = String(result[0].result.charge_id);
  await funds.adjust(charge, {
    external_ref: "up",
    target_atoms: "150",
    reason: "ADMIN_CORRECTION",
  });
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    debt_atoms: "150",
    freeze_reason: "DEBT",
    credit_excess_atoms: "50",
  });
  await funds.adjust(charge, {
    external_ref: "down",
    target_atoms: "50",
    reason: "ADMIN_CORRECTION",
  });
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    available_atoms: "1",
    debt_atoms: "50",
    freeze_reason: null,
    available_credit_atoms: "50",
  });
  await invariant(p.id);
});
test("admission and settlement avoid randomly scattered new index keys for an existing request UUID", async () => {
  const tenant = await project();
  await credit(tenant.id, "1000");
  const { request, attempt } = await admit(tenant.credential);
  await complete(request, attempt);
  const ids = await db<
    { charge_id: string; hold_id: string; journal_id: string; kind: string }[]
  >`SELECT c.id AS charge_id,h.id AS hold_id,j.id AS journal_id,j.kind
    FROM billing_charges c JOIN billing_holds h ON h.id=c.hold_id
    JOIN money_journal j ON j.wallet_id=h.wallet_id AND j.reference IN ('hold:'||h.request_id,'settle:'||h.request_id)
    WHERE c.request_id=${request} ORDER BY j.kind`;
  expect(ids.map((row) => row.kind)).toEqual(["HOLD", "SETTLE"]);
  const timeOrdered = /^[a-f0-9]{8}-[a-f0-9]{4}-7[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
  expect(attempt).toMatch(timeOrdered);
  for (const row of ids) {
    expect(row.hold_id).toBe(request);
    expect(row.charge_id).toMatch(timeOrdered);
    expect(row.journal_id).toMatch(timeOrdered);
  }
  expect(await invariant(tenant.id)).toMatchObject({ reserved_atoms: "0" });
});

test("a concurrent quote burst must not starve independent funding behind a locked tariff catalog", async () => {
  const tenant = await project();
  await credit(tenant.id, "1000");
  const url = new URL(process.env.DATABASE_URL!);
  url.pathname = `/${database}`;
  const constrained = createDatabase(url.href, 2);
  const isolated = new FundsStore(constrained);
  const acquired = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const lock = db.begin(async (tx) => {
    await tx`LOCK TABLE billing_tariffs IN ACCESS EXCLUSIVE MODE`;
    acquired.resolve();
    await release.promise;
  });
  await acquired.promise;
  const quotes = Array.from({ length: 32 }, () =>
    isolated.quote("funds-model", "chat", "USD", [route.provider_id], { inputCharacters: "1" }, 1),
  );
  let creditFinished = false;
  let credited: Promise<unknown> | undefined;
  try {
    const deadline = Date.now() + 1000;
    let waiting = false;
    while (!waiting && Date.now() < deadline) {
      waiting = (
        await db`SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%DISTINCT ON(provider.id)%') AS waiting`
      )[0].waiting;
      if (!waiting) await Bun.sleep(5);
    }
    expect(waiting).toBe(true);
    credited = isolated
      .credit(tenant.id, {
        currency: "USD",
        external_ref: "during-quote-load",
        amount_atoms: "1",
        kind: "CASH",
        priority: 100,
        models: [],
        operations: [],
      })
      .then(() => {
        creditFinished = true;
      });
    await Promise.race([credited, Bun.sleep(300)]);
    expect(creditFinished).toBe(true);
  } finally {
    release.resolve();
    await lock;
    await Promise.allSettled([...quotes, ...(credited ? [credited] : [])]);
    await constrained.close();
  }
  const captured = await Promise.all(quotes);
  expect(captured).toHaveLength(32);
  expect(captured.every((quote) => quote.prices[route.provider_id] === pricingId)).toBe(true);
  expect(await invariant(tenant.id)).toMatchObject({
    available_atoms: "1001",
    reserved_atoms: "0",
  });
});

test("covered settlement and an over-hold peer on the same wallet preserve exact independent charges", async () => {
  const mixed = await project();
  const peer = await project();
  await credit(mixed.id, "1000");
  await credit(peer.id, "1000");
  const covered = await admit(mixed.credential);
  const over = await admit(mixed.credential);
  const independent = await admit(peer.credential);
  const events: UsageEvidence[] = [covered, over, independent].map((record, index) => ({
    attemptId: record.attempt,
    source: "execution",
    state: "SUCCEEDED",
    finality: "FINAL",
    requestState: "SUCCEEDED",
    usage:
      index === 1
        ? { input_tokens: "14", output_tokens: "10" }
        : { input_tokens: "7", output_tokens: "5" },
  }));
  expect(await store.persistUsage(events)).toEqual({ SETTLED: 3 });
  expect((await funds.receipt(covered.request, mixed.id)).charged_atoms).toBe("56");
  expect((await funds.receipt(over.request, mixed.id)).charged_atoms).toBe("112");
  expect((await funds.receipt(independent.request, peer.id)).charged_atoms).toBe("56");
  expect(await store.persistUsage(events)).toEqual({ REPLAYED: 3 });
  expect(await invariant(mixed.id)).toMatchObject({
    spent_atoms: "168",
    available_atoms: "832",
    reserved_atoms: "0",
  });
  expect(await invariant(peer.id)).toMatchObject({
    spent_atoms: "56",
    available_atoms: "944",
    reserved_atoms: "0",
  });
});

test("a 256-event transaction preserves every receipt and repeated atomic job at the batch boundary", async () => {
  const tenant = await project();
  await credit(tenant.id, "30000");
  const events: UsageEvidence[] = [];
  for (let index = 0; index < 256; index++) {
    const record = await admit(tenant.credential);
    events.push({
      attemptId: record.attempt,
      source: "execution",
      state: "SUCCEEDED",
      finality: "FINAL",
      requestState: "SUCCEEDED",
      usage: { input_tokens: "7", output_tokens: "5" },
    });
  }
  const rows = await db<
    {
      result: Record<string, { status: string; billing: { state: string; charged_atoms: string } }>;
    }[]
  >`SELECT persist_usage_batch(${events}::jsonb) AS result`;
  expect(Object.keys(rows[0]!.result)).toHaveLength(256);
  for (const result of Object.values(rows[0]!.result))
    expect(result).toMatchObject({
      status: "APPLIED",
      billing: { state: "SETTLED", charged_atoms: "56" },
    });
  expect(
    await Promise.all([
      store.persistUsage(events.slice(0, 128)),
      store.persistUsage(events.slice(128)),
    ]),
  ).toEqual([{ REPLAYED: 128 }, { REPLAYED: 128 }]);
  expect(await invariant(tenant.id)).toMatchObject({
    available_atoms: "15664",
    spent_atoms: "14336",
    reserved_atoms: "0",
  });
});

test("a locked wallet does not block admission or evidence for another wallet", async () => {
  const hot = await project();
  const cold = await project();
  await credit(hot.id, "1000");
  await credit(cold.id, "1000");
  const quote = {
    currency: "USD",
    amount_atoms: "100",
    input_characters: "0",
    prices: { [route.provider_id]: pricingId },
  };
  const unlocked = Promise.withResolvers<void>();
  const locked = Promise.withResolvers<void>();
  const lock = db.begin(async (tx) => {
    await tx`SELECT id FROM wallets WHERE project_id=${hot.id} FOR UPDATE`;
    locked.resolve();
    await unlocked.promise;
  });
  await locked.promise;
  const hotAdmission = store.admitInitial(
    crypto.randomUUID(),
    hot.credential,
    "chat",
    "funds-model",
    null,
    "hot",
    route,
    quote,
  );
  const coldAdmission = store.admitInitial(
    crypto.randomUUID(),
    cold.credential,
    "chat",
    "funds-model",
    null,
    "cold",
    route,
    quote,
  );
  let coldCompleted = false;
  void coldAdmission.then(
    () => {
      coldCompleted = true;
    },
    () => {},
  );
  try {
    await Promise.race([coldAdmission, Bun.sleep(300)]);
    expect(coldCompleted).toBe(true);
  } finally {
    unlocked.resolve();
    await lock;
    await Promise.allSettled([hotAdmission, coldAdmission]);
  }
  expect((await invariant(hot.id)).reserved_atoms).toBe("100");
  expect((await invariant(cold.id)).reserved_atoms).toBe("100");
  const admitted = await Promise.all([hotAdmission, coldAdmission]);
  const unlockedAgain = Promise.withResolvers<void>(),
    lockedAgain = Promise.withResolvers<void>();
  const secondLock = db.begin(async (tx) => {
    await tx`SELECT id FROM wallets WHERE project_id=${hot.id} FOR UPDATE`;
    lockedAgain.resolve();
    await unlockedAgain.promise;
  });
  await lockedAgain.promise;
  const ingestion = store.persistUsage(
    admitted.map((record) => ({
      attemptId: record.attempt_id!,
      source: "execution",
      state: "SUCCEEDED" as const,
      finality: "FINAL" as const,
      usage: { input_tokens: "7", output_tokens: "5" },
      requestState: "SUCCEEDED" as const,
    })),
  );
  try {
    const end = Date.now() + 1000;
    let settled = false;
    while (Date.now() < end) {
      settled =
        (await db`SELECT state FROM billing_holds WHERE request_id=${admitted[1]!.record.id}`)[0]
          ?.state === "SETTLED";
      if (settled) break;
      await Bun.sleep(5);
    }
    expect(settled).toBe(true);
  } finally {
    unlockedAgain.resolve();
    await secondLock;
    await ingestion;
  }
  expect((await invariant(hot.id)).reserved_atoms).toBe("0");
  expect((await invariant(cold.id)).reserved_atoms).toBe("0");
});

beforeAll(async () => {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL required");
  owner = createDatabase(base);
  await owner.unsafe(`CREATE DATABASE ${database}`);
  const url = new URL(base);
  url.pathname = `/${database}`;
  db = createDatabase(url.toString());
  await migrate(db);
  store = new Store(db);
  funds = new FundsStore(db);
  const provider = crypto.randomUUID();
  await store.createProvider(
    provider,
    {
      name: "funds",
      kind: "openai",
      base_url: "http://fixture.invalid/v1",
      rpm: 100000,
      concurrency: 1000,
    },
    "encrypted-fixture",
  );
  await store.createModel({ name: "funds-model", operations: ["chat"], max_output_tokens: 100 });
  await store.createRoute("funds-model", {
    provider_id: provider,
    upstream_model: "mock",
    operations: ["chat"],
    priority: 1,
    weight: 1,
    pricing: {
      currency: "USD",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "1", denominator: "1" },
      },
    },
    ceilings: { input_tokens: "16777216", output_tokens: "1000000" },
  });
  route = (await store.routes("funds-model", "chat"))[0]!;
  const tariff = await funds.setTariff({
    model: "funds-model",
    operation: "chat",
    pricing: {
      currency: "USD",
      rates: {
        input_tokens: { numerator: "3", denominator: "1" },
        output_tokens: { numerator: "7", denominator: "1" },
      },
    },
    ceilings: { input_tokens: "100", output_tokens: "100" },
    basis: "PROVIDER_USAGE",
  });
  pricingId = tariff.pricing_id;
});
afterAll(async () => {
  await db?.close();
  if (owner) {
    await owner.unsafe(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
    await owner.close();
  }
});

test("foreign-key readers do not make a funded wallet appear busy", async () => {
  const p = await project();
  await credit(p.id, "100");
  const result = await db.begin(async (tx) => {
    await tx`SELECT id FROM wallets WHERE project_id=${p.id} FOR KEY SHARE`;
    return admit(p.credential, "100");
  });
  await complete(result.request, result.attempt);
  const wallet = await invariant(p.id);
  expect(wallet.available_atoms).toBe("44");
  expect(wallet.reserved_atoms).toBe("0");
  expect(wallet.spent_atoms).toBe("56");
});

test("credit is concurrent-idempotent and currency isolated", async () => {
  const p = await project();
  const input = {
    currency: "USD",
    external_ref: "invoice",
    amount_atoms: "1000",
    kind: "CASH" as const,
    priority: 100,
    models: [],
    operations: [],
  };
  const result = await Promise.all(Array.from({ length: 8 }, () => funds.credit(p.id, input)));
  expect(new Set(result.map((r) => r.id)).size).toBe(1);
  await expect(funds.credit(p.id, { ...input, amount_atoms: "1001" })).rejects.toMatchObject({
    status: 409,
  });
  await funds.credit(p.id, { ...input, currency: "EUR", amount_atoms: "2000" });
  expect((await funds.wallets(p.id)).length).toBe(2);
  await invariant(p.id);
  const wallets = (await funds.wallets(p.id)) as { currency: string; available_atoms: string }[];
  expect(wallets.find((w) => w.currency === "EUR")?.available_atoms).toBe("2000");
});
test("gift-first reservation and duplicate settlement charge exactly 56 atoms", async () => {
  const p = await project();
  await credit(p.id, "1000");
  await credit(p.id, "60", "GIFT");
  const q = await admit(p.credential);
  const held = await invariant(p.id);
  expect(held.reserved_atoms).toBe("100");
  expect(held.available_atoms).toBe("960");
  const a = await complete(q.request, q.attempt);
  expect(a[0].result.state).toBe("SETTLED");
  const replays = await Promise.all(
    Array.from(
      { length: 8 },
      () => db`SELECT (settle_requests(ARRAY[${q.request}::uuid])->${q.request}::text) AS result`,
    ),
  );
  expect(replays.every((r) => r[0].result.replayed)).toBe(true);
  const w = await invariant(p.id);
  expect(w.available_atoms).toBe("1004");
  expect(w.spent_atoms).toBe("56");
  expect(w.reserved_atoms).toBe("0");
  const lots =
    await db`SELECT kind,spent_atoms::text FROM wallet_lots WHERE wallet_id=(SELECT id FROM wallets WHERE project_id=${p.id}) ORDER BY kind`;
  expect(
    lots.find((l: { kind: string; spent_atoms: string }) => l.kind === "GIFT").spent_atoms,
  ).toBe("56");
});
test("concurrent holds never overspend and failures roll back request creation", async () => {
  const p = await project();
  await credit(p.id, "1000");
  const all = await Promise.allSettled(
    Array.from({ length: 20 }, () => admit(p.credential, "100")),
  );
  expect(all.filter((r) => r.status === "fulfilled")).toHaveLength(10);
  const w = await invariant(p.id);
  expect(w.available_atoms).toBe("0");
  expect(w.reserved_atoms).toBe("1000");
  const count = await db`SELECT count(*)::int AS n FROM gateway_requests WHERE project_id=${p.id}`;
  expect(count[0].n).toBe(10);
});
test("missing final usage stays held until explicit reconciliation", async () => {
  const p = await project();
  await credit(p.id, "200");
  const q = await admit(p.credential);
  await store.persistUsage([
    {
      source: "execution",
      attemptId: q.attempt,
      state: "SUCCEEDED",
      finality: "UNKNOWN",
      usage: {},
      requestState: "SUCCEEDED",
    },
  ]);
  const result =
    await db`SELECT (settle_requests(ARRAY[${q.request}::uuid])->${q.request}::text) AS result`;
  expect(result[0].result.state).toBe("WAITING");
  expect((await invariant(p.id)).reserved_atoms).toBe("100");
  await funds.resolve(q.request, {
    external_ref: "invoice",
    action: "SETTLE",
    usage: { input_tokens: "7", output_tokens: "5" },
    pricing_id: pricingId,
    reason: "PROVIDER_INVOICE",
  });
  const resolved =
    await db`SELECT (settle_requests(ARRAY[${q.request}::uuid])->${q.request}::text) AS result`;
  expect(resolved[0].result.charged_atoms).toBe("56");
  await invariant(p.id);
});

test("Batch cancellation settles once even when final rows precede the creation evidence", async () => {
  const p = await project();
  await credit(p.id, "100");
  const perRequest = await funds.setTariff({
    model: "funds-model",
    operation: "chat",
    provider_id: route.provider_id,
    pricing: { currency: "USD", rates: { requests: { numerator: "7", denominator: "1" } } },
    ceilings: { requests: "1" },
    basis: "REQUEST_COMPLETION",
  });
  for (const finalFirst of [false, true]) {
    const { request, attempt } = await admit(p.credential, "7", "USD", perRequest.pricing_id);
    const resource = await store.saveResource(
      p.id,
      route.provider_id,
      `batch-${request}`,
      "batch",
      "funds-model",
      attempt,
    );
    const creation: UsageEvidence = {
      source: "execution",
      attemptId: attempt,
      state: "SUCCEEDED",
      finality: "FINAL",
      usage: { requests: "1" },
      requestState: "SUCCEEDED",
    };
    const final: UsageEvidence = {
      source: "batch_cancelled",
      attemptId: attempt,
      state: "FAILED",
      finality: "FINAL",
      usage: { requests: "1", input_tokens: "7", output_tokens: "5" },
    };
    await store.updateResource(resource, "PENDING_USAGE", "cancelled");
    for (const evidence of finalFirst ? [final, creation] : [creation, final])
      await store.persistUsage([evidence]);
    expect(await funds.receipt(request, p.id)).toMatchObject({
      billing_outcome: "CANCELLED",
      hold_state: "RELEASED",
      charged_atoms: "0",
    });
    expect((await invariant(p.id)).available_atoms).toBe("100");
  }
});
test("expired reserved gift is consumed but its unused hold and refund cannot revive", async () => {
  const p = await project();
  await credit(p.id, "100", "GIFT", { expires_at: new Date(Date.now() + 1000).toISOString() });
  const q = await admit(p.credential);
  await Bun.sleep(1100);
  const result = await complete(q.request, q.attempt);
  expect(result[0].result.charged_atoms).toBe("56");
  let w = await invariant(p.id);
  expect(w.expired_atoms).toBe("44");
  expect(w.available_atoms).toBe("0");
  await funds.refund(String(result[0].result.charge_id), {
    external_ref: "expired-refund",
    amount_atoms: "56",
    reason: "CUSTOMER_REFUND",
  });
  w = await invariant(p.id);
  expect(w.expired_atoms).toBe("100");
  expect(w.spent_atoms).toBe("0");
  expect(w.available_atoms).toBe("0");
});
test("over-hold usage records debt, freezes admission, then cash topup pays debt", async () => {
  const p = await project();
  await credit(p.id, "100");
  const q = await admit(p.credential, "50");
  const result = await complete(q.request, q.attempt, { input_tokens: "50", output_tokens: "50" });
  expect(result[0].result.charged_atoms).toBe("500");
  let w = await invariant(p.id);
  expect(w.debt_atoms).toBe("400");
  expect(w.freeze_reason).toBe("DEBT");
  await expect(admit(p.credential, "0")).rejects.toMatchObject({ errno: "P0402" });
  await credit(p.id, "450");
  await db`SELECT (settle_requests(ARRAY[${q.request}::uuid])->${q.request}::text)`;
  w = await invariant(p.id);
  expect(w.debt_atoms).toBe("0");
  expect(w.available_atoms).toBe("50");
  expect(w.freeze_reason).toBeNull();
});
test("refund and adjustment are idempotent and preserve lot sums", async () => {
  const p = await project();
  await credit(p.id, "1000");
  const q = await admit(p.credential);
  const result = await complete(q.request, q.attempt);
  const charge = String(result[0].result.charge_id);
  const refund = { external_ref: "refund", amount_atoms: "6", reason: "CUSTOMER_REFUND" as const };
  await funds.refund(charge, refund);
  await funds.refund(charge, refund);
  expect((await invariant(p.id)).spent_atoms).toBe("50");
  const adjustment = {
    external_ref: "adjust",
    target_atoms: "70",
    reason: "ADMIN_CORRECTION" as const,
  };
  await funds.adjust(charge, adjustment);
  await funds.adjust(charge, adjustment);
  expect((await invariant(p.id)).spent_atoms).toBe("70");
  await funds.adjust(charge, { ...adjustment, external_ref: "down", target_atoms: "10" });
  expect((await invariant(p.id)).spent_atoms).toBe("10");
});
test("gift scopes and configured cash-first priority govern reservation", async () => {
  const p = await project();
  await credit(p.id, "100", "GIFT", { models: ["another-model"] });
  await expect(admit(p.credential, "1")).rejects.toMatchObject({ errno: "P0402" });
  await credit(p.id, "100");
  await credit(p.id, "100", "GIFT");
  await funds.configure(p.id, "USD", { policy: "CASH_FIRST" });
  const q = await admit(p.credential, "100");
  await complete(q.request, q.attempt);
  const lots =
    await db`SELECT kind,models,spent_atoms::text FROM wallet_lots WHERE wallet_id=(SELECT id FROM wallets WHERE project_id=${p.id})`;
  expect(
    lots.find((l: { kind: string; spent_atoms: string }) => l.kind === "CASH").spent_atoms,
  ).toBe("56");
  await invariant(p.id);
});
test("SQL price rounds the exact sum once including sub-atom fractions", async () => {
  const rows =
    await db`SELECT exact_billing_price('{"input_tokens":"1","output_tokens":"1"}', '{"input_tokens":{"numerator":"1","denominator":"3"},"output_tokens":{"numerator":"1","denominator":"3"}}')::text AS atoms`;
  expect(rows[0].atoms).toBe("1");
});

test("mixed cache TTL charges remain exact and missing TTL evidence retains the hold until enrichment", async () => {
  const p = await project();
  await credit(p.id, "1000");
  const rates = {
    cache_write_5m_tokens: { numerator: "5", denominator: "4" },
    cache_write_1h_tokens: { numerator: "2", denominator: "1" },
  };
  const tariff = await funds.setTariff({
    model: "funds-model",
    operation: "chat",
    pricing: { currency: "USD", rates },
    ceilings: { cache_write_5m_tokens: "100", cache_write_1h_tokens: "100" },
    basis: "PROVIDER_USAGE",
  });
  const q = await admit(p.credential, "325", "USD", tariff.pricing_id);
  await complete(q.request, q.attempt, { cache_write_tokens: "200" });
  expect((await funds.receipt(q.request, p.id)).charged_atoms).toBeNull();
  expect((await invariant(p.id)).reserved_atoms).toBe("325");
  await store.persistUsage([
    {
      attemptId: q.attempt,
      source: "provider_invoice",
      state: "SUCCEEDED",
      finality: "FINAL",
      usage: {
        cache_write_tokens: "200",
        cache_write_5m_tokens: "100",
        cache_write_1h_tokens: "100",
      },
    },
  ]);
  expect((await funds.receipt(q.request, p.id)).charged_atoms).toBe("325");
  expect((await invariant(p.id)).spent_atoms).toBe("325");
  expect(await store.tokenSummary(q.request)).toEqual({ complete: false, tokens: 200 });
  const [cost] =
    await db`SELECT exact_billing_price(${{ cache_write_5m_tokens: "100", cache_write_1h_tokens: "100" }}::jsonb,${rates}::jsonb)::text AS atoms`;
  expect(cost.atoms).toBe("325");
  const replay = {
    attemptId: q.attempt,
    source: "provider_invoice",
    state: "SUCCEEDED",
    finality: "FINAL",
    usage: {
      cache_write_tokens: "200",
      cache_write_5m_tokens: "100",
      cache_write_1h_tokens: "100",
    },
  } as const;
  await store.persistUsage([replay]);
  expect((await invariant(p.id)).spent_atoms).toBe("325");
});

test("SQL rejects aggregate plus TTL rates and inconsistent cache write partitions", async () => {
  const rates = {
    cache_write_tokens: { numerator: "1", denominator: "1" },
    cache_write_1h_tokens: { numerator: "2", denominator: "1" },
  };
  await expect(
    db`SELECT exact_billing_price('{"cache_write_tokens":"10","cache_write_1h_tokens":"10"}',${rates}::jsonb)`,
  ).rejects.toThrow("duplicate cache write rate");
  await expect(
    db`INSERT INTO billing_prices(id,currency,rates,ceilings,basis) VALUES(${crypto.randomUUID()},'USD',${rates}::jsonb,'{"cache_write_tokens":"10","cache_write_1h_tokens":"10"}','PROVIDER_USAGE')`,
  ).rejects.toThrow("cache write");
  const p = await project();
  await credit(p.id, "100");
  const q = await admit(p.credential);
  await expect(
    db`SELECT record_usage_evidence(${q.attempt}::uuid,${crypto.randomUUID()}::uuid,'execution','SUCCEEDED','FINAL','{"cache_write_tokens":"3","cache_write_5m_tokens":"1","cache_write_1h_tokens":"1"}',0,true,NULL,NULL)`,
  ).rejects.toThrow("cache write partitions");
});

test("refund waiting across gift expiry cannot resurrect credit or block later funding", async () => {
  const p = await project();
  const expires = Date.now() + 900;
  await credit(p.id, "100", "GIFT", { expires_at: new Date(expires).toISOString() });
  const q = await admit(p.credential);
  const result = await complete(q.request, q.attempt);
  const charge = String(result[0].result.charge_id);
  let locked!: () => void;
  const holding = new Promise<void>((r) => {
    locked = r;
  });
  let unlock!: () => void;
  const release = new Promise<void>((r) => {
    unlock = r;
  });
  const blocker = db.begin(async (tx) => {
    await tx`SELECT id FROM wallets WHERE project_id=${p.id} FOR UPDATE`;
    locked();
    await release;
    await tx`SELECT expire_wallet(id) FROM wallets WHERE project_id=${p.id}`;
  });
  await holding;
  const refund = funds.refund(charge, {
    external_ref: "cross-expiry",
    amount_atoms: "10",
    reason: "CUSTOMER_REFUND",
  });
  try {
    const until = Date.now() + 500;
    let waiting = false;
    while (Date.now() < until) {
      const rows =
        await owner`SELECT 1 FROM pg_stat_activity WHERE datname=${database} AND wait_event_type='Lock' AND query LIKE '%refund_billing_charge%'`;
      if (rows.length) {
        waiting = true;
        break;
      }
      await Bun.sleep(5);
    }
    expect(waiting).toBe(true);
    await Bun.sleep(Math.max(0, expires - Date.now() + 50));
  } finally {
    unlock();
  }
  await blocker;
  await refund;
  const w = await invariant(p.id);
  expect(w.available_atoms).toBe("0");
  expect(w.expired_atoms).toBe("54");
  expect(w.spent_atoms).toBe("46");
  await credit(p.id, "1");
  expect((await invariant(p.id)).available_atoms).toBe("1");
});
test("only the delivered retry is charged while every attempt cost remains available", async () => {
  const p = await project();
  await credit(p.id, "1000");
  const q = await admit(p.credential);
  await store.persistUsage([
    {
      source: "execution",
      attemptId: q.attempt,
      state: "UNKNOWN",
      finality: "FINAL",
      usage: { input_tokens: "20", output_tokens: "20" },
    },
  ]);
  const second = await store.startAttempt(q.request, 2, route);
  const result = await complete(q.request, second);
  expect(result[0].result.charged_atoms).toBe("56");
  const facts =
    await db`SELECT count(*)::int AS n,sum(cost_atoms)::text AS total FROM usage_events WHERE request_id=${q.request}`;
  expect(facts[0]).toMatchObject({ n: 2, total: "52" });
  expect((await invariant(p.id)).spent_atoms).toBe("56");
});
test("conflicting FINAL evidence freezes funds even after settlement until explicit resolution", async () => {
  const p = await project();
  await credit(p.id, "1000");
  const q = await admit(p.credential);
  await complete(q.request, q.attempt);
  const conflict = await store.persistUsage([
    {
      attemptId: q.attempt,
      source: "provider_invoice",
      state: "SUCCEEDED",
      finality: "FINAL",
      usage: { input_tokens: "8", output_tokens: "5" },
    },
  ]);
  expect(conflict).toMatchObject({ CONFLICT: 1 });
  await invariant(p.id);
  expect(((await funds.wallets(p.id)) as { freeze_reason: string }[])[0]?.freeze_reason).toBe(
    "EVIDENCE_CONFLICT",
  );
  await expect(admit(p.credential, "1")).rejects.toMatchObject({ errno: "P0402" });
  await funds.resolve(q.request, {
    external_ref: "reviewed-invoice",
    action: "SETTLE",
    usage: { input_tokens: "8", output_tokens: "5" },
    pricing_id: pricingId,
    reason: "EVIDENCE_RECONCILIATION",
  });
  await db`SELECT (settle_requests(ARRAY[${q.request}::uuid])->${q.request}::text)`;
  const w = await invariant(p.id);
  expect(w.freeze_reason).toBeNull();
  expect(w.spent_atoms).toBe("59");
});
test("strict BullMQ evidence retries PG failure and duplicate deliveries debit once", async () => {
  const { UsageProducer, UsageConsumer, usageChannel } = await import("@gateway/runtime");
  const p = await project();
  await credit(p.id, "1000");
  const q = await admit(p.credential);
  const options = {
    url: process.env.QUEUE_REDIS_URL!,
    namespace: `funds-queue:${crypto.randomUUID()}`,
    replicas: 0,
    timeoutMs: 1000,
  };
  let failing = true;
  let deliveries = 0;
  const producer = new UsageProducer({ ...options, channel: usageChannel });
  const consumer = new UsageConsumer({
    channel: usageChannel,
    ...options,
    process: async (events) => {
      deliveries++;
      if (failing) throw new Error("injected PG unavailable");
      return store.persistUsage(events);
    },
  });
  const evidence = {
    attemptId: q.attempt,
    source: "execution",
    state: "SUCCEEDED" as const,
    finality: "FINAL" as const,
    usage: { input_tokens: "7", output_tokens: "5" },
    requestState: "SUCCEEDED" as const,
  };
  try {
    await producer.ready();
    await consumer.ready();
    await producer.publish(evidence);
    const until = Date.now() + 1000;
    while (!deliveries && Date.now() < until) await Bun.sleep(5);
    expect(deliveries).toBeGreaterThan(0);
    expect((await funds.receipt(q.request, p.id)).charged_atoms).toBeNull();
    expect((await invariant(p.id)).reserved_atoms).toBe("100");
    failing = false;
    const deadline = Date.now() + 4000;
    while ((await funds.receipt(q.request, p.id)).charged_atoms !== "56" && Date.now() < deadline)
      await Bun.sleep(10);
    expect((await funds.receipt(q.request, p.id)).charged_atoms).toBe("56");
    await producer.publish(evidence);
    await Bun.sleep(100);
    expect((await invariant(p.id)).spent_atoms).toBe("56");
    const journals =
      await db`SELECT id FROM money_journal WHERE reference=${`settle:${q.request}`}`;
    expect(journals).toHaveLength(1);
  } finally {
    producer.close();
    await consumer.close();
    const redis = new Bun.RedisClient(options.url);
    try {
      let cursor = "0";
      do {
        const [next, keys] = (await redis.send("SCAN", [
          cursor,
          "MATCH",
          `{${options.namespace}:usage}:*`,
          "COUNT",
          "1000",
        ])) as [string, string[]];
        cursor = next;
        if (keys.length) await redis.send("UNLINK", keys);
      } while (cursor !== "0");
    } finally {
      redis.close();
    }
  }
});

test("partial debt refund and downward adjustment conserve funded, receivable and waived amounts", async () => {
  const p = await project();
  await credit(p.id, "10");
  const q = await admit(p.credential, "10");
  const r = await complete(q.request, q.attempt, { input_tokens: "0", output_tokens: "3" });
  const charge = String(r[0].result.charge_id);
  expect((await invariant(p.id)).debt_atoms).toBe("11");
  await funds.refund(charge, {
    external_ref: "waive",
    amount_atoms: "5",
    reason: "CUSTOMER_REFUND",
  });
  expect((await invariant(p.id)).debt_atoms).toBe("6");
  await funds.adjust(charge, {
    external_ref: "downward",
    target_atoms: "8",
    reason: "ADMIN_CORRECTION",
  });
  const w = await invariant(p.id);
  expect(w.debt_atoms).toBe("0");
  expect(w.spent_atoms).toBe("8");
  expect(w.available_atoms).toBe("2");
  const c =
    await db`SELECT amount_atoms::text,funded_atoms::text,debt_atoms::text,debt_refunded_atoms::text,refunded_atoms::text FROM billing_charges WHERE id=${charge}`;
  expect(c[0]).toMatchObject({
    amount_atoms: "21",
    funded_atoms: "10",
    debt_atoms: "0",
    debt_refunded_atoms: "11",
    refunded_atoms: "13",
  });
});

test("late completed, cancelled or failed resources from an earlier attempt cannot replace delivered retry", async () => {
  for (const status of ["completed", "cancelled", "failed"]) {
    const p = await project();
    await credit(p.id, "1000");
    const q = await admit(p.credential);
    const resource = await store.saveResource(
      p.id,
      route.provider_id,
      `late-${crypto.randomUUID()}`,
      "response",
      "funds-model",
      q.attempt,
    );
    await store.persistUsage([
      {
        source: "execution",
        attemptId: q.attempt,
        state: "UNKNOWN",
        finality: "FINAL",
        usage: { input_tokens: "20", output_tokens: "20" },
      },
    ]);
    const second = await store.startAttempt(q.request, 2, route);
    await store.persistUsage([
      {
        source: "execution",
        attemptId: second,
        state: "SUCCEEDED",
        finality: "FINAL",
        usage: { input_tokens: "7", output_tokens: "5" },
        requestState: "SUCCEEDED",
      },
    ]);
    await store.updateResource(resource, "COMPLETED", status);
    const selected =
      await db`SELECT selected_attempt_id,billing_outcome FROM gateway_requests WHERE id=${q.request}`;
    expect(selected[0]).toMatchObject({ selected_attempt_id: second, billing_outcome: "SUCCESS" });
    const rows =
      await db`SELECT (settle_requests(ARRAY[${q.request}::uuid])->${q.request}::text) AS result`;
    expect(rows[0].result.charged_atoms).toBe("56");
    await invariant(p.id);
  }
});

test("terminal rejection arriving before earlier attempt evidence converges to full release", async () => {
  const p = await project();
  await credit(p.id, "100");
  const q = await admit(p.credential);
  const later = await store.startAttempt(q.request, 2, route);
  await store.persistUsage([
    {
      attemptId: later,
      source: "execution_outcome",
      state: "NOT_EXECUTED",
      finality: "UNKNOWN",
      usage: {},
      requestState: "FAILED",
    },
  ]);
  expect((await funds.receipt(q.request, p.id)).hold_state).toBe("RECONCILING");
  expect((await invariant(p.id)).reserved_atoms).toBe("100");
  await store.persistUsage([
    {
      attemptId: q.attempt,
      source: "execution",
      state: "NOT_EXECUTED",
      finality: "UNKNOWN",
      usage: {},
    },
  ]);
  const receipt = await funds.receipt(q.request, p.id);
  expect(receipt.billing_outcome).toBe("FAILED");
  expect(receipt.hold_state).toBe("RELEASED");
  expect((await invariant(p.id)).available_atoms).toBe("100");
});

test("incompatible old database is rejected without modifying its records", async () => {
  const name = `${database}_old`;
  await owner.unsafe(`CREATE DATABASE ${name}`);
  const url = new URL(process.env.DATABASE_URL!);
  url.pathname = `/${name}`;
  const old = createDatabase(url.toString(), 1);
  try {
    await old`CREATE TABLE schema_migrations(version integer PRIMARY KEY)`;
    await old`INSERT INTO schema_migrations VALUES(8)`;
    await old`CREATE TABLE retained_record(value text)`;
    await old`INSERT INTO retained_record VALUES('keep')`;
    const { assemble } = await import("../../apps/gateway/src/assembly.ts");
    const { loadConfig } = await import("../../apps/gateway/src/config.ts");
    for (const version of [8, 2026111005]) {
      await old`UPDATE schema_migrations SET version=${version}`;
      await expect(migrate(old)).rejects.toThrow("Incompatible database schema");
      await expect(
        assemble(
          loadConfig({
            ...process.env,
            NODE_ENV: "test",
            DATABASE_URL: url.toString(),
            REDIS_NAMESPACE: `incompatible:${crypto.randomUUID()}`,
            ADMIN_KEY: "test-" + "a".repeat(40),
            ENCRYPTION_KEY: "11".repeat(32),
            KEY_PEPPER: "p".repeat(40),
          }),
        ),
      ).rejects.toThrow("Incompatible database schema");
      expect((await old`SELECT value FROM retained_record`)[0]?.value).toBe("keep");
      expect((await old`SELECT version FROM schema_migrations`)[0]?.version).toBe(version);
    }
  } finally {
    await old.close();
    await owner.unsafe(`DROP DATABASE ${name} WITH (FORCE)`);
  }
});

test("explicit RELEASE with a captured pricing reference releases the original hold", async () => {
  const p = await project();
  await credit(p.id, "200");
  const q = await admit(p.credential);
  await funds.resolve(q.request, {
    external_ref: "release-with-price",
    action: "RELEASE",
    pricing_id: pricingId,
    usage: {},
    reason: "PROVIDER_INVOICE",
  });
  const rows =
    await db`SELECT (settle_requests(ARRAY[${q.request}::uuid])->${q.request}::text) AS result`;
  expect(rows[0].result.state).toBe("RELEASED");
  const w = await invariant(p.id);
  expect(w.available_atoms).toBe("200");
  expect(w.reserved_atoms).toBe("0");
});

test("a retained failed usage job is alerted on but does not close admission until failed jobs reach their own bound; teardown leaves no queue metadata", async () => {
  const { UsageProducer, usageChannel } = await import("@gateway/runtime");
  const id = crypto.randomUUID();
  const namespace = `test-usage:failed:${id}`;
  const neighborNamespace = `test-usage:failed-neighbor:${id}`;
  const cleanup = createUsageQueueCleanup(namespace);
  const neighborCleanup = createUsageQueueCleanup(neighborNamespace);
  for (const unowned of ["", "*", "test-usage:*", `${namespace}:*`, `queue-failed:${id}`])
    expect(() => createUsageQueueCleanup(unowned)).toThrow("owned test namespace");
  const options = {
    url: process.env.QUEUE_REDIS_URL!,
    namespace,
    replicas: 0,
    timeoutMs: 1000,
    maxPendingJobs: 2,
  };
  const producer = new UsageProducer({ ...options, channel: usageChannel });
  const redis = new Bun.RedisClient(options.url);
  const neighbor = new UsageProducer({
    channel: usageChannel,
    ...options,
    namespace: neighborNamespace,
  });
  let neighborKeys: string[] = [];
  try {
    try {
      await neighbor.ready();
      await neighbor.publish({
        attemptId: crypto.randomUUID(),
        source: "execution",
        state: "SUCCEEDED",
        finality: "FINAL",
        usage: { input_tokens: "1" },
      });
      neighborKeys = await neighborCleanup.keys(options.url);
      expect(neighborKeys.length).toBeGreaterThan(0);
      await producer.ready();
      await redis.send("ZADD", [
        `{${namespace}:usage}:usage-v1:failed`,
        String(Date.now()),
        "retained",
      ]);
      expect((await producer.stats()).failed).toBe(1);
      await Bun.sleep(150);
      await producer.ready();
      await redis.send("ZADD", [
        `{${namespace}:usage}:usage-v1:failed`,
        String(Date.now()),
        "retained-2",
      ]);
      await Bun.sleep(150);
      await expect(producer.ready()).rejects.toMatchObject({
        status: 503,
        code: "usage_queue_unavailable",
      });
    } finally {
      producer.close();
      neighbor.close();
      redis.close();
      await cleanup.remove(options.url);
    }
    expect(await cleanup.keys(options.url)).toHaveLength(0);
    expect(await neighborCleanup.keys(options.url)).toEqual(neighborKeys);
    const reader = new UsageProducer({
      channel: usageChannel,
      ...options,
      namespace: neighborNamespace,
    });
    try {
      await reader.ready();
      expect((await reader.stats()).waiting).toBe(1);
    } finally {
      reader.close();
    }
  } finally {
    await neighborCleanup.remove(options.url);
  }
});

test("queue backlog blocks new admission while accepting facts for already-admitted calls", async () => {
  const { UsageProducer, UsageConsumer, usageChannel } = await import("@gateway/runtime");
  const p = await project();
  await credit(p.id, "1000");
  const requests = await Promise.all([admit(p.credential), admit(p.credential)]);
  const options = {
    url: process.env.QUEUE_REDIS_URL!,
    namespace: `queue-pressure:${crypto.randomUUID()}`,
    replicas: 0,
    timeoutMs: 1000,
    maxPendingJobs: 1,
  };
  const producer = new UsageProducer({ ...options, channel: usageChannel });
  let consumer: import("@gateway/runtime").UsageConsumer | undefined;
  const event = (attemptId: string) => ({
    attemptId,
    source: "execution",
    state: "SUCCEEDED" as const,
    finality: "FINAL" as const,
    usage: { input_tokens: "7", output_tokens: "5" },
    requestState: "SUCCEEDED" as const,
  });
  try {
    await producer.ready();
    await producer.publish(event(requests[0]!.attempt));
    await expect(producer.ready()).rejects.toMatchObject({ status: 503 });
    await producer.publish(event(requests[1]!.attempt));
    expect((await producer.stats()).waiting).toBe(2);
    consumer = new UsageConsumer({
      channel: usageChannel,
      ...options,
      process: (events) => store.persistUsage(events),
    });
    await consumer.ready();
    const end = Date.now() + 5000;
    while ((await invariant(p.id)).spent_atoms !== "112" && Date.now() < end) await Bun.sleep(10);
    expect((await invariant(p.id)).spent_atoms).toBe("112");
    await producer.ready();
  } finally {
    producer.close();
    await consumer?.close();
    const redis = new Bun.RedisClient(options.url);
    try {
      let cursor = "0";
      do {
        const [next, keys] = (await redis.send("SCAN", [
          cursor,
          "MATCH",
          `{${options.namespace}:usage}:*`,
          "COUNT",
          "1000",
        ])) as [string, string[]];
        cursor = next;
        if (keys.length) await redis.send("UNLINK", keys);
      } while (cursor !== "0");
    } finally {
      redis.close();
    }
  }
});

test("every permitted in-flight operation can finish a simultaneous durable evidence burst", async () => {
  const { UsageProducer, usageChannel } = await import("@gateway/runtime");
  const options = {
    url: process.env.QUEUE_REDIS_URL!,
    namespace: `queue-burst:${crypto.randomUUID()}`,
    replicas: 0,
    timeoutMs: 2000,
    maxInFlight: 10000,
  };
  const producer = new UsageProducer({ ...options, channel: usageChannel });
  try {
    await producer.ready();
    const outcomes = await Promise.allSettled(
      Array.from({ length: 10000 }, () =>
        producer.publish({
          attemptId: crypto.randomUUID(),
          source: "execution",
          state: "SUCCEEDED",
          finality: "FINAL",
          usage: { input_tokens: "7", output_tokens: "5" },
          requestState: "SUCCEEDED",
        }),
      ),
    );
    expect(outcomes.filter((r) => r.status === "rejected")).toHaveLength(0);
    expect((await producer.stats()).waiting).toBe(Math.ceil(10000 / 64));
  } finally {
    producer.close();
    const redis = new Bun.RedisClient(options.url);
    try {
      let cursor = "0";
      do {
        const [next, keys] = (await redis.send("SCAN", [
          cursor,
          "MATCH",
          `{${options.namespace}:usage}:*`,
          "COUNT",
          "1000",
        ])) as [string, string[]];
        cursor = next;
        if (keys.length) await redis.send("UNLINK", keys);
      } while (cursor !== "0");
    } finally {
      redis.close();
    }
  }
});

test("indexed recovery handles unknown holds and settled debt without releasing missing evidence", async () => {
  const p = await project();
  await credit(p.id, "1000");
  const unknown = await admit(p.credential, "100");
  const debt = await admit(p.credential, "100");
  await store.persistUsage([
    {
      attemptId: unknown.attempt,
      source: "execution",
      state: "SUCCEEDED",
      finality: "UNKNOWN",
      usage: {},
      requestState: "SUCCEEDED",
    },
  ]);
  await complete(debt.request, debt.attempt, { input_tokens: "1000", output_tokens: "0" });
  expect((await invariant(p.id)).debt_atoms).toBe("2100");
  await credit(p.id, "2200");
  await db`UPDATE billing_holds SET next_reconcile_at=now() WHERE request_id=ANY(${[unknown.request, debt.request]}::uuid[])`;
  expect(await funds.reconcileKnownHolds()).toBe(2);
  let wallet = await invariant(p.id);
  expect(wallet.debt_atoms).toBe("0");
  expect(wallet.reserved_atoms).toBe("100");
  expect(wallet.available_atoms).toBe("100");
  expect((await funds.receipt(unknown.request, p.id)).hold_state).toBe("RECONCILING");
  await funds.resolve(unknown.request, {
    external_ref: "explicit-release",
    action: "RELEASE",
    usage: {},
    reason: "PROVIDER_INVOICE",
  });
  expect(await funds.reconcileKnownHolds()).toBe(1);
  wallet = await invariant(p.id);
  expect(wallet.reserved_atoms).toBe("0");
  expect(wallet.available_atoms).toBe("200");
  expect(wallet.spent_atoms).toBe("3000");
});

test("one admission batch observes prior allocations across cash and gift sources", async () => {
  const p = await project();
  await credit(p.id, "940");
  await credit(p.id, "60", "GIFT");
  const inputs = Array.from({ length: 20 }, () => ({
    id: crypto.randomUUID(),
    key_id: p.credential.id,
    project_id: p.id,
    operation: "chat",
    model: "funds-model",
    digest: null,
    fingerprint: "batch",
    route_id: route.id,
    cost_pricing_id: route.pricing_id,
    quote: {
      currency: "USD",
      amount_atoms: "100",
      input_characters: "0",
      prices: { [route.provider_id]: pricingId },
    },
  }));
  const rows = await db`SELECT admit_gateway_batch(${inputs}::jsonb) AS result`;
  const results = Object.values(rows[0].result) as { created?: boolean; error?: string }[];
  expect(results.filter((r) => r.created)).toHaveLength(10);
  expect(results.filter((r) => r.error === "P0402")).toHaveLength(10);
  const wallet = await invariant(p.id);
  expect(wallet.available_atoms).toBe("0");
  expect(wallet.reserved_atoms).toBe("1000");
  const lots = await db<
    { kind: string; reserved_atoms: string }[]
  >`SELECT kind,reserved_atoms::text FROM wallet_lots WHERE wallet_id=(SELECT id FROM wallets WHERE project_id=${p.id})`;
  expect(lots.find((l) => l.kind === "GIFT")?.reserved_atoms).toBe("60");
  expect(lots.find((l) => l.kind === "CASH")?.reserved_atoms).toBe("940");
});

test("returned cash wakes prior debt collection while returned gifts cannot repay it", async () => {
  for (const [operation, kind] of [
    ["refund", "CASH"],
    ["release", "CASH"],
    ["release", "GIFT"],
  ] as const) {
    const p = await project();
    await credit(p.id, "200", kind);
    const a = await admit(p.credential, "100");
    const b = await admit(p.credential, "100");
    let charge: string | undefined;
    if (operation === "refund")
      charge = String(
        (await complete(a.request, a.attempt, { input_tokens: "31", output_tokens: "1" }))[0].result
          .charge_id,
      );
    await complete(b.request, b.attempt, { input_tokens: "50", output_tokens: "0" });
    expect((await invariant(p.id)).debt_atoms).toBe("50");
    if (operation === "refund")
      await funds.refund(charge!, {
        external_ref: "return-cash",
        amount_atoms: "100",
        reason: "CUSTOMER_REFUND",
      });
    else
      await store.persistUsage([
        {
          attemptId: a.attempt,
          source: "execution",
          state: "NOT_EXECUTED",
          finality: "UNKNOWN",
          usage: {},
          requestState: "FAILED",
        },
      ]);
    const recovered = await funds.reconcileKnownHolds();
    if (kind === "CASH") expect(recovered).toBeGreaterThanOrEqual(1);
    const wallet = await invariant(p.id);
    expect(wallet.debt_atoms).toBe(kind === "CASH" ? "0" : "50");
    expect(wallet.available_atoms).toBe(kind === "CASH" ? "50" : "100");
    expect(wallet.freeze_reason).toBe(kind === "CASH" ? null : "DEBT");
  }
});

test("journal validation rejects malformed, unbalanced, cross-wallet and wrong-currency entries", async () => {
  const p = await project();
  const other = await project();
  await credit(p.id, "10");
  await credit(other.id, "10");
  const sources = await db<
    { id: string; wallet_id: string; project_id: string }[]
  >`SELECT l.id,l.wallet_id,w.project_id FROM wallet_lots l JOIN wallets w ON w.id=l.wallet_id WHERE w.project_id=ANY(${[p.id, other.id]}::uuid[])`;
  const own = sources.find((l) => l.project_id === p.id)!;
  const foreign = sources.find((l) => l.project_id === other.id)!;
  const leg = { account: `lot:${own.id}:available`, atoms: "1" };
  const opposite = { account: "system:funding", atoms: "-1" };
  const invalid: unknown[] = [
    [{ ...leg, content: "forbidden" }, opposite],
    [{ account: leg.account }, opposite],
    [{ ...leg, atoms: 1 }, opposite],
    [{ ...leg, atoms: "1.0" }, opposite],
    [leg, { ...opposite, atoms: "-2" }],
    [{ ...leg, account: `lot:${foreign.id}:available` }, opposite],
  ];
  for (const lines of invalid)
    await expect(
      db`INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(${crypto.randomUUID()},${own.wallet_id},${crypto.randomUUID()},'ADJUSTMENT','USD',${lines}::jsonb)`,
    ).rejects.toThrow();
  await expect(
    db`INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(${crypto.randomUUID()},${own.wallet_id},${crypto.randomUUID()},'ADJUSTMENT','EUR',${[leg, opposite]}::jsonb)`,
  ).rejects.toThrow();
  let accepted = false;
  await expect(
    db.begin(async (tx) => {
      await tx`INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(${crypto.randomUUID()},${own.wallet_id},${crypto.randomUUID()},'ADJUSTMENT','USD',${[leg, opposite]}::jsonb)`;
      accepted = true;
      throw new Error("rollback fixture");
    }),
  ).rejects.toThrow("rollback fixture");
  expect(accepted).toBe(true);
  await invariant(p.id);
});

test("debt collection pays known obligations while leaving unresolved disputed charges held", async () => {
  const p = await project();
  await credit(p.id, "100");
  const a = await admit(p.credential, "50");
  const b = await admit(p.credential, "50");
  await complete(a.request, a.attempt, { input_tokens: "31", output_tokens: "1" });
  await complete(b.request, b.attempt, { input_tokens: "31", output_tokens: "1" });
  await store.persistUsage([
    {
      attemptId: a.attempt,
      source: "disputed-final",
      state: "SUCCEEDED",
      finality: "FINAL",
      usage: { input_tokens: "32", output_tokens: "1" },
    },
  ]);
  await credit(p.id, "50");
  await funds.reconcileKnownHolds();
  const left = await funds.receipt(a.request, p.id);
  const right = await funds.receipt(b.request, p.id);
  expect(left.debt_atoms).toBe("50");
  expect(right.debt_atoms).toBe("0");
  const wallet = await invariant(p.id);
  expect(wallet.available_atoms).toBe("0");
  expect(wallet.debt_atoms).toBe("50");
  expect(wallet.freeze_reason).toBe("EVIDENCE_CONFLICT");
});

test("a post-calculation monetary overflow cannot contaminate a valid peer settlement", async () => {
  const p = await project();
  const max = "9".repeat(38);
  await credit(p.id, max);
  const initial = await admit(p.credential, max);
  await complete(initial.request, initial.attempt, {
    input_tokens: (BigInt(max) / 3n).toString(),
    output_tokens: "0",
  });
  await credit(p.id, "2", "CASH", { priority: 101 });
  const a = await admit(p.credential, "1");
  const b = await admit(p.credential, "1");
  const events = [
    {
      attemptId: a.attempt,
      source: "execution",
      state: "SUCCEEDED",
      finality: "FINAL",
      usage: { input_tokens: "1", output_tokens: "0" },
      requestState: "SUCCEEDED",
    },
    {
      attemptId: b.attempt,
      source: "execution",
      state: "NOT_EXECUTED",
      finality: "UNKNOWN",
      usage: {},
      requestState: "FAILED",
    },
  ];
  const rows = await db`SELECT persist_usage_batch(${events}::jsonb) AS result`;
  expect(rows[0].result["0"].status).toBe("ERROR");
  expect(rows[0].result["1"].billing.state).toBe("RELEASED");
  const wallet = await invariant(p.id);
  expect(wallet.spent_atoms).toBe(max);
  expect(wallet.reserved_atoms).toBe("1");
  expect(wallet.available_atoms).toBe("1");
  expect(wallet.debt_atoms).toBe("0");
  expect((await funds.receipt(a.request, p.id)).charged_atoms).toBeNull();
  expect((await funds.receipt(b.request, p.id)).charged_atoms).toBe("0");
});

test("one batch keeps currency, scoped grants, captured pricing and duplicate facts independent", async () => {
  const p = await project();
  await credit(p.id, "200");
  await credit(p.id, "40", "GIFT", { models: ["funds-model"] });
  await credit(p.id, "200", "CASH", { currency: "EUR" });
  await credit(p.id, "40", "GIFT", { currency: "EUR", models: ["unrelated"] });
  const euro = await funds.setTariff({
    model: "funds-model",
    operation: "chat",
    basis: "PROVIDER_USAGE",
    pricing: {
      currency: "EUR",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "2", denominator: "1" },
      },
    },
    ceilings: { input_tokens: "100", output_tokens: "100" },
  });
  const usd = await admit(p.credential);
  const eur = await admit(p.credential, "100", "EUR", euro.pricing_id);
  const event = (attemptId: string) => ({
    attemptId,
    source: "execution",
    state: "SUCCEEDED" as const,
    finality: "FINAL" as const,
    usage: { input_tokens: "7", output_tokens: "5" },
    requestState: "SUCCEEDED" as const,
  });
  const counts = await store.persistUsage([
    event(usd.attempt),
    event(eur.attempt),
    event(usd.attempt),
  ]);
  expect(counts.SETTLED).toBe(2);
  expect(counts.REPLAYED).toBe(1);
  await invariant(p.id);
  const wallets = await db<
    { currency: string; available_atoms: string; spent_atoms: string; reserved_atoms: string }[]
  >`SELECT currency,available_atoms::text,spent_atoms::text,reserved_atoms::text FROM wallets WHERE project_id=${p.id}`;
  expect(wallets.find((w) => w.currency === "USD")).toMatchObject({
    available_atoms: "184",
    spent_atoms: "56",
    reserved_atoms: "0",
  });
  expect(wallets.find((w) => w.currency === "EUR")).toMatchObject({
    available_atoms: "223",
    spent_atoms: "17",
    reserved_atoms: "0",
  });
  const gift = await db<
    { currency: string; spent_atoms: string }[]
  >`SELECT w.currency,l.spent_atoms::text FROM wallet_lots l JOIN wallets w ON w.id=l.wallet_id WHERE w.project_id=${p.id} AND l.kind='GIFT'`;
  expect(gift.find((l) => l.currency === "USD")?.spent_atoms).toBe("40");
  expect(gift.find((l) => l.currency === "EUR")?.spent_atoms).toBe("0");
  const evidence = await db<
    { currency: string; cost_atoms: string }[]
  >`SELECT e.currency,e.cost_atoms::text FROM usage_events e WHERE e.request_id=ANY(${[usd.request, eur.request]}::uuid[])`;
  expect(evidence).toHaveLength(2);
  expect(evidence.every((e) => e.currency === "USD" && e.cost_atoms === "12")).toBe(true);
});

test("contradictory final facts in the same batch cannot charge before the dispute is known", async () => {
  const p = await project();
  await credit(p.id, "1000");
  const q = await admit(p.credential);
  const event = {
    attemptId: q.attempt,
    state: "SUCCEEDED" as const,
    finality: "FINAL" as const,
    requestState: "SUCCEEDED" as const,
  };
  await store.persistUsage([
    { ...event, source: "execution", usage: { input_tokens: "7", output_tokens: "5" } },
    { ...event, source: "async-final", usage: { input_tokens: "8", output_tokens: "5" } },
  ]);
  expect((await funds.receipt(q.request, p.id)).charged_atoms).toBeNull();
  const wallet = await invariant(p.id);
  expect(wallet.reserved_atoms).toBe("100");
  expect(wallet.spent_atoms).toBe("0");
  expect(wallet.freeze_reason).toBe("EVIDENCE_CONFLICT");
});

test("pre-collection overflow cannot roll back another wallet's valid fact and charge", async () => {
  const p = await project();
  const max = "9".repeat(38);
  await credit(p.id, max);
  const initial = await admit(p.credential, max);
  await complete(initial.request, initial.attempt, {
    input_tokens: (BigInt(max) / 3n).toString(),
    output_tokens: "0",
  });
  const debt = await admit(p.credential, "0");
  await complete(debt.request, debt.attempt, { input_tokens: "1", output_tokens: "0" });
  await credit(p.id, "1", "CASH", { priority: 101 });
  const peer = await project();
  await credit(peer.id, "100");
  const good = await admit(peer.credential);
  const facts = [
    {
      attemptId: debt.attempt,
      source: "execution",
      state: "SUCCEEDED",
      finality: "FINAL",
      usage: { input_tokens: "1", output_tokens: "0" },
      requestState: "SUCCEEDED",
    },
    {
      attemptId: good.attempt,
      source: "execution",
      state: "SUCCEEDED",
      finality: "FINAL",
      usage: { input_tokens: "7", output_tokens: "5" },
      requestState: "SUCCEEDED",
    },
  ];
  const rows = await db`SELECT persist_usage_batch(${facts}::jsonb) AS result`;
  expect(rows[0].result["0"].status).toBe("ERROR");
  expect(rows[0].result["1"].billing.state).toBe("SETTLED");
  const wallet = await invariant(p.id);
  expect(wallet.spent_atoms).toBe(max);
  expect(wallet.available_atoms).toBe("1");
  expect(wallet.debt_atoms).toBe("3");
  expect((await invariant(peer.id)).spent_atoms).toBe("56");
});

test("fully spent refundable buckets retain their slot and refunds do not exceed the source budget", async () => {
  const p = await project();
  let first: string | undefined;
  for (let priority = 0; priority < 256; priority++) {
    await credit(p.id, "3", "CASH", { priority });
    const q = await admit(p.credential, "3");
    const rows = await complete(q.request, q.attempt, { input_tokens: "1", output_tokens: "0" });
    first ??= String(rows[0].result.charge_id);
  }
  await expect(credit(p.id, "3", "CASH", { priority: 256 })).rejects.toMatchObject({ status: 422 });
  await funds.refund(first!, {
    external_ref: "origin-refund",
    amount_atoms: "3",
    reason: "CUSTOMER_REFUND",
  });
  await credit(p.id, "3", "CASH", { priority: 0 });
  const count =
    await db`SELECT count(*)::int AS n FROM wallet_lots WHERE wallet_id=(SELECT id FROM wallets WHERE project_id=${p.id})`;
  expect(count[0].n).toBe(256);
  const wallet = await invariant(p.id);
  expect(wallet.available_atoms).toBe("6");
  expect(wallet.spent_atoms).toBe("765");
});

test("completed replay cursors skip historical scans without missing later live evidence", async () => {
  const p = await project();
  await credit(p.id, "100");
  const q = await admit(p.credential);
  const live = await store.createConsumer(`live-${crypto.randomUUID()}`, false);
  expect((await store.claimEvents(live, 500)).events).toHaveLength(0);
  const event = (source: string) => ({
    attemptId: q.attempt,
    source,
    state: "SUCCEEDED" as const,
    finality: "FINAL" as const,
    usage: { input_tokens: "7", output_tokens: "5" },
    requestState: "SUCCEEDED" as const,
  });
  for (let offset = 0; offset < 1001; offset += 128)
    await store.persistUsage(
      Array.from({ length: Math.min(128, 1001 - offset) }, (_, i) =>
        event(`history_${offset + i}`),
      ),
    );
  const drain = async (consumer: string, expected: number) => {
    const seen = new Set<string>();
    for (;;) {
      const page = await store.claimEvents(consumer, 500);
      if (!page.events.length) break;
      for (const value of page.events) {
        expect(seen.has(value.id)).toBe(false);
        seen.add(value.id);
      }
      expect(
        await store.ackEvents(
          consumer,
          page.lease_token,
          page.events.map((e) => e.id),
        ),
      ).toBe(page.events.length);
    }
    expect(seen.size).toBe(expected);
  };
  await drain(live, 1001);
  const history = await db`SELECT count(*)::int AS n FROM usage_events`;
  const replay = await store.createConsumer(`replay-${crypto.randomUUID()}`, true);
  await drain(replay, Number(history[0].n));
  await store.persistUsage([event("after_replay")]);
  await drain(live, 1);
  await drain(replay, 1);
  const wallet = await invariant(p.id);
  expect(wallet.spent_atoms).toBe("56");
});

test("batch admission isolates rejected allocations, authorizes each item and arbitrates repeated intents", async () => {
  const p = await project();
  await credit(p.id, "100");
  const denied = await store.createCredential(
    p.id,
    { name: "billing-only", models: [], operations: ["chat"], role: "BILLING" },
    crypto.randomUUID(),
    "test",
  );
  const input = (amount: string) => ({
    id: crypto.randomUUID(),
    key_id: p.credential.id,
    project_id: p.id,
    operation: "chat",
    model: "funds-model",
    digest: null as string | null,
    fingerprint: "batch",
    route_id: route.id,
    cost_pricing_id: route.pricing_id,
    quote: { currency: "USD", amount_atoms: amount, prices: { [route.provider_id]: pricingId } },
  });
  const accepted = { ...input("60"), digest: "unique-intent" };
  const items = [
    input("200"),
    { ...input("1"), cost_pricing_id: crypto.randomUUID() },
    { ...input("1"), key_id: denied },
    accepted,
    { ...accepted, id: crypto.randomUUID() },
    { ...accepted, id: crypto.randomUUID(), fingerprint: "conflict" },
    { ...input("1"), id: accepted.id },
    input("40"),
    input("1"),
  ];
  const rows = await db`SELECT admit_gateway_batch(${items}::jsonb) AS result`;
  const result = rows[0].result;
  expect(result["0"].error).toBe("P0402");
  expect(result["1"].error).toBe("P0422");
  expect(result["2"].error).toBe("P0403");
  expect(result["3"].created).toBe(true);
  expect(result["4"].created).toBe(false);
  expect(result["4"].record.id).toBe(accepted.id);
  expect(result["5"].error).toBe("P0409");
  expect(result["6"].error).toBe("23505");
  expect(result["7"].created).toBe(true);
  expect(result["8"].error).toBe("P0402");
  expect((await invariant(p.id)).reserved_atoms).toBe("100");
  const counts =
    await db`SELECT (SELECT count(*)::int FROM gateway_requests WHERE project_id=${p.id}) AS requests,(SELECT count(*)::int FROM attempts a JOIN gateway_requests q ON q.id=a.request_id WHERE q.project_id=${p.id}) AS attempts`;
  expect(counts[0].requests).toBe(2);
  expect(counts[0].attempts).toBe(2);
});

test("expiry overflow on one wallet does not roll back a valid peer admission", async () => {
  const p = await project();
  const max = "9".repeat(38);
  await credit(p.id, max, "GIFT", { expires_at: new Date(Date.now() + 100).toISOString() });
  await Bun.sleep(150);
  await admit(p.credential, "0");
  expect((await invariant(p.id)).expired_atoms).toBe(max);
  await credit(p.id, "1", "GIFT", { expires_at: new Date(Date.now() + 100).toISOString() });
  await Bun.sleep(150);
  const peer = await project();
  await credit(peer.id, "100");
  const input = (credential: Credential, amount: string) => ({
    id: crypto.randomUUID(),
    key_id: credential.id,
    project_id: credential.project_id,
    operation: "chat",
    model: "funds-model",
    digest: null,
    fingerprint: "expiry",
    route_id: route.id,
    cost_pricing_id: route.pricing_id,
    quote: { currency: "USD", amount_atoms: amount, prices: { [route.provider_id]: pricingId } },
  });
  const items = [input(p.credential, "0"), input(peer.credential, "100")];
  const rows = await db`SELECT admit_gateway_batch(${items}::jsonb) AS result`;
  expect(rows[0].result["0"].error).toBe("22003");
  expect(rows[0].result["1"].created).toBe(true);
  expect((await invariant(peer.id)).reserved_atoms).toBe("100");
  expect((await invariant(p.id)).available_atoms).toBe("1");
});

test("concurrent intents across different currency wallets still reserve exactly once", async () => {
  const p = await project();
  await credit(p.id, "100");
  await credit(p.id, "100", "CASH", { currency: "CNY" });
  const cny = crypto.randomUUID();
  await db`INSERT INTO billing_prices(id,currency,rates,ceilings,basis) SELECT ${cny},'CNY',rates,ceilings,basis FROM billing_prices WHERE id=${pricingId}`;
  const inputs = Array.from({ length: 20 }, (_, i) => ({
    id: crypto.randomUUID(),
    key_id: p.credential.id,
    project_id: p.id,
    operation: "chat",
    model: "funds-model",
    digest: "cross-currency-intent",
    fingerprint: "same-intent",
    route_id: route.id,
    cost_pricing_id: route.pricing_id,
    quote: {
      currency: i % 2 ? "USD" : "CNY",
      amount_atoms: "100",
      prices: { [route.provider_id]: i % 2 ? pricingId : cny },
    },
  }));
  const rows = await Promise.all(
    inputs.map(async (input) => {
      const deadline = Date.now() + 2000;
      let rows = await db`SELECT admit_gateway_batch(${[input]}::jsonb)->'0' AS result`;
      while (rows[0].result.error === "55P03" && Date.now() < deadline) {
        await Bun.sleep(5);
        rows = await db`SELECT admit_gateway_batch(${[input]}::jsonb)->'0' AS result`;
      }
      return rows;
    }),
  );
  const results = rows.map((row) => row[0].result);
  expect(results.filter((result) => result.created)).toHaveLength(1);
  expect(results.filter((result) => result.created === false)).toHaveLength(19);
  expect(new Set(results.map((result) => result.record.id)).size).toBe(1);
  await invariant(p.id);
  const summary =
    await db`SELECT sum(available_atoms)::text AS available,sum(reserved_atoms)::text AS reserved FROM wallets WHERE project_id=${p.id}`;
  expect(summary[0].available).toBe("100");
  expect(summary[0].reserved).toBe("100");
  const attempts =
    await db`SELECT count(*)::int AS n FROM attempts a JOIN gateway_requests q ON q.id=a.request_id WHERE q.project_id=${p.id}`;
  expect(attempts[0].n).toBe(1);
});

test("cross-job batching never splits conflicting facts in one atomic input group", async () => {
  const p = await project(),
    disputed = await project();
  await credit(p.id, "1000");
  await credit(disputed.id, "1000");
  const first = await admit(p.credential),
    second = await admit(disputed.credential);
  const batching = new Store(db);
  const filler = {
    attemptId: first.attempt,
    source: "execution",
    state: "SUCCEEDED" as const,
    finality: "FINAL" as const,
    usage: { input_tokens: "7", output_tokens: "5" },
    requestState: "SUCCEEDED" as const,
  };
  await Promise.all([
    batching.persistUsage(Array.from({ length: 127 }, () => ({ ...filler }))),
    batching.persistUsage([
      {
        ...filler,
        attemptId: second.attempt,
        source: "first_final",
        usage: { input_tokens: "1", output_tokens: "1" },
      },
      {
        ...filler,
        attemptId: second.attempt,
        source: "conflicting_final",
        usage: { input_tokens: "2", output_tokens: "1" },
      },
    ]),
  ]);
  expect((await db`SELECT id FROM billing_charges WHERE request_id=${second.request}`).length).toBe(
    0,
  );
  expect((await invariant(disputed.id)).reserved_atoms).toBe("100");
  expect((await invariant(disputed.id)).freeze_reason).toBe("EVIDENCE_CONFLICT");
});
