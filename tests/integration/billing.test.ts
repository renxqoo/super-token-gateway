import { afterAll, beforeAll, expect, test } from "bun:test";
import type { SQL } from "bun";
import type { CoreEvidenceGroupV1, ExecutionReply } from "@gateway/contracts";
import { CoreRpcError } from "@gateway/core-client";
import type { Usage } from "@gateway/kernel";
import { UsageProducer, evidenceChannel } from "@gateway/runtime";
import { sha256 } from "@gateway/runtime/crypto";
import {
  createCustomerFundsStore,
  createDatabase,
  createEventStore,
  migrate,
  recordEventConsumer,
} from "@gateway/storage";
import { createSettlementStore } from "@gateway/storage/settlement";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";
import {
  committed,
  event,
  granted,
  type Client,
  type EventBody,
  type Fixture,
  type Setup,
} from "../support/core-settlement.ts";
import { createUsageQueueCleanup } from "../support/usage-queue-cleanup.ts";
import { createSettlementWorker } from "../../apps/core-worker/src/settlement/worker.ts";

let f: Fixture;
let db: SQL;
let client: Client;
let funds: ReturnType<typeof createCustomerFundsStore>;
let settlement: ReturnType<typeof createSettlementStore>;
const finished = Promise.withResolvers<void>();
let running: Promise<void>;

beforeAll(async () => {
  const ready = Promise.withResolvers<void>();
  running = withFinancial(async (fixture, owner) => {
    f = fixture;
    db = owner;
    client = f.client();
    await client.startup();
    funds = createCustomerFundsStore(db);
    settlement = createSettlementStore(db);
    ready.resolve();
    await finished.promise;
  });
  await Promise.race([ready.promise, running]);
}, 120000);
afterAll(async () => {
  finished.resolve();
  await running;
}, 120000);

type Rates = Record<string, { numerator: string; denominator: string }>;
type Scope = { setup: Setup; hold?: string; pricing?: string; custom?: boolean };
type Project = { id: string; setup: Setup };
type Admitted = Awaited<ReturnType<typeof granted>> & {
  request: string;
  attempt: string;
  pricing: string;
  setup: Setup;
  ceilings: Record<string, string>;
};
type Signed = ReturnType<typeof event>;

const standard: Rates = {
  input_tokens: { numerator: "3", denominator: "1" },
  output_tokens: { numerator: "7", denominator: "1" },
};
const max = "9".repeat(38);
const insufficient = { status: 402, code: "insufficient_funds" };

async function scope(): Promise<Scope> {
  const setup = await f.bootstrap("USD", "9".repeat(37), 100, "PROVIDER_USAGE", {
    rates: {
      input_tokens: { numerator: "0", denominator: "1" },
      output_tokens: { numerator: "0", denominator: "1" },
    },
    ceilings: { input_tokens: "16777216", output_tokens: "1000000" },
  });
  const limits = await f.request(
    `/providers/${setup.provider.id}`,
    { concurrency: 100000, rpm: 10000000 },
    undefined,
    "PATCH",
  );
  if (limits.status !== 200) throw new Error(`Provider limits: ${JSON.stringify(limits)}`);
  return { setup };
}

async function setTariff(s: Scope, rates: Rates, ceilings: Record<string, string>) {
  const response = await f.request("/tariffs", {
    model: s.setup.model,
    operation: "chat",
    provider_id: s.setup.provider.id,
    pricing: { currency: "USD", rates },
    ceilings,
    basis: "PROVIDER_USAGE",
  });
  if (response.status !== 201) throw new Error(`Tariff: ${JSON.stringify(response)}`);
  return String(response.value.pricing_id);
}

async function tariff(s: Scope, amount: bigint) {
  const { input, output } = hold(amount);
  s.pricing = await setTariff(s, standard, {
    input_tokens: input,
    output_tokens: String(output),
  });
  s.hold = String(amount);
  s.custom = false;
  return s.pricing;
}

async function custom(s: Scope, rates: Rates, ceilings: Record<string, string>) {
  s.pricing = await setTariff(s, rates, ceilings);
  delete s.hold;
  s.custom = true;
  return s.pricing;
}

async function key(s: Scope, project: string, role?: string) {
  const created = await f.request(`/projects/${project}/keys`, {
    name: "funds",
    models: [s.setup.model],
    operations: ["chat"],
    ...(role ? { role } : {}),
  });
  if (created.status !== 201) throw new Error(`Key: ${JSON.stringify(created)}`);
  return { id: String(created.value.id), key: String(created.value.key) };
}

async function project(s: Scope): Promise<Project> {
  const created = await f.request("/projects", {
    name: crypto.randomUUID(),
    billing_currency: "USD",
    rpm: 10000000,
    tpm: 1000000000,
    concurrency: 1000000,
  });
  const id = String(created.value.id);
  return { id, setup: { ...s.setup, project: { id }, key: await key(s, id) } };
}

async function elsewhere(p: Project) {
  const s = await scope();
  return {
    s,
    p: { id: p.id, setup: { ...s.setup, project: { id: p.id }, key: await key(s, p.id) } },
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

function hold(amount: bigint) {
  for (const output of [10, ...Array.from({ length: 100 }, (_, i) => 100 - i)]) {
    const rest = amount - 7n * BigInt(output);
    if (rest >= 0n && rest % 3n === 0n) return { input: String(rest / 3n), output };
  }
  throw new Error(`No 3·input + 7·output hold equals ${amount}`);
}

async function admit(
  s: Scope,
  p: Project,
  amount = "100",
  options: Parameters<typeof granted>[3] = {},
): Promise<Admitted> {
  let output = 10;
  if (!s.custom) {
    output = hold(BigInt(amount)).output;
    if (s.hold !== amount) await tariff(s, BigInt(amount));
  }
  const ceilings = { input_tokens: "100", output_tokens: String(output) };
  try {
    const admitted = await granted(db, p.setup, client, { ceilings, ...options });
    const request = admitted.prepared.requestId;
    expect(
      (await db`SELECT amount_atoms::text FROM billing_holds WHERE request_id=${request}`)[0],
    ).toEqual({ amount_atoms: amount });
    return {
      ...admitted,
      request,
      attempt: admitted.prepared.attemptId,
      pricing: s.pricing!,
      setup: p.setup,
      ceilings,
    };
  } catch (error) {
    if (!(error instanceof CoreRpcError)) throw error;
    throw { status: error.status, code: error.detail.code };
  }
}

async function retry(q: Admitted): Promise<Admitted> {
  const prepared = await prepareFinancialAdmission(q.setup, client, {
    requestId: q.request,
    deadline: q.prepared.deadline,
    ceilings: q.ceilings,
  });
  const next = (await client.execution(
    "execution_next_v1",
    {
      context_id: prepared.data.context_id,
      dispatch_projection: prepared.data.dispatch_projection,
      dispatched_digest: prepared.data.dispatched_digest,
      manifest: prepared.data.manifest,
      prep_session_id: prepared.data.prep_session_id,
      prep_binding_digest: prepared.data.prep_binding_digest,
      request_id: q.request,
      previous_attempt_id: q.attempt,
      expected_ordinal: 2,
      visible_output: false,
    },
    { deadlineAt: prepared.deadline },
  )) as ExecutionReply;
  await client.execution("execution_begin_v1", prepared.beginData(next.receipt), {
    deadlineAt: prepared.deadline,
  });
  return { ...q, prepared, owner: { client, permit_epoch: "1" }, attempt: prepared.attemptId };
}

function fact(q: Admitted, fields: Partial<EventBody>) {
  return event(q.owner, q.attempt, fields);
}

function final(q: Admitted, usage: Usage, fields: Partial<EventBody> = {}) {
  return fact(q, {
    finality: "FINAL",
    usage,
    final_dimensions: Object.keys(usage) as EventBody["final_dimensions"],
    attempt_state: "SUCCEEDED",
    request_state: "SUCCEEDED",
    ...fields,
  });
}

function unknown(q: Admitted) {
  return fact(q, { finality: "UNKNOWN", attempt_state: "UNKNOWN" });
}

function notExecuted(q: Admitted, fields: Partial<EventBody> = {}) {
  return fact(q, { finality: "NOT_EXECUTED", attempt_state: "NOT_EXECUTED", ...fields });
}

async function persist(...events: Signed[]) {
  return (await settlement.persist([{ events }]))[0]!;
}

async function settle(request: string) {
  return db`SELECT (settle_requests(ARRAY[${request}::uuid])->${request}::text) AS result`;
}

async function complete(q: Admitted, usage: Usage = { input_tokens: "7", output_tokens: "5" }) {
  committed(await persist(final(q, usage)));
  return settle(q.request);
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

test("expected admission rejections leave no request behind and do not disturb funded admissions", async () => {
  const s = await scope();
  const funded = await project(s);
  await credit(funded.id, "1000");
  const broke = await project(s);
  await credit(broke.id, "1");
  const frozen = await project(s);
  await credit(frozen.id, "1000");
  await funds.configure(frozen.id, "USD", { frozen: true });
  const brokeDigest = sha256(crypto.randomUUID());
  await admit(s, funded, "10", { intent: null });
  await expect(admit(s, broke, "100", { intent: brokeDigest })).rejects.toEqual(insufficient);
  await expect(admit(s, frozen, "10", { intent: null })).rejects.toEqual(insufficient);
  await admit(s, funded, "10");
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
  expect((await funds.wallets(frozen.id))[0]).toMatchObject({
    available_atoms: "1000",
    reserved_atoms: "0",
  });
});

test("holds that cannot progress without new evidence back off and do not delay debt collection", async () => {
  const s = await scope();
  const waiting = await project(s);
  await credit(waiting.id, "1000");
  for (let i = 0; i < 40; i++) {
    const q = await admit(s, waiting, "10");
    committed(await persist(unknown(q)));
  }
  const [{ id: wallet }] = await db`SELECT id FROM wallets WHERE project_id=${waiting.id}`;
  await db`UPDATE billing_holds SET created_at=now()-interval '2 hours',next_reconcile_at=now()-interval '1 minute' WHERE wallet_id=${wallet}`;
  const indebted = await project(s);
  await credit(indebted.id, "10");
  const q = await admit(s, indebted, "10");
  await complete(q);
  expect((await funds.wallets(indebted.id))[0]).toMatchObject({
    debt_atoms: "46",
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
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1", "GIFT", { models: ["other"] });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "1000",
  });
  const indebted = await admit(s, p, "60");
  await complete(indebted);
  expect((await funds.wallets(p.id))[0]).toMatchObject({ debt_atoms: "56", freeze_reason: null });
  await credit(p.id, "100", "GIFT");
  const gifted = await admit(s, p, "60");
  await complete(gifted);
  const [held] =
    await db`SELECT next_reconcile_at>now()+interval '59 minutes' AS deferred FROM billing_holds WHERE request_id=${indebted.request}`;
  expect(held.deferred).toBe(true);
  expect((await funds.wallets(p.id))[0]).toMatchObject({ debt_atoms: "56" });
  await credit(p.id, "100");
  while ((await funds.reconcileKnownHolds(32)) > 0);
  expect((await funds.wallets(p.id))[0]).toMatchObject({ debt_atoms: "0" });
  await invariant(p.id);
});

test("concurrent requests reserve existing debt and in-flight credit before admitting more work", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "100");
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "50",
  });
  await tariff(s, 60n);
  const outcomes = await Promise.allSettled(Array.from({ length: 3 }, () => admit(s, p, "60")));
  const accepted = outcomes.filter((r) => r.status === "fulfilled").map((r) => r.value);
  expect(accepted).toHaveLength(2);
  expect(outcomes.flatMap((r) => (r.status === "rejected" ? [r.reason] : []))).toEqual([
    insufficient,
  ]);
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    available_atoms: "0",
    reserved_atoms: "100",
    credit_reserved_atoms: "20",
    available_credit_atoms: "30",
  });
  const credit_atoms = new Map(
    (
      await db<
        { request_id: string; credit_atoms: string }[]
      >`SELECT request_id,credit_atoms::text FROM billing_holds WHERE request_id=ANY(${accepted.map((q) => q.request)}::uuid[])`
    ).map((row) => [row.request_id, BigInt(row.credit_atoms)]),
  );
  accepted.sort((x, y) => Number(credit_atoms.get(x.request)! - credit_atoms.get(y.request)!));
  expect([...credit_atoms.values()].sort()).toEqual([0n, 20n]);
  for (const q of accepted) await complete(q);
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    credit_reserved_atoms: "0",
    debt_atoms: "12",
    available_credit_atoms: "38",
    freeze_reason: null,
  });
  await invariant(p.id);
});

test("unknown credit holds remain committed until an explicit release and duplicate release is harmless", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1", "GIFT", { models: ["other"] });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "60",
  });
  const q = await admit(s, p, "60");
  committed(await persist(unknown(q)));
  await expect(admit(s, p, "10")).rejects.toEqual(insufficient);
  expect((await funds.wallets(p.id))[0]).toMatchObject({ credit_reserved_atoms: "60" });
  const resolution = {
    external_ref: "release",
    action: "RELEASE" as const,
    usage: {},
    reason: "ADMIN_WRITE_OFF" as const,
  };
  await funds.resolve(q.request, resolution);
  await settle(q.request);
  await funds.resolve(q.request, resolution);
  await settle(q.request);
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    reserved_atoms: "0",
    credit_reserved_atoms: "0",
    debt_atoms: "0",
    available_credit_atoms: "60",
  });
  await invariant(p.id);
});

test("credit settlement and refunds debit once, gifts do not repay old debt, and cash restores credit", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1", "GIFT", { models: ["other"] });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "100",
  });
  const q = await admit(s, p, "60");
  const settled = await complete(q);
  await complete(q);
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    debt_atoms: "56",
    credit_reserved_atoms: "0",
  });
  await credit(p.id, "100", "GIFT");
  await settle(q.request);
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
  await settle(q.request);
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    debt_atoms: "0",
    available_atoms: "111",
    available_credit_atoms: "100",
  });
  await invariant(p.id);
});

test("actual overrun keeps the full receivable and freezes new promises beyond the credit limit", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1");
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "50",
  });
  const q = await admit(s, p, "50");
  await complete(q, { input_tokens: "50", output_tokens: "50" });
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    available_atoms: "0",
    credit_reserved_atoms: "0",
    debt_atoms: "499",
    credit_excess_atoms: "449",
    available_credit_atoms: "0",
    freeze_reason: "DEBT",
  });
  const next = await elsewhere(p);
  await expect(admit(next.s, next.p, "10")).rejects.toEqual(insufficient);
  await credit(p.id, "449");
  await settle(q.request);
  expect((await funds.wallets(p.id))[0]).toMatchObject({ debt_atoms: "50", freeze_reason: null });
  await credit(p.id, "10");
  await admit(next.s, next.p, "10");
  await invariant(p.id);
});

test("credit policy retries preserve their original result without overwriting a newer policy", async () => {
  const s = await scope();
  const p = await project(s);
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
  const q = await admit(s, p, "60");
  await expect(
    funds.configureCredit(p.id, "USD", {
      external_ref: "lower",
      expected_version: "2",
      credit_limit_atoms: "58",
    }),
  ).rejects.toMatchObject({ status: 409 });
  await funds.configure(p.id, "USD", { frozen: true });
  await expect(admit(s, p, "10")).rejects.toEqual(insufficient);
  await complete(q);
  expect((await funds.wallets(p.id))[0]).toMatchObject({ freeze_reason: "MANUAL" });
  await invariant(p.id);
});

test("credit atoms above IEEE754 remain exact and cannot fund another currency", async () => {
  const s = await scope();
  const p = await project(s);
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
  const q = await admit(s, p, "9007199254740993");
  await expect(admit(s, p, "10")).rejects.toEqual(insufficient);
  expect(
    (await funds.wallets(p.id)).find((w) => (w as { currency: string }).currency === "USD"),
  ).toMatchObject({ credit_reserved_atoms: "9007199254740993", available_credit_atoms: "0" });
  await complete(q);
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
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1", "GIFT", { models: ["other"] });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "open",
    expected_version: "0",
    credit_limit_atoms: "1000",
  });
  await tariff(s, 100n);
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
    Promise.allSettled(Array.from({ length: 10 }, () => admit(s, p, "100"))),
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
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1", "GIFT", { models: ["other"] });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "100",
  });
  const q = await admit(s, p, "60");
  const result = await complete(q);
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
  const s = await scope();
  const tenant = await project(s);
  await credit(tenant.id, "1000");
  const q = await admit(s, tenant);
  await complete(q);
  const ids = await db<
    { charge_id: string; hold_id: string; journal_id: string; kind: string }[]
  >`SELECT c.id AS charge_id,h.id AS hold_id,j.id AS journal_id,j.kind
    FROM billing_charges c JOIN billing_holds h ON h.id=c.hold_id
    JOIN money_journal j ON j.wallet_id=h.wallet_id AND j.reference IN ('hold:'||h.request_id,'settle:'||h.request_id)
    WHERE c.request_id=${q.request} ORDER BY j.kind`;
  expect(ids.map((row) => row.kind)).toEqual(["HOLD", "SETTLE"]);
  const timeOrdered = /^[a-f0-9]{8}-[a-f0-9]{4}-7[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
  for (const row of ids) {
    expect(row.hold_id).toBe(q.request);
    expect(row.charge_id).toMatch(timeOrdered);
    expect(row.journal_id).toMatch(timeOrdered);
  }
  expect(await invariant(tenant.id)).toMatchObject({ reserved_atoms: "0" });
});

test("covered settlement and an over-hold peer on the same wallet preserve exact independent charges", async () => {
  const s = await scope();
  const mixed = await project(s);
  const peer = await project(s);
  await credit(mixed.id, "1000");
  await credit(peer.id, "1000");
  const covered = await admit(s, mixed);
  const over = await admit(s, mixed);
  const independent = await admit(s, peer);
  const groups = [covered, over, independent].map((record, index) => ({
    events: [
      final(
        record,
        index === 1
          ? { input_tokens: "14", output_tokens: "10" }
          : { input_tokens: "7", output_tokens: "5" },
      ),
    ],
  }));
  const first = await settlement.persist(groups);
  expect(
    first.map((group) => committed(group).receipts.map((r) => [r.outcome, r.duplicate])),
  ).toEqual([[["ACCEPTED", false]], [["ACCEPTED", false]], [["ACCEPTED", false]]]);
  expect((await funds.receipt(covered.request, mixed.id)).charged_atoms).toBe("56");
  expect((await funds.receipt(over.request, mixed.id)).charged_atoms).toBe("112");
  expect((await funds.receipt(independent.request, peer.id)).charged_atoms).toBe("56");
  const replay = await settlement.persist(groups);
  expect(replay.map((group) => committed(group).receipts.map((r) => r.duplicate))).toEqual([
    [true],
    [true],
    [true],
  ]);
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

test("a 128-group evidence batch preserves every receipt and a repeated batch at the boundary is a replay", async () => {
  const s = await scope();
  const tenant = await project(s);
  await credit(tenant.id, "30000");
  const groups: CoreEvidenceGroupV1[] = [];
  for (let index = 0; index < 128; index++)
    groups.push({
      events: [final(await admit(s, tenant), { input_tokens: "7", output_tokens: "5" })],
    });
  const results = await settlement.persist(groups);
  expect(results).toHaveLength(128);
  for (const result of results)
    expect(committed(result).receipts.map((r) => [r.outcome, r.duplicate])).toEqual([
      ["ACCEPTED", false],
    ]);
  const charges = await db<
    { amount_atoms: string; n: number }[]
  >`SELECT c.amount_atoms::text,count(*)::int AS n FROM billing_charges c JOIN gateway_requests q ON q.id=c.request_id WHERE q.project_id=${tenant.id} GROUP BY c.amount_atoms`;
  expect([...charges]).toEqual([{ amount_atoms: "56", n: 128 }]);
  const replays = [
    ...(await settlement.persist(groups.slice(0, 64))),
    ...(await settlement.persist(groups.slice(64))),
  ];
  for (const result of replays)
    expect(committed(result).receipts.map((r) => r.duplicate)).toEqual([true]);
  expect(await invariant(tenant.id)).toMatchObject({
    available_atoms: "22832",
    spent_atoms: "7168",
    reserved_atoms: "0",
  });
});

test("a locked wallet does not block evidence for another wallet in the same batch", async () => {
  const s = await scope();
  const hot = await project(s);
  const cold = await project(s);
  await credit(hot.id, "1000");
  await credit(cold.id, "1000");
  const hotRequest = await admit(s, hot);
  const coldRequest = await admit(s, cold);
  const usage = { input_tokens: "7", output_tokens: "5" };
  const unlocked = Promise.withResolvers<void>(),
    locked = Promise.withResolvers<void>();
  const lock = db.begin(async (tx) => {
    await tx`SELECT id FROM wallets WHERE project_id=${hot.id} FOR UPDATE`;
    locked.resolve();
    await unlocked.promise;
  });
  await locked.promise;
  let results: Awaited<ReturnType<typeof settlement.persist>>;
  try {
    const started = performance.now();
    results = await settlement.persist([
      { events: [final(hotRequest, usage)] },
      { events: [final(coldRequest, usage)] },
    ]);
    expect(performance.now() - started).toBeLessThan(5000);
  } finally {
    unlocked.resolve();
    await lock;
  }
  expect(results[0]).toEqual({ status: "ERROR", error: "55P03" });
  committed(results[1]);
  expect((await invariant(cold.id)).reserved_atoms).toBe("0");
  expect((await invariant(hot.id)).reserved_atoms).toBe("100");
  committed(await persist(final(hotRequest, usage)));
  expect((await invariant(hot.id)).reserved_atoms).toBe("0");
  expect((await funds.receipt(hotRequest.request, hot.id)).charged_atoms).toBe("56");
  expect((await funds.receipt(coldRequest.request, cold.id)).charged_atoms).toBe("56");
});

test("foreign-key readers do not make a funded wallet appear busy", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "100");
  await tariff(s, 100n);
  const result = await db.begin(async (tx) => {
    await tx`SELECT id FROM wallets WHERE project_id=${p.id} FOR KEY SHARE`;
    return admit(s, p, "100");
  });
  await complete(result);
  const wallet = await invariant(p.id);
  expect(wallet.available_atoms).toBe("44");
  expect(wallet.reserved_atoms).toBe("0");
  expect(wallet.spent_atoms).toBe("56");
});

test("credit is concurrent-idempotent and currency isolated", async () => {
  const p = await project(await scope());
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
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1000");
  await credit(p.id, "60", "GIFT");
  const q = await admit(s, p);
  const held = await invariant(p.id);
  expect(held.reserved_atoms).toBe("100");
  expect(held.available_atoms).toBe("960");
  const a = await complete(q);
  expect(a[0].result.state).toBe("SETTLED");
  const replays = await Promise.all(Array.from({ length: 8 }, () => settle(q.request)));
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
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1000");
  await tariff(s, 100n);
  const all = await Promise.allSettled(Array.from({ length: 20 }, () => admit(s, p, "100")));
  expect(all.filter((r) => r.status === "fulfilled")).toHaveLength(10);
  expect(all.flatMap((r) => (r.status === "rejected" ? [r.reason] : []))).toEqual(
    Array.from({ length: 10 }, () => insufficient),
  );
  const w = await invariant(p.id);
  expect(w.available_atoms).toBe("0");
  expect(w.reserved_atoms).toBe("1000");
  const count = await db`SELECT count(*)::int AS n FROM gateway_requests WHERE project_id=${p.id}`;
  expect(count[0].n).toBe(10);
});

test("concurrent admissions observe prior allocations across cash and gift sources", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "940");
  await credit(p.id, "60", "GIFT");
  await tariff(s, 100n);
  const all = await Promise.allSettled(Array.from({ length: 20 }, () => admit(s, p, "100")));
  expect(all.filter((r) => r.status === "fulfilled")).toHaveLength(10);
  expect(all.flatMap((r) => (r.status === "rejected" ? [r.reason] : []))).toEqual(
    Array.from({ length: 10 }, () => insufficient),
  );
  const wallet = await invariant(p.id);
  expect(wallet.available_atoms).toBe("0");
  expect(wallet.reserved_atoms).toBe("1000");
  const lots = await db<
    { kind: string; reserved_atoms: string }[]
  >`SELECT kind,reserved_atoms::text FROM wallet_lots WHERE wallet_id=(SELECT id FROM wallets WHERE project_id=${p.id})`;
  expect(lots.find((l) => l.kind === "GIFT")?.reserved_atoms).toBe("60");
  expect(lots.find((l) => l.kind === "CASH")?.reserved_atoms).toBe("940");
});

test("missing final usage stays held until explicit reconciliation", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "200");
  const q = await admit(s, p);
  committed(
    await persist(
      fact(q, { finality: "UNKNOWN", attempt_state: "SUCCEEDED", request_state: "SUCCEEDED" }),
    ),
  );
  const result = await settle(q.request);
  expect(result[0].result.state).toBe("WAITING");
  expect((await invariant(p.id)).reserved_atoms).toBe("100");
  await funds.resolve(q.request, {
    external_ref: "invoice",
    action: "SETTLE",
    usage: { input_tokens: "7", output_tokens: "5" },
    pricing_id: q.pricing,
    reason: "PROVIDER_INVOICE",
  });
  const resolved = await settle(q.request);
  expect(resolved[0].result.charged_atoms).toBe("56");
  await invariant(p.id);
});

test("expired reserved gift is consumed but its unused hold and refund cannot revive", async () => {
  const s = await scope();
  const p = await project(s);
  await tariff(s, 100n);
  await credit(p.id, "100", "GIFT", { expires_at: new Date(Date.now() + 1000).toISOString() });
  const q = await admit(s, p);
  await Bun.sleep(1100);
  const result = await complete(q);
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
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "100");
  const q = await admit(s, p, "50");
  const result = await complete(q, { input_tokens: "50", output_tokens: "50" });
  expect(result[0].result.charged_atoms).toBe("500");
  let w = await invariant(p.id);
  expect(w.debt_atoms).toBe("400");
  expect(w.freeze_reason).toBe("DEBT");
  const next = await elsewhere(p);
  await expect(admit(next.s, next.p, "10")).rejects.toEqual(insufficient);
  await credit(p.id, "450");
  await settle(q.request);
  w = await invariant(p.id);
  expect(w.debt_atoms).toBe("0");
  expect(w.available_atoms).toBe("50");
  expect(w.freeze_reason).toBeNull();
});

test("refund and adjustment are idempotent and preserve lot sums", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1000");
  const q = await admit(s, p);
  const result = await complete(q);
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
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "100", "GIFT", { models: ["another-model"] });
  await expect(admit(s, p, "10")).rejects.toEqual(insufficient);
  await credit(p.id, "100");
  await credit(p.id, "100", "GIFT");
  await funds.configure(p.id, "USD", { policy: "CASH_FIRST" });
  const q = await admit(s, p, "100");
  await complete(q);
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
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1000");
  const rates = {
    cache_write_5m_tokens: { numerator: "5", denominator: "4" },
    cache_write_1h_tokens: { numerator: "2", denominator: "1" },
  };
  await custom(s, rates, { cache_write_5m_tokens: "100", cache_write_1h_tokens: "100" });
  const q = await admit(s, p, "325");
  committed(await persist(final(q, { cache_write_tokens: "200" })));
  expect((await funds.receipt(q.request, p.id)).charged_atoms).toBeNull();
  expect((await invariant(p.id)).reserved_atoms).toBe("325");
  const enrichment = final(
    q,
    { cache_write_tokens: "200", cache_write_5m_tokens: "100", cache_write_1h_tokens: "100" },
    { source: "provider_invoice" },
  );
  committed(await persist(enrichment));
  expect((await funds.receipt(q.request, p.id)).charged_atoms).toBe("325");
  expect((await invariant(p.id)).spent_atoms).toBe("325");
  const [cost] =
    await db`SELECT exact_billing_price(${{ cache_write_5m_tokens: "100", cache_write_1h_tokens: "100" }}::jsonb,${rates}::jsonb)::text AS atoms`;
  expect(cost.atoms).toBe("325");
  committed(await persist(enrichment));
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
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "100");
  const q = await admit(s, p);
  const inconsistent = {
    input_tokens: "7",
    output_tokens: "5",
    cache_write_tokens: "3",
    cache_write_5m_tokens: "1",
    cache_write_1h_tokens: "1",
  };
  expect(() => final(q, inconsistent)).toThrow("Provider cache write partitions disagree");
  const signed = final(q, { input_tokens: "7", output_tokens: "5" });
  await expect(
    settlement.persist([
      {
        events: [{ ...signed, usage: inconsistent, final_dimensions: Object.keys(inconsistent) }],
      },
    ]),
  ).rejects.toThrow("Provider cache write partitions disagree");
  expect((await invariant(p.id)).reserved_atoms).toBe("100");
});

test("refund waiting across gift expiry cannot resurrect credit or block later funding", async () => {
  const s = await scope();
  const p = await project(s);
  await tariff(s, 100n);
  const expires = Date.now() + 900;
  await credit(p.id, "100", "GIFT", { expires_at: new Date(expires).toISOString() });
  const q = await admit(s, p);
  const result = await complete(q);
  const charge = String(result[0].result.charge_id);
  const holding = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const blocker = db.begin(async (tx) => {
    await tx`SELECT id FROM wallets WHERE project_id=${p.id} FOR UPDATE`;
    holding.resolve();
    await release.promise;
    await tx`SELECT expire_wallet(id) FROM wallets WHERE project_id=${p.id}`;
  });
  await holding.promise;
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
        await db`SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%refund_billing_charge%'`;
      if (rows.length) {
        waiting = true;
        break;
      }
      await Bun.sleep(5);
    }
    expect(waiting).toBe(true);
    await Bun.sleep(Math.max(0, expires - Date.now() + 50));
  } finally {
    release.resolve();
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

test("only the delivered retry is charged while every attempt keeps its own usage record", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1000");
  const q = await admit(s, p);
  committed(
    await persist(
      fact(q, {
        finality: "FINAL",
        usage: { input_tokens: "20", output_tokens: "10" },
        final_dimensions: ["input_tokens", "output_tokens"],
        attempt_state: "UNKNOWN",
      }),
    ),
  );
  const second = await retry(q);
  const result = await complete(second);
  expect(result[0].result.charged_atoms).toBe("56");
  const facts = await db<
    { attempt_id: string }[]
  >`SELECT attempt_id FROM usage_events WHERE request_id=${q.request}`;
  expect(new Set(facts.map((row) => row.attempt_id))).toEqual(new Set([q.attempt, second.attempt]));
  expect(
    (await db`SELECT selected_attempt_id FROM gateway_requests WHERE id=${q.request}`)[0]
      .selected_attempt_id,
  ).toBe(second.attempt);
  expect((await invariant(p.id)).spent_atoms).toBe("56");
});

test("conflicting FINAL evidence freezes funds even after settlement until explicit resolution", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1000");
  const q = await admit(s, p);
  await complete(q);
  const conflict = await persist(
    final(q, { input_tokens: "8", output_tokens: "5" }, { source: "provider_invoice" }),
  );
  expect(committed(conflict).receipts.map((r) => r.outcome)).toEqual(["CONFLICT"]);
  await invariant(p.id);
  expect(((await funds.wallets(p.id)) as { freeze_reason: string }[])[0]?.freeze_reason).toBe(
    "EVIDENCE_CONFLICT",
  );
  const next = await elsewhere(p);
  await expect(admit(next.s, next.p, "10")).rejects.toEqual(insufficient);
  await funds.resolve(q.request, {
    external_ref: "reviewed-invoice",
    action: "SETTLE",
    usage: { input_tokens: "8", output_tokens: "5" },
    pricing_id: q.pricing,
    reason: "EVIDENCE_RECONCILIATION",
  });
  await settle(q.request);
  const w = await invariant(p.id);
  expect(w.freeze_reason).toBeNull();
  expect(w.spent_atoms).toBe("59");
});

test("partial debt refund and downward adjustment conserve funded, receivable and waived amounts", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "10");
  const q = await admit(s, p, "10");
  const r = await complete(q, { input_tokens: "0", output_tokens: "3" });
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

test("terminal rejection arriving before earlier attempt evidence converges to full release", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "100");
  const q = await admit(s, p);
  const later = await retry(q);
  committed(await persist(notExecuted(later, { request_state: "FAILED" })));
  expect((await funds.receipt(q.request, p.id)).hold_state).toBe("RECONCILING");
  expect((await invariant(p.id)).reserved_atoms).toBe("100");
  committed(await persist(notExecuted(q)));
  const receipt = await funds.receipt(q.request, p.id);
  expect(receipt.billing_outcome).toBe("FAILED");
  expect(receipt.hold_state).toBe("RELEASED");
  expect((await invariant(p.id)).available_atoms).toBe("100");
});

test("incompatible old database is rejected without modifying its records", async () => {
  const name = `gateway_test_funds_old_${crypto.randomUUID().replaceAll("-", "")}`;
  const maintenance = createDatabase(process.env.DATABASE_URL!, 1);
  await maintenance.unsafe(`CREATE DATABASE ${name}`);
  const url = new URL(process.env.DATABASE_URL!);
  url.pathname = `/${name}`;
  const old = createDatabase(url.toString(), 1);
  try {
    await old`CREATE TABLE schema_migrations(version integer PRIMARY KEY)`;
    await old`INSERT INTO schema_migrations VALUES(8)`;
    await old`CREATE TABLE retained_record(value text)`;
    await old`INSERT INTO retained_record VALUES('keep')`;
    for (const version of [8, 2026111005]) {
      await old`UPDATE schema_migrations SET version=${version}`;
      await expect(migrate(old)).rejects.toThrow("Incompatible database schema");
      expect((await old`SELECT value FROM retained_record`)[0]?.value).toBe("keep");
      expect((await old`SELECT version FROM schema_migrations`)[0]?.version).toBe(version);
    }
  } finally {
    await old.close();
    await maintenance.unsafe(`DROP DATABASE ${name} WITH (FORCE)`);
    await maintenance.close();
  }
});

test("explicit RELEASE with a captured pricing reference releases the original hold", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "200");
  const q = await admit(s, p);
  await funds.resolve(q.request, {
    external_ref: "release-with-price",
    action: "RELEASE",
    pricing_id: q.pricing,
    usage: {},
    reason: "PROVIDER_INVOICE",
  });
  const rows = await settle(q.request);
  expect(rows[0].result.state).toBe("RELEASED");
  const w = await invariant(p.id);
  expect(w.available_atoms).toBe("200");
  expect(w.reserved_atoms).toBe("0");
});

test("a retained failed evidence job is alerted on but does not close admission until failed jobs reach their own bound; teardown leaves no queue metadata", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1000");
  const q = await admit(s, p);
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
  const producer = new UsageProducer({ ...options, channel: evidenceChannel });
  const redis = new Bun.RedisClient(options.url);
  const neighbor = new UsageProducer({
    ...options,
    channel: evidenceChannel,
    namespace: neighborNamespace,
  });
  let neighborKeys: string[] = [];
  try {
    try {
      await neighbor.ready();
      await neighbor.publish({ events: [final(q, { input_tokens: "1", output_tokens: "0" })] });
      neighborKeys = await neighborCleanup.keys(options.url);
      expect(neighborKeys.length).toBeGreaterThan(0);
      await producer.ready();
      const failed = `{${namespace}:${evidenceChannel.scope}}:${evidenceChannel.name}:failed`;
      await redis.send("ZADD", [failed, String(Date.now()), "retained"]);
      expect((await producer.stats()).failed).toBe(1);
      await Bun.sleep(150);
      await producer.ready();
      await redis.send("ZADD", [failed, String(Date.now()), "retained-2"]);
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
      ...options,
      channel: evidenceChannel,
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

test("evidence queue backlog blocks new admission while accepting facts for already-admitted calls", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1000");
  const requests = [await admit(s, p), await admit(s, p)];
  const namespace = `test-usage:pressure:${crypto.randomUUID()}`;
  const cleanup = createUsageQueueCleanup(namespace);
  const options = {
    url: process.env.QUEUE_REDIS_URL!,
    namespace,
    replicas: 0,
    timeoutMs: 1000,
    maxPendingJobs: 1,
  };
  const producer = new UsageProducer({ ...options, channel: evidenceChannel });
  let worker: ReturnType<typeof createSettlementWorker> | undefined;
  const group = (q: Admitted) => ({
    events: [final(q, { input_tokens: "7", output_tokens: "5" })],
  });
  try {
    await producer.ready();
    await producer.publish(group(requests[0]!));
    await expect(producer.ready()).rejects.toMatchObject({ status: 503 });
    await producer.publish(group(requests[1]!));
    expect((await producer.stats()).waiting).toBe(2);
    worker = createSettlementWorker({ ...options, db });
    await worker.ready();
    const end = Date.now() + 5000;
    while ((await invariant(p.id)).spent_atoms !== "112" && Date.now() < end) await Bun.sleep(10);
    expect((await invariant(p.id)).spent_atoms).toBe("112");
    await producer.ready();
  } finally {
    producer.close();
    await worker?.close();
    await cleanup.remove(options.url);
  }
});

test("every permitted in-flight operation can finish a simultaneous durable evidence burst", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1000");
  const q = await admit(s, p);
  const namespace = `test-usage:burst:${crypto.randomUUID()}`;
  const cleanup = createUsageQueueCleanup(namespace);
  const options = {
    url: process.env.QUEUE_REDIS_URL!,
    namespace,
    replicas: 0,
    timeoutMs: 2000,
    maxInFlight: 10000,
  };
  const producer = new UsageProducer({ ...options, channel: evidenceChannel });
  const group = { events: [final(q, { input_tokens: "7", output_tokens: "5" })] };
  try {
    await producer.ready();
    const outcomes = await Promise.allSettled(
      Array.from({ length: 10000 }, () => producer.publish(group)),
    );
    expect(outcomes.filter((r) => r.status === "rejected")).toHaveLength(0);
    expect((await producer.stats()).waiting).toBe(Math.ceil(10000 / 64));
  } finally {
    producer.close();
    await cleanup.remove(options.url);
  }
});

test("indexed recovery handles unknown holds and settled debt without releasing missing evidence", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1000");
  const unknownHold = await admit(s, p, "100");
  const debt = await admit(s, p, "100");
  committed(
    await persist(
      fact(unknownHold, {
        finality: "UNKNOWN",
        attempt_state: "SUCCEEDED",
        request_state: "SUCCEEDED",
      }),
    ),
  );
  await complete(debt, { input_tokens: "1000", output_tokens: "0" });
  expect((await invariant(p.id)).debt_atoms).toBe("2100");
  while ((await funds.reconcileKnownHolds(32)) > 0);
  await credit(p.id, "2200");
  await db`UPDATE billing_holds SET next_reconcile_at=now() WHERE request_id=ANY(${[unknownHold.request, debt.request]}::uuid[])`;
  expect(await funds.reconcileKnownHolds()).toBe(2);
  let wallet = await invariant(p.id);
  expect(wallet.debt_atoms).toBe("0");
  expect(wallet.reserved_atoms).toBe("100");
  expect(wallet.available_atoms).toBe("100");
  expect((await funds.receipt(unknownHold.request, p.id)).hold_state).toBe("RECONCILING");
  await funds.resolve(unknownHold.request, {
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

test("returned cash wakes prior debt collection while returned gifts cannot repay it", async () => {
  for (const [operation, kind] of [
    ["refund", "CASH"],
    ["release", "CASH"],
    ["release", "GIFT"],
  ] as const) {
    const s = await scope();
    const p = await project(s);
    await credit(p.id, "200", kind);
    const a = await admit(s, p, "100");
    const b = await admit(s, p, "100");
    let charge: string | undefined;
    if (operation === "refund")
      charge = String(
        (await complete(a, { input_tokens: "31", output_tokens: "1" }))[0].result.charge_id,
      );
    await complete(b, { input_tokens: "50", output_tokens: "0" });
    expect((await invariant(p.id)).debt_atoms).toBe("50");
    if (operation === "refund")
      await funds.refund(charge!, {
        external_ref: "return-cash",
        amount_atoms: "100",
        reason: "CUSTOMER_REFUND",
      });
    else committed(await persist(notExecuted(a, { request_state: "FAILED" })));
    const recovered = await funds.reconcileKnownHolds();
    if (kind === "CASH") expect(recovered).toBeGreaterThanOrEqual(1);
    const wallet = await invariant(p.id);
    expect(wallet.debt_atoms).toBe(kind === "CASH" ? "0" : "50");
    expect(wallet.available_atoms).toBe(kind === "CASH" ? "50" : "100");
    expect(wallet.freeze_reason).toBe(kind === "CASH" ? null : "DEBT");
  }
});

test("journal validation rejects malformed, unbalanced, cross-wallet and wrong-currency entries", async () => {
  const s = await scope();
  const p = await project(s);
  const other = await project(s);
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
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "100");
  const a = await admit(s, p, "50");
  const b = await admit(s, p, "50");
  await complete(a, { input_tokens: "31", output_tokens: "1" });
  await complete(b, { input_tokens: "31", output_tokens: "1" });
  const disputed = await persist(
    final(a, { input_tokens: "32", output_tokens: "1" }, { source: "provider_invoice" }),
  );
  expect(committed(disputed).receipts.map((r) => r.outcome)).toEqual(["CONFLICT"]);
  await credit(p.id, "50");
  await funds.reconcileKnownHolds();
  const left = await funds.receipt(a.request, p.id);
  const right = await funds.receipt(b.request, p.id);
  expect(left.debt_atoms).toBe("50");
  expect(right.debt_atoms).toBe("0");
  const wallet = await invariant(p.id);
  expect(wallet.available_atoms).toBe("0");
  expect(wallet.debt_atoms).toBe("50");
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    freeze_reason: "EVIDENCE_CONFLICT",
    evidence_disputed: true,
  });
});

test("a post-calculation monetary overflow cannot contaminate a valid peer settlement", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, max);
  const initial = await admit(s, p, max);
  await complete(initial, { input_tokens: (BigInt(max) / 3n).toString(), output_tokens: "0" });
  await credit(p.id, "20", "CASH", { priority: 101 });
  const next = await elsewhere(p);
  const a = await admit(next.s, next.p, "10");
  const b = await admit(next.s, next.p, "10");
  const results = await settlement.persist([
    { events: [final(a, { input_tokens: "1", output_tokens: "0" })] },
    { events: [notExecuted(b, { request_state: "FAILED" })] },
  ]);
  expect(results[0]).toEqual({ status: "ERROR", error: "P0500" });
  committed(results[1]);
  const wallet = await invariant(p.id);
  expect(wallet.spent_atoms).toBe(max);
  expect(wallet.reserved_atoms).toBe("10");
  expect(wallet.available_atoms).toBe("10");
  expect(wallet.debt_atoms).toBe("0");
  expect((await funds.receipt(a.request, p.id)).charged_atoms).toBeNull();
  expect((await funds.receipt(b.request, p.id)).charged_atoms).toBe("0");
});

test("one batch keeps scoped grants, captured pricing, other currencies and duplicate facts independent", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "200");
  await credit(p.id, "40", "GIFT", { models: [s.setup.model] });
  await credit(p.id, "200", "CASH", { currency: "EUR" });
  await credit(p.id, "40", "GIFT", { currency: "EUR", models: [s.setup.model] });
  const q = await admit(s, p);
  await tariff(s, 10n);
  const usage = final(q, { input_tokens: "7", output_tokens: "5" });
  expect(
    committed(await persist(usage, usage)).receipts.map((r) => [r.outcome, r.duplicate]),
  ).toEqual([
    ["ACCEPTED", false],
    ["ACCEPTED", true],
  ]);
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
    available_atoms: "240",
    spent_atoms: "0",
    reserved_atoms: "0",
  });
  const gift = await db<
    { currency: string; spent_atoms: string }[]
  >`SELECT w.currency,l.spent_atoms::text FROM wallet_lots l JOIN wallets w ON w.id=l.wallet_id WHERE w.project_id=${p.id} AND l.kind='GIFT'`;
  expect(gift.find((l) => l.currency === "USD")?.spent_atoms).toBe("40");
  expect(gift.find((l) => l.currency === "EUR")?.spent_atoms).toBe("0");
});

test("contradictory final facts in the same batch cannot charge before the dispute is known", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1000");
  const q = await admit(s, p);
  const group = await persist(
    final(q, { input_tokens: "7", output_tokens: "5" }),
    final(q, { input_tokens: "8", output_tokens: "5" }, { source: "provider_invoice" }),
  );
  expect(committed(group).receipts.map((r) => r.outcome)).toEqual(["CONFLICT", "CONFLICT"]);
  expect((await funds.receipt(q.request, p.id)).charged_atoms).toBeNull();
  expect((await settle(q.request))[0].result.state).toBe("CONFLICT");
  expect((await funds.receipt(q.request, p.id)).charged_atoms).toBeNull();
  const wallet = await invariant(p.id);
  expect(wallet.reserved_atoms).toBe("100");
  expect(wallet.spent_atoms).toBe("0");
  expect(wallet.freeze_reason).toBe("EVIDENCE_CONFLICT");
});

test("pre-collection overflow cannot roll back another wallet's valid fact and charge", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, max);
  const initial = await admit(s, p, max);
  await complete(initial, { input_tokens: (BigInt(max) / 3n).toString(), output_tokens: "0" });
  await funds.configureCredit(p.id, "USD", {
    external_ref: "limit",
    expected_version: "0",
    credit_limit_atoms: "10",
  });
  const next = await elsewhere(p);
  const debt = await admit(next.s, next.p, "10");
  await credit(p.id, "1", "CASH", { priority: 101 });
  const peer = await project(next.s);
  await credit(peer.id, "100");
  const good = await admit(next.s, peer);
  const results = await settlement.persist([
    { events: [final(debt, { input_tokens: "1", output_tokens: "0" })] },
    { events: [final(good, { input_tokens: "7", output_tokens: "5" })] },
  ]);
  expect(results[0]).toEqual({ status: "ERROR", error: "P0500" });
  committed(results[1]);
  const wallet = await invariant(p.id);
  expect(wallet.spent_atoms).toBe(max);
  expect(wallet.available_atoms).toBe("1");
  expect((await invariant(peer.id)).spent_atoms).toBe("56");
});

test("fully spent refundable buckets retain their slot and refunds do not exceed the source budget", async () => {
  const s = await scope();
  const p = await project(s);
  await custom(s, { input_tokens: { numerator: "3", denominator: "1" } }, { input_tokens: "1" });
  let first: string | undefined;
  for (let priority = 0; priority < 256; priority++) {
    await credit(p.id, "3", "CASH", { priority });
    const q = await admit(s, p, "3");
    const rows = await complete(q, { input_tokens: "1", output_tokens: "0" });
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
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "1000000");
  const consumer = (name: string, replay: boolean) =>
    db.begin((tx) => recordEventConsumer(tx, `${name}-${crypto.randomUUID()}`, replay));
  const events = createEventStore(db);
  const live = await consumer("live", false);
  expect((await events.claim(live, 500)).events).toHaveLength(0);
  const settled = async (count: number) => {
    const admitted: Admitted[] = [];
    for (let index = 0; index < count; index++) admitted.push(await admit(s, p));
    for (let offset = 0; offset < count; offset += 128)
      for (const result of await settlement.persist(
        admitted
          .slice(offset, offset + 128)
          .map((q) => ({ events: [final(q, { input_tokens: "7", output_tokens: "5" })] })),
      ))
        committed(result);
  };
  await settled(1001);
  const drain = async (id: string) => {
    const seen = new Set<string>();
    for (;;) {
      const page = await events.claim(id, 500);
      if (!page.events.length) break;
      for (const value of page.events) {
        expect(seen.has(value.id)).toBe(false);
        seen.add(value.id);
      }
      expect(
        await events.ack(
          id,
          page.lease_token,
          page.events.map((e) => e.id),
        ),
      ).toBe(page.events.length);
    }
    return seen.size;
  };
  expect(await drain(live)).toBe(1001);
  const history = (await db`SELECT count(*)::int AS n FROM usage_events`)[0].n;
  const replay = await consumer("replay", true);
  expect(await drain(replay)).toBe(history);
  await settled(1);
  expect(await drain(live)).toBe(1);
  expect(await drain(replay)).toBe(1);
  expect((await invariant(p.id)).spent_atoms).toBe(String(1002 * 56));
});

test("intent arbitration, key permission and funds are decided per admission and only funded intents reserve", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "100");
  const billing = { ...p, setup: { ...p.setup, key: await key(s, p.id, "BILLING") } };
  const intent = sha256(crypto.randomUUID());
  await expect(admit(s, p, "200")).rejects.toEqual(insufficient);
  await expect(admit(s, billing, "10")).rejects.toMatchObject({ status: 403 });
  const accepted = await admit(s, p, "60", { intent });
  await expect(admit(s, p, "60", { intent })).rejects.toMatchObject({
    status: 409,
    code: "already_accepted",
  });
  await expect(
    admit(s, p, "60", { intent, fingerprint: sha256("conflicting-input") }),
  ).rejects.toMatchObject({ status: 409, code: "idempotency_conflict" });
  await expect(admit(s, p, "60", { requestId: accepted.request })).rejects.toMatchObject({
    status: 409,
  });
  await admit(s, p, "40");
  await expect(admit(s, p, "10")).rejects.toEqual(insufficient);
  expect((await invariant(p.id)).reserved_atoms).toBe("100");
  const counts =
    await db`SELECT (SELECT count(*)::int FROM gateway_requests WHERE project_id=${p.id}) AS requests,(SELECT count(*)::int FROM attempts a JOIN gateway_requests q ON q.id=a.request_id WHERE q.project_id=${p.id}) AS attempts`;
  expect(counts[0]).toEqual({ requests: 2, attempts: 2 });
});

test("expiry overflow on one wallet does not roll back a valid peer admission", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, max, "GIFT", { expires_at: new Date(Date.now() + 100).toISOString() });
  await Bun.sleep(150);
  await db`SELECT expire_wallet(id) FROM wallets WHERE project_id=${p.id}`;
  expect((await invariant(p.id)).expired_atoms).toBe(max);
  await credit(p.id, "1", "GIFT", { expires_at: new Date(Date.now() + 100).toISOString() });
  await credit(p.id, "10");
  await Bun.sleep(150);
  const peer = await project(s);
  await credit(peer.id, "100");
  await tariff(s, 10n);
  const outcomes = await Promise.allSettled([admit(s, p, "10"), admit(s, peer, "10")]);
  expect(outcomes[0].status).toBe("rejected");
  expect(outcomes[1].status).toBe("fulfilled");
  expect((await invariant(peer.id)).reserved_atoms).toBe("10");
  expect(await invariant(p.id)).toMatchObject({ available_atoms: "11", reserved_atoms: "0" });
});

test("concurrent admissions of one intent reserve exactly once", async () => {
  const s = await scope();
  const p = await project(s);
  await credit(p.id, "100");
  await tariff(s, 100n);
  const intent = sha256(crypto.randomUUID());
  const outcomes = await Promise.allSettled(
    Array.from({ length: 20 }, () => admit(s, p, "100", { intent })),
  );
  expect(outcomes.filter((r) => r.status === "fulfilled")).toHaveLength(1);
  expect(outcomes.flatMap((r) => (r.status === "rejected" ? [r.reason] : []))).toEqual(
    Array.from({ length: 19 }, () => ({ status: 409, code: "already_accepted" })),
  );
  await invariant(p.id);
  expect((await funds.wallets(p.id))[0]).toMatchObject({
    available_atoms: "0",
    reserved_atoms: "100",
  });
  const attempts =
    await db`SELECT count(*)::int AS n FROM attempts a JOIN gateway_requests q ON q.id=a.request_id WHERE q.project_id=${p.id}`;
  expect(attempts[0].n).toBe(1);
});
