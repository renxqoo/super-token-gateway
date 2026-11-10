import { afterAll, beforeAll, expect, test } from "bun:test";
import { createDatabase, migrate, Store, FundsStore } from "@gateway/storage";
import type { SQL } from "bun";
import type { Route, Credential, Usage } from "@gateway/kernel";
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
  const rows = await db`SELECT admit_gateway_batch(${[payload]}::jsonb)->'0' AS result`;
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
    w.reserved_atoms<>coalesce((SELECT sum(h.amount_atoms) FROM billing_holds h WHERE h.wallet_id=w.id AND h.state IN ('HELD','RECONCILING')),0)
  )`;
  expect(unpaid).toHaveLength(0);
  return rows[0];
}
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
  await expect(admit(p.credential, "0")).rejects.toBeDefined();
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
  await expect(admit(p.credential, "1")).rejects.toBeDefined();
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
  await expect(admit(p.credential, "1")).rejects.toBeDefined();
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
  const { UsageProducer, UsageConsumer } = await import("@gateway/runtime");
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
  const producer = new UsageProducer(options);
  const consumer = new UsageConsumer({
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

test("queue backlog blocks new admission while accepting facts for already-admitted calls", async () => {
  const { UsageProducer, UsageConsumer } = await import("@gateway/runtime");
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
  const producer = new UsageProducer(options);
  let consumer: InstanceType<typeof UsageConsumer> | undefined;
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
    consumer = new UsageConsumer({ ...options, process: (events) => store.persistUsage(events) });
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
  const { UsageProducer } = await import("@gateway/runtime");
  const options = {
    url: process.env.QUEUE_REDIS_URL!,
    namespace: `queue-burst:${crypto.randomUUID()}`,
    replicas: 0,
    timeoutMs: 2000,
    maxInFlight: 10000,
  };
  const producer = new UsageProducer(options);
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
    inputs.map((input) => db`SELECT admit_gateway_batch(${[input]}::jsonb)->'0' AS result`),
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
