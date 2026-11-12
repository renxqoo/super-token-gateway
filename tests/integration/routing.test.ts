import { afterAll, afterEach, beforeAll, expect, spyOn, test } from "bun:test";
import { createDatabase, migrate } from "@gateway/storage";
import type { SQL } from "bun";
import { assemble } from "../../apps/gateway/src/assembly.ts";
import { createApp } from "../../apps/gateway/src/app.ts";
import { loadConfig } from "../../apps/gateway/src/config.ts";
import { BillingWorker } from "../../apps/gateway/src/billing-worker.ts";
import type { Dependencies } from "../../apps/gateway/src/context.ts";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";
import type { Scenario } from "../../apps/mock-providers/src/app.ts";

const suffix = crypto.randomUUID().replaceAll("-", "");
const database = `gateway_test_routing_${suffix}`;
const namespace = `routing:${suffix}`;
const adminKey = "routing-admin-" + "a".repeat(40);
const controlToken = "routing-mock-control";
const mocks = Array.from({ length: 3 }, () => ({
  ...createMockApp(controlToken),
  gates: new Map<string, Promise<void>[]>(),
  arrivals: new Map<string, number[]>(),
  retryHeaders: new Map<string, string>(),
}));
const mockServers: ReturnType<typeof Bun.serve>[] = [];
const fixtureServers: ReturnType<typeof Bun.serve>[] = [];
const instances: Dependencies[] = [];
let owner: SQL;
let db: SQL;
let billing: BillingWorker;
let management: ReturnType<typeof Bun.serve>;
let origin: string;
let consumer: string;

async function waitFor(predicate: () => Promise<boolean>, timeout = 5000): Promise<void> {
  const end = Date.now() + timeout;
  while (!(await predicate())) {
    if (Date.now() >= end) throw new Error("Routing condition timed out");
    await Bun.sleep(10);
  }
}

async function manage(path: string, body?: unknown, method = "POST") {
  const response = await fetch(`${origin}/admin${path}`, {
    method,
    headers: { authorization: `Bearer ${adminKey}`, "content-type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  expect(response.ok).toBe(true);
  return (await response.json()) as Record<string, unknown>;
}

function serve(deps: Dependencies) {
  const app = createApp(deps);
  return Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    idleTimeout: 0,
    fetch: (request, server) =>
      app.fetch(request, { peerAddress: server.requestIP(request)?.address }),
  });
}

beforeAll(async () => {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("Run routing integration tests with .env.local");
  owner = createDatabase(base, 2);
  await owner.unsafe(`CREATE DATABASE ${database}`);
  const url = new URL(base);
  url.pathname = `/${database}`;
  db = createDatabase(url.href, 8);
  await migrate(db);
  for (const mock of mocks)
    mockServers.push(
      Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        idleTimeout: 0,
        fetch: async (request) => {
          const model =
            request.method === "POST" &&
            request.headers.get("content-type")?.includes("application/json")
              ? String(((await request.clone().json()) as { model?: string }).model ?? "")
              : "";
          const response = await mock.app.fetch(request);
          if (model) {
            const arrivals = mock.arrivals.get(model) ?? [];
            arrivals.push(Date.now());
            mock.arrivals.set(model, arrivals);
          }
          const retryAfter = mock.retryHeaders.get(model);
          if (retryAfter !== undefined) response.headers.set("retry-after", retryAfter);
          if (response.ok) await mock.gates.get(model)?.shift();
          return response;
        },
      }),
    );
  for (let i = 0; i < 2; i++)
    instances.push(
      await assemble(
        loadConfig({
          ...process.env,
          NODE_ENV: "test",
          DATABASE_URL: url.href,
          ADMIN_KEY: adminKey,
          ENCRYPTION_KEY: "33".repeat(32),
          KEY_PEPPER: "routing-pepper-" + "p".repeat(32),
          REDIS_NAMESPACE: namespace,
          REDIS_EXTRA_URLS: "",
          ALLOW_PRIVATE_UPSTREAM: "true",
          DB_POOL_SIZE: "8",
          MAX_ATTEMPTS: "3",
          TOTAL_TIMEOUT_MS: "3500",
          HEADERS_TIMEOUT_MS: "150",
          STREAM_IDLE_MS: "150",
        }),
      ),
    );
  management = serve(instances[0]!);
  origin = `http://127.0.0.1:${management.port}`;
  billing = new BillingWorker(instances[0]!);
  await billing.start();
  consumer = await instances[0]!.store.createConsumer(`routing-${suffix}`, false);
});

async function drain() {
  await waitFor(
    async () =>
      instances.every((instance) => instance.lifecycle.size === 0) &&
      Object.values(await instances[0]!.usage.producer.stats()).every((n) => n === 0),
  );
}

afterEach(async () => {
  try {
    await drain();
  } finally {
    for (const server of fixtureServers) await server.stop(true);
    fixtureServers.length = 0;
  }
});

afterAll(async () => {
  for (const server of fixtureServers) await server.stop(true);
  if (management) await management.stop(true);
  for (const instance of instances) await instance.lifecycle.drain(4000);
  await billing?.close();
  for (const instance of instances) {
    instance.usage.close();
    instance.transport.close();
    await instance.database.close();
  }
  if (instances[0]) {
    const queue = new Bun.RedisClient(instances[0].config.QUEUE_REDIS_URL);
    try {
      for (const [client, pattern] of [
        [instances[0].limits.redis, `${namespace}:*`],
        [queue, `{${namespace}:usage}:*`],
      ] as const) {
        let cursor = "0";
        do {
          const [next, keys] = (await client.send("SCAN", [
            cursor,
            "MATCH",
            pattern,
            "COUNT",
            "1000",
          ])) as [string, string[]];
          cursor = next;
          if (keys.length) await client.send("UNLINK", keys);
        } while (cursor !== "0");
      }
    } finally {
      queue.close();
      for (const instance of instances) instance.limits.close();
    }
  }
  for (const server of mockServers) await server.stop(true);
  if (db) await db.close();
  if (owner) {
    await owner.unsafe(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
    await owner.close();
  }
});

const costRates = [
  {
    input_tokens: { numerator: "3", denominator: "1" },
    output_tokens: { numerator: "7", denominator: "1" },
    cached_input_tokens: { numerator: "1", denominator: "1" },
  },
  {
    input_tokens: { numerator: "5", denominator: "1" },
    output_tokens: { numerator: "9", denominator: "1" },
    cached_input_tokens: { numerator: "1", denominator: "1" },
  },
  {
    input_tokens: { numerator: "7", denominator: "1" },
    output_tokens: { numerator: "11", denominator: "1" },
    cached_input_tokens: { numerator: "1", denominator: "1" },
  },
];
const customerRates = [
  {
    input_tokens: { numerator: "2", denominator: "1" },
    output_tokens: { numerator: "3", denominator: "1" },
    cached_input_tokens: { numerator: "1", denominator: "1" },
  },
  {
    input_tokens: { numerator: "2", denominator: "3" },
    output_tokens: { numerator: "5", denominator: "2" },
    cached_input_tokens: { numerator: "1", denominator: "4" },
  },
  { requests: { numerator: "29", denominator: "1" } },
];

async function fixture(
  scenarios: Scenario[] = [{}, {}, {}],
  options: {
    attempts?: number;
    timeout?: number;
    headersTimeout?: number;
    strategy?: string;
    priorities?: number[];
    weights?: number[];
    firstBaseUrl?: string;
  } = {},
) {
  const model = `routing-${crypto.randomUUID()}`;
  const project = String(
    (await manage("/projects", { name: model, rpm: 100000, tpm: 100000000, concurrency: 1000 })).id,
  );
  await instances[0]!.funds.credit(project, {
    currency: "USD",
    external_ref: model,
    amount_atoms: "100000",
    kind: "CASH",
    priority: 100,
    models: [],
    operations: [],
  });
  const key = String(
    (await manage(`/projects/${project}/keys`, { name: model, role: "ADMIN" })).key,
  );
  await manage("/models", {
    name: model,
    operations: ["chat"],
    max_output_tokens: 8,
    routing_strategy: options.strategy ?? "WEIGHTED",
  });
  const providers: string[] = [];
  const routes: string[] = [];
  for (let i = 0; i < 3; i++) {
    mocks[i]!.scenarios.set(model, { output: `channel-${i}`, ...scenarios[i] });
    const provider = String(
      (
        await manage("/providers", {
          name: `${model}-${i}`,
          kind: "openai",
          base_url:
            i === 0 && options.firstBaseUrl
              ? options.firstBaseUrl
              : `http://127.0.0.1:${mockServers[i]!.port}/openai/v1`,
          api_key: "routing-fixture-secret",
          rpm: 100000,
          concurrency: 1000,
        })
      ).id,
    );
    providers.push(provider);
    routes.push(
      String(
        (
          await manage(`/models/${model}/routes`, {
            provider_id: provider,
            upstream_model: model,
            operations: ["chat"],
            priority: options.priorities?.[i] ?? 30 - i * 10,
            weight: options.weights?.[i] ?? 1,
            pricing: { currency: "USD", rates: costRates[i] },
            ceilings: {
              input_tokens: "16777216",
              output_tokens: "1000000",
              cached_input_tokens: "16777216",
              cache_write_tokens: "2000000",
              cache_write_5m_tokens: "1000000",
              cache_write_1h_tokens: "1000000",
              requests: "1",
              images: "1000",
              audio_ms: "3600000",
              music_ms: "3600000",
              video_ms: "3600000",
              audio_seconds: "3600",
              video_seconds: "3600",
              characters: "16777216",
            },
          })
        ).id,
      ),
    );
    await manage("/tariffs", {
      model,
      operation: "chat",
      provider_id: provider,
      pricing: { currency: "USD", rates: customerRates[i] },
      ceilings: { input_tokens: "7", cached_input_tokens: "3", output_tokens: "8", requests: "1" },
      basis: "PROVIDER_USAGE",
    });
  }
  const urls = instances.map((instance) => {
    const server = serve({
      ...instance,
      config: {
        ...instance.config,
        MAX_ATTEMPTS: options.attempts ?? 3,
        TOTAL_TIMEOUT_MS: options.timeout ?? 3500,
        HEADERS_TIMEOUT_MS: options.headersTimeout ?? 150,
      },
    });
    fixtureServers.push(server);
    return `http://127.0.0.1:${server.port}`;
  });
  return {
    project,
    model,
    key,
    providers,
    routes,
    async counts() {
      return Promise.all(
        mockServers.map(async (server) => {
          const response = await fetch(`http://127.0.0.1:${server.port}/__control/stats`, {
            headers: { authorization: `Bearer ${controlToken}` },
          });
          const stats = (await response.json()) as { attempts: Record<string, number> };
          return stats.attempts[model] ?? 0;
        }),
      );
    },
    send(extra: Record<string, unknown> = {}, instance = 0, signal?: AbortSignal, intent?: string) {
      return fetch(`${urls[instance]}/v1/chat/completions`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${key}`,
          "content-type": "application/json",
          ...(intent ? { "idempotency-key": intent } : {}),
        },
        body: JSON.stringify({
          model,
          messages: [{ role: "user", content: "route me" }],
          max_completion_tokens: 8,
          ...extra,
        }),
        ...(signal ? { signal } : {}),
      });
    },
  };
}

async function audit(
  f: Awaited<ReturnType<typeof fixture>>,
  spent: string,
  reserved = "0",
  funded = "100000",
) {
  await drain();
  const [wallet] =
    await db`SELECT available_atoms::text AS available,reserved_atoms::text AS reserved,spent_atoms::text AS spent,debt_atoms::text AS debt,expired_atoms::text AS expired FROM wallets WHERE project_id=${f.project} AND currency='USD'`;
  expect(wallet).toEqual({
    available: String(BigInt(funded) - BigInt(spent) - BigInt(reserved)),
    reserved,
    spent,
    debt: "0",
    expired: "0",
  });
  const inconsistent = await db`SELECT w.id FROM wallets w WHERE w.project_id=${f.project} AND (
    w.available_atoms<>(SELECT coalesce(sum(available_atoms),0) FROM wallet_lots WHERE wallet_id=w.id)
    OR w.reserved_atoms<>(SELECT coalesce(sum(reserved_atoms),0) FROM wallet_lots WHERE wallet_id=w.id)
    OR w.spent_atoms<>(SELECT coalesce(sum(spent_atoms),0) FROM wallet_lots WHERE wallet_id=w.id)
    OR w.expired_atoms<>(SELECT coalesce(sum(expired_atoms),0) FROM wallet_lots WHERE wallet_id=w.id)
    OR w.spent_atoms<>(SELECT coalesce(sum(c.funded_atoms),0) FROM billing_charges c JOIN billing_holds h ON h.id=c.hold_id WHERE h.wallet_id=w.id)
    OR w.reserved_atoms<>(SELECT coalesce(sum(amount_atoms),0) FROM billing_holds WHERE wallet_id=w.id AND state IN ('HELD','RECONCILING')))`;
  expect(inconsistent).toHaveLength(0);
  expect(
    await db`SELECT j.id FROM money_journal j JOIN wallets w ON w.id=j.wallet_id WHERE w.project_id=${f.project} AND (SELECT sum((line->>'atoms')::numeric) FROM jsonb_array_elements(j.lines) line)<>0`,
  ).toHaveLength(0);
  expect(
    await db`SELECT l.id FROM wallet_lots l JOIN wallets w ON w.id=l.wallet_id WHERE w.project_id=${f.project} AND (
    l.available_atoms<>(SELECT coalesce(sum((line->>'atoms')::numeric),0) FROM money_journal j CROSS JOIN LATERAL jsonb_array_elements(j.lines) line WHERE j.wallet_id=w.id AND line->>'account'='lot:'||l.id||':available')
    OR l.reserved_atoms<>(SELECT coalesce(sum((line->>'atoms')::numeric),0) FROM money_journal j CROSS JOIN LATERAL jsonb_array_elements(j.lines) line WHERE j.wallet_id=w.id AND line->>'account'='lot:'||l.id||':reserved')
    OR l.expired_atoms<>(SELECT coalesce(sum((line->>'atoms')::numeric),0) FROM money_journal j CROSS JOIN LATERAL jsonb_array_elements(j.lines) line WHERE j.wallet_id=w.id AND line->>'account'='lot:'||l.id||':expired')
    OR l.spent_atoms<>(SELECT coalesce(sum(amount_atoms-refunded_atoms),0) FROM charge_allocations WHERE lot_id=l.id)
    OR l.issued_atoms<>(SELECT coalesce(sum(amount_atoms),0) FROM funding_operations WHERE lot_id=l.id))`,
  ).toHaveLength(0);
  expect(
    (
      await db`SELECT coalesce(sum((line->>'atoms')::numeric),0)::text AS revenue FROM money_journal j JOIN wallets w ON w.id=j.wallet_id CROSS JOIN LATERAL jsonb_array_elements(j.lines) line WHERE w.project_id=${f.project} AND line->>'account'='system:revenue'`
    )[0]!.revenue,
  ).toBe(spent);
  expect(
    await db`SELECT c.id FROM billing_charges c JOIN gateway_requests q ON q.id=c.request_id JOIN billing_holds h ON h.id=c.hold_id LEFT JOIN attempts a ON a.id=q.selected_attempt_id WHERE q.project_id=${f.project} AND (h.request_id<>q.id OR c.currency<>'USD' OR c.debt_atoms<>0 OR c.refunded_atoms<>0 OR c.amount_atoms<>c.funded_atoms OR (c.amount_atoms>0 AND (c.evidence_id IS DISTINCT FROM a.evidence_id OR c.pricing_id IS DISTINCT FROM a.billing_pricing_id)))`,
  ).toHaveLength(0);
  for (;;) {
    const page = await instances[0]!.store.claimEvents(consumer, 500);
    if (!page.events.length) break;
    expect(
      await instances[0]!.store.ackEvents(
        consumer,
        page.lease_token,
        page.events.map((event) => event.id),
      ),
    ).toBe(page.events.length);
  }
  expect(
    await db`SELECT e.id FROM usage_events e LEFT JOIN event_deliveries d ON d.event_id=e.id AND d.consumer_id=${consumer} WHERE e.project_id=${f.project} AND d.state IS DISTINCT FROM 'ACKED'`,
  ).toHaveLength(0);
  for (const scope of [`project:${f.project}`, ...f.providers.map((id) => `provider:${id}`)])
    expect(
      Number(await instances[0]!.limits.command("ZCARD", [`${namespace}:{${scope}}:active`])),
    ).toBe(0);
  return db<
    {
      id: string;
      provider_id: string;
      ordinal: number;
      state: string;
      finality: string | null;
      cost: string | null;
      cost_complete: boolean | null;
      usage: Record<string, string>;
    }[]
  >`SELECT a.id,a.provider_id,a.ordinal,a.state,e.finality,e.cost_atoms::text AS cost,e.cost_complete,e.usage FROM attempts a JOIN gateway_requests q ON q.id=a.request_id LEFT JOIN usage_events e ON e.id=a.evidence_id WHERE q.project_id=${f.project} ORDER BY q.created_at,q.id,a.ordinal`;
}

test("incompatible route policies are excluded from dispatch and the captured customer quote", async () => {
  const f = await fixture();
  await manage(
    `/routes/${f.routes[0]}`,
    { request_policy: { blocked_parameters: ["temperature"] } },
    "PATCH",
  );
  const response = await f.send({ temperature: 0 });
  expect(response.status).toBe(200);
  await response.json();
  const attempts = await audit(f, "18");
  expect(await f.counts()).toEqual([0, 1, 0]);
  expect(attempts.map((a) => a.provider_id)).toEqual([f.providers[1]!]);
  const [request] =
    await db`SELECT billing_quote FROM gateway_requests WHERE project_id=${f.project}`;
  expect(Object.keys(request!.billing_quote.prices).sort()).toEqual(
    [f.providers[1]!, f.providers[2]!].sort(),
  );
});

test("all model policies rejecting a request leaves no hold and policy replacement restores dispatch", async () => {
  const f = await fixture();
  for (const id of f.routes)
    await manage(
      `/routes/${id}`,
      { request_policy: { parameter_ranges: { max_completion_tokens: { maximum: 4 } } } },
      "PATCH",
    );
  const denied = await f.send();
  expect(denied.status).toBe(422);
  expect(await denied.json()).toMatchObject({ error: { code: "unsupported_capability" } });
  expect(await f.counts()).toEqual([0, 0, 0]);
  expect((await db`SELECT id FROM gateway_requests WHERE project_id=${f.project}`).length).toBe(0);
  await audit(f, "0");
  await manage(`/routes/${f.routes[0]}`, { request_policy: {} }, "PATCH");
  const accepted = await f.send();
  expect(accepted.status).toBe(200);
  await accepted.json();
  expect(await f.counts()).toEqual([1, 0, 0]);
  await audit(f, "32");
});

test("Batch row incompatibility skips its provider before quoting and uploads through a compatible provider", async () => {
  const f = await fixture();
  await manage(`/models/${f.model}`, { operations: ["chat", "files", "batches"] }, "PATCH");
  for (let i = 0; i < f.providers.length; i++)
    await manage(`/models/${f.model}/routes`, {
      provider_id: f.providers[i],
      upstream_model: f.model,
      operations: ["files"],
      priority: 30 - i * 10,
      weight: 1,
      pricing: { currency: "USD", rates: { requests: { numerator: "1", denominator: "1" } } },
      ceilings: { requests: "1" },
    });
  await manage("/tariffs", {
    model: f.model,
    operation: "files",
    pricing: { currency: "USD", rates: { requests: { numerator: "1", denominator: "1" } } },
    ceilings: { requests: "1" },
    basis: "REQUEST_COMPLETION",
  });
  await manage(
    `/routes/${f.routes[0]}`,
    { request_policy: { blocked_parameters: ["temperature"] } },
    "PATCH",
  );
  const form = new FormData();
  form.set("purpose", "batch");
  form.set(
    "file",
    new File(
      [
        JSON.stringify({
          custom_id: "policy",
          method: "POST",
          url: "/v1/chat/completions",
          body: { model: f.model, messages: [{ role: "user", content: "x" }], temperature: 0 },
        }),
      ],
      "policy.jsonl",
    ),
  );
  const response = await fetch(`${origin}/v1/files`, {
    method: "POST",
    headers: { authorization: `Bearer ${f.key}` },
    body: form,
  });
  expect(response.status).toBe(200);
  const resource = await instances[0]!.store.resource(
    String(((await response.json()) as { id: string }).id),
    f.project,
  );
  expect(resource.provider_id).toBe(f.providers[1]!);
  await audit(f, "1");
  const [request] =
    await db`SELECT billing_quote FROM gateway_requests WHERE project_id=${f.project}`;
  expect(Object.keys(request!.billing_quote.prices).sort()).toEqual(
    [f.providers[1]!, f.providers[2]!].sort(),
  );
});

for (const status of [408, 409, 429, 500, 502, 503, 504, 529])
  test(`HTTP ${status} switches to a healthy provider and charges its fractional tariff exactly once`, async () => {
    const f = await fixture([{ status }, {}, {}]);
    const response = await f.send();
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("channel-1");
    expect(await f.counts()).toEqual([1, 1, 0]);
    const rows = await audit(f, "18");
    expect(
      rows.map((row) => [row.provider_id, row.state, row.finality, row.cost_complete]),
    ).toEqual([
      [f.providers[0]!, "UNKNOWN", "UNKNOWN", false],
      [f.providers[1]!, "SUCCEEDED", "FINAL", true],
    ]);
    expect(rows[1]!.cost).toBe("83");
    const receipt = await instances[0]!.funds.receipt(
      response.headers.get("x-gateway-request-id")!,
      f.project,
    );
    expect(receipt).toMatchObject({
      charged_atoms: "18",
      funded_atoms: "18",
      debt_atoms: "0",
      hold_state: "SETTLED",
    });
  });

for (const status of [401, 403, 404])
  test(`HTTP ${status} on one channel switches without charging the rejected attempt`, async () => {
    const f = await fixture([{ status }, {}, {}]);
    const response = await f.send();
    expect(response.status).toBe(200);
    await response.text();
    expect(await f.counts()).toEqual([1, 1, 0]);
    const rows = await audit(f, "18");
    expect(rows.map((row) => row.state)).toEqual(["NOT_EXECUTED", "SUCCEEDED"]);
  });

for (const status of [400, 422])
  test(`HTTP ${status} stops retries and releases the complete hold`, async () => {
    const f = await fixture([{ status }, {}, {}]);
    const response = await f.send();
    expect(response.status).toBe(400);
    await response.text();
    expect(await f.counts()).toEqual([1, 0, 0]);
    expect((await audit(f, "0")).map((row) => row.state)).toEqual(["NOT_EXECUTED"]);
    expect(
      (
        await db`SELECT h.state,c.amount_atoms::text AS amount FROM billing_holds h JOIN billing_charges c ON c.hold_id=h.id JOIN gateway_requests q ON q.id=h.request_id WHERE q.project_id=${f.project}`
      )[0],
    ).toEqual({ state: "RELEASED", amount: "0" });
  });

test("all unhealthy channels stop at the global attempt budget and retain unknown funding", async () => {
  const f = await fixture([{ status: 503 }, { status: 502 }, { status: 504 }]);
  const response = await f.send();
  expect(response.status).toBe(502);
  await response.text();
  expect(await f.counts()).toEqual([1, 1, 1]);
  expect((await audit(f, "0", "41")).map((row) => row.state)).toEqual([
    "UNKNOWN",
    "UNKNOWN",
    "UNKNOWN",
  ]);
  expect(
    await db`SELECT c.id FROM billing_charges c JOIN gateway_requests q ON q.id=c.request_id WHERE q.project_id=${f.project}`,
  ).toHaveLength(0);
});

test("a one-attempt budget prevents fallback and preserves its unknown hold", async () => {
  const f = await fixture([{ status: 503 }, {}, {}], { attempts: 1 });
  const response = await f.send();
  expect(response.status).toBe(502);
  await response.text();
  expect(await f.counts()).toEqual([1, 0, 0]);
  expect(await audit(f, "0", "41")).toHaveLength(1);
});

test("a known costly failed attempt remains separate from the delivered provider's customer charge", async () => {
  const f = await fixture([{ status: 503, errorUsage: true }, {}, {}]);
  const response = await f.send();
  expect(response.status).toBe(200);
  await response.text();
  const rows = await audit(f, "18");
  expect(rows.map((row) => [row.state, row.finality, row.cost])).toEqual([
    ["PARTIAL", "PARTIAL", "59"],
    ["SUCCEEDED", "FINAL", "83"],
  ]);
});

for (const scenario of [{ malformed: true }, { headerDelayMs: 400 }])
  test(`${scenario.malformed ? "malformed JSON" : "header timeout"} falls back with its unknown evidence intact`, async () => {
    const f = await fixture([scenario, {}, {}]);
    const response = await f.send();
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("channel-1");
    expect(await f.counts()).toEqual([1, 1, 0]);
    expect((await audit(f, "18")).map((row) => row.state)).toEqual(["UNKNOWN", "SUCCEEDED"]);
  });

test("a refused upstream connection is never charged and routes to a healthy channel", async () => {
  const unavailable = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: () => new Response("unused"),
  });
  const firstBaseUrl = `http://127.0.0.1:${unavailable.port}/openai/v1`;
  await unavailable.stop(true);
  const f = await fixture(undefined, { firstBaseUrl });
  const response = await f.send();
  expect(response.status).toBe(200);
  expect(await response.text()).toContain("channel-1");
  expect(await f.counts()).toEqual([0, 1, 0]);
  const rows = await audit(f, "18");
  expect(rows.map((row) => row.state)).toEqual(["NOT_EXECUTED", "SUCCEEDED"]);
  expect(rows[0]!.cost).toBe("0");
});

test("an unusable successful payload retains reported cost while falling back", async () => {
  const f = await fixture([{ emptyChoices: true }, {}, {}]);
  const response = await f.send();
  expect(response.status).toBe(200);
  expect(await response.text()).toContain("channel-1");
  expect(await f.counts()).toEqual([1, 1, 0]);
  const rows = await audit(f, "18");
  expect(rows.map((row) => [row.state, row.cost])).toEqual([
    ["PARTIAL", "59"],
    ["SUCCEEDED", "83"],
  ]);
});

test("a failed-over charge consumes the selected price from gift funding and releases cash", async () => {
  const f = await fixture([{ status: 503 }, {}, {}]);
  await instances[0]!.funds.credit(f.project, {
    currency: "USD",
    external_ref: `${f.model}-gift`,
    amount_atoms: "40",
    kind: "GIFT",
    priority: 100,
    models: [f.model],
    operations: ["chat"],
  });
  await manage(`/projects/${f.project}/wallets/USD`, { policy: "GIFT_FIRST" }, "PATCH");
  const response = await f.send();
  expect(response.status).toBe(200);
  await response.text();
  await audit(f, "18", "0", "100040");
  expect(
    await db`SELECT l.kind,l.available_atoms::text AS available,l.reserved_atoms::text AS reserved,l.spent_atoms::text AS spent FROM wallet_lots l JOIN wallets w ON w.id=l.wallet_id WHERE w.project_id=${f.project} ORDER BY l.kind`,
  ).toEqual([
    { kind: "CASH", available: "100000", reserved: "0", spent: "0" },
    { kind: "GIFT", available: "22", reserved: "0", spent: "18" },
  ]);
});

test("an upstream stream that stalls before its first event can switch safely", async () => {
  const f = await fixture([{ firstEventDelayMs: 400 }, {}, {}]);
  const response = await f.send({ stream: true });
  expect(response.status).toBe(200);
  const body = await response.text();
  expect(body).toContain("channel-1");
  expect(body).toContain("[DONE]");
  expect(body).not.toContain("gateway_stream_error");
  expect(await f.counts()).toEqual([1, 1, 0]);
  await audit(f, "18");
});

test("a stream truncated before usage retains unknown funding and never dispatches a replacement generation", async () => {
  const f = await fixture([{ truncate: true }, {}, {}]);
  const response = await f.send({ stream: true });
  const body = await response.text();
  expect(body).toContain("channel-0");
  expect(body).toContain("gateway_stream_error");
  expect(body).not.toContain("[DONE]");
  expect(await f.counts()).toEqual([1, 0, 0]);
  const rows = await audit(f, "0", "41");
  expect(rows).toHaveLength(1);
  expect(rows[0]!.state).toBe("UNKNOWN");
  expect(rows[0]!.finality).toBe("UNKNOWN");
  expect(rows[0]!.cost_complete).toBe(false);
});

test("the total deadline interrupts Retry-After and prevents a later dispatch", async () => {
  const f = await fixture([{ status: 429 }, {}, {}], { timeout: 250 });
  await manage(`/routes/${f.routes[1]}`, { enabled: false }, "PATCH");
  await manage(`/routes/${f.routes[2]}`, { enabled: false }, "PATCH");
  const started = performance.now();
  const response = await f.send();
  expect(response.status).toBe(504);
  await response.text();
  expect(performance.now() - started).toBeLessThan(1000);
  expect(await f.counts()).toEqual([1, 0, 0]);
  await audit(f, "0", "41");
});

test("late usage for a failed-over attempt cannot replace or duplicate the delivered charge", async () => {
  const f = await fixture([{ status: 503 }, {}, {}]);
  const response = await f.send();
  expect(response.status).toBe(200);
  await response.text();
  const before = await audit(f, "18");
  const evidence = {
    attemptId: String(before[0]!.id),
    source: "late_provider_usage",
    state: "SUCCEEDED" as const,
    finality: "FINAL" as const,
    usage: { input_tokens: "7", cached_input_tokens: "3", output_tokens: "5", requests: "1" },
  };
  await instances[0]!.store.persistUsage([evidence]);
  await instances[0]!.store.persistUsage([evidence]);
  const after = await audit(f, "18");
  expect(after.map((row) => row.cost)).toEqual(["59", "83"]);
  expect(
    (await db`SELECT selected_attempt_id FROM gateway_requests WHERE project_id=${f.project}`)[0]!
      .selected_attempt_id,
  ).toBe(before[1]!.id);
  expect(
    await db`SELECT id FROM billing_charges WHERE request_id=${response.headers.get("x-gateway-request-id")}`,
  ).toHaveLength(1);
  expect(await f.counts()).toEqual([1, 1, 0]);
});

test("round-robin distributes real calls across equal priorities with provider-specific prices", async () => {
  const f = await fixture(undefined, { strategy: "ROUND_ROBIN", priorities: [10, 10, 10] });
  for (let i = 0; i < 3; i++) {
    const response = await f.send();
    expect(response.status).toBe(200);
    await response.text();
  }
  expect(await f.counts()).toEqual([1, 1, 1]);
  await audit(f, "79");
});

test("least-latency uses shared provider measurements while priority remains dominant", async () => {
  const f = await fixture(undefined, { strategy: "LEAST_LATENCY", priorities: [10, 10, 10] });
  await instances[0]!.limits.observe(f.providers[0]!, false, 400);
  await instances[0]!.limits.observe(f.providers[1]!, false, 10);
  await instances[0]!.limits.observe(f.providers[2]!, false, 200);
  const response = await f.send({}, 1);
  expect(response.status).toBe(200);
  expect(await response.text()).toContain("channel-1");
  expect(await f.counts()).toEqual([0, 1, 0]);
  await audit(f, "18");
});

test("higher route priority wins over a faster lower-priority provider", async () => {
  const f = await fixture(undefined, { strategy: "LEAST_LATENCY" });
  await instances[0]!.limits.observe(f.providers[0]!, false, 400);
  await instances[0]!.limits.observe(f.providers[1]!, false, 10);
  const response = await f.send();
  expect(response.status).toBe(200);
  expect(await response.text()).toContain("channel-0");
  expect(await f.counts()).toEqual([1, 0, 0]);
  await audit(f, "32");
});

test("weighted routing changes its preferred channel after live weight updates", async () => {
  const f = await fixture(undefined, { priorities: [10, 10, 10], weights: [1, 9, 1] });
  const random = spyOn(Math, "random");
  try {
    random.mockReturnValue(0.5);
    let response = await f.send();
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("channel-1");
    await manage(`/routes/${f.routes[1]}`, { weight: 1 }, "PATCH");
    await manage(`/routes/${f.routes[2]}`, { weight: 9 }, "PATCH");
    response = await f.send();
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("channel-2");
  } finally {
    random.mockRestore();
  }
  await audit(f, "47");
});

test("concurrent duplicate intents across gateways execute one failed-over generation and one charge", async () => {
  const f = await fixture([{ status: 401 }, {}, {}]);
  const intent = crypto.randomUUID();
  const responses = await Promise.all(
    Array.from({ length: 20 }, (_, i) => f.send({}, i % 2, undefined, intent)),
  );
  expect(responses.filter((response) => response.status === 200)).toHaveLength(1);
  expect(responses.filter((response) => response.status === 409)).toHaveLength(19);
  await Promise.all(responses.map((response) => response.text()));
  expect(await f.counts()).toEqual([1, 1, 0]);
  expect(await audit(f, "18")).toHaveLength(2);
});

test("a single channel honors Retry-After and succeeds on the final allowed attempt", async () => {
  const f = await fixture([{ status: 503, failFirst: 2 }, {}, {}]);
  await manage(`/routes/${f.routes[1]}`, { enabled: false }, "PATCH");
  await manage(`/routes/${f.routes[2]}`, { enabled: false }, "PATCH");
  const response = await f.send();
  expect(response.status).toBe(200);
  await response.text();
  expect(await f.counts()).toEqual([3, 0, 0]);
  const times = mocks[0]!.arrivals.get(f.model)!;
  expect(times[1]! - times[0]!).toBeGreaterThanOrEqual(950);
  expect(times[2]! - times[1]!).toBeGreaterThanOrEqual(950);
  expect((await audit(f, "32")).map((row) => row.state)).toEqual([
    "UNKNOWN",
    "UNKNOWN",
    "SUCCEEDED",
  ]);
});

test("a five-attempt budget is shared across providers and repeated visits", async () => {
  const f = await fixture([{ status: 503 }, { status: 503 }, { status: 503 }], { attempts: 5 });
  const response = await f.send();
  expect(response.status).toBe(502);
  await response.text();
  expect(await f.counts()).toEqual([2, 2, 1]);
  expect(await audit(f, "0", "41")).toHaveLength(5);
});

test("an HTTP-date Retry-After delays redispatch until the advertised time", async () => {
  const f = await fixture([{ status: 429, failFirst: 1 }, {}, {}]);
  await manage(`/routes/${f.routes[1]}`, { enabled: false }, "PATCH");
  await manage(`/routes/${f.routes[2]}`, { enabled: false }, "PATCH");
  const retryAt = Math.ceil(Date.now() / 1000) * 1000 + 1000;
  mocks[0]!.retryHeaders.set(f.model, new Date(retryAt).toUTCString());
  const response = await f.send();
  expect(response.status).toBe(200);
  await response.text();
  expect(await f.counts()).toEqual([2, 0, 0]);
  expect(mocks[0]!.arrivals.get(f.model)![1]!).toBeGreaterThanOrEqual(retryAt - 10);
  await audit(f, "32");
});

test("an invalid Retry-After falls back to bounded backoff and a valid result", async () => {
  const f = await fixture([{ status: 429, failFirst: 1 }, {}, {}], { timeout: 1500 });
  await manage(`/routes/${f.routes[1]}`, { enabled: false }, "PATCH");
  await manage(`/routes/${f.routes[2]}`, { enabled: false }, "PATCH");
  mocks[0]!.retryHeaders.set(f.model, "unparseable");
  const response = await f.send();
  expect(response.status).toBe(200);
  await response.text();
  expect(await f.counts()).toEqual([2, 0, 0]);
  await audit(f, "32");
});

test("a long Retry-After cannot extend the total request budget", async () => {
  const f = await fixture([{ status: 429 }, {}, {}], { timeout: 250 });
  await manage(`/routes/${f.routes[1]}`, { enabled: false }, "PATCH");
  await manage(`/routes/${f.routes[2]}`, { enabled: false }, "PATCH");
  mocks[0]!.retryHeaders.set(f.model, "86400");
  const response = await f.send();
  expect(response.status).toBe(504);
  await response.text();
  expect(await f.counts()).toEqual([1, 0, 0]);
  await audit(f, "0", "41");
});

test("a full provider concurrency budget falls back without creating a denied attempt", async () => {
  const f = await fixture();
  await manage(`/providers/${f.providers[0]}`, { concurrency: 1 }, "PATCH");
  const release = await instances[0]!.limits.enter(
    `provider:${f.providers[0]}`,
    "occupied",
    100000,
    1,
    10000,
  );
  try {
    const response = await f.send();
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("channel-1");
    expect(await f.counts()).toEqual([0, 1, 0]);
  } finally {
    await release();
  }
  expect(await audit(f, "18")).toHaveLength(1);
});

test("disabled routes and providers are excluded before any upstream call", async () => {
  const f = await fixture();
  await manage(`/routes/${f.routes[0]}`, { enabled: false }, "PATCH");
  await manage(`/providers/${f.providers[1]}`, { enabled: false }, "PATCH");
  const response = await f.send();
  expect(response.status).toBe(200);
  expect(await response.text()).toContain("channel-2");
  expect(await f.counts()).toEqual([0, 0, 1]);
  await audit(f, "29");
});

test("all disabled channels reject before reserving funds or creating attempts", async () => {
  const f = await fixture();
  for (const route of f.routes) await manage(`/routes/${route}`, { enabled: false }, "PATCH");
  const response = await f.send();
  expect(response.status).toBe(503);
  await response.text();
  expect(await f.counts()).toEqual([0, 0, 0]);
  expect(await audit(f, "0")).toHaveLength(0);
  expect(
    await db`SELECT id FROM billing_holds WHERE request_id IN (SELECT id FROM gateway_requests WHERE project_id=${f.project})`,
  ).toHaveLength(0);
});

test("an oversized authorization error cannot be treated as known unexecuted usage", async () => {
  const f = await fixture([{ status: 401, errorBodyBytes: 70000 }, {}, {}], { attempts: 1 });
  const response = await f.send();
  expect(response.status).toBe(400);
  await response.text();
  expect(await f.counts()).toEqual([1, 0, 0]);
  const rows = await audit(f, "0", "41");
  expect(rows[0]!.state).toBe("UNKNOWN");
  expect(rows[0]!.cost_complete).toBe(false);
});

test("client cancellation after a retryable rejection releases permits and prevents another dispatch", async () => {
  const f = await fixture([{ status: 429 }, {}, {}]);
  await manage(`/routes/${f.routes[1]}`, { enabled: false }, "PATCH");
  await manage(`/routes/${f.routes[2]}`, { enabled: false }, "PATCH");
  const controller = new AbortController();
  const result = f.send({}, 0, controller.signal).catch((error: unknown) => error);
  try {
    await waitFor(
      async () =>
        (
          await db`SELECT a.id FROM attempts a JOIN gateway_requests q ON q.id=a.request_id WHERE q.project_id=${f.project} AND a.error_code='upstream_http_429' AND a.evidence_id IS NOT NULL`
        ).length === 1,
    );
    expect(await f.counts()).toEqual([1, 0, 0]);
    controller.abort();
    expect(await result).toBeInstanceOf(Error);
    await audit(f, "0", "41");
    expect(await f.counts()).toEqual([1, 0, 0]);
  } finally {
    controller.abort();
    await result;
  }
});

test("a frozen wallet rejects routing before any provider executes", async () => {
  const f = await fixture();
  await manage(`/projects/${f.project}/wallets/USD`, { frozen: true }, "PATCH");
  const response = await f.send();
  expect(response.status).toBe(402);
  await response.text();
  expect(await f.counts()).toEqual([0, 0, 0]);
  expect(await audit(f, "0")).toHaveLength(0);
});

test("a completed response with missing token dimensions holds funds until final evidence arrives exactly once", async () => {
  const f = await fixture([{ omitUsage: true }, {}, {}]);
  const response = await f.send();
  expect(response.status).toBe(200);
  await response.text();
  const rows = await audit(f, "0", "41");
  expect(rows[0]!.state).toBe("SUCCEEDED");
  expect(rows[0]!.finality).toBe("FINAL");
  expect(rows[0]!.usage).toEqual({ requests: "1" });
  expect(rows[0]!.cost_complete).toBe(false);
  const evidence = {
    attemptId: rows[0]!.id,
    source: "late_provider_usage",
    state: "SUCCEEDED" as const,
    finality: "FINAL" as const,
    usage: { input_tokens: "7", cached_input_tokens: "3", output_tokens: "5", requests: "1" },
  };
  await instances[0]!.store.persistUsage([evidence]);
  await instances[0]!.store.persistUsage([evidence]);
  expect((await audit(f, "32"))[0]!.cost).toBe("59");
  expect(await f.counts()).toEqual([1, 0, 0]);
});

test("concurrent independent failovers on one wallet produce ten exact charges", async () => {
  const f = await fixture([{ status: 401 }, {}, {}]);
  const responses = await Promise.all(Array.from({ length: 10 }, (_, i) => f.send({}, i % 2)));
  expect(responses.every((response) => response.status === 200)).toBe(true);
  await Promise.all(responses.map((response) => response.text()));
  const counts = await f.counts();
  expect(counts[0]!).toBeGreaterThanOrEqual(1);
  expect(counts[0]!).toBeLessThanOrEqual(10);
  expect(counts.slice(1)).toEqual([10, 0]);
  await audit(f, "180");
  expect(
    await db`SELECT c.id FROM billing_charges c JOIN gateway_requests q ON q.id=c.request_id WHERE q.project_id=${f.project}`,
  ).toHaveLength(10);
});

test("operator reconciliation releases an all-unknown hold once without replaying generation", async () => {
  const f = await fixture([{ status: 503 }, { status: 503 }, { status: 503 }]);
  const response = await f.send();
  expect(response.status).toBe(502);
  await response.text();
  await audit(f, "0", "41");
  const request = (await db`SELECT id FROM gateway_requests WHERE project_id=${f.project}`)[0]!.id;
  const resolution = { external_ref: f.model, action: "RELEASE", reason: "ADMIN_WRITE_OFF" };
  await manage(`/requests/${request}/resolutions`, resolution);
  await manage(`/requests/${request}/resolutions`, resolution);
  await instances[0]!.funds.reconcileKnownHolds(256);
  await waitFor(
    async () =>
      (await instances[0]!.funds.receipt(String(request), f.project)).hold_state === "RELEASED",
  );
  await audit(f, "0");
  expect(await db`SELECT id FROM billing_charges WHERE request_id=${request}`).toHaveLength(1);
  expect(await db`SELECT id FROM billing_resolutions WHERE request_id=${request}`).toHaveLength(1);
  expect(await f.counts()).toEqual([1, 1, 1]);
});

test("released probe observations cannot overwrite a replacement probe or its result", async () => {
  const provider = crypto.randomUUID();
  for (let i = 0; i < 3; i++) await instances[0]!.limits.observe(provider, true, 10, 10);
  await Bun.sleep(20);
  const old = await instances[0]!.limits.probe(provider, 1000, "old");
  expect(old).toBeDefined();
  await old!();
  const current = await instances[1]!.limits.probe(provider, 1000, "current");
  expect(current).toBeDefined();
  try {
    await old!.observe(false, 10);
    expect(await instances[0]!.limits.probe(provider, 1000, "excess")).toBeUndefined();
    await current!.observe(false, 20);
    for (let i = 0; i < 3; i++) await old!.observe(true, 10);
    expect((await instances[0]!.limits.health(provider)).open).toBe(false);
  } finally {
    await current!();
  }
});

test("expired probe observations cannot silently remove half-open admission", async () => {
  const provider = crypto.randomUUID();
  for (let i = 0; i < 3; i++) await instances[0]!.limits.observe(provider, true, 10, 10);
  await Bun.sleep(20);
  const expired = await instances[0]!.limits.probe(provider, 5, "expired");
  expect(expired).toBeDefined();
  await Bun.sleep(20);
  await expired!.observe(false, 10);
  const current = await instances[1]!.limits.probe(provider, 1000, "current");
  expect(current).toBeDefined();
  try {
    expect(await instances[0]!.limits.probe(provider, 1000, "excess")).toBeUndefined();
    await current!.observe(true, 10);
    await expired!.observe(false, 10);
    expect((await instances[0]!.limits.health(provider)).open).toBe(true);
  } finally {
    await current!();
  }
});

test("a pre-breaker success cannot erase failures from a recovered circuit generation", async () => {
  const provider = crypto.randomUUID();
  const old = await instances[0]!.limits.probe(provider, 1000, "old-closed");
  expect(old).toBeDefined();
  const failing = await Promise.all(
    Array.from({ length: 3 }, (_, i) => instances[1]!.limits.probe(provider, 1000, `failed-${i}`)),
  );
  for (const permit of failing) {
    expect(permit).toBeDefined();
    await permit!.observe(true, 10);
    await permit!();
  }
  await Bun.sleep(10020);
  const probe = await instances[1]!.limits.probe(provider, 1000, "recovery");
  expect(probe).toBeDefined();
  try {
    await probe!.observe(false, 10);
    const current = await Promise.all(
      Array.from({ length: 3 }, (_, i) =>
        instances[1]!.limits.probe(provider, 1000, `current-${i}`),
      ),
    );
    for (const permit of current) expect(permit).toBeDefined();
    await current[0]!.observe(true, 10);
    await current[1]!.observe(true, 10);
    await old!.observe(false, 10);
    await current[2]!.observe(true, 10);
    expect((await instances[0]!.limits.health(provider)).open).toBe(true);
    await Promise.all(current.map((permit) => permit!()));
  } finally {
    await old!();
    await probe!();
  }
});

test("a long half-open lease cannot expire with the shorter health retention window", async () => {
  const provider = crypto.randomUUID();
  for (let i = 0; i < 3; i++) await instances[0]!.limits.observe(provider, true, 10, 10);
  await Bun.sleep(20);
  const probe = await instances[0]!.limits.probe(provider, 700000, "long-probe");
  expect(probe).toBeDefined();
  try {
    expect(
      Number(await instances[0]!.limits.command("PTTL", [`${namespace}:health:${provider}`])),
    ).toBeGreaterThanOrEqual(700000);
    expect(await instances[1]!.limits.probe(provider, 1000, "excess")).toBeUndefined();
  } finally {
    await probe!();
  }
});

test("a late pre-breaker success cannot close a circuit while its new probe is pending", async () => {
  const f = await fixture(undefined, { timeout: 25000, headersTimeout: 20000 });
  const oldGate = Promise.withResolvers<void>();
  const probeGate = Promise.withResolvers<void>();
  mocks[0]!.gates.set(f.model, [oldGate.promise, probeGate.promise]);
  const oldRequest = f.send().then(async (response) => {
    expect(response.status).toBe(200);
    return response.text();
  });
  let probeRequest: Promise<string> | undefined;
  try {
    await waitFor(async () => (await f.counts())[0] === 1);
    mocks[0]!.scenarios.set(f.model, { status: 503 });
    for (let i = 0; i < 3; i++) {
      const response = await f.send({}, 1);
      expect(response.status).toBe(200);
      await response.text();
    }
    expect((await instances[0]!.limits.health(f.providers[0]!)).open).toBe(true);
    mocks[0]!.scenarios.set(f.model, { output: "channel-0" });
    await Bun.sleep(10020);
    probeRequest = f.send({}, 1).then(async (response) => {
      expect(response.status).toBe(200);
      return response.text();
    });
    await waitFor(async () => (await f.counts())[0] === 5);
    oldGate.resolve();
    expect(await oldRequest).toContain("channel-0");
    const response = await f.send();
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("channel-1");
    expect(await f.counts()).toEqual([5, 4, 0]);
  } finally {
    oldGate.resolve();
    probeGate.resolve();
    mocks[0]!.gates.delete(f.model);
    await oldRequest;
    await probeRequest;
  }
  await audit(f, "136");
});

test("circuit opening skips a failed channel and recovery permits a single cross-instance probe", async () => {
  const f = await fixture([{ status: 503 }, {}, {}], { headersTimeout: 2000 });
  for (let i = 0; i < 4; i++) {
    const response = await f.send({}, i % 2);
    expect(response.status).toBe(200);
    await response.text();
  }
  expect(await f.counts()).toEqual([3, 4, 0]);
  expect((await instances[1]!.limits.health(f.providers[0]!)).open).toBe(true);
  mocks[0]!.scenarios.set(f.model, { output: "channel-0" });
  const gate = Promise.withResolvers<void>();
  mocks[0]!.gates.set(f.model, [gate.promise]);
  await Bun.sleep(10020);
  let completed = 0;
  const requests = Array.from({ length: 12 }, (_, i) =>
    f.send({}, i % 2).then(async (response) => {
      expect(response.status).toBe(200);
      const body = await response.text();
      completed++;
      return body;
    }),
  );
  try {
    await waitFor(async () => completed === 11, 1500);
    expect(await f.counts()).toEqual([4, 15, 0]);
  } finally {
    gate.resolve();
    mocks[0]!.gates.delete(f.model);
    await Promise.all(requests);
  }
  expect((await instances[1]!.limits.health(f.providers[0]!)).open).toBe(false);
  const response = await f.send();
  expect(await response.text()).toContain("channel-0");
  expect(await f.counts()).toEqual([5, 15, 0]);
  await audit(f, "334");
});
