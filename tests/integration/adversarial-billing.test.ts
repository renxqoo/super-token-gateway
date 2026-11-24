import { afterAll, beforeAll, expect, test } from "bun:test";
import type { SQL } from "bun";
import { createDatabase, migrate } from "@gateway/ledger";
import { createGatewayRuntime, type GatewayConfig } from "@gateway/gateway";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const testDbName = `gateway_adv_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const adminUrl = new URL(baseDbUrl);
adminUrl.pathname = "/postgres";
const targetUrl = new URL(baseDbUrl);
targetUrl.pathname = `/${testDbName}`;
const ADMIN_TOKEN = "adv-admin-token-0123456789abcdef";

let adminDb: SQL;
let db: SQL;

beforeAll(async () => {
  adminDb = createDatabase(adminUrl.toString(), 2);
  await adminDb.unsafe(`CREATE DATABASE ${testDbName}`);
  db = createDatabase(targetUrl.toString(), 16);
  await migrate(db);
});
afterAll(async () => {
  await db?.close();
  if (adminDb) {
    await adminDb.unsafe(
      `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${testDbName}' AND pid <> pg_backend_pid()`,
    );
    await adminDb.unsafe(`DROP DATABASE IF EXISTS ${testDbName}`);
    await adminDb.close();
  }
});

function makeConfig(overrides: Partial<GatewayConfig> = {}): GatewayConfig {
  return {
    mode: "gateway",
    host: "127.0.0.1",
    port: 0,
    internalHost: "127.0.0.1",
    internalPort: 0,
    databaseUrl: targetUrl.toString(),
    dbMaxConnections: 16,
    keyPepper: "adv-pepper-secret-0123456789abcdef",
    encryptionKey: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    adminToken: ADMIN_TOKEN,
    allowPrivateUpstreams: true,
    privateUpstreamCidrs: ["127.0.0.0/8"],
    trustedProxyCidrs: [],
    maxInFlight: 500,
    maxBodyBytes: 2 * 1024 * 1024,
    holdTtlMs: 30_000,
    attemptTimeoutMs: 5000,
    snapshotPollMs: 0,
    microBatchWindowMs: 2,
    microBatchMaxSize: 32,
    ...overrides,
  };
}

async function internalCall(
  app: { fetch: (req: Request) => Response | Promise<Response> },
  method: string,
  path: string,
  body?: unknown,
  token = ADMIN_TOKEN,
) {
  const res = await app.fetch(
    new Request(`http://internal.local${path}`, {
      method,
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    }),
  );
  const text = await res.text();
  let data: Record<string, unknown> = {};
  try {
    data = JSON.parse(text) as Record<string, unknown>;
  } catch {
    data = { raw: text };
  }
  return { status: res.status, data, headers: res.headers };
}

type Wallet = {
  available_atoms: string;
  reserved_atoms: string;
  spent_atoms: string;
};

async function wallet(runtime: ReturnType<typeof createGatewayRuntime>, projectId: string) {
  const r = await internalCall(runtime.internalApp, "GET", `/internal/v1/wallets/${projectId}`);
  expect(r.status).toBe(200);
  return r.data as unknown as Wallet;
}

async function waitFor(condition: () => Promise<boolean>, ms = 4000) {
  const deadline = Date.now() + ms;
  while (!(await condition())) {
    if (Date.now() >= deadline) throw new Error("condition not met in time");
    await Bun.sleep(20);
  }
}

async function seed(runtime: ReturnType<typeof createGatewayRuntime>, mockOrigin: string) {
  const call = async (method: string, path: string, body?: unknown) => {
    const r = await internalCall(runtime.internalApp, method, path, body);
    expect(r.status, JSON.stringify(r.data)).toBe(path === "/internal/v1/prices" ? 201 : 200);
    return r.data as Record<string, unknown>;
  };
  const project = await call("PUT", "/internal/v1/projects", {
    name: `adv-${crypto.randomUUID().slice(0, 8)}`,
    currency: "USD",
    rpm: 1_000_000,
    tpm: 100_000_000,
    concurrency: 1000,
  });
  const projectId = String(project.id);
  await call("POST", `/internal/v1/wallets/${projectId}/credits`, {
    currency: "USD",
    external_ref: `adv-credit-${crypto.randomUUID()}`,
    amount_atoms: "1000000000",
    kind: "CASH",
  });
  const key = await call("PUT", "/internal/v1/api-keys", { project_id: projectId, name: "adv" });
  const account = await call("PUT", "/internal/v1/upstream-accounts", {
    name: `adv-up-${crypto.randomUUID().slice(0, 8)}`,
  });
  await call("POST", `/internal/v1/upstream-accounts/${account.id}/recharges`, {
    currency: "USD",
    external_ref: `adv-sup-${crypto.randomUUID()}`,
    amount_atoms: "1000000000",
  });
  const channel = await call("PUT", "/internal/v1/channels", {
    account_id: account.id,
    kind: "openai",
    name: `adv-ch-${crypto.randomUUID().slice(0, 8)}`,
    base_url: `${mockOrigin}/openai`,
    api_key: "mock-adv",
  });
  const customer = await call("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    currency: "USD",
    rates: { input_tokens: { numerator: "1", denominator: "1" } },
    ceilings: { input_tokens: "100" },
  });
  const supplier = await call("POST", "/internal/v1/prices", {
    side: "SUPPLIER",
    currency: "USD",
    rates: { input_tokens: { numerator: "1", denominator: "1" } },
    ceilings: { input_tokens: "100" },
  });
  return { projectId, key: String(key.key), channel, customer, supplier, call };
}

function chatRequest(key: string, model: string, extra: Record<string, unknown> = {}) {
  return new Request("http://gateway.local/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
    body: JSON.stringify({
      model,
      stream: true,
      messages: [{ role: "user", content: "attack" }],
      max_tokens: 50,
      ...extra,
    }),
  });
}

function realServer(runtime: ReturnType<typeof createGatewayRuntime>) {
  return Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: runtime.publicApp.fetch });
}

test("symptom: fixed input ceilings reject short text on a Key that can pay for the request", async () => {
  const mock = createMockApp("adv-ctrl");
  const mockServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: mock.app.fetch,
  });
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const { projectId, channel, supplier, call } = await seed(runtime, mockServer.url.origin);
    const model = "adv-dynamic-reservation";
    const customer = await call("POST", "/internal/v1/prices", {
      side: "CUSTOMER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        cached_input_tokens: { numerator: "1", denominator: "2" },
      },
      ceilings: { input_tokens: "100000", cached_input_tokens: "100000" },
    });
    await call("PUT", "/internal/v1/models", {
      name: model,
      operations: ["chat"],
      max_output_tokens: 50,
    });
    await call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: channel.id,
      upstream_model: model,
      operations: ["chat"],
      billing_price_id: customer.id,
      cost_version_id: supplier.id,
    });
    const createKey = (name: string, limit: string) =>
      call("PUT", "/internal/v1/api-keys", {
        project_id: projectId,
        name,
        spend_limit_atoms: limit,
      });
    const budget = await createKey("adv-small-budget", "1000");
    const tinyBudget = await createKey("adv-tiny-budget", "1");
    const request = (key: unknown, content: unknown) =>
      runtime.publicApp.fetch(
        chatRequest(String(key), model, {
          stream: false,
          messages: [{ role: "user", content }],
        }),
      );
    const short = await request(budget.key, "Hello");
    const output = await short.json();
    expect(short.status, JSON.stringify(output)).toBe(200);
    const shortId = short.headers.get("x-request-id")!;
    const [charge] = await db`
      SELECT h.held_atoms::text, c.customer_atoms::text
      FROM public.billing_holds h JOIN public.billing_charges c USING (request_id)
      WHERE h.request_id = ${shortId}
    `;
    expect(charge?.customer_atoms).toBe("9");
    expect(BigInt(charge?.held_atoms)).toBeLessThan(1000n);

    for (const [key, content] of [
      [budget.key, "a".repeat(2000)],
      [tinyBudget.key, "Hello"],
      [budget.key, [{ type: "image_url", image_url: { url: "https://example.invalid/image" } }]],
    ]) {
      const denied = await request(key, content);
      expect(denied.status).toBe(402);
      expect(await denied.json()).toMatchObject({ error: { code: "key_spend_limit_exceeded" } });
    }
  } finally {
    await runtime.close(1000);
    mockServer.stop(true);
  }
});

test("symptom: client cancels SSE mid-stream after consuming output -> hold is UNKNOWN, nothing spent, nothing refunded, funds fully reserved", async () => {
  const mock = createMockApp("adv-ctrl");
  const mockServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: mock.app.fetch,
  });
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const { projectId, key, channel, customer, supplier, call } = await seed(
      runtime,
      mockServer.url.origin,
    );
    const model = `adv-cancel-${crypto.randomUUID().slice(0, 6)}`;
    await call("PUT", "/internal/v1/models", {
      name: model,
      operations: ["chat"],
      max_output_tokens: 50,
    });
    await call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: channel.id,
      upstream_model: "adv-slow",
      operations: ["chat"],
      billing_price_id: customer.id,
      cost_version_id: supplier.id,
    });
    mock.scenarios.set("adv-slow", { chunkDelayMs: 40, repeat: 20 });

    const before = await wallet(runtime, projectId);
    const ac = new AbortController();
    const res = await runtime.publicApp.fetch(chatRequest(key, model), {
      signal: ac.signal,
    } as never);
    expect(res.status).toBe(200);
    const requestId = res.headers.get("x-request-id")!;
    const reader = res.body!.getReader();
    let received = "";
    for (let i = 0; i < 5; i++) {
      const step = await reader.read();
      if (step.done) break;
      received += new TextDecoder().decode(step.value);
    }
    expect(received).toContain("Hello");
    ac.abort();
    await reader.cancel().catch(() => undefined);

    await waitFor(async () => {
      const rows = await db`SELECT state FROM public.billing_holds WHERE request_id = ${requestId}`;
      return rows[0]?.state === "UNKNOWN";
    });
    expect(runtime.lifecycle.size).toBe(0);

    const during = await wallet(runtime, projectId);
    const holdRow = (
      await db`SELECT held_atoms::text AS h FROM public.billing_holds WHERE request_id = ${requestId}`
    )[0]!;
    const held = BigInt(holdRow.h as string);
    expect(BigInt(during.spent_atoms) - BigInt(before.spent_atoms)).toBe(0n);
    expect(BigInt(during.available_atoms)).toBe(BigInt(before.available_atoms) - held);
    expect(BigInt(during.reserved_atoms) - BigInt(before.reserved_atoms)).toBe(held);

    const audit = await internalCall(runtime.internalApp, "GET", "/internal/v1/audit/conservation");
    expect(audit.status).toBe(200);
    expect(audit.data.ok).toBe(true);
    expect(audit.data.unknown_holds).toBe(1);

    const resolve = await internalCall(
      runtime.internalApp,
      "POST",
      `/internal/v1/requests/${requestId}/resolve`,
      {
        action: "SETTLE",
        billing_price_id: customer.id,
        usage: { input_tokens: "10", output_tokens: "5" },
        supplier_cost_atoms: "15",
        reason: "ops-measured-actual-usage",
        actor: "ops",
      },
    );
    expect(resolve.status, JSON.stringify(resolve.data)).toBe(200);
    const after = await wallet(runtime, projectId);
    expect(BigInt(after.available_atoms)).toBe(BigInt(before.available_atoms) - 10n);
    expect(BigInt(after.spent_atoms) - BigInt(before.spent_atoms)).toBe(10n);
    expect(BigInt(after.reserved_atoms)).toBe(BigInt(before.reserved_atoms));
    const audit2 = await internalCall(
      runtime.internalApp,
      "GET",
      "/internal/v1/audit/conservation",
    );
    expect(audit2.data.ok).toBe(true);
    expect(audit2.data.unknown_holds).toBe(0);
    expect(audit2.data.open_holds).toBe(0);
  } finally {
    await runtime.close();
    await mockServer.stop(true);
  }
});

test("symptom: RELEASE on a cancelled request refunds exactly the hold and never more", async () => {
  const mock = createMockApp("adv-ctrl");
  const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const { projectId, key, channel, customer, supplier, call } = await seed(
      runtime,
      mockServer.url.origin,
    );
    const model = `adv-rel-${crypto.randomUUID().slice(0, 6)}`;
    await call("PUT", "/internal/v1/models", {
      name: model,
      operations: ["chat"],
      max_output_tokens: 50,
    });
    await call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: channel.id,
      upstream_model: "adv-slow",
      operations: ["chat"],
      billing_price_id: customer.id,
      cost_version_id: supplier.id,
    });
    mock.scenarios.set("adv-slow", { chunkDelayMs: 150, repeat: 20 });
    const before = await wallet(runtime, projectId);
    const res = await runtime.publicApp.fetch(chatRequest(key, model));
    const requestId = res.headers.get("x-request-id")!;
    const reader = res.body!.getReader();
    await reader.read();
    await reader.cancel();
    await waitFor(async () => {
      const rows = await db`SELECT state FROM public.billing_holds WHERE request_id = ${requestId}`;
      return rows[0]?.state === "UNKNOWN";
    });
    const resolve = await internalCall(
      runtime.internalApp,
      "POST",
      `/internal/v1/requests/${requestId}/resolve`,
      {
        action: "RELEASE",
        reason: "no-output-confirmed",
        actor: "ops",
      },
    );
    expect(resolve.status, JSON.stringify(resolve.data)).toBe(200);
    const after = await wallet(runtime, projectId);
    expect(after).toEqual(before);
    const again = await internalCall(
      runtime.internalApp,
      "POST",
      `/internal/v1/requests/${requestId}/resolve`,
      {
        action: "SETTLE",
        usage: { input_tokens: "10", output_tokens: "5" },
        reason: "double-resolve-attack",
        actor: "ops",
      },
    );
    expect(again.data).toMatchObject({ replayed: true, state: "RELEASE" });
    const after2 = await wallet(runtime, projectId);
    expect(after2).toEqual(before);
  } finally {
    await runtime.close();
    await mockServer.stop(true);
  }
});

test("symptom: a cancel storm of 100 concurrent SSE requests leaves funds exactly conserved with no negative balances and no duplicates", async () => {
  const mock = createMockApp("adv-ctrl");
  const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
  const runtime = createGatewayRuntime({ config: makeConfig({ maxInFlight: 2000 }), db });
  try {
    await runtime.start();
    const { projectId, key, channel, customer, supplier, call } = await seed(
      runtime,
      mockServer.url.origin,
    );
    const model = `adv-storm-${crypto.randomUUID().slice(0, 6)}`;
    await call("PUT", "/internal/v1/models", {
      name: model,
      operations: ["chat"],
      max_output_tokens: 50,
    });
    await call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: channel.id,
      upstream_model: "adv-storm",
      operations: ["chat"],
      billing_price_id: customer.id,
      cost_version_id: supplier.id,
    });
    mock.scenarios.set("adv-storm", { chunkDelayMs: 200, repeat: 30 });
    const before = await wallet(runtime, projectId);
    const publicServer = realServer(runtime);
    const N = 100;
    const results = await Promise.all(
      Array.from({ length: N }, async () => {
        const ac = new AbortController();
        const res = await fetch(`http://127.0.0.1:${publicServer.port}/v1/chat/completions`, {
          method: "POST",
          headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model,
            stream: true,
            messages: [{ role: "user", content: "attack" }],
            max_tokens: 50,
          }),
          signal: ac.signal,
        }).catch(() => null);
        if (!res || res.status !== 200)
          return { status: res?.status ?? 0, id: null as string | null };
        const id = res.headers.get("x-request-id");
        const reader = res.body!.getReader();
        await reader.read();
        await Bun.sleep(5 + Math.random() * 30);
        ac.abort();
        await reader.cancel().catch(() => undefined);
        return { status: 200, id };
      }),
    );
    publicServer.stop(true);
    expect(results.every((r) => r.status === 200)).toBe(true);
    const ids = results.map((r) => r.id!);
    expect(new Set(ids).size).toBe(N);
    await waitFor(
      async () =>
        (await db`SELECT count(*)::int AS n FROM public.billing_holds WHERE state = 'HELD'`)[0]!
          .n === 0,
    );
    expect(runtime.lifecycle.size).toBe(0);
    const holds = await db<
      { state: string; h: string }[]
    >`SELECT state, held_atoms::text AS h FROM public.billing_holds WHERE request_id = ANY(${ids})`;
    const totalHeld = holds.reduce((acc: bigint, h) => acc + BigInt(h.h), 0n);
    await waitFor(async () => {
      const w = await wallet(runtime, projectId);
      return BigInt(w.reserved_atoms) - BigInt(before.reserved_atoms) === totalHeld;
    }, 8000);
    expect(holds.length).toBe(N);
    expect(holds.every((h) => h.state === "UNKNOWN")).toBe(true);
    const during = await wallet(runtime, projectId);
    expect(BigInt(during.reserved_atoms) - BigInt(before.reserved_atoms)).toBe(totalHeld);
    expect(BigInt(during.spent_atoms)).toBe(BigInt(before.spent_atoms));
    expect(BigInt(during.available_atoms) >= 0n).toBe(true);
    const audit = await internalCall(runtime.internalApp, "GET", "/internal/v1/audit/conservation");
    expect(audit.data.ok).toBe(true);
    expect(audit.data.unknown_holds).toBe(N);
    for (const id of ids)
      await internalCall(runtime.internalApp, "POST", `/internal/v1/requests/${id}/resolve`, {
        action: "RELEASE",
        reason: "storm-cleanup",
        actor: "ops",
      });
    const after = await wallet(runtime, projectId);
    expect(after).toEqual(before);
    const audit2 = await internalCall(
      runtime.internalApp,
      "GET",
      "/internal/v1/audit/conservation",
    );
    expect(audit2.data.ok).toBe(true);
    expect(audit2.data.unknown_holds).toBe(0);
  } finally {
    await runtime.close();
    await mockServer.stop(true);
  }
});

test("symptom: replaying the same Idempotency-Key returns one charge, not two", async () => {
  const mock = createMockApp("adv-ctrl");
  const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const { projectId, key, channel, customer, supplier, call } = await seed(
      runtime,
      mockServer.url.origin,
    );
    const model = `adv-idem-${crypto.randomUUID().slice(0, 6)}`;
    await call("PUT", "/internal/v1/models", {
      name: model,
      operations: ["chat"],
      max_output_tokens: 50,
    });
    await call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: channel.id,
      upstream_model: "adv-idem",
      operations: ["chat"],
      billing_price_id: customer.id,
      cost_version_id: supplier.id,
    });
    const before = await wallet(runtime, projectId);
    const idem = crypto.randomUUID();
    const send = () =>
      runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: {
            authorization: `Bearer ${key}`,
            "content-type": "application/json",
            "idempotency-key": idem,
          },
          body: JSON.stringify({
            model,
            stream: false,
            messages: [{ role: "user", content: "replay" }],
            max_tokens: 20,
          }),
        }),
      );
    const responses = await Promise.all([send(), send(), send(), send(), send()]);
    const ok = responses.filter((r) => r.status === 200);
    const rejected = responses.filter((r) => r.status === 409);
    expect(ok.length).toBe(1);
    expect(rejected.length).toBe(4);
    for (const r of rejected) {
      expect(((await r.json()) as { error: { code: string } }).error.code).toBe(
        "idempotency_in_progress",
      );
    }
    await Promise.all(ok.map((r) => r.text()));
    const replay = await send();
    expect(replay.status).toBe(409);
    expect(((await replay.json()) as { error: { code: string } }).error.code).toBe(
      "idempotency_replayed",
    );
    const charges =
      await db`SELECT count(*)::int AS n, count(DISTINCT request_id)::int AS u FROM public.billing_charges WHERE api_key_id = (SELECT id FROM public.api_keys WHERE project_id = ${projectId})`;
    expect(charges[0]!.n).toBe(1);
    expect(charges[0]!.u).toBe(1);
    const after = await wallet(runtime, projectId);
    expect(BigInt(after.reserved_atoms)).toBe(BigInt(before.reserved_atoms));
    const audit = await internalCall(runtime.internalApp, "GET", "/internal/v1/audit/conservation");
    expect(audit.data.ok).toBe(true);
  } finally {
    await runtime.close();
    await mockServer.stop(true);
  }
});

test("symptom: invalid auth, unknown model, oversized body and malformed JSON never create a hold", async () => {
  const mock = createMockApp("adv-ctrl");
  const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
  const runtime = createGatewayRuntime({ config: makeConfig({ maxBodyBytes: 64 * 1024 }), db });
  try {
    await runtime.start();
    const { projectId, key } = await seed(runtime, mockServer.url.origin);
    const before = await wallet(runtime, projectId);
    const badAuth = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: "Bearer sk-forged", "content-type": "application/json" },
        body: JSON.stringify({ model: "x", messages: [] }),
      }),
    );
    expect(badAuth.status).toBe(401);
    const badKey = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: {
          authorization: "Bearer ${key}".replace("${key}", key) + "x",
          "content-type": "application/json",
        },
        body: JSON.stringify({ model: "x", messages: [] }),
      }),
    );
    expect(badKey.status).toBe(401);
    const noAuth = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ model: "x", messages: [] }),
      }),
    );
    expect(noAuth.status).toBe(401);
    const unknown = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "does-not-exist",
          messages: [{ role: "user", content: "x" }],
        }),
      }),
    );
    expect(unknown.status).toBe(404);
    const huge = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "x",
          messages: [{ role: "user", content: "A".repeat(100_000) }],
        }),
      }),
    );
    expect(huge.status).toBe(413);
    const malformed = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
        body: "{not json",
      }),
    );
    expect([400, 422]).toContain(malformed.status);
    const emptyModel = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
        body: JSON.stringify({ model: "", messages: [{ role: "user", content: "x" }] }),
      }),
    );
    expect([400, 404, 422]).toContain(emptyModel.status);
    const sqlInjection = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${key}' OR 1=1 --`, "content-type": "application/json" },
        body: JSON.stringify({ model: "x' DROP TABLE billing_holds; --", messages: [] }),
      }),
    );
    expect(sqlInjection.status).toBe(401);
    const after = await wallet(runtime, projectId);
    expect(after).toEqual(before);
    const holds =
      await db`SELECT count(*)::int AS n FROM public.billing_holds WHERE project_id = ${projectId}`;
    expect(holds[0]!.n).toBe(0);
  } finally {
    await runtime.close();
    await mockServer.stop(true);
  }
});

test("symptom: client disconnects before the upstream answers -> hold becomes UNKNOWN within 2.5s, nothing charged, ops RELEASE restores the wallet exactly", async () => {
  const mock = createMockApp("adv-ctrl");
  const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const { projectId, key, channel, customer, supplier, call } = await seed(
      runtime,
      mockServer.url.origin,
    );
    const model = `adv-prehdr-${crypto.randomUUID().slice(0, 6)}`;
    await call("PUT", "/internal/v1/models", {
      name: model,
      operations: ["chat"],
      max_output_tokens: 50,
    });
    await call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: channel.id,
      upstream_model: "adv-prehdr",
      operations: ["chat"],
      billing_price_id: customer.id,
      cost_version_id: supplier.id,
    });
    mock.scenarios.set("adv-prehdr", { headerDelayMs: 3000 });
    const before = await wallet(runtime, projectId);
    const publicServer = realServer(runtime);
    const t0 = performance.now();
    const ac = new AbortController();
    const pending = fetch(`http://127.0.0.1:${publicServer.port}/v1/chat/completions`, {
      method: "POST",
      headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
      body: JSON.stringify({
        model,
        stream: true,
        messages: [{ role: "user", content: "attack" }],
        max_tokens: 50,
      }),
      signal: ac.signal,
    }).catch(() => undefined);
    await waitFor(async () => {
      const w = await wallet(runtime, projectId);
      return BigInt(w.reserved_atoms) > BigInt(before.reserved_atoms);
    });
    ac.abort();
    await pending;
    const [holdRow] =
      await db`SELECT request_id FROM public.billing_holds WHERE project_id = ${projectId} ORDER BY created_at DESC LIMIT 1`;
    await waitFor(async () => {
      const rows =
        await db`SELECT state FROM public.billing_holds WHERE request_id = ${holdRow!.request_id}`;
      return rows[0]?.state === "UNKNOWN";
    }, 2500);
    const settledWithinMs = performance.now() - t0;
    expect(settledWithinMs).toBeLessThan(2500);
    expect(runtime.lifecycle.size).toBe(0);
    const during = await wallet(runtime, projectId);
    expect(BigInt(during.spent_atoms)).toBe(BigInt(before.spent_atoms));
    expect(BigInt(during.reserved_atoms) - BigInt(before.reserved_atoms)).toBe(100n);
    const charges =
      await db`SELECT count(*)::int AS n FROM public.billing_charges WHERE project_id = ${projectId}`;
    expect(charges[0]!.n).toBe(0);
    const audit = await internalCall(runtime.internalApp, "GET", "/internal/v1/audit/conservation");
    expect(audit.data.ok).toBe(true);
    expect(audit.data.open_holds).toBe(0);
    expect(audit.data.unknown_holds).toBe(1);
    const resolve = await internalCall(
      runtime.internalApp,
      "POST",
      `/internal/v1/requests/${holdRow!.request_id}/resolve`,
      { action: "RELEASE", reason: "client-gone-before-upstream-headers", actor: "ops" },
    );
    expect(resolve.status, JSON.stringify(resolve.data)).toBe(200);
    const after = await wallet(runtime, projectId);
    expect(after).toEqual(before);
    publicServer.stop(true);
  } finally {
    await runtime.close();
    await mockServer.stop(true);
  }
});

test("symptom: wallet with insufficient funds is rejected before any upstream call and leaves no hold", async () => {
  const mock = createMockApp("adv-ctrl");
  const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const { key, channel, customer, supplier, call } = await seed(runtime, mockServer.url.origin);
    const poor = await call("PUT", "/internal/v1/projects", {
      name: `adv-poor-${crypto.randomUUID().slice(0, 6)}`,
      currency: "USD",
      rpm: 10,
      tpm: 100,
      concurrency: 1,
    });
    const poorKey = await call("PUT", "/internal/v1/api-keys", {
      project_id: poor.id,
      name: "poor",
    });
    const model = `adv-poor-${crypto.randomUUID().slice(0, 6)}`;
    await call("PUT", "/internal/v1/models", {
      name: model,
      operations: ["chat"],
      max_output_tokens: 50,
    });
    await call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: channel.id,
      upstream_model: "adv-poor",
      operations: ["chat"],
      billing_price_id: customer.id,
      cost_version_id: supplier.id,
    });
    const requestsBefore = mock.stats.requests;
    const res = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${poorKey.key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model,
          stream: false,
          messages: [{ role: "user", content: "x" }],
          max_tokens: 20,
        }),
      }),
    );
    expect([402, 403]).toContain(res.status);
    expect(mock.stats.requests).toBe(requestsBefore);
    const holds =
      await db`SELECT count(*)::int AS n FROM public.billing_holds WHERE project_id = ${poor.id}`;
    expect(holds[0]!.n).toBe(0);
    void key;
  } finally {
    await runtime.close();
    await mockServer.stop(true);
  }
});

test("symptom: internal admin API rejects a forged or missing token and never exposes resolve", async () => {
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const forged = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/requests/00000000-0000-0000-0000-000000000000/resolve",
      {
        action: "RELEASE",
        reason: "forged",
      },
      "wrong-token",
    );
    expect(forged.status).toBe(401);
    const missing = await runtime.publicApp.fetch(
      new Request(
        "http://gateway.local/internal/v1/requests/00000000-0000-0000-0000-000000000000/resolve",
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ action: "RELEASE", reason: "x" }),
        },
      ),
    );
    expect([401, 404]).toContain(missing.status);
    const auditForged = await runtime.publicApp.fetch(
      new Request("http://gateway.local/internal/v1/audit/conservation", { method: "GET" }),
    );
    expect([401, 404]).toContain(auditForged.status);
  } finally {
    await runtime.close();
  }
});
