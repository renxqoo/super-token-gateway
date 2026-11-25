import { afterAll, beforeAll, expect, spyOn, test } from "bun:test";
import { SQL } from "bun";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createDatabase, migrate } from "@gateway/ledger";
import { createGatewayRuntime, type GatewayConfig } from "@gateway/gateway";
import { WasmCapacityError } from "@gateway/plugin-engine";
import { encodeSSE } from "@gateway/protocol";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";
import { createTraceStore } from "../../apps/gateway/src/trace-storage.ts";

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

test.each(["lock", "statement"])(
  "symptom: a pre-dispatch wallet %s timeout returns HTTP 500 instead of retryable overload",
  async (timeout) => {
    const connection =
      timeout === "lock" ? { lock_timeout: 30 } : { lock_timeout: 5000, statement_timeout: 100 };
    const shortDb = new SQL(targetUrl.toString(), { max: 8, connection });
    const runtime = createGatewayRuntime({ config: makeConfig(), db: shortDb });
    const mock = createMockApp("lock-admission-fixture");
    const upstream = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
    const locked = Promise.withResolvers<void>();
    const unlock = Promise.withResolvers<void>();
    let transaction: Promise<unknown> | undefined;
    try {
      await runtime.start();
      const fixture = await seed(runtime, upstream.url.origin);
      const model = "ledger-lock-admission";
      await fixture.call("PUT", "/internal/v1/models", { name: model, operations: ["chat"] });
      await fixture.call("PUT", "/internal/v1/routes", {
        model_name: model,
        channel_id: fixture.channel.id,
        upstream_model: model,
        operations: ["chat"],
        billing_price_id: fixture.customer.id,
        cost_version_id: fixture.supplier.id,
      });
      transaction = db.begin(async (tx) => {
        await tx`SELECT id FROM public.wallets WHERE project_id=${fixture.projectId}::uuid FOR UPDATE`;
        locked.resolve();
        await unlock.promise;
      });
      await Promise.race([locked.promise, transaction]);
      const request = chatRequest(fixture.key, model, { stream: false });
      request.headers.set("idempotency-key", "lock-timeout-retry");
      const result = await runtime.publicApp.fetch(request);
      expect(result.status).toBe(503);
      expect(await result.json()).toMatchObject({ error: { code: "ledger_busy" } });
      const requestId = result.headers.get("x-request-id");
      const [held] =
        await db`SELECT count(*)::int AS n FROM public.billing_holds WHERE request_id=${requestId}::uuid`;
      expect(held?.n).toBe(0);
      unlock.resolve();
      await transaction;
      const retry = chatRequest(fixture.key, model, { stream: false });
      retry.headers.set("idempotency-key", "lock-timeout-retry");
      const succeeded = await runtime.publicApp.fetch(retry);
      expect(succeeded.status).toBe(200);
      const [charged] =
        await db`SELECT count(*)::int AS n FROM public.billing_charges WHERE project_id=${fixture.projectId}::uuid`;
      expect(charged?.n).toBe(1);
    } finally {
      unlock.resolve();
      await transaction;
      await runtime.close();
      upstream.stop(true);
      await shortDb.close();
    }
  },
);

test("symptom: an open fail-closed output Webhook still dispatches paid upstream requests", async () => {
  let calls = 0;
  let webhookHealthy = false;
  let recovering = false;
  const upstreamReached = Promise.withResolvers<void>();
  const upstreamProceed = Promise.withResolvers<void>();
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch() {
      calls++;
      if (recovering) {
        upstreamReached.resolve();
        await upstreamProceed.promise;
      }
      return Response.json({
        id: "test",
        object: "chat.completion",
        model: "fixture",
        choices: [
          {
            index: 0,
            message: { role: "assistant", content: "restricted" },
            finish_reason: "stop",
          },
        ],
        usage: { prompt_tokens: 5, completion_tokens: 1, total_tokens: 6 },
      });
    },
  });
  const webhook = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: () =>
      webhookHealthy ? Response.json({ action: "PASS" }) : new Response(null, { status: 503 }),
  });
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const fixture = await seed(runtime, upstream.url.origin);
    const plugin = await fixture.call("PUT", "/internal/v1/plugins", {
      name: "webhook-admission-billing",
      version: "1",
      kind: "WEBHOOK",
      webhook_config: {
        url: webhook.url.href,
        failure_policy: "FAIL_CLOSED",
        circuit_failure_threshold: 1,
        circuit_open_ms: 1000,
      },
      manifest: { hooks: ["on_response", "on_stream_end"] },
    });
    await fixture.call("POST", "/internal/v1/plugin-releases", {
      id: crypto.randomUUID(),
      expected_epoch: runtime.snapshot.get().epoch,
      targets: [{ plugin_id: plugin.id }],
      samples: { [String(plugin.id)]: { webhook_reply: { action: "PASS" } } },
    });
    const model = "webhook-open-admission";
    await fixture.call("PUT", "/internal/v1/models", { name: model, operations: ["chat"] });
    await fixture.call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: fixture.channel.id,
      upstream_model: model,
      operations: ["chat"],
      billing_price_id: fixture.customer.id,
      cost_version_id: fixture.supplier.id,
      plugin_pipeline: [{ plugin_id: plugin.id, priority: 0 }],
    });
    const idempotent = () => {
      const request = chatRequest(fixture.key, model, { stream: false });
      request.headers.set("idempotency-key", "output-policy-replay");
      return request;
    };
    const first = await runtime.publicApp.fetch(idempotent());
    expect(first.status).toBe(503);
    expect(calls).toBe(1);
    const replay = await runtime.publicApp.fetch(idempotent());
    expect(replay.status).toBe(409);
    expect(replay.headers.get("x-request-id")).toBe(first.headers.get("x-request-id"));
    await Promise.all(
      [false, true].map(async (stream) => {
        const blocked = await runtime.publicApp.fetch(chatRequest(fixture.key, model, { stream }));
        expect(blocked.status).toBe(503);
        expect(await blocked.json()).toMatchObject({
          error: { rule_code: "webhook_circuit_open" },
        });
      }),
    );
    expect(calls).toBe(1);
    const [charge] =
      await db`SELECT count(*)::int AS entries, sum(customer_atoms)::text AS customer,
      sum(supplier_cost_atoms)::text AS supplier FROM public.billing_charges WHERE project_id=${fixture.projectId}::uuid`;
    expect(charge).toEqual({ entries: 1, customer: "0", supplier: "5" });
    await Bun.sleep(1100);
    webhookHealthy = true;
    recovering = true;
    const probe = runtime.publicApp.fetch(chatRequest(fixture.key, model, { stream: false }));
    await Promise.race([
      upstreamReached.promise,
      Promise.resolve(probe).then(() => {
        throw new Error("The half-open probe did not reach the upstream");
      }),
    ]);
    try {
      const rejected = await runtime.publicApp.fetch(
        chatRequest(fixture.key, model, { stream: false }),
      );
      expect(rejected.status).toBe(503);
      expect(await rejected.json()).toMatchObject({ error: { rule_code: "webhook_circuit_open" } });
      expect(calls).toBe(2);
    } finally {
      upstreamProceed.resolve();
    }
    expect((await probe).status).toBe(200);
    expect(
      (await runtime.publicApp.fetch(chatRequest(fixture.key, model, { stream: false }))).status,
    ).toBe(200);
    expect(calls).toBe(3);
    const [recovered] =
      await db`SELECT count(*)::int AS entries, sum(customer_atoms)::text AS customer,
      sum(supplier_cost_atoms)::text AS supplier FROM public.billing_charges WHERE project_id=${fixture.projectId}::uuid`;
    expect(recovered).toEqual({ entries: 3, customer: "10", supplier: "15" });
    expect((await runtime.ledger.auditConservation()).ok).toBe(true);
  } finally {
    upstreamProceed.resolve();
    await runtime.close();
    webhook.stop(true);
    upstream.stop(true);
  }
});

test("symptom: a saturated fail-closed output WASM queue still dispatches paid upstream requests", async () => {
  let full = false;
  let upstreamCalls = 0;
  const executor = {
    runtimeKind: "mock" as const,
    acceptsEgress: () => !full,
    compile() {},
    async invoke() {
      if (full) throw new WasmCapacityError("wasm_queue_full", "Fixture queue is full");
      return new TextEncoder().encode('{"action":"PASS"}');
    },
    close() {},
  };
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      upstreamCalls++;
      return Response.json({
        id: "wasm-admission",
        object: "chat.completion",
        choices: [
          { index: 0, message: { role: "assistant", content: "answer" }, finish_reason: "stop" },
        ],
        usage: { prompt_tokens: 5, completion_tokens: 1, total_tokens: 6 },
      });
    },
  });
  const runtime = createGatewayRuntime({ config: makeConfig(), db, contentWasmExecutor: executor });
  try {
    await runtime.start();
    const fixture = await seed(runtime, upstream.url.origin);
    const plugin = await fixture.call("POST", "/internal/v1/plugins/builtins/pii-guard/install", {
      manifest: { hooks: ["on_response", "on_stream_end"] },
    });
    await fixture.call("POST", "/internal/v1/plugin-releases", {
      id: crypto.randomUUID(),
      expected_epoch: runtime.snapshot.get().epoch,
      targets: [{ plugin_id: plugin.id }],
      samples: { [String(plugin.id)]: { text: "sample@example.com" } },
    });
    const model = "wasm-output-admission";
    await fixture.call("PUT", "/internal/v1/models", { name: model, operations: ["chat"] });
    await fixture.call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: fixture.channel.id,
      upstream_model: model,
      operations: ["chat"],
      billing_price_id: fixture.customer.id,
      cost_version_id: fixture.supplier.id,
      plugin_pipeline: [{ plugin_id: plugin.id, priority: 0 }],
    });
    const request = (stream: boolean) => {
      const req = chatRequest(fixture.key, model, { stream });
      req.headers.set("idempotency-key", `wasm-capacity-${stream}`);
      return req;
    };
    full = true;
    for (const stream of [false, true]) {
      const rejected = await runtime.publicApp.fetch(request(stream));
      expect(rejected.status).toBe(503);
      expect(await rejected.json()).toMatchObject({ error: { rule_code: "wasm_queue_full" } });
      expect(upstreamCalls).toBe(0);
    }
    const [before] = await db`
      SELECT count(*)::int AS holds FROM public.billing_holds WHERE project_id=${fixture.projectId}::uuid`;
    expect(before?.holds).toBe(0);
    full = false;
    const accepted = await runtime.publicApp.fetch(request(false));
    expect(accepted.status).toBe(200);
    await accepted.text();
    full = true;
    const replay = await runtime.publicApp.fetch(request(false));
    expect(replay.status).toBe(409);
    expect(replay.headers.get("x-request-id")).toBe(accepted.headers.get("x-request-id"));
    expect(upstreamCalls).toBe(1);
    const metrics = await runtime.publicApp.fetch(new Request("http://gateway.local/metrics"));
    expect(await metrics.text()).toContain("gateway_wasm_admission_rejections_total 2\n");
    const [after] = await db`
      SELECT count(*)::int AS charges, sum(customer_atoms)::text AS customer,
        sum(supplier_cost_atoms)::text AS supplier
      FROM public.billing_charges WHERE project_id=${fixture.projectId}::uuid`;
    expect(after).toEqual({ charges: 1, customer: "5", supplier: "5" });
    expect(await runtime.ledger.auditConservation()).toMatchObject({
      ok: true,
      open_holds: 0,
      unknown_holds: 0,
    });
  } finally {
    await runtime.close();
    upstream.stop(true);
  }
});

test("first text traces link the delayed SSE text to the client and skip reasoning and role frames", async () => {
  const traceDbName = `trace_${testDbName}`;
  const traceRole = `${traceDbName}_writer`;
  const traceUrl = new URL(adminUrl);
  traceUrl.pathname = `/${traceDbName}`;
  await adminDb.unsafe(`CREATE DATABASE ${traceDbName}`);
  const traceDb = createDatabase(traceUrl.toString(), 1);
  await traceDb.unsafe(
    await Bun.file(new URL("../../infra/postgres/trace-schema.sql", import.meta.url)).text(),
  );
  const writerUrl = new URL(traceUrl);
  writerUrl.username = traceRole;
  writerUrl.password = crypto.randomUUID();
  await traceDb.unsafe(`CREATE ROLE ${traceRole} LOGIN PASSWORD '${writerUrl.password}'`);
  await traceDb.unsafe(`GRANT USAGE ON SCHEMA telemetry TO ${traceRole}`);
  await traceDb.unsafe(`GRANT SELECT, INSERT, DELETE ON telemetry.spans TO ${traceRole}`);
  const traceStore = createTraceStore(writerUrl.toString(), 1000);
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: () =>
      new Response(
        new ReadableStream({
          async start(writer) {
            writer.enqueue(encodeSSE({ choices: [{ index: 0, delta: { role: "assistant" } }] }));
            writer.enqueue(
              encodeSSE({ choices: [{ index: 0, delta: { reasoning_content: "hidden" } }] }),
            );
            await Bun.sleep(100);
            writer.enqueue(encodeSSE({ choices: [{ index: 0, delta: { content: "visible" } }] }));
            await Bun.sleep(80);
            writer.enqueue(
              encodeSSE({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }] }),
            );
            writer.enqueue(
              encodeSSE({
                choices: [],
                usage: { prompt_tokens: 5, completion_tokens: 1, total_tokens: 6 },
              }),
            );
            writer.enqueue(encodeSSE("[DONE]"));
            writer.close();
          },
        }),
        { headers: { "content-type": "text/event-stream" } },
      ),
  });
  const runtime = createGatewayRuntime({
    config: makeConfig({
      accessLog: true,
      traceMetrics: true,
      traceExport: {
        databaseUrl: writerUrl.toString(),
        sampleBasisPoints: 10_000,
        slowFirstTextMs: 3000,
        maxAttempts: 3,
        maxRecords: 4,
        maxBytes: 32_768,
        maxPerProject: 3,
        priorityReserve: 1,
        batchSize: 2,
        batchDeadlineMs: 700,
        flushMs: 25,
        timeoutMs: 2000,
        retryLimit: 0,
        retentionSeconds: 120,
        cleanupIntervalMs: 1000,
        cleanupBatch: 2,
      },
    }),
    db,
  });
  const logs = spyOn(console, "log").mockImplementation(() => undefined);
  const dir = await mkdtemp(join(tmpdir(), "stg-first-text-"));
  let server: ReturnType<typeof realServer> | undefined;
  try {
    await runtime.start();
    const { key, channel, customer, supplier, call } = await seed(runtime, upstream.url.origin);
    const model = "first-text-trace";
    await call("PUT", "/internal/v1/models", {
      name: model,
      operations: ["chat"],
      max_output_tokens: 32,
    });
    await call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: channel.id,
      upstream_model: model,
      operations: ["chat"],
      billing_price_id: customer.id,
      cost_version_id: supplier.id,
    });
    server = realServer(runtime);
    const inputFile = join(dir, "request.json");
    await Bun.write(
      inputFile,
      JSON.stringify({ model, messages: [{ role: "user", content: "test" }], max_tokens: 16 }),
    );
    const probe = Bun.spawn(
      [
        process.execPath,
        "--no-env-file",
        new URL("../../scripts/trace-first-text.ts", import.meta.url).pathname,
        "--url",
        `${server.url.origin}/v1/chat/completions`,
        "--request",
        inputFile,
        "--timeout-ms",
        "4000",
        "--persist",
      ],
      {
        env: {
          ...process.env,
          GATEWAY_API_KEY: key,
          TRACE_DATABASE_URL: writerUrl.toString(),
          TRACE_RETENTION_SECONDS: "120",
          TRACE_CLIENT_JOIN_WAIT_MS: "4000",
        },
        stdout: "pipe",
        stderr: "pipe",
      },
    );
    const [exit, stdout, stderr] = await Promise.all([
      probe.exited,
      new Response(probe.stdout).text(),
      new Response(probe.stderr).text(),
    ]);
    expect({ exit, stderr }, stdout).toEqual({ exit: 0, stderr: "" });
    const client = JSON.parse(stdout);
    const access = logs.mock.calls
      .map(([line]) => JSON.parse(String(line)))
      .find((row) => row.request_id === client.request_id);
    expect(access).toMatchObject({ status: 200, trace_id: client.trace_id });
    const trace = access.first_text_trace;
    expect(trace.upstream_headers_ms).toBeGreaterThanOrEqual(trace.attempt_start_ms);
    expect(trace.upstream_first_text_ms - trace.upstream_headers_ms).toBeGreaterThan(60);
    expect(trace.gateway_first_text_enqueued_ms).toBeGreaterThanOrEqual(
      trace.upstream_first_text_ms,
    );
    expect(access.duration_ms - trace.gateway_first_text_enqueued_ms).toBeGreaterThan(40);
    expect(client.client_first_text_ms - client.client_headers_ms).toBeGreaterThan(60);
    expect(client.client_duration_ms - client.client_first_text_ms).toBeGreaterThan(40);
    expect(JSON.stringify(access)).not.toContain("hidden");
    expect(JSON.stringify(access)).not.toContain("visible");
    expect(client.trace_persisted).toBe(true);
    const stored = await traceStore.read(client.request_id);
    expect(stored.map(({ span }) => span.name).sort()).toEqual([
      "client.inference",
      "gateway.inference",
      "gateway.upstream.attempt",
    ]);
    const gatewaySpan = stored.find(({ span }) => span.name === "gateway.inference")!.span;
    const attemptSpan = stored.find(({ span }) => span.name === "gateway.upstream.attempt")!.span;
    expect(gatewaySpan.parentSpanId).toBe(client.client_span_id);
    expect(attemptSpan.parentSpanId).toBe(gatewaySpan.spanId);
    expect(attemptSpan.events.map((event) => event.name)).toEqual(
      expect.arrayContaining([
        "http.request.written",
        "http.response.headers",
        "upstream.first_text",
      ]),
    );
    expect(JSON.stringify(stored)).not.toContain("visible");
    const metrics = async () => (await runtime.metricsResponse()).text();
    expect(await metrics()).toContain(
      'gateway_sse_phase_seconds_count{operation="chat",stage="upstream_first_text"} 1',
    );
    expect(await metrics()).toContain("gateway_sse_waiting_first_text 0");

    await traceDb`UPDATE telemetry.spans SET expires_at = now() - interval '1 second'`;
    await waitFor(async () => /gateway_trace_spans_expired_total 3\n/.test(await metrics()));
    expect(await metrics()).toContain("gateway_trace_cleanup_failed_runs_total 0");

    const locked = Promise.withResolvers<void>();
    const unlock = Promise.withResolvers<void>();
    const lock = traceDb.begin(async (tx) => {
      await tx`LOCK TABLE telemetry.spans IN ACCESS EXCLUSIVE MODE`;
      locked.resolve();
      await unlock.promise;
    });
    await Promise.race([locked.promise, lock]);
    try {
      const replies = await Promise.all(
        Array.from({ length: 8 }, () => runtime.publicApp.fetch(chatRequest(key, model))),
      );
      await Promise.all(
        replies.map(async (response) => {
          expect(response.status).toBe(200);
          expect(await response.text()).toContain("[DONE]");
        }),
      );
      await waitFor(async () =>
        /gateway_trace_export_failed_batches_total [1-9]/.test(await metrics()),
      );
      expect(await metrics()).toMatch(/gateway_trace_export_timed_out_batches_total [1-9]/);
      expect(await metrics()).toMatch(/gateway_trace_dropped_total\{priority="ordinary"\} [1-9]/);
    } finally {
      unlock.resolve();
      await lock;
    }
    await waitFor(async () => /gateway_trace_queue_records 0\n/.test(await metrics()));
    const recovered = await runtime.publicApp.fetch(chatRequest(key, model));
    expect(await recovered.text()).toContain("[DONE]");
    await waitFor(
      async () => (await traceStore.read(recovered.headers.get("x-request-id")!)).length === 2,
    );
    expect((await runtime.ledger.auditConservation()).ok).toBe(true);
  } finally {
    logs.mockRestore();
    await server?.stop(true);
    await runtime.close();
    await upstream.stop(true);
    await traceStore.close();
    await traceDb.close();
    await adminDb.unsafe(`DROP DATABASE ${traceDbName} WITH (FORCE)`);
    await adminDb.unsafe(`DROP ROLE ${traceRole}`);
    await rm(dir, { recursive: true, force: true });
  }
});

test("symptom: a freshly issued Key is rejected as expired when Gateway runs outside UTC", async () => {
  const originalTimeZone = process.env.TZ;
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    process.env.TZ = "Asia/Singapore";
    await runtime.start();
    const project = await internalCall(runtime.internalApp, "PUT", "/internal/v1/projects", {
      name: "key-expiration-timezone",
    });
    expect(project.status).toBe(200);
    const now = Date.now();
    const issue = (expiresAt: string) =>
      internalCall(runtime.internalApp, "PUT", "/internal/v1/api-keys", {
        project_id: project.data.id,
        expires_at: expiresAt,
      });
    const listModels = (key: unknown) =>
      runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/models", {
          headers: { authorization: `Bearer ${String(key)}` },
        }),
      );

    const currentExpiry = new Date(now + 15 * 60_000).toISOString();
    const current = await issue(currentExpiry);
    expect(current.status).toBe(200);
    expect((await listModels(current.data.key)).status).toBe(200);
    expect(current.data.expires_at).toBe(currentExpiry);

    process.env.TZ = "America/Los_Angeles";
    const pastExpiry = new Date(now - 15 * 60_000).toISOString();
    const expired = await issue(pastExpiry);
    expect(expired.status).toBe(200);
    expect(expired.data.expires_at).toBe(pastExpiry);
    const rejected = await listModels(expired.data.key);
    expect(rejected.status).toBe(401);
    expect(await rejected.json()).toMatchObject({ error: { code: "key_expired" } });
  } finally {
    if (originalTimeZone === undefined) delete process.env.TZ;
    else process.env.TZ = originalTimeZone;
    await runtime.close();
  }
});

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
    mock.scenarios.set("adv-storm", { streamDurationMs: 6000, repeat: 30 });
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
          return { status: res?.status ?? 0, id: null as string | null, firstChunkBytes: 0 };
        const id = res.headers.get("x-request-id");
        const reader = res.body!.getReader();
        const first = await reader.read();
        await Bun.sleep(5 + Math.random() * 30);
        ac.abort();
        await reader.cancel().catch(() => undefined);
        return { status: 200, id, firstChunkBytes: first.value?.byteLength ?? 0 };
      }),
    );
    publicServer.stop(true);
    expect(results.every((r) => r.status === 200 && r.firstChunkBytes > 0)).toBe(true);
    await waitFor(async () => mock.stats.active === 0);
    expect(mock.stats.cancellations).toBe(N);
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
    const routeHealth = await internalCall(
      runtime.internalApp,
      "GET",
      "/internal/v1/health/channels",
    );
    expect(routeHealth.status).toBe(200);
    const channelHealth = (
      routeHealth.data.channels as {
        channel_id: string;
        state: string;
        consecutive_failures: number;
      }[]
    ).find((health) => health.channel_id === channel.id);
    expect(channelHealth?.state ?? "CLOSED").toBe("CLOSED");
    expect(channelHealth?.consecutive_failures ?? 0).toBe(0);
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

test("symptom: an upstream business error prematurely releases an evidence-required hold", async () => {
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: () =>
      Response.json(
        { vendor: { code: "BUSY" } },
        {
          headers: { "x-vendor-request-id": "vendor-throttle-reconciliation" },
        },
      ),
  });
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const { projectId, key, channel, customer, supplier, call } = await seed(
      runtime,
      `http://127.0.0.1:${upstream.port}`,
    );
    const model = "reconciliation-provider-error";
    const routeId = crypto.randomUUID();
    await call("PUT", "/internal/v1/models", {
      name: model,
      operations: ["chat"],
      max_output_tokens: 64,
    });
    const putRoute = (definitive: boolean) =>
      call("PUT", "/internal/v1/routes", {
        id: routeId,
        model_name: model,
        channel_id: channel.id,
        upstream_model: model,
        operations: ["chat"],
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
        adapter_overrides: {
          reconciliation_response_id_header: "x-vendor-request-id",
          error_rules: [
            {
              path: "vendor.code",
              equals: "BUSY",
              retry_kind: "client_error",
              http_status: 429,
              ...(definitive ? { execution: "NOT_EXECUTED" } : {}),
            },
          ],
        },
      });

    await putRoute(false);
    const unknown = await runtime.publicApp.fetch(chatRequest(key, model, { stream: false }));
    expect(unknown.status).toBe(429);
    await unknown.text();
    const requestId = unknown.headers.get("x-request-id")!;
    const [hold] =
      await db`SELECT state, supplier_holds FROM public.billing_holds WHERE request_id=${requestId}`;
    expect(hold.state).toBe("UNKNOWN");
    expect(hold.supplier_holds).toMatchObject([
      { state: "UNKNOWN", provider_request_id: "vendor-throttle-reconciliation" },
    ]);

    const wrongReceipt = {
      request_id: requestId,
      attempt_id: hold.supplier_holds[0].attempt_id,
      provider_request_id: "other-provider-request",
      account_id: hold.supplier_holds[0].account_id,
      channel_id: channel.id,
      currency: "USD",
      evidence_ref: "supplier-proof",
      verdict: "NOT_EXECUTED",
    };
    const rejected = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/reconciliation/receipts",
      { actor: "fixture-finance", receipts: [wrongReceipt] },
    );
    expect(rejected.data).toEqual({
      results: [{ attempt_id: wrongReceipt.attempt_id, status: "identity_conflict" }],
    });
    expect(BigInt((await wallet(runtime, projectId)).reserved_atoms)).toBeGreaterThan(0n);

    await putRoute(true);
    const certain = await runtime.publicApp.fetch(chatRequest(key, model, { stream: false }));
    expect(certain.status).toBe(429);
    await certain.text();
    const [released] =
      await db`SELECT state FROM public.billing_holds WHERE request_id=${certain.headers.get("x-request-id")}`;
    expect(released.state).toBe("RELEASED");

    const valid = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/reconciliation/receipts",
      {
        actor: "fixture-finance",
        receipts: [{ ...wrongReceipt, provider_request_id: "vendor-throttle-reconciliation" }],
      },
    );
    expect(valid.data).toEqual({
      results: [{ attempt_id: wrongReceipt.attempt_id, status: "accepted" }],
    });
    expect(await runtime.ledger.processReconciliation(5)).toEqual({ receipts: 1, waivers: 0 });
    expect(await wallet(runtime, projectId)).toMatchObject({
      reserved_atoms: "0",
      spent_atoms: "0",
    });
    expect((await runtime.ledger.auditConservation()).ok).toBe(true);
  } finally {
    await runtime.close();
    await upstream.stop(true);
  }
});
