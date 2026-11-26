import { expect, spyOn, test } from "bun:test";
import { SQL } from "bun";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createDatabase } from "@gateway/ledger";
import { createGatewayRuntime } from "@gateway/gateway";
import { WasmCapacityError } from "@gateway/plugin-engine";
import { encodeSSE } from "@gateway/protocol";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";
import { createTraceStore } from "../../apps/gateway/src/trace-storage.ts";
import {
  targetUrl,
  makeConfig,
  seed,
  db,
  chatRequest,
  testDbName,
  adminUrl,
  adminDb,
  realServer,
  waitFor,
} from "./adversarial-billing-fixture.ts";
import { registerBillingFixture } from "./adversarial-billing-fixture.ts";
import { registerCancellationCases } from "./adversarial-cancellation-cases.ts";
import { registerRecoveryCases } from "./adversarial-recovery-cases.ts";

registerBillingFixture();

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
        await tx`SELECT id FROM public.wallets WHERE billing_account_id=${fixture.billingAccountId}::uuid FOR UPDATE`;
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
        await db`SELECT count(*)::int AS n FROM public.billing_charges WHERE billing_account_id=${fixture.billingAccountId}::uuid`;
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
      sum(supplier_cost_atoms)::text AS supplier FROM public.billing_charges WHERE billing_account_id=${fixture.billingAccountId}::uuid`;
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
      sum(supplier_cost_atoms)::text AS supplier FROM public.billing_charges WHERE billing_account_id=${fixture.billingAccountId}::uuid`;
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
      SELECT count(*)::int AS holds FROM public.billing_holds WHERE billing_account_id=${fixture.billingAccountId}::uuid`;
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
      FROM public.billing_charges WHERE billing_account_id=${fixture.billingAccountId}::uuid`;
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

test("Trace setup and expiration require the current billing account column", async () => {
  const traceDbName = `trace_init_${testDbName}`;
  const traceUrl = new URL(adminUrl);
  traceUrl.pathname = `/${traceDbName}`;
  const initialize = async () => {
    const processResult = Bun.spawn(
      [
        process.execPath,
        "--no-env-file",
        new URL("../../scripts/trace-db.ts", import.meta.url).pathname,
        "migrate",
      ],
      {
        env: { ...process.env, TRACE_DATABASE_URL: traceUrl.toString() },
        stdout: "ignore",
        stderr: "pipe",
      },
    );
    const [exit, stderr] = await Promise.all([
      processResult.exited,
      new Response(processResult.stderr).text(),
    ]);
    return { exit, stderr };
  };
  await adminDb.unsafe(`CREATE DATABASE ${traceDbName}`);
  const traceDb = createDatabase(traceUrl.toString(), 1);
  const traceStore = createTraceStore(traceUrl.toString(), 1000);
  try {
    await traceDb.unsafe(
      "CREATE TABLE public.console_schema_migrations (version integer PRIMARY KEY)",
    );
    expect((await initialize()).exit).toBe(1);
    await traceDb.unsafe("DROP TABLE public.console_schema_migrations");
    expect(await initialize()).toEqual({ exit: 0, stderr: "" });
    expect(await initialize()).toEqual({ exit: 0, stderr: "" });
    const traceId = "a".repeat(32);
    await traceDb.unsafe(
      "CREATE TABLE public.console_schema_migrations (version integer PRIMARY KEY)",
    );
    await expect(traceStore.read(traceId)).rejects.toThrow("Invalid telemetry database");
    await traceDb.unsafe("DROP TABLE public.console_schema_migrations");
    await traceDb`
      INSERT INTO telemetry.spans (trace_id, span_id, request_id, billing_account_id, source, started_at, expires_at, span)
      VALUES (${traceId}, ${"b".repeat(16)}, ${crypto.randomUUID()}, ${crypto.randomUUID()},
        'gateway', now() - interval '2 seconds', now() - interval '1 second', '{}'::jsonb)
    `;
    await traceDb`ALTER TABLE telemetry.spans RENAME COLUMN billing_account_id TO project_id`;
    const rejected = await initialize();
    expect(rejected.exit).toBe(1);
    expect(rejected.stderr).toContain("billing_account_id");
    await expect(traceStore.expire(1)).rejects.toThrow(/billing_account_id/);
    expect(await traceDb`SELECT trace_id FROM telemetry.spans`).toEqual([{ trace_id: traceId }]);
  } finally {
    await traceStore.close();
    await traceDb.close();
    await adminDb.unsafe(`DROP DATABASE ${traceDbName} WITH (FORCE)`);
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
        maxPerBillingAccount: 3,
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
    const { key, billingAccountId, channel, customer, supplier, call } = await seed(
      runtime,
      upstream.url.origin,
    );
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
    expect(stored.map(({ billing_account_id }) => billing_account_id)).toEqual([
      billingAccountId,
      billingAccountId,
      billingAccountId,
    ]);
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

registerCancellationCases();
registerRecoveryCases();
