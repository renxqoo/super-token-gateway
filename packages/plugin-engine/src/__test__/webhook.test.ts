import { expect, test } from "bun:test";
import type { PluginRecord } from "@gateway/ledger";
import {
  createWasmtimeExecutor,
  PluginPipelineEngine,
  WasmCapacityError,
  WebhookPluginExecutor,
  type WasmRuntimeExecutor,
} from "../index.ts";
import { buildInfiniteLoopWasm, buildCrossTenantProbeWasm } from "./wasm-fixtures.ts";

test("G3.2 external HTTP Webhook HOLD_UNTIL_APPROVED leaks zero banned bytes and STREAM_AND_CUTOFF aborts upstream", async () => {
  const seen: string[] = [];
  const mockFetch = (async (_url: string | URL | Request, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body ?? "{}")) as { text: string };
    seen.push(body.text);
    if (body.text.includes("BANNED_WORD")) {
      return new Response(
        JSON.stringify({
          action: "BLOCK",
          rule_code: "SAFETY_VIOLATION",
          http_status: 400,
          safe_message: "Blocked by safety webhook",
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }
    return new Response(JSON.stringify({ action: "PASS" }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }) as unknown as typeof fetch;

  const engine = new PluginPipelineEngine({
    webhookExecutor: new WebhookPluginExecutor({ fetchImpl: mockFetch }),
  });

  const holdPlugin: PluginRecord = {
    id: "wh-hold",
    name: "safety-hold",
    version: "1.0.0",
    kind: "WEBHOOK",
    capability: "CONTENT",
    provider_config: null,
    digest: "a".repeat(64),
    wasm_bytes: null,
    webhook_config: {
      url: "http://safety.local/check",
      stream_mode: "HOLD_UNTIL_APPROVED",
      window_chars: 16,
      charge_policy: "PLATFORM_ABSORB",
    },
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
  };

  let holdAborted = false;
  const holdSession = engine.createStreamSession(
    [{ plugin_id: holdPlugin.id, priority: 1 }],
    new Map([[holdPlugin.id, holdPlugin]]),
    { request_id: "r3", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    () => {
      holdAborted = true;
    },
  );
  let holdEmitted = "";
  const p1 = await holdSession.push("Safe intro ");
  holdEmitted += p1.emit;
  const p2 = await holdSession.push("contains BANNED_WORD!");
  holdEmitted += p2.emit;
  const holdEnd = await holdSession.finalize();
  expect(holdEnd.blocked).toBe(true);
  expect(holdEnd.rule_code).toBe("SAFETY_VIOLATION");
  expect(holdEnd.charge_policy).toBe("PLATFORM_ABSORB");
  expect(holdAborted).toBe(true);
  expect(holdEmitted).not.toContain("BANNED_WORD");

  const cutoffPlugin: PluginRecord = {
    ...holdPlugin,
    id: "wh-cutoff",
    webhook_config: {
      url: "http://safety.local/check",
      stream_mode: "STREAM_AND_CUTOFF",
      window_chars: 10,
      charge_policy: "CHARGE_CUSTOMER",
    },
  };
  let cutoffAborted = false;
  const cutoffSession = engine.createStreamSession(
    [{ plugin_id: cutoffPlugin.id, priority: 1 }],
    new Map([[cutoffPlugin.id, cutoffPlugin]]),
    { request_id: "r4", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    () => {
      cutoffAborted = true;
    },
  );
  const c1 = await cutoffSession.push("BANNED_WORD here.");
  expect(c1.emit).toBe("BANNED_WORD here.");
  const cutoffEnd = await cutoffSession.finalize();
  expect(cutoffEnd.blocked).toBe(true);
  expect(cutoffEnd.charge_policy).toBe("CHARGE_CUSTOMER");
  expect(cutoffAborted).toBe(true);
  expect(seen.length).toBeGreaterThanOrEqual(2);

  const firstVerdict = Promise.withResolvers<Response>();
  let checks = 0;
  const slowEngine = new PluginPipelineEngine({
    webhookExecutor: new WebhookPluginExecutor({
      fetchImpl: (async () => {
        checks++;
        return checks === 1 ? firstVerdict.promise : Response.json({ action: "PASS" });
      }) as unknown as typeof fetch,
    }),
  });
  const slowSession = slowEngine.createStreamSession(
    [{ plugin_id: cutoffPlugin.id, priority: 1 }],
    new Map([[cutoffPlugin.id, cutoffPlugin]]),
    { request_id: "slow-webhook", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    () => undefined,
  );
  expect((await slowSession.push("Safe first sentence.")).emit).toBe("Safe first sentence.");
  const second = slowSession.push("Safe second sentence.");
  const overlappingChecks = checks;
  firstVerdict.resolve(Response.json({ action: "PASS" }));
  expect((await second).emit).toBe("Safe second sentence.");
  const audit = (await slowSession.finalize()).audit;
  expect(overlappingChecks).toBe(1);
  expect(checks).toBe(2);
  expect(audit).toHaveLength(1);
  expect(audit[0]).toMatchObject({ hook: "on_stream_chunk", action: "PASS", executions: 2 });
});

test("symptom: a successful Webhook callback from before the failure incorrectly closes the circuit", async () => {
  const stale = Promise.withResolvers<Response>();
  let calls = 0;
  const executor = new WebhookPluginExecutor({
    fetchImpl: async () => (++calls === 1 ? stale.promise : new Response(null, { status: 503 })),
  });
  const config = { url: "http://fixture.local", circuit_failure_threshold: 1 };
  const input = {
    hook: "on_response" as const,
    request_id: "old",
    billing_account_id: "p",
    model: "m",
    operation: "chat",
    text: "fixture",
  };
  const old = executor.invoke("webhook", config, input);
  const opening = await executor.invoke("webhook", config, { ...input, request_id: "open" });
  expect(opening).toMatchObject({ action: "BLOCK", rule_code: "webhook_http_503" });
  stale.resolve(Response.json({ action: "PASS" }));
  expect(await old).toMatchObject({ action: "PASS" });
  const after = await executor.invoke("webhook", config, { ...input, request_id: "after" });
  expect(after).toMatchObject({ action: "BLOCK", rule_code: "webhook_circuit_open" });
  expect(calls).toBe(2);
});

test("G3.2 webhook FAIL_CLOSED blocks and trips circuit while FAIL_OPEN passes through on outage", async () => {
  let calls = 0;
  const failingFetch = (async () => {
    calls++;
    return new Response("error", { status: 503 });
  }) as unknown as typeof fetch;

  const engine = new PluginPipelineEngine({
    webhookExecutor: new WebhookPluginExecutor({ fetchImpl: failingFetch }),
  });

  const closedPlugin: PluginRecord = {
    id: "wh-closed",
    name: "closed",
    version: "1.0.0",
    kind: "WEBHOOK",
    capability: "CONTENT",
    provider_config: null,
    digest: "b".repeat(64),
    wasm_bytes: null,
    webhook_config: {
      url: "http://down.local/check",
      failure_policy: "FAIL_CLOSED",
      circuit_failure_threshold: 2,
      circuit_open_ms: 60_000,
    },
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
  };

  const r1 = await engine.runUnary(
    "on_response",
    [{ plugin_id: closedPlugin.id, priority: 1 }],
    new Map([[closedPlugin.id, closedPlugin]]),
    { request_id: "r5", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    "hi",
  );
  const r2 = await engine.runUnary(
    "on_response",
    [{ plugin_id: closedPlugin.id, priority: 1 }],
    new Map([[closedPlugin.id, closedPlugin]]),
    { request_id: "r6", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    "hi",
  );
  const r3 = await engine.runUnary(
    "on_response",
    [{ plugin_id: closedPlugin.id, priority: 1 }],
    new Map([[closedPlugin.id, closedPlugin]]),
    { request_id: "r7", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    "hi",
  );
  expect(r1.blocked).toBe(true);
  expect(r2.blocked).toBe(true);
  expect(r3).toMatchObject({ blocked: true, rule_code: "webhook_circuit_open" });
  expect(calls).toBe(2);

  const openPlugin: PluginRecord = {
    ...closedPlugin,
    id: "wh-open",
    webhook_config: {
      url: "http://down.local/check",
      failure_policy: "FAIL_OPEN",
    },
  };
  const r4 = await engine.runUnary(
    "on_response",
    [{ plugin_id: openPlugin.id, priority: 1 }],
    new Map([[openPlugin.id, openPlugin]]),
    { request_id: "r8", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    "hello",
  );
  expect(r4).toMatchObject({ blocked: false, text: "hello" });
});

test("G3.2 combined HOLD + CUTOFF webhooks keep separate buffers, respect manifest.hooks, and recover after circuit open window", async () => {
  let nowMs = 1000;
  let webhookUp = false;
  const calledHooks: { url: string; hook: string; text: string }[] = [];

  const mockFetch = (async (url: string, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body ?? "{}")) as { hook: string; text: string };
    calledHooks.push({ url, hook: body.hook, text: body.text });
    if (url.includes("/flaky")) {
      if (!webhookUp) return new Response("down", { status: 503 });
      return Response.json({ action: "PASS" });
    }
    return Response.json({ action: "PASS" });
  }) as unknown as typeof fetch;

  const engine = new PluginPipelineEngine({
    webhookExecutor: new WebhookPluginExecutor({
      fetchImpl: mockFetch,
      now: () => nowMs,
    }),
  });

  const holdWh: PluginRecord = {
    id: "wh-combo-hold",
    name: "combo-hold",
    version: "1.0.0",
    kind: "WEBHOOK",
    capability: "CONTENT",
    provider_config: null,
    digest: "c".repeat(64),
    wasm_bytes: null,
    webhook_config: {
      url: "http://mod.local/hold",
      stream_mode: "HOLD_UNTIL_APPROVED",
      window_chars: 100,
    },
    signature: null,
    signing_key_id: null,
    manifest: { hooks: ["on_stream_chunk", "on_stream_end"] },
    enabled: true,
  };
  const cutoffWh: PluginRecord = {
    ...holdWh,
    id: "wh-combo-cutoff",
    name: "combo-cutoff",
    webhook_config: {
      url: "http://mod.local/cutoff",
      stream_mode: "STREAM_AND_CUTOFF",
      window_chars: 100,
    },
  };
  const plugins = new Map([
    [holdWh.id, holdWh],
    [cutoffWh.id, cutoffWh],
  ]);

  const preReq = await engine.runUnary(
    "on_request",
    [
      { plugin_id: holdWh.id, priority: 1 },
      { plugin_id: cutoffWh.id, priority: 2 },
    ],
    plugins,
    { request_id: "hook-filter", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    "prompt text",
  );
  expect(preReq.blocked).toBe(false);
  expect(calledHooks.length).toBe(0);

  const session = engine.createStreamSession(
    [
      { plugin_id: cutoffWh.id, priority: 1 },
      { plugin_id: holdWh.id, priority: 2 },
    ],
    plugins,
    { request_id: "combo-1", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    () => undefined,
  );
  let emitted = "";
  emitted += (await session.push("Hello ")).emit;
  emitted += (await session.push("world")).emit;
  const end = await session.finalize();
  expect(end.blocked).toBe(false);
  emitted += end.emit;
  expect(emitted).toBe("Hello world");
  const holdCalls = calledHooks.filter((c) => c.url.includes("/hold"));
  const cutoffCalls = calledHooks.filter((c) => c.url.includes("/cutoff"));
  expect(holdCalls.map((c) => c.text).join("")).toBe("Hello world");
  expect(cutoffCalls.map((c) => c.text).join("")).toBe("Hello world");

  const flakyWh: PluginRecord = {
    ...holdWh,
    id: "wh-flaky",
    name: "flaky",
    webhook_config: {
      url: "http://mod.local/flaky",
      failure_policy: "FAIL_CLOSED",
      circuit_failure_threshold: 2,
      circuit_open_ms: 5000,
    },
    manifest: {},
  };
  const flakyMap = new Map([[flakyWh.id, flakyWh]]);
  const f1 = await engine.runUnary(
    "on_response",
    [{ plugin_id: flakyWh.id, priority: 1 }],
    flakyMap,
    { request_id: "fl-1", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    "x",
  );
  const f2 = await engine.runUnary(
    "on_response",
    [{ plugin_id: flakyWh.id, priority: 1 }],
    flakyMap,
    { request_id: "fl-2", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    "x",
  );
  const f3 = await engine.runUnary(
    "on_response",
    [{ plugin_id: flakyWh.id, priority: 1 }],
    flakyMap,
    { request_id: "fl-3", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    "x",
  );
  expect(f1.blocked).toBe(true);
  expect(f2.blocked).toBe(true);
  expect(f3).toMatchObject({ blocked: true, rule_code: "webhook_circuit_open" });

  webhookUp = true;
  nowMs += 6000;
  const f4 = await engine.runUnary(
    "on_response",
    [{ plugin_id: flakyWh.id, priority: 1 }],
    flakyMap,
    { request_id: "fl-4", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    "recovered",
  );
  expect(f4).toMatchObject({ blocked: false, text: "recovered" });
});

test("a content Webhook applies policy based on the billing account field", async () => {
  const executor = new WebhookPluginExecutor({
    fetchImpl: async (_url, init) => {
      const payload = JSON.parse(String(init.body)) as Record<string, unknown>;
      return Response.json(
        payload.billing_account_id === "configured-account" && !("project_id" in payload)
          ? { action: "BLOCK", rule_code: "configured_account_policy" }
          : { action: "PASS" },
      );
    },
  });
  expect(
    await executor.invoke(
      "account-policy",
      {
        url: "https://policy.example.test/hook",
        failure_policy: "FAIL_CLOSED",
      },
      {
        hook: "on_request",
        request_id: "request",
        billing_account_id: "configured-account",
        model: "model",
        operation: "chat",
        text: "input",
      },
    ),
  ).toMatchObject({ action: "BLOCK", rule_code: "configured_account_policy" });
});

test("D3/D6/D2b webhook invalid-200 tripping, half-open probe, bounded response, concurrency limit, egress priority, ingress shedding, and on_request cache", async () => {
  let nowMs = 1000;
  let invalidCalls = 0;
  const badJsonFetch = (async () => {
    invalidCalls++;
    return Response.json({ unexpected: true });
  }) as unknown as typeof fetch;
  const whExec = new WebhookPluginExecutor({ fetchImpl: badJsonFetch, now: () => nowMs });
  const badCfg = {
    url: "http://mod.local/invalid",
    failure_policy: "FAIL_CLOSED" as const,
    circuit_failure_threshold: 2,
    circuit_open_ms: 5000,
  };
  const sampleInput = {
    hook: "on_response" as const,
    request_id: "r",
    billing_account_id: "p1",
    model: "m",
    operation: "chat",
    text: "hi",
  };
  const d1 = await whExec.invoke("wh-bad", badCfg, sampleInput);
  const d2 = await whExec.invoke("wh-bad", badCfg, sampleInput);
  const d3 = await whExec.invoke("wh-bad", badCfg, sampleInput);
  expect(d1).toMatchObject({
    action: "BLOCK",
    outcome_kind: "plugin_fault",
    rule_code: "webhook_invalid_reply",
  });
  expect(d2).toMatchObject({
    action: "BLOCK",
    outcome_kind: "plugin_fault",
    rule_code: "webhook_invalid_reply",
  });
  expect(d3).toMatchObject({
    action: "BLOCK",
    outcome_kind: "host_capacity",
    rule_code: "webhook_circuit_open",
  });
  expect(invalidCalls).toBe(2);

  const probeGate = Promise.withResolvers<Response>();
  let probeCalls = 0;
  const halfOpenExec = new WebhookPluginExecutor({
    fetchImpl: (async () => {
      probeCalls++;
      if (probeCalls <= 2) return new Response("err", { status: 503 });
      return probeGate.promise;
    }) as unknown as typeof fetch,
    now: () => nowMs,
  });
  await halfOpenExec.invoke("wh-ho", badCfg, sampleInput);
  await halfOpenExec.invoke("wh-ho", badCfg, sampleInput);
  nowMs += 6000;
  const probeReq = halfOpenExec.invoke("wh-ho", badCfg, sampleInput);
  const concurrentDuringProbe = await halfOpenExec.invoke("wh-ho", badCfg, sampleInput);
  expect(concurrentDuringProbe).toMatchObject({
    action: "BLOCK",
    outcome_kind: "host_capacity",
    rule_code: "webhook_circuit_open",
  });
  expect(probeCalls).toBe(3);
  probeGate.resolve(Response.json({ action: "PASS" }));
  expect(await probeReq).toMatchObject({ action: "PASS", outcome_kind: "content_decision" });

  const hugeExec = new WebhookPluginExecutor({
    fetchImpl: (async () =>
      new Response("x".repeat(2048), { status: 200 })) as unknown as typeof fetch,
  });
  const hugeRes = await hugeExec.invoke(
    "wh-huge",
    { url: "http://mod.local/huge", max_response_bytes: 128 },
    sampleInput,
  );
  expect(hugeRes).toMatchObject({
    action: "BLOCK",
    outcome_kind: "plugin_fault",
    rule_code: "webhook_response_too_large",
  });

  const concGate = Promise.withResolvers<Response>();
  const concExec = new WebhookPluginExecutor({
    fetchImpl: (async () => concGate.promise) as unknown as typeof fetch,
  });
  const pActive = concExec.invoke(
    "wh-conc",
    { url: "http://mod.local/slow", max_concurrency: 1 },
    sampleInput,
  );
  const pRejected = await concExec.invoke(
    "wh-conc",
    { url: "http://mod.local/slow", max_concurrency: 1 },
    sampleInput,
  );
  expect(pRejected).toMatchObject({
    action: "BLOCK",
    outcome_kind: "host_capacity",
    rule_code: "webhook_concurrency_limit",
  });
  concGate.resolve(Response.json({ action: "PASS" }));
  expect(await pActive).toMatchObject({ action: "PASS" });

  let mockCalls = 0;
  const countingWasm: WasmRuntimeExecutor = {
    runtimeKind: "mock",
    compile() {},
    async invoke(_id, _hook, input) {
      mockCalls++;
      const payload = JSON.parse(new TextDecoder().decode(input)) as { text: string };
      return new TextEncoder().encode(
        JSON.stringify({ action: "TRANSFORM", text: payload.text.toUpperCase() }),
      );
    },
    close() {},
  };
  const detBytes = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
  const detDigest = new Bun.CryptoHasher("sha256").update(detBytes).digest("hex");
  const detPlugin: PluginRecord = {
    id: "p-det",
    name: "det-wasm",
    version: "1.0.0",
    kind: "WASM",
    capability: "CONTENT",
    provider_config: null,
    digest: detDigest,
    wasm_bytes: detBytes,
    webhook_config: null,
    signature: null,
    signing_key_id: null,
    manifest: { hooks: ["on_request"], deterministic_text_only: true },
    enabled: true,
  };
  const cacheEngine = new PluginPipelineEngine({ wasmExecutor: countingWasm });
  const c1 = await cacheEngine.runUnary(
    "on_request",
    [{ plugin_id: detPlugin.id, priority: 1 }],
    new Map([[detPlugin.id, detPlugin]]),
    { request_id: "c1", billing_account_id: "proj-1", model: "m", operation: "chat" },
    "hello",
  );
  const c2 = await cacheEngine.runUnary(
    "on_request",
    [{ plugin_id: detPlugin.id, priority: 1 }],
    new Map([[detPlugin.id, detPlugin]]),
    { request_id: "c2", billing_account_id: "proj-1", model: "m", operation: "chat" },
    "hello",
  );
  expect(c1.text).toBe("HELLO");
  expect(c2.text).toBe("HELLO");
  expect(mockCalls).toBe(1);

  const sheddingExec = createWasmtimeExecutor({
    poolSize: 1,
    maxPending: 2,
    ingressShedRatio: 0.5,
    minTimeoutMs: 50,
    maxTimeoutMs: 500,
  });
  const loopBytes = buildInfiniteLoopWasm("on_response");
  const fastBytes = buildCrossTenantProbeWasm();
  sheddingExec.compile("loop", "loop-d", loopBytes);
  sheddingExec.compile("fast", "fast-d", fastBytes);
  try {
    await sheddingExec.selfCheck?.();
    const hold = sheddingExec
      .invoke("loop", "on_response", new Uint8Array(), {
        fuelBudget: 50_000_000,
        maxMemoryBytes: 4 * 1024 * 1024,
        priority: "egress",
        tenantId: "t0",
      })
      .catch(() => undefined);
    const queuedEgress = sheddingExec.invoke("fast", "on_response", new Uint8Array(), {
      fuelBudget: 2_000_000,
      maxMemoryBytes: 4 * 1024 * 1024,
      priority: "egress",
      tenantId: "t-egress",
    });
    const shedIngress = await sheddingExec
      .invoke("fast", "on_response", new Uint8Array(), {
        fuelBudget: 2_000_000,
        maxMemoryBytes: 4 * 1024 * 1024,
        priority: "ingress",
        tenantId: "t-ingress",
      })
      .catch((err: unknown) => err);
    expect(shedIngress).toBeInstanceOf(WasmCapacityError);
    expect((shedIngress as WasmCapacityError).code).toBe("wasm_ingress_shed");
    expect(await queuedEgress).toBeInstanceOf(Uint8Array);
    await hold;
  } finally {
    sheddingExec.close();
  }
});

test("symptom: compressed webhook response with small Content-Length buffers past max_response_bytes instead of cancelling stream", async () => {
  const input = {
    hook: "on_request" as const,
    request_id: "req-gzip-bound",
    billing_account_id: "acct-1",
    model: "gpt-4o",
    operation: "chat" as const,
    text: "hello",
  };
  const cfg = {
    url: "https://mod.example.test/hook",
    failure_policy: "FAIL_CLOSED" as const,
    max_response_bytes: 65_536,
  };

  const okExec = new WebhookPluginExecutor({
    fetchImpl: async () =>
      new Response(JSON.stringify({ action: "PASS" }), {
        status: 200,
        headers: {
          "content-type": "application/json",
          "content-encoding": "gzip",
          "content-length": "18",
        },
      }),
  });
  expect(await okExec.invoke("wh-gzip-ok", cfg, input)).toMatchObject({ action: "PASS" });

  let chunksRead = 0;
  let cancelled = false;
  const oversizedExec = new WebhookPluginExecutor({
    fetchImpl: async () =>
      new Response(
        new ReadableStream<Uint8Array>({
          pull(controller) {
            chunksRead++;
            if (chunksRead <= 20) controller.enqueue(new Uint8Array(32_768));
            else controller.close();
          },
          cancel() {
            cancelled = true;
          },
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
            "content-encoding": "gzip",
            "content-length": "512",
          },
        },
      ),
  });
  const blocked = await oversizedExec.invoke("wh-gzip-big", cfg, input);
  expect(blocked).toMatchObject({
    action: "BLOCK",
    outcome_kind: "plugin_fault",
    rule_code: "webhook_response_too_large",
  });
  expect(cancelled).toBe(true);
  expect(chunksRead).toBeLessThanOrEqual(4);
});
