import { expect, test } from "bun:test";
import { createKey } from "@gateway/runtime";
import { getBuiltinProvider } from "@gateway/plugin-engine";
import { handleInferenceRequest } from "../proxy.ts";
import type { ProxyDependencies } from "../inference/types.ts";
import {
  makeApiKey,
  makeBillingAccount,
  makeConfig,
  makeFakeDeps,
  makeSnapshot,
} from "./proxy-fixture.ts";
import { invokeInference, jsonBody } from "./proxy-response-harness.ts";

test("symptom: Idempotency-Key unary abort cancels pre-dispatch before connection_ready and enters grace after connection_ready", async () => {
  const preDispatch = await invokeInference(jsonBody, {
    cancel: true,
    delayMs: 25,
    headers: { "idempotency-key": "idem-pre-dispatch" },
    skipPrepareDispatch: true,
    emitTiming: "pre_dispatch",
  });
  expect(preDispatch.status).toBe(499);
  expect(preDispatch.finalized).toEqual(["RELEASED"]);
  expect(preDispatch.transportCancelled).toBe(true);

  const postConnect = await invokeInference(jsonBody, {
    cancel: true,
    delayMs: 25,
    headers: { "idempotency-key": "idem-post-connect" },
    skipPrepareDispatch: true,
    emitTiming: "connection_ready",
  });
  expect(postConnect.status).toBe(499);
  expect(postConnect.finalized).toEqual(["SETTLED"]);
  expect(postConnect.transportCancelled).toBe(false);
  expect(postConnect.settledUsages).toEqual([{ input_tokens: "3", output_tokens: "2" }]);
});

test("symptom: non-fast chunked unary body read settles within grace window and aborts when grace or attemptTimeoutMs expires", async () => {
  const settled = await invokeInference(jsonBody, {
    cancel: true,
    streamBody: true,
    delayMs: 20,
    configOverrides: { unaryClientAbortGraceMs: 200, attemptTimeoutMs: 500 },
  });
  expect(settled.status).toBe(499);
  expect(settled.finalized).toEqual(["SETTLED"]);
  expect(settled.settledUsages).toEqual([{ input_tokens: "3", output_tokens: "2" }]);

  const expiredGrace = await invokeInference(jsonBody, {
    cancel: true,
    streamBody: true,
    delayMs: 80,
    configOverrides: { unaryClientAbortGraceMs: 15, attemptTimeoutMs: 500 },
  });
  expect(expiredGrace.status).toBe(499);
  expect(expiredGrace.finalized).toEqual(["UNKNOWN"]);

  const cappedByAttempt = await invokeInference(jsonBody, {
    cancel: true,
    delayMs: 80,
    configOverrides: { unaryClientAbortGraceMs: 500, attemptTimeoutMs: 20 },
  });
  expect(cappedByAttempt.status).toBe(499);
  expect(cappedByAttempt.finalized).toEqual(["UNKNOWN"]);
  expect(cappedByAttempt.transportCancelled).toBe(true);
});

test("symptom: unary hold expires_at covers full effectiveAttemptMs even when unaryClientAbortGraceMs is shorter", async () => {
  const beforeMs = Date.now();
  const response = await invokeInference(jsonBody, {
    configOverrides: {
      holdTtlMs: 30_000,
      attemptTimeoutMs: 300_000,
      unaryClientAbortGraceMs: 60_000,
    },
  });
  expect(response.status).toBe(200);
  const ttlMs = new Date(response.holdExpiresAt!).getTime() - beforeMs;
  expect(ttlMs).toBeGreaterThanOrEqual(325_000);
});

test("symptom: non-Anthropic route with cache_write_tokens pricing falls back to ceiling reservation", async () => {
  const response = await invokeInference(jsonBody, {
    operation: "chat",
    customerRates: {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "1", denominator: "1" },
      cache_write_tokens: { numerator: "2", denominator: "1" },
    },
  });
  expect(response.status).toBe(200);
  expect(response.requestedCustomerAtoms).toBe("3100");
});

test("symptom: interrupted stream with complete observed usage settles without tagging estimated metric", async () => {
  const sseBody =
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"content":"done"}}],"usage":{"prompt_tokens":3,"completion_tokens":2}}\n\n' +
    'data: {"truncated":true';
  const response = await invokeInference(sseBody, { stream: true, fast: true });
  expect(response.status).toBe(200);
  expect(response.finalized).toEqual(["SETTLED"]);
  expect(response.settledUsages).toEqual([{ input_tokens: "3", output_tokens: "2" }]);
  expect(response.estimatedStreamSettlements).toBe(0);
});

test("symptom: stream already settled before terminal frame client disconnect neither double-settles nor tags estimated", async () => {
  const billingAccount = makeBillingAccount(crypto.randomUUID(), 10);
  const key = createKey();
  const snap = makeSnapshot(1, [billingAccount], [makeApiKey(key, billingAccount.id)]);
  const accountId = crypto.randomUUID();
  const channelId = crypto.randomUUID();
  const pluginId = crypto.randomUUID();
  const provider = getBuiltinProvider("openai")!;
  snap.plugins.set(pluginId, {
    id: pluginId,
    name: provider.name,
    version: provider.version,
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    digest: provider.digest,
    wasm_bytes: null,
    webhook_config: null,
    provider_config: provider.provider_config,
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
    builtin: true,
  });
  snap.accounts.set(accountId, { id: accountId, name: "u", state: "ACTIVE", revision: "1" });
  snap.channels.set(channelId, {
    id: channelId,
    account_id: accountId,
    name: "ch",
    kind: "openai",
    provider_plugin_id: pluginId,
    base_url: "http://127.0.0.1",
    secret_ciphertext: "",
    adapter_config: {},
    rpm: 1000,
    concurrency: 100,
    enabled: true,
    routing_policy: {},
    config_revision: 1,
  });
  const [cp, sp] = (["CUSTOMER", "SUPPLIER"] as const).map((side) => {
    const p = {
      id: crypto.randomUUID(),
      side,
      currency: "USD",
      basis: "PROVIDER_USAGE" as const,
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "1", denominator: "1" },
      },
      ceilings: { input_tokens: "1000", output_tokens: "1000" },
      source_digest: "test",
    };
    snap.prices.set(p.id, p);
    return p.id;
  });
  snap.models.set("test-chat", {
    name: "test-chat",
    operations: ["chat"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 100,
    enabled: true,
  });
  snap.routesByModel.set("test-chat", [
    {
      id: crypto.randomUUID(),
      model_name: "test-chat",
      channel_id: channelId,
      upstream_model: "test-chat",
      operations: ["chat"],
      billing_price_id: cp!,
      cost_version_id: sp!,
      priority: 1,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 1 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: {},
    },
  ]);
  const deps = makeFakeDeps(snap, makeConfig());
  const reqCtrl = new AbortController();
  let commitCalls = 0;
  let unknownCalls = 0;
  const settledGate = Promise.withResolvers<void>();
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "req", held_atoms: "1000" };
    },
    async commitTerminal() {
      commitCalls++;
      reqCtrl.abort(new Error("Client disconnected right after settlement"));
      settledGate.resolve();
      return { ok: true, charge_id: "charge", supplier_cost_atoms: "5" };
    },
    async markUnknown() {
      unknownCalls++;
    },
    async releaseHold() {},
  } as unknown as ProxyDependencies["ledger"];
  const sse =
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"content":"hello"}}]}\n\n' +
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2}}\n\n' +
    "data: [DONE]\n\n";
  const fetchFast: typeof deps.transport.fetchFast = async () => ({
    fast: false,
    response: new Response(new TextEncoder().encode(sse), {
      headers: { "content-type": "text/event-stream" },
    }),
  });
  deps.transport.prepareDispatch = async (url) => ({
    fetchFast: (init) => fetchFast(url, init, true),
    release() {},
  });
  deps.transport.fetchFast = fetchFast;
  try {
    const res = await handleInferenceRequest(
      {
        req: {
          raw: new Request("http://gateway.test/v1/chat/completions", {
            method: "POST",
            signal: reqCtrl.signal,
            headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
            body: JSON.stringify({
              model: "test-chat",
              stream: true,
              messages: [{ role: "user", content: "Hi" }],
            }),
          }),
          query: () => undefined,
        },
        json: (value, status = 200, headers = {}) => Response.json(value, { status, headers }),
      },
      deps,
      "chat",
    );
    const reader = res.body!.getReader();
    await reader.read();
    await settledGate.promise;
    await reader.cancel().catch(() => undefined);
    await Bun.sleep(20);
    expect(commitCalls).toBe(1);
    expect(unknownCalls).toBe(0);
    expect(deps.metrics.estimatedStreamSettlements).toBe(0);
  } finally {
    deps.transport.close();
    deps.state?.close();
  }
});

test("symptom: client disconnect at upstream step.done boundary settles observed output instead of marking UNKNOWN", async () => {
  const billingAccount = makeBillingAccount(crypto.randomUUID(), 10);
  const key = createKey();
  const snap = makeSnapshot(1, [billingAccount], [makeApiKey(key, billingAccount.id)]);
  const accountId = crypto.randomUUID();
  const channelId = crypto.randomUUID();
  const pluginId = crypto.randomUUID();
  const provider = getBuiltinProvider("openai")!;
  snap.plugins.set(pluginId, {
    id: pluginId,
    name: provider.name,
    version: provider.version,
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    digest: provider.digest,
    wasm_bytes: null,
    webhook_config: null,
    provider_config: provider.provider_config,
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
    builtin: true,
  });
  snap.accounts.set(accountId, { id: accountId, name: "u", state: "ACTIVE", revision: "1" });
  snap.channels.set(channelId, {
    id: channelId,
    account_id: accountId,
    name: "ch",
    kind: "openai",
    provider_plugin_id: pluginId,
    base_url: "http://127.0.0.1",
    secret_ciphertext: "",
    adapter_config: {},
    rpm: 1000,
    concurrency: 100,
    enabled: true,
    routing_policy: {},
    config_revision: 1,
  });
  const [cp, sp] = (["CUSTOMER", "SUPPLIER"] as const).map((side) => {
    const p = {
      id: crypto.randomUUID(),
      side,
      currency: "USD",
      basis: "PROVIDER_USAGE" as const,
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "1", denominator: "1" },
      },
      ceilings: { input_tokens: "1000", output_tokens: "1000" },
      source_digest: "test",
    };
    snap.prices.set(p.id, p);
    return p.id;
  });
  snap.models.set("test-chat", {
    name: "test-chat",
    operations: ["chat"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 100,
    enabled: true,
  });
  snap.routesByModel.set("test-chat", [
    {
      id: crypto.randomUUID(),
      model_name: "test-chat",
      channel_id: channelId,
      upstream_model: "test-chat",
      operations: ["chat"],
      billing_price_id: cp!,
      cost_version_id: sp!,
      priority: 1,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 1 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: {},
    },
  ]);
  const deps = makeFakeDeps(snap, makeConfig());
  const reqCtrl = new AbortController();
  const finalized: string[] = [];
  const doneGate = Promise.withResolvers<void>();
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "req", held_atoms: "1000" };
    },
    async commitTerminal() {
      finalized.push("SETTLED");
      doneGate.resolve();
      return { ok: true, charge_id: "charge", supplier_cost_atoms: "5" };
    },
    async markUnknown() {
      finalized.push("UNKNOWN");
      doneGate.resolve();
    },
    async releaseHold() {},
  } as unknown as ProxyDependencies["ledger"];
  const encoder = new TextEncoder();
  const chunkDelivered = Promise.withResolvers<void>();
  let pullCount = 0;
  const fetchFast: typeof deps.transport.fetchFast = async () => ({
    fast: false,
    response: new Response(
      new ReadableStream<Uint8Array>({
        async pull(ctrl) {
          pullCount++;
          if (pullCount === 1) {
            ctrl.enqueue(
              encoder.encode(
                'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"content":"partial answer"}}]}\n\n',
              ),
            );
            return;
          }
          await chunkDelivered.promise;
          ctrl.close();
          Promise.resolve().then(() => {
            reqCtrl.abort(new Error("Client disconnected at EOF boundary"));
          });
        },
      }),
      { headers: { "content-type": "text/event-stream" } },
    ),
  });
  deps.transport.prepareDispatch = async (url) => ({
    fetchFast: (init) => fetchFast(url, init, true),
    release() {},
  });
  deps.transport.fetchFast = fetchFast;
  try {
    const res = await handleInferenceRequest(
      {
        req: {
          raw: new Request("http://gateway.test/v1/chat/completions", {
            method: "POST",
            signal: reqCtrl.signal,
            headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
            body: JSON.stringify({
              model: "test-chat",
              stream: true,
              messages: [{ role: "user", content: "Hi" }],
            }),
          }),
          query: () => undefined,
        },
        json: (value, status = 200, headers = {}) => Response.json(value, { status, headers }),
      },
      deps,
      "chat",
    );
    const reader = res.body!.getReader();
    const first = await reader.read();
    expect(new TextDecoder().decode(first.value)).toContain("partial answer");
    chunkDelivered.resolve();
    await doneGate.promise;
    await reader.cancel().catch(() => undefined);
    expect(finalized).toEqual(["SETTLED"]);
    expect(deps.metrics.estimatedStreamSettlements).toBe(1);
  } finally {
    deps.transport.close();
    deps.state?.close();
  }
});
