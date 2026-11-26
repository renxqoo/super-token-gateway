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

const jsonBody =
  '{"model":"test-chat","choices":[{"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2}}';

async function invokeChat(body: string, { stream = false, fast = false, cancel = false } = {}) {
  const billingAccount = makeBillingAccount(crypto.randomUUID(), 10);
  const key = createKey();
  const snap = makeSnapshot(1, [billingAccount], [makeApiKey(key, billingAccount.id)]);
  const accountId = crypto.randomUUID();
  const channelId = crypto.randomUUID();
  const pluginId = crypto.randomUUID();
  const model = "test-chat";
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
  snap.accounts.set(accountId, {
    id: accountId,
    name: "chat-upstream",
    state: "ACTIVE",
    revision: "1",
  });
  snap.channels.set(channelId, {
    id: channelId,
    account_id: accountId,
    name: "chat-channel",
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
  const [customerPrice, supplierPrice] = (["CUSTOMER", "SUPPLIER"] as const).map((side) => {
    const price = {
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
    snap.prices.set(price.id, price);
    return price.id;
  });
  snap.models.set(model, {
    name: model,
    operations: ["chat"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 100,
    enabled: true,
  });
  snap.routesByModel.set(model, [
    {
      id: crypto.randomUUID(),
      model_name: model,
      channel_id: channelId,
      upstream_model: model,
      operations: ["chat"],
      billing_price_id: customerPrice!,
      cost_version_id: supplierPrice!,
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
  const controller = new AbortController();
  let transportCancelled = false;
  const finalized: string[] = [];
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "req", held_atoms: "1000" };
    },
    async commitTerminal() {
      finalized.push("SETTLED");
      return { ok: true, charge_id: "charge", supplier_cost_atoms: "5" };
    },
    async markUnknown() {
      finalized.push("UNKNOWN");
    },
    async releaseHold() {
      finalized.push("RELEASED");
    },
  } as unknown as ProxyDependencies["ledger"];
  const contentType = stream ? "text/event-stream" : "application/json";
  const bytes = new TextEncoder().encode(body);
  const fetchFast: typeof deps.transport.fetchFast = async (_url, init) => {
    if (stream && !fast) {
      const response = new Response(bytes, { headers: { "content-type": contentType } });
      return { fast: false, response };
    }
    const result = {
      fast: true as const,
      status: 200,
      contentType,
      headers: { "content-type": contentType },
      body: bytes,
    };
    if (!cancel) return result;
    const cancelled = Promise.withResolvers<never>();
    const detach = init.onCancelRegister?.((reason) => {
      transportCancelled = true;
      cancelled.reject(reason);
    });
    try {
      controller.abort(new Error("Client disconnected during upstream work"));
      return await Promise.race([cancelled.promise, Promise.resolve(result)]);
    } finally {
      detach?.();
    }
  };
  deps.transport.prepareDispatch = async (url) => ({
    fetchFast: (init) => fetchFast(url, init, true),
    release() {},
  });
  deps.transport.fetchFast = fetchFast;
  const raw = new Request("http://gateway.test/v1/chat/completions", {
    method: "POST",
    signal: controller.signal,
    headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
    body: JSON.stringify({ model, stream, messages: [{ role: "user", content: "Hi" }] }),
  });
  try {
    const response = await handleInferenceRequest(
      {
        req: { raw, query: () => undefined },
        json: (value, status = 200, headers = {}) => Response.json(value, { status, headers }),
      },
      deps,
      "chat",
    );
    const wire = new TextDecoder("utf-8", { ignoreBOM: true }).decode(await response.arrayBuffer());
    return { status: response.status, wire, finalized, transportCancelled };
  } finally {
    deps.transport.close();
    deps.state?.close();
  }
}

test("symptom: a buffered response settles after the client disconnects", async () => {
  const response = await invokeChat(jsonBody, { cancel: true });
  expect({ status: response.status, finalized: response.finalized }, response.wire).toEqual({
    status: 499,
    finalized: ["UNKNOWN"],
  });
  expect(JSON.parse(response.wire)).toMatchObject({ error: { code: "client_aborted" } });
  expect(response.transportCancelled).toBe(true);
});

test("symptom: a charged JSON response passes an upstream BOM to strict clients", async () => {
  const response = await invokeChat(`\uFEFF${jsonBody}`);
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["SETTLED"]);
  expect(JSON.parse(response.wire)).toMatchObject({
    usage: { prompt_tokens: 3, completion_tokens: 2 },
  });
});

test("symptom: a later SSE error in the same upstream read drops already validated text", async () => {
  const response = await invokeChat(
    'data: {"choices":[{"index":0,"delta":{"content":"valid answer prefix"}}]}\n\n' +
      'event: error\ndata: {"error":{"message":"Provider failed"}}\n\n',
    { stream: true },
  );
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["UNKNOWN"]);
  expect(response.wire).toContain("provider_stream_error");
  expect(response.wire).toContain("valid answer prefix");
  expect(response.wire.indexOf("valid answer prefix")).toBeLessThan(
    response.wire.indexOf("event: error"),
  );
});

test("fast-buffered SSE settles exact usage and returns complete SSE frames", async () => {
  const sseBody =
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"role":"assistant","content":"hello"}}]}\n\n' +
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2}}\n\n' +
    "data: [DONE]\n\n";
  const response = await invokeChat(sseBody, { stream: true, fast: true });
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["SETTLED"]);
  expect(response.wire).toContain('"content":"hello"');
  expect(response.wire).toContain("data: [DONE]\n\n");
});

test("fast-buffered SSE marks unknown and returns stream_interrupted on incomplete trailing frame", async () => {
  const sseBody =
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"content":"partial"}}]}\n\n' +
    'data: {"truncated":true';
  const response = await invokeChat(sseBody, { stream: true, fast: true });
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["UNKNOWN"]);
  expect(response.wire).toContain('"content":"partial"');
  expect(response.wire).toContain("stream_interrupted");
  expect(response.wire).not.toContain("data: [DONE]");
});

test("fast-buffered SSE marks unknown and holds terminal frame when usage metadata is missing", async () => {
  const sseBody =
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"content":"answer"}}]}\n\n' +
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n' +
    "data: [DONE]\n\n";
  const response = await invokeChat(sseBody, { stream: true, fast: true });
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["UNKNOWN"]);
  expect(response.wire).toContain('"content":"answer"');
  expect(response.wire).toContain("invalid_upstream_usage");
  expect(response.wire).not.toContain("data: [DONE]");
});

test("symptom: fast-buffered SSE drops already validated text when a later event errors", async () => {
  const response = await invokeChat(
    'data: {"choices":[{"index":0,"delta":{"content":"valid fast prefix"}}]}\n\n' +
      'event: error\ndata: {"error":{"message":"Provider failed"}}\n\n',
    { stream: true, fast: true },
  );
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["UNKNOWN"]);
  expect(response.wire).toContain("provider_stream_error");
  expect(response.wire).toContain("valid fast prefix");
  expect(response.wire.indexOf("valid fast prefix")).toBeLessThan(
    response.wire.indexOf("event: error"),
  );
});
