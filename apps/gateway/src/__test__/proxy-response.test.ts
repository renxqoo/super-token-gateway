import { expect, test } from "bun:test";
import { createKey } from "@gateway/runtime";
import type { AdapterOverrides } from "@gateway/ledger";
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

async function invokeInference(
  body: string,
  {
    stream = false,
    fast = false,
    cancel = false,
    operation = "chat",
    fields = {},
    overrides = {},
  }: {
    stream?: boolean;
    fast?: boolean;
    cancel?: boolean;
    operation?: "chat" | "completions" | "responses" | "gemini" | "messages";
    fields?: Record<string, unknown>;
    overrides?: AdapterOverrides;
  } = {},
) {
  const billingAccount = makeBillingAccount(crypto.randomUUID(), 10);
  const key = createKey();
  const snap = makeSnapshot(1, [billingAccount], [makeApiKey(key, billingAccount.id)]);
  const accountId = crypto.randomUUID();
  const channelId = crypto.randomUUID();
  const pluginId = crypto.randomUUID();
  const model = "test-chat";
  const kind =
    operation === "gemini" ? "gemini" : operation === "messages" ? "anthropic" : "openai";
  const provider = getBuiltinProvider(kind)!;
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
    kind,
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
    operations: [operation],
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
      operations: [operation],
      billing_price_id: customerPrice!,
      cost_version_id: supplierPrice!,
      priority: 1,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 1 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: overrides,
    },
  ]);
  const deps = makeFakeDeps(snap, makeConfig());
  const controller = new AbortController();
  let transportCancelled = false;
  const finalized: string[] = [];
  let dispatches = 0;
  let holds = 0;
  deps.ledger = {
    async acquireHold() {
      holds++;
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
    dispatches++;
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
  const path = {
    chat: "/v1/chat/completions",
    completions: "/v1/completions",
    responses: "/v1/responses",
    messages: "/v1/messages",
    gemini: `/v1beta/models/${model}:generateContent`,
  }[operation];
  const raw = new Request(`http://gateway.test${path}`, {
    method: "POST",
    signal: controller.signal,
    headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
    body: JSON.stringify({
      model,
      stream,
      ...(operation === "chat" || operation === "messages"
        ? { messages: [{ role: "user", content: "Hi" }] }
        : {}),
      ...fields,
    }),
  });
  try {
    const response = await handleInferenceRequest(
      {
        req: { raw, query: () => undefined },
        json: (value, status = 200, headers = {}) => Response.json(value, { status, headers }),
      },
      deps,
      operation,
    );
    const wire = new TextDecoder("utf-8", { ignoreBOM: true }).decode(await response.arrayBuffer());
    return { status: response.status, wire, finalized, transportCancelled, dispatches, holds };
  } finally {
    deps.transport.close();
    deps.state?.close();
  }
}

test("symptom: a buffered response settles after the client disconnects", async () => {
  const response = await invokeInference(jsonBody, { cancel: true });
  expect({ status: response.status, finalized: response.finalized }, response.wire).toEqual({
    status: 499,
    finalized: ["UNKNOWN"],
  });
  expect(JSON.parse(response.wire)).toMatchObject({ error: { code: "client_aborted" } });
  expect(response.transportCancelled).toBe(true);
});

test("symptom: a charged JSON response passes an upstream BOM to strict clients", async () => {
  const response = await invokeInference(`\uFEFF${jsonBody}`);
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["SETTLED"]);
  expect(JSON.parse(response.wire)).toMatchObject({
    usage: { prompt_tokens: 3, completion_tokens: 2 },
  });
});

test("symptom: a later SSE error in the same upstream read drops already validated text", async () => {
  const response = await invokeInference(
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
  const response = await invokeInference(sseBody, { stream: true, fast: true });
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["SETTLED"]);
  expect(response.wire).toContain('"content":"hello"');
  expect(response.wire).toContain("data: [DONE]\n\n");
});

test("fast-buffered SSE marks unknown and returns stream_interrupted on incomplete trailing frame", async () => {
  const sseBody =
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"content":"partial"}}]}\n\n' +
    'data: {"truncated":true';
  const response = await invokeInference(sseBody, { stream: true, fast: true });
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
  const response = await invokeInference(sseBody, { stream: true, fast: true });
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["UNKNOWN"]);
  expect(response.wire).toContain('"content":"answer"');
  expect(response.wire).toContain("invalid_upstream_usage");
  expect(response.wire).not.toContain("data: [DONE]");
});

test("symptom: fast-buffered SSE drops already validated text when a later event errors", async () => {
  const response = await invokeInference(
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

test.each([
  { operation: "chat" as const, fields: { n: 2 }, code: "billing_ceiling" },
  {
    operation: "completions" as const,
    fields: { prompt: ["a", "b"], best_of: 2 },
    code: "billing_ceiling",
  },
  {
    operation: "gemini" as const,
    fields: { contents: [{ parts: [{ text: "Hi" }] }], generationConfig: { candidateCount: 2 } },
    code: "billing_ceiling",
  },
  {
    operation: "responses" as const,
    fields: { input: "Hi", previous_response_id: "resp_previous" },
    code: "unmetered_context",
  },
  {
    operation: "responses" as const,
    fields: { input: [{ type: "item_reference", id: "item_old" }] },
    code: "unmetered_context",
  },
  {
    operation: "gemini" as const,
    fields: { contents: [{ parts: [{ text: "Hi" }] }], cachedContent: "cachedContents/existing" },
    code: "unmetered_context",
  },
  {
    operation: "responses" as const,
    fields: { input: "Hi", tools: [{ type: "shell", environment: { type: "container_auto" } }] },
    code: "unmetered_tool",
  },
])(
  "symptom: $operation forwards an unbounded request ($fields)",
  async ({ operation, fields, code }) => {
    const response = await invokeInference(jsonBody, { operation, fields });
    expect(response.dispatches, response.wire).toBe(0);
    expect(response.status, response.wire).toBe(422);
    expect(JSON.parse(response.wire)).toMatchObject({
      error: operation === "gemini" ? { gateway_code: code } : { code },
    });
    expect(response.finalized).not.toContain("SETTLED");
  },
);

test("symptom: a route transform adds a hosted tool after client validation", async () => {
  const response = await invokeInference(jsonBody, {
    overrides: {
      request_transforms: [{ op: "force_set", path: "web_search_options", value: {} }],
    },
  });
  expect(response.dispatches, response.wire).toBe(0);
  expect(response.status, response.wire).toBe(422);
  expect(JSON.parse(response.wire)).toMatchObject({ error: { code: "unmetered_tool" } });
  expect(response.holds).toBe(0);
  expect(response.finalized).toEqual([]);
});

test("symptom: JSON settles when the provider omits metered output tokens", async () => {
  const response = await invokeInference(jsonBody.replace(',"completion_tokens":2', ""));
  expect(response.status, response.wire).toBe(502);
  expect(response.finalized).toEqual(["UNKNOWN"]);
  expect(JSON.parse(response.wire)).toMatchObject({ error: { code: "invalid_upstream_usage" } });
});

const chatStreamUsageCases: {
  fast: boolean;
  opening: string;
  overrides: AdapterOverrides;
  evidence: string;
}[] = [
  { fast: false, opening: "", overrides: {}, evidence: "missing" },
  {
    fast: true,
    opening: '"usage":{"prompt_tokens":3,"completion_tokens":1}',
    overrides: {},
    evidence: "stale native",
  },
  {
    fast: true,
    opening: '"meter":{"output":1}',
    overrides: {
      usage_aliases: [{ target: "output_tokens", source_path: "meter.output" }],
    },
    evidence: "stale mapped",
  },
];
test.each(chatStreamUsageCases)(
  "symptom: Chat SSE settles with $evidence output usage",
  async ({ fast, opening, overrides }) => {
    const response = await invokeInference(
      (opening
        ? `data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"beginning"}}],${opening}}\n\n`
        : "") +
        'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3}}\n\n' +
        "data: [DONE]\n\n",
      { stream: true, fast, overrides },
    );
    expect(response.status, response.wire).toBe(200);
    expect(response.finalized).toEqual(["UNKNOWN"]);
    expect(response.wire).toContain("invalid_upstream_usage");
    expect(response.wire).not.toContain("data: [DONE]");
  },
);

test("an explicit zero metered output settles normally", async () => {
  const response = await invokeInference(
    jsonBody.replace('"completion_tokens":2', '"completion_tokens":0'),
    {
      fields: { n: 1 },
    },
  );
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["SETTLED"]);
});

test("symptom: Anthropic SSE settles provisional output when final usage is missing", async () => {
  const response = await invokeInference(
    [
      {
        type: "message_start",
        message: {
          id: "msg_1",
          model: "test-chat",
          role: "assistant",
          content: [],
          usage: { input_tokens: 10, output_tokens: 1 },
        },
      },
      { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
      {
        type: "content_block_delta",
        index: 0,
        delta: { type: "text_delta", text: "An actual answer" },
      },
      { type: "content_block_stop", index: 0 },
      { type: "message_delta", delta: { stop_reason: "end_turn" } },
      { type: "message_stop" },
    ]
      .map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`)
      .join(""),
    { operation: "messages", stream: true },
  );
  expect(response.status, response.wire).toBe(200);
  expect(response.wire).toContain("An actual answer");
  expect(response.wire).toContain("invalid_upstream_usage");
  expect(response.wire).not.toContain("event: message_stop");
  expect(response.finalized).toEqual(["UNKNOWN"]);
});

test("symptom: Gemini SSE settles stale output usage after another content frame", async () => {
  const response = await invokeInference(
    [
      {
        candidates: [{ index: 0, content: { role: "model", parts: [{ text: "Beginning" }] } }],
        usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 1 },
      },
      {
        candidates: [
          {
            index: 0,
            content: { role: "model", parts: [{ text: "Rest of the answer" }] },
            finishReason: "STOP",
          },
        ],
        usageMetadata: { promptTokenCount: 10 },
      },
    ]
      .map((event) => `data: ${JSON.stringify(event)}\n\n`)
      .join(""),
    { operation: "gemini", stream: true, fields: { contents: [{ parts: [{ text: "Hi" }] }] } },
  );
  expect(response.status, response.wire).toBe(200);
  expect(response.wire).toContain("Beginning");
  expect(response.wire).toContain("invalid_upstream_usage");
  expect(response.wire).not.toContain('"finishReason":"STOP"');
  expect(response.finalized).toEqual(["UNKNOWN"]);
});

test("Responses accepts an explicitly local shell", async () => {
  const response = await invokeInference(
    '{"id":"resp_test","object":"response","model":"test-chat","status":"completed","output":[],"usage":{"input_tokens":3,"output_tokens":0}}',
    {
      operation: "responses",
      fields: { input: "Hi", tools: [{ type: "shell", environment: { type: "local" } }] },
    },
  );
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["SETTLED"]);
});
