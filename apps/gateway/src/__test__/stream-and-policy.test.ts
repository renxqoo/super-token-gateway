import { expect, test } from "bun:test";
import type { PluginRecord, RouteRecord } from "@gateway/ledger";
import {
  getBuiltinProvider,
  PluginPipelineEngine,
  WebhookPluginExecutor,
} from "@gateway/plugin-engine";
import { createKey, VertexTokens } from "@gateway/runtime";
import { handleInferenceRequest } from "../proxy.ts";
import { createRouteInput } from "../inference/policy.ts";
import { createInferenceResources } from "../inference/resources.ts";
import { createInferenceTelemetry } from "../inference/telemetry.ts";
import type { InferenceRequest, ProxyDependencies } from "../inference/types.ts";
import { MeteredRequestError } from "../request-metering.ts";
import { splitSseChunkText, tryPrepareUpstreamRequestSync } from "../upstream.ts";
import {
  makeApiKey,
  makeBillingAccount,
  makeConfig,
  makeFakeDeps,
  makeSnapshot,
} from "./proxy-fixture.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

test("splitSseChunkText extracts text, stripped remainder, refusal flags, and Anthropic block markers in one parse", () => {
  const chatOnlyText = enc.encode(
    `data: ${JSON.stringify({ id: "c1", choices: [{ index: 0, delta: { content: "hello" }, finish_reason: null }] })}\n\n`,
  );
  expect(splitSseChunkText(chatOnlyText, "chat")).toEqual({
    text: "hello",
    remainder: null,
    hasOtherText: false,
  });

  const chatWithMeta = enc.encode(
    `data: ${JSON.stringify({ id: "c1", choices: [{ index: 0, delta: { role: "assistant", content: "secret" }, finish_reason: "stop" }], usage: { prompt_tokens: 2, completion_tokens: 1 } })}\n\n`,
  );
  const splitChat = splitSseChunkText(chatWithMeta, "chat");
  expect(splitChat.text).toBe("secret");
  expect(splitChat.hasOtherText).toBe(false);
  expect(splitChat.remainderHasText).toBe(false);
  expect(splitChat.remainder).not.toBeNull();
  const splitChatRemStr = dec.decode(splitChat.remainder!);
  expect(splitChatRemStr.startsWith("data: ")).toBe(true);
  expect(splitChatRemStr.endsWith("\n\n")).toBe(true);
  expect(JSON.parse(splitChatRemStr.slice(6).trim())).toEqual({
    id: "c1",
    choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: "stop" }],
    usage: { prompt_tokens: 2, completion_tokens: 1 },
  });

  const chatNoRem = splitSseChunkText(chatWithMeta, "chat", false);
  expect(chatNoRem).toEqual({ text: "secret", remainder: null, hasOtherText: false });

  const chatRefusal = enc.encode(
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { refusal: "cannot comply" } }] })}\n\n`,
  );
  expect(splitSseChunkText(chatRefusal, "chat")).toEqual({
    text: null,
    remainder: null,
    hasOtherText: true,
  });

  const chatRoleOnly = enc.encode(
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { role: "assistant" } }] })}\n\n`,
  );
  expect(splitSseChunkText(chatRoleOnly, "chat")).toEqual({
    text: null,
    remainder: null,
    hasOtherText: false,
  });

  const antStart = enc.encode(
    'event: content_block_start\ndata: {"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}\n\n',
  );
  expect(splitSseChunkText(antStart, "messages")).toEqual({
    text: null,
    remainder: null,
    hasOtherText: false,
    anthropicBlock: { kind: "text_start", index: 2 },
  });

  const antDelta = enc.encode(
    'event: content_block_delta\ndata: {"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"hi"}}\n\n',
  );
  expect(splitSseChunkText(antDelta, "messages")).toEqual({
    text: "hi",
    remainder: null,
    hasOtherText: false,
  });

  const antStop = enc.encode(
    'event: content_block_stop\ndata: {"type":"content_block_stop","index":2}\n\n',
  );
  expect(splitSseChunkText(antStop, "messages")).toEqual({
    text: null,
    remainder: null,
    hasOtherText: false,
    anthropicBlock: { kind: "block_stop", index: 2 },
  });

  const gemChunk = enc.encode(
    `data: ${JSON.stringify({ candidates: [{ index: 0, content: { role: "model", parts: [{ text: "plan", thought: true }, { text: "answer" }] }, finishReason: "STOP" }], usageMetadata: { promptTokenCount: 1, candidatesTokenCount: 1 } })}\n\n`,
  );
  const splitGem = splitSseChunkText(gemChunk, "gemini");
  expect(splitGem.text).toBe("answer");
  expect(splitGem.remainder).not.toBeNull();
  const gemRemText = dec.decode(splitGem.remainder!);
  expect(gemRemText).toContain('"thought":true');
  expect(gemRemText).toContain('"finishReason":"STOP"');
  expect(gemRemText).not.toContain('"answer"');

  expect(splitSseChunkText(enc.encode("data: [DONE]\n\n"), "chat")).toEqual({
    text: null,
    remainder: null,
    hasOtherText: false,
  });
  expect(splitSseChunkText(enc.encode("data: {broken\n\n"), "chat")).toEqual({
    text: null,
    remainder: null,
    hasOtherText: false,
  });
});

test("splitSseChunkText supports WHATWG SSE framing, co-occurring refusal/choices, error suppression, and multi-event fallback", () => {
  const cases: readonly [
    string,
    "chat" | "messages" | "responses" | "gemini",
    string,
    { text: string | null; remainder: null; hasOtherText: boolean | undefined },
  ][] = [
    [
      "spaceless chat CRLF",
      "chat",
      'data:{"choices":[{"index":0,"delta":{"content":"compact"}}]}\r\n\r\n',
      { text: "compact", remainder: null, hasOtherText: false },
    ],
    [
      "spaceless messages event",
      "messages",
      'event:content_block_start\r\ndata:{"index":0,"content_block":{"type":"text","text":"lead"}}\r\n\r\n',
      { text: null, remainder: null, hasOtherText: true },
    ],
    [
      "chat choices[1] content",
      "chat",
      'data: {"choices":[{"index":0,"delta":{"role":"assistant"}},{"index":1,"delta":{"content":"branch-2"}}]}\n\n',
      { text: null, remainder: null, hasOtherText: true },
    ],
    [
      "gemini candidates[1] text",
      "gemini",
      'data: {"candidates":[{"index":0,"content":{"parts":[{"text":"plan","thought":true}]}},{"index":1,"content":{"parts":[{"text":"alt"}]}}]}\n\n',
      { text: null, remainder: null, hasOtherText: true },
    ],
    [
      "responses refusal delta via event",
      "responses",
      'event: response.refusal.delta\ndata: {"delta":"blocked"}\n\n',
      { text: null, remainder: null, hasOtherText: true },
    ],
    [
      "event: error suppresses refusal",
      "chat",
      'event: error\ndata: {"choices":[{"index":0,"delta":{"refusal":"blocked"}}]}\n\n',
      { text: null, remainder: null, hasOtherText: false },
    ],
    [
      "obj.error suppresses refusal",
      "chat",
      'data: {"error":{"message":"boom"},"choices":[{"index":0,"delta":{"refusal":"blocked"}}]}\n\n',
      { text: null, remainder: null, hasOtherText: false },
    ],
    [
      "response.failed suppresses delta",
      "responses",
      'data: {"type":"response.failed","delta":"failed"}\n\n',
      { text: null, remainder: null, hasOtherText: false },
    ],
    [
      "multi-event LF",
      "chat",
      'data: {"choices":[{"index":0,"delta":{"role":"assistant"}}]}\n\ndata: {"choices":[{"index":0,"delta":{"content":"second"}}]}\n\n',
      { text: null, remainder: null, hasOtherText: undefined },
    ],
    [
      "multi-event CRLF",
      "chat",
      'data:{"a":1}\r\n\r\ndata:{"choices":[{"index":0,"delta":{"content":"hi"}}]}\r\n\r\n',
      { text: null, remainder: null, hasOtherText: undefined },
    ],
    [
      "comment then data multi-event",
      "chat",
      ': keepalive\n\ndata: {"choices":[{"index":0,"delta":{"content":"after"}}]}\n\n',
      { text: null, remainder: null, hasOtherText: undefined },
    ],
  ];
  for (const [label, protocol, wire, expected] of cases) {
    expect(splitSseChunkText(enc.encode(wire), protocol), label).toEqual(expected);
  }

  const contentAndRefusal = enc.encode(
    'data: {"id":"c2","choices":[{"index":0,"delta":{"content":"hi","refusal":"cannot comply"}}]}\n\n',
  );
  const splitRefusal = splitSseChunkText(contentAndRefusal, "chat");
  expect(splitRefusal.text).toBe("hi");
  expect(splitRefusal.hasOtherText).toBe(false);
  expect(splitRefusal.remainderHasText).toBe(true);
  expect(JSON.parse(dec.decode(splitRefusal.remainder!).slice(6).trim())).toEqual({
    id: "c2",
    choices: [{ index: 0, delta: { refusal: "cannot comply" } }],
  });

  const twoChoiceContent = enc.encode(
    'data: {"choices":[{"index":0,"delta":{"content":"branch-1"}},{"index":1,"delta":{"content":"branch-2"}}]}\n\n',
  );
  const splitTwoChoice = splitSseChunkText(twoChoiceContent, "chat");
  expect(splitTwoChoice.text).toBe("branch-1");
  expect(splitTwoChoice.remainderHasText).toBe(true);
  expect(JSON.parse(dec.decode(splitTwoChoice.remainder!).slice(6).trim())).toEqual({
    choices: [
      { index: 0, delta: {} },
      { index: 1, delta: { content: "branch-2" } },
    ],
  });
});

test("createRouteInput skips resources.link and transformRequestTexts synchronously when manifest.hooks excludes on_request", async () => {
  const billingAccount = makeBillingAccount(crypto.randomUUID(), 10);
  const key = createKey();
  const snap = makeSnapshot(1, [billingAccount], [makeApiKey(key, billingAccount.id)]);
  const whCalls: string[] = [];
  const whPlugin: PluginRecord = {
    id: "wh-out-only",
    name: "out-only-wh",
    version: "1.0.0",
    kind: "WEBHOOK",
    capability: "CONTENT",
    provider_config: null,
    digest: "a".repeat(64),
    wasm_bytes: null,
    webhook_config: { url: "http://mod.local/check" },
    signature: null,
    signing_key_id: null,
    manifest: { hooks: ["on_response", "on_stream_chunk", "on_stream_end"] },
    enabled: true,
  };
  const whReqPlugin: PluginRecord = {
    ...whPlugin,
    id: "wh-req",
    name: "req-wh",
    manifest: { hooks: ["on_request"] },
  };
  const tamperedWasm: PluginRecord = {
    id: "wasm-bad-digest",
    name: "bad-wasm",
    version: "1.0.0",
    kind: "WASM",
    capability: "CONTENT",
    provider_config: null,
    digest: "f".repeat(64),
    wasm_bytes: new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]),
    webhook_config: null,
    signature: null,
    signing_key_id: null,
    manifest: { hooks: ["on_response"] },
    enabled: true,
  };
  snap.plugins.set(whPlugin.id, whPlugin);
  snap.plugins.set(whReqPlugin.id, whReqPlugin);
  snap.plugins.set(tamperedWasm.id, tamperedWasm);

  const deps = makeFakeDeps(snap, makeConfig({ platformAbsorbMaxCountPerMinute: 1 }));
  deps.pluginEngine = new PluginPipelineEngine({
    webhookExecutor: new WebhookPluginExecutor({
      fetchImpl: async (_url, init) => {
        const payload = JSON.parse(String(init.body)) as { hook: string };
        whCalls.push(payload.hook);
        return {
          fast: true,
          status: 200,
          headers: { "content-type": "application/json" },
          body: enc.encode(JSON.stringify({ action: "PASS" })),
        };
      },
    }),
  });

  const controller = new AbortController();
  const raw = new Request("http://gateway.test/v1/chat/completions", {
    method: "POST",
    signal: controller.signal,
  });
  const resources = createInferenceResources(deps, deps.state!, controller.signal);
  let linkCalls = 0;
  const origLink = resources.link.bind(resources);
  resources.link = (...args) => {
    linkCalls++;
    return origLink(...args);
  };
  const clientBody = {
    model: "test-chat",
    messages: [{ role: "user", content: "hello world" }],
  };
  const trace = {
    traceId: "0".repeat(32),
    spanId: "0".repeat(16),
    flags: "01",
    traceparent: `00-${"0".repeat(32)}-${"0".repeat(16)}-01`,
  };
  const telemetry = createInferenceTelemetry(deps, "chat", "req-1", trace, performance.now());
  const request: InferenceRequest = {
    c: {
      req: { raw, query: () => undefined },
      json: (data, status = 200, headers = {}) => Response.json(data, { status, headers }),
    },
    deps,
    state: deps.state!,
    requestId: "req-1",
    operation: "chat",
    trace,
    requestStart: performance.now(),
    snap,
    key: makeApiKey(key, billingAccount.id),
    billingAccount,
    model: {
      name: "test-chat",
      operations: ["chat"],
      routing_strategy: "WEIGHTED",
      max_output_tokens: 100,
      enabled: true,
    },
    clientBody,
    multipartEntries: undefined,
    isMultipartOp: false,
    isStream: false,
    outputBound: 100,
    initialChars: 11,
    firstByteDeadlineMs: null,
    maxStreamDurationMs: 60_000,
  };
  const prepareInput = createRouteInput(request, resources, telemetry);
  const pluginSignal = new AbortController().signal;
  const makeCandidate = (pluginId: string) =>
    ({
      route: {
        id: "r1",
        model_name: "test-chat",
        channel_id: "ch1",
        upstream_model: "test-chat",
        operations: ["chat"],
        billing_price_id: "p1",
        cost_version_id: "p2",
        priority: 1,
        weight: 100,
        enabled: true,
        retry_policy: {},
        request_policy: {},
        plugin_pipeline: [{ plugin_id: pluginId, priority: 1 }],
        adapter_overrides: {},
      } satisfies RouteRecord,
    }) as Parameters<typeof prepareInput>[0];

  try {
    const skipped = prepareInput(makeCandidate(whPlugin.id), pluginSignal);
    expect(skipped instanceof Promise).toBe(false);
    expect(skipped instanceof Response).toBe(false);
    if (!(skipped instanceof Promise) && !(skipped instanceof Response)) {
      expect(skipped.body).toBe(clientBody);
      expect(skipped.audit).toEqual([]);
    }
    expect(linkCalls).toBe(0);
    expect(whCalls).toEqual([]);

    const ran = await prepareInput(makeCandidate(whReqPlugin.id), pluginSignal);
    expect(ran instanceof Response).toBe(false);
    expect(linkCalls).toBe(1);
    expect(whCalls).toEqual(["on_request"]);

    const tampered = await prepareInput(makeCandidate(tamperedWasm.id), pluginSignal);
    expect(tampered instanceof Response).toBe(true);
    if (tampered instanceof Response) {
      expect(tampered.status).toBe(500);
      expect(await tampered.json()).toMatchObject({
        error: { code: "plugin_execution_failed", rule_code: "plugin_digest_mismatch" },
      });
    }

    deps.metrics.recordPlatformAbsorb(billingAccount.id, "100");
    const shed = prepareInput(makeCandidate(whPlugin.id), pluginSignal);
    expect(shed instanceof Response).toBe(true);
    if (shed instanceof Response) {
      expect(shed.status).toBe(503);
      expect(await shed.json()).toMatchObject({ error: { code: "plugin_capacity_unavailable" } });
    }
  } finally {
    deps.transport.close();
    deps.state?.close();
  }
});

test("WebhookPluginExecutor fast buffered response enforces status, size limits, and streaming moderation", async () => {
  const seenHooks: string[] = [];
  const executor = new WebhookPluginExecutor({
    fetchImpl: async (_url, init) => {
      const payload = JSON.parse(String(init.body)) as { hook: string; text: string };
      seenHooks.push(payload.hook);
      if (payload.text.includes("OVERSIZE")) {
        return {
          fast: true,
          status: 200,
          headers: { "content-type": "application/json" },
          body: new Uint8Array(256),
        };
      }
      if (payload.text.includes("ERR503")) {
        return {
          fast: true,
          status: 503,
          headers: { "content-type": "text/plain" },
          body: enc.encode("unavailable"),
        };
      }
      return {
        fast: true,
        status: 200,
        headers: { "content-type": "application/json" },
        body: enc.encode(
          JSON.stringify({
            action: "TRANSFORM",
            text: payload.text.replaceAll("SECRET", "[MASKED]"),
          }),
        ),
      };
    },
  });

  const oversized = await executor.invoke(
    "wh-fast-size",
    { url: "http://mod.local/check", max_response_bytes: 64 },
    {
      hook: "on_response",
      request_id: "r1",
      billing_account_id: "a1",
      model: "m",
      operation: "chat",
      text: "OVERSIZE",
    },
  );
  expect(oversized).toMatchObject({
    action: "BLOCK",
    outcome_kind: "plugin_fault",
    rule_code: "webhook_response_too_large",
  });

  const httpErr = await executor.invoke(
    "wh-fast-err",
    { url: "http://mod.local/check" },
    {
      hook: "on_response",
      request_id: "r2",
      billing_account_id: "a1",
      model: "m",
      operation: "chat",
      text: "ERR503",
    },
  );
  expect(httpErr).toMatchObject({
    action: "BLOCK",
    outcome_kind: "host_capacity",
    rule_code: "webhook_http_503",
  });

  const billingAccount = makeBillingAccount(crypto.randomUUID(), 10);
  const key = createKey();
  const snap = makeSnapshot(1, [billingAccount], [makeApiKey(key, billingAccount.id)]);
  const accountId = crypto.randomUUID();
  const channelId = crypto.randomUUID();
  const providerId = crypto.randomUUID();
  const modPluginId = crypto.randomUUID();
  const provider = getBuiltinProvider("openai")!;
  snap.plugins.set(providerId, {
    id: providerId,
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
  snap.plugins.set(modPluginId, {
    id: modPluginId,
    name: "stream-mod",
    version: "1.0.0",
    capability: "CONTENT",
    kind: "WEBHOOK",
    digest: "b".repeat(64),
    wasm_bytes: null,
    webhook_config: {
      url: "http://mod.local/check",
      stream_mode: "HOLD_UNTIL_APPROVED",
      window_chars: 16,
    },
    provider_config: null,
    signature: null,
    signing_key_id: null,
    manifest: { hooks: ["on_stream_chunk", "on_stream_end"] },
    enabled: true,
  });
  snap.accounts.set(accountId, {
    id: accountId,
    name: "up",
    state: "ACTIVE",
    revision: "1",
  });
  snap.channels.set(channelId, {
    id: channelId,
    account_id: accountId,
    name: "ch",
    kind: "openai",
    provider_plugin_id: providerId,
    base_url: "http://127.0.0.1",
    secret_ciphertext: "",
    adapter_config: {},
    rpm: 1000,
    concurrency: 100,
    enabled: true,
    routing_policy: {},
    config_revision: 1,
  });
  const [custPrice, supPrice] = (["CUSTOMER", "SUPPLIER"] as const).map((side) => {
    const id = crypto.randomUUID();
    snap.prices.set(id, {
      id,
      side,
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "1", denominator: "1" },
      },
      ceilings: { input_tokens: "1000", output_tokens: "1000" },
      source_digest: "t",
    });
    return id;
  });
  snap.models.set("mod-chat", {
    name: "mod-chat",
    operations: ["chat"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 100,
    enabled: true,
  });
  snap.routesByModel.set("mod-chat", [
    {
      id: crypto.randomUUID(),
      model_name: "mod-chat",
      channel_id: channelId,
      upstream_model: "mod-chat",
      operations: ["chat"],
      billing_price_id: custPrice!,
      cost_version_id: supPrice!,
      priority: 1,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 1 },
      request_policy: {},
      plugin_pipeline: [{ plugin_id: modPluginId, priority: 1 }],
      adapter_overrides: {},
    },
  ]);

  const deps = makeFakeDeps(snap, makeConfig());
  deps.pluginEngine = new PluginPipelineEngine({ webhookExecutor: executor });
  let settled = false;
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "r-mod", held_atoms: "100" };
    },
    async commitTerminal() {
      settled = true;
      return { ok: true, charge_id: "chg-1", supplier_cost_atoms: "5" };
    },
    async markUnknown() {},
    async releaseHold() {},
  } as unknown as ProxyDependencies["ledger"];
  const sseWire =
    'data: {"id":"c1","object":"chat.completion.chunk","model":"mod-chat","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello SECRET."}}]}\n\n' +
    'data: {"id":"c1","object":"chat.completion.chunk","model":"mod-chat","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2}}\n\n' +
    "data: [DONE]\n\n";
  const fetchFast: typeof deps.transport.fetchFast = async () => ({
    fast: false,
    response: new Response(enc.encode(sseWire), {
      headers: { "content-type": "text/event-stream" },
    }),
  });
  deps.transport.prepareDispatch = async (url) => ({
    fetchFast: (init) => fetchFast(url, init, true),
    release() {},
  });
  deps.transport.fetchFast = fetchFast;
  seenHooks.length = 0;
  try {
    const resp = await handleInferenceRequest(
      {
        req: {
          raw: new Request("http://gateway.test/v1/chat/completions", {
            method: "POST",
            headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
            body: JSON.stringify({
              model: "mod-chat",
              stream: true,
              messages: [{ role: "user", content: "hi" }],
            }),
          }),
          query: () => undefined,
        },
        json: (v, s = 200, h = {}) => Response.json(v, { status: s, headers: h }),
      },
      deps,
      "chat",
    );
    expect(resp.status).toBe(200);
    const body = await resp.text();
    expect(body).toContain("Hello [MASKED].");
    expect(body).not.toContain("SECRET");
    expect(body).toContain("data: [DONE]");
    expect(settled).toBe(true);
    expect(seenHooks).not.toContain("on_request");
  } finally {
    deps.transport.close();
    deps.state?.close();
  }
});

test("tryPrepareUpstreamRequestSync validates direct callers and re-validates after request_transforms even with alreadyValidated", () => {
  const provider = getBuiltinProvider("openai")!;
  const pluginId = crypto.randomUUID();
  const snap = {
    plugins: new Map([
      [
        pluginId,
        {
          id: pluginId,
          name: provider.name,
          version: provider.version,
          capability: "PROVIDER" as const,
          kind: "DECLARATIVE" as const,
          digest: provider.digest,
          wasm_bytes: null,
          webhook_config: null,
          provider_config: provider.provider_config,
          signature: null,
          signing_key_id: null,
          manifest: {},
          enabled: true,
          builtin: true,
        },
      ],
    ]),
    providerPluginsByName: new Map(),
  };
  const channel = {
    id: crypto.randomUUID(),
    account_id: crypto.randomUUID(),
    name: "ch",
    kind: "openai",
    provider_plugin_id: pluginId,
    base_url: "https://api.example.test",
    secret_ciphertext: "",
    adapter_config: {},
    rpm: 1000,
    concurrency: 100,
    enabled: true,
    routing_policy: {},
    config_revision: 1,
  };
  const plainRoute: RouteRecord = {
    id: crypto.randomUUID(),
    model_name: "gpt-4o",
    channel_id: channel.id,
    upstream_model: "gpt-4o-up",
    operations: ["chat"],
    billing_price_id: "p1",
    cost_version_id: "p2",
    priority: 1,
    weight: 100,
    enabled: true,
    retry_policy: {},
    request_policy: {},
    plugin_pipeline: [],
    adapter_overrides: {},
  };
  const transformRoute: RouteRecord = {
    ...plainRoute,
    id: crypto.randomUUID(),
    adapter_overrides: {
      request_transforms: [{ op: "force_set", path: "web_search_options", value: {} }],
    },
  };
  const vertexTokens = new VertexTokens(async () => new Response("{}"));

  expect(() =>
    tryPrepareUpstreamRequestSync({
      channel,
      route: plainRoute,
      snapshot: snap,
      secret: "sk-test",
      operation: "chat",
      clientBody: { model: "gpt-4o", messages: [{ role: "user", content: "hi" }], n: 2 },
      isStream: false,
      vertexTokens,
      wasmExecutor: null,
    }),
  ).toThrow(MeteredRequestError);

  expect(() =>
    tryPrepareUpstreamRequestSync({
      channel,
      route: transformRoute,
      snapshot: snap,
      secret: "sk-test",
      operation: "chat",
      clientBody: { model: "gpt-4o", messages: [{ role: "user", content: "hi" }] },
      isStream: false,
      alreadyValidated: true,
      vertexTokens,
      wasmExecutor: null,
    }),
  ).toThrow(MeteredRequestError);

  const clientBody: Record<string, unknown> = {
    model: "gpt-4o",
    messages: [{ role: "user", content: "hi" }],
  };
  const prepared = tryPrepareUpstreamRequestSync({
    channel,
    route: plainRoute,
    snapshot: snap,
    secret: "sk-test",
    operation: "chat",
    clientBody,
    outputBound: 50,
    isStream: false,
    canMutateClientBody: true,
    alreadyValidated: true,
    vertexTokens,
    wasmExecutor: null,
  });
  expect(prepared).not.toBeNull();
  expect(clientBody.model).toBe("gpt-4o-up");
  expect(clientBody.max_completion_tokens).toBe(50);
  expect(JSON.parse(String(prepared!.body))).toMatchObject({
    model: "gpt-4o-up",
    max_completion_tokens: 50,
  });
});
