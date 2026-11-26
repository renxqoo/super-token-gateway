import { expect, test } from "bun:test";
import type { ChannelRecord, PluginRecord, RouteRecord } from "@gateway/ledger";
import { createKey, SmartRouter, VertexTokens } from "@gateway/runtime";
import { getBuiltinProvider } from "@gateway/plugin-engine";
import type { ProxyDependencies } from "../inference/types.ts";
import { handleInferenceRequest } from "../proxy.ts";
import { tryPrepareUpstreamRequestSync } from "../upstream.ts";
import {
  makeBillingAccount,
  makeSnapshot,
  makeApiKey,
  makeConfig,
  makeFakeDeps,
} from "./proxy-fixture.ts";

test("symptom: tryPrepareUpstreamRequestSync never marks unsafe bearer secrets headersValidated and preserves content-type", () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111111", 10);
  const snap = makeSnapshot(1, [proj], []);
  const preset = getBuiltinProvider("openai")!;
  const pluginId = "88888888-8888-4888-8888-888888888888";
  const pluginRecord: PluginRecord = {
    id: pluginId,
    name: preset.name,
    version: preset.version,
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    digest: preset.digest,
    wasm_bytes: null,
    webhook_config: null,
    provider_config: preset.provider_config,
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
    builtin: true,
  };
  snap.plugins.set(pluginId, pluginRecord);
  snap.providerPluginsByName.set("openai", pluginRecord);
  const channel: ChannelRecord = {
    id: "33333333-3333-4333-8333-333333333333",
    account_id: "44444444-4444-4444-8444-444444444444",
    name: "openai-ch",
    kind: "openai",
    provider_plugin_id: pluginId,
    base_url: "https://api.openai.com",
    secret_ciphertext: "",
    adapter_config: { extra_headers: { "content-type": "text/plain", "x-custom": "1" } },
    rpm: 1000,
    concurrency: 100,
    enabled: true,
    routing_policy: {},
    config_revision: 1,
  };
  const route: RouteRecord = {
    id: "55555555-5555-4555-8555-555555555555",
    model_name: "gpt-4o",
    channel_id: channel.id,
    upstream_model: "gpt-4o",
    operations: ["chat"],
    billing_price_id: "66666666-6666-4666-8666-666666666666",
    cost_version_id: "77777777-7777-4777-8777-777777777777",
    priority: 1,
    weight: 100,
    enabled: true,
    retry_policy: {},
    request_policy: {},
    plugin_pipeline: [],
    adapter_overrides: {},
  };
  const baseBody = { model: "gpt-4o", messages: [{ role: "user", content: "hi" }] };
  const vertexTokens = new VertexTokens();

  const withExtra = tryPrepareUpstreamRequestSync({
    snapshot: snap,
    channel,
    route,
    operation: "chat",
    clientBody: baseBody,
    isStream: false,
    secret: "sk-valid-key",
    vertexTokens,
    wasmExecutor: null,
  });
  expect(withExtra).not.toBeNull();
  const extraHeaders = withExtra!.headers as Record<string, string>;
  expect(extraHeaders["content-type"]).toBe("application/json");
  expect(withExtra!.headersValidated).toBe(false);

  const cleanChannel: ChannelRecord = { ...channel, adapter_config: {} };
  const crlfSecret = tryPrepareUpstreamRequestSync({
    snapshot: snap,
    channel: cleanChannel,
    route,
    operation: "chat",
    clientBody: baseBody,
    isStream: false,
    secret: "sk-bad\r\nX-Injected: 1",
    vertexTokens,
    wasmExecutor: null,
  });
  expect(crlfSecret).not.toBeNull();
  expect(crlfSecret!.headersValidated).toBe(false);

  const unicodeSecret = tryPrepareUpstreamRequestSync({
    snapshot: snap,
    channel: cleanChannel,
    route,
    operation: "chat",
    clientBody: baseBody,
    isStream: false,
    secret: "sk-bad\u010d\u010aX-Injected: 1",
    vertexTokens,
    wasmExecutor: null,
  });
  expect(unicodeSecret).not.toBeNull();
  expect(unicodeSecret!.headersValidated).toBe(false);

  const cleanSecret = tryPrepareUpstreamRequestSync({
    snapshot: snap,
    channel: cleanChannel,
    route,
    operation: "chat",
    clientBody: baseBody,
    isStream: false,
    secret: "sk-clean-123",
    vertexTokens,
    wasmExecutor: null,
  });
  expect(cleanSecret).not.toBeNull();
  expect(cleanSecret!.headersValidated).toBe(true);
});

test("failover skips newly-OPEN backup channels without redundant switchSupplierAttempt or upstream dispatch, and honors firstByteTimeoutMs and maxAmbiguousRetriesPerMinute", async () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111111", 10);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const preset = getBuiltinProvider("openai")!;
  const pluginId = "88888888-8888-4888-8888-888888888888";
  const pluginRecord: PluginRecord = {
    id: pluginId,
    name: preset.name,
    version: preset.version,
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    digest: preset.digest,
    wasm_bytes: null,
    webhook_config: null,
    provider_config: preset.provider_config,
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
    builtin: true,
  };
  snap.plugins.set(pluginId, pluginRecord);
  snap.providerPluginsByName.set("openai", pluginRecord);

  const billingPrice = {
    id: "66666666-6666-4666-8666-666666666666",
    side: "CUSTOMER" as const,
    currency: "USD",
    basis: "PROVIDER_USAGE" as const,
    rates: {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "1", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", output_tokens: "1000" },
    source_digest: "d1",
  };
  const costPrice = {
    id: "77777777-7777-4777-8777-777777777777",
    side: "SUPPLIER" as const,
    currency: "USD",
    basis: "PROVIDER_USAGE" as const,
    rates: {
      input_tokens: { numerator: "1", denominator: "2" },
      output_tokens: { numerator: "1", denominator: "2" },
    },
    ceilings: { input_tokens: "1000", output_tokens: "1000" },
    source_digest: "d2",
  };
  snap.prices.set(billingPrice.id, billingPrice);
  snap.prices.set(costPrice.id, costPrice);
  snap.models.set("gpt-4o", {
    name: "gpt-4o",
    operations: ["chat"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 1000,
    enabled: true,
  });

  const hits = { bad1: 0, bad2: 0, hang: 0, good: 0 };
  let routerRef: SmartRouter | null = null;
  const chBad1Id = "33333333-3333-4333-8333-333333333301";
  const chBad2Id = "33333333-3333-4333-8333-333333333302";
  const chGoodId = "33333333-3333-4333-8333-333333333303";
  const chHangId = "33333333-3333-4333-8333-333333333304";
  const acctId = "44444444-4444-4444-8444-444444444444";
  snap.accounts.set(acctId, { id: acctId, name: "acct-1", state: "ACTIVE", revision: "1" });

  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      if (path.startsWith("/bad1/")) {
        hits.bad1++;
        routerRef?.recordFailure(chBad2Id, "retryable");
        routerRef?.recordFailure(chBad2Id, "retryable");
        routerRef?.recordFailure(chBad2Id, "retryable");
        return new Response("upstream error", { status: 500 });
      }
      if (path.startsWith("/bad2/")) {
        hits.bad2++;
        return new Response("should not be reached", { status: 500 });
      }
      if (path.startsWith("/hang/")) {
        hits.hang++;
        await Bun.sleep(2000);
        return Response.json({
          id: "late",
          object: "chat.completion",
          model: "gpt-4o",
          choices: [
            { index: 0, message: { role: "assistant", content: "late" }, finish_reason: "stop" },
          ],
          usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
        });
      }
      hits.good++;
      return Response.json({
        id: "ok-1",
        object: "chat.completion",
        model: "gpt-4o",
        choices: [
          { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
        ],
        usage: { prompt_tokens: 5, completion_tokens: 5, total_tokens: 10 },
      });
    },
  });

  const makeCh = (id: string, name: string, prefix: string): ChannelRecord => ({
    id,
    account_id: acctId,
    name,
    kind: "openai",
    provider_plugin_id: pluginId,
    base_url: `http://127.0.0.1:${upstream.port}/${prefix}`,
    secret_ciphertext: "",
    adapter_config: {},
    rpm: 10_000,
    concurrency: 100,
    enabled: true,
    routing_policy: {},
    config_revision: 1,
  });
  const makeRt = (
    id: string,
    channelId: string,
    priority: number,
    retryPolicy: RouteRecord["retry_policy"] = {},
  ): RouteRecord => ({
    id,
    model_name: "gpt-4o",
    channel_id: channelId,
    upstream_model: "gpt-4o",
    operations: ["chat"],
    billing_price_id: billingPrice.id,
    cost_version_id: costPrice.id,
    priority,
    weight: 100,
    enabled: true,
    retry_policy: retryPolicy,
    request_policy: {},
    plugin_pipeline: [],
    adapter_overrides: {},
  });

  snap.channels.set(chBad1Id, makeCh(chBad1Id, "ch-bad-1", "bad1"));
  snap.channels.set(chBad2Id, makeCh(chBad2Id, "ch-bad-2", "bad2"));
  snap.channels.set(chGoodId, makeCh(chGoodId, "ch-good", "good"));
  snap.channels.set(chHangId, makeCh(chHangId, "ch-hang", "hang"));
  snap.models.set("gpt-4o-hang", {
    name: "gpt-4o-hang",
    operations: ["chat"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 1000,
    enabled: true,
  });
  snap.routesByModel.set("gpt-4o", [
    makeRt("r1", chBad1Id, 30, { max_attempts: 3 }),
    makeRt("r2", chBad2Id, 20, { max_attempts: 3 }),
    makeRt("r3", chGoodId, 10, { max_attempts: 3 }),
  ]);
  snap.routesByModel.set("gpt-4o-hang", [
    {
      ...makeRt("r-hang", chHangId, 30, {
        max_attempts: 2,
        attempt_timeout_ms: 5_000,
        retry_on_ambiguous: true,
      }),
      model_name: "gpt-4o-hang",
    },
    {
      ...makeRt("r-good", chGoodId, 10, { max_attempts: 2 }),
      model_name: "gpt-4o-hang",
    },
  ]);

  const switchTargets: string[] = [];
  const config = makeConfig({
    attemptTimeoutMs: 3_000,
    firstByteTimeoutMs: 500,
    maxAmbiguousRetriesPerMinute: 1,
  });
  const deps = makeFakeDeps(snap, config);
  routerRef = deps.router;
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "req", held_atoms: "1000" };
    },
    async switchSupplierAttempt(input: { next_channel_id: string }) {
      switchTargets.push(input.next_channel_id);
      return { ok: true, held_atoms: "1000" };
    },
    async commitTerminal() {
      return {
        ok: true,
        replayed: false,
        charge_id: "chg-1",
        customer_atoms: "10",
        supplier_cost_atoms: "5",
      };
    },
    async releaseHold() {},
    async markUnknown() {},
  } as unknown as ProxyDependencies["ledger"];

  const invoke = (model = "gpt-4o") =>
    handleInferenceRequest(
      {
        req: {
          raw: new Request("http://127.0.0.1/v1/chat/completions", {
            method: "POST",
            headers: { authorization: `Bearer ${rawKey}`, "content-type": "application/json" },
            body: JSON.stringify({ model, messages: [{ role: "user", content: "hi" }] }),
          }),
          query: () => undefined,
        },
        json(data, status, headers) {
          return new Response(JSON.stringify(data), {
            status: status ?? 200,
            headers: { "content-type": "application/json", ...headers },
          });
        },
      },
      deps,
      "chat",
    );

  try {
    const res1 = await invoke("gpt-4o");
    expect(res1.status).toBe(200);
    expect(res1.headers.get("x-gateway-channel")).toBe("ch-good");
    expect(hits.bad1).toBe(1);
    expect(hits.bad2).toBe(0);
    expect(hits.good).toBe(1);
    expect(switchTargets).toEqual([chGoodId]);

    const t0 = performance.now();
    const res2 = await invoke("gpt-4o-hang");
    const elapsed2 = performance.now() - t0;
    expect(res2.status).toBe(200);
    expect(res2.headers.get("x-gateway-channel")).toBe("ch-good");
    expect(hits.hang).toBe(1);
    expect(elapsed2).toBeGreaterThanOrEqual(200);
    expect(elapsed2).toBeLessThan(650);

    const res3 = await invoke("gpt-4o-hang");
    expect(res3.status).toBe(504);
    expect(hits.hang).toBe(2);
    expect(await res3.json()).toMatchObject({ error: { code: "upstream_timeout" } });
    expect(deps.metrics.ambiguousRetriesCappedTotal).toBe(1);
  } finally {
    deps.state?.close();
    deps.transport.close();
    await upstream.stop(true);
  }
});

test("proxy retries transient hold finalization failures and preserves upstream error status instead of returning 500", async () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111199", 10);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const preset = getBuiltinProvider("openai")!;
  const pluginId = "88888888-8888-4888-8888-888888888899";
  const pluginRecord: PluginRecord = {
    id: pluginId,
    name: preset.name,
    version: preset.version,
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    digest: preset.digest,
    wasm_bytes: null,
    webhook_config: null,
    provider_config: preset.provider_config,
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
    builtin: true,
  };
  snap.plugins.set(pluginId, pluginRecord);
  snap.providerPluginsByName.set("openai", pluginRecord);
  const billingPrice = {
    id: "66666666-6666-4666-8666-666666666699",
    side: "CUSTOMER" as const,
    currency: "USD",
    basis: "PROVIDER_USAGE" as const,
    rates: {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "1", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", output_tokens: "1000" },
    source_digest: "d1",
  };
  const costPrice = {
    id: "77777777-7777-4777-8777-777777777799",
    side: "SUPPLIER" as const,
    currency: "USD",
    basis: "PROVIDER_USAGE" as const,
    rates: {
      input_tokens: { numerator: "1", denominator: "2" },
      output_tokens: { numerator: "1", denominator: "2" },
    },
    ceilings: { input_tokens: "1000", output_tokens: "1000" },
    source_digest: "d2",
  };
  snap.prices.set(billingPrice.id, billingPrice);
  snap.prices.set(costPrice.id, costPrice);
  snap.models.set("gpt-4o", {
    name: "gpt-4o",
    operations: ["chat"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 1000,
    enabled: true,
  });
  const acctId = "44444444-4444-4444-8444-444444444499";
  snap.accounts.set(acctId, { id: acctId, name: "acct-400", state: "ACTIVE", revision: "1" });
  let upstreamMode: "client_400" | "server_502" | "broken_stream" = "client_400";
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      if (upstreamMode === "server_502") {
        return new Response("bad gateway", { status: 502 });
      }
      if (upstreamMode === "broken_stream") {
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(
              new TextEncoder().encode(
                'data: {"id":"cmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":"stop"}]}\n\n',
              ),
            );
            controller.enqueue(new TextEncoder().encode("data: [DONE]\n\n"));
            controller.close();
          },
        });
        return new Response(body, {
          status: 200,
          headers: { "content-type": "text/event-stream; charset=utf-8" },
        });
      }
      return Response.json({ error: { message: "bad input" } }, { status: 400 });
    },
  });
  const chId = "33333333-3333-4333-8333-333333333399";
  snap.channels.set(chId, {
    id: chId,
    account_id: acctId,
    name: "ch-400",
    kind: "openai",
    provider_plugin_id: pluginId,
    base_url: `http://127.0.0.1:${upstream.port}`,
    secret_ciphertext: "",
    adapter_config: {},
    rpm: 10_000,
    concurrency: 100,
    enabled: true,
    routing_policy: {},
    config_revision: 1,
  });
  snap.routesByModel.set("gpt-4o", [
    {
      id: "r-400",
      model_name: "gpt-4o",
      channel_id: chId,
      upstream_model: "gpt-4o",
      operations: ["chat"],
      billing_price_id: billingPrice.id,
      cost_version_id: costPrice.id,
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
  let releaseAttempts = 0;
  let unknownAttempts = 0;
  let ledgerMode: "recover" | "always_fail" = "recover";
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "req", held_atoms: "1000" };
    },
    async releaseHold() {
      releaseAttempts++;
      if (ledgerMode === "always_fail" || releaseAttempts < 2) {
        const err = new Error("lock timeout") as Error & { errno?: string };
        err.errno = "55P03";
        throw err;
      }
      return true;
    },
    async markUnknown() {
      unknownAttempts++;
      if (ledgerMode === "always_fail" || unknownAttempts < 2) {
        const err = new Error("lock timeout") as Error & { errno?: string };
        err.errno = "55P03";
        throw err;
      }
      return true;
    },
  } as unknown as ProxyDependencies["ledger"];

  const invoke = (stream = false) =>
    handleInferenceRequest(
      {
        req: {
          raw: new Request("http://127.0.0.1/v1/chat/completions", {
            method: "POST",
            headers: { authorization: `Bearer ${rawKey}`, "content-type": "application/json" },
            body: JSON.stringify({
              model: "gpt-4o",
              stream,
              messages: [{ role: "user", content: "hi" }],
            }),
          }),
          query: () => undefined,
        },
        json(data, status, headers) {
          return new Response(JSON.stringify(data), {
            status: status ?? 200,
            headers: { "content-type": "application/json", ...headers },
          });
        },
      },
      deps,
      "chat",
    );

  try {
    upstreamMode = "client_400";
    ledgerMode = "recover";
    const res1 = await invoke();
    expect(res1.status).toBe(400);
    expect(releaseAttempts).toBe(2);
    expect(unknownAttempts).toBe(0);

    ledgerMode = "always_fail";
    releaseAttempts = 0;
    const res2 = await invoke();
    expect(res2.status).toBe(400);
    expect(releaseAttempts).toBe(2);
    expect(unknownAttempts).toBe(0);

    upstreamMode = "server_502";
    ledgerMode = "always_fail";
    releaseAttempts = 0;
    unknownAttempts = 0;
    const res3 = await invoke();
    expect(res3.status).toBe(502);
    expect(releaseAttempts).toBe(2);
    expect(unknownAttempts).toBe(0);

    upstreamMode = "broken_stream";
    ledgerMode = "recover";
    releaseAttempts = 0;
    unknownAttempts = 0;
    const res4 = await invoke(true);
    expect(res4.status).toBe(200);
    const sseText = await res4.text();
    expect(sseText).toContain("invalid_upstream_usage");
    expect(unknownAttempts).toBe(2);
    expect(releaseAttempts).toBe(0);
  } finally {
    deps.state?.close();
    deps.transport.close();
    await upstream.stop(true);
  }
});

test("symptom: WASM adapt_response non-client logical errors expose unsanitized non-4xx/5xx HTTP status codes", async () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111188", 10);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const wasmBytes = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
  const wasmDigest = new Bun.CryptoHasher("sha256").update(wasmBytes).digest("hex");
  const pluginId = "22222222-2222-4222-8222-222222222288";
  const plugin = {
    id: pluginId,
    name: "custom-wasm-status",
    version: "1.0.0",
    kind: "WASM" as const,
    capability: "PROVIDER" as const,
    provider_config: {
      base_dialect: "openai_chat" as const,
      usage_dialect: "openai" as const,
      auth: { scheme: "bearer" as const },
      hooks: ["adapt_request" as const, "adapt_response" as const],
      grants: [],
    },
    digest: wasmDigest,
    wasm_bytes: wasmBytes,
    webhook_config: null,
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
  };
  snap.plugins.set(pluginId, plugin);
  snap.providerPluginsByName.set(plugin.name, plugin);
  const billingPrice = {
    id: "66666666-6666-4666-8666-666666666688",
    side: "CUSTOMER" as const,
    currency: "USD",
    basis: "PROVIDER_USAGE" as const,
    rates: {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "1", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", output_tokens: "1000" },
    source_digest: "d1",
  };
  const costPrice = {
    id: "77777777-7777-4777-8777-777777777788",
    side: "SUPPLIER" as const,
    currency: "USD",
    basis: "PROVIDER_USAGE" as const,
    rates: {
      input_tokens: { numerator: "1", denominator: "2" },
      output_tokens: { numerator: "1", denominator: "2" },
    },
    ceilings: { input_tokens: "1000", output_tokens: "1000" },
    source_digest: "d2",
  };
  snap.prices.set(billingPrice.id, billingPrice);
  snap.prices.set(costPrice.id, costPrice);
  snap.models.set("gpt-4o", {
    name: "gpt-4o",
    operations: ["chat"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 1000,
    enabled: true,
  });
  const acctId = "44444444-4444-4444-8444-444444444488";
  snap.accounts.set(acctId, { id: acctId, name: "acct-wasm", state: "ACTIVE", revision: "1" });
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: () => Response.json({ raw: "upstream-body" }, { status: 200 }),
  });
  const chId = "33333333-3333-4333-8333-333333333388";
  snap.channels.set(chId, {
    id: chId,
    account_id: acctId,
    name: "ch-wasm",
    kind: plugin.name,
    provider_plugin_id: pluginId,
    base_url: `http://127.0.0.1:${upstream.port}`,
    secret_ciphertext: "",
    adapter_config: {},
    rpm: 10_000,
    concurrency: 100,
    enabled: true,
    routing_policy: {},
    config_revision: 1,
  });
  snap.routesByModel.set("gpt-4o", [
    {
      id: "r-wasm",
      model_name: "gpt-4o",
      channel_id: chId,
      upstream_model: "gpt-4o",
      operations: ["chat"],
      billing_price_id: billingPrice.id,
      cost_version_id: costPrice.id,
      priority: 1,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 1 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: {},
    },
  ]);

  let nextWasmReply: Record<string, unknown> = {
    retry_kind: "retryable",
    status: 200,
    error_message: "logical failure",
  };
  const enc = new TextEncoder();
  const deps = makeFakeDeps(snap, makeConfig());
  deps.wasmExecutor = {
    runtimeKind: "mock",
    compile() {},
    async invoke(_id, hook) {
      if (hook === "adapt_request") {
        return enc.encode(JSON.stringify({ path: "/v1/custom", headers: {}, body: { ok: true } }));
      }
      return enc.encode(JSON.stringify(nextWasmReply));
    },
    close() {},
  };
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "req", held_atoms: "1000" };
    },
    async releaseHold() {
      return true;
    },
    async markUnknown() {
      return true;
    },
  } as unknown as ProxyDependencies["ledger"];

  const invoke = () =>
    handleInferenceRequest(
      {
        req: {
          raw: new Request("http://127.0.0.1/v1/chat/completions", {
            method: "POST",
            headers: { authorization: `Bearer ${rawKey}`, "content-type": "application/json" },
            body: JSON.stringify({ model: "gpt-4o", messages: [{ role: "user", content: "hi" }] }),
          }),
          query: () => undefined,
        },
        json(data, status, headers) {
          return new Response(JSON.stringify(data), {
            status: status ?? 200,
            headers: { "content-type": "application/json", ...headers },
          });
        },
      },
      deps,
      "chat",
    );

  try {
    nextWasmReply = { retry_kind: "retryable", status: 200, error_message: "logical failure" };
    const r200 = await invoke();
    expect(r200.status).toBe(502);
    expect(await r200.json()).toMatchObject({ error: { code: "upstream_error" } });

    nextWasmReply = { retry_kind: "rate_limit", status: 204, error_message: "throttled" };
    const r204 = await invoke();
    expect(r204.status).toBe(429);
    expect(await r204.json()).toMatchObject({ error: { code: "upstream_rate_limited" } });

    nextWasmReply = { retry_kind: "retryable", status: 503, error_message: "overloaded" };
    const r503 = await invoke();
    expect(r503.status).toBe(503);
  } finally {
    deps.state?.close();
    deps.transport.close();
    await upstream.stop(true);
  }
});
