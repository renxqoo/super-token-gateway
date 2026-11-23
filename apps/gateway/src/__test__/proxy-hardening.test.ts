import { expect, test } from "bun:test";
import { createConnection } from "node:net";
import type {
  ConfigSnapshot,
  ApiKeyRecord,
  ChannelRecord,
  PluginRecord,
  ProjectRecord,
  RouteRecord,
} from "@gateway/ledger";
import {
  ClusterCoordinator,
  createTransport,
  Lifecycle,
  SmartRouter,
  verifier,
  VertexTokens,
} from "@gateway/runtime";
import { getBuiltinProvider, PluginPipelineEngine } from "@gateway/plugin-engine";
import type { GatewayConfig } from "../config.ts";
import { GatewayMetrics } from "../observability.ts";
import { authenticateRequest, handleInferenceRequest, type ProxyDependencies } from "../proxy.ts";
import { ProxyRuntimeState } from "../proxy-state.ts";
import type { SnapshotManager } from "../snapshot.ts";
import { tryPrepareUpstreamRequestSync } from "../upstream.ts";

const TEST_PEPPER = "test-pepper-0123456789abcdef";

function makeConfig(overrides: Partial<GatewayConfig> = {}): GatewayConfig {
  return {
    mode: "gateway",
    host: "127.0.0.1",
    port: 0,
    internalHost: "127.0.0.1",
    internalPort: 0,
    databaseUrl: "postgres://u:p@127.0.0.1:5432/db",
    dbMaxConnections: 4,
    keyPepper: TEST_PEPPER,
    encryptionKey: "ab".repeat(32),
    adminToken: "admin-token-0123456789abcdef",
    allowPrivateUpstreams: true,
    privateUpstreamCidrs: [],
    trustedProxyCidrs: [],
    maxInFlight: 8,
    maxBodyBytes: 1024 * 1024,
    holdTtlMs: 60_000,
    attemptTimeoutMs: 5_000,
    snapshotPollMs: 0,
    microBatchWindowMs: 2,
    microBatchMaxSize: 64,
    ...overrides,
  };
}

function makeProject(id: string, concurrency: number): ProjectRecord {
  return {
    id,
    name: id,
    external_ref: null,
    currency: "USD",
    enabled: true,
    rpm: 10_000,
    tpm: 10_000_000,
    concurrency,
  };
}

function makeApiKey(rawKey: string, projectId: string, pepper = TEST_PEPPER): ApiKeyRecord {
  return {
    id: `key-${rawKey}`,
    project_id: projectId,
    name: rawKey,
    prefix: rawKey.slice(0, 8),
    verifier: verifier(rawKey, pepper),
    role: "INVOKER",
    models: [],
    operations: [],
    allowed_cidrs: [],
    enabled: true,
    expires_at: null,
    spend_limit_atoms: null,
    spend_limit_window: "LIFETIME",
  };
}

function makeSnapshot(
  epoch: number,
  projects: ProjectRecord[],
  keys: ApiKeyRecord[],
): ConfigSnapshot {
  return {
    epoch,
    lineageToken: "11111111-1111-4111-8111-111111111111",
    projects: new Map(projects.map((p) => [p.id, p])),
    keysByVerifier: new Map(keys.map((k) => [k.verifier, k])),
    accounts: new Map(),
    channels: new Map(),
    models: new Map(),
    prices: new Map(),
    pricingOverridesByProject: new Map(),
    routesByModel: new Map(),
    plugins: new Map(),
    revokedPlugins: new Set(),
    providerPluginsByName: new Map(),
  };
}

function makeFakeDeps(snap: ConfigSnapshot, config: GatewayConfig): ProxyDependencies {
  const noopSignal = new AbortController().signal;
  const snapshot = {
    isDiverged: () => false,
    isFresh: () => true,
    get: () => snap,
    isNoopPluginSignal: (s: AbortSignal) => s === noopSignal,
    pluginSignal: () => noopSignal,
    decryptChannelSecret: () => "",
  } as unknown as SnapshotManager;
  const transport = createTransport(16);
  const cluster = new ClusterCoordinator();
  const router = new SmartRouter();
  const lifecycle = new Lifecycle(config.maxInFlight);
  const metrics = new GatewayMetrics(false);
  const pluginEngine = new PluginPipelineEngine({ wasmExecutor: null });
  const vertexTokens = new VertexTokens();
  const ledger = {} as ProxyDependencies["ledger"];
  const state = new ProxyRuntimeState();
  return {
    config,
    snapshot,
    ledger,
    router,
    cluster,
    transport,
    vertexTokens,
    pluginEngine,
    wasmExecutor: null,
    lifecycle,
    metrics,
    state,
  };
}

function sendSlowChunkedUpload(
  port: number,
  apiKey: string,
  initialChunk: string,
): { response: Promise<string>; close: () => void } {
  const socket = createConnection({ host: "127.0.0.1", port });
  const chunks: Buffer[] = [];
  const response = new Promise<string>((resolve) => {
    socket.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    socket.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    socket.on("close", () => resolve(Buffer.concat(chunks).toString("utf8")));
    socket.on("error", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
  socket.on("connect", () => {
    const hexLen = Buffer.byteLength(initialChunk, "utf8").toString(16);
    socket.write(
      `POST /v1/chat/completions HTTP/1.1\r\n` +
        `Host: 127.0.0.1:${port}\r\n` +
        `Authorization: Bearer ${apiKey}\r\n` +
        `Content-Type: application/json\r\n` +
        `Transfer-Encoding: chunked\r\n` +
        `Connection: close\r\n\r\n` +
        `${hexLen}\r\n${initialChunk}\r\n`,
    );
  });
  return {
    response,
    close: () => socket.destroy(),
  };
}

function sendSlowJsonUpload(
  port: number,
  apiKey: string,
  contentLength: number,
  initialBody: string,
): { response: Promise<string>; close: () => void } {
  const socket = createConnection({ host: "127.0.0.1", port });
  const chunks: Buffer[] = [];
  const response = new Promise<string>((resolve) => {
    socket.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    socket.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    socket.on("close", () => resolve(Buffer.concat(chunks).toString("utf8")));
    socket.on("error", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
  socket.on("connect", () => {
    socket.write(
      `POST /v1/chat/completions HTTP/1.1\r\n` +
        `Host: 127.0.0.1:${port}\r\n` +
        `Authorization: Bearer ${apiKey}\r\n` +
        `Content-Type: application/json\r\n` +
        `Content-Length: ${contentLength}\r\n` +
        `Connection: close\r\n\r\n` +
        initialBody,
    );
  });
  return {
    response,
    close: () => socket.destroy(),
  };
}

test("F1 symptom: slow JSON upload times out with 408 and frees admission slot instead of hanging indefinitely", async () => {
  const proj = makeProject("11111111-1111-4111-8111-111111111111", 4);
  const rawKey = "sk-slow-upload-key-1";
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const config = makeConfig({
    maxInFlight: 4,
    attemptTimeoutMs: 500,
    ...({ bodyReadTimeoutMs: 120 } as Partial<GatewayConfig>),
  });
  const deps = makeFakeDeps(snap, config);
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(req) {
      return handleInferenceRequest(
        {
          req: { raw: req, query: () => undefined },
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
    },
  });
  const conn = sendSlowJsonUpload(server.port!, rawKey, 200, '{"model":"gpt"');
  try {
    const raw = await Promise.race([
      conn.response,
      Bun.sleep(1000).then(() => "TIMED_OUT_WAITING_FOR_RESPONSE"),
    ]);
    expect(raw).toContain("408");
    expect(raw).toContain("request_timeout");
    await Bun.sleep(30);
    expect(deps.lifecycle.size).toBe(0);
  } finally {
    conn.close();
    deps.state?.close();
    deps.transport.close();
    await server.stop(true);
  }
});

test("F1 symptom: slow chunked JSON upload times out with 408 and frees admission slot", async () => {
  const proj = makeProject("11111111-1111-4111-8111-111111111111", 4);
  const rawKey = "sk-slow-chunked-key-1";
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const config = makeConfig({
    maxInFlight: 4,
    attemptTimeoutMs: 500,
    bodyReadTimeoutMs: 120,
  });
  const deps = makeFakeDeps(snap, config);
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(req) {
      return handleInferenceRequest(
        {
          req: { raw: req, query: () => undefined },
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
    },
  });
  const conn = sendSlowChunkedUpload(server.port!, rawKey, '{"model":"gpt"');
  try {
    const raw = await Promise.race([
      conn.response,
      Bun.sleep(1000).then(() => "TIMED_OUT_WAITING_FOR_RESPONSE"),
    ]);
    expect(raw).toContain("408");
    expect(raw).toContain("request_timeout");
    await Bun.sleep(30);
    expect(deps.lifecycle.size).toBe(0);
  } finally {
    conn.close();
    deps.state?.close();
    deps.transport.close();
    await server.stop(true);
  }
});

test("F1 symptom: single tenant slow uploads are capped at project concurrency before body read and cannot starve other tenants", async () => {
  const projA = makeProject("11111111-1111-4111-8111-111111111111", 2);
  const projB = makeProject("22222222-2222-4222-8222-222222222222", 2);
  const keyA = "sk-tenant-a-slow";
  const keyB = "sk-tenant-b-healthy";
  const snap = makeSnapshot(
    1,
    [projA, projB],
    [makeApiKey(keyA, projA.id), makeApiKey(keyB, projB.id)],
  );
  const config = makeConfig({
    maxInFlight: 4,
    attemptTimeoutMs: 2_000,
    ...({ bodyReadTimeoutMs: 1_500 } as Partial<GatewayConfig>),
  });
  const deps = makeFakeDeps(snap, config);
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(req) {
      return handleInferenceRequest(
        {
          req: { raw: req, query: () => undefined },
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
    },
  });
  const slow1 = sendSlowJsonUpload(server.port!, keyA, 200, "{");
  const slow2 = sendSlowJsonUpload(server.port!, keyA, 200, "{");
  try {
    const deadline = Date.now() + 1000;
    while (deps.lifecycle.size < 2 && Date.now() < deadline) await Bun.sleep(5);
    expect(deps.lifecycle.size).toBe(2);

    const thirdA = sendSlowJsonUpload(server.port!, keyA, 200, "{");
    const thirdARaw = await Promise.race([
      thirdA.response,
      Bun.sleep(600).then(() => "THIRD_A_BLOCKED_ON_BODY_READ"),
    ]);
    thirdA.close();
    expect(thirdARaw).toContain("429");
    expect(thirdARaw).toContain("rate_limit_concurrency");

    const resB = await fetch(`http://127.0.0.1:${server.port}/v1/chat/completions`, {
      method: "POST",
      headers: { authorization: `Bearer ${keyB}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "missing-model" }),
    });
    expect(resB.status).toBe(404);
  } finally {
    slow1.close();
    slow2.close();
    deps.state?.close();
    deps.transport.close();
    await server.stop(true);
  }
});

test("F8 symptom: distinct runtime instances isolate auth caches instead of sharing module globals", () => {
  const projA = makeProject("11111111-1111-4111-8111-111111111111", 10);
  const projB = makeProject("22222222-2222-4222-8222-222222222222", 10);
  const rawKey = "sk-shared-key-token";
  const snapA = makeSnapshot(1, [projA], [makeApiKey(rawKey, projA.id)]);
  const snapB = makeSnapshot(1, [projB], [makeApiKey(rawKey, projB.id)]);
  const config = makeConfig();
  const makeCtx = () => ({
    req: {
      raw: new Request("http://127.0.0.1/v1/chat/completions", {
        headers: { authorization: `Bearer ${rawKey}` },
      }),
      query: () => undefined,
    },
    json: (d: unknown, s?: number) => Response.json(d, { status: s ?? 200 }),
  });
  const stateA = new ProxyRuntimeState(2);
  const stateB = new ProxyRuntimeState(2);
  try {
    const resA = authenticateRequest(makeCtx(), snapA, config, "chat", undefined, stateA);
    expect(resA.project.id).toBe(projA.id);
    const resB = authenticateRequest(makeCtx(), snapB, config, "chat", undefined, stateB);
    expect(resB.project.id).toBe(projB.id);
    const resNoState = authenticateRequest(makeCtx(), snapB, config, "chat");
    expect(resNoState.project.id).toBe(projB.id);
  } finally {
    stateA.close();
    stateB.close();
  }
});

test("F8: ProxyRuntimeState LRU evicts least-recently-used key instead of wiping entire cache", () => {
  const proj = makeProject("11111111-1111-4111-8111-111111111111", 10);
  const k0 = makeApiKey("sk-lru-0", proj.id);
  const k1 = makeApiKey("sk-lru-1", proj.id);
  const k2 = makeApiKey("sk-lru-2", proj.id);
  const snap = makeSnapshot(1, [proj], [k0, k1, k2]);
  const state = new ProxyRuntimeState(2);
  try {
    expect(state.lookupVerifiedKey("sk-lru-0", snap, TEST_PEPPER)?.id).toBe(k0.id);
    expect(state.lookupVerifiedKey("sk-lru-1", snap, TEST_PEPPER)?.id).toBe(k1.id);
    expect(state.lookupVerifiedKey("sk-lru-0", snap, TEST_PEPPER)?.id).toBe(k0.id);
    expect(state.lookupVerifiedKey("sk-lru-2", snap, TEST_PEPPER)?.id).toBe(k2.id);
    snap.keysByVerifier.clear();
    expect(state.lookupVerifiedKey("sk-lru-0", snap, TEST_PEPPER)?.id).toBe(k0.id);
    expect(state.lookupVerifiedKey("sk-lru-2", snap, TEST_PEPPER)?.id).toBe(k2.id);
    expect(state.lookupVerifiedKey("sk-lru-1", snap, TEST_PEPPER)).toBeUndefined();
  } finally {
    state.close();
  }
});

test("symptom: tryPrepareUpstreamRequestSync never marks unsafe bearer secrets headersValidated and preserves content-type", () => {
  const proj = makeProject("11111111-1111-4111-8111-111111111111", 10);
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
  const proj = makeProject("11111111-1111-4111-8111-111111111111", 10);
  const rawKey = "sk-failover-budget-key";
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
