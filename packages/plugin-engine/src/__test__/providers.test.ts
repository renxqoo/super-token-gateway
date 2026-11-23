import { expect, test } from "bun:test";
import { generateKeyPairSync, sign } from "node:crypto";
import { resolve } from "node:path";
import type { PluginRecord, WasmProviderConfig } from "@gateway/ledger";
import { parseProviderJson } from "@gateway/protocol";
import {
  applyBodyTransforms,
  applyResponseTransforms,
  assertSafeEndpointTemplate,
  assertSafeFieldPath,
  assertSafeRelativePath,
  createWasmtimeExecutor,
  createFfiWasmtimeExecutor,
  evaluateErrorRules,
  extractUsageAliases,
  getBuiltinPlugin,
  getBuiltinProvider,
  invokeWasmProviderRequest,
  invokeWasmProviderResponse,
  invokeWasmProviderStreamEvent,
  listBuiltinPlugins,
  listBuiltinProviders,
  mergeAdapterOverrides,
  renderEndpointUrl,
  resolveProviderPlugin,
  resolveWasmtimeLibraryPath,
  sanitizeProviderHeaders,
  WasmPluginFaultError,
  type WasmRuntimeExecutor,
} from "../index.ts";

const EXPECTED_PRESETS = [
  "openai",
  "openai_compatible",
  "anthropic",
  "gemini",
  "azure",
  "bedrock",
  "vertex",
  "deepseek",
  "ollama",
  "qwen",
  "volcengine",
  "zhipu",
  "moonshot",
  "minimax",
  "openrouter",
];

test("G7.2 all 15 built-in provider presets are exposed cleanly and match protocol/auth contracts", () => {
  const presets = listBuiltinProviders();
  expect(presets.map((p) => p.name)).toEqual(EXPECTED_PRESETS);

  const allBuiltins = listBuiltinPlugins();
  expect(allBuiltins.length).toBe(16);
  expect(listBuiltinPlugins("CONTENT").map((p) => p.name)).toEqual(["pii-guard"]);
  expect(listBuiltinPlugins("PROVIDER").map((p) => p.name)).toEqual(EXPECTED_PRESETS);

  for (const name of EXPECTED_PRESETS) {
    const p = getBuiltinProvider(name);
    expect(p).not.toBeNull();
    expect(p!.capability).toBe("PROVIDER");
    expect(p!.kind).toBe("DECLARATIVE");
    expect(getBuiltinPlugin(name)?.capability).toBe("PROVIDER");
  }

  expect(getBuiltinProvider("bedrock")?.provider_config.stream_framing).toBe("bedrock_eventstream");
  expect(getBuiltinProvider("bedrock")?.provider_config.auth).toEqual({
    scheme: "aws_sigv4",
    service: "bedrock",
  });
  expect(getBuiltinProvider("vertex")?.provider_config.auth).toEqual({
    scheme: "gcp_vertex_oauth",
  });
  expect(getBuiltinProvider("azure")?.provider_config.auth).toEqual({
    scheme: "azure_key",
    default_api_version: "2024-10-21",
  });
  expect(getBuiltinProvider("qwen")?.provider_config.endpoints.chat?.path).toBe(
    "/compatible-mode/v1/chat/completions",
  );
  expect(getBuiltinProvider("minimax")?.provider_config.endpoints.chat?.path).toBe(
    "/v1/text/chatcompletion_v2",
  );
});

test("Provider resolution follows strict 3-level priority: db_pinned > db_override > builtin", () => {
  const dbOverride: PluginRecord = {
    id: "00000000-0000-0000-0000-000000000001",
    name: "deepseek",
    version: "2.0.0",
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    digest: "a".repeat(64),
    wasm_bytes: null,
    webhook_config: null,
    provider_config: {
      base_dialect: "openai_chat",
      usage_dialect: "openai",
      auth: { scheme: "bearer" },
      endpoints: { default: { path: "/v2/deepseek/chat" } },
    },
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
  };
  const dbPinned: PluginRecord = {
    ...dbOverride,
    id: "00000000-0000-0000-0000-000000000002",
    version: "1.5.0",
    provider_config: {
      base_dialect: "openai_chat",
      usage_dialect: "openai",
      auth: { scheme: "bearer" },
      endpoints: { default: { path: "/v1.5/deepseek/chat" } },
    },
  };
  const snap = {
    plugins: new Map<string, PluginRecord>([
      [dbOverride.id, dbOverride],
      [dbPinned.id, dbPinned],
    ]),
    providerPluginsByName: new Map<string, PluginRecord>([["deepseek", dbOverride]]),
  };

  const r1 = resolveProviderPlugin("deepseek", dbPinned.id, snap);
  expect(r1?.source).toBe("db_pinned");
  expect(r1?.version).toBe("1.5.0");

  const r2 = resolveProviderPlugin("deepseek", null, snap);
  expect(r2?.source).toBe("db_override");
  expect(r2?.version).toBe("2.0.0");

  const r3 = resolveProviderPlugin("openai", null, snap);
  expect(r3?.source).toBe("builtin");
  expect(r3?.name).toBe("openai");

  const r4 = resolveProviderPlugin("deepseek", "00000000-0000-0000-0000-000000000099", snap);
  expect(r4).toBeNull();

  expect(resolveProviderPlugin("nonexistent_vendor", null, snap)).toBeNull();
});

test("Declarative transforms, endpoint rendering, error rules, usage aliases, and path/header security boundaries", () => {
  const original = {
    model: "m1",
    max_completion_tokens: 512,
    temperature: 1.8,
    extra: { keep: true, drop_me: "x" },
    flag: "off",
    stream_options: { include_usage: true },
  };
  const transformed = applyBodyTransforms(original, [
    { op: "rename", from: "max_completion_tokens", to: "max_tokens", overwrite: false },
    { op: "drop", path: "extra.drop_me" },
    { op: "clamp", path: "temperature", min: 0.01, max: 0.99 },
    { op: "set_default", path: "thinking.mode", value: "auto" },
    { op: "force_set", path: "provider_tag", value: "v2" },
    { op: "delete_if_equals", path: "flag", value: "off" },
    { op: "move_to", from: "stream_options", to: "opts.stream" },
  ]);
  expect(original.max_completion_tokens).toBe(512);
  expect(original.temperature).toBe(1.8);
  expect(original.extra.drop_me).toBe("x");
  expect(transformed).toEqual({
    model: "m1",
    max_tokens: 512,
    temperature: 0.99,
    extra: { keep: true },
    thinking: { mode: "auto" },
    provider_tag: "v2",
    opts: { stream: { include_usage: true } },
  });

  const respOut = applyResponseTransforms(
    { choices: [{ message: { content: "hi" } }], vendor_meta: "secret" },
    [
      { op: "drop", path: "vendor_meta" },
      { op: "set_default", path: "object", value: "chat.completion" },
    ],
  );
  expect(respOut).toEqual({
    choices: [{ message: { content: "hi" } }],
    object: "chat.completion",
  });

  const gemini = getBuiltinProvider("gemini")!;
  const geminiUrl = renderEndpointUrl({
    baseUrl: "https://generativelanguage.googleapis.com/",
    config: gemini.provider_config,
    operation: "gemini",
    upstreamModel: "gemini-2.5-pro",
    isStream: true,
    extraQuery: { trace: "1" },
  });
  expect(geminiUrl.toString()).toBe(
    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse&trace=1",
  );

  const minimax = getBuiltinProvider("minimax")!;
  const mmErr = evaluateErrorRules(
    200,
    { base_resp: { status_code: 1002, status_msg: "rate limit reached" } },
    minimax.provider_config.error_rules ?? [],
  );
  expect(mmErr).toEqual({ retry_kind: "rate_limit", message: "rate limit reached" });

  const catchAllRules = [
    ...(minimax.provider_config.error_rules ?? []),
    {
      when_http_status: [200],
      path: "base_resp.status_code",
      not_equals: 0,
      retry_kind: "client_error" as const,
      http_status: 400,
      message_path: "base_resp.status_msg",
    },
  ];
  expect(
    evaluateErrorRules(
      200,
      { base_resp: { status_code: 0, status_msg: "success" } },
      catchAllRules,
    ),
  ).toBeNull();
  expect(
    evaluateErrorRules(
      200,
      { base_resp: { status_code: 2049, status_msg: "invalid api key" } },
      catchAllRules,
    ),
  ).toEqual({
    retry_kind: "client_error",
    http_status: 400,
    message: "invalid api key",
  });

  const zhipu = getBuiltinProvider("zhipu")!;
  const zpErr = evaluateErrorRules(
    200,
    { error: { code: "1302", message: "concurrency exceeded" } },
    zhipu.provider_config.error_rules ?? [],
  );
  expect(zpErr).toEqual({ retry_kind: "rate_limit", message: "concurrency exceeded" });

  const aliases = extractUsageAliases(
    { usage: { prompt_cache_hit_tokens: 42 }, custom: { out: 9 } },
    [
      { target: "cached_input_tokens", source_path: "usage.prompt_cache_hit_tokens" },
      { target: "output_tokens", source_path: "custom.out" },
    ],
  );
  expect(aliases).toEqual({ cached_input_tokens: "42", output_tokens: "9" });

  const merged = mergeAdapterOverrides(
    getBuiltinProvider("deepseek")!.provider_config,
    { extra_headers: { "X-Channel": "ch1" } },
    {
      extra_headers: { "X-Route": "r1" },
      request_transforms: [{ op: "drop", path: "temperature" }],
    },
  );
  expect(merged.extra_headers).toEqual({ "X-Channel": "ch1", "X-Route": "r1" });
  expect(merged.request_transforms.length).toBe(3);

  expect(() => assertSafeFieldPath("__proto__.polluted")).toThrow();
  expect(() => assertSafeFieldPath("a.constructor.b")).toThrow();
  expect(() => assertSafeFieldPath("a.prototype")).toThrow();
  expect(() => assertSafeFieldPath("a[0].b")).toThrow();
  expect(() => assertSafeRelativePath("https://evil.com/x")).toThrow();
  expect(() => assertSafeRelativePath("/v1/../secret")).toThrow();
  expect(() => assertSafeRelativePath("//evil.com/x")).toThrow();
  expect(() => assertSafeEndpointTemplate("/v1/${secret}")).toThrow();

  const cleanHeaders = sanitizeProviderHeaders({
    "X-Custom": "ok",
    Host: "evil.com",
    Authorization: "Bearer stolen",
    "x-api-key": "stolen",
    "api-key": "stolen",
    "x-goog-api-key": "stolen",
    "Content-Length": "999",
    "Bad-CRLF": "a\r\nb",
  });
  expect(cleanHeaders).toEqual({ "X-Custom": "ok" });
});

async function expectRejects(fn: () => Promise<unknown>, pattern?: RegExp): Promise<void> {
  try {
    await fn();
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (pattern) expect(msg).toMatch(pattern);
    return;
  }
  throw new Error("Expected promise to reject");
}

async function runWasmProviderSuite(executor: WasmRuntimeExecutor) {
  const fixturePath = resolve(import.meta.dir, "../../../../tests/fixtures/example-provider.wasm");
  const wasmBytes = new Uint8Array(await Bun.file(fixturePath).arrayBuffer());
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const pubPem = publicKey.export({ type: "spki", format: "pem" }).toString();
  const digest = new Bun.CryptoHasher("sha256").update(wasmBytes).digest("hex");
  const signature = sign(null, Buffer.from(digest, "utf8"), privateKey).toString("base64");

  const config: WasmProviderConfig = {
    base_dialect: "passthrough",
    usage_dialect: "custom",
    auth: { scheme: "bearer" },
    hooks: ["adapt_request", "adapt_response", "adapt_stream_event"],
    grants: ["extract_usage"],
    fuel_budget: 2_000_000,
    max_memory_bytes: 8 * 1024 * 1024,
  };
  const plugin: PluginRecord = {
    id: "wasm-prov-1",
    name: "acme-proprietary",
    version: "1.0.0",
    capability: "PROVIDER",
    kind: "WASM",
    digest,
    wasm_bytes: wasmBytes,
    webhook_config: null,
    provider_config: config,
    signature,
    signing_key_id: "k1",
    manifest: {},
    enabled: true,
  };

  const reqOut = await invokeWasmProviderRequest({
    executor,
    plugin,
    config,
    signingPublicKeyPem: pubPem,
    input: {
      operation: "chat",
      upstream_model: "acme-9b",
      stream: false,
      client_protocol: "chat",
      body: { messages: [{ role: "user", content: "hello acme" }] },
      headers: { "X-Client": "unit" },
    },
  });
  expect(reqOut.path).toBe("/v9/proprietary/generate");
  expect(reqOut.query).toEqual({ fmt: "acme_v9" });
  expect(reqOut.headers).toEqual({ "X-Acme-Proto": "v9" });
  expect(reqOut.body).toEqual({
    target_engine: "acme-9b",
    prompt_text: "hello acme",
    streaming: false,
    secret_seen_in_wasm: false,
  });

  await expectRejects(
    () =>
      invokeWasmProviderRequest({
        executor,
        plugin,
        config,
        signingPublicKeyPem: pubPem,
        input: {
          operation: "chat",
          upstream_model: "acme-9b",
          stream: false,
          client_protocol: "chat",
          body: { messages: [{ role: "user", content: "TRIGGER_HOST_ESCAPE" }] },
          headers: {},
        },
      }),
    /Unsafe provider relative path/,
  );

  await expectRejects(() =>
    invokeWasmProviderRequest({
      executor,
      plugin,
      config: { ...config, fuel_budget: 50_000 },
      signingPublicKeyPem: pubPem,
      input: {
        operation: "chat",
        upstream_model: "acme-9b",
        stream: false,
        client_protocol: "chat",
        body: { messages: [{ role: "user", content: "TRIGGER_INFINITE_LOOP" }] },
        headers: {},
      },
    }),
  );

  const respOut = await invokeWasmProviderResponse({
    executor,
    plugin,
    config,
    signingPublicKeyPem: pubPem,
    input: {
      operation: "chat",
      upstream_model: "acme-9b",
      client_protocol: "chat",
      status: 200,
      headers: {},
      body: { answer: "acme reply", tok_in: 14, tok_out: 6, tok_cached: 4 },
    },
  });
  expect(respOut.usage).toEqual({ input_tokens: 14, output_tokens: 6, cached_input_tokens: 4 });
  expect(
    (respOut.body as { choices: { message: { content: string } }[] }).choices[0]!.message.content,
  ).toBe("acme reply");

  const errResp = await invokeWasmProviderResponse({
    executor,
    plugin,
    config,
    signingPublicKeyPem: pubPem,
    input: {
      operation: "chat",
      upstream_model: "acme-9b",
      client_protocol: "chat",
      status: 200,
      headers: {},
      body: { err_code: 9002 },
    },
  });
  expect(errResp.retry_kind).toBe("rate_limit");
  expect(errResp.retry_after_ms).toBe(1000);

  await expectRejects(
    () =>
      invokeWasmProviderResponse({
        executor,
        plugin,
        config: { ...config, grants: [] },
        signingPublicKeyPem: pubPem,
        input: {
          operation: "chat",
          upstream_model: "acme-9b",
          client_protocol: "chat",
          status: 200,
          headers: {},
          body: { answer: "no grant", tok_in: 5, tok_out: 2 },
        },
      }),
    /extract_usage/,
  );

  const s1 = await invokeWasmProviderStreamEvent({
    executor,
    plugin,
    config,
    signingPublicKeyPem: pubPem,
    input: {
      operation: "chat",
      upstream_model: "acme-9b",
      client_protocol: "chat",
      event: "message",
      data: JSON.stringify({ piece: "Hello " }),
      state: "",
    },
  });
  expect(s1.state).toBe("1");
  expect(s1.done).toBe(false);
  expect(s1.chunks?.length).toBe(1);

  const s2 = await invokeWasmProviderStreamEvent({
    executor,
    plugin,
    config,
    signingPublicKeyPem: pubPem,
    input: {
      operation: "chat",
      upstream_model: "acme-9b",
      client_protocol: "chat",
      event: "message",
      data: JSON.stringify({ finish: true, tok_in: 12, tok_out: 7 }),
      state: s1.state ?? "",
    },
  });
  expect(s2.done).toBe(true);
  expect(s2.usage).toEqual({ input_tokens: 12, output_tokens: 7 });
  expect((s2.chunks?.[0] as { x_frames?: number })?.x_frames).toBe(1);

  await expectRejects(
    () =>
      invokeWasmProviderStreamEvent({
        executor,
        plugin,
        config,
        signingPublicKeyPem: pubPem,
        input: {
          operation: "chat",
          upstream_model: "acme-9b",
          client_protocol: "chat",
          event: "message",
          data: "HUGE_STATE",
          state: "",
        },
      }),
    /4096 bytes/,
  );
}

test("G7.6 Real compiled Rust WASM proprietary provider passes full lifecycle and security matrix on native Rust wasmtime worker pool and FFI", async () => {
  const workerExec = createWasmtimeExecutor({ poolSize: 2, minTimeoutMs: 40, maxTimeoutMs: 500 });
  try {
    await workerExec.selfCheck?.();
    await runWasmProviderSuite(workerExec);
  } finally {
    workerExec.close();
  }

  const libPath = resolveWasmtimeLibraryPath()!;
  const ffiExec = createFfiWasmtimeExecutor({ libraryPath: libPath });
  try {
    await runWasmProviderSuite(ffiExec);
  } finally {
    ffiExec.close();
  }
});

test("a failed WASM revision cannot leave its previous revision unavailable in a worker slot", async () => {
  const previous = new Uint8Array(
    await Bun.file(
      resolve(import.meta.dir, "../../../../tests/fixtures/example-provider.wasm"),
    ).arrayBuffer(),
  );
  const different = getBuiltinPlugin("pii-guard");
  if (different?.capability !== "CONTENT") throw new Error("Missing content WASM fixture");
  const previousDigest = new Bun.CryptoHasher("sha256").update(previous).digest("hex");
  const executor = createWasmtimeExecutor({
    poolSize: 1,
    minTimeoutMs: 2_000,
  });
  const input = new TextEncoder().encode(
    JSON.stringify({ operation: "chat", upstream_model: "fixture", body: {} }),
  );
  const invokeRequest = async () => {
    const result = await executor.invoke("fixture", "adapt_request", input, {
      fuelBudget: 5_000_000,
      maxMemoryBytes: 16 * 1024 * 1024,
    });
    return JSON.parse(new TextDecoder().decode(result));
  };
  try {
    executor.compile("fixture", previousDigest, previous);
    expect((await invokeRequest()).path).toBe("/v9/proprietary/generate");
    executor.compile("fixture", different.digest, different.wasm_bytes);
    const failure = await invokeRequest().catch((err: Error) => err.message);
    expect(failure).toContain("missing exported hook");
    executor.compile("fixture", previousDigest, previous);
    expect((await invokeRequest()).path).toBe("/v9/proprietary/generate");
  } finally {
    executor.close();
  }
});

test("two different values for the same provider usage alias cannot silently replace one another", () => {
  expect(() =>
    extractUsageAliases({ vendor: { charged: 10, reported: 12 } }, [
      { target: "input_tokens", source_path: "vendor.charged" },
      { target: "input_tokens", source_path: "vendor.reported" },
    ]),
  ).toThrow("Provider usage alias conflict on input_tokens");
});

test("renaming an upstream usage field preserves its original decimal precision", () => {
  const raw = parseProviderJson('{"vendor":{"duration":0.123456789012345678}}') as Record<
    string,
    unknown
  >;
  const adapted = applyResponseTransforms(raw, [
    { op: "rename", from: "vendor.duration", to: "usage.duration" },
  ]);
  expect(
    extractUsageAliases(adapted, [{ target: "audio_seconds", source_path: "usage.duration" }]),
  ).toEqual({ audio_seconds: "0.123456789012345678" });
});

test("malformed JSON returned by WASM provider adapt_response throws WasmPluginFaultError instead of blaming upstream", async () => {
  const bytes = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
  const digest = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
  const plugin: PluginRecord = {
    id: "p-bad-json-provider",
    name: "bad-json-provider",
    version: "1.0.0",
    capability: "PROVIDER",
    kind: "WASM",
    digest,
    wasm_bytes: bytes,
    webhook_config: null,
    provider_config: {
      base_dialect: "passthrough",
      usage_dialect: "custom",
      auth: { scheme: "bearer" },
      hooks: ["adapt_response"],
      grants: ["extract_usage"],
    },
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
  };
  const badJsonExec: WasmRuntimeExecutor = {
    runtimeKind: "mock",
    compile() {},
    async invoke() {
      return new TextEncoder().encode("{not-valid-json");
    },
    close() {},
  };
  let caught: unknown = null;
  try {
    await invokeWasmProviderResponse({
      executor: badJsonExec,
      plugin,
      config: plugin.provider_config as WasmProviderConfig,
      input: {
        operation: "chat",
        upstream_model: "m",
        client_protocol: "chat",
        status: 200,
        headers: {},
        body: {},
      },
    });
  } catch (err) {
    caught = err;
  }
  expect(caught).toBeInstanceOf(WasmPluginFaultError);
  expect((caught as WasmPluginFaultError).code).toBe("wasm_execution_failed");
});
