import { expect, test } from "bun:test";
import type { ChannelRecord } from "@gateway/ledger";
import { createKey, SmartRouter } from "@gateway/runtime";
import { getBuiltinProvider } from "@gateway/plugin-engine";
import { formatProtocolError } from "@gateway/protocol";
import type { ProxyDependencies } from "../inference/types.ts";
import { handleInferenceRequest } from "../proxy.ts";
import { parseRetryAfterMs } from "../upstream.ts";
import {
  makeApiKey,
  makeBillingAccount,
  makeConfig,
  makeFakeDeps,
  makeSnapshot,
} from "./proxy-fixture.ts";

test("client disconnects during unary and SSE requests never trip channel circuit breaker", async () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111141", 10);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const preset = getBuiltinProvider("openai")!;
  const pluginId = "88888888-8888-4888-8888-888888888841";
  snap.plugins.set(pluginId, {
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
  });
  const custPriceId = "66666666-6666-4666-8666-666666666641";
  const supPriceId = "77777777-7777-4777-8777-777777777741";
  for (const [id, side] of [
    [custPriceId, "CUSTOMER"],
    [supPriceId, "SUPPLIER"],
  ] as const) {
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
      source_digest: "d1",
    });
  }
  const acctId = "44444444-4444-4444-8444-444444444441";
  const primaryId = "33333333-3333-4333-8333-333333333341";
  const backupId = "33333333-3333-4333-8333-333333333342";
  snap.accounts.set(acctId, { id: acctId, name: "acct-cancel", state: "ACTIVE", revision: "1" });
  snap.models.set("gpt-cancel", {
    name: "gpt-cancel",
    operations: ["chat"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 1000,
    enabled: true,
  });

  let hangPrimary = true;
  let primaryHits = 0;
  let backupHits = 0;
  let arrived = Promise.withResolvers<void>();
  const pendingReleases: Array<() => void> = [];
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      if (path.startsWith("/backup/")) {
        backupHits++;
        return Response.json({
          id: "bk-1",
          object: "chat.completion",
          model: "gpt-cancel",
          choices: [
            { index: 0, message: { role: "assistant", content: "backup" }, finish_reason: "stop" },
          ],
          usage: { prompt_tokens: 2, completion_tokens: 2, total_tokens: 4 },
        });
      }
      primaryHits++;
      const body = (await req.json()) as { stream?: boolean };
      if (body.stream && hangPrimary) {
        let cancelStream!: () => void;
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(
              new TextEncoder().encode(
                'data: {"id":"c1","object":"chat.completion.chunk","model":"gpt-cancel","choices":[{"index":0,"delta":{"content":"hi"}}]}\n\n',
              ),
            );
            arrived.resolve();
            cancelStream = () => {
              try {
                controller.close();
              } catch {
                void 0;
              }
            };
            pendingReleases.push(cancelStream);
          },
        });
        return new Response(stream, { headers: { "content-type": "text/event-stream" } });
      }
      if (hangPrimary) {
        arrived.resolve();
        await new Promise<void>((resolve) => {
          pendingReleases.push(resolve);
          req.signal.addEventListener("abort", () => resolve(), { once: true });
        });
      }
      return Response.json({
        id: "ok-primary",
        object: "chat.completion",
        model: "gpt-cancel",
        choices: [
          {
            index: 0,
            message: { role: "assistant", content: "primary-ok" },
            finish_reason: "stop",
          },
        ],
        usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 },
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
  snap.channels.set(primaryId, makeCh(primaryId, "ch-primary", "primary"));
  snap.channels.set(backupId, makeCh(backupId, "ch-backup", "backup"));
  snap.routesByModel.set("gpt-cancel", [
    {
      id: "r-primary",
      model_name: "gpt-cancel",
      channel_id: primaryId,
      upstream_model: "gpt-cancel",
      operations: ["chat"],
      billing_price_id: custPriceId,
      cost_version_id: supPriceId,
      priority: 30,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 2 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: {},
    },
    {
      id: "r-backup",
      model_name: "gpt-cancel",
      channel_id: backupId,
      upstream_model: "gpt-cancel",
      operations: ["chat"],
      billing_price_id: custPriceId,
      cost_version_id: supPriceId,
      priority: 10,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 2 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: {},
    },
  ]);

  const deps = makeFakeDeps(
    snap,
    makeConfig({
      unaryClientAbortGraceMs: 0,
      attemptTimeoutMs: 5_000,
      streamEstimatedSettlement: false,
    }),
  );
  deps.router = new SmartRouter({ failureThreshold: 3 });
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "req", held_atoms: "1000" };
    },
    async switchSupplierAttempt() {
      return { ok: true, held_atoms: "1000" };
    },
    async commitTerminal() {
      return {
        ok: true,
        replayed: false,
        charge_id: "chg",
        customer_atoms: "5",
        supplier_cost_atoms: "5",
      };
    },
    async releaseHold() {},
    async markUnknown() {},
  } as unknown as ProxyDependencies["ledger"];

  const invoke = (stream: boolean, signal: AbortSignal) =>
    handleInferenceRequest(
      {
        req: {
          raw: new Request("http://127.0.0.1/v1/chat/completions", {
            method: "POST",
            signal,
            headers: { authorization: `Bearer ${rawKey}`, "content-type": "application/json" },
            body: JSON.stringify({
              model: "gpt-cancel",
              stream,
              messages: [{ role: "user", content: "hi" }],
            }),
          }),
          query: () => undefined,
        },
        json: (data, status, headers) =>
          new Response(JSON.stringify(data), {
            status: status ?? 200,
            headers: { "content-type": "application/json", ...headers },
          }),
      },
      deps,
      "chat",
    );

  try {
    for (let i = 0; i < 2; i++) {
      arrived = Promise.withResolvers<void>();
      const ac = new AbortController();
      const pending = invoke(false, ac.signal);
      await arrived.promise;
      ac.abort(new Error("client cancelled unary"));
      const resp = await pending;
      expect(resp.status).toBe(499);
    }

    for (let i = 0; i < 2; i++) {
      arrived = Promise.withResolvers<void>();
      const ac = new AbortController();
      const resp = await invoke(true, ac.signal);
      expect(resp.status).toBe(200);
      const reader = resp.body!.getReader();
      const first = await reader.read();
      expect(new TextDecoder().decode(first.value)).toContain('"content":"hi"');
      ac.abort(new Error("client cancelled stream"));
      await reader.cancel();
      reader.releaseLock();
    }

    const healthBefore = deps.router.snapshot().find((entry) => entry.channel_id === primaryId);
    expect(healthBefore?.state ?? "CLOSED").toBe("CLOSED");
    expect(healthBefore?.consecutive_failures ?? 0).toBe(0);

    hangPrimary = false;
    const healthy = await invoke(false, new AbortController().signal);
    expect(healthy.status).toBe(200);
    expect(healthy.headers.get("x-gateway-channel")).toBe("ch-primary");
    expect(primaryHits).toBe(5);
    expect(backupHits).toBe(0);
  } finally {
    for (const release of pendingReleases) release();
    deps.state?.close();
    deps.transport.close();
    await upstream.stop(true);
  }
});

test("Anthropic 529 overloaded fails over with retryable accounting and parseRetryAfterMs parses numeric and HTTP-date boundaries", async () => {
  expect(parseRetryAfterMs("12")).toBe(12_000);
  expect(parseRetryAfterMs("0.5")).toBe(500);
  expect(parseRetryAfterMs("0")).toBe(0);
  const futureDate = new Date(Date.now() + 10_000).toUTCString();
  const parsedFuture = parseRetryAfterMs(futureDate);
  expect(parsedFuture).toBeGreaterThan(0);
  expect(parsedFuture).toBeLessThanOrEqual(11_000);
  expect(parseRetryAfterMs("Wed, 21 Oct 2015 07:28:00 GMT")).toBe(0);
  expect(parseRetryAfterMs("not-a-date")).toBeUndefined();
  expect(parseRetryAfterMs("")).toBeUndefined();
  expect(parseRetryAfterMs(null)).toBeUndefined();
  expect(formatProtocolError("messages", 529, "overloaded", "Overloaded", "req-529")).toEqual({
    type: "error",
    error: {
      type: "overloaded_error",
      code: "overloaded",
      message: "Overloaded",
      request_id: "req-529",
    },
  });

  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111142", 10);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const preset = getBuiltinProvider("anthropic")!;
  const pluginId = "88888888-8888-4888-8888-888888888842";
  snap.plugins.set(pluginId, {
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
  });
  const custPriceId = "66666666-6666-4666-8666-666666666642";
  const supPriceId = "77777777-7777-4777-8777-777777777742";
  for (const [id, side] of [
    [custPriceId, "CUSTOMER"],
    [supPriceId, "SUPPLIER"],
  ] as const) {
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
      source_digest: "d1",
    });
  }
  const acctId = "44444444-4444-4444-8444-444444444442";
  const primaryId = "33333333-3333-4333-8333-333333333343";
  const backupId = "33333333-3333-4333-8333-333333333344";
  snap.accounts.set(acctId, { id: acctId, name: "acct-529", state: "ACTIVE", revision: "1" });
  snap.models.set("claude-529", {
    name: "claude-529",
    operations: ["messages"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 1000,
    enabled: true,
  });

  let backupStatus = 200;
  let primaryHits = 0;
  let backupHits = 0;
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(req) {
      const path = new URL(req.url).pathname;
      if (path.startsWith("/primary/")) {
        primaryHits++;
        return Response.json(
          { type: "error", error: { type: "overloaded_error", message: "Overloaded" } },
          { status: 529, headers: { "retry-after": "2" } },
        );
      }
      backupHits++;
      if (backupStatus === 529) {
        return Response.json(
          { type: "error", error: { type: "overloaded_error", message: "Backup overloaded" } },
          { status: 529 },
        );
      }
      return Response.json({
        id: "msg_backup",
        type: "message",
        role: "assistant",
        model: "claude-529",
        content: [{ type: "text", text: "from backup" }],
        stop_reason: "end_turn",
        usage: { input_tokens: 4, output_tokens: 3 },
      });
    },
  });

  const makeCh = (id: string, name: string, prefix: string): ChannelRecord => ({
    id,
    account_id: acctId,
    name,
    kind: "anthropic",
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
  snap.channels.set(primaryId, makeCh(primaryId, "ch-primary", "primary"));
  snap.channels.set(backupId, makeCh(backupId, "ch-backup", "backup"));
  snap.routesByModel.set("claude-529", [
    {
      id: "r-529-1",
      model_name: "claude-529",
      channel_id: primaryId,
      upstream_model: "claude-529",
      operations: ["messages"],
      billing_price_id: custPriceId,
      cost_version_id: supPriceId,
      priority: 30,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 2 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: {},
    },
    {
      id: "r-529-2",
      model_name: "claude-529",
      channel_id: backupId,
      upstream_model: "claude-529",
      operations: ["messages"],
      billing_price_id: custPriceId,
      cost_version_id: supPriceId,
      priority: 10,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 2 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: {},
    },
  ]);

  const deps = makeFakeDeps(snap, makeConfig());
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "req", held_atoms: "1000" };
    },
    async switchSupplierAttempt() {
      return { ok: true, held_atoms: "1000" };
    },
    async commitTerminal() {
      return {
        ok: true,
        replayed: false,
        charge_id: "chg",
        customer_atoms: "7",
        supplier_cost_atoms: "7",
      };
    },
    async releaseHold() {},
    async markUnknown() {},
  } as unknown as ProxyDependencies["ledger"];

  const invoke = () =>
    handleInferenceRequest(
      {
        req: {
          raw: new Request("http://127.0.0.1/v1/messages", {
            method: "POST",
            headers: { "x-api-key": rawKey, "content-type": "application/json" },
            body: JSON.stringify({
              model: "claude-529",
              max_tokens: 64,
              messages: [{ role: "user", content: "hi" }],
            }),
          }),
          query: () => undefined,
        },
        json: (data, status, headers) =>
          new Response(JSON.stringify(data), {
            status: status ?? 200,
            headers: { "content-type": "application/json", ...headers },
          }),
      },
      deps,
      "messages",
    );

  try {
    const failoverRes = await invoke();
    expect(failoverRes.status).toBe(200);
    expect(failoverRes.headers.get("x-gateway-channel")).toBe("ch-backup");
    expect(primaryHits).toBe(1);
    expect(backupHits).toBe(1);
    expect(
      deps.router.snapshot().find((s) => s.channel_id === primaryId)?.consecutive_failures,
    ).toBe(1);

    backupStatus = 529;
    const exhausted = await invoke();
    expect(exhausted.status).toBe(502);
    expect(await exhausted.json()).toMatchObject({
      type: "error",
      error: { type: "api_error", code: "upstream_error" },
    });
  } finally {
    deps.state?.close();
    deps.transport.close();
    await upstream.stop(true);
  }
});

test("local conversion_loss, request_policy rejections, and upstream 400 never trip circuit breaker while upstream 401 trips immediately", async () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111143", 10);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const preset = getBuiltinProvider("anthropic")!;
  const pluginId = "88888888-8888-4888-8888-888888888843";
  snap.plugins.set(pluginId, {
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
  });
  const custPriceId = "66666666-6666-4666-8666-666666666643";
  const supPriceId = "77777777-7777-4777-8777-777777777743";
  for (const [id, side] of [
    [custPriceId, "CUSTOMER"],
    [supPriceId, "SUPPLIER"],
  ] as const) {
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
      source_digest: "d1",
    });
  }
  const acctId = "44444444-4444-4444-8444-444444444443";
  const chId = "33333333-3333-4333-8333-333333333345";
  snap.accounts.set(acctId, { id: acctId, name: "acct-local-err", state: "ACTIVE", revision: "1" });
  snap.models.set("chat-via-anthropic", {
    name: "chat-via-anthropic",
    operations: ["chat"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 1000,
    enabled: true,
  });

  let upstreamStatus = 400;
  let upstreamHits = 0;
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      upstreamHits++;
      return Response.json(
        { error: { message: `status ${upstreamStatus}` } },
        { status: upstreamStatus },
      );
    },
  });
  snap.channels.set(chId, {
    id: chId,
    account_id: acctId,
    name: "ch-local-err",
    kind: "anthropic",
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
  snap.routesByModel.set("chat-via-anthropic", [
    {
      id: "r-local-err",
      model_name: "chat-via-anthropic",
      channel_id: chId,
      upstream_model: "claude-up",
      operations: ["chat"],
      billing_price_id: custPriceId,
      cost_version_id: supPriceId,
      priority: 10,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 1 },
      request_policy: { blocked_parameters: ["seed"] },
      plugin_pipeline: [],
      adapter_overrides: {},
    },
  ]);

  const deps = makeFakeDeps(snap, makeConfig());
  deps.router = new SmartRouter({ failureThreshold: 3 });
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "req", held_atoms: "1000" };
    },
    async releaseHold() {},
    async markUnknown() {},
  } as unknown as ProxyDependencies["ledger"];

  const invoke = (extra: Record<string, unknown> = {}) =>
    handleInferenceRequest(
      {
        req: {
          raw: new Request("http://127.0.0.1/v1/chat/completions", {
            method: "POST",
            headers: { authorization: `Bearer ${rawKey}`, "content-type": "application/json" },
            body: JSON.stringify({
              model: "chat-via-anthropic",
              messages: [{ role: "user", content: "hi" }],
              ...extra,
            }),
          }),
          query: () => undefined,
        },
        json: (data, status, headers) =>
          new Response(JSON.stringify(data), {
            status: status ?? 200,
            headers: { "content-type": "application/json", ...headers },
          }),
      },
      deps,
      "chat",
    );

  try {
    for (let i = 0; i < 4; i++) {
      expect((await invoke({ logprobs: true })).status).toBe(422);
      expect((await invoke({ seed: 7 })).status).toBe(422);
    }
    expect(upstreamHits).toBe(0);

    upstreamStatus = 400;
    for (let i = 0; i < 4; i++) {
      expect((await invoke()).status).toBe(400);
    }
    expect(upstreamHits).toBe(4);
    const beforeAuthErr = deps.router.snapshot().find((s) => s.channel_id === chId);
    expect(beforeAuthErr?.state ?? "CLOSED").toBe("CLOSED");
    expect(beforeAuthErr?.consecutive_failures ?? 0).toBe(0);

    upstreamStatus = 401;
    expect((await invoke()).status).toBe(502);
    expect(deps.router.snapshot().find((s) => s.channel_id === chId)?.state).toBe("OPEN");

    const hitsAtTrip = upstreamHits;
    const afterTrip = await invoke();
    expect(afterTrip.status).toBe(503);
    expect(upstreamHits).toBe(hitsAtTrip);
  } finally {
    deps.state?.close();
    deps.transport.close();
    await upstream.stop(true);
  }
});

test("symptom: parseRetryAfterMs treats negative and non-HTTP-date numeric headers as zero cooldown instead of undefined", () => {
  for (const invalid of ["-5", "-100", "-0.5", "+5", "1e3", "2025-01-01", "9".repeat(400)]) {
    expect(parseRetryAfterMs(invalid), invalid).toBeUndefined();
  }
  expect(parseRetryAfterMs("12")).toBe(12_000);
  expect(parseRetryAfterMs("0.5")).toBe(500);
  expect(parseRetryAfterMs("0")).toBe(0);
  expect(parseRetryAfterMs("Sunday, 06-Nov-94 08:49:37 GMT")).toBe(0);
  expect(parseRetryAfterMs("Sun Nov  6 08:49:37 1994")).toBe(0);
  const futureImf = new Date(Date.now() + 15_000).toUTCString();
  const futureMs = parseRetryAfterMs(futureImf);
  expect(futureMs).toBeGreaterThan(0);
  expect(futureMs).toBeLessThanOrEqual(16_000);
});
