import { expect, test } from "bun:test";
import { calculateCost } from "@gateway/kernel";
import type { CommitTerminalInput, PluginRecord, SwitchSupplierInput } from "@gateway/ledger";
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

test("failover switchSupplier tops up only when backup route is pricier and releases hold when top-up fails on balance or key spend limit", async () => {
  async function runFailover(options: {
    primaryOutputRate: string;
    backupOutputRate: string;
    switchError?: { code: "insufficient_balance" | "key_spend_limit_exceeded"; message: string };
  }) {
    const billingAccount = makeBillingAccount(crypto.randomUUID(), 10);
    const key = createKey();
    const snap = makeSnapshot(1, [billingAccount], [makeApiKey(key, billingAccount.id)]);
    const provider = getBuiltinProvider("openai")!;
    const pluginId = crypto.randomUUID();
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
    const makeRouteFixture = (name: string, outputRate: string, priority: number) => {
      const accountId = crypto.randomUUID();
      const channelId = crypto.randomUUID();
      snap.accounts.set(accountId, { id: accountId, name, state: "ACTIVE", revision: "1" });
      snap.channels.set(channelId, {
        id: channelId,
        account_id: accountId,
        name,
        kind: "openai",
        provider_plugin_id: pluginId,
        base_url: `http://${name}.local`,
        secret_ciphertext: "",
        adapter_config: {},
        rpm: 1000,
        concurrency: 100,
        enabled: true,
        routing_policy: {},
        config_revision: 1,
      });
      const cp = {
        id: crypto.randomUUID(),
        side: "CUSTOMER" as const,
        currency: "USD",
        basis: "PROVIDER_USAGE" as const,
        rates: {
          input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: outputRate, denominator: "1" },
        },
        ceilings: { input_tokens: "1000", output_tokens: "1000" },
        source_digest: name,
      };
      const sp = { ...cp, id: crypto.randomUUID(), side: "SUPPLIER" as const };
      snap.prices.set(cp.id, cp);
      snap.prices.set(sp.id, sp);
      return {
        id: crypto.randomUUID(),
        model_name: "failover-chat",
        channel_id: channelId,
        upstream_model: "failover-chat",
        operations: ["chat" as const],
        billing_price_id: cp.id,
        cost_version_id: sp.id,
        priority,
        weight: 100,
        enabled: true,
        retry_policy: { max_attempts: 2 },
        request_policy: {},
        plugin_pipeline: [],
        adapter_overrides: {},
      };
    };
    snap.models.set("failover-chat", {
      name: "failover-chat",
      operations: ["chat"],
      routing_strategy: "WEIGHTED",
      max_output_tokens: 100,
      enabled: true,
    });
    snap.routesByModel.set("failover-chat", [
      makeRouteFixture("primary", options.primaryOutputRate, 10),
      makeRouteFixture("backup", options.backupOutputRate, 5),
    ]);

    const deps = makeFakeDeps(snap, makeConfig());
    const switchInputs: SwitchSupplierInput[] = [];
    const finalized: string[] = [];
    let heldAtoms = "0";
    deps.ledger = {
      async acquireHold(input: { customer_atoms: string }) {
        heldAtoms = input.customer_atoms;
        return { ok: true, replayed: false, request_id: "req", held_atoms: heldAtoms };
      },
      async switchSupplierAttempt(input: SwitchSupplierInput) {
        switchInputs.push(input);
        if (options.switchError) {
          return { ok: false, ...options.switchError };
        }
        heldAtoms = input.required_customer_atoms ?? heldAtoms;
        return { ok: true, held_atoms: heldAtoms };
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

    let attemptCount = 0;
    const fetchFast: typeof deps.transport.fetchFast = async () => {
      attemptCount++;
      if (attemptCount === 1) {
        return {
          fast: true,
          status: 503,
          contentType: "application/json",
          headers: { "content-type": "application/json" },
          body: new TextEncoder().encode('{"error":{"message":"overloaded"}}'),
        };
      }
      return {
        fast: true,
        status: 200,
        contentType: "application/json",
        headers: { "content-type": "application/json" },
        body: new TextEncoder().encode(jsonBody),
      };
    };
    deps.transport.prepareDispatch = async (url) => ({
      fetchFast: (init) => fetchFast(url, init, true),
      release() {},
    });
    deps.transport.fetchFast = fetchFast;
    try {
      const resp = await handleInferenceRequest(
        {
          req: {
            raw: new Request("http://gateway.test/v1/chat/completions", {
              method: "POST",
              headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
              body: JSON.stringify({
                model: "failover-chat",
                messages: [{ role: "user", content: "Hi" }],
              }),
            }),
            query: () => undefined,
          },
          json: (v, s = 200, h = {}) => Response.json(v, { status: s, headers: h }),
        },
        deps,
        "chat",
      );
      return {
        status: resp.status,
        body: await resp.text(),
        switchInputs,
        finalized,
        attemptCount,
      };
    } finally {
      deps.transport.close();
      deps.state?.close();
    }
  }

  const pricier = await runFailover({ primaryOutputRate: "1", backupOutputRate: "5" });
  expect(pricier.status).toBe(200);
  expect(pricier.finalized).toEqual(["SETTLED"]);
  expect(pricier.switchInputs).toHaveLength(1);
  expect(pricier.switchInputs[0]!.required_customer_atoms).toBeDefined();
  expect(Number(pricier.switchInputs[0]!.required_customer_atoms)).toBeGreaterThan(100);

  const cheaper = await runFailover({ primaryOutputRate: "5", backupOutputRate: "1" });
  expect(cheaper.status).toBe(200);
  expect(cheaper.switchInputs).toHaveLength(1);
  expect(cheaper.switchInputs[0]!.required_customer_atoms).toBeUndefined();

  for (const code of ["insufficient_balance", "key_spend_limit_exceeded"] as const) {
    const rejected = await runFailover({
      primaryOutputRate: "1",
      backupOutputRate: "5",
      switchError: { code, message: `rejected:${code}` },
    });
    expect(rejected.status, code).toBe(402);
    expect(rejected.body, code).toContain(code);
    expect(rejected.finalized, code).toEqual(["RELEASED"]);
    expect(rejected.attemptCount, code).toBe(1);
  }
});

test("cross-protocol bridge routes settle identical normalized usage and cost atoms for the same upstream Anthropic response", async () => {
  const anthropicRichBody = JSON.stringify({
    id: "msg_parity",
    type: "message",
    role: "assistant",
    model: "test-chat",
    content: [{ type: "text", text: "parity answer" }],
    stop_reason: "end_turn",
    usage: {
      input_tokens: 40,
      output_tokens: 15,
      cache_read_input_tokens: 120,
      cache_creation_input_tokens: 30,
      cache_creation: { ephemeral_5m_input_tokens: 20, ephemeral_1h_input_tokens: 10 },
    },
  });
  const rates = {
    input_tokens: { numerator: "3", denominator: "1" },
    output_tokens: { numerator: "15", denominator: "1" },
    cached_input_tokens: { numerator: "3", denominator: "10" },
    cache_write_5m_tokens: { numerator: "15", denominator: "4" },
    cache_write_1h_tokens: { numerator: "6", denominator: "1" },
  };
  const nativeMessages = await invokeInference(anthropicRichBody, {
    operation: "messages",
    upstreamKind: "anthropic",
    customerRates: rates,
    supplierRates: rates,
    heldAtoms: "50000",
    fields: { max_tokens: 64 },
  });
  const viaChat = await invokeInference(anthropicRichBody, {
    operation: "chat",
    upstreamKind: "anthropic",
    customerRates: rates,
    supplierRates: rates,
    heldAtoms: "50000",
  });
  const viaResponses = await invokeInference(anthropicRichBody, {
    operation: "responses",
    upstreamKind: "anthropic",
    customerRates: rates,
    supplierRates: rates,
    heldAtoms: "50000",
    fields: { input: "Hi" },
  });

  expect(nativeMessages.status).toBe(200);
  expect(viaChat.status).toBe(200);
  expect(viaResponses.status).toBe(200);
  const expectedUsage = {
    input_tokens: "40",
    output_tokens: "15",
    cached_input_tokens: "120",
    cache_write_tokens: "30",
    cache_write_5m_tokens: "20",
    cache_write_1h_tokens: "10",
  };
  expect(nativeMessages.settledUsages).toEqual([expectedUsage]);
  expect(viaChat.settledUsages).toEqual([expectedUsage]);
  expect(viaResponses.settledUsages).toEqual([expectedUsage]);

  const pricing = { currency: "USD", rates };
  const nativeAtoms = calculateCost(nativeMessages.settledUsages[0]!, pricing);
  expect(calculateCost(viaChat.settledUsages[0]!, pricing)).toBe(nativeAtoms);
  expect(calculateCost(viaResponses.settledUsages[0]!, pricing)).toBe(nativeAtoms);
});

test("mid-stream Anthropic disconnect preserves input/cache counts, tool-only disconnect estimates output, and stream errors mark UNKNOWN without estimation", async () => {
  const anthropicRates = {
    input_tokens: { numerator: "1", denominator: "1" },
    output_tokens: { numerator: "1", denominator: "1" },
    cached_input_tokens: { numerator: "1", denominator: "10" },
    cache_write_tokens: { numerator: "2", denominator: "1" },
  };
  const anthropicTruncatedSse =
    'event: message_start\ndata: {"type":"message_start","message":{"id":"m1","type":"message","role":"assistant","model":"test-chat","content":[],"usage":{"input_tokens":30,"output_tokens":1,"cache_read_input_tokens":2000,"cache_creation_input_tokens":100}}}\n\n' +
    'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n' +
    'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"This response is long enough to exceed one placeholder token cleanly."}}\n\n';
  const anthropicCut = await invokeInference(anthropicTruncatedSse, {
    operation: "messages",
    upstreamKind: "anthropic",
    stream: true,
    customerRates: anthropicRates,
    supplierRates: anthropicRates,
    heldAtoms: "10000",
    fields: { max_tokens: 64 },
  });
  expect(anthropicCut.status).toBe(200);
  expect(anthropicCut.finalized).toEqual(["SETTLED"]);
  expect(anthropicCut.estimatedStreamSettlements).toBe(1);
  expect(anthropicCut.settledUsages[0]).toMatchObject({
    input_tokens: "30",
    cached_input_tokens: "2000",
    cache_write_tokens: "100",
  });
  expect(Number(anthropicCut.settledUsages[0]!.output_tokens)).toBeGreaterThan(1);

  const toolOnlyTruncatedSse =
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"fetch_metrics","arguments":"{\\"region\\":\\"us-east-1\\"}"}}]}}]}\n\n';
  const toolCut = await invokeInference(toolOnlyTruncatedSse, {
    operation: "chat",
    stream: true,
    heldAtoms: "10000",
  });
  expect(toolCut.status).toBe(200);
  expect(toolCut.finalized).toEqual(["SETTLED"]);
  expect(toolCut.estimatedStreamSettlements).toBe(1);
  expect(Number(toolCut.settledUsages[0]!.output_tokens)).toBeGreaterThan(0);

  const errorSse =
    'event: error\ndata: {"type":"error","error":{"type":"overloaded_error","message":"boom"}}\n\n';
  const streamErr = await invokeInference(errorSse, {
    operation: "messages",
    upstreamKind: "anthropic",
    stream: true,
    heldAtoms: "10000",
    fields: { max_tokens: 64 },
  });
  expect(streamErr.finalized).toEqual(["UNKNOWN"]);
  expect(streamErr.estimatedStreamSettlements).toBe(0);
});

test("moderation blocks charge zero on pre-request reject, CHARGE_CUSTOMER on content post-response reject, and PLATFORM_ABSORB on plugin fault", async () => {
  async function runModeration(mode: "pre_block" | "post_content_block" | "post_plugin_fault") {
    const billingAccount = makeBillingAccount(crypto.randomUUID(), 10);
    const key = createKey();
    const snap = makeSnapshot(1, [billingAccount], [makeApiKey(key, billingAccount.id)]);
    const accountId = crypto.randomUUID();
    const channelId = crypto.randomUUID();
    const providerPluginId = crypto.randomUUID();
    const modPluginId = crypto.randomUUID();
    const provider = getBuiltinProvider("openai")!;
    snap.plugins.set(providerPluginId, {
      id: providerPluginId,
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
    const modPlugin: PluginRecord = {
      id: modPluginId,
      name: "guard",
      version: "1.0.0",
      capability: "CONTENT",
      kind: "WEBHOOK",
      digest: "d".repeat(64),
      wasm_bytes: null,
      webhook_config: {
        url: "http://guard.local/check",
        timeout_ms: 1000,
        failure_policy: "FAIL_CLOSED",
      },
      provider_config: null,
      signature: null,
      signing_key_id: null,
      manifest: {},
      enabled: true,
    };
    snap.plugins.set(modPluginId, modPlugin);
    snap.accounts.set(accountId, { id: accountId, name: "u", state: "ACTIVE", revision: "1" });
    snap.channels.set(channelId, {
      id: channelId,
      account_id: accountId,
      name: "ch",
      kind: "openai",
      provider_plugin_id: providerPluginId,
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
        billing_price_id: cp!,
        cost_version_id: sp!,
        priority: 1,
        weight: 100,
        enabled: true,
        retry_policy: { max_attempts: 1 },
        request_policy: {},
        plugin_pipeline: [
          { plugin_id: modPluginId, priority: 1, enabled: true, charge_policy: "CHARGE_CUSTOMER" },
        ],
        adapter_overrides: {},
      },
    ]);

    const deps = makeFakeDeps(snap, makeConfig());
    let holds = 0;
    const commits: CommitTerminalInput[] = [];
    deps.ledger = {
      async acquireHold() {
        holds++;
        return { ok: true, replayed: false, request_id: "req", held_atoms: "1000" };
      },
      async commitTerminal(input: CommitTerminalInput) {
        commits.push(input);
        return { ok: true, charge_id: "charge", supplier_cost_atoms: "5" };
      },
      async markUnknown() {},
      async releaseHold() {},
    } as unknown as ProxyDependencies["ledger"];
    deps.pluginEngine = {
      hasActiveHook(hook: string) {
        return (
          (mode === "pre_block" && hook === "on_request") ||
          (mode !== "pre_block" && hook === "on_response")
        );
      },
      admitOutputPolicies() {
        return { ok: true, release() {} };
      },
      async runUnary() {
        if (mode === "pre_block") {
          return {
            blocked: true,
            text: "",
            outcome_kind: "content_decision",
            http_status: 400,
            rule_code: "PII_INPUT",
            safe_message: "Blocked input",
            charge_policy: "CHARGE_CUSTOMER",
            audit: [],
          };
        }
        if (mode === "post_content_block") {
          return {
            blocked: true,
            text: "",
            outcome_kind: "content_decision",
            http_status: 400,
            rule_code: "POLICY_OUTPUT",
            safe_message: "Blocked output",
            charge_policy: "CHARGE_CUSTOMER",
            audit: [],
          };
        }
        return {
          blocked: true,
          text: "",
          outcome_kind: "plugin_fault",
          safe_message: "Guard crashed",
          charge_policy: "CHARGE_CUSTOMER",
          audit: [],
        };
      },
    } as unknown as ProxyDependencies["pluginEngine"];
    const fetchFast: typeof deps.transport.fetchFast = async () => ({
      fast: true,
      status: 200,
      contentType: "application/json",
      headers: { "content-type": "application/json" },
      body: new TextEncoder().encode(jsonBody),
    });
    deps.transport.prepareDispatch = async (url) => ({
      fetchFast: (init) => fetchFast(url, init, true),
      release() {},
    });
    deps.transport.fetchFast = fetchFast;
    try {
      const resp = await handleInferenceRequest(
        {
          req: {
            raw: new Request("http://gateway.test/v1/chat/completions", {
              method: "POST",
              headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
              body: JSON.stringify({
                model: "mod-chat",
                messages: [{ role: "user", content: "Hi" }],
              }),
            }),
            query: () => undefined,
          },
          json: (v, s = 200, h = {}) => Response.json(v, { status: s, headers: h }),
        },
        deps,
        "chat",
      );
      return {
        status: resp.status,
        holds,
        commits,
        platformAbsorbExceeded: deps.metrics.isTenantPlatformAbsorbExceeded(billingAccount.id, 1),
      };
    } finally {
      deps.transport.close();
      deps.state?.close();
    }
  }

  const pre = await runModeration("pre_block");
  expect(pre.status).toBe(400);
  expect(pre.holds).toBe(0);
  expect(pre.commits).toHaveLength(0);

  const postContent = await runModeration("post_content_block");
  expect(postContent.status).toBe(400);
  expect(postContent.holds).toBe(1);
  expect(postContent.commits).toHaveLength(1);
  expect(postContent.commits[0]!.usage).toEqual({ input_tokens: "3", output_tokens: "2" });
  expect(postContent.commits[0]!.charge_policy).toBe("CHARGE_CUSTOMER");
  expect(postContent.commits[0]!.moderation_blocked).toBe(true);
  expect(postContent.platformAbsorbExceeded).toBe(false);

  const postFault = await runModeration("post_plugin_fault");
  expect(postFault.status).toBe(500);
  expect(postFault.commits).toHaveLength(1);
  expect(postFault.commits[0]!.usage).toEqual({ input_tokens: "3", output_tokens: "2" });
  expect(postFault.commits[0]!.charge_policy).toBe("PLATFORM_ABSORB");
  expect(postFault.commits[0]!.moderation_blocked).toBe(true);
  expect(postFault.platformAbsorbExceeded).toBe(true);
});

test("non-JSON speech responses settle under character or request pricing bases and reject PROVIDER_USAGE with 502 UNKNOWN", async () => {
  const byChars = await invokeInference("ID3-binary-audio-bytes", {
    operation: "speech",
    fields: { input: "hello world", voice: "alloy" },
    responseContentType: "audio/mpeg",
    customerBasis: "INPUT_CHARACTERS",
    supplierBasis: "REQUEST_COMPLETION",
  });
  expect(byChars.status).toBe(200);
  expect(byChars.finalized).toEqual(["SETTLED"]);
  expect(byChars.settledUsages).toEqual([{ requests: "1" }]);

  const byUsage = await invokeInference("ID3-binary-audio-bytes", {
    operation: "speech",
    fields: { input: "hello world", voice: "alloy" },
    responseContentType: "audio/mpeg",
    customerBasis: "PROVIDER_USAGE",
    supplierBasis: "PROVIDER_USAGE",
  });
  expect(byUsage.status).toBe(502);
  expect(byUsage.wire).toContain("invalid_upstream_usage");
  expect(byUsage.finalized).toEqual(["UNKNOWN"]);
});
