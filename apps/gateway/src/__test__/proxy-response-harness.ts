import { createKey, UpstreamPreDispatchError } from "@gateway/runtime";
import type { AdapterOverrides, CommitTerminalInput } from "@gateway/ledger";
import { getBuiltinProvider } from "@gateway/plugin-engine";
import type { GatewayConfig } from "../config.ts";
import type { Usage } from "@gateway/kernel";
import { handleInferenceRequest } from "../proxy.ts";
import type { ProxyDependencies } from "../inference/types.ts";
import {
  makeApiKey,
  makeBillingAccount,
  makeConfig,
  makeFakeDeps,
  makeSnapshot,
} from "./proxy-fixture.ts";

export const jsonBody =
  '{"model":"test-chat","choices":[{"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2}}';

export async function invokeInference(
  body: string,
  {
    stream = false,
    fast = false,
    cancel = false,
    cancelBeforeDispatch = false,
    delayMs = 0,
    operation = "chat",
    fields = {},
    overrides = {},
    configOverrides = {},
    heldAtoms = "1000",
    headers = {},
    customerRates,
    supplierRates,
    customerBasis = "PROVIDER_USAGE",
    supplierBasis = "PROVIDER_USAGE",
    skipPrepareDispatch = false,
    emitTiming = "none",
    streamBody = false,
    upstreamKind,
    modelMaxOutputTokens = 100,
    multipartBody,
    rawJsonBody,
    responseContentType,
    checkConsoleAdmission,
  }: {
    stream?: boolean;
    fast?: boolean;
    cancel?: boolean;
    cancelBeforeDispatch?: boolean;
    delayMs?: number;
    operation?:
      | "chat"
      | "completions"
      | "responses"
      | "gemini"
      | "messages"
      | "images"
      | "image_edits"
      | "speech";
    fields?: Record<string, unknown>;
    overrides?: AdapterOverrides;
    configOverrides?: Partial<GatewayConfig>;
    heldAtoms?: string;
    headers?: Record<string, string>;
    customerRates?: Record<string, { numerator: string; denominator: string }>;
    supplierRates?: Record<string, { numerator: string; denominator: string }>;
    customerBasis?: "PROVIDER_USAGE" | "INPUT_CHARACTERS" | "REQUEST_COMPLETION";
    supplierBasis?: "PROVIDER_USAGE" | "INPUT_CHARACTERS" | "REQUEST_COMPLETION";
    skipPrepareDispatch?: boolean;
    emitTiming?: "none" | "pre_dispatch" | "connection_ready";
    streamBody?: boolean;
    upstreamKind?: "openai" | "anthropic" | "gemini";
    modelMaxOutputTokens?: number;
    multipartBody?: FormData;
    rawJsonBody?: string;
    responseContentType?: string;
    checkConsoleAdmission?: ProxyDependencies["checkConsoleAdmission"];
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
    upstreamKind ??
    (operation === "gemini" ? "gemini" : operation === "messages" ? "anthropic" : "openai");
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
    const basis = side === "CUSTOMER" ? customerBasis : supplierBasis;
    const explicitRates = side === "CUSTOMER" ? customerRates : supplierRates;
    const rates =
      explicitRates ??
      (basis === "INPUT_CHARACTERS"
        ? { characters: { numerator: "1", denominator: "1" } }
        : basis === "REQUEST_COMPLETION"
          ? { requests: { numerator: "10", denominator: "1" } }
          : operation === "images" || operation === "image_edits"
            ? { images: { numerator: "10", denominator: "1" } }
            : {
                input_tokens: { numerator: "1", denominator: "1" },
                output_tokens: { numerator: "1", denominator: "1" },
              });
    const ceilings: Record<string, string> =
      basis === "INPUT_CHARACTERS"
        ? { characters: "10000" }
        : basis === "REQUEST_COMPLETION"
          ? { requests: "1" }
          : operation === "images" || operation === "image_edits"
            ? { images: "10" }
            : { input_tokens: "1000", output_tokens: String(Math.max(1000, modelMaxOutputTokens)) };
    if (rates.cache_write_tokens) ceilings.cache_write_tokens = "1000";
    if (rates.cache_write_5m_tokens) ceilings.cache_write_5m_tokens = "1000";
    if (rates.cache_write_1h_tokens) ceilings.cache_write_1h_tokens = "1000";
    if (rates.cached_input_tokens) ceilings.cached_input_tokens = "1000";
    const price = {
      id: crypto.randomUUID(),
      side,
      currency: "USD",
      basis,
      rates,
      ceilings,
      source_digest: "test",
    };
    snap.prices.set(price.id, price);
    return price.id;
  });
  snap.models.set(model, {
    name: model,
    operations: [operation],
    routing_strategy: "WEIGHTED",
    max_output_tokens: modelMaxOutputTokens,
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
  const deps = makeFakeDeps(snap, { ...makeConfig(), ...configOverrides });
  if (checkConsoleAdmission) deps.checkConsoleAdmission = checkConsoleAdmission;
  const controller = new AbortController();
  let transportCancelled = false;
  const finalized: string[] = [];
  const settledUsages: Usage[] = [];
  const settledInputs: CommitTerminalInput[] = [];
  let dispatches = 0;
  let holds = 0;
  let requestedCustomerAtoms: string | undefined;
  let holdExpiresAt: string | undefined;
  const dispatchedBodies: string[] = [];
  deps.ledger = {
    async acquireHold(input: { customer_atoms: string; expires_at: string }) {
      holds++;
      requestedCustomerAtoms = input.customer_atoms;
      holdExpiresAt = input.expires_at;
      if (cancelBeforeDispatch) {
        controller.abort(new Error("Client disconnected before upstream dispatch"));
      }
      return { ok: true, replayed: false, request_id: "req", held_atoms: heldAtoms };
    },
    async findIdempotentHold() {
      return null;
    },
    async commitTerminal(input: CommitTerminalInput) {
      finalized.push("SETTLED");
      settledUsages.push(input.usage);
      settledInputs.push(input);
      return { ok: true, charge_id: "charge", supplier_cost_atoms: "5" };
    },
    async markUnknown() {
      finalized.push("UNKNOWN");
    },
    async releaseHold() {
      finalized.push("RELEASED");
    },
  } as unknown as ProxyDependencies["ledger"];
  const contentType = responseContentType ?? (stream ? "text/event-stream" : "application/json");
  const bytes = new TextEncoder().encode(body);
  const fetchFast: typeof deps.transport.fetchFast = async (_url, init) => {
    dispatches++;
    if (typeof init.body === "string") {
      dispatchedBodies.push(init.body);
    } else if (init.body instanceof Uint8Array) {
      dispatchedBodies.push(new TextDecoder().decode(init.body));
    }
    if (stream && !fast) {
      const response = new Response(bytes, { headers: { "content-type": contentType } });
      return { fast: false, response };
    }
    if (streamBody) {
      const response = new Response(
        new ReadableStream<Uint8Array>({
          async start(streamCtrl) {
            if (cancel) {
              controller.abort(new Error("Client disconnected during chunked body read"));
            }
            if (delayMs > 0) await Bun.sleep(delayMs);
            streamCtrl.enqueue(bytes);
            streamCtrl.close();
          },
        }),
        { headers: { "content-type": contentType } },
      );
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
      cancelled.reject(
        emitTiming === "pre_dispatch"
          ? new UpstreamPreDispatchError(
              "upstream_not_dispatched",
              "Upstream request cancelled before dispatch",
            )
          : reason,
      );
    });
    try {
      if (emitTiming === "pre_dispatch") {
        init.onTiming?.("dns_start", performance.now());
      } else if (emitTiming === "connection_ready") {
        init.onTiming?.("connection_ready", performance.now());
      }
      controller.abort(new Error("Client disconnected during upstream work"));
      const finish = delayMs > 0 ? Bun.sleep(delayMs).then(() => result) : Promise.resolve(result);
      return await Promise.race([cancelled.promise, finish]);
    } finally {
      detach?.();
    }
  };
  if (!skipPrepareDispatch) {
    deps.transport.prepareDispatch = async (url) => ({
      fetchFast: (init) => fetchFast(url, init, true),
      release() {},
    });
  }
  deps.transport.fetchFast = fetchFast;
  const path = {
    chat: "/v1/chat/completions",
    completions: "/v1/completions",
    responses: "/v1/responses",
    messages: "/v1/messages",
    gemini: `/v1beta/models/${model}:generateContent`,
    images: "/v1/images/generations",
    image_edits: "/v1/images/edits",
    speech: "/v1/audio/speech",
  }[operation];
  const raw = multipartBody
    ? new Request(`http://gateway.test${path}`, {
        method: "POST",
        signal: controller.signal,
        headers: { authorization: `Bearer ${key}`, ...headers },
        body: multipartBody,
      })
    : new Request(`http://gateway.test${path}`, {
        method: "POST",
        signal: controller.signal,
        headers: { authorization: `Bearer ${key}`, "content-type": "application/json", ...headers },
        body:
          rawJsonBody ??
          JSON.stringify({
            model,
            ...(operation === "images" || operation === "image_edits" || operation === "speech"
              ? {}
              : { stream }),
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
    return {
      status: response.status,
      wire,
      finalized,
      settledUsages,
      settledInputs,
      transportCancelled,
      dispatches,
      holds,
      requestedCustomerAtoms,
      holdExpiresAt,
      dispatchedBodies,
      estimatedStreamSettlements: deps.metrics.estimatedStreamSettlements,
    };
  } finally {
    deps.transport.close();
    deps.state?.close();
  }
}
