import { GatewayError, optionalObject } from "@gateway/contracts";
import type { Operation, Protocol } from "@gateway/kernel";
import type { ChannelRecord, ConfigSnapshot, RouteRecord } from "@gateway/ledger";
import {
  invokeWasmProviderRequest,
  renderEndpointUrl,
  resolvePinnedProviderPlugin,
  sanitizeProviderHeaders,
  WasmCapacityError,
  WasmPluginFaultError,
  type ResolvedProvider,
  type WasmRuntimeExecutor,
} from "@gateway/plugin-engine";
import { stringifyExact, validateRequestPolicy, type ConversionMode } from "@gateway/protocol";
import { applyAuthToRequest, isValidHttpHeaderValue, type VertexTokens } from "@gateway/runtime";
import {
  type MergedProviderOverrides,
  mergeOverridesForResolved,
  upstreamProtocolFor,
  type PreparedUpstreamRequest,
  buildPreparedPayload,
  joinBase,
  encodeMultipartWithModel,
  isConversationalOperation,
  capKnownOutput,
} from "./upstream-payload.ts";

export { previewProviderRequest } from "./provider-request-preview.ts";

export {
  extractRequestText,
  replaceRequestText,
  transformRequestTexts,
  extractResponseText,
  replaceResponseText,
} from "./upstream-content.ts";

export {
  BedrockEventStreamDecoder,
  extractChunkTextFromEncodedSse,
  inspectAnthropicBlockEvent,
  stripTextFromSseChunk,
  buildTextDeltaSse,
} from "./upstream-stream.ts";

const encoder = new TextEncoder();

type CachedProviderPlan = {
  snapshot: Pick<ConfigSnapshot, "plugins" | "providerPluginsByName">;
  channel: ChannelRecord;
  resolved: ResolvedProvider;
  overrides: MergedProviderOverrides;
  extraHeaderEntries: readonly [string, string][];
  staticEndpoints: Map<string, URL>;
};

const textChatFields = new Set([
  "model",
  "messages",
  "stream",
  "stream_options",
  "max_tokens",
  "max_completion_tokens",
  "temperature",
  "top_p",
  "stop",
  "n",
  "frequency_penalty",
  "presence_penalty",
  "seed",
  "tools",
  "tool_choice",
  "parallel_tool_calls",
  "reasoning_effort",
  "reasoning_split",
  "response_format",
  "user",
]);

const textMessageFields = new Set([
  "role",
  "content",
  "name",
  "tool_calls",
  "tool_call_id",
  "function_call",
  "reasoning_content",
]);

function estimateTextChatInput(payload: Record<string, unknown>, body: string): number | undefined {
  if (
    !Array.isArray(payload.messages) ||
    Object.keys(payload).some((key) => !textChatFields.has(key)) ||
    (payload.n !== undefined && payload.n !== 1)
  )
    return undefined;
  const tools = payload.tools ?? [];
  if (!Array.isArray(tools) || tools.some((tool) => optionalObject(tool).type !== "function")) {
    return undefined;
  }
  for (const value of payload.messages) {
    const message = optionalObject(value);
    if (
      typeof message.role !== "string" ||
      Object.keys(message).some((key) => !textMessageFields.has(key))
    ) {
      return undefined;
    }
    const content = message.content;
    if (content === undefined || content === null || typeof content === "string") continue;
    if (
      !Array.isArray(content) ||
      content.some((value) => {
        const part = optionalObject(value);
        return (
          part.type !== "text" || typeof part.text !== "string" || Object.keys(part).length !== 2
        );
      })
    )
      return undefined;
  }
  return Buffer.byteLength(body) + 128 + 32 * (payload.messages.length + tools.length);
}

const providerPlans = new WeakMap<RouteRecord, CachedProviderPlan>();

function getProviderPlan(
  snapshot: Pick<ConfigSnapshot, "plugins" | "providerPluginsByName">,
  channel: ChannelRecord,
  route: RouteRecord,
): CachedProviderPlan {
  const cached = providerPlans.get(route);
  if (cached && cached.channel === channel && cached.snapshot === snapshot) return cached;
  const resolved = channel.provider_plugin_id
    ? resolvePinnedProviderPlugin(channel.provider_plugin_id, snapshot)
    : null;
  if (!resolved) {
    throw new GatewayError(
      503,
      "provider_plugin_unavailable",
      `Provider plugin unavailable for channel ${channel.kind}`,
    );
  }
  const overrides = mergeOverridesForResolved(
    resolved,
    channel.adapter_config,
    route.adapter_overrides,
  );
  const sanitized = sanitizeProviderHeaders(overrides.extra_headers);
  const extraHeaderEntries: [string, string][] = [];
  for (const [k, v] of Object.entries(sanitized)) {
    extraHeaderEntries.push([k.toLowerCase(), v]);
  }
  const plan: CachedProviderPlan = {
    snapshot,
    channel,
    resolved,
    overrides,
    extraHeaderEntries,
    staticEndpoints: new Map(),
  };
  providerPlans.set(route, plan);
  return plan;
}

export function resolveRouteUpstreamProtocol(
  snapshot: Pick<ConfigSnapshot, "plugins" | "providerPluginsByName">,
  channel: ChannelRecord,
  route: RouteRecord,
  operation: Operation,
): Protocol | null {
  try {
    const { resolved } = getProviderPlan(snapshot, channel, route);
    return upstreamProtocolFor(resolved, operation);
  } catch {
    return null;
  }
}

export type PrepareUpstreamOptions = {
  channel: ChannelRecord;
  route: RouteRecord;
  snapshot: Pick<ConfigSnapshot, "plugins" | "providerPluginsByName">;
  secret: string;
  operation: Operation;
  clientBody: Record<string, unknown>;
  outputBound?: number;
  multipartEntries?: readonly [string, string | File][];
  isStream: boolean;
  canMutateClientBody?: boolean;
  estimateInputTokens?: boolean;
  vertexTokens: VertexTokens;
  wasmExecutor: WasmRuntimeExecutor | null;
  signingPublicKeyPem?: string;
  tenantId?: string;
  traceparent?: string;
  onFieldDropped?: (source: Protocol, target: Protocol, field: string) => void;
  onFieldRejected?: (source: Protocol, target: Protocol, field: string) => void;
  signal: AbortSignal;
};

export function tryPrepareUpstreamRequestSync(
  options: Omit<PrepareUpstreamOptions, "signal">,
): PreparedUpstreamRequest | null {
  const { channel, route, snapshot, secret, operation, clientBody, multipartEntries, isStream } =
    options;
  if (multipartEntries) return null;
  const plan = getProviderPlan(snapshot, channel, route);
  const { resolved, overrides: mergedOverrides, extraHeaderEntries, staticEndpoints } = plan;
  if (resolved.kind !== "DECLARATIVE") return null;
  const auth = resolved.config.auth;
  if (auth.scheme !== "none" && auth.scheme !== "bearer" && auth.scheme !== "header") {
    return null;
  }

  if (route.request_policy && Object.keys(route.request_policy).length > 0) {
    validateRequestPolicy(clientBody, route.request_policy);
  }

  const upstreamProtocol = upstreamProtocolFor(resolved, operation);
  const usageDialect = resolved.config.usage_dialect;
  const isBedrockStream = isStream && resolved.config.stream_framing === "bedrock_eventstream";

  const conversionMode: ConversionMode =
    route.request_policy?.conversion_mode === "compat" ? "compat" : "strict";
  const payloadOpts: Parameters<typeof buildPreparedPayload>[0] = {
    resolved,
    merged: mergedOverrides,
    operation,
    upstreamProtocol,
    upstreamModel: route.upstream_model,
    clientBody,
    isStream,
    conversionMode,
  };
  if (options.canMutateClientBody) payloadOpts.canMutateClientBody = true;
  if (options.onFieldDropped) payloadOpts.onFieldDropped = options.onFieldDropped;
  if (options.onFieldRejected) payloadOpts.onFieldRejected = options.onFieldRejected;
  if (options.outputBound !== undefined) payloadOpts.outputBound = options.outputBound;
  const { payload, droppedFields } = buildPreparedPayload(payloadOpts);

  const endpointKey = `${operation}:${isStream ? 1 : 0}`;
  let url = staticEndpoints.get(endpointKey);
  if (!url) {
    try {
      url = renderEndpointUrl({
        baseUrl: channel.base_url,
        config: resolved.config,
        operation,
        upstreamModel: route.upstream_model,
        isStream,
        ...(mergedOverrides.path_override ? { pathOverride: mergedOverrides.path_override } : {}),
        extraQuery: mergedOverrides.query_params,
      });
    } catch (err) {
      throw new GatewayError(
        502,
        "provider_adapter_failed",
        err instanceof Error ? err.message : "Failed to render provider endpoint URL",
      );
    }
    staticEndpoints.set(endpointKey, url);
  }

  const headers: Record<string, string> = {};
  if (options.traceparent) headers.traceparent = options.traceparent;
  if (isBedrockStream) headers.accept = "application/vnd.amazon.eventstream";
  else if (isStream) headers.accept = "text/event-stream";
  for (let i = 0; i < extraHeaderEntries.length; i++) {
    const pair = extraHeaderEntries[i]!;
    headers[pair[0]] = pair[1];
  }
  headers["content-type"] = "application/json";
  let secretValid = true;
  if (auth.scheme === "bearer" && secret) {
    secretValid = isValidHttpHeaderValue(secret);
    headers.authorization = `Bearer ${secret}`;
  } else if (auth.scheme === "header" && secret) {
    secretValid = isValidHttpHeaderValue(secret);
    headers[auth.header_name.toLowerCase()] = `${auth.prefix ?? ""}${secret}`;
  }

  const body = stringifyExact(payload);
  const estimatedInputTokens =
    options.estimateInputTokens && upstreamProtocol === "chat" && usageDialect === "openai"
      ? estimateTextChatInput(payload, body)
      : undefined;
  return {
    url,
    headers,
    headersValidated: extraHeaderEntries.length === 0 && auth.scheme !== "header" && secretValid,
    body,
    ...(estimatedInputTokens !== undefined ? { estimatedInputTokens } : {}),
    upstreamProtocol,
    usageDialect,
    isBedrockStream,
    resolvedProvider: resolved,
    mergedOverrides,
    droppedFields,
  };
}

export async function prepareUpstreamRequest(
  options: PrepareUpstreamOptions,
): Promise<PreparedUpstreamRequest> {
  const {
    channel,
    route,
    snapshot,
    secret,
    operation,
    clientBody,
    multipartEntries,
    isStream,
    vertexTokens,
    wasmExecutor,
    signingPublicKeyPem,
    traceparent,
    signal,
  } = options;

  if (route.request_policy && Object.keys(route.request_policy).length > 0) {
    validateRequestPolicy(clientBody, route.request_policy);
  }

  const { resolved, overrides: mergedOverrides } = getProviderPlan(snapshot, channel, route);
  const upstreamProtocol = upstreamProtocolFor(resolved, operation);
  const usageDialect = resolved.config.usage_dialect;
  const isBedrockStream = isStream && resolved.config.stream_framing === "bedrock_eventstream";

  const headers = new Headers();
  if (traceparent) headers.set("traceparent", traceparent);
  if (isBedrockStream) headers.set("accept", "application/vnd.amazon.eventstream");
  else if (isStream) headers.set("accept", "text/event-stream");
  for (const [k, v] of Object.entries(sanitizeProviderHeaders(mergedOverrides.extra_headers))) {
    headers.set(k, v);
  }

  if (multipartEntries) {
    const defaultMultipartPath =
      operation === "transcription"
        ? "/v1/audio/transcriptions"
        : operation === "translation"
          ? "/v1/audio/translations"
          : "/v1/images/edits";
    const url =
      resolved.kind === "DECLARATIVE" &&
      (resolved.config.endpoints[operation] ||
        resolved.config.endpoints.default ||
        mergedOverrides.path_override)
        ? renderEndpointUrl({
            baseUrl: channel.base_url,
            config: resolved.config,
            operation,
            upstreamModel: route.upstream_model,
            isStream: false,
            ...(mergedOverrides.path_override
              ? { pathOverride: mergedOverrides.path_override }
              : {}),
            extraQuery: mergedOverrides.query_params,
          })
        : joinBase(channel.base_url, mergedOverrides.path_override ?? defaultMultipartPath);
    const encoded = await encodeMultipartWithModel(multipartEntries, route.upstream_model);
    headers.set("content-type", encoded.contentType);
    await applyAuthToRequest({
      auth: resolved.config.auth,
      secret,
      url,
      headers,
      body: encoded.bytes,
      vertexTokens,
      signal,
    });
    return {
      url,
      headers,
      body: encoded.bytes,
      upstreamProtocol: null,
      usageDialect,
      isBedrockStream: false,
      resolvedProvider: resolved,
      mergedOverrides,
      droppedFields: [],
    };
  }

  headers.set("content-type", "application/json");
  const conversionMode: ConversionMode =
    route.request_policy?.conversion_mode === "compat" ? "compat" : "strict";
  const { payload, droppedFields } = buildPreparedPayload({
    resolved,
    merged: mergedOverrides,
    operation,
    upstreamProtocol,
    upstreamModel: route.upstream_model,
    clientBody,
    isStream,
    conversionMode,
    ...(options.onFieldDropped ? { onFieldDropped: options.onFieldDropped } : {}),
    ...(options.onFieldRejected ? { onFieldRejected: options.onFieldRejected } : {}),
    ...(options.outputBound === undefined ? {} : { outputBound: options.outputBound }),
  });

  let url: URL;
  let bodyBytes: Uint8Array;

  if (resolved.kind === "DECLARATIVE") {
    try {
      url = renderEndpointUrl({
        baseUrl: channel.base_url,
        config: resolved.config,
        operation,
        upstreamModel: route.upstream_model,
        isStream,
        ...(mergedOverrides.path_override ? { pathOverride: mergedOverrides.path_override } : {}),
        extraQuery: mergedOverrides.query_params,
      });
    } catch (err) {
      throw new GatewayError(
        502,
        "provider_adapter_failed",
        err instanceof Error ? err.message : "Failed to render provider endpoint URL",
      );
    }
    bodyBytes = encoder.encode(stringifyExact(payload));
  } else {
    let wasmOut: Awaited<ReturnType<typeof invokeWasmProviderRequest>>;
    try {
      wasmOut = await invokeWasmProviderRequest({
        executor: wasmExecutor,
        plugin: resolved.plugin,
        config: resolved.config,
        ...(options.tenantId ? { tenantId: options.tenantId } : {}),
        ...(options.signal ? { signal: options.signal } : {}),
        ...(signingPublicKeyPem ? { signingPublicKeyPem } : {}),
        input: {
          operation,
          upstream_model: route.upstream_model,
          stream: isStream,
          client_protocol: isConversationalOperation(operation) ? operation : null,
          body: payload,
          headers: sanitizeProviderHeaders(mergedOverrides.extra_headers),
        },
      });
    } catch (err) {
      if (err instanceof WasmCapacityError) {
        throw new GatewayError(503, "provider_capacity_unavailable", err.message);
      }
      if (err instanceof WasmPluginFaultError) {
        throw new GatewayError(502, "provider_plugin_fault", err.message);
      }
      if (err instanceof GatewayError) throw err;
      throw new GatewayError(
        502,
        "provider_adapter_failed",
        err instanceof Error ? err.message : "WASM provider adapt_request failed",
      );
    }
    url = joinBase(channel.base_url, mergedOverrides.path_override ?? wasmOut.path);
    for (const [k, v] of Object.entries(wasmOut.query ?? {})) {
      url.searchParams.set(k, v);
    }
    for (const [k, v] of Object.entries(mergedOverrides.query_params)) {
      url.searchParams.set(k, v);
    }
    for (const [k, v] of Object.entries(wasmOut.headers ?? {})) {
      headers.set(k, v);
    }
    if (
      options.outputBound !== undefined &&
      upstreamProtocol &&
      resolved.config.base_dialect !== "passthrough"
    ) {
      capKnownOutput(wasmOut.body, upstreamProtocol, options.outputBound);
    }
    bodyBytes = encoder.encode(stringifyExact(wasmOut.body));
  }

  await applyAuthToRequest({
    auth: resolved.config.auth,
    secret,
    url,
    headers,
    body: bodyBytes,
    vertexTokens,
    signal,
  });

  return {
    url,
    headers,
    body: bodyBytes,
    upstreamProtocol,
    usageDialect,
    isBedrockStream,
    resolvedProvider: resolved,
    mergedOverrides,
    droppedFields,
  };
}

export function parseRetryAfterMs(header: string | null): number | undefined {
  if (!header) return undefined;
  const trimmed = header.trim();
  if (/^\d+(?:\.\d+)?$/.test(trimmed)) {
    return Math.max(0, Math.round(Number(trimmed) * 1000));
  }
  const ts = Date.parse(trimmed);
  if (Number.isFinite(ts)) {
    return Math.max(0, ts - Date.now());
  }
  return undefined;
}

export {
  isConversationalOperation,
  upstreamProtocolFor,
  mergeOverridesForResolved,
  mergeUsageWithAliases,
  mergeUsageSources,
  type MergedProviderOverrides,
  type PreparedUpstreamRequest,
} from "./upstream-payload.ts";
