import { GatewayError, optionalObject } from "@gateway/contracts";
import type { Operation, Protocol, Usage } from "@gateway/kernel";
import type {
  AdapterOverrides,
  AuthSchemeConfig,
  ChannelRecord,
  ConfigSnapshot,
  DeclarativeProviderConfig,
  RouteRecord,
  UsageAliasRule,
  UsageDialect,
} from "@gateway/ledger";
import {
  applyBodyTransforms,
  applyResponseTransforms,
  evaluateErrorRules,
  extractUsageAliases,
  invokeWasmProviderRequest,
  invokeWasmProviderResponse,
  mergeAdapterOverrides,
  renderEndpointUrl,
  resolvePinnedProviderPlugin,
  resolveProviderPlugin,
  sanitizeProviderHeaders,
  WasmCapacityError,
  WasmPluginFaultError,
  type MatchedProviderError,
  type ResolvedProvider,
  type WasmRuntimeExecutor,
} from "@gateway/plugin-engine";
import {
  clearExactProperty,
  copyExact,
  decodeRequest,
  decodeResponse,
  encodeRequest,
  encodeResponse,
  encodeSSE,
  extractUsage,
  stringifyExact,
  validateRequestPolicy,
  validateUsageObject,
  type ConversionMode,
  type SSEEvent,
} from "@gateway/protocol";
import { applyAuthToRequest, type VertexTokens } from "@gateway/runtime";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function isConversationalOperation(op: Operation): op is Protocol {
  return op === "chat" || op === "responses" || op === "messages" || op === "gemini";
}

export function upstreamProtocolFor(
  resolved: ResolvedProvider,
  clientOp: Operation,
): Protocol | null {
  if (!isConversationalOperation(clientOp)) return null;
  const dialect = resolved.config.base_dialect;
  if (dialect === "anthropic_messages") return "messages";
  if (dialect === "gemini") return "gemini";
  if (dialect === "openai_responses") return "responses";
  if (dialect === "openai_chat") {
    if (
      clientOp === "responses" &&
      resolved.kind === "DECLARATIVE" &&
      resolved.config.endpoints.responses
    ) {
      return "responses";
    }
    return "chat";
  }
  return null;
}

function joinBase(baseUrl: string, path: string): URL {
  const base = baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl;
  return new URL(`${base}${path}`);
}

export type MergedProviderOverrides = ReturnType<typeof mergeAdapterOverrides>;

export function mergeOverridesForResolved(
  resolved: ResolvedProvider,
  channelConfig?: AdapterOverrides,
  routeOverrides?: AdapterOverrides,
): MergedProviderOverrides {
  if (resolved.kind === "DECLARATIVE") {
    return mergeAdapterOverrides(resolved.config, channelConfig, routeOverrides);
  }
  const emptyBase: DeclarativeProviderConfig = {
    base_dialect: resolved.config.base_dialect,
    usage_dialect: resolved.config.usage_dialect,
    auth: resolved.config.auth,
    endpoints: {},
    ...(resolved.config.extra_headers ? { extra_headers: resolved.config.extra_headers } : {}),
  };
  return mergeAdapterOverrides(emptyBase, channelConfig, routeOverrides);
}

export function mergeUsageWithAliases(
  baseUsage: Usage | undefined,
  rawBody: Record<string, unknown>,
  aliasRules: readonly UsageAliasRule[],
): Usage | undefined {
  if (aliasRules.length === 0) return baseUsage;
  const aliasRaw = extractUsageAliases(rawBody, aliasRules);
  if (!aliasRaw) return baseUsage;
  return mergeUsageSources(baseUsage, validateUsageObject(aliasRaw));
}

export function mergeUsageSources(baseUsage?: Usage, aliasUsage?: Usage): Usage | undefined {
  if (!aliasUsage) return baseUsage;
  if (!baseUsage) return aliasUsage;
  const merged: Record<string, string> = { ...(baseUsage as Record<string, string>) };
  for (const [k, v] of Object.entries(aliasUsage)) {
    if (v === undefined) continue;
    if (merged[k] !== undefined && merged[k] !== v) {
      throw new GatewayError(502, "usage_alias_conflict", `Provider usage alias conflict on ${k}`);
    }
    merged[k] = v;
  }
  return validateUsageObject(merged);
}

export type PreparedUpstreamRequest = {
  url: URL;
  headers: Headers;
  body: Uint8Array;
  upstreamProtocol: Protocol | null;
  usageDialect: UsageDialect;
  isBedrockStream: boolean;
  resolvedProvider: ResolvedProvider;
  mergedOverrides: MergedProviderOverrides;
  droppedFields: string[];
};

async function encodeMultipartWithModel(
  sourceEntries: readonly [string, string | File][],
  upstreamModel: string,
): Promise<{ contentType: string; bytes: Uint8Array }> {
  const form = new FormData();
  let replaced = false;
  for (const [k, v] of sourceEntries) {
    if (k === "model") {
      form.append("model", upstreamModel);
      replaced = true;
    } else {
      form.append(k, v);
    }
  }
  if (!replaced) form.append("model", upstreamModel);
  const tmp = new Response(form);
  const contentType = tmp.headers.get("content-type") ?? "multipart/form-data";
  const bytes = new Uint8Array(await tmp.arrayBuffer());
  return { contentType, bytes };
}

function capKnownOutput(body: Record<string, unknown>, protocol: Protocol, bound: number): void {
  const cap = (value: unknown) =>
    typeof value === "number" && Number.isSafeInteger(value) && value > 0
      ? Math.min(value, bound)
      : bound;
  if (protocol === "chat") {
    const name =
      "max_tokens" in body && !("max_completion_tokens" in body)
        ? "max_tokens"
        : "max_completion_tokens";
    body[name] = cap(body[name]);
    clearExactProperty(body, name);
    if (name === "max_completion_tokens") delete body.max_tokens;
  } else if (protocol === "responses") {
    body.max_output_tokens = cap(body.max_output_tokens);
    clearExactProperty(body, "max_output_tokens");
  } else if (protocol === "messages") {
    body.max_tokens = cap(body.max_tokens);
    clearExactProperty(body, "max_tokens");
  } else {
    const config = optionalObject(body.generationConfig);
    body.generationConfig = copyExact(config, {
      ...config,
      maxOutputTokens: cap(config.maxOutputTokens),
    });
    clearExactProperty(body.generationConfig as object, "maxOutputTokens");
  }
}

function buildPreparedPayload(options: {
  resolved: ResolvedProvider;
  merged: MergedProviderOverrides;
  operation: Operation;
  upstreamProtocol: Protocol | null;
  upstreamModel: string;
  clientBody: Record<string, unknown>;
  isStream: boolean;
  outputBound?: number;
  conversionMode?: ConversionMode;
  onFieldDropped?: (source: Protocol, target: Protocol, field: string) => void;
  onFieldRejected?: (source: Protocol, target: Protocol, field: string) => void;
}): { payload: Record<string, unknown>; droppedFields: string[] } {
  const { resolved, merged, operation, upstreamProtocol, upstreamModel, clientBody, isStream } =
    options;
  const droppedFields: string[] = [];
  let payload: Record<string, unknown>;
  if (isConversationalOperation(operation) && upstreamProtocol) {
    if (operation === upstreamProtocol) {
      payload = copyExact(clientBody, { ...clientBody });
    } else {
      const mode: ConversionMode = options.conversionMode ?? "strict";
      const normalized = decodeRequest(clientBody, operation, {
        mode,
        ...(options.onFieldDropped
          ? {
              onFieldDropped: (field) =>
                options.onFieldDropped?.(operation, upstreamProtocol, field),
            }
          : {}),
        ...(options.onFieldRejected
          ? {
              onFieldRejected: (field) =>
                options.onFieldRejected?.(operation, upstreamProtocol, field),
            }
          : {}),
      });
      payload = encodeRequest(normalized, upstreamProtocol, upstreamModel, {
        mode,
        droppedFields,
        ...(options.onFieldDropped
          ? {
              onFieldDropped: (field) =>
                options.onFieldDropped?.(operation, upstreamProtocol, field),
            }
          : {}),
        ...(options.onFieldRejected
          ? {
              onFieldRejected: (field) =>
                options.onFieldRejected?.(operation, upstreamProtocol, field),
            }
          : {}),
      });
    }
  } else {
    payload = copyExact(clientBody, { ...clientBody });
  }

  const protocol = upstreamProtocol ?? (isConversationalOperation(operation) ? operation : null);
  if (options.outputBound !== undefined && protocol) {
    capKnownOutput(payload, protocol, options.outputBound);
  }
  if (resolved.kind === "DECLARATIVE") {
    const cfg = resolved.config;
    if (cfg.model_field === null) {
      delete payload.model;
    } else {
      const modelField = cfg.model_field ?? "model";
      payload[modelField] = upstreamModel;
      if (modelField !== "model") delete payload.model;
    }
    if (cfg.stream_field === null) {
      delete payload.stream;
    } else if (typeof cfg.stream_field === "string" && cfg.stream_field !== "stream") {
      payload[cfg.stream_field] = isStream;
      delete payload.stream;
    }
    if (cfg.include_stream_usage_option && isStream && upstreamProtocol === "chat") {
      payload.stream_options = { include_usage: true };
    }
    if (merged.request_transforms.length > 0) {
      payload = applyBodyTransforms(payload, merged.request_transforms);
    }
  } else if (merged.request_transforms.length > 0) {
    payload = applyBodyTransforms(payload, merged.request_transforms);
  }

  if (options.outputBound !== undefined && upstreamProtocol) {
    capKnownOutput(payload, upstreamProtocol, options.outputBound);
  }
  if (options.outputBound !== undefined && operation === "completions") {
    const requested = payload.max_tokens;
    payload.max_tokens =
      typeof requested === "number" && Number.isSafeInteger(requested) && requested > 0
        ? Math.min(requested, options.outputBound)
        : options.outputBound;
  }
  return { payload, droppedFields };
}

const providerPlans = new WeakMap<
  RouteRecord,
  {
    snapshot: Pick<ConfigSnapshot, "plugins" | "providerPluginsByName">;
    channel: ChannelRecord;
    resolved: ResolvedProvider;
    overrides: MergedProviderOverrides;
  }
>();

function getProviderPlan(
  snapshot: Pick<ConfigSnapshot, "plugins" | "providerPluginsByName">,
  channel: ChannelRecord,
  route: RouteRecord,
) {
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
  const plan = {
    snapshot,
    channel,
    resolved,
    overrides: mergeOverridesForResolved(resolved, channel.adapter_config, route.adapter_overrides),
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

export async function prepareUpstreamRequest(options: {
  channel: ChannelRecord;
  route: RouteRecord;
  snapshot: Pick<ConfigSnapshot, "plugins" | "providerPluginsByName">;
  secret: string;
  operation: Operation;
  clientBody: Record<string, unknown>;
  outputBound?: number;
  multipartEntries?: readonly [string, string | File][];
  isStream: boolean;
  vertexTokens: VertexTokens;
  wasmExecutor: WasmRuntimeExecutor | null;
  signingPublicKeyPem?: string;
  tenantId?: string;
  traceparent?: string;
  onFieldDropped?: (source: Protocol, target: Protocol, field: string) => void;
  onFieldRejected?: (source: Protocol, target: Protocol, field: string) => void;
  signal: AbortSignal;
}): Promise<PreparedUpstreamRequest> {
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

function applyRedactedAuthPreview(auth: AuthSchemeConfig, url: URL, headers: Headers): void {
  switch (auth.scheme) {
    case "none":
      return;
    case "bearer":
    case "gcp_vertex_oauth":
      headers.set("authorization", "Bearer <REDACTED>");
      return;
    case "header":
      headers.set(auth.header_name, `${auth.prefix ?? ""}<REDACTED>`);
      return;
    case "query":
      url.searchParams.set(auth.param_name, "<REDACTED>");
      return;
    case "azure_key":
      if (!url.searchParams.has("api-version")) {
        url.searchParams.set("api-version", auth.default_api_version ?? "2024-10-21");
      }
      headers.set("api-key", "<REDACTED>");
      return;
    case "aws_sigv4":
      headers.set("authorization", "AWS4-HMAC-SHA256 <REDACTED>");
      return;
    case "hmac_sha256":
      headers.set(auth.timestamp_header, "<TIMESTAMP>");
      headers.set(auth.signature_header, `${auth.signature_prefix ?? ""}<REDACTED>`);
      if (auth.key_id_header) headers.set(auth.key_id_header, "<REDACTED>");
      return;
  }
}

export async function previewProviderRequest(options: {
  provider: string;
  providerPluginId?: string | null;
  baseUrl: string;
  operation: Operation;
  upstreamModel: string;
  stream: boolean;
  clientBody: Record<string, unknown>;
  channelAdapterConfig?: AdapterOverrides;
  routeAdapterOverrides?: AdapterOverrides;
  mockUpstreamResponse?: {
    status: number;
    headers?: Record<string, string> | undefined;
    body: Record<string, unknown>;
  };
  snapshot: Pick<ConfigSnapshot, "plugins" | "providerPluginsByName">;
  wasmExecutor: WasmRuntimeExecutor | null;
  signingPublicKeyPem?: string;
}): Promise<{
  resolved_provider: {
    name: string;
    version: string;
    source: ResolvedProvider["source"];
    kind: ResolvedProvider["kind"];
    plugin_id: string | null;
  };
  prepared_request: {
    url: string;
    headers: Record<string, string>;
    body: Record<string, unknown>;
  };
  decoded_response?: {
    status: number;
    retry_kind: MatchedProviderError["retry_kind"] | null;
    error_message?: string;
    retry_after_ms?: number;
    body?: Record<string, unknown>;
    extracted_usage: Usage | null;
  };
}> {
  const resolved = resolveProviderPlugin(
    options.provider,
    options.providerPluginId,
    options.snapshot,
  );
  if (!resolved) {
    throw new GatewayError(
      404,
      "unknown_provider",
      `Provider plugin not found: ${options.provider}`,
    );
  }
  const merged = mergeOverridesForResolved(
    resolved,
    options.channelAdapterConfig,
    options.routeAdapterOverrides,
  );
  const upstreamProtocol = upstreamProtocolFor(resolved, options.operation);
  const isBedrockStream =
    options.stream && resolved.config.stream_framing === "bedrock_eventstream";

  const headers = new Headers();
  headers.set("content-type", "application/json");
  if (isBedrockStream) headers.set("accept", "application/vnd.amazon.eventstream");
  else if (options.stream) headers.set("accept", "text/event-stream");
  for (const [k, v] of Object.entries(sanitizeProviderHeaders(merged.extra_headers))) {
    headers.set(k, v);
  }

  const { payload } = buildPreparedPayload({
    resolved,
    merged,
    operation: options.operation,
    upstreamProtocol,
    upstreamModel: options.upstreamModel,
    clientBody: options.clientBody,
    isStream: options.stream,
  });

  let url: URL;
  let finalBody: Record<string, unknown>;
  if (resolved.kind === "DECLARATIVE") {
    url = renderEndpointUrl({
      baseUrl: options.baseUrl,
      config: resolved.config,
      operation: options.operation,
      upstreamModel: options.upstreamModel,
      isStream: options.stream,
      ...(merged.path_override ? { pathOverride: merged.path_override } : {}),
      extraQuery: merged.query_params,
    });
    finalBody = payload;
  } else {
    const wasmOut = await invokeWasmProviderRequest({
      executor: options.wasmExecutor,
      plugin: resolved.plugin,
      config: resolved.config,
      ...(options.signingPublicKeyPem ? { signingPublicKeyPem: options.signingPublicKeyPem } : {}),
      input: {
        operation: options.operation,
        upstream_model: options.upstreamModel,
        stream: options.stream,
        client_protocol: isConversationalOperation(options.operation) ? options.operation : null,
        body: payload,
        headers: sanitizeProviderHeaders(merged.extra_headers),
      },
    });
    url = joinBase(options.baseUrl, merged.path_override ?? wasmOut.path);
    for (const [k, v] of Object.entries(wasmOut.query ?? {})) {
      url.searchParams.set(k, v);
    }
    for (const [k, v] of Object.entries(merged.query_params)) {
      url.searchParams.set(k, v);
    }
    for (const [k, v] of Object.entries(wasmOut.headers ?? {})) {
      headers.set(k, v);
    }
    finalBody = wasmOut.body;
  }

  applyRedactedAuthPreview(resolved.config.auth, url, headers);
  const plainHeaders: Record<string, string> = {};
  headers.forEach((v, k) => {
    plainHeaders[k] = v;
  });

  const result: {
    resolved_provider: {
      name: string;
      version: string;
      source: ResolvedProvider["source"];
      kind: ResolvedProvider["kind"];
      plugin_id: string | null;
    };
    prepared_request: {
      url: string;
      headers: Record<string, string>;
      body: Record<string, unknown>;
    };
    decoded_response?: {
      status: number;
      retry_kind: MatchedProviderError["retry_kind"] | null;
      error_message?: string;
      retry_after_ms?: number;
      body?: Record<string, unknown>;
      extracted_usage: Usage | null;
    };
  } = {
    resolved_provider: {
      name: resolved.name,
      version: resolved.version,
      source: resolved.source,
      kind: resolved.kind,
      plugin_id: resolved.plugin_id,
    },
    prepared_request: {
      url: url.toString(),
      headers: plainHeaders,
      body: finalBody,
    },
  };

  if (options.mockUpstreamResponse) {
    const mockResp = options.mockUpstreamResponse;
    if (resolved.kind === "WASM" && resolved.config.hooks.includes("adapt_response")) {
      const wasmResp = await invokeWasmProviderResponse({
        executor: options.wasmExecutor,
        plugin: resolved.plugin,
        config: resolved.config,
        ...(options.signingPublicKeyPem
          ? { signingPublicKeyPem: options.signingPublicKeyPem }
          : {}),
        input: {
          operation: options.operation,
          upstream_model: options.upstreamModel,
          client_protocol: isConversationalOperation(options.operation) ? options.operation : null,
          status: mockResp.status,
          headers: mockResp.headers ?? {},
          body: mockResp.body,
        },
      });
      const extractedUsage = wasmResp.usage
        ? validateUsageObject(wasmResp.usage)
        : resolved.config.usage_dialect !== "custom"
          ? (extractUsage(mockResp.body, resolved.config.usage_dialect) ?? null)
          : null;
      result.decoded_response = {
        status: wasmResp.status ?? mockResp.status,
        retry_kind: wasmResp.retry_kind ?? null,
        ...(wasmResp.error_message ? { error_message: wasmResp.error_message } : {}),
        ...(wasmResp.retry_after_ms !== undefined
          ? { retry_after_ms: wasmResp.retry_after_ms }
          : {}),
        ...(wasmResp.body ? { body: wasmResp.body } : {}),
        extracted_usage: extractedUsage,
      };
    } else {
      const matched = evaluateErrorRules(mockResp.status, mockResp.body, merged.error_rules);
      if (matched) {
        result.decoded_response = {
          status: matched.http_status ?? mockResp.status,
          retry_kind: matched.retry_kind,
          ...(matched.message ? { error_message: matched.message } : {}),
          ...(matched.retry_after_ms !== undefined
            ? { retry_after_ms: matched.retry_after_ms }
            : {}),
          extracted_usage: null,
        };
      } else {
        const transformed = applyResponseTransforms(mockResp.body, merged.response_transforms);
        const baseUsage =
          resolved.config.usage_dialect === "custom"
            ? undefined
            : extractUsage(transformed, resolved.config.usage_dialect);
        const usage = mergeUsageWithAliases(baseUsage, transformed, merged.usage_aliases) ?? null;
        let clientBodyOut = transformed;
        if (
          isConversationalOperation(options.operation) &&
          upstreamProtocol &&
          upstreamProtocol !== options.operation
        ) {
          const completion = decodeResponse(
            transformed,
            upstreamProtocol,
            resolved.config.usage_dialect,
          );
          clientBodyOut = encodeResponse(
            completion,
            options.operation,
            typeof options.clientBody.model === "string"
              ? options.clientBody.model
              : options.upstreamModel,
          );
        }
        result.decoded_response = {
          status: mockResp.status,
          retry_kind: null,
          body: clientBodyOut,
          extracted_usage: usage,
        };
      }
    }
  }

  return result;
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

export class BedrockEventStreamDecoder {
  private buffer = new Uint8Array(0);

  feed(chunk: Uint8Array): SSEEvent[] {
    const merged = new Uint8Array(this.buffer.length + chunk.length);
    merged.set(this.buffer, 0);
    merged.set(chunk, this.buffer.length);
    let offset = 0;
    const events: SSEEvent[] = [];

    while (merged.length - offset >= 16) {
      const view = new DataView(merged.buffer, merged.byteOffset + offset, merged.length - offset);
      const totalLen = view.getUint32(0, false);
      const headersLen = view.getUint32(4, false);
      if (totalLen < 16 || totalLen > 4 * 1024 * 1024) {
        throw new GatewayError(502, "invalid_upstream", "Invalid Bedrock event stream frame");
      }
      if (merged.length - offset < totalLen) break;
      const payloadStart = offset + 12 + headersLen;
      const payloadEnd = offset + totalLen - 4;
      const payloadBytes = merged.subarray(payloadStart, payloadEnd);
      offset += totalLen;
      try {
        const wrapper = JSON.parse(decoder.decode(payloadBytes)) as { bytes?: string };
        if (typeof wrapper.bytes === "string") {
          const decoded = Buffer.from(wrapper.bytes, "base64").toString("utf8");
          const parsed = JSON.parse(decoded) as { type?: string };
          const eventType = typeof parsed.type === "string" ? parsed.type : "message";
          events.push({
            event: eventType,
            data: decoded,
            id: "",
            wire: `event: ${eventType}\ndata: ${decoded}\n\n`,
          });
        }
      } catch {
        throw new GatewayError(502, "invalid_upstream", "Malformed Bedrock event payload");
      }
    }

    this.buffer = merged.slice(offset);
    return events;
  }
}

export function extractRequestText(body: Record<string, unknown>, operation: Operation): string {
  const parts: string[] = [];
  if (operation === "chat" && Array.isArray(body.messages)) {
    for (const m of body.messages) {
      const msg = optionalObject(m);
      if (typeof msg.content === "string") parts.push(msg.content);
      else if (Array.isArray(msg.content)) {
        for (const p of msg.content) {
          const part = optionalObject(p);
          if (typeof part.text === "string") parts.push(part.text);
        }
      }
    }
  } else if (operation === "messages") {
    if (typeof body.system === "string") parts.push(body.system);
    else if (Array.isArray(body.system)) {
      for (const p of body.system) {
        const part = optionalObject(p);
        if (typeof part.text === "string") parts.push(part.text);
      }
    }
    if (Array.isArray(body.messages)) {
      for (const m of body.messages) {
        const msg = optionalObject(m);
        if (typeof msg.content === "string") parts.push(msg.content);
        else if (Array.isArray(msg.content)) {
          for (const p of msg.content) {
            const part = optionalObject(p);
            if (typeof part.text === "string") parts.push(part.text);
          }
        }
      }
    }
  } else if (operation === "responses") {
    if (typeof body.instructions === "string") parts.push(body.instructions);
    if (typeof body.input === "string") parts.push(body.input);
    else if (Array.isArray(body.input)) {
      for (const item of body.input) {
        const obj = optionalObject(item);
        if (typeof obj.content === "string") parts.push(obj.content);
        else if (Array.isArray(obj.content)) {
          for (const p of obj.content) {
            const part = optionalObject(p);
            if (typeof part.text === "string") parts.push(part.text);
          }
        }
      }
    }
  } else if (operation === "gemini") {
    const sys = optionalObject(body.systemInstruction);
    if (Array.isArray(sys.parts)) {
      for (const p of sys.parts) {
        const part = optionalObject(p);
        if (typeof part.text === "string") parts.push(part.text);
      }
    }
    if (Array.isArray(body.contents)) {
      for (const c of body.contents) {
        const content = optionalObject(c);
        if (Array.isArray(content.parts)) {
          for (const p of content.parts) {
            const part = optionalObject(p);
            if (typeof part.text === "string") parts.push(part.text);
          }
        }
      }
    }
  } else if (typeof body.prompt === "string") {
    parts.push(body.prompt);
  } else if (typeof body.input === "string") {
    parts.push(body.input);
  }
  return parts.join("\n");
}

export function replaceRequestText(
  body: Record<string, unknown>,
  operation: Operation,
  nextText: string,
): Record<string, unknown> {
  if ((operation === "chat" || operation === "messages") && Array.isArray(body.messages)) {
    const messages = body.messages.map((m) => ({ ...optionalObject(m) }));
    for (let i = messages.length - 1; i >= 0; i--) {
      const msg = messages[i]!;
      if (msg.role === "user") {
        if (typeof msg.content === "string") {
          msg.content = nextText;
          return copyExact(body, { ...body, messages });
        }
        if (Array.isArray(msg.content)) {
          const parts = msg.content.map((p) => ({ ...optionalObject(p) }));
          const idx = parts.findIndex((p) => typeof p.text === "string");
          if (idx >= 0) {
            parts[idx]!.text = nextText;
            msg.content = parts;
            return copyExact(body, { ...body, messages });
          }
        }
      }
    }
    return copyExact(body, { ...body, messages });
  }
  if (operation === "responses") {
    if (typeof body.input === "string") {
      return copyExact(body, { ...body, input: nextText });
    }
    if (Array.isArray(body.input) && body.input.length > 0) {
      const items = body.input.map((it) => ({ ...optionalObject(it) }));
      for (let i = items.length - 1; i >= 0; i--) {
        if (typeof items[i]!.content === "string") {
          items[i]!.content = nextText;
          return copyExact(body, { ...body, input: items });
        }
      }
    }
    return body;
  }
  if (operation === "gemini" && Array.isArray(body.contents) && body.contents.length > 0) {
    const contents = body.contents.map((c) => ({ ...optionalObject(c) }));
    const last = contents[contents.length - 1]!;
    last.parts = [{ text: nextText }];
    return copyExact(body, { ...body, contents });
  }
  if (typeof body.prompt === "string") {
    return copyExact(body, { ...body, prompt: nextText });
  }
  if (typeof body.input === "string") {
    return copyExact(body, { ...body, input: nextText });
  }
  return body;
}

export async function transformRequestTexts(
  body: Record<string, unknown>,
  operation: Operation,
  transform: (text: string) => Promise<{ blocked: boolean; text: string }>,
): Promise<{ blocked: boolean; body: Record<string, unknown> }> {
  if (operation === "chat" && Array.isArray(body.messages)) {
    let changed = false;
    const messages = body.messages.map((m) => ({ ...optionalObject(m) }));
    for (const msg of messages) {
      if (typeof msg.content === "string" && msg.content.length > 0) {
        const res = await transform(msg.content);
        if (res.blocked) return { blocked: true, body };
        if (res.text !== msg.content) {
          msg.content = res.text;
          changed = true;
        }
      } else if (Array.isArray(msg.content)) {
        const parts = msg.content.map((p) => ({ ...optionalObject(p) }));
        let partChanged = false;
        for (const part of parts) {
          if (typeof part.text === "string" && part.text.length > 0) {
            const res = await transform(part.text);
            if (res.blocked) return { blocked: true, body };
            if (res.text !== part.text) {
              part.text = res.text;
              partChanged = true;
            }
          }
        }
        if (partChanged) {
          msg.content = parts;
          changed = true;
        }
      }
    }
    return { blocked: false, body: changed ? copyExact(body, { ...body, messages }) : body };
  }

  if (operation === "messages") {
    let changed = false;
    let nextSystem = body.system;
    if (typeof body.system === "string" && body.system.length > 0) {
      const res = await transform(body.system);
      if (res.blocked) return { blocked: true, body };
      if (res.text !== body.system) {
        nextSystem = res.text;
        changed = true;
      }
    } else if (Array.isArray(body.system)) {
      const sysParts = body.system.map((p) => ({ ...optionalObject(p) }));
      let sysChanged = false;
      for (const part of sysParts) {
        if (typeof part.text === "string" && part.text.length > 0) {
          const res = await transform(part.text);
          if (res.blocked) return { blocked: true, body };
          if (res.text !== part.text) {
            part.text = res.text;
            sysChanged = true;
          }
        }
      }
      if (sysChanged) {
        nextSystem = sysParts;
        changed = true;
      }
    }
    const messages = Array.isArray(body.messages)
      ? body.messages.map((m) => ({ ...optionalObject(m) }))
      : null;
    if (messages) {
      for (const msg of messages) {
        if (typeof msg.content === "string" && msg.content.length > 0) {
          const res = await transform(msg.content);
          if (res.blocked) return { blocked: true, body };
          if (res.text !== msg.content) {
            msg.content = res.text;
            changed = true;
          }
        } else if (Array.isArray(msg.content)) {
          const parts = msg.content.map((p) => ({ ...optionalObject(p) }));
          let partChanged = false;
          for (const part of parts) {
            if (typeof part.text === "string" && part.text.length > 0) {
              const res = await transform(part.text);
              if (res.blocked) return { blocked: true, body };
              if (res.text !== part.text) {
                part.text = res.text;
                partChanged = true;
              }
            }
          }
          if (partChanged) {
            msg.content = parts;
            changed = true;
          }
        }
      }
    }
    if (!changed) return { blocked: false, body };
    return {
      blocked: false,
      body: copyExact(body, {
        ...body,
        ...(nextSystem !== body.system ? { system: nextSystem } : {}),
        ...(messages ? { messages } : {}),
      }),
    };
  }

  if (operation === "responses") {
    let changed = false;
    let nextInstructions = body.instructions;
    if (typeof body.instructions === "string" && body.instructions.length > 0) {
      const res = await transform(body.instructions);
      if (res.blocked) return { blocked: true, body };
      if (res.text !== body.instructions) {
        nextInstructions = res.text;
        changed = true;
      }
    }
    let nextInput = body.input;
    if (typeof body.input === "string" && body.input.length > 0) {
      const res = await transform(body.input);
      if (res.blocked) return { blocked: true, body };
      if (res.text !== body.input) {
        nextInput = res.text;
        changed = true;
      }
    } else if (Array.isArray(body.input)) {
      const items = body.input.map((it) => ({ ...optionalObject(it) }));
      let inputChanged = false;
      for (const item of items) {
        if (typeof item.content === "string" && item.content.length > 0) {
          const res = await transform(item.content);
          if (res.blocked) return { blocked: true, body };
          if (res.text !== item.content) {
            item.content = res.text;
            inputChanged = true;
          }
        } else if (Array.isArray(item.content)) {
          const parts = item.content.map((p) => ({ ...optionalObject(p) }));
          let partChanged = false;
          for (const part of parts) {
            if (typeof part.text === "string" && part.text.length > 0) {
              const res = await transform(part.text);
              if (res.blocked) return { blocked: true, body };
              if (res.text !== part.text) {
                part.text = res.text;
                partChanged = true;
              }
            }
          }
          if (partChanged) {
            item.content = parts;
            inputChanged = true;
          }
        }
      }
      if (inputChanged) {
        nextInput = items;
        changed = true;
      }
    }
    if (!changed) return { blocked: false, body };
    return {
      blocked: false,
      body: copyExact(body, {
        ...body,
        ...(nextInstructions !== body.instructions ? { instructions: nextInstructions } : {}),
        ...(nextInput !== body.input ? { input: nextInput } : {}),
      }),
    };
  }

  if (operation === "gemini") {
    let changed = false;
    let nextSystemInstruction = body.systemInstruction;
    const sysObj = optionalObject(body.systemInstruction);
    if (Array.isArray(sysObj.parts)) {
      const sysParts = sysObj.parts.map((p) => ({ ...optionalObject(p) }));
      let sysChanged = false;
      for (const part of sysParts) {
        if (typeof part.text === "string" && part.text.length > 0) {
          const res = await transform(part.text);
          if (res.blocked) return { blocked: true, body };
          if (res.text !== part.text) {
            part.text = res.text;
            sysChanged = true;
          }
        }
      }
      if (sysChanged) {
        nextSystemInstruction = copyExact(sysObj, { ...sysObj, parts: sysParts });
        changed = true;
      }
    }
    const contents = Array.isArray(body.contents)
      ? body.contents.map((c) => ({ ...optionalObject(c) }))
      : null;
    if (contents) {
      for (const c of contents) {
        if (Array.isArray(c.parts)) {
          const parts = c.parts.map((p) => ({ ...optionalObject(p) }));
          let partChanged = false;
          for (const part of parts) {
            if (typeof part.text === "string" && part.text.length > 0) {
              const res = await transform(part.text);
              if (res.blocked) return { blocked: true, body };
              if (res.text !== part.text) {
                part.text = res.text;
                partChanged = true;
              }
            }
          }
          if (partChanged) {
            c.parts = parts;
            changed = true;
          }
        }
      }
    }
    if (!changed) return { blocked: false, body };
    return {
      blocked: false,
      body: copyExact(body, {
        ...body,
        ...(nextSystemInstruction !== body.systemInstruction
          ? { systemInstruction: nextSystemInstruction }
          : {}),
        ...(contents ? { contents } : {}),
      }),
    };
  }

  const singleText = extractRequestText(body, operation);
  if (!singleText) return { blocked: false, body };
  const res = await transform(singleText);
  if (res.blocked) return { blocked: true, body };
  if (res.text === singleText) return { blocked: false, body };
  return { blocked: false, body: replaceRequestText(body, operation, res.text) };
}

export function extractResponseText(body: Record<string, unknown>, protocol: Protocol): string {
  if (protocol === "chat") {
    const choices = Array.isArray(body.choices) ? body.choices : [];
    const first = optionalObject(choices[0]);
    const msg = optionalObject(first.message);
    return typeof msg.content === "string" ? msg.content : "";
  }
  if (protocol === "messages") {
    const content = Array.isArray(body.content) ? body.content : [];
    return content
      .map((b) => {
        const block = optionalObject(b);
        return block.type === "text" && typeof block.text === "string" ? block.text : "";
      })
      .join("");
  }
  if (protocol === "responses") {
    const output = Array.isArray(body.output) ? body.output : [];
    const out: string[] = [];
    for (const item of output) {
      const obj = optionalObject(item);
      if (Array.isArray(obj.content)) {
        for (const p of obj.content) {
          const part = optionalObject(p);
          if (typeof part.text === "string") out.push(part.text);
        }
      }
    }
    return out.join("");
  }
  const candidates = Array.isArray(body.candidates) ? body.candidates : [];
  const content = optionalObject(optionalObject(candidates[0]).content);
  const parts = Array.isArray(content.parts) ? content.parts : [];
  return parts
    .map((p) => {
      const part = optionalObject(p);
      return typeof part.text === "string" && part.thought !== true ? part.text : "";
    })
    .join("");
}

export function replaceResponseText(
  body: Record<string, unknown>,
  protocol: Protocol,
  nextText: string,
): Record<string, unknown> {
  if (protocol === "chat" && Array.isArray(body.choices) && body.choices.length > 0) {
    const choice = { ...optionalObject(body.choices[0]) };
    const message = { ...optionalObject(choice.message), content: nextText };
    choice.message = message;
    return { ...body, choices: [choice, ...body.choices.slice(1)] };
  }
  if (protocol === "messages") {
    const existing = Array.isArray(body.content) ? body.content : [];
    const nonText = existing.filter((b) => optionalObject(b).type !== "text");
    return { ...body, content: [{ type: "text", text: nextText }, ...nonText] };
  }
  if (protocol === "responses") {
    const existing = Array.isArray(body.output) ? body.output : [];
    const nonMsg = existing.filter((it) => optionalObject(it).type !== "message");
    return {
      ...body,
      output: [
        {
          id: "msg_0",
          type: "message",
          role: "assistant",
          status: "completed",
          content: [{ type: "output_text", text: nextText, annotations: [] }],
        },
        ...nonMsg,
      ],
    };
  }
  if (protocol === "gemini" && Array.isArray(body.candidates) && body.candidates.length > 0) {
    const cand = { ...optionalObject(body.candidates[0]) };
    const existingParts = Array.isArray(optionalObject(cand.content).parts)
      ? (optionalObject(cand.content).parts as unknown[])
      : [];
    const nonTextParts = existingParts.filter(
      (p) => typeof optionalObject(p).text !== "string" || optionalObject(p).thought === true,
    );
    cand.content = { role: "model", parts: [...nonTextParts, { text: nextText }] };
    return { ...body, candidates: [cand, ...body.candidates.slice(1)] };
  }
  return body;
}

export function extractChunkTextFromEncodedSse(
  chunkBytes: Uint8Array,
  protocol: Protocol,
): string | null {
  const raw = decoder.decode(chunkBytes);
  const dataLines = raw
    .split("\n")
    .filter((l) => l.startsWith("data: "))
    .map((l) => l.slice(6));
  if (dataLines.length === 0) return null;
  const payload = dataLines.join("\n").trim();
  if (!payload || payload === "[DONE]") return null;
  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(payload) as Record<string, unknown>;
  } catch {
    return null;
  }
  if (protocol === "chat") {
    const choices = Array.isArray(obj.choices) ? obj.choices : [];
    const delta = optionalObject(optionalObject(choices[0]).delta);
    return typeof delta.content === "string" ? delta.content : null;
  }
  if (protocol === "messages") {
    const delta = optionalObject(obj.delta);
    return delta.type === "text_delta" && typeof delta.text === "string" ? delta.text : null;
  }
  if (protocol === "responses") {
    return obj.type === "response.output_text.delta" && typeof obj.delta === "string"
      ? obj.delta
      : null;
  }
  const cands = Array.isArray(obj.candidates) ? obj.candidates : [];
  const parts = Array.isArray(optionalObject(optionalObject(cands[0]).content).parts)
    ? (optionalObject(optionalObject(cands[0]).content).parts as unknown[])
    : [];
  const texts = parts
    .map((p) => {
      const part = optionalObject(p);
      return typeof part.text === "string" && part.thought !== true ? part.text : "";
    })
    .join("");
  return texts.length > 0 ? texts : null;
}

export function inspectAnthropicBlockEvent(
  chunkBytes: Uint8Array,
): { kind: "text_start" | "block_stop"; index: number } | null {
  const raw = decoder.decode(chunkBytes);
  const dataLines = raw
    .split("\n")
    .filter((l) => l.startsWith("data: "))
    .map((l) => l.slice(6));
  if (dataLines.length === 0) return null;
  const payload = dataLines.join("\n").trim();
  if (!payload || payload === "[DONE]") return null;
  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(payload) as Record<string, unknown>;
  } catch {
    return null;
  }
  const index = typeof obj.index === "number" && Number.isInteger(obj.index) ? obj.index : 0;
  if (obj.type === "content_block_start" && optionalObject(obj.content_block).type === "text") {
    return { kind: "text_start", index };
  }
  if (obj.type === "content_block_stop") {
    return { kind: "block_stop", index };
  }
  return null;
}

export function stripTextFromSseChunk(
  chunkBytes: Uint8Array,
  protocol: Protocol,
): Uint8Array | null {
  if (protocol === "messages" || protocol === "responses") return null;
  const raw = decoder.decode(chunkBytes);
  const dataLines = raw
    .split("\n")
    .filter((l) => l.startsWith("data: "))
    .map((l) => l.slice(6));
  if (dataLines.length === 0) return null;
  const payload = dataLines.join("\n").trim();
  if (!payload || payload === "[DONE]") return null;
  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(payload) as Record<string, unknown>;
  } catch {
    return null;
  }
  if (protocol === "chat") {
    const choices = Array.isArray(obj.choices) ? obj.choices : [];
    if (choices.length === 0) return null;
    const first = { ...optionalObject(choices[0]) };
    const delta = { ...optionalObject(first.delta) };
    delete delta.content;
    first.delta = delta;
    const hasMeta =
      Object.keys(delta).length > 0 ||
      (first.finish_reason !== null && first.finish_reason !== undefined) ||
      obj.usage !== undefined;
    if (!hasMeta) return null;
    return encodeSSE({ ...obj, choices: [first, ...choices.slice(1)] });
  }
  const cands = Array.isArray(obj.candidates) ? obj.candidates : [];
  if (cands.length === 0) return null;
  const first = { ...optionalObject(cands[0]) };
  const content = optionalObject(first.content);
  const parts = Array.isArray(content.parts) ? (content.parts as unknown[]) : [];
  const remainingParts = parts.filter(
    (p) => typeof optionalObject(p).text !== "string" || optionalObject(p).thought === true,
  );
  const hasMeta =
    remainingParts.length > 0 ||
    (first.finishReason !== null && first.finishReason !== undefined) ||
    obj.usageMetadata !== undefined;
  if (!hasMeta) return null;
  if (remainingParts.length > 0) {
    first.content = { ...content, parts: remainingParts };
  } else {
    delete first.content;
  }
  return encodeSSE({ ...obj, candidates: [first, ...cands.slice(1)] });
}

export function buildTextDeltaSse(
  protocol: Protocol,
  model: string,
  text: string,
  streamId: string,
  blockIndex = 0,
): Uint8Array {
  if (protocol === "chat") {
    return encodeSSE({
      id: streamId,
      object: "chat.completion.chunk",
      model,
      choices: [{ index: 0, delta: { content: text }, finish_reason: null }],
    });
  }
  if (protocol === "messages") {
    return encodeSSE(
      {
        type: "content_block_delta",
        index: blockIndex,
        delta: { type: "text_delta", text },
      },
      "content_block_delta",
    );
  }
  if (protocol === "responses") {
    return encodeSSE(
      {
        type: "response.output_text.delta",
        output_index: 0,
        item_id: "msg_0",
        content_index: 0,
        delta: text,
      },
      "response.output_text.delta",
    );
  }
  return encodeSSE({
    candidates: [{ index: 0, content: { role: "model", parts: [{ text }] } }],
  });
}
