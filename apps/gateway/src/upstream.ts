import { GatewayError } from "@gateway/contracts";
import { canonical, type Operation, type Protocol } from "@gateway/kernel";
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
import { validateMeteredRequest, type MeteredValidationOptions } from "./request-metering.ts";
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
  splitSseChunkText,
  buildTextDeltaSse,
  type SplitSseChunkResult,
} from "./upstream-stream.ts";

import {
  estimateTextChatInput,
  estimateTextGeminiInput,
  estimateTextInputForProtocol,
  estimateTextMessagesInput,
  estimateTextResponsesInput,
} from "./upstream-input-estimate.ts";

export {
  estimateTextChatInput,
  estimateTextGeminiInput,
  estimateTextResponsesInput,
  estimateTextMessagesInput,
};

const encoder = new TextEncoder();

type CachedProviderPlan = {
  snapshot: Pick<ConfigSnapshot, "plugins" | "providerPluginsByName">;
  channel: ChannelRecord;
  resolved: ResolvedProvider;
  overrides: MergedProviderOverrides;
  extraHeaderEntries: readonly [string, string][];
  staticEndpoints: Map<string, URL>;
  hasRequestPolicy: boolean;
  payloadPreservesMetering: boolean;
  authHeaderKey: string | null;
  authPrefix: string;
  lastSecret: string | undefined;
  lastSecretValid: boolean;
  lastAuthValue: string;
};

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
  const auth = resolved.config.auth;
  const authHeaderKey =
    auth.scheme === "bearer"
      ? "authorization"
      : auth.scheme === "header"
        ? auth.header_name.toLowerCase()
        : null;
  const authPrefix =
    auth.scheme === "bearer" ? "Bearer " : auth.scheme === "header" ? (auth.prefix ?? "") : "";
  const cfg = resolved.kind === "DECLARATIVE" ? resolved.config : null;
  const payloadPreservesMetering =
    cfg !== null &&
    overrides.request_transforms.length === 0 &&
    (cfg.model_field === undefined || cfg.model_field === null || cfg.model_field === "model") &&
    (cfg.stream_field === undefined || cfg.stream_field === null || cfg.stream_field === "stream");
  const plan: CachedProviderPlan = {
    snapshot,
    channel,
    resolved,
    overrides,
    extraHeaderEntries,
    staticEndpoints: new Map(),
    hasRequestPolicy: Boolean(route.request_policy && Object.keys(route.request_policy).length > 0),
    payloadPreservesMetering,
    authHeaderKey,
    authPrefix,
    lastSecret: undefined,
    lastSecretValid: true,
    lastAuthValue: "",
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

export function adaptiveRouteProfile(
  snapshot: ConfigSnapshot,
  channel: ChannelRecord,
  route: RouteRecord,
) {
  try {
    const { resolved, overrides, extraHeaderEntries } = getProviderPlan(snapshot, channel, route);
    const declarative = resolved.kind === "DECLARATIVE" ? resolved.config : null;
    const endpoint = declarative?.endpoints.chat ?? declarative?.endpoints.default;
    const compatibility = new Bun.CryptoHasher("sha256")
      .update(
        canonical({
          protocol: upstreamProtocolFor(resolved, "chat"),
          usageDialect: resolved.config.usage_dialect,
          streamFraming: resolved.config.stream_framing ?? "sse",
          includeStreamUsage: declarative?.include_stream_usage_option ?? false,
          wasmDigest: resolved.kind === "WASM" ? resolved.plugin.digest : null,
          modelField: declarative?.model_field === undefined ? "model" : declarative.model_field,
          streamField:
            declarative?.stream_field === undefined ? "stream" : declarative.stream_field,
          reasoningField: declarative?.reasoning_field ?? null,
          query: { ...endpoint?.query, ...overrides.query_params },
          streamQuery: { ...endpoint?.query, ...endpoint?.stream_query, ...overrides.query_params },
          headers: Object.fromEntries(extraHeaderEntries),
          requestTransforms: overrides.request_transforms,
          responseTransforms: overrides.response_transforms,
          usageAliases: overrides.usage_aliases,
          requestPolicy: route.request_policy,
          pipeline: route.plugin_pipeline
            .filter((step) => step.enabled !== false)
            .map((step) => ({
              ...step,
              digest: snapshot.plugins.get(step.plugin_id)?.digest ?? null,
            })),
        }),
      )
      .digest("hex");
    const revision = new Bun.CryptoHasher("sha256")
      .update(
        canonical({
          compatibility,
          baseUrl: channel.base_url,
          account: channel.account_id,
          endpoint: endpoint ?? null,
          providerVersion: resolved.version,
          provider: resolved.source === "builtin" ? resolved.config : resolved.plugin.digest,
          upstreamModel: route.upstream_model,
          channelAdapter: channel.adapter_config,
          routeAdapter: route.adapter_overrides,
        }),
      )
      .digest("hex");
    return { compatibility, revision };
  } catch {
    return undefined;
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
  alreadyValidated?: boolean;
  meteredValidation?: MeteredValidationOptions;
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

function assertProviderDestination(baseUrl: string, url: URL): void {
  if (url.origin !== new URL(baseUrl).origin) {
    throw new GatewayError(
      502,
      "invalid_provider_config",
      "Upstream request escaped its configured origin",
    );
  }
}

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

  if (plan.hasRequestPolicy && route.request_policy) {
    validateRequestPolicy(clientBody, route.request_policy);
  }

  const upstreamProtocol = upstreamProtocolFor(resolved, operation);
  const usageDialect = resolved.config.usage_dialect;
  const isBedrockStream = isStream && resolved.config.stream_framing === "bedrock_eventstream";

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
    assertProviderDestination(channel.base_url, url);
    staticEndpoints.set(endpointKey, url);
  }

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
  if (!(
    options.alreadyValidated &&
    plan.payloadPreservesMetering &&
    (upstreamProtocol === null || upstreamProtocol === operation)
  )) {
    validateMeteredRequest(payload, upstreamProtocol ?? operation, options.meteredValidation);
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
  if (plan.authHeaderKey && secret) {
    if (plan.lastSecret !== secret) {
      plan.lastSecret = secret;
      plan.lastSecretValid = isValidHttpHeaderValue(secret);
      plan.lastAuthValue = `${plan.authPrefix}${secret}`;
    }
    secretValid = plan.lastSecretValid;
    headers[plan.authHeaderKey] = plan.lastAuthValue;
  }

  const body = stringifyExact(payload);
  const estimatedInputTokens = options.estimateInputTokens
    ? estimateTextInputForProtocol(upstreamProtocol, usageDialect, payload, body)
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

  const plan = getProviderPlan(snapshot, channel, route);
  if (plan.hasRequestPolicy && route.request_policy) {
    validateRequestPolicy(clientBody, route.request_policy);
  }

  const { resolved, overrides: mergedOverrides } = plan;
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
    assertProviderDestination(channel.base_url, url);
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
    ...(options.canMutateClientBody ? { canMutateClientBody: true } : {}),
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
    if (!(
      options.alreadyValidated &&
      plan.payloadPreservesMetering &&
      (upstreamProtocol === null || upstreamProtocol === operation)
    )) {
      validateMeteredRequest(payload, upstreamProtocol ?? operation, options.meteredValidation);
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
    if (resolved.config.base_dialect !== "passthrough") {
      validateMeteredRequest(
        wasmOut.body,
        upstreamProtocol ?? operation,
        options.meteredValidation,
      );
    }
    bodyBytes = encoder.encode(stringifyExact(wasmOut.body));
  }

  assertProviderDestination(channel.base_url, url);
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

const RETRY_AFTER_HTTP_DATE =
  /^(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s\d{2}\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s\d{4}\s\d{2}:\d{2}:\d{2}\sGMT|(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s\d{2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{2}\s\d{2}:\d{2}:\d{2}\sGMT|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s(?:\d{2}|\s\d)\s\d{2}:\d{2}:\d{2}\s\d{4})$/i;

export function parseRetryAfterMs(header: string | null): number | undefined {
  if (!header) return undefined;
  const trimmed = header.trim();
  if (/^\d+(?:\.\d+)?$/.test(trimmed)) {
    const seconds = Number(trimmed);
    if (!Number.isFinite(seconds)) return undefined;
    return Math.max(0, Math.round(seconds * 1000));
  }
  if (!RETRY_AFTER_HTTP_DATE.test(trimmed)) return undefined;
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
  mergeStreamUsageAliases,
  mergeUsageSources,
  type MergedProviderOverrides,
  type PreparedUpstreamRequest,
} from "./upstream-payload.ts";
