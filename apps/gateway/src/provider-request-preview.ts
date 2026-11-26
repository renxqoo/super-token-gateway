import { GatewayError } from "@gateway/contracts";
import type { Operation, Usage } from "@gateway/kernel";
import type { AdapterOverrides, AuthSchemeConfig, ConfigSnapshot } from "@gateway/ledger";
import {
  applyResponseTransforms,
  evaluateErrorRules,
  invokeWasmProviderRequest,
  invokeWasmProviderResponse,
  renderEndpointUrl,
  resolveProviderPlugin,
  sanitizeProviderHeaders,
  type MatchedProviderError,
  type ResolvedProvider,
  type WasmRuntimeExecutor,
} from "@gateway/plugin-engine";
import {
  decodeResponse,
  encodeResponse,
  extractUsage,
  validateUsageObject,
} from "@gateway/protocol";
import {
  mergeOverridesForResolved,
  upstreamProtocolFor,
  buildPreparedPayload,
  isConversationalOperation,
  joinBase,
  mergeUsageWithAliases,
} from "./upstream-payload.ts";

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
