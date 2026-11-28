import { GatewayError, optionalObject } from "@gateway/contracts";
import type { Operation, Protocol, Usage } from "@gateway/kernel";
import type {
  AdapterOverrides,
  DeclarativeProviderConfig,
  UsageAliasRule,
  UsageDialect,
} from "@gateway/ledger";
import {
  applyBodyTransforms,
  extractUsageAliases,
  mergeAdapterOverrides,
  type ResolvedProvider,
} from "@gateway/plugin-engine";
import {
  clearExactProperty,
  copyExact,
  decodeRequest,
  encodeRequest,
  validateUsageObject,
  type ConversionMode,
} from "@gateway/protocol";

export function isConversationalOperation(op: Operation): op is Protocol {
  return op === "chat" || op === "responses" || op === "messages" || op === "gemini";
}

export const multipartPaths = {
  transcription: "/v1/audio/transcriptions",
  translation: "/v1/audio/translations",
  image_edits: "/v1/images/edits",
} as const;

export function isMultipartOperation(
  operation: Operation,
): operation is keyof typeof multipartPaths {
  return Object.hasOwn(multipartPaths, operation);
}

export function declaresProviderOperation(
  config: DeclarativeProviderConfig,
  operation: Operation,
  pathOverride?: string | null,
): boolean {
  return Boolean(
    config.endpoints[operation] ||
    pathOverride ||
    (isConversationalOperation(operation) && config.endpoints.default),
  );
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

export function joinBase(baseUrl: string, path: string): URL {
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
    ...(resolved.config.error_diagnostics
      ? { error_diagnostics: resolved.config.error_diagnostics }
      : {}),
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

export function mergeStreamUsageAliases(
  previous: Usage | undefined,
  event: Record<string, unknown>,
  rules: readonly UsageAliasRule[],
  outputUsageBoundary: "replace" | "ignore" | undefined,
): Usage | undefined {
  if (outputUsageBoundary === "replace" && previous) {
    delete previous.output_tokens;
    delete previous.reasoning_tokens;
  }
  const usage = mergeUsageWithAliases(previous, event, rules) ?? previous;
  if (outputUsageBoundary === "ignore" && usage) {
    delete usage.output_tokens;
    delete usage.reasoning_tokens;
  }
  return usage;
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
  headers: Headers | Record<string, string>;
  headersValidated?: boolean;
  body: string | Uint8Array;
  estimatedInputTokens?: number;
  upstreamProtocol: Protocol | null;
  usageDialect: UsageDialect;
  isBedrockStream: boolean;
  resolvedProvider: ResolvedProvider;
  mergedOverrides: MergedProviderOverrides;
  droppedFields: string[];
};

export async function encodeMultipartWithModel(
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

export function capKnownOutput(
  body: Record<string, unknown>,
  protocol: Protocol,
  bound: number,
): void {
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
    const hasCamel =
      typeof body.generationConfig === "object" &&
      body.generationConfig !== null &&
      !Array.isArray(body.generationConfig);
    const hasSnake =
      typeof body.generation_config === "object" &&
      body.generation_config !== null &&
      !Array.isArray(body.generation_config);
    const usesSnake = !hasCamel && hasSnake;
    const configKey = usesSnake ? "generation_config" : "generationConfig";
    const config = optionalObject(body[configKey]);
    const fieldKey =
      usesSnake || ("max_output_tokens" in config && !("maxOutputTokens" in config))
        ? "max_output_tokens"
        : "maxOutputTokens";
    const otherFieldKey =
      fieldKey === "max_output_tokens" ? "maxOutputTokens" : "max_output_tokens";
    const nextConfig: Record<string, unknown> = {
      ...config,
      [fieldKey]: cap(config[fieldKey] ?? config[otherFieldKey]),
    };
    delete nextConfig[otherFieldKey];
    body[configKey] = copyExact(config, nextConfig);
    delete body[usesSnake ? "generationConfig" : "generation_config"];
    clearExactProperty(body[configKey] as object, fieldKey);
  }
}

export function buildPreparedPayload(options: {
  resolved: ResolvedProvider;
  merged: MergedProviderOverrides;
  operation: Operation;
  upstreamProtocol: Protocol | null;
  upstreamModel: string;
  clientBody: Record<string, unknown>;
  isStream: boolean;
  outputBound?: number;
  conversionMode?: ConversionMode;
  canMutateClientBody?: boolean;
  onFieldDropped?: (source: Protocol, target: Protocol, field: string) => void;
  onFieldRejected?: (source: Protocol, target: Protocol, field: string) => void;
}): { payload: Record<string, unknown>; droppedFields: string[] } {
  const { resolved, merged, operation, upstreamProtocol, upstreamModel, clientBody, isStream } =
    options;
  const droppedFields: string[] = [];
  let payload: Record<string, unknown>;
  if (isConversationalOperation(operation) && upstreamProtocol) {
    if (operation === upstreamProtocol) {
      payload = options.canMutateClientBody ? clientBody : copyExact(clientBody, { ...clientBody });
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
        ...(options.outputBound !== undefined ? { defaultMaxTokens: options.outputBound } : {}),
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
    payload = options.canMutateClientBody ? clientBody : copyExact(clientBody, { ...clientBody });
  }

  const hasTransforms = merged.request_transforms.length > 0;
  const protocol = upstreamProtocol ?? (isConversationalOperation(operation) ? operation : null);
  if (options.outputBound !== undefined && protocol && (hasTransforms || !upstreamProtocol)) {
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
      payload.stream_options = {
        ...optionalObject(payload.stream_options),
        include_usage: true,
      };
    }
    if (hasTransforms) {
      payload = applyBodyTransforms(payload, merged.request_transforms);
    }
  } else if (hasTransforms) {
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
