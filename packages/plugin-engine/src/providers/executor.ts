import { GatewayError } from "@gateway/contracts";
import {
  clearExactProperty,
  copyExact,
  numberLiteral,
  parseProviderJson,
  readUsageQuantity,
  setExactProperty,
  stringifyExact,
  trackExactChild,
} from "@gateway/protocol";
import type {
  AdapterOverrides,
  BodyTransformRule,
  ConfigSnapshot,
  DeclarativeProviderConfig,
  ErrorMatchRule,
  PluginRecord,
  ResponseTransformRule,
  UsageAliasRule,
  WasmProviderConfig,
} from "@gateway/ledger";
import type {
  ProviderHook,
  ProviderRetryKind,
  WasmProviderRequestInput,
  WasmProviderRequestOutput,
  WasmProviderResponseInput,
  WasmProviderResponseOutput,
  WasmProviderStreamInput,
  WasmProviderStreamOutput,
  WasmRuntimeExecutor,
} from "../types.ts";
import { verifyPluginIntegrity, WasmCapacityError, WasmPluginFaultError } from "../wasm-host.ts";
import { getBuiltinProviderReadonly, type BuiltinProviderPreset } from "./builtins.ts";

const SAFE_PATH_RE = /^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+){0,7}$/;
const FORBIDDEN_SEGMENTS = new Set(["__proto__", "constructor", "prototype"]);
const PROTECTED_HEADER_NAMES = new Set([
  "host",
  "content-length",
  "transfer-encoding",
  "connection",
  "authorization",
  "x-api-key",
  "api-key",
  "x-goog-api-key",
]);
const ALLOWED_TEMPLATE_VARS = new Set(["model", "encoded_model", "operation", "stream_action"]);
const MAX_WASM_STREAM_STATE_BYTES = 4096;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function assertSafeFieldPath(path: string): string[] {
  if (!SAFE_PATH_RE.test(path)) {
    throw new Error(`Invalid field path: ${path}`);
  }
  const parts = path.split(".");
  for (const seg of parts) {
    if (FORBIDDEN_SEGMENTS.has(seg)) {
      throw new Error(`Forbidden field path segment: ${seg}`);
    }
  }
  return parts;
}

export function assertSafeRelativePath(path: string): string {
  if (
    !path.startsWith("/") ||
    path.includes("://") ||
    path.includes("..") ||
    path.includes("\r") ||
    path.includes("\n") ||
    path.includes("\0") ||
    path.startsWith("//")
  ) {
    throw new Error(`Unsafe provider relative path: ${path}`);
  }
  return path;
}

export function assertSafeEndpointTemplate(tpl: string): void {
  assertSafeRelativePath(tpl.replace(/\$\{[a-z_]+\}/g, "x"));
  for (const match of tpl.matchAll(/\$\{([^}]+)\}/g)) {
    if (!ALLOWED_TEMPLATE_VARS.has(match[1] ?? "")) {
      throw new Error(`Unsupported endpoint template variable: ${match[1]}`);
    }
  }
}

export function isDeclarativeProviderConfig(
  cfg: DeclarativeProviderConfig | WasmProviderConfig,
): cfg is DeclarativeProviderConfig {
  return "endpoints" in cfg;
}

export type ResolvedProvider =
  | {
      source: "db_pinned" | "db_override";
      name: string;
      version: string;
      plugin_id: string;
      kind: "DECLARATIVE";
      config: DeclarativeProviderConfig;
      plugin: PluginRecord;
    }
  | {
      source: "db_pinned" | "db_override";
      name: string;
      version: string;
      plugin_id: string;
      kind: "WASM";
      config: WasmProviderConfig;
      plugin: PluginRecord;
    }
  | {
      source: "builtin";
      name: string;
      version: string;
      plugin_id: null;
      kind: "DECLARATIVE";
      config: DeclarativeProviderConfig;
      preset: BuiltinProviderPreset;
    };

export function resolvePinnedProviderPlugin(
  providerPluginId: string,
  snapshot: Pick<ConfigSnapshot, "plugins"> | null,
): ResolvedProvider | null {
  const pinned = snapshot?.plugins.get(providerPluginId);
  if (!pinned || !pinned.enabled || pinned.capability !== "PROVIDER" || !pinned.provider_config) {
    return null;
  }
  if (pinned.kind === "WASM") {
    return {
      source: "db_pinned",
      name: pinned.name,
      version: pinned.version,
      plugin_id: pinned.id,
      kind: "WASM",
      config: pinned.provider_config as WasmProviderConfig,
      plugin: pinned,
    };
  }
  return {
    source: "db_pinned",
    name: pinned.name,
    version: pinned.version,
    plugin_id: pinned.id,
    kind: "DECLARATIVE",
    config: pinned.provider_config as DeclarativeProviderConfig,
    plugin: pinned,
  };
}

export function resolveProviderPlugin(
  channelKind: string,
  providerPluginId: string | null | undefined,
  snapshot: Pick<ConfigSnapshot, "plugins" | "providerPluginsByName"> | null,
): ResolvedProvider | null {
  if (providerPluginId) {
    return resolvePinnedProviderPlugin(providerPluginId, snapshot);
  }

  const dbPlugin = snapshot?.providerPluginsByName.get(channelKind);
  if (
    dbPlugin &&
    dbPlugin.enabled &&
    dbPlugin.capability === "PROVIDER" &&
    dbPlugin.provider_config
  ) {
    if (dbPlugin.kind === "WASM") {
      return {
        source: "db_override",
        name: dbPlugin.name,
        version: dbPlugin.version,
        plugin_id: dbPlugin.id,
        kind: "WASM",
        config: dbPlugin.provider_config as WasmProviderConfig,
        plugin: dbPlugin,
      };
    }
    return {
      source: "db_override",
      name: dbPlugin.name,
      version: dbPlugin.version,
      plugin_id: dbPlugin.id,
      kind: "DECLARATIVE",
      config: dbPlugin.provider_config as DeclarativeProviderConfig,
      plugin: dbPlugin,
    };
  }

  const builtin = getBuiltinProviderReadonly(channelKind);
  if (!builtin) return null;
  return {
    source: "builtin",
    name: builtin.name,
    version: builtin.version,
    plugin_id: null,
    kind: "DECLARATIVE",
    config: builtin.provider_config,
    preset: builtin,
  };
}

export function getAtPath(obj: unknown, parts: readonly string[]): unknown {
  let cur: unknown = obj;
  for (const seg of parts) {
    if (cur === null || typeof cur !== "object" || Array.isArray(cur)) return undefined;
    if (!Object.hasOwn(cur, seg)) return undefined;
    cur = (cur as Record<string, unknown>)[seg];
  }
  return cur;
}

function setAtPath(
  root: Record<string, unknown>,
  parts: readonly string[],
  value: unknown,
  literal?: string,
): void {
  const ancestors: Record<string, unknown>[] = [];
  const carryExact = literal !== undefined || (value !== null && typeof value === "object");
  let cur: Record<string, unknown> = root;
  for (let i = 0; i < parts.length - 1; i++) {
    const seg = parts[i]!;
    if (carryExact) ancestors.push(cur);
    const existing = Object.hasOwn(cur, seg) ? cur[seg] : undefined;
    if (existing !== null && typeof existing === "object" && !Array.isArray(existing)) {
      const cloned = copyExact(existing, { ...(existing as Record<string, unknown>) });
      cur[seg] = cloned;
      cur = cloned;
    } else {
      const created: Record<string, unknown> = {};
      cur[seg] = created;
      cur = created;
    }
  }
  const key = parts[parts.length - 1]!;
  clearExactProperty(cur, key);
  cur[key] = value;
  if (literal !== undefined) setExactProperty(cur, key, literal);
  if (value !== null && typeof value === "object") trackExactChild(cur, value);
  for (const parent of ancestors.reverse()) {
    trackExactChild(parent, cur);
    cur = parent;
  }
}

function deleteAtPath(root: Record<string, unknown>, parts: readonly string[]): void {
  let cur: Record<string, unknown> = root;
  for (let i = 0; i < parts.length - 1; i++) {
    const seg = parts[i]!;
    if (!Object.hasOwn(cur, seg)) return;
    const next = cur[seg];
    if (next === null || typeof next !== "object" || Array.isArray(next)) return;
    const cloned = copyExact(next, { ...(next as Record<string, unknown>) });
    cur[seg] = cloned;
    cur = cloned;
  }
  const key = parts[parts.length - 1]!;
  clearExactProperty(cur, key);
  delete cur[key];
}

function moveAtPath(
  root: Record<string, unknown>,
  from: string,
  to: string,
  overwrite?: boolean,
): void {
  const fromParts = assertSafeFieldPath(from);
  const toParts = assertSafeFieldPath(to);
  if (from === to) return;
  const value = getAtPath(root, fromParts);
  if (value === undefined) return;
  if (overwrite || getAtPath(root, toParts) === undefined) {
    const parent = getAtPath(root, fromParts.slice(0, -1));
    const literal =
      typeof value === "number" && parent !== null && typeof parent === "object"
        ? numberLiteral(parent, fromParts.at(-1)!)
        : undefined;
    setAtPath(root, toParts, value, literal);
  }
  deleteAtPath(root, fromParts);
}

export function mergeAdapterOverrides(
  base: DeclarativeProviderConfig,
  channelConfig?: AdapterOverrides,
  routeOverrides?: AdapterOverrides,
): {
  extra_headers: Record<string, string>;
  query_params: Record<string, string>;
  path_override?: string;
  request_transforms: BodyTransformRule[];
  response_transforms: ResponseTransformRule[];
  error_rules: ErrorMatchRule[];
  usage_aliases: UsageAliasRule[];
} {
  const extra_headers: Record<string, string> = {
    ...base.extra_headers,
    ...channelConfig?.extra_headers,
    ...routeOverrides?.extra_headers,
  };
  const query_params: Record<string, string> = {
    ...channelConfig?.query_params,
    ...routeOverrides?.query_params,
  };
  const path_override = routeOverrides?.path_override ?? channelConfig?.path_override;
  const request_transforms: BodyTransformRule[] = [
    ...(base.request_transforms ?? []),
    ...(channelConfig?.request_transforms ?? []),
    ...(routeOverrides?.request_transforms ?? []),
  ];
  const response_transforms: ResponseTransformRule[] = [
    ...(base.response_transforms ?? []),
    ...(channelConfig?.response_transforms ?? []),
    ...(routeOverrides?.response_transforms ?? []),
  ];
  const error_rules: ErrorMatchRule[] = [
    ...(base.error_rules ?? []),
    ...(channelConfig?.error_rules ?? []),
    ...(routeOverrides?.error_rules ?? []),
  ];
  const usage_aliases: UsageAliasRule[] = [
    ...(base.usage_aliases ?? []),
    ...(channelConfig?.usage_aliases ?? []),
    ...(routeOverrides?.usage_aliases ?? []),
  ];
  return {
    extra_headers,
    query_params,
    ...(path_override ? { path_override } : {}),
    request_transforms,
    response_transforms,
    error_rules,
    usage_aliases,
  };
}

export function applyBodyTransforms(
  body: Record<string, unknown>,
  rules: readonly BodyTransformRule[],
): Record<string, unknown> {
  if (rules.length === 0) return body;
  const out = copyExact(body, { ...body });
  for (const rule of rules) {
    switch (rule.op) {
      case "rename":
      case "move_to": {
        moveAtPath(out, rule.from, rule.to, rule.overwrite);
        break;
      }
      case "drop": {
        deleteAtPath(out, assertSafeFieldPath(rule.path));
        break;
      }
      case "set_default": {
        const parts = assertSafeFieldPath(rule.path);
        if (getAtPath(out, parts) === undefined) {
          setAtPath(out, parts, structuredClone(rule.value));
        }
        break;
      }
      case "force_set": {
        const parts = assertSafeFieldPath(rule.path);
        setAtPath(out, parts, structuredClone(rule.value));
        break;
      }
      case "clamp": {
        const parts = assertSafeFieldPath(rule.path);
        const cur = getAtPath(out, parts);
        if (typeof cur === "number" && Number.isFinite(cur)) {
          let next = cur;
          if (rule.min !== undefined && next < rule.min) next = rule.min;
          if (rule.max !== undefined && next > rule.max) next = rule.max;
          if (next !== cur) setAtPath(out, parts, next);
        }
        break;
      }
      case "delete_if_equals": {
        const parts = assertSafeFieldPath(rule.path);
        if (getAtPath(out, parts) === rule.value) {
          deleteAtPath(out, parts);
        }
        break;
      }
    }
  }
  return out;
}

export function applyResponseTransforms(
  body: Record<string, unknown>,
  rules: readonly ResponseTransformRule[],
): Record<string, unknown> {
  if (rules.length === 0) return body;
  const out = copyExact(body, { ...body });
  for (const rule of rules) {
    if (rule.op === "rename") {
      moveAtPath(out, rule.from, rule.to, rule.overwrite);
    } else if (rule.op === "drop") {
      deleteAtPath(out, assertSafeFieldPath(rule.path));
    } else if (rule.op === "set_default") {
      const parts = assertSafeFieldPath(rule.path);
      if (getAtPath(out, parts) === undefined) {
        setAtPath(out, parts, structuredClone(rule.value));
      }
    }
  }
  return out;
}

export function renderEndpointUrl(options: {
  baseUrl: string;
  config: DeclarativeProviderConfig;
  operation: string;
  upstreamModel: string;
  isStream: boolean;
  pathOverride?: string;
  extraQuery?: Record<string, string>;
}): URL {
  const tpl =
    options.config.endpoints[options.operation as keyof DeclarativeProviderConfig["endpoints"]] ??
    options.config.endpoints.default;
  if (!tpl && !options.pathOverride) {
    throw new Error(`Provider missing endpoint template for operation ${options.operation}`);
  }
  const rawPath =
    options.pathOverride ?? (options.isStream && tpl?.stream_path ? tpl.stream_path : tpl!.path);
  let renderedPath = rawPath;
  if (rawPath.includes("${")) {
    const vars: Record<string, string> = {
      model: options.upstreamModel,
      encoded_model: encodeURIComponent(options.upstreamModel),
      operation: options.operation,
      stream_action: options.isStream ? "streamGenerateContent" : "generateContent",
    };
    renderedPath = rawPath.replace(/\$\{([a-z_]+)\}/g, (_, key: string) => {
      if (!(key in vars)) throw new Error(`Unsupported endpoint template variable: ${key}`);
      return vars[key]!;
    });
  }
  assertSafeRelativePath(renderedPath);
  const base = options.baseUrl.endsWith("/") ? options.baseUrl.slice(0, -1) : options.baseUrl;
  const url = new URL(`${base}${renderedPath}`);
  for (const [k, v] of Object.entries(tpl?.query ?? {})) {
    url.searchParams.set(k, v);
  }
  if (options.isStream) {
    for (const [k, v] of Object.entries(tpl?.stream_query ?? {})) {
      url.searchParams.set(k, v);
    }
  }
  for (const [k, v] of Object.entries(options.extraQuery ?? {})) {
    url.searchParams.set(k, v);
  }
  return url;
}

export type MatchedProviderError = {
  retry_kind: ProviderRetryKind;
  http_status?: number;
  message?: string;
  retry_after_ms?: number;
};

export function evaluateErrorRules(
  httpStatus: number,
  body: unknown,
  rules: readonly ErrorMatchRule[],
): MatchedProviderError | null {
  if (rules.length === 0 || body === null || typeof body !== "object") return null;
  for (const rule of rules) {
    const statuses = rule.when_http_status ?? [200];
    if (!statuses.includes(httpStatus)) continue;
    const val = getAtPath(body, assertSafeFieldPath(rule.path));
    if (val === undefined) continue;
    let matched = false;
    if (rule.equals !== undefined && val === rule.equals) matched = true;
    else if (rule.not_equals !== undefined && val !== rule.not_equals) matched = true;
    else if (rule.in !== undefined && rule.in.includes(val as string | number)) matched = true;
    else if (rule.regex !== undefined && typeof val === "string") {
      if (rule.regex.length <= 128 && new RegExp(rule.regex).test(val)) matched = true;
    }
    if (!matched) continue;
    const msgVal = rule.message_path
      ? getAtPath(body, assertSafeFieldPath(rule.message_path))
      : undefined;
    const raVal = rule.retry_after_path
      ? getAtPath(body, assertSafeFieldPath(rule.retry_after_path))
      : undefined;
    const retryAfterMs =
      typeof raVal === "number" && Number.isFinite(raVal) && raVal >= 0
        ? Math.round(raVal * 1000)
        : typeof raVal === "string" && /^\d+(?:\.\d+)?$/.test(raVal.trim())
          ? Math.round(Number(raVal.trim()) * 1000)
          : undefined;
    return {
      retry_kind: rule.retry_kind,
      ...(rule.http_status !== undefined ? { http_status: rule.http_status } : {}),
      ...(typeof msgVal === "string" && msgVal.length > 0 ? { message: msgVal } : {}),
      ...(retryAfterMs !== undefined ? { retry_after_ms: retryAfterMs } : {}),
    };
  }
  return null;
}

export function extractUsageAliases(
  body: unknown,
  rules: readonly UsageAliasRule[],
): Record<string, string> | null {
  if (rules.length === 0 || body === null || typeof body !== "object") return null;
  const out: Record<string, string> = {};
  let found = false;
  for (const rule of rules) {
    const parts = assertSafeFieldPath(rule.source_path);
    const key = parts.pop()!;
    const parent = getAtPath(body, parts);
    if (
      parent === null ||
      typeof parent !== "object" ||
      Array.isArray(parent) ||
      !Object.hasOwn(parent, key)
    )
      continue;
    const value = readUsageQuantity(parent as Record<string, unknown>, key, rule.target);
    if (value === undefined) continue;
    if (out[rule.target] !== undefined && out[rule.target] !== value) {
      throw new GatewayError(
        502,
        "usage_alias_conflict",
        `Provider usage alias conflict on ${rule.target}`,
      );
    }
    out[rule.target] = value;
    found = true;
  }
  return found ? out : null;
}

export function sanitizeProviderHeaders(
  headers: Record<string, string> | undefined,
): Record<string, string> {
  const out: Record<string, string> = {};
  if (!headers) return out;
  for (const [k, v] of Object.entries(headers)) {
    const lower = k.toLowerCase().trim();
    if (!lower || PROTECTED_HEADER_NAMES.has(lower)) continue;
    if (lower.includes("\r") || lower.includes("\n") || v.includes("\r") || v.includes("\n")) {
      continue;
    }
    out[k] = v;
  }
  return out;
}

export async function invokeWasmProviderRequest(options: {
  executor: WasmRuntimeExecutor | null;
  plugin: PluginRecord;
  config: WasmProviderConfig;
  input: WasmProviderRequestInput;
  signingPublicKeyPem?: string;
  signal?: AbortSignal;
  tenantId?: string;
}): Promise<WasmProviderRequestOutput> {
  const raw = await invokeWasmProviderRaw(options, "adapt_request", options.input);
  if (typeof raw.path !== "string") {
    throw new WasmPluginFaultError(
      "wasm_execution_failed",
      "WASM adapt_request reply missing path",
    );
  }
  let safePath: string;
  try {
    safePath = assertSafeRelativePath(raw.path);
  } catch (err) {
    throw new WasmPluginFaultError(
      "wasm_execution_failed",
      err instanceof Error ? err.message : "WASM adapt_request returned unsafe path",
    );
  }
  if (raw.body === null || typeof raw.body !== "object" || Array.isArray(raw.body)) {
    throw new WasmPluginFaultError(
      "wasm_execution_failed",
      "WASM adapt_request reply missing object body",
    );
  }
  const query: Record<string, string> = {};
  if (raw.query && typeof raw.query === "object" && !Array.isArray(raw.query)) {
    for (const [k, v] of Object.entries(raw.query as Record<string, unknown>)) {
      if (typeof v === "string") query[k] = v;
    }
  }
  const rawHeaders: Record<string, string> = {};
  if (raw.headers && typeof raw.headers === "object" && !Array.isArray(raw.headers)) {
    for (const [k, v] of Object.entries(raw.headers as Record<string, unknown>)) {
      if (typeof v === "string") rawHeaders[k] = v;
    }
  }
  return {
    path: safePath,
    ...(Object.keys(query).length > 0 ? { query } : {}),
    headers: sanitizeProviderHeaders(rawHeaders),
    body: raw.body as Record<string, unknown>,
  };
}

export async function invokeWasmProviderResponse(options: {
  executor: WasmRuntimeExecutor | null;
  plugin: PluginRecord;
  config: WasmProviderConfig;
  input: WasmProviderResponseInput;
  signingPublicKeyPem?: string;
  signal?: AbortSignal;
  tenantId?: string;
}): Promise<WasmProviderResponseOutput> {
  const raw = await invokeWasmProviderRaw(options, "adapt_response", options.input);
  const canExtractUsage = Boolean(options.config.grants?.includes("extract_usage"));
  if (raw.usage !== undefined && !canExtractUsage) {
    throw new WasmPluginFaultError(
      "wasm_execution_failed",
      "WASM provider plugin lacks extract_usage grant",
    );
  }
  return {
    ...(typeof raw.status === "number" &&
    Number.isInteger(raw.status) &&
    raw.status >= 100 &&
    raw.status <= 599
      ? { status: raw.status }
      : {}),
    ...(raw.body && typeof raw.body === "object" && !Array.isArray(raw.body)
      ? { body: raw.body as Record<string, unknown> }
      : {}),
    ...(raw.usage && typeof raw.usage === "object" && !Array.isArray(raw.usage)
      ? { usage: raw.usage as Record<string, unknown> }
      : {}),
    ...(isRetryKind(raw.retry_kind) ? { retry_kind: raw.retry_kind } : {}),
    ...(typeof raw.retry_after_ms === "number" ? { retry_after_ms: raw.retry_after_ms } : {}),
    ...(typeof raw.error_message === "string" ? { error_message: raw.error_message } : {}),
  };
}

export async function invokeWasmProviderStreamEvent(options: {
  executor: WasmRuntimeExecutor | null;
  plugin: PluginRecord;
  config: WasmProviderConfig;
  input: WasmProviderStreamInput;
  signingPublicKeyPem?: string;
  signal?: AbortSignal;
  tenantId?: string;
}): Promise<WasmProviderStreamOutput> {
  if (encoder.encode(options.input.state).byteLength > MAX_WASM_STREAM_STATE_BYTES) {
    throw new WasmPluginFaultError(
      "wasm_execution_failed",
      "WASM stream state exceeds 4096 bytes limit",
    );
  }
  const raw = await invokeWasmProviderRaw(options, "adapt_stream_event", options.input);
  const canExtractUsage = Boolean(options.config.grants?.includes("extract_usage"));
  if (raw.usage !== undefined && !canExtractUsage) {
    throw new WasmPluginFaultError(
      "wasm_execution_failed",
      "WASM provider plugin lacks extract_usage grant",
    );
  }
  if (
    typeof raw.state === "string" &&
    encoder.encode(raw.state).byteLength > MAX_WASM_STREAM_STATE_BYTES
  ) {
    throw new WasmPluginFaultError(
      "wasm_execution_failed",
      "WASM returned stream state exceeding 4096 bytes limit",
    );
  }
  const chunks = Array.isArray(raw.chunks)
    ? raw.chunks.filter(
        (c): c is Record<string, unknown> =>
          c !== null && typeof c === "object" && !Array.isArray(c),
      )
    : undefined;
  return {
    ...(chunks ? { chunks } : {}),
    ...(raw.usage && typeof raw.usage === "object" && !Array.isArray(raw.usage)
      ? { usage: raw.usage as Record<string, unknown> }
      : {}),
    ...(typeof raw.done === "boolean" ? { done: raw.done } : {}),
    ...(typeof raw.state === "string" ? { state: raw.state } : {}),
    ...(isRetryKind(raw.retry_kind) ? { retry_kind: raw.retry_kind } : {}),
    ...(typeof raw.retry_after_ms === "number" ? { retry_after_ms: raw.retry_after_ms } : {}),
    ...(typeof raw.error_message === "string" ? { error_message: raw.error_message } : {}),
  };
}

function isRetryKind(value: unknown): value is ProviderRetryKind {
  return (
    value === "retryable" ||
    value === "rate_limit" ||
    value === "auth_error" ||
    value === "client_error"
  );
}

type ProviderIntegrityResult =
  { ok: true } | { ok: false; reason: "missing_bytes" | "digest_mismatch" | "signature_invalid" };

const providerIntegrityByBuffer = new WeakMap<Uint8Array, Map<string, ProviderIntegrityResult>>();

function checkProviderIntegrity(
  plugin: PluginRecord,
  signingPublicKeyPem: string | undefined,
): ProviderIntegrityResult {
  if (!plugin.wasm_bytes) return { ok: false, reason: "missing_bytes" };
  const metaKey = `${plugin.id}:${plugin.digest}:${plugin.signature ?? ""}:${signingPublicKeyPem ?? ""}`;
  let perBuf = providerIntegrityByBuffer.get(plugin.wasm_bytes);
  const cached = perBuf?.get(metaKey);
  if (cached) return cached;
  const verified = verifyPluginIntegrity(plugin, signingPublicKeyPem);
  if (!perBuf) {
    perBuf = new Map();
    providerIntegrityByBuffer.set(plugin.wasm_bytes, perBuf);
  }
  if (perBuf.size >= 16) perBuf.clear();
  perBuf.set(metaKey, verified);
  return verified;
}

async function invokeWasmProviderRaw(
  options: {
    executor: WasmRuntimeExecutor | null;
    plugin: PluginRecord;
    config: WasmProviderConfig;
    signingPublicKeyPem?: string;
    signal?: AbortSignal;
    tenantId?: string;
  },
  hook: ProviderHook,
  payload: WasmProviderRequestInput | WasmProviderResponseInput | WasmProviderStreamInput,
): Promise<Record<string, unknown>> {
  const { executor, plugin, config, signingPublicKeyPem, signal, tenantId } = options;
  signal?.throwIfAborted();
  const integrity = checkProviderIntegrity(plugin, signingPublicKeyPem);
  if (!integrity.ok) {
    throw new WasmPluginFaultError(
      "wasm_execution_failed",
      integrity.reason === "digest_mismatch"
        ? "Provider WASM plugin digest verification failed"
        : "Provider WASM plugin signature verification failed",
    );
  }
  if (!executor || !plugin.wasm_bytes) {
    throw new WasmCapacityError("wasm_queue_full", "WASM runtime unavailable for provider plugin");
  }
  if (!config.hooks.includes(hook)) {
    throw new WasmPluginFaultError(
      "wasm_execution_failed",
      `Provider WASM plugin does not declare hook ${hook}`,
    );
  }
  executor.compile(plugin.id, plugin.digest, plugin.wasm_bytes);
  const input =
    "body" in payload && payload.body && typeof payload.body === "object"
      ? trackExactChild(payload, payload.body)
      : payload;
  const inputBytes = encoder.encode(stringifyExact(input));
  const rawOut = await executor.invoke(plugin.id, hook, inputBytes, {
    fuelBudget: config.fuel_budget ?? 5_000_000,
    maxMemoryBytes: config.max_memory_bytes ?? 16 * 1024 * 1024,
    priority: hook === "adapt_request" ? "ingress" : "egress",
    ...(tenantId ? { tenantId } : {}),
    ...(signal ? { signal } : {}),
  });
  signal?.throwIfAborted();
  let parsed: unknown;
  try {
    parsed = parseProviderJson(decoder.decode(rawOut));
  } catch (err) {
    throw new WasmPluginFaultError(
      "wasm_execution_failed",
      `Provider WASM hook ${hook} returned invalid JSON: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new WasmPluginFaultError(
      "wasm_execution_failed",
      `Provider WASM hook ${hook} returned non-object JSON`,
    );
  }
  return parsed as Record<string, unknown>;
}
