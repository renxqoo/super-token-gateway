import { parseProviderJson, stringifyExact, trackExactChild } from "@gateway/protocol";
import type { PluginRecord, WasmProviderConfig } from "@gateway/ledger";
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
import { assertSafeRelativePath, sanitizeProviderHeaders } from "./guards.ts";

const MAX_WASM_STREAM_STATE_BYTES = 4096;

const encoder = new TextEncoder();

const decoder = new TextDecoder();

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
