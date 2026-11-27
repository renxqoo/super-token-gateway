import type { WebhookPluginConfig } from "@gateway/ledger";
import type { PluginDecision, PluginHookInput, PluginOutcomeKind } from "./types.ts";

type WebhookCircuitState = {
  failures: number;
  openUntilMs: number;
  probeInFlight: boolean;
  probeOwner: string | undefined;
  generation: number;
  inFlight: number;
};

export type WebhookFastResponse = {
  fast: true;
  status: number;
  headers: Headers | Record<string, string>;
  body: Uint8Array;
};

export type WebhookFetchResult =
  Response | WebhookFastResponse | { fast: false; response: Response };

export type WebhookFetchFn = (url: string, init: RequestInit) => Promise<WebhookFetchResult>;

export type WebhookPluginStats = {
  inFlight: number;
  failures: number;
  open: boolean;
  halfOpen: boolean;
};

export type WebhookExecutorStats = Record<string, WebhookPluginStats>;

const DEFAULT_WEBHOOK_TIMEOUT_MS = 200;
const DEFAULT_WEBHOOK_MAX_CONCURRENCY = 256;
const DEFAULT_WEBHOOK_MAX_RESPONSE_BYTES = 1_048_576;
const utf8Decoder = new TextDecoder();
const sanitizedWebhookHeaders = new WeakMap<WebhookPluginConfig, Record<string, string>>();

function webhookHeadersFor(cfg: WebhookPluginConfig): Record<string, string> {
  const cached = sanitizedWebhookHeaders.get(cfg);
  if (cached) return cached;
  const headers: Record<string, string> = {};
  if (cfg.headers) {
    for (const [k, v] of Object.entries(cfg.headers)) {
      const lower = k.toLowerCase();
      if (
        lower === "host" ||
        lower === "content-length" ||
        lower === "transfer-encoding" ||
        lower === "connection" ||
        lower === "keep-alive" ||
        lower === "te" ||
        lower === "upgrade" ||
        lower === "proxy-connection" ||
        lower === "content-type"
      ) {
        continue;
      }
      headers[k] = v;
    }
  }
  headers["content-type"] = "application/json";
  sanitizedWebhookHeaders.set(cfg, headers);
  return headers;
}

async function readBoundedResponseText(response: Response, maxBytes: number): Promise<string> {
  const clHeader = response.headers.get("content-length");
  if (clHeader !== null && /^\d+$/.test(clHeader)) {
    const cl = Number(clHeader);
    if (cl > maxBytes) {
      void response.body?.cancel().catch(() => undefined);
      throw new Error("webhook_response_too_large");
    }
    if (!response.headers.has("content-encoding") && cl <= 262_144) {
      const bytes = await response.bytes();
      if (bytes.byteLength > maxBytes) throw new Error("webhook_response_too_large");
      return bytes.byteLength === 0 ? "" : utf8Decoder.decode(bytes);
    }
  }
  const reader = response.body?.getReader();
  if (!reader) return "";
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      if (!value || value.byteLength === 0) continue;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel().catch(() => undefined);
        throw new Error("webhook_response_too_large");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  if (chunks.length === 0) return "";
  if (chunks.length === 1) return utf8Decoder.decode(chunks[0]!);
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return utf8Decoder.decode(merged);
}

export class WebhookPluginExecutor {
  private readonly circuits = new Map<string, WebhookCircuitState>();
  private readonly fetchImpl: WebhookFetchFn;
  private readonly now: () => number;

  constructor(options: { fetchImpl?: WebhookFetchFn; now?: () => number } = {}) {
    this.fetchImpl = options.fetchImpl ?? ((url, init) => fetch(url, init));
    this.now = options.now ?? (() => performance.now());
  }

  private getState(pluginId: string): WebhookCircuitState {
    let st = this.circuits.get(pluginId);
    if (!st) {
      st = {
        failures: 0,
        openUntilMs: 0,
        probeInFlight: false,
        probeOwner: undefined,
        generation: 0,
        inFlight: 0,
      };
      this.circuits.set(pluginId, st);
    }
    return st;
  }

  private failDecision(
    cfg: WebhookPluginConfig,
    outcomeKind: PluginOutcomeKind,
    reason: string,
    safeMessage: string,
  ): PluginDecision {
    if ((cfg.failure_policy ?? "FAIL_CLOSED") === "FAIL_OPEN") {
      return {
        action: "PASS",
        outcome_kind: outcomeKind,
        fail_open_bypassed: true,
        fault_code: reason,
      };
    }
    return {
      action: "BLOCK",
      rule_code: reason,
      http_status: outcomeKind === "host_capacity" ? 503 : 500,
      safe_message: safeMessage,
      outcome_kind: outcomeKind,
    };
  }

  private recordFailure(
    st: WebhookCircuitState,
    cfg: WebhookPluginConfig,
    isProbe: boolean,
    generation: number,
    outcomeKind: PluginOutcomeKind,
    reason: string,
  ): PluginDecision {
    if (st.generation !== generation)
      return this.failDecision(cfg, outcomeKind, reason, "Content moderation service unavailable");
    const threshold = cfg.circuit_failure_threshold ?? 5;
    const openMs = cfg.circuit_open_ms ?? 15_000;
    st.failures++;
    if (isProbe || st.failures >= threshold) {
      st.probeInFlight = false;
      st.probeOwner = undefined;
      st.generation++;
      st.openUntilMs = this.now() + openMs;
    }
    return this.failDecision(cfg, outcomeKind, reason, "Content moderation service unavailable");
  }

  private unavailable(st: WebhookCircuitState, cfg: WebhookPluginConfig, requestId: string) {
    if (
      st.openUntilMs > this.now() ||
      (st.openUntilMs > 0 && (st.probeInFlight || (st.probeOwner && st.probeOwner !== requestId)))
    )
      return {
        code: "webhook_circuit_open",
        message: "Content moderation service temporarily unavailable",
      };
    if (st.inFlight >= (cfg.max_concurrency ?? DEFAULT_WEBHOOK_MAX_CONCURRENCY))
      return {
        code: "webhook_concurrency_limit",
        message: "Content moderation service capacity exceeded",
      };
    return undefined;
  }

  admitOutput(
    pluginId: string,
    cfg: WebhookPluginConfig,
    requestId: string,
  ): { ok: true; release?: () => void } | { ok: false; code: string; message: string } {
    const st = this.circuits.get(pluginId);
    if (!st || cfg.failure_policy === "FAIL_OPEN") return { ok: true };
    const unavailable = this.unavailable(st, cfg, requestId);
    if (unavailable) return { ok: false, ...unavailable };
    if (st.openUntilMs === 0) return { ok: true };
    st.probeOwner = requestId;
    return {
      ok: true,
      release() {
        if (st.probeOwner === requestId) st.probeOwner = undefined;
      },
    };
  }

  stats(): WebhookExecutorStats {
    const now = this.now();
    const out: WebhookExecutorStats = {};
    for (const [id, st] of this.circuits) {
      out[id] = {
        inFlight: st.inFlight,
        failures: st.failures,
        open: st.openUntilMs > now,
        halfOpen: st.probeInFlight,
      };
    }
    return out;
  }

  async invoke(
    pluginId: string,
    cfg: WebhookPluginConfig,
    input: PluginHookInput,
    signal?: AbortSignal,
  ): Promise<PluginDecision> {
    signal?.throwIfAborted();
    const st = this.getState(pluginId);
    const unavailable = this.unavailable(st, cfg, input.request_id);
    if (unavailable)
      return this.failDecision(cfg, "host_capacity", unavailable.code, unavailable.message);
    const isProbe = st.openUntilMs > 0;
    const generation = st.generation;
    if (isProbe) st.probeInFlight = true;
    st.inFlight++;
    const recordFailure = (kind: PluginOutcomeKind, code: string) =>
      this.recordFailure(st, cfg, isProbe, generation, kind, code);
    const recordSuccess = () => {
      if (st.generation !== generation) return;
      st.failures = 0;
      st.openUntilMs = 0;
      st.probeOwner = undefined;
      st.probeInFlight = false;
    };
    const timeoutMs = cfg.timeout_ms ?? DEFAULT_WEBHOOK_TIMEOUT_MS;
    const maxResponseBytes = cfg.max_response_bytes ?? DEFAULT_WEBHOOK_MAX_RESPONSE_BYTES;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error("webhook_timeout")), timeoutMs);
    const onAbort = () => controller.abort(signal?.reason);
    signal?.addEventListener("abort", onAbort, { once: true });

    try {
      const rawResult = await this.fetchImpl(cfg.url, {
        method: "POST",
        headers: webhookHeadersFor(cfg),
        body: JSON.stringify(input),
        signal: controller.signal,
      });
      signal?.throwIfAborted();
      let rawText: string;
      if (!(rawResult instanceof Response) && rawResult.fast) {
        if (rawResult.status < 200 || rawResult.status >= 300) {
          const kind: PluginOutcomeKind =
            rawResult.status >= 500 || rawResult.status === 429 ? "host_capacity" : "plugin_fault";
          return recordFailure(kind, `webhook_http_${rawResult.status}`);
        }
        if (rawResult.body.byteLength > maxResponseBytes) {
          throw new Error("webhook_response_too_large");
        }
        rawText = rawResult.body.byteLength === 0 ? "" : utf8Decoder.decode(rawResult.body);
      } else {
        const response = rawResult instanceof Response ? rawResult : rawResult.response;
        if (!response.ok) {
          void response.body?.cancel().catch(() => undefined);
          const kind: PluginOutcomeKind =
            response.status >= 500 || response.status === 429 ? "host_capacity" : "plugin_fault";
          return recordFailure(kind, `webhook_http_${response.status}`);
        }
        rawText = await readBoundedResponseText(response, maxResponseBytes);
      }
      signal?.throwIfAborted();
      const body = JSON.parse(rawText) as Record<string, unknown>;
      const action = body?.action;
      if (action === "PASS") {
        recordSuccess();
        return { action: "PASS", outcome_kind: "content_decision" };
      }
      if (action === "TRANSFORM" && typeof body.text === "string") {
        recordSuccess();
        return { action: "TRANSFORM", text: body.text, outcome_kind: "content_decision" };
      }
      if (action === "BLOCK") {
        recordSuccess();
        const safeStatus =
          typeof body.http_status === "number" &&
          Number.isInteger(body.http_status) &&
          body.http_status >= 400 &&
          body.http_status <= 599
            ? body.http_status
            : 400;
        return {
          action: "BLOCK",
          rule_code: typeof body.rule_code === "string" ? body.rule_code : "webhook_blocked",
          http_status: safeStatus,
          safe_message:
            typeof body.safe_message === "string"
              ? body.safe_message
              : "Content blocked by moderation policy",
          outcome_kind: "content_decision",
        };
      }
      return recordFailure("plugin_fault", "webhook_invalid_reply");
    } catch (error) {
      signal?.throwIfAborted();
      const msg = error instanceof Error ? error.message : String(error);
      if (msg === "webhook_response_too_large") {
        return recordFailure("plugin_fault", "webhook_response_too_large");
      }
      if (error instanceof SyntaxError) {
        return recordFailure("plugin_fault", "webhook_invalid_reply");
      }
      const isTimeout =
        controller.signal.aborted &&
        controller.signal.reason instanceof Error &&
        controller.signal.reason.message === "webhook_timeout";
      return recordFailure("host_capacity", isTimeout ? "webhook_timeout" : "webhook_error");
    } finally {
      st.inFlight = Math.max(0, st.inFlight - 1);
      if (isProbe && st.generation === generation) st.probeInFlight = false;
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    }
  }
}
