import type { WebhookPluginConfig } from "@gateway/ledger";
import type { PluginDecision, PluginHookInput, PluginOutcomeKind } from "./types.ts";

type WebhookCircuitState = {
  failures: number;
  openUntilMs: number;
  probeInFlight: boolean;
  inFlight: number;
};

export type WebhookFetchFn = (url: string, init: RequestInit) => Promise<Response>;

export type WebhookPluginStats = {
  inFlight: number;
  failures: number;
  open: boolean;
  halfOpen: boolean;
};

export type WebhookExecutorStats = Record<string, WebhookPluginStats>;

const DEFAULT_WEBHOOK_TIMEOUT_MS = 200;
const DEFAULT_WEBHOOK_MAX_CONCURRENCY = 64;
const DEFAULT_WEBHOOK_MAX_RESPONSE_BYTES = 1_048_576;

async function readBoundedResponseText(response: Response, maxBytes: number): Promise<string> {
  const clHeader = response.headers.get("content-length");
  if (clHeader !== null && /^\d+$/.test(clHeader) && Number(clHeader) > maxBytes) {
    void response.body?.cancel().catch(() => undefined);
    throw new Error("webhook_response_too_large");
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
  if (chunks.length === 1) return new TextDecoder().decode(chunks[0]!);
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(merged);
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
      st = { failures: 0, openUntilMs: 0, probeInFlight: false, inFlight: 0 };
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
    outcomeKind: PluginOutcomeKind,
    reason: string,
  ): PluginDecision {
    const threshold = cfg.circuit_failure_threshold ?? 5;
    const openMs = cfg.circuit_open_ms ?? 15_000;
    if (isProbe) {
      st.probeInFlight = false;
      st.failures = Math.max(st.failures + 1, threshold);
      st.openUntilMs = this.now() + openMs;
    } else {
      st.failures++;
      if (st.failures >= threshold) {
        st.openUntilMs = this.now() + openMs;
      }
    }
    return this.failDecision(cfg, outcomeKind, reason, "Content moderation service unavailable");
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
    const now = this.now();
    let isProbe = false;

    if (st.openUntilMs > now) {
      return this.failDecision(
        cfg,
        "host_capacity",
        "webhook_circuit_open",
        "Content moderation service temporarily unavailable",
      );
    }
    if (st.openUntilMs > 0) {
      if (st.probeInFlight) {
        return this.failDecision(
          cfg,
          "host_capacity",
          "webhook_circuit_open",
          "Content moderation service temporarily unavailable",
        );
      }
      st.probeInFlight = true;
      isProbe = true;
    }

    const maxConcurrency = cfg.max_concurrency ?? DEFAULT_WEBHOOK_MAX_CONCURRENCY;
    if (st.inFlight >= maxConcurrency) {
      if (isProbe) st.probeInFlight = false;
      return this.failDecision(
        cfg,
        "host_capacity",
        "webhook_concurrency_limit",
        "Content moderation service capacity exceeded",
      );
    }

    st.inFlight++;
    const timeoutMs = cfg.timeout_ms ?? DEFAULT_WEBHOOK_TIMEOUT_MS;
    const maxResponseBytes = cfg.max_response_bytes ?? DEFAULT_WEBHOOK_MAX_RESPONSE_BYTES;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error("webhook_timeout")), timeoutMs);
    const onAbort = () => controller.abort(signal?.reason);
    signal?.addEventListener("abort", onAbort, { once: true });

    try {
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
      const response = await this.fetchImpl(cfg.url, {
        method: "POST",
        headers,
        body: JSON.stringify(input),
        signal: controller.signal,
      });
      signal?.throwIfAborted();
      if (!response.ok) {
        void response.body?.cancel().catch(() => undefined);
        const kind: PluginOutcomeKind =
          response.status >= 500 || response.status === 429 ? "host_capacity" : "plugin_fault";
        return this.recordFailure(st, cfg, isProbe, kind, `webhook_http_${response.status}`);
      }
      const rawText = await readBoundedResponseText(response, maxResponseBytes);
      signal?.throwIfAborted();
      const body = JSON.parse(rawText) as Record<string, unknown>;
      const action = body?.action;
      if (action === "PASS") {
        st.failures = 0;
        st.openUntilMs = 0;
        st.probeInFlight = false;
        return { action: "PASS", outcome_kind: "content_decision" };
      }
      if (action === "TRANSFORM" && typeof body.text === "string") {
        st.failures = 0;
        st.openUntilMs = 0;
        st.probeInFlight = false;
        return { action: "TRANSFORM", text: body.text, outcome_kind: "content_decision" };
      }
      if (action === "BLOCK") {
        st.failures = 0;
        st.openUntilMs = 0;
        st.probeInFlight = false;
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
      return this.recordFailure(st, cfg, isProbe, "plugin_fault", "webhook_invalid_reply");
    } catch (error) {
      signal?.throwIfAborted();
      const msg = error instanceof Error ? error.message : String(error);
      if (msg === "webhook_response_too_large") {
        return this.recordFailure(st, cfg, isProbe, "plugin_fault", "webhook_response_too_large");
      }
      if (error instanceof SyntaxError) {
        return this.recordFailure(st, cfg, isProbe, "plugin_fault", "webhook_invalid_reply");
      }
      const isTimeout =
        controller.signal.aborted &&
        controller.signal.reason instanceof Error &&
        controller.signal.reason.message === "webhook_timeout";
      return this.recordFailure(
        st,
        cfg,
        isProbe,
        "host_capacity",
        isTimeout ? "webhook_timeout" : "webhook_error",
      );
    } finally {
      st.inFlight = Math.max(0, st.inFlight - 1);
      if (isProbe && st.probeInFlight) st.probeInFlight = false;
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    }
  }
}
