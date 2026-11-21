import type { WebhookPluginConfig } from "@gateway/ledger";
import type { PluginDecision, PluginHookInput } from "./types.ts";

type WebhookCircuitState = {
  failures: number;
  openUntilMs: number;
};

export class WebhookPluginExecutor {
  private readonly circuits = new Map<string, WebhookCircuitState>();
  private readonly fetchImpl: typeof fetch;
  private readonly now: () => number;

  constructor(options: { fetchImpl?: typeof fetch; now?: () => number } = {}) {
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.now = options.now ?? (() => performance.now());
  }

  private handleFailure(
    pluginId: string,
    cfg: WebhookPluginConfig,
    reason: string,
  ): PluginDecision {
    const threshold = cfg.circuit_failure_threshold ?? 5;
    const openMs = cfg.circuit_open_ms ?? 15_000;
    const st = this.circuits.get(pluginId) ?? { failures: 0, openUntilMs: 0 };
    st.failures++;
    if (st.failures >= threshold) {
      st.openUntilMs = this.now() + openMs;
    }
    this.circuits.set(pluginId, st);
    if ((cfg.failure_policy ?? "FAIL_CLOSED") === "FAIL_OPEN") {
      return { action: "PASS" };
    }
    return {
      action: "BLOCK",
      rule_code: reason,
      http_status: 503,
      safe_message: "Content moderation service unavailable",
    };
  }

  async invoke(
    pluginId: string,
    cfg: WebhookPluginConfig,
    input: PluginHookInput,
    signal?: AbortSignal,
  ): Promise<PluginDecision> {
    const st = this.circuits.get(pluginId);
    if (st && st.openUntilMs > this.now()) {
      if ((cfg.failure_policy ?? "FAIL_CLOSED") === "FAIL_OPEN") {
        return { action: "PASS" };
      }
      return {
        action: "BLOCK",
        rule_code: "webhook_circuit_open",
        http_status: 503,
        safe_message: "Content moderation service temporarily unavailable",
      };
    }

    const timeoutMs = cfg.timeout_ms ?? 250;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error("webhook_timeout")), timeoutMs);
    const onAbort = () => controller.abort(signal?.reason);
    signal?.addEventListener("abort", onAbort, { once: true });

    try {
      const response = await this.fetchImpl(cfg.url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...cfg.headers,
        },
        body: JSON.stringify(input),
        signal: controller.signal,
      });
      if (!response.ok) {
        return this.handleFailure(pluginId, cfg, `webhook_http_${response.status}`);
      }
      const body = (await response.json()) as Record<string, unknown>;
      if (st) {
        st.failures = 0;
        st.openUntilMs = 0;
      }
      const action = body.action;
      if (action === "PASS") return { action: "PASS" };
      if (action === "TRANSFORM" && typeof body.text === "string") {
        return { action: "TRANSFORM", text: body.text };
      }
      if (action === "BLOCK") {
        return {
          action: "BLOCK",
          rule_code: typeof body.rule_code === "string" ? body.rule_code : "webhook_blocked",
          http_status: typeof body.http_status === "number" ? body.http_status : 400,
          safe_message:
            typeof body.safe_message === "string"
              ? body.safe_message
              : "Content blocked by moderation policy",
        };
      }
      return this.handleFailure(pluginId, cfg, "webhook_invalid_reply");
    } catch {
      return this.handleFailure(pluginId, cfg, "webhook_error");
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    }
  }
}
