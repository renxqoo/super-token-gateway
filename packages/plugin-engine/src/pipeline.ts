import type {
  ChargePolicy,
  PluginAuditEntry,
  PluginPipelineStep,
  PluginRecord,
  WasmPluginManifest,
} from "@gateway/ledger";
import type {
  PipelineExecutionResult,
  PluginDecision,
  PluginHook,
  PluginHookInput,
  WasmRuntimeExecutor,
} from "./types.ts";
import { verifyPluginSignature } from "./wasm-host.ts";
import { WebhookPluginExecutor } from "./webhook.ts";

const DEFAULT_MAX_WINDOW_BYTES = 4096;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

export type PluginPipelineOptions = {
  wasmExecutor?: WasmRuntimeExecutor | null;
  webhookExecutor?: WebhookPluginExecutor;
  signingPublicKeyPem?: string;
};

type ResolvedStep = {
  plugin: PluginRecord;
  chargePolicy: ChargePolicy;
};

export class PluginPipelineEngine {
  private readonly wasm: WasmRuntimeExecutor | null;
  private readonly webhook: WebhookPluginExecutor;
  private readonly signingPublicKeyPem: string | undefined;

  constructor(options: PluginPipelineOptions = {}) {
    this.wasm = options.wasmExecutor ?? null;
    this.webhook = options.webhookExecutor ?? new WebhookPluginExecutor();
    this.signingPublicKeyPem = options.signingPublicKeyPem;
  }

  private resolveSteps(
    steps: readonly PluginPipelineStep[],
    plugins: ReadonlyMap<string, PluginRecord>,
  ): ResolvedStep[] {
    const out: ResolvedStep[] = [];
    const sorted = [...steps]
      .filter((s) => s.enabled !== false)
      .sort((a, b) => a.priority - b.priority);
    for (const step of sorted) {
      const plugin = plugins.get(step.plugin_id);
      if (!plugin || !plugin.enabled || plugin.capability !== "CONTENT") continue;
      const manifestPolicy =
        plugin.kind === "WEBHOOK"
          ? plugin.webhook_config?.charge_policy
          : (plugin.manifest as Partial<WasmPluginManifest>).charge_policy;
      out.push({
        plugin,
        chargePolicy: step.charge_policy ?? manifestPolicy ?? "CHARGE_CUSTOMER",
      });
    }
    return out;
  }

  private async invokeSingle(
    step: ResolvedStep,
    input: PluginHookInput,
    signal?: AbortSignal,
  ): Promise<{ decision: PluginDecision; latencyUs: number }> {
    const started = performance.now();
    const { plugin } = step;
    if (plugin.kind === "WEBHOOK") {
      const whManifest = plugin.manifest as Partial<WasmPluginManifest>;
      if (
        Array.isArray(whManifest.hooks) &&
        whManifest.hooks.length > 0 &&
        !whManifest.hooks.includes(input.hook)
      ) {
        return {
          decision: { action: "PASS" },
          latencyUs: Math.round((performance.now() - started) * 1000),
        };
      }
      if (!plugin.webhook_config) {
        return {
          decision: { action: "PASS" },
          latencyUs: Math.round((performance.now() - started) * 1000),
        };
      }
      const decision = await this.webhook.invoke(plugin.id, plugin.webhook_config, input, signal);
      return { decision, latencyUs: Math.round((performance.now() - started) * 1000) };
    }

    const manifest = plugin.manifest as Partial<WasmPluginManifest>;
    if (!verifyPluginSignature(plugin.digest, plugin.signature, this.signingPublicKeyPem)) {
      return {
        decision: {
          action: "BLOCK",
          rule_code: "plugin_signature_invalid",
          http_status: 500,
          safe_message: "Plugin integrity verification failed",
        },
        latencyUs: Math.round((performance.now() - started) * 1000),
      };
    }

    if (Array.isArray(manifest.hooks) && !manifest.hooks.includes(input.hook)) {
      return {
        decision: { action: "PASS" },
        latencyUs: Math.round((performance.now() - started) * 1000),
      };
    }

    const failPolicy = manifest.failure_policy ?? "FAIL_CLOSED";
    if (!this.wasm || !plugin.wasm_bytes) {
      const latencyUs = Math.round((performance.now() - started) * 1000);
      if (failPolicy === "FAIL_OPEN") return { decision: { action: "PASS" }, latencyUs };
      return {
        decision: {
          action: "BLOCK",
          rule_code: "wasm_runtime_unavailable",
          http_status: 503,
          safe_message: "Plugin runtime unavailable",
        },
        latencyUs,
      };
    }

    try {
      this.wasm.compile(plugin.id, plugin.digest, plugin.wasm_bytes);
      const payloadBytes = encoder.encode(JSON.stringify(input));
      const rawOut = await this.wasm.invoke(plugin.id, input.hook, payloadBytes, {
        fuelBudget: manifest.fuel_budget ?? 2_000_000,
        maxMemoryBytes: manifest.max_memory_bytes ?? 32 * 1024 * 1024,
      });
      const parsed = JSON.parse(decoder.decode(rawOut)) as Record<string, unknown>;
      const latencyUs = Math.round((performance.now() - started) * 1000);
      if (parsed.action === "PASS") return { decision: { action: "PASS" }, latencyUs };
      if (parsed.action === "TRANSFORM" && typeof parsed.text === "string") {
        return { decision: { action: "TRANSFORM", text: parsed.text }, latencyUs };
      }
      if (parsed.action === "BLOCK") {
        return {
          decision: {
            action: "BLOCK",
            rule_code: typeof parsed.rule_code === "string" ? parsed.rule_code : "wasm_blocked",
            http_status: typeof parsed.http_status === "number" ? parsed.http_status : 400,
            safe_message:
              typeof parsed.safe_message === "string"
                ? parsed.safe_message
                : "Content blocked by plugin policy",
          },
          latencyUs,
        };
      }
      throw new Error("Invalid WASM plugin reply");
    } catch {
      const latencyUs = Math.round((performance.now() - started) * 1000);
      if (failPolicy === "FAIL_OPEN") return { decision: { action: "PASS" }, latencyUs };
      return {
        decision: {
          action: "BLOCK",
          rule_code: "wasm_execution_failed",
          http_status: 500,
          safe_message: "Plugin execution failed",
        },
        latencyUs,
      };
    }
  }

  async runUnary(
    hook: PluginHook,
    steps: readonly PluginPipelineStep[],
    plugins: ReadonlyMap<string, PluginRecord>,
    meta: { request_id: string; project_id: string; model: string; operation: string },
    text: string,
    signal?: AbortSignal,
  ): Promise<PipelineExecutionResult> {
    const resolved = this.resolveSteps(steps, plugins);
    let currentText = text;
    const audit: PluginAuditEntry[] = [];
    for (const step of resolved) {
      const { decision, latencyUs } = await this.invokeSingle(
        step,
        {
          hook,
          request_id: meta.request_id,
          project_id: meta.project_id,
          model: meta.model,
          operation: meta.operation,
          text: currentText,
          final: true,
        },
        signal,
      );
      audit.push({
        plugin_id: step.plugin.id,
        version: step.plugin.version,
        hook,
        action: decision.action,
        ...(decision.action === "BLOCK" ? { rule_code: decision.rule_code } : {}),
        latency_us: latencyUs,
      });
      if (decision.action === "TRANSFORM") {
        currentText = decision.text;
      } else if (decision.action === "BLOCK") {
        return {
          blocked: true,
          text: "",
          rule_code: decision.rule_code,
          http_status: decision.http_status ?? 400,
          safe_message: decision.safe_message ?? "Content blocked",
          charge_policy: step.chargePolicy,
          audit,
        };
      }
    }
    return {
      blocked: false,
      text: currentText,
      charge_policy: "CHARGE_CUSTOMER",
      audit,
    };
  }

  createStreamSession(
    steps: readonly PluginPipelineStep[],
    plugins: ReadonlyMap<string, PluginRecord>,
    meta: { request_id: string; project_id: string; model: string; operation: string },
    onAbortUpstream: () => void,
    signal?: AbortSignal,
  ) {
    const resolved = this.resolveSteps(steps, plugins);
    const audit: PluginAuditEntry[] = [];
    const auditByAction = new Map<string, PluginAuditEntry>();
    let blockedState: {
      rule_code: string;
      http_status: number;
      safe_message: string;
      charge_policy: ChargePolicy;
    } | null = null;

    let holdBuffer = "";
    let lastHoldAt = performance.now();
    let cutoffBuffer = "";
    let lastCutoffAt = performance.now();
    let pendingCutoff: Promise<string | null> | undefined;

    const hasHoldWebhook = resolved.some(
      (s) =>
        s.plugin.kind === "WEBHOOK" &&
        (s.plugin.webhook_config?.stream_mode ?? "HOLD_UNTIL_APPROVED") === "HOLD_UNTIL_APPROVED",
    );
    const hasCutoffWebhook = resolved.some(
      (s) =>
        s.plugin.kind === "WEBHOOK" && s.plugin.webhook_config?.stream_mode === "STREAM_AND_CUTOFF",
    );
    const maxWindowBytes = Math.min(
      DEFAULT_MAX_WINDOW_BYTES,
      Math.max(
        0,
        ...resolved.map((s) =>
          s.plugin.kind === "WASM"
            ? ((s.plugin.manifest as Partial<WasmPluginManifest>).window_bytes ?? 0)
            : (s.plugin.webhook_config?.max_buffer_bytes ?? 0),
        ),
      ),
    );
    const webhookWindowChars = Math.min(
      ...resolved
        .filter((s) => s.plugin.kind === "WEBHOOK")
        .map((s) => s.plugin.webhook_config?.window_chars ?? 256),
      256,
    );
    const webhookWindowMs = Math.min(
      ...resolved
        .filter((s) => s.plugin.kind === "WEBHOOK")
        .map((s) => s.plugin.webhook_config?.window_ms ?? 300),
      300,
    );
    let wasmTail = "";

    const runStepList = async (
      stepList: ResolvedStep[],
      hook: PluginHook,
      text: string,
      isFinal: boolean,
    ): Promise<string | null> => {
      let current = text;
      for (const step of stepList) {
        const { decision, latencyUs } = await this.invokeSingle(
          step,
          {
            hook,
            request_id: meta.request_id,
            project_id: meta.project_id,
            model: meta.model,
            operation: meta.operation,
            text: current,
            final: isFinal,
          },
          signal,
        );
        const auditKey = JSON.stringify([step.plugin.id, hook, decision.action]);
        const previous = auditByAction.get(auditKey);
        if (previous) {
          previous.executions = (previous.executions ?? 1) + 1;
          previous.latency_us += latencyUs;
        } else {
          const entry: PluginAuditEntry = {
            plugin_id: step.plugin.id,
            version: step.plugin.version,
            hook,
            action: decision.action,
            ...(decision.action === "BLOCK" ? { rule_code: decision.rule_code } : {}),
            latency_us: latencyUs,
            executions: 1,
          };
          auditByAction.set(auditKey, entry);
          audit.push(entry);
        }
        if (decision.action === "TRANSFORM") {
          current = decision.text;
        } else if (decision.action === "BLOCK") {
          blockedState = {
            rule_code: decision.rule_code,
            http_status: decision.http_status ?? 400,
            safe_message: decision.safe_message ?? "Content blocked",
            charge_policy: step.chargePolicy,
          };
          onAbortUpstream();
          return null;
        }
      }
      return current;
    };

    const wasmSteps = resolved.filter((s) => s.plugin.kind === "WASM");
    const holdWebhookSteps = resolved.filter(
      (s) =>
        s.plugin.kind === "WEBHOOK" &&
        (s.plugin.webhook_config?.stream_mode ?? "HOLD_UNTIL_APPROVED") === "HOLD_UNTIL_APPROVED",
    );
    const cutoffWebhookSteps = resolved.filter(
      (s) =>
        s.plugin.kind === "WEBHOOK" && s.plugin.webhook_config?.stream_mode === "STREAM_AND_CUTOFF",
    );

    const wasmLookahead = Math.min(
      DEFAULT_MAX_WINDOW_BYTES,
      Math.max(
        0,
        ...wasmSteps.map(
          (s) => (s.plugin.manifest as Partial<WasmPluginManifest>).window_bytes ?? 0,
        ),
      ),
    );

    return {
      isBlocked(): boolean {
        return blockedState !== null;
      },

      async push(chunkText: string): Promise<{ emit: string; blocked: boolean }> {
        if (blockedState) return { emit: "", blocked: true };
        if (resolved.length === 0) return { emit: chunkText, blocked: false };

        let textAfterWasm = chunkText;
        if (wasmSteps.length > 0) {
          if (wasmLookahead > 0) {
            const combined = wasmTail + chunkText;
            const processed = await runStepList(wasmSteps, "on_stream_chunk", combined, false);
            if (processed === null) return { emit: "", blocked: true };
            if (processed.length > wasmLookahead) {
              let safeLen = processed.length - wasmLookahead;
              if (safeLen > 0) {
                const boundaryCode = processed.charCodeAt(safeLen - 1);
                if (boundaryCode >= 0xd800 && boundaryCode <= 0xdbff) {
                  safeLen--;
                }
              }
              textAfterWasm = processed.slice(0, safeLen);
              wasmTail = processed.slice(safeLen);
            } else {
              wasmTail = processed;
              textAfterWasm = "";
            }
          } else {
            const processed = await runStepList(wasmSteps, "on_stream_chunk", chunkText, false);
            if (processed === null) return { emit: "", blocked: true };
            textAfterWasm = processed;
          }
        }

        if (!textAfterWasm) return { emit: "", blocked: false };

        if (hasCutoffWebhook) {
          cutoffBuffer += textAfterWasm;
          const now = performance.now();
          if (
            cutoffBuffer.length >= webhookWindowChars ||
            now - lastCutoffAt >= webhookWindowMs ||
            /[.!?\n。！？]$/.test(cutoffBuffer)
          ) {
            const sample = cutoffBuffer;
            cutoffBuffer = "";
            lastCutoffAt = now;
            if (pendingCutoff) {
              await pendingCutoff;
              if (blockedState) return { emit: "", blocked: true };
            }
            pendingCutoff = runStepList(cutoffWebhookSteps, "on_stream_chunk", sample, false);
            void pendingCutoff.catch(() => undefined);
          }
          if (!hasHoldWebhook) {
            return { emit: textAfterWasm, blocked: blockedState !== null };
          }
        }

        if (hasHoldWebhook) {
          holdBuffer += textAfterWasm;
          const now = performance.now();
          const shouldFlush =
            holdBuffer.length >= webhookWindowChars ||
            holdBuffer.length >= (maxWindowBytes || 4096) ||
            now - lastHoldAt >= webhookWindowMs ||
            /[.!?\n。！？]$/.test(holdBuffer);
          if (!shouldFlush) {
            return { emit: "", blocked: false };
          }
          const batch = holdBuffer;
          holdBuffer = "";
          lastHoldAt = now;
          const approved = await runStepList(holdWebhookSteps, "on_stream_chunk", batch, false);
          if (approved === null) return { emit: "", blocked: true };
          return { emit: approved, blocked: false };
        }

        return { emit: textAfterWasm, blocked: false };
      },

      async finalize(): Promise<{
        emit: string;
        blocked: boolean;
        rule_code?: string;
        http_status?: number;
        safe_message?: string;
        charge_policy: ChargePolicy;
        audit: PluginAuditEntry[];
      }> {
        if (pendingCutoff) await pendingCutoff;
        if (blockedState) {
          return {
            emit: "",
            blocked: true,
            rule_code: blockedState.rule_code,
            http_status: blockedState.http_status,
            safe_message: blockedState.safe_message,
            charge_policy: blockedState.charge_policy,
            audit,
          };
        }

        let tail = wasmTail;
        wasmTail = "";
        if (tail.length > 0 && wasmSteps.length > 0) {
          const processed = await runStepList(wasmSteps, "on_stream_end", tail, true);
          if (processed === null) {
            return {
              emit: "",
              blocked: true,
              rule_code: blockedState!.rule_code,
              http_status: blockedState!.http_status,
              safe_message: blockedState!.safe_message,
              charge_policy: blockedState!.charge_policy,
              audit,
            };
          }
          tail = processed;
        }

        let emitOut = tail;
        if (hasHoldWebhook) {
          const finalText = holdBuffer + tail;
          holdBuffer = "";
          if (finalText.length > 0) {
            const approved = await runStepList(holdWebhookSteps, "on_stream_end", finalText, true);
            if (approved === null) {
              return {
                emit: "",
                blocked: true,
                rule_code: blockedState!.rule_code,
                http_status: blockedState!.http_status,
                safe_message: blockedState!.safe_message,
                charge_policy: blockedState!.charge_policy,
                audit,
              };
            }
            emitOut = approved;
          } else {
            emitOut = "";
          }
        }

        if (hasCutoffWebhook && (cutoffBuffer.length > 0 || tail.length > 0)) {
          const finalSample = cutoffBuffer + tail;
          cutoffBuffer = "";
          const approved = await runStepList(
            cutoffWebhookSteps,
            "on_stream_end",
            finalSample,
            true,
          );
          if (approved === null) {
            return {
              emit: "",
              blocked: true,
              rule_code: blockedState!.rule_code,
              http_status: blockedState!.http_status,
              safe_message: blockedState!.safe_message,
              charge_policy: blockedState!.charge_policy,
              audit,
            };
          }
        }

        return {
          emit: emitOut,
          blocked: false,
          charge_policy: "CHARGE_CUSTOMER",
          audit,
        };
      },
    };
  }
}
