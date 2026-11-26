import type {
  ChargePolicy,
  PluginAuditEntry,
  PluginRecord,
  WasmPluginManifest,
} from "@gateway/ledger";
import type { PluginDecision, PluginHook, PluginHookInput, PluginOutcomeKind } from "./types.ts";
import { safeSliceBoundary } from "./pipeline-text.ts";

const DEFAULT_MAX_WINDOW_BYTES = 4096;

export type ResolvedStep = {
  plugin: PluginRecord;
  chargePolicy: ChargePolicy;
};

export function createContentStreamSession(
  resolved: ResolvedStep[],
  meta: { request_id: string; billing_account_id: string; model: string; operation: string },
  onAbortUpstream: () => void,
  signal: AbortSignal | undefined,
  invokeSingle: (
    step: ResolvedStep,
    input: PluginHookInput,
    signal?: AbortSignal,
  ) => Promise<{ decision: PluginDecision; latencyUs: number }>,
) {
  const audit: PluginAuditEntry[] = [];
  const auditByAction = new Map<string, PluginAuditEntry>();
  let failOpenCount = 0;
  let blockedState: {
    rule_code: string;
    http_status: number;
    safe_message: string;
    charge_policy: ChargePolicy;
    outcome_kind: PluginOutcomeKind;
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
  const cutoffAbort = hasCutoffWebhook ? new AbortController() : undefined;
  const sessionSignal =
    cutoffAbort && signal
      ? AbortSignal.any([signal, cutoffAbort.signal])
      : (cutoffAbort?.signal ?? signal);
  const webhookSteps = resolved.filter((s) => s.plugin.kind === "WEBHOOK");
  const maxHoldBufferBytes =
    webhookSteps.length > 0
      ? Math.min(...webhookSteps.map((s) => s.plugin.webhook_config?.max_buffer_bytes ?? 4096))
      : DEFAULT_MAX_WINDOW_BYTES;
  const webhookWindowChars =
    webhookSteps.length > 0
      ? Math.min(...webhookSteps.map((s) => s.plugin.webhook_config?.window_chars ?? 256))
      : 256;
  const webhookWindowMs =
    webhookSteps.length > 0
      ? Math.min(...webhookSteps.map((s) => s.plugin.webhook_config?.window_ms ?? 300))
      : 300;
  let wasmTail = "";

  const runStepList = async (
    stepList: ResolvedStep[],
    hook: PluginHook,
    text: string,
    isFinal: boolean,
  ): Promise<string | null> => {
    let current = text;
    for (const step of stepList) {
      const { decision, latencyUs } = await invokeSingle(
        step,
        {
          hook,
          request_id: meta.request_id,
          billing_account_id: meta.billing_account_id,
          model: meta.model,
          operation: meta.operation,
          text: current,
          final: isFinal,
        },
        sessionSignal,
      );
      const outcomeKind: PluginOutcomeKind = decision.outcome_kind ?? "content_decision";
      const failOpen = Boolean(decision.action === "PASS" && decision.fail_open_bypassed);
      if (failOpen) failOpenCount++;
      const auditKey = JSON.stringify([
        step.plugin.id,
        hook,
        decision.action,
        outcomeKind,
        failOpen,
      ]);
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
          outcome_kind: outcomeKind,
          ...(failOpen ? { fail_open: true } : {}),
          ...(decision.action === "BLOCK"
            ? { rule_code: decision.rule_code }
            : decision.action === "PASS" && decision.fault_code
              ? { rule_code: decision.fault_code }
              : {}),
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
          charge_policy: outcomeKind === "content_decision" ? step.chargePolicy : "PLATFORM_ABSORB",
          outcome_kind: outcomeKind,
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
      ...wasmSteps.map((s) => (s.plugin.manifest as Partial<WasmPluginManifest>).window_bytes ?? 0),
    ),
  );

  return {
    isBlocked(): boolean {
      return blockedState !== null;
    },

    async cancelPending(): Promise<void> {
      if (!pendingCutoff) return;
      cutoffAbort?.abort(new Error("Stream moderation cancelled"));
      await pendingCutoff.catch(() => undefined);
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
            const safeLen = safeSliceBoundary(processed, processed.length - wasmLookahead);
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
          holdBuffer.length >= maxHoldBufferBytes ||
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
      outcome_kind: PluginOutcomeKind;
      fail_open_count: number;
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
          outcome_kind: blockedState.outcome_kind,
          fail_open_count: failOpenCount,
          audit,
        };
      }

      let tail = wasmTail;
      wasmTail = "";
      const hasWasmEndHook = wasmSteps.some((step) => {
        const hooks = (step.plugin.manifest as Partial<WasmPluginManifest>).hooks;
        return !hooks || hooks.includes("on_stream_end");
      });
      if (wasmSteps.length > 0 && (tail.length > 0 || hasWasmEndHook)) {
        const processed = await runStepList(wasmSteps, "on_stream_end", tail, true);
        if (processed === null) {
          return {
            emit: "",
            blocked: true,
            rule_code: blockedState!.rule_code,
            http_status: blockedState!.http_status,
            safe_message: blockedState!.safe_message,
            charge_policy: blockedState!.charge_policy,
            outcome_kind: blockedState!.outcome_kind,
            fail_open_count: failOpenCount,
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
              outcome_kind: blockedState!.outcome_kind,
              fail_open_count: failOpenCount,
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
        const approved = await runStepList(cutoffWebhookSteps, "on_stream_end", finalSample, true);
        if (approved === null) {
          return {
            emit: "",
            blocked: true,
            rule_code: blockedState!.rule_code,
            http_status: blockedState!.http_status,
            safe_message: blockedState!.safe_message,
            charge_policy: blockedState!.charge_policy,
            outcome_kind: blockedState!.outcome_kind,
            fail_open_count: failOpenCount,
            audit,
          };
        }
      }

      return {
        emit: emitOut,
        blocked: false,
        charge_policy: "CHARGE_CUSTOMER",
        outcome_kind: "content_decision",
        fail_open_count: failOpenCount,
        audit,
      };
    },
  };
}
