import { GatewayError } from "@gateway/contracts";
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
  PluginOutcomeKind,
  WasmInvokePriority,
  WasmRuntimeExecutor,
} from "./types.ts";
import { verifyPluginIntegrity, WasmCapacityError, WasmPluginFaultError } from "./wasm-host.ts";
import { WebhookPluginExecutor } from "./webhook.ts";

const DEFAULT_MAX_WINDOW_BYTES = 4096;
const DEFAULT_MAX_INPUT_BYTES = 32_768;
const MAX_DYNAMIC_FUEL_BUDGET = 50_000_000;
const RESULT_CACHE_LIMIT = 4096;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

type IntegrityResult =
  { ok: true } | { ok: false; reason: "missing_bytes" | "digest_mismatch" | "signature_invalid" };

function sanitizeBlockStatus(value: unknown, fallback = 400): number {
  return typeof value === "number" && Number.isInteger(value) && value >= 400 && value <= 599
    ? value
    : fallback;
}

function sliceTailForMaxBytes(text: string, maxBytes: number): { head: string; tail: string } {
  if (maxBytes <= 0 || text.length === 0) return { head: text, tail: "" };
  let bytes = 0;
  let cut = text.length;
  while (cut > 0) {
    let prev = cut - 1;
    const code = text.charCodeAt(prev);
    if (code >= 0xdc00 && code <= 0xdfff && prev > 0) {
      const high = text.charCodeAt(prev - 1);
      if (high >= 0xd800 && high <= 0xdbff) prev--;
    }
    const charBytes = encoder.encode(text.slice(prev, cut)).byteLength;
    if (bytes + charBytes > maxBytes) break;
    bytes += charBytes;
    cut = prev;
  }
  const safeCut = safeSliceBoundary(text, cut);
  return { head: text.slice(0, safeCut), tail: text.slice(safeCut) };
}

export type PluginObservation = {
  plugin_id: string;
  plugin_name: string;
  hook: PluginHook;
  kind: "WASM" | "WEBHOOK";
  latency_ms: number;
  outcome_kind: PluginOutcomeKind;
  blocked: boolean;
  fail_open: boolean;
  rule_code?: string;
};

export type PluginPipelineOptions = {
  wasmExecutor?: WasmRuntimeExecutor | null;
  webhookExecutor?: WebhookPluginExecutor;
  signingPublicKeyPem?: string;
  onObservation?: (obs: PluginObservation) => void;
};

type ResolvedStep = {
  plugin: PluginRecord;
  chargePolicy: ChargePolicy;
};

function safeSliceBoundary(text: string, targetIndex: number): number {
  let idx = Math.max(0, Math.min(text.length, targetIndex));
  if (idx > 0 && idx < text.length) {
    const code = text.charCodeAt(idx - 1);
    if (code >= 0xd800 && code <= 0xdbff) idx--;
  }
  return idx;
}

function splitChunkForMaxBytes(text: string, start: number, maxBytes: number): number {
  const approx = Math.min(text.length, start + maxBytes);
  let end = safeSliceBoundary(text, approx);
  if (end <= start) end = Math.min(text.length, start + 1);
  while (end > start + 1 && encoder.encode(text.slice(start, end)).byteLength > maxBytes) {
    const excess = Math.max(1, Math.floor((end - start) / 4));
    end = safeSliceBoundary(text, end - excess);
  }
  return Math.max(start + 1, end);
}

export class PluginPipelineEngine {
  private readonly wasm: WasmRuntimeExecutor | null;
  private readonly webhook: WebhookPluginExecutor;
  private readonly signingPublicKeyPem: string | undefined;
  private readonly onObservation: ((obs: PluginObservation) => void) | undefined;
  private readonly integrityByBuffer = new WeakMap<Uint8Array, Map<string, IntegrityResult>>();
  private readonly resultCache = new Map<string, PluginDecision>();

  constructor(options: PluginPipelineOptions = {}) {
    this.wasm = options.wasmExecutor ?? null;
    this.webhook = options.webhookExecutor ?? new WebhookPluginExecutor();
    this.signingPublicKeyPem = options.signingPublicKeyPem;
    this.onObservation = options.onObservation;
  }

  private checkIntegrity(plugin: PluginRecord): IntegrityResult {
    if (!plugin.wasm_bytes) return { ok: false, reason: "missing_bytes" };
    const metaKey = `${plugin.id}:${plugin.digest}:${plugin.signature ?? ""}`;
    let perBuf = this.integrityByBuffer.get(plugin.wasm_bytes);
    const cached = perBuf?.get(metaKey);
    if (cached) return cached;
    const verified = verifyPluginIntegrity(plugin, this.signingPublicKeyPem);
    if (!perBuf) {
      perBuf = new Map();
      this.integrityByBuffer.set(plugin.wasm_bytes, perBuf);
    }
    if (perBuf.size >= 16) perBuf.clear();
    perBuf.set(metaKey, verified);
    return verified;
  }

  private getCachedDecision(cacheKey: string): PluginDecision | undefined {
    const hit = this.resultCache.get(cacheKey);
    if (!hit) return undefined;
    this.resultCache.delete(cacheKey);
    this.resultCache.set(cacheKey, hit);
    return hit;
  }

  private setCachedDecision(cacheKey: string, decision: PluginDecision): void {
    if (this.resultCache.size >= RESULT_CACHE_LIMIT) {
      const oldest = this.resultCache.keys().next().value;
      if (oldest !== undefined) this.resultCache.delete(oldest);
    }
    this.resultCache.set(cacheKey, decision);
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
      if (!plugin || !plugin.enabled || plugin.capability !== "CONTENT") {
        throw new GatewayError(
          503,
          "invalid_plugin_binding",
          "Required content plugin is unavailable",
        );
      }
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

  private computeFuelBudget(manifest: Partial<WasmPluginManifest>, textBytes: number): number {
    const base = manifest.fuel_budget ?? 2_000_000;
    const perByte = manifest.fuel_per_byte ?? 0;
    if (perByte <= 0) return base;
    return Math.min(MAX_DYNAMIC_FUEL_BUDGET, base + Math.ceil(perByte * textBytes));
  }

  private async invokeWasmOnce(
    plugin: PluginRecord,
    manifest: Partial<WasmPluginManifest>,
    input: PluginHookInput,
    priority: WasmInvokePriority,
    signal?: AbortSignal,
  ): Promise<PluginDecision> {
    this.wasm!.compile(plugin.id, plugin.digest, plugin.wasm_bytes!);
    const payloadBytes = encoder.encode(JSON.stringify(input));
    const textBytes = encoder.encode(input.text).byteLength;
    const rawOut = await this.wasm!.invoke(plugin.id, input.hook, payloadBytes, {
      fuelBudget: this.computeFuelBudget(manifest, textBytes),
      maxMemoryBytes: manifest.max_memory_bytes ?? 32 * 1024 * 1024,
      priority,
      tenantId: input.project_id,
      ...(signal ? { signal } : {}),
    });
    signal?.throwIfAborted();
    const parsed = JSON.parse(decoder.decode(rawOut)) as Record<string, unknown>;
    if (parsed.action === "PASS") {
      return { action: "PASS", outcome_kind: "content_decision" };
    }
    if (parsed.action === "TRANSFORM" && typeof parsed.text === "string") {
      return { action: "TRANSFORM", text: parsed.text, outcome_kind: "content_decision" };
    }
    if (parsed.action === "BLOCK") {
      return {
        action: "BLOCK",
        rule_code: typeof parsed.rule_code === "string" ? parsed.rule_code : "wasm_blocked",
        http_status: sanitizeBlockStatus(parsed.http_status, 400),
        safe_message:
          typeof parsed.safe_message === "string"
            ? parsed.safe_message
            : "Content blocked by plugin policy",
        outcome_kind: "content_decision",
      };
    }
    throw new WasmPluginFaultError("wasm_execution_failed", "Invalid WASM plugin reply");
  }

  admitOutputPolicies(
    steps: readonly PluginPipelineStep[],
    plugins: ReadonlyMap<string, PluginRecord>,
    requestId: string,
    stream: boolean,
  ):
    | { ok: true; release?: () => void }
    | { ok: false; kind: "WASM" | "WEBHOOK"; code: string; message: string } {
    let releases: (() => void)[] | undefined;
    const release = () => {
      for (const dispose of releases ?? []) dispose();
    };
    for (const { plugin } of this.resolveSteps(steps, plugins)) {
      const manifest = plugin.manifest as Partial<WasmPluginManifest>;
      const hooks = manifest.hooks;
      if (
        Array.isArray(hooks) &&
        (plugin.kind === "WASM" || hooks.length > 0) &&
        !(stream
          ? hooks.includes("on_stream_chunk") || hooks.includes("on_stream_end")
          : hooks.includes("on_response"))
      )
        continue;
      if (plugin.kind === "WASM") {
        if (manifest.failure_policy === "FAIL_OPEN") continue;
        if (this.wasm && this.wasm.acceptsEgress?.() !== false) continue;
        release();
        return {
          ok: false,
          kind: "WASM",
          code: this.wasm ? "wasm_queue_full" : "wasm_runtime_unavailable",
          message: "Plugin runtime capacity unavailable",
        };
      }
      if (plugin.kind !== "WEBHOOK" || !plugin.webhook_config) continue;
      const admission = this.webhook.admitOutput(plugin.id, plugin.webhook_config, requestId);
      if (!admission.ok) {
        release();
        return { ...admission, kind: "WEBHOOK" };
      }
      if (admission.release) (releases ??= []).push(admission.release);
    }
    return releases ? { ok: true, release } : { ok: true };
  }

  private async invokeWasmSegmented(
    plugin: PluginRecord,
    manifest: Partial<WasmPluginManifest>,
    input: PluginHookInput,
    priority: WasmInvokePriority,
    maxInputBytes: number,
    signal?: AbortSignal,
  ): Promise<PluginDecision> {
    const overlap = Math.min(
      Math.max(64, manifest.window_bytes ?? 64),
      Math.max(64, Math.floor(maxInputBytes / 4)),
    );
    const stepBytes = Math.max(256, maxInputBytes - overlap);
    let pos = 0;
    let tail = "";
    let changed = false;
    const emitted: string[] = [];

    while (pos < input.text.length) {
      const nextEnd = splitChunkForMaxBytes(input.text, pos, stepBytes);
      const isLast = nextEnd >= input.text.length;
      const slice = input.text.slice(pos, nextEnd);
      const combined = tail + slice;
      const decision = await this.invokeWasmOnce(
        plugin,
        manifest,
        { ...input, text: combined, final: isLast },
        priority,
        signal,
      );
      if (decision.action === "BLOCK") return decision;
      const processed = decision.action === "TRANSFORM" ? decision.text : combined;
      if (decision.action === "TRANSFORM") changed = true;
      if (isLast) {
        emitted.push(processed);
        tail = "";
      } else {
        const split = sliceTailForMaxBytes(processed, overlap);
        if (split.head.length > 0) emitted.push(split.head);
        tail = split.tail;
      }
      pos = nextEnd;
    }

    if (!changed) return { action: "PASS", outcome_kind: "content_decision" };
    return { action: "TRANSFORM", text: emitted.join(""), outcome_kind: "content_decision" };
  }

  private async invokeSingle(
    step: ResolvedStep,
    input: PluginHookInput,
    signal?: AbortSignal,
  ): Promise<{ decision: PluginDecision; latencyUs: number }> {
    signal?.throwIfAborted();
    const started = performance.now();
    const { plugin } = step;

    const finish = (decision: PluginDecision): { decision: PluginDecision; latencyUs: number } => {
      const latencyMs = performance.now() - started;
      const latencyUs = Math.round(latencyMs * 1000);
      const outcomeKind: PluginOutcomeKind = decision.outcome_kind ?? "content_decision";
      this.onObservation?.({
        plugin_id: plugin.id,
        plugin_name: plugin.name,
        hook: input.hook,
        kind: plugin.kind === "WEBHOOK" ? "WEBHOOK" : "WASM",
        latency_ms: latencyMs,
        outcome_kind: outcomeKind,
        blocked: decision.action === "BLOCK",
        fail_open: Boolean(decision.action === "PASS" && decision.fail_open_bypassed),
        ...(decision.action === "BLOCK"
          ? { rule_code: decision.rule_code }
          : decision.action === "PASS" && decision.fault_code
            ? { rule_code: decision.fault_code }
            : {}),
      });
      return { decision, latencyUs };
    };

    if (plugin.kind === "WEBHOOK") {
      const whManifest = plugin.manifest as Partial<WasmPluginManifest>;
      if (
        Array.isArray(whManifest.hooks) &&
        whManifest.hooks.length > 0 &&
        !whManifest.hooks.includes(input.hook)
      ) {
        return finish({ action: "PASS", outcome_kind: "content_decision" });
      }
      if (!plugin.webhook_config) {
        return finish({ action: "PASS", outcome_kind: "content_decision" });
      }
      const decision = await this.webhook.invoke(plugin.id, plugin.webhook_config, input, signal);
      signal?.throwIfAborted();
      return finish(decision);
    }

    const manifest = plugin.manifest as Partial<WasmPluginManifest>;
    const integrity = this.checkIntegrity(plugin);
    if (!integrity.ok) {
      return finish({
        action: "BLOCK",
        rule_code:
          integrity.reason === "digest_mismatch"
            ? "plugin_digest_mismatch"
            : integrity.reason === "missing_bytes"
              ? "wasm_runtime_unavailable"
              : "plugin_signature_invalid",
        http_status: 500,
        safe_message: "Plugin integrity verification failed",
        outcome_kind: "plugin_fault",
      });
    }

    if (Array.isArray(manifest.hooks) && !manifest.hooks.includes(input.hook)) {
      return finish({ action: "PASS", outcome_kind: "content_decision" });
    }

    const failPolicy = manifest.failure_policy ?? "FAIL_CLOSED";
    if (!this.wasm || !plugin.wasm_bytes) {
      if (failPolicy === "FAIL_OPEN") {
        return finish({
          action: "PASS",
          outcome_kind: "host_capacity",
          fail_open_bypassed: true,
          fault_code: "wasm_runtime_unavailable",
        });
      }
      return finish({
        action: "BLOCK",
        rule_code: "wasm_runtime_unavailable",
        http_status: 503,
        safe_message: "Plugin runtime unavailable",
        outcome_kind: "host_capacity",
      });
    }

    const useCache = manifest.deterministic_text_only === true && input.hook === "on_request";
    let cacheKey = "";
    if (useCache) {
      const textHash = new Bun.CryptoHasher("sha256").update(input.text).digest("hex");
      cacheKey = `${plugin.digest}:${input.project_id}:${input.hook}:${textHash}`;
      const cached = this.getCachedDecision(cacheKey);
      if (cached) return finish(cached);
    }

    const priority: WasmInvokePriority = input.hook === "on_request" ? "ingress" : "egress";

    try {
      const maxInputBytes = manifest.max_input_bytes ?? DEFAULT_MAX_INPUT_BYTES;
      const inputTextBytes = encoder.encode(input.text).byteLength;
      const decision =
        inputTextBytes > maxInputBytes &&
        (input.hook === "on_request" || input.hook === "on_response")
          ? await this.invokeWasmSegmented(plugin, manifest, input, priority, maxInputBytes, signal)
          : await this.invokeWasmOnce(plugin, manifest, input, priority, signal);
      if (useCache && decision.outcome_kind === "content_decision") {
        this.setCachedDecision(cacheKey, decision);
      }
      return finish(decision);
    } catch (error) {
      signal?.throwIfAborted();
      const isCapacity = error instanceof WasmCapacityError;
      const outcomeKind: PluginOutcomeKind = isCapacity ? "host_capacity" : "plugin_fault";
      const faultCode = isCapacity
        ? error.code
        : error instanceof WasmPluginFaultError
          ? error.code
          : "wasm_execution_failed";
      if (failPolicy === "FAIL_OPEN") {
        return finish({
          action: "PASS",
          outcome_kind: outcomeKind,
          fail_open_bypassed: true,
          fault_code: faultCode,
        });
      }
      return finish({
        action: "BLOCK",
        rule_code: faultCode,
        http_status: isCapacity ? 503 : 500,
        safe_message: isCapacity
          ? "Plugin capacity temporarily unavailable"
          : "Plugin execution failed",
        outcome_kind: outcomeKind,
      });
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
    let failOpenCount = 0;
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
      const outcomeKind: PluginOutcomeKind = decision.outcome_kind ?? "content_decision";
      const failOpen = Boolean(decision.action === "PASS" && decision.fail_open_bypassed);
      if (failOpen) failOpenCount++;
      audit.push({
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
          charge_policy: outcomeKind === "content_decision" ? step.chargePolicy : "PLATFORM_ABSORB",
          outcome_kind: outcomeKind,
          fail_open_count: failOpenCount,
          audit,
        };
      }
    }
    return {
      blocked: false,
      text: currentText,
      charge_policy: "CHARGE_CUSTOMER",
      outcome_kind: "content_decision",
      fail_open_count: failOpenCount,
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
            charge_policy:
              outcomeKind === "content_decision" ? step.chargePolicy : "PLATFORM_ABSORB",
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
}
