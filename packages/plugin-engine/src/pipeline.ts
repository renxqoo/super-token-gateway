import { GatewayError } from "@gateway/contracts";
import type {
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
import { verifyPluginIntegrity } from "./wasm-host.ts";
import { WasmCapacityError, WasmPluginFaultError } from "./wasm-errors.ts";
import { WebhookPluginExecutor } from "./webhook.ts";

import { createContentStreamSession, type ResolvedStep } from "./pipeline-stream.ts";
import { sliceTailForMaxBytes, splitChunkForMaxBytes } from "./pipeline-text.ts";

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
    knownTextBytes?: number,
  ): Promise<PluginDecision> {
    this.wasm!.compile(plugin.id, plugin.digest, plugin.wasm_bytes!);
    const payloadBytes = encoder.encode(JSON.stringify(input));
    const perByte = manifest.fuel_per_byte ?? 0;
    const fuelBudget =
      perByte > 0
        ? this.computeFuelBudget(manifest, knownTextBytes ?? Buffer.byteLength(input.text, "utf8"))
        : (manifest.fuel_budget ?? 2_000_000);
    const rawOut = await this.wasm!.invoke(plugin.id, input.hook, payloadBytes, {
      fuelBudget,
      maxMemoryBytes: manifest.max_memory_bytes ?? 32 * 1024 * 1024,
      priority,
      tenantId: input.billing_account_id,
      transferInput: true,
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

  hasActiveHook(
    hook: PluginHook,
    steps: readonly PluginPipelineStep[],
    plugins: ReadonlyMap<string, PluginRecord>,
  ): boolean {
    for (let i = 0; i < steps.length; i++) {
      const step = steps[i]!;
      if (step.enabled === false) continue;
      const plugin = plugins.get(step.plugin_id);
      if (!plugin || !plugin.enabled || plugin.capability !== "CONTENT") return true;
      const manifest = plugin.manifest as Partial<WasmPluginManifest>;
      if (plugin.kind === "WEBHOOK") {
        if (!plugin.webhook_config) continue;
        if (
          Array.isArray(manifest.hooks) &&
          manifest.hooks.length > 0 &&
          !manifest.hooks.includes(hook)
        ) {
          continue;
        }
        return true;
      }
      if (!this.checkIntegrity(plugin).ok) return true;
      if (Array.isArray(manifest.hooks) && !manifest.hooks.includes(hook)) continue;
      return true;
    }
    return false;
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
      cacheKey = `${plugin.digest}:${input.billing_account_id}:${input.hook}:${textHash}`;
      const cached = this.getCachedDecision(cacheKey);
      if (cached) return finish(cached);
    }

    const priority: WasmInvokePriority = input.hook === "on_request" ? "ingress" : "egress";

    try {
      const needsSegmentCheck = input.hook === "on_request" || input.hook === "on_response";
      const maxInputBytes = manifest.max_input_bytes ?? DEFAULT_MAX_INPUT_BYTES;
      const inputTextBytes =
        needsSegmentCheck && input.text.length * 3 > maxInputBytes
          ? Buffer.byteLength(input.text, "utf8")
          : undefined;
      const decision =
        inputTextBytes !== undefined && inputTextBytes > maxInputBytes
          ? await this.invokeWasmSegmented(plugin, manifest, input, priority, maxInputBytes, signal)
          : await this.invokeWasmOnce(plugin, manifest, input, priority, signal, inputTextBytes);
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
    meta: { request_id: string; billing_account_id: string; model: string; operation: string },
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
          billing_account_id: meta.billing_account_id,
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
    meta: { request_id: string; billing_account_id: string; model: string; operation: string },
    onAbortUpstream: () => void,
    signal?: AbortSignal,
  ) {
    const resolved = this.resolveSteps(steps, plugins);
    return createContentStreamSession(
      resolved,
      meta,
      onAbortUpstream,
      signal,
      (step, input, requestSignal) => this.invokeSingle(step, input, requestSignal),
    );
  }
}
