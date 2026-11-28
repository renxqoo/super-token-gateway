import { GatewayError, object, optionalObject } from "@gateway/contracts";
import type { AdaptiveRoutingPolicy, Operation } from "@gateway/kernel";
import type { ModelRecord } from "@gateway/ledger";
import { copyExact, numberLiteral, parseClientJson, requestCharacters } from "@gateway/protocol";
import { declaredRequestSize } from "../public-ingress.ts";
import { ProxyRuntimeState } from "../proxy-state.ts";
import type { TraceContext } from "../observability.ts";
import { inspectMeteredRequestSignals, validateMeteredRequest } from "../request-metering.ts";
import { snapshotSupportsHostedTools, snapshotSupportsServiceTier } from "../pricing.ts";
import type {
  InferenceContext,
  InferenceFastHandoff,
  ProxyDependencies,
  InferenceRequest,
} from "./types.ts";
import { authenticateRequest, billingAccountQuotaScope } from "./auth.ts";
import { readRequestBytesBounded, utf8FatalDecoder } from "./body.ts";
import { parseMultipartFormData } from "./multipart.ts";
import type { InferenceTelemetry } from "./telemetry.ts";
import type { InferenceResources } from "./resources.ts";

const defaultAdaptiveRouting: AdaptiveRoutingPolicy = {
  operations: ["chat"],
  mode: "active",
  first_content_target_ms: 1500,
};

function validatePositiveOutputBound(
  parent: Record<string, unknown>,
  key: string,
  field: string,
): number {
  const raw = parent[key];
  if (
    typeof raw !== "number" ||
    !Number.isSafeInteger(raw) ||
    raw <= 0 ||
    numberLiteral(parent, key) !== undefined
  ) {
    throw new GatewayError(400, "invalid_request", `${field} must be a positive integer`);
  }
  return raw;
}

function resolveOutputBound(
  body: Record<string, unknown>,
  operation: Operation,
  model: ModelRecord,
): number {
  const genCamel = optionalObject(body.generationConfig);
  const genSnake = optionalObject(body.generation_config);
  const candidates: [string, Record<string, unknown>, string, unknown][] = [
    ["max_completion_tokens", body, "max_completion_tokens", body.max_completion_tokens],
    ["max_tokens", body, "max_tokens", body.max_tokens],
    ["max_output_tokens", body, "max_output_tokens", body.max_output_tokens],
    ["generationConfig.maxOutputTokens", genCamel, "maxOutputTokens", genCamel.maxOutputTokens],
    [
      "generationConfig.max_output_tokens",
      genCamel,
      "max_output_tokens",
      genCamel.max_output_tokens,
    ],
    ["generation_config.maxOutputTokens", genSnake, "maxOutputTokens", genSnake.maxOutputTokens],
    [
      "generation_config.max_output_tokens",
      genSnake,
      "max_output_tokens",
      genSnake.max_output_tokens,
    ],
  ].filter(([, , , value]) => value !== undefined && value !== null) as [
    string,
    Record<string, unknown>,
    string,
    unknown,
  ][];

  if (
    operation !== "chat" &&
    operation !== "completions" &&
    operation !== "responses" &&
    operation !== "messages" &&
    operation !== "gemini"
  ) {
    return model.max_output_tokens;
  }

  const allowedByOp: Record<
    "chat" | "completions" | "responses" | "messages" | "gemini",
    ReadonlySet<string>
  > = {
    chat: new Set(["max_completion_tokens", "max_tokens"]),
    completions: new Set(["max_tokens"]),
    responses: new Set(["max_output_tokens"]),
    messages: new Set(["max_tokens"]),
    gemini: new Set([
      "generationConfig.maxOutputTokens",
      "generationConfig.max_output_tokens",
      "generation_config.maxOutputTokens",
      "generation_config.max_output_tokens",
    ]),
  };
  if (
    operation === "gemini" &&
    body.generationConfig !== undefined &&
    body.generationConfig !== null &&
    body.generation_config !== undefined &&
    body.generation_config !== null
  ) {
    throw new GatewayError(
      400,
      "invalid_request",
      "Cannot specify both generationConfig and generation_config",
    );
  }
  const allowed = allowedByOp[operation];
  for (const [field] of candidates) {
    if (!allowed.has(field)) {
      throw new GatewayError(
        400,
        "invalid_request",
        `Output token bound field ${field} is not valid for ${operation}`,
      );
    }
  }
  if (candidates.length > 1) {
    throw new GatewayError(
      400,
      "invalid_request",
      `Multiple output token bound fields specified: ${candidates.map(([f]) => f).join(", ")}`,
    );
  }
  if (candidates.length === 1) {
    const [field, parent, key] = candidates[0]!;
    return Math.min(validatePositiveOutputBound(parent, key, field), model.max_output_tokens);
  }
  return model.max_output_tokens;
}

function validateImageRequestCount(
  body: Record<string, unknown>,
  operation: Operation,
  isMultipartOp: boolean,
  multipartEntries?: readonly [string, string | File][],
): void {
  if (operation !== "images" && operation !== "image_edits") return;
  if (isMultipartOp && multipartEntries) {
    let nCount = 0;
    for (const [key] of multipartEntries) {
      if (key === "n") nCount++;
    }
    if (nCount > 1) {
      throw new GatewayError(400, "invalid_request", "Multiple n fields specified");
    }
  }
  if (body.n === undefined || (!isMultipartOp && body.n === null)) return;
  if (isMultipartOp && typeof body.n === "string") {
    const trimmed = body.n.trim();
    if (/^[1-9]\d*$/.test(trimmed)) {
      const parsed = Number(trimmed);
      if (Number.isSafeInteger(parsed) && parsed > 0) return;
    }
    throw new GatewayError(400, "invalid_request", "n must be a positive integer");
  }
  if (
    typeof body.n !== "number" ||
    !Number.isSafeInteger(body.n) ||
    body.n <= 0 ||
    numberLiteral(body, "n") !== undefined
  ) {
    throw new GatewayError(400, "invalid_request", "n must be a positive integer");
  }
}

export async function admitInferenceRequest(
  c: InferenceContext,
  deps: ProxyDependencies,
  state: ProxyRuntimeState,
  operation: Operation,
  requestId: string,
  trace: TraceContext,
  requestStart: number,
  resources: InferenceResources,
  telemetry: InferenceTelemetry,
  pathOverride?: { model: string; stream: boolean },
  peerAddress?: string | (() => string | undefined),
  handoff?: InferenceFastHandoff,
): Promise<InferenceRequest> {
  const deferredIngress = handoff?.deferredIngress;
  let ingressChecked = !deferredIngress;
  try {
    resources.enter();
    const reqHeaders = c.req.raw.headers;
    let snap: InferenceRequest["snap"];
    let key: InferenceRequest["key"];
    let billingAccount: InferenceRequest["billingAccount"];
    if (handoff?.preAuth) {
      snap = handoff.preAuth.snap;
      key = handoff.preAuth.key;
      billingAccount = handoff.preAuth.billingAccount;
      telemetry.setBillingAccount(billingAccount.id);
    } else {
      if (deps.snapshot.isDiverged()) {
        throw new GatewayError(
          503,
          "config_diverged",
          "Gateway configuration epoch diverged from database lineage",
        );
      }
      if (!deps.snapshot.isFresh()) {
        throw new GatewayError(503, "config_unavailable", "Gateway configuration is stale");
      }
      snap = deps.snapshot.get();
      const auth = authenticateRequest(c, snap, deps.config, operation, peerAddress, state);
      key = auth.key;
      billingAccount = auth.billingAccount;
      telemetry.setBillingAccount(billingAccount.id);
      const check = deps.checkConsoleAdmission(billingAccount, key);
      if (check) await check;
    }

    const localAccountRelease = state.tryEnterBillingAccount(
      billingAccount.id,
      billingAccount.concurrency,
    );
    if (!localAccountRelease) {
      throw new GatewayError(429, "rate_limit_concurrency", "Billing account rate limit exceeded");
    }
    resources.setLocalAccount(localAccountRelease);

    const contentType = reqHeaders.get("content-type") ?? "";
    const isMultipartOp =
      (operation === "transcription" ||
        operation === "translation" ||
        operation === "image_edits") &&
      contentType.toLowerCase().includes("multipart/form-data");

    const bodyReadTimeoutMs =
      deps.config.bodyReadTimeoutMs ?? Math.min(deps.config.attemptTimeoutMs, 15_000);
    let bodyReadLimit = deps.config.maxBodyBytes;
    let declaredSize: number | null | undefined =
      handoff && "declaredSize" in handoff ? (handoff.declaredSize ?? null) : undefined;
    if (deps.bodyBufferBudget) {
      const parsed =
        declaredSize !== undefined
          ? (declaredSize ?? undefined)
          : declaredRequestSize(c.req.raw, bodyReadLimit);
      declaredSize = parsed ?? null;
      bodyReadLimit = parsed ?? bodyReadLimit;
      resources.setBodyBuffer(deps.bodyBufferBudget.reserve(bodyReadLimit));
    }
    let clientBody: Record<string, unknown>;
    let multipartEntries: [string, string | File][] | undefined;

    if (isMultipartOp) {
      const received = await readRequestBytesBounded(
        c.req.raw,
        bodyReadLimit,
        bodyReadTimeoutMs,
        state.deadlines,
        resources.isDraining,
        resources.setAbortNotify,
        declaredSize,
      );
      const entries = parseMultipartFormData(received, contentType);
      const meta: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
      for (const [key, value] of entries) {
        meta[key] =
          typeof value === "string"
            ? value
            : { filename: value.name, size: value.size, type: value.type };
      }
      multipartEntries = entries;
      if (
        meta.stream !== undefined &&
        meta.stream !== "true" &&
        meta.stream !== "false" &&
        meta.stream !== true &&
        meta.stream !== false
      ) {
        throw new GatewayError(400, "invalid_request", "stream must be a boolean");
      }
      clientBody = {
        ...meta,
        stream: meta.stream === "true" || meta.stream === true,
      };
    } else {
      const rawBytes = await readRequestBytesBounded(
        c.req.raw,
        bodyReadLimit,
        bodyReadTimeoutMs,
        state.deadlines,
        resources.isDraining,
        resources.setAbortNotify,
        declaredSize,
      );
      const parsedRaw = object(parseClientJson(utf8FatalDecoder.decode(rawBytes)));
      clientBody = pathOverride
        ? copyExact(parsedRaw, {
            ...parsedRaw,
            model: pathOverride.model,
            stream: pathOverride.stream,
          })
        : parsedRaw;
    }

    const modelName = typeof clientBody.model === "string" ? clientBody.model.trim() : "";
    if (!modelName) throw new GatewayError(400, "invalid_request", "Missing model");
    if (key.models.length > 0 && !key.models.includes(modelName)) {
      throw new GatewayError(403, "model_not_allowed", "API key cannot access this model");
    }
    const model = snap.models.get(modelName);
    if (!model || !model.enabled || !model.operations.includes(operation)) {
      throw new GatewayError(404, "model_not_found", "Model or operation is unavailable");
    }
    telemetry.setModel(model.name);

    const isStream =
      clientBody.stream === true || (operation === "speech" && clientBody.stream_format === "sse");
    const meteredSignals = inspectMeteredRequestSignals(clientBody, operation);
    validateMeteredRequest(clientBody, operation, {
      allowPricedServiceTier: meteredSignals.serviceTier
        ? snapshotSupportsServiceTier(snap, meteredSignals.serviceTier)
        : false,
      allowPricedHostedTools:
        meteredSignals.hostedTools.size > 0
          ? snapshotSupportsHostedTools(
              snap,
              model.name,
              operation,
              meteredSignals.hostedTools,
              isStream,
            )
          : false,
    });
    validateImageRequestCount(clientBody, operation, isMultipartOp, multipartEntries);

    if (operation === "videos" && isStream) {
      throw new GatewayError(
        400,
        "invalid_request",
        "Video task creation does not support streaming",
      );
    }
    const configuredRouting = model.adaptive_routing;
    const adaptivePolicy =
      operation === "chat" && configuredRouting !== false
        ? (configuredRouting ??
          (model.routing_strategy === "WEIGHTED" ? defaultAdaptiveRouting : undefined))
        : undefined;
    telemetry.beginResponseTracking(isStream, adaptivePolicy?.first_content_target_ms);
    const outputBound = resolveOutputBound(clientBody, operation, model);

    const initialChars = isMultipartOp ? undefined : requestCharacters(operation, clientBody);
    const estimatedTokens = Math.max(
      1,
      Math.ceil((initialChars ?? 200) / 4) + Math.min(outputBound, 4096),
    );
    let projAcq: Awaited<ReturnType<typeof deps.cluster.acquireRateLimit>>;
    if (deferredIngress) {
      ingressChecked = true;
      deferredIngress.beginWait();
      try {
        projAcq = await deps.cluster.acquireRateLimit(
          billingAccountQuotaScope(billingAccount.id),
          {
            rpm: billingAccount.rpm,
            tpm: billingAccount.tpm,
            concurrency: billingAccount.concurrency,
          },
          estimatedTokens,
          deps.config.holdTtlMs,
          resources.revokeAccountPermit,
          deferredIngress.spec,
        );
      } finally {
        deferredIngress.endWait();
      }
      if (c.req.raw.signal.aborted) {
        if (projAcq.ok && projAcq.release) projAcq.release();
        throw new GatewayError(499, "client_aborted", "Client disconnected");
      }
    } else {
      projAcq = await deps.cluster.acquireRateLimit(
        billingAccountQuotaScope(billingAccount.id),
        {
          rpm: billingAccount.rpm,
          tpm: billingAccount.tpm,
          concurrency: billingAccount.concurrency,
        },
        estimatedTokens,
        deps.config.holdTtlMs,
        resources.revokeAccountPermit,
      );
    }
    if (!projAcq.ok) {
      if (projAcq.reason === "ip_rate_limited" && deferredIngress) {
        throw deferredIngress.denyError("ip_rate_limited", projAcq.retryAfterMs);
      }
      if (projAcq.reason === "ingress_quota_unavailable" && deferredIngress) {
        throw deferredIngress.denyError("ingress_quota_unavailable");
      }
      if (projAcq.reason === "unavailable" || projAcq.reason === "ingress_quota_unavailable") {
        throw new GatewayError(
          503,
          "rate_limit_unavailable",
          "Shared request quota is unavailable",
        );
      }
      throw new GatewayError(
        429,
        `rate_limit_${projAcq.reason}`,
        "Billing account rate limit exceeded",
      );
    }
    if (projAcq.release) resources.setAccountLimit(projAcq.release);
    const asyncTasks = deps.asyncTasks;
    if (operation === "videos" && asyncTasks) {
      const limit = Math.min(
        billingAccount.concurrency,
        deps.config.maxActiveAsyncTasksPerBillingAccount ?? 10,
      );
      const leaseMs = Math.max(
        2 * 3_600_000,
        Math.max(deps.config.holdTtlMs, deps.config.attemptTimeoutMs, 300_000) * 10 + 120_000,
      );
      const admitted = await asyncTasks.reserveTaskSlot({
        requestId,
        billingAccountId: billingAccount.id,
        limit,
        cancelCooldownMs: deps.config.asyncTaskCancelCooldownMs ?? 0,
        leaseMs,
      });
      if (!admitted) {
        throw new GatewayError(
          429,
          "rate_limit_async_tasks",
          "Too many concurrent active async video tasks for this billingAccount",
        );
      }
      resources.setVideoAdmission(() => asyncTasks.releaseTaskSlot(requestId, billingAccount.id));
    }
    const maxStreamDurationMs = deps.config.streamMaxDurationMs ?? 300_000;
    const firstByteDeadlineMs =
      deps.config.firstByteTimeoutMs !== undefined
        ? requestStart + deps.config.firstByteTimeoutMs
        : null;
    return {
      c,
      deps,
      state,
      operation,
      requestId,
      trace,
      requestStart,
      snap,
      key,
      billingAccount,
      model,
      clientBody,
      multipartEntries,
      isMultipartOp,
      isStream,
      outputBound,
      initialChars,
      adaptivePolicy,
      strictAdaptiveSwitch: operation === "chat" && Boolean(configuredRouting),
      maxStreamDurationMs,
      firstByteDeadlineMs,
    };
  } catch (err) {
    if (deferredIngress && !ingressChecked) {
      ingressChecked = true;
      await deferredIngress.flushEarly();
    }
    throw err;
  }
}
