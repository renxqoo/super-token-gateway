import { GatewayError, object, optionalObject } from "@gateway/contracts";
import type { Operation } from "@gateway/kernel";
import type { ModelRecord } from "@gateway/ledger";
import { copyExact, parseClientJson, requestCharacters } from "@gateway/protocol";
import { declaredRequestSize } from "../public-ingress.ts";
import { ProxyRuntimeState } from "../proxy-state.ts";
import type { TraceContext } from "../observability.ts";
import { validateMeteredRequest } from "../request-metering.ts";
import type {
  InferenceContext,
  InferenceFastHandoff,
  ProxyDependencies,
  InferenceRequest,
} from "./types.ts";
import { authenticateRequest, billingAccountQuotaScope } from "./auth.ts";
import { readRequestBytesBounded, utf8FatalDecoder } from "./body.ts";
import type { InferenceTelemetry } from "./telemetry.ts";
import type { InferenceResources } from "./resources.ts";

function resolveOutputBound(body: Record<string, unknown>, model: ModelRecord): number {
  const gen = optionalObject(body.generationConfig);
  const raw =
    body.max_completion_tokens ?? body.max_tokens ?? body.max_output_tokens ?? gen.maxOutputTokens;
  if (typeof raw === "number" && Number.isSafeInteger(raw) && raw > 0) {
    return Math.min(raw, model.max_output_tokens);
  }
  return model.max_output_tokens;
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
  nativeHttp = false,
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
        true,
        declaredSize,
      );
      const form = await new Response(received as BodyInit, {
        headers: { "content-type": contentType },
      })
        .formData()
        .catch(() => {
          throw new GatewayError(400, "invalid_request", "Invalid multipart body");
        });
      const entries: [string, string | File][] = [];
      const meta: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
      for (const [key, value] of form.entries() as Iterable<[string, string | File]>) {
        entries.push([key, value]);
        meta[key] =
          typeof value === "string"
            ? value
            : { filename: value.name, size: value.size, type: value.type };
      }
      multipartEntries = entries;
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
        !nativeHttp && deps.bodyBufferBudget !== undefined,
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
    telemetry.setModel(modelName);
    if (key.models.length > 0 && !key.models.includes(modelName)) {
      throw new GatewayError(403, "model_not_allowed", "API key cannot access this model");
    }
    const model = snap.models.get(modelName);
    if (!model || !model.enabled || !model.operations.includes(operation)) {
      throw new GatewayError(404, "model_not_found", "Model or operation is unavailable");
    }

    if (operation === "videos" && deps.asyncTasks) {
      const activeTasks = await deps.asyncTasks.countActiveTasks(
        billingAccount.id,
        deps.config.asyncTaskCancelCooldownMs ?? 0,
      );
      const maxActive = Math.min(
        billingAccount.concurrency,
        deps.config.maxActiveAsyncTasksPerBillingAccount ?? 10,
      );
      if (activeTasks >= maxActive) {
        throw new GatewayError(
          429,
          "rate_limit_async_tasks",
          "Too many concurrent active async video tasks for this billingAccount",
        );
      }
    }

    validateMeteredRequest(clientBody, operation);

    const isStream =
      clientBody.stream === true || (operation === "speech" && clientBody.stream_format === "sse");
    telemetry.beginResponseTracking(isStream);
    const outputBound = resolveOutputBound(clientBody, model);

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
