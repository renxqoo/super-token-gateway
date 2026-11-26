import type { PluginAuditEntry, RouteRecord } from "@gateway/ledger";
import type { PluginOutcomeKind } from "@gateway/plugin-engine";
import { formatProtocolError, requestCharacters } from "@gateway/protocol";
import type { HydratedCandidate } from "../proxy-state.ts";
import { isConversationalOperation, transformRequestTexts } from "../upstream.ts";
import type { InferenceRequest } from "./types.ts";
import { responseHeaders, sanitizeHttpErrorStatus } from "./http.ts";
import type { InferenceTelemetry } from "./telemetry.ts";
import type { InferenceResources } from "./resources.ts";
import type { InferenceAccounting } from "./accounting.ts";

export function classifyPluginBlock(
  outcomeKind: PluginOutcomeKind | undefined,
  httpStatus: number | undefined,
  safeMessage: string | undefined,
): {
  isContentDecision: boolean;
  status: number;
  code: string;
  message: string;
  extraHeaders: Record<string, string>;
} {
  const kind = outcomeKind ?? "content_decision";
  if (kind === "host_capacity") {
    return {
      isContentDecision: false,
      status: 503,
      code: "plugin_capacity_unavailable",
      message: safeMessage ?? "Plugin runtime capacity unavailable",
      extraHeaders: { "retry-after": "1" },
    };
  }
  if (kind === "plugin_fault") {
    return {
      isContentDecision: false,
      status: 500,
      code: "plugin_execution_failed",
      message: safeMessage ?? "Plugin execution failed",
      extraHeaders: {},
    };
  }
  return {
    isContentDecision: true,
    status: sanitizeHttpErrorStatus(httpStatus, 400),
    code: "content_blocked",
    message: safeMessage ?? "Content blocked by moderation policy",
    extraHeaders: {},
  };
}

export function createRouteInput(
  request: InferenceRequest,
  resources: InferenceResources,
  telemetry: InferenceTelemetry,
) {
  const {
    c,
    deps,
    snap,
    operation,
    billingAccount,
    model,
    requestId,
    trace,
    clientBody,
    initialChars,
    isMultipartOp,
  } = request;
  const emptyAudit: PluginAuditEntry[] = [];
  let preRequestCache: Map<
    string,
    {
      body: Record<string, unknown>;
      chars: number | undefined;
      measurement: { inputCharacters?: string | undefined; images?: number | undefined };
      audit: PluginAuditEntry[];
    }
  > | null = null;
  const pipelineKeyOf = (steps: readonly RouteRecord["plugin_pipeline"][number][]) => {
    if (steps.length === 0) return "";
    return steps
      .filter((s) => s.enabled !== false)
      .map((s) => `${s.plugin_id}:${s.priority}:${s.charge_policy ?? ""}`)
      .join("|");
  };
  function prepareRouteInput(candidate: HydratedCandidate, pluginSignal: AbortSignal) {
    pluginSignal.throwIfAborted();
    const steps = candidate.route.plugin_pipeline;
    if (steps.length === 0 && !isMultipartOp) {
      const measurement = {
        inputCharacters: initialChars === undefined ? undefined : String(initialChars),
        images:
          operation === "images" || operation === "image_edits"
            ? Number(clientBody.n ?? 1)
            : undefined,
      };
      return {
        body: clientBody,
        chars: initialChars,
        measurement,
        audit: emptyAudit,
        pluginSignal,
      };
    }
    return prepareWithPolicy(candidate, pluginSignal);
  }

  async function prepareWithPolicy(candidate: HydratedCandidate, pluginSignal: AbortSignal) {
    const steps = candidate.route.plugin_pipeline;
    const pipeKey = isMultipartOp ? "__multipart__" : pipelineKeyOf(steps);
    const cached = preRequestCache?.get(pipeKey);
    if (cached) {
      return { ...cached, pluginSignal };
    }
    let body = clientBody;
    const localAudit: PluginAuditEntry[] = [];
    if (steps.length > 0 && !isMultipartOp) {
      if (
        deps.metrics.isTenantPlatformAbsorbExceeded(
          billingAccount.id,
          deps.config.platformAbsorbMaxCountPerMinute ?? 100,
          deps.config.platformAbsorbMaxAtomsPerMinute,
        )
      ) {
        telemetry.emitAccessLog(503, "plugin_capacity_unavailable");
        return c.json(
          formatProtocolError(
            operation,
            503,
            "plugin_capacity_unavailable",
            "Tenant plugin fault absorption budget exceeded",
            requestId,
          ),
          503,
          responseHeaders(requestId, trace, { "retry-after": "5" }),
        );
      }
      let preBlocked: {
        outcome_kind?: PluginOutcomeKind;
        rule_code?: string;
        http_status?: number;
        safe_message?: string;
      } | null = null;
      const policyLinked = resources.link([c.req.raw.signal, pluginSignal]);
      let transformed: Awaited<ReturnType<typeof transformRequestTexts>>;
      try {
        transformed = await transformRequestTexts(clientBody, operation, async (partText) => {
          const preRes = await deps.pluginEngine.runUnary(
            "on_request",
            steps,
            snap.plugins,
            {
              request_id: requestId,
              billing_account_id: billingAccount.id,
              model: model.name,
              operation,
            },
            partText,
            policyLinked.signal,
          );
          localAudit.push(...preRes.audit);
          if (preRes.blocked) {
            preBlocked = {
              ...(preRes.outcome_kind !== undefined ? { outcome_kind: preRes.outcome_kind } : {}),
              ...(preRes.rule_code !== undefined ? { rule_code: preRes.rule_code } : {}),
              ...(preRes.http_status !== undefined ? { http_status: preRes.http_status } : {}),
              ...(preRes.safe_message !== undefined ? { safe_message: preRes.safe_message } : {}),
            };
            return { blocked: true, text: "" };
          }
          return { blocked: false, text: preRes.text };
        });
      } finally {
        policyLinked.dispose();
      }
      if (transformed.blocked && preBlocked) {
        const blk = preBlocked as {
          outcome_kind?: PluginOutcomeKind;
          rule_code?: string;
          http_status?: number;
          safe_message?: string;
        };
        const classified = classifyPluginBlock(blk.outcome_kind, blk.http_status, blk.safe_message);
        if (classified.isContentDecision) {
          telemetry.moderationBlocked();
        }
        telemetry.emitAccessLog(classified.status, blk.rule_code ?? classified.code);
        return c.json(
          formatProtocolError(
            operation,
            classified.status,
            classified.code,
            classified.message,
            requestId,
            blk.rule_code ? { rule_code: blk.rule_code } : undefined,
          ),
          classified.status as 400,
          responseHeaders(requestId, trace, classified.extraHeaders),
        );
      }
      body = transformed.body;
    }

    const chars =
      body === clientBody && !isMultipartOp ? initialChars : requestCharacters(operation, body);
    const measurement = {
      inputCharacters: chars === undefined ? undefined : String(chars),
      images:
        operation === "images" || operation === "image_edits" ? Number(body.n ?? 1) : undefined,
    };
    const entry = { body, chars, measurement, audit: localAudit };
    if (!preRequestCache) preRequestCache = new Map();
    preRequestCache.set(pipeKey, entry);
    return { ...entry, pluginSignal };
  }
  return prepareRouteInput;
}

export type RouteInput = Exclude<
  Awaited<ReturnType<ReturnType<typeof createRouteInput>>>,
  Response
>;

export function createRouteOutputAdmission(
  request: InferenceRequest,
  resources: InferenceResources,
  telemetry: InferenceTelemetry,
  accounting: InferenceAccounting,
) {
  const { c, deps, operation, snap, requestId, trace, isStream } = request;
  const admitOutputPolicy = (candidate: HydratedCandidate) => {
    resources.clearOutputAdmission();
    if (!isConversationalOperation(operation) || !candidate.route.plugin_pipeline.length)
      return undefined;
    const admission = deps.pluginEngine.admitOutputPolicies(
      candidate.route.plugin_pipeline,
      snap.plugins,
      requestId,
      isStream,
    );
    if (admission.ok) {
      resources.setOutputAdmission(admission.release);
      return undefined;
    }
    return admission;
  };

  const rejectOutputPolicy = async (failure: {
    kind: "WASM" | "WEBHOOK";
    code: string;
    message: string;
  }) => {
    if (failure.kind === "WASM") deps.metrics.wasmAdmissionRejections++;
    else deps.metrics.webhookAdmissionRejections++;
    await accounting.finishFailedRequest();
    const classified = classifyPluginBlock("host_capacity", undefined, failure.message);
    telemetry.emitAccessLog(classified.status, failure.code);
    return c.json(
      formatProtocolError(
        operation,
        classified.status,
        classified.code,
        classified.message,
        requestId,
        { rule_code: failure.code },
      ),
      classified.status as 400,
      responseHeaders(requestId, trace, classified.extraHeaders),
    );
  };
  return { admitOutputPolicy, rejectOutputPolicy };
}

export type RouteOutputAdmission = ReturnType<typeof createRouteOutputAdmission>;
