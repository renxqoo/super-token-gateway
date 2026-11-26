import { GatewayError } from "@gateway/contracts";
import { describeProviderError } from "@gateway/plugin-engine";
import { formatProtocolError } from "@gateway/protocol";
import { UpstreamPreDispatchError } from "@gateway/runtime";
import type { FastUpstreamResult } from "@gateway/runtime";
import type { EffectiveCustomerPrice } from "../pricing.ts";
import {
  isConversationalOperation,
  parseRetryAfterMs,
  prepareUpstreamRequest,
  tryPrepareUpstreamRequestSync,
} from "../upstream.ts";
import type { PreparedUpstreamRequest } from "../upstream.ts";
import type { InferenceRequest } from "./types.ts";
import { responseHeaders, isRetryableStatus, getHeaderValue } from "./http.ts";
import { utf8FatalDecoder } from "./body.ts";
import type { RouteInput, RouteOutputAdmission } from "./policy.ts";
import { channelFailureKind } from "./telemetry.ts";
import type { InferenceTelemetry } from "./telemetry.ts";
import type { InferenceResources } from "./resources.ts";
import type { InferenceAccounting } from "./accounting.ts";
import type { InferenceRoutes } from "./candidates.ts";
import type { UpstreamPreparation, DispatchReservation } from "./upstream-attempt.ts";
import type { AttemptResponse, RetryAttempt } from "./attempt-response.ts";
import { handleUnaryResponse } from "./unary.ts";
import { inspectStreamPrelude } from "./stream-prelude.ts";
import { respondToStream } from "./stream.ts";

export async function dispatchInferenceAttempt(
  request: InferenceRequest,
  resources: InferenceResources,
  telemetry: InferenceTelemetry,
  accounting: InferenceAccounting,
  routes: InferenceRoutes,
  reservation: DispatchReservation,
  preparation: UpstreamPreparation,
  output: RouteOutputAdmission,
  options: {
    cursor: number;
    attemptId: string;
    attemptNumber: number;
    attemptTimeoutMs: number;
    baseAttemptTimeoutMs: number;
    preparedAttempt: PreparedUpstreamRequest | null;
    attemptInput: RouteInput;
    candBilling: EffectiveCustomerPrice;
    hasIdempotencyKey: boolean;
  },
): Promise<Response | RetryAttempt> {
  const { c, deps, operation, requestId, trace, isStream, isMultipartOp } = request;
  const {
    cursor,
    attemptId: activeAttemptId,
    attemptNumber,
    attemptTimeoutMs,
    baseAttemptTimeoutMs,
    preparedAttempt,
    attemptInput,
    candBilling,
    hasIdempotencyKey,
  } = options;
  const {
    body: attemptBody,
    measurement,
    chars,
    pluginSignal,
    audit: preRequestAudit,
  } = attemptInput;
  const entry = routes.selected[cursor]!;
  const cand = entry.route;
  const hasBackup = cursor + 1 < routes.selected.length;
  const singleRoute = routes.selected.length === 1;
  const releaseBackups = () => routes.releaseUnusedProbes(cursor + 1);
  const releaseProbe = () => routes.releaseProbeFor(cand.channel.id);
  const completeProbe = () => routes.markProbeCompleted(cand.channel.id);
  const { upstreamOptions } = preparation;
  const { admitOutputPolicy, rejectOutputPolicy } = output;
  const attempt = resources.startAttempt(pluginSignal, attemptTimeoutMs);
  const startedAt = performance.now();
  telemetry.beginAttempt(
    cand.channel.name,
    activeAttemptId,
    attemptNumber,
    startedAt,
    reservation.timing,
  );
  const providerIdHeader =
    cand.route.adapter_overrides.reconciliation_response_id_header ??
    cand.channel.adapter_config.reconciliation_response_id_header;
  const requiresEvidence =
    cand.route.adapter_overrides.reconciliation_evidence_required ??
    cand.channel.adapter_config.reconciliation_evidence_required ??
    Boolean(providerIdHeader);
  const rejectedOutcome = (notExecuted = false): "FAILED" | "UNKNOWN" => {
    if (!requiresEvidence || notExecuted) return "FAILED";
    accounting.noteUnknownAttempt();
    return "UNKNOWN";
  };
  let routingAttempt = deps.router.beginAttempt(cand.channel.id, entry.probe);
  let upstreamSuccessObserved = false;
  const observeAttemptFailure = (
    kind: "retryable" | "rate_limit" | "auth_error",
    retryAfterMs?: number,
    latencyMs?: number,
  ) => deps.router.recordFailure(cand.channel.id, kind, retryAfterMs, latencyMs, routingAttempt);
  const observeAttemptSuccess = (latencyMs: number) => {
    if (upstreamSuccessObserved) return;
    upstreamSuccessObserved = deps.router.recordSuccess(cand.channel.id, latencyMs, routingAttempt);
    if (upstreamSuccessObserved && routingAttempt.probe) {
      routingAttempt = deps.router.beginAttempt(cand.channel.id);
    }
  };
  let fastResult: FastUpstreamResult;
  let prepared: Awaited<ReturnType<typeof prepareUpstreamRequest>>;
  try {
    pluginSignal.throwIfAborted();
    if (resources.interrupted(true)) throw resources.admissionError(resources.quotaLost(true));
    if (preparedAttempt) {
      prepared = preparedAttempt;
    } else {
      const canMutateClientBody =
        singleRoute &&
        cand.route.plugin_pipeline.length === 0 &&
        !hasIdempotencyKey &&
        !isMultipartOp;
      const options = upstreamOptions(cand, attemptBody, canMutateClientBody);
      prepared =
        tryPrepareUpstreamRequestSync(options) ??
        (await prepareUpstreamRequest({ ...options, signal: attempt.signal() }));
    }
  } catch (err) {
    if (err instanceof GatewayError && /^(?:conversion_loss$|provider_)/.test(err.code)) {
      telemetry.recordAttemptFailure(err.code);
    }
    attempt.finishExchange();
    pluginSignal.throwIfAborted();
    if (err instanceof GatewayError && err.code === "conversion_loss" && hasBackup) {
      releaseProbe();
      return { outcome: "FAILED", error: err };
    }
    if (
      err instanceof GatewayError &&
      (err.code === "provider_capacity_unavailable" || err.code === "provider_plugin_fault")
    ) {
      releaseProbe();
      releaseBackups();
      await accounting.finishFailedRequest();
      throw err;
    }
    if (
      err instanceof GatewayError &&
      (err.code === "provider_plugin_unavailable" || err.code === "provider_adapter_failed")
    ) {
      deps.metrics.recordUpstreamError(cand.channel.name, err.code);
      observeAttemptFailure("retryable", undefined, performance.now() - startedAt);
      completeProbe();
      return { outcome: "FAILED", error: err };
    }
    releaseProbe();
    releaseBackups();
    await accounting.finishFailedRequest();
    throw err;
  }

  try {
    telemetry.setStage("transport");
    pluginSignal.throwIfAborted();
    if (!deps.router.peekAvailable(cand.channel.id, entry.probe)) {
      throw new GatewayError(503, "no_route", "No healthy channel available for this model");
    }
    const unavailablePolicy = admitOutputPolicy(cand);
    if (unavailablePolicy) {
      attempt.dispose();
      return await rejectOutputPolicy(unavailablePolicy);
    }
    const upstreamRequest: Parameters<typeof deps.transport.fetchFast>[1] = {
      method: "POST",
      headers: prepared.headers,
      body: prepared.body as BodyInit,
      onCancelRegister: attempt.onCancelRegister,
      dispatchDeadline: performance.now() + attemptTimeoutMs,
    };
    if (prepared.headersValidated) upstreamRequest.headersValidated = true;
    const timingObserver = telemetry.requestTrace?.transportObserver;
    if (timingObserver) upstreamRequest.onTiming = timingObserver;
    accounting.setDispatched(true);
    fastResult = reservation.dispatch
      ? await reservation.dispatch.fetchFast(upstreamRequest)
      : await deps.transport.fetchFast(
          prepared.url,
          upstreamRequest,
          deps.config.allowPrivateUpstreams,
          deps.config.privateUpstreamCidrs,
        );
  } catch (err) {
    attempt.finishExchange();
    const isPreDispatch =
      err instanceof UpstreamPreDispatchError ||
      (err instanceof Error &&
        /ECONNREFUSED|ENOTFOUND|Unable to connect|ConnectionRefused/i.test(err.message));
    if (isPreDispatch) accounting.setDispatched(false);
    if (pluginSignal.aborted) throw pluginSignal.reason;
    if (err instanceof GatewayError && err.code === "no_route") {
      releaseProbe();
      return { outcome: "FAILED", error: err };
    }
    const quotaLost = resources.quotaLost(true);
    if (resources.interrupted(true)) {
      releaseProbe();
      releaseBackups();
      await accounting.finalizeHold(
        isPreDispatch && !accounting.unknownAttempt ? "release" : "unknown",
      );
      throw resources.admissionError(quotaLost);
    }
    const classified =
      err instanceof UpstreamPreDispatchError ? reservation.preDispatchGatewayError(err) : null;
    const diagnosticCode =
      err instanceof GatewayError
        ? err.code
        : isPreDispatch
          ? "upstream_connect"
          : attempt.aborted
            ? "upstream_timeout"
            : "transport_interrupted";
    telemetry.recordAttemptFailure(
      diagnosticCode,
      channelFailureKind(diagnosticCode),
      undefined,
      isPreDispatch ? "pre_dispatch" : "transport",
    );
    if (isPreDispatch) {
      const kind = err instanceof UpstreamPreDispatchError ? err.code : "upstream_connect";
      deps.metrics.recordUpstreamError(cand.channel.name, kind);
      if (kind === "upstream_connect" || kind === "upstream_tls" || kind === "upstream_dns") {
        deps.router.recordDialFailure(cand.channel.id, routingAttempt);
      }
      releaseProbe();
    } else {
      deps.metrics.recordUpstreamError(cand.channel.name, "timeout");
      deps.router.recordDialFailure(cand.channel.id, routingAttempt);
      releaseProbe();
    }
    const outcome = isPreDispatch ? "FAILED" : "UNKNOWN";
    if (outcome === "UNKNOWN") accounting.noteUnknownAttempt();
    return {
      outcome,
      error: new GatewayError(
        classified?.status ?? (isPreDispatch ? 503 : 504),
        classified?.code ?? (isPreDispatch ? "upstream_unreachable" : "upstream_timeout"),
        classified?.message ?? (err instanceof Error ? err.message : "Upstream request failed"),
      ),
    };
  }

  const upstreamResp = fastResult.fast ? null : fastResult.response;
  const upstreamStatus = fastResult.fast ? fastResult.status : fastResult.response.status;
  const upstreamHeaders = fastResult.fast ? fastResult.headers : fastResult.response.headers;
  const diagnostics = deps.traceQueue ? prepared.mergedOverrides.error_diagnostics : undefined;
  const describeFailure = diagnostics?.summaries
    ? (body?: unknown) => {
        try {
          return describeProviderError(
            diagnostics,
            diagnostics.code_header
              ? getHeaderValue(upstreamHeaders, diagnostics.code_header.toLowerCase())
              : null,
            body,
          );
        } catch {
          deps.metrics.traceCaptureFailures++;
          return undefined;
        }
      }
    : undefined;
  const diagnosticBodyLimit = deps.config.traceExport?.errorBodyMaxBytes ?? 8192;
  const describeEventFailure =
    describeFailure && diagnostics?.code_path
      ? (data: string) => {
          if (data.length > diagnosticBodyLimit || Buffer.byteLength(data) > diagnosticBodyLimit)
            return describeFailure();
          try {
            return describeFailure(JSON.parse(data));
          } catch {
            return describeFailure();
          }
        }
      : undefined;
  telemetry.requestTrace?.observeHeaders(upstreamStatus);
  const diagnosticIdHeader = diagnostics?.request_id_header ?? providerIdHeader;
  if (diagnosticIdHeader && deps.traceQueue) {
    const providerId = getHeaderValue(upstreamHeaders, diagnosticIdHeader.toLowerCase());
    if (
      providerId &&
      /^[A-Za-z0-9._:-]{1,128}$/.test(providerId) &&
      !/^(?:sk[_-]|eyJ)/.test(providerId)
    ) {
      telemetry.requestTrace?.bindProviderRequestId(providerId);
    }
  }
  const upstreamOk = upstreamStatus >= 200 && upstreamStatus < 300;
  if (providerIdHeader) {
    const providerId = getHeaderValue(upstreamHeaders, providerIdHeader.toLowerCase());
    if (providerId && /^[A-Za-z0-9._:/-]{1,256}$/.test(providerId)) {
      await accounting.rememberProviderRequestId(activeAttemptId, providerId);
    }
  }

  if (pluginSignal.aborted) {
    attempt.dispose();
    void upstreamResp?.body?.cancel(pluginSignal.reason).catch(() => undefined);
    throw pluginSignal.reason;
  }
  const latencyMs = performance.now() - startedAt;
  telemetry.setLatency(latencyMs);
  if (!upstreamOk) {
    telemetry.setStage("http_response");
    let detail = describeFailure?.();
    if (!detail && describeFailure && diagnostics?.code_path && diagnosticBodyLimit > 0) {
      const bytes = fastResult.fast
        ? fastResult.body
        : deps.transport.bufferedBody(fastResult.response);
      const contentType = getHeaderValue(upstreamHeaders, "content-type") ?? "";
      if (
        bytes?.byteLength &&
        bytes.byteLength <= diagnosticBodyLimit &&
        /^application\/(?:json|[^;\s]+\+json)(?:\s*;|$)/i.test(contentType)
      ) {
        try {
          detail = describeFailure(JSON.parse(utf8FatalDecoder.decode(bytes)));
        } catch {
          detail = undefined;
        }
      }
    }
    const retryKind = isRetryableStatus(upstreamStatus);
    telemetry.recordAttemptFailure(
      "upstream_http_error",
      retryKind ?? (upstreamStatus >= 400 && upstreamStatus < 500 ? "client_error" : "other"),
      detail,
    );
    attempt.dispose();
    void upstreamResp?.body?.cancel().catch(() => undefined);
    resources.clearChannelLimit();
    const outcome = rejectedOutcome();
    if (retryKind) {
      const retryAfterHdr = fastResult.fast
        ? getHeaderValue(fastResult.headers, "retry-after")
        : fastResult.response.headers.get("retry-after");
      const retryAfterMs = parseRetryAfterMs(retryAfterHdr);
      deps.metrics.recordUpstreamError(cand.channel.name, `http_${upstreamStatus}`);
      observeAttemptFailure(retryKind, retryAfterMs, latencyMs);
      completeProbe();
      return {
        outcome,
        error: new GatewayError(
          upstreamStatus === 429 ? 429 : 502,
          retryKind === "rate_limit"
            ? "upstream_rate_limited"
            : retryKind === "auth_error"
              ? "upstream_auth_error"
              : "upstream_error",
          `Upstream returned HTTP ${upstreamStatus}`,
        ),
      };
    }
    releaseProbe();
    releaseBackups();
    await accounting.finishFailedRequest();
    telemetry.emitAccessLog(upstreamStatus, "upstream_client_error");
    return c.json(
      formatProtocolError(
        operation,
        upstreamStatus,
        "upstream_client_error",
        `Upstream rejected request with HTTP ${upstreamStatus}`,
        requestId,
      ),
      upstreamStatus as 400,
      responseHeaders(requestId, trace),
    );
  }

  const upstreamContentType =
    (fastResult.fast
      ? fastResult.contentType || getHeaderValue(fastResult.headers, "content-type")
      : fastResult.response.headers.get("content-type")) ?? "application/json";
  const isUpstreamSse = upstreamContentType.toLowerCase().includes("text/event-stream");
  const isUpstreamJson = upstreamContentType.toLowerCase().includes("application/json");
  const isFakeJsonOnStream =
    isStream && !isUpstreamSse && isUpstreamJson && !prepared.isBedrockStream;

  const context: AttemptResponse = {
    request,
    resources,
    telemetry,
    accounting,
    attempt,
    candidate: cand,
    customer: candBilling,
    prepared,
    pluginSignal,
    upstreamResp,
    fastResult,
    upstreamStatus,
    upstreamContentType,
    isUpstreamSse,
    isUpstreamJson,
    isFakeJsonOnStream,
    latencyMs,
    startedAt,
    chars,
    authorizedImages: measurement.images,
    preRequestAudit,
    settle: accounting.bindSettlement(activeAttemptId, cand, candBilling, chars, pluginSignal),
    rejectedOutcome,
    releaseBackups,
    releaseProbe,
    completeProbe,
    observeAttemptSuccess,
    observeAttemptFailure,
    describeFailure,
    describeEventFailure,
  };
  if ((!isStream && !isUpstreamSse) || isFakeJsonOnStream) return handleUnaryResponse(context);

  const isWasmStreamProvider =
    prepared.resolvedProvider.kind === "WASM" &&
    prepared.resolvedProvider.config.hooks.includes("adapt_stream_event") &&
    isConversationalOperation(operation);
  const tracedUpstreamProtocol =
    prepared.upstreamProtocol ??
    (!isWasmStreamProvider && isConversationalOperation(operation) ? operation : null);
  const prelude = await inspectStreamPrelude(context, isWasmStreamProvider, tracedUpstreamProtocol);
  if ("outcome" in prelude) return prelude;

  releaseBackups();
  return respondToStream(
    context,
    prelude.upstreamBody,
    tracedUpstreamProtocol,
    prelude.wasmPrelude,
    isWasmStreamProvider,
    () => routes.takeProbe(cand.channel.id),
    baseAttemptTimeoutMs,
  );
}
