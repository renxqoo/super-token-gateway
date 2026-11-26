import type { Operation, Protocol } from "@gateway/kernel";
import { fastUuid } from "../proxy-state.ts";
import type { TraceContext } from "../observability.ts";
import { createRequestTrace, createTransportTiming } from "../first-text-trace.ts";
import type { ChannelFailureKind, ChannelFailureStage, TraceAttempt } from "../trace-types.ts";
import { isConversationalOperation } from "../upstream.ts";
import type { ProxyDependencies } from "./types.ts";

export function channelFailureKind(code: string): ChannelFailureKind {
  switch (code) {
    case "upstream_dns":
      return "dns";
    case "upstream_tls":
      return "tls";
    case "upstream_connect":
    case "upstream_unreachable":
      return "connect";
    case "transport_interrupted":
    case "stream_interrupted":
      return "interrupted";
    case "upstream_not_dispatched":
    case "upstream_timeout":
      return "timeout";
    case "upstream_capacity":
    case "channel_saturated":
    case "provider_capacity_unavailable":
      return "capacity";
    case "upstream_rate_limited":
      return "rate_limit";
    case "upstream_auth_error":
      return "auth_error";
    case "upstream_client_error":
      return "client_error";
    case "invalid_upstream":
    case "invalid_upstream_usage":
    case "unexpected_trailing_event":
    case "stream_truncated":
    case "empty_stream":
    case "usage_alias_conflict":
    case "conversion_loss":
    case "provider_stream_error":
      return "protocol";
    default:
      return "other";
  }
}

export function createInferenceTelemetry(
  deps: ProxyDependencies,
  operation: Operation,
  requestId: string,
  trace: TraceContext,
  requestStart: number,
) {
  let logBillingAccountId: string | null = null;
  let logModel: string | null = null;
  let logStream = false;
  let logChannel: string | null = null;
  let logAttempts = 0;
  let logFailover = false;
  let logModBlocked = false;
  let logTtftMs: number | null = null;
  let requestTrace: ReturnType<typeof createRequestTrace> | undefined;
  let traceProtocol: Protocol | null = null;
  let traceChannel: string | undefined;
  let failureRecorded = false;
  let channelStage: ChannelFailureStage = "request_prepare";
  const recordAttemptFailure = (
    code: string,
    kind: ChannelFailureKind = channelFailureKind(code),
    details?: TraceAttempt["providerError"],
    stage = channelStage,
  ) => {
    if (!traceChannel || failureRecorded) return;
    failureRecorded = true;
    deps.metrics.recordChannelFailure(traceChannel, stage, kind);
    requestTrace?.failAttempt(performance.now(), stage, kind, code, details);
  };
  const recordPreDispatchFailure = (
    channel: string,
    code: string,
    kind: ChannelFailureKind,
    timing?: ReturnType<typeof createTransportTiming>,
    id?: string,
    startedAt?: number,
  ) => {
    deps.metrics.recordChannelFailure(channel, "pre_dispatch", kind);
    if (!deps.traceQueue || !requestTrace) return;
    const at = performance.now();
    requestTrace.recordPreDispatchFailure(
      id ?? fastUuid(),
      channel,
      startedAt ?? at,
      at,
      code,
      kind,
      timing?.values ?? createTransportTiming().values,
    );
  };
  let waitingForFirstText = false;
  const stopWaitingForFirstText = () => {
    if (!waitingForFirstText) return;
    waitingForFirstText = false;
    deps.metrics.waitingFirstText--;
  };
  let logged = false;

  const emitAccessLog = (status: number, errorCode: string | null) => {
    if (logged) return;
    logged = true;
    const ended = performance.now();
    const durationMs = ended - requestStart;
    if (!errorCode && !requestTrace && !waitingForFirstText && !deps.metrics.isAccessLogEnabled) {
      deps.metrics.observeRequestFast(operation, status, logChannel, durationMs, logTtftMs);
      return;
    }
    stopWaitingForFirstText();
    try {
      if (
        errorCode &&
        /^(?:upstream_|invalid_upstream|provider_|unexpected_trailing_event$|missing_usage$)/.test(
          errorCode,
        )
      ) {
        recordAttemptFailure(errorCode);
      }
      if (requestTrace) {
        requestTrace.endAttempt(ended);
        const measurement = requestTrace.measurement;
        if (traceProtocol && deps.config.traceMetrics)
          deps.metrics.observeStreamPhases(operation, status, measurement);
        if (deps.traceQueue && logBillingAccountId)
          deps.traceQueue.offer({
            traceId: trace.traceId,
            spanId: trace.spanId,
            ...(trace.parentSpanId ? { parentSpanId: trace.parentSpanId } : {}),
            requestId,
            billingAccountId: logBillingAccountId,
            model: logModel,
            operation,
            status,
            firstTextTracked: traceProtocol !== null,
            error:
              errorCode && /^[A-Za-z0-9_.:-]{1,80}$/.test(errorCode)
                ? errorCode
                : errorCode
                  ? "request_failed"
                  : null,
            origin: performance.timeOrigin,
            start: requestStart,
            end: ended,
            ...(measurement.enqueued === undefined ? {} : { enqueued: measurement.enqueued }),
            ...(measurement.admission ? { admission: measurement.admission } : {}),
            ...requestTrace.completedAttempts,
          });
      }
    } catch {
      deps.metrics.traceCaptureFailures++;
    }
    if (!deps.metrics.isAccessLogEnabled) {
      deps.metrics.observeRequestFast(operation, status, logChannel, durationMs, logTtftMs);
      return;
    }
    deps.metrics.observeRequest({
      trace_id: trace.traceId,
      request_id: requestId,
      billing_account_id: logBillingAccountId,
      model: logModel,
      operation,
      stream: logStream,
      channel: logChannel,
      status,
      attempts: logAttempts,
      failover: logFailover,
      moderation_blocked: logModBlocked,
      duration_ms: durationMs,
      ttft_ms: logTtftMs,
      ...(requestTrace && traceProtocol
        ? { first_text_trace: requestTrace.record(trace.spanId) }
        : {}),
      error_code: errorCode,
    });
  };

  return {
    get requestTrace() {
      return requestTrace;
    },
    get trackFirstText() {
      return traceProtocol !== null;
    },
    setBillingAccount(id: string) {
      logBillingAccountId = id;
    },
    setModel(model: string) {
      logModel = model;
    },
    setChannel(channel: string) {
      logChannel = channel;
    },
    setStage(stage: ChannelFailureStage) {
      channelStage = stage;
    },
    setLatency(latency: number) {
      logTtftMs = latency;
    },
    beginResponseTracking(isStream: boolean) {
      logStream = isStream;
      traceProtocol = isStream && isConversationalOperation(operation) ? operation : null;
      if (
        deps.traceQueue ||
        (traceProtocol && (deps.config.traceMetrics || deps.metrics.isAccessLogEnabled))
      ) {
        requestTrace = createRequestTrace(
          requestStart,
          traceProtocol,
          deps.config.traceExport?.maxAttempts,
          stopWaitingForFirstText,
        );
        if (traceProtocol && deps.config.traceMetrics) {
          waitingForFirstText = true;
          deps.metrics.waitingFirstText++;
        }
      }
    },
    beginAttempt(
      channel: string,
      id: string,
      number: number,
      startedAt: number,
      timing: ReturnType<typeof createTransportTiming> | undefined,
    ) {
      logAttempts = number;
      traceChannel = channel;
      failureRecorded = false;
      channelStage = "request_prepare";
      requestTrace?.beginAttempt(startedAt, { id, number, channel }, timing);
    },
    failover() {
      logFailover = true;
      deps.metrics.failoversTotal++;
    },
    moderationBlocked() {
      logModBlocked = true;
      deps.metrics.moderationBlocksTotal++;
    },
    recordAttemptFailure,
    recordPreDispatchFailure,
    emitAccessLog,
  };
}

export type InferenceTelemetry = ReturnType<typeof createInferenceTelemetry>;
