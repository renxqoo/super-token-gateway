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

export class InferenceTelemetry {
  private logBillingAccountId: string | null = null;
  private logModel: string | null = null;
  private logStream = false;
  private logChannel: string | null = null;
  private logAttempts = 0;
  private logFailover = false;
  private logModBlocked = false;
  private logTtftMs: number | null = null;
  requestTrace: ReturnType<typeof createRequestTrace> | undefined;
  private traceProtocol: Protocol | null = null;
  private traceChannel: string | undefined;
  private failureRecorded = false;
  private channelStage: ChannelFailureStage = "request_prepare";
  private waitingForFirstText = false;
  private logged = false;

  constructor(
    private readonly deps: ProxyDependencies,
    private readonly operation: Operation,
    private readonly requestId: string,
    private readonly trace: TraceContext,
    private readonly requestStart: number,
  ) {}

  get trackFirstText(): boolean {
    return this.traceProtocol !== null;
  }

  setBillingAccount(id: string): void {
    this.logBillingAccountId = id;
  }

  setModel(model: string): void {
    this.logModel = model;
  }

  setChannel(channel: string): void {
    this.logChannel = channel;
  }

  setStage(stage: ChannelFailureStage): void {
    this.channelStage = stage;
  }

  setLatency(latency: number): void {
    this.logTtftMs = latency;
  }

  private readonly stopWaitingForFirstText = (): void => {
    if (!this.waitingForFirstText) return;
    this.waitingForFirstText = false;
    this.deps.metrics.waitingFirstText--;
  };

  beginResponseTracking(isStream: boolean): void {
    this.logStream = isStream;
    this.traceProtocol =
      isStream && isConversationalOperation(this.operation) ? this.operation : null;
    if (
      this.deps.traceQueue ||
      (this.traceProtocol &&
        (this.deps.config.traceMetrics || this.deps.metrics.isAccessLogEnabled))
    ) {
      this.requestTrace = createRequestTrace(
        this.requestStart,
        this.traceProtocol,
        this.deps.config.traceExport?.maxAttempts,
        this.stopWaitingForFirstText,
      );
      if (this.traceProtocol && this.deps.config.traceMetrics) {
        this.waitingForFirstText = true;
        this.deps.metrics.waitingFirstText++;
      }
    }
  }

  beginAttempt(
    channel: string,
    id: string,
    number: number,
    startedAt: number,
    timing: ReturnType<typeof createTransportTiming> | undefined,
  ): void {
    this.logAttempts = number;
    this.traceChannel = channel;
    this.failureRecorded = false;
    this.channelStage = "request_prepare";
    this.requestTrace?.beginAttempt(startedAt, { id, number, channel }, timing);
  }

  failover(): void {
    this.logFailover = true;
    this.deps.metrics.failoversTotal++;
  }

  moderationBlocked(): void {
    this.logModBlocked = true;
    this.deps.metrics.moderationBlocksTotal++;
  }

  recordAttemptFailure(
    code: string,
    kind: ChannelFailureKind = channelFailureKind(code),
    details?: TraceAttempt["providerError"],
    stage = this.channelStage,
  ): void {
    if (!this.traceChannel || this.failureRecorded) return;
    this.failureRecorded = true;
    this.deps.metrics.recordChannelFailure(this.traceChannel, stage, kind);
    this.requestTrace?.failAttempt(performance.now(), stage, kind, code, details);
  }

  recordPreDispatchFailure(
    channel: string,
    code: string,
    kind: ChannelFailureKind,
    timing?: ReturnType<typeof createTransportTiming>,
    id?: string,
    startedAt?: number,
  ): void {
    this.deps.metrics.recordChannelFailure(channel, "pre_dispatch", kind);
    if (!this.deps.traceQueue || !this.requestTrace) return;
    const at = performance.now();
    this.requestTrace.recordPreDispatchFailure(
      id ?? fastUuid(),
      channel,
      startedAt ?? at,
      at,
      code,
      kind,
      timing?.values ?? createTransportTiming().values,
    );
  }

  emitAccessLog(status: number, errorCode: string | null): void {
    if (this.logged) return;
    this.logged = true;
    const ended = performance.now();
    const durationMs = ended - this.requestStart;
    if (
      !errorCode &&
      !this.requestTrace &&
      !this.waitingForFirstText &&
      !this.deps.metrics.isAccessLogEnabled
    ) {
      this.deps.metrics.observeRequestFast(
        this.operation,
        status,
        this.logChannel,
        durationMs,
        this.logTtftMs,
      );
      return;
    }
    this.stopWaitingForFirstText();
    try {
      if (
        errorCode &&
        /^(?:upstream_|invalid_upstream|provider_|unexpected_trailing_event$|missing_usage$)/.test(
          errorCode,
        )
      ) {
        this.recordAttemptFailure(errorCode);
      }
      if (this.requestTrace) {
        this.requestTrace.endAttempt(ended);
        const measurement = this.requestTrace.measurement;
        if (this.traceProtocol && this.deps.config.traceMetrics)
          this.deps.metrics.observeStreamPhases(this.operation, status, measurement);
        if (this.deps.traceQueue && this.logBillingAccountId)
          this.deps.traceQueue.offer({
            traceId: this.trace.traceId,
            spanId: this.trace.spanId,
            ...(this.trace.parentSpanId ? { parentSpanId: this.trace.parentSpanId } : {}),
            requestId: this.requestId,
            billingAccountId: this.logBillingAccountId,
            model: this.logModel,
            operation: this.operation,
            status,
            firstTextTracked: this.traceProtocol !== null,
            error:
              errorCode && /^[A-Za-z0-9_.:-]{1,80}$/.test(errorCode)
                ? errorCode
                : errorCode
                  ? "request_failed"
                  : null,
            origin: performance.timeOrigin,
            start: this.requestStart,
            end: ended,
            ...(measurement.enqueued === undefined ? {} : { enqueued: measurement.enqueued }),
            ...(measurement.admission ? { admission: measurement.admission } : {}),
            ...this.requestTrace.completedAttempts,
          });
      }
    } catch {
      this.deps.metrics.traceCaptureFailures++;
    }
    if (!this.deps.metrics.isAccessLogEnabled) {
      this.deps.metrics.observeRequestFast(
        this.operation,
        status,
        this.logChannel,
        durationMs,
        this.logTtftMs,
      );
      return;
    }
    this.deps.metrics.observeRequest({
      trace_id: this.trace.traceId,
      request_id: this.requestId,
      billing_account_id: this.logBillingAccountId,
      model: this.logModel,
      operation: this.operation,
      stream: this.logStream,
      channel: this.logChannel,
      status,
      attempts: this.logAttempts,
      failover: this.logFailover,
      moderation_blocked: this.logModBlocked,
      duration_ms: durationMs,
      ttft_ms: this.logTtftMs,
      ...(this.requestTrace && this.traceProtocol
        ? { first_text_trace: this.requestTrace.record(this.trace.spanId) }
        : {}),
      error_code: errorCode,
    });
  }
}

export function createInferenceTelemetry(
  deps: ProxyDependencies,
  operation: Operation,
  requestId: string,
  trace: TraceContext,
  requestStart: number,
): InferenceTelemetry {
  return new InferenceTelemetry(deps, operation, requestId, trace, requestStart);
}
