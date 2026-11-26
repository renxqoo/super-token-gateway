import type { Protocol } from "@gateway/kernel";
import { hasSseText, SSEParser, type SSEEvent } from "@gateway/protocol";
import type { UpstreamTimingObserver } from "@gateway/runtime";
import type {
  ChannelFailureKind,
  ChannelFailureStage,
  TraceAttempt,
  TransportTiming,
} from "./trace-types.ts";

type TracedAttempt = { id: string; number: number; channel: string };

export type FirstTextTraceRecord = {
  span_id: string;
  request_start_unix_ms: number;
  upstream_attempt: TracedAttempt | null;
  attempt_start_ms: number | null;
  upstream_headers_ms: number | null;
  upstream_first_text_ms: number | null;
  gateway_first_text_enqueued_ms: number | null;
};

export function createTransportTiming() {
  const values: TransportTiming = { wait_ms: 0, dial_count: 0, connection_reused: false };
  let waiting: number | undefined;
  const observe: UpstreamTimingObserver = (phase, at) => {
    if (phase === "wait_start") {
      values.first_wait ??= at;
      waiting = at;
    } else if (phase === "wait_end") {
      values.last_wait = at;
      if (waiting !== undefined) values.wait_ms += Math.max(0, at - waiting);
      waiting = undefined;
    } else if (phase === "connection_reused") {
      values.connection_reused = true;
    } else {
      if (phase === "dial_start") {
        values.dial_count++;
        delete values.tcp_ready;
        delete values.tls_ready;
      }
      values[phase] = at;
    }
  };
  return { values, observe };
}

export function createRequestTrace(
  requestStart: number,
  protocol: Protocol | null,
  maxAttempts = 0,
  onFirstText?: () => void,
) {
  let attempt: TraceAttempt | undefined;
  let dispatchNumber = 0;
  let sequence = 0;
  let timing: ReturnType<typeof createTransportTiming> | undefined;
  let enqueued: number | undefined;
  const attempts: TraceAttempt[] = [];
  let omittedAttempts = 0;
  let failedAttempts = 0;
  let settledAttemptId: string | undefined;
  let admission: TransportTiming | undefined;
  const round = (at: number | null) =>
    at === null ? null : Number((at - requestStart).toFixed(2));
  let outputParser: SSEParser | undefined;

  function addAttempt(entry: TraceAttempt) {
    if (attempts.length < maxAttempts) attempts.push(entry);
    else if (maxAttempts > 0) {
      attempts[maxAttempts - 1] = entry;
      omittedAttempts++;
    }
  }

  return {
    beginAttempt(
      at: number,
      identity: TracedAttempt,
      prepared?: ReturnType<typeof createTransportTiming>,
    ) {
      if (attempt && attempt.end === undefined) {
        attempt.end = at;
        attempt.error ??= "attempt_replaced";
      }
      timing = prepared ?? createTransportTiming();
      if (admission === timing.values) admission = undefined;
      dispatchNumber = identity.number;
      attempt = { ...identity, number: ++sequence, start: at, transport: timing.values };
      addAttempt(attempt);
      enqueued = undefined;
      outputParser = undefined;
    },
    get transportObserver() {
      return timing?.observe;
    },
    observeAdmission(timingValues: TransportTiming) {
      admission = timingValues;
    },
    observeHeaders(status: number) {
      if (attempt) {
        attempt.transport.response_headers ??= performance.now();
        attempt.status = status;
      }
    },
    bindProviderRequestId(id: string) {
      if (attempt) attempt.providerRequestId = id;
    },
    recordSettlement(id: string) {
      settledAttemptId = id;
    },
    recordPreDispatchFailure(
      id: string,
      channel: string,
      start: number,
      at: number,
      error: string,
      kind: ChannelFailureKind,
      transport: TransportTiming,
    ) {
      failedAttempts++;
      addAttempt({
        id,
        channel,
        number: ++sequence,
        start,
        end: at,
        error,
        transport,
        failureStage: "pre_dispatch",
        failureKind: kind,
      });
    },
    failAttempt(
      at: number,
      stage: ChannelFailureStage,
      kind: ChannelFailureKind,
      code: string,
      providerError?: TraceAttempt["providerError"],
    ) {
      if (!attempt || attempt.end !== undefined) return;
      failedAttempts++;
      attempt.end = at;
      attempt.error = code;
      attempt.failureStage = stage;
      attempt.failureKind = kind;
      if (providerError) attempt.providerError = providerError;
    },
    endAttempt(at: number) {
      if (!attempt || attempt.end !== undefined) return;
      attempt.end = at;
    },
    get measurement() {
      return { requestStart, attempt, enqueued, admission };
    },
    get completedAttempts() {
      return {
        attempts,
        omittedAttempts,
        failedAttempts,
        ...(settledAttemptId ? { settledAttemptId } : {}),
      };
    },
    markUpstreamFirstText() {
      if (protocol && attempt && attempt.firstText === undefined) {
        attempt.firstText = performance.now();
      }
    },
    markFirstTextEnqueued() {
      if (!protocol || enqueued !== undefined) return;
      enqueued = performance.now();
      outputParser = undefined;
      onFirstText?.();
    },
    observeUpstream(events: readonly SSEEvent[], source: Protocol | null) {
      if (
        protocol &&
        attempt &&
        attempt.firstText === undefined &&
        source &&
        events.some((event) => hasSseText(event, source))
      )
        attempt.firstText = performance.now();
    },
    observeEnqueued(chunk: Uint8Array): boolean {
      if (!protocol) return true;
      const at = performance.now();
      try {
        outputParser ??= new SSEParser();
        if (!outputParser.feed(chunk).some((event) => hasSseText(event, protocol))) return false;
        enqueued = at;
        onFirstText?.();
      } catch {
        outputParser = undefined;
        return true;
      }
      outputParser = undefined;
      return true;
    },
    record(spanId: string): FirstTextTraceRecord {
      return {
        span_id: spanId,
        request_start_unix_ms: performance.timeOrigin + requestStart,
        upstream_attempt: attempt
          ? { id: attempt.id, number: dispatchNumber, channel: attempt.channel }
          : null,
        attempt_start_ms: round(attempt?.start ?? null),
        upstream_headers_ms: round(attempt?.transport.response_headers ?? null),
        upstream_first_text_ms: round(attempt?.firstText ?? null),
        gateway_first_text_enqueued_ms: round(enqueued ?? null),
      };
    },
  };
}
