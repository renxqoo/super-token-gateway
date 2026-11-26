import type { Protocol } from "@gateway/kernel";
import { formatProtocolSseError } from "@gateway/protocol";
import { createStreamChunkReader, createStreamRelay } from "../stream-relay.ts";
import { isConversationalOperation } from "../upstream.ts";
import { channelResponseHeaders } from "./http.ts";
import type { AttemptResponse, StreamResponse } from "./attempt-response.ts";
import type { WasmPrelude } from "./stream-prelude.ts";
import { relayMediaStream } from "./media-stream.ts";
import { relayConversationStream } from "./conversation-stream.ts";

export function respondToStream(
  context: AttemptResponse,
  upstreamBody: ReadableStream<Uint8Array> | null,
  tracedUpstreamProtocol: Protocol | null,
  wasmPrelude: WasmPrelude | undefined,
  isWasmStreamProvider: boolean,
  takeProbe: () => symbol | undefined,
  baseAttemptTimeoutMs: number,
): Response {
  const {
    request: { c, deps, state, requestId, trace, operation, maxStreamDurationMs },
    resources,
    telemetry,
    accounting,
    attempt,
    candidate,
    prepared,
    pluginSignal,
    observeAttemptSuccess,
    latencyMs,
  } = context;
  telemetry.setStage("stream");
  let headers: Record<string, string>;
  try {
    headers = channelResponseHeaders(requestId, trace, candidate.channel, {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache",
      ...(prepared.droppedFields.length > 0
        ? { "x-gateway-dropped-fields": prepared.droppedFields.join(",") }
        : {}),
    });
  } catch (error) {
    attempt.abort(error);
    void upstreamBody?.cancel(error).catch(() => undefined);
    throw error;
  }
  const streamProbe = takeProbe();

  attempt.cancelTimeout();
  const streamLinked = resources.link([c.req.raw.signal, pluginSignal], true, false);
  const streamSignal = streamLinked.signal;
  resources.setAbortNotify((reason) => {
    attempt.abort(reason);
    streamLinked.trigger(reason);
  });
  const cancelDuration =
    maxStreamDurationMs >= 5000
      ? state.deadlines.scheduleCoarse(maxStreamDurationMs, () => {
          const reason = new Error("Upstream stream exceeded its maximum duration");
          streamLinked.trigger(reason);
          attempt.abort(reason);
        })
      : state.deadlines.schedule(maxStreamDurationMs, () => {
          const reason = new Error("Upstream stream exceeded its maximum duration");
          streamLinked.trigger(reason);
          attempt.abort(reason);
        });

  let clientCancelled = false;
  let streamProbeDone = false;
  const streamContext: StreamResponse = {
    context,
    tracedUpstreamProtocol,
    streamSignal,
    settle(details) {
      streamSignal.throwIfAborted();
      return context.settle(details);
    },
    clientCancelled: () => clientCancelled || c.req.raw.signal.aborted,
    markProbeDone() {
      streamProbeDone = true;
    },
    confirmStreamProbe() {
      if (streamProbe && !streamProbeDone) {
        streamProbeDone = true;
        observeAttemptSuccess(latencyMs);
      }
    },
  };

  const releaseRequest = resources.handToStream();
  deps.metrics.activeStreams++;
  function finish() {
    deps.metrics.activeStreams--;
    cancelDuration();
    streamLinked.dispose();
    if (!streamProbeDone && streamProbe)
      deps.router.releaseProbe(candidate.channel.id, streamProbe);
    releaseRequest();
  }

  const isConversation =
    isConversationalOperation(operation) &&
    (prepared.upstreamProtocol !== null || isWasmStreamProvider);
  const stream = createStreamRelay(
    async (writer) => {
      const reader = upstreamBody?.getReader();
      const chunkReader = reader
        ? createStreamChunkReader(
            reader,
            deps.config.streamIdleTimeoutMs ?? baseAttemptTimeoutMs,
            () => {
              if (!streamSignal.aborted)
                telemetry.recordAttemptFailure("upstream_stream_idle_timeout", "timeout");
              attempt.abort(new Error("Upstream stream exceeded its idle timeout"));
            },
            streamSignal,
          )
        : null;
      try {
        if (isConversation) {
          await relayConversationStream(
            streamContext,
            writer,
            chunkReader,
            reader,
            operation,
            wasmPrelude,
          );
        } else {
          await relayMediaStream(streamContext, writer, chunkReader);
        }
      } finally {
        chunkReader?.dispose();
        if (reader) {
          await reader.cancel().catch(() => undefined);
          reader.releaseLock();
        }
      }
    },
    {
      signal: streamSignal,
      cancel() {
        clientCancelled = true;
        const reason = new Error("client_cancelled");
        streamLinked.trigger(reason);
        attempt.abort(reason);
      },
      fail: () => accounting.finalizeHold("unknown"),
      abortedFrame() {
        if (c.req.raw.signal.aborted) return undefined;
        if (pluginSignal.aborted) {
          telemetry.emitAccessLog(503, "plugin_revoked");
          return formatProtocolSseError(
            operation,
            503,
            "plugin_revoked",
            pluginSignal.reason.message,
            requestId,
          );
        }
        const quotaLost = resources.quotaLost(true);
        return formatProtocolSseError(
          operation,
          503,
          quotaLost ? "rate_limit_unavailable" : "stream_interrupted",
          quotaLost ? "Shared concurrency permit was revoked" : "Gateway interrupted the stream",
          requestId,
        );
      },
      finish,
      ...(telemetry.requestTrace && telemetry.trackFirstText
        ? {
            onEnqueued: telemetry.requestTrace.observeEnqueued,
            onFirstTextEnqueued: telemetry.requestTrace.markFirstTextEnqueued,
          }
        : {}),
    },
  );
  return new Response(stream, { status: 200, headers });
}
