import { GatewayError, object } from "@gateway/contracts";
import type { Usage } from "@gateway/kernel";
import { evaluateErrorRules, getAtPath } from "@gateway/plugin-engine";
import type { MatchedProviderError } from "@gateway/plugin-engine";
import { encodeSSE, extractUsage, parseProviderJson, SSEParser } from "@gateway/protocol";
import type { SSEEvent } from "@gateway/protocol";
import { createStreamChunkReader, createTerminalBuffer } from "../stream-relay.ts";
import type { StreamWriter } from "../stream-relay.ts";
import { mergeUsageWithAliases } from "../upstream.ts";
import { buildUsageForOperation, recordImageArtifact, mediaCompletionEvents } from "./usage.ts";
import type { StreamResponse } from "./attempt-response.ts";

const utf8Encoder = new TextEncoder();

export async function relayMediaStream(
  stream: StreamResponse,
  controller: StreamWriter,
  chunkReader: ReturnType<typeof createStreamChunkReader> | null,
) {
  const {
    context: {
      request: { deps, operation, requestId },
      accounting,
      telemetry,
      candidate: cand,
      customer: candBilling,
      prepared,
      upstreamStatus,
      authorizedImages,
      preRequestAudit,
      latencyMs,
      startedAt,
      describeFailure,
      observeAttemptSuccess,
      observeAttemptFailure,
    },
    tracedUpstreamProtocol,
    streamSignal,
    settle,
    markProbeDone,
    clientCancelled,
  } = stream;
  const { usageDialect } = prepared;
  const { error_rules: errorRules, media_stream_terminal: mediaTerminal } =
    prepared.mergedOverrides;
  const configuredTerminalEvent =
    mediaTerminal && "event" in mediaTerminal ? mediaTerminal.event : undefined;
  const terminalByType = (type: string) =>
    type === "done" || type === "completed" || type === mediaCompletionEvents[operation];
  const parser = new SSEParser();
  let observedUsage: Usage | undefined;
  const heldChunks = createTerminalBuffer();
  let terminal = false;
  let sawDone = false;
  const imageStream = operation === "images" || operation === "image_edits";
  let deliveredImages: Set<string> | undefined;
  const failure: { provider?: MatchedProviderError } = {};
  const processEvent = (event: SSEEvent) => {
    if (sawDone && event.data) {
      throw new GatewayError(
        502,
        "unexpected_trailing_event",
        "Upstream sent media data after [DONE]",
      );
    }
    if (event.event === "error" && !event.data.trimStart().startsWith("{")) {
      throw new GatewayError(
        502,
        "upstream_stream_error",
        "Upstream reported a media stream error",
      );
    }
    const namedTerminal = configuredTerminalEvent === event.event;
    if (event.data === "[DONE]") {
      terminal = true;
      sawDone = true;
    } else if (event.data && (!namedTerminal || event.data.trimStart().startsWith("{"))) {
      const parsed = object(parseProviderJson(event.data));
      const providerError = evaluateErrorRules(upstreamStatus, parsed, errorRules);
      if (providerError || event.event === "error" || parsed.type === "error" || parsed.error) {
        telemetry.recordAttemptFailure(
          "upstream_stream_error",
          providerError?.retry_kind ?? "protocol",
          describeFailure?.(parsed),
        );
        if (providerError) failure.provider = providerError;
        throw new GatewayError(
          providerError?.http_status ??
            (providerError?.retry_kind === "client_error"
              ? 400
              : providerError?.retry_kind === "rate_limit"
                ? 429
                : 502),
          "upstream_stream_error",
          providerError?.message ?? "Upstream reported a media stream error",
        );
      }
      const baseUsage =
        usageDialect === "custom"
          ? observedUsage
          : (extractUsage(parsed, usageDialect, observedUsage) ?? observedUsage);
      observedUsage =
        mergeUsageWithAliases(baseUsage, parsed, prepared.mergedOverrides.usage_aliases) ??
        baseUsage;
      if (
        imageStream &&
        (event.event === mediaCompletionEvents[operation] ||
          parsed.type === mediaCompletionEvents[operation])
      ) {
        deliveredImages ??= new Set();
        recordImageArtifact(deliveredImages, parsed);
      }
      terminal ||= mediaTerminal
        ? "parts" in mediaTerminal &&
          getAtPath(parsed, mediaTerminal.parts) === mediaTerminal.equals
        : typeof parsed.type === "string" && terminalByType(parsed.type);
    }
    terminal ||= namedTerminal || (!mediaTerminal && terminalByType(event.event));
    const bytes = utf8Encoder.encode(event.wire);
    if (!terminal) return controller.enqueue(bytes);
    heldChunks.push(bytes);
  };
  try {
    if (!chunkReader) {
      throw new GatewayError(502, "empty_stream", "Upstream returned empty body");
    }
    while (true) {
      const step = await chunkReader.read();
      if (step.done) break;
      const events = parser.feed(step.value);
      telemetry.requestTrace?.observeUpstream(events, tracedUpstreamProtocol);
      for (const event of events) {
        const enqueue = processEvent(event);
        if (enqueue) await enqueue;
      }
    }
    if (clientCancelled()) {
      await accounting.finalizeHold("unknown");
      telemetry.emitAccessLog(499, "client_aborted");
      return;
    }
    const events = parser.feed(new Uint8Array(), true);
    telemetry.requestTrace?.observeUpstream(events, tracedUpstreamProtocol);
    for (const event of events) {
      const enqueue = processEvent(event);
      if (enqueue) await enqueue;
    }
    if (!terminal || parser.incomplete) {
      throw new GatewayError(
        502,
        "stream_interrupted",
        "Media stream ended without a complete terminal event",
      );
    }
    const usage = buildUsageForOperation(
      operation,
      candBilling.price,
      cand.costPrice,
      observedUsage,
      deliveredImages?.size,
      authorizedImages,
    );
    if (!usage) {
      await accounting.finalizeHold("unknown");
      telemetry.emitAccessLog(502, "invalid_upstream_usage");
      await controller.enqueue(
        encodeSSE(
          {
            error: {
              code: "invalid_upstream_usage",
              message: "Media stream completed without usage",
              request_id: requestId,
            },
          },
          "error",
        ),
      );
      controller.close();
      return;
    }
    await settle({
      usage,
      plugin_audit: preRequestAudit,
      charge_policy: "CHARGE_CUSTOMER",
      moderation_blocked: false,
    });
    markProbeDone();
    observeAttemptSuccess(latencyMs);
    for (const ch of heldChunks) await controller.enqueue(ch);
    telemetry.emitAccessLog(200, null);
    controller.close();
  } catch (err) {
    const abortedByClient = clientCancelled();
    await accounting.finalizeHold("unknown");
    if (abortedByClient) {
      telemetry.emitAccessLog(499, "client_aborted");
      return;
    }
    const code = err instanceof GatewayError ? err.code : "stream_interrupted";
    const kind = failure.provider?.retry_kind ?? "retryable";
    if (code !== "ledger_commit_failed" && !streamSignal.aborted && kind !== "client_error") {
      telemetry.recordAttemptFailure(code);
      deps.metrics.recordUpstreamError(cand.channel.name, "media_stream_error");
      markProbeDone();
      observeAttemptFailure(kind, failure.provider?.retry_after_ms, performance.now() - startedAt);
    }
    telemetry.emitAccessLog(err instanceof GatewayError ? err.status : 502, code);
    try {
      await controller.enqueue(
        encodeSSE(
          {
            error: {
              code,
              message: err instanceof Error ? err.message : "Media stream failed",
              request_id: requestId,
            },
          },
          "error",
        ),
      );
      controller.close();
    } catch {
      void 0;
    }
  }
}
