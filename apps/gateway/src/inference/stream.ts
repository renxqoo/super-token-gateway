import { GatewayError, object } from "@gateway/contracts";
import type { Protocol, Usage } from "@gateway/kernel";
import { evaluateErrorRules } from "@gateway/plugin-engine";
import {
  formatProtocolSseError,
  parseProviderJson,
  SSEParser,
  StreamTranslator,
} from "@gateway/protocol";
import type { SSEEvent } from "@gateway/protocol";
import { createStreamChunkReader, createStreamRelay } from "../stream-relay.ts";
import {
  isConversationalOperation,
  mergeUsageSources,
  mergeStreamUsageAliases,
} from "../upstream.ts";
import { channelResponseHeaders } from "./http.ts";
import type { AttemptResponse, StreamResponse } from "./attempt-response.ts";
import type { WasmPrelude } from "./stream-prelude.ts";
import { channelFailureKind } from "./telemetry.ts";
import {
  buildEstimatedStreamUsage,
  buildUsageForOperation,
  canEstimateStreamSettlementError,
} from "./usage.ts";
import { relayMediaStream } from "./media-stream.ts";
import { relayConversationStream } from "./conversation-stream.ts";

async function respondToFastBufferedSse(
  context: AttemptResponse,
  fastSseBody: Uint8Array,
  headers: Record<string, string>,
  takeProbe: () => symbol | undefined,
): Promise<Response> {
  const {
    request: { c, deps, state, requestId, operation, model },
    resources,
    telemetry,
    accounting,
    attempt,
    candidate: cand,
    customer: candBilling,
    prepared,
    pluginSignal,
    chars,
    preRequestAudit,
    observeAttemptSuccess,
    observeAttemptFailure,
    describeFailure,
    describeEventFailure,
    latencyMs,
    startedAt,
  } = context;
  const streamProbe = takeProbe();
  let streamProbeDone = false;
  let streamFailed = false;
  let translating: SSEEvent | undefined;
  let translator: StreamTranslator | undefined;
  let streamAliasUsage: Usage | undefined;
  const nonTerminalChunks: Uint8Array[] = [];
  let nonTerminalBytes = 0;
  let sawFirstText = false;
  const buildErrorBody = (errorFrame: Uint8Array): BodyInit => {
    if (nonTerminalChunks.length === 0) return errorFrame as BodyInit;
    const parts = [...nonTerminalChunks, errorFrame];
    const body = Buffer.concat(parts, nonTerminalBytes + errorFrame.byteLength);
    if (telemetry.requestTrace && telemetry.trackFirstText) {
      if (sawFirstText) telemetry.requestTrace.markFirstTextEnqueued();
      telemetry.requestTrace.observeEnqueued(body);
    }
    return body as BodyInit;
  };
  attempt.finishExchange();
  const releaseRequest = resources.handToStream();
  deps.metrics.activeStreams++;
  try {
    if (pluginSignal.aborted) {
      await accounting.finalizeHold("unknown");
      telemetry.emitAccessLog(503, "plugin_revoked");
      return new Response(
        formatProtocolSseError(
          operation,
          503,
          "plugin_revoked",
          pluginSignal.reason.message,
          requestId,
        ) as BodyInit,
        { status: 200, headers },
      );
    }
    const parser = new SSEParser();
    translator = new StreamTranslator(
      prepared.upstreamProtocol!,
      operation as Protocol,
      prepared.usageDialect,
      model.name,
    );
    const { error_rules, usage_aliases } = prepared.mergedOverrides;
    const hasStreamRules = error_rules.length > 0 || usage_aliases.length > 0;
    const events = parser.feed(fastSseBody, true);
    const terminalChunks: Uint8Array[] = [];
    let terminalBytes = 0;
    for (let i = 0; i < events.length; i++) {
      const event = events[i]!;
      let parsedEv: Record<string, unknown> | undefined;
      if (hasStreamRules && event.data && event.data !== "[DONE]") {
        parsedEv = object(parseProviderJson(event.data));
        const matchedErr = evaluateErrorRules(200, parsedEv, error_rules);
        if (matchedErr) {
          telemetry.recordAttemptFailure(
            "upstream_stream_error",
            matchedErr.retry_kind,
            describeFailure?.(parsedEv),
          );
          throw new GatewayError(
            matchedErr.http_status ?? (matchedErr.retry_kind === "rate_limit" ? 429 : 502),
            "upstream_stream_error",
            matchedErr.message ?? "Upstream stream error",
          );
        }
      }
      if (describeFailure) translating = event;
      const translated = translator.consume(event, telemetry.waitingForRoutingContent);
      translating = undefined;
      if (parsedEv && usage_aliases.length > 0) {
        streamAliasUsage = mergeStreamUsageAliases(
          streamAliasUsage,
          parsedEv,
          usage_aliases,
          translated.outputUsageBoundary,
        );
      }
      if (translated.hasSourceText) telemetry.requestTrace?.markUpstreamFirstText();
      if (translated.hasSourceContent) telemetry.observeRoutingContent();
      if (translated.hasTargetText) sawFirstText = true;
      if (streamProbe && !streamProbeDone && translated.chunks.length && event.event !== "error") {
        streamProbeDone = true;
        observeAttemptSuccess(latencyMs);
      }
      const isTerminal = translated.terminal || translator.holdTerminal;
      for (let j = 0; j < translated.chunks.length; j++) {
        const chunk = translated.chunks[j]!;
        if (isTerminal) {
          terminalChunks.push(chunk);
          terminalBytes += chunk.byteLength;
        } else {
          nonTerminalChunks.push(chunk);
          nonTerminalBytes += chunk.byteLength;
        }
      }
    }
    if (parser.incomplete) {
      throw new GatewayError(
        502,
        "stream_interrupted",
        "Upstream ended with an incomplete stream frame",
      );
    }
    translator.assertComplete();
    const finalized = translator.finalize();
    for (let j = 0; j < finalized.length; j++) {
      const chunk = finalized[j]!;
      terminalChunks.push(chunk);
      terminalBytes += chunk.byteLength;
    }
    const combinedStreamUsage = mergeUsageSources(translator.usage, streamAliasUsage);
    let usage = buildUsageForOperation(
      operation,
      candBilling.price,
      cand.costPrice,
      combinedStreamUsage,
      undefined,
      undefined,
      candBilling.customerSnapshot,
      candBilling.supplierSnapshot,
      candBilling.meteredAllowedHostedTools,
    );
    let estimatedSettlement = false;
    if (!usage && deps.config.streamEstimatedSettlement !== false) {
      const limits = accounting.heldLimits;
      const estimated = buildEstimatedStreamUsage({
        operation,
        customerPrice: candBilling.price,
        supplierPrice: cand.costPrice,
        customerPricing: state.pricingOf(candBilling.price),
        supplierPricing: state.pricingOf(cand.costPrice),
        ...(candBilling.customerSnapshot ? { customerSnapshot: candBilling.customerSnapshot } : {}),
        ...(candBilling.supplierSnapshot ? { supplierSnapshot: candBilling.supplierSnapshot } : {}),
        authorizedHostedTools: candBilling.meteredAllowedHostedTools ?? null,
        observedUsage: combinedStreamUsage,
        preparedBody: prepared.body,
        inputChars: chars,
        outputStats: translator.observedOutput,
        streamCompleted: true,
        heldCustomerAtoms: limits.customerAtoms,
        heldSupplierAtoms: limits.supplierAtoms,
      });
      if (estimated) {
        usage = estimated.usage;
        estimatedSettlement = estimated.estimated;
      }
    }
    if (!usage) {
      streamFailed = true;
      deps.metrics.recordUpstreamError(cand.channel.name, "missing_usage");
      streamProbeDone = true;
      observeAttemptFailure("retryable", undefined, performance.now() - startedAt);
      await accounting.finalizeHold("unknown");
      telemetry.emitAccessLog(502, "invalid_upstream_usage");
      return new Response(
        buildErrorBody(
          formatProtocolSseError(
            operation,
            502,
            "invalid_upstream_usage",
            "Stream finished without usage metadata",
            requestId,
          ),
        ),
        { status: 200, headers },
      );
    }
    if (pluginSignal.aborted) {
      await accounting.finalizeHold("unknown");
      telemetry.emitAccessLog(503, "plugin_revoked");
      return new Response(
        buildErrorBody(
          formatProtocolSseError(
            operation,
            503,
            "plugin_revoked",
            pluginSignal.reason.message,
            requestId,
          ),
        ),
        { status: 200, headers },
      );
    }
    if (resources.serverInterrupted(true)) {
      const quotaLost = resources.quotaLost(true);
      await accounting.finalizeHold("unknown");
      telemetry.emitAccessLog(503, quotaLost ? "rate_limit_unavailable" : "stream_interrupted");
      return new Response(
        buildErrorBody(
          formatProtocolSseError(
            operation,
            503,
            quotaLost ? "rate_limit_unavailable" : "stream_interrupted",
            quotaLost ? "Shared concurrency permit was revoked" : "Gateway interrupted the stream",
            requestId,
          ),
        ),
        { status: 200, headers },
      );
    }
    if (c.req.raw.signal.aborted) {
      if (deps.config.streamEstimatedSettlement !== false) {
        await context.settle({
          usage,
          plugin_audit: preRequestAudit,
          charge_policy: "CHARGE_CUSTOMER",
          moderation_blocked: false,
          allowAfterClientAbort: true,
        });
        if (estimatedSettlement) telemetry.noteEstimatedUsage();
        streamProbeDone = true;
        observeAttemptSuccess(latencyMs);
      } else {
        await accounting.finalizeHold("unknown");
      }
      telemetry.emitAccessLog(499, "client_aborted");
      return new Response(null, { status: 499 });
    }
    telemetry.noteRoutingOutcome("complete");
    await context.settle({
      usage,
      plugin_audit: preRequestAudit,
      charge_policy: "CHARGE_CUSTOMER",
      moderation_blocked: false,
    });
    if (estimatedSettlement) telemetry.noteEstimatedUsage();
    streamProbeDone = true;
    observeAttemptSuccess(latencyMs);
    const totalBytes = nonTerminalBytes + terminalBytes;
    const totalChunks = nonTerminalChunks.length + terminalChunks.length;
    let body: Uint8Array;
    if (totalChunks === 0) {
      body = new Uint8Array(0);
    } else if (totalChunks === 1) {
      body = nonTerminalChunks.length === 1 ? nonTerminalChunks[0]! : terminalChunks[0]!;
    } else if (terminalChunks.length === 0) {
      body = Buffer.concat(nonTerminalChunks, totalBytes);
    } else if (nonTerminalChunks.length === 0) {
      body = Buffer.concat(terminalChunks, totalBytes);
    } else {
      body = Buffer.concat([...nonTerminalChunks, ...terminalChunks], totalBytes);
    }
    if (telemetry.requestTrace && telemetry.trackFirstText) {
      if (sawFirstText) telemetry.requestTrace.markFirstTextEnqueued();
      if (body.byteLength > 0) telemetry.requestTrace.observeEnqueued(body);
    }
    telemetry.emitAccessLog(200, null);
    return new Response(body as BodyInit, { status: 200, headers });
  } catch (err) {
    const abortedByClient = c.req.raw.signal.aborted;
    const status = err instanceof GatewayError ? err.status : 502;
    const code = err instanceof GatewayError ? err.code : "stream_interrupted";
    if (!streamFailed && !accounting.isSettled) {
      let settledByEstimate = false;
      if (
        deps.config.streamEstimatedSettlement !== false &&
        translator &&
        !pluginSignal.aborted &&
        !resources.serverInterrupted(true) &&
        canEstimateStreamSettlementError(code)
      ) {
        let observedUsage: Usage | undefined;
        let aliasConflict = false;
        try {
          observedUsage = mergeUsageSources(translator.usage, streamAliasUsage);
        } catch {
          aliasConflict = true;
        }
        if (!aliasConflict) {
          const limits = accounting.heldLimits;
          const estimated = buildEstimatedStreamUsage({
            operation,
            customerPrice: candBilling.price,
            supplierPrice: cand.costPrice,
            customerPricing: state.pricingOf(candBilling.price),
            supplierPricing: state.pricingOf(cand.costPrice),
            ...(candBilling.customerSnapshot
              ? { customerSnapshot: candBilling.customerSnapshot }
              : {}),
            ...(candBilling.supplierSnapshot
              ? { supplierSnapshot: candBilling.supplierSnapshot }
              : {}),
            authorizedHostedTools: candBilling.meteredAllowedHostedTools ?? null,
            observedUsage,
            preparedBody: prepared.body,
            inputChars: chars,
            outputStats: translator.observedOutput,
            streamCompleted: false,
            heldCustomerAtoms: limits.customerAtoms,
            heldSupplierAtoms: limits.supplierAtoms,
          });
          if (estimated) {
            try {
              await context.settle({
                usage: estimated.usage,
                plugin_audit: preRequestAudit,
                charge_policy: "CHARGE_CUSTOMER",
                moderation_blocked: false,
                allowAfterClientAbort: true,
              });
              if (estimated.estimated) telemetry.noteEstimatedUsage();
              settledByEstimate = true;
            } catch {
              settledByEstimate = false;
            }
          }
        }
      }
      if (!settledByEstimate) {
        await accounting.finalizeHold("unknown");
      }
      if (!abortedByClient && !pluginSignal.aborted && code !== "ledger_commit_failed") {
        const details =
          code === "provider_stream_error" && translating
            ? (describeEventFailure?.(translating.data) ?? describeFailure?.())
            : undefined;
        telemetry.recordAttemptFailure(code, channelFailureKind(code), details);
        deps.metrics.recordUpstreamError(cand.channel.name, "stream_interrupted");
        streamProbeDone = true;
        observeAttemptFailure("retryable", undefined, performance.now() - startedAt);
      }
    }
    if (abortedByClient) {
      telemetry.emitAccessLog(499, "client_aborted");
      return new Response(null, { status: 499 });
    }
    telemetry.emitAccessLog(status, code);
    const message = err instanceof Error ? err.message : "Stream failed";
    return new Response(
      buildErrorBody(formatProtocolSseError(operation, status, code, message, requestId)),
      { status: 200, headers },
    );
  } finally {
    deps.metrics.activeStreams--;
    if (!streamProbeDone && streamProbe) deps.router.releaseProbe(cand.channel.id, streamProbe);
    releaseRequest();
  }
}

export function respondToStream(
  context: AttemptResponse,
  upstreamBody: ReadableStream<Uint8Array> | null,
  tracedUpstreamProtocol: Protocol | null,
  wasmPrelude: WasmPrelude | undefined,
  isWasmStreamProvider: boolean,
  takeProbe: () => symbol | undefined,
  baseAttemptTimeoutMs: number,
  fastSseBody?: Uint8Array,
): Response | Promise<Response> {
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
  const isConversation =
    isConversationalOperation(operation) &&
    (prepared.upstreamProtocol !== null || isWasmStreamProvider);
  if (
    fastSseBody !== undefined &&
    isConversation &&
    candidate.route.plugin_pipeline.length === 0 &&
    !isWasmStreamProvider &&
    !prepared.isBedrockStream &&
    prepared.upstreamProtocol !== null
  ) {
    return respondToFastBufferedSse(context, fastSseBody, headers, takeProbe);
  }
  const resolvedUpstreamBody =
    upstreamBody ?? (fastSseBody !== undefined ? new Response(fastSseBody as BodyInit).body : null);
  const streamProbe = takeProbe();

  attempt.cancelTimeout();
  const streamLinked = resources.link([c.req.raw.signal, pluginSignal], true, false);
  const streamSignal = streamLinked.signal;
  resources.setAbortNotify((reason) => {
    attempt.abort(reason);
    streamLinked.trigger(reason);
  });
  let durationTimeout: Error | undefined;
  const expireDuration = () => {
    durationTimeout = new Error("Upstream stream exceeded its maximum duration");
    streamLinked.trigger(durationTimeout);
    attempt.abort(durationTimeout);
  };
  const cancelDuration =
    maxStreamDurationMs >= 5000
      ? state.deadlines.scheduleCoarse(maxStreamDurationMs, expireDuration)
      : state.deadlines.schedule(maxStreamDurationMs, expireDuration);

  let clientCancelled = false;
  let streamProbeDone = false;
  const streamContext: StreamResponse = {
    context,
    tracedUpstreamProtocol,
    streamSignal,
    get durationTimeout() {
      return durationTimeout;
    },
    settle(details) {
      if (details.allowAfterClientAbort) {
        pluginSignal.throwIfAborted();
        if (resources.serverInterrupted(true)) {
          throw resources.serverAdmissionError(resources.quotaLost(true));
        }
      } else {
        streamSignal.throwIfAborted();
      }
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

  const stream = createStreamRelay(
    async (writer) => {
      const reader = resolvedUpstreamBody?.getReader();
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
