import { GatewayError } from "@gateway/contracts";
import type { Protocol } from "@gateway/kernel";
import {
  evaluateErrorRules,
  invokeWasmProviderStreamEvent,
  WasmCapacityError,
  WasmPluginFaultError,
} from "@gateway/plugin-engine";
import type { MatchedProviderError } from "@gateway/plugin-engine";
import { parseProviderJson, SSEParser } from "@gateway/protocol";
import type { SSEEvent } from "@gateway/protocol";
import { peekStream } from "../stream-relay.ts";
import type { TraceAttempt } from "../trace-types.ts";
import { BedrockEventStreamDecoder, isConversationalOperation } from "../upstream.ts";
import type { AttemptResponse, RetryAttempt } from "./attempt-response.ts";

export type WasmPrelude = {
  event: string;
  data: string;
  result: Awaited<ReturnType<typeof invokeWasmProviderStreamEvent>>;
};

export async function inspectStreamPrelude(
  context: AttemptResponse,
  isWasmStreamProvider: boolean,
  tracedUpstreamProtocol: Protocol | null,
): Promise<
  | {
      upstreamBody: ReadableStream<Uint8Array> | null;
      fastSseBody?: Uint8Array;
      wasmPrelude: WasmPrelude | undefined;
    }
  | RetryAttempt
> {
  const {
    request: { deps, operation, billingAccount, routing },
    resources,
    telemetry,
    accounting,
    candidate: cand,
    prepared,
    attempt,
    pluginSignal,
    fastResult,
    upstreamResp,
    isUpstreamSse,
    startedAt,
    upstreamStatus,
    describeFailure,
    describeEventFailure,
    releaseBackups,
    releaseProbe,
    completeProbe,
    observeAttemptFailure,
    rejectedOutcome,
  } = context;
  const mediaTerminal = prepared.mergedOverrides.media_stream_terminal;
  const configuredTerminalEvent =
    mediaTerminal && "event" in mediaTerminal ? mediaTerminal.event : undefined;
  const needsPrelude =
    (isWasmStreamProvider ||
      prepared.mergedOverrides.error_rules.some(
        (rule) =>
          !routing.failover.requireExecutionEvidence ||
          (rule.when_http_status ?? [200]).includes(upstreamStatus),
      )) &&
    (isUpstreamSse || prepared.isBedrockStream);
  if (fastResult.fast && isUpstreamSse && !prepared.isBedrockStream && !needsPrelude) {
    return { upstreamBody: null, fastSseBody: fastResult.body, wasmPrelude: undefined };
  }
  let upstreamBody: ReadableStream<Uint8Array> | null = fastResult.fast
    ? new Response(fastResult.body as BodyInit).body
    : upstreamResp!.body;
  let wasmPrelude: WasmPrelude | undefined;
  if (upstreamBody && needsPrelude) {
    telemetry.setStage("stream_prelude");
    const parser = new SSEParser();
    const bedrock = prepared.isBedrockStream ? new BedrockEventStreamDecoder() : null;
    let logicalError: MatchedProviderError | null = null;
    let providerError: TraceAttempt["providerError"];
    try {
      const peeked = await peekStream(
        upstreamBody,
        (chunk): SSEEvent | undefined => {
          const events = bedrock ? bedrock.feed(chunk) : parser.feed(chunk);
          telemetry.requestTrace?.observeUpstream(events, tracedUpstreamProtocol);
          return events.find((frame) => frame.data !== "");
        },
        attempt.signal(),
      );
      upstreamBody = peeked.body;
      const first = peeked.found;
      if (first && isWasmStreamProvider && prepared.resolvedProvider.kind === "WASM") {
        const adaptationStarted = telemetry.routingProcessingStart;
        const result = await invokeWasmProviderStreamEvent({
          executor: deps.wasmExecutor,
          plugin: prepared.resolvedProvider.plugin,
          config: prepared.resolvedProvider.config,
          tenantId: billingAccount.id,
          signal: attempt.signal(),
          ...(deps.config.pluginSigningPublicKeyPem
            ? { signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem }
            : {}),
          input: {
            operation,
            upstream_model: cand.route.upstream_model,
            client_protocol: isConversationalOperation(operation) ? operation : null,
            event: first.event,
            data: first.data,
            state: "",
          },
        });
        telemetry.excludeRoutingProcessing(adaptationStarted);
        wasmPrelude = { event: first.event, data: first.data, result };
        if (result.retry_kind) {
          providerError = describeEventFailure?.(first.data) ?? describeFailure?.();
          logicalError = {
            retry_kind: result.retry_kind,
            ...(result.retry_after_ms === undefined
              ? {}
              : { retry_after_ms: result.retry_after_ms }),
            ...(result.error_message === undefined ? {} : { message: result.error_message }),
          };
        }
      } else if (
        first &&
        first.data !== "[DONE]" &&
        !(first.event === configuredTerminalEvent && !first.data.trimStart().startsWith("{"))
      ) {
        const data = parseProviderJson(first.data);
        logicalError = evaluateErrorRules(
          upstreamStatus,
          data,
          prepared.mergedOverrides.error_rules,
        );
        if (logicalError) providerError = describeFailure?.(data);
      }
    } catch (err) {
      attempt.dispose();
      await upstreamBody?.cancel().catch(() => undefined);
      resources.clearChannelLimit();
      pluginSignal.throwIfAborted();
      const quotaLost = resources.quotaLost(true);
      if (resources.interrupted(true)) {
        releaseProbe();
        releaseBackups();
        await accounting.finalizeHold("unknown");
        throw resources.admissionError(quotaLost);
      }
      if (err instanceof WasmCapacityError) {
        releaseProbe();
        throw new GatewayError(503, "provider_capacity_unavailable", err.message);
      }
      if (err instanceof WasmPluginFaultError) {
        releaseProbe();
        throw new GatewayError(502, "provider_plugin_fault", err.message);
      }
      if (attempt.aborted) {
        telemetry.recordAttemptFailure("upstream_timeout");
        deps.metrics.recordUpstreamError(cand.channel.name, "timeout");
        observeAttemptFailure("retryable", undefined, performance.now() - startedAt);
        completeProbe();
        accounting.noteUnknownAttempt();
        return {
          outcome: "UNKNOWN",
          error: new GatewayError(
            504,
            "upstream_timeout",
            attempt.reason instanceof Error
              ? attempt.reason.message
              : "Upstream stream prelude timed out",
          ),
        };
      }
      throw new GatewayError(
        502,
        "upstream_stream_error",
        "Upstream stream prelude could not be read",
      );
    }
    if (logicalError) {
      telemetry.recordAttemptFailure(
        `logical_${logicalError.retry_kind}`,
        logicalError.retry_kind,
        providerError,
      );
      const outcome = rejectedOutcome(logicalError.execution === "NOT_EXECUTED");
      attempt.dispose();
      await upstreamBody?.cancel().catch(() => undefined);
      resources.clearChannelLimit();
      if (logicalError.retry_kind === "client_error") {
        await accounting.finishFailedRequest();
        throw new GatewayError(
          logicalError.http_status ?? 400,
          "upstream_client_error",
          logicalError.message ?? "Upstream rejected the request",
        );
      }
      deps.metrics.recordUpstreamError(cand.channel.name, `logical_${logicalError.retry_kind}`);
      observeAttemptFailure(
        logicalError.retry_kind,
        logicalError.retry_after_ms,
        performance.now() - startedAt,
      );
      completeProbe();
      return {
        outcome,
        error: new GatewayError(
          logicalError.http_status ?? (logicalError.retry_kind === "rate_limit" ? 429 : 502),
          logicalError.retry_kind === "rate_limit" ? "upstream_rate_limited" : "upstream_error",
          logicalError.message ?? "Upstream stream rejected the request",
        ),
      };
    }
  }
  return { upstreamBody, wasmPrelude };
}
