import { GatewayError, object } from "@gateway/contracts";
import type { Protocol, Usage } from "@gateway/kernel";
import type { PluginAuditEntry } from "@gateway/ledger";
import {
  evaluateErrorRules,
  invokeWasmProviderStreamEvent,
  WasmCapacityError,
  WasmPluginFaultError,
} from "@gateway/plugin-engine";
import {
  encodeSSE,
  formatProtocolSseError,
  parseProviderJson,
  SSEParser,
  StreamTranslator,
  validateUsageObject,
} from "@gateway/protocol";
import type { SSEEvent } from "@gateway/protocol";
import { createResponsesModeration } from "../responses-moderation.ts";
import { createStreamChunkReader, createTerminalBuffer } from "../stream-relay.ts";
import type { StreamWriter } from "../stream-relay.ts";
import {
  BedrockEventStreamDecoder,
  buildTextDeltaSse,
  extractChunkTextFromEncodedSse,
  inspectAnthropicBlockEvent,
  mergeUsageSources,
  mergeStreamUsageAliases,
  stripTextFromSseChunk,
} from "../upstream.ts";
import { classifyPluginBlock } from "./policy.ts";
import { buildUsageForOperation, buildModerationUsageFallback } from "./usage.ts";
import { channelFailureKind } from "./telemetry.ts";
import type { WasmPrelude } from "./stream-prelude.ts";
import type { StreamResponse } from "./attempt-response.ts";

const EMPTY_BYTES = new Uint8Array(0);

export async function relayConversationStream(
  stream: StreamResponse,
  controller: StreamWriter,
  chunkReader: ReturnType<typeof createStreamChunkReader> | null,
  reader: ReadableStreamDefaultReader<Uint8Array> | undefined,
  clientProtocol: Protocol,
  wasmPrelude: WasmPrelude | undefined,
) {
  const {
    context: {
      request: { deps, operation, requestId, snap, billingAccount, model },
      accounting,
      telemetry,
      attempt,
      candidate: cand,
      customer: candBilling,
      prepared,
      preRequestAudit,
      chars,
      describeFailure,
      describeEventFailure,
      latencyMs,
      startedAt,
      observeAttemptSuccess,
      observeAttemptFailure,
    },
    streamSignal,
    settle,
    clientCancelled,
    confirmStreamProbe,
    markProbeDone,
  } = stream;
  const provider = prepared.resolvedProvider;
  const wasmProvider =
    provider.kind === "WASM" && provider.config.hooks.includes("adapt_stream_event")
      ? provider
      : null;
  const translator = wasmProvider
    ? null
    : new StreamTranslator(
        prepared.upstreamProtocol!,
        clientProtocol,
        prepared.usageDialect,
        model.name,
      );
  const parser = new SSEParser();
  const bedrockDecoder = prepared.isBedrockStream ? new BedrockEventStreamDecoder() : null;
  const { error_rules, usage_aliases } = prepared.mergedOverrides;
  const hasStreamRules = error_rules.length > 0 || usage_aliases.length > 0;
  let streamAliasUsage: Usage | undefined;
  let translating: SSEEvent | undefined;
  let wasmState = "";
  let wasmUsage: Usage | undefined;
  let wasmTerminal = false;
  let streamId: string | undefined;
  const getStreamId = () => (streamId ??= `gen_${requestId.replaceAll("-", "")}`);
  const hasPlugins = cand.route.plugin_pipeline.length > 0;
  const openContentSession = () =>
    deps.pluginEngine.createStreamSession(
      cand.route.plugin_pipeline,
      snap.plugins,
      {
        request_id: requestId,
        billing_account_id: billingAccount.id,
        model: model.name,
        operation,
      },
      () => attempt.abort(new Error("moderation_blocked")),
      streamSignal,
    );
  const session = hasPlugins && clientProtocol !== "responses" ? openContentSession() : null;
  const responsesModeration =
    hasPlugins && clientProtocol === "responses"
      ? createResponsesModeration(openContentSession)
      : null;
  const heldApprovedChunks = hasPlugins ? createTerminalBuffer() : null;
  const heldTerminalChunks = createTerminalBuffer();
  const streamPluginAudit: PluginAuditEntry[] = [];
  let observedOutputChars = 0;
  let activeTextBlockIndex = -1;
  let streamFailed = false;
  let blockedEarly = false;

  async function handleStreamChunk(chunk: Uint8Array, isTerminal: boolean) {
    if (wasmProvider && isTerminal) {
      heldTerminalChunks.push(chunk);
      return;
    }
    if (responsesModeration) {
      if (responsesModeration.defer(chunk)) {
        heldTerminalChunks.push(chunk);
        return;
      }
      const result = await responsesModeration.push(chunk);
      if (!result) {
        if (isTerminal) heldTerminalChunks.push(chunk);
        else await controller.enqueue(responsesModeration.rewrite(chunk));
      } else {
        observedOutputChars += result.chars;
        if (result.blocked) {
          blockedEarly = true;
          return;
        }
        if (result.rendered) {
          if (isTerminal) heldApprovedChunks!.push(result.rendered);
          else await controller.enqueue(result.rendered);
        }
      }
      return;
    }
    if (!session) {
      if (isTerminal) heldTerminalChunks.push(chunk);
      else await controller.enqueue(chunk);
      return;
    }
    if (!wasmProvider && clientProtocol === "messages") {
      const blk = inspectAnthropicBlockEvent(chunk);
      if (blk?.kind === "text_start") {
        activeTextBlockIndex = blk.index;
      } else if (blk?.kind === "block_stop" && blk.index === activeTextBlockIndex) {
        heldTerminalChunks.push(chunk);
        return;
      }
    }
    const deltaText = extractChunkTextFromEncodedSse(chunk, clientProtocol);
    if (deltaText === null) {
      if (isTerminal) heldTerminalChunks.push(chunk);
      else await controller.enqueue(chunk);
      return;
    }
    observedOutputChars += deltaText.length;
    const mod = await session.push(deltaText);
    if (mod.blocked) {
      blockedEarly = true;
      return;
    }
    if (mod.emit.length > 0) {
      const rendered = buildTextDeltaSse(
        clientProtocol,
        model.name,
        mod.emit,
        getStreamId(),
        activeTextBlockIndex >= 0 ? activeTextBlockIndex : 0,
      );
      if (isTerminal) heldApprovedChunks!.push(rendered);
      else await controller.enqueue(rendered);
    }
    const remainder = wasmProvider ? null : stripTextFromSseChunk(chunk, clientProtocol);
    if (remainder) {
      if (isTerminal) heldTerminalChunks.push(remainder);
      else await controller.enqueue(remainder);
    }
  }

  async function processWasmEvent(
    wasmProvider: Extract<typeof provider, { kind: "WASM" }>,
    ev: SSEEvent,
  ) {
    if (!ev.data) return;
    const cached =
      wasmPrelude?.event === ev.event && wasmPrelude.data === ev.data
        ? wasmPrelude.result
        : undefined;
    wasmPrelude = undefined;
    const out =
      cached ??
      (await invokeWasmProviderStreamEvent({
        executor: deps.wasmExecutor,
        plugin: wasmProvider.plugin,
        config: wasmProvider.config,
        tenantId: billingAccount.id,
        signal: streamSignal,
        ...(deps.config.pluginSigningPublicKeyPem
          ? { signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem }
          : {}),
        input: {
          operation,
          upstream_model: cand.route.upstream_model,
          client_protocol: clientProtocol,
          event: ev.event,
          data: ev.data,
          state: wasmState,
        },
      }));
    if (out.state !== undefined) wasmState = out.state;
    if (out.retry_kind) {
      telemetry.recordAttemptFailure(
        "upstream_stream_error",
        out.retry_kind,
        describeEventFailure?.(ev.data) ?? describeFailure?.(),
      );
      throw new GatewayError(
        out.retry_kind === "rate_limit" ? 429 : 502,
        "upstream_stream_error",
        out.error_message ?? "Upstream stream reported error",
      );
    }
    if (out.chunks?.length) confirmStreamProbe();
    if (out.usage !== undefined) {
      const validated = validateUsageObject(out.usage);
      wasmUsage = { ...wasmUsage, ...validated };
    }
    for (const chunkObj of out.chunks ?? []) {
      const evName =
        clientProtocol === "messages" && typeof chunkObj.type === "string"
          ? chunkObj.type
          : undefined;
      const encoded = encodeSSE(chunkObj, evName);
      const terminal =
        (clientProtocol === "responses" &&
          ["response.completed", "response.incomplete"].includes(String(chunkObj.type))) ||
        (clientProtocol === "messages" && chunkObj.type === "message_stop");
      await handleStreamChunk(encoded, terminal);
      if (blockedEarly) break;
    }
    wasmTerminal ||= out.done === true;
  }

  function translateEvent(translator: StreamTranslator, ev: SSEEvent) {
    let parsedEv: Record<string, unknown> | undefined;
    if (hasStreamRules && ev.data && ev.data !== "[DONE]") {
      parsedEv = object(parseProviderJson(ev.data));
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
    if (describeFailure) translating = ev;
    const translated = translator.consume(ev);
    translating = undefined;
    if (parsedEv && usage_aliases.length > 0) {
      streamAliasUsage = mergeStreamUsageAliases(
        streamAliasUsage,
        parsedEv,
        usage_aliases,
        translated.outputUsageBoundary,
      );
    }
    return translated;
  }

  try {
    if (!reader || !chunkReader) {
      throw new GatewayError(502, "empty_stream", "Upstream returned empty body");
    }
    while (true) {
      let step: Awaited<ReturnType<typeof chunkReader.read>>;
      try {
        step = await chunkReader.read();
      } catch (readErr) {
        if (session?.isBlocked() || responsesModeration?.isBlocked()) {
          blockedEarly = true;
          break;
        }
        throw readErr;
      }
      if (step.done && (bedrockDecoder || clientCancelled())) break;
      const events = step.done
        ? parser.feed(EMPTY_BYTES, true)
        : bedrockDecoder
          ? bedrockDecoder.feed(step.value)
          : parser.feed(step.value);
      if (wasmProvider) {
        telemetry.requestTrace?.observeUpstream(events, prepared.upstreamProtocol);
      }
      let batchOut: Uint8Array[] | undefined;
      let batchBytes = 0;
      let batchHasText = false;
      const flushBatchOut = async () => {
        if (!batchOut || batchOut.length === 0) return;
        const out = batchOut.length === 1 ? batchOut[0]! : Buffer.concat(batchOut, batchBytes);
        const hasText = batchHasText;
        batchOut = undefined;
        batchBytes = 0;
        batchHasText = false;
        const pending = controller.enqueue(out, hasText);
        if (pending) await pending;
      };
      for (const event of events) {
        if (wasmProvider) {
          await processWasmEvent(wasmProvider, event);
        } else if (translator) {
          let translated: ReturnType<StreamTranslator["consume"]>;
          try {
            translated = translateEvent(translator, event);
          } catch (error) {
            await flushBatchOut();
            throw error;
          }
          if (translated.hasSourceText) telemetry.requestTrace?.markUpstreamFirstText();
          if (!step.done && translated.chunks.length && event.event !== "error")
            confirmStreamProbe();
          const isTerminal = step.done || translated.terminal || translator.holdTerminal;
          const knownHasText =
            !session && !responsesModeration ? Boolean(translated.hasTargetText) : undefined;
          for (const chunk of translated.chunks) {
            if (!session && !responsesModeration) {
              if (isTerminal) heldTerminalChunks.push(chunk);
              else {
                if (knownHasText) batchHasText = true;
                (batchOut ??= []).push(chunk);
                batchBytes += chunk.byteLength;
                if (batchOut.length >= 3 || batchBytes >= 1024) {
                  await flushBatchOut();
                }
              }
            } else {
              await handleStreamChunk(chunk, isTerminal);
              if (blockedEarly) break;
            }
          }
        }
        if (blockedEarly) break;
      }
      if (!blockedEarly) await flushBatchOut();
      if (blockedEarly) {
        await reader.cancel().catch(() => undefined);
        break;
      }
      if (step.done) break;
    }
    if (clientCancelled()) {
      await (session?.cancelPending() ?? responsesModeration?.cancelPending());
      await accounting.finalizeHold("unknown");
      telemetry.emitAccessLog(499, "client_aborted");
      return;
    }
    if (!blockedEarly && !session?.isBlocked() && !responsesModeration?.isBlocked()) {
      if (bedrockDecoder?.incomplete || parser.incomplete) {
        throw new GatewayError(
          502,
          "stream_interrupted",
          "Upstream ended with an incomplete stream frame",
        );
      }
      if (translator) translator.assertComplete();
      else if (!wasmTerminal) {
        throw new GatewayError(
          502,
          "stream_interrupted",
          "Provider stream ended without a terminal event",
        );
      }
    }
    const combinedStreamUsage = translator
      ? mergeUsageSources(translator.usage, streamAliasUsage)
      : wasmUsage;

    if (session || responsesModeration) {
      const finalMod = responsesModeration
        ? await responsesModeration.finalize()
        : { ...(await session!.finalize()), frames: [] };
      streamPluginAudit.push(...finalMod.audit);
      if (finalMod.blocked) {
        const classified = classifyPluginBlock(
          finalMod.outcome_kind,
          finalMod.http_status,
          finalMod.safe_message,
        );
        const effectiveChargePolicy = classified.isContentDecision
          ? finalMod.charge_policy
          : "PLATFORM_ABSORB";
        if (classified.isContentDecision) {
          telemetry.moderationBlocked();
        }
        const modUsage = buildModerationUsageFallback(
          combinedStreamUsage,
          chars ?? 0,
          observedOutputChars,
        );
        const commitRes = await settle({
          usage: modUsage,
          plugin_audit: [...preRequestAudit, ...streamPluginAudit],
          charge_policy: effectiveChargePolicy,
          moderation_blocked: true,
        });
        if (!classified.isContentDecision) {
          deps.metrics.recordPlatformAbsorb(billingAccount.id, commitRes.supplier_cost_atoms);
        }
        markProbeDone();
        observeAttemptSuccess(latencyMs);
        telemetry.emitAccessLog(classified.status, finalMod.rule_code ?? classified.code);
        await controller.enqueue(
          formatProtocolSseError(
            operation,
            classified.status,
            classified.code,
            classified.message,
            requestId,
            finalMod.rule_code ? { rule_code: finalMod.rule_code } : undefined,
          ),
        );
        controller.close();
        return;
      }
      for (const frame of finalMod.frames) {
        if (wasmProvider) await controller.enqueue(frame);
        else heldApprovedChunks!.push(frame);
      }
      if (finalMod.emit.length > 0) {
        const chunk = buildTextDeltaSse(
          clientProtocol,
          model.name,
          finalMod.emit,
          getStreamId(),
          activeTextBlockIndex >= 0 ? activeTextBlockIndex : 0,
        );
        if (wasmProvider) await controller.enqueue(chunk);
        else heldApprovedChunks!.push(chunk);
      }
    }

    if (translator) {
      heldTerminalChunks.push(...translator.finalize());
    } else if (clientProtocol === "chat") {
      heldTerminalChunks.push(encodeSSE("[DONE]"));
    }

    const usage = buildUsageForOperation(
      operation,
      candBilling.price,
      cand.costPrice,
      combinedStreamUsage,
    );
    if (!usage) {
      streamFailed = true;
      deps.metrics.recordUpstreamError(cand.channel.name, "missing_usage");
      markProbeDone();
      observeAttemptFailure("retryable", undefined, performance.now() - startedAt);
      await accounting.finalizeHold("unknown");
      telemetry.emitAccessLog(502, "invalid_upstream_usage");
      await controller.enqueue(
        formatProtocolSseError(
          operation,
          502,
          "invalid_upstream_usage",
          "Stream finished without usage metadata",
          requestId,
        ),
      );
      controller.close();
      return;
    }

    await settle({
      usage,
      plugin_audit:
        streamPluginAudit.length === 0
          ? preRequestAudit
          : preRequestAudit.length === 0
            ? streamPluginAudit
            : [...preRequestAudit, ...streamPluginAudit],
      charge_policy: "CHARGE_CUSTOMER",
      moderation_blocked: false,
    });

    markProbeDone();
    observeAttemptSuccess(latencyMs);
    if (heldApprovedChunks) {
      for (const chunk of heldApprovedChunks) {
        await controller.enqueue(chunk);
      }
    }
    if (!responsesModeration && heldTerminalChunks.length > 1) {
      await controller.enqueue(Buffer.concat([...heldTerminalChunks]));
    } else {
      for (const chunk of heldTerminalChunks) {
        await controller.enqueue(responsesModeration?.rewrite(chunk, true) ?? chunk);
      }
    }
    telemetry.emitAccessLog(200, null);
    controller.close();
  } catch (err) {
    await (session?.cancelPending() ?? responsesModeration?.cancelPending());
    const abortedByClient = clientCancelled();
    const localFault = err instanceof WasmCapacityError || err instanceof WasmPluginFaultError;
    const status =
      err instanceof WasmCapacityError ? 503 : err instanceof GatewayError ? err.status : 502;
    const code =
      err instanceof WasmCapacityError
        ? "provider_capacity_unavailable"
        : err instanceof WasmPluginFaultError
          ? "provider_plugin_fault"
          : err instanceof GatewayError
            ? err.code
            : "stream_interrupted";
    if (!streamFailed) {
      await accounting.finalizeHold("unknown");
      if (
        !abortedByClient &&
        !streamSignal.aborted &&
        !localFault &&
        code !== "ledger_commit_failed"
      ) {
        const details =
          code === "provider_stream_error" && translating
            ? (describeEventFailure?.(translating.data) ?? describeFailure?.())
            : undefined;
        telemetry.recordAttemptFailure(code, channelFailureKind(code), details);
        deps.metrics.recordUpstreamError(cand.channel.name, "stream_interrupted");
        markProbeDone();
        observeAttemptFailure("retryable", undefined, performance.now() - startedAt);
      }
    }
    if (abortedByClient) {
      telemetry.emitAccessLog(499, "client_aborted");
      return;
    }
    if (localFault && !streamSignal.aborted) telemetry.recordAttemptFailure(code);
    telemetry.emitAccessLog(status, code);
    try {
      const message = err instanceof Error ? err.message : "Stream failed";
      await controller.enqueue(formatProtocolSseError(operation, status, code, message, requestId));
      controller.close();
    } catch {
      void 0;
    }
  }
}
