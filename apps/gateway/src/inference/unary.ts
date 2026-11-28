import { GatewayError, object } from "@gateway/contracts";
import type { Usage } from "@gateway/kernel";
import {
  applyResponseTransforms,
  evaluateErrorRules,
  invokeWasmProviderResponse,
  WasmCapacityError,
  WasmPluginFaultError,
  type MatchedProviderError,
} from "@gateway/plugin-engine";
import {
  copyExact,
  decodeResponse,
  encodeResponse,
  extractUsage,
  formatProtocolError,
  parseProviderJson,
  stringifyExact,
  validateUsageObject,
} from "@gateway/protocol";
import { extractUpstreamTaskId } from "../async-tasks.ts";
import { fastUuid } from "../proxy-state.ts";
import {
  extractResponseText,
  isConversationalOperation,
  mergeUsageWithAliases,
  replaceResponseText,
} from "../upstream.ts";
import {
  channelJsonHeaders,
  channelResponseHeaders,
  responseHeaders,
  sanitizeHttpErrorStatus,
} from "./http.ts";
import { readResponseBytesFast, utf8FatalDecoder } from "./body.ts";
import { classifyPluginBlock } from "./policy.ts";
import { buildUsageForOperation, countImageArtifacts } from "./usage.ts";
import type { InferenceResources } from "./resources.ts";
import type { AttemptResponse, RetryAttempt } from "./attempt-response.ts";

export async function handleUnaryResponse(
  context: AttemptResponse,
): Promise<Response | RetryAttempt> {
  const {
    request: { c, deps, snap, operation, requestId, trace, billingAccount, model },
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
    isUpstreamJson,
    isFakeJsonOnStream,
    latencyMs,
    authorizedImages,
    preRequestAudit,
    settle,
    rejectedOutcome,
    releaseBackups,
    releaseProbe,
    completeProbe,
    observeAttemptFailure,
    observeAttemptSuccess,
    describeFailure,
  } = context;
  telemetry.setStage("response_body");
  const allowClientAbortGrace = attempt.hasPostDispatchGrace;
  let postResponse: ReturnType<InferenceResources["link"]> | undefined;
  const getPostResponseSignal = () => {
    postResponse ??= resources.link(
      allowClientAbortGrace ? [pluginSignal] : [c.req.raw.signal, pluginSignal],
    );
    return postResponse.signal;
  };
  try {
    if (allowClientAbortGrace ? resources.serverInterrupted() : resources.interrupted()) {
      throw allowClientAbortGrace
        ? resources.serverAdmissionError(resources.quotaLost())
        : resources.admissionError(resources.quotaLost());
    }
    let respBytes: Uint8Array;
    if (fastResult.fast) {
      if (attempt.aborted) throw attempt.reason ?? new Error("Upstream request aborted");
      if (fastResult.body.byteLength > deps.config.maxBodyBytes) {
        throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
      }
      respBytes = fastResult.body;
    } else {
      try {
        respBytes = await readResponseBytesFast(
          upstreamResp!,
          deps.config.maxBodyBytes,
          attempt.signal(),
          deps.transport.bufferedBody(upstreamResp!),
        );
      } catch (error) {
        if (
          !c.req.raw.signal.aborted &&
          !pluginSignal.aborted &&
          !resources.interrupted(true) &&
          !(error instanceof GatewayError && error.code === "body_too_large")
        ) {
          telemetry.noteRoutingOutcome("failed");
        }
        if (c.req.raw.signal.aborted && attempt.aborted && !(error instanceof GatewayError)) {
          throw new GatewayError(499, "client_aborted", "Client disconnected");
        }
        throw error;
      }
    }
    telemetry.observeRoutingUnaryBody();
    attempt.finishExchange();
    pluginSignal.throwIfAborted();

    if (!isUpstreamJson) {
      releaseBackups();
      const usage = buildUsageForOperation(
        operation,
        candBilling.price,
        cand.costPrice,
        undefined,
        undefined,
        undefined,
        candBilling.customerSnapshot,
        candBilling.supplierSnapshot,
        candBilling.meteredAllowedHostedTools,
      );
      if (!usage) {
        await accounting.finalizeHold("unknown");
        throw new GatewayError(
          502,
          "invalid_upstream_usage",
          "Non-JSON response requires character or request pricing basis",
        );
      }
      const headers = channelResponseHeaders(requestId, trace, cand.channel, {
        "content-type": upstreamContentType,
      });
      telemetry.noteRoutingOutcome("complete");
      await settle({
        usage,
        plugin_audit: preRequestAudit,
        charge_policy: "CHARGE_CUSTOMER",
        moderation_blocked: false,
        allowAfterClientAbort: allowClientAbortGrace,
      });
      pluginSignal.throwIfAborted();
      observeAttemptSuccess(latencyMs);
      completeProbe();
      if (c.req.raw.signal.aborted) {
        telemetry.emitAccessLog(499, "client_aborted");
        return new Response(null, { status: 499 });
      }
      telemetry.emitAccessLog(200, null);
      return new Response(respBytes as BodyInit, {
        status: 200,
        headers,
      });
    }

    let rawJson: Record<string, unknown>;
    try {
      rawJson = object(parseProviderJson(utf8FatalDecoder.decode(respBytes)));
    } catch (error) {
      telemetry.recordAttemptFailure("invalid_upstream");
      if (error instanceof TypeError) {
        throw new GatewayError(502, "invalid_upstream_json", "Provider returned invalid UTF-8");
      }
      throw error;
    }
    const provider = prepared.resolvedProvider;
    let wasmResponse: Awaited<ReturnType<typeof invokeWasmProviderResponse>> | undefined;
    if (provider.kind === "WASM" && provider.config.hooks.includes("adapt_response")) {
      const headers = fastResult.fast ? fastResult.headers : fastResult.response.headers;
      try {
        wasmResponse = await invokeWasmProviderResponse({
          executor: deps.wasmExecutor,
          plugin: provider.plugin,
          config: provider.config,
          tenantId: billingAccount.id,
          signal: getPostResponseSignal(),
          ...(deps.config.pluginSigningPublicKeyPem
            ? { signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem }
            : {}),
          input: {
            operation,
            upstream_model: cand.route.upstream_model,
            client_protocol: isConversationalOperation(operation) ? operation : null,
            status: upstreamStatus,
            headers: headers instanceof Headers ? Object.fromEntries(headers) : { ...headers },
            body: rawJson,
          },
        });
      } catch (err) {
        pluginSignal.throwIfAborted();
        releaseBackups();
        await accounting.finalizeHold("unknown");
        if (allowClientAbortGrace ? resources.serverInterrupted() : resources.interrupted()) {
          releaseProbe();
          throw allowClientAbortGrace
            ? resources.serverAdmissionError(resources.quotaLost())
            : resources.admissionError(resources.quotaLost());
        }
        if (err instanceof WasmCapacityError) {
          releaseProbe();
          throw new GatewayError(503, "provider_capacity_unavailable", err.message);
        }
        if (err instanceof WasmPluginFaultError) {
          releaseProbe();
          throw new GatewayError(502, "provider_plugin_fault", err.message);
        }
        observeAttemptFailure("retryable", undefined, latencyMs);
        completeProbe();
        throw new GatewayError(
          502,
          "provider_adapter_failed",
          err instanceof Error ? err.message : "WASM provider adapt_response failed",
        );
      }

      if (wasmResponse.retry_kind) {
        return await handleLogicalError(
          {
            retry_kind: wasmResponse.retry_kind,
            ...(wasmResponse.status !== undefined && { http_status: wasmResponse.status }),
            ...(wasmResponse.error_message !== undefined && {
              message: wasmResponse.error_message,
            }),
            ...(wasmResponse.retry_after_ms !== undefined && {
              retry_after_ms: wasmResponse.retry_after_ms,
            }),
          },
          rawJson,
          true,
        );
      }
    } else if (prepared.mergedOverrides.error_rules.length > 0) {
      const matched = evaluateErrorRules(
        upstreamStatus,
        rawJson,
        prepared.mergedOverrides.error_rules,
      );
      if (matched) return await handleLogicalError(matched, rawJson, false);
    }

    releaseBackups();
    if (isFakeJsonOnStream) {
      await accounting.finalizeHold("unknown");
      throw new GatewayError(
        502,
        "invalid_upstream",
        "Upstream returned JSON body instead of event stream",
      );
    }
    const { response_transforms, usage_aliases } = prepared.mergedOverrides;
    let responseModified = Boolean(wasmResponse || response_transforms.length > 0);
    const adaptedJson = wasmResponse
      ? (wasmResponse.body ?? rawJson)
      : response_transforms.length > 0
        ? applyResponseTransforms(rawJson, response_transforms)
        : rawJson;

    let rawUsage: Usage | undefined;
    try {
      if (wasmResponse?.usage !== undefined) {
        rawUsage = validateUsageObject(wasmResponse.usage);
      } else if (prepared.usageDialect !== "custom") {
        rawUsage = extractUsage(wasmResponse ? rawJson : adaptedJson, prepared.usageDialect);
      }
      if (!wasmResponse && usage_aliases.length > 0) {
        rawUsage = mergeUsageWithAliases(rawUsage, adaptedJson, usage_aliases);
      }
    } catch (err) {
      await accounting.finalizeHold("unknown");
      observeAttemptFailure("retryable", undefined, latencyMs);
      completeProbe();
      throw err;
    }

    let clientJson: Record<string, unknown>;
    if (
      isConversationalOperation(operation) &&
      prepared.upstreamProtocol &&
      prepared.upstreamProtocol !== operation
    ) {
      const completion = decodeResponse(
        adaptedJson,
        prepared.upstreamProtocol,
        prepared.usageDialect,
      );
      clientJson = encodeResponse(completion, operation, model.name);
      responseModified = true;
    } else {
      clientJson = wasmResponse ? copyExact(adaptedJson, { ...adaptedJson }) : adaptedJson;
      if (typeof clientJson.model === "string" && clientJson.model !== model.name) {
        clientJson.model = model.name;
        responseModified = true;
      }
    }

    const usage = buildUsageForOperation(
      operation,
      candBilling.price,
      cand.costPrice,
      rawUsage,
      (operation === "images" || operation === "image_edits") && Array.isArray(clientJson.data)
        ? countImageArtifacts(clientJson.data)
        : undefined,
      authorizedImages,
      candBilling.customerSnapshot,
      candBilling.supplierSnapshot,
      candBilling.meteredAllowedHostedTools,
    );
    if (!usage) {
      await accounting.finalizeHold("unknown");
      observeAttemptFailure("retryable", undefined, latencyMs);
      completeProbe();
      throw new GatewayError(502, "invalid_upstream_usage", "Upstream omitted usage metadata");
    }
    telemetry.noteRoutingOutcome("complete");

    let block:
      | (ReturnType<typeof classifyPluginBlock> & {
          rule: string;
          chargePolicy: "CHARGE_CUSTOMER" | "PLATFORM_ABSORB";
        })
      | undefined;
    let allAudit = preRequestAudit;

    if (
      cand.route.plugin_pipeline.length > 0 &&
      isConversationalOperation(operation) &&
      deps.pluginEngine.hasActiveHook("on_response", cand.route.plugin_pipeline, snap.plugins)
    ) {
      const respText = extractResponseText(clientJson, operation);
      const postRes = await deps.pluginEngine.runUnary(
        "on_response",
        cand.route.plugin_pipeline,
        snap.plugins,
        {
          request_id: requestId,
          billing_account_id: billingAccount.id,
          model: model.name,
          operation,
        },
        respText,
        getPostResponseSignal(),
      );
      allAudit = [...preRequestAudit, ...postRes.audit];
      if (postRes.blocked) {
        const decision = classifyPluginBlock(
          postRes.outcome_kind,
          postRes.http_status,
          postRes.safe_message,
        );
        block = {
          ...decision,
          rule: postRes.rule_code ?? decision.code,
          chargePolicy: decision.isContentDecision ? postRes.charge_policy : "PLATFORM_ABSORB",
        };
        if (decision.isContentDecision) telemetry.moderationBlocked();
      } else if (postRes.text !== respText) {
        clientJson = replaceResponseText(clientJson, operation, postRes.text);
        responseModified = true;
      }
    }

    const headers = block
      ? channelResponseHeaders(requestId, trace, cand.channel, {
          ...block.extraHeaders,
          ...(prepared.droppedFields.length > 0
            ? { "x-gateway-dropped-fields": prepared.droppedFields.join(",") }
            : {}),
        })
      : channelJsonHeaders(requestId, trace, cand.channel, prepared.droppedFields);
    const upstreamTaskId =
      operation === "videos" && deps.asyncTasks && !block
        ? extractUpstreamTaskId(
            clientJson,
            provider.kind === "DECLARATIVE" ? (provider.config.async_task_config ?? null) : null,
          )
        : null;

    const commitRes = await settle({
      usage,
      plugin_audit: allAudit,
      charge_policy: block?.chargePolicy ?? "CHARGE_CUSTOMER",
      moderation_blocked: block !== undefined,
      allowAfterClientAbort: allowClientAbortGrace,
      ...(upstreamTaskId
        ? {
            async_task: {
              id: `vid_${fastUuid().replaceAll("-", "")}`,
              upstream_task_id: upstreamTaskId,
              result_json: clientJson,
            },
          }
        : {}),
    });
    if (block && !block.isContentDecision) {
      deps.metrics.recordPlatformAbsorb(billingAccount.id, commitRes.supplier_cost_atoms);
    }

    if (upstreamTaskId) {
      const taskId = commitRes.async_task_id;
      if (!taskId) {
        throw new GatewayError(503, "ledger_commit_failed", "Ledger could not confirm video task");
      }
      clientJson = {
        ...clientJson,
        gateway_task_id: taskId,
        ...(typeof clientJson.id !== "string" ? { id: taskId } : {}),
      };
      responseModified = true;
    }

    pluginSignal.throwIfAborted();
    observeAttemptSuccess(latencyMs);
    completeProbe();
    if (c.req.raw.signal.aborted) {
      telemetry.emitAccessLog(499, "client_aborted");
      return new Response(null, { status: 499 });
    }
    if (block) {
      telemetry.emitAccessLog(block.status, block.rule);
      return c.json(
        formatProtocolError(
          operation,
          block.status,
          block.code,
          block.message,
          requestId,
          block.rule ? { rule_code: block.rule } : undefined,
        ),
        block.status as 400,
        headers,
      );
    }
    telemetry.emitAccessLog(200, null);
    if (respBytes[0] === 0xef && respBytes[1] === 0xbb && respBytes[2] === 0xbf) {
      respBytes = respBytes.subarray(3);
    }
    return new Response(responseModified ? stringifyExact(clientJson) : (respBytes as BodyInit), {
      status: 200,
      headers,
    });
  } finally {
    attempt.dispose();
    postResponse?.dispose();
  }

  async function handleLogicalError(
    logical: MatchedProviderError,
    body: Record<string, unknown>,
    fromWasm: boolean,
  ): Promise<Response | RetryAttempt> {
    const { retry_kind, http_status, message, retry_after_ms, execution } = logical;
    if (operation === "videos" && execution === "NOT_EXECUTED") {
      resources.setVideoAdmissionRetained(false);
    }
    const failure = `logical_${retry_kind}`;
    telemetry.recordAttemptFailure(failure, retry_kind, describeFailure?.(body));
    const outcome = rejectedOutcome(execution === "NOT_EXECUTED");
    if (retry_kind === "client_error") {
      releaseProbe();
      releaseBackups();
      await accounting.finishFailedRequest();
      if (c.req.raw.signal.aborted) {
        telemetry.emitAccessLog(499, "client_aborted");
        return new Response(null, { status: 499 });
      }
      const status = fromWasm ? sanitizeHttpErrorStatus(http_status) : (http_status ?? 400);
      telemetry.emitAccessLog(status, "upstream_client_error");
      return c.json(
        formatProtocolError(
          operation,
          status,
          "upstream_client_error",
          message ?? "Upstream rejected request",
          requestId,
        ),
        status as 400,
        responseHeaders(requestId, trace),
      );
    }

    deps.metrics.recordUpstreamError(cand.channel.name, failure);
    observeAttemptFailure(retry_kind, retry_after_ms, latencyMs);
    completeProbe();
    const fallbackStatus = retry_kind === "rate_limit" ? 429 : 502;
    return {
      outcome,
      error: new GatewayError(
        fromWasm
          ? sanitizeHttpErrorStatus(http_status, fallbackStatus)
          : (http_status ?? fallbackStatus),
        retry_kind === "rate_limit"
          ? "upstream_rate_limited"
          : retry_kind === "auth_error"
            ? "upstream_auth_error"
            : "upstream_error",
        message ?? "Upstream logical error",
      ),
    };
  }
}
