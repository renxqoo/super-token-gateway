import {
  GatewayError,
  canonical,
  extractUsage,
  providerObject as object,
  sha256,
  parseProviderJson,
  stringifyExact,
  copyExact,
} from "@gateway/kernel";
import type { Operation, Protocol, Route, Usage, Finality } from "@gateway/kernel";
import {
  StreamTranslator,
  MediaStreamMeter,
  blockedPrompt,
  decodeResponse,
  encodeResponse,
  prepareRequest,
} from "@gateway/connectors";
import { readBounded } from "@gateway/runtime";
import type { StreamMeter } from "@gateway/connectors";
import type { RequestRecord } from "@gateway/storage";
import type { RatePermit } from "@gateway/runtime";
import type { Auth, Dependencies } from "./context.ts";
import { chooseRoutes } from "./routing.ts";
import { sendUpstream } from "./upstream.ts";
import { publishEvidence } from "./metering.ts";
import { prepareStream, relayStream } from "./relay-stream.ts";
import { relayBinary } from "./relay-binary.ts";
import type { BatchInput } from "./batch-input.ts";
import { rewriteBatchInput } from "./batch-input.ts";
import { applyOutputBound } from "./input-policy.ts";

const textProtocols = new Set<string>(["chat", "responses", "messages", "gemini"]);
export interface ExecuteInput {
  auth: Auth;
  request: Request;
  requestId: string;
  operation: Operation;
  model: string;
  body: Record<string, unknown>;
  form?: FormData;
  pinnedProviderId?: string;
  pinnedUpstreamModel?: string;
  batchInput?: BatchInput;
  reserveMemory?: (bytes: number) => void;
  transformResponse?: (
    body: Record<string, unknown>,
    route: Route,
    attemptId: string,
    batch?: { endpoint: string; upstreamModel: string },
  ) => Promise<Record<string, unknown>>;
}

export async function execute(deps: Dependencies, input: ExecuteInput): Promise<Response> {
  await deps.usage.producer.ready();
  deps = { ...deps, usage: deps.usage.scope(input.operation) };
  const { credential, project } = input.auth;
  if (!["ADMIN", "INVOKER"].includes(credential.role ?? "INVOKER"))
    throw new GatewayError(403, "role_denied", "Credential role cannot invoke models");
  if (
    !credential.operations.includes(input.operation) ||
    (credential.models.length && !credential.models.includes(input.model))
  )
    throw new GatewayError(
      403,
      "scope_denied",
      "Credential does not allow this operation or model",
    );
  const lifecycle = deps.lifecycle.enter();
  const controller = lifecycle.controller;
  const onClientAbort = () => controller.abort(new Error("Client disconnected"));
  input.request.signal.addEventListener("abort", onClientAbort, { once: true });
  if (input.request.signal.aborted) controller.abort(new Error("Client disconnected"));
  const timer = setTimeout(
    () => controller.abort(new Error("Request deadline reached")),
    deps.config.TOTAL_TIMEOUT_MS,
  );
  let projectRelease: RatePermit | undefined;
  const additionalPermits: RatePermit[] = [];
  let providerRelease: (() => Promise<void>) | undefined;
  let cleaned = false;
  let streaming = false;
  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    clearTimeout(timer);
    input.request.signal.removeEventListener("abort", onClientAbort);
    if (projectRelease) {
      try {
        if (!admittedId || replayed) await projectRelease.adjustTokens(0);
        else {
          const summary = deps.usage.summary();
          if (summary.complete) await projectRelease.adjustTokens(summary.tokens);
        }
      } catch {
        deps.metrics.increment("token_reconciliation_failures_total");
      }
    }
    for (const permit of additionalPermits) {
      try {
        if (!admittedId || replayed) await permit.adjustTokens(0);
        else {
          const summary = deps.usage.summary();
          if (summary.complete) await permit.adjustTokens(summary.tokens);
        }
      } catch {
        deps.metrics.increment("token_reconciliation_failures_total");
      }
      await permit();
    }
    await providerRelease?.();
    await projectRelease?.();
    lifecycle.release();
  };
  let admittedId: string | undefined;
  let replayed = false;
  try {
    controller.signal.throwIfAborted();
    const fingerprint = sha256(canonical(input.body));
    const modelPolicy = await deps.store.model(input.model);
    const outputBound = applyOutputBound(
      input.body,
      input.operation,
      modelPolicy.max_output_tokens,
    );
    const estimatedTokens =
      (new TextEncoder().encode(JSON.stringify(input.body)).byteLength + outputBound) *
      deps.config.MAX_ATTEMPTS;
    projectRelease = await deps.limits.enter(
      `project:${project.id}`,
      input.requestId,
      project.rpm,
      project.concurrency,
      deps.config.TOTAL_TIMEOUT_MS + 10000,
      estimatedTokens,
      project.tpm,
    );
    const scopes: { scope: string; rpm: number; tpm: number; concurrency: number }[] = [];
    if (project.organization_id)
      scopes.push({
        scope: `organization:${project.organization_id}`,
        rpm: project.organization_rpm!,
        tpm: project.organization_tpm!,
        concurrency: project.organization_concurrency!,
      });
    if (credential.principal_id)
      scopes.push({
        scope: `principal:${credential.principal_id}`,
        rpm: credential.principal_rpm!,
        tpm: credential.principal_tpm!,
        concurrency: credential.principal_concurrency!,
      });
    if (credential.rpm || credential.tpm || credential.concurrency)
      scopes.push({
        scope: `key:${credential.id}`,
        rpm: credential.rpm ?? project.rpm,
        tpm: credential.tpm ?? project.tpm,
        concurrency: credential.concurrency ?? project.concurrency,
      });
    for (const scope of scopes)
      additionalPermits.push(
        await deps.limits.enter(
          scope.scope,
          input.requestId,
          scope.rpm,
          scope.concurrency,
          deps.config.TOTAL_TIMEOUT_MS + 10000,
          estimatedTokens,
          scope.tpm,
        ),
      );
    const key = input.request.headers.get("idempotency-key");
    if (key && !/^[\x21-\x7e]{1,200}$/.test(key))
      throw new GatewayError(
        400,
        "invalid_idempotency_key",
        "Idempotency key must contain 1 to 200 printable ASCII characters",
      );
    let routes = (await chooseRoutes(deps, input.model, input.operation)).filter(
      (route) => !input.pinnedProviderId || route.provider_id === input.pinnedProviderId,
    );
    if (!routes.length)
      throw new GatewayError(
        422,
        "resource_provider_mismatch",
        "No route matches the original resource provider",
      );
    const currency = input.request.headers.get("x-gateway-currency") ?? project.billing_currency;
    if (!/^[A-Z]{3,8}$/.test(currency))
      throw new GatewayError(
        400,
        "invalid_currency",
        "Billing currency requires an uppercase currency code",
      );
    const quote = await deps.funds.quote(
      input.model,
      input.operation,
      currency,
      [...new Set(routes.map((route) => route.provider_id))],
      input.body,
      outputBound,
    );
    routes = routes.filter((route) => Object.hasOwn(quote.prices, route.provider_id));
    let admission: { record: RequestRecord; created: boolean; attempt_id?: string } | undefined;
    let lastError: unknown = new GatewayError(503, "no_route", "No eligible upstream");
    let physicalAttempts = 0;
    const retryAt = new Map<string, number>();
    // 候选数量与同渠道重试共享一次总尝试预算。
    for (
      let step = 0;
      physicalAttempts < deps.config.MAX_ATTEMPTS &&
      step < routes.length + deps.config.MAX_ATTEMPTS;
      step++
    ) {
      controller.signal.throwIfAborted();
      const route = routes[step % routes.length];
      if (!route) break;
      let target: Protocol | Operation = input.operation;
      let outbound: Record<string, unknown> = copyExact(input.body, {
        ...input.body,
        model: route.upstream_model,
      });
      if (textProtocols.has(input.operation)) {
        try {
          const prepared = prepareRequest(
            input.body,
            input.operation as Protocol,
            route.kind,
            route.upstream_model,
          );
          outbound = prepared.body;
          target = prepared.protocol;
        } catch (error) {
          lastError = error;
          if (routes.length === 1) throw error;
          continue;
        }
      } else if (
        route.kind !== "openai" &&
        !(route.kind === "ollama" && input.operation === "embeddings") &&
        !(input.operation === "videos" && route.kind === "gemini")
      ) {
        lastError = new GatewayError(
          422,
          "unsupported_capability",
          "This provider does not support the requested native operation",
        );
        continue;
      }
      if (input.operation === "batches" || input.operation === "files") delete outbound.model;
      if (input.operation === "videos" && route.kind === "gemini") {
        if (
          Object.keys(input.body).some(
            (field) => !["model", "prompt", "seconds", "aspect_ratio", "size"].includes(field),
          )
        )
          throw new GatewayError(
            422,
            "conversion_loss",
            "This video adapter supports prompt, duration and aspect ratio; unsupported fields cannot be converted",
          );
        const size = input.body.size;
        const aspect = size === "1280x720" ? "16:9" : size === "720x1280" ? "9:16" : undefined;
        if (
          (size !== undefined && !aspect) ||
          (aspect && input.body.aspect_ratio && aspect !== input.body.aspect_ratio)
        )
          throw new GatewayError(
            422,
            "conversion_loss",
            "Video size cannot be represented by this provider",
          );
        outbound = {
          instances: [{ prompt: input.body.prompt }],
          parameters: {
            durationSeconds: Number(input.body.seconds ?? 8),
            sampleCount: 1,
            ...(aspect || input.body.aspect_ratio
              ? { aspectRatio: aspect ?? input.body.aspect_ratio }
              : {}),
          },
        };
      }
      if (input.pinnedUpstreamModel && route.upstream_model !== input.pinnedUpstreamModel) continue;
      const batchFile = input.batchInput
        ? await rewriteBatchInput(deps, input.auth, input.batchInput, route)
        : undefined;
      try {
        providerRelease = await deps.limits.enter(
          `provider:${route.provider_id}`,
          `${input.requestId}:${step}`,
          route.rpm,
          route.concurrency,
          deps.config.TOTAL_TIMEOUT_MS + 10000,
        );
      } catch (error) {
        lastError = error;
        continue;
      }
      if (!(await deps.limits.probe(route.provider_id, deps.config.TOTAL_TIMEOUT_MS + 10000))) {
        await providerRelease?.();
        providerRelease = undefined;
        continue;
      }
      const delay = (retryAt.get(route.provider_id) ?? 0) - Date.now();
      if (delay > 0)
        await new Promise<void>((resolve, reject) => {
          const abort = () => {
            clearTimeout(wait);
            reject(controller.signal.reason);
          };
          const wait = setTimeout(() => {
            controller.signal.removeEventListener("abort", abort);
            resolve();
          }, delay);
          controller.signal.addEventListener("abort", abort, { once: true });
          if (controller.signal.aborted) abort();
        });
      controller.signal.throwIfAborted();
      let attemptId: string;
      if (!admission) {
        admission = await deps.store.admitInitial(
          input.requestId,
          credential,
          input.operation,
          input.model,
          key ? sha256(key) : null,
          fingerprint,
          route,
          quote,
        );
        admittedId = admission.record.id;
        if (!admission.created) {
          replayed = true;
          return Response.json(
            {
              error: {
                code: "already_accepted",
                message: "Request already exists; response content is not stored",
              },
              request_id: input.requestId,
              original_request_id: admission.record.id,
              state: admission.record.state,
            },
            {
              status: 409,
              headers: {
                "content-type": "application/json",
                "x-request-id": input.requestId,
                "x-gateway-request-id": admission.record.id,
              },
            },
          );
        }
        attemptId = admission.attempt_id!;
        physicalAttempts = 1;
      } else {
        attemptId = await deps.store.startAttempt(admission.record.id, ++physicalAttempts, route);
      }
      deps.usage.register(attemptId);
      deps.metrics.increment("upstream_attempts_total");
      const started = performance.now();
      let response: Response | undefined;
      let headersTimer: ReturnType<typeof setTimeout> | undefined;
      const attemptController = new AbortController();
      const abortAttempt = () => attemptController.abort(controller.signal.reason);
      controller.signal.addEventListener("abort", abortAttempt, { once: true });
      if (controller.signal.aborted) abortAttempt();
      let recorded = false;
      let resultReceived = false;
      let observedUsage: Usage | undefined;
      let observedFinality: Finality = "UNKNOWN";
      let finalizationStarted = false;
      try {
        const stream = input.body.stream === true || input.body.stream_format === "sse";
        let payload: BodyInit = stringifyExact(outbound);
        let contentType: string | undefined;
        if (input.form && !(input.operation === "videos" && route.kind === "gemini")) {
          const form = new FormData();
          for (const [name, value] of input.form.entries())
            if (name !== "model")
              form.append(name, name === "file" && batchFile ? batchFile.file : value);
          if (input.operation !== "files") form.set("model", route.upstream_model);
          const encoded = new Request("http://serialization.invalid", {
            method: "POST",
            body: form,
          });
          contentType = encoded.headers.get("content-type") ?? undefined;
          payload = await encoded.arrayBuffer();
        }
        headersTimer = setTimeout(
          () => attemptController.abort(new Error("Upstream response header deadline")),
          deps.config.HEADERS_TIMEOUT_MS,
        );
        response = await sendUpstream(
          route,
          deps.config,
          target,
          payload,
          stream,
          attemptController.signal,
          input.request.headers,
          contentType,
        );
        clearTimeout(headersTimer);
        const providerRequestId = response.headers.get("x-request-id") ?? undefined;
        if (!response.ok) {
          let errorUsage: Usage | undefined;
          let classifiedError = false;
          try {
            const bytes = await readBounded(
              response.body,
              Math.min(65536, deps.config.MAX_RESPONSE_BYTES),
              AbortSignal.any([
                attemptController.signal,
                AbortSignal.timeout(deps.config.HEADERS_TIMEOUT_MS),
              ]),
              input.reserveMemory,
            );
            errorUsage = extractUsage(
              object(parseProviderJson(new TextDecoder("utf-8", { fatal: true }).decode(bytes))),
              route.kind,
            );
            classifiedError = true;
          } catch {
            await response.body?.cancel().catch(() => undefined);
          }
          const retryable = [408, 409, 429, 500, 502, 503, 504, 529].includes(response.status);
          await publishEvidence(
            deps,
            attemptId,
            errorUsage
              ? "PARTIAL"
              : classifiedError && [400, 401, 403, 404, 422].includes(response.status)
                ? "NOT_EXECUTED"
                : "UNKNOWN",
            errorUsage ? "PARTIAL" : "UNKNOWN",
            errorUsage,
            `upstream_http_${response.status}`,
            providerRequestId,
          );
          recorded = true;
          await deps.limits
            .observe(
              route.provider_id,
              retryable || response.status === 401 || response.status === 403,
              performance.now() - started,
            )
            .catch(() => deps.metrics.increment("health_observation_failures_total"));
          lastError = new GatewayError(
            response.status === 429 ? 429 : response.status >= 500 ? 502 : 400,
            "upstream_rejected",
            "Provider rejected the request",
          );
          if (!retryable && ![401, 403, 404].includes(response.status)) throw lastError;
          const header = response.headers.get("retry-after");
          const retryMs =
            header && /^\d+(\.\d+)?$/.test(header)
              ? Number(header) * 1000
              : header
                ? Date.parse(header) - Date.now()
                : 0;
          const backoff = Math.max(
            Number.isFinite(retryMs) ? retryMs : 0,
            30 * 2 ** (physicalAttempts - 1) + Math.random() * 30,
          );
          retryAt.set(route.provider_id, Date.now() + Math.min(backoff, 3_600_000));
          continue;
        }
        if (
          stream &&
          (textProtocols.has(input.operation) ||
            ["images", "image_edits", "transcription", "speech"].includes(input.operation))
        ) {
          const prepared = await prepareStream(
            response,
            deps,
            attemptController,
            input.reserveMemory,
          );
          const translator: StreamMeter = textProtocols.has(input.operation)
            ? new StreamTranslator(
                target as Protocol,
                input.operation as Protocol,
                route.kind,
                input.model,
                deps.config.MAX_EVENT_BYTES,
                target === "gemini"
                  ? Number(
                      (input.body.generationConfig as Record<string, unknown> | undefined)
                        ?.candidateCount ?? 1,
                    )
                  : target === "chat"
                    ? Number(input.body.n ?? 1)
                    : 1,
              )
            : new MediaStreamMeter(input.operation);
          const publicResponseId = `response_${input.requestId.replaceAll("-", "")}`;
          if (target === "responses" && input.operation === "responses")
            translator.publicId = publicResponseId;
          let responseRegistered = false;
          const streamCleanup = async () => {
            controller.signal.removeEventListener("abort", abortAttempt);
            await deps.limits
              .observe(
                route.provider_id,
                !translator.terminal || translator.failed,
                performance.now() - started,
              )
              .catch(() => deps.metrics.increment("health_observation_failures_total"));
            await cleanup();
          };
          let body: ReadableStream<Uint8Array>;
          try {
            body = await relayStream(
              deps,
              prepared,
              translator,
              attemptId,
              admission.record.id,
              attemptController,
              streamCleanup,
              async () => {
                if (!responseRegistered && translator.publicId && translator.remoteId) {
                  finalizationStarted = true;
                  await deps.store.saveResource(
                    project.id,
                    route.provider_id,
                    translator.remoteId,
                    "response",
                    input.model,
                    attemptId,
                    publicResponseId,
                  );
                  if (input.body.store === false)
                    await deps.store.updateResource(
                      publicResponseId,
                      input.body.store === false || translator.usage
                        ? "COMPLETED"
                        : "PENDING_USAGE",
                    );
                  responseRegistered = true;
                }
              },
              input.reserveMemory,
              async () => {
                if (responseRegistered && translator.terminal && !translator.failed)
                  await deps.store.updateResource(
                    publicResponseId,
                    input.body.store === false || translator.usage ? "COMPLETED" : "PENDING_USAGE",
                    "completed",
                  );
              },
            );
          } catch (error) {
            observedUsage = translator.usage;
            observedFinality = translator.terminal && !translator.failed ? "FINAL" : "PARTIAL";
            throw error;
          }
          streaming = true;
          deps.metrics.increment("streams_started_total");
          return new Response(body, {
            headers: {
              "content-type": "text/event-stream; charset=utf-8",
              "cache-control": "no-cache, no-transform",
              "x-accel-buffering": "no",
              "x-request-id": input.requestId,
              "x-gateway-request-id": admission.record.id,
            },
          });
        }
        if (
          ["speech", "transcription", "translation"].includes(input.operation) &&
          !response.headers.get("content-type")?.includes("json")
        ) {
          const raw = response.body;
          if (!raw) throw new GatewayError(502, "invalid_upstream", "Provider returned no audio");
          // 二进制输出不收集；只能记录已知输入字符量，不能把它宣称为精确音频时长。
          const usage: Usage = {
            characters: String(
              typeof input.body.input === "string" ? [...input.body.input].length : 0,
            ),
            requests: "1",
          };
          const body = relayBinary(
            deps,
            raw,
            attemptId,
            admission.record.id,
            attemptController,
            async () => {
              controller.signal.removeEventListener("abort", abortAttempt);
              await deps.limits
                .observe(
                  route.provider_id,
                  attemptController.signal.aborted,
                  performance.now() - started,
                )
                .catch(() => deps.metrics.increment("health_observation_failures_total"));
              await cleanup();
            },
            input.operation === "speech" ? usage : undefined,
          );
          streaming = true;
          return new Response(body, {
            headers: {
              "content-type": response.headers.get("content-type") ?? "application/octet-stream",
              "x-request-id": input.requestId,
              "x-gateway-request-id": admission.record.id,
            },
          });
        }
        const bytes = await readBounded(
          response.body,
          deps.config.MAX_RESPONSE_BYTES,
          attemptController.signal,
          input.reserveMemory,
        );
        const value = object(
          parseProviderJson(new TextDecoder("utf-8", { fatal: true }).decode(bytes)),
        );
        observedUsage = extractUsage(value, route.kind);
        observedFinality = "PARTIAL";
        if (value.error)
          throw new GatewayError(502, "provider_failed", "Provider returned an error response");
        if (
          (target === "chat" &&
            (!Array.isArray(value.choices) ||
              value.choices.length !== Number(outbound.n ?? 1) ||
              value.choices.some((raw) => {
                if (!raw || typeof raw !== "object" || Array.isArray(raw)) return true;
                const choice = raw as Record<string, unknown>;
                const message = choice.message as Record<string, unknown> | null | undefined;
                return (
                  !choice.message ||
                  typeof choice.message !== "object" ||
                  Array.isArray(choice.message) ||
                  !message ||
                  !["content", "tool_calls", "function_call", "audio", "refusal"].some((field) =>
                    Object.hasOwn(message, field),
                  ) ||
                  (Object.hasOwn(message, "content") &&
                    message.content !== null &&
                    typeof message.content !== "string" &&
                    !Array.isArray(message.content)) ||
                  typeof choice.finish_reason !== "string" ||
                  !choice.finish_reason
                );
              }))) ||
          (target === "responses" && !Array.isArray(value.output)) ||
          (target === "messages" && !Array.isArray(value.content)) ||
          (target === "gemini" && !Array.isArray(value.candidates) && !blockedPrompt(value))
        )
          throw new GatewayError(
            502,
            "invalid_upstream",
            "Provider returned an invalid response shape",
          );
        resultReceived = true;
        let usage = observedUsage;
        observedFinality = "FINAL";
        if (input.operation === "images" || input.operation === "image_edits") {
          const data = Array.isArray(value.data) ? value.data : [];
          const generated = data.filter(
            (item) =>
              typeof item === "object" &&
              item !== null &&
              (("url" in item && typeof item.url === "string" && !!item.url) ||
                ("b64_json" in item && typeof item.b64_json === "string" && !!item.b64_json)),
          );
          if (generated.length) usage = { ...usage, images: String(generated.length) };
        }
        usage = { ...usage, requests: "1" };
        observedUsage = usage;
        let output = value;
        if (textProtocols.has(input.operation) && target !== input.operation)
          output = encodeResponse(
            decodeResponse(value, target as Protocol, route.kind),
            input.operation as Protocol,
            input.model,
          );
        else if (typeof output.model === "string")
          output = copyExact(output, { ...output, model: input.model });
        finalizationStarted = true;
        if (input.transformResponse)
          output = await input.transformResponse(
            output,
            route,
            attemptId,
            input.batchInput && batchFile
              ? { endpoint: input.batchInput.endpoint, upstreamModel: batchFile.upstreamModel }
              : undefined,
          );
        await publishEvidence(
          deps,
          attemptId,
          "SUCCEEDED",
          usage ? "FINAL" : "UNKNOWN",
          usage,
          undefined,
          providerRequestId,
          "SUCCEEDED",
        );
        recorded = true;
        await deps.limits
          .observe(route.provider_id, false, performance.now() - started)
          .catch(() => deps.metrics.increment("health_observation_failures_total"));
        deps.metrics.increment("requests_succeeded_total");
        return new Response(stringifyExact(output), {
          status: response.status,
          headers: {
            "content-type": "application/json",
            "x-request-id": input.requestId,
            "x-gateway-request-id": admission.record.id,
          },
        });
      } catch (error) {
        lastError = error;
        if (!recorded && !streaming) {
          await publishEvidence(
            deps,
            attemptId,
            observedUsage ? (observedFinality === "FINAL" ? "SUCCEEDED" : "PARTIAL") : "UNKNOWN",
            observedUsage ? observedFinality : "UNKNOWN",
            observedUsage,
            controller.signal.aborted ? "request_cancelled" : "upstream_interrupted",
          );
          recorded = true;
        }
        await response?.body?.cancel().catch(() => undefined);
        if (error instanceof GatewayError && ["memory_limited", "overloaded"].includes(error.code))
          throw error;
        if (resultReceived && finalizationStarted)
          throw new GatewayError(
            502,
            "result_finalize_failed",
            "Provider completed but local finalization failed; query the existing request",
          );
        if (error instanceof GatewayError && [400, 413, 422].includes(error.status)) throw error;
        if (controller.signal.aborted)
          throw new GatewayError(
            504,
            "request_deadline",
            "Request deadline reached or client disconnected",
          );
        await deps.limits
          .observe(route.provider_id, true, performance.now() - started)
          .catch(() => deps.metrics.increment("health_observation_failures_total"));
      } finally {
        clearTimeout(headersTimer);
        if (!streaming) {
          controller.signal.removeEventListener("abort", abortAttempt);
          attemptController.abort();
          await providerRelease?.();
          providerRelease = undefined;
        }
      }
    }
    if (admittedId) await deps.usage.finish();
    if (lastError instanceof GatewayError) throw lastError;
    throw new GatewayError(502, "upstream_unavailable", "Providers did not return a usable result");
  } catch (error) {
    if (admittedId && !streaming && !replayed)
      await deps.usage.finish().catch(() => deps.metrics.increment("request_state_failures_total"));
    throw error;
  } finally {
    if (!streaming) await cleanup();
  }
}
