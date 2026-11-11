import {
  GatewayError,
  canonical,
  extractUsage,
  providerObject as object,
  sha256,
  parseProviderJson,
  stringifyExact,
} from "@gateway/kernel";
import type {
  BatchResourceMetadata,
  Resource,
  Operation,
  Protocol,
  Route,
  Usage,
  Finality,
} from "@gateway/kernel";
import {
  StreamTranslator,
  MediaStreamMeter,
  measureResponse,
  validateCompletion,
  presentResponse,
  prepareOperation,
} from "@gateway/connectors";
import { readBounded, UpstreamPreDispatchError } from "@gateway/runtime";
import type { StreamMeter } from "@gateway/connectors";
import type { RequestRecord } from "@gateway/storage";
import type { RatePermit } from "@gateway/runtime";
import type { Auth, Dependencies } from "./context.ts";
import { chooseRoutes } from "./routing.ts";
import { sendUpstream } from "./upstream.ts";
import { publishEvidence } from "./metering.ts";
import { prepareStream, relayStream } from "./relay-stream.ts";
import { relayBinary } from "./relay-binary.ts";
import type { BatchInput, BatchPreparation } from "./batch-input.ts";
import { readBatchManifest, rewriteBatchInput, prepareBatchInput } from "./batch-input.ts";
import { applyOutputBound, rejectUnmeteredTools } from "./input-policy.ts";

const textProtocols = new Set<string>(["chat", "responses", "messages", "gemini"]);
const transportFailures: Record<string, string> = {
  EADDRNOTAVAIL: "local_ports",
  EMFILE: "process_files",
  ENFILE: "system_files",
  ECONNRESET: "connection_reset",
  ECONNREFUSED: "connection_refused",
  ETIMEDOUT: "timeout",
  EPIPE: "broken_pipe",
  ECONNABORTED: "connection_aborted",
  ConnectionClosed: "connection_closed",
  upstream_dns: "dns",
  upstream_tls: "tls_handshake",
  upstream_connect: "connect",
  upstream_capacity: "connection_pool_full",
  upstream_not_dispatched: "not_dispatched",
  unsafe_upstream: "address_blocked",
};
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
  batchSource?: Resource;
  reserveMemory?: (bytes: number) => void;
  transformResponse?: (
    body: Record<string, unknown>,
    route: Route,
    attemptId: string,
    batch?: BatchResourceMetadata,
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
    rejectUnmeteredTools(input.body, input.operation);
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
    let preparationError: unknown;
    const compatibleRoutes: Route[] = [];
    const preparedBatches = new Map<string, BatchPreparation>();
    for (const route of routes) {
      if (input.pinnedUpstreamModel && route.upstream_model !== input.pinnedUpstreamModel) continue;
      try {
        prepareOperation(
          input.body,
          input.operation,
          route.kind,
          route.upstream_model,
          route.request_policy,
        );
        if (input.batchInput)
          preparedBatches.set(
            route.id,
            await prepareBatchInput(deps, input.auth, input.batchInput, route),
          );
        compatibleRoutes.push(route);
      } catch (error) {
        if (!(error instanceof GatewayError)) throw error;
        preparationError = error;
      }
    }
    routes = compatibleRoutes;
    if (!routes.length)
      throw (
        preparationError ??
        new GatewayError(
          422,
          "unsupported_capability",
          "No compatible route model accepts this request",
        )
      );
    const currency = input.request.headers.get("x-gateway-currency") ?? project.billing_currency;
    if (!/^[A-Z]{3,8}$/.test(currency))
      throw new GatewayError(
        400,
        "invalid_currency",
        "Billing currency requires an uppercase currency code",
      );
    let batchMetadata: BatchResourceMetadata | undefined;
    if (input.batchSource) {
      const source = input.batchSource;
      const route = routes.find(
        (candidate) =>
          candidate.provider_id === source.provider_id &&
          candidate.upstream_model === source.batch_upstream_model,
      );
      if (!route || !source.batch_endpoint)
        throw new GatewayError(
          422,
          "resource_provider_mismatch",
          "No route matches the original batch input",
        );
      let manifest = source.batch_manifest;
      if (!manifest) {
        const release = await deps.limits.enter(
          `provider:${route.provider_id}`,
          `${input.requestId}:batch_input`,
          route.rpm,
          route.concurrency,
          deps.config.TOTAL_TIMEOUT_MS + 10000,
        );
        try {
          manifest = await readBatchManifest(deps, source, controller.signal, input.reserveMemory);
        } finally {
          await release();
        }
      }
      batchMetadata = {
        endpoint: source.batch_endpoint,
        upstreamModel: route.upstream_model,
        manifest,
      };
    }
    const quoteBody =
      input.form &&
      (input.operation === "images" || input.operation === "image_edits") &&
      typeof input.body.n === "string"
        ? { ...input.body, n: Number(input.body.n) }
        : input.body;
    const quote = await deps.funds.quote(
      input.model,
      input.operation,
      currency,
      [...new Set(routes.map((route) => route.provider_id))],
      quoteBody,
      outputBound,
      batchMetadata?.manifest?.images,
    );
    routes = routes.filter((route) => Object.hasOwn(quote.prices, route.provider_id));
    let admission: { record: RequestRecord; created: boolean; attempt_id?: string } | undefined;
    let lastError: unknown = new GatewayError(503, "no_route", "No eligible upstream");
    let physicalAttempts = 0;
    const retryAt = new Map<string, number>();
    for (
      let step = 0;
      physicalAttempts < deps.config.MAX_ATTEMPTS &&
      step < routes.length + deps.config.MAX_ATTEMPTS;
      step++
    ) {
      controller.signal.throwIfAborted();
      const route = routes[step % routes.length];
      if (!route) break;
      const prepared = prepareOperation(
        input.body,
        input.operation,
        route.kind,
        route.upstream_model,
        route.request_policy,
      );
      const outbound = prepared.body;
      const target = prepared.protocol;
      const batchFile = input.batchInput
        ? await rewriteBatchInput(
            deps,
            input.auth,
            input.batchInput,
            preparedBatches.get(route.id)!,
          )
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
      const releaseProbe = await deps.limits.probe(
        route.provider_id,
        deps.config.TOTAL_TIMEOUT_MS + 10000,
        `${input.requestId}:${step}`,
      );
      if (!releaseProbe) {
        await providerRelease?.();
        providerRelease = undefined;
        continue;
      }
      const releaseProvider = providerRelease;
      providerRelease = async () => {
        await releaseProbe().catch(() =>
          deps.metrics.increment("health_probe_release_failures_total"),
        );
        await releaseProvider?.();
      };
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
          controller.signal,
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
          deps.transport,
          target,
          payload,
          stream,
          attemptController.signal,
          input.request.headers,
          contentType,
          input.reserveMemory,
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
          await releaseProbe
            .observe(
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
            await releaseProbe
              .observe(!translator.terminal || translator.failed, performance.now() - started)
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
          let usage: Usage | undefined;
          if (input.operation === "speech" && typeof input.body.input === "string") {
            let characters = 0;
            for (const _ of input.body.input) characters++;
            usage = { characters: String(characters) };
          }
          const body = relayBinary(
            deps,
            raw,
            attemptId,
            admission.record.id,
            attemptController,
            async () => {
              controller.signal.removeEventListener("abort", abortAttempt);
              await releaseProbe
                .observe(attemptController.signal.aborted, performance.now() - started)
                .catch(() => deps.metrics.increment("health_observation_failures_total"));
              await cleanup();
            },
            usage,
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
        observedUsage = input.operation === "batches" ? undefined : extractUsage(value, route.kind);
        observedFinality = "PARTIAL";
        validateCompletion(value, target, Number(outbound.n ?? 1));
        resultReceived = true;
        observedFinality = "FINAL";
        const usage =
          input.operation === "batches"
            ? { requests: "1" }
            : measureResponse(value, route.kind, input.operation);
        observedUsage = usage;
        let output = presentResponse(value, target, input.operation, route.kind, input.model);
        finalizationStarted = true;
        if (input.transformResponse)
          output = await input.transformResponse(
            output,
            route,
            attemptId,
            input.batchInput && batchFile
              ? {
                  endpoint: input.batchInput.endpoint,
                  upstreamModel: batchFile.upstreamModel,
                  manifest: input.batchInput.manifest,
                }
              : batchMetadata,
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
        await releaseProbe
          .observe(false, performance.now() - started)
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
        const failureCode = (error as { code?: unknown } | undefined)?.code;
        const localCapacity =
          !response &&
          ["EADDRNOTAVAIL", "EMFILE", "ENFILE", "upstream_capacity"].includes(String(failureCode));
        const notDispatched =
          localCapacity ||
          error instanceof UpstreamPreDispatchError ||
          (error instanceof GatewayError && error.code === "upstream_not_dispatched");
        if (!resultReceived) {
          const failure = error as { code?: unknown } | undefined;
          const code = typeof failure?.code === "string" ? failure.code : "";
          const category = Object.hasOwn(transportFailures, code)
            ? transportFailures[code]
            : "unclassified";
          deps.metrics.increment(`upstream_failure_${category}_total`);
        }
        if (!recorded && !streaming) {
          await publishEvidence(
            deps,
            attemptId,
            notDispatched
              ? "NOT_EXECUTED"
              : observedUsage
                ? observedFinality === "FINAL"
                  ? "SUCCEEDED"
                  : "PARTIAL"
                : "UNKNOWN",
            observedUsage ? observedFinality : "UNKNOWN",
            observedUsage,
            notDispatched
              ? "upstream_not_dispatched"
              : controller.signal.aborted
                ? "request_cancelled"
                : "upstream_interrupted",
          );
          recorded = true;
        }
        await response?.body?.cancel().catch(() => undefined);
        if (localCapacity)
          throw new GatewayError(
            503,
            "upstream_capacity",
            "Local upstream connection capacity exhausted",
          );
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
        if (error instanceof UpstreamPreDispatchError && error.code === "upstream_not_dispatched")
          continue;
        await releaseProbe
          .observe(true, performance.now() - started)
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
    if (controller.signal.aborted)
      throw new GatewayError(
        504,
        "request_deadline",
        "Request deadline reached or client disconnected",
      );
    throw error;
  } finally {
    if (!streaming) await cleanup();
  }
}
