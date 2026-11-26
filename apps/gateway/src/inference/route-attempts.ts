import { GatewayError } from "@gateway/contracts";
import { formatProtocolError } from "@gateway/protocol";
import { UpstreamPreDispatchError } from "@gateway/runtime";
import type { EffectiveCustomerPrice } from "../pricing.ts";
import { fastUuid } from "../proxy-state.ts";
import type { HydratedCandidate } from "../proxy-state.ts";
import type { PreparedUpstreamRequest } from "../upstream.ts";
import type { InferenceRequest } from "./types.ts";
import { responseHeaders } from "./http.ts";
import { createRouteInput, createRouteOutputAdmission } from "./policy.ts";
import type { RouteInput } from "./policy.ts";
import type { InferenceTelemetry } from "./telemetry.ts";
import type { InferenceResources } from "./resources.ts";
import { mapHoldError } from "./accounting.ts";
import type { InferenceAccounting } from "./accounting.ts";
import { selectInferenceRoutes } from "./candidates.ts";
import { createUpstreamPreparation, createDispatchReservation } from "./upstream-attempt.ts";
import { dispatchInferenceAttempt } from "./dispatch.ts";

export async function routeInference(
  request: InferenceRequest,
  resources: InferenceResources,
  telemetry: InferenceTelemetry,
  accounting: InferenceAccounting,
): Promise<Response> {
  const { c, deps, operation, trace, firstByteDeadlineMs } = request;
  const routes = selectInferenceRoutes(request);
  const { selected, releaseProbeFor, releaseUnusedProbes, releaseAllPendingProbes } = routes;
  const reservation = createDispatchReservation(request, resources, telemetry);
  const { reserveDispatch, releaseDispatch, preDispatchGatewayError } = reservation;
  const preparation = createUpstreamPreparation(request);
  const { prepareForReservation } = preparation;
  const output = createRouteOutputAdmission(request, resources, telemetry, accounting);
  const { admitOutputPolicy, rejectOutputPolicy } = output;
  const prepareInput = createRouteInput(request, resources, telemetry);
  let activePluginSignal: AbortSignal | undefined;
  const prepareRouteInput = (candidate: HydratedCandidate) => {
    const signal = deps.snapshot.pluginSignal(candidate.route, candidate.channel);
    activePluginSignal = signal;
    return prepareInput(candidate, signal);
  };
  const replayedResponse = (priorId: string, priorState: string | undefined) => {
    const inProgress = priorState === "HELD";
    const code = inProgress ? "idempotency_in_progress" : "idempotency_replayed";
    const message = inProgress
      ? "Request with this Idempotency-Key is already in progress"
      : `Request already finalized with state ${priorState ?? "UNKNOWN"}`;
    telemetry.emitAccessLog(409, code);
    return c.json(
      formatProtocolError(operation, 409, code, message, priorId),
      409,
      responseHeaders(priorId, trace),
    );
  };

  try {
    const input = prepareRouteInput(selected[0]!.route);
    const firstInput = input instanceof Promise ? await input : input;
    if (firstInput instanceof Response) return firstInput;
    const identity = accounting.identifyIdempotency(
      c.req.raw.headers.get("idempotency-key"),
      firstInput.body,
    );
    const idempotency = identity ? await identity : null;
    let firstReservation:
      | {
          index: number;
          attemptId: string;
          input: RouteInput;
          prepared: PreparedUpstreamRequest | null;
          customer: EffectiveCustomerPrice;
        }
      | undefined;
    let lastHoldErr: GatewayError | null = null;

    for (let idx = 0; idx < selected.length; idx++) {
      const entry = selected[idx]!;
      const cand = entry.route;
      if (!deps.router.peekAvailable(cand.channel.id, entry.probe)) {
        releaseProbeFor(cand.channel.id);
        continue;
      }
      if (firstByteDeadlineMs !== null && performance.now() >= firstByteDeadlineMs) {
        releaseUnusedProbes(idx);
        throw new GatewayError(
          504,
          "upstream_timeout",
          "Request exceeded first-byte timeout budget",
        );
      }
      const preparedInput = idx === 0 ? firstInput : await prepareRouteInput(cand);
      if (preparedInput instanceof Response) return preparedInput;
      const unavailablePolicy = admitOutputPolicy(cand);
      if (unavailablePolicy) {
        if (idempotency) {
          const prior = await accounting.findReplay(idempotency);
          if (prior) return replayedResponse(prior.requestId, prior.state);
        }
        return await rejectOutputPolicy(unavailablePolicy);
      }
      const customer = accounting.customerPrice(cand);
      const prepared = prepareForReservation(cand, preparedInput.body, customer.price);

      let quote: ReturnType<InferenceAccounting["quote"]>;
      try {
        quote = accounting.quote(cand, customer, {
          ...preparedInput.measurement,
          estimatedInputTokens: prepared?.estimatedInputTokens,
        });
      } catch (err) {
        lastHoldErr =
          err instanceof GatewayError
            ? err
            : new GatewayError(
                422,
                "billing_ceiling",
                err instanceof Error ? err.message : "Quote error",
              );
        releaseProbeFor(cand.channel.id);
        continue;
      }
      preparedInput.pluginSignal.throwIfAborted();
      if (resources.interrupted()) {
        throw resources.admissionError(resources.quotaLost());
      }
      try {
        if (!idempotency) {
          const pending = reserveDispatch(cand, prepared, preparedInput.pluginSignal);
          if (pending) await pending;
        }
      } catch (error) {
        releaseDispatch();
        preparedInput.pluginSignal.throwIfAborted();
        if (!(error instanceof UpstreamPreDispatchError)) throw error;
        lastHoldErr = preDispatchGatewayError(error);
        releaseProbeFor(cand.channel.id);
        continue;
      }
      if (!deps.router.peekAvailable(cand.channel.id, entry.probe)) {
        releaseDispatch();
        releaseProbeFor(cand.channel.id);
        continue;
      }
      if (resources.interrupted()) {
        throw resources.admissionError(resources.quotaLost());
      }
      const attemptId = fastUuid();
      const holdRes = await accounting.acquire(attemptId, quote, idempotency);
      if (holdRes.ok) {
        if (holdRes.replayed) {
          releaseUnusedProbes(idx);
          return replayedResponse(holdRes.request_id, holdRes.existing_state);
        }
        firstReservation = { index: idx, attemptId, input: preparedInput, prepared, customer };
        break;
      }
      const lackedSupplierFunding =
        holdRes.code === "supplier_unavailable" || holdRes.code === "supplier_insufficient_balance";
      if (lackedSupplierFunding)
        telemetry.recordPreDispatchFailure(
          cand.channel.name,
          holdRes.code,
          "supplier_funding",
          reservation.timing,
          attemptId,
        );
      releaseDispatch();
      releaseProbeFor(cand.channel.id);
      if (lackedSupplierFunding) {
        lastHoldErr = mapHoldError(holdRes.code, holdRes.message);
        continue;
      }
      releaseUnusedProbes(idx + 1);
      throw mapHoldError(holdRes.code, holdRes.message);
    }

    if (!firstReservation) {
      throw lastHoldErr ?? new GatewayError(503, "no_route", "No funded route available");
    }

    const activeIndex = firstReservation.index;
    let activeAttemptId = firstReservation.attemptId;
    const maxAttempts = Math.min(
      selected.length - activeIndex,
      Math.max(1, selected[activeIndex]!.route.route.retry_policy.max_attempts ?? 3),
    );
    let attemptsMade = 0;
    let lastError: GatewayError | undefined;
    let pendingSwitchFrom: { attemptId: string; outcome: "FAILED" | "UNKNOWN" } | null = null;

    for (
      let cursor = activeIndex;
      cursor < selected.length && attemptsMade < maxAttempts;
      cursor++
    ) {
      if (cursor !== activeIndex) releaseDispatch();
      const entry = selected[cursor]!;
      const cand = entry.route;
      const mayRetryAmbiguous = () => {
        const limit = deps.config.maxAmbiguousRetriesPerMinute;
        return (
          Boolean(cand.route.retry_policy.retry_on_ambiguous) &&
          (limit === undefined || deps.metrics.tryConsumeAmbiguousRetry(cand.channel.id, limit))
        );
      };
      if (!deps.router.peekAvailable(cand.channel.id, entry.probe)) {
        releaseProbeFor(cand.channel.id);
        if (cursor === activeIndex && !pendingSwitchFrom) {
          pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
        }
        if (attemptsMade === 0 && (!lastError || lastError.code === "upstream_failed")) {
          lastError = new GatewayError(
            503,
            "no_route",
            "No healthy channel available for this model",
          );
        }
        continue;
      }
      if (firstByteDeadlineMs !== null && performance.now() >= firstByteDeadlineMs) {
        releaseUnusedProbes(cursor);
        if (attemptsMade === 0) {
          lastError = new GatewayError(
            504,
            "upstream_timeout",
            "Request exceeded first-byte timeout budget",
          );
        }
        break;
      }
      const firstAttempt = cursor === activeIndex ? firstReservation : undefined;
      const candBilling = firstAttempt?.customer ?? accounting.customerPrice(cand);
      telemetry.setChannel(cand.channel.name);
      accounting.setDispatched(false);
      const attemptInput = firstAttempt?.input ?? (await prepareRouteInput(cand));
      if (attemptInput instanceof Response) {
        releaseAllPendingProbes();
        await accounting.finishFailedRequest();
        return attemptInput;
      }
      if (cursor !== activeIndex) {
        const unavailablePolicy = admitOutputPolicy(cand);
        if (unavailablePolicy) return await rejectOutputPolicy(unavailablePolicy);
      }
      const { body: attemptBody, measurement: inputMeasurement, pluginSignal } = attemptInput;
      pluginSignal.throwIfAborted();
      const preparedAttempt = firstAttempt
        ? firstAttempt.prepared
        : prepareForReservation(cand, attemptBody, candBilling.price);

      if (pendingSwitchFrom) {
        try {
          await reserveDispatch(cand, preparedAttempt, pluginSignal);
        } catch (error) {
          releaseDispatch();
          pluginSignal.throwIfAborted();
          if (!(error instanceof UpstreamPreDispatchError)) throw error;
          const rejection = preDispatchGatewayError(error);
          lastError = rejection;
          releaseProbeFor(cand.channel.id);
          continue;
        }
        if (!deps.router.peekAvailable(cand.channel.id, entry.probe)) {
          releaseDispatch();
          releaseProbeFor(cand.channel.id);
          continue;
        }

        let quote: ReturnType<InferenceAccounting["quote"]>;
        try {
          quote = accounting.quote(cand, candBilling, {
            ...inputMeasurement,
            estimatedInputTokens: preparedAttempt?.estimatedInputTokens,
          });
        } catch (err) {
          releaseProbeFor(cand.channel.id);
          lastError = new GatewayError(
            422,
            "billing_ceiling",
            err instanceof Error ? err.message : "Fallback quote error",
          );
          continue;
        }
        const nextAttemptId = fastUuid();
        const sw = await accounting.switchSupplier(pendingSwitchFrom, nextAttemptId, quote);
        if (!sw.ok) {
          releaseProbeFor(cand.channel.id);
          if (sw.code === "supplier_unavailable" || sw.code === "supplier_insufficient_balance") {
            telemetry.recordPreDispatchFailure(
              cand.channel.name,
              sw.code,
              "supplier_funding",
              reservation.timing,
              nextAttemptId,
            );
            continue;
          }
          releaseUnusedProbes(cursor + 1);
          await accounting.finishFailedRequest();
          throw mapHoldError(sw.code, sw.message);
        }
        activeAttemptId = nextAttemptId;
        pendingSwitchFrom = null;
        telemetry.failover();
      }

      if (firstByteDeadlineMs !== null && performance.now() >= firstByteDeadlineMs) {
        releaseUnusedProbes(cursor);
        if (attemptsMade === 0) {
          lastError = new GatewayError(
            504,
            "upstream_timeout",
            "Request exceeded first-byte timeout budget",
          );
        }
        break;
      }
      resources.resetChannelPermit();
      const chLimit = await deps.cluster.acquireRateLimit(
        `channel:${cand.channel.id}`,
        {
          rpm: cand.channel.rpm,
          tpm: Number.MAX_SAFE_INTEGER,
          concurrency: cand.channel.concurrency,
        },
        1,
        deps.config.holdTtlMs,
        resources.revokeChannelPermit,
      );
      if (!chLimit.ok) {
        if (chLimit.reason === "unavailable") {
          await accounting.finishFailedRequest();
          throw new GatewayError(
            503,
            "rate_limit_unavailable",
            "Shared channel quota is unavailable",
          );
        }
        telemetry.recordPreDispatchFailure(
          cand.channel.name,
          "channel_saturated",
          "capacity",
          reservation.timing,
          activeAttemptId,
        );
        releaseProbeFor(cand.channel.id);
        pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
        lastError = new GatewayError(
          429,
          "channel_saturated",
          "Upstream channel concurrency or RPM limit reached",
        );
        continue;
      }
      if (chLimit.release) resources.setChannelLimit(chLimit.release);
      const baseAttemptTimeoutMs =
        cand.route.retry_policy.attempt_timeout_ms ?? deps.config.attemptTimeoutMs;
      let attemptTimeoutMs = baseAttemptTimeoutMs;
      if (firstByteDeadlineMs !== null) {
        const remainingTotalMs = Math.floor(firstByteDeadlineMs - performance.now());
        if (remainingTotalMs <= 0) {
          resources.clearChannelLimit();
          releaseUnusedProbes(cursor);
          if (attemptsMade === 0) {
            lastError = new GatewayError(
              504,
              "upstream_timeout",
              "Request exceeded first-byte timeout budget",
            );
          }
          break;
        }
        let healthyBackupsLeft = 0;
        const maxFutureAttempts = maxAttempts - attemptsMade - 1;
        for (
          let j = cursor + 1;
          j < selected.length && healthyBackupsLeft < maxFutureAttempts;
          j++
        ) {
          const nextEntry = selected[j]!;
          if (deps.router.peekAvailable(nextEntry.route.channel.id, nextEntry.probe)) {
            healthyBackupsLeft++;
          }
        }
        attemptTimeoutMs =
          healthyBackupsLeft > 0
            ? Math.min(
                baseAttemptTimeoutMs,
                Math.max(
                  Math.min(100, remainingTotalMs),
                  Math.floor(remainingTotalMs / (healthyBackupsLeft + 1)),
                ),
              )
            : Math.min(baseAttemptTimeoutMs, remainingTotalMs);
      }
      const result = await dispatchInferenceAttempt(
        request,
        resources,
        telemetry,
        accounting,
        routes,
        reservation,
        preparation,
        output,
        {
          cursor,
          attemptId: activeAttemptId,
          attemptNumber: ++attemptsMade,
          attemptTimeoutMs,
          baseAttemptTimeoutMs,
          preparedAttempt,
          attemptInput,
          candBilling,
          hasIdempotencyKey: idempotency !== null,
        },
      );
      if (result instanceof Response) return result;
      lastError = result.error;
      if (result.outcome === "UNKNOWN" && !mayRetryAmbiguous()) break;
      pendingSwitchFrom = { attemptId: activeAttemptId, outcome: result.outcome };
    }
    releaseAllPendingProbes();
    await accounting.finishFailedRequest();
    throw lastError ?? new GatewayError(502, "upstream_failed", "All upstream channels failed");
  } catch (error) {
    releaseAllPendingProbes();
    const failure = activePluginSignal?.aborted ? activePluginSignal.reason : error;
    if (!resources.streamOwnsRequest) await accounting.rejectOutstandingHold();
    throw failure;
  } finally {
    releaseDispatch();
    releaseAllPendingProbes();
  }
}
