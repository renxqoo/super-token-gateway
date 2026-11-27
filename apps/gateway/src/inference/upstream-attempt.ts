import { GatewayError } from "@gateway/contracts";
import type { PriceVersionRecord } from "@gateway/ledger";
import { UpstreamPreDispatchError } from "@gateway/runtime";
import type { PreparedDispatch } from "@gateway/runtime";
import type { HydratedCandidate } from "../proxy-state.ts";
import { MeteredRequestError } from "../request-metering.ts";
import { createTransportTiming } from "../first-text-trace.ts";
import { tryPrepareUpstreamRequestSync } from "../upstream.ts";
import type { PreparedUpstreamRequest, PrepareUpstreamOptions } from "../upstream.ts";
import type { InferenceRequest } from "./types.ts";
import { channelFailureKind } from "./telemetry.ts";
import type { InferenceTelemetry } from "./telemetry.ts";
import type { InferenceResources } from "./resources.ts";

function hasNonZeroCacheWriteRate(price: PriceVersionRecord): boolean {
  const { cache_write_tokens, cache_write_5m_tokens, cache_write_1h_tokens } = price.rates;
  return (
    (cache_write_tokens !== undefined && cache_write_tokens.numerator !== "0") ||
    (cache_write_5m_tokens !== undefined && cache_write_5m_tokens.numerator !== "0") ||
    (cache_write_1h_tokens !== undefined && cache_write_1h_tokens.numerator !== "0")
  );
}

export function createUpstreamPreparation(request: InferenceRequest) {
  const { deps, snap, operation, outputBound, isStream, billingAccount, trace, multipartEntries } =
    request;
  const onFieldDropped: NonNullable<PrepareUpstreamOptions["onFieldDropped"]> = (
    source,
    target,
    field,
  ) => {
    deps.metrics.recordProtocolFieldDropped(source, target, field);
  };
  const onFieldRejected: NonNullable<PrepareUpstreamOptions["onFieldRejected"]> = (
    source,
    target,
    field,
  ) => {
    deps.metrics.recordProtocolFieldRejected(source, target, field);
  };
  function upstreamOptions(
    cand: HydratedCandidate,
    body: Record<string, unknown>,
    canMutateClientBody = false,
  ): Omit<PrepareUpstreamOptions, "signal"> {
    const opts: Omit<PrepareUpstreamOptions, "signal"> = {
      channel: cand.channel,
      route: cand.route,
      snapshot: snap,
      secret: deps.snapshot.decryptChannelSecret(cand.channel),
      operation,
      clientBody: body,
      outputBound,
      isStream,
      canMutateClientBody,
      alreadyValidated: true,
      vertexTokens: deps.vertexTokens,
      wasmExecutor: deps.wasmExecutor,
      tenantId: billingAccount.id,
      onFieldDropped,
      onFieldRejected,
      traceparent: trace.traceparent,
    };
    if (multipartEntries) opts.multipartEntries = multipartEntries;
    if (deps.config.pluginSigningPublicKeyPem) {
      opts.signingPublicKeyPem = deps.config.pluginSigningPublicKeyPem;
    }
    return opts;
  }

  function prepareForReservation(
    cand: HydratedCandidate,
    body: Record<string, unknown>,
    customerPrice: PriceVersionRecord,
    canMutateClientBody = false,
  ): PreparedUpstreamRequest | null {
    try {
      const opts = upstreamOptions(cand, body, canMutateClientBody) as Parameters<
        typeof tryPrepareUpstreamRequestSync
      >[0];
      opts.estimateInputTokens =
        (operation === "chat" || operation === "responses" || operation === "messages") &&
        Boolean(customerPrice.rates.input_tokens || cand.costPrice.rates.input_tokens);
      const prepared = tryPrepareUpstreamRequestSync(opts);
      if (
        prepared &&
        prepared.estimatedInputTokens !== undefined &&
        prepared.usageDialect !== "anthropic" &&
        (hasNonZeroCacheWriteRate(customerPrice) || hasNonZeroCacheWriteRate(cand.costPrice))
      ) {
        delete prepared.estimatedInputTokens;
      }
      return prepared;
    } catch (error) {
      if (error instanceof MeteredRequestError) throw error;
      return null;
    }
  }
  return { upstreamOptions, prepareForReservation };
}

export type UpstreamPreparation = ReturnType<typeof createUpstreamPreparation>;

export function createDispatchReservation(
  request: InferenceRequest,
  resources: InferenceResources,
  telemetry: InferenceTelemetry,
) {
  const { c, deps, firstByteDeadlineMs, snap, billingAccount, key } = request;
  let activeDispatch: PreparedDispatch | undefined;
  let activeDispatchTiming: ReturnType<typeof createTransportTiming> | undefined;
  const reserveDispatch = (
    candidate: HydratedCandidate,
    prepared: PreparedUpstreamRequest | null,
    pluginSignal: AbortSignal,
  ): void | Promise<void> => {
    if (!prepared) return;
    if (
      !c.req.raw.signal.aborted &&
      !pluginSignal.aborted &&
      !resources.interrupted() &&
      deps.snapshot.isFresh() &&
      deps.snapshot.get().epoch === snap.epoch
    ) {
      const fast = deps.transport.tryPrepareDispatchSync?.(
        prepared.url,
        deps.config.allowPrivateUpstreams,
        deps.config.privateUpstreamCidrs,
      );
      if (fast) {
        activeDispatch = fast;
        if (telemetry.requestTrace) {
          const timing = createTransportTiming();
          timing.values.connection_reused = true;
          telemetry.requestTrace.observeAdmission(timing.values);
          activeDispatchTiming = timing;
        } else {
          activeDispatchTiming = undefined;
        }
        const currentAdmission = deps.checkConsoleAdmission(billingAccount, key);
        if (!currentAdmission) return;
        return currentAdmission.then(() => {
          pluginSignal.throwIfAborted();
        });
      }
    }
    return reserveDispatchAsync(candidate, prepared, pluginSignal);
  };
  const reserveDispatchAsync = async (
    candidate: HydratedCandidate,
    prepared: PreparedUpstreamRequest,
    pluginSignal: AbortSignal,
  ) => {
    const linked = resources.link([c.req.raw.signal, pluginSignal]);
    const timing = telemetry.requestTrace ? createTransportTiming() : undefined;
    const startedAt = timing ? performance.now() : 0;
    if (timing) telemetry.requestTrace?.observeAdmission(timing.values);
    try {
      activeDispatch = await deps.transport.prepareDispatch(
        prepared.url,
        typeof prepared.body === "string"
          ? Buffer.byteLength(prepared.body)
          : prepared.body.byteLength,
        {
          signal: linked.signal,
          ...(timing ? { onTiming: timing.observe } : {}),
          dispatchDeadline: Math.min(
            firstByteDeadlineMs ?? Infinity,
            performance.now() +
              (candidate.route.retry_policy.attempt_timeout_ms ?? deps.config.attemptTimeoutMs),
          ),
        },
        deps.config.allowPrivateUpstreams,
        deps.config.privateUpstreamCidrs,
      );
      activeDispatchTiming = timing;
    } catch (error) {
      if (error instanceof UpstreamPreDispatchError && !linked.signal.aborted) {
        telemetry.recordPreDispatchFailure(
          candidate.channel.name,
          error.code,
          channelFailureKind(error.code),
          timing,
          undefined,
          startedAt,
        );
      }
      throw error;
    } finally {
      linked.dispose();
    }
    pluginSignal.throwIfAborted();
    if (resources.interrupted()) {
      throw resources.admissionError(resources.quotaLost());
    }
    if (!deps.snapshot.isFresh() || deps.snapshot.get().epoch !== snap.epoch) {
      throw new GatewayError(
        503,
        "config_unavailable",
        "Gateway configuration changed before dispatch",
      );
    }
    const currentAdmission = deps.checkConsoleAdmission(billingAccount, key);
    if (currentAdmission) await currentAdmission;
    pluginSignal.throwIfAborted();
  };
  const releaseDispatch = () => {
    activeDispatch?.release();
    activeDispatch = undefined;
    activeDispatchTiming = undefined;
  };
  const preDispatchGatewayError = (error: UpstreamPreDispatchError) => {
    if (error.code === "upstream_capacity") return error;
    if (error.code === "upstream_not_dispatched") {
      if (resources.interrupted(true)) {
        return resources.admissionError(resources.quotaLost(true));
      }
      return new GatewayError(504, "upstream_timeout", error.message);
    }
    return new GatewayError(503, "upstream_unreachable", error.message);
  };
  return {
    reserveDispatch,
    releaseDispatch,
    preDispatchGatewayError,
    get timing() {
      return activeDispatchTiming;
    },
    get dispatch() {
      return activeDispatch;
    },
  };
}

export type DispatchReservation = ReturnType<typeof createDispatchReservation>;
