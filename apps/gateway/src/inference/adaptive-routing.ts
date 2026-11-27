import { GatewayError } from "@gateway/contracts";
import {
  calculateCompiledCostAtoms,
  compilePricing,
  type AdaptiveRoutingPolicy,
  type CompiledPricing,
} from "@gateway/kernel";
import type { PriceVersionRecord } from "@gateway/ledger";
import type { HydratedCandidate } from "../proxy-state.ts";
import type { AdaptiveDecision } from "../routing-metrics.ts";
import { adaptiveRouteProfile, estimateTextChatInput } from "../upstream.ts";
import { supplierReconciliation } from "./http.ts";
import type { InferenceRequest } from "./types.ts";

const profiles = new WeakMap<HydratedCandidate, ReturnType<typeof adaptiveRouteProfile>>();
const prices = new WeakMap<PriceVersionRecord, CompiledPricing | null>();
const tokenDimensions = new Set(["input_tokens", "output_tokens", "cached_input_tokens"]);

function hasUniformRetryPolicy(group: readonly HydratedCandidate[], defaultTimeoutMs: number) {
  const first = group[0]!;
  const {
    max_attempts = 3,
    attempt_timeout_ms = defaultTimeoutMs,
    retry_on_ambiguous = false,
  } = first.route.retry_policy;
  const { requiresEvidence } = supplierReconciliation(first);
  return group.every((candidate) => {
    const retry = candidate.route.retry_policy;
    return (
      (retry.max_attempts ?? 3) === max_attempts &&
      (retry.attempt_timeout_ms ?? defaultTimeoutMs) === attempt_timeout_ms &&
      (retry.retry_on_ambiguous ?? false) === retry_on_ambiguous &&
      supplierReconciliation(candidate).requiresEvidence === requiresEvidence
    );
  });
}

function comparablePrice(price: PriceVersionRecord) {
  const cached = prices.get(price);
  if (cached !== undefined) return cached;
  let compiled: CompiledPricing | null = null;
  const rates =
    price.basis === "REQUEST_COMPLETION"
      ? price.rates.requests
        ? { requests: price.rates.requests }
        : {}
      : price.basis === "PROVIDER_USAGE" &&
          Object.entries(price.rates).every(
            ([dimension, rate]) => rate?.numerator === "0" || tokenDimensions.has(dimension),
          )
        ? price.rates
        : null;
  if (rates) {
    try {
      compiled = compilePricing({ currency: price.currency, rates });
    } catch {
      compiled = null;
    }
  }
  prices.set(price, compiled);
  return compiled;
}

export function enforceSupplierReserveLimit(
  policy: AdaptiveRoutingPolicy | undefined,
  currency: string,
  atoms: string,
): void {
  const limit = policy?.supplier_reserve_limit;
  if (limit && (limit.currency !== currency || BigInt(atoms) > BigInt(limit.atoms))) {
    throw new GatewayError(
      503,
      "supplier_reserve_limit",
      "No permitted supplier reservation at the configured currency and limit",
    );
  }
}

export function adaptiveOrdering(
  request: InferenceRequest,
  hydrated: readonly HydratedCandidate[],
) {
  const {
    adaptivePolicy: policy,
    deps,
    isStream,
    model,
    operation,
    outputBound,
    strictAdaptiveSwitch,
    snap,
    state,
  } = request;
  if (!policy) return;

  const observe = (decision: AdaptiveDecision) =>
    deps.metrics.routing.observeDecision(model.name, policy.mode, decision);
  const modelKey = `${model.name}\0${operation}\0${isStream}`;
  const routeKeys = new Map<string, string>();
  let compatibility: string | undefined;
  let customerPriceId: string | undefined;
  let compatible = model.routing_strategy === "WEIGHTED";
  for (const candidate of hydrated) {
    if (
      !candidate.enabled ||
      !candidate.channel_enabled ||
      !candidate.account_active ||
      !candidate.operations.includes(operation)
    )
      continue;
    if (!profiles.has(candidate)) {
      profiles.set(candidate, adaptiveRouteProfile(snap, candidate.channel, candidate.route));
    }
    const profile = profiles.get(candidate);
    if (
      !profile ||
      (compatibility !== undefined && profile.compatibility !== compatibility) ||
      (customerPriceId !== undefined && candidate.billingPrice.id !== customerPriceId)
    ) {
      compatible = false;
    }
    if (!profile) continue;
    compatibility ??= profile.compatibility;
    customerPriceId ??= candidate.billingPrice.id;
    routeKeys.set(
      candidate.id,
      `${candidate.id}\0${modelKey}\0${profile.revision}\0${policy.first_content_target_ms}`,
    );
  }
  if (!compatible) observe("incompatible");

  let inputTokens: string | null | undefined;
  const stats = state.adaptiveRouting;
  const factors = (group: readonly HydratedCandidate[]) => {
    if (!compatible) return undefined;
    if (!strictAdaptiveSwitch && !hasUniformRetryPolicy(group, deps.config.attemptTimeoutMs)) {
      observe("incompatible");
      return undefined;
    }
    const estimatedOutput = stats.outputEstimate(modelKey, outputBound);
    const supplierCurrency = group[0]!.costPrice.currency;
    const costs: { id: string; quality: number; atoms: bigint }[] = [];
    let minimum: bigint | undefined;
    for (const candidate of group) {
      const tariff = comparablePrice(candidate.costPrice);
      if (
        !tariff ||
        tariff.currency !== supplierCurrency ||
        (estimatedOutput === undefined &&
          tariff.entries.some((entry) => entry.dimension === "output_tokens"))
      ) {
        observe("incomparable");
        return undefined;
      }
      const routeKey = routeKeys.get(candidate.id);
      const quality = routeKey === undefined ? undefined : stats.quality(routeKey);
      if (quality === undefined) {
        observe("insufficient");
        return undefined;
      }
      if (candidate.costPrice.basis === "PROVIDER_USAGE" && tariff.entries.length > 0) {
        if (inputTokens === undefined) {
          let estimate: number | undefined;
          try {
            estimate = estimateTextChatInput(request.clientBody);
          } catch {
            estimate = undefined;
          }
          inputTokens =
            estimate === undefined ? null : String(Math.max(1, Math.ceil(estimate / 4)));
        }
        if (inputTokens === null) {
          observe("incomparable");
          return undefined;
        }
      }
      let atoms: bigint;
      try {
        atoms = calculateCompiledCostAtoms(
          candidate.costPrice.basis === "REQUEST_COMPLETION"
            ? { requests: "1" }
            : { input_tokens: inputTokens ?? "0", output_tokens: String(estimatedOutput ?? 0) },
          tariff,
        );
      } catch {
        observe("incomparable");
        return undefined;
      }
      if (minimum === undefined || atoms < minimum) minimum = atoms;
      costs.push({ id: candidate.id, quality, atoms });
    }
    if (policy.mode === "active") observe("applied");
    return new Map(
      costs.map(({ id, quality, atoms }) => [
        id,
        quality * (atoms === 0n ? 1 : Math.max(0.25, Number((minimum! * 1000n) / atoms) / 1000)),
      ]),
    );
  };
  return {
    feedback(routeId: string, complete: boolean, onTime?: boolean, outputTokens?: number) {
      const routeKey = routeKeys.get(routeId);
      if (routeKey !== undefined) {
        stats.observe({
          routeKey,
          modelKey,
          complete,
          ...(onTime !== undefined ? { onTime } : {}),
          ...(outputTokens !== undefined ? { outputTokens } : {}),
        });
      }
    },
    ordering: {
      mode: policy.mode,
      factors,
      compare: (changed: boolean) => observe(changed ? "different" : "same"),
    },
  };
}
