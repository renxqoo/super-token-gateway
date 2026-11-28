import {
  calculateCompiledCostAtoms,
  compilePricing,
  type Operation,
  type CompiledPricing,
} from "@gateway/kernel";
import {
  hasPricingSpec,
  type ConfigSnapshot,
  type ModelRecord,
  type PriceVersionRecord,
} from "@gateway/ledger";
import type { AdaptiveRoutingStats } from "../adaptive-routing-stats.ts";
import type { HydratedCandidate } from "../proxy-state.ts";
import type { AdaptiveDecision } from "../routing-metrics.ts";
import { adaptiveRouteProfile, estimateTextChatInput } from "../upstream.ts";
import {
  defaultRouteAttempts,
  routeAttemptTimeoutMs,
  supplierReconciliation,
} from "./failover-policy.ts";
import type { InferenceRoutingPolicy } from "./routing-policy.ts";

const profiles = new WeakMap<HydratedCandidate, ReturnType<typeof adaptiveRouteProfile>>();
const prices = new WeakMap<PriceVersionRecord, CompiledPricing | null>();
const tokenDimensions = new Set(["input_tokens", "output_tokens", "cached_input_tokens"]);

function hasUniformRetryPolicy(group: readonly HydratedCandidate[], defaultTimeoutMs: number) {
  const first = group[0]!;
  const {
    max_attempts: maxAttempts = defaultRouteAttempts,
    attempt_timeout_ms: timeout = defaultTimeoutMs,
    retry_on_ambiguous: retryOnAmbiguous = false,
  } = first.route.retry_policy;
  const { requiresEvidence } = supplierReconciliation(first);
  return group.every((candidate) => {
    const retry = candidate.route.retry_policy;
    return (
      (retry.max_attempts ?? defaultRouteAttempts) === maxAttempts &&
      routeAttemptTimeoutMs(retry, defaultTimeoutMs) === timeout &&
      (retry.retry_on_ambiguous ?? false) === retryOnAmbiguous &&
      supplierReconciliation(candidate).requiresEvidence === requiresEvidence
    );
  });
}

function comparablePrice(price: PriceVersionRecord) {
  const cached = prices.get(price);
  if (cached !== undefined) return cached;
  if (hasPricingSpec(price.pricing_spec)) {
    prices.set(price, null);
    return null;
  }
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

export function adaptiveOrdering(
  options: {
    policy: NonNullable<InferenceRoutingPolicy["ordering"]>;
    snapshot: ConfigSnapshot;
    model: Pick<ModelRecord, "name" | "routing_strategy">;
    operation: Operation;
    isStream: boolean;
    outputBound: number;
    clientBody: Record<string, unknown>;
    defaultAttemptTimeoutMs: number;
    stats: AdaptiveRoutingStats;
    observe: (decision: AdaptiveDecision) => void;
  },
  hydrated: readonly HydratedCandidate[],
) {
  const {
    policy,
    snapshot,
    model,
    operation,
    isStream,
    outputBound,
    clientBody,
    defaultAttemptTimeoutMs,
    stats,
    observe,
  } = options;
  const modelKey = `${model.name}\0${operation}\0${isStream}`;
  const routeKeys = new Map<string, string>();
  const weighted = model.routing_strategy === "WEIGHTED";
  const active = policy.mode === "active";
  for (const candidate of hydrated) {
    if (
      !candidate.enabled ||
      !candidate.channel_enabled ||
      !candidate.account_active ||
      !candidate.operations.includes(operation)
    )
      continue;
    if (!profiles.has(candidate)) {
      profiles.set(candidate, adaptiveRouteProfile(snapshot, candidate.channel, candidate.route));
    }
    const profile = profiles.get(candidate);
    if (!profile) continue;
    routeKeys.set(
      candidate.id,
      `${candidate.id}\0${modelKey}\0${profile.revision}\0${policy.first_content_target_ms}`,
    );
  }
  if (!weighted) observe("incompatible");

  function hasRouteScopedModifier(group: readonly HydratedCandidate[]) {
    return snapshot.pricingModifiers?.some(
      ({ enabled, match }) =>
        enabled &&
        (match.channel_id !== undefined || match.upstream_account_id !== undefined) &&
        (!match.model || match.model === "*" || match.model === model.name) &&
        (!match.operation || match.operation === "*" || match.operation === operation) &&
        (!match.execution_mode || match.execution_mode === "SYNC") &&
        group.some(
          ({ channel, account }) =>
            (match.channel_id === undefined || match.channel_id === channel.id) &&
            (match.upstream_account_id === undefined || match.upstream_account_id === account.id),
        ),
    );
  }

  function groupCompatible(group: readonly HydratedCandidate[]) {
    const first = group[0]!;
    const profile = profiles.get(first);
    return (
      profile !== null &&
      profile !== undefined &&
      group.every(
        (candidate) =>
          candidate.billingPrice.id === first.billingPrice.id &&
          profiles.get(candidate)?.compatibility === profile.compatibility,
      ) &&
      !hasRouteScopedModifier(group)
    );
  }

  let inputTokens: string | null | undefined;
  const factors = (group: readonly HydratedCandidate[]) => {
    if (!weighted) return undefined;
    if (
      !groupCompatible(group) ||
      (policy.requireUniformRetryPolicy && !hasUniformRetryPolicy(group, defaultAttemptTimeoutMs))
    ) {
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
            estimate = estimateTextChatInput(clientBody);
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
    if (active) observe("applied");
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
