import {
  DomainError,
  evaluateMargin,
  evaluateSnapshotBreakdown,
  parseIsoTimestampMs,
  quoteSnapshotReservation,
  resolvePricingSnapshotForCard,
  type AttrValue,
  type CostBreakdown,
  type ExecutionMode,
  type MarginEvaluation,
  type MarginPolicy,
  type Operation,
  type PricingContext,
  type PricingSnapshot,
  type SnapshotReservationMeasurement,
  type SnapshotReservationQuote,
  type Usage,
} from "@gateway/kernel";
import { rateCardFromPriceVersion, type RouteRecord, type ConfigSnapshot } from "@gateway/ledger";
import {
  applyLegacyOverrideMultiplierToSnapshot,
  canRouteMatchCurrency,
  getCompiledRatingPlan,
  resolveEffectiveCustomerPrice,
} from "./pricing.ts";

function serializeBreakdown(b: CostBreakdown) {
  return {
    currency: b.currency,
    rate_card_id: b.rate_card_id,
    matched_cliff_tier_index: b.matched_cliff_tier_index,
    components: b.components,
    pre_modifier_numerator: b.pre_modifier_numerator,
    pre_modifier_denominator: b.pre_modifier_denominator,
    combined_multiplier: b.combined_multiplier,
    raw_atoms: b.raw_atoms.toString(),
    clamped_by: b.clamped_by,
    total_atoms: b.total_atoms.toString(),
  };
}

function serializeMargin(m: MarginEvaluation) {
  return {
    comparable: m.comparable,
    customer_currency: m.customer_currency,
    supplier_currency: m.supplier_currency,
    customer_atoms: m.customer_atoms.toString(),
    supplier_atoms: m.supplier_atoms.toString(),
    normalized_supplier_atoms:
      m.normalized_supplier_atoms !== null ? m.normalized_supplier_atoms.toString() : null,
    gross_margin_atoms: m.gross_margin_atoms !== null ? m.gross_margin_atoms.toString() : null,
    margin_bps: m.margin_bps !== null ? m.margin_bps.toString() : null,
    is_negative_margin: m.is_negative_margin,
    allowed: m.allowed,
    reason: m.reason,
  };
}

function findActiveFxRational(
  snap: ConfigSnapshot,
  baseCurrency: string,
  quoteCurrency: string,
  ratedAtIso: string,
): { numerator: string; denominator: string } | undefined {
  if (baseCurrency === quoteCurrency) return undefined;
  const ratedAtMs = parseIsoTimestampMs(ratedAtIso);
  let bestFrom = -Infinity;
  let best: { numerator: string; denominator: string } | undefined;
  let tie = false;
  for (const fx of snap.fxRateVersions ?? []) {
    if (fx.base_currency !== baseCurrency || fx.quote_currency !== quoteCurrency) continue;
    const fromMs = parseIsoTimestampMs(fx.effective_from);
    const toMs = fx.effective_to !== null ? parseIsoTimestampMs(fx.effective_to) : null;
    if (ratedAtMs < fromMs || (toMs !== null && ratedAtMs >= toMs)) continue;
    if (fromMs > bestFrom) {
      bestFrom = fromMs;
      best = { numerator: fx.rate_numerator, denominator: fx.rate_denominator };
      tie = false;
    } else if (fromMs === bestFrom) {
      tie = true;
    }
  }
  if (tie) {
    throw new DomainError(
      "ambiguous_pricing",
      `Multiple active FX rates match ${baseCurrency}->${quoteCurrency}`,
    );
  }
  return best;
}

export function simulatePricing(
  snap: ConfigSnapshot,
  input: {
    model: string;
    operation: Operation;
    currency: string;
    route_id?: string | undefined;
    billing_account_id?: string | undefined;
    customer_group_id?: string | undefined;
    channel_id?: string | undefined;
    upstream_account_id?: string | undefined;
    rated_at?: string | undefined;
    execution_mode?: ExecutionMode | undefined;
    service_tier?: string | undefined;
    region?: string | undefined;
    attrs?: Record<string, AttrValue> | undefined;
    usage: Usage;
    input_characters?: string | undefined;
    output_bound?: number | undefined;
    estimated_input_tokens?: number | undefined;
    images?: number | undefined;
    margin_policy?:
      | {
          allow_negative_margin?: boolean | undefined;
          min_margin_bps?: number | undefined;
          max_loss_atoms?: string | undefined;
        }
      | undefined;
  },
  draftRoutes?: readonly RouteRecord[],
) {
  const modelRoutes = (draftRoutes ?? snap.routesByModel.get(input.model) ?? []).filter(
    (r) =>
      r.enabled &&
      r.operations.includes(input.operation) &&
      (!input.channel_id || r.channel_id === input.channel_id),
  );
  const route = input.route_id
    ? modelRoutes.find((r) => r.id === input.route_id)
    : (modelRoutes.find((r) => snap.prices.get(r.billing_price_id)?.currency === input.currency) ??
      modelRoutes.find((r) => {
        const bp = snap.prices.get(r.billing_price_id);
        return (
          bp !== undefined &&
          canRouteMatchCurrency(snap, bp.currency, input.currency, input.model, input.operation)
        );
      }));
  if (!route) {
    throw new DomainError(
      "tariff_unavailable",
      `No enabled route found for ${input.model}/${input.operation} in ${input.currency}`,
    );
  }
  const baseCustomerPrice = snap.prices.get(route.billing_price_id);
  const baseSupplierPrice = snap.prices.get(route.cost_version_id);
  if (!baseCustomerPrice || !baseSupplierPrice) {
    throw new DomainError("tariff_unavailable", "Route price versions are missing from snapshot");
  }
  const channel = snap.channels.get(route.channel_id);
  const billingAccount = input.billing_account_id
    ? snap.billingAccounts.get(input.billing_account_id)
    : undefined;
  const customerGroupId = input.customer_group_id ?? billingAccount?.customer_group_id ?? undefined;
  const ratedAt = input.rated_at ?? new Date().toISOString();
  const plan = getCompiledRatingPlan(snap);

  let effectiveBaseCustomer = baseCustomerPrice;
  let legacyMultOverride: { id: string; num: string; den: string } | null = null;
  let hasLegacyExact = false;
  if (input.billing_account_id) {
    const legacy = resolveEffectiveCustomerPrice(
      snap,
      input.billing_account_id,
      input.model,
      input.operation,
      baseCustomerPrice,
      input.currency,
    );
    if (legacy.overrideId !== null) {
      if (legacy.discountNumerator !== null && legacy.discountDenominator !== null) {
        legacyMultOverride = {
          id: legacy.overrideId,
          num: legacy.discountNumerator,
          den: legacy.discountDenominator,
        };
      } else {
        effectiveBaseCustomer = legacy.price;
        hasLegacyExact = true;
      }
    }
  }

  const customerContext: PricingContext = {
    side: "CUSTOMER",
    model: input.model,
    operation: input.operation,
    currency: input.currency,
    rated_at: ratedAt,
    execution_mode: input.execution_mode ?? "SYNC",
    channel_id: input.channel_id ?? route.channel_id,
    ...((input.upstream_account_id ?? channel?.account_id)
      ? { upstream_account_id: input.upstream_account_id ?? channel!.account_id }
      : {}),
    ...(input.billing_account_id ? { billing_account_id: input.billing_account_id } : {}),
    ...(customerGroupId ? { customer_group_id: customerGroupId } : {}),
    ...(input.service_tier ? { service_tier: input.service_tier } : {}),
    ...(input.region ? { region: input.region } : {}),
    ...(input.attrs ? { attrs: input.attrs } : {}),
  };

  let customerSnapshot: PricingSnapshot;
  if (hasLegacyExact) {
    let superseded: PricingSnapshot | null = null;
    try {
      const probe = resolvePricingSnapshotForCard(
        plan,
        rateCardFromPriceVersion(baseCustomerPrice),
        customerContext,
      );
      if (
        probe.applied_modifiers.some(
          (m) =>
            m.layer === "COMMERCIAL" &&
            (m.stacking === "EXCLUSIVE" || m.kind === "EXACT_RATE_CARD"),
        )
      ) {
        superseded = probe;
      }
    } catch {
      superseded = null;
    }
    customerSnapshot =
      superseded ??
      resolvePricingSnapshotForCard(
        plan,
        rateCardFromPriceVersion(effectiveBaseCustomer),
        customerContext,
      );
  } else {
    customerSnapshot = resolvePricingSnapshotForCard(
      plan,
      rateCardFromPriceVersion(baseCustomerPrice),
      customerContext,
    );
    if (legacyMultOverride) {
      customerSnapshot = applyLegacyOverrideMultiplierToSnapshot(
        customerSnapshot,
        legacyMultOverride.id,
        legacyMultOverride.num,
        legacyMultOverride.den,
      ).snapshot;
    }
  }

  const supplierContext: PricingContext = {
    side: "SUPPLIER",
    model: input.model,
    operation: input.operation,
    currency: baseSupplierPrice.currency,
    rated_at: ratedAt,
    execution_mode: input.execution_mode ?? "SYNC",
    channel_id: input.channel_id ?? route.channel_id,
    ...((input.upstream_account_id ?? channel?.account_id)
      ? { upstream_account_id: input.upstream_account_id ?? channel!.account_id }
      : {}),
    ...(input.billing_account_id ? { billing_account_id: input.billing_account_id } : {}),
    ...(input.service_tier ? { service_tier: input.service_tier } : {}),
    ...(input.region ? { region: input.region } : {}),
    ...(input.attrs ? { attrs: input.attrs } : {}),
  };
  const supplierSnapshot = resolvePricingSnapshotForCard(
    plan,
    rateCardFromPriceVersion(baseSupplierPrice),
    supplierContext,
  );

  const hasUsage =
    Object.keys(input.usage).length > 0 ||
    input.input_characters !== undefined ||
    customerSnapshot.basis === "REQUEST_COMPLETION";
  const customerBreakdown = hasUsage
    ? evaluateSnapshotBreakdown(customerSnapshot, input.usage, input.input_characters)
    : null;
  const supplierBreakdown = hasUsage
    ? evaluateSnapshotBreakdown(supplierSnapshot, input.usage, input.input_characters)
    : null;

  let customerReservation: SnapshotReservationQuote | null = null;
  let supplierReservation: SnapshotReservationQuote | null = null;
  if (
    input.output_bound !== undefined ||
    input.estimated_input_tokens !== undefined ||
    input.images !== undefined ||
    input.input_characters !== undefined
  ) {
    const measurement: SnapshotReservationMeasurement = {
      ...(input.input_characters !== undefined ? { inputCharacters: input.input_characters } : {}),
      ...(input.estimated_input_tokens !== undefined
        ? { estimatedInputTokens: input.estimated_input_tokens }
        : {}),
      ...(input.images !== undefined ? { images: input.images } : {}),
    };
    const outBound = input.output_bound ?? 0;
    customerReservation = quoteSnapshotReservation(customerSnapshot, measurement, outBound);
    supplierReservation = quoteSnapshotReservation(supplierSnapshot, measurement, outBound);
  }

  let margin: ReturnType<typeof serializeMargin> | null = null;
  if (customerBreakdown && supplierBreakdown) {
    const marginPolicy: MarginPolicy | undefined = input.margin_policy
      ? {
          ...(input.margin_policy.allow_negative_margin !== undefined
            ? { allow_negative_margin: input.margin_policy.allow_negative_margin }
            : {}),
          ...(input.margin_policy.min_margin_bps !== undefined
            ? { min_margin_bps: BigInt(input.margin_policy.min_margin_bps) }
            : {}),
          ...(input.margin_policy.max_loss_atoms !== undefined
            ? { max_loss_atoms: BigInt(input.margin_policy.max_loss_atoms) }
            : {}),
        }
      : undefined;
    const fxToCustomer = findActiveFxRational(
      snap,
      supplierBreakdown.currency,
      customerBreakdown.currency,
      ratedAt,
    );
    margin = serializeMargin(
      evaluateMargin({
        customerAtoms: customerBreakdown.total_atoms,
        customerCurrency: customerBreakdown.currency,
        supplierAtoms: supplierBreakdown.total_atoms,
        supplierCurrency: supplierBreakdown.currency,
        ...(fxToCustomer ? { supplierToCustomerFx: fxToCustomer } : {}),
        ...(marginPolicy ? { policy: marginPolicy } : {}),
      }),
    );
  }

  return {
    route_id: route.id,
    rated_at: ratedAt,
    customer_snapshot: customerSnapshot,
    supplier_snapshot: supplierSnapshot,
    ...(customerBreakdown ? { customer_breakdown: serializeBreakdown(customerBreakdown) } : {}),
    ...(supplierBreakdown ? { supplier_breakdown: serializeBreakdown(supplierBreakdown) } : {}),
    ...(customerReservation ? { customer_reservation: customerReservation } : {}),
    ...(supplierReservation ? { supplier_reservation: supplierReservation } : {}),
    ...(margin ? { margin } : {}),
  };
}
