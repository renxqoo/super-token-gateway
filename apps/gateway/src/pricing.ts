import {
  compileRatingPlan,
  formatRational,
  multiplyRational,
  parseDerivedPositiveRational,
  parseRational,
  resolveAccountPricingOverride,
  resolvePricingSnapshotForCard,
  type CompiledRatingPlan,
  type Dimension,
  type FxRateVersion,
  type HostedToolDimension,
  type Operation,
  type PriceModifier,
  type PricingContext,
  type PricingSnapshot,
  type RateCard,
} from "@gateway/kernel";
import {
  hasPricingSpec,
  rateCardFromPriceVersion,
  type BillingAccountRecord,
  type ChannelRecord,
  type ConfigSnapshot,
  type PriceVersionRecord,
  type PricingModifierRecord,
  type RouteRecord,
} from "@gateway/ledger";
import { resolvePinnedProviderPlugin } from "@gateway/plugin-engine";
import { LruMap, type HydratedCandidate } from "./proxy-state.ts";
import { MeteredRequestError, type MeteredRequestSignals } from "./request-metering.ts";
import { upstreamProtocolFor } from "./upstream-payload.ts";

export type EffectiveCustomerPrice = {
  price: PriceVersionRecord;
  overrideId: string | null;
  discountNumerator: string | null;
  discountDenominator: string | null;
  cacheKey: string;
  customerSnapshot?: PricingSnapshot;
  supplierSnapshot?: PricingSnapshot;
  meteredAllowedServiceTier?: string;
  meteredAllowedHostedTools?: ReadonlySet<HostedToolDimension>;
};

const resolvedCacheBySnapshot = new WeakMap<
  ConfigSnapshot,
  LruMap<string, EffectiveCustomerPrice>
>();
const compiledPlanBySnapshot = new WeakMap<ConfigSnapshot, CompiledRatingPlan>();

export function resolveEffectiveCustomerPrice(
  snap: ConfigSnapshot,
  billingAccountId: string,
  modelName: string,
  operation: Operation,
  basePrice: PriceVersionRecord,
  targetCurrency?: string,
): EffectiveCustomerPrice {
  const overrides = snap.pricingOverridesByBillingAccount.get(billingAccountId);
  if (!overrides || overrides.length === 0) {
    return {
      price: basePrice,
      overrideId: null,
      discountNumerator: null,
      discountDenominator: null,
      cacheKey: basePrice.id,
    };
  }

  let snapCache = resolvedCacheBySnapshot.get(snap);
  if (!snapCache) {
    snapCache = new LruMap<string, EffectiveCustomerPrice>(8192);
    resolvedCacheBySnapshot.set(snap, snapCache);
  }
  const currencyKey = targetCurrency ?? basePrice.currency;
  const lookupKey = `${billingAccountId}\0${basePrice.id}\0${modelName}\0${operation}\0${currencyKey}`;
  const cached = snapCache.get(lookupKey);
  if (cached) return cached;

  let matched = resolveAccountPricingOverride({
    modelName,
    operation,
    currency: currencyKey,
    listRates: basePrice.rates,
    overrides,
    exactPricesById: snap.prices,
    invalidPolicy: "STOP",
  });
  if (matched.kind === "NONE" && currencyKey !== basePrice.currency) {
    matched = resolveAccountPricingOverride({
      modelName,
      operation,
      currency: basePrice.currency,
      listRates: basePrice.rates,
      overrides,
      exactPricesById: snap.prices,
      invalidPolicy: "STOP",
    });
  }

  let resolved: EffectiveCustomerPrice = {
    price: basePrice,
    overrideId: null,
    discountNumerator: null,
    discountDenominator: null,
    cacheKey: basePrice.id,
  };

  if (matched.kind === "EXACT_PRICE") {
    const customPrice = snap.prices.get(matched.billing_price_id);
    if (customPrice) {
      resolved = {
        price: customPrice,
        overrideId: matched.override_id,
        discountNumerator: null,
        discountDenominator: null,
        cacheKey: customPrice.id,
      };
    }
  } else if (matched.kind === "MULTIPLIER") {
    resolved = {
      price: {
        ...basePrice,
        rates: matched.rates,
      },
      overrideId: matched.override_id,
      discountNumerator: matched.multiplier_numerator,
      discountDenominator: matched.multiplier_denominator,
      cacheKey: `${basePrice.id}:m:${matched.override_id}:${matched.multiplier_numerator}/${matched.multiplier_denominator}`,
    };
  }

  snapCache.set(lookupKey, resolved);
  return resolved;
}

function toKernelModifier(
  rec: PricingModifierRecord,
  prices: ReadonlyMap<string, PriceVersionRecord>,
): PriceModifier | null {
  if (rec.kind === "EXACT_RATE_CARD") {
    if (!rec.rate_card_price_version_id) return null;
    const price = prices.get(rec.rate_card_price_version_id);
    if (!price) return null;
    return {
      id: rec.id,
      layer: rec.layer,
      kind: "EXACT_RATE_CARD",
      stacking: rec.stacking,
      priority: rec.priority,
      enabled: rec.enabled,
      ...(rec.effective_from !== null ? { effective_from: rec.effective_from } : {}),
      ...(rec.effective_to !== null ? { effective_to: rec.effective_to } : {}),
      ...(rec.time_windows !== null && rec.time_windows.length > 0
        ? { time_windows: rec.time_windows }
        : {}),
      match: rec.match,
      rate_card: rateCardFromPriceVersion(price),
    };
  }
  if (!rec.multiplier_numerator || !rec.multiplier_denominator) return null;
  return {
    id: rec.id,
    layer: rec.layer,
    kind: "MULTIPLIER",
    stacking: rec.stacking,
    priority: rec.priority,
    enabled: rec.enabled,
    ...(rec.effective_from !== null ? { effective_from: rec.effective_from } : {}),
    ...(rec.effective_to !== null ? { effective_to: rec.effective_to } : {}),
    ...(rec.time_windows !== null && rec.time_windows.length > 0
      ? { time_windows: rec.time_windows }
      : {}),
    match: rec.match,
    multiplier: {
      numerator: rec.multiplier_numerator,
      denominator: rec.multiplier_denominator,
    },
  };
}

export function getCompiledRatingPlan(snap: ConfigSnapshot): CompiledRatingPlan {
  const cached = compiledPlanBySnapshot.get(snap);
  if (cached) return cached;
  const cards: RateCard[] = [];
  for (const price of snap.prices.values()) {
    cards.push(rateCardFromPriceVersion(price));
  }
  const modifiers: PriceModifier[] = [];
  for (const rec of snap.pricingModifiers ?? []) {
    const mod = toKernelModifier(rec, snap.prices);
    if (mod) modifiers.push(mod);
  }
  const fxRates: FxRateVersion[] = (snap.fxRateVersions ?? []).map((fx) => ({
    id: fx.id,
    base_currency: fx.base_currency,
    quote_currency: fx.quote_currency,
    rate: { numerator: fx.rate_numerator, denominator: fx.rate_denominator },
    effective_from: fx.effective_from,
    ...(fx.effective_to !== null ? { effective_to: fx.effective_to } : {}),
  }));
  const plan = compileRatingPlan({ rateCards: cards, modifiers, fxRates });
  compiledPlanBySnapshot.set(snap, plan);
  return plan;
}

export function cardPricesDimension(card: RateCard, dim: Dimension): boolean {
  if (card.rates[dim] !== undefined) return true;
  if (dim === "output_tokens" && card.reasoning_rate !== undefined) return true;
  if (card.progressive?.[dim] !== undefined) return true;
  if (card.cliff) {
    for (const tier of card.cliff.tiers) {
      if (tier.rates[dim] !== undefined) return true;
      if (dim === "output_tokens" && tier.reasoning_rate !== undefined) return true;
    }
  }
  if (card.matrix) {
    for (const entry of card.matrix) {
      if (entry.rates[dim] !== undefined) return true;
      if (dim === "output_tokens" && entry.reasoning_rate !== undefined) return true;
    }
  }
  return false;
}

function cardPricesDimensionEveryTier(card: RateCard, dim: Dimension): boolean {
  const baseOrProg = card.rates[dim] !== undefined || card.progressive?.[dim] !== undefined;
  if (!card.cliff) return baseOrProg;
  if (baseOrProg) return true;
  return card.cliff.tiers.every((tier) => tier.rates[dim] !== undefined);
}

function hasPositiveCeiling(card: RateCard, dim: Dimension): boolean {
  const raw = card.ceilings[dim];
  if (raw === undefined) return false;
  try {
    return BigInt(raw) > 0n;
  } catch {
    return false;
  }
}

function routeExtractsHostedTool(
  snap: ConfigSnapshot,
  channel: ChannelRecord | undefined,
  route: RouteRecord,
  operation: Operation,
  toolDim: HostedToolDimension,
  isStream?: boolean,
): boolean {
  if (!channel || !channel.provider_plugin_id) return false;
  const resolved = resolvePinnedProviderPlugin(channel.provider_plugin_id, snap);
  if (!resolved) return false;
  if (resolved.kind === "WASM") {
    if (!resolved.config.grants?.includes("extract_usage")) return false;
    if (isStream === true) return resolved.config.hooks.includes("adapt_stream_event");
    if (isStream === false) return resolved.config.hooks.includes("adapt_response");
    return (
      resolved.config.hooks.includes("adapt_response") &&
      resolved.config.hooks.includes("adapt_stream_event")
    );
  }
  const aliases = [
    ...(resolved.config.usage_aliases ?? []),
    ...(channel.adapter_config.usage_aliases ?? []),
    ...(route.adapter_overrides.usage_aliases ?? []),
  ];
  if (aliases.some((alias) => alias.target === toolDim)) return true;
  const upstreamProto = upstreamProtocolFor(resolved, operation);
  const usageDialect = resolved.config.usage_dialect;
  if (upstreamProto === "responses" && usageDialect === "openai") return true;
  if (
    upstreamProto === "messages" &&
    usageDialect === "anthropic" &&
    toolDim !== "file_search_calls"
  ) {
    return true;
  }
  return false;
}

export function hasPricedDimensionInPriceVersion(
  price: PriceVersionRecord,
  dim: Dimension,
): boolean {
  return cardPricesDimension(rateCardFromPriceVersion(price), dim);
}

export function hasNonZeroCacheWriteInPriceVersion(price: PriceVersionRecord): boolean {
  const card = rateCardFromPriceVersion(price);
  const checkRates = (rates: RateCard["rates"]) =>
    (rates.cache_write_tokens !== undefined && rates.cache_write_tokens.numerator !== "0") ||
    (rates.cache_write_5m_tokens !== undefined && rates.cache_write_5m_tokens.numerator !== "0") ||
    (rates.cache_write_1h_tokens !== undefined && rates.cache_write_1h_tokens.numerator !== "0");
  if (checkRates(card.rates)) return true;
  for (const dim of [
    "cache_write_tokens",
    "cache_write_5m_tokens",
    "cache_write_1h_tokens",
  ] as const) {
    const bands = card.progressive?.[dim];
    if (bands?.some((b) => b.rate.numerator !== "0")) return true;
  }
  if (card.cliff?.tiers.some((t) => checkRates(t.rates))) return true;
  if (card.matrix?.some((m) => checkRates(m.rates))) return true;
  return false;
}

export function snapshotSupportsServiceTier(snap: ConfigSnapshot, serviceTier: string): boolean {
  const mods = snap.pricingModifiers;
  if (!mods || mods.length === 0) return false;
  return mods.some(
    (m) => m.enabled && m.match.side !== "SUPPLIER" && m.match.service_tier === serviceTier,
  );
}

export function snapshotSupportsHostedTools(
  snap: ConfigSnapshot,
  modelName: string,
  operation: Operation,
  tools: ReadonlySet<HostedToolDimension>,
  isStream?: boolean,
): boolean {
  if (tools.size === 0) return true;
  const routes = snap.routesByModel.get(modelName) ?? [];
  const toolList = [...tools];
  for (const r of routes) {
    if (!r.enabled || !r.operations.includes(operation)) continue;
    const channel = snap.channels.get(r.channel_id);
    if (
      toolList.every((dim) => routeExtractsHostedTool(snap, channel, r, operation, dim, isStream))
    ) {
      return true;
    }
  }
  return false;
}

function hasAnyFxPair(snap: ConfigSnapshot, baseCurrency: string, quoteCurrency: string): boolean {
  return (
    snap.fxRateVersions?.some(
      (fx) => fx.base_currency === baseCurrency && fx.quote_currency === quoteCurrency,
    ) ?? false
  );
}

export function canRouteMatchCurrency(
  snap: ConfigSnapshot,
  billingPriceCurrency: string,
  targetCurrency: string,
  modelName?: string,
  operation?: Operation,
): boolean {
  if (billingPriceCurrency === targetCurrency) return true;
  if (hasAnyFxPair(snap, billingPriceCurrency, targetCurrency)) return true;
  if (
    snap.pricingModifiers?.some((m) => {
      if (
        !m.enabled ||
        m.kind !== "EXACT_RATE_CARD" ||
        m.match.side === "SUPPLIER" ||
        !m.rate_card_price_version_id
      ) {
        return false;
      }
      if (modelName && m.match.model && m.match.model !== "*" && m.match.model !== modelName) {
        return false;
      }
      if (
        operation &&
        m.match.operation &&
        m.match.operation !== "*" &&
        m.match.operation !== operation
      ) {
        return false;
      }
      const cardPrice = snap.prices.get(m.rate_card_price_version_id);
      if (!cardPrice || cardPrice.side !== "CUSTOMER") return false;
      return (
        cardPrice.currency === targetCurrency ||
        hasAnyFxPair(snap, cardPrice.currency, targetCurrency)
      );
    })
  ) {
    return true;
  }
  return false;
}

export function applyLegacyOverrideMultiplierToSnapshot(
  snapshot: PricingSnapshot,
  overrideId: string,
  num: string,
  den: string,
): { snapshot: PricingSnapshot; applied: boolean } {
  const hasExclusiveCommercial = snapshot.applied_modifiers.some(
    (m) => m.layer === "COMMERCIAL" && m.stacking === "EXCLUSIVE",
  );
  if (hasExclusiveCommercial) return { snapshot, applied: false };
  const current = parseDerivedPositiveRational(snapshot.combined_multiplier);
  const factor = parseRational({ numerator: num, denominator: den });
  const combined = multiplyRational(current, factor);
  return {
    applied: true,
    snapshot: {
      ...snapshot,
      combined_multiplier: formatRational(combined),
      applied_modifiers: [
        ...snapshot.applied_modifiers,
        {
          id: overrideId,
          layer: "COMMERCIAL",
          kind: "MULTIPLIER",
          stacking: "MULTIPLY",
          multiplier: { numerator: num, denominator: den },
        },
      ],
    },
  };
}

function modifierRecordById(snap: ConfigSnapshot, id: string): PricingModifierRecord | undefined {
  return snap.pricingModifiers?.find((m) => m.id === id);
}

function isTrivialSnapshot(snapshot: PricingSnapshot, basePrice: PriceVersionRecord): boolean {
  return (
    !hasPricingSpec(basePrice.pricing_spec) &&
    snapshot.rate_card_id === basePrice.id &&
    snapshot.currency === basePrice.currency &&
    snapshot.applied_modifiers.length === 0 &&
    snapshot.fx_version_id === null &&
    snapshot.matched_matrix_index === null &&
    snapshot.combined_multiplier.numerator === "1" &&
    snapshot.combined_multiplier.denominator === "1"
  );
}

export function resolveAttemptPricing(input: {
  snap: ConfigSnapshot;
  billingAccount: BillingAccountRecord;
  modelName: string;
  operation: Operation;
  candidate: HydratedCandidate;
  ratedAt: string | (() => string);
  signals: MeteredRequestSignals;
  isStream?: boolean;
}): EffectiveCustomerPrice {
  const { snap, billingAccount, modelName, operation, candidate, signals, isStream } = input;
  const legacy = resolveEffectiveCustomerPrice(
    snap,
    billingAccount.id,
    modelName,
    operation,
    candidate.billingPrice,
    billingAccount.currency,
  );
  const hasModifiers = (snap.pricingModifiers?.length ?? 0) > 0;
  const hasCustomerCardSpec = hasPricingSpec(legacy.price.pricing_spec);
  const hasSupplierCardSpec = hasPricingSpec(candidate.costPrice.pricing_spec);
  const needsFx = legacy.price.currency !== billingAccount.currency;

  const maybeNeedsPlan =
    hasCustomerCardSpec ||
    hasSupplierCardSpec ||
    hasModifiers ||
    needsFx ||
    Boolean(signals.serviceTier) ||
    signals.hostedTools.size > 0;

  if (!maybeNeedsPlan) {
    return legacy;
  }

  const plan = getCompiledRatingPlan(snap);
  const hasCustomerModCandidates =
    (plan.modifiersBySideModel.CUSTOMER.get(modelName)?.length ?? 0) > 0 ||
    plan.wildcardModifiersBySide.CUSTOMER.length > 0;
  const hasSupplierModCandidates =
    (plan.modifiersBySideModel.SUPPLIER.get(modelName)?.length ?? 0) > 0 ||
    plan.wildcardModifiersBySide.SUPPLIER.length > 0;

  const evaluateCustomerSnapshot =
    hasCustomerCardSpec ||
    hasCustomerModCandidates ||
    needsFx ||
    Boolean(signals.serviceTier) ||
    signals.hostedTools.size > 0;
  const evaluateSupplierSnapshot =
    hasSupplierCardSpec || hasSupplierModCandidates || signals.hostedTools.size > 0;

  if (!evaluateCustomerSnapshot && !evaluateSupplierSnapshot) {
    return legacy;
  }

  const ratedAt = typeof input.ratedAt === "function" ? input.ratedAt() : input.ratedAt;
  let customerSnapshot: PricingSnapshot | undefined;
  let supplierSnapshot: PricingSnapshot | undefined;
  let effectiveOverrideId = legacy.overrideId;
  let effectiveDiscNum = legacy.discountNumerator;
  let effectiveDiscDen = legacy.discountDenominator;

  if (evaluateCustomerSnapshot) {
    const customerContext: PricingContext = {
      side: "CUSTOMER",
      model: modelName,
      operation,
      currency: billingAccount.currency,
      rated_at: ratedAt,
      execution_mode: "SYNC",
      billing_account_id: billingAccount.id,
      channel_id: candidate.channel.id,
      upstream_account_id: candidate.account.id,
      ...(billingAccount.customer_group_id
        ? { customer_group_id: billingAccount.customer_group_id }
        : {}),
      ...(signals.serviceTier ? { service_tier: signals.serviceTier } : {}),
      ...(signals.attrs ? { attrs: signals.attrs } : {}),
    };

    const hasLegacyExact = legacy.overrideId !== null && legacy.discountNumerator === null;
    const hasLegacyMult =
      legacy.overrideId !== null &&
      legacy.discountNumerator !== null &&
      legacy.discountDenominator !== null;

    if (hasLegacyExact) {
      let commercialSupersedes = false;
      if (hasCustomerModCandidates) {
        try {
          const listProbe = resolvePricingSnapshotForCard(
            plan,
            rateCardFromPriceVersion(candidate.billingPrice),
            customerContext,
          );
          if (
            listProbe.applied_modifiers.some(
              (m) =>
                m.layer === "COMMERCIAL" &&
                (m.stacking === "EXCLUSIVE" || m.kind === "EXACT_RATE_CARD"),
            )
          ) {
            customerSnapshot = listProbe;
            effectiveOverrideId = null;
            effectiveDiscNum = null;
            effectiveDiscDen = null;
            commercialSupersedes = true;
          }
        } catch {
          commercialSupersedes = false;
        }
      }
      if (!commercialSupersedes) {
        customerSnapshot = resolvePricingSnapshotForCard(
          plan,
          rateCardFromPriceVersion(legacy.price),
          customerContext,
        );
      }
    } else {
      let resolved = resolvePricingSnapshotForCard(
        plan,
        rateCardFromPriceVersion(candidate.billingPrice),
        customerContext,
      );
      if (hasLegacyMult) {
        const folded = applyLegacyOverrideMultiplierToSnapshot(
          resolved,
          legacy.overrideId!,
          legacy.discountNumerator!,
          legacy.discountDenominator!,
        );
        resolved = folded.snapshot;
        if (!folded.applied) {
          effectiveOverrideId = null;
          effectiveDiscNum = null;
          effectiveDiscDen = null;
        }
      }
      customerSnapshot = resolved;
    }
  }

  if (evaluateSupplierSnapshot) {
    const supplierContext: PricingContext = {
      side: "SUPPLIER",
      model: modelName,
      operation,
      currency: candidate.costPrice.currency,
      rated_at: ratedAt,
      execution_mode: "SYNC",
      billing_account_id: billingAccount.id,
      channel_id: candidate.channel.id,
      upstream_account_id: candidate.account.id,
      ...(signals.serviceTier ? { service_tier: signals.serviceTier } : {}),
      ...(signals.attrs ? { attrs: signals.attrs } : {}),
    };
    supplierSnapshot = resolvePricingSnapshotForCard(
      plan,
      rateCardFromPriceVersion(candidate.costPrice),
      supplierContext,
    );
  }

  if (signals.serviceTier) {
    const matchedTier =
      customerSnapshot?.applied_modifiers.some(
        (applied) =>
          modifierRecordById(snap, applied.id)?.match.service_tier === signals.serviceTier,
      ) ?? false;
    if (!matchedTier) {
      throw new MeteredRequestError(
        "billing_ceiling",
        "Non-default service_tier without a billing dimension is not supported",
      );
    }
  }

  if (signals.hostedTools.size > 0) {
    const custCard = customerSnapshot?.effective_card ?? rateCardFromPriceVersion(legacy.price);
    const supCard =
      supplierSnapshot?.effective_card ?? rateCardFromPriceVersion(candidate.costPrice);
    for (const toolDim of signals.hostedTools) {
      if (
        !routeExtractsHostedTool(
          snap,
          candidate.channel,
          candidate.route,
          operation,
          toolDim,
          isStream,
        ) ||
        !cardPricesDimensionEveryTier(custCard, toolDim) ||
        !hasPositiveCeiling(custCard, toolDim) ||
        !cardPricesDimensionEveryTier(supCard, toolDim) ||
        !hasPositiveCeiling(supCard, toolDim)
      ) {
        throw new MeteredRequestError(
          "unmetered_tool",
          "Server-executed tools without a billing dimension are not supported",
        );
      }
    }
  }

  const keepCustomerSnapshot =
    customerSnapshot !== undefined &&
    (signals.hostedTools.size > 0 ||
      Boolean(signals.serviceTier) ||
      !isTrivialSnapshot(customerSnapshot, legacy.price));
  const keepSupplierSnapshot =
    supplierSnapshot !== undefined &&
    (signals.hostedTools.size > 0 || !isTrivialSnapshot(supplierSnapshot, candidate.costPrice));

  return {
    ...legacy,
    overrideId: effectiveOverrideId,
    discountNumerator: effectiveDiscNum,
    discountDenominator: effectiveDiscDen,
    ...(keepCustomerSnapshot && customerSnapshot ? { customerSnapshot } : {}),
    ...(keepSupplierSnapshot && supplierSnapshot ? { supplierSnapshot } : {}),
    ...(signals.serviceTier ? { meteredAllowedServiceTier: signals.serviceTier } : {}),
    ...(signals.hostedTools.size > 0 ? { meteredAllowedHostedTools: signals.hostedTools } : {}),
  };
}
