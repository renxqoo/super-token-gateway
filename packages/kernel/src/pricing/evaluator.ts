import { DomainError } from "../errors.ts";
import { hasPricedTokenUsage } from "../cost.ts";
import { cacheWritePartitionMatches, validateUsage } from "../validation.ts";
import { dimensions, type Dimension, type Pricing, type Usage } from "../values.ts";
import {
  compileRateCard,
  type CompiledCliffTier,
  type CompiledProgressiveBand,
  type CompiledRateCard,
} from "./compiler.ts";
import {
  addRational,
  ceilRational,
  compareRational,
  formatDecimalRational,
  multiplyRational,
  parseDecimalToRational,
  parseDerivedPositiveRational,
  parseRational,
  quantizeQuantity,
  reduceRational,
  ZERO_RATIONAL,
} from "./rational.ts";
import { isDecimalDimension, validatePricingSnapshot } from "./validate.ts";
import type {
  CostBreakdown,
  CostComponentBreakdown,
  ExactRational,
  PricingSnapshot,
} from "./types.ts";

export type CompiledSnapshot = {
  snapshot: PricingSnapshot;
  card: CompiledRateCard;
  combinedMultiplier: ExactRational;
  fxRate: ExactRational | null;
  effectiveMinChargeAtoms: bigint | null;
  effectiveMaxChargeAtoms: bigint | null;
};

const snapshotCompileCache = new WeakMap<PricingSnapshot, CompiledSnapshot>();

export function compileSnapshot(snapshot: PricingSnapshot): CompiledSnapshot {
  const cached = snapshotCompileCache.get(snapshot);
  if (cached) return cached;
  validatePricingSnapshot(snapshot);
  const card = compileRateCard(snapshot.effective_card);
  const combinedMultiplier = parseDerivedPositiveRational(snapshot.combined_multiplier);
  const fxRate = snapshot.fx_rate ? parseRational(snapshot.fx_rate) : null;
  const effectiveMinChargeAtoms =
    card.minChargeAtoms !== null
      ? fxRate
        ? ceilRational(
            multiplyRational({ numerator: card.minChargeAtoms, denominator: 1n }, fxRate),
          )
        : card.minChargeAtoms
      : null;
  const effectiveMaxChargeAtoms =
    card.maxChargeAtoms !== null
      ? fxRate
        ? ceilRational(
            multiplyRational({ numerator: card.maxChargeAtoms, denominator: 1n }, fxRate),
          )
        : card.maxChargeAtoms
      : null;
  const compiled: CompiledSnapshot = {
    snapshot,
    card,
    combinedMultiplier,
    fxRate,
    effectiveMinChargeAtoms,
    effectiveMaxChargeAtoms,
  };
  snapshotCompileCache.set(snapshot, compiled);
  return compiled;
}

export function computeCliffDriverSum(
  usage: Usage,
  driver: readonly Dimension[],
  hasAggregateCacheWrite: boolean,
  hasTtlCacheWrite: boolean,
): bigint {
  let sum = 0n;
  const skipTtl = hasAggregateCacheWrite && usage.cache_write_tokens !== undefined;
  let addedSynthCache = false;
  for (let i = 0; i < driver.length; i++) {
    const dim = driver[i]!;
    if (skipTtl && (dim === "cache_write_5m_tokens" || dim === "cache_write_1h_tokens")) {
      continue;
    }
    const raw = usage[dim];
    if (raw !== undefined) {
      sum += BigInt(raw);
    } else if (dim === "cache_write_tokens" && !hasTtlCacheWrite && !addedSynthCache) {
      addedSynthCache = true;
      if (usage.cache_write_5m_tokens !== undefined) sum += BigInt(usage.cache_write_5m_tokens);
      if (usage.cache_write_1h_tokens !== undefined) sum += BigInt(usage.cache_write_1h_tokens);
    }
  }
  return sum;
}

export function selectCliffTierIndex(card: CompiledRateCard, usage: Usage): number | null {
  if (!card.cliff) return null;
  const driverSum = computeCliffDriverSum(
    usage,
    card.cliff.driver,
    card.cliff.hasAggregateCacheWrite,
    card.cliff.hasTtlCacheWrite,
  );
  let matched = 0;
  const tiers = card.cliff.tiers;
  for (let i = 0; i < tiers.length; i++) {
    if (driverSum >= tiers[i]!.minDriver) matched = i;
    else break;
  }
  return matched;
}

function evaluateProgressive(
  quantity: ExactRational,
  bands: readonly CompiledProgressiveBand[],
): ExactRational {
  let prev = ZERO_RATIONAL;
  let total = ZERO_RATIONAL;
  for (let i = 0; i < bands.length; i++) {
    const band = bands[i]!;
    if (band.upTo === null) {
      const span = reduceRational(
        quantity.numerator * prev.denominator - prev.numerator * quantity.denominator,
        quantity.denominator * prev.denominator,
      );
      if (span.numerator > 0n) {
        total = addRational(total, multiplyRational(span, band.rate));
      }
      break;
    }
    const cmp = compareRational(quantity, band.upTo);
    const upper = cmp <= 0 ? quantity : band.upTo;
    const span = reduceRational(
      upper.numerator * prev.denominator - prev.numerator * upper.denominator,
      upper.denominator * prev.denominator,
    );
    if (span.numerator > 0n) {
      total = addRational(total, multiplyRational(span, band.rate));
    }
    if (cmp <= 0) break;
    prev = band.upTo;
  }
  return total;
}

function hasPositiveProgressiveRate(
  bands: readonly CompiledProgressiveBand[] | undefined,
): boolean {
  if (!bands) return false;
  for (let i = 0; i < bands.length; i++) {
    if (bands[i]!.rate.numerator > 0n) return true;
  }
  return false;
}

function isDimensionPricedPositive(
  dim: Dimension,
  activeRates: Partial<Record<Dimension, ExactRational>>,
  progressive: Partial<Record<Dimension, readonly CompiledProgressiveBand[]>>,
  cliffOverrides?: Partial<Record<Dimension, ExactRational>>,
): boolean {
  if (cliffOverrides?.[dim] !== undefined) {
    return cliffOverrides[dim]!.numerator > 0n;
  }
  if (progressive[dim] !== undefined) {
    return hasPositiveProgressiveRate(progressive[dim]);
  }
  const rate = activeRates[dim];
  return rate !== undefined && rate.numerator > 0n;
}

function checkCacheWriteCompleteness(
  usage: Usage,
  activeRates: Partial<Record<Dimension, ExactRational>>,
  progressive: Partial<Record<Dimension, readonly CompiledProgressiveBand[]>>,
  cliffOverrides?: Partial<Record<Dimension, ExactRational>>,
): Usage {
  const total = usage.cache_write_tokens;
  const write5m = usage.cache_write_5m_tokens;
  const write1h = usage.cache_write_1h_tokens;
  if (!cacheWritePartitionMatches(total, write5m, write1h)) {
    throw new DomainError("usage_alias_conflict", "Provider cache write partitions disagree");
  }
  const cacheWritesReported = total !== undefined || write5m !== undefined || write1h !== undefined;
  const noCacheWrites = total === "0" && (write5m ?? "0") === "0" && (write1h ?? "0") === "0";
  const pricesTotal = isDimensionPricedPositive(
    "cache_write_tokens",
    activeRates,
    progressive,
    cliffOverrides,
  );
  const prices5m = isDimensionPricedPositive(
    "cache_write_5m_tokens",
    activeRates,
    progressive,
    cliffOverrides,
  );
  const prices1h = isDimensionPricedPositive(
    "cache_write_1h_tokens",
    activeRates,
    progressive,
    cliffOverrides,
  );
  const needsTotal = pricesTotal && total === undefined;
  if (
    cacheWritesReported &&
    !noCacheWrites &&
    ((needsTotal && (write5m === undefined || write1h === undefined)) ||
      (prices5m && write5m === undefined) ||
      (prices1h && write1h === undefined))
  ) {
    throw new DomainError(
      "invalid_upstream_usage",
      "Upstream omitted cache write usage required by pricing",
    );
  }
  if (needsTotal && write5m !== undefined && write1h !== undefined) {
    return {
      ...usage,
      cache_write_tokens: (BigInt(write5m) + BigInt(write1h)).toString(),
    };
  }
  return usage;
}

function hasRatedDimensionUsage(
  usage: Usage,
  card: CompiledRateCard,
  activeRates: Partial<Record<Dimension, ExactRational>>,
  cliffTier: CompiledCliffTier | null,
  requireCacheEvidence: boolean,
): boolean {
  const reasoningRate = cliffTier?.reasoningRate ?? card.reasoningRate;
  if (
    reasoningRate !== null &&
    reasoningRate.numerator > 0n &&
    (usage.output_tokens === undefined || usage.reasoning_tokens === undefined)
  ) {
    return false;
  }
  for (const dim of card.activeDimensions) {
    if (usage[dim] !== undefined) continue;
    if (
      !requireCacheEvidence &&
      (dim === "cached_input_tokens" ||
        dim === "cache_write_tokens" ||
        dim === "cache_write_5m_tokens" ||
        dim === "cache_write_1h_tokens")
    ) {
      continue;
    }
    if (
      usage.cache_write_tokens === "0" &&
      (dim === "cache_write_5m_tokens" || dim === "cache_write_1h_tokens")
    ) {
      continue;
    }
    if (isDimensionPricedPositive(dim, activeRates, card.progressive, cliffTier?.rates)) {
      return false;
    }
  }
  return true;
}

function withTtlCliffEvidence(usage: Usage, card: CompiledRateCard): Usage {
  const cliff = card.cliff;
  if (!cliff?.hasTtlCacheWrite || cliff.hasAggregateCacheWrite) return usage;
  let complete = usage;
  for (const dim of cliff.driver) {
    if (dim !== "cache_write_5m_tokens" && dim !== "cache_write_1h_tokens") continue;
    if (complete[dim] !== undefined) continue;
    const total = complete.cache_write_tokens;
    const other =
      complete[dim === "cache_write_5m_tokens" ? "cache_write_1h_tokens" : "cache_write_5m_tokens"];
    if (total === "0") {
      complete = { ...complete, [dim]: "0" };
    } else if (total !== undefined && other !== undefined) {
      complete = { ...complete, [dim]: (BigInt(total) - BigInt(other)).toString() };
    } else {
      throw new DomainError(
        "invalid_upstream_usage",
        "Upstream omitted cache write usage required by pricing",
      );
    }
  }
  return complete;
}

export function evaluateCompiledSnapshotInternal(
  compiled: CompiledSnapshot,
  rawUsage: Usage,
  options?: {
    inputCharacters?: string | undefined;
    forcedCliffTierIndex?: number | null | undefined;
    collectBreakdown?: boolean;
    skipRequiredTokenCheck?: boolean;
    requireRatedDimensions?: boolean;
  },
): CostBreakdown {
  const { snapshot, card } = compiled;
  let effectiveUsage: Usage;
  if (snapshot.basis === "REQUEST_COMPLETION") {
    effectiveUsage = { requests: "1" };
  } else if (snapshot.basis === "INPUT_CHARACTERS") {
    effectiveUsage = {
      characters: options?.inputCharacters ?? rawUsage.characters ?? "0",
      requests: "1",
    };
  } else {
    effectiveUsage = rawUsage;
  }
  validateUsage(effectiveUsage);
  if (snapshot.basis === "PROVIDER_USAGE" && options?.requireRatedDimensions) {
    effectiveUsage = withTtlCliffEvidence(effectiveUsage, card);
  }

  const cliffIndex =
    options?.forcedCliffTierIndex !== undefined
      ? options.forcedCliffTierIndex
      : selectCliffTierIndex(card, effectiveUsage);
  const cliffTier = cliffIndex !== null && card.cliff ? card.cliff.tiers[cliffIndex]! : null;

  const activeRates: Partial<Record<Dimension, ExactRational>> = cliffTier
    ? { ...card.rates, ...cliffTier.rates }
    : card.rates;
  const activeReasoningRate = cliffTier?.reasoningRate ?? card.reasoningRate;

  if (snapshot.basis === "PROVIDER_USAGE" && !options?.skipRequiredTokenCheck) {
    const rawRateView: Pricing["rates"] = {};
    if (
      isDimensionPricedPositive("input_tokens", activeRates, card.progressive, cliffTier?.rates)
    ) {
      rawRateView.input_tokens = { numerator: "1", denominator: "1" };
    }
    if (
      isDimensionPricedPositive("output_tokens", activeRates, card.progressive, cliffTier?.rates) ||
      (activeReasoningRate !== null && activeReasoningRate.numerator > 0n)
    ) {
      rawRateView.output_tokens = { numerator: "1", denominator: "1" };
    }
    if (!hasPricedTokenUsage(effectiveUsage, rawRateView)) {
      throw new DomainError(
        "invalid_upstream_usage",
        "Upstream omitted token usage required by pricing",
      );
    }
    effectiveUsage = checkCacheWriteCompleteness(
      effectiveUsage,
      activeRates,
      card.progressive,
      cliffTier?.rates,
    );
    if (
      options?.requireRatedDimensions &&
      !hasRatedDimensionUsage(effectiveUsage, card, activeRates, cliffTier, false)
    ) {
      throw new DomainError("invalid_upstream_usage", "Upstream omitted usage required by pricing");
    }
  }

  let totalRational = ZERO_RATIONAL;
  const components: CostComponentBreakdown[] = [];
  const collect = options?.collectBreakdown === true;

  for (let i = 0; i < card.activeDimensions.length; i++) {
    const dim = card.activeDimensions[i]!;
    const rawText = effectiveUsage[dim];
    if (rawText === undefined || rawText === "0") continue;

    const rawQty = isDecimalDimension(dim)
      ? parseDecimalToRational(rawText)
      : { numerator: BigInt(rawText), denominator: 1n };
    if (rawQty.numerator === 0n) continue;

    const q = card.quantizers[dim];
    const quantized = q ? quantizeQuantity(rawQty, q.minUnits, q.step) : rawQty;

    if (
      dim === "output_tokens" &&
      activeReasoningRate !== null &&
      effectiveUsage.reasoning_tokens !== undefined &&
      effectiveUsage.reasoning_tokens !== "0"
    ) {
      const reasoningQty: ExactRational = {
        numerator: BigInt(effectiveUsage.reasoning_tokens),
        denominator: 1n,
      };
      const nonReasoningNum = quantized.numerator - reasoningQty.numerator * quantized.denominator;
      const nonReasoningQty =
        nonReasoningNum > 0n
          ? reduceRational(nonReasoningNum, quantized.denominator)
          : ZERO_RATIONAL;

      const reasoningCost = multiplyRational(reasoningQty, activeReasoningRate);
      totalRational = addRational(totalRational, reasoningCost);
      if (collect) {
        components.push({
          dimension: "reasoning_tokens",
          curve: "REASONING",
          raw_quantity: effectiveUsage.reasoning_tokens,
          quantized_quantity: effectiveUsage.reasoning_tokens,
          numerator: reasoningCost.numerator.toString(),
          denominator: reasoningCost.denominator.toString(),
        });
      }
      if (nonReasoningQty.numerator > 0n) {
        const progBands = card.progressive[dim];
        const useProgressive = progBands !== undefined && !cliffTier?.rates[dim];
        const outCost = useProgressive
          ? evaluateProgressive(nonReasoningQty, progBands)
          : activeRates[dim]
            ? multiplyRational(nonReasoningQty, activeRates[dim]!)
            : ZERO_RATIONAL;
        totalRational = addRational(totalRational, outCost);
        if (collect) {
          components.push({
            dimension: dim,
            curve: useProgressive ? "PROGRESSIVE" : cliffTier?.rates[dim] ? "CLIFF" : "FLAT",
            raw_quantity: formatDecimalRational(nonReasoningQty),
            quantized_quantity: formatDecimalRational(nonReasoningQty),
            numerator: outCost.numerator.toString(),
            denominator: outCost.denominator.toString(),
          });
        }
      }
      continue;
    }

    const progBands = card.progressive[dim];
    let dimCost: ExactRational;
    let curve: "FLAT" | "CLIFF" | "PROGRESSIVE";
    if (progBands && !cliffTier?.rates[dim]) {
      dimCost = evaluateProgressive(quantized, progBands);
      curve = "PROGRESSIVE";
    } else {
      const rate = activeRates[dim];
      if (!rate || rate.numerator === 0n) continue;
      dimCost = multiplyRational(quantized, rate);
      curve = cliffTier?.rates[dim] ? "CLIFF" : "FLAT";
    }
    totalRational = addRational(totalRational, dimCost);
    if (collect) {
      components.push({
        dimension: dim,
        curve,
        raw_quantity: rawText,
        quantized_quantity: formatDecimalRational(quantized),
        numerator: dimCost.numerator.toString(),
        denominator: dimCost.denominator.toString(),
      });
    }
  }

  const finalRational = multiplyRational(totalRational, compiled.combinedMultiplier);
  const rawAtoms = ceilRational(finalRational);
  let totalAtoms = rawAtoms;
  let clampedBy: "MIN_CHARGE" | "MAX_CHARGE" | null = null;
  if (compiled.effectiveMinChargeAtoms !== null && totalAtoms < compiled.effectiveMinChargeAtoms) {
    totalAtoms = compiled.effectiveMinChargeAtoms;
    clampedBy = "MIN_CHARGE";
  }
  if (compiled.effectiveMaxChargeAtoms !== null && totalAtoms > compiled.effectiveMaxChargeAtoms) {
    totalAtoms = compiled.effectiveMaxChargeAtoms;
    clampedBy = "MAX_CHARGE";
  }

  return {
    currency: snapshot.currency,
    rate_card_id: snapshot.rate_card_id,
    matched_cliff_tier_index: cliffIndex,
    components,
    pre_modifier_numerator: totalRational.numerator.toString(),
    pre_modifier_denominator: totalRational.denominator.toString(),
    combined_multiplier: snapshot.combined_multiplier,
    raw_atoms: rawAtoms,
    clamped_by: clampedBy,
    total_atoms: totalAtoms,
  };
}

export function evaluateCompiledSnapshotAtoms(
  compiled: CompiledSnapshot,
  usage: Usage,
  inputCharacters?: string,
): bigint {
  return evaluateCompiledSnapshotInternal(compiled, usage, {
    inputCharacters,
    collectBreakdown: false,
    requireRatedDimensions: true,
  }).total_atoms;
}

export function evaluateSnapshotAtoms(
  snapshot: PricingSnapshot,
  usage: Usage,
  inputCharacters?: string,
): bigint {
  return evaluateCompiledSnapshotAtoms(compileSnapshot(snapshot), usage, inputCharacters);
}

export function hasCompleteSnapshotUsage(snapshot: PricingSnapshot, usage: Usage): boolean {
  if (snapshot.basis === "REQUEST_COMPLETION") return true;
  if (snapshot.basis === "INPUT_CHARACTERS") return usage.characters !== undefined;
  let compiled: CompiledSnapshot;
  try {
    compiled = compileSnapshot(snapshot);
    validateUsage(usage);
    usage = withTtlCliffEvidence(usage, compiled.card);
  } catch {
    return false;
  }
  const { card } = compiled;
  if (card.cliff) {
    for (let i = 0; i < card.cliff.driver.length; i++) {
      const dim = card.cliff.driver[i]!;
      if (dim === "cache_write_tokens") {
        if (
          usage.cache_write_tokens === undefined &&
          (usage.cache_write_5m_tokens === undefined || usage.cache_write_1h_tokens === undefined)
        ) {
          return false;
        }
        continue;
      }
      if (dim === "cache_write_5m_tokens" || dim === "cache_write_1h_tokens") {
        if (usage[dim] === undefined && usage.cache_write_tokens === undefined) {
          return false;
        }
        continue;
      }
      if (usage[dim] === undefined) return false;
    }
  }
  const cliffIndex = selectCliffTierIndex(card, usage);
  const cliffTier = cliffIndex !== null && card.cliff ? card.cliff.tiers[cliffIndex]! : null;
  const activeRates: Partial<Record<Dimension, ExactRational>> = cliffTier
    ? { ...card.rates, ...cliffTier.rates }
    : card.rates;
  try {
    const checked = checkCacheWriteCompleteness(
      usage,
      activeRates,
      card.progressive,
      cliffTier?.rates,
    );
    return hasRatedDimensionUsage(checked, card, activeRates, cliffTier, true);
  } catch {
    return false;
  }
}

export function evaluateSnapshotBreakdown(
  snapshot: PricingSnapshot,
  usage: Usage,
  inputCharacters?: string,
): CostBreakdown {
  return evaluateCompiledSnapshotInternal(compileSnapshot(snapshot), usage, {
    inputCharacters,
    collectBreakdown: true,
  });
}

export { dimensions };
