import { dimensions, type Dimension } from "../values.ts";
import { parseDecimalToRational, parseRational, UNIT_RATIONAL } from "./rational.ts";
import { compileTimeWindow, parseIsoTimestampMs, type CompiledTimeWindow } from "./time.ts";
import { validateFxRateVersion, validatePriceModifier, validateRateCard } from "./validate.ts";
import type {
  ExactRational,
  FxRateVersion,
  PriceModifier,
  PricingSide,
  RateCard,
} from "./types.ts";

export type CompiledQuantizer = {
  minUnits: ExactRational | null;
  step: ExactRational | null;
};

export type CompiledProgressiveBand = {
  upTo: ExactRational | null;
  rate: ExactRational;
};

export type CompiledCliffTier = {
  minDriver: bigint;
  rates: Partial<Record<Dimension, ExactRational>>;
  reasoningRate: ExactRational | null;
};

export type CompiledCliff = {
  driver: readonly Dimension[];
  hasAggregateCacheWrite: boolean;
  hasTtlCacheWrite: boolean;
  tiers: readonly CompiledCliffTier[];
};

export type CompiledRateCard = {
  raw: RateCard;
  rates: Partial<Record<Dimension, ExactRational>>;
  reasoningRate: ExactRational | null;
  quantizers: Partial<Record<Dimension, CompiledQuantizer>>;
  cliff: CompiledCliff | null;
  progressive: Partial<Record<Dimension, readonly CompiledProgressiveBand[]>>;
  minChargeAtoms: bigint | null;
  maxChargeAtoms: bigint | null;
  activeDimensions: readonly Dimension[];
};

export type CompiledModifier = {
  raw: PriceModifier;
  fromMs: number | null;
  toMs: number | null;
  timeWindows: readonly CompiledTimeWindow[];
  multiplier: ExactRational | null;
  compiledCard: CompiledRateCard | null;
  attrEntries: readonly [string, string | number | boolean][];
};

export type CompiledFxRate = {
  raw: FxRateVersion;
  rate: ExactRational;
  fromMs: number | null;
  toMs: number | null;
};

export type CompiledRatingPlan = {
  cardsBySide: Record<PricingSide, readonly RateCard[]>;
  cardsBySideModel: Record<PricingSide, Map<string, RateCard[]>>;
  wildcardCardsBySide: Record<PricingSide, RateCard[]>;
  modifiersBySideModel: Record<PricingSide, Map<string, CompiledModifier[]>>;
  wildcardModifiersBySide: Record<PricingSide, CompiledModifier[]>;
  fxByQuote: Map<string, CompiledFxRate[]>;
  compiledCardCache: WeakMap<RateCard, CompiledRateCard>;
};

function compileRatesMap(
  rates: Partial<Record<Dimension, { numerator: string; denominator: string }>>,
): Partial<Record<Dimension, ExactRational>> {
  const out: Partial<Record<Dimension, ExactRational>> = {};
  for (const [key, val] of Object.entries(rates)) {
    if (!val) continue;
    out[key as Dimension] = parseRational(val);
  }
  return out;
}

export function compileRateCard(card: RateCard): CompiledRateCard {
  const rates = compileRatesMap(card.rates);
  const reasoningRate = card.reasoning_rate ? parseRational(card.reasoning_rate) : null;
  const quantizers: Partial<Record<Dimension, CompiledQuantizer>> = {};
  if (card.quantizers) {
    for (const [dim, q] of Object.entries(card.quantizers)) {
      if (!q) continue;
      quantizers[dim as Dimension] = {
        minUnits: q.min_units !== undefined ? parseDecimalToRational(q.min_units) : null,
        step: q.step !== undefined ? parseDecimalToRational(q.step) : null,
      };
    }
  }
  let cliff: CompiledCliff | null = null;
  if (card.cliff) {
    const driverSet = new Set<string>(card.cliff.driver);
    cliff = {
      driver: card.cliff.driver,
      hasAggregateCacheWrite: driverSet.has("cache_write_tokens"),
      hasTtlCacheWrite:
        driverSet.has("cache_write_5m_tokens") || driverSet.has("cache_write_1h_tokens"),
      tiers: card.cliff.tiers.map((t) => ({
        minDriver: BigInt(t.min_driver),
        rates: compileRatesMap(t.rates),
        reasoningRate: t.reasoning_rate ? parseRational(t.reasoning_rate) : null,
      })),
    };
  }
  const progressive: Partial<Record<Dimension, readonly CompiledProgressiveBand[]>> = {};
  if (card.progressive) {
    for (const [dim, bands] of Object.entries(card.progressive)) {
      if (!bands) continue;
      progressive[dim as Dimension] = bands.map((b) => ({
        upTo: b.up_to === null ? null : parseDecimalToRational(b.up_to),
        rate: parseRational(b.rate),
      }));
    }
  }
  const dimSet = new Set<Dimension>();
  for (const dim of dimensions) {
    if (rates[dim] !== undefined || progressive[dim] !== undefined) dimSet.add(dim);
  }
  if (cliff) {
    for (const tier of cliff.tiers) {
      for (const dim of Object.keys(tier.rates)) dimSet.add(dim as Dimension);
    }
  }
  if (reasoningRate !== null || (cliff?.tiers.some((t) => t.reasoningRate !== null) ?? false)) {
    dimSet.add("output_tokens");
  }
  return {
    raw: card,
    rates,
    reasoningRate,
    quantizers,
    cliff,
    progressive,
    minChargeAtoms: card.min_charge_atoms !== undefined ? BigInt(card.min_charge_atoms) : null,
    maxChargeAtoms: card.max_charge_atoms !== undefined ? BigInt(card.max_charge_atoms) : null,
    activeDimensions: [...dimSet],
  };
}

export function compileRatingPlan(input: {
  rateCards: readonly RateCard[];
  modifiers?: readonly PriceModifier[];
  fxRates?: readonly FxRateVersion[];
  requireCeilings?: boolean;
}): CompiledRatingPlan {
  const cardsBySide: Record<PricingSide, RateCard[]> = { CUSTOMER: [], SUPPLIER: [] };
  const cardsBySideModel: Record<PricingSide, Map<string, RateCard[]>> = {
    CUSTOMER: new Map(),
    SUPPLIER: new Map(),
  };
  const wildcardCardsBySide: Record<PricingSide, RateCard[]> = { CUSTOMER: [], SUPPLIER: [] };
  const compiledCardCache = new WeakMap<RateCard, CompiledRateCard>();

  for (const card of input.rateCards) {
    validateRateCard(
      card,
      input.requireCeilings !== undefined ? { requireCeilings: input.requireCeilings } : undefined,
    );
    compiledCardCache.set(card, compileRateCard(card));
    cardsBySide[card.side].push(card);
    if (!card.model || card.model === "*") {
      wildcardCardsBySide[card.side].push(card);
    } else {
      let bucket = cardsBySideModel[card.side].get(card.model);
      if (!bucket) {
        bucket = [];
        cardsBySideModel[card.side].set(card.model, bucket);
      }
      bucket.push(card);
    }
  }

  const modifiersBySideModel: Record<PricingSide, Map<string, CompiledModifier[]>> = {
    CUSTOMER: new Map(),
    SUPPLIER: new Map(),
  };
  const wildcardModifiersBySide: Record<PricingSide, CompiledModifier[]> = {
    CUSTOMER: [],
    SUPPLIER: [],
  };

  const sortedModifiers = [...(input.modifiers ?? [])].sort((a, b) =>
    a.priority !== b.priority ? a.priority - b.priority : a.id.localeCompare(b.id),
  );
  for (const mod of sortedModifiers) {
    validatePriceModifier(
      mod,
      input.requireCeilings !== undefined ? { requireCeilings: input.requireCeilings } : undefined,
    );
    if (!mod.enabled) continue;
    const compiled: CompiledModifier = {
      raw: mod,
      fromMs: mod.effective_from !== undefined ? parseIsoTimestampMs(mod.effective_from) : null,
      toMs: mod.effective_to !== undefined ? parseIsoTimestampMs(mod.effective_to) : null,
      timeWindows: (mod.time_windows ?? []).map(compileTimeWindow),
      multiplier: mod.multiplier ? parseRational(mod.multiplier) : null,
      compiledCard: mod.rate_card ? compileRateCard(mod.rate_card) : null,
      attrEntries: mod.match.attrs ? Object.entries(mod.match.attrs) : [],
    };
    const sides: PricingSide[] = mod.match.side
      ? [mod.match.side]
      : mod.kind === "EXACT_RATE_CARD" && mod.rate_card
        ? [mod.rate_card.side]
        : ["CUSTOMER", "SUPPLIER"];
    const modelKey = mod.match.model;
    for (const side of sides) {
      if (!modelKey || modelKey === "*") {
        wildcardModifiersBySide[side].push(compiled);
      } else {
        let bucket = modifiersBySideModel[side].get(modelKey);
        if (!bucket) {
          bucket = [];
          modifiersBySideModel[side].set(modelKey, bucket);
        }
        bucket.push(compiled);
      }
    }
  }

  const fxByQuote = new Map<string, CompiledFxRate[]>();
  for (const fx of input.fxRates ?? []) {
    validateFxRateVersion(fx);
    const compiledFx: CompiledFxRate = {
      raw: fx,
      rate: parseRational(fx.rate),
      fromMs: fx.effective_from !== undefined ? parseIsoTimestampMs(fx.effective_from) : null,
      toMs: fx.effective_to !== undefined ? parseIsoTimestampMs(fx.effective_to) : null,
    };
    let bucket = fxByQuote.get(fx.quote_currency);
    if (!bucket) {
      bucket = [];
      fxByQuote.set(fx.quote_currency, bucket);
    }
    bucket.push(compiledFx);
  }

  return {
    cardsBySide,
    cardsBySideModel,
    wildcardCardsBySide,
    modifiersBySideModel,
    wildcardModifiersBySide,
    fxByQuote,
    compiledCardCache,
  };
}

export function getOrCompileRateCard(
  card: RateCard,
  cache?: WeakMap<RateCard, CompiledRateCard>,
): CompiledRateCard {
  const hit = cache?.get(card);
  if (hit) return hit;
  const compiled = compileRateCard(card);
  cache?.set(card, compiled);
  return compiled;
}

export { UNIT_RATIONAL };
