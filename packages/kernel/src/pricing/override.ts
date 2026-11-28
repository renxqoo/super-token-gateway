import type { Pricing } from "../values.ts";
import { scaleRatesExact } from "./rational.ts";
import type {
  AccountPricingOverrideCandidate,
  OverridePriceLookup,
  PricingBasis,
  PricingSide,
  PricingSnapshot,
  RateCard,
} from "./types.ts";

export type ResolvedAccountOverride =
  | { kind: "NONE" }
  | {
      kind: "EXACT_PRICE";
      override_id: string;
      billing_price_id: string;
      rates: Pricing["rates"];
    }
  | {
      kind: "MULTIPLIER";
      override_id: string;
      multiplier_numerator: string;
      multiplier_denominator: string;
      rates: Pricing["rates"];
    };

function overrideSpecificity(
  o: AccountPricingOverrideCandidate,
  modelName: string,
  operation: string,
): number {
  if (!o.enabled) return 99;
  const exactModel = o.model_name === modelName;
  const wildModel = o.model_name === "*";
  const exactOp = o.operation === operation;
  const wildOp = o.operation === "*";
  if (o.override_kind === "EXACT_PRICE") {
    if (exactModel && exactOp) return 1;
    if (exactModel && wildOp) return 2;
    return 99;
  }
  if (exactModel && exactOp) return 3;
  if (exactModel && wildOp) return 4;
  if (wildModel && exactOp) return 5;
  if (wildModel && wildOp) return 6;
  return 99;
}

export function resolveAccountPricingOverride(input: {
  modelName: string;
  operation: string;
  currency: string;
  listRates: Pricing["rates"];
  overrides: readonly AccountPricingOverrideCandidate[];
  exactPricesById: ReadonlyMap<string, OverridePriceLookup>;
  invalidPolicy?: "STOP" | "SKIP";
}): ResolvedAccountOverride {
  const invalidPolicy = input.invalidPolicy ?? "STOP";
  if (invalidPolicy === "STOP") {
    let bestPrio = 99;
    let bestOverride: AccountPricingOverrideCandidate | null = null;
    for (let i = 0; i < input.overrides.length; i++) {
      const o = input.overrides[i]!;
      const prio = overrideSpecificity(o, input.modelName, input.operation);
      if (prio < bestPrio) {
        bestPrio = prio;
        bestOverride = o;
      }
    }
    if (!bestOverride) return { kind: "NONE" };
    if (bestOverride.override_kind === "EXACT_PRICE" && bestOverride.billing_price_id) {
      const snap = input.exactPricesById.get(bestOverride.billing_price_id);
      if (snap && snap.side === "CUSTOMER" && snap.currency === input.currency) {
        return {
          kind: "EXACT_PRICE",
          override_id: bestOverride.id,
          billing_price_id: bestOverride.billing_price_id,
          rates: snap.rates,
        };
      }
      return { kind: "NONE" };
    }
    if (
      bestOverride.override_kind === "MULTIPLIER" &&
      bestOverride.multiplier_numerator &&
      bestOverride.multiplier_denominator
    ) {
      const num = BigInt(bestOverride.multiplier_numerator);
      const den = BigInt(bestOverride.multiplier_denominator);
      if (num > 0n && den > 0n) {
        const scaled = scaleRatesExact(input.listRates, num, den);
        if (scaled) {
          return {
            kind: "MULTIPLIER",
            override_id: bestOverride.id,
            multiplier_numerator: bestOverride.multiplier_numerator,
            multiplier_denominator: bestOverride.multiplier_denominator,
            rates: scaled,
          };
        }
      }
    }
    return { kind: "NONE" };
  }

  let bestPrio = 99;
  let result: ResolvedAccountOverride = { kind: "NONE" };

  for (let i = 0; i < input.overrides.length; i++) {
    const o = input.overrides[i]!;
    const prio = overrideSpecificity(o, input.modelName, input.operation);
    if (prio >= bestPrio) continue;
    if (o.override_kind === "EXACT_PRICE" && o.billing_price_id) {
      const snap = input.exactPricesById.get(o.billing_price_id);
      if (snap && snap.side === "CUSTOMER" && snap.currency === input.currency) {
        bestPrio = prio;
        result = {
          kind: "EXACT_PRICE",
          override_id: o.id,
          billing_price_id: o.billing_price_id,
          rates: snap.rates,
        };
      }
    } else if (
      o.override_kind === "MULTIPLIER" &&
      o.multiplier_numerator &&
      o.multiplier_denominator
    ) {
      const num = BigInt(o.multiplier_numerator);
      const den = BigInt(o.multiplier_denominator);
      if (num > 0n && den > 0n) {
        const scaled = scaleRatesExact(input.listRates, num, den);
        if (scaled) {
          bestPrio = prio;
          result = {
            kind: "MULTIPLIER",
            override_id: o.id,
            multiplier_numerator: o.multiplier_numerator,
            multiplier_denominator: o.multiplier_denominator,
            rates: scaled,
          };
        }
      }
    }
  }

  return result;
}

export function snapshotFromFlatPrice(input: {
  id: string;
  side: PricingSide;
  currency: string;
  basis: PricingBasis;
  rates: Pricing["rates"];
  ceilings: Record<string, string>;
  rated_at?: string;
  discount?: { id: string; numerator: string; denominator: string } | null;
}): PricingSnapshot {
  const card: RateCard = {
    id: input.id,
    side: input.side,
    currency: input.currency,
    basis: input.basis,
    rates: input.rates,
    ceilings: input.ceilings,
  };
  return {
    version: 1,
    rated_at: input.rated_at ?? "1970-01-01T00:00:00.000Z",
    side: input.side,
    currency: input.currency,
    base_currency: input.currency,
    rate_card_id: input.id,
    basis: input.basis,
    effective_card: card,
    matched_matrix_index: null,
    applied_modifiers: input.discount
      ? [
          {
            id: input.discount.id,
            layer: "COMMERCIAL",
            kind: "MULTIPLIER",
            stacking: "EXCLUSIVE",
            multiplier: {
              numerator: input.discount.numerator,
              denominator: input.discount.denominator,
            },
          },
        ]
      : [],
    fx_version_id: null,
    fx_rate: null,
    combined_multiplier: input.discount
      ? { numerator: input.discount.numerator, denominator: input.discount.denominator }
      : { numerator: "1", denominator: "1" },
  };
}
