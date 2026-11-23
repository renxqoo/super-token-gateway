import type { Operation, Pricing } from "@gateway/kernel";
import type {
  ConfigSnapshot,
  PriceVersionRecord,
  ProjectPricingOverrideRecord,
} from "@gateway/ledger";
import { LruMap } from "./proxy-state.ts";

export type EffectiveCustomerPrice = {
  price: PriceVersionRecord;
  overrideId: string | null;
  discountNumerator: string | null;
  discountDenominator: string | null;
  cacheKey: string;
};

const resolvedCacheBySnapshot = new WeakMap<
  ConfigSnapshot,
  LruMap<string, EffectiveCustomerPrice>
>();

function gcd(a: bigint, b: bigint): bigint {
  let x = a < 0n ? -a : a;
  let y = b < 0n ? -b : b;
  while (y !== 0n) {
    const t = x % y;
    x = y;
    y = t;
  }
  return x === 0n ? 1n : x;
}

function matchPriority(
  override: ProjectPricingOverrideRecord,
  modelName: string,
  operation: Operation,
): number {
  if (!override.enabled) return 99;
  const exactModel = override.model_name === modelName;
  const wildModel = override.model_name === "*";
  const exactOp = override.operation === operation;
  const wildOp = override.operation === "*";
  if (override.override_kind === "EXACT_PRICE") {
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

function multiplyRates(rates: Pricing["rates"], num: bigint, den: bigint): Pricing["rates"] {
  const out: Record<string, { numerator: string; denominator: string }> = {};
  for (const [metric, rate] of Object.entries(rates)) {
    if (!rate) continue;
    const rawNum = BigInt(rate.numerator) * num;
    const rawDen = BigInt(rate.denominator) * den;
    const g = gcd(rawNum, rawDen);
    out[metric] = {
      numerator: (rawNum / g).toString(),
      denominator: (rawDen / g).toString(),
    };
  }
  return out as Pricing["rates"];
}

export function resolveEffectiveCustomerPrice(
  snap: ConfigSnapshot,
  projectId: string,
  modelName: string,
  operation: Operation,
  basePrice: PriceVersionRecord,
): EffectiveCustomerPrice {
  const overrides = snap.pricingOverridesByProject.get(projectId);
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
  const lookupKey = `${projectId}\0${basePrice.id}\0${modelName}\0${operation}`;
  const cached = snapCache.get(lookupKey);
  if (cached) return cached;

  let best: ProjectPricingOverrideRecord | null = null;
  let bestPrio = 99;
  for (const o of overrides) {
    const p = matchPriority(o, modelName, operation);
    if (p < bestPrio) {
      bestPrio = p;
      best = o;
    }
  }

  let resolved: EffectiveCustomerPrice = {
    price: basePrice,
    overrideId: null,
    discountNumerator: null,
    discountDenominator: null,
    cacheKey: basePrice.id,
  };

  if (best) {
    if (best.override_kind === "EXACT_PRICE" && best.billing_price_id) {
      const customPrice = snap.prices.get(best.billing_price_id);
      if (
        customPrice &&
        customPrice.side === "CUSTOMER" &&
        customPrice.currency === basePrice.currency
      ) {
        resolved = {
          price: customPrice,
          overrideId: best.id,
          discountNumerator: null,
          discountDenominator: null,
          cacheKey: customPrice.id,
        };
      }
    } else if (
      best.override_kind === "MULTIPLIER" &&
      best.multiplier_numerator &&
      best.multiplier_denominator
    ) {
      const num = BigInt(best.multiplier_numerator);
      const den = BigInt(best.multiplier_denominator);
      if (num > 0n && den > 0n) {
        const derivedPrice: PriceVersionRecord = {
          ...basePrice,
          rates: multiplyRates(basePrice.rates, num, den),
        };
        resolved = {
          price: derivedPrice,
          overrideId: best.id,
          discountNumerator: best.multiplier_numerator,
          discountDenominator: best.multiplier_denominator,
          cacheKey: `${basePrice.id}:m:${best.id}:${best.multiplier_numerator}/${best.multiplier_denominator}`,
        };
      }
    }
  }

  snapCache.set(lookupKey, resolved);
  return resolved;
}
