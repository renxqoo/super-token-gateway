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

function multiplyRates(rates: Pricing["rates"], num: bigint, den: bigint): Pricing["rates"] | null {
  const out: Record<string, { numerator: string; denominator: string }> = {};
  for (const [metric, rate] of Object.entries(rates)) {
    if (!rate) continue;
    const rawNum = BigInt(rate.numerator) * num;
    const rawDen = BigInt(rate.denominator) * den;
    const g = gcd(rawNum, rawDen);
    const nStr = (rawNum / g).toString();
    const dStr = (rawDen / g).toString();
    if (nStr.length > 38 || dStr.length > 38) return null;
    out[metric] = {
      numerator: nStr,
      denominator: dStr,
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

  let bestPrio = 99;
  let resolved: EffectiveCustomerPrice = {
    price: basePrice,
    overrideId: null,
    discountNumerator: null,
    discountDenominator: null,
    cacheKey: basePrice.id,
  };

  for (const o of overrides) {
    const p = matchPriority(o, modelName, operation);
    if (p >= bestPrio) continue;
    if (o.override_kind === "EXACT_PRICE" && o.billing_price_id) {
      const customPrice = snap.prices.get(o.billing_price_id);
      if (
        customPrice &&
        customPrice.side === "CUSTOMER" &&
        customPrice.currency === basePrice.currency
      ) {
        bestPrio = p;
        resolved = {
          price: customPrice,
          overrideId: o.id,
          discountNumerator: null,
          discountDenominator: null,
          cacheKey: customPrice.id,
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
        const derivedRates = multiplyRates(basePrice.rates, num, den);
        if (derivedRates) {
          bestPrio = p;
          const derivedPrice: PriceVersionRecord = {
            ...basePrice,
            rates: derivedRates,
          };
          resolved = {
            price: derivedPrice,
            overrideId: o.id,
            discountNumerator: o.multiplier_numerator,
            discountDenominator: o.multiplier_denominator,
            cacheKey: `${basePrice.id}:m:${o.id}:${o.multiplier_numerator}/${o.multiplier_denominator}`,
          };
        }
      }
    }
  }

  snapCache.set(lookupKey, resolved);
  return resolved;
}
