import { calculateCost } from "./cost.ts";
import { validatePricing, validateUsage } from "./validation.ts";
import { dimensions } from "./values.ts";
import type { Dimension, Pricing, Usage } from "./values.ts";

export interface UpstreamCostSnapshot {
  usage: Usage;
  final_dimensions: readonly Dimension[];
}

export interface UpstreamCostInput {
  pricing: Pricing;
  estimated_quantities: Usage;
  head?: UpstreamCostSnapshot;
  trusted_partial_max?: Usage;
  unresolved_disputes?: readonly UpstreamCostSnapshot[];
}

export interface UpstreamCostResult {
  estimated_atoms: string;
  cost_atoms: string;
  remaining_reserved_atoms: string;
  exposure_atoms: string;
  complete: boolean;
  has_unresolved_disputes: boolean;
  overrun: boolean;
  overrun_dimensions: Dimension[];
  partial_risk_dimensions: Dimension[];
}

function exceeds(quantity: string, bound: string): boolean {
  const [whole, fraction = ""] = quantity.split(".");
  const [boundWhole, boundFraction = ""] = bound.split(".");
  return (
    BigInt(`${whole}${fraction.padEnd(18, "0")}`) >
    BigInt(`${boundWhole}${boundFraction.padEnd(18, "0")}`)
  );
}

function priceSnapshot(
  snapshot: UpstreamCostSnapshot,
  pricing: Pricing,
  estimated: Usage,
): {
  cost: bigint;
  exposure: bigint;
  exposure_usage: Usage;
  finalized: Set<Dimension>;
  overrun_dimensions: Dimension[];
} {
  validateUsage(snapshot.usage);
  if (!Array.isArray(snapshot.final_dimensions))
    throw new TypeError("Final dimensions must be an array");
  const finalized = new Set<Dimension>(snapshot.final_dimensions);
  for (const dimension of finalized) {
    if (!(dimensions as readonly string[]).includes(dimension))
      throw new TypeError("Unknown final dimension");
    if (snapshot.usage[dimension] === undefined)
      throw new TypeError("Final dimensions require explicit quantities");
  }
  const known: Usage = {};
  const exposure_usage: Usage = {};
  const overrun_dimensions: Dimension[] = [];
  for (const dimension of dimensions) {
    if (!pricing.rates[dimension]) continue;
    const bound = estimated[dimension]!;
    if (finalized.has(dimension)) {
      const quantity = snapshot.usage[dimension]!;
      known[dimension] = quantity;
      exposure_usage[dimension] = quantity;
      if (exceeds(quantity, bound)) overrun_dimensions.push(dimension);
    } else {
      exposure_usage[dimension] = bound;
    }
  }
  return {
    cost: BigInt(calculateCost(known, pricing)),
    exposure: BigInt(calculateCost(exposure_usage, pricing)),
    exposure_usage,
    finalized,
    overrun_dimensions,
  };
}

export function calculateUpstreamCost(input: UpstreamCostInput): UpstreamCostResult {
  const {
    pricing,
    estimated_quantities,
    trusted_partial_max = {},
    unresolved_disputes = [],
  } = input;
  validatePricing(pricing);
  for (const rate of Object.values(pricing.rates))
    if (typeof rate.numerator !== "string" || typeof rate.denominator !== "string")
      throw new TypeError("Upstream rates require exact string values");
  validateUsage(estimated_quantities);
  validateUsage(trusted_partial_max);
  if (!Object.keys(pricing.rates).length) throw new TypeError("Upstream pricing cannot be empty");
  if (!Array.isArray(unresolved_disputes))
    throw new TypeError("Unresolved disputes must be an array");
  for (const dimension of dimensions)
    if (pricing.rates[dimension] && estimated_quantities[dimension] === undefined)
      throw new TypeError("Every priced dimension requires a captured estimated quantity");
  const estimated = BigInt(calculateCost(estimated_quantities, pricing));
  const head = priceSnapshot(
    input.head ?? { usage: {}, final_dimensions: [] },
    pricing,
    estimated_quantities,
  );
  const risk_usage: Usage = { ...head.exposure_usage };
  const partial_risk_dimensions: Dimension[] = [];
  for (const dimension of dimensions) {
    if (!pricing.rates[dimension] || head.finalized.has(dimension)) continue;
    const observed = trusted_partial_max[dimension];
    if (observed !== undefined && exceeds(observed, estimated_quantities[dimension]!)) {
      risk_usage[dimension] = observed;
      partial_risk_dimensions.push(dimension);
    }
  }
  let exposure = BigInt(calculateCost(risk_usage, pricing));
  for (const dispute of unresolved_disputes) {
    const disputed = priceSnapshot(dispute, pricing, estimated_quantities).exposure;
    if (disputed > exposure) exposure = disputed;
  }
  return {
    estimated_atoms: estimated.toString(),
    cost_atoms: head.cost.toString(),
    remaining_reserved_atoms: (exposure - head.cost).toString(),
    exposure_atoms: exposure.toString(),
    complete: dimensions.every(
      (dimension) => !pricing.rates[dimension] || head.finalized.has(dimension),
    ),
    has_unresolved_disputes: unresolved_disputes.length > 0,
    overrun: head.exposure > estimated || head.overrun_dimensions.length > 0,
    overrun_dimensions: head.overrun_dimensions,
    partial_risk_dimensions,
  };
}
