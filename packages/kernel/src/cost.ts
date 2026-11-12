import { dimensions } from "./values.ts";
import { validatePricing, validateUsage } from "./validation.ts";
import type { Pricing, Usage } from "./values.ts";

const maxAtoms = 10n ** 38n - 1n;
function gcd(a: bigint, b: bigint): bigint {
  while (b !== 0n) {
    const remainder = a % b;
    a = b;
    b = remainder;
  }
  return a;
}

export function calculateCost(usage: Usage, pricing: Pricing): string {
  validateUsage(usage);
  validatePricing(pricing);
  let numerator = 0n;
  let denominator = 1n;
  for (const dimension of dimensions) {
    const rate = pricing.rates[dimension];
    if (!rate) continue;
    const parts = (usage[dimension] ?? "0").split(".");
    const quantity = BigInt(parts.join(""));
    const divisor = BigInt(rate.denominator) * 10n ** BigInt(parts[1]?.length ?? 0);
    numerator = numerator * divisor + quantity * BigInt(rate.numerator) * denominator;
    denominator *= divisor;
    const common = gcd(numerator, denominator);
    numerator /= common;
    denominator /= common;
  }
  const result = (numerator + denominator - 1n) / denominator;
  if (result > maxAtoms) throw new RangeError("Cost exceeds NUMERIC(38,0)");
  return result.toString();
}

export function count(value: unknown): string | undefined {
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return String(value);
  if (typeof value === "string" && /^(0|[1-9][0-9]{0,37})$/.test(value)) return value;
  return undefined;
}
