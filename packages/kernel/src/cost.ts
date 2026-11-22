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
    const quantityText = usage[dimension];
    if (!rate || quantityText === undefined || quantityText === "0" || rate.numerator === "0")
      continue;
    const decimal = quantityText.indexOf(".");
    const quantity = BigInt(decimal < 0 ? quantityText : quantityText.replace(".", ""));
    const product = quantity * BigInt(rate.numerator);
    const rateDivisor = BigInt(rate.denominator);
    if (decimal < 0 && rateDivisor === 1n) {
      numerator += product * denominator;
      continue;
    }
    const divisor =
      decimal < 0 ? rateDivisor : rateDivisor * 10n ** BigInt(quantityText.length - decimal - 1);
    numerator = numerator * divisor + product * denominator;
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
