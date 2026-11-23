import { dimensions } from "./values.ts";
import { validatePricing, validateUsage } from "./validation.ts";
import type { Dimension, Pricing, Usage } from "./values.ts";

const maxAtoms = 10n ** 38n - 1n;
const maxSafeRate = 500_000_000n;
const pricedDimensions = new Set<string>(dimensions);

export type CompiledRateEntry = {
  dimension: Dimension;
  numerator: bigint;
  denominator: bigint;
  numSafe: number;
  unitDen: boolean;
  isDecimalDim: boolean;
};

export type CompiledPricing = {
  currency: string;
  entries: readonly CompiledRateEntry[];
  allUnitSafe: boolean;
};

function gcd(a: bigint, b: bigint): bigint {
  while (b !== 0n) {
    const remainder = a % b;
    a = b;
    b = remainder;
  }
  return a;
}

function isNonNegativeIntString(value: string): boolean {
  const len = value.length;
  if (len === 0 || len > 38) return false;
  const c0 = value.charCodeAt(0);
  if (c0 === 48) return len === 1;
  if (c0 < 49 || c0 > 57) return false;
  for (let i = 1; i < len; i++) {
    const c = value.charCodeAt(i);
    if (c < 48 || c > 57) return false;
  }
  return true;
}

function isDecimalDurationString(value: string): boolean {
  const dot = value.indexOf(".");
  if (dot < 0) return isNonNegativeIntString(value);
  const intPart = value.slice(0, dot);
  const fracPart = value.slice(dot + 1);
  if (!isNonNegativeIntString(intPart)) return false;
  const fLen = fracPart.length;
  if (fLen === 0 || fLen > 18) return false;
  for (let i = 0; i < fLen; i++) {
    const c = fracPart.charCodeAt(i);
    if (c < 48 || c > 57) return false;
  }
  return true;
}

function validateUsageFast(usage: Usage): void {
  const obj = usage as Record<string, unknown>;
  for (const key in obj) {
    if (!Object.hasOwn(obj, key)) continue;
    const value = obj[key];
    if (key !== "reasoning_tokens" && !pricedDimensions.has(key)) {
      throw new TypeError("Unknown usage dimension");
    }
    const decimal = key === "audio_seconds" || key === "video_seconds";
    if (
      typeof value !== "string" ||
      !(decimal ? isDecimalDurationString(value) : isNonNegativeIntString(value))
    ) {
      throw new TypeError("Usage counters must be exact nonnegative integers or decimal durations");
    }
  }
  if (
    usage.cache_write_tokens !== undefined &&
    usage.cache_write_5m_tokens !== undefined &&
    usage.cache_write_1h_tokens !== undefined &&
    BigInt(usage.cache_write_tokens) !==
      BigInt(usage.cache_write_5m_tokens) + BigInt(usage.cache_write_1h_tokens)
  ) {
    throw new TypeError("Provider cache write partitions disagree");
  }
}

export function compilePricing(pricing: Pricing): CompiledPricing {
  validatePricing(pricing);
  const entries: CompiledRateEntry[] = [];
  let allUnitSafe = true;
  for (const dimension of dimensions) {
    const rate = pricing.rates[dimension];
    if (!rate || rate.numerator === "0") continue;
    const numerator = BigInt(rate.numerator);
    const denominator = BigInt(rate.denominator);
    const unitDen = denominator === 1n;
    const isDecimalDim = dimension === "audio_seconds" || dimension === "video_seconds";
    if (!unitDen || isDecimalDim || numerator > maxSafeRate) allUnitSafe = false;
    entries.push({
      dimension,
      numerator,
      denominator,
      numSafe: unitDen && numerator <= maxSafeRate ? Number(numerator) : 0,
      unitDen,
      isDecimalDim,
    });
  }
  return { currency: pricing.currency, entries, allUnitSafe };
}

export function calculateCompiledCostAtoms(usage: Usage, compiled: CompiledPricing): bigint {
  validateUsageFast(usage);
  const entries = compiled.entries;
  const len = entries.length;
  if (compiled.allUnitSafe) {
    let total = 0;
    let overflowed = false;
    for (let i = 0; i < len; i++) {
      const entry = entries[i]!;
      const quantityText = usage[entry.dimension];
      if (quantityText === undefined || quantityText === "0") continue;
      if (quantityText.length > 6) {
        overflowed = true;
        break;
      }
      total += Number(quantityText) * entry.numSafe;
    }
    if (!overflowed) return BigInt(total);
  }
  let numerator = 0n;
  let denominator = 1n;
  for (let i = 0; i < len; i++) {
    const entry = entries[i]!;
    const quantityText = usage[entry.dimension];
    if (quantityText === undefined || quantityText === "0") continue;
    const decimal = entry.isDecimalDim ? quantityText.indexOf(".") : -1;
    const quantity = BigInt(decimal < 0 ? quantityText : quantityText.replace(".", ""));
    const product = quantity * entry.numerator;
    if (decimal < 0 && entry.unitDen) {
      numerator += product * denominator;
      continue;
    }
    const divisor =
      decimal < 0
        ? entry.denominator
        : entry.denominator * 10n ** BigInt(quantityText.length - decimal - 1);
    numerator = numerator * divisor + product * denominator;
    denominator *= divisor;
    const common = gcd(numerator, denominator);
    numerator /= common;
    denominator /= common;
  }
  const result = (numerator + denominator - 1n) / denominator;
  if (result > maxAtoms) throw new RangeError("Cost exceeds NUMERIC(38,0)");
  return result;
}

export function calculateCompiledCost(usage: Usage, compiled: CompiledPricing): string {
  return calculateCompiledCostAtoms(usage, compiled).toString();
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
  if (typeof value === "string" && isNonNegativeIntString(value)) return value;
  return undefined;
}
