import type { Pricing } from "../values.ts";
import type { ExactRational, Rational } from "./types.ts";

export const MAX_ATOMS = 10n ** 38n - 1n;
export const UNIT_RATIONAL: ExactRational = { numerator: 1n, denominator: 1n };
export const ZERO_RATIONAL: ExactRational = { numerator: 0n, denominator: 1n };

const nonNegIntRegex = /^(0|[1-9][0-9]{0,37})$/;
const posIntRegex = /^[1-9][0-9]{0,37}$/;
const unboundedPosIntRegex = /^[1-9][0-9]{0,255}$/;
const decimalRegex = /^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$/;

export function isNonNegIntString(value: unknown): value is string {
  return typeof value === "string" && nonNegIntRegex.test(value);
}

export function isPosIntString(value: unknown): value is string {
  return typeof value === "string" && posIntRegex.test(value);
}

export function isDecimalString(value: unknown): value is string {
  return typeof value === "string" && decimalRegex.test(value);
}

export function parseDerivedPositiveRational(r: Rational): ExactRational {
  if (
    typeof r.numerator !== "string" ||
    typeof r.denominator !== "string" ||
    !unboundedPosIntRegex.test(r.numerator) ||
    !unboundedPosIntRegex.test(r.denominator)
  ) {
    throw new TypeError("Invalid derived exact rational");
  }
  return reduceRational(BigInt(r.numerator), BigInt(r.denominator));
}

export function gcd(a: bigint, b: bigint): bigint {
  let x = a < 0n ? -a : a;
  let y = b < 0n ? -b : b;
  while (y !== 0n) {
    const r = x % y;
    x = y;
    y = r;
  }
  return x === 0n ? 1n : x;
}

export function reduceRational(numerator: bigint, denominator: bigint): ExactRational {
  if (denominator <= 0n) throw new RangeError("Rational denominator must be positive");
  if (numerator === 0n) return ZERO_RATIONAL;
  const g = gcd(numerator, denominator);
  return { numerator: numerator / g, denominator: denominator / g };
}

export function parseRational(r: Rational): ExactRational {
  if (!isNonNegIntString(r.numerator) || !isPosIntString(r.denominator)) {
    throw new TypeError("Invalid exact rational");
  }
  return reduceRational(BigInt(r.numerator), BigInt(r.denominator));
}

export function formatRational(r: ExactRational): Rational {
  const reduced = reduceRational(r.numerator, r.denominator);
  return {
    numerator: reduced.numerator.toString(),
    denominator: reduced.denominator.toString(),
  };
}

export function multiplyRational(a: ExactRational, b: ExactRational): ExactRational {
  if (a.numerator === 0n || b.numerator === 0n) return ZERO_RATIONAL;
  return reduceRational(a.numerator * b.numerator, a.denominator * b.denominator);
}

export function addRational(a: ExactRational, b: ExactRational): ExactRational {
  if (a.numerator === 0n) return b;
  if (b.numerator === 0n) return a;
  if (a.denominator === b.denominator) {
    return reduceRational(a.numerator + b.numerator, a.denominator);
  }
  return reduceRational(
    a.numerator * b.denominator + b.numerator * a.denominator,
    a.denominator * b.denominator,
  );
}

export function compareRational(a: ExactRational, b: ExactRational): -1 | 0 | 1 {
  const lhs = a.numerator * b.denominator;
  const rhs = b.numerator * a.denominator;
  if (lhs < rhs) return -1;
  if (lhs > rhs) return 1;
  return 0;
}

export function ceilRational(r: ExactRational): bigint {
  if (r.numerator < 0n) throw new RangeError("Negative rational cost is not supported");
  if (r.numerator === 0n) return 0n;
  const value = (r.numerator + r.denominator - 1n) / r.denominator;
  if (value > MAX_ATOMS) throw new RangeError("Cost exceeds NUMERIC(38,0)");
  return value;
}

export function parseDecimalToRational(raw: string): ExactRational {
  if (!isDecimalString(raw)) throw new TypeError("Invalid decimal quantity");
  const dot = raw.indexOf(".");
  if (dot < 0) return { numerator: BigInt(raw), denominator: 1n };
  const intPart = raw.slice(0, dot);
  const fracPart = raw.slice(dot + 1);
  const denom = 10n ** BigInt(fracPart.length);
  return reduceRational(BigInt(`${intPart}${fracPart}`), denom);
}

export function formatDecimalRational(r: ExactRational): string {
  if (r.denominator === 1n) return r.numerator.toString();
  const intPart = r.numerator / r.denominator;
  const rem = r.numerator % r.denominator;
  if (rem === 0n) return intPart.toString();
  let d = r.denominator;
  let twos = 0;
  let fives = 0;
  while (d % 2n === 0n) {
    d /= 2n;
    twos++;
  }
  while (d % 5n === 0n) {
    d /= 5n;
    fives++;
  }
  if (d === 1n) {
    const scale = Math.max(twos, fives);
    const scaled = (rem * 10n ** BigInt(scale)) / r.denominator;
    const frac = scaled.toString().padStart(scale, "0").replace(/0+$/, "");
    return frac.length > 0 ? `${intPart.toString()}.${frac}` : intPart.toString();
  }
  return `${r.numerator.toString()}/${r.denominator.toString()}`;
}

export function quantizeQuantity(
  raw: ExactRational,
  minUnits: ExactRational | null,
  step: ExactRational | null,
): ExactRational {
  if (raw.numerator === 0n) return ZERO_RATIONAL;
  let effective = raw;
  if (minUnits && compareRational(effective, minUnits) < 0) {
    effective = minUnits;
  }
  if (step && step.numerator > 0n) {
    const ratioNum = effective.numerator * step.denominator;
    const ratioDen = effective.denominator * step.numerator;
    const steps = (ratioNum + ratioDen - 1n) / ratioDen;
    effective = reduceRational(steps * step.numerator, step.denominator);
  }
  return effective;
}

export function scaleRatesExact(
  rates: Pricing["rates"],
  numerator: bigint,
  denominator: bigint,
): Pricing["rates"] | null {
  if (numerator <= 0n || denominator <= 0n) return null;
  const out: Record<string, { numerator: string; denominator: string }> = {};
  for (const [metric, rate] of Object.entries(rates)) {
    if (!rate) continue;
    const rawNum = BigInt(rate.numerator) * numerator;
    const rawDen = BigInt(rate.denominator) * denominator;
    const g = gcd(rawNum, rawDen);
    const nStr = (rawNum / g).toString();
    const dStr = (rawDen / g).toString();
    if (nStr.length > 38 || dStr.length > 38) return null;
    out[metric] = { numerator: nStr, denominator: dStr };
  }
  return out as Pricing["rates"];
}
