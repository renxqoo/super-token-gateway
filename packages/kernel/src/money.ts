export const AMOUNT_DECIMALS = 18;
export const ATOMS_PER_UNIT = 10n ** 18n;

const MAX_ATOMS = 10n ** 38n - 1n;

export type ParseDecimalOptions = {
  allowNegative?: boolean;
  allowZero?: boolean;
  decimals?: number;
};

export type FormatAtomsOptions = {
  decimals?: number;
  fixedDecimals?: number;
  minDecimals?: number;
  trimTrailingZeros?: boolean;
};

export function parseDecimalToAtoms(raw: string, options: ParseDecimalOptions = {}): string {
  if (typeof raw !== "string" || raw.length === 0) {
    throw new TypeError("Amount must be a non-empty decimal string");
  }
  const decimals = options.decimals ?? AMOUNT_DECIMALS;
  if (!Number.isInteger(decimals) || decimals < 0 || decimals > 18) {
    throw new RangeError("decimals must be an integer between 0 and 18");
  }
  const match = /^(-?)(0|[1-9][0-9]*)(?:\.([0-9]+))?$/.exec(raw);
  if (!match) {
    throw new TypeError("Invalid decimal amount string");
  }
  const neg = match[1] === "-";
  const intPart = match[2]!;
  const fracPart = match[3] ?? "";
  if (fracPart.length > decimals) {
    throw new RangeError(`Decimal amount exceeds ${decimals} fractional digits`);
  }
  const paddedFrac = fracPart.padEnd(decimals, "0");
  const combined = BigInt(`${intPart}${paddedFrac}`);
  if (combined > MAX_ATOMS) {
    throw new RangeError("Amount exceeds NUMERIC(38,0)");
  }
  if (combined === 0n) {
    if (neg) throw new TypeError("Negative zero is not allowed");
    if (!options.allowZero) throw new RangeError("Amount must be non-zero");
    return "0";
  }
  if (neg && !options.allowNegative) {
    throw new RangeError("Negative amount is not allowed");
  }
  return neg ? `-${combined.toString()}` : combined.toString();
}

export function formatAtomsToDecimal(
  raw: string | bigint,
  options: FormatAtomsOptions = {},
): string {
  const decimals = options.decimals ?? AMOUNT_DECIMALS;
  if (!Number.isInteger(decimals) || decimals < 0 || decimals > 18) {
    throw new RangeError("decimals must be an integer between 0 and 18");
  }
  let value: bigint;
  if (typeof raw === "bigint") {
    value = raw;
  } else {
    if (!/^-?(0|[1-9][0-9]{0,37})$/.test(raw)) {
      throw new TypeError("Invalid integer atoms string");
    }
    value = BigInt(raw);
  }
  const neg = value < 0n;
  const abs = neg ? -value : value;
  const scale = 10n ** BigInt(decimals);
  const intPart = (abs / scale).toString();
  if (decimals === 0) {
    return `${neg ? "-" : ""}${intPart}`;
  }
  let frac = (abs % scale).toString().padStart(decimals, "0");
  if (options.fixedDecimals !== undefined) {
    const fixed = Math.min(decimals, Math.max(0, Math.trunc(options.fixedDecimals)));
    frac = frac.slice(0, fixed);
    return fixed === 0 ? `${neg ? "-" : ""}${intPart}` : `${neg ? "-" : ""}${intPart}.${frac}`;
  }
  if (options.trimTrailingZeros) {
    const minDec = Math.min(decimals, Math.max(0, Math.trunc(options.minDecimals ?? 0)));
    let end = frac.length;
    while (end > minDec && frac.charCodeAt(end - 1) === 48) end--;
    frac = frac.slice(0, end);
    return frac.length === 0
      ? `${neg ? "-" : ""}${intPart}`
      : `${neg ? "-" : ""}${intPart}.${frac}`;
  }
  return `${neg ? "-" : ""}${intPart}.${frac}`;
}
