import { DomainError } from "./errors.ts";

export function normalizedNumber(source: string): {
  digits: string;
  exponent: number;
  negative: boolean;
} {
  if (source.length > 200)
    throw new DomainError("numeric_limit", "Numeric literal exceeds protocol budget");
  const match = /^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d{1,5}))?$/.exec(source);
  if (!match) throw new DomainError("invalid_numeric_literal", "Invalid protocol numeric literal");
  let digits = `${match[2]}${match[3] ?? ""}`.replace(/^0+/, "");
  let exponent = Number(match[4] ?? "0") - (match[3]?.length ?? 0);
  if (!digits) return { digits: "0", exponent: 0, negative: false };
  const trimmed = digits.replace(/0+$/, "");
  exponent += digits.length - trimmed.length;
  digits = trimmed;
  return { digits, exponent, negative: match[1] === "-" };
}
