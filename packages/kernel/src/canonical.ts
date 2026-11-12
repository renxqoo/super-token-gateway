import { DomainError } from "./errors.ts";
import { normalizedNumber } from "./numbers.ts";
export function canonical(
  value: unknown,
  numberLiteral: (parent: object, key: string) => string | undefined = () => undefined,
  depth = 0,
): string {
  if (depth > 64) throw new DomainError("invalid_request", "JSON nesting exceeds limit");
  if (Array.isArray(value))
    return `[${value
      .map((v: unknown, index: number) => {
        const literal = typeof v === "number" ? numberLiteral(value, String(index)) : undefined;
        if (literal) {
          const number = normalizedNumber(literal);
          return `${number.negative ? "-" : ""}${number.digits}e${number.exponent}`;
        }
        return canonical(v, numberLiteral, depth + 1);
      })
      .join(",")}]`;
  if (typeof value === "object" && value !== null) {
    const obj = value as Record<string, unknown>;
    return `{${Object.keys(obj)
      .sort()
      .map((k) => {
        const literal = typeof obj[k] === "number" ? numberLiteral(obj, k) : undefined;
        if (literal) {
          const number = normalizedNumber(literal);
          return `${JSON.stringify(k)}:${number.negative ? "-" : ""}${number.digits}e${number.exponent}`;
        }
        return `${JSON.stringify(k)}:${canonical(obj[k], numberLiteral, depth + 1)}`;
      })
      .join(",")}}`;
  }
  const result = JSON.stringify(value);
  if (result === undefined) throw new Error("Invalid JSON value");
  return result;
}
