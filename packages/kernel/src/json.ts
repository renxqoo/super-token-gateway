import { GatewayError } from "./errors.ts";

const numericLiterals = new WeakMap<object, Map<string, string>>();
const rawJson = (JSON as JSON & { rawJSON: (source: string) => unknown }).rawJSON;

export function normalizedNumber(source: string): {
  digits: string;
  exponent: number;
  negative: boolean;
} {
  if (source.length > 200)
    throw new GatewayError(502, "numeric_limit", "Numeric literal exceeds protocol budget");
  const match = /^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d{1,5}))?$/.exec(source);
  if (!match)
    throw new GatewayError(502, "invalid_numeric_literal", "Invalid protocol numeric literal");
  let digits = `${match[2]}${match[3] ?? ""}`.replace(/^0+/, "");
  let exponent = Number(match[4] ?? "0") - (match[3]?.length ?? 0);
  if (!digits) return { digits: "0", exponent: 0, negative: false };
  const trimmed = digits.replace(/0+$/, "");
  exponent += digits.length - trimmed.length;
  digits = trimmed;
  return { digits, exponent, negative: match[1] === "-" };
}

export function numberLiteral(parent: object, key: string): string | undefined {
  return numericLiterals.get(parent)?.get(key);
}

function parseExactJson(source: string, preserveMeteringIntegers: boolean): unknown {
  let depth = 0;
  let string = false;
  let escaped = false;
  let fields = 0;
  for (let i = 0; i < source.length; i++) {
    const ch = source[i];
    if (string) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') string = false;
      continue;
    }
    if (ch === '"') string = true;
    else if (ch === "{" || ch === "[") {
      if (++depth > 64)
        throw new GatewayError(502, "json_limit", "JSON nesting exceeds protocol budget");
    } else if (ch === "}" || ch === "]") depth--;
    else if (ch === ":" || ch === ",") {
      if (++fields > 100000)
        throw new GatewayError(502, "json_limit", "JSON field count exceeds protocol budget");
    }
  }
  let tracked = 0;
  const parse = JSON.parse as (
    source: string,
    reviver: (this: object, key: string, value: unknown, context?: { source?: string }) => unknown,
  ) => unknown;
  try {
    return parse(source, function (key, value, context) {
      if (typeof value !== "number" || !context?.source) return value;
      if (!preserveMeteringIntegers || !Number.isInteger(value) || Number.isSafeInteger(value)) {
        const serialized = String(value);
        if (serialized === context.source) return value;
        const exact = normalizedNumber(context.source);
        const rounded = Number.isFinite(value) ? normalizedNumber(serialized) : undefined;
        if (
          rounded &&
          exact.digits === rounded.digits &&
          exact.exponent === rounded.exponent &&
          exact.negative === rounded.negative
        )
          return value;
      }
      if (++tracked > 10000)
        throw new GatewayError(502, "numeric_limit", "Too many extended precision numeric fields");
      let fields = numericLiterals.get(this);
      if (!fields) {
        fields = new Map();
        numericLiterals.set(this, fields);
      }
      fields.set(key, context.source);
      return value;
    });
  } catch (error) {
    if (error instanceof GatewayError) throw error;
    throw new GatewayError(502, "invalid_upstream_json", "Provider returned malformed JSON");
  }
}

export function parseProviderJson(source: string): unknown {
  return parseExactJson(source, true);
}

export function stringifyExact(value: unknown): string {
  const serialized = JSON.stringify(value, function (this: object, key, current: unknown) {
    const literal = typeof current === "number" ? numberLiteral(this, key) : undefined;
    return literal === undefined ? current : rawJson(literal);
  });
  if (serialized === undefined) throw new TypeError("Expected a JSON value");
  return serialized;
}

export function exactInteger(parent: Record<string, unknown>, key: string): string | undefined {
  const literal = numberLiteral(parent, key);
  if (!literal) return undefined;
  const value = normalizedNumber(literal);
  if (value.negative || value.exponent < 0 || value.digits.length + value.exponent > 38)
    throw new GatewayError(
      502,
      "invalid_upstream_usage",
      "Usage must be a nonnegative 38-digit integer",
    );
  return value.digits + "0".repeat(value.exponent);
}

// Object spread loses the sidecar used for JSON numbers beyond IEEE-754 precision.
export function copyExact<T extends object>(source: object, target: T): T {
  const fields = numericLiterals.get(source);
  if (fields) {
    const copied = new Map<string, string>();
    for (const [key, literal] of fields)
      if (Object.is(Reflect.get(source, key), Reflect.get(target, key))) copied.set(key, literal);
    if (copied.size) numericLiterals.set(target, copied);
  }
  return target;
}

export function parseClientJson(source: string): unknown {
  try {
    return parseExactJson(source, false);
  } catch {
    throw new GatewayError(
      400,
      "invalid_request",
      "Request JSON is malformed or exceeds protocol limits",
    );
  }
}
