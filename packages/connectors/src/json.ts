import { GatewayError } from "@gateway/contracts";
import { normalizedNumber, canonical, DomainError } from "@gateway/kernel";
const numericLiterals = new WeakMap<object, Map<string, string>>();
export function normalizedProviderNumber(source: string): ReturnType<typeof normalizedNumber> {
  try {
    return normalizedNumber(source);
  } catch (error) {
    if (error instanceof DomainError) throw new GatewayError(502, error.code, error.message);
    throw error;
  }
}
export function numberLiteral(parent: object, key: string): string | undefined {
  return numericLiterals.get(parent)?.get(key);
}
export function canonicalExact(value: unknown): string {
  try {
    return canonical(value, numberLiteral);
  } catch (error) {
    if (error instanceof DomainError) throw new GatewayError(400, error.code, error.message);
    throw error;
  }
}
const rawJson = (JSON as JSON & { rawJSON: (source: string) => unknown }).rawJSON;
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
    if (error instanceof DomainError) throw new GatewayError(502, error.code, error.message);
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
  const value = normalizedProviderNumber(literal);
  if (value.negative || value.exponent < 0 || value.digits.length + value.exponent > 38)
    throw new GatewayError(
      502,
      "invalid_upstream_usage",
      "Usage must be a nonnegative 38-digit integer",
    );
  return value.digits + "0".repeat(value.exponent);
}

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
