import { GatewayError } from "@gateway/contracts";
import { normalizedNumber, DomainError } from "@gateway/kernel";

const numericLiterals = new WeakMap<object, Map<string, { literal: string; value: number }>>();
const hasTrackedExact = new WeakSet<object>();

export function normalizedProviderNumber(source: string): ReturnType<typeof normalizedNumber> {
  try {
    return normalizedNumber(source);
  } catch (error) {
    if (error instanceof DomainError) throw new GatewayError(502, error.code, error.message);
    throw error;
  }
}

export function hasExactLiterals(parent: object): boolean {
  return numericLiterals.has(parent);
}

export function numberLiteral(parent: object, key: string): string | undefined {
  const entry = numericLiterals.get(parent)?.get(key);
  return entry && Object.is(entry.value, Reflect.get(parent, key)) ? entry.literal : undefined;
}

export function clearExactProperty(parent: object, key: string): void {
  numericLiterals.get(parent)?.delete(key);
}

export function setExactProperty(parent: object, key: string, literal: string): void {
  const value = Reflect.get(parent, key);
  if (typeof value !== "number") throw new TypeError("Exact numeric property must be a number");
  let fields = numericLiterals.get(parent);
  if (!fields) numericLiterals.set(parent, (fields = new Map()));
  fields.set(key, { literal, value });
  hasTrackedExact.add(parent);
}

export function trackExactChild<T extends object>(parent: T, child: object): T {
  if (hasTrackedExact.has(child)) hasTrackedExact.add(parent);
  return parent;
}

const rawJson = (JSON as JSON & { rawJSON: (source: string) => unknown }).rawJSON;

function parseExactJson(source: string, preserveMeteringIntegers: boolean): unknown {
  let depth = 0;
  let string = false;
  let escaped = false;
  let fields = 0;
  let needsReviver = false;
  let numLen = 0;
  let inNumber = false;
  for (let i = 0; i < source.length; i++) {
    const code = source.charCodeAt(i);
    if (string) {
      if (escaped) escaped = false;
      else if (code === 92) escaped = true;
      else if (code === 34) string = false;
      continue;
    }
    if (code === 34) {
      string = true;
      inNumber = false;
      numLen = 0;
    } else if (code === 123 || code === 91) {
      inNumber = false;
      numLen = 0;
      if (++depth > 64) {
        throw new GatewayError(502, "json_limit", "JSON nesting exceeds protocol budget");
      }
    } else if (code === 125 || code === 93) {
      inNumber = false;
      numLen = 0;
      depth--;
    } else if (code === 58 || code === 44) {
      inNumber = false;
      numLen = 0;
      if (++fields > 100000) {
        throw new GatewayError(502, "json_limit", "JSON field count exceeds protocol budget");
      }
    } else if (!needsReviver) {
      if ((code >= 48 && code <= 57) || code === 45) {
        if (!inNumber) {
          inNumber = true;
          numLen = 1;
        } else if (++numLen >= 15) {
          needsReviver = true;
        }
      } else if (inNumber && (code === 46 || code === 101 || code === 69 || code === 43)) {
        needsReviver = true;
      } else {
        inNumber = false;
        numLen = 0;
      }
    }
  }

  if (!needsReviver) {
    try {
      return JSON.parse(source);
    } catch {
      throw new GatewayError(502, "invalid_upstream_json", "Provider returned malformed JSON");
    }
  }

  let tracked = 0;
  const parse = JSON.parse as (
    source: string,
    reviver: (this: object, key: string, value: unknown, context?: { source?: string }) => unknown,
  ) => unknown;
  try {
    const parsed = parse(source, function (key, value, context) {
      if (value !== null && typeof value === "object" && hasTrackedExact.has(value)) {
        hasTrackedExact.add(this);
        return value;
      }
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
        ) {
          return value;
        }
      }
      if (++tracked > 10000) {
        throw new GatewayError(502, "numeric_limit", "Too many extended precision numeric fields");
      }
      let map = numericLiterals.get(this);
      if (!map) {
        map = new Map();
        numericLiterals.set(this, map);
      }
      map.set(key, { literal: context.source, value });
      hasTrackedExact.add(this);
      return value;
    });
    if (tracked > 0 && parsed !== null && typeof parsed === "object") {
      hasTrackedExact.add(parsed);
    }
    return parsed;
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
  const needsExact = value !== null && typeof value === "object" && hasTrackedExact.has(value);
  const serialized = needsExact
    ? JSON.stringify(value, function (this: object, key, current: unknown) {
        const literal = typeof current === "number" ? numberLiteral(this, key) : undefined;
        return literal === undefined ? current : rawJson(literal);
      })
    : JSON.stringify(value);
  if (serialized === undefined) throw new TypeError("Expected a JSON value");
  return serialized;
}

export function exactInteger(parent: Record<string, unknown>, key: string): string | undefined {
  const literal = numberLiteral(parent, key);
  if (!literal) return undefined;
  const value = normalizedProviderNumber(literal);
  if (value.negative || value.exponent < 0 || value.digits.length + value.exponent > 38) {
    throw new GatewayError(
      502,
      "invalid_upstream_usage",
      "Usage must be a nonnegative 38-digit integer",
    );
  }
  return value.digits + "0".repeat(value.exponent);
}

export function copyExact<T extends object>(source: object, target: T): T {
  if (hasTrackedExact.has(source)) hasTrackedExact.add(target);
  const fields = numericLiterals.get(source);
  if (fields) {
    const copied = new Map<string, { literal: string; value: number }>();
    for (const [key, entry] of fields) {
      if (
        Object.is(entry.value, Reflect.get(source, key)) &&
        Object.is(entry.value, Reflect.get(target, key))
      ) {
        copied.set(key, entry);
      }
    }
    if (copied.size) {
      numericLiterals.set(target, copied);
      hasTrackedExact.add(target);
    }
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
