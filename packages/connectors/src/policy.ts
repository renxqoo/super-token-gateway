import { GatewayError, normalizedNumber, numberLiteral } from "@gateway/kernel";
import type { RequestPolicy } from "@gateway/kernel";

function* valuesAt(
  value: unknown,
  path: string[],
  offset = 0,
): Generator<{ value: unknown; literal?: string }> {
  if (value === null || typeof value !== "object") return;
  const keys = path[offset] === "*" ? Object.keys(value) : [path[offset]!];
  for (const key of keys) {
    if (!Object.hasOwn(value, key)) continue;
    const child = Reflect.get(value, key) as unknown;
    if (offset + 1 < path.length) yield* valuesAt(child, path, offset + 1);
    else
      yield {
        value: child,
        ...(typeof child === "number"
          ? { literal: numberLiteral(value, key) ?? String(child) }
          : {}),
      };
  }
}

function compareNumber(literal: string, bound: number): number {
  const left = normalizedNumber(literal);
  const right = normalizedNumber(String(bound));
  if (left.negative !== right.negative) return left.negative ? -1 : 1;
  let order: number;
  if (left.digits === "0" || right.digits === "0")
    order = left.digits === right.digits ? 0 : left.digits === "0" ? -1 : 1;
  else {
    order = Math.sign(left.digits.length + left.exponent - right.digits.length - right.exponent);
    if (!order) {
      const length = Math.max(left.digits.length, right.digits.length);
      const a = left.digits.padEnd(length, "0");
      const b = right.digits.padEnd(length, "0");
      order = a === b ? 0 : a < b ? -1 : 1;
    }
  }
  return left.negative ? -order : order;
}

export function validateRequestPolicy(
  body: Record<string, unknown>,
  policy: RequestPolicy = {},
): void {
  const fail = (field: string): never => {
    throw new GatewayError(
      422,
      "unsupported_capability",
      `Route model policy cannot accept ${field}`,
    );
  };
  if (policy.allowed_parameters)
    for (const key of Object.keys(body)) if (!policy.allowed_parameters.includes(key)) fail(key);
  for (const path of policy.blocked_parameters ?? [])
    for (const _value of valuesAt(body, path.split("."))) fail(path);
  for (const [path, range] of Object.entries(policy.parameter_ranges ?? {}))
    for (const { value, literal } of valuesAt(body, path.split(".")))
      if (
        typeof value !== "number" ||
        !Number.isFinite(value) ||
        (range.minimum !== undefined && compareNumber(literal!, range.minimum) < 0) ||
        (range.maximum !== undefined && compareNumber(literal!, range.maximum) > 0)
      )
        fail(path);
}
