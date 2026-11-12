import { stringify, LosslessNumber, isLosslessNumber } from "lossless-json";
import type { AdapterFailureCode } from "./abi.ts";
declare const Host: {
  base64ToArrayBuffer(value: string): ArrayBuffer;
  arrayBufferToBase64(value: ArrayBuffer): string;
};
export type GuestInput = {
  abi_version: 1;
  hook: string;
  session: {
    id: string;
    sequence: number;
    tenant: string;
    attempt: string;
    incarnation: string;
    plugin: string;
    config: string;
  };
  config: Record<string, unknown>;
  payload: string;
  output_budget: number;
};
export type GuestHost = { inputString(): string; outputString(value: string): void };
export class AdapterFailure extends Error {
  constructor(
    readonly status: 400 | 413 | 422 | 502,
    readonly code: AdapterFailureCode,
  ) {
    super(code);
  }
}
export function exactParse(
  source: string,
  status: 400 | 502 = 502,
  numericMode: "lossless" | "number" = "lossless",
): unknown {
  let cursor = 0,
    fields = 0;
  const number = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
  function space() {
    while (cursor < source.length && /[\x20\t\r\n]/.test(source[cursor]!)) cursor++;
  }
  function string(): string {
    const start = cursor++;
    for (;;) {
      const end = source.indexOf('"', cursor);
      if (end < 0) throw 0;
      let slashes = 0;
      for (let i = end - 1; i >= start && source[i] === "\\"; i--) slashes++;
      cursor = end + 1;
      if (slashes % 2 === 0) return JSON.parse(source.slice(start, cursor)) as string;
    }
  }
  function value(depth: number): unknown {
    if (depth > 64 || ++fields > 100000) throw new AdapterFailure(status, "json_limit");
    space();
    if (source[cursor] === '"') return string();
    if (source[cursor] === "{" || source[cursor] === "[") {
      const array = source[cursor++] === "[",
        end = array ? "]" : "}";
      const result: Record<string, unknown> | unknown[] = array
        ? []
        : (Object.create(null) as Record<string, unknown>);
      const seen = new Set<string>();
      space();
      if (source[cursor] === end) {
        cursor++;
        return result;
      }
      for (;;) {
        space();
        if (array) (result as unknown[]).push(value(depth + 1));
        else {
          if (source[cursor] !== '"') throw 0;
          const key = string();
          if (seen.has(key)) throw 0;
          seen.add(key);
          space();
          if (source[cursor++] !== ":") throw 0;
          (result as Record<string, unknown>)[key] = value(depth + 1);
        }
        space();
        const next = source[cursor++];
        if (next === end) return result;
        if (next !== ",") throw 0;
      }
    }
    for (const [keyword, literal] of [
      ["true", true],
      ["false", false],
      ["null", null],
    ] as const)
      if (source.startsWith(keyword, cursor)) {
        cursor += keyword.length;
        return literal;
      }
    number.lastIndex = cursor;
    const match = number.exec(source);
    if (!match) throw 0;
    cursor = number.lastIndex;
    if (match[0].length > 200) throw new AdapterFailure(status, "numeric_limit");
    return numericMode === "number" ? Number(match[0]) : new LosslessNumber(match[0]);
  }
  try {
    const result = value(0);
    space();
    if (cursor !== source.length) throw 0;
    return result;
  } catch (error) {
    if (error instanceof AdapterFailure) throw error;
    throw new AdapterFailure(status, "invalid_json");
  }
}
export function exactStringify(value: unknown): string {
  const result = stringify(value);
  if (result === undefined) throw new AdapterFailure(502, "invalid_json");
  return result;
}
export function object(value: unknown, status: 400 | 422 | 502 = 502): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value) || isLosslessNumber(value))
    throw new AdapterFailure(status, "invalid_object");
  return value as Record<string, unknown>;
}
export function integer(value: unknown, status: 400 | 422 | 502 = 502): string {
  const source = isLosslessNumber(value)
    ? value.value
    : typeof value === "string"
      ? value
      : typeof value === "number" && Number.isSafeInteger(value)
        ? String(value)
        : "";
  if (source.length > 200) throw new AdapterFailure(status, "numeric_limit");
  const match = /^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d{1,5}))?$/.exec(source);
  if (!match) throw new AdapterFailure(status, "invalid_upstream_usage");
  let digits = `${match[2]}${match[3] ?? ""}`.replace(/^0+/, ""),
    exponent = Number(match[4] ?? "0") - (match[3]?.length ?? 0);
  if (!digits) return "0";
  const trimmed = digits.replace(/0+$/, "");
  exponent += digits.length - trimmed.length;
  digits = trimmed;
  if (match[1] || exponent < 0 || digits.length + exponent > 38)
    throw new AdapterFailure(status, "invalid_upstream_usage");
  return digits + "0".repeat(exponent);
}
export function smallInteger(value: unknown, max = 1000000, status: 400 | 422 | 502 = 502): number {
  const source = integer(value, status);
  if (source.length > 15 || Number(source) > max) throw new AdapterFailure(status, "integer_limit");
  return Number(source);
}
export function decode64(source: string): Uint8Array {
  if (source.length % 4) throw new AdapterFailure(400, "invalid_base64");
  try {
    return new Uint8Array(Host.base64ToArrayBuffer(source));
  } catch {
    throw new AdapterFailure(400, "invalid_base64");
  }
}
export function encode64(bytes: Uint8Array): string {
  return Host.arrayBufferToBase64(
    bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer,
  );
}
export function text64(value: string): string {
  return encode64(new TextEncoder().encode(value));
}
export function runGuest(
  host: GuestHost,
  handler: (input: GuestInput, state: unknown) => { result: unknown; state: unknown },
) {
  const envelope = object(exactParse(host.inputString()));
  const input = JSON.parse(
    new TextDecoder("utf-8", { fatal: true }).decode(decode64(String(envelope.input))),
  ) as GuestInput;
  let output;
  try {
    if (
      input.abi_version !== 1 ||
      !input.session ||
      !Number.isSafeInteger(input.session.sequence) ||
      input.session.sequence < 0 ||
      !Number.isSafeInteger(input.output_budget) ||
      input.output_budget < 1
    )
      throw new AdapterFailure(400, "abi_invalid");
    output = handler(input, envelope.state);
  } catch (error) {
    if (!(error instanceof AdapterFailure)) throw error;
    output = {
      result: { abi_version: 1, kind: "failure", status: error.status, code: error.code },
      state: null,
    };
  }
  host.outputString(
    exactStringify({ output: text64(JSON.stringify(output.result)), state: output.state }),
  );
}
export { LosslessNumber };
export type { JSONDocumentDescriptor, JSONNodeFact, JSONDocumentPayload } from "./abi.ts";
export type { AdapterFailureCode } from "./abi.ts";
