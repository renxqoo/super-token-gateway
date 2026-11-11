import {
  createCipheriv,
  createDecipheriv,
  createHash,
  createHmac,
  randomBytes,
  timingSafeEqual,
} from "node:crypto";
import { GatewayError } from "./errors.ts";
import { normalizedNumber, numberLiteral } from "./json.ts";

export const sha256 = (value: string | Uint8Array): string =>
  createHash("sha256").update(value).digest("hex");

export function batchIdentityDigest(identities: Iterable<string>): string {
  return sha256(JSON.stringify([...identities].sort()));
}

export function verifier(value: string, pepper: string): string {
  return createHmac("sha256", pepper).update(value).digest("hex");
}

export function secureEqual(a: string, b: string): boolean {
  const left = Buffer.from(a);
  const right = Buffer.from(b);
  return left.byteLength === right.byteLength && timingSafeEqual(left, right);
}

export function createKey(): string {
  return `gw_${randomBytes(32).toString("base64url")}`;
}

export function encrypt(secret: string, keyHex: string, context: string): string {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", Buffer.from(keyHex, "hex"), iv);
  cipher.setAAD(Buffer.from(context));
  const encrypted = Buffer.concat([cipher.update(secret, "utf8"), cipher.final()]);
  return Buffer.concat([iv, cipher.getAuthTag(), encrypted]).toString("base64");
}

export function decrypt(value: string, keyHex: string, context: string): string {
  const data = Buffer.from(value, "base64");
  const cipher = createDecipheriv("aes-256-gcm", Buffer.from(keyHex, "hex"), data.subarray(0, 12));
  cipher.setAAD(Buffer.from(context));
  cipher.setAuthTag(data.subarray(12, 28));
  return Buffer.concat([cipher.update(data.subarray(28)), cipher.final()]).toString("utf8");
}

export function canonical(value: unknown, depth = 0): string {
  if (depth > 64) throw new GatewayError(400, "invalid_request", "JSON nesting exceeds limit");
  if (Array.isArray(value))
    return `[${value
      .map((v: unknown, index: number) => {
        const literal = typeof v === "number" ? numberLiteral(value, String(index)) : undefined;
        if (literal) {
          const number = normalizedNumber(literal);
          return `${number.negative ? "-" : ""}${number.digits}e${number.exponent}`;
        }
        return canonical(v, depth + 1);
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
        return `${JSON.stringify(k)}:${canonical(obj[k], depth + 1)}`;
      })
      .join(",")}}`;
  }
  const result = JSON.stringify(value);
  if (result === undefined) throw new Error("Invalid JSON value");
  return result;
}
