import {
  createPublicKey,
  verify,
  createCipheriv,
  createDecipheriv,
  createHash,
  createHmac,
  randomBytes,
  timingSafeEqual,
} from "node:crypto";

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

export function canonicalEd25519Spki(value: string): Buffer {
  const bytes = decodeCanonicalBase64url(value, 128);
  const key = createPublicKey({ key: bytes, format: "der", type: "spki" });
  const canonical = key.export({ format: "der", type: "spki" });
  if (key.asymmetricKeyType !== "ed25519" || bytes.length !== 44 || !canonical.equals(bytes))
    throw new Error("Invalid public key");
  return bytes;
}
export function decodeCanonicalBase64url(value: string, maxBytes: number): Buffer {
  if (!/^[A-Za-z0-9_-]+$/.test(value) || value.length > Math.ceil((maxBytes * 4) / 3))
    throw new Error("Invalid encoding");
  const bytes = Buffer.from(value, "base64url");
  if (bytes.length > maxBytes || bytes.toString("base64url") !== value)
    throw new Error("Invalid encoding");
  return bytes;
}
export function verifyEd25519Proof(
  spki: Uint8Array,
  fields: readonly string[],
  signature: string,
): boolean {
  try {
    const bytes = decodeCanonicalBase64url(signature, 64);
    return (
      bytes.length === 64 &&
      verify(
        null,
        Buffer.from(JSON.stringify(fields)),
        createPublicKey({ key: Buffer.from(spki), type: "spki", format: "der" }),
        bytes,
      )
    );
  } catch {
    return false;
  }
}
