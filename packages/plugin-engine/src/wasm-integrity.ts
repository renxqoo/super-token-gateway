import { createPublicKey, verify, type KeyObject } from "node:crypto";

const publicKeyCache = new Map<string, KeyObject>();

function cachedPublicKey(pem: string): KeyObject {
  const hit = publicKeyCache.get(pem);
  if (hit) return hit;
  if (publicKeyCache.size >= 32) publicKeyCache.clear();
  const key = createPublicKey(pem);
  publicKeyCache.set(pem, key);
  return key;
}

export function verifyPluginSignature(
  digestHex: string,
  signatureBase64: string | null,
  publicKeyPem: string | undefined,
): boolean {
  if (!publicKeyPem) return true;
  if (!signatureBase64) return false;
  try {
    const pub = cachedPublicKey(publicKeyPem);
    return verify(
      null,
      Buffer.from(digestHex, "utf8"),
      pub,
      Buffer.from(signatureBase64, "base64"),
    );
  } catch {
    return false;
  }
}

export function verifyPluginIntegrity(
  plugin: {
    digest: string;
    wasm_bytes: Uint8Array | null;
    signature: string | null;
  },
  publicKeyPem: string | undefined,
): { ok: true } | { ok: false; reason: "missing_bytes" | "digest_mismatch" | "signature_invalid" } {
  if (!plugin.wasm_bytes) return { ok: false, reason: "missing_bytes" };
  const actualDigest = new Bun.CryptoHasher("sha256").update(plugin.wasm_bytes).digest("hex");
  if (actualDigest !== plugin.digest) return { ok: false, reason: "digest_mismatch" };
  if (!verifyPluginSignature(plugin.digest, plugin.signature, publicKeyPem)) {
    return { ok: false, reason: "signature_invalid" };
  }
  return { ok: true };
}
