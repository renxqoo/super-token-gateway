import { createPublicKey, verify } from "node:crypto";
import { z } from "zod";
import { canonical } from "@gateway/kernel";
import { GatewayError } from "@gateway/contracts";
import { bundledPackageFiles } from "@gateway/adapters";
import {
  bundledReleaseIds,
  manifestSchema,
  packageDigest,
  publisherAdmits,
  releasePolicySchema,
  verifyPackage,
  sha256,
  type PackageFile,
} from "@gateway/plugin-sdk";
export const validationPolicy = Object.freeze({
  id: "gateway-fixtures-v1",
  abi_version: 1,
  wall_ms: 60000,
  lease_ms: 90000,
  result_bytes: 4096,
  native: Object.freeze({ slots: 1, callMs: 1000, compileMs: 5000, memoryPages: 1024 }),
});
export const validationPolicyDigest = sha256(canonical(validationPolicy));
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const keyId = z.string().regex(/^[A-Za-z0-9_.-]{1,100}$/);
export const releaseDescriptorSchema = z
  .object({
    format: z.literal(1),
    release_id: z.string().regex(/^[a-z][a-z0-9.-]{1,100}-\d+\.\d+\.\d+$/),
    source: z.string().regex(/^(bundled|upload):[a-z][a-z0-9.-]{1,100}-\d+\.\d+\.\d+$/),
    name: manifestSchema.shape.id,
    version: manifestSchema.shape.version,
    package_digest: digest,
    envelope_digest: digest,
    abi_version: z.literal(1),
    config_schema_digest: digest,
    fixture_digest: digest,
    validation_policy_digest: digest,
    runtime_digest: digest,
    files: z
      .array(
        z
          .object({
            path: z.string().min(1).max(200),
            bytes: z
              .number()
              .int()
              .min(0)
              .max(32 << 20),
            sha256: digest,
          })
          .strict(),
      )
      .min(6)
      .max(128),
  })
  .strict();
export const signedReleaseSchema = z
  .object({
    descriptor: releaseDescriptorSchema,
    signature: z
      .object({
        algorithm: z.literal("Ed25519"),
        key_id: keyId,
        value: z.string().regex(/^[A-Za-z0-9+/]{86}==$/),
      })
      .strict(),
  })
  .strict();
export type SignedRelease = z.infer<typeof signedReleaseSchema>;
export function describeRelease(
  files: readonly PackageFile[],
  envelope: Uint8Array,
  runtimeDigest: string,
  origin: "bundled" | "upload" = "bundled",
) {
  const file = (path: string) => files.find((f) => f.path === path)!.bytes;
  const manifest = manifestSchema.parse(
    JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(file("manifest.json"))),
  );
  const releaseId = `${manifest.id}-${manifest.version}`;
  return releaseDescriptorSchema.parse({
    format: 1,
    release_id: releaseId,
    source: `${origin}:${releaseId}`,
    name: manifest.id,
    version: manifest.version,
    package_digest: packageDigest(files),
    envelope_digest: sha256(envelope),
    abi_version: 1,
    config_schema_digest: sha256(file("config.schema.json")),
    fixture_digest: sha256(file("fixtures/cases.json")),
    validation_policy_digest: validationPolicyDigest,
    runtime_digest: runtimeDigest,
    files: [...files]
      .sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0))
      .map((f) => ({ path: f.path, bytes: f.bytes.length, sha256: sha256(f.bytes) })),
  });
}
export async function verifyRelease(
  bytes: Uint8Array,
  signedInput: unknown,
  policyInput: unknown,
  runtimeDigest: string,
  now = Date.now(),
) {
  try {
    const signed = signedReleaseSchema.parse(signedInput),
      policy = releasePolicySchema.parse(policyInput),
      d = signed.descriptor;
    if (policy.revoked || Date.parse(policy.expires_at) <= now)
      throw new Error("RELEASE_POLICY_EXPIRED");
    if (d.source.startsWith("upload:"))
      return verifyPublisherRelease(bytes, signed, policy, runtimeDigest, now);
    const releaseId = bundledReleaseIds.find((id) => id === d.release_id);
    if (!releaseId || d.source !== `bundled:${releaseId}`) throw new Error("RELEASE_NOT_ALLOWED");
    if (
      new Set(policy.keys.map((k) => k.id)).size !== policy.keys.length ||
      new Set(policy.releases.map((r) => r.release_id)).size !== policy.releases.length
    )
      throw new Error("RELEASE_POLICY_DUPLICATE");
    const allowed = policy.releases.find((r) => r.release_id === d.release_id);
    if (!allowed || allowed.revoked || allowed.descriptor_digest !== sha256(canonical(d)))
      throw new Error("RELEASE_NOT_ALLOWED");
    for (const field of [
      "package_digest",
      "abi_version",
      "config_schema_digest",
      "fixture_digest",
      "validation_policy_digest",
      "runtime_digest",
    ] as const)
      if (allowed[field] !== d[field]) throw new Error("RELEASE_POLICY_MISMATCH");
    let trustExpiresAt = Date.parse(policy.expires_at);
    const usable = (id: string, purpose: "package" | "release") => {
      const key = policy.keys.find((k) => k.id === id);
      if (
        !key ||
        key.revoked ||
        Date.parse(key.not_before) > now ||
        Date.parse(key.expires_at) <= now ||
        !key.purposes.includes(purpose) ||
        !key.release_ids.includes(releaseId)
      )
        throw new Error("RELEASE_KEY_DENIED");
      trustExpiresAt = Math.min(trustExpiresAt, Date.parse(key.expires_at));
      const publicKey = createPublicKey(key.public_key);
      if (publicKey.asymmetricKeyType !== "ed25519") throw new Error("RELEASE_KEY_TYPE");
      return publicKey;
    };
    if (
      !verify(
        null,
        Buffer.from(`gateway-plugin-release-v1\0${canonical(d)}`),
        usable(signed.signature.key_id, "release"),
        Buffer.from(signed.signature.value, "base64"),
      )
    )
      throw new Error("RELEASE_SIGNATURE");
    if (
      bytes.length > 44 << 20 ||
      sha256(bytes) !== d.envelope_digest ||
      d.runtime_digest !== runtimeDigest ||
      d.validation_policy_digest !== validationPolicyDigest
    )
      throw new Error("RELEASE_CONTENT_MISMATCH");
    const envelope = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as {
      signature: { key_id: string; value: string };
    };
    const pkg = verifyPackage(
      bytes,
      new Map([[envelope.signature.key_id, usable(envelope.signature.key_id, "package")]]),
    );
    const bundled = await bundledPackageFiles(releaseId);
    if (
      pkg.digest !== packageDigest(bundled) ||
      canonical(describeRelease(pkg.files, bytes, runtimeDigest)) !== canonical(d)
    )
      throw new Error("RELEASE_BUNDLE_MISMATCH");
    return {
      pkg,
      signed,
      policy,
      packageSignature: envelope.signature,
      trustExpiresAt: new Date(trustExpiresAt).toISOString(),
    };
  } catch (error) {
    if (error instanceof GatewayError) throw error;
    throw new GatewayError(
      400,
      "untrusted_plugin_release",
      "Plugin release does not satisfy the configured trust policy",
    );
  }
}

function verifyPublisherRelease(
  bytes: Uint8Array,
  signed: SignedRelease,
  policy: z.infer<typeof releasePolicySchema>,
  runtimeDigest: string,
  now: number,
) {
  const d = signed.descriptor;
  const ids = [...policy.keys, ...policy.publishers].map((k) => k.id);
  if (new Set(ids).size !== ids.length) throw new Error("RELEASE_POLICY_DUPLICATE");
  if (policy.releases.some((r) => r.release_id === d.release_id))
    throw new Error("RELEASE_NOT_ALLOWED");
  const publisher = policy.publishers.find((p) => p.id === signed.signature.key_id);
  if (
    !publisher ||
    publisher.revoked ||
    Date.parse(publisher.not_before) > now ||
    Date.parse(publisher.expires_at) <= now ||
    !publisherAdmits(publisher, { id: d.name })
  )
    throw new Error("RELEASE_KEY_DENIED");
  const publicKey = createPublicKey(publisher.public_key);
  if (publicKey.asymmetricKeyType !== "ed25519") throw new Error("RELEASE_KEY_TYPE");
  if (
    !verify(
      null,
      Buffer.from(`gateway-plugin-release-v1\0${canonical(d)}`),
      publicKey,
      Buffer.from(signed.signature.value, "base64"),
    )
  )
    throw new Error("RELEASE_SIGNATURE");
  if (
    bytes.length > 44 << 20 ||
    sha256(bytes) !== d.envelope_digest ||
    d.runtime_digest !== runtimeDigest ||
    d.validation_policy_digest !== validationPolicyDigest
  )
    throw new Error("RELEASE_CONTENT_MISMATCH");
  const pkg = verifyPackage(bytes, new Map([[publisher.id, publicKey]]));
  if (canonical(describeRelease(pkg.files, bytes, runtimeDigest, "upload")) !== canonical(d))
    throw new Error("RELEASE_CONTENT_MISMATCH");
  return {
    pkg,
    signed,
    policy,
    packageSignature: { key_id: publisher.id, value: packageSignatureValue(bytes) },
    trustExpiresAt: new Date(
      Math.min(Date.parse(policy.expires_at), Date.parse(publisher.expires_at)),
    ).toISOString(),
  };
}
function packageSignatureValue(bytes: Uint8Array): string {
  return (
    JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as {
      signature: { value: string };
    }
  ).signature.value;
}
