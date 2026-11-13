import { createPublicKey, type KeyObject } from "node:crypto";
import { z } from "zod";

export const bundledReleaseId = "gateway.openai-chat-1.0.0" as const;
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const keyId = z.string().regex(/^[A-Za-z0-9_.-]{1,100}$/);
export const releasePolicySchema = z
  .object({
    format: z.literal(1),
    expires_at: z.iso.datetime(),
    revoked: z.boolean(),
    keys: z
      .array(
        z
          .object({
            id: keyId,
            public_key: z.string().min(1).max(4096),
            not_before: z.iso.datetime(),
            expires_at: z.iso.datetime(),
            revoked: z.boolean(),
            purposes: z.array(z.enum(["package", "release"])).min(1),
            release_ids: z.array(z.literal(bundledReleaseId)).min(1),
          })
          .strict(),
      )
      .min(1)
      .max(100),
    releases: z
      .array(
        z
          .object({
            release_id: z.literal(bundledReleaseId),
            descriptor_digest: digest,
            package_digest: digest,
            abi_version: z.literal(1),
            config_schema_digest: digest,
            fixture_digest: digest,
            validation_policy_digest: digest,
            runtime_digest: digest,
            revoked: z.boolean(),
          })
          .strict(),
      )
      .min(1)
      .max(100),
  })
  .strict();
export type ReleasePolicy = z.infer<typeof releasePolicySchema>;

export function packageTrust(policy: ReleasePolicy, now = Date.now()) {
  if (policy.revoked || Date.parse(policy.expires_at) <= now)
    throw new Error("RELEASE_POLICY_EXPIRED");
  if (
    new Set(policy.keys.map((k) => k.id)).size !== policy.keys.length ||
    new Set(policy.releases.map((r) => r.release_id)).size !== policy.releases.length
  )
    throw new Error("RELEASE_POLICY_DUPLICATE");
  const usable = policy.keys.filter(
    (key) =>
      !key.revoked &&
      key.purposes.includes("package") &&
      Date.parse(key.not_before) <= now &&
      Date.parse(key.expires_at) > now,
  );
  const publicKeys = new Map<string, KeyObject>();
  for (const key of usable) {
    const publicKey = createPublicKey(key.public_key);
    if (publicKey.asymmetricKeyType !== "ed25519") throw new Error("RELEASE_KEY_TYPE");
    publicKeys.set(key.id, publicKey);
  }
  const byDigest = new Map<string, Map<string, KeyObject>>();
  for (const release of policy.releases) {
    if (release.revoked) continue;
    const keys = byDigest.get(release.package_digest) ?? new Map<string, KeyObject>();
    for (const key of usable)
      if (key.release_ids.includes(release.release_id)) keys.set(key.id, publicKeys.get(key.id)!);
    byDigest.set(release.package_digest, keys);
  }
  return (packageDigest: string): ReadonlyMap<string, KeyObject> =>
    byDigest.get(packageDigest) ?? new Map();
}
