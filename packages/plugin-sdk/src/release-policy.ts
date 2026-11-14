import { createPublicKey, type KeyObject } from "node:crypto";
import { z } from "zod";

export const openAIChatReleaseId = "gateway.openai-chat-1.3.0" as const;
export const anthropicMessagesReleaseId = "gateway.anthropic-messages-1.2.0" as const;
export const bedrockAnthropicReleaseId = "gateway.bedrock-anthropic-1.1.0" as const;
export const geminiGenerateReleaseId = "gateway.gemini-generate-1.2.0" as const;
export const bundledReleaseIds = [
  openAIChatReleaseId,
  anthropicMessagesReleaseId,
  geminiGenerateReleaseId,
  bedrockAnthropicReleaseId,
] as const;
export type BundledReleaseId = (typeof bundledReleaseIds)[number];
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const keyId = z.string().regex(/^[A-Za-z0-9_.-]{1,100}$/);
export const publisherNamespaceSchema = z
  .string()
  .regex(/^[a-z][a-z0-9-]{1,40}$/)
  .refine((value) => value !== "gateway");
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
            release_ids: z.array(z.enum(bundledReleaseIds)).min(1),
          })
          .strict(),
      )
      .min(1)
      .max(100),
    releases: z
      .array(
        z
          .object({
            release_id: z.enum(bundledReleaseIds),
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
    publishers: z
      .array(
        z
          .object({
            id: keyId,
            public_key: z.string().min(1).max(4096),
            not_before: z.iso.datetime(),
            expires_at: z.iso.datetime(),
            revoked: z.boolean(),
            namespace: publisherNamespaceSchema,
          })
          .strict(),
      )
      .max(100)
      .default([]),
  })
  .strict();
export type ReleasePolicy = z.infer<typeof releasePolicySchema>;
export function publisherAdmits(
  publisher: { namespace: string },
  manifest: { id: string },
): boolean {
  return manifest.id.startsWith(`${publisher.namespace}.`);
}

export function packageTrust(policy: ReleasePolicy, now = Date.now()) {
  if (policy.revoked || Date.parse(policy.expires_at) <= now)
    throw new Error("RELEASE_POLICY_EXPIRED");
  const ids = [...policy.keys, ...policy.publishers].map((k) => k.id);
  if (
    new Set(ids).size !== ids.length ||
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
  const publishers = new Map<string, { key: KeyObject; namespace: string }>();
  for (const publisher of policy.publishers) {
    if (
      publisher.revoked ||
      Date.parse(publisher.not_before) > now ||
      Date.parse(publisher.expires_at) <= now
    )
      continue;
    const publicKey = createPublicKey(publisher.public_key);
    if (publicKey.asymmetricKeyType !== "ed25519") throw new Error("RELEASE_KEY_TYPE");
    publishers.set(publisher.id, { key: publicKey, namespace: publisher.namespace });
  }
  return Object.assign(
    (packageDigest: string): ReadonlyMap<string, KeyObject> => {
      const keys = new Map(byDigest.get(packageDigest));
      for (const [id, publisher] of publishers) keys.set(id, publisher.key);
      return keys;
    },
    {
      admits(pkg: { digest: string; signingKeyId: string; manifest: { id: string } }): boolean {
        if (byDigest.get(pkg.digest)?.has(pkg.signingKeyId)) return true;
        const publisher = publishers.get(pkg.signingKeyId);
        return publisher !== undefined && publisherAdmits(publisher, pkg.manifest);
      },
    },
  );
}
