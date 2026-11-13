import { expect, test } from "bun:test";
import { generateKeyPairSync, type KeyObject } from "node:crypto";
import { bundledReleaseId, packageTrust, releasePolicySchema, verifyPackage } from "../index.ts";
import { syntheticChatPackage } from "../../../../tests/support/plugin-package.ts";

const now = Date.parse("2030-06-01T00:00:00.000Z");
const digestA = "a".repeat(64);

function policy(
  publicKey: KeyObject,
  packageDigest: string,
  key: Record<string, unknown> = {},
  release: Record<string, unknown> = {},
  top: Record<string, unknown> = {},
) {
  return releasePolicySchema.parse({
    format: 1,
    expires_at: "2031-01-01T00:00:00.000Z",
    revoked: false,
    keys: [
      {
        id: "deployment-key",
        public_key: publicKey.export({ format: "pem", type: "spki" }).toString(),
        not_before: "2030-01-01T00:00:00.000Z",
        expires_at: "2031-01-01T00:00:00.000Z",
        revoked: false,
        purposes: ["package"],
        release_ids: [bundledReleaseId],
        ...key,
      },
    ],
    releases: [
      {
        release_id: bundledReleaseId,
        descriptor_digest: "b".repeat(64),
        package_digest: packageDigest,
        abi_version: 1,
        config_schema_digest: "c".repeat(64),
        fixture_digest: "d".repeat(64),
        validation_policy_digest: "e".repeat(64),
        runtime_digest: "f".repeat(64),
        revoked: false,
        ...release,
      },
    ],
    ...top,
  });
}

test("a listed release's package verifies with its bound key and an unbound signer is refused", async () => {
  const keys = generateKeyPairSync("ed25519");
  const signed = await syntheticChatPackage(keys);
  const trust = (publicKey: KeyObject) =>
    packageTrust(policy(publicKey, signed.pkg.digest, { id: "synthetic-test-only" }), now);
  expect(verifyPackage(signed.bytes, trust(keys.publicKey)(signed.pkg.digest)).digest).toBe(
    signed.pkg.digest,
  );
  expect(trust(keys.publicKey)(digestA).size).toBe(0);
  const stranger = generateKeyPairSync("ed25519").publicKey;
  expect(() => verifyPackage(signed.bytes, trust(stranger)(signed.pkg.digest))).toThrow();
});

test("revoked releases, release-only keys and keys outside their validity window grant no package trust", () => {
  const { publicKey } = generateKeyPairSync("ed25519");
  for (const [key, release] of [
    [{}, { revoked: true }],
    [{ purposes: ["release"] }, {}],
    [{ revoked: true }, {}],
    [{ not_before: "2030-07-01T00:00:00.000Z" }, {}],
    [{ expires_at: "2030-06-01T00:00:00.000Z" }, {}],
  ] as const)
    expect(packageTrust(policy(publicKey, digestA, key, release), now)(digestA).size).toBe(0);
  expect(packageTrust(policy(publicKey, digestA), now)(digestA).size).toBe(1);
});

test("an expired or revoked policy and a non-Ed25519 package key are refused outright", () => {
  const { publicKey } = generateKeyPairSync("ed25519");
  expect(() =>
    packageTrust(
      policy(publicKey, digestA, {}, {}, { expires_at: "2030-06-01T00:00:00.000Z" }),
      now,
    ),
  ).toThrow("RELEASE_POLICY_EXPIRED");
  expect(() => packageTrust(policy(publicKey, digestA, {}, {}, { revoked: true }), now)).toThrow(
    "RELEASE_POLICY_EXPIRED",
  );
  const rsa = generateKeyPairSync("rsa", { modulusLength: 2048 }).publicKey;
  expect(() => packageTrust(policy(rsa, digestA), now)).toThrow("RELEASE_KEY_TYPE");
});
