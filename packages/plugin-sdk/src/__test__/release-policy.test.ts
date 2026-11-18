import { expect, test } from "bun:test";
import { generateKeyPairSync, type KeyObject } from "node:crypto";
import { openAIChatReleaseId, packageTrust, releasePolicySchema, verifyPackage } from "../index.ts";
import { syntheticChatPackage } from "../../../../tests/support/plugin-package.ts";
import { syntheticSignedPackage } from "../../../../tests/support/plugin-package.ts";
import { openAIChatPackageFiles } from "@gateway/adapters";

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
        release_ids: [openAIChatReleaseId],
        ...key,
      },
    ],
    releases: [
      {
        release_id: openAIChatReleaseId,
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

test("platform classification uses authenticated private package identity and cannot be forged by public field mutation", async () => {
  const keys = generateKeyPairSync("ed25519");
  const signed = await syntheticChatPackage(keys);
  const trust = packageTrust(
    policy(keys.publicKey, signed.pkg.digest, { id: "synthetic-test-only" }),
    now,
  );
  expect(trust.isPlatform(signed.pkg, now)).toBe(true);
  const copy = { ...signed.pkg };
  signed.pkg.digest = "9".repeat(64);
  signed.pkg.signingKeyId = "unlisted";
  signed.pkg.manifest.id = "acme.chat";
  expect(trust.isPlatform(signed.pkg, now)).toBe(true);
  expect(() => trust.isPlatform(copy, now)).toThrow();
  expect(trust.isPlatform(signed.pkg, Date.parse("2031-01-01T00:00:00.000Z"))).toBe(false);
});

test("a matching key id cannot impersonate the actual deployment signing key for platform execution", async () => {
  const deployment = generateKeyPairSync("ed25519");
  const legitimate = await syntheticChatPackage(deployment);
  const stranger = syntheticSignedPackage(
    [...legitimate.pkg.files],
    generateKeyPairSync("ed25519"),
  );
  const trust = packageTrust(
    policy(deployment.publicKey, legitimate.pkg.digest, { id: "synthetic-test-only" }),
    now,
  );
  expect(stranger.pkg.digest).toBe(legitimate.pkg.digest);
  expect(trust.isPlatform(legitimate.pkg, now)).toBe(true);
  expect(trust.isPlatform(stranger.pkg, now)).toBe(false);
});

test("publisher admission never grants platform execution even when its public view impersonates an approved package", async () => {
  const platform = generateKeyPairSync("ed25519"),
    publisherKey = generateKeyPairSync("ed25519");
  const files = (await openAIChatPackageFiles()).map((file) =>
    file.path === "manifest.json"
      ? {
          path: file.path,
          bytes: new TextEncoder().encode(
            JSON.stringify({
              ...JSON.parse(new TextDecoder().decode(file.bytes)),
              id: "acme.chat",
            }),
          ),
        }
      : file,
  );
  const uploaded = syntheticSignedPackage(files, publisherKey).pkg;
  const trusted = packageTrust(
    policy(
      platform.publicKey,
      digestA,
      {},
      {},
      { publishers: [publisher(publisherKey.publicKey, { id: "synthetic-test-only" })] },
    ),
    now,
  );
  expect(trusted.admits(uploaded)).toBe(true);
  expect(trusted.isPlatform(uploaded, now)).toBe(false);
  uploaded.digest = digestA;
  uploaded.signingKeyId = "deployment-key";
  uploaded.manifest.id = "gateway.openai-chat";
  expect(trusted.isPlatform(uploaded, now)).toBe(false);
});

test("platform classification rejects revoked, unbound and naturally expired grants", async () => {
  const keys = generateKeyPairSync("ed25519");
  const signed = await syntheticChatPackage(keys);
  for (const [key, release] of [
    [{ expires_at: "2030-06-01T00:00:01.000Z" }, {}],
    [{}, { revoked: true }],
    [{ purposes: ["release"] }, {}],
  ] as const) {
    const trusted = packageTrust(
      policy(keys.publicKey, signed.pkg.digest, { id: "synthetic-test-only", ...key }, release),
      now,
    );
    expect(trusted.isPlatform(signed.pkg, now + 1000)).toBe(false);
  }
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

function publisher(publicKey: KeyObject, overrides: Record<string, unknown> = {}) {
  return {
    id: "publisher-key",
    public_key: publicKey.export({ format: "pem", type: "spki" }).toString(),
    not_before: "2030-01-01T00:00:00.000Z",
    expires_at: "2031-01-01T00:00:00.000Z",
    revoked: false,
    namespace: "acme",
    ...overrides,
  };
}

test("a trusted publisher admits unlisted digests only for plugins inside its namespace", () => {
  const deployment = generateKeyPairSync("ed25519"),
    acme = generateKeyPairSync("ed25519");
  const trust = packageTrust(
    policy(deployment.publicKey, digestA, {}, {}, { publishers: [publisher(acme.publicKey)] }),
    now,
  );
  const unlisted = "9".repeat(64);
  expect([...trust(unlisted).keys()]).toEqual(["publisher-key"]);
  expect(
    trust.admits({
      digest: unlisted,
      signingKeyId: "publisher-key",
      manifest: { id: "acme.chat" },
    }),
  ).toBe(true);
  for (const id of ["gateway.openai-chat", "acmex.chat", "acme"])
    expect(
      trust.admits({ digest: unlisted, signingKeyId: "publisher-key", manifest: { id } }),
    ).toBe(false);
  expect(
    trust.admits({ digest: digestA, signingKeyId: "deployment-key", manifest: { id: "any" } }),
  ).toBe(true);
  expect(
    trust.admits({ digest: unlisted, signingKeyId: "deployment-key", manifest: { id: "acme.x" } }),
  ).toBe(false);
});

test("revoked, expired, not-yet-valid or gateway-namespace publishers grant no trust", () => {
  const deployment = generateKeyPairSync("ed25519"),
    acme = generateKeyPairSync("ed25519");
  for (const overrides of [
    { revoked: true },
    { expires_at: "2030-05-01T00:00:00.000Z" },
    { not_before: "2030-07-01T00:00:00.000Z" },
  ]) {
    const trust = packageTrust(
      policy(
        deployment.publicKey,
        digestA,
        {},
        {},
        {
          publishers: [publisher(acme.publicKey, overrides)],
        },
      ),
      now,
    );
    expect(trust("9".repeat(64)).size).toBe(0);
    expect(
      trust.admits({
        digest: "9".repeat(64),
        signingKeyId: "publisher-key",
        manifest: { id: "acme.chat" },
      }),
    ).toBe(false);
  }
  expect(() =>
    policy(
      deployment.publicKey,
      digestA,
      {},
      {},
      {
        publishers: [publisher(acme.publicKey, { namespace: "gateway" })],
      },
    ),
  ).toThrow();
  expect(() =>
    packageTrust(
      policy(
        deployment.publicKey,
        digestA,
        {},
        {},
        {
          publishers: [publisher(acme.publicKey, { id: "deployment-key" })],
        },
      ),
      now,
    ),
  ).toThrow("RELEASE_POLICY_DUPLICATE");
});
