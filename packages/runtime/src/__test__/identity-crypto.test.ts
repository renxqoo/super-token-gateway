import { expect, test } from "bun:test";
import { generateKeyPairSync, sign } from "node:crypto";
import {
  canonicalEd25519Spki,
  decodeCanonicalBase64url,
  sha256,
  verifyEd25519Proof,
} from "../crypto.ts";
test("startup public key aliases cannot obtain different canonical key identities", () => {
  const pair = generateKeyPairSync("ed25519"),
    spki = pair.publicKey.export({ type: "spki", format: "der" }),
    encoded = spki.toString("base64url");
  expect(canonicalEd25519Spki(encoded)).toEqual(spki);
  for (const alias of [
    encoded + "=",
    ` ${encoded}`,
    pair.publicKey.export({ type: "spki", format: "pem" }).toString(),
    spki.toString("base64"),
    Buffer.concat([spki, Buffer.from([0])]).toString("base64url"),
  ])
    expect(() => canonicalEd25519Spki(alias)).toThrow();
  const wrong = generateKeyPairSync("ec", { namedCurve: "prime256v1" });
  expect(() =>
    canonicalEd25519Spki(
      wrong.publicKey.export({ type: "spki", format: "der" }).toString("base64url"),
    ),
  ).toThrow();
  expect(sha256(canonicalEd25519Spki(encoded))).toBe(sha256(spki));
});
test("every signed field including domain and audience binds exact UTF8 bytes", () => {
  const pair = generateKeyPairSync("ed25519"),
    spki = pair.publicKey.export({ type: "spki", format: "der" }),
    fields = [
      "stg.internal.command",
      "1",
      "cluster/core-api",
      "gateway",
      "A",
      "session",
      "incarnation",
      "kind",
      "command",
      "POST",
      "/exact",
      "raw-hash",
      "deadline",
      "trace",
    ];
  const signature = sign(null, Buffer.from(JSON.stringify(fields)), pair.privateKey).toString(
    "base64url",
  );
  expect(verifyEd25519Proof(spki, fields, signature)).toBe(true);
  for (let i = 0; i < fields.length; i++) {
    const changed = [...fields];
    changed[i] += "x";
    expect(verifyEd25519Proof(spki, changed, signature)).toBe(false);
  }
  expect(verifyEd25519Proof(spki, fields, signature + "=")).toBe(false);
  expect(verifyEd25519Proof(spki, [...fields, ""], signature)).toBe(false);
  expect(() => decodeCanonicalBase64url("AB", 32)).toThrow();
});
