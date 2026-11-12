import { generateKeyPairSync, sign } from "node:crypto";
import { openAIChatPackageFiles } from "@gateway/adapters";
import { packageDigest, verifyPackage, type PackageFile } from "@gateway/plugin-sdk";

export function syntheticSignedPackage(files: PackageFile[]) {
  const key = generateKeyPairSync("ed25519");
  const envelope = {
    format: 1,
    files: files.map((file) => ({
      path: file.path,
      data: Buffer.from(file.bytes).toString("base64"),
    })),
    signature: {
      algorithm: "Ed25519",
      key_id: "synthetic-test-only",
      value: sign(null, Buffer.from(packageDigest(files), "hex"), key.privateKey).toString(
        "base64",
      ),
    },
  };
  const trust = new Map([["synthetic-test-only", key.publicKey]]);
  const bytes = new TextEncoder().encode(JSON.stringify(envelope));
  return { bytes, trust, envelope, pkg: verifyPackage(bytes, trust) };
}
export async function syntheticChatPackage() {
  return syntheticSignedPackage(await openAIChatPackageFiles());
}
export async function syntheticAdversarialPackage() {
  const files = await openAIChatPackageFiles(),
    module = await Bun.file(new URL("./plugins/isolation.wasm", import.meta.url)).bytes();
  const encode = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
  return syntheticSignedPackage(
    files
      .filter((file) => file.path !== "source-hashes.json")
      .map((file) => {
        if (file.path === "module.wasm") return { path: file.path, bytes: module };
        if (file.path === "manifest.json")
          return {
            path: file.path,
            bytes: encode({
              ...JSON.parse(new TextDecoder().decode(file.bytes)),
              id: "gateway.synthetic-adversary",
            }),
          };
        if (file.path === "config.schema.json") {
          const schema = JSON.parse(new TextDecoder().decode(file.bytes));
          schema.properties.fixture_mode = { type: "string", maxLength: 100 };
          schema.properties.foreign_document = { type: "string", maxLength: 100 };
          return { path: file.path, bytes: encode(schema) };
        }
        if (file.path === "provenance.json")
          return {
            path: file.path,
            bytes: encode({
              source: "tests/support/plugins/isolation.ts",
              tools: { purpose: "synthetic adversarial WASM; no production trust" },
              licenses: ["MIT"],
            }),
          };
        return file;
      }),
  );
}
