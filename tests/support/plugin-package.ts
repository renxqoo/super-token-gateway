import { canonical } from "@gateway/kernel";
import { describeRelease } from "../../apps/core-api/src/plugins/releases.ts";
import { generateKeyPairSync, sign, type KeyObject } from "node:crypto";
import { openAIChatPackageFiles } from "@gateway/adapters";
import { packageDigest, verifyPackage, type PackageFile } from "@gateway/plugin-sdk";

export function syntheticSignedPackage(
  files: PackageFile[],
  key: { privateKey: KeyObject; publicKey: KeyObject } = generateKeyPairSync("ed25519"),
) {
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
export async function syntheticChatPackage(key?: { privateKey: KeyObject; publicKey: KeyObject }) {
  return syntheticSignedPackage(await openAIChatPackageFiles(), key);
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
export async function publisherUpload(
  key: { privateKey: KeyObject },
  runtimeDigest: string,
  manifest: { id: string; version: string },
  keyId = "synthetic-publisher",
) {
  const files = (await openAIChatPackageFiles()).map((file) =>
    file.path === "manifest.json"
      ? {
          path: file.path,
          bytes: new TextEncoder().encode(
            JSON.stringify({ ...JSON.parse(new TextDecoder().decode(file.bytes)), ...manifest }),
          ),
        }
      : file,
  );
  const bytes = new TextEncoder().encode(
    JSON.stringify({
      format: 1,
      files: files.map((file) => ({
        path: file.path,
        data: Buffer.from(file.bytes).toString("base64"),
      })),
      signature: {
        algorithm: "Ed25519",
        key_id: keyId,
        value: sign(null, Buffer.from(packageDigest(files), "hex"), key.privateKey).toString(
          "base64",
        ),
      },
    }),
  );
  const descriptor = describeRelease(files, bytes, runtimeDigest, "upload");
  return {
    bytes,
    package_base64: Buffer.from(bytes).toString("base64"),
    release: {
      descriptor,
      signature: {
        algorithm: "Ed25519",
        key_id: keyId,
        value: sign(
          null,
          Buffer.from(`gateway-plugin-release-v1\0${canonical(descriptor)}`),
          key.privateKey,
        ).toString("base64"),
      },
    },
  };
}
