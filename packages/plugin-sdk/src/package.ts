import { verify, type KeyObject } from "node:crypto";
import { z } from "zod";
import { base64Schema, capabilitiesSchema, sha256 } from "./abi.ts";
import { configSchemaSchema, type ConfigSchema } from "./config.ts";
import { fixtureSuiteSchema } from "./fixtures.ts";
import { exactParse } from "./guest.ts";

export const manifestSchema = z
  .object({
    format: z.literal(1),
    id: z.string().regex(/^[a-z][a-z0-9.-]{1,100}$/),
    version: z.string().regex(/^\d+\.\d+\.\d+$/),
    abi_version: z.literal(1),
    sdk_version: z.literal("1.0.0"),
    module: z.literal("module.wasm"),
    entrypoint: z.string().regex(/^[A-Za-z][A-Za-z0-9_]{0,63}$/),
    config_schema: z.literal("config.schema.json"),
    capabilities: capabilitiesSchema,
  })
  .strict();
export type PluginManifest = z.infer<typeof manifestSchema>;
export type PackageFile = { path: string; bytes: Uint8Array };
const envelopeSchema = z
  .object({
    format: z.literal(1),
    files: z
      .array(z.object({ path: z.string().min(1).max(200), data: base64Schema }).strict())
      .min(6)
      .max(128),
    signature: z
      .object({
        algorithm: z.literal("Ed25519"),
        key_id: z.string().regex(/^[A-Za-z0-9_.-]{1,100}$/),
        value: base64Schema,
      })
      .strict(),
  })
  .strict();
export type VerifiedPackage = {
  digest: string;
  moduleHash: string;
  module: Uint8Array;
  manifest: PluginManifest;
  configSchema: ConfigSchema;
  files: readonly PackageFile[];
};
const verified = new WeakMap<VerifiedPackage, VerifiedPackage>();
export function verifiedPackageSnapshot(pkg: VerifiedPackage): VerifiedPackage {
  const original = verified.get(pkg);
  if (!original) throw new Error("PACKAGE_NOT_VERIFIED");
  return structuredClone(original);
}
export function packageDigest(files: readonly PackageFile[]): string {
  if (files.length < 6 || files.length > 128) throw new Error("PACKAGE_FILES");
  const hasher = new Bun.CryptoHasher("sha256").update("gateway-plugin-package-v1\0");
  const seen = new Set<string>();
  let total = 0,
    fixtures = 0;
  for (const file of [...files].sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0))) {
    if (
      !/^[A-Za-z0-9][A-Za-z0-9_./-]{0,199}$/.test(file.path) ||
      file.path.split("/").some((p) => p === "." || p === ".." || !p) ||
      file.path.split("/").length > 4 ||
      seen.has(file.path)
    )
      throw new Error("PACKAGE_PATH");
    seen.add(file.path);
    total += file.bytes.length;
    if (file.path.startsWith("fixtures/")) fixtures += file.bytes.length;
    const limit =
      file.path === "module.wasm"
        ? 8 << 20
        : file.path.startsWith("fixtures/")
          ? 2 << 20
          : 256 << 10;
    if (file.bytes.length > limit || total > 32 << 20 || fixtures > 2 << 20)
      throw new Error("PACKAGE_BUDGET");
    hasher
      .update(`${new TextEncoder().encode(file.path).length}:${file.path}:${file.bytes.length}:`)
      .update(file.bytes);
  }
  for (const required of [
    "manifest.json",
    "module.wasm",
    "config.schema.json",
    "LICENSE",
    "provenance.json",
    "fixtures/cases.json",
  ])
    if (!seen.has(required)) throw new Error("PACKAGE_MISSING");
  return hasher.digest("hex");
}
export function verifyPackage(
  bytes: Uint8Array,
  trust: ReadonlyMap<string, KeyObject>,
): VerifiedPackage {
  if (bytes.length > 44 << 20) throw new Error("PACKAGE_BUDGET");
  const envelope = envelopeSchema.parse(
    exactParse(new TextDecoder("utf-8", { fatal: true }).decode(bytes), 502, "number"),
  );
  const files = envelope.files.map((file) => ({
    path: file.path,
    bytes: new Uint8Array(Buffer.from(file.data, "base64")),
  }));
  const digest = packageDigest(files);
  const key = trust.get(envelope.signature.key_id);
  if (
    !key ||
    key.type !== "public" ||
    key.asymmetricKeyType !== "ed25519" ||
    !verify(null, Buffer.from(digest, "hex"), key, Buffer.from(envelope.signature.value, "base64"))
  )
    throw new Error("PACKAGE_SIGNATURE");
  const get = (path: string) => files.find((f) => f.path === path)!.bytes;
  const decode = (path: string) =>
    exactParse(new TextDecoder("utf-8", { fatal: true }).decode(get(path)), 502, "number");
  const manifest = manifestSchema.parse(decode("manifest.json"));
  const configSchema = configSchemaSchema.parse(decode("config.schema.json"));
  z.object({
    source: z.string().min(1).max(2048),
    tools: z.record(z.string(), z.string().max(2048)),
    licenses: z.array(z.string().min(1).max(200)).min(1).max(20),
  })
    .strict()
    .parse(decode("provenance.json"));
  fixtureSuiteSchema.parse(decode("fixtures/cases.json"));
  if (!get("LICENSE").length) throw new Error("PACKAGE_LICENSE");
  const module = get("module.wasm");
  if (
    module.length < 8 ||
    !module.slice(0, 8).every((n, i) => n === [0, 97, 115, 109, 1, 0, 0, 0][i])
  )
    throw new Error("PACKAGE_MODULE");
  const result = { digest, moduleHash: sha256(module), module, manifest, configSchema, files };
  verified.set(result, structuredClone(result));
  return result;
}
