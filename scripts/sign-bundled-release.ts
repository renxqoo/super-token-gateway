import { createPrivateKey, sign } from "node:crypto";
import { parseArgs } from "node:util";
import { mkdir, writeFile, stat, rm } from "node:fs/promises";
import { resolve } from "node:path";
import { openAIChatPackageFiles } from "@gateway/adapters";
import { packageDigest, sha256 } from "@gateway/plugin-sdk";
import { canonical } from "@gateway/kernel";
import {
  bundledReleaseId,
  describeRelease,
  signedReleaseSchema,
} from "../apps/core-api/src/plugins/releases.ts";
const { values } = parseArgs({
  options: {
    "private-key-file": { type: "string" },
    "key-id": { type: "string" },
    "output-directory": { type: "string" },
    "helper-executable": { type: "string" },
  },
  strict: true,
});
const keyPath = values["private-key-file"],
  keyId = values["key-id"],
  output = values["output-directory"],
  helper = values["helper-executable"];
if (!keyPath || !keyId || !output || !helper || !/^[A-Za-z0-9_.-]{1,100}$/.test(keyId))
  throw new Error("Required: --private-key-file --key-id --output-directory --helper-executable");
const permissions = await stat(keyPath);
if (!permissions.isFile() || (permissions.mode & 0o077) !== 0)
  throw new Error("Signing key must be a private regular file with mode 0600");
const key = createPrivateKey(await Bun.file(keyPath).text());
if (key.asymmetricKeyType !== "ed25519") throw new Error("An Ed25519 private key is required");
const files = await openAIChatPackageFiles();
const envelope = new TextEncoder().encode(
  JSON.stringify({
    format: 1,
    files: files.map((f) => ({ path: f.path, data: Buffer.from(f.bytes).toString("base64") })),
    signature: {
      algorithm: "Ed25519",
      key_id: keyId,
      value: sign(null, Buffer.from(packageDigest(files), "hex"), key).toString("base64"),
    },
  }),
);
const descriptor = describeRelease(files, envelope, sha256(await Bun.file(helper).bytes()));
const release = signedReleaseSchema.parse({
  descriptor,
  signature: {
    algorithm: "Ed25519",
    key_id: keyId,
    value: sign(
      null,
      Buffer.from(`gateway-plugin-release-v1\0${canonical(descriptor)}`),
      key,
    ).toString("base64"),
  },
});
const directory = resolve(output, bundledReleaseId);
await mkdir(resolve(output), { recursive: true, mode: 0o700 });
await mkdir(directory, { mode: 0o700 });
try {
  await writeFile(resolve(directory, "package.json"), envelope, { mode: 0o600, flag: "wx" });
  await writeFile(resolve(directory, "release.json"), JSON.stringify(release, null, 2) + "\n", {
    mode: 0o600,
    flag: "wx",
  });
} catch (error) {
  await rm(directory, { recursive: true, force: true });
  throw error;
}
console.log(JSON.stringify({ descriptor_digest: sha256(canonical(descriptor)), ...descriptor }));
