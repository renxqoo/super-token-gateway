import { expect, test } from "bun:test";
import { openAIChatPackageFiles } from "@gateway/adapters";
import { packageDigest, verifyPackage, validateConfig, preparedSchema } from "../index.ts";
import {
  syntheticChatPackage,
  syntheticSignedPackage,
} from "../../../../tests/support/plugin-package.ts";

test("signed package bytes are immutable identities and unsigned or tampered replacement is refused", async () => {
  const signed = await syntheticChatPackage();
  expect(() => verifyPackage(signed.bytes, new Map())).toThrow("PACKAGE_SIGNATURE");
  const envelope = structuredClone(signed.envelope);
  envelope.files.find((f) => f.path === "LICENSE")!.data =
    Buffer.from("tampered").toString("base64");
  expect(() =>
    verifyPackage(new TextEncoder().encode(JSON.stringify(envelope)), signed.trust),
  ).toThrow("PACKAGE_SIGNATURE");
  const unsigned = { format: 1, files: signed.envelope.files };
  expect(() =>
    verifyPackage(new TextEncoder().encode(JSON.stringify(unsigned)), signed.trust),
  ).toThrow();
  expect(packageDigest([...(await openAIChatPackageFiles())].reverse())).toBe(signed.pkg.digest);
});
test("package path boundaries, duplicates, missing required files and fixture budgets reject before execution", async () => {
  const files = await openAIChatPackageFiles();
  for (const path of [
    "../module.wasm",
    "/module.wasm",
    "a//b",
    "a/./b",
    "a/../b",
    "a\\b",
    "a/b/c/d/e",
    "https://synthetic/module.wasm",
  ])
    expect(() => packageDigest([...files, { path, bytes: new Uint8Array() }])).toThrow(
      "PACKAGE_PATH",
    );
  expect(() => packageDigest([...files, files[0]!])).toThrow("PACKAGE_PATH");
  expect(() => packageDigest(files.filter((f) => f.path !== "LICENSE"))).toThrow("PACKAGE_MISSING");
  expect(() =>
    packageDigest([...files, { path: "fixtures/oversize", bytes: new Uint8Array(2 << 20) }]),
  ).toThrow("PACKAGE_BUDGET");
});
test("unknown ABI, manifest fields, operations and dimensions cannot be authenticated into validity", async () => {
  const files = await openAIChatPackageFiles();
  for (const change of [
    { abi_version: 2 },
    { extra: "unknown" },
    {
      capabilities: {
        ...JSON.parse(
          new TextDecoder().decode(files.find((f) => f.path === "manifest.json")!.bytes),
        ).capabilities,
        operations: ["mint_balance"],
      },
    },
    {
      capabilities: {
        ...JSON.parse(
          new TextDecoder().decode(files.find((f) => f.path === "manifest.json")!.bytes),
        ).capabilities,
        dimensions: ["customer_charge"],
      },
    },
  ]) {
    const changed = files.map((file) =>
      file.path === "manifest.json"
        ? {
            path: file.path,
            bytes: new TextEncoder().encode(
              JSON.stringify({ ...JSON.parse(new TextDecoder().decode(file.bytes)), ...change }),
            ),
          }
        : file,
    );
    expect(() => syntheticSignedPackage(changed)).toThrow();
  }
});
test("config validation rejects unknown, missing, out of range and secret fields before guest invocation", async () => {
  const { pkg } = await syntheticChatPackage();
  const valid = {
    model_map: { logical: "upstream" },
    auth: "bearer",
    max_input_tokens: 10000,
    max_output_tokens: 100,
  };
  expect(validateConfig(pkg.configSchema, valid)).toEqual(valid);
  for (const config of [
    { ...valid, api_key: "synthetic-secret" },
    { ...valid, max_output_tokens: 0 },
    { ...valid, max_input_tokens: 1.5 },
    { ...valid, model_map: { logical: 2 } },
    { ...valid, auth: "sign-anything" },
    {},
  ])
    expect(() => validateConfig(pkg.configSchema, config)).toThrow();
});
test("prepared output cannot choose an origin, traverse paths or inject credentials and headers", () => {
  const request = {
    method: "POST",
    path: "/v1/chat/completions",
    headers: { "content-type": "application/json" },
    body: { document: crypto.randomUUID(), edits: [] },
    auth: { type: "bearer" },
    manifest: {
      operation: "chat",
      model: "logical",
      upstream_model: "upstream",
      stream: false,
      ceilings: { output_tokens: "10" },
      input_bytes: 2,
    },
  };
  expect(preparedSchema.safeParse(request).success).toBe(true);
  for (const path of [
    "https://synthetic/v1",
    "//synthetic/v1",
    "/a/../b",
    "/a/%2e%2e/b",
    "/a/%2F/b",
    "/a\\b",
  ])
    expect(preparedSchema.safeParse({ ...request, path }).success).toBe(false);
  for (const headers of [
    { authorization: "synthetic" },
    { cookie: "synthetic" },
    { "x-safe": "x\r\ny" },
    { host: "synthetic" },
  ])
    expect(preparedSchema.safeParse({ ...request, headers }).success).toBe(false);
  expect(
    preparedSchema.safeParse({ ...request, auth: { type: "bearer", secret: "synthetic" } }).success,
  ).toBe(false);
});
test("duplicate JSON keys cannot give a signed manifest or outer envelope ambiguous meaning", async () => {
  const signed = await syntheticChatPackage(),
    source = new TextDecoder().decode(signed.bytes);
  expect(() =>
    verifyPackage(
      new TextEncoder().encode(source.replace('"format":1', '"format":1,"format":1')),
      signed.trust,
    ),
  ).toThrow("invalid_json");
  const files = await openAIChatPackageFiles();
  const ambiguous = files.map((file) =>
    file.path === "manifest.json"
      ? {
          path: file.path,
          bytes: new TextEncoder().encode(
            new TextDecoder().decode(file.bytes).replace('"format": 1', '"format": 1,"format": 1'),
          ),
        }
      : file,
  );
  expect(() => syntheticSignedPackage(ambiguous)).toThrow("invalid_json");
});
