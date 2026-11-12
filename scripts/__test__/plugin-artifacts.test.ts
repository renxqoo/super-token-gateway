import { expect, test } from "bun:test";
test("checked-in WASM cannot silently drift from its TypeScript SDK, adapter and pinned builder", async () => {
  const hashes = (await Bun.file(
    "packages/adapters/openai-chat/package/source-hashes.json",
  ).json()) as Record<string, { bytes: number; sha256: string }>;
  for (const path of [
    "packages/plugin-sdk/src/guest.ts",
    "packages/adapters/openai-chat/plugin.ts",
    "packages/adapters/openai-chat/package/module.wasm",
    "scripts/build-plugins.ts",
  ])
    expect(hashes[path]).toBeDefined();
  for (const [path, expected] of Object.entries(hashes)) {
    const bytes = await Bun.file(path).bytes();
    expect(bytes.length).toBe(expected.bytes);
    expect(new Bun.CryptoHasher("sha256").update(bytes).digest("hex")).toBe(expected.sha256);
  }
  const fixture = (await Bun.file("tests/support/plugins/provenance.json").json()) as {
    files: Record<string, { bytes: number; sha256: string }>;
    sdk: { path: string; bytes: number; sha256: string };
  };
  for (const [path, expected] of Object.entries(fixture.files)) {
    const bytes = await Bun.file(`tests/support/plugins/${path}`).bytes();
    expect(bytes.length).toBe(expected.bytes);
    expect(new Bun.CryptoHasher("sha256").update(bytes).digest("hex")).toBe(expected.sha256);
  }
  const sdk = await Bun.file(fixture.sdk.path).bytes();
  expect(sdk.length).toBe(fixture.sdk.bytes);
  expect(new Bun.CryptoHasher("sha256").update(sdk).digest("hex")).toBe(fixture.sdk.sha256);
});
