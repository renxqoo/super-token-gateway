import { expect, test } from "bun:test";
import {
  ownPluginModule,
  pluginModuleInfo,
  pluginModuleBase64,
  verifiedPackageRuntime,
  verifiedPackageSnapshot,
  type PluginModule,
} from "../index.ts";
import { syntheticChatPackage } from "../../../../tests/support/plugin-package.ts";

test("owned module bytes cannot be replaced by mutations to a caller's Buffer or descriptor", () => {
  const bytes = Buffer.from([0, 97, 115, 109, 1, 0, 0, 0]);
  const module = ownPluginModule(bytes);
  bytes.fill(255);
  expect([...Buffer.from(pluginModuleBase64(module), "base64")]).toEqual([
    0, 97, 115, 109, 1, 0, 0, 0,
  ]);
  const info = pluginModuleInfo(module);
  expect(() => Object.assign(info, { hash: "f".repeat(64), byteLength: 0 })).toThrow();
  expect(pluginModuleInfo(module).byteLength).toBe(8);
  expect(() => pluginModuleBase64({ ...module } as PluginModule)).toThrow("MODULE_NOT_OWNED");
  expect(() => pluginModuleInfo({} as PluginModule)).toThrow("MODULE_NOT_OWNED");
});

test("execution metadata and module retain the signed package after public views and snapshots are changed", async () => {
  const signed = await syntheticChatPackage();
  const expectedModule = signed.envelope.files.find((file) => file.path === "module.wasm")!.data;
  const runtime = verifiedPackageRuntime(signed.pkg);
  const snapshot = verifiedPackageSnapshot(signed.pkg);
  signed.pkg.module.fill(0);
  snapshot.module.fill(0);
  signed.pkg.digest = "f".repeat(64);
  signed.pkg.moduleHash = "e".repeat(64);
  signed.pkg.manifest.entrypoint = "replacement";
  signed.pkg.configSchema.required.length = 0;
  expect(pluginModuleBase64(runtime.module)).toBe(expectedModule);
  expect(runtime.manifest.entrypoint).toBe("invoke");
  expect(runtime.configSchema.required).toContain("model_map");
  expect(() => {
    runtime.manifest.capabilities.operations.length = 0;
  }).toThrow();
  expect(() => {
    runtime.configSchema.required.length = 0;
  }).toThrow();
  expect(() => verifiedPackageRuntime({ ...signed.pkg })).toThrow("PACKAGE_NOT_VERIFIED");
});
