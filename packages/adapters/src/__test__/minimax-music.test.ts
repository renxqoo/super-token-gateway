import { afterAll, expect, test } from "bun:test";
import { generateKeyPairSync } from "node:crypto";
import { createPluginSupervisor, runPluginFixtures } from "@gateway/plugin-host";
import { syntheticSignedPackage } from "../../../../tests/support/plugin-package.ts";
import { minimaxMusicPackageFiles } from "../index.ts";

const supervisor = createPluginSupervisor({
  executable: new URL("../../../../.local/plugin-host", import.meta.url).pathname,
  native: { slots: 1 },
});
afterAll(async () => {
  await supervisor.close();
});

test("MiniMax music fixtures execute through the production WASM ABI", async () => {
  const signed = syntheticSignedPackage(
    await minimaxMusicPackageFiles(),
    generateKeyPairSync("ed25519"),
  );
  const result = await runPluginFixtures(supervisor, signed.pkg);
  expect(result.cases).toBe(11);
  expect(result.steps).toBe(11);
});
