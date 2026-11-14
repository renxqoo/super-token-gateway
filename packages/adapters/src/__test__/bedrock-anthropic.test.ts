import { afterAll, expect, test } from "bun:test";
import { generateKeyPairSync } from "node:crypto";
import { createPluginSupervisor, runPluginFixtures } from "@gateway/plugin-host";
import { syntheticSignedPackage } from "../../../../tests/support/plugin-package.ts";
import { bedrockAnthropicPackageFiles } from "../index.ts";

const supervisor = createPluginSupervisor({
  executable: new URL("../../../../.local/plugin-host", import.meta.url).pathname,
  native: { slots: 2 },
});
afterAll(async () => {
  await supervisor.close();
});

test("package synthetic fixtures execute through the production WASM ABI with the event stream codec", async () => {
  const signed = syntheticSignedPackage(
    await bedrockAnthropicPackageFiles(),
    generateKeyPairSync("ed25519"),
  );
  const result = await runPluginFixtures(supervisor, signed.pkg);
  expect(result.cases).toBe(9);
  expect(result.steps).toBe(16);
});
