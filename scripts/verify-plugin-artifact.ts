import assert from "node:assert/strict";
import { generateKeyPairSync, sign } from "node:crypto";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
const dist = new URL("../dist/", import.meta.url),
  isolated = await mkdtemp(join(tmpdir(), "gateway-plugin-artifact-"));
try {
  for (const path of ["native", "plugin-host", "adapters", "openai-chat"])
    await cp(new URL(path, dist), join(isolated, path), { recursive: true });
  const built = (await import(
    pathToFileURL(join(isolated, "plugin-host/index.js")).href
  )) as typeof import("../packages/plugin-host/src/index.ts");
  const adapters = (await import(
    pathToFileURL(join(isolated, "adapters/index.js")).href
  )) as typeof import("../packages/adapters/src/index.ts");
  const files = await adapters.openAIChatPackageFiles(),
    keys = generateKeyPairSync("ed25519"),
    digest = built.packageDigest(files);
  const envelope = {
    format: 1,
    files: files.map((file) => ({
      path: file.path,
      data: Buffer.from(file.bytes).toString("base64"),
    })),
    signature: {
      algorithm: "Ed25519",
      key_id: "synthetic-artifact-only",
      value: sign(null, Buffer.from(digest, "hex"), keys.privateKey).toString("base64"),
    },
  };
  const pkg = built.verifyPackage(
    new TextEncoder().encode(JSON.stringify(envelope)),
    new Map([["synthetic-artifact-only", keys.publicKey]]),
  );
  const supervisor = built.createPluginSupervisor({
    executable: join(isolated, "native/plugin-host"),
  });
  let owned: number[] = [];
  try {
    const result = await built.runPluginFixtures(supervisor, pkg),
      stats = await supervisor.stats();
    const suite = built.fixtureSuiteSchema.parse(
      JSON.parse(
        new TextDecoder().decode(files.find((file) => file.path === "fixtures/cases.json")!.bytes),
      ),
    );
    assert.equal(result.cases, suite.cases.length);
    assert.equal(
      result.steps,
      suite.cases.reduce((n, c) => n + c.steps.length, 0),
    );
    assert.equal(result.state.json.bytes, 0);
    assert.equal(result.state.stateBytes, 0);
    owned = [supervisor.snapshot().pid!, ...(stats!.workers as number[])];
    console.log(
      JSON.stringify({
        artifact: "isolated-plugin",
        cases: result.cases,
        steps: result.steps,
        digest,
        pending: stats!.pending,
        diagnostics: supervisor.snapshot().diagnostics,
      }),
    );
  } catch (error) {
    console.error(
      JSON.stringify({
        artifact: "isolated-plugin",
        error: error instanceof built.PluginHostError ? error.code : "ARTIFACT_FAILED",
        diagnostics: supervisor.snapshot().diagnostics,
      }),
    );
    throw error;
  } finally {
    await supervisor.close();
    assert.equal(supervisor.snapshot().timers, 0);
    assert.equal(supervisor.snapshot().inflight, 0);
    assert.equal(supervisor.snapshot().pending, 0);
    assert.equal(supervisor.snapshot().waiters, 0);
  }
  for (const pid of owned) assert.throws(() => process.kill(pid, 0));
} finally {
  await rm(isolated, { recursive: true, force: true });
}
