import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { build } from "esbuild";
import { createBuiltinBuildSnapshot } from "../builtin-build-snapshot.ts";

test("Wasm and Worker builds share frozen source bytes and dependency resolution across concurrent repository edits", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-builtin-snapshot-"));
  try {
    await mkdir(resolve(directory, "node_modules/fixture-dependency"), { recursive: true });
    await writeFile(resolve(directory, "entry.ts"), 'export { value } from "fixture-dependency";');
    await writeFile(
      resolve(directory, "node_modules/fixture-dependency/package.json"),
      '{"main":"old.js"}',
    );
    await writeFile(
      resolve(directory, "node_modules/fixture-dependency/old.js"),
      'export const value="original";',
    );
    await writeFile(
      resolve(directory, "node_modules/fixture-dependency/new.js"),
      'export const value="new-target";',
    );
    const snapshot = createBuiltinBuildSnapshot(directory);
    const options = {
      absWorkingDir: directory,
      entryPoints: ["entry.ts"],
      bundle: true,
      format: "esm" as const,
      write: false,
      plugins: [snapshot.plugin],
    };
    const first = await build(options);
    await writeFile(
      resolve(directory, "node_modules/fixture-dependency/old.js"),
      'export const value="changed-original";',
    );
    await writeFile(
      resolve(directory, "node_modules/fixture-dependency/package.json"),
      '{"main":"new.js"}',
    );
    const second = await build(options);
    for (const result of [first, second]) {
      const module = await import(
        `data:text/javascript;base64,${Buffer.from(result.outputFiles![0]!.contents).toString("base64")}`
      );
      expect(module.value).toBe("original");
    }
    const exposed = await snapshot.capture("node_modules/fixture-dependency/old.js");
    exposed.fill(0);
    expect(
      new TextDecoder().decode(await snapshot.capture("node_modules/fixture-dependency/old.js")),
    ).toBe('export const value="original";');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
