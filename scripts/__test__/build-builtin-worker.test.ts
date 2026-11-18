import { expect, spyOn, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { buildBuiltinWorker, builtinProofInputPath } from "../build-builtin-worker.ts";

const digest = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");

test("a changed Wasm declaration invalidates the portable proof before any runtime asset is published", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-builtin-stale-test-"));
  const changed = resolve(directory, "changed.d.ts");
  await Bun.write(changed, "export function changed(): void;");
  const original = Bun.file;
  const file = spyOn(Bun, "file").mockImplementation((path: unknown, options?: BlobPropertyBag) => {
    const candidate =
      typeof path === "string"
        ? resolve(path)
        : path instanceof URL
          ? fileURLToPath(path)
          : undefined;
    return Reflect.apply(original, Bun, [
      candidate === resolve("packages/adapters/openai-chat/plugin.d.ts") ? changed : path,
      options,
    ]) as ReturnType<typeof Bun.file>;
  });
  try {
    const error = await buildBuiltinWorker(directory).catch((failure: unknown) => failure);
    expect(error).toMatchObject({
      message: "BUILTIN_PROOF_STALE:packages/adapters/openai-chat/plugin.d.ts",
    });
    expect(await Bun.file(resolve(directory, "worker.js")).exists()).toBe(false);
  } finally {
    file.mockRestore();
    await rm(directory, { recursive: true, force: true });
  }
});

test("builtin builds are reproducible and bind the complete dependency graph, existing Wasm and actual engine", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-builtin-build-test-"));
  try {
    const first = await buildBuiltinWorker(directory);
    const code = await readFile(resolve(directory, "worker.js"));
    expect(first.worker_digest).toBe(digest(code));
    expect(first.runtime_digest).toBe(digest(await readFile(process.execPath)));
    expect(first.artifact_digest).toBe(
      digest(
        Buffer.from(
          JSON.stringify([
            "stg.internal.builtin-worker-artifact",
            "1",
            first.worker_digest,
            first.runtime_digest,
            "1",
            "1",
          ]),
        ),
      ),
    );
    expect(
      Object.keys(first.sources).some((path) =>
        path.endsWith("lossless-json/lib/esm/LosslessNumber.js"),
      ),
    ).toBe(true);
    for (const [path, expected] of Object.entries(first.sources))
      if (path !== "$generated-entry")
        expect(digest(await readFile(builtinProofInputPath(path)))).toBe(expected);
    for (const record of first.records) {
      const name = record.release_id.slice("gateway.".length).replace(/-\d+\.\d+\.\d+$/, "");
      const wasm = digest(await readFile(`packages/adapters/${name}/package/module.wasm`));
      expect(Object.hasOwn(record.sources, `packages/adapters/${name}/plugin.d.ts`)).toBe(true);
      expect(record.module_hash).toBe(wasm);
      expect(record.verification_runs).toEqual([wasm, wasm, wasm]);
    }
    const second = await buildBuiltinWorker(directory);
    expect(await readFile(resolve(directory, "worker.js"))).toEqual(code);
    expect(second).toEqual(first);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}, 60000);
