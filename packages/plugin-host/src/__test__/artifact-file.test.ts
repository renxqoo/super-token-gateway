import { expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtemp, open, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { readPluginArtifact } from "../artifact-file.ts";

test("artifact reads preserve exact bytes at the admitted size and reject sparse oversized files and directories", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-artifact-budget-"));
  try {
    const path = resolve(directory, "asset");
    const bytes = new Uint8Array([0, 1, 128, 255]);
    await writeFile(path, bytes);
    expect(await readPluginArtifact(path, 4)).toEqual(bytes);
    const file = await open(path, "r+");
    try {
      await file.truncate(1 << 30);
    } finally {
      await file.close();
    }
    expect(await readPluginArtifact(path, 1024).catch((error: unknown) => error)).toMatchObject({
      message: "PLUGIN_ARTIFACT_BUDGET",
    });
    expect(
      await readPluginArtifact(directory, 1024).catch((error: unknown) => error),
    ).toMatchObject({ message: "PLUGIN_ARTIFACT_BUDGET" });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("pre-cancelled artifact reads stop before opening a missing file and invalid budgets cannot allocate", async () => {
  const controller = new AbortController();
  const reason = new Error("OWNED_ARTIFACT_CANCELLED");
  controller.abort(reason);
  expect(
    await readPluginArtifact("/missing-owned-test-artifact", 1024, controller.signal).catch(
      (error: unknown) => error,
    ),
  ).toBe(reason);
  for (const limit of [0, -1, 0.5, Number.NaN, (256 << 20) + 1])
    expect(
      await readPluginArtifact("/missing-owned-test-artifact", limit).catch(
        (error: unknown) => error,
      ),
    ).toMatchObject({ message: "PLUGIN_ARTIFACT_BUDGET_INVALID" });
});

test("a FIFO without a writer is rejected without waiting for another process to open it", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-artifact-fifo-"));
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const path = resolve(directory, "asset");
    execFileSync("mkfifo", [path]);
    const pending = readPluginArtifact(path, 1024).catch((error: unknown) => error);
    const blocked = new Promise<string>((done) => {
      timer = setTimeout(() => done("OWNED_FIFO_BLOCKED"), 200);
    });
    const result = await Promise.race([pending, blocked]);
    if (result === "OWNED_FIFO_BLOCKED") {
      const writer = await open(path, "w");
      await writer.close();
      await pending;
    }
    expect(result).toMatchObject({ message: "PLUGIN_ARTIFACT_BUDGET" });
  } finally {
    clearTimeout(timer);
    await rm(directory, { recursive: true, force: true });
  }
});
