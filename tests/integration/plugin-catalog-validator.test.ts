import { expect, test } from "bun:test";
import { appendFile } from "node:fs/promises";
import {
  syntheticAdversarialPackage,
  syntheticChatPackage,
  syntheticSignedPackage,
} from "../support/plugin-package.ts";
import { catalogHelper } from "../support/catalog.ts";
import { validateCatalogPackage } from "../../apps/core-api/src/plugins/validator.ts";
async function negativeFixture(mode: "trap" | "loop" | "wrong-expectation") {
  const original =
    mode === "wrong-expectation"
      ? await syntheticChatPackage()
      : await syntheticAdversarialPackage();
  const suite = {
    format: 1,
    config: {
      model_map: { logical: "synthetic" },
      max_input_tokens: 10000,
      max_output_tokens: 1,
      auth: "bearer",
      ...(mode !== "wrong-expectation" ? { fixture_mode: mode } : {}),
    },
    cases: [
      {
        name: `Synthetic ${mode} fixture`,
        steps: [
          {
            action: "json",
            hook: "prepare",
            metadata: { protocol: "chat", operation: "chat", model: "logical" },
            body: Buffer.from('{"a":{"b":1}}').toString("base64"),
            expected: { abi_version: 1, kind: "disposed" },
          },
        ],
      },
    ],
  };
  return syntheticSignedPackage(
    original.pkg.files.map((f) =>
      f.path === "fixtures/cases.json"
        ? { path: f.path, bytes: new TextEncoder().encode(JSON.stringify(suite)) }
        : f,
    ),
  ).pkg;
}
for (const [mode, expected] of [
  ["trap", "GUEST_TRAP"],
  ["loop", "CALL_TIMEOUT"],
  ["wrong-expectation", "FIXTURE_EXPECTATION"],
] as const)
  test(`real fixture ${mode} fails validation with confirmed process reap`, async () => {
    const pkg = await negativeFixture(mode);
    let pid: number | undefined;
    const result = await validateCatalogPackage(pkg, catalogHelper, {
      onResource: async (resource) => {
        pid = resource.pid;
        await appendFile(
          ".local/T04b1-helper-resources.jsonl",
          JSON.stringify({ event: "started", ...resource }) + "\n",
          { mode: 0o600 },
        );
      },
    });
    expect(result.cleanup_confirmed).toBe(true);
    expect(result.error_code).toBe(expected);
    expect(pid).toBeGreaterThan(1);
    expect(() => process.kill(-pid!, 0)).toThrow();
    await appendFile(
      ".local/T04b1-helper-resources.jsonl",
      JSON.stringify({ event: "reaped", pid }) + "\n",
      { mode: 0o600 },
    );
  }, 30000);
test("wall timeout closes and reaps the actual helper before reporting failed validation", async () => {
  const pkg = await negativeFixture("loop");
  let pid: number | undefined;
  const result = await validateCatalogPackage(pkg, catalogHelper, {
    wallMs: 1,
    onResource: async (resource) => {
      pid = resource.pid;
    },
  });
  expect(result.cleanup_confirmed).toBe(true);
  expect(result.error_code).toBe("VALIDATION_TIMEOUT");
  expect(pid).toBeGreaterThan(1);
  expect(() => process.kill(-pid!, 0)).toThrow();
});
test("active cancellation closes and reaps the actual helper without a success result", async () => {
  const pkg = await negativeFixture("loop"),
    controller = new AbortController();
  let pid: number | undefined;
  const result = await validateCatalogPackage(pkg, catalogHelper, {
    signal: controller.signal,
    onResource: async (resource) => {
      pid = resource.pid;
      controller.abort();
    },
  });
  expect(result.cleanup_confirmed).toBe(true);
  expect(result.error_code).toBe("CALL_CANCELLED");
  expect(pid).toBeGreaterThan(1);
  expect(() => process.kill(-pid!, 0)).toThrow();
});

test("actual helper crash is a failed validation with confirmed process group reap", async () => {
  const signed = await syntheticChatPackage();
  let pid: number | undefined;
  const result = await validateCatalogPackage(signed.pkg, catalogHelper, {
    onResource: async (resource) => {
      pid = resource.pid;
      process.kill(-resource.pid, "SIGKILL");
    },
  });
  expect(result.cleanup_confirmed).toBe(true);
  expect(["HELPER_EXITED", "HELPER_EOF"]).toContain(result.error_code ?? "UNEXPECTED_SUCCESS");
  expect(() => process.kill(-pid!, 0)).toThrow();
});
