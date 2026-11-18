import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { parseBuiltinWorkerArtifact } from "../builtin-artifact.ts";

const code = new TextEncoder().encode("synthetic-worker");
const worker = createHash("sha256").update(code).digest("hex");
const runtime = "a".repeat(64);
const artifact = createHash("sha256")
  .update(JSON.stringify(["stg.internal.builtin-worker-artifact", "1", worker, runtime, "1", "1"]))
  .digest("hex");
const registry = () => ({
  format: 1,
  abi_version: 1,
  state_version: 1,
  runtime: "1.4.4",
  worker_digest: worker,
  runtime_digest: runtime,
  artifact_digest: artifact,
  sources: { "packages/synthetic.ts": "b".repeat(64) },
  compiler_digest: "c".repeat(64),
  binaryen_archive_digest: "d".repeat(64),
  records: [
    {
      release_id: "gateway.synthetic-1.0.0",
      package_digest: "e".repeat(64),
      module_hash: "f".repeat(64),
      manifest_digest: "0".repeat(64),
      config_schema_digest: "1".repeat(64),
      sources: {},
      verification_runs: ["f".repeat(64), "f".repeat(64), "f".repeat(64)],
    },
  ],
});

test("builtin artifact parsing binds actual code and engine into the independently calculated versioned identity", () => {
  expect(parseBuiltinWorkerArtifact(registry(), code, runtime).artifact_digest).toBe(artifact);
  expect(() => parseBuiltinWorkerArtifact(registry(), new Uint8Array([1]), runtime)).toThrow(
    "BUILTIN_ARTIFACT_INVALID",
  );
  expect(() => parseBuiltinWorkerArtifact(registry(), code, "b".repeat(64))).toThrow(
    "BUILTIN_ARTIFACT_INVALID",
  );
  expect(() =>
    parseBuiltinWorkerArtifact({ ...registry(), artifact_digest: "0".repeat(64) }, code, runtime),
  ).toThrow("BUILTIN_ARTIFACT_INVALID");
});

test("builtin artifact parsing rejects unsupported versions, false Wasm proof and source path escapes", () => {
  for (const field of ["format", "abi_version", "state_version"])
    expect(() => parseBuiltinWorkerArtifact({ ...registry(), [field]: 2 }, code, runtime)).toThrow(
      "BUILTIN_ARTIFACT_INVALID",
    );
  const wrong = registry();
  wrong.records[0]!.verification_runs[1] = "0".repeat(64);
  expect(() => parseBuiltinWorkerArtifact(wrong, code, runtime)).toThrow(
    "BUILTIN_ARTIFACT_INVALID",
  );
  for (const path of [
    "/etc/passwd",
    "packages/../../.env.local",
    "$dependency/pkg/../../secret",
    ".env.local",
  ])
    expect(() =>
      parseBuiltinWorkerArtifact(
        { ...registry(), sources: { [path]: "a".repeat(64) } },
        code,
        runtime,
      ),
    ).toThrow("BUILTIN_ARTIFACT_INVALID");
});
