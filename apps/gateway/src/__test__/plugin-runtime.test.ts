import { expect, test } from "bun:test";
import { generateKeyPairSync } from "node:crypto";
import { resolve } from "node:path";
import {
  openAIChatReleaseId,
  packageTrust,
  releasePolicySchema,
  type VerifiedPackage,
} from "@gateway/plugin-sdk";
import { openAIChatPackageFiles } from "@gateway/adapters";
import { resolvePluginExecutable } from "@gateway/plugin-host";
import {
  syntheticChatPackage,
  syntheticSignedPackage,
} from "../../../../tests/support/plugin-package.ts";
import { createGatewayPluginRuntime } from "../plugin-runtime.ts";

async function setup(files?: Awaited<ReturnType<typeof openAIChatPackageFiles>>) {
  const keys = generateKeyPairSync("ed25519");
  const signed = files ? syntheticSignedPackage(files, keys) : await syntheticChatPackage(keys);
  const trust = packageTrust(
    releasePolicySchema.parse({
      format: 1,
      expires_at: "2099-01-01T00:00:00.000Z",
      revoked: false,
      keys: [
        {
          id: "synthetic-test-only",
          public_key: keys.publicKey.export({ format: "pem", type: "spki" }).toString(),
          not_before: "2020-01-01T00:00:00.000Z",
          expires_at: "2099-01-01T00:00:00.000Z",
          revoked: false,
          purposes: ["package"],
          release_ids: [openAIChatReleaseId],
        },
      ],
      releases: [
        {
          release_id: openAIChatReleaseId,
          descriptor_digest: "b".repeat(64),
          package_digest: signed.pkg.digest,
          abi_version: 1,
          config_schema_digest: "c".repeat(64),
          fixture_digest: "d".repeat(64),
          validation_policy_digest: "e".repeat(64),
          runtime_digest: "f".repeat(64),
          revoked: false,
        },
      ],
    }),
  );
  const registry = await Bun.file(".local/builtin-adapters/registry.json").json();
  const worker = {
    version: 1 as const,
    kind: "builtin-worker" as const,
    artifact_digest: registry.artifact_digest as string,
  };
  const wasm = {
    version: 1 as const,
    kind: "wasm" as const,
    artifact_digest: resolvePluginExecutable(resolve(".local/plugin-host")).digest,
  };
  const host = await createGatewayPluginRuntime({
    executable: resolve(".local/plugin-host"),
    trust,
    maxAttempts: 4,
  });
  return { host, pkg: signed.pkg, worker, wasm };
}

const options = () => ({
  tenant: "runtime-tenant",
  attempt: crypto.randomUUID(),
  incarnation: crypto.randomUUID(),
  deadlineAt: Date.now() + 10000,
  config: {
    model_map: { logical: "upstream" },
    max_output_tokens: 100,
    max_input_tokens: 10000,
    auth: "bearer",
  },
});

test("only a verified platform package matching both local build and Core approval uses the Worker", async () => {
  const { host, pkg, worker, wasm } = await setup();
  try {
    const bound = host.bind(pkg, options(), [wasm, worker]);
    expect(bound.execution).toEqual(worker);
    expect(Object.isFrozen(bound.execution)).toBe(true);
    expect(await bound.attempt.invoke("describe")).toMatchObject({ kind: "description" });
    expect(host.snapshot().worker.started).toBeGreaterThan(0);
    expect(host.snapshot().wasm.starts).toBe(0);
    bound.attempt.dispose();
  } finally {
    await host.close();
  }
  expect(host.snapshot()).toMatchObject({
    attempts: 0,
    worker: { workers: 0, pending: 0, timers: 0 },
    wasm: { dead: true },
  });
});

test("missing or unrelated Core approval rejects before any attempt or runtime starts", async () => {
  const { host, pkg, worker } = await setup();
  try {
    for (const allowed of [[], [{ ...worker, artifact_digest: "0".repeat(64) }]])
      expect(() => host.bind(pkg, options(), allowed)).toThrow("EXECUTION_UNAVAILABLE");
    expect(host.snapshot()).toMatchObject({
      attempts: 0,
      worker: { started: 0 },
      wasm: { starts: 0 },
    });
  } finally {
    await host.close();
  }
});

test("a private verification made with an unrelated public key cannot impersonate the platform signer", async () => {
  const { host, pkg, worker } = await setup();
  try {
    const stranger = syntheticSignedPackage([...pkg.files], generateKeyPairSync("ed25519")).pkg;
    expect(() => host.bind(stranger, options(), [worker])).toThrow("EXECUTION_UNAVAILABLE");
    expect(host.snapshot().worker.started).toBe(0);
  } finally {
    await host.close();
  }
});

test("an approved Wasm attempt stays Wasm and the resource default never selects a Worker", async () => {
  const { host, pkg, wasm } = await setup();
  try {
    const bound = host.bind(pkg, options(), [wasm]);
    expect(bound.execution).toEqual(wasm);
    expect(await bound.attempt.invoke("describe")).toMatchObject({ kind: "description" });
    const resource = host.create(pkg, options());
    expect(await resource.invoke("describe")).toMatchObject({ kind: "description" });
    expect(host.snapshot().worker.started).toBe(0);
    expect(host.snapshot().wasm.starts).toBe(1);
    bound.attempt.dispose();
    resource.dispose();
  } finally {
    await host.close();
  }
});

test("public package mutation cannot select a different execution identity and forged objects cannot bind", async () => {
  const { host, pkg, worker } = await setup();
  try {
    const forged = { ...pkg } as VerifiedPackage;
    pkg.digest = "0".repeat(64);
    pkg.moduleHash = "1".repeat(64);
    pkg.manifest.id = "acme.forged";
    pkg.module.fill(0);
    const bound = host.bind(pkg, options(), [worker]);
    expect(bound.execution).toEqual(worker);
    expect(await bound.attempt.invoke("describe")).toMatchObject({ kind: "description" });
    bound.attempt.dispose();
    expect(() => host.bind(forged, options(), [worker])).toThrow("PACKAGE_NOT_VERIFIED");
  } finally {
    await host.close();
  }
});

test("platform signing alone cannot enable unregistered package bytes", async () => {
  const files = (await openAIChatPackageFiles()).map((file) =>
    file.path === "LICENSE"
      ? { ...file, bytes: Buffer.concat([file.bytes, Buffer.from("\nchanged package\n")]) }
      : file,
  );
  const { host, pkg, worker, wasm } = await setup(files);
  try {
    expect(() => host.bind(pkg, options(), [worker])).toThrow("EXECUTION_UNAVAILABLE");
    const bound = host.bind(pkg, options(), [worker, wasm]);
    expect(bound.execution).toEqual(wasm);
    bound.attempt.dispose();
  } finally {
    await host.close();
  }
});

test("batch siblings preserve the parent's captured configuration and executor and cannot outlive disposal", async () => {
  const { host, pkg, worker } = await setup();
  try {
    const input = options();
    const bound = host.bind(pkg, input, [worker]);
    input.config.model_map.logical = "mutable-canary";
    const sibling = bound.createSibling(crypto.randomUUID());
    const prepared = await sibling.transformJSON(
      "prepare",
      { protocol: "chat", operation: "chat", model: "logical" },
      new TextEncoder().encode(
        '{"model":"logical","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":2}',
      ),
    );
    expect(JSON.parse(new TextDecoder().decode(prepared.body)).model).toBe("upstream");
    expect(host.snapshot()).toMatchObject({ attempts: 2, wasm: { starts: 0 } });
    bound.attempt.dispose();
    expect(sibling.snapshot().disposed).toBe(true);
    expect(() => bound.createSibling(crypto.randomUUID())).toThrow("ATTEMPT_CLOSED");
    expect(host.snapshot().attempts).toBe(0);
  } finally {
    await host.close();
  }
});

test("automatic primary cancellation closes every existing batch sibling before it can execute", async () => {
  const { host, pkg, worker } = await setup();
  try {
    const bound = host.bind(pkg, options(), [worker]);
    const sibling = bound.createSibling(crypto.randomUUID());
    const cancelled = new AbortController();
    cancelled.abort();
    const error = await bound.attempt
      .invoke("describe", undefined, cancelled.signal)
      .catch((failure: unknown) => failure);
    expect(error).toMatchObject({ code: "CALL_CANCELLED" });
    expect(sibling.snapshot().disposed).toBe(true);
    expect(host.snapshot().attempts).toBe(0);
  } finally {
    await host.close();
  }
});
