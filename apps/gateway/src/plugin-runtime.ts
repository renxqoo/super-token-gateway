import { dirname, resolve } from "node:path";
import { executionRuntimeV1Schema, type ExecutionRuntimeV1 } from "@gateway/contracts";
import {
  parseBuiltinWorkerArtifact,
  pluginModuleInfo,
  validateConfig,
  verifiedPackageRuntime,
  type VerifiedPackage,
} from "@gateway/plugin-sdk";
import {
  createBuiltinWorkerExecutor,
  createPluginAttempts,
  createPluginSupervisor,
  resolvePluginExecutable,
  readPluginArtifact,
  PluginHostError,
  type AttemptOptions,
} from "@gateway/plugin-host";
import type { PackageTrust } from "./packages.ts";

export async function createGatewayPluginRuntime(options: {
  executable: string;
  trust: PackageTrust;
  directory?: string;
  maxAttempts?: number;
}) {
  if (Bun.version !== "1.4.4") throw new Error("BUILTIN_RUNTIME_VERSION");
  const pinned = resolvePluginExecutable(options.executable);
  const directory = options.directory ?? resolve(dirname(options.executable), "builtin-adapters");
  const registryBytes = await readPluginArtifact(resolve(directory, "registry.json"), 4 << 20);
  const code = await readPluginArtifact(resolve(directory, "worker.js"), 16 << 20);
  const registry = parseBuiltinWorkerArtifact(
    JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(registryBytes)),
    code,
    new Bun.CryptoHasher("sha256")
      .update(await readPluginArtifact(process.execPath, 256 << 20))
      .digest("hex"),
  );
  const records = new Map(registry.records.map((record) => [record.package_digest, record]));
  if (records.size !== registry.records.length) throw new Error("BUILTIN_ARTIFACT_INVALID");
  const maxAttempts = options.maxAttempts ?? 128;
  const wasm = createPluginSupervisor({
    executable: pinned.executable,
    expectedExecutableDigest: pinned.digest,
    maxPending: maxAttempts,
  });
  const worker = createBuiltinWorkerExecutor({
    code,
    digest: registry.worker_digest,
    modules: registry.records.map((record) => record.module_hash),
    maxPending: maxAttempts,
  });
  let host: ReturnType<typeof createPluginAttempts>;
  try {
    host = createPluginAttempts(wasm, {
      maxAttempts,
      maxStateBytes: maxAttempts * (256 << 10),
      additionalExecutors: [worker],
    });
  } catch (error) {
    await Promise.allSettled([worker.close(), wasm.close()]);
    throw error;
  }
  const wasmRuntime: ExecutionRuntimeV1 = Object.freeze({
    version: 1,
    kind: "wasm",
    artifact_digest: pinned.digest,
  });
  const workerRuntime: ExecutionRuntimeV1 = Object.freeze({
    version: 1,
    kind: "builtin-worker",
    artifact_digest: registry.artifact_digest,
  });
  function bind(
    pkg: VerifiedPackage,
    input: AttemptOptions,
    allowed: readonly ExecutionRuntimeV1[],
  ) {
    if (allowed.length > 17) throw new PluginHostError("CONFIG_INVALID");
    const runtimes = allowed.map((item) => executionRuntimeV1Schema.parse(item));
    const permits = (expected: ExecutionRuntimeV1) =>
      runtimes.some(
        (item) => item.kind === expected.kind && item.artifact_digest === expected.artifact_digest,
      );
    const original = verifiedPackageRuntime(pkg);
    const record = records.get(original.digest);
    const eligible =
      record !== undefined &&
      record.module_hash === pluginModuleInfo(original.module).hash &&
      record.release_id === `${original.manifest.id}-${original.manifest.version}` &&
      options.trust.isPlatform(pkg);
    const execution = eligible && permits(workerRuntime) ? workerRuntime : wasmRuntime;
    if (!permits(execution)) throw new PluginHostError("EXECUTION_UNAVAILABLE");
    const executor = execution.kind === "builtin-worker" ? worker : wasm;
    const captured = { ...input, config: validateConfig(original.configSchema, input.config) };
    const primary = host.create(pkg, captured, executor);
    const siblings = new Set<ReturnType<typeof host.create>>();
    primary.onDispose(() => {
      for (const sibling of siblings) sibling.dispose();
      siblings.clear();
    });
    return Object.freeze({
      execution,
      attempt: primary,
      createSibling(attemptId: string) {
        if (primary.snapshot().disposed) throw new PluginHostError("ATTEMPT_CLOSED");
        const sibling = host.create(pkg, { ...captured, attempt: attemptId }, executor);
        siblings.add(sibling);
        sibling.onDispose(() => {
          siblings.delete(sibling);
        });
        return sibling;
      },
    });
  }
  return {
    create: (pkg: VerifiedPackage, input: AttemptOptions) => host.create(pkg, input),
    reservePreparation: host.reservePreparation,
    bind,
    dispose: host.dispose,
    close: host.close,
    snapshot: () => ({ ...host.snapshot(), worker: worker.snapshot(), wasm: wasm.snapshot() }),
  };
}
