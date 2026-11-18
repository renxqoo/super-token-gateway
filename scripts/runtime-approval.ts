import type { SQL } from "bun";
import { resolve } from "node:path";
import { z } from "zod";
import { canonical } from "@gateway/kernel";
import { executionRuntimeV1Schema } from "@gateway/contracts";
import {
  packageTrust,
  parseBuiltinWorkerArtifact,
  pluginModuleInfo,
  releasePolicySchema,
  sha256,
  verifyPackage,
  verifiedPackageRuntime,
  verifiedPackageSnapshot,
} from "@gateway/plugin-sdk";
import {
  createBuiltinWorkerExecutor,
  resolvePluginExecutable,
  runPluginFixtures,
  readPluginArtifact,
} from "@gateway/plugin-host";
import { validateCatalogPackage } from "../apps/core-api/src/plugins/validator.ts";
import { validationPolicy } from "../apps/core-api/src/plugins/releases.ts";
import {
  DatabaseCommitUnknown,
  DatabaseDeadlineExceeded,
  runDatabaseQuery,
  withDatabaseTransaction,
  withDatabaseConnection,
} from "@gateway/storage";

export class RuntimeApprovalOutcomeUnknown extends Error {
  constructor(cause: unknown) {
    super("RUNTIME_APPROVAL_OUTCOME_UNKNOWN", { cause });
  }
}

const inputSchema = z.strictObject({
  packageDigest: z.string().regex(/^[a-f0-9]{64}$/),
  policyFile: z.string().min(1),
  helperExecutable: z.string().min(1),
  kind: z.enum(["wasm", "builtin-worker"]),
  builtinDirectory: z.string().min(1).optional(),
  actor: z.string().min(1).max(100),
  reason: z.string().min(1).max(1000),
});

export async function approvePluginRuntime(
  db: SQL,
  input: z.infer<typeof inputSchema>,
  limits: { signal?: AbortSignal; wallMs?: number } = {},
) {
  const wallMs = limits.wallMs ?? validationPolicy.wall_ms;
  if (!Number.isSafeInteger(wallMs) || wallMs < 1 || wallMs > validationPolicy.wall_ms)
    throw new Error("RUNTIME_APPROVAL_BUDGET_INVALID");
  const controller = new AbortController();
  const deadlineAt = Date.now() + wallMs;
  const until = performance.now() + wallMs;
  const cancel = () => controller.abort(limits.signal?.reason);
  const timer = setTimeout(() => controller.abort(new Error("RUNTIME_APPROVAL_DEADLINE")), wallMs);
  limits.signal?.addEventListener("abort", cancel, { once: true });
  try {
    if (limits.signal?.aborted) cancel();
    return await approveWithinDeadline(db, input, controller.signal, deadlineAt, until);
  } catch (error) {
    if (error instanceof DatabaseCommitUnknown) throw new RuntimeApprovalOutcomeUnknown(error);
    if (error instanceof RuntimeApprovalOutcomeUnknown) throw error;
    controller.signal.throwIfAborted();
    if (
      error instanceof DatabaseDeadlineExceeded ||
      (error as { errno?: string } | null)?.errno === "25P04"
    )
      throw new Error("RUNTIME_APPROVAL_DEADLINE", { cause: error });
    throw error;
  } finally {
    clearTimeout(timer);
    limits.signal?.removeEventListener("abort", cancel);
  }
}

async function approveWithinDeadline(
  db: SQL,
  input: z.infer<typeof inputSchema>,
  signal: AbortSignal,
  deadlineAt: number,
  until: number,
) {
  const options = inputSchema.parse(input);
  signal?.throwIfAborted();
  const policyBytes = await readPluginArtifact(options.policyFile, 1 << 20, signal);
  const policy = releasePolicySchema.parse(
    JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(policyBytes)),
  );
  const pinned = resolvePluginExecutable(options.helperExecutable);
  const [stored] = await withDatabaseConnection(
    db,
    { signal, deadline: until },
    (connection, querySignal) =>
      runDatabaseQuery(
        connection`SELECT package_bytes,manifest,signing_key_id FROM public.plugin_packages WHERE digest=${options.packageDigest}`,
        querySignal,
      ),
  );
  if (!stored) throw new Error("RUNTIME_PACKAGE_MISSING");
  const envelope = new Uint8Array(stored.package_bytes as Buffer);
  const trust = packageTrust(policy);
  const pkg = verifyPackage(envelope, trust(options.packageDigest));
  const original = verifiedPackageRuntime(pkg);
  if (
    original.digest !== options.packageDigest ||
    !trust.admits(pkg) ||
    original.signingKeyId !== stored.signing_key_id ||
    canonical(original.manifest) !== canonical(stored.manifest)
  )
    throw new Error("RUNTIME_PACKAGE_MISMATCH");
  const files = verifiedPackageSnapshot(pkg).files;
  const fileDigest = (path: string) => sha256(files.find((file) => file.path === path)!.bytes);
  let worker: ReturnType<typeof createBuiltinWorkerExecutor> | undefined;
  let native:
    | {
        worker_digest: string;
        runtime_digest: string;
        artifact_digest: string;
        registry_digest: string;
      }
    | undefined;
  try {
    if (options.kind === "builtin-worker") {
      if (!options.builtinDirectory || !trust.isPlatform(pkg))
        throw new Error("RUNTIME_PLATFORM_REQUIRED");
      const registryBytes = await readPluginArtifact(
        resolve(options.builtinDirectory, "registry.json"),
        4 << 20,
        signal,
      );
      const code = await readPluginArtifact(
        resolve(options.builtinDirectory, "worker.js"),
        16 << 20,
        signal,
      );
      const registry = parseBuiltinWorkerArtifact(
        JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(registryBytes)),
        code,
        sha256(await readPluginArtifact(process.execPath, 256 << 20, signal)),
      );
      const record = registry.records.find((item) => item.package_digest === original.digest);
      if (
        !record ||
        record.release_id !== `${original.manifest.id}-${original.manifest.version}` ||
        record.module_hash !== pluginModuleInfo(original.module).hash ||
        record.manifest_digest !== fileDigest("manifest.json") ||
        record.config_schema_digest !== fileDigest("config.schema.json")
      )
        throw new Error("RUNTIME_REGISTRY_MISMATCH");
      native = {
        worker_digest: registry.worker_digest,
        runtime_digest: registry.runtime_digest,
        artifact_digest: registry.artifact_digest,
        registry_digest: sha256(registryBytes),
      };
      worker = createBuiltinWorkerExecutor({
        code,
        digest: registry.worker_digest,
        modules: [record.module_hash],
      });
    }
    const validation = await validateCatalogPackage(
      pkg,
      pinned.executable,
      signal ? { signal } : {},
    );
    if (
      validation.error_code ||
      !validation.cleanup_confirmed ||
      validation.cases < 1 ||
      validation.steps < 1
    )
      throw new Error("RUNTIME_WASM_VALIDATION_FAILED");
    if (worker) {
      const abort = () => {
        void worker!.close().catch(() => {});
      };
      signal?.addEventListener("abort", abort, { once: true });
      try {
        signal?.throwIfAborted();
        const actual = await runPluginFixtures(worker, pkg);
        if (actual.cases !== validation.cases || actual.steps !== validation.steps)
          throw new Error("RUNTIME_WORKER_VALIDATION_FAILED");
      } finally {
        signal?.removeEventListener("abort", abort);
        await worker.close();
      }
      const state = worker.snapshot();
      if (state.workers || state.pending || state.timers || state.pendingBytes)
        throw new Error("RUNTIME_WORKER_CLEANUP_FAILED");
    }
    signal?.throwIfAborted();
    const signingKey = [...policy.keys, ...policy.publishers].find(
      (key) => key.id === original.signingKeyId,
    )!;
    const facts = {
      version: 1,
      module_digest: pluginModuleInfo(original.module).hash,
      manifest_digest: fileDigest("manifest.json"),
      config_schema_digest: fileDigest("config.schema.json"),
      fixture_digest: fileDigest("fixtures/cases.json"),
      policy_digest: sha256(policyBytes),
      signer_digest: original.signingKeyDigest,
      runtime_digest: native?.runtime_digest ?? pinned.digest,
      ...(native
        ? { worker_digest: native.worker_digest, registry_digest: native.registry_digest }
        : {}),
      verified_at: new Date().toISOString(),
      approval_deadline: new Date(deadlineAt).toISOString(),
      trust_until: new Date(
        Math.min(Date.parse(policy.expires_at), Date.parse(signingKey.expires_at)),
      ).toISOString(),
      validation: { cases: validation.cases, steps: validation.steps, cleanup_confirmed: true },
    };
    const runtime = executionRuntimeV1Schema.parse({
      version: 1,
      kind: options.kind,
      artifact_digest: native?.artifact_digest ?? pinned.digest,
    });
    await withDatabaseTransaction(
      db,
      { signal, deadline: until },
      async (tx, transactionSignal) => {
        const current = packageTrust(policy);
        const rechecked = verifyPackage(envelope, current(options.packageDigest));
        if (!current.admits(rechecked) || (native && !current.isPlatform(rechecked)))
          throw new Error("RUNTIME_TRUST_EXPIRED");
        transactionSignal.throwIfAborted();
        const [row] = await runDatabaseQuery(
          tx`SELECT package_bytes,manifest,signing_key_id FROM public.plugin_packages WHERE digest=${options.packageDigest} FOR SHARE`,
          transactionSignal,
        );
        if (
          !row ||
          !Buffer.from(row.package_bytes as Buffer).equals(envelope) ||
          canonical(row.manifest) !== canonical(stored.manifest) ||
          row.signing_key_id !== stored.signing_key_id
        )
          throw new Error("RUNTIME_PACKAGE_CHANGED");
        const [time] = await runDatabaseQuery(tx`SELECT clock_timestamp() now`, transactionSignal);
        const at = new Date(time.now).getTime();
        const keys = [...policy.keys, ...policy.publishers].find(
          (key) => key.id === original.signingKeyId,
        );
        if (
          !keys ||
          keys.revoked ||
          at < Date.parse(keys.not_before) ||
          at >= Math.min(Date.parse(policy.expires_at), Date.parse(keys.expires_at))
        )
          throw new Error("RUNTIME_TRUST_EXPIRED");
        await runDatabaseQuery(
          tx`SELECT core_execution_runtime.approve_v1(${options.packageDigest},${runtime}::jsonb,${facts}::jsonb,true,${options.actor},${options.reason})`,
          transactionSignal,
        );
        transactionSignal.throwIfAborted();
      },
    );
    return {
      package_digest: original.digest,
      execution: runtime,
      validation: facts.validation,
      verified_at: facts.verified_at,
    };
  } finally {
    await worker?.close();
  }
}
