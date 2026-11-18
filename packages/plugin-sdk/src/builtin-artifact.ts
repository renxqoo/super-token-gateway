import { z } from "zod";
import { sha256 } from "./abi.ts";

const digest = z.string().regex(/^[a-f0-9]{64}$/);
const sourcePath = z
  .string()
  .max(1000)
  .refine(
    (path) =>
      /^(?:packages\/|scripts\/|package\.json$|tsconfig\.json$|bun\.lock$|\$dependency\/|\$generated-entry$)/.test(
        path,
      ) &&
      !path.includes("\\") &&
      !path.split("/").some((part) => part === ".." || part === "." || !part),
  );
const sources = z.record(sourcePath, digest).refine((value) => Object.keys(value).length <= 4096);
const record = z
  .strictObject({
    release_id: z.string().regex(/^gateway\.[a-z-]+-\d+\.\d+\.\d+$/),
    package_digest: digest,
    module_hash: digest,
    manifest_digest: digest,
    config_schema_digest: digest,
    sources,
    verification_runs: z.tuple([digest, digest, digest]),
  })
  .refine((value) => value.verification_runs.every((run) => run === value.module_hash));
const common = {
  format: z.literal(1),
  abi_version: z.literal(1),
  state_version: z.literal(1),
  runtime: z.literal("1.4.4"),
  worker_digest: digest,
  sources,
  compiler_digest: digest,
  binaryen_archive_digest: digest,
  records: z.array(record).min(1).max(16),
};
export const builtinWorkerRegistrySchema = z.strictObject({
  ...common,
  runtime_digest: digest,
  artifact_digest: digest,
});
export const builtinWorkerProofSchema = z.strictObject({
  ...common,
  worker_base64: z
    .string()
    .min(1)
    .max(24 << 20),
});
export type BuiltinWorkerRegistry = z.infer<typeof builtinWorkerRegistrySchema>;

export function builtinWorkerArtifactDigest(worker: string, runtime: string): string {
  return sha256(
    JSON.stringify(["stg.internal.builtin-worker-artifact", "1", worker, runtime, "1", "1"]),
  );
}

export function parseBuiltinWorkerArtifact(
  input: unknown,
  code: Uint8Array,
  runtimeDigest: string,
): BuiltinWorkerRegistry {
  const result = builtinWorkerRegistrySchema.safeParse(input);
  if (
    !result.success ||
    code.length > 16 << 20 ||
    result.data.worker_digest !== sha256(code) ||
    result.data.runtime_digest !== runtimeDigest ||
    result.data.artifact_digest !==
      builtinWorkerArtifactDigest(result.data.worker_digest, runtimeDigest)
  )
    throw new Error("BUILTIN_ARTIFACT_INVALID");
  return result.data;
}
