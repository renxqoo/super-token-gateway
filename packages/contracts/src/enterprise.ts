import { z } from "zod";
import { atomsSchema } from "./billing.ts";

export const digestSchema = z.string().regex(/^[a-f0-9]{64}$/);
export const opaqueResourceIdSchema = z
  .string()
  .regex(/^[A-Za-z0-9_:/.-]{1,500}$/)
  .refine(
    (value) =>
      !value.includes("://") && !value.split("/").some((part) => part === "." || part === ".."),
    "Expected a stable opaque resource identity",
  );
export const batchManifestSchema = z
  .object({
    items: z.number().int().min(1).max(Number.MAX_SAFE_INTEGER),
    idsSha256: digestSchema,
    images: z.number().int().min(1).max(Number.MAX_SAFE_INTEGER).optional(),
  })
  .strict();
export const batchResourceMetadataSchema = z
  .object({
    endpoint: z
      .string()
      .refine(
        (value) =>
          [
            "/v1/chat/completions",
            "/v1/responses",
            "/v1/embeddings",
            "/v1/completions",
            "/v1/moderations",
            "/v1/images/generations",
            "/v1/images/edits",
          ].includes(value),
        "Unsupported batch endpoint",
      ),
    upstreamModel: z.string().min(1).max(200),
    manifest: batchManifestSchema.optional(),
  })
  .strict();
export const commandEnvelopeSchema = z
  .object({
    command_id: z.uuid(),
    body_digest: digestSchema,
    caller_incarnation: z.uuid(),
    deadline_at: z.iso.datetime(),
    trace_id: z.string().min(1).max(200),
    contract_version: z.literal(1),
  })
  .strict();
export const pluginBindingSchema = z
  .object({
    id: z.uuid(),
    channel_id: z.uuid(),
    plugin_version_id: z.uuid(),
    package_digest: digestSchema,
    config_digest: digestSchema,
    config_revision: atomsSchema,
  })
  .strict();
export const resourceCommitSchema = z
  .object({
    public_id: z.string().regex(/^[A-Za-z0-9_-]{1,200}$/),
    remote_id: opaqueResourceIdSchema,
    kind: z.enum(["file", "batch", "video", "response", "operation"]),
    project_id: z.uuid(),
    channel_id: z.uuid(),
    attempt_id: z.uuid().optional(),
    model: z.string().min(1).max(200).optional(),
    binding_id: z.uuid(),
    state: z.enum(["ACTIVE", "PENDING_USAGE", "COMPLETED", "CANCELLED", "FAILED", "DELETED"]),
    batch: batchResourceMetadataSchema.optional(),
  })
  .strict();
export type CommandEnvelope = z.infer<typeof commandEnvelopeSchema>;
export type PluginBinding = z.infer<typeof pluginBindingSchema>;
export type ResourceCommit = z.infer<typeof resourceCommitSchema>;
export type BatchManifest = z.infer<typeof batchManifestSchema>;
export type BatchResourceMetadata = z.infer<typeof batchResourceMetadataSchema>;

export { upstreamCostConfigurationSchema, type UpstreamCostConfiguration } from "./public.ts";
