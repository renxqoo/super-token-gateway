import { z } from "zod";
import { operations } from "@gateway/kernel";
import { atomsSchema, currencySchema } from "./billing.ts";
import { usageSchema } from "./public.ts";
import { digestSchema } from "./enterprise.ts";
import {
  internalInstantSchema,
  internalUuidSchema,
  dispatchProjectionV1Schema,
  candidateRefV1Schema,
} from "./internal-identity.ts";
export const executionKinds = [
  "execution_context_v1",
  "execution_admit_v1",
  "execution_next_v1",
  "execution_begin_v1",
  "execution_abort_v1",
  "execution_query_v1",
  "execution_quota_v1",
  "execution_quota_recovery_v1",
  "execution_abort_recovery_v1",
] as const;
export const executionKindSchema = z.enum(executionKinds);
export const executionEnvelopeSchema = z.strictObject({
  command_id: internalUuidSchema,
  body_digest: digestSchema,
  caller_incarnation: internalUuidSchema,
  deadline_at: internalInstantSchema,
  trace_id: z.string().regex(/^[A-Za-z0-9_-]{1,100}$/),
  contract_version: z.literal(1),
});
const normalizedBillableUsageSchema = usageSchema.transform((value) => {
  const result = { ...value };
  for (const key of Object.keys(result) as (keyof typeof result)[]) {
    const quantity = result[key];
    if (quantity?.includes(".")) result[key] = quantity.replace(/0+$/, "").replace(/\.$/, "");
  }
  return result;
});
export const billableManifestV1Schema = z.strictObject({
  version: z.literal(1),
  operation: z.enum(operations),
  model: z.string().min(1).max(150),
  upstream_model: z.string().min(1).max(200),
  stream: z.boolean(),
  input_bytes: z
    .number()
    .int()
    .min(0)
    .max(16 << 20),
  ceilings: normalizedBillableUsageSchema,
  quantities: normalizedBillableUsageSchema,
  input_characters: atomsSchema.optional(),
  images: atomsSchema.optional(),
  resource_ids: z.array(internalUuidSchema).max(128),
});
const prep = { prep_session_id: internalUuidSchema, prep_binding_digest: digestSchema };
const dispatch = {
  context_id: internalUuidSchema,
  dispatch_projection: dispatchProjectionV1Schema,
  dispatched_digest: digestSchema,
  manifest: billableManifestV1Schema,
  ...prep,
};
export const executionDataSchemas = {
  execution_abort_recovery_v1: z.strictObject({ attempt_id: internalUuidSchema }),
  execution_quota_recovery_v1: z.strictObject({ limit: z.number().int().min(1).max(100) }),
  execution_context_v1: z.strictObject({
    customer_token: z.string().min(1).max(16384),
    ip: z
      .string()
      .max(64)
      .regex(/^[0-9a-fA-F:.]{2,64}$/),
    operation: z.enum(operations),
    model: z.string().min(1).max(150),
    fingerprint: digestSchema,
  }),
  execution_admit_v1: z.strictObject({
    ...dispatch,
    project_id: internalUuidSchema,
    key_id: internalUuidSchema,
    intent_digest: digestSchema.nullable(),
    fingerprint: digestSchema,
    customer_currency: currencySchema,
  }),
  execution_next_v1: z.strictObject({
    ...dispatch,
    request_id: internalUuidSchema,
    previous_attempt_id: internalUuidSchema,
    expected_ordinal: z.number().int().min(2).max(5),
    visible_output: z.literal(false),
  }),
  execution_begin_v1: z.strictObject({
    attempt_id: internalUuidSchema,
    permit_epoch: atomsSchema,
    authorization_plan_digest: digestSchema,
    dispatched_digest: digestSchema,
    ...prep,
  }),
  execution_abort_v1: z.strictObject({
    attempt_id: internalUuidSchema,
    permit_epoch: atomsSchema,
    reason: z.enum(["CANCELLED", "PREPARE_DISPOSED", "DEADLINE", "QUOTA_EXPIRED"]),
    close_request: z.boolean().default(false),
  }),
  execution_query_v1: z.strictObject({
    kind: z.enum([
      "execution_admit_v1",
      "execution_next_v1",
      "execution_begin_v1",
      "execution_abort_v1",
      "execution_quota_v1",
      "execution_abort_recovery_v1",
    ]),
    command_id: internalUuidSchema,
  }),
  execution_quota_v1: z.strictObject({
    attempt_id: internalUuidSchema,
    permit_epoch: atomsSchema,
    action: z.enum(["ADOPT", "RENEW", "CLOSE", "RELEASE"]),
    reason: z.enum(["COMPLETED", "FAILED", "CANCELLED", "DRAINED"]).optional(),
  }),
} as const;
export const hostAuthorizationPlanV1Schema = z.strictObject({
  version: z.literal(1),
  owner: z.strictObject({
    service: z.literal("gateway"),
    instance_id: z.string().regex(/^[A-Za-z0-9_-]{1,100}$/),
    session_id: internalUuidSchema,
    incarnation_id: internalUuidSchema,
  }),
  attempt_id: internalUuidSchema,
  permit_epoch: atomsSchema,
  dispatched_digest: digestSchema,
  dispatch_projection: dispatchProjectionV1Schema,
});
export const executionReceiptSchema = z.strictObject({
  command_state: z.enum(["COMMITTED", "REJECTED", "NOT_FOUND"]),
  code: z
    .string()
    .regex(/^[a-z_]{1,80}$/)
    .optional(),
  request_id: internalUuidSchema.optional(),
  attempt_id: internalUuidSchema.optional(),
  ordinal: z.number().int().positive().optional(),
  state: z.enum(["RESERVED", "SEND_GRANTED", "ABORTED", "UNKNOWN", "TERMINAL"]).optional(),
  permit_epoch: atomsSchema.optional(),
  deadline_at: internalInstantSchema.optional(),
  customer_atoms: atomsSchema.optional(),
  customer_currency: currencySchema.optional(),
  supplier_atoms: atomsSchema.optional(),
  supplier_currency: currencySchema.optional(),
  quota_lease_ids: z.array(digestSchema).max(5).optional(),
  authorization_plan: hostAuthorizationPlanV1Schema.optional(),
  authorization_plan_digest: digestSchema.optional(),
});
export const transientSendSchema = z.strictObject({
  grant_id: internalUuidSchema,
  owner_session_id: internalUuidSchema,
  incarnation_id: internalUuidSchema,
  attempt_id: internalUuidSchema,
  permit_epoch: atomsSchema,
  authorization_plan_digest: digestSchema,
  deadline_at: internalInstantSchema,
  remaining_ms: z.number().int().positive().max(30000),
  auth_material: z.discriminatedUnion("type", [
    z.strictObject({ type: z.literal("none") }),
    z.strictObject({ type: z.literal("bearer"), value: z.string().max(4096) }),
    z.strictObject({
      type: z.literal("api-key"),
      header: z.enum(["api-key", "x-api-key"]),
      value: z.string().max(4096),
    }),
  ]),
});
export const candidateMetadataSchema = z.strictObject({
  ref: candidateRefV1Schema,
  origin: z.string().max(250),
  upstream_model: z.string().max(200),
  credential_ciphertext_sha256: digestSchema,
  config: z.record(z.string(), z.unknown()),
  cost_currency: currencySchema,
  cost_rates: z.record(z.string(), z.unknown()),
  cost_ceilings: usageSchema,
  customer_currency: currencySchema,
  customer_rates: z.record(z.string(), z.unknown()),
  customer_ceilings: usageSchema,
  customer_basis: z.enum(["PROVIDER_USAGE", "INPUT_CHARACTERS", "REQUEST_COMPLETION"]),
  request_policy: z.record(z.string(), z.unknown()),
  routing: z.strictObject({ priority: z.number().int(), weight: z.number().positive() }),
  channel_limits: z.strictObject({
    rpm: z.number().int().positive(),
    concurrency: z.number().int().positive(),
  }),
});
export type ExecutionKind = z.infer<typeof executionKindSchema>;
export type BillableManifestV1 = z.infer<typeof billableManifestV1Schema>;
export type ExecutionReceipt = z.infer<typeof executionReceiptSchema>;
export type ExecutionEnvelope = z.infer<typeof executionEnvelopeSchema>;
export const executionContextResponseSchema = z.strictObject({
  owner: hostAuthorizationPlanV1Schema.shape.owner,
  context_id: internalUuidSchema,
  project_id: internalUuidSchema,
  key_id: internalUuidSchema,
  customer_currency: currencySchema,
  expires_at: internalInstantSchema,
  operation: z.enum(operations),
  model: z.string().min(1).max(150),
  max_output_tokens: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  routing_strategy: z.enum(["WEIGHTED", "ROUND_ROBIN", "LEAST_LATENCY"]),
  execution_budget: z.strictObject({ max_attempts: z.number().int().min(1).max(5) }),
  scopes: z.array(
    z.strictObject({
      kind: z.enum(["organization", "principal", "project", "key"]),
      id: internalUuidSchema,
      rpm: z.number().int().positive(),
      tpm: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
      concurrency: z.number().int().positive(),
    }),
  ),
  candidates: z.array(candidateMetadataSchema).max(128),
});
export const executionReplySchema = z.strictObject({
  receipt: executionReceiptSchema,
  transient_send: transientSendSchema.optional(),
});
export type ExecutionReply = z.infer<typeof executionReplySchema>;
export type ExecutionContextResponse = z.infer<typeof executionContextResponseSchema>;
export function prepBindingV1(input: {
  owner: z.infer<typeof hostAuthorizationPlanV1Schema>["owner"];
  project_id: string;
  projection: z.infer<typeof dispatchProjectionV1Schema>;
  prep_session_id: string;
  host_config_sha256: string;
}) {
  const { owner, projection, prep_session_id, project_id, host_config_sha256 } = input;
  return {
    session: {
      id: prep_session_id,
      tenant: project_id,
      attempt: projection.proposed_attempt_id,
      incarnation: owner.incarnation_id,
      plugin: projection.candidate_ref.package_digest,
      config: host_config_sha256,
    },
    owner: { service: owner.service, instance_id: owner.instance_id, session_id: owner.session_id },
    request_id: projection.proposed_request_id,
    operation: projection.operation,
    binding_id: projection.candidate_ref.binding_id,
    config_revision: projection.candidate_ref.config_revision,
    config_digest: projection.candidate_ref.config_digest,
  };
}
export const evidenceSources = [
  "execution",
  "execution_outcome",
  "recovery_interruption",
  "async_partial",
  "async_final",
  "batch_completed",
  "batch_cancelled",
  "batch_failed",
  "provider_invoice",
] as const;
const evidenceRevisionSchema = z.string().regex(/^[1-9][0-9]{0,37}$/);
const evidenceEventFields = {
  event_id: internalUuidSchema,
  attempt_id: internalUuidSchema,
  origin_identity: z.string().regex(/^gateway:[A-Za-z0-9_-]{1,100}$/),
  origin_incarnation: internalUuidSchema,
  origin_session_id: internalUuidSchema,
  permit_epoch: evidenceRevisionSchema,
  source: z.enum(evidenceSources),
  source_revision: evidenceRevisionSchema,
  finality: z.enum(["FINAL", "PARTIAL", "UNKNOWN", "ESTIMATED", "NOT_EXECUTED"]),
  usage: usageSchema,
  final_dimensions: z.array(z.string()).max(15),
  attempt_state: z.enum(["SUCCEEDED", "FAILED", "UNKNOWN", "PARTIAL", "NOT_EXECUTED"]).optional(),
  request_state: z.enum(["SUCCEEDED", "FAILED", "CANCELLED", "UNKNOWN"]).optional(),
  error_code: z
    .string()
    .regex(/^[a-z][a-z0-9_]{0,79}$/)
    .optional(),
  provider_request_id: z
    .string()
    .regex(/^[A-Za-z0-9._:/-]{1,256}$/)
    .optional(),
};
function refineEvidenceEvent(
  value: z.infer<z.ZodObject<typeof evidenceEventFields>>,
  context: z.RefinementCtx,
) {
  const finals = new Set(value.final_dimensions);
  if (finals.size !== value.final_dimensions.length)
    context.addIssue({ code: "custom", message: "Duplicate final dimension" });
  for (const dimension of finals)
    if (!(dimension in value.usage))
      context.addIssue({ code: "custom", message: "Final dimension lacks a quantity" });
  if ((value.finality === "UNKNOWN" || value.finality === "ESTIMATED") && finals.size > 0)
    context.addIssue({ code: "custom", message: "Non-factual evidence cannot be final" });
  if (
    value.finality === "NOT_EXECUTED" &&
    (Object.keys(value.usage).length > 0 ||
      finals.size > 0 ||
      (value.attempt_state ?? "NOT_EXECUTED") !== "NOT_EXECUTED")
  )
    context.addIssue({ code: "custom", message: "NOT_EXECUTED carries no quantities" });
}
export const coreEvidenceEventV1Schema = z
  .strictObject(evidenceEventFields)
  .superRefine(refineEvidenceEvent);
export const signedCoreEvidenceEventV1Schema = z
  .strictObject({ ...evidenceEventFields, signature: z.string().regex(/^[A-Za-z0-9_-]{86}$/) })
  .superRefine(refineEvidenceEvent);
export const coreEvidenceGroupV1Schema = z.strictObject({
  events: z.array(signedCoreEvidenceEventV1Schema).min(1).max(64),
});
export const coreEvidenceBatchV1Schema = z.strictObject({
  version: z.literal(1),
  groups: z.array(coreEvidenceGroupV1Schema).min(1).max(128),
});
export function evidenceProofFields(
  event: Pick<CoreEvidenceEventV1, "origin_identity" | "origin_session_id" | "origin_incarnation">,
  bodySha256: string,
): string[] {
  return [
    "stg.internal.evidence",
    "1",
    event.origin_identity,
    event.origin_session_id,
    event.origin_incarnation,
    bodySha256,
  ];
}
export const coreEvidenceGroupResultSchema = z.discriminatedUnion("status", [
  z.strictObject({
    status: z.literal("COMMITTED"),
    receipts: z.array(
      z.strictObject({
        event_id: internalUuidSchema,
        receipt_id: internalUuidSchema,
        outcome: z.enum([
          "ACCEPTED",
          "STALE",
          "ESTIMATED",
          "REVISION_REPLAY",
          "CONFLICT",
          "OVERFLOW",
          "QUARANTINED",
        ]),
        duplicate: z.boolean(),
      }),
    ),
    billing: z.record(z.string(), z.unknown()),
  }),
  z.strictObject({ status: z.literal("REJECTED"), error: z.string() }),
  z.strictObject({ status: z.literal("ERROR"), error: z.string() }),
]);
export type CoreEvidenceEventV1 = z.infer<typeof coreEvidenceEventV1Schema>;
export type CoreEvidenceGroupV1 = z.infer<typeof coreEvidenceGroupV1Schema>;
export type SignedCoreEvidenceEventV1 = z.infer<typeof signedCoreEvidenceEventV1Schema>;
export type CoreEvidenceBatchV1 = z.infer<typeof coreEvidenceBatchV1Schema>;
export type CoreEvidenceGroupResult = z.infer<typeof coreEvidenceGroupResultSchema>;
