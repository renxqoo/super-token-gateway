import { z } from "zod";
import { operations } from "@gateway/kernel";
import { digestSchema } from "./enterprise.ts";

export const internalServiceSchema = z.enum(["gateway", "core-api", "core-worker"]);
export const internalUuidSchema = z
  .string()
  .regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
export const internalInstantSchema = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
  .refine((v) => Number.isFinite(Date.parse(v)) && new Date(v).toISOString() === v);
export const internalInstanceSchema = z.string().regex(/^[A-Za-z0-9_-]{1,100}$/);
const nonce = z.string().regex(/^[A-Za-z0-9_-]{43}$/);
export const internalSignatureSchema = z.string().regex(/^[A-Za-z0-9_-]{86}$/);
const revision = z.string().regex(/^[1-9][0-9]{0,37}$/);
export const startupChallengeRequestSchema = z.strictObject({
  registration_id: internalUuidSchema,
  public_key_spki: z.string().regex(/^[A-Za-z0-9_-]{59}$/),
  boot_nonce: nonce,
  contract_version: z.literal(1),
});
export const startupChallengeSchema = z.strictObject({
  challenge_id: internalUuidSchema,
  registration_id: internalUuidSchema,
  audience: z.string().regex(/^[A-Za-z0-9_.-]{1,100}\/core-api$/),
  service: internalServiceSchema,
  instance_id: internalInstanceSchema,
  certificate_sha256: digestSchema,
  public_key_sha256: digestSchema,
  boot_nonce: nonce,
  server_nonce: nonce,
  challenge_expires_at: internalInstantSchema,
});
export const startupRegistrationRequestSchema = z.strictObject({
  challenge_id: internalUuidSchema,
  registration_id: internalUuidSchema,
  signature: internalSignatureSchema,
  contract_version: z.literal(1),
});
export const startupReceiptSchema = z.strictObject({
  session_id: internalUuidSchema,
  incarnation_id: internalUuidSchema,
  service: internalServiceSchema,
  instance_id: internalInstanceSchema,
  certificate_sha256: digestSchema,
  public_key_sha256: digestSchema,
  expires_at: internalInstantSchema,
});
export const startupRegistrationResponseSchema = z.strictObject({
  receipt: startupReceiptSchema,
  current: z.boolean(),
  replayed: z.boolean(),
});
export const internalProofSchema = z.strictObject({
  session_id: internalUuidSchema,
  incarnation_id: internalUuidSchema,
  command_type: z.enum([
    "identity_current_v1",
    "execution_context_v1",
    "execution_admit_v1",
    "execution_next_v1",
    "execution_begin_v1",
    "execution_abort_v1",
    "execution_query_v1",
    "execution_quota_v1",
    "execution_quota_recovery_v1",
    "execution_abort_recovery_v1",
  ]),
  command_id: internalUuidSchema,
  deadline_at: internalInstantSchema,
  trace_id: z.string().regex(/^[A-Za-z0-9_-]{1,100}$/),
  contract_version: z.literal(1),
  raw_body_sha256: digestSchema,
  signature: internalSignatureSchema,
});
export const internalCurrentRequestSchema = z.strictObject({
  envelope: z.strictObject({
    command_id: internalUuidSchema,
    body_digest: digestSchema,
    caller_incarnation: internalUuidSchema,
    deadline_at: internalInstantSchema,
    trace_id: z.string().regex(/^[A-Za-z0-9_-]{1,100}$/),
    contract_version: z.literal(1),
  }),
  data: z.strictObject({}),
});
export const internalCurrentResponseSchema = z.strictObject({
  command_id: internalUuidSchema,
  kind: z.literal("identity_current_v1"),
  receipt: startupReceiptSchema,
  current: z.literal(true),
  authorization_revision: revision,
});
export const executionRejectionCodeSchema = z.enum([
  "insufficient_funds",
  "upstream_funds_unavailable",
  "already_accepted",
  "idempotency_conflict",
  "proposed_id_conflict",
  "context_stale",
  "context_invalid",
  "candidate_stale",
  "cost_unbounded",
  "cost_ceiling",
  "estimate_invalid",
  "quote_invalid",
  "request_mismatch",
  "execution_closed",
  "retry_quota_exceeded",
  "retry_conflict",
  "retry_quote_exceeded",
  "permit_not_reserved",
  "begin_precondition",
  "internal_request_invalid",
  "abort_closure_conflict",
  "prep_binding_invalid",
  "amount_overflow",
  "billing_ceiling",
  "tariff_unavailable",
  "invalid_tariff",
  "quota_budget_exceeded",
  "output_bound",
  "invalid_request",
  "recovery_not_reserved",
  "recovery_not_eligible",
]);
export function executionRejectionStatus(code: string) {
  if (code === "context_invalid") return 403;
  if (code === "internal_request_invalid") return 400;
  if (code === "insufficient_funds") return 402;
  if (code === "upstream_funds_unavailable") return 503;
  return [
    "candidate_stale",
    "context_stale",
    "already_accepted",
    "idempotency_conflict",
    "proposed_id_conflict",
    "request_mismatch",
    "execution_closed",
    "retry_conflict",
    "permit_not_reserved",
    "abort_closure_conflict",
    "recovery_not_reserved",
    "recovery_not_eligible",
  ].includes(code)
    ? 409
    : 422;
}
export const internalErrorSchema = z.strictObject({
  code: z.union([
    executionRejectionCodeSchema,
    z.enum([
      "internal_proof_invalid",
      "internal_session_invalid",
      "internal_peer_forbidden",
      "internal_owner_mismatch",
      "internal_identity_revoked",
      "internal_request_invalid",
      "registration_conflict",
      "startup_key_reused",
      "startup_boot_conflict",
      "internal_deadline",
      "internal_body_too_large",
      "internal_capacity",
      "identity_busy",
      "identity_store_unavailable",
      "command_conflict",
      "quota_unavailable",
      "quota_release_denied",
      "candidate_stale",
      "cost_unbounded",
      "customer_forbidden",
      "context_invalid",
    ]),
  ]),
  category: z.enum(["AUTHENTICATION", "VALIDATION", "DEPENDENCY"]),
  retryability: z.enum(["NONE", "SAME_COMMAND", "STATUS_ONLY"]),
  command_state: z.enum(["NOT_ATTEMPTED", "UNKNOWN", "REJECTED"]),
  request_id: internalUuidSchema.optional(),
  attempt_id: internalUuidSchema.optional(),
  message: z.string().max(100),
});
export const candidateRefV1Schema = z.strictObject({
  route_id: internalUuidSchema,
  channel_id: internalUuidSchema,
  binding_id: internalUuidSchema,
  plugin_version_id: internalUuidSchema,
  package_digest: digestSchema,
  config_revision: revision,
  config_digest: digestSchema,
  cost_version_id: internalUuidSchema,
  customer_price_id: internalUuidSchema,
  candidate_digest: digestSchema,
});
export const dispatchAuthSchema = z.discriminatedUnion("type", [
  z.strictObject({ type: z.literal("none") }),
  z.strictObject({ type: z.literal("bearer") }),
  z.strictObject({ type: z.literal("api-key"), header: z.enum(["api-key", "x-api-key"]) }),
]);
export const dispatchProjectionV1Schema = z.strictObject({
  version: z.literal(1),
  proposed_request_id: internalUuidSchema,
  proposed_attempt_id: internalUuidSchema,
  origin: z
    .string()
    .max(250)
    .refine((value) => {
      try {
        const u = new URL(value);
        return u.protocol === "https:" && u.origin === value && !u.username && !u.password;
      } catch {
        return false;
      }
    }),
  method: z.enum(["GET", "POST", "DELETE"]),
  operation: z.enum(operations),
  request_target_sha256: digestSchema,
  final_headers_digest: digestSchema,
  final_body_sha256: digestSchema,
  candidate_ref: candidateRefV1Schema,
  auth: dispatchAuthSchema,
  credential_ciphertext_sha256: digestSchema,
  billable_manifest_sha256: digestSchema,
  deadline_at: internalInstantSchema,
});
export type InternalService = z.infer<typeof internalServiceSchema>;
export type StartupChallenge = z.infer<typeof startupChallengeSchema>;
export type StartupReceipt = z.infer<typeof startupReceiptSchema>;
export type InternalProof = z.infer<typeof internalProofSchema>;
export type DispatchProjectionV1 = z.infer<typeof dispatchProjectionV1Schema>;
export type CoreVerifiedPeerProjection = {
  service: InternalService;
  instance_id: string;
  certificate_sha256: string;
  instance_revision: string;
  certificate_revision: string;
  raw_body_sha256: string;
  endpoint: "challenges" | "registrations";
};
export type CoreVerifiedCallerProjection = Omit<CoreVerifiedPeerProjection, "endpoint"> & {
  session_id: string;
  incarnation_id: string;
  key_sha256: string;
  session_revision: string;
  command_type:
    | "identity_current_v1"
    | "execution_context_v1"
    | "execution_admit_v1"
    | "execution_next_v1"
    | "execution_begin_v1"
    | "execution_abort_v1"
    | "execution_query_v1"
    | "execution_quota_v1"
    | "execution_quota_recovery_v1"
    | "execution_abort_recovery_v1";
  command_id: string;
  method: "POST";
  normalized_path: string;
  deadline_at: string;
  trace_id: string;
};
export function registrationProofFields(c: StartupChallenge): string[] {
  return [
    "stg.internal.startup-registration",
    "1",
    c.audience,
    c.service,
    c.instance_id,
    c.certificate_sha256,
    c.registration_id,
    c.challenge_id,
    c.public_key_sha256,
    c.boot_nonce,
    c.server_nonce,
    c.challenge_expires_at,
  ];
}
export function commandProofFields(
  audience: string,
  owner: { service: InternalService; instance_id: string },
  p: Omit<InternalProof, "signature">,
  method: string,
  path: string,
): string[] {
  return [
    "stg.internal.command",
    "1",
    audience,
    owner.service,
    owner.instance_id,
    p.session_id,
    p.incarnation_id,
    p.command_type,
    p.command_id,
    method,
    path,
    p.raw_body_sha256,
    p.deadline_at,
    p.trace_id,
  ];
}
