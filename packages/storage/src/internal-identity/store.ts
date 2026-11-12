import type { SQL } from "bun";
import { z } from "zod";
import {
  internalServiceSchema,
  internalInstanceSchema,
  internalUuidSchema,
  digestSchema,
  startupChallengeSchema,
  startupRegistrationResponseSchema,
  internalCurrentResponseSchema,
  type CoreVerifiedPeerProjection,
  type CoreVerifiedCallerProjection,
} from "@gateway/contracts";
const nullableInstant = z.string().nullable();
const peerSchema = z.object({
  service: internalServiceSchema,
  instance_id: internalInstanceSchema,
  certificate_sha256: digestSchema,
  uri_san: z.string(),
  subject_altname_exact: z.string(),
  not_before: z.string(),
  not_after: z.string(),
  retired_at: nullableInstant,
  security_revoked_at: nullableInstant,
  instance_enabled: z.boolean(),
  instance_security_revoked_at: nullableInstant,
  instance_revision: z.string(),
  certificate_revision: z.string(),
});
const sessionSchema = z.object({
  session_id: internalUuidSchema,
  incarnation_id: internalUuidSchema,
  service: internalServiceSchema,
  instance_id: internalInstanceSchema,
  certificate_sha256: digestSchema,
  public_key_spki: z.string(),
  public_key_sha256: digestSchema,
  expires_at: z.string(),
  disabled_at: nullableInstant,
  security_revoked_at: nullableInstant,
  session_revision: z.string(),
});
const registrationSchema = z.object({
  ...startupChallengeSchema.shape,
  public_key_spki: z.string(),
  registration_digest: digestSchema,
  state: z.enum(["PENDING", "REGISTERED", "EXPIRED"]),
});
function peerScalars(p: CoreVerifiedPeerProjection) {
  return {
    service: p.service,
    instance_id: p.instance_id,
    certificate_sha256: p.certificate_sha256,
    instance_revision: p.instance_revision,
    certificate_revision: p.certificate_revision,
    raw_body_sha256: p.raw_body_sha256,
    endpoint: p.endpoint,
  };
}
export function createInternalIdentityStore(db: SQL, signal?: AbortSignal) {
  async function run<T>(query: SQL.Query<T>) {
    const cancel = () => {
      query.cancel();
    };
    signal?.addEventListener("abort", cancel, { once: true });
    if (signal?.aborted) cancel();
    try {
      return await query;
    } finally {
      signal?.removeEventListener("abort", cancel);
    }
  }
  return {
    async readPeerDirectory(hash: string) {
      const value = (await run(db`SELECT internal_identity_read_peer_v1(${hash}) AS value`))[0]
        ?.value;
      return value ? peerSchema.parse(value) : undefined;
    },
    async readStartupSession(id: string) {
      const value = (
        await run(db`SELECT internal_identity_read_session_v1(${id}::uuid) AS value`)
      )[0]?.value;
      return value ? sessionSchema.parse(value) : undefined;
    },
    async readRegistration(id: string) {
      const value = (
        await run(db`SELECT internal_identity_read_registration_v1(${id}::uuid) AS value`)
      )[0]?.value;
      return value ? registrationSchema.parse(value) : undefined;
    },
    async challenge(
      peer: CoreVerifiedPeerProjection,
      request: { registration_id: string; public_key_spki: Buffer; boot_nonce: Buffer },
      audience: string,
      digest: string,
      nonce: Buffer,
      ttl: number,
    ) {
      const value = (
        await run(
          db`SELECT internal_identity_challenge_v1(${peerScalars(peer)}::jsonb,${request.registration_id}::uuid,${request.public_key_spki}::bytea,${request.boot_nonce}::bytea,${audience},${digest},${nonce}::bytea,${ttl}) AS value`,
        )
      )[0]?.value;
      return z.object(startupChallengeSchema.shape).parse(value);
    },
    async register(
      peer: CoreVerifiedPeerProjection,
      challengeId: string,
      registrationId: string,
      digest: string,
      ttl: number,
    ) {
      const value = (
        await run(
          db`SELECT internal_identity_register_v1(${peerScalars(peer)}::jsonb,${challengeId}::uuid,${registrationId}::uuid,${digest},${ttl}) AS value`,
        )
      )[0]?.value;
      return startupRegistrationResponseSchema.parse(value);
    },
    async current(projection: CoreVerifiedCallerProjection) {
      const p = {
        service: projection.service,
        instance_id: projection.instance_id,
        certificate_sha256: projection.certificate_sha256,
        instance_revision: projection.instance_revision,
        certificate_revision: projection.certificate_revision,
        raw_body_sha256: projection.raw_body_sha256,
        session_id: projection.session_id,
        incarnation_id: projection.incarnation_id,
        key_sha256: projection.key_sha256,
        session_revision: projection.session_revision,
        command_type: projection.command_type,
        command_id: projection.command_id,
        method: projection.method,
        normalized_path: projection.normalized_path,
        deadline_at: projection.deadline_at,
        trace_id: projection.trace_id,
      };
      const value = (await run(db`SELECT internal_identity_current_v1(${p}::jsonb) AS value`))[0]
        ?.value;
      return internalCurrentResponseSchema.parse(value);
    },
  };
}
export type InternalIdentityStore = ReturnType<typeof createInternalIdentityStore>;
