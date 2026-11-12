import { sql } from "drizzle-orm";
import {
  pgTable,
  text,
  boolean,
  timestamp,
  numeric,
  uuid,
  primaryKey,
  unique,
  index,
  check,
  foreignKey,
  customType,
} from "drizzle-orm/pg-core";
import { jsonb } from "./jsonb.ts";
const bytes = customType<{ data: Buffer }>({ dataType: () => "bytea" });
const instant = (name: string) => timestamp(name, { withTimezone: true, mode: "string" });
const revision = () =>
  numeric("authorization_revision", { precision: 38, scale: 0 }).default("1").notNull();
export const internalServiceInstances = pgTable(
  "internal_service_instances",
  {
    service: text().notNull(),
    instanceId: text("instance_id").notNull(),
    enabled: boolean().default(true).notNull(),
    authorizationRevision: revision(),
    securityRevokedAt: instant("security_revoked_at"),
    securityReason: text("security_reason"),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.service, t.instanceId] }),
    check(
      "identity_instance_shape",
      sql`service IN ('gateway','core-api','core-worker') AND instance_id ~ '^[A-Za-z0-9_-]{1,100}$' AND authorization_revision>0`,
    ),
    check(
      "identity_instance_security",
      sql`(security_revoked_at IS NULL AND security_reason IS NULL) OR (security_revoked_at IS NOT NULL AND security_reason='SECURITY_INCIDENT')`,
    ),
  ],
);
export const internalServiceCertificates = pgTable(
  "internal_service_certificates",
  {
    certificateSha256: text("certificate_sha256").primaryKey(),
    service: text().notNull(),
    instanceId: text("instance_id").notNull(),
    uriSan: text("uri_san").notNull(),
    subjectAltnameExact: text("subject_altname_exact").notNull(),
    notBefore: instant("not_before").notNull(),
    notAfter: instant("not_after").notNull(),
    retiredAt: instant("retired_at"),
    securityRevokedAt: instant("security_revoked_at"),
    authorizationRevision: revision(),
  },
  (t) => [
    foreignKey({
      columns: [t.service, t.instanceId],
      foreignColumns: [internalServiceInstances.service, internalServiceInstances.instanceId],
    }),
    index("identity_cert_instance_idx").on(t.service, t.instanceId, t.certificateSha256),
    unique().on(t.certificateSha256, t.service, t.instanceId),
    check(
      "identity_cert_shape",
      sql`certificate_sha256 ~ '^[a-f0-9]{64}$' AND length(uri_san) BETWEEN 1 AND 250 AND length(subject_altname_exact) BETWEEN 1 AND 2000 AND not_after>not_before AND authorization_revision>0`,
    ),
  ],
);
export const internalStartupSessions = pgTable(
  "internal_startup_sessions",
  {
    sessionId: uuid("session_id").primaryKey(),
    incarnationId: uuid("incarnation_id").notNull().unique(),
    service: text().notNull(),
    instanceId: text("instance_id").notNull(),
    certificateSha256: text("certificate_sha256").notNull(),
    publicKeySpki: bytes("public_key_spki").notNull(),
    publicKeySha256: text("public_key_sha256").notNull().unique(),
    bootNonce: bytes("boot_nonce").notNull(),
    registeredAt: instant("registered_at").notNull(),
    expiresAt: instant("expires_at").notNull(),
    disabledAt: instant("disabled_at"),
    securityRevokedAt: instant("security_revoked_at"),
    authorizationRevision: revision(),
  },
  (t) => [
    foreignKey({
      columns: [t.service, t.instanceId],
      foreignColumns: [internalServiceInstances.service, internalServiceInstances.instanceId],
    }),
    foreignKey({
      columns: [t.certificateSha256, t.service, t.instanceId],
      foreignColumns: [
        internalServiceCertificates.certificateSha256,
        internalServiceCertificates.service,
        internalServiceCertificates.instanceId,
      ],
    }),
    unique().on(t.service, t.instanceId, t.bootNonce),
    index("identity_session_instance_idx").on(t.service, t.instanceId, t.expiresAt, t.sessionId),
    check(
      "identity_session_shape",
      sql`octet_length(public_key_spki)=44 AND encode(sha256(public_key_spki),'hex')=public_key_sha256 AND octet_length(boot_nonce)=32 AND expires_at>registered_at AND authorization_revision>0`,
    ),
  ],
);
export const internalStartupRegistrations = pgTable(
  "internal_startup_registrations",
  {
    challengeId: uuid("challenge_id").primaryKey(),
    service: text().notNull(),
    instanceId: text("instance_id").notNull(),
    registrationId: uuid("registration_id").notNull(),
    certificateSha256: text("certificate_sha256").notNull(),
    audience: text().notNull(),
    publicKeySpki: bytes("public_key_spki").notNull(),
    publicKeySha256: text("public_key_sha256").notNull(),
    bootNonce: bytes("boot_nonce").notNull(),
    serverNonce: bytes("server_nonce").notNull(),
    registrationDigest: text("registration_digest").notNull(),
    challengeExpiresAt: instant("challenge_expires_at").notNull(),
    createdAt: instant("created_at").defaultNow().notNull(),
    state: text().default("PENDING").notNull(),
    sessionId: uuid("session_id")
      .unique()
      .references(() => internalStartupSessions.sessionId),
    receipt: jsonb(),
  },
  (t) => [
    foreignKey({
      columns: [t.service, t.instanceId],
      foreignColumns: [internalServiceInstances.service, internalServiceInstances.instanceId],
    }),
    foreignKey({
      columns: [t.certificateSha256, t.service, t.instanceId],
      foreignColumns: [
        internalServiceCertificates.certificateSha256,
        internalServiceCertificates.service,
        internalServiceCertificates.instanceId,
      ],
    }),
    unique().on(t.service, t.instanceId, t.registrationId),
    index("identity_pending_expiry_idx").on(t.state, t.challengeExpiresAt, t.challengeId),
    check(
      "identity_registration_shape",
      sql`octet_length(public_key_spki)=44 AND encode(sha256(public_key_spki),'hex')=public_key_sha256 AND octet_length(boot_nonce)=32 AND octet_length(server_nonce)=32 AND registration_digest ~ '^[a-f0-9]{64}$' AND audience ~ '^[A-Za-z0-9_.-]{1,100}/core-api$'`,
    ),
    check(
      "identity_registration_state",
      sql`(state='REGISTERED' AND session_id IS NOT NULL AND receipt IS NOT NULL) OR (state IN ('PENDING','EXPIRED') AND session_id IS NULL AND receipt IS NULL)`,
    ),
  ],
);
