import { sql } from "drizzle-orm";
import {
  pgTable,
  uuid,
  text,
  boolean,
  numeric,
  timestamp,
  integer,
  primaryKey,
  index,
  unique,
  uniqueIndex,
  check,
  foreignKey,
  type AnyPgColumn,
} from "drizzle-orm/pg-core";
import { jsonb } from "./jsonb.ts";
import {
  attempts,
  costVersions,
  gatewayRequests,
  apiKeys,
  projects,
  routes,
  usageEventIndex,
} from "./existing.ts";
import {
  channels,
  upstreamAccounts,
  upstreamAccountBalances,
  channelPluginBindings,
  financialVouchers,
} from "./catalog.ts";

import { internalStartupSessions } from "./internal-identity.ts";

const atoms = (name: string) => numeric(name, { precision: 38, scale: 0 });
const instant = (name: string) => timestamp(name, { withTimezone: true, mode: "string" });
export const executionCommands = pgTable(
  "execution_commands",
  {
    id: uuid().primaryKey(),
    commandId: uuid("command_id").notNull(),
    kind: text().notNull(),
    callerIdentity: text("caller_identity").notNull(),
    ownerSessionId: uuid("owner_session_id").references(() => internalStartupSessions.sessionId),
    callerIncarnation: uuid("caller_incarnation").notNull(),
    bodyDigest: text("body_digest").notNull(),
    deadlineAt: instant("deadline_at").notNull(),
    traceId: text("trace_id").notNull(),
    contractVersion: integer("contract_version").notNull(),
    state: text().notNull(),
    requestId: uuid("request_id").references(() => gatewayRequests.id),
    attemptId: uuid("attempt_id").references(() => attempts.id),
    rejectionCode: text("rejection_code"),
    receipt: jsonb().default({}).notNull(),
    contextId: uuid("context_id").references((): AnyPgColumn => executionContexts.id),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  (t) => [
    unique().on(t.kind, t.callerIdentity, t.commandId),
    check("command_state", sql`state IN ('COMMITTED','REJECTED')`),
    check("command_digest", sql`body_digest ~ '^[a-f0-9]{64}$' AND contract_version>0`),
  ],
);
export const executionPermits = pgTable(
  "execution_permits",
  {
    attemptId: uuid("attempt_id")
      .primaryKey()
      .references(() => attempts.id),
    commandId: uuid("command_id")
      .notNull()
      .references(() => executionCommands.id),
    gatewayInstanceId: text("gateway_instance_id").notNull(),
    ownerSessionId: uuid("owner_session_id").references(() => internalStartupSessions.sessionId),
    ownerKeySha256: text("owner_key_sha256"),
    ownerService: text("owner_service"),
    certificateSha256: text("certificate_sha256"),
    incarnationId: uuid("incarnation_id").notNull(),
    permitEpoch: atoms("permit_epoch").notNull(),
    bindingId: uuid("binding_id")
      .notNull()
      .references(() => channelPluginBindings.id),
    dispatchedDigest: text("dispatched_digest").notNull(),
    routeId: uuid("route_id").references(() => routes.id),
    contextId: uuid("context_id").references((): AnyPgColumn => executionContexts.id),
    snapshot: jsonb().default({}).notNull(),
    manifest: jsonb().default({}).notNull(),
    manifestDigest: text("manifest_digest"),
    candidateDigest: text("candidate_digest"),
    authorizationPlanDigest: text("authorization_plan_digest"),
    prepSessionId: uuid("prep_session_id"),
    prepBindingDigest: text("prep_binding_digest"),
    state: text().notNull(),
    expiresAt: instant("expires_at").notNull(),
    grantedAt: instant("granted_at"),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  () => [
    check("permit_state", sql`state IN ('RESERVED','SEND_GRANTED','ABORTED','UNKNOWN','TERMINAL')`),
    check("permit_epoch", sql`permit_epoch>=0 AND dispatched_digest ~ '^[a-f0-9]{64}$'`),
  ],
);
export const upstreamFundingOperations = pgTable(
  "upstream_funding_operations",
  {
    id: uuid().primaryKey(),
    accountId: uuid("account_id")
      .notNull()
      .references(() => upstreamAccounts.id),
    kind: text().notNull(),
    externalRef: text("external_ref").notNull(),
    payloadDigest: text("payload_digest").notNull(),
    paymentCurrency: text("payment_currency"),
    paymentAtoms: atoms("payment_atoms"),
    creditedCurrency: text("credited_currency").notNull(),
    creditedAtoms: atoms("credited_atoms").notNull(),
    voucherId: uuid("voucher_id").references(() => financialVouchers.id),
    voucherDigest: text("voucher_digest"),
    completedAt: instant("completed_at").notNull(),
    actor: text().notNull(),
    reason: text().notNull(),
    receipt: jsonb().notNull(),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  (t) => [
    unique().on(t.accountId, t.kind, t.externalRef),
    index("upstream_funding_voucher_idx").on(t.voucherId),
    foreignKey({
      name: "funding_voucher_digest_fk",
      columns: [t.voucherId, t.voucherDigest],
      foreignColumns: [financialVouchers.id, financialVouchers.digest],
    }),
    check("upstream_funding_kind", sql`kind IN ('RECHARGE','ADJUSTMENT','TEST_OPENING')`),
    check(
      "upstream_funding_ref",
      sql`length(external_ref) BETWEEN 1 AND 200 AND payload_digest ~ '^[a-f0-9]{64}$'`,
    ),
    check(
      "upstream_funding_currency",
      sql`credited_currency ~ '^[A-Z]{3,8}$' AND (payment_currency IS NULL OR payment_currency ~ '^[A-Z]{3,8}$')`,
    ),
    check(
      "upstream_recharge_fact",
      sql`kind<>'RECHARGE' OR (payment_atoms IS NOT NULL AND payment_atoms>0 AND credited_atoms>0 AND voucher_id IS NOT NULL AND voucher_digest IS NOT NULL AND payment_currency IS NOT NULL)`,
    ),
  ],
);
export const upstreamHolds = pgTable(
  "upstream_holds",
  {
    attemptId: uuid("attempt_id")
      .primaryKey()
      .references(() => attempts.id),
    accountId: uuid("account_id").notNull(),
    channelId: uuid("channel_id")
      .notNull()
      .references((): AnyPgColumn => channels.id),
    currency: text().notNull(),
    estimatedAtoms: atoms("estimated_atoms").notNull(),
    estimatedQuantities: jsonb("estimated_quantities"),
    estimatedQuantitiesDigest: text("estimated_quantities_digest"),
    estimateManifestDigest: text("estimate_manifest_digest"),
    estimateAlgorithmVersion: integer("estimate_algorithm_version"),
    costVersionId: uuid("cost_version_id")
      .notNull()
      .references(() => costVersions.id),
    state: text().notNull(),
    finalCostAtoms: atoms("final_cost_atoms"),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  (t) => [
    foreignKey({
      columns: [t.accountId, t.currency],
      foreignColumns: [upstreamAccountBalances.accountId, upstreamAccountBalances.currency],
    }),
    index("upstream_hold_account_state_idx").on(t.accountId, t.currency, t.state, t.attemptId),
    check("upstream_hold_state", sql`state IN ('HELD','UNKNOWN','PARTIAL','SETTLED','RELEASED')`),
    check(
      "upstream_hold_amounts",
      sql`estimated_atoms>=0 AND (final_cost_atoms IS NULL OR final_cost_atoms>=0)`,
    ),
  ],
);
export const upstreamCosts = pgTable(
  "upstream_costs",
  {
    attemptId: uuid("attempt_id")
      .notNull()
      .references(() => attempts.id),
    revision: atoms("revision").notNull(),
    currency: text().notNull(),
    costAtoms: atoms("cost_atoms").notNull(),
    complete: boolean().notNull(),
    disputed: boolean().default(false).notNull(),
    evidenceDigest: text("evidence_digest").notNull(),
    createdAt: instant("created_at").defaultNow().notNull(),
    evidenceId: uuid("evidence_id").notNull(),
    costVersionId: uuid("cost_version_id")
      .notNull()
      .references(() => costVersions.id),
  },
  (t) => [
    primaryKey({ columns: [t.attemptId, t.revision] }),
    unique("upstream_cost_evidence").on(t.evidenceId),
    foreignKey({
      columns: [t.evidenceId, t.attemptId],
      foreignColumns: [usageEventIndex.id, usageEventIndex.attemptId],
      name: "upstream_cost_evidence_attempt",
    }),
    check(
      "upstream_cost_exact",
      sql`revision>=1 AND cost_atoms>=0 AND evidence_digest ~ '^[a-f0-9]{64}$'`,
    ),
  ],
);
export const coreEvidenceReceipts = pgTable(
  "core_evidence_receipts",
  {
    id: uuid().primaryKey(),
    originIdentity: text("origin_identity").notNull(),
    originIncarnation: uuid("origin_incarnation").notNull(),
    originSessionId: uuid("origin_session_id")
      .notNull()
      .references(() => internalStartupSessions.sessionId),
    eventId: uuid("event_id").notNull(),
    attemptId: uuid("attempt_id")
      .notNull()
      .references(() => attempts.id),
    source: text().notNull(),
    sourceRevision: atoms("source_revision").notNull(),
    bodyDigest: text("body_digest").notNull(),
    eventClaim: boolean("event_claim").notNull(),
    revisionClaim: boolean("revision_claim").notNull(),
    outcome: text().notNull(),
    finality: text().notNull(),
    attemptState: text("attempt_state"),
    requestState: text("request_state"),
    errorCode: text("error_code"),
    providerRequestId: text("provider_request_id"),
    rawEvidenceId: uuid("raw_evidence_id").notNull(),
    canonicalEvidenceId: uuid("canonical_evidence_id"),
    disputedEvidenceId: uuid("disputed_evidence_id"),
    conflictId: uuid("conflict_id"),
    riskDimensions: text("risk_dimensions").array().notNull(),
    numericFault: text("numeric_fault"),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  (t) => [
    unique("core_receipt_delivery").on(t.originIdentity, t.eventId, t.bodyDigest),
    uniqueIndex("core_receipt_event_claim")
      .on(t.originIdentity, t.eventId)
      .where(sql`event_claim`),
    uniqueIndex("core_receipt_revision_claim")
      .on(t.attemptId, t.originIdentity, t.source, t.sourceRevision)
      .where(sql`revision_claim`),
    index("core_receipt_attempt_source").on(t.attemptId, t.source, t.sourceRevision),
    foreignKey({
      columns: [t.rawEvidenceId, t.attemptId],
      foreignColumns: [usageEventIndex.id, usageEventIndex.attemptId],
      name: "core_receipt_raw",
    }),
    foreignKey({
      columns: [t.canonicalEvidenceId, t.attemptId],
      foreignColumns: [usageEventIndex.id, usageEventIndex.attemptId],
      name: "core_receipt_canonical",
    }),
    foreignKey({
      columns: [t.disputedEvidenceId, t.attemptId],
      foreignColumns: [usageEventIndex.id, usageEventIndex.attemptId],
      name: "core_receipt_disputed",
    }),
    check(
      "core_receipt_source",
      sql`source IN ('execution','execution_outcome','recovery_interruption','async_partial','async_final','batch_completed','batch_cancelled','batch_failed','provider_invoice') AND source_revision>=1 AND body_digest ~ '^[a-f0-9]{64}$' AND origin_identity ~ '^gateway:[A-Za-z0-9_-]{1,100}$'`,
    ),
    check(
      "core_receipt_outcome",
      sql`outcome IN ('ACCEPTED','STALE','ESTIMATED','REVISION_REPLAY','CONFLICT','OVERFLOW') AND finality IN ('FINAL','PARTIAL','UNKNOWN','ESTIMATED','NOT_EXECUTED')
        AND (outcome<>'CONFLICT' OR (conflict_id IS NOT NULL AND disputed_evidence_id IS NOT NULL AND NOT revision_claim))
        AND (outcome<>'OVERFLOW' OR (numeric_fault='COST_OVERFLOW' AND disputed_evidence_id IS NOT NULL))
        AND (outcome NOT IN ('ACCEPTED','REVISION_REPLAY') OR (canonical_evidence_id IS NOT NULL AND disputed_evidence_id IS NULL AND conflict_id IS NULL))
        AND (numeric_fault IS NULL OR numeric_fault='COST_OVERFLOW')
        AND (attempt_state IS NULL OR attempt_state IN ('SUCCEEDED','FAILED','UNKNOWN','PARTIAL','NOT_EXECUTED'))
        AND (request_state IS NULL OR request_state IN ('SUCCEEDED','FAILED','CANCELLED','UNKNOWN'))
        AND (error_code IS NULL OR error_code ~ '^[a-z][a-z0-9_]{0,79}$')
        AND (provider_request_id IS NULL OR provider_request_id ~ '^[A-Za-z0-9._:/-]{1,256}$')
        AND cardinality(risk_dimensions)<=16`,
    ),
  ],
);
export const upstreamCostHeads = pgTable(
  "upstream_cost_heads",
  {
    attemptId: uuid("attempt_id").primaryKey(),
    revision: atoms("revision").notNull(),
  },
  (t) => [
    foreignKey({
      columns: [t.attemptId, t.revision],
      foreignColumns: [upstreamCosts.attemptId, upstreamCosts.revision],
    }),
  ],
);
export const upstreamJournals = pgTable(
  "upstream_journals",
  {
    id: uuid().primaryKey(),
    accountId: uuid("account_id")
      .notNull()
      .references(() => upstreamAccounts.id),
    channelId: uuid("channel_id").references((): AnyPgColumn => channels.id),
    attemptId: uuid("attempt_id").references(() => attempts.id),
    operationId: uuid("operation_id").references(() => upstreamFundingOperations.id),
    currency: text().notNull(),
    kind: text().notNull(),
    reference: text().notNull().unique(),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  (t) => [
    unique().on(t.id, t.currency),
    check("upstream_journal_currency", sql`currency ~ '^[A-Z]{3,8}$'`),
  ],
);
export const upstreamEntries = pgTable(
  "upstream_entries",
  {
    journalId: uuid("journal_id").notNull(),
    ordinal: integer().notNull(),
    currency: text().notNull(),
    account: text().notNull(),
    atoms: atoms("atoms").notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.journalId, t.ordinal] }),
    foreignKey({
      columns: [t.journalId, t.currency],
      foreignColumns: [upstreamJournals.id, upstreamJournals.currency],
    }),
    check("upstream_entry_ordinal", sql`ordinal>=0`),
  ],
);
export const balanceObservations = pgTable(
  "balance_observations",
  {
    id: uuid().primaryKey(),
    accountId: uuid("account_id")
      .notNull()
      .references(() => upstreamAccounts.id),
    channelId: uuid("channel_id").references((): AnyPgColumn => channels.id),
    bindingId: uuid("binding_id").references(() => channelPluginBindings.id),
    currency: text().notNull(),
    observedAtoms: atoms("observed_atoms"),
    observedAt: instant("observed_at").notNull(),
    source: text().notNull(),
    result: text().notNull(),
    code: text(),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  () => [
    check("observation_currency", sql`currency ~ '^[A-Z]{3,8}$'`),
    check("observation_result", sql`result IN ('OBSERVED','UNSUPPORTED','FAILED')`),
  ],
);
export const reconciliations = pgTable(
  "reconciliations",
  {
    id: uuid().primaryKey(),
    accountId: uuid("account_id")
      .notNull()
      .references(() => upstreamAccounts.id),
    currency: text().notNull(),
    revision: atoms("revision").notNull(),
    bookAtoms: atoms("book_atoms").notNull(),
    observedAtoms: atoms("observed_atoms"),
    invoiceAtoms: atoms("invoice_atoms"),
    differenceAtoms: atoms("difference_atoms").notNull(),
    voucherId: uuid("voucher_id").references(() => financialVouchers.id),
    adjustmentId: uuid("adjustment_id").references(() => upstreamFundingOperations.id),
    actor: text().notNull(),
    reason: text().notNull(),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  (t) => [
    unique().on(t.accountId, t.currency, t.revision),
    check("reconciliation_revision", sql`revision>=0 AND currency ~ '^[A-Z]{3,8}$'`),
  ],
);
export const upstreamAuditEvents = pgTable("upstream_audit_events", {
  id: uuid().primaryKey(),
  accountId: uuid("account_id").references(() => upstreamAccounts.id),
  channelId: uuid("channel_id").references((): AnyPgColumn => channels.id),
  actor: text().notNull(),
  action: text().notNull(),
  operationId: uuid("operation_id").references(() => upstreamFundingOperations.id),
  revision: atoms("revision"),
  oldDigest: text("old_digest"),
  newDigest: text("new_digest"),
  reason: text().notNull(),
  createdAt: instant("created_at").defaultNow().notNull(),
});

export const executionContexts = pgTable("execution_contexts", {
  id: uuid().primaryKey(),
  projectId: uuid("project_id")
    .notNull()
    .references(() => projects.id),
  keyId: uuid("key_id")
    .notNull()
    .references(() => apiKeys.id),
  ownerSessionId: uuid("owner_session_id")
    .notNull()
    .references(() => internalStartupSessions.sessionId),
  operation: text().notNull(),
  model: text().notNull(),
  ip: text().notNull(),
  fingerprint: text().notNull(),
  snapshot: jsonb().notNull(),
  expiresAt: instant("expires_at").notNull(),
  createdAt: instant("created_at").defaultNow().notNull(),
});
export const executionQuotaLeases = pgTable(
  "execution_quota_leases",
  {
    leaseId: text("lease_id").primaryKey(),
    attemptId: uuid("attempt_id")
      .notNull()
      .references(() => attempts.id),
    commandId: uuid("command_id")
      .notNull()
      .references(() => executionCommands.id),
    requestId: uuid("request_id")
      .notNull()
      .references(() => gatewayRequests.id),
    scopeKind: text("scope_kind").notNull(),
    scopeId: text("scope_id").notNull(),
    state: text().notNull(),
    revision: atoms("revision").default("0").notNull(),
    descriptor: jsonb().notNull(),
  },
  (t) => [
    unique().on(t.attemptId, t.scopeKind, t.scopeId),
    uniqueIndex("execution_request_common_quota_unique")
      .on(t.requestId, t.scopeKind, t.scopeId)
      .where(sql`scope_kind<>'channel'`),
    check(
      "execution_quota_state",
      sql`state IN ('ADOPTION_PENDING','ACTIVE','RELEASE_PENDING','RELEASED','EXPIRED')`,
    ),
  ],
);
