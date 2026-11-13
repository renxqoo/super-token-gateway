import { channels, channelPluginBindings } from "./catalog.ts";
import {
  pgTable,
  integer,
  timestamp,
  index,
  foreignKey,
  unique,
  check,
  uuid,
  text,
  boolean,
  bigint,
  doublePrecision,
  numeric,
  type PgTableExtraConfigValue,
  primaryKey,
  pgView,
  uniqueIndex,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { jsonb } from "./jsonb.ts";

export const schemaMigrations = pgTable("schema_migrations", {
  version: integer().primaryKey().notNull(),
  checksum: text().notNull(),
  manifest: jsonb().notNull(),
  appliedAt: timestamp("applied_at", { withTimezone: true, mode: "string" }).defaultNow().notNull(),
});

export const apiKeys = pgTable(
  "api_keys",
  {
    id: uuid().primaryKey().notNull(),
    projectId: uuid("project_id").notNull(),
    name: text().notNull(),
    verifier: text().notNull(),
    prefix: text().notNull(),
    models: text().array().notNull(),
    operations: text().array().notNull(),
    enabled: boolean().default(true).notNull(),
    expiresAt: timestamp("expires_at", { withTimezone: true, mode: "string" }),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    principalId: uuid("principal_id"),
    role: text().default("INVOKER").notNull(),
    allowedCidrs: text("allowed_cidrs")
      .array()
      .default(sql`'{}'::text[]`)
      .notNull(),
    rpm: integer(),
    tpm: bigint({ mode: "bigint" }),
    concurrency: integer(),
  },
  (table): PgTableExtraConfigValue[] => [
    index("api_keys_project").using(
      "btree",
      table.projectId.asc().nullsLast(),
      table.createdAt.asc().nullsLast(),
    ),
    foreignKey({
      columns: [table.principalId],
      foreignColumns: [principals.id],
      name: "api_keys_principal_id_fkey",
    }),
    foreignKey({
      columns: [table.projectId],
      foreignColumns: [projects.id],
      name: "api_keys_project_id_fkey",
    }),
    unique("api_keys_verifier_key").on(table.verifier),
    check("api_keys_concurrency_check", sql`concurrency > 0`),
    check(
      "api_keys_role_check",
      sql`role = ANY (ARRAY['ADMIN'::text, 'INVOKER'::text, 'BILLING'::text, 'VIEWER'::text])`,
    ),
    check("api_keys_rpm_check", sql`rpm > 0`),
    check("api_keys_tpm_check", sql`(tpm > 0) AND (tpm <= '9007199254740991'::bigint)`),
  ],
);

export const billingPrices = pgTable(
  "billing_prices",
  {
    id: uuid().primaryKey().notNull(),
    currency: text().notNull(),
    rates: jsonb().notNull(),
    ceilings: jsonb().notNull(),
    basis: text().default("PROVIDER_USAGE").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    unique("billing_price_currency_unique").on(table.id, table.currency),
    check(
      "billing_prices_basis_check",
      sql`basis = ANY (ARRAY['PROVIDER_USAGE'::text, 'INPUT_CHARACTERS'::text, 'REQUEST_COMPLETION'::text])`,
    ),
    check("billing_prices_ceilings_check", sql`jsonb_typeof(ceilings) = 'object'::text`),
    check("billing_prices_currency_check", sql`currency ~ '^[A-Z]{3,8}$'::text`),
    check("billing_prices_rates_check", sql`jsonb_typeof(rates) = 'object'::text`),
  ],
);

export const billingTariffs = pgTable(
  "billing_tariffs",
  {
    id: uuid().primaryKey().notNull(),
    model: text().notNull(),
    operation: text().notNull(),
    providerId: uuid("provider_id"),
    currency: text().notNull(),
    pricingId: uuid("pricing_id").notNull(),
    enabled: boolean().default(true).notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    foreignKey({
      columns: [table.model],
      foreignColumns: [models.name],
      name: "billing_tariffs_model_fkey",
    }),
    foreignKey({
      columns: [table.pricingId],
      foreignColumns: [billingPrices.id],
      name: "billing_tariffs_pricing_id_fkey",
    }),
    foreignKey({
      columns: [table.providerId],
      foreignColumns: [channels.id],
      name: "billing_tariffs_provider_id_fkey",
    }),
    foreignKey({
      columns: [table.pricingId, table.currency],
      foreignColumns: [billingPrices.id, billingPrices.currency],
      name: "tariff_currency_matches",
    }),
    unique("billing_tariffs_model_operation_provider_id_currency_key")
      .on(table.model, table.operation, table.providerId, table.currency)
      .nullsNotDistinct(),
    check("billing_tariffs_currency_check", sql`currency ~ '^[A-Z]{3,8}$'::text`),
  ],
);

export const costVersions = pgTable(
  "cost_versions",
  {
    id: uuid().primaryKey().notNull(),
    currency: text().notNull(),
    rates: jsonb().notNull(),
    ceilings: jsonb().default({}).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
  },
  (): PgTableExtraConfigValue[] => [
    check("cost_versions_currency_check", sql`currency ~ '^[A-Z]{3,8}$'::text`),
    check("cost_versions_ceilings_check", sql`jsonb_typeof(ceilings)='object'`),
    check("cost_versions_rates_check", sql`jsonb_typeof(rates) = 'object'::text`),
  ],
);

export const eventConsumers = pgTable(
  "event_consumers",
  {
    id: uuid().primaryKey().notNull(),
    name: text().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    replayCursor: bigint("replay_cursor", { mode: "bigint" })
      .default(sql`0`)
      .notNull(),
    replayUntil: bigint("replay_until", { mode: "bigint" })
      .default(sql`0`)
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [unique("event_consumers_name_key").on(table.name)],
);

export const models = pgTable(
  "models",
  {
    name: text().primaryKey().notNull(),
    operations: text().array().notNull(),
    enabled: boolean().default(true).notNull(),
    maxOutputTokens: integer("max_output_tokens").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    routingStrategy: text("routing_strategy").default("WEIGHTED").notNull(),
  },
  (): PgTableExtraConfigValue[] => [
    check("models_max_output_tokens_check", sql`max_output_tokens > 0`),
    check(
      "models_routing_strategy_check",
      sql`routing_strategy = ANY (ARRAY['WEIGHTED'::text, 'ROUND_ROBIN'::text, 'LEAST_LATENCY'::text])`,
    ),
  ],
);

export const organizations = pgTable(
  "organizations",
  {
    id: uuid().primaryKey().notNull(),
    externalRef: text("external_ref").notNull(),
    name: text().notNull(),
    enabled: boolean().default(true).notNull(),
    rpm: integer().default(100000).notNull(),
    tpm: bigint({ mode: "bigint" })
      .default(sql`1000000000`)
      .notNull(),
    concurrency: integer().default(10000).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    unique("organizations_external_ref_key").on(table.externalRef),
    check("organizations_concurrency_check", sql`concurrency > 0`),
    check("organizations_rpm_check", sql`rpm > 0`),
    check("organizations_tpm_check", sql`(tpm > 0) AND (tpm <= '9007199254740991'::bigint)`),
  ],
);

export const principals = pgTable(
  "principals",
  {
    id: uuid().primaryKey().notNull(),
    organizationId: uuid("organization_id").notNull(),
    externalRef: text("external_ref").notNull(),
    role: text().notNull(),
    enabled: boolean().default(true).notNull(),
    rpm: integer().default(10000).notNull(),
    tpm: bigint({ mode: "bigint" })
      .default(sql`100000000`)
      .notNull(),
    concurrency: integer().default(1000).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    foreignKey({
      columns: [table.organizationId],
      foreignColumns: [organizations.id],
      name: "principals_organization_id_fkey",
    }),
    unique("principals_organization_id_external_ref_key").on(
      table.organizationId,
      table.externalRef,
    ),
    check("principals_concurrency_check", sql`concurrency > 0`),
    check(
      "principals_role_check",
      sql`role = ANY (ARRAY['OWNER'::text, 'ADMIN'::text, 'MEMBER'::text, 'BILLING'::text, 'VIEWER'::text])`,
    ),
    check("principals_rpm_check", sql`rpm > 0`),
    check("principals_tpm_check", sql`(tpm > 0) AND (tpm <= '9007199254740991'::bigint)`),
  ],
);

export const projects = pgTable(
  "projects",
  {
    id: uuid().primaryKey().notNull(),
    name: text().notNull(),
    externalRef: text("external_ref"),
    enabled: boolean().default(true).notNull(),
    rpm: integer().notNull(),
    tpm: integer().notNull(),
    concurrency: integer().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    billingCurrency: text("billing_currency").default("USD").notNull(),
    organizationId: uuid("organization_id"),
  },
  (table): PgTableExtraConfigValue[] => [
    foreignKey({
      columns: [table.organizationId],
      foreignColumns: [organizations.id],
      name: "projects_organization_id_fkey",
    }),
    unique("projects_external_ref_key").on(table.externalRef),
    check("projects_billing_currency_check", sql`billing_currency ~ '^[A-Z]{3,8}$'::text`),
    check("projects_concurrency_check", sql`concurrency > 0`),
    check("projects_rpm_check", sql`rpm > 0`),
    check("projects_tpm_check", sql`tpm > 0`),
  ],
);

export const routes = pgTable(
  "routes",
  {
    id: uuid().primaryKey().notNull(),
    requestPolicy: jsonb("request_policy").default({}).notNull(),
    modelName: text("model_name").notNull(),
    providerId: uuid("provider_id").notNull(),
    upstreamModel: text("upstream_model").notNull(),
    operations: text().array().notNull(),
    priority: integer().notNull(),
    weight: doublePrecision().notNull(),
    pricingId: uuid("pricing_id").notNull(),
    enabled: boolean().default(true).notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    index("routes_lookup")
      .using("btree", table.modelName.asc().nullsLast(), table.priority.desc().nullsFirst())
      .where(sql`enabled`),
    foreignKey({
      columns: [table.modelName],
      foreignColumns: [models.name],
      name: "routes_model_name_fkey",
    }),
    foreignKey({
      columns: [table.pricingId],
      foreignColumns: [costVersions.id],
      name: "routes_pricing_id_fkey",
    }),
    foreignKey({
      columns: [table.providerId],
      foreignColumns: [channels.id],
      name: "routes_provider_id_fkey",
    }),
    unique("routes_binding_unique").on(
      table.modelName,
      table.providerId,
      table.upstreamModel,
      table.operations,
    ),
    check("routes_request_policy_check", sql`jsonb_typeof(request_policy) = 'object'::text`),
    check("routes_weight_check", sql`weight > (0)::double precision`),
  ],
);

export const resources = pgTable(
  "resources",
  {
    bindingId: uuid("binding_id").references(() => channelPluginBindings.id),
    id: text().primaryKey().notNull(),
    projectId: uuid("project_id").notNull(),
    providerId: uuid("provider_id").notNull(),
    remoteId: text("remote_id").notNull(),
    kind: text().notNull(),
    model: text(),
    state: text().default("ACTIVE").notNull(),
    attemptId: uuid("attempt_id"),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    nextPollAt: timestamp("next_poll_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    pollToken: uuid("poll_token"),
    pollUntil: timestamp("poll_until", { withTimezone: true, mode: "string" }),
    batchEndpoint: text("batch_endpoint"),
    batchUpstreamModel: text("batch_upstream_model"),
    batchManifest: jsonb("batch_manifest"),
    pollCount: integer("poll_count").default(0).notNull(),
    upstreamState: text("upstream_state"),
  },
  (table): PgTableExtraConfigValue[] => [
    index("resources_poll")
      .using("btree", table.nextPollAt.asc().nullsLast(), table.id.asc().nullsLast())
      .where(
        sql`((state = ANY (ARRAY['ACTIVE'::text, 'PENDING_USAGE'::text])) AND (kind = ANY (ARRAY['batch'::text, 'video'::text, 'response'::text, 'operation'::text])))`,
      ),
    index("resources_project").using(
      "btree",
      table.projectId.asc().nullsLast(),
      table.kind.asc().nullsLast(),
      table.createdAt.desc().nullsFirst(),
      table.id.asc().nullsLast(),
    ),
    foreignKey({
      columns: [table.attemptId],
      foreignColumns: [attempts.id],
      name: "resources_attempt_id_fkey",
    }),
    foreignKey({
      columns: [table.projectId],
      foreignColumns: [projects.id],
      name: "resources_project_id_fkey",
    }),
    foreignKey({
      columns: [table.providerId],
      foreignColumns: [channels.id],
      name: "resources_provider_id_fkey",
    }),
    unique("resources_project_id_provider_id_kind_remote_id_key").on(
      table.projectId,
      table.providerId,
      table.remoteId,
      table.kind,
    ),
    check(
      "resource_opaque_identity",
      sql`length(remote_id) BETWEEN 1 AND 500 AND remote_id ~ '^[A-Za-z0-9_:/.-]+$' AND remote_id !~ '://' AND remote_id !~ '(^|/)[.]{1,2}(/|$)'`,
    ),
    check(
      "resource_batch_metadata",
      sql`batch_manifest IS NULL OR (jsonb_typeof(batch_manifest)='object' AND batch_manifest-ARRAY['items','idsSha256','images']='{}'::jsonb AND batch_manifest ?& ARRAY['items','idsSha256'] AND jsonb_typeof(batch_manifest->'items')='number' AND batch_manifest->>'items' ~ '^[1-9][0-9]{0,15}$' AND (batch_manifest->>'items')::numeric<=9007199254740991 AND jsonb_typeof(batch_manifest->'idsSha256')='string' AND batch_manifest->>'idsSha256' ~ '^[a-f0-9]{64}$' AND (NOT batch_manifest?'images' OR jsonb_typeof(batch_manifest->'images')='number' AND batch_manifest->>'images' ~ '^[1-9][0-9]{0,15}$' AND (batch_manifest->>'images')::numeric<=9007199254740991)) IS TRUE`,
    ),
    check(
      "resources_kind_check",
      sql`kind = ANY (ARRAY['file'::text, 'batch'::text, 'video'::text, 'response'::text, 'operation'::text])`,
    ),
    check(
      "resources_upstream_state_check",
      sql`upstream_state = ANY (ARRAY['queued'::text, 'pending'::text, 'in_progress'::text, 'running'::text, 'completed'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text, 'canceled'::text, 'cancelling'::text, 'validating'::text, 'finalizing'::text, 'expired'::text, 'processed'::text])`,
    ),
  ],
);

export const billingAdjustments = pgTable(
  "billing_adjustments",
  {
    id: uuid().primaryKey().notNull(),
    chargeId: uuid("charge_id").notNull(),
    externalRef: text("external_ref").notNull(),
    fingerprint: text().notNull(),
    targetAtoms: numeric("target_atoms", { precision: 38, scale: 0 }).notNull(),
    deltaAtoms: numeric("delta_atoms", { precision: 38, scale: 0 }).notNull(),
    reason: text().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    foreignKey({
      columns: [table.chargeId],
      foreignColumns: [billingCharges.id],
      name: "billing_adjustments_charge_id_fkey",
    }),
    unique("billing_adjustments_charge_id_external_ref_key").on(table.chargeId, table.externalRef),
    check(
      "billing_adjustments_reason_check",
      sql`reason = ANY (ARRAY['ADMIN_CORRECTION'::text, 'PROVIDER_RECONCILIATION'::text])`,
    ),
    check("billing_adjustments_target_atoms_check", sql`target_atoms >= (0)::numeric`),
  ],
);

export const auditEvents = pgTable("audit_events", {
  id: uuid().primaryKey().notNull(),
  action: text().notNull(),
  resourceId: text("resource_id").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).defaultNow().notNull(),
  actor: text(),
  details: jsonb(),
});

export const usageConflicts = pgTable(
  "usage_conflicts",
  {
    id: uuid().primaryKey().notNull(),
    attemptId: uuid("attempt_id").notNull(),
    existingEventId: uuid("existing_event_id").notNull(),
    source: text().notNull(),
    finality: text().notNull(),
    usage: jsonb().notNull(),
    fingerprint: text().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    incomingEvidenceId: uuid("incoming_evidence_id"),
    disputedEvidenceId: uuid("disputed_evidence_id"),
    incomingReceiptId: uuid("incoming_receipt_id"),
  },
  (table): PgTableExtraConfigValue[] => [
    foreignKey({
      columns: [table.incomingEvidenceId, table.attemptId],
      foreignColumns: [usageEventIndex.id, usageEventIndex.attemptId],
      name: "usage_conflict_incoming",
    }),
    foreignKey({
      columns: [table.disputedEvidenceId, table.attemptId],
      foreignColumns: [usageEventIndex.id, usageEventIndex.attemptId],
      name: "usage_conflict_disputed",
    }),
    check(
      "usage_conflict_core_links",
      sql`(incoming_evidence_id IS NULL AND disputed_evidence_id IS NULL AND incoming_receipt_id IS NULL) OR (incoming_evidence_id IS NOT NULL AND disputed_evidence_id IS NOT NULL AND incoming_receipt_id IS NOT NULL)`,
    ),
    index("usage_conflicts_attempt").using("btree", table.attemptId.asc().nullsLast()),
    foreignKey({
      columns: [table.attemptId],
      foreignColumns: [attempts.id],
      name: "usage_conflicts_attempt_id_fkey",
    }),
    foreignKey({
      columns: [table.existingEventId],
      foreignColumns: [usageEventIndex.id],
      name: "usage_conflicts_existing_event_id_fkey",
    }),
    unique("usage_conflicts_attempt_id_source_fingerprint_key").on(
      table.attemptId,
      table.source,
      table.fingerprint,
    ),
  ],
);

export const fundingOperations = pgTable(
  "funding_operations",
  {
    id: uuid().primaryKey().notNull(),
    walletId: uuid("wallet_id").notNull(),
    externalRef: text("external_ref").notNull(),
    fingerprint: text().notNull(),
    kind: text().notNull(),
    amountAtoms: numeric("amount_atoms", { precision: 38, scale: 0 }).notNull(),
    lotId: uuid("lot_id"),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    foreignKey({
      columns: [table.lotId],
      foreignColumns: [walletLots.id],
      name: "funding_operations_lot_id_fkey",
    }),
    foreignKey({
      columns: [table.walletId],
      foreignColumns: [wallets.id],
      name: "funding_operations_wallet_id_fkey",
    }),
    unique("funding_operations_wallet_id_external_ref_key").on(table.walletId, table.externalRef),
    check("funding_operations_amount_atoms_check", sql`amount_atoms > (0)::numeric`),
    check("funding_operations_kind_check", sql`kind = ANY (ARRAY['CREDIT'::text, 'REFUND'::text])`),
  ],
);

export const billingRefunds = pgTable(
  "billing_refunds",
  {
    id: uuid().primaryKey().notNull(),
    chargeId: uuid("charge_id").notNull(),
    externalRef: text("external_ref").notNull(),
    amountAtoms: numeric("amount_atoms", { precision: 38, scale: 0 }).notNull(),
    fingerprint: text().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    reason: text().default("CUSTOMER_REFUND").notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    foreignKey({
      columns: [table.chargeId],
      foreignColumns: [billingCharges.id],
      name: "billing_refunds_charge_id_fkey",
    }),
    unique("billing_refunds_charge_id_external_ref_key").on(table.chargeId, table.externalRef),
    check("billing_refunds_amount_atoms_check", sql`amount_atoms > (0)::numeric`),
    check(
      "billing_refunds_reason_check",
      sql`reason = ANY (ARRAY['CUSTOMER_REFUND'::text, 'ADMIN_CORRECTION'::text, 'PROVIDER_RECONCILIATION'::text])`,
    ),
  ],
);

export const billingResolutions = pgTable(
  "billing_resolutions",
  {
    id: uuid().primaryKey().notNull(),
    requestId: uuid("request_id").notNull(),
    externalRef: text("external_ref").notNull(),
    fingerprint: text().notNull(),
    action: text().notNull(),
    usage: jsonb().notNull(),
    reason: text().notNull(),
    pricingId: uuid("pricing_id"),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    seq: bigint({ mode: "bigint" }).generatedAlwaysAsIdentity({
      name: "billing_resolutions_seq_seq",
    }),
  },
  (table): PgTableExtraConfigValue[] => [
    foreignKey({
      columns: [table.pricingId],
      foreignColumns: [billingPrices.id],
      name: "billing_resolutions_pricing_id_fkey",
    }),
    foreignKey({
      columns: [table.requestId],
      foreignColumns: [gatewayRequests.id],
      name: "billing_resolutions_request_id_fkey",
    }),
    unique("billing_resolutions_request_id_external_ref_key").on(
      table.requestId,
      table.externalRef,
    ),
    unique("billing_resolutions_seq_key").on(table.seq),
    check(
      "billing_resolutions_action_check",
      sql`action = ANY (ARRAY['RELEASE'::text, 'SETTLE'::text])`,
    ),
    check(
      "billing_resolutions_reason_check",
      sql`reason = ANY (ARRAY['PROVIDER_INVOICE'::text, 'ADMIN_WRITE_OFF'::text, 'EVIDENCE_RECONCILIATION'::text])`,
    ),
  ],
);

export const attempts = pgTable(
  "attempts",
  {
    bindingId: uuid("binding_id").references(() => channelPluginBindings.id),
    id: uuid().primaryKey().notNull(),
    requestId: uuid("request_id").notNull(),
    ordinal: integer().notNull(),
    providerId: uuid("provider_id").notNull(),
    upstreamModel: text("upstream_model").notNull(),
    pricingId: uuid("pricing_id").notNull(),
    state: text().notNull(),
    providerRequestId: text("provider_request_id"),
    errorCode: text("error_code"),
    startedAt: timestamp("started_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    finishedAt: timestamp("finished_at", { withTimezone: true, mode: "string" }),
    evidenceId: uuid("evidence_id"),
    meteringConflict: boolean("metering_conflict").default(false).notNull(),
    billingPricingId: uuid("billing_pricing_id"),
  },
  (table): PgTableExtraConfigValue[] => [
    index("attempts_stale")
      .using("btree", table.startedAt.asc().nullsLast())
      .where(sql`(state = 'DISPATCHING'::text)`),
    foreignKey({
      columns: [table.billingPricingId],
      foreignColumns: [billingPrices.id],
      name: "attempts_billing_pricing_id_fkey",
    }),
    foreignKey({
      columns: [table.evidenceId],
      foreignColumns: [usageEventIndex.id],
      name: "attempts_evidence_id_fkey",
    }),
    foreignKey({
      columns: [table.pricingId],
      foreignColumns: [costVersions.id],
      name: "attempts_pricing_id_fkey",
    }),
    foreignKey({
      columns: [table.providerId],
      foreignColumns: [channels.id],
      name: "attempts_provider_id_fkey",
    }),
    foreignKey({
      columns: [table.requestId],
      foreignColumns: [gatewayRequests.id],
      name: "attempts_request_id_fkey",
    }),
    unique("attempts_request_id_ordinal_key").on(table.requestId, table.ordinal),
    check("attempts_error_code_check", sql`error_code ~ '^[a-z0-9_]{1,80}$'::text`),
    check("attempts_ordinal_check", sql`ordinal > 0`),
    check(
      "attempts_state_check",
      sql`state = ANY (ARRAY['DISPATCHING'::text, 'SUCCEEDED'::text, 'NOT_EXECUTED'::text, 'UNKNOWN'::text, 'PARTIAL'::text, 'FAILED'::text])`,
    ),
  ],
);

export const billingCharges = pgTable(
  "billing_charges",
  {
    id: uuid().primaryKey().notNull(),
    requestId: uuid("request_id").notNull(),
    holdId: uuid("hold_id").notNull(),
    pricingId: uuid("pricing_id"),
    evidenceId: uuid("evidence_id"),
    currency: text().notNull(),
    amountAtoms: numeric("amount_atoms", { precision: 38, scale: 0 }).notNull(),
    fundedAtoms: numeric("funded_atoms", { precision: 38, scale: 0 }).notNull(),
    debtAtoms: numeric("debt_atoms", { precision: 38, scale: 0 }).notNull(),
    refundedAtoms: numeric("refunded_atoms", { precision: 38, scale: 0 }).default("0").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    debtRefundedAtoms: numeric("debt_refunded_atoms", { precision: 38, scale: 0 })
      .default("0")
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    index("charges_unpaid")
      .using(
        "btree",
        table.holdId.asc().nullsLast(),
        table.createdAt.asc().nullsLast(),
        table.id.asc().nullsLast(),
      )
      .where(sql`(debt_atoms > (0)::numeric)`),
    foreignKey({
      columns: [table.evidenceId],
      foreignColumns: [usageEventIndex.id],
      name: "billing_charges_evidence_id_fkey",
    }),
    foreignKey({
      columns: [table.holdId],
      foreignColumns: [billingHolds.id],
      name: "billing_charges_hold_id_fkey",
    }),
    foreignKey({
      columns: [table.pricingId],
      foreignColumns: [billingPrices.id],
      name: "billing_charges_pricing_id_fkey",
    }),
    foreignKey({
      columns: [table.requestId],
      foreignColumns: [gatewayRequests.id],
      name: "billing_charges_request_id_fkey",
    }),
    unique("billing_charges_request_id_key").on(table.requestId),
    unique("billing_charges_hold_id_key").on(table.holdId),
    check("billing_charges_amount_atoms_check", sql`amount_atoms >= (0)::numeric`),
    check(
      "billing_charges_check",
      sql`(refunded_atoms >= (0)::numeric) AND (refunded_atoms <= amount_atoms)`,
    ),
    check("billing_charges_debt_atoms_check", sql`debt_atoms >= (0)::numeric`),
    check("billing_charges_debt_refunded_atoms_check", sql`debt_refunded_atoms >= (0)::numeric`),
    check("billing_charges_funded_atoms_check", sql`funded_atoms >= (0)::numeric`),
    check(
      "charge_value_conserved",
      sql`(amount_atoms = ((funded_atoms + debt_atoms) + debt_refunded_atoms)) AND (refunded_atoms >= debt_refunded_atoms) AND ((refunded_atoms - debt_refunded_atoms) <= funded_atoms)`,
    ),
  ],
);

export const walletLots = pgTable(
  "wallet_lots",
  {
    id: uuid().primaryKey().notNull(),
    walletId: uuid("wallet_id").notNull(),
    kind: text().notNull(),
    bucketDigest: text("bucket_digest").notNull(),
    priority: integer().default(100).notNull(),
    expiresAt: timestamp("expires_at", { withTimezone: true, mode: "string" }),
    models: text()
      .array()
      .default(sql`'{}'::text[]`)
      .notNull(),
    operations: text()
      .array()
      .default(sql`'{}'::text[]`)
      .notNull(),
    issuedAtoms: numeric("issued_atoms", { precision: 38, scale: 0 }).notNull(),
    availableAtoms: numeric("available_atoms", { precision: 38, scale: 0 }).default("0").notNull(),
    reservedAtoms: numeric("reserved_atoms", { precision: 38, scale: 0 }).default("0").notNull(),
    spentAtoms: numeric("spent_atoms", { precision: 38, scale: 0 }).default("0").notNull(),
    expiredAtoms: numeric("expired_atoms", { precision: 38, scale: 0 }).default("0").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    index("wallet_lots_expiring")
      .using("btree", table.expiresAt.asc().nullsLast(), table.id.asc().nullsLast())
      .where(sql`(kind = 'GIFT'::text)`),
    index("wallet_lots_wallet").using(
      "btree",
      table.walletId.asc().nullsLast(),
      table.kind.asc().nullsLast(),
      table.priority.asc().nullsLast(),
      table.expiresAt.asc().nullsLast(),
      table.id.asc().nullsLast(),
    ),
    foreignKey({
      columns: [table.walletId],
      foreignColumns: [wallets.id],
      name: "wallet_lots_wallet_id_fkey",
    }),
    unique("wallet_lots_wallet_id_bucket_digest_key").on(table.walletId, table.bucketDigest),
    check("wallet_lots_available_atoms_check", sql`available_atoms >= (0)::numeric`),
    check(
      "wallet_lots_check",
      sql`issued_atoms = (((available_atoms + reserved_atoms) + spent_atoms) + expired_atoms)`,
    ),
    check("wallet_lots_check1", sql`(kind = 'GIFT'::text) OR (expires_at IS NULL)`),
    check("wallet_lots_expired_atoms_check", sql`expired_atoms >= (0)::numeric`),
    check("wallet_lots_issued_atoms_check", sql`issued_atoms >= (0)::numeric`),
    check("wallet_lots_kind_check", sql`kind = ANY (ARRAY['CASH'::text, 'GIFT'::text])`),
    check("wallet_lots_reserved_atoms_check", sql`reserved_atoms >= (0)::numeric`),
    check("wallet_lots_spent_atoms_check", sql`spent_atoms >= (0)::numeric`),
  ],
);

export const gatewayRequests = pgTable(
  "gateway_requests",
  {
    id: uuid().primaryKey().notNull(),
    projectId: uuid("project_id").notNull(),
    keyId: uuid("key_id").notNull(),
    operation: text().notNull(),
    model: text().notNull(),
    intentDigest: text("intent_digest"),
    fingerprint: text().notNull(),
    state: text().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    billingCurrency: text("billing_currency"),
    billingQuote: jsonb("billing_quote"),
    executionBudget: jsonb("execution_budget"),
    executionClosedAt: timestamp("execution_closed_at", { withTimezone: true, mode: "string" }),
    selectedAttemptId: uuid("selected_attempt_id"),
    billingOutcome: text("billing_outcome").default("PENDING").notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    index("requests_project").using(
      "btree",
      table.projectId.asc().nullsLast(),
      table.createdAt.desc().nullsFirst(),
      table.id.asc().nullsLast(),
    ),
    foreignKey({
      columns: [table.keyId],
      foreignColumns: [apiKeys.id],
      name: "gateway_requests_key_id_fkey",
    }),
    foreignKey({
      columns: [table.projectId],
      foreignColumns: [projects.id],
      name: "gateway_requests_project_id_fkey",
    }),
    foreignKey({
      columns: [table.selectedAttemptId],
      foreignColumns: [attempts.id],
      name: "gateway_requests_selected_attempt_id_fkey",
    }),
    unique("gateway_requests_project_intent_unique").on(table.projectId, table.intentDigest),
    unique("gateway_requests_project_id_key_id_operation_intent_digest_key").on(
      table.projectId,
      table.keyId,
      table.operation,
      table.intentDigest,
    ),
    check(
      "gateway_requests_billing_outcome_check",
      sql`billing_outcome = ANY (ARRAY['PENDING'::text, 'SUCCESS'::text, 'CANCELLED'::text, 'FAILED'::text, 'UNKNOWN'::text])`,
    ),
    check(
      "gateway_requests_state_check",
      sql`state = ANY (ARRAY['ADMITTED'::text, 'RUNNING'::text, 'SUCCEEDED'::text, 'FAILED'::text, 'UNKNOWN'::text, 'CANCELLED'::text])`,
    ),
  ],
);

export const usageEventIndex = pgTable(
  "usage_event_index",
  {
    id: uuid().primaryKey().notNull(),
    seq: bigint({ mode: "bigint" }).generatedAlwaysAsIdentity({ name: "usage_events_seq_seq" }),
    projectId: uuid("project_id").notNull(),
    requestId: uuid("request_id").notNull(),
    attemptId: uuid("attempt_id").notNull(),
    source: text().notNull(),
    finality: text().notNull(),
    costAtoms: numeric("cost_atoms", { precision: 38, scale: 0 }),
    currency: text().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    costComplete: boolean("cost_complete").default(false).notNull(),
    recordKind: text("record_kind").default("GATEWAY").notNull(),
    snapshotRevision: numeric("snapshot_revision", { precision: 38, scale: 0 }),
    snapshotDigest: text("snapshot_digest"),
    finalDimensions: text("final_dimensions").array(),
    parentEvidenceId: uuid("parent_evidence_id"),
    inputFactId: uuid("input_fact_id"),
  },
  (table): PgTableExtraConfigValue[] => [
    index("usage_archive_range").using(
      "btree",
      table.createdAt.asc().nullsLast(),
      table.id.asc().nullsLast(),
    ),
    index("usage_project").using(
      "btree",
      table.projectId.asc().nullsLast(),
      table.createdAt.desc().nullsFirst(),
      table.id.asc().nullsLast(),
    ),
    foreignKey({
      columns: [table.attemptId],
      foreignColumns: [attempts.id],
      name: "usage_events_attempt_id_fkey",
    }),
    foreignKey({
      columns: [table.projectId],
      foreignColumns: [projects.id],
      name: "usage_events_project_id_fkey",
    }),
    foreignKey({
      columns: [table.requestId],
      foreignColumns: [gatewayRequests.id],
      name: "usage_events_request_id_fkey",
    }),
    foreignKey({
      columns: [table.id, table.createdAt],
      foreignColumns: [usagePayloads.id, usagePayloads.createdAt],
      name: "usage_index_payload",
    }),
    unique("usage_index_time").on(table.id, table.createdAt),
    unique("usage_events_seq_key").on(table.seq),
    unique("usage_index_attempt_identity").on(table.id, table.attemptId),
    uniqueIndex("usage_gateway_source_unique")
      .on(table.attemptId, table.source)
      .where(sql`record_kind='GATEWAY'`),
    uniqueIndex("usage_snapshot_revision_unique")
      .on(table.attemptId, table.snapshotRevision)
      .where(sql`snapshot_revision IS NOT NULL`),
    index("usage_attempt_source").on(table.attemptId, table.source, table.seq),
    foreignKey({
      columns: [table.parentEvidenceId, table.attemptId],
      foreignColumns: [table.id, table.attemptId],
      name: "usage_parent_evidence",
    }),
    foreignKey({
      columns: [table.inputFactId, table.attemptId],
      foreignColumns: [table.id, table.attemptId],
      name: "usage_input_fact",
    }),
    check("usage_events_cost_atoms_check", sql`cost_atoms >= (0)::numeric`),
    check(
      "usage_record_kind",
      sql`record_kind IN ('GATEWAY','RAW','CANONICAL','DISPUTED') AND CASE record_kind
        WHEN 'GATEWAY' THEN cost_atoms IS NOT NULL AND snapshot_revision IS NULL AND snapshot_digest IS NULL AND final_dimensions IS NULL AND parent_evidence_id IS NULL AND input_fact_id IS NULL
        WHEN 'RAW' THEN cost_atoms IS NULL AND NOT cost_complete AND snapshot_revision>=1 AND snapshot_digest ~ '^[a-f0-9]{64}$' AND final_dimensions IS NOT NULL AND parent_evidence_id IS NULL AND input_fact_id IS NULL
        WHEN 'CANONICAL' THEN cost_atoms IS NOT NULL AND snapshot_revision>=1 AND snapshot_digest ~ '^[a-f0-9]{64}$' AND final_dimensions IS NOT NULL AND input_fact_id IS NOT NULL
        ELSE snapshot_revision>=1 AND snapshot_digest ~ '^[a-f0-9]{64}$' AND final_dimensions IS NOT NULL AND input_fact_id IS NOT NULL AND (cost_atoms IS NOT NULL OR NOT cost_complete) END`,
    ),
    check(
      "usage_events_finality_check",
      sql`finality = ANY (ARRAY['FINAL'::text, 'PARTIAL'::text, 'UNKNOWN'::text, 'ESTIMATED'::text])`,
    ),
  ],
);

export const moneyEntries = pgTable(
  "money_entries",
  {
    id: uuid().primaryKey().notNull(),
    walletId: uuid("wallet_id").notNull(),
    reference: text().notNull(),
    kind: text().notNull(),
    currency: text().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    index("journal_archive_range").using(
      "btree",
      table.createdAt.asc().nullsLast(),
      table.id.asc().nullsLast(),
    ),
    index("journal_wallet").using(
      "btree",
      table.walletId.asc().nullsLast(),
      table.createdAt.desc().nullsFirst(),
      table.id.asc().nullsLast(),
    ),
    foreignKey({
      columns: [table.walletId],
      foreignColumns: [wallets.id],
      name: "money_journal_wallet_id_fkey",
    }),
    foreignKey({
      columns: [table.id, table.createdAt],
      foreignColumns: [journalPayloads.id, journalPayloads.createdAt],
      name: "journal_index_payload",
    }),
    unique("journal_index_time").on(table.id, table.createdAt),
    unique("money_journal_wallet_id_reference_key").on(table.walletId, table.reference),
    check(
      "money_journal_kind_check",
      sql`kind = ANY (ARRAY['CREDIT'::text, 'HOLD'::text, 'SETTLE'::text, 'RELEASE'::text, 'REFUND'::text, 'EXPIRE'::text, 'DEBT_COLLECTION'::text, 'ADJUSTMENT'::text])`,
    ),
  ],
);

export const wallets = pgTable(
  "wallets",
  {
    id: uuid().primaryKey().notNull(),
    projectId: uuid("project_id").notNull(),
    currency: text().notNull(),
    policy: text().default("GIFT_FIRST").notNull(),
    availableAtoms: numeric("available_atoms", { precision: 38, scale: 0 }).default("0").notNull(),
    reservedAtoms: numeric("reserved_atoms", { precision: 38, scale: 0 }).default("0").notNull(),
    spentAtoms: numeric("spent_atoms", { precision: 38, scale: 0 }).default("0").notNull(),
    expiredAtoms: numeric("expired_atoms", { precision: 38, scale: 0 }).default("0").notNull(),
    debtAtoms: numeric("debt_atoms", { precision: 38, scale: 0 }).default("0").notNull(),
    freezeReason: text("freeze_reason"),
    hasExpiringLots: boolean("has_expiring_lots").default(false).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    manualFrozen: boolean("manual_frozen").default(false).notNull(),
    evidenceDisputed: boolean("evidence_disputed").default(false).notNull(),
    creditLimitAtoms: numeric("credit_limit_atoms", { precision: 38, scale: 0 })
      .default("0")
      .notNull(),
    creditReservedAtoms: numeric("credit_reserved_atoms", { precision: 38, scale: 0 })
      .default("0")
      .notNull(),
    creditPolicyVersion: numeric("credit_policy_version", { precision: 38, scale: 0 })
      .default("0")
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    foreignKey({
      columns: [table.projectId],
      foreignColumns: [projects.id],
      name: "wallets_project_id_fkey",
    }),
    unique("wallets_project_id_currency_key").on(table.projectId, table.currency),
    check("wallets_available_atoms_check", sql`available_atoms >= (0)::numeric`),
    check("wallets_currency_check", sql`currency ~ '^[A-Z]{3,8}$'::text`),
    check("wallets_debt_atoms_check", sql`debt_atoms >= (0)::numeric`),
    check("wallets_expired_atoms_check", sql`expired_atoms >= (0)::numeric`),
    check(
      "wallets_freeze_reason_check",
      sql`freeze_reason = ANY (ARRAY['MANUAL'::text, 'DEBT'::text, 'EVIDENCE_CONFLICT'::text])`,
    ),
    check(
      "wallets_policy_check",
      sql`policy = ANY (ARRAY['GIFT_FIRST'::text, 'CASH_FIRST'::text, 'EARLIEST_EXPIRY'::text])`,
    ),
    check("wallets_reserved_atoms_check", sql`reserved_atoms >= (0)::numeric`),
    check("wallets_spent_atoms_check", sql`spent_atoms >= (0)::numeric`),
    check("wallets_credit_limit_atoms_check", sql`credit_limit_atoms >= (0)::numeric`),
    check("wallets_credit_reserved_atoms_check", sql`credit_reserved_atoms >= (0)::numeric`),
    check("wallets_credit_policy_version_check", sql`credit_policy_version >= (0)::numeric`),
  ],
);

export const billingHolds = pgTable(
  "billing_holds",
  {
    id: uuid().primaryKey().notNull(),
    requestId: uuid("request_id").notNull(),
    walletId: uuid("wallet_id").notNull(),
    amountAtoms: numeric("amount_atoms", { precision: 38, scale: 0 }).notNull(),
    state: text().default("HELD").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    closedAt: timestamp("closed_at", { withTimezone: true, mode: "string" }),
    nextReconcileAt: timestamp("next_reconcile_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
    reconcileUntil: timestamp("reconcile_until", { withTimezone: true, mode: "string" }),
    reconcileToken: uuid("reconcile_token"),
    reconcileReason: text("reconcile_reason"),
    creditAtoms: numeric("credit_atoms", { precision: 38, scale: 0 }).default("0").notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    index("billing_holds_wallet")
      .using(
        "btree",
        table.walletId.asc().nullsLast(),
        table.createdAt.asc().nullsLast(),
        table.id.asc().nullsLast(),
      )
      .where(sql`(state = ANY (ARRAY['HELD'::text, 'RECONCILING'::text]))`),
    index("holds_all_wallet").using(
      "btree",
      table.walletId.asc().nullsLast(),
      table.requestId.asc().nullsLast(),
    ),
    index("holds_reconcile_due")
      .using("btree", table.nextReconcileAt.asc().nullsLast(), table.requestId.asc().nullsLast())
      .where(sql`(state = ANY (ARRAY['HELD'::text, 'RECONCILING'::text]))`),
    foreignKey({
      columns: [table.requestId],
      foreignColumns: [gatewayRequests.id],
      name: "billing_holds_request_id_fkey",
    }),
    foreignKey({
      columns: [table.walletId],
      foreignColumns: [wallets.id],
      name: "billing_holds_wallet_id_fkey",
    }),
    unique("billing_holds_request_id_key").on(table.requestId),
    check("billing_holds_amount_atoms_check", sql`amount_atoms >= (0)::numeric`),
    check(
      "billing_holds_state_check",
      sql`state = ANY (ARRAY['HELD'::text, 'RECONCILING'::text, 'SETTLED'::text, 'RELEASED'::text])`,
    ),
    check(
      "billing_holds_check",
      sql`(credit_atoms >= (0)::numeric) AND (credit_atoms <= amount_atoms)`,
    ),
  ],
);

export const walletCreditPolicies = pgTable(
  "wallet_credit_policies",
  {
    id: uuid().primaryKey().notNull(),
    walletId: uuid("wallet_id").notNull(),
    externalRef: text("external_ref").notNull(),
    fingerprint: text().notNull(),
    creditLimitAtoms: numeric("credit_limit_atoms", { precision: 38, scale: 0 }).notNull(),
    version: numeric({ precision: 38, scale: 0 }).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    foreignKey({
      columns: [table.walletId],
      foreignColumns: [wallets.id],
      name: "wallet_credit_policies_wallet_id_fkey",
    }),
    unique("wallet_credit_policies_wallet_id_external_ref_key").on(
      table.walletId,
      table.externalRef,
    ),
    unique("wallet_credit_policies_wallet_id_version_key").on(table.walletId, table.version),
    check(
      "wallet_credit_policies_external_ref_check",
      sql`(length(external_ref) >= 1) AND (length(external_ref) <= 200)`,
    ),
    check(
      "wallet_credit_policies_credit_limit_atoms_check",
      sql`credit_limit_atoms >= (0)::numeric`,
    ),
    check("wallet_credit_policies_version_check", sql`version > (0)::numeric`),
  ],
);

export const walletDisputes = pgTable(
  "wallet_disputes",
  {
    walletId: uuid("wallet_id").notNull(),
    requestId: uuid("request_id").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    primaryKey({ columns: [table.walletId, table.requestId], name: "wallet_disputes_pkey" }),
  ],
);

export const holdAllocations = pgTable(
  "hold_allocations",
  {
    holdId: uuid("hold_id").notNull(),
    lotId: uuid("lot_id").notNull(),
    ordinal: integer().notNull(),
    amountAtoms: numeric("amount_atoms", { precision: 38, scale: 0 }).notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    foreignKey({
      columns: [table.holdId],
      foreignColumns: [billingHolds.id],
      name: "hold_allocations_hold_id_fkey",
    }),
    foreignKey({
      columns: [table.lotId],
      foreignColumns: [walletLots.id],
      name: "hold_allocations_lot_id_fkey",
    }),
    primaryKey({ columns: [table.holdId, table.lotId], name: "hold_allocations_pkey" }),
    check("hold_allocations_amount_atoms_check", sql`amount_atoms > (0)::numeric`),
  ],
);

export const chargeAllocations = pgTable(
  "charge_allocations",
  {
    chargeId: uuid("charge_id").notNull(),
    lotId: uuid("lot_id").notNull(),
    ordinal: integer().notNull(),
    amountAtoms: numeric("amount_atoms", { precision: 38, scale: 0 }).notNull(),
    refundedAtoms: numeric("refunded_atoms", { precision: 38, scale: 0 }).default("0").notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    foreignKey({
      columns: [table.chargeId],
      foreignColumns: [billingCharges.id],
      name: "charge_allocations_charge_id_fkey",
    }),
    foreignKey({
      columns: [table.lotId],
      foreignColumns: [walletLots.id],
      name: "charge_allocations_lot_id_fkey",
    }),
    primaryKey({ columns: [table.chargeId, table.lotId], name: "charge_allocations_pkey" }),
    check("charge_allocations_amount_atoms_check", sql`amount_atoms > (0)::numeric`),
    check(
      "charge_allocations_check",
      sql`(refunded_atoms >= (0)::numeric) AND (refunded_atoms <= amount_atoms)`,
    ),
  ],
);

export const eventDeliveries = pgTable(
  "event_deliveries",
  {
    consumerId: uuid("consumer_id").notNull(),
    eventId: uuid("event_id").notNull(),
    state: text().default("PENDING").notNull(),
    leaseToken: uuid("lease_token"),
    leaseUntil: timestamp("lease_until", { withTimezone: true, mode: "string" }),
    attempts: integer().default(0).notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    index("delivery_due")
      .using("btree", table.consumerId.asc().nullsLast(), table.eventId.asc().nullsLast())
      .where(sql`(state <> 'ACKED'::text)`),
    foreignKey({
      columns: [table.consumerId],
      foreignColumns: [eventConsumers.id],
      name: "event_deliveries_consumer_id_fkey",
    }),
    foreignKey({
      columns: [table.eventId],
      foreignColumns: [usageEventIndex.id],
      name: "event_deliveries_event_id_fkey",
    }),
    primaryKey({ columns: [table.consumerId, table.eventId], name: "event_deliveries_pkey" }),
    check(
      "event_deliveries_state_check",
      sql`state = ANY (ARRAY['PENDING'::text, 'LEASED'::text, 'ACKED'::text])`,
    ),
  ],
);
export const usageEvents = pgView("usage_events", {
  id: uuid(),
  seq: bigint({ mode: "bigint" }),
  projectId: uuid("project_id"),
  requestId: uuid("request_id"),
  attemptId: uuid("attempt_id"),
  source: text(),
  finality: text(),
  usage: jsonb(),
  costAtoms: numeric("cost_atoms", { precision: 38, scale: 0 }),
  currency: text(),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).defaultNow(),
  costComplete: boolean("cost_complete").default(false),
  recordKind: text("record_kind"),
  snapshotRevision: numeric("snapshot_revision", { precision: 38, scale: 0 }),
  finalDimensions: text("final_dimensions").array(),
}).as(
  sql`SELECT i.id, i.seq, i.project_id, i.request_id, i.attempt_id, i.source, i.finality, p.usage, i.cost_atoms, i.currency, i.created_at, i.cost_complete, i.record_kind, i.snapshot_revision, i.final_dimensions FROM usage_event_index i JOIN usage_payloads p ON p.id = i.id AND p.created_at = i.created_at WHERE i.record_kind <> 'RAW'`,
);

export const moneyJournal = pgView("money_journal", {
  id: uuid(),
  walletId: uuid("wallet_id"),
  reference: text(),
  kind: text(),
  currency: text(),
  lines: jsonb(),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).defaultNow(),
}).as(
  sql`SELECT i.id, i.wallet_id, i.reference, i.kind, i.currency, p.lines, i.created_at FROM money_entries i JOIN journal_payloads p ON p.id = i.id AND p.created_at = i.created_at`,
);
export const usagePayloads = pgTable(
  "usage_payloads",
  {
    id: uuid().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).notNull(),
    usage: jsonb().notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    primaryKey({ columns: [table.id, table.createdAt] }),
    foreignKey({
      columns: [table.id, table.createdAt],
      foreignColumns: [usageEventIndex.id, usageEventIndex.createdAt],
      name: "usage_payloads_id_created_at_fkey",
    }),
    check("usage_payloads_usage_check", sql`jsonb_typeof(usage)='object'`),
  ],
);
export const journalPayloads = pgTable(
  "journal_payloads",
  {
    id: uuid().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).notNull(),
    lines: jsonb().notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    primaryKey({ columns: [table.id, table.createdAt] }),
    foreignKey({
      columns: [table.id, table.createdAt],
      foreignColumns: [moneyEntries.id, moneyEntries.createdAt],
      name: "journal_payloads_id_created_at_fkey",
    }),
    check("journal_payloads_lines_check", sql`jsonb_typeof(lines)='array'`),
  ],
);
