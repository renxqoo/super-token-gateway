import type { SQL } from "bun";
import { drizzle } from "drizzle-orm/bun-sql";
import {
  bigint,
  bigserial,
  boolean,
  foreignKey,
  index,
  integer,
  numeric,
  pgTable,
  primaryKey,
  smallint,
  text,
  timestamp,
  unique,
  uniqueIndex,
  uuid,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import type { Usage } from "@gateway/kernel";
import type {
  ChargeLotAllocation,
  ChargePolicy,
  ChargePricingSnapshot,
  HoldLotAllocation,
  LeaseLotAllocation,
  LotKind,
  PluginAuditEntry,
  SupplierHoldEntry,
  WalletPolicy,
} from "./types.ts";
import {
  billingAccounts,
  upstreamAccounts,
  pgJsonb,
  priceVersions,
  pricingModifiers,
  fxRateVersions,
  channels,
  schemaMigrations,
  configEpoch,
  consoleOrganizationAdmission,
  apiKeys,
  models,
  plugins,
  pluginReleases,
  pluginRevocations,
  gatewayConfigNodes,
  pluginReleaseAcks,
  routes,
} from "./schema-catalog.ts";
export {
  schemaMigrations,
  configEpoch,
  billingAccounts,
  consoleOrganizationAdmission,
  apiKeys,
  upstreamAccounts,
  plugins,
  pluginReleases,
  pluginRevocations,
  gatewayConfigNodes,
  pluginReleaseAcks,
  channels,
  models,
  priceVersions,
  pricingModifiers,
  fxRateVersions,
  routes,
} from "./schema-catalog.ts";

export const wallets = pgTable(
  "wallets",
  {
    id: uuid("id").primaryKey(),
    billingAccountId: uuid("billing_account_id")
      .notNull()
      .references(() => billingAccounts.id),
    currency: text("currency").notNull(),
    policy: text("policy").$type<WalletPolicy>().notNull().default("GIFT_FIRST"),
    availableAtoms: numeric("available_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    reservedAtoms: numeric("reserved_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    spentAtoms: numeric("spent_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    expiredAtoms: numeric("expired_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    debtAtoms: numeric("debt_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    creditLimitAtoms: numeric("credit_limit_atoms", { precision: 38, scale: 0 })
      .notNull()
      .default("0"),
    creditReservedAtoms: numeric("credit_reserved_atoms", { precision: 38, scale: 0 })
      .notNull()
      .default("0"),
    creditPolicyVersion: numeric("credit_policy_version", { precision: 38, scale: 0 })
      .notNull()
      .default("0"),
    manualFrozen: boolean("manual_frozen").notNull().default(false),
    evidenceDisputed: boolean("evidence_disputed").notNull().default(false),
    freezeReason: text("freeze_reason"),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [unique("wallets_billing_account_id_currency_key").on(t.billingAccountId, t.currency)],
);

export const walletLots = pgTable(
  "wallet_lots",
  {
    id: uuid("id").primaryKey(),
    walletId: uuid("wallet_id")
      .notNull()
      .references(() => wallets.id),
    externalRef: text("external_ref").notNull(),
    kind: text("kind").$type<LotKind>().notNull(),
    priority: integer("priority").notNull().default(100),
    models: text("models")
      .array()
      .notNull()
      .default(sql`'{}'::text[]`),
    operations: text("operations")
      .array()
      .notNull()
      .default(sql`'{}'::text[]`),
    expiresAt: timestamp("expires_at", { withTimezone: true, mode: "string" }),
    issuedAtoms: numeric("issued_atoms", { precision: 38, scale: 0 }).notNull(),
    availableAtoms: numeric("available_atoms", { precision: 38, scale: 0 }).notNull(),
    reservedAtoms: numeric("reserved_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    spentAtoms: numeric("spent_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    expiredAtoms: numeric("expired_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    unique("wallet_lots_wallet_id_external_ref_key").on(t.walletId, t.externalRef),
    index("wallet_lots_avail_idx").on(t.walletId, t.priority, t.expiresAt, t.createdAt),
    index("wallet_lots_expiry_idx").on(t.expiresAt),
  ],
);

export const supplierRootBalances = pgTable(
  "supplier_root_balances",
  {
    accountId: uuid("account_id")
      .notNull()
      .references(() => upstreamAccounts.id),
    currency: text("currency").notNull(),
    balanceAtoms: numeric("balance_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    spentAtoms: numeric("spent_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    revision: numeric("revision", { precision: 38, scale: 0 }).notNull().default("0"),
  },
  (t) => [primaryKey({ columns: [t.accountId, t.currency] })],
);

export const supplierCells = pgTable(
  "supplier_cells",
  {
    accountId: uuid("account_id").notNull(),
    currency: text("currency").notNull(),
    cell: smallint("cell").notNull(),
    balanceAtoms: numeric("balance_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    reservedAtoms: numeric("reserved_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    spentAtoms: numeric("spent_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    revision: numeric("revision", { precision: 38, scale: 0 }).notNull().default("0"),
  },
  (t) => [
    primaryKey({ columns: [t.accountId, t.currency, t.cell] }),
    foreignKey({
      columns: [t.accountId, t.currency],
      foreignColumns: [supplierRootBalances.accountId, supplierRootBalances.currency],
    }),
  ],
);

export const walletLeases = pgTable(
  "wallet_leases",
  {
    id: uuid("id").primaryKey(),
    nodeId: text("node_id").notNull(),
    walletId: uuid("wallet_id")
      .notNull()
      .references(() => wallets.id),
    currency: text("currency").notNull(),
    lotAllocations: pgJsonb<LeaseLotAllocation[]>("lot_allocations")
      .notNull()
      .default(sql`'[]'::jsonb`),
    remainingAtoms: numeric("remaining_atoms", { precision: 38, scale: 0 }).notNull(),
    spentAtoms: numeric("spent_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    spentLotId: uuid("spent_lot_id"),
    earliestLotExpiresAt: timestamp("earliest_lot_expires_at", {
      withTimezone: true,
      mode: "string",
    }),
    state: text("state").$type<"ACTIVE" | "CLOSED">().notNull().default("ACTIVE"),
    expiresAt: timestamp("expires_at", { withTimezone: true, mode: "string" }).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    index("wallet_leases_active_idx").on(t.expiresAt),
    index("wallet_leases_wallet_idx").on(t.walletId),
  ],
);

export const supplierLeases = pgTable(
  "supplier_leases",
  {
    id: uuid("id").primaryKey(),
    nodeId: text("node_id").notNull(),
    accountId: uuid("account_id")
      .notNull()
      .references(() => upstreamAccounts.id),
    currency: text("currency").notNull(),
    cell: smallint("cell").notNull(),
    remainingAtoms: numeric("remaining_atoms", { precision: 38, scale: 0 }).notNull(),
    spentAtoms: numeric("spent_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    state: text("state").$type<"ACTIVE" | "CLOSED">().notNull().default("ACTIVE"),
    expiresAt: timestamp("expires_at", { withTimezone: true, mode: "string" }).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    index("supplier_leases_active_idx").on(t.expiresAt),
    index("supplier_leases_account_active_idx").on(t.accountId, t.currency, t.cell),
  ],
);

export const billingHolds = pgTable(
  "billing_holds",
  {
    requestId: uuid("request_id").primaryKey(),
    billingAccountId: uuid("billing_account_id").notNull(),
    walletId: uuid("wallet_id").notNull(),
    apiKeyId: uuid("api_key_id"),
    keyReservedAtoms: numeric("key_reserved_atoms", { precision: 38, scale: 0 })
      .notNull()
      .default("0"),
    pricingOverrideId: uuid("pricing_override_id"),
    discountNumerator: numeric("discount_numerator", { precision: 38, scale: 0 }),
    discountDenominator: numeric("discount_denominator", { precision: 38, scale: 0 }),
    operation: text("operation").notNull(),
    model: text("model").notNull(),
    idempotencyKey: text("idempotency_key"),
    fingerprint: text("fingerprint").notNull(),
    heldAtoms: numeric("held_atoms", { precision: 38, scale: 0 }).notNull(),
    creditAtoms: numeric("credit_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    lotAllocations: pgJsonb<HoldLotAllocation[]>("lot_allocations")
      .notNull()
      .default(sql`'[]'::jsonb`),
    supplierHolds: pgJsonb<SupplierHoldEntry[]>("supplier_holds")
      .notNull()
      .default(sql`'[]'::jsonb`),
    walletLeaseId: uuid("wallet_lease_id"),
    supplierLeaseId: uuid("supplier_lease_id"),
    state: text("state")
      .$type<"HELD" | "SETTLED" | "RELEASED" | "UNKNOWN" | "CONFLICT">()
      .notNull()
      .default("HELD"),
    expiresAt: timestamp("expires_at", { withTimezone: true, mode: "string" }).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
    closedAt: timestamp("closed_at", { withTimezone: true, mode: "string" }),
    reconciliationStartedAt: timestamp("reconciliation_started_at", {
      withTimezone: true,
      mode: "date",
    }),
  },
  (t) => [
    uniqueIndex("billing_holds_idempotency_idx")
      .on(t.billingAccountId, t.operation, t.idempotencyKey)
      .where(sql`${t.idempotencyKey} IS NOT NULL`),
    index("billing_holds_open_idx").on(t.expiresAt),
    index("billing_reconciliation_unknown_idx")
      .on(t.createdAt, t.requestId)
      .where(sql`${t.state} = 'UNKNOWN'`),
    index("billing_reconciliation_supplier_pending_idx")
      .on(t.createdAt, t.requestId)
      .where(
        sql`${t.state} IN ('SETTLED', 'RELEASED') AND ${t.supplierHolds} @> '[{"state":"UNKNOWN"}]'`,
      ),
  ],
);

export const billingCharges = pgTable(
  "billing_charges",
  {
    id: uuid("id").primaryKey(),
    requestId: uuid("request_id").notNull().unique(),
    billingAccountId: uuid("billing_account_id").notNull(),
    walletId: uuid("wallet_id").notNull(),
    apiKeyId: uuid("api_key_id"),
    selectedAttemptId: uuid("selected_attempt_id"),
    channelId: uuid("channel_id"),
    model: text("model").notNull(),
    operation: text("operation").notNull(),
    currency: text("currency").notNull(),
    billingPriceId: uuid("billing_price_id"),
    costVersionId: uuid("cost_version_id"),
    pricingOverrideId: uuid("pricing_override_id"),
    discountNumerator: numeric("discount_numerator", { precision: 38, scale: 0 }),
    discountDenominator: numeric("discount_denominator", { precision: 38, scale: 0 }),
    pricingSnapshot: pgJsonb<ChargePricingSnapshot | null>("pricing_snapshot"),
    usage: pgJsonb<Usage>("usage")
      .notNull()
      .default(sql`'{}'::jsonb`),
    usageDigest: text("usage_digest").notNull(),
    customerAtoms: numeric("customer_atoms", { precision: 38, scale: 0 }).notNull(),
    fundedAtoms: numeric("funded_atoms", { precision: 38, scale: 0 }).notNull(),
    debtAtoms: numeric("debt_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    refundedAtoms: numeric("refunded_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    debtRefundedAtoms: numeric("debt_refunded_atoms", { precision: 38, scale: 0 })
      .notNull()
      .default("0"),
    supplierCostAtoms: numeric("supplier_cost_atoms", { precision: 38, scale: 0 })
      .notNull()
      .default("0"),
    supplierCurrency: text("supplier_currency"),
    lotAllocations: pgJsonb<ChargeLotAllocation[]>("lot_allocations")
      .notNull()
      .default(sql`'[]'::jsonb`),
    pluginAudit: pgJsonb<PluginAuditEntry[]>("plugin_audit")
      .notNull()
      .default(sql`'[]'::jsonb`),
    chargePolicy: text("charge_policy").$type<ChargePolicy>().notNull().default("CHARGE_CUSTOMER"),
    outcome: text("outcome")
      .$type<"SETTLED" | "RELEASED" | "DEBT" | "MODERATION_BLOCKED" | "UNKNOWN_RESOLVED">()
      .notNull(),
    customerJournalId: uuid("customer_journal_id"),
    supplierJournalId: uuid("supplier_journal_id"),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    index("billing_charges_billing_account_created_idx").on(t.billingAccountId, t.createdAt),
    index("billing_charges_wallet_debt_idx").on(t.walletId, t.createdAt),
  ],
);

export const ledgerEntries = pgTable(
  "ledger_entries",
  {
    journalId: uuid("journal_id").notNull(),
    ordinal: smallint("ordinal").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
    side: text("side").$type<"CUSTOMER" | "SUPPLIER">().notNull(),
    entityId: uuid("entity_id").notNull(),
    requestId: uuid("request_id"),
    reference: text("reference").notNull(),
    kind: text("kind").notNull(),
    currency: text("currency").notNull(),
    account: text("account").notNull(),
    atoms: numeric("atoms", { precision: 38, scale: 0 }).notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.journalId, t.ordinal, t.createdAt] }),
    index("ledger_entries_entity_idx").on(t.side, t.entityId, t.createdAt),
    index("ledger_entries_request_idx").on(t.requestId),
  ],
);

export const fundingOperations = pgTable(
  "funding_operations",
  {
    id: uuid("id").primaryKey(),
    side: text("side").$type<"CUSTOMER" | "SUPPLIER">().notNull(),
    entityId: uuid("entity_id").notNull(),
    kind: text("kind").notNull(),
    externalRef: text("external_ref").notNull(),
    fingerprint: text("fingerprint").notNull(),
    amountAtoms: numeric("amount_atoms", { precision: 38, scale: 0 }).notNull(),
    currency: text("currency").notNull(),
    reason: text("reason").notNull().default(""),
    payload: pgJsonb<Record<string, unknown>>("payload")
      .notNull()
      .default(sql`'{}'::jsonb`),
    receipt: pgJsonb<Record<string, unknown>>("receipt")
      .notNull()
      .default(sql`'{}'::jsonb`),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    unique("funding_operations_side_entity_id_kind_external_ref_key").on(
      t.side,
      t.entityId,
      t.kind,
      t.externalRef,
    ),
  ],
);

export const billingResolutions = pgTable("billing_resolutions", {
  id: uuid("id").primaryKey(),
  requestId: uuid("request_id")
    .notNull()
    .unique()
    .references(() => billingHolds.requestId),
  action: text("action").$type<"SETTLE" | "RELEASE">().notNull(),
  billingPriceId: uuid("billing_price_id").references(() => priceVersions.id),
  usage: pgJsonb<Usage>("usage")
    .notNull()
    .default(sql`'{}'::jsonb`),
  reason: text("reason").notNull(),
  actor: text("actor").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
});

export const outboxEvents = pgTable(
  "outbox_events",
  {
    seq: bigserial("seq", { mode: "bigint" }).primaryKey(),
    eventId: uuid("event_id").notNull().unique(),
    kind: text("kind").notNull(),
    requestId: uuid("request_id"),
    billingAccountId: uuid("billing_account_id"),
    shard: smallint("shard").notNull().default(0),
    payload: pgJsonb<Record<string, unknown>>("payload").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [index("outbox_events_shard_seq_idx").on(t.shard, t.seq)],
);

export const outboxConsumers = pgTable("outbox_consumers", {
  name: text("name").primaryKey(),
});

export const outboxConsumerShards = pgTable(
  "outbox_consumer_shards",
  {
    name: text("name").notNull(),
    shard: smallint("shard").notNull(),
    ackedSeq: bigint("acked_seq", { mode: "bigint" })
      .notNull()
      .default(sql`0`),
    leaseToken: uuid("lease_token"),
    leasedThroughSeq: bigint("leased_through_seq", { mode: "bigint" }),
    leasedUntil: timestamp("leased_until", { withTimezone: true, mode: "string" }),
    updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [primaryKey({ columns: [t.name, t.shard] })],
);

export const adminAuditEvents = pgTable(
  "admin_audit_events",
  {
    requestId: uuid("request_id").notNull(),
    phase: text("phase").notNull(),
    actor: text("actor").notNull(),
    action: text("action").notNull(),
    target: text("target").notNull(),
    status: smallint("status"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .default(sql`clock_timestamp()`),
  },
  (t) => [primaryKey({ columns: [t.requestId, t.phase] })],
);

export const adminAuditPending = pgTable(
  "admin_audit_pending",
  {
    requestId: uuid("request_id").primaryKey(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .default(sql`clock_timestamp()`),
  },
  (t) => [index("admin_audit_pending_created_idx").on(t.createdAt)],
);

export const billingAccountPricingOverrides = pgTable(
  "billing_account_pricing_overrides",
  {
    id: uuid("id").primaryKey(),
    billingAccountId: uuid("billing_account_id")
      .notNull()
      .references(() => billingAccounts.id),
    modelName: text("model_name").notNull(),
    operation: text("operation").notNull().default("*"),
    overrideKind: text("override_kind").$type<"EXACT_PRICE" | "MULTIPLIER">().notNull(),
    billingPriceId: uuid("billing_price_id").references(() => priceVersions.id),
    multiplierNumerator: numeric("multiplier_numerator", { precision: 38, scale: 0 }),
    multiplierDenominator: numeric("multiplier_denominator", { precision: 38, scale: 0 }),
    enabled: boolean("enabled").notNull().default(true),
    sourceRevision: integer("source_revision").notNull().default(0),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    uniqueIndex("billing_account_pricing_overrides_scope_uniq").on(
      t.billingAccountId,
      t.modelName,
      t.operation,
    ),
    index("billing_account_pricing_overrides_billing_account_idx").on(t.billingAccountId),
  ],
);

export const asyncTaskAdmissions = pgTable(
  "async_task_admissions",
  {
    requestId: uuid("request_id").primaryKey(),
    billingAccountId: uuid("billing_account_id")
      .notNull()
      .references(() => billingAccounts.id),
    expiresAt: timestamp("expires_at", { withTimezone: true, mode: "string" }).notNull(),
  },
  (t) => [
    index("async_task_admissions_account_expiry_idx").on(t.billingAccountId, t.expiresAt),
    index("async_task_admissions_expiry_idx").on(t.expiresAt, t.requestId),
  ],
);

export const asyncTasks = pgTable(
  "async_tasks",
  {
    id: text("id").primaryKey(),
    requestId: uuid("request_id").notNull().unique(),
    chargeId: uuid("charge_id").references(() => billingCharges.id),
    billingAccountId: uuid("billing_account_id")
      .notNull()
      .references(() => billingAccounts.id),
    apiKeyId: uuid("api_key_id"),
    channelId: uuid("channel_id")
      .notNull()
      .references(() => channels.id),
    model: text("model").notNull(),
    operation: text("operation").notNull(),
    upstreamTaskId: text("upstream_task_id").notNull(),
    status: text("status")
      .$type<"QUEUED" | "PROCESSING" | "SUCCEEDED" | "FAILED" | "CANCELLED">()
      .notNull(),
    userCancelled: boolean("user_cancelled").notNull().default(false),
    upstreamBilled: boolean("upstream_billed").notNull().default(false),
    failureCategory: text("failure_category").$type<
      | "UPSTREAM_SYSTEM"
      | "CONTENT_POLICY"
      | "INVALID_INPUT"
      | "USER_CANCELLED"
      | "UPSTREAM_BILLED"
      | "TIMEOUT"
      | "UNKNOWN"
      | null
    >(),
    refundEligibility: text("refund_eligibility")
      .$type<
        | "PENDING"
        | "AUTO_REFUNDED"
        | "DENIED_USER_CANCELLED"
        | "DENIED_POLICY_OR_CLIENT"
        | "DENIED_UPSTREAM_BILLED"
        | "DENIED_ABUSE_LIMIT"
        | "MANUAL_REVIEW"
        | "NOT_APPLICABLE"
      >()
      .notNull()
      .default("PENDING"),
    refundId: uuid("refund_id"),
    resultJson: pgJsonb<Record<string, unknown>>("result_json")
      .notNull()
      .default(sql`'{}'::jsonb`),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
    completedAt: timestamp("completed_at", { withTimezone: true, mode: "string" }),
  },
  (t) => [
    index("async_tasks_billing_account_status_idx").on(t.billingAccountId, t.status),
    index("async_tasks_active_idx")
      .on(t.status, t.updatedAt)
      .where(sql`${t.status} IN ('QUEUED', 'PROCESSING')`),
    index("async_tasks_auto_refund_idx")
      .on(t.billingAccountId, t.refundEligibility, t.completedAt)
      .where(sql`${t.refundEligibility} = 'AUTO_REFUNDED'`),
  ],
);

export const billingReconciliationReceipts = pgTable(
  "billing_reconciliation_receipts",
  {
    attemptId: uuid("attempt_id").primaryKey(),
    requestId: uuid("request_id")
      .notNull()
      .references(() => billingHolds.requestId),
    accountId: uuid("account_id")
      .notNull()
      .references(() => upstreamAccounts.id),
    channelId: uuid("channel_id")
      .notNull()
      .references(() => channels.id),
    currency: text("currency").notNull(),
    providerRequestId: text("provider_request_id").notNull(),
    evidenceRef: text("evidence_ref").notNull(),
    matchReason: text("match_reason"),
    actor: text("actor").notNull(),
    verdict: text("verdict").$type<"NOT_EXECUTED" | "FINAL">().notNull(),
    supplierCostAtoms: numeric("supplier_cost_atoms", { precision: 38, scale: 0 }),
    customerUsage: pgJsonb<Usage>("customer_usage"),
    customerActor: text("customer_actor"),
    state: text("state")
      .$type<"PENDING" | "APPLIED" | "REVIEW" | "DISMISSED">()
      .notNull()
      .default("PENDING"),
    result: text("result"),
    reviewHistory: pgJsonb<Record<string, unknown>[]>("review_history")
      .notNull()
      .default(sql`'[]'::jsonb`),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
    reviewedAt: timestamp("reviewed_at", { withTimezone: true, mode: "date" }),
  },
  (t) => [
    unique().on(t.accountId, t.providerRequestId),
    unique().on(t.accountId, t.evidenceRef),
    index("billing_reconciliation_receipts_due_idx")
      .on(t.createdAt, t.attemptId)
      .where(sql`${t.state} = 'PENDING'`),
    index("billing_reconciliation_receipts_request_idx").on(t.requestId, t.createdAt, t.attemptId),
  ],
);

export const billingReconciliationBudgets = pgTable("billing_reconciliation_budgets", {
  currency: text("currency").primaryKey(),
  limitAtoms: numeric("limit_atoms", { precision: 38, scale: 0 }).notNull(),
  usedAtoms: numeric("used_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
});

export const billingReconciliationPolicies = pgTable(
  "billing_reconciliation_policies",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    billingAccountId: uuid("billing_account_id").references(() => billingAccounts.id),
    channelId: uuid("channel_id")
      .notNull()
      .references(() => channels.id),
    supplierCurrency: text("supplier_currency")
      .notNull()
      .references(() => billingReconciliationBudgets.currency),
    enabled: boolean("enabled").notNull(),
    includeHistorical: boolean("include_historical").notNull(),
    eligibleSince: timestamp("eligible_since", { withTimezone: true, mode: "date" })
      .notNull()
      .defaultNow(),
    ageSeconds: integer("age_seconds").notNull(),
    maxCustomerAtoms: numeric("max_customer_atoms", { precision: 38, scale: 0 }).notNull(),
    limitAtoms: numeric("limit_atoms", { precision: 38, scale: 0 }).notNull(),
    usedAtoms: numeric("used_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
  },
  (t) => [
    uniqueIndex("billing_reconciliation_policies_account_scope_idx")
      .on(t.billingAccountId, t.channelId, t.supplierCurrency)
      .where(sql`${t.billingAccountId} IS NOT NULL`),
    uniqueIndex("billing_reconciliation_policies_channel_default_idx")
      .on(t.channelId, t.supplierCurrency)
      .where(sql`${t.billingAccountId} IS NULL`),
  ],
);

export const billingReconciliationWaivers = pgTable("billing_reconciliation_waivers", {
  requestId: uuid("request_id")
    .primaryKey()
    .references(() => billingHolds.requestId),
  attemptId: uuid("attempt_id").notNull(),
  policyId: uuid("policy_id")
    .notNull()
    .references(() => billingReconciliationPolicies.id),
  billingAccountId: uuid("billing_account_id").notNull(),
  channelId: uuid("channel_id").notNull(),
  supplierCurrency: text("supplier_currency").notNull(),
  supplierRiskAtoms: numeric("supplier_risk_atoms", { precision: 38, scale: 0 }).notNull(),
  customerReleasedAtoms: numeric("customer_released_atoms", {
    precision: 38,
    scale: 0,
  }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const billingReconciliationPolls = pgTable("billing_reconciliation_polls", {
  attemptId: uuid("attempt_id").primaryKey(),
  requestId: uuid("request_id")
    .notNull()
    .references(() => billingHolds.requestId),
  pollCount: integer("poll_count").notNull().default(1),
  nextAt: timestamp("next_at", { withTimezone: true, mode: "date" }).notNull(),
});

export const ledgerSchema = {
  adminAuditEvents,
  adminAuditPending,
  schemaMigrations,
  configEpoch,
  billingAccounts,
  consoleOrganizationAdmission,
  apiKeys,
  upstreamAccounts,
  channels,
  models,
  priceVersions,
  pricingModifiers,
  fxRateVersions,
  billingAccountPricingOverrides,
  asyncTasks,
  asyncTaskAdmissions,
  plugins,
  pluginReleases,
  pluginRevocations,
  gatewayConfigNodes,
  pluginReleaseAcks,
  routes,
  wallets,
  walletLots,
  supplierRootBalances,
  supplierCells,
  walletLeases,
  supplierLeases,
  billingHolds,
  billingCharges,
  ledgerEntries,
  fundingOperations,
  billingResolutions,
  billingReconciliationReceipts,
  billingReconciliationBudgets,
  billingReconciliationPolicies,
  billingReconciliationWaivers,
  billingReconciliationPolls,
  outboxEvents,
  outboxConsumers,
  outboxConsumerShards,
};

export function createDrizzle(db: SQL) {
  return drizzle(db, { schema: ledgerSchema });
}

export type LedgerDrizzle = ReturnType<typeof createDrizzle>;
