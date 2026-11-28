import type { SQL } from "bun";
import type { ModelOnboardingInput, RouteReleaseIntent } from "@gateway/contracts";
import { drizzle } from "drizzle-orm/bun-sql";
import {
  type AnyPgColumn,
  bigint,
  boolean,
  date,
  doublePrecision,
  integer,
  jsonb,
  numeric,
  pgTable,
  primaryKey,
  smallint,
  text,
  timestamp,
  unique,
  uuid,
} from "drizzle-orm/pg-core";
import type { AdaptiveRoutingSetting, Pricing } from "@gateway/kernel";

export type OrgKind = "PERSONAL" | "ENTERPRISE";
export type OrgRole = "OWNER" | "ADMIN" | "FINANCE" | "DEVELOPER" | "VIEWER";
export type SyncStatus = "PENDING" | "SYNCED" | "FAILED";
export type SpendLimitWindow = "LIFETIME" | "DAILY" | "MONTHLY";
export type SessionScope = "CLIENT" | "ADMIN";

export const users = pgTable("users", {
  id: uuid("id").primaryKey(),
  email: text("email").notNull().unique(),
  passwordHash: text("password_hash").notNull(),
  displayName: text("display_name").notNull(),
  isPlatformAdmin: boolean("is_platform_admin").notNull().default(false),
  status: text("status").$type<"ACTIVE" | "SUSPENDED">().notNull().default("ACTIVE"),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const sessions = pgTable("sessions", {
  id: uuid("id").primaryKey(),
  userId: uuid("user_id")
    .notNull()
    .references(() => users.id, { onDelete: "cascade" }),
  scope: text("scope").$type<SessionScope>().notNull().default("CLIENT"),
  tokenHash: text("token_hash").notNull().unique(),
  refreshTokenHash: text("refresh_token_hash").notNull().unique(),
  expiresAt: timestamp("expires_at", { withTimezone: true, mode: "date" }).notNull(),
  refreshExpiresAt: timestamp("refresh_expires_at", {
    withTimezone: true,
    mode: "date",
  }).notNull(),
  revokedAt: timestamp("revoked_at", { withTimezone: true, mode: "date" }),
  userAgent: text("user_agent"),
  ipAddress: text("ip_address"),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const organizations = pgTable("organizations", {
  id: uuid("id").primaryKey(),
  slug: text("slug").notNull().unique(),
  name: text("name").notNull(),
  kind: text("kind").$type<OrgKind>().notNull(),
  ownerUserId: uuid("owner_user_id")
    .notNull()
    .references(() => users.id),
  status: text("status").$type<"ACTIVE" | "SUSPENDED">().notNull().default("ACTIVE"),
  inferenceTargetStatus: text("inference_target_status")
    .$type<"ACTIVE" | "SUSPENDED">()
    .notNull()
    .default("ACTIVE"),
  inferenceRevision: integer("inference_revision").notNull().default(0),
  inferenceSyncedRevision: integer("inference_synced_revision").notNull().default(0),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const orgMembers = pgTable(
  "org_members",
  {
    orgId: uuid("org_id")
      .notNull()
      .references(() => organizations.id, { onDelete: "cascade" }),
    userId: uuid("user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    role: text("role").$type<OrgRole>().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  },
  (t) => [primaryKey({ columns: [t.orgId, t.userId] })],
);

export const orgInvitations = pgTable("org_invitations", {
  id: uuid("id").primaryKey(),
  orgId: uuid("org_id")
    .notNull()
    .references(() => organizations.id, { onDelete: "cascade" }),
  email: text("email").notNull(),
  role: text("role").$type<Exclude<OrgRole, "OWNER">>().notNull(),
  tokenHash: text("token_hash").notNull().unique(),
  invitedBy: uuid("invited_by")
    .notNull()
    .references(() => users.id),
  status: text("status")
    .$type<"PENDING" | "ACCEPTED" | "REVOKED" | "EXPIRED">()
    .notNull()
    .default("PENDING"),
  expiresAt: timestamp("expires_at", { withTimezone: true, mode: "date" }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const billingAccounts = pgTable("billing_accounts", {
  id: uuid("id").primaryKey(),
  orgId: uuid("org_id")
    .notNull()
    .references(() => organizations.id, { onDelete: "cascade" }),
  gatewayBillingAccountId: uuid("gateway_billing_account_id").notNull().unique(),
  name: text("name").notNull(),
  currency: text("currency").notNull().default("USD"),
  rpm: integer("rpm").notNull().default(6000),
  tpm: bigint("tpm", { mode: "number" }).notNull().default(10_000_000),
  concurrency: integer("concurrency").notNull().default(1000),
  developerKeySpendLimitAtoms: numeric("developer_key_spend_limit_atoms", {
    precision: 38,
    scale: 0,
  }),
  developerMaxActiveKeys: integer("developer_max_active_keys"),
  enabled: boolean("enabled").notNull().default(true),
  gatewaySynced: boolean("gateway_synced").notNull().default(false),
  desiredRevision: integer("desired_revision").notNull().default(1),
  syncedRevision: integer("synced_revision").notNull().default(0),
  syncStatus: text("sync_status").$type<SyncStatus>().notNull().default("PENDING"),
  lastSyncError: text("last_sync_error"),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const apiKeyRecords = pgTable("api_key_records", {
  id: uuid("id").primaryKey(),
  billingAccountId: uuid("billing_account_id")
    .notNull()
    .references(() => billingAccounts.id, { onDelete: "cascade" }),
  createdBy: uuid("created_by")
    .notNull()
    .references(() => users.id),
  rotatedFromKeyId: uuid("rotated_from_key_id").references((): AnyPgColumn => apiKeyRecords.id, {
    onDelete: "set null",
  }),
  name: text("name").notNull(),
  keyPrefix: text("key_prefix").notNull(),
  role: text("role")
    .$type<"ADMIN" | "BILLING" | "INVOKER" | "VIEWER">()
    .notNull()
    .default("INVOKER"),
  allowedCidrs: text("allowed_cidrs").array().notNull().default([]),
  models: text("models").array().notNull().default([]),
  operations: text("operations").array().notNull().default([]),
  spendLimitAtoms: numeric("spend_limit_atoms", { precision: 38, scale: 0 }),
  spendLimitWindow: text("spend_limit_window")
    .$type<SpendLimitWindow>()
    .notNull()
    .default("LIFETIME"),
  enabled: boolean("enabled").notNull().default(true),
  expiresAt: timestamp("expires_at", { withTimezone: true, mode: "date" }),
  revokedAt: timestamp("revoked_at", { withTimezone: true, mode: "date" }),
  desiredRevision: integer("desired_revision").notNull().default(1),
  syncedRevision: integer("synced_revision").notNull().default(0),
  syncStatus: text("sync_status").$type<SyncStatus>().notNull().default("PENDING"),
  lastSyncError: text("last_sync_error"),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const upstreamAccounts = pgTable("upstream_accounts", {
  id: uuid("id").primaryKey(),
  name: text("name").notNull().unique(),
  state: text("state")
    .$type<"ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED">()
    .notNull()
    .default("ACTIVE"),
  pendingState: text("pending_state").$type<"ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED">(),
  expectedState: text("expected_state").$type<"ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED">(),
  desiredRevision: integer("desired_revision").notNull().default(1),
  syncedRevision: integer("synced_revision").notNull().default(0),
  syncStatus: text("sync_status").$type<SyncStatus>().notNull().default("PENDING"),
  lastSyncError: text("last_sync_error"),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const channels = pgTable("channels", {
  id: uuid("id").primaryKey(),
  accountId: uuid("account_id")
    .notNull()
    .references(() => upstreamAccounts.id),
  name: text("name").notNull(),
  kind: text("kind").notNull(),
  providerPluginId: uuid("provider_plugin_id"),
  baseUrl: text("base_url").notNull(),
  keyHint: text("key_hint").notNull().default(""),
  secretSyncPending: boolean("secret_sync_pending").notNull().default(false),
  rpm: integer("rpm").notNull().default(1_000_000),
  concurrency: integer("concurrency").notNull().default(1000),
  enabled: boolean("enabled").notNull().default(true),
  routingPolicy: jsonb("routing_policy").$type<Record<string, unknown>>().notNull().default({}),
  adapterConfig: jsonb("adapter_config").$type<Record<string, unknown>>().notNull().default({}),
  desiredRevision: integer("desired_revision").notNull().default(1),
  syncedRevision: integer("synced_revision").notNull().default(0),
  syncStatus: text("sync_status").$type<SyncStatus>().notNull().default("PENDING"),
  lastSyncError: text("last_sync_error"),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const pricingTemplates = pgTable("pricing_templates", {
  id: uuid("id").primaryKey(),
  name: text("name").notNull().unique(),
  side: text("side").$type<"CUSTOMER" | "SUPPLIER">().notNull(),
  currency: text("currency").notNull().default("USD"),
  basis: text("basis")
    .$type<"PROVIDER_USAGE" | "REQUEST_COMPLETION" | "INPUT_CHARACTERS">()
    .notNull()
    .default("PROVIDER_USAGE"),
  rates: jsonb("rates").$type<Pricing["rates"]>().notNull(),
  ceilings: jsonb("ceilings").$type<Record<string, string>>().notNull().default({}),
  currentPriceVersionId: uuid("current_price_version_id"),
  pendingPriceVersionId: uuid("pending_price_version_id"),
  pendingPriceRevision: integer("pending_price_revision"),
  desiredRevision: integer("desired_revision").notNull().default(1),
  syncedRevision: integer("synced_revision").notNull().default(0),
  syncStatus: text("sync_status").$type<SyncStatus>().notNull().default("PENDING"),
  lastSyncError: text("last_sync_error"),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const priceVersionSnapshots = pgTable("price_version_snapshots", {
  id: uuid("id").primaryKey(),
  templateId: uuid("template_id").references(() => pricingTemplates.id, { onDelete: "set null" }),
  side: text("side").$type<"CUSTOMER" | "SUPPLIER">().notNull(),
  currency: text("currency").notNull().default("USD"),
  basis: text("basis")
    .$type<"PROVIDER_USAGE" | "REQUEST_COMPLETION" | "INPUT_CHARACTERS">()
    .notNull()
    .default("PROVIDER_USAGE"),
  rates: jsonb("rates").$type<Pricing["rates"]>().notNull(),
  ceilings: jsonb("ceilings").$type<Record<string, string>>().notNull().default({}),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const routeReleaseRequests = pgTable("route_release_requests", {
  id: uuid("id").primaryKey(),
  modelName: text("model_name").notNull(),
  actorUserId: uuid("actor_user_id").notNull(),
  actorEmail: text("actor_email").notNull(),
  digest: text("digest").notNull(),
  intent: jsonb("intent").$type<RouteReleaseIntent | ModelOnboardingInput>().notNull(),
  state: text("state").$type<"PENDING" | "COMMITTED" | "REJECTED">().notNull().default("PENDING"),
  rejection: jsonb("rejection").$type<{ status: number; code: string; message: string }>(),
  nextAttemptAt: timestamp("next_attempt_at", { withTimezone: true, mode: "date" })
    .notNull()
    .defaultNow(),
});

export const modelCatalog = pgTable("model_catalog", {
  name: text("name").primaryKey(),
  displayName: text("display_name").notNull(),
  providerBrand: text("provider_brand").notNull().default("gateway"),
  description: text("description").notNull().default(""),
  category: text("category").notNull().default("chat"),
  contextLength: integer("context_length").notNull().default(128_000),
  maxOutputTokens: integer("max_output_tokens").notNull().default(32_768),
  operations: text("operations").array().notNull(),
  routingStrategy: text("routing_strategy")
    .$type<"WEIGHTED" | "ROUND_ROBIN" | "LEAST_LATENCY">()
    .notNull()
    .default("WEIGHTED"),
  adaptiveRouting: jsonb("adaptive_routing").$type<AdaptiveRoutingSetting>(),
  tags: text("tags").array().notNull().default([]),
  listPriceVersionId: uuid("list_price_version_id"),
  listRates: jsonb("list_rates").$type<Pricing["rates"]>().notNull().default({}),
  currency: text("currency").notNull().default("USD"),
  isPublic: boolean("is_public").notNull().default(true),
  enabled: boolean("enabled").notNull().default(true),
  pendingReleaseId: uuid("pending_release_id").references(() => routeReleaseRequests.id),
  desiredRevision: integer("desired_revision").notNull().default(1),
  syncedRevision: integer("synced_revision").notNull().default(0),
  syncStatus: text("sync_status").$type<SyncStatus>().notNull().default("PENDING"),
  lastSyncError: text("last_sync_error"),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const routes = pgTable("routes", {
  id: uuid("id").primaryKey(),
  modelName: text("model_name")
    .notNull()
    .references(() => modelCatalog.name, { onDelete: "cascade" }),
  channelId: uuid("channel_id")
    .notNull()
    .references(() => channels.id),
  upstreamModel: text("upstream_model").notNull(),
  operations: text("operations").array().notNull(),
  priority: integer("priority").notNull().default(0),
  weight: doublePrecision("weight").notNull().default(1),
  billingPriceId: uuid("billing_price_id").notNull(),
  costVersionId: uuid("cost_version_id").notNull(),
  requestPolicy: jsonb("request_policy").$type<Record<string, unknown>>().notNull().default({}),
  adapterOverrides: jsonb("adapter_overrides")
    .$type<Record<string, unknown>>()
    .notNull()
    .default({}),
  retryPolicy: jsonb("retry_policy").$type<Record<string, unknown>>().notNull().default({}),
  enabled: boolean("enabled").notNull().default(true),
  pendingReleaseId: uuid("pending_release_id").references(() => routeReleaseRequests.id),
  desiredRevision: integer("desired_revision").notNull().default(1),
  syncedRevision: integer("synced_revision").notNull().default(0),
  syncStatus: text("sync_status").$type<SyncStatus>().notNull().default("PENDING"),
  lastSyncError: text("last_sync_error"),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const pricingOverrides = pgTable(
  "pricing_overrides",
  {
    id: uuid("id").primaryKey(),
    billingAccountId: uuid("billing_account_id")
      .notNull()
      .references(() => billingAccounts.id, { onDelete: "cascade" }),
    modelName: text("model_name").notNull(),
    operation: text("operation").notNull().default("*"),
    overrideKind: text("override_kind").$type<"EXACT_PRICE" | "MULTIPLIER">().notNull(),
    billingPriceId: uuid("billing_price_id"),
    multiplierNumerator: numeric("multiplier_numerator", { precision: 38, scale: 0 }),
    multiplierDenominator: numeric("multiplier_denominator", { precision: 38, scale: 0 }),
    enabled: boolean("enabled").notNull().default(true),
    desiredRevision: integer("desired_revision").notNull().default(1),
    syncedRevision: integer("synced_revision").notNull().default(0),
    syncStatus: text("sync_status").$type<SyncStatus>().notNull().default("PENDING"),
    lastSyncError: text("last_sync_error"),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  },
  (t) => [
    unique("pricing_overrides_billing_account_id_model_name_operation_key").on(
      t.billingAccountId,
      t.modelName,
      t.operation,
    ),
  ],
);

export const usageEvents = pgTable("usage_events", {
  eventId: uuid("event_id").primaryKey(),
  seq: bigint("seq", { mode: "number" }),
  kind: text("kind").notNull(),
  requestId: uuid("request_id"),
  chargeId: uuid("charge_id"),
  billingAccountId: uuid("billing_account_id"),
  orgId: uuid("org_id"),
  apiKeyId: uuid("api_key_id"),
  model: text("model"),
  operation: text("operation"),
  channelId: uuid("channel_id"),
  outcome: text("outcome"),
  currency: text("currency").notNull().default("USD"),
  customerAtoms: numeric("customer_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
  supplierCostAtoms: numeric("supplier_cost_atoms", { precision: 38, scale: 0 })
    .notNull()
    .default("0"),
  inputTokens: bigint("input_tokens", { mode: "number" }).notNull().default(0),
  outputTokens: bigint("output_tokens", { mode: "number" }).notNull().default(0),
  cachedInputTokens: bigint("cached_input_tokens", { mode: "number" }).notNull().default(0),
  reasoningTokens: bigint("reasoning_tokens", { mode: "number" }).notNull().default(0),
  pricingOverrideId: uuid("pricing_override_id"),
  payload: jsonb("payload").$type<Record<string, unknown>>().notNull().default({}),
  occurredAt: timestamp("occurred_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const usageHourlyRollups = pgTable(
  "usage_hourly_rollups",
  {
    bucketHour: timestamp("bucket_hour", { withTimezone: true, mode: "date" }).notNull(),
    billingAccountId: uuid("billing_account_id").notNull(),
    apiKeyId: text("api_key_id").notNull(),
    model: text("model").notNull(),
    operation: text("operation").notNull(),
    currency: text("currency").notNull(),
    requestCount: bigint("request_count", { mode: "number" }).notNull().default(0),
    customerAtoms: numeric("customer_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    supplierCostAtoms: numeric("supplier_cost_atoms", { precision: 38, scale: 0 })
      .notNull()
      .default("0"),
    inputTokens: bigint("input_tokens", { mode: "number" }).notNull().default(0),
    outputTokens: bigint("output_tokens", { mode: "number" }).notNull().default(0),
    cachedInputTokens: bigint("cached_input_tokens", { mode: "number" }).notNull().default(0),
    reasoningTokens: bigint("reasoning_tokens", { mode: "number" }).notNull().default(0),
  },
  (t) => [
    primaryKey({
      columns: [t.bucketHour, t.billingAccountId, t.apiKeyId, t.model, t.operation, t.currency],
    }),
  ],
);

export const usageDailyRollups = pgTable(
  "usage_daily_rollups",
  {
    bucketDay: date("bucket_day", { mode: "string" }).notNull(),
    billingAccountId: uuid("billing_account_id").notNull(),
    apiKeyId: text("api_key_id").notNull(),
    model: text("model").notNull(),
    operation: text("operation").notNull(),
    currency: text("currency").notNull(),
    requestCount: bigint("request_count", { mode: "number" }).notNull().default(0),
    customerAtoms: numeric("customer_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    supplierCostAtoms: numeric("supplier_cost_atoms", { precision: 38, scale: 0 })
      .notNull()
      .default("0"),
    inputTokens: bigint("input_tokens", { mode: "number" }).notNull().default(0),
    outputTokens: bigint("output_tokens", { mode: "number" }).notNull().default(0),
    cachedInputTokens: bigint("cached_input_tokens", { mode: "number" }).notNull().default(0),
    reasoningTokens: bigint("reasoning_tokens", { mode: "number" }).notNull().default(0),
  },
  (t) => [
    primaryKey({
      columns: [t.bucketDay, t.billingAccountId, t.apiKeyId, t.model, t.operation, t.currency],
    }),
  ],
);

export const alertRules = pgTable("alert_rules", {
  id: uuid("id").primaryKey(),
  billingAccountId: uuid("billing_account_id")
    .notNull()
    .references(() => billingAccounts.id, { onDelete: "cascade" }),
  kind: text("kind").$type<"LOW_BALANCE" | "DAILY_SPEND">().notNull(),
  thresholdAtoms: numeric("threshold_atoms", { precision: 38, scale: 0 }).notNull(),
  webhookUrl: text("webhook_url"),
  enabled: boolean("enabled").notNull().default(true),
  lastTriggeredAt: timestamp("last_triggered_at", { withTimezone: true, mode: "date" }),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const alertEvalQueue = pgTable("alert_eval_queue", {
  billingAccountId: uuid("billing_account_id")
    .primaryKey()
    .references(() => billingAccounts.id, { onDelete: "cascade" }),
  dirtyAt: timestamp("dirty_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
  generation: uuid("generation").notNull().defaultRandom(),
  nextAttemptAt: timestamp("next_attempt_at", { withTimezone: true, mode: "date" })
    .notNull()
    .defaultNow(),
});

export const alertDeliveries = pgTable("alert_deliveries", {
  id: uuid("id").primaryKey(),
  ruleId: uuid("rule_id")
    .notNull()
    .references(() => alertRules.id, { onDelete: "cascade" }),
  billingAccountId: uuid("billing_account_id").notNull(),
  status: text("status").$type<"RECORDED" | "PENDING" | "DELIVERED" | "FAILED">().notNull(),
  webhookUrl: text("webhook_url"),
  attempts: integer("attempts").notNull().default(0),
  nextAttemptAt: timestamp("next_attempt_at", { withTimezone: true, mode: "date" })
    .notNull()
    .defaultNow(),
  lastHttpStatus: integer("last_http_status"),
  lastError: text("last_error"),
  deliveredAt: timestamp("delivered_at", { withTimezone: true, mode: "date" }),
  payload: jsonb("payload").$type<Record<string, unknown>>().notNull().default({}),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const consoleAuditLogs = pgTable("console_audit_logs", {
  id: uuid("id").primaryKey(),
  actorUserId: uuid("actor_user_id"),
  actorEmail: text("actor_email"),
  orgId: uuid("org_id"),
  action: text("action").notNull(),
  targetType: text("target_type").notNull(),
  targetId: text("target_id").notNull(),
  metadata: jsonb("metadata").$type<Record<string, unknown>>().notNull().default({}),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const platformCreditDefaults = pgTable("platform_credit_defaults", {
  id: smallint("id").primaryKey(),
  defaultCreditLimitAtoms: numeric("default_credit_limit_atoms", {
    precision: 38,
    scale: 0,
  })
    .notNull()
    .default("0"),
  defaultWalletPolicy: text("default_wallet_policy")
    .$type<"GIFT_FIRST" | "CASH_FIRST" | "EARLIEST_EXPIRY">()
    .notNull()
    .default("GIFT_FIRST"),
  updatedBy: uuid("updated_by").references(() => users.id, { onDelete: "set null" }),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "date" }).notNull().defaultNow(),
});

export const consoleSchema = {
  users,
  sessions,
  organizations,
  orgMembers,
  orgInvitations,
  billingAccounts,
  apiKeyRecords,
  upstreamAccounts,
  channels,
  pricingTemplates,
  priceVersionSnapshots,
  modelCatalog,
  routes,
  routeReleaseRequests,
  pricingOverrides,
  usageEvents,
  usageHourlyRollups,
  usageDailyRollups,
  alertRules,
  alertEvalQueue,
  alertDeliveries,
  consoleAuditLogs,
  platformCreditDefaults,
};

export function createConsoleDrizzle(db: SQL) {
  return drizzle({ client: db, schema: consoleSchema });
}

export type ConsoleDrizzle = ReturnType<typeof createConsoleDrizzle>;
