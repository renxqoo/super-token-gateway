import type { SQL } from "bun";
import { drizzle } from "drizzle-orm/bun-sql";
import {
  bigint,
  bigserial,
  boolean,
  customType,
  doublePrecision,
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
import type { Operation, Pricing, Usage } from "@gateway/kernel";
import type {
  AdapterOverrides,
  ChargeLotAllocation,
  ChargePolicy,
  ChannelKind,
  HoldLotAllocation,
  LeaseLotAllocation,
  LotKind,
  PluginAuditEntry,
  PluginCapability,
  PluginKind,
  PluginPipelineStep,
  PriceBasis,
  PriceSide,
  ProviderPluginConfig,
  RouteRetryPolicy,
  RoutingStrategy,
  SupplierHoldEntry,
  WalletPolicy,
  WebhookPluginConfig,
} from "./types.ts";

function pgJsonb<T>(name: string) {
  return customType<{ data: T; driverData: T }>({
    dataType() {
      return "jsonb";
    },
    toDriver(value: T): T {
      return value;
    },
    fromDriver(value: unknown): T {
      if (typeof value === "string") {
        try {
          return JSON.parse(value) as T;
        } catch {
          return value as T;
        }
      }
      return value as T;
    },
  })(name);
}

const bytea = customType<{ data: Uint8Array; driverData: Uint8Array }>({
  dataType() {
    return "bytea";
  },
  toDriver(value: Uint8Array): Uint8Array {
    return value;
  },
  fromDriver(value: unknown): Uint8Array {
    if (value instanceof Uint8Array) return value;
    if (typeof value === "string" && value.startsWith("\\x")) {
      return new Uint8Array(Buffer.from(value.slice(2), "hex"));
    }
    return new Uint8Array(value as ArrayBufferLike);
  },
});

export const schemaMigrations = pgTable("schema_migrations", {
  version: integer("version").primaryKey(),
  checksum: text("checksum").notNull(),
  manifest: pgJsonb<Record<string, unknown>>("manifest").notNull(),
  appliedAt: timestamp("applied_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
});

export const configEpoch = pgTable("config_epoch", {
  id: smallint("id").primaryKey(),
  version: bigint("version", { mode: "number" }).notNull().default(1),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
});

export const projects = pgTable("projects", {
  id: uuid("id").primaryKey(),
  externalRef: text("external_ref").unique(),
  name: text("name").notNull(),
  currency: text("currency").notNull().default("USD"),
  rpm: integer("rpm").notNull(),
  tpm: bigint("tpm", { mode: "number" }).notNull(),
  concurrency: integer("concurrency").notNull(),
  enabled: boolean("enabled").notNull().default(true),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
});

export const apiKeys = pgTable(
  "api_keys",
  {
    id: uuid("id").primaryKey(),
    projectId: uuid("project_id")
      .notNull()
      .references(() => projects.id),
    name: text("name").notNull(),
    verifier: text("verifier").notNull().unique(),
    prefix: text("prefix").notNull(),
    role: text("role")
      .$type<"ADMIN" | "BILLING" | "INVOKER" | "VIEWER">()
      .notNull()
      .default("INVOKER"),
    allowedCidrs: text("allowed_cidrs")
      .array()
      .notNull()
      .default(sql`'{}'::text[]`),
    models: text("models")
      .array()
      .notNull()
      .default(sql`'{}'::text[]`),
    operations: text("operations")
      .array()
      .notNull()
      .default(sql`'{}'::text[]`),
    enabled: boolean("enabled").notNull().default(true),
    expiresAt: timestamp("expires_at", { withTimezone: true, mode: "string" }),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [index("api_keys_project_idx").on(t.projectId)],
);

export const upstreamAccounts = pgTable("upstream_accounts", {
  id: uuid("id").primaryKey(),
  name: text("name").notNull().unique(),
  state: text("state")
    .$type<"ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED">()
    .notNull()
    .default("ACTIVE"),
  revision: numeric("revision", { precision: 38, scale: 0 }).notNull().default("0"),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
});

export const plugins = pgTable(
  "plugins",
  {
    id: uuid("id").primaryKey(),
    name: text("name").notNull(),
    version: text("version").notNull(),
    capability: text("capability").$type<PluginCapability>().notNull().default("CONTENT"),
    kind: text("kind").$type<PluginKind>().notNull(),
    digest: text("digest").notNull(),
    wasmBytes: bytea("wasm_bytes"),
    webhookConfig: pgJsonb<WebhookPluginConfig | null>("webhook_config"),
    providerConfig: pgJsonb<ProviderPluginConfig | null>("provider_config"),
    signature: text("signature"),
    signingKeyId: text("signing_key_id"),
    manifest: pgJsonb<Record<string, unknown>>("manifest")
      .notNull()
      .default(sql`'{}'::jsonb`),
    enabled: boolean("enabled").notNull().default(true),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [unique("plugins_name_version_key").on(t.name, t.version)],
);

export const channels = pgTable(
  "channels",
  {
    id: uuid("id").primaryKey(),
    accountId: uuid("account_id")
      .notNull()
      .references(() => upstreamAccounts.id),
    name: text("name").notNull(),
    kind: text("kind").$type<ChannelKind>().notNull(),
    providerPluginId: uuid("provider_plugin_id").references(() => plugins.id),
    baseUrl: text("base_url").notNull(),
    secretCiphertext: text("secret_ciphertext").notNull(),
    rpm: integer("rpm").notNull(),
    concurrency: integer("concurrency").notNull(),
    enabled: boolean("enabled").notNull().default(true),
    routingPolicy: pgJsonb<Record<string, unknown>>("routing_policy")
      .notNull()
      .default(sql`'{}'::jsonb`),
    adapterConfig: pgJsonb<AdapterOverrides>("adapter_config")
      .notNull()
      .default(sql`'{}'::jsonb`),
    configRevision: bigint("config_revision", { mode: "number" }).notNull().default(1),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [index("channels_account_idx").on(t.accountId)],
);

export const models = pgTable("models", {
  name: text("name").primaryKey(),
  operations: text("operations").array().$type<Operation[]>().notNull(),
  routingStrategy: text("routing_strategy").$type<RoutingStrategy>().notNull().default("WEIGHTED"),
  maxOutputTokens: integer("max_output_tokens").notNull(),
  enabled: boolean("enabled").notNull().default(true),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
});

export const priceVersions = pgTable("price_versions", {
  id: uuid("id").primaryKey(),
  side: text("side").$type<PriceSide>().notNull(),
  currency: text("currency").notNull(),
  basis: text("basis").$type<PriceBasis>().notNull().default("PROVIDER_USAGE"),
  rates: pgJsonb<Pricing["rates"]>("rates").notNull(),
  ceilings: pgJsonb<Record<string, string>>("ceilings")
    .notNull()
    .default(sql`'{}'::jsonb`),
  sourceDigest: text("source_digest").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
});

export const routes = pgTable(
  "routes",
  {
    id: uuid("id").primaryKey(),
    modelName: text("model_name")
      .notNull()
      .references(() => models.name),
    channelId: uuid("channel_id")
      .notNull()
      .references(() => channels.id),
    upstreamModel: text("upstream_model").notNull(),
    operations: text("operations").array().$type<Operation[]>().notNull(),
    priority: integer("priority").notNull().default(0),
    weight: doublePrecision("weight").notNull().default(1),
    billingPriceId: uuid("billing_price_id")
      .notNull()
      .references(() => priceVersions.id),
    costVersionId: uuid("cost_version_id")
      .notNull()
      .references(() => priceVersions.id),
    requestPolicy: pgJsonb<Record<string, unknown>>("request_policy")
      .notNull()
      .default(sql`'{}'::jsonb`),
    adapterOverrides: pgJsonb<AdapterOverrides>("adapter_overrides")
      .notNull()
      .default(sql`'{}'::jsonb`),
    pluginPipeline: pgJsonb<PluginPipelineStep[]>("plugin_pipeline")
      .notNull()
      .default(sql`'[]'::jsonb`),
    retryPolicy: pgJsonb<RouteRetryPolicy>("retry_policy")
      .notNull()
      .default(sql`'{}'::jsonb`),
    enabled: boolean("enabled").notNull().default(true),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    index("routes_model_lookup_idx")
      .on(t.modelName, t.priority)
      .where(sql`${t.enabled}`),
  ],
);

export const wallets = pgTable(
  "wallets",
  {
    id: uuid("id").primaryKey(),
    projectId: uuid("project_id")
      .notNull()
      .references(() => projects.id),
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
  (t) => [unique("wallets_project_id_currency_key").on(t.projectId, t.currency)],
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
    state: text("state").$type<"ACTIVE" | "CLOSED">().notNull().default("ACTIVE"),
    expiresAt: timestamp("expires_at", { withTimezone: true, mode: "string" }).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [index("supplier_leases_active_idx").on(t.expiresAt)],
);

export const billingHolds = pgTable(
  "billing_holds",
  {
    requestId: uuid("request_id").primaryKey(),
    projectId: uuid("project_id")
      .notNull()
      .references(() => projects.id),
    walletId: uuid("wallet_id")
      .notNull()
      .references(() => wallets.id),
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
    walletLeaseId: uuid("wallet_lease_id").references(() => walletLeases.id),
    supplierLeaseId: uuid("supplier_lease_id").references(() => supplierLeases.id),
    state: text("state")
      .$type<"HELD" | "SETTLED" | "RELEASED" | "UNKNOWN" | "CONFLICT">()
      .notNull()
      .default("HELD"),
    expiresAt: timestamp("expires_at", { withTimezone: true, mode: "string" }).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
    closedAt: timestamp("closed_at", { withTimezone: true, mode: "string" }),
  },
  (t) => [
    uniqueIndex("billing_holds_idempotency_idx")
      .on(t.projectId, t.operation, t.idempotencyKey)
      .where(sql`${t.idempotencyKey} IS NOT NULL`),
    index("billing_holds_open_idx").on(t.expiresAt),
    index("billing_holds_wallet_lease_idx").on(t.walletLeaseId),
    index("billing_holds_supplier_lease_idx").on(t.supplierLeaseId),
  ],
);

export const billingCharges = pgTable(
  "billing_charges",
  {
    id: uuid("id").primaryKey(),
    requestId: uuid("request_id")
      .notNull()
      .unique()
      .references(() => billingHolds.requestId),
    projectId: uuid("project_id")
      .notNull()
      .references(() => projects.id),
    walletId: uuid("wallet_id")
      .notNull()
      .references(() => wallets.id),
    selectedAttemptId: uuid("selected_attempt_id"),
    channelId: uuid("channel_id").references(() => channels.id),
    model: text("model").notNull(),
    operation: text("operation").notNull(),
    currency: text("currency").notNull(),
    billingPriceId: uuid("billing_price_id").references(() => priceVersions.id),
    costVersionId: uuid("cost_version_id").references(() => priceVersions.id),
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
    index("billing_charges_project_created_idx").on(t.projectId, t.createdAt),
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
    projectId: uuid("project_id"),
    payload: pgJsonb<Record<string, unknown>>("payload").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [index("outbox_events_project_idx").on(t.projectId, t.seq)],
);

export const outboxConsumers = pgTable("outbox_consumers", {
  name: text("name").primaryKey(),
  ackedSeq: bigint("acked_seq", { mode: "bigint" })
    .notNull()
    .default(sql`0`),
  leaseToken: uuid("lease_token"),
  leasedUntil: timestamp("leased_until", { withTimezone: true, mode: "string" }),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
});

export const ledgerSchema = {
  schemaMigrations,
  configEpoch,
  projects,
  apiKeys,
  upstreamAccounts,
  channels,
  models,
  priceVersions,
  plugins,
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
  outboxEvents,
  outboxConsumers,
};

export function createDrizzle(db: SQL) {
  return drizzle(db, { schema: ledgerSchema });
}

export type LedgerDrizzle = ReturnType<typeof createDrizzle>;
