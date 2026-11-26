import {
  type AnyPgColumn,
  bigint,
  boolean,
  customType,
  doublePrecision,
  index,
  integer,
  numeric,
  pgTable,
  primaryKey,
  smallint,
  text,
  timestamp,
  unique,
  uuid,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import type { Operation, Pricing } from "@gateway/kernel";
import type {
  AdapterOverrides,
  ChannelKind,
  PluginCapability,
  PluginKind,
  PluginPipelineStep,
  PluginReleaseChange,
  PriceBasis,
  PriceSide,
  ProviderPluginConfig,
  RouteRetryPolicy,
  RoutingStrategy,
  WebhookPluginConfig,
} from "./types.ts";

export function pgJsonb<T>(name: string) {
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
  lineageToken: uuid("lineage_token")
    .notNull()
    .default(sql`gen_random_uuid()`),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
});

export const billingAccounts = pgTable("billing_accounts", {
  id: uuid("id").primaryKey(),
  externalRef: text("external_ref").unique(),
  name: text("name").notNull(),
  currency: text("currency").notNull().default("USD"),
  rpm: integer("rpm").notNull(),
  tpm: bigint("tpm", { mode: "number" }).notNull(),
  concurrency: integer("concurrency").notNull(),
  enabled: boolean("enabled").notNull().default(true),
  sourceRevision: integer("source_revision").notNull().default(0),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
});

export const consoleOrganizationAdmission = pgTable("console_organization_admission", {
  organizationId: uuid("organization_id").primaryKey(),
  suspended: boolean("suspended").notNull(),
  sourceRevision: integer("source_revision").notNull(),
});

export const apiKeys = pgTable(
  "api_keys",
  {
    id: uuid("id").primaryKey(),
    billingAccountId: uuid("billing_account_id")
      .notNull()
      .references(() => billingAccounts.id),
    name: text("name").notNull(),
    resourceScopeId: uuid("resource_scope_id"),
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
    expiresAt: timestamp("expires_at", { withTimezone: true, mode: "date" }),
    spendLimitAtoms: numeric("spend_limit_atoms", { precision: 38, scale: 0 }),
    spendLimitWindow: text("spend_limit_window")
      .$type<"LIFETIME" | "DAILY" | "MONTHLY">()
      .notNull()
      .default("LIFETIME"),
    spendWindowStart: timestamp("spend_window_start", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
    spentAtoms: numeric("spent_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    reservedAtoms: numeric("reserved_atoms", { precision: 38, scale: 0 }).notNull().default("0"),
    sourceRevision: integer("source_revision").notNull().default(0),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [index("api_keys_billing_account_idx").on(t.billingAccountId)],
);

export const upstreamAccounts = pgTable("upstream_accounts", {
  id: uuid("id").primaryKey(),
  name: text("name").notNull().unique(),
  state: text("state")
    .$type<"ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED">()
    .notNull()
    .default("ACTIVE"),
  revision: numeric("revision", { precision: 38, scale: 0 }).notNull().default("0"),
  sourceRevision: integer("source_revision").notNull().default(0),
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
    enabled: boolean("enabled").notNull().default(false),
    builtin: boolean("builtin").notNull().default(false),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [unique("plugins_name_version_key").on(t.name, t.version)],
);

export const pluginReleases = pgTable("plugin_releases", {
  id: uuid("id").primaryKey(),
  requestDigest: text("request_digest").notNull(),
  actor: text("actor").notNull(),
  configEpoch: bigint("config_epoch", { mode: "number" })
    .notNull()
    .unique("plugin_releases_config_epoch_key"),
  expectedNodes: uuid("expected_nodes")
    .array()
    .notNull()
    .default(sql`'{}'`),
  changes: pgJsonb<PluginReleaseChange[]>("changes").notNull(),
  rollbackOf: uuid("rollback_of").references((): AnyPgColumn => pluginReleases.id),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
});

export const pluginRevocations = pgTable("plugin_revocations", {
  pluginId: uuid("plugin_id")
    .primaryKey()
    .references(() => plugins.id),
  actor: text("actor").notNull(),
  reason: text("reason").notNull(),
  configEpoch: bigint("config_epoch", { mode: "number" }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true, mode: "string" }).notNull().defaultNow(),
});

export const gatewayConfigNodes = pgTable("gateway_config_nodes", {
  id: uuid("id").primaryKey(),
  appliedEpoch: bigint("applied_epoch", { mode: "number" }).notNull(),
  lastSeenAt: timestamp("last_seen_at", { withTimezone: true, mode: "string" })
    .notNull()
    .default(sql`clock_timestamp()`),
  stoppedAt: timestamp("stopped_at", { withTimezone: true, mode: "string" }),
});

export const pluginReleaseAcks = pgTable(
  "plugin_release_acks",
  {
    configEpoch: bigint("config_epoch", { mode: "number" })
      .notNull()
      .references(() => pluginReleases.configEpoch),
    nodeId: uuid("node_id")
      .notNull()
      .references(() => gatewayConfigNodes.id),
    appliedAt: timestamp("applied_at", { withTimezone: true, mode: "string" })
      .notNull()
      .default(sql`clock_timestamp()`),
  },
  (t) => [primaryKey({ columns: [t.configEpoch, t.nodeId] })],
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
    sourceRevision: integer("source_revision").notNull().default(0),
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
  sourceRevision: integer("source_revision").notNull().default(0),
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
    sourceRevision: integer("source_revision").notNull().default(0),
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
