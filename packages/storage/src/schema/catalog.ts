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
  unique,
  check,
  foreignKey,
  customType,
  type PgTableExtraConfigValue,
  type AnyPgColumn,
} from "drizzle-orm/pg-core";
import { jsonb } from "./jsonb.ts";

const atoms = (name: string) => numeric(name, { precision: 38, scale: 0 });
const instant = (name: string) => timestamp(name, { withTimezone: true, mode: "string" });
const bytes = customType<{ data: Buffer; driverData: Buffer }>({ dataType: () => "bytea" });
export const supplierBrands = pgTable("supplier_brands", {
  id: uuid().primaryKey(),
  name: text().notNull().unique(),
  enabled: boolean().default(true).notNull(),
  createdAt: instant("created_at").defaultNow().notNull(),
});

export const upstreamAccounts = pgTable(
  "upstream_accounts",
  {
    id: uuid().primaryKey(),
    brandId: uuid("brand_id")
      .notNull()
      .references(() => supplierBrands.id),
    name: text().notNull(),
    mode: text().default("PREPAID").notNull(),
    state: text().default("ACTIVE").notNull(),
    revision: atoms("revision").default("0").notNull(),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  (t) => [
    unique().on(t.id, t.brandId),
    unique().on(t.brandId, t.name),
    check("upstream_account_mode", sql`mode='PREPAID'`),
    check(
      "upstream_account_state",
      sql`state IN ('ACTIVE','DISABLED','RECONCILE_REQUIRED','QUARANTINED')`,
    ),
    check("upstream_account_revision", sql`revision>=0`),
  ],
);

export const upstreamAccountBalances = pgTable(
  "upstream_account_balances",
  {
    accountId: uuid("account_id")
      .notNull()
      .references(() => upstreamAccounts.id),
    currency: text().notNull(),
    balanceAtoms: atoms("balance_atoms").default("0").notNull(),
    reservedAtoms: atoms("reserved_atoms").default("0").notNull(),
    spentAtoms: atoms("spent_atoms").default("0").notNull(),
    adjustmentAtoms: atoms("adjustment_atoms").default("0").notNull(),
    revision: atoms("revision").default("0").notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.accountId, t.currency] }),
    check("upstream_balance_currency", sql`currency ~ '^[A-Z]{3,8}$'`),
    check(
      "upstream_balance_nonnegative",
      sql`reserved_atoms>=0 AND spent_atoms>=0 AND revision>=0`,
    ),
  ],
);

export const technicalAdministrators = pgTable("technical_administrators", {
  id: uuid().primaryKey(),
  name: text().notNull(),
  enabled: boolean().default(true).notNull(),
  permissions: text().array().notNull(),
  createdAt: instant("created_at").defaultNow().notNull(),
});

export const administratorCredentials = pgTable("administrator_credentials", {
  id: uuid().primaryKey(),
  administratorId: uuid("administrator_id")
    .notNull()
    .references(() => technicalAdministrators.id),
  verifier: text().notNull().unique(),
  prefix: text().notNull(),
  enabled: boolean().default(true).notNull(),
  expiresAt: instant("expires_at"),
  createdAt: instant("created_at").defaultNow().notNull(),
});

export const pluginPackages = pgTable(
  "plugin_packages",
  {
    digest: text().primaryKey(),
    packageBytes: bytes("package_bytes").notNull(),
    manifest: jsonb().notNull(),
    signature: text().notNull(),
    signingKeyId: text("signing_key_id").notNull(),
    releaseDescriptor: jsonb("release_descriptor"),
    releaseSignature: text("release_signature"),
    releaseKeyId: text("release_key_id"),
    uploadedBy: text("uploaded_by").notNull(),
    uploadedAt: instant("uploaded_at").defaultNow().notNull(),
  },
  () => [
    check("plugin_digest", sql`digest ~ '^[a-f0-9]{64}$'`),
    check(
      "plugin_package_size",
      sql`octet_length(package_bytes)>0 AND octet_length(package_bytes)<=46137344`,
    ),
    check("plugin_manifest", sql`jsonb_typeof(manifest)='object'`),
  ],
);

export const pluginVersions = pgTable(
  "plugin_versions",
  {
    id: uuid().primaryKey(),
    name: text().notNull(),
    version: text().notNull(),
    packageDigest: text("package_digest")
      .notNull()
      .references(() => pluginPackages.digest),
    abiVersion: integer("abi_version").notNull(),
    configSchemaDigest: text("config_schema_digest").notNull(),
    state: text().default("UPLOADED").notNull(),
    quarantined: boolean().default(false).notNull(),
    revision: atoms("revision").default("0").notNull(),
    validationResult: jsonb("validation_result"),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  (t) => [
    unique().on(t.name, t.version),
    check(
      "plugin_version_state",
      sql`state IN ('UPLOADED','VALIDATING','VALIDATED','PUBLISHED','REJECTED','DISABLED')`,
    ),
    check("plugin_version_revision", sql`abi_version>0 AND revision>=0`),
    check("plugin_config_digest", sql`config_schema_digest ~ '^[a-f0-9]{64}$'`),
  ],
);

export const channelPluginBindings = pgTable(
  "channel_plugin_bindings",
  {
    id: uuid().primaryKey(),
    channelId: uuid("channel_id")
      .notNull()
      .references((): AnyPgColumn => channels.id),
    pluginVersionId: uuid("plugin_version_id")
      .notNull()
      .references(() => pluginVersions.id),
    accountId: uuid("account_id"),
    brandId: uuid("brand_id"),
    secretReference: text("secret_reference"),
    configRevision: atoms("config_revision").notNull(),
    config: jsonb().notNull(),
    configDigest: text("config_digest").notNull(),
    actor: text().notNull(),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  (t) => [
    unique().on(t.channelId, t.configRevision),
    check(
      "binding_config",
      sql`jsonb_typeof(config)='object' AND config_revision>=0 AND config_digest ~ '^[a-f0-9]{64}$'`,
    ),
  ],
);

export const financialVouchers = pgTable(
  "financial_vouchers",
  {
    id: uuid().primaryKey(),
    digest: text().notNull().unique(),
    contentType: text("content_type").notNull(),
    content: bytes().notNull(),
    actor: text().notNull(),
    expiresAt: instant("expires_at").notNull(),
    createdAt: instant("created_at").defaultNow().notNull(),
  },
  (t) => [
    unique("financial_vouchers_id_digest_key").on(t.id, t.digest),
    check("voucher_content_digest", sql`digest=encode(sha256(content),'hex')`),
    check("voucher_size", sql`octet_length(content)>0 AND octet_length(content)<=5242880`),
    check("voucher_type", sql`content_type IN ('image/png','image/jpeg','application/pdf')`),
    check("voucher_digest", sql`digest ~ '^[a-f0-9]{64}$'`),
  ],
);

export const channels = pgTable(
  "channels",
  {
    brandId: uuid("brand_id")
      .notNull()
      .references((): AnyPgColumn => supplierBrands.id),
    accountId: uuid("account_id").notNull(),
    bindingId: uuid("binding_id").references((): AnyPgColumn => channelPluginBindings.id),
    configRevision: numeric("config_revision", { precision: 38, scale: 0 }).default("0").notNull(),
    id: uuid().primaryKey().notNull(),
    name: text().notNull(),
    kind: text().notNull(),
    baseUrl: text("base_url").notNull(),
    secret: text().notNull(),
    enabled: boolean().default(true).notNull(),
    concurrency: integer().notNull(),
    rpm: integer().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
  },
  (table): PgTableExtraConfigValue[] => [
    unique("channels_name_key").on(table.name),
    foreignKey({
      columns: [table.accountId, table.brandId],
      foreignColumns: [upstreamAccounts.id, upstreamAccounts.brandId],
      name: "channel_account_brand",
    }),
    check("channels_concurrency_check", sql`concurrency > 0`),
    check("channels_rpm_check", sql`rpm > 0`),
    check(
      "channels_kind_check",
      sql`kind = ANY (ARRAY['openai'::text, 'anthropic'::text, 'gemini'::text, 'deepseek'::text, 'ollama'::text, 'azure'::text, 'bedrock'::text, 'vertex'::text])`,
    ),
  ],
);
