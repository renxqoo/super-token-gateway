import { sql } from "drizzle-orm";
import {
  pgTable,
  uuid,
  text,
  numeric,
  timestamp,
  unique,
  check,
  uniqueIndex,
  integer,
} from "drizzle-orm/pg-core";
import { jsonb } from "./jsonb.ts";
import { pluginVersions, pluginPackages } from "./catalog.ts";
import { executionCommands } from "./enterprise.ts";
const instant = (name: string) => timestamp(name, { withTimezone: true, mode: "string" });
export const pluginValidationRuns = pgTable(
  "plugin_validation_runs",
  {
    id: uuid().primaryKey(),
    versionId: uuid("version_id")
      .notNull()
      .references(() => pluginVersions.id),
    commandId: uuid("command_id")
      .notNull()
      .references(() => executionCommands.id),
    packageDigest: text("package_digest")
      .notNull()
      .references(() => pluginPackages.digest),
    fixtureDigest: text("fixture_digest").notNull(),
    policyDigest: text("policy_digest").notNull(),
    runtimeDigest: text("runtime_digest").notNull(),
    actor: text().notNull(),
    state: text().notNull(),
    leaseOwner: uuid("lease_owner").notNull(),
    leaseEpoch: numeric("lease_epoch", { precision: 38, scale: 0 }).notNull(),
    leaseExpiresAt: instant("lease_expires_at").notNull(),
    resourcePid: integer("resource_pid"),
    resourceConnection: text("resource_connection"),
    result: jsonb(),
    resultDigest: text("result_digest"),
    errorCode: text("error_code"),
    startedAt: instant("started_at").defaultNow().notNull(),
    finishedAt: instant("finished_at"),
  },
  (t) => [
    unique().on(t.commandId),
    uniqueIndex("plugin_one_active_validation")
      .on(t.versionId)
      .where(sql`state IN ('PENDING','RUNNING','CLEANUP_PENDING')`),
    check(
      "plugin_run_state",
      sql`state IN ('PENDING','RUNNING','CLEANUP_PENDING','SUCCEEDED','FAILED')`,
    ),
    check("plugin_run_epoch", sql`lease_epoch>0`),
    check(
      "plugin_run_digests",
      sql`fixture_digest ~ '^[a-f0-9]{64}$' AND policy_digest ~ '^[a-f0-9]{64}$' AND runtime_digest ~ '^[a-f0-9]{64}$' AND (result_digest IS NULL OR result_digest ~ '^[a-f0-9]{64}$')`,
    ),
    check(
      "plugin_run_result",
      sql`(result IS NULL OR (jsonb_typeof(result)='object' AND octet_length(result::text)<=4096)) AND (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,79}$')`,
    ),
  ],
);
export const pluginPublicationReceipts = pgTable("plugin_publication_receipts", {
  id: uuid().primaryKey(),
  versionId: uuid("version_id")
    .notNull()
    .unique()
    .references(() => pluginVersions.id),
  commandId: uuid("command_id")
    .notNull()
    .unique()
    .references(() => executionCommands.id),
  validationRunId: uuid("validation_run_id")
    .notNull()
    .references(() => pluginValidationRuns.id),
  packageDigest: text("package_digest")
    .notNull()
    .references(() => pluginPackages.digest),
  resultDigest: text("result_digest").notNull(),
  policyDigest: text("policy_digest").notNull(),
  actor: text().notNull(),
  receipt: jsonb().notNull(),
  createdAt: instant("created_at").defaultNow().notNull(),
});
export const pluginCatalogReceipts = pgTable("plugin_catalog_receipts", {
  commandId: uuid("command_id")
    .primaryKey()
    .references(() => executionCommands.id),
  receipt: jsonb().notNull(),
  createdAt: instant("created_at").defaultNow().notNull(),
});
