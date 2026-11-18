import { sql } from "drizzle-orm";
import { boolean, check, pgSchema, primaryKey, text, timestamp } from "drizzle-orm/pg-core";
import { pluginPackages } from "./catalog.ts";
import { jsonb } from "./jsonb.ts";

export const executionRuntime = pgSchema("core_execution_runtime");
export const executionRuntimeApprovals = executionRuntime.table(
  "approvals",
  {
    packageDigest: text("package_digest")
      .notNull()
      .references(() => pluginPackages.digest),
    kind: text().notNull(),
    artifactDigest: text("artifact_digest").notNull(),
    active: boolean().notNull(),
    facts: jsonb().notNull(),
    actor: text().notNull(),
    reason: text().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true, mode: "string" })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.packageDigest, t.kind, t.artifactDigest] }),
    check("execution_runtime_kind", sql`kind IN ('wasm','builtin-worker')`),
    check("execution_runtime_digest", sql`artifact_digest ~ '^[a-f0-9]{64}$'`),
    check(
      "execution_runtime_facts",
      sql`jsonb_typeof(facts)='object' AND octet_length(facts::text)<=16384`,
    ),
    check(
      "execution_runtime_actor",
      sql`length(actor) BETWEEN 1 AND 100 AND length(reason) BETWEEN 1 AND 1000`,
    ),
  ],
);
