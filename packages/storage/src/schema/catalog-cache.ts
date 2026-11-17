import { sql } from "drizzle-orm";
import {
  boolean,
  check,
  customType,
  integer,
  numeric,
  pgSchema,
  primaryKey,
  text,
} from "drizzle-orm/pg-core";
import { jsonb } from "./jsonb.ts";

export const catalogCache = pgSchema("core_catalog_cache");
const bytes = customType<{ data: Buffer }>({ dataType: () => "bytea" });

export const catalogEpoch = catalogCache.table(
  "epoch",
  {
    singleton: boolean().primaryKey().default(true),
    revision: numeric({ precision: 38, scale: 0 }).notNull().default("1"),
  },
  () => [
    check("catalog_epoch_singleton", sql`singleton IS TRUE`),
    check("catalog_epoch_revision", sql`revision>0`),
  ],
);

export const catalogCacheUsage = catalogCache.table(
  "usage",
  {
    singleton: boolean().primaryKey().default(true),
    byteCount: integer("byte_count").notNull().default(0),
    entryCount: integer("entry_count").notNull().default(0),
  },
  () => [
    check("catalog_cache_usage_singleton", sql`singleton IS TRUE`),
    check(
      "catalog_cache_usage_bounds",
      sql`byte_count BETWEEN 0 AND 67108864 AND entry_count BETWEEN 0 AND 4096`,
    ),
  ],
);

export const catalogCandidateCache = catalogCache.table(
  "candidates",
  {
    catalogRevision: numeric("catalog_revision", { precision: 38, scale: 0 }).notNull(),
    scopeDigest: bytes("scope_digest").notNull(),
    model: text().notNull(),
    operation: text().notNull(),
    currency: text().notNull(),
    candidates: jsonb().$type<unknown[]>().notNull(),
    payloadBytes: integer("payload_bytes").notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.catalogRevision, t.scopeDigest] }),
    check("catalog_cache_identity", sql`catalog_revision>0 AND octet_length(scope_digest)=32`),
    check(
      "catalog_cache_payload",
      sql`jsonb_typeof(candidates)='array' AND jsonb_array_length(candidates)<=128 AND payload_bytes BETWEEN 1 AND 67108864`,
    ),
  ],
);
