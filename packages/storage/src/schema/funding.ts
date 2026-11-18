import { sql } from "drizzle-orm";
import {
  check,
  foreignKey,
  integer,
  numeric,
  pgSchema,
  primaryKey,
  smallint,
  text,
  timestamp,
  unique,
  uuid,
  type AnyPgColumn,
} from "drizzle-orm/pg-core";
import { upstreamAccountBalances } from "./catalog.ts";
import { billingPrices, costVersions } from "./existing.ts";

export const funding = pgSchema("core_funding");
const atoms = (name: string) => numeric(name, { precision: 38, scale: 0 });

export const upstreamBalanceCells = funding.table(
  "balance_cells",
  {
    accountId: uuid("account_id").notNull(),
    currency: text().notNull(),
    cell: smallint().notNull(),
    balanceAtoms: atoms("balance_atoms").default("0").notNull(),
    reservedAtoms: atoms("reserved_atoms").default("0").notNull(),
    spentAtoms: atoms("spent_atoms").default("0").notNull(),
    revision: atoms("revision").default("0").notNull(),
    spentLimit: atoms("spent_limit").notNull(),
    revisionLimit: atoms("revision_limit").notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.accountId, t.currency, t.cell] }),
    foreignKey({
      columns: [t.accountId, t.currency],
      foreignColumns: [upstreamAccountBalances.accountId, upstreamAccountBalances.currency],
    }),
    check("funding_cell_range", sql`cell BETWEEN 0 AND 15`),
    check(
      "funding_cell_counters",
      sql`reserved_atoms>=0 AND spent_atoms>=0 AND revision>=0 AND spent_limit>=spent_atoms AND revision_limit>=revision`,
    ),
  ],
);

export const upstreamBudgetTransfers = funding.table(
  "budget_transfers",
  {
    id: uuid().primaryKey(),
    accountId: uuid("account_id").notNull(),
    currency: text().notNull(),
    cell: smallint().notNull(),
    kind: text().notNull(),
    reference: text().notNull(),
    balanceDelta: atoms("balance_delta").notNull(),
    spentAllocationAtoms: atoms("spent_allocation_atoms").default("0").notNull(),
    spentLimitDelta: atoms("spent_limit_delta").default("0").notNull(),
    revisionLimitDelta: atoms("revision_limit_delta").default("0").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true, mode: "string" })
      .defaultNow()
      .notNull(),
  },
  (t) => [
    foreignKey({
      columns: [t.accountId, t.currency, t.cell],
      foreignColumns: [
        upstreamBalanceCells.accountId,
        upstreamBalanceCells.currency,
        upstreamBalanceCells.cell,
      ],
    }),
    unique("funding_transfer_reference").on(t.accountId, t.currency, t.reference),
    check(
      "funding_transfer_kind",
      sql`kind IN ('GRANT','RECLAIM','HEADROOM','MIGRATION','REBASE')`,
    ),
    check(
      "funding_transfer_history",
      sql`spent_allocation_atoms>=0 AND (kind='MIGRATION' OR spent_allocation_atoms=0)`,
    ),
  ],
);

function programColumns(source: () => AnyPgColumn) {
  return {
    priceId: uuid("price_id").primaryKey().references(source),
    compilerVersion: integer("compiler_version").notNull(),
    sourceDigest: text("source_digest").notNull(),
    dimensions: text().array().notNull(),
    weights: numeric().array().notNull(),
    denominator: numeric().notNull(),
  };
}

function programChecks() {
  return [
    check("price_program_version", sql`compiler_version=1`),
    check("price_program_digest", sql`source_digest ~ '^[a-f0-9]{64}$'`),
    check(
      "price_program_shape",
      sql`cardinality(dimensions)=cardinality(weights) AND array_position(dimensions,NULL) IS NULL AND array_position(weights,NULL) IS NULL AND denominator>0 AND denominator=trunc(denominator) AND 0<=ALL(weights)`,
    ),
  ];
}

export const compiledCostPrices = funding.table(
  "cost_prices",
  programColumns(() => costVersions.id),
  programChecks,
);

export const compiledBillingPrices = funding.table(
  "billing_prices",
  programColumns(() => billingPrices.id),
  programChecks,
);
