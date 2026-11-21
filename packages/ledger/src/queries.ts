import type { SQL } from "bun";
import { and, asc, desc, eq, gte, inArray, lt, or, sql, type SQL as DrizzleSQL } from "drizzle-orm";
import { billingCharges, billingHolds, createDrizzle, ledgerEntries } from "./schema.ts";

export function createLedgerQueries(db: SQL) {
  const orm = createDrizzle(db);

  return {
    async listCharges(input: {
      project_id?: string;
      outcome?: "SETTLED" | "RELEASED" | "DEBT" | "MODERATION_BLOCKED" | "UNKNOWN_RESOLVED";
      from?: string;
      to?: string;
      limit?: number;
      offset?: number;
    }) {
      const limit = Math.min(200, Math.max(1, input.limit ?? 50));
      const offset = Math.max(0, input.offset ?? 0);
      const conds: DrizzleSQL[] = [];
      if (input.project_id) conds.push(eq(billingCharges.projectId, input.project_id));
      if (input.outcome) conds.push(eq(billingCharges.outcome, input.outcome));
      if (input.from) conds.push(gte(billingCharges.createdAt, input.from));
      if (input.to) conds.push(lt(billingCharges.createdAt, input.to));
      const where = conds.length > 0 ? and(...conds) : undefined;

      const [items, countRows] = await Promise.all([
        orm
          .select()
          .from(billingCharges)
          .where(where)
          .orderBy(desc(billingCharges.createdAt), desc(billingCharges.id))
          .limit(limit)
          .offset(offset),
        orm
          .select({ total: sql<number>`count(*)::int` })
          .from(billingCharges)
          .where(where),
      ]);
      return { items, total: Number(countRows[0]?.total ?? 0), limit, offset };
    },

    async listLedgerEntries(input: {
      side?: "CUSTOMER" | "SUPPLIER";
      entity_id?: string;
      request_id?: string;
      kind?: string;
      from?: string;
      to?: string;
      limit?: number;
      offset?: number;
    }) {
      const limit = Math.min(200, Math.max(1, input.limit ?? 50));
      const offset = Math.max(0, input.offset ?? 0);
      const conds: DrizzleSQL[] = [];
      if (input.side) conds.push(eq(ledgerEntries.side, input.side));
      if (input.entity_id) conds.push(eq(ledgerEntries.entityId, input.entity_id));
      if (input.request_id) {
        const chRows = await orm
          .select({
            customerJournalId: billingCharges.customerJournalId,
            supplierJournalId: billingCharges.supplierJournalId,
          })
          .from(billingCharges)
          .where(eq(billingCharges.requestId, input.request_id));
        const jids: string[] = [];
        const ch = chRows[0];
        if (ch?.customerJournalId) jids.push(ch.customerJournalId);
        if (ch?.supplierJournalId) jids.push(ch.supplierJournalId);
        if (jids.length > 0) {
          conds.push(
            or(
              eq(ledgerEntries.requestId, input.request_id),
              inArray(ledgerEntries.journalId, jids),
            )!,
          );
        } else {
          conds.push(eq(ledgerEntries.requestId, input.request_id));
        }
      }
      if (input.kind) conds.push(eq(ledgerEntries.kind, input.kind));
      if (input.from) conds.push(gte(ledgerEntries.createdAt, input.from));
      if (input.to) conds.push(lt(ledgerEntries.createdAt, input.to));
      const where = conds.length > 0 ? and(...conds) : undefined;

      const [items, countRows] = await Promise.all([
        orm
          .select()
          .from(ledgerEntries)
          .where(where)
          .orderBy(
            desc(ledgerEntries.createdAt),
            asc(ledgerEntries.journalId),
            asc(ledgerEntries.ordinal),
          )
          .limit(limit)
          .offset(offset),
        orm
          .select({ total: sql<number>`count(*)::int` })
          .from(ledgerEntries)
          .where(where),
      ]);
      return { items, total: Number(countRows[0]?.total ?? 0), limit, offset };
    },

    async listHolds(input: {
      state?: "HELD" | "SETTLED" | "RELEASED" | "UNKNOWN" | "CONFLICT";
      project_id?: string;
      limit?: number;
      offset?: number;
    }) {
      const limit = Math.min(200, Math.max(1, input.limit ?? 50));
      const offset = Math.max(0, input.offset ?? 0);
      const conds: DrizzleSQL[] = [];
      if (input.state) conds.push(eq(billingHolds.state, input.state));
      if (input.project_id) conds.push(eq(billingHolds.projectId, input.project_id));
      const where = conds.length > 0 ? and(...conds) : undefined;

      const [items, countRows] = await Promise.all([
        orm
          .select()
          .from(billingHolds)
          .where(where)
          .orderBy(desc(billingHolds.createdAt))
          .limit(limit)
          .offset(offset),
        orm
          .select({ total: sql<number>`count(*)::int` })
          .from(billingHolds)
          .where(where),
      ]);
      return { items, total: Number(countRows[0]?.total ?? 0), limit, offset };
    },

    async getRequestBilling(requestId: string) {
      const [holds, charges] = await Promise.all([
        orm.select().from(billingHolds).where(eq(billingHolds.requestId, requestId)),
        orm.select().from(billingCharges).where(eq(billingCharges.requestId, requestId)),
      ]);
      const ch = charges[0] ?? null;
      const jids: string[] = [];
      if (ch?.customerJournalId) jids.push(ch.customerJournalId);
      if (ch?.supplierJournalId) jids.push(ch.supplierJournalId);
      const entryWhere =
        jids.length > 0
          ? or(eq(ledgerEntries.requestId, requestId), inArray(ledgerEntries.journalId, jids))
          : eq(ledgerEntries.requestId, requestId);
      const entries = await orm
        .select()
        .from(ledgerEntries)
        .where(entryWhere)
        .orderBy(
          asc(ledgerEntries.createdAt),
          asc(ledgerEntries.journalId),
          asc(ledgerEntries.ordinal),
        );
      return {
        hold: holds[0] ?? null,
        charge: ch,
        entries,
      };
    },
  };
}
