import type { SQL } from "bun";
import { and, asc, desc, eq, gte, inArray, lt, or, sql, type SQL as DrizzleSQL } from "drizzle-orm";
import { billingCharges, billingHolds, createDrizzle, ledgerEntries } from "./schema.ts";
import type {
  FinanceSummaryCurrencyRow,
  FinanceSummaryDimensionRow,
  FinanceSummaryReport,
  FundingOperationRecord,
} from "./types.ts";

function requestIdBoundary(time: Date): string {
  const timestamp = time.getTime();
  if (!Number.isFinite(timestamp) || timestamp < 0 || timestamp > 0xffffffffffff) {
    throw new Error("The report timestamp is out of UUIDv7 range");
  }
  const hex = timestamp.toString(16).padStart(12, "0");
  return `${hex.slice(0, 8)}-${hex.slice(8)}-7000-8000-000000000000`;
}

export function createLedgerQueries(db: SQL) {
  const orm = createDrizzle(db);

  return {
    async getRoutingCostReport({ model, from, to }: { model: string; from: Date; to: Date }) {
      const durationMs = to.getTime() - from.getTime();
      if (!Number.isFinite(durationMs) || durationMs <= 0 || durationMs > 3_600_000) {
        throw new Error(
          "The routing cost report requires an increasing window of at most one hour",
        );
      }
      const startId = requestIdBoundary(from);
      const endId = requestIdBoundary(to);

      return db.begin(async (tx) => {
        await tx`SET TRANSACTION READ ONLY`;
        await tx`SET LOCAL statement_timeout = '5s'`;
        const [report] = await tx<
          {
            cohort_requests: number;
            currencies: {
              currency: string | null;
              supplier_cost_atoms: string;
              unknown_reserved_atoms: string;
              unknown_attempts: number;
            }[];
          }[]
        >`
          WITH cohort AS MATERIALIZED (
            SELECT request_id, supplier_holds
            FROM public.billing_holds
            WHERE model = ${model} AND operation = 'chat'
              AND request_id >= ${startId}::uuid AND request_id < ${endId}::uuid
              AND substring(request_id::text, 15, 1) = '7'
          ), charges AS MATERIALIZED (
            SELECT charge.request_id, charge.supplier_currency, charge.supplier_cost_atoms,
                   charge.supplier_journal_id
            FROM cohort JOIN public.billing_charges charge USING (request_id)
          ), expenses AS (
            SELECT supplier_currency AS currency, supplier_cost_atoms AS atoms
            FROM charges WHERE supplier_currency IS NOT NULL OR supplier_cost_atoms <> 0
            UNION ALL
            SELECT entry.currency, entry.atoms
            FROM cohort JOIN public.ledger_entries entry USING (request_id)
            WHERE entry.side = 'SUPPLIER' AND entry.kind = 'SUPPLIER_COST'
              AND entry.account = 'upstream:expense'
              AND starts_with(entry.reference, 'supplier_attempt_resolution:')
              AND NOT EXISTS (
                SELECT 1 FROM charges
                WHERE charges.request_id = entry.request_id AND charges.supplier_journal_id = entry.journal_id
              )
          ), cost AS (
            SELECT currency, sum(atoms) AS atoms FROM expenses GROUP BY currency
          ), unresolved AS (
            SELECT attempt->>'currency' AS currency, sum((attempt->>'held_atoms')::numeric) AS atoms,
                   count(*)::int AS attempts
            FROM cohort CROSS JOIN LATERAL jsonb_array_elements(supplier_holds) attempt
            WHERE attempt->>'state' = 'UNKNOWN'
            GROUP BY attempt->>'currency'
          )
          SELECT (SELECT count(*)::int FROM cohort) AS cohort_requests, (
            SELECT coalesce(jsonb_agg(jsonb_build_object(
              'currency', currency,
              'supplier_cost_atoms', coalesce(cost.atoms, 0)::text,
              'unknown_reserved_atoms', coalesce(unresolved.atoms, 0)::text,
              'unknown_attempts', coalesce(unresolved.attempts, 0)
            ) ORDER BY currency), '[]'::jsonb)
            FROM cost FULL JOIN unresolved USING (currency)
          ) AS currencies
        `;
        return report!;
      });
    },

    async listCharges(input: {
      billing_account_id?: string;
      api_key_id?: string;
      outcome?: "SETTLED" | "RELEASED" | "DEBT" | "MODERATION_BLOCKED" | "UNKNOWN_RESOLVED";
      from?: string;
      to?: string;
      limit?: number;
      offset?: number;
    }) {
      const limit = Math.min(200, Math.max(1, input.limit ?? 50));
      const offset = Math.max(0, input.offset ?? 0);
      const conds: DrizzleSQL[] = [];
      if (input.billing_account_id)
        conds.push(eq(billingCharges.billingAccountId, input.billing_account_id));
      if (input.api_key_id) conds.push(eq(billingCharges.apiKeyId, input.api_key_id));
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

    async listFundingOperations(input: {
      side?: "CUSTOMER" | "SUPPLIER";
      entity_id?: string;
      billing_account_id?: string;
      account_id?: string;
      kind?: "CREDIT" | "REFUND" | "ADJUST" | "SUPPLIER_RECHARGE" | "SUPPLIER_ADJUST";
      currency?: string;
      from?: string;
      to?: string;
      limit?: number;
      offset?: number;
    }): Promise<{
      items: FundingOperationRecord[];
      total: number;
      limit: number;
      offset: number;
    }> {
      const limit = Math.min(200, Math.max(1, input.limit ?? 50));
      const offset = Math.max(0, input.offset ?? 0);
      const side = input.side ?? null;
      const entityId = input.entity_id ?? null;
      const billingAccountId = input.billing_account_id ?? null;
      const accountId = input.account_id ?? null;
      const kind = input.kind ?? null;
      const currency = input.currency ?? null;
      const from = input.from ?? null;
      const to = input.to ?? null;

      const [rows, countRows] = await Promise.all([
        db<
          {
            id: string;
            side: "CUSTOMER" | "SUPPLIER";
            entity_id: string;
            billing_account_id: string | null;
            wallet_id: string | null;
            charge_id: string | null;
            account_id: string | null;
            kind: "CREDIT" | "REFUND" | "ADJUST" | "SUPPLIER_RECHARGE" | "SUPPLIER_ADJUST";
            external_ref: string;
            amount_atoms: string;
            currency: string;
            reason: string;
            payload: Record<string, unknown>;
            receipt: Record<string, unknown>;
            created_at: Date | string;
          }[]
        >`
          SELECT
            f.id::text AS id,
            f.side,
            f.entity_id::text AS entity_id,
            coalesce(w.billing_account_id, bc.billing_account_id)::text AS billing_account_id,
            coalesce(w.id, bc.wallet_id)::text AS wallet_id,
            bc.id::text AS charge_id,
            CASE WHEN f.side = 'SUPPLIER' THEN f.entity_id::text ELSE NULL END AS account_id,
            f.kind,
            f.external_ref,
            f.amount_atoms::text AS amount_atoms,
            f.currency,
            f.reason,
            f.payload,
            f.receipt,
            f.created_at
          FROM public.funding_operations f
          LEFT JOIN public.wallets w ON f.side = 'CUSTOMER' AND w.id = f.entity_id
          LEFT JOIN public.billing_charges bc ON f.side = 'CUSTOMER' AND bc.id = f.entity_id
          WHERE (${side}::text IS NULL OR f.side = ${side}::text)
            AND (${entityId}::uuid IS NULL OR f.entity_id = ${entityId}::uuid)
            AND (${billingAccountId}::uuid IS NULL OR coalesce(w.billing_account_id, bc.billing_account_id) = ${billingAccountId}::uuid)
            AND (${accountId}::uuid IS NULL OR (f.side = 'SUPPLIER' AND f.entity_id = ${accountId}::uuid))
            AND (${kind}::text IS NULL OR f.kind = ${kind}::text)
            AND (${currency}::text IS NULL OR f.currency = ${currency}::text)
            AND (${from}::timestamptz IS NULL OR f.created_at >= ${from}::timestamptz)
            AND (${to}::timestamptz IS NULL OR f.created_at < ${to}::timestamptz)
          ORDER BY f.created_at DESC, f.id DESC
          LIMIT ${limit}
          OFFSET ${offset}
        `,
        db<{ total: number }[]>`
          SELECT count(*)::int AS total
          FROM public.funding_operations f
          LEFT JOIN public.wallets w ON f.side = 'CUSTOMER' AND w.id = f.entity_id
          LEFT JOIN public.billing_charges bc ON f.side = 'CUSTOMER' AND bc.id = f.entity_id
          WHERE (${side}::text IS NULL OR f.side = ${side}::text)
            AND (${entityId}::uuid IS NULL OR f.entity_id = ${entityId}::uuid)
            AND (${billingAccountId}::uuid IS NULL OR coalesce(w.billing_account_id, bc.billing_account_id) = ${billingAccountId}::uuid)
            AND (${accountId}::uuid IS NULL OR (f.side = 'SUPPLIER' AND f.entity_id = ${accountId}::uuid))
            AND (${kind}::text IS NULL OR f.kind = ${kind}::text)
            AND (${currency}::text IS NULL OR f.currency = ${currency}::text)
            AND (${from}::timestamptz IS NULL OR f.created_at >= ${from}::timestamptz)
            AND (${to}::timestamptz IS NULL OR f.created_at < ${to}::timestamptz)
        `,
      ]);

      return {
        items: rows.map((r) => ({
          id: r.id,
          side: r.side,
          entity_id: r.entity_id,
          billing_account_id: r.billing_account_id,
          wallet_id: r.wallet_id,
          charge_id: r.charge_id,
          account_id: r.account_id,
          kind: r.kind,
          external_ref: r.external_ref,
          amount_atoms: r.amount_atoms,
          currency: r.currency,
          reason: r.reason,
          payload: r.payload ?? {},
          receipt: r.receipt ?? {},
          created_at:
            r.created_at instanceof Date ? r.created_at.toISOString() : String(r.created_at),
        })),
        total: Number(countRows[0]?.total ?? 0),
        limit,
        offset,
      };
    },

    async getFinanceSummary(input: {
      from?: string;
      to?: string;
      billing_account_id?: string;
      currency?: string;
    }): Promise<FinanceSummaryReport> {
      const from = input.from ?? null;
      const to = input.to ?? null;
      const billingAccountId = input.billing_account_id ?? null;
      const currency = input.currency ?? null;

      const [chargeByCurr, fundingByCurr, modelRows, billingAccountRows, channelRows] =
        await Promise.all([
          db<
            {
              currency: string;
              charge_count: number;
              moderation_absorbed_count: number;
              customer_billed_atoms: string;
              customer_funded_atoms: string;
              customer_debt_atoms: string;
              customer_refunded_atoms: string;
              supplier_cost_atoms: string;
            }[]
          >`
          SELECT
            currency,
            count(*)::int AS charge_count,
            count(*) FILTER (WHERE outcome = 'MODERATION_BLOCKED' AND customer_atoms = 0)::int AS moderation_absorbed_count,
            coalesce(sum(customer_atoms), 0)::text AS customer_billed_atoms,
            coalesce(sum(funded_atoms), 0)::text AS customer_funded_atoms,
            coalesce(sum(debt_atoms), 0)::text AS customer_debt_atoms,
            coalesce(sum(refunded_atoms), 0)::text AS customer_refunded_atoms,
            coalesce(sum(CASE
              WHEN supplier_currency IS NULL OR supplier_currency = currency THEN supplier_cost_atoms
              WHEN pricing_snapshot->'customer'->>'base_currency' = supplier_currency
                AND (pricing_snapshot->'customer'->'fx_rate'->>'numerator') ~ '^[1-9][0-9]*$'
                AND (pricing_snapshot->'customer'->'fx_rate'->>'denominator') ~ '^[1-9][0-9]*$'
              THEN div(
                supplier_cost_atoms * (pricing_snapshot->'customer'->'fx_rate'->>'numerator')::numeric
                  + (pricing_snapshot->'customer'->'fx_rate'->>'denominator')::numeric - 1,
                (pricing_snapshot->'customer'->'fx_rate'->>'denominator')::numeric
              )
              ELSE 0
            END), 0)::text AS supplier_cost_atoms
          FROM public.billing_charges
          WHERE (${billingAccountId}::uuid IS NULL OR billing_account_id = ${billingAccountId}::uuid)
            AND (${currency}::text IS NULL OR currency = ${currency}::text)
            AND (${from}::timestamptz IS NULL OR created_at >= ${from}::timestamptz)
            AND (${to}::timestamptz IS NULL OR created_at < ${to}::timestamptz)
          GROUP BY currency
          ORDER BY currency ASC
        `,
          db<
            {
              currency: string;
              cash_topup_atoms: string;
              grant_issued_atoms: string;
              customer_adjustment_net_atoms: string;
              supplier_recharge_atoms: string;
              supplier_adjustment_net_atoms: string;
            }[]
          >`
          SELECT
            f.currency,
            coalesce(sum(f.amount_atoms) FILTER (
              WHERE f.side = 'CUSTOMER' AND f.kind = 'CREDIT'
                AND coalesce(f.payload->>'kind', f.receipt->>'kind', wl.kind, 'CASH') = 'CASH'
            ), 0)::text AS cash_topup_atoms,
            coalesce(sum(f.amount_atoms) FILTER (
              WHERE f.side = 'CUSTOMER' AND f.kind = 'CREDIT'
                AND coalesce(f.payload->>'kind', f.receipt->>'kind', wl.kind, 'CASH') = 'GRANT'
            ), 0)::text AS grant_issued_atoms,
            coalesce(sum(f.amount_atoms) FILTER (
              WHERE f.side = 'CUSTOMER' AND f.kind = 'ADJUST'
            ), 0)::text AS customer_adjustment_net_atoms,
            coalesce(sum(f.amount_atoms) FILTER (
              WHERE f.side = 'SUPPLIER' AND f.kind = 'SUPPLIER_RECHARGE'
            ), 0)::text AS supplier_recharge_atoms,
            coalesce(sum(f.amount_atoms) FILTER (
              WHERE f.side = 'SUPPLIER' AND f.kind = 'SUPPLIER_ADJUST'
            ), 0)::text AS supplier_adjustment_net_atoms
          FROM public.funding_operations f
          LEFT JOIN public.wallets w ON f.side = 'CUSTOMER' AND w.id = f.entity_id
          LEFT JOIN public.billing_charges bc ON f.side = 'CUSTOMER' AND bc.id = f.entity_id
          LEFT JOIN public.wallet_lots wl
            ON f.side = 'CUSTOMER' AND f.kind = 'CREDIT'
            AND wl.id = CASE
              WHEN (f.receipt->>'lot_id') ~ '^[0-9a-fA-F-]{36}$'
              THEN (f.receipt->>'lot_id')::uuid
              ELSE NULL
            END
          WHERE (${billingAccountId}::uuid IS NULL OR (f.side = 'CUSTOMER' AND coalesce(w.billing_account_id, bc.billing_account_id) = ${billingAccountId}::uuid))
            AND (${currency}::text IS NULL OR f.currency = ${currency}::text)
            AND (${from}::timestamptz IS NULL OR f.created_at >= ${from}::timestamptz)
            AND (${to}::timestamptz IS NULL OR f.created_at < ${to}::timestamptz)
          GROUP BY f.currency
          ORDER BY f.currency ASC
        `,
          db<
            {
              key: string;
              currency: string;
              charge_count: number;
              customer_billed_atoms: string;
              customer_refunded_atoms: string;
              supplier_cost_atoms: string;
            }[]
          >`
          SELECT
            model AS key,
            currency,
            count(*)::int AS charge_count,
            coalesce(sum(customer_atoms), 0)::text AS customer_billed_atoms,
            coalesce(sum(refunded_atoms), 0)::text AS customer_refunded_atoms,
            coalesce(sum(CASE
              WHEN supplier_currency IS NULL OR supplier_currency = currency THEN supplier_cost_atoms
              WHEN pricing_snapshot->'customer'->>'base_currency' = supplier_currency
                AND (pricing_snapshot->'customer'->'fx_rate'->>'numerator') ~ '^[1-9][0-9]*$'
                AND (pricing_snapshot->'customer'->'fx_rate'->>'denominator') ~ '^[1-9][0-9]*$'
              THEN div(
                supplier_cost_atoms * (pricing_snapshot->'customer'->'fx_rate'->>'numerator')::numeric
                  + (pricing_snapshot->'customer'->'fx_rate'->>'denominator')::numeric - 1,
                (pricing_snapshot->'customer'->'fx_rate'->>'denominator')::numeric
              )
              ELSE 0
            END), 0)::text AS supplier_cost_atoms
          FROM public.billing_charges
          WHERE (${billingAccountId}::uuid IS NULL OR billing_account_id = ${billingAccountId}::uuid)
            AND (${currency}::text IS NULL OR currency = ${currency}::text)
            AND (${from}::timestamptz IS NULL OR created_at >= ${from}::timestamptz)
            AND (${to}::timestamptz IS NULL OR created_at < ${to}::timestamptz)
          GROUP BY model, currency
          ORDER BY sum(customer_atoms) DESC, model ASC
          LIMIT 50
        `,
          db<
            {
              key: string;
              currency: string;
              charge_count: number;
              customer_billed_atoms: string;
              customer_refunded_atoms: string;
              supplier_cost_atoms: string;
            }[]
          >`
          SELECT
            billing_account_id::text AS key,
            currency,
            count(*)::int AS charge_count,
            coalesce(sum(customer_atoms), 0)::text AS customer_billed_atoms,
            coalesce(sum(refunded_atoms), 0)::text AS customer_refunded_atoms,
            coalesce(sum(CASE
              WHEN supplier_currency IS NULL OR supplier_currency = currency THEN supplier_cost_atoms
              WHEN pricing_snapshot->'customer'->>'base_currency' = supplier_currency
                AND (pricing_snapshot->'customer'->'fx_rate'->>'numerator') ~ '^[1-9][0-9]*$'
                AND (pricing_snapshot->'customer'->'fx_rate'->>'denominator') ~ '^[1-9][0-9]*$'
              THEN div(
                supplier_cost_atoms * (pricing_snapshot->'customer'->'fx_rate'->>'numerator')::numeric
                  + (pricing_snapshot->'customer'->'fx_rate'->>'denominator')::numeric - 1,
                (pricing_snapshot->'customer'->'fx_rate'->>'denominator')::numeric
              )
              ELSE 0
            END), 0)::text AS supplier_cost_atoms
          FROM public.billing_charges
          WHERE (${billingAccountId}::uuid IS NULL OR billing_account_id = ${billingAccountId}::uuid)
            AND (${currency}::text IS NULL OR currency = ${currency}::text)
            AND (${from}::timestamptz IS NULL OR created_at >= ${from}::timestamptz)
            AND (${to}::timestamptz IS NULL OR created_at < ${to}::timestamptz)
          GROUP BY billing_account_id, currency
          ORDER BY sum(customer_atoms) DESC, billing_account_id ASC
          LIMIT 50
        `,
          db<
            {
              key: string;
              currency: string;
              charge_count: number;
              customer_billed_atoms: string;
              customer_refunded_atoms: string;
              supplier_cost_atoms: string;
            }[]
          >`
          SELECT
            coalesce(channel_id::text, 'unknown') AS key,
            currency,
            count(*)::int AS charge_count,
            coalesce(sum(customer_atoms), 0)::text AS customer_billed_atoms,
            coalesce(sum(refunded_atoms), 0)::text AS customer_refunded_atoms,
            coalesce(sum(CASE
              WHEN supplier_currency IS NULL OR supplier_currency = currency THEN supplier_cost_atoms
              WHEN pricing_snapshot->'customer'->>'base_currency' = supplier_currency
                AND (pricing_snapshot->'customer'->'fx_rate'->>'numerator') ~ '^[1-9][0-9]*$'
                AND (pricing_snapshot->'customer'->'fx_rate'->>'denominator') ~ '^[1-9][0-9]*$'
              THEN div(
                supplier_cost_atoms * (pricing_snapshot->'customer'->'fx_rate'->>'numerator')::numeric
                  + (pricing_snapshot->'customer'->'fx_rate'->>'denominator')::numeric - 1,
                (pricing_snapshot->'customer'->'fx_rate'->>'denominator')::numeric
              )
              ELSE 0
            END), 0)::text AS supplier_cost_atoms
          FROM public.billing_charges
          WHERE (${billingAccountId}::uuid IS NULL OR billing_account_id = ${billingAccountId}::uuid)
            AND (${currency}::text IS NULL OR currency = ${currency}::text)
            AND (${from}::timestamptz IS NULL OR created_at >= ${from}::timestamptz)
            AND (${to}::timestamptz IS NULL OR created_at < ${to}::timestamptz)
          GROUP BY channel_id, currency
          ORDER BY coalesce(sum(CASE
              WHEN supplier_currency IS NULL OR supplier_currency = currency THEN supplier_cost_atoms
              WHEN pricing_snapshot->'customer'->>'base_currency' = supplier_currency
                AND (pricing_snapshot->'customer'->'fx_rate'->>'numerator') ~ '^[1-9][0-9]*$'
                AND (pricing_snapshot->'customer'->'fx_rate'->>'denominator') ~ '^[1-9][0-9]*$'
              THEN div(
                supplier_cost_atoms * (pricing_snapshot->'customer'->'fx_rate'->>'numerator')::numeric
                  + (pricing_snapshot->'customer'->'fx_rate'->>'denominator')::numeric - 1,
                (pricing_snapshot->'customer'->'fx_rate'->>'denominator')::numeric
              )
              ELSE 0
            END), 0) DESC, key ASC
          LIMIT 50
        `,
        ]);

      const currencies = new Set<string>([
        ...chargeByCurr.map((r) => r.currency),
        ...fundingByCurr.map((r) => r.currency),
      ]);
      const chargeMap = new Map(chargeByCurr.map((r) => [r.currency, r]));
      const fundingMap = new Map(fundingByCurr.map((r) => [r.currency, r]));

      const by_currency: FinanceSummaryCurrencyRow[] = [...currencies].sort().map((curr) => {
        const ch = chargeMap.get(curr);
        const fn = fundingMap.get(curr);
        const billed = BigInt(ch?.customer_billed_atoms ?? "0");
        const refunded = BigInt(ch?.customer_refunded_atoms ?? "0");
        const supCost = BigInt(ch?.supplier_cost_atoms ?? "0");
        const netRev = billed - refunded;
        return {
          currency: curr,
          charge_count: Number(ch?.charge_count ?? 0),
          moderation_absorbed_count: Number(ch?.moderation_absorbed_count ?? 0),
          customer_billed_atoms: billed.toString(),
          customer_funded_atoms: ch?.customer_funded_atoms ?? "0",
          customer_debt_atoms: ch?.customer_debt_atoms ?? "0",
          customer_refunded_atoms: refunded.toString(),
          net_customer_revenue_atoms: netRev.toString(),
          supplier_cost_atoms: supCost.toString(),
          gross_margin_atoms: (netRev - supCost).toString(),
          cash_topup_atoms: fn?.cash_topup_atoms ?? "0",
          grant_issued_atoms: fn?.grant_issued_atoms ?? "0",
          customer_adjustment_net_atoms: fn?.customer_adjustment_net_atoms ?? "0",
          supplier_recharge_atoms: fn?.supplier_recharge_atoms ?? "0",
          supplier_adjustment_net_atoms: fn?.supplier_adjustment_net_atoms ?? "0",
        };
      });

      const toDimRow = (r: {
        key: string;
        currency: string;
        charge_count: number;
        customer_billed_atoms: string;
        customer_refunded_atoms: string;
        supplier_cost_atoms: string;
      }): FinanceSummaryDimensionRow => {
        const billed = BigInt(r.customer_billed_atoms);
        const refunded = BigInt(r.customer_refunded_atoms);
        const sup = BigInt(r.supplier_cost_atoms);
        const netRev = billed - refunded;
        return {
          key: r.key,
          currency: r.currency,
          charge_count: Number(r.charge_count),
          customer_billed_atoms: billed.toString(),
          customer_refunded_atoms: refunded.toString(),
          net_customer_revenue_atoms: netRev.toString(),
          supplier_cost_atoms: sup.toString(),
          gross_margin_atoms: (netRev - sup).toString(),
        };
      };

      return {
        from,
        to,
        billing_account_id: billingAccountId,
        currency,
        by_currency,
        by_model: modelRows.map(toDimRow),
        by_billing_account: billingAccountRows.map(toDimRow),
        by_channel: channelRows.map(toDimRow),
      };
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
      billing_account_id?: string;
      limit?: number;
      offset?: number;
    }) {
      const limit = Math.min(200, Math.max(1, input.limit ?? 50));
      const offset = Math.max(0, input.offset ?? 0);
      const conds: DrizzleSQL[] = [];
      if (input.state) conds.push(eq(billingHolds.state, input.state));
      if (input.billing_account_id)
        conds.push(eq(billingHolds.billingAccountId, input.billing_account_id));
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
