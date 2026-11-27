import type { SQL } from "bun";
import type { ConservationAudit } from "./types.ts";

export async function auditConservation(db: SQL): Promise<ConservationAudit> {
  const [audit] = await db`
        WITH open_holds AS MATERIALIZED (
          SELECT * FROM public.billing_holds WHERE state IN ('HELD', 'UNKNOWN')
        ), wallet_claims AS (
          SELECT wallet_id,
            sum(held_atoms - credit_atoms) FILTER (WHERE wallet_lease_id IS NULL) AS reserved,
            sum(credit_atoms) AS credit
          FROM open_holds GROUP BY wallet_id
        ), wallet_grants AS (
          SELECT wallet_id,
            sum(remaining_atoms + spent_atoms) AS reserved,
            sum(spent_atoms) AS spent
          FROM public.wallet_leases WHERE state = 'ACTIVE' GROUP BY wallet_id
        ), wallet_lot_spent AS (
          SELECT spent_lot_id AS lot_id, sum(spent_atoms) AS spent
          FROM public.wallet_leases
          WHERE state = 'ACTIVE' AND spent_atoms > 0
          GROUP BY spent_lot_id
        ), supplier_claims AS (
          SELECT (entry->>'account_id')::uuid AS account_id,
            entry->>'currency' AS currency, (entry->>'cell')::int AS cell,
            sum((entry->>'held_atoms')::numeric) AS reserved
          FROM public.billing_holds h
          CROSS JOIN LATERAL jsonb_array_elements(h.supplier_holds) AS entry
          WHERE h.supplier_lease_id IS NULL AND entry->>'state' IN ('HELD', 'UNKNOWN')
          GROUP BY 1, 2, 3
        ), supplier_grants AS (
          SELECT account_id, currency, cell,
            sum(remaining_atoms + spent_atoms) AS reserved,
            sum(spent_atoms) AS spent
          FROM public.supplier_leases WHERE state = 'ACTIVE' GROUP BY account_id, currency, cell
        ), supplier_lease_spent AS (
          SELECT account_id, currency, sum(spent) AS spent
          FROM supplier_grants
          GROUP BY account_id, currency
        ), key_claims AS (
          SELECT api_key_id, sum(key_reserved_atoms) AS reserved
          FROM open_holds
          WHERE api_key_id IS NOT NULL AND key_reserved_atoms > 0
          GROUP BY api_key_id
        ), customer_ledger AS (
          SELECT entity_id AS wallet_id,
            coalesce(sum(atoms) FILTER (WHERE account LIKE 'lot:%:available'), 0) AS lot_avail,
            coalesce(sum(atoms) FILTER (WHERE account LIKE 'lot:%:expired'), 0) AS lot_expired,
            coalesce(-sum(atoms) FILTER (WHERE account = 'system:receivable'), 0) AS debt
          FROM public.ledger_entries
          WHERE side = 'CUSTOMER'
          GROUP BY entity_id
        ), supplier_totals AS (
          SELECT r.account_id, r.currency,
            r.balance_atoms + coalesce(sum(c.balance_atoms), 0) - coalesce(sls.spent, 0) AS balance_atoms,
            r.spent_atoms + coalesce(sum(c.spent_atoms), 0) + coalesce(sls.spent, 0) AS spent_atoms
          FROM public.supplier_root_balances r
          LEFT JOIN public.supplier_cells c
            ON c.account_id = r.account_id AND c.currency = r.currency
          LEFT JOIN supplier_lease_spent sls
            ON sls.account_id = r.account_id AND sls.currency = r.currency
          GROUP BY r.account_id, r.currency, r.balance_atoms, r.spent_atoms, sls.spent
        ), supplier_ledger AS (
          SELECT entity_id AS account_id, currency,
            coalesce(sum(atoms) FILTER (WHERE account = 'upstream:available'), 0) AS balance_atoms,
            coalesce(sum(atoms) FILTER (WHERE account = 'upstream:expense'), 0) AS spent_atoms
          FROM public.ledger_entries
          WHERE side = 'SUPPLIER'
          GROUP BY entity_id, currency
        )
        SELECT
          (SELECT count(*) FROM (
            SELECT journal_id FROM public.ledger_entries
            GROUP BY journal_id HAVING count(*) < 2 OR sum(atoms) <> 0
          ) bad) AS unbalanced_journals,
          (SELECT count(*) FROM public.wallet_lots
            WHERE issued_atoms <> available_atoms + reserved_atoms + spent_atoms + expired_atoms) +
          (SELECT count(*) FROM public.wallet_leases l
            WHERE (l.state = 'ACTIVE' AND (
                l.remaining_atoms <> coalesce((
                  SELECT sum((entry->>'amount_atoms')::numeric)
                  FROM jsonb_array_elements(l.lot_allocations) entry
                ), 0)
                OR (l.spent_atoms > 0 AND (
                  l.spent_lot_id IS NULL
                  OR NOT EXISTS (
                    SELECT 1 FROM public.wallet_lots lot
                    WHERE lot.id = l.spent_lot_id AND lot.wallet_id = l.wallet_id
                  )
                ))
              ))
              OR (l.state <> 'ACTIVE' AND (l.remaining_atoms <> 0 OR l.spent_atoms <> 0))) +
          (SELECT count(*) FROM wallet_lot_spent ls
            LEFT JOIN public.wallet_lots lot ON lot.id = ls.lot_id
            WHERE lot.id IS NULL OR lot.reserved_atoms < ls.spent) AS broken_lots,
          (SELECT count(*) FROM public.wallets w
            LEFT JOIN (
              SELECT wallet_id, sum(available_atoms) AS avail, sum(reserved_atoms) AS res,
                sum(spent_atoms) AS spent, sum(expired_atoms) AS exp
              FROM public.wallet_lots GROUP BY wallet_id
            ) l ON l.wallet_id = w.id
            WHERE w.available_atoms <> coalesce(l.avail, 0)
               OR w.reserved_atoms <> coalesce(l.res, 0)
               OR w.spent_atoms <> coalesce(l.spent, 0)
               OR w.expired_atoms <> coalesce(l.exp, 0)) AS wallet_mismatches,
          (SELECT count(*) FROM public.wallets w
            LEFT JOIN wallet_claims c ON c.wallet_id = w.id
            LEFT JOIN wallet_grants g ON g.wallet_id = w.id
            WHERE w.reserved_atoms <> coalesce(c.reserved, 0) + coalesce(g.reserved, 0)
               OR w.credit_reserved_atoms <> coalesce(c.credit, 0)) +
          (SELECT count(*) FROM public.supplier_cells s
            LEFT JOIN supplier_claims c USING (account_id, currency, cell)
            LEFT JOIN supplier_grants g USING (account_id, currency, cell)
            WHERE s.reserved_atoms <> coalesce(c.reserved, 0) + coalesce(g.reserved, 0)) +
          (SELECT count(*) FROM open_holds h
            LEFT JOIN public.wallet_leases w ON w.id = h.wallet_lease_id
            LEFT JOIN public.supplier_leases s ON s.id = h.supplier_lease_id
            WHERE (h.wallet_lease_id IS NOT NULL AND w.state IS DISTINCT FROM 'ACTIVE')
               OR (h.supplier_lease_id IS NOT NULL AND s.state IS DISTINCT FROM 'ACTIVE')
          ) +
          (SELECT count(*) FROM public.api_keys k
            LEFT JOIN key_claims kc ON kc.api_key_id = k.id
            WHERE k.reserved_atoms <> coalesce(kc.reserved, 0)
          ) AS reservation_mismatches,
          (SELECT count(*) FROM public.wallets w
            LEFT JOIN wallet_grants g ON g.wallet_id = w.id
            FULL OUTER JOIN customer_ledger cl ON cl.wallet_id = w.id
            WHERE coalesce(w.available_atoms + w.reserved_atoms - coalesce(g.spent, 0), 0) <> coalesce(cl.lot_avail, 0)
               OR coalesce(w.expired_atoms, 0) <> coalesce(cl.lot_expired, 0)
               OR coalesce(w.debt_atoms, 0) <> coalesce(cl.debt, 0)
          ) AS customer_ledger_mismatches,
          (SELECT count(*) FROM supplier_totals st
            FULL OUTER JOIN supplier_ledger sl
              ON sl.account_id = st.account_id AND sl.currency = st.currency
            WHERE coalesce(st.balance_atoms, 0) <> coalesce(sl.balance_atoms, 0)
               OR coalesce(st.spent_atoms, 0) <> coalesce(sl.spent_atoms, 0)
          ) AS supplier_ledger_mismatches,
          (SELECT count(*) FROM open_holds WHERE state = 'HELD') AS open_holds,
          (SELECT count(*) FROM open_holds WHERE state = 'UNKNOWN') AS unknown_holds
      `;
  const unbalancedJournals = Number(audit!.unbalanced_journals);
  const brokenLots = Number(audit!.broken_lots);
  const walletMismatches = Number(audit!.wallet_mismatches);
  const reservationMismatches = Number(audit!.reservation_mismatches);
  const customerLedgerMismatches = Number(audit!.customer_ledger_mismatches);
  const supplierLedgerMismatches = Number(audit!.supplier_ledger_mismatches);
  return {
    ok:
      unbalancedJournals +
        brokenLots +
        walletMismatches +
        reservationMismatches +
        customerLedgerMismatches +
        supplierLedgerMismatches ===
      0,
    unbalanced_journals: unbalancedJournals,
    broken_lots: brokenLots,
    wallet_mismatches: walletMismatches,
    reservation_mismatches: reservationMismatches,
    customer_ledger_mismatches: customerLedgerMismatches,
    supplier_ledger_mismatches: supplierLedgerMismatches,
    open_holds: Number(audit!.open_holds),
    unknown_holds: Number(audit!.unknown_holds),
  };
}
