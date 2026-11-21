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
          SELECT wallet_id, sum(remaining_atoms) AS reserved
          FROM public.wallet_leases WHERE state = 'ACTIVE' GROUP BY wallet_id
        ), supplier_claims AS (
          SELECT (entry->>'account_id')::uuid AS account_id,
            entry->>'currency' AS currency, (entry->>'cell')::int AS cell,
            sum((entry->>'held_atoms')::numeric) AS reserved
          FROM public.billing_holds h
          CROSS JOIN LATERAL jsonb_array_elements(h.supplier_holds) AS entry
          WHERE h.supplier_lease_id IS NULL AND entry->>'state' IN ('HELD', 'UNKNOWN')
          GROUP BY 1, 2, 3
        ), supplier_grants AS (
          SELECT account_id, currency, cell, sum(remaining_atoms) AS reserved
          FROM public.supplier_leases WHERE state = 'ACTIVE' GROUP BY account_id, currency, cell
        )
        SELECT
          (SELECT count(*) FROM (
            SELECT journal_id FROM public.ledger_entries
            GROUP BY journal_id HAVING count(*) < 2 OR sum(atoms) <> 0
          ) bad) AS unbalanced_journals,
          (SELECT count(*) FROM public.wallet_lots
            WHERE issued_atoms <> available_atoms + reserved_atoms + spent_atoms + expired_atoms) +
          (SELECT count(*) FROM public.wallet_leases l
            WHERE l.state = 'ACTIVE' AND l.remaining_atoms <> coalesce((
              SELECT sum((entry->>'amount_atoms')::numeric)
              FROM jsonb_array_elements(l.lot_allocations) entry
            ), 0)) AS broken_lots,
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
          ) AS reservation_mismatches,
          (SELECT count(*) FROM open_holds WHERE state = 'HELD') AS open_holds,
          (SELECT count(*) FROM open_holds WHERE state = 'UNKNOWN') AS unknown_holds
      `;
  const unbalancedJournals = Number(audit!.unbalanced_journals);
  const brokenLots = Number(audit!.broken_lots);
  const walletMismatches = Number(audit!.wallet_mismatches);
  const reservationMismatches = Number(audit!.reservation_mismatches);
  return {
    ok: unbalancedJournals + brokenLots + walletMismatches + reservationMismatches === 0,
    unbalanced_journals: unbalancedJournals,
    broken_lots: brokenLots,
    wallet_mismatches: walletMismatches,
    reservation_mismatches: reservationMismatches,
    open_holds: Number(audit!.open_holds),
    unknown_holds: Number(audit!.unknown_holds),
  };
}
