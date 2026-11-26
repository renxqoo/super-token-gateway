import type { SQL } from "bun";
import { cellKey, toCellRow, type SupplierCellRow } from "./funds.ts";

export type CellTarget = {
  account_id: string;
  currency: string;
  cell: number;
};

function dedupeSortedCells(targets: CellTarget[]): CellTarget[] {
  const seen = new Set<string>();
  const out: CellTarget[] = [];
  for (const t of targets) {
    const k = cellKey(t.account_id, t.currency, t.cell);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(t);
  }
  out.sort(
    (a, b) =>
      a.account_id.localeCompare(b.account_id) ||
      a.currency.localeCompare(b.currency) ||
      a.cell - b.cell,
  );
  return out;
}

export async function lockExactCells(tx: SQL, targets: CellTarget[]): Promise<SupplierCellRow[]> {
  if (targets.length === 0) return [];
  const sorted = dedupeSortedCells(targets);
  const rows = await tx`
    SELECT c.account_id, c.currency, c.cell, c.balance_atoms::text, c.reserved_atoms::text, c.spent_atoms::text, c.revision::text
    FROM public.supplier_cells c
    JOIN unnest(
      ${sorted.map((t) => t.account_id)}::uuid[],
      ${sorted.map((t) => t.currency)}::text[],
      ${sorted.map((t) => t.cell)}::smallint[]
    ) AS u(account_id, currency, cell)
      ON c.account_id = u.account_id AND c.currency = u.currency AND c.cell = u.cell
    ORDER BY c.account_id, c.currency, c.cell
    FOR NO KEY UPDATE
  `;
  return rows.map((r: Record<string, unknown>) => toCellRow(r));
}
