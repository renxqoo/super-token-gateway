import type { SQL } from "bun";
import { and, eq, sql } from "drizzle-orm";
import { supplierCells, supplierRootBalances, type LedgerDrizzle } from "./schema.ts";
import type { RechargeSupplierInput } from "./types.ts";
import {
  CELL_COUNT,
  parseAtoms,
  sha256Hex,
  toCellRow,
  insertJournalLines,
  flushDirtyCells,
  type SupplierCellRow,
} from "./funds.ts";

export function createSupplierFunds(db: SQL, orm: LedgerDrizzle) {
  return {
    async rechargeSupplier(input: RechargeSupplierInput): Promise<{
      operation_id: string;
      replayed: boolean;
      total_balance_atoms: string;
    }> {
      const atoms = parseAtoms(input.amount_atoms);
      if (atoms === null || atoms <= 0n) throw new Error("Recharge amount must be positive");
      const fingerprint = sha256Hex({
        account_id: input.account_id,
        currency: input.currency,
        amount_atoms: input.amount_atoms,
        reason: input.reason ?? "",
      });
      return db.begin(async (tx) => {
        const existingOp = await tx`
          SELECT id, fingerprint, receipt FROM public.funding_operations
          WHERE side = 'SUPPLIER' AND entity_id = ${input.account_id} AND kind = 'SUPPLIER_RECHARGE' AND external_ref = ${input.external_ref}
        `;
        if (existingOp.length > 0) {
          if (String(existingOp[0]!.fingerprint) !== fingerprint) {
            throw new Error("Supplier recharge external_ref conflict");
          }
          const rec = existingOp[0]!.receipt as { total_balance_atoms: string };
          return {
            operation_id: String(existingOp[0]!.id),
            replayed: true,
            total_balance_atoms: rec.total_balance_atoms,
          };
        }

        await tx`
          INSERT INTO public.supplier_root_balances (account_id, currency, balance_atoms)
          VALUES (${input.account_id}, ${input.currency}, 0)
          ON CONFLICT (account_id, currency) DO NOTHING
        `;
        await tx`
          INSERT INTO public.supplier_cells (account_id, currency, cell, balance_atoms)
          SELECT ${input.account_id}, ${input.currency}, g::smallint, 0
          FROM generate_series(0, ${CELL_COUNT - 1}) AS g
          ON CONFLICT (account_id, currency, cell) DO NOTHING
        `;
        const rootRows = await tx`
          SELECT balance_atoms::text, revision::text
          FROM public.supplier_root_balances
          WHERE account_id = ${input.account_id} AND currency = ${input.currency}
          FOR NO KEY UPDATE
        `;
        const cellsRaw = await tx`
          SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
          FROM public.supplier_cells
          WHERE account_id = ${input.account_id} AND currency = ${input.currency}
          ORDER BY cell
          FOR NO KEY UPDATE
        `;
        const cells: SupplierCellRow[] = cellsRaw.map((r: Record<string, unknown>) => toCellRow(r));
        const rootBal = BigInt(String(rootRows[0]!.balance_atoms)) + atoms;
        const perCell = rootBal / BigInt(CELL_COUNT);
        const rem = rootBal % BigInt(CELL_COUNT);
        for (let i = 0; i < cells.length; i++) {
          const share = perCell + (BigInt(i) < rem ? 1n : 0n);
          cells[i]!.balance_atoms += share;
          cells[i]!.revision += 1n;
          cells[i]!.dirty = true;
        }
        await flushDirtyCells(tx, cells);
        await tx`
          UPDATE public.supplier_root_balances
          SET balance_atoms = 0, revision = revision + 1
          WHERE account_id = ${input.account_id} AND currency = ${input.currency}
        `;
        const total = cells.reduce((acc, c) => acc + c.balance_atoms, 0n);
        const opId = crypto.randomUUID();
        await insertJournalLines(tx, [
          {
            journal_id: opId,
            ordinal: 0,
            side: "SUPPLIER",
            entity_id: input.account_id,
            request_id: null,
            reference: `supplier_recharge:${input.external_ref}`,
            kind: "SUPPLIER_RECHARGE",
            currency: input.currency,
            account: "upstream:available",
            atoms,
          },
          {
            journal_id: opId,
            ordinal: 1,
            side: "SUPPLIER",
            entity_id: input.account_id,
            request_id: null,
            reference: `supplier_recharge:${input.external_ref}`,
            kind: "SUPPLIER_RECHARGE",
            currency: input.currency,
            account: "upstream:funding",
            atoms: -atoms,
          },
        ]);
        const receipt = { total_balance_atoms: total.toString() };
        await tx`
          INSERT INTO public.funding_operations (id, side, entity_id, kind, external_ref, fingerprint, amount_atoms, currency, reason, receipt)
          VALUES (${opId}, 'SUPPLIER', ${input.account_id}, 'SUPPLIER_RECHARGE', ${input.external_ref}, ${fingerprint}, ${atoms.toString()}::numeric, ${input.currency}, ${input.reason ?? ""}, ${receipt}::jsonb)
        `;
        return { operation_id: opId, replayed: false, total_balance_atoms: total.toString() };
      });
    },

    async rebalanceSupplierCells(accountId: string, currency: string): Promise<boolean> {
      return db.begin(async (tx) => {
        const rootRows = await tx`
          SELECT balance_atoms::text FROM public.supplier_root_balances
          WHERE account_id = ${accountId} AND currency = ${currency} FOR NO KEY UPDATE
        `;
        if (!rootRows.length) return false;
        const cellsRaw = await tx`
          SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
          FROM public.supplier_cells
          WHERE account_id = ${accountId} AND currency = ${currency}
          ORDER BY cell FOR NO KEY UPDATE
        `;
        if (!cellsRaw.length) return false;
        const cells: SupplierCellRow[] = cellsRaw.map((r: Record<string, unknown>) => toCellRow(r));
        let freePool = BigInt(String(rootRows[0]!.balance_atoms));
        for (const c of cells) {
          const free = c.balance_atoms - c.reserved_atoms;
          if (free > 0n) {
            c.balance_atoms -= free;
            freePool += free;
            c.dirty = true;
          }
        }
        if (freePool <= 0n) return false;
        const perCell = freePool / BigInt(cells.length);
        const rem = freePool % BigInt(cells.length);
        for (let i = 0; i < cells.length; i++) {
          const grant = perCell + (BigInt(i) < rem ? 1n : 0n);
          cells[i]!.balance_atoms += grant;
          cells[i]!.revision += 1n;
          cells[i]!.dirty = true;
        }
        await flushDirtyCells(tx, cells);
        await tx`
          UPDATE public.supplier_root_balances
          SET balance_atoms = 0, revision = revision + 1
          WHERE account_id = ${accountId} AND currency = ${currency}
        `;
        return true;
      });
    },

    async getSupplierBalance(
      accountId: string,
      currency: string,
    ): Promise<{ balance_atoms: string; reserved_atoms: string; spent_atoms: string }> {
      const [rootRows, cellRows] = await Promise.all([
        orm
          .select({
            balanceAtoms: supplierRootBalances.balanceAtoms,
            spentAtoms: supplierRootBalances.spentAtoms,
          })
          .from(supplierRootBalances)
          .where(
            and(
              eq(supplierRootBalances.accountId, accountId),
              eq(supplierRootBalances.currency, currency),
            ),
          ),
        orm
          .select({
            bal: sql<string>`coalesce(sum(${supplierCells.balanceAtoms}), 0)::text`,
            res: sql<string>`coalesce(sum(${supplierCells.reservedAtoms}), 0)::text`,
            spent: sql<string>`coalesce(sum(${supplierCells.spentAtoms}), 0)::text`,
          })
          .from(supplierCells)
          .where(and(eq(supplierCells.accountId, accountId), eq(supplierCells.currency, currency))),
      ]);
      const rootBal = BigInt(rootRows[0]?.balanceAtoms ?? "0");
      const rootSpent = BigInt(rootRows[0]?.spentAtoms ?? "0");
      const cellBal = BigInt(cellRows[0]?.bal ?? "0");
      const cellRes = BigInt(cellRows[0]?.res ?? "0");
      const cellSpent = BigInt(cellRows[0]?.spent ?? "0");
      return {
        balance_atoms: (rootBal + cellBal).toString(),
        reserved_atoms: cellRes.toString(),
        spent_atoms: (rootSpent + cellSpent).toString(),
      };
    },
  };
}
