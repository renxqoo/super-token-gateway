import type { SQL } from "bun";
import { DomainError } from "@gateway/kernel";
import type {
  AdjustSupplierBalanceInput,
  AdjustSupplierBalanceResult,
  RechargeSupplierInput,
} from "./types.ts";
import {
  CELL_COUNT,
  parseAtoms,
  parseSignedAtoms,
  sha256Hex,
  toCellRow,
  insertJournalLines,
  flushDirtyCells,
  type SupplierCellRow,
} from "./funds.ts";

export function createSupplierFunds(db: SQL) {
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
            throw new DomainError("adjustment_conflict", "Supplier recharge external_ref conflict");
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
        const [leaseSpend] = await tx<{ spent: string }[]>`
          SELECT coalesce(sum(spent_atoms), 0)::text AS spent
          FROM public.supplier_leases
          WHERE account_id = ${input.account_id}::uuid
            AND currency = ${input.currency}
            AND state = 'ACTIVE'
        `;
        const deferredSpent = BigInt(leaseSpend?.spent ?? "0");
        const total = cells.reduce((acc, c) => acc + c.balance_atoms, 0n) - deferredSpent;
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

    async adjustSupplierBalance(
      input: AdjustSupplierBalanceInput,
    ): Promise<AdjustSupplierBalanceResult> {
      const delta = parseSignedAtoms(input.delta_atoms);
      if (delta === null || delta === 0n) {
        throw new DomainError(
          "validation_error",
          "Supplier adjustment delta_atoms must be non-zero",
        );
      }
      const allowNegative = Boolean(input.allow_negative);
      const fingerprint = sha256Hex({
        account_id: input.account_id,
        currency: input.currency,
        delta_atoms: input.delta_atoms,
        allow_negative: allowNegative,
        reason: input.reason,
      });

      return db.begin(async (tx) => {
        const acctRows = await tx<
          { id: string; state: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED" }[]
        >`
          SELECT id::text AS id, state
          FROM public.upstream_accounts
          WHERE id = ${input.account_id}::uuid
          FOR NO KEY UPDATE
        `;
        const acct = acctRows[0];
        if (!acct) {
          throw new DomainError("not_found", "Upstream account not found");
        }

        const existingOp = await tx`
          SELECT id, fingerprint, receipt FROM public.funding_operations
          WHERE side = 'SUPPLIER' AND entity_id = ${input.account_id}::uuid
            AND kind = 'SUPPLIER_ADJUST' AND external_ref = ${input.external_ref}
        `;
        if (existingOp.length > 0) {
          if (String(existingOp[0]!.fingerprint) !== fingerprint) {
            throw new DomainError(
              "adjustment_conflict",
              "Supplier adjustment external_ref conflict",
            );
          }
          const rec = existingOp[0]!.receipt as {
            delta_atoms: string;
            total_balance_atoms: string;
            reserved_atoms: string;
            account_state: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED";
          };
          return {
            operation_id: String(existingOp[0]!.id),
            replayed: true,
            delta_atoms: rec.delta_atoms,
            total_balance_atoms: rec.total_balance_atoms,
            reserved_atoms: rec.reserved_atoms,
            account_state: rec.account_state,
          };
        }

        await tx`
          INSERT INTO public.supplier_root_balances (account_id, currency, balance_atoms)
          VALUES (${input.account_id}::uuid, ${input.currency}, 0)
          ON CONFLICT (account_id, currency) DO NOTHING
        `;
        await tx`
          INSERT INTO public.supplier_cells (account_id, currency, cell, balance_atoms)
          SELECT ${input.account_id}::uuid, ${input.currency}, g::smallint, 0
          FROM generate_series(0, ${CELL_COUNT - 1}) AS g
          ON CONFLICT (account_id, currency, cell) DO NOTHING
        `;
        const rootRows = await tx`
          SELECT balance_atoms::text, revision::text
          FROM public.supplier_root_balances
          WHERE account_id = ${input.account_id}::uuid AND currency = ${input.currency}
          FOR NO KEY UPDATE
        `;
        const cellsRaw = await tx`
          SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
          FROM public.supplier_cells
          WHERE account_id = ${input.account_id}::uuid AND currency = ${input.currency}
          ORDER BY cell
          FOR NO KEY UPDATE
        `;
        const cells: SupplierCellRow[] = cellsRaw.map((r: Record<string, unknown>) => toCellRow(r));
        let rootBal = BigInt(String(rootRows[0]!.balance_atoms));
        let accountState = acct.state;
        let bumpedEpoch: number | undefined;

        if (delta > 0n) {
          const totalToDistribute = rootBal + delta;
          if (totalToDistribute >= 0n) {
            const perCell = totalToDistribute / BigInt(CELL_COUNT);
            const rem = totalToDistribute % BigInt(CELL_COUNT);
            for (let i = 0; i < cells.length; i++) {
              const share = perCell + (BigInt(i) < rem ? 1n : 0n);
              cells[i]!.balance_atoms += share;
              cells[i]!.revision += 1n;
              cells[i]!.dirty = true;
            }
            rootBal = 0n;
          } else {
            rootBal = totalToDistribute;
          }
        } else {
          const amount = -delta;
          let totalFree = rootBal > 0n ? rootBal : 0n;
          for (const c of cells) {
            const free = c.balance_atoms - c.reserved_atoms;
            if (free > 0n) totalFree += free;
          }
          if (!allowNegative && amount > totalFree) {
            throw new DomainError(
              "supplier_insufficient_balance",
              "Supplier account has insufficient available balance for negative adjustment",
            );
          }
          let rem = amount;
          if (rootBal > 0n && rem > 0n) {
            const takeRoot = rem < rootBal ? rem : rootBal;
            rootBal -= takeRoot;
            rem -= takeRoot;
          }
          if (rem > 0n) {
            const ordered = [...cells].sort((a, b) => {
              const fa = a.balance_atoms - a.reserved_atoms;
              const fb = b.balance_atoms - b.reserved_atoms;
              return fb > fa ? 1 : fb < fa ? -1 : a.cell - b.cell;
            });
            for (const c of ordered) {
              if (rem === 0n) break;
              const free = c.balance_atoms - c.reserved_atoms;
              if (free <= 0n) continue;
              const take = rem < free ? rem : free;
              c.balance_atoms -= take;
              c.revision += 1n;
              c.dirty = true;
              rem -= take;
            }
          }
          if (rem > 0n) {
            cells[0]!.balance_atoms -= rem;
            cells[0]!.revision += 1n;
            cells[0]!.dirty = true;
            rem = 0n;
          }
        }

        await flushDirtyCells(tx, cells);
        await tx`
          UPDATE public.supplier_root_balances
          SET balance_atoms = ${rootBal.toString()}::numeric, revision = revision + 1
          WHERE account_id = ${input.account_id}::uuid AND currency = ${input.currency}
        `;

        const leaseSpendRows = await tx<{ cell: number; spent: string }[]>`
          SELECT cell, coalesce(sum(spent_atoms), 0)::text AS spent
          FROM public.supplier_leases
          WHERE account_id = ${input.account_id}::uuid
            AND currency = ${input.currency}
            AND state = 'ACTIVE'
            AND spent_atoms > 0
          GROUP BY cell
        `;
        const deferredByCell = new Map<number, bigint>();
        let totalDeferredSpent = 0n;
        for (const r of leaseSpendRows) {
          const spent = BigInt(r.spent);
          deferredByCell.set(Number(r.cell), spent);
          totalDeferredSpent += spent;
        }
        const totalBalance =
          rootBal + cells.reduce((acc, c) => acc + c.balance_atoms, 0n) - totalDeferredSpent;
        const totalReserved =
          cells.reduce((acc, c) => acc + c.reserved_atoms, 0n) - totalDeferredSpent;
        const breached =
          totalBalance < 0n ||
          cells.some((c) => {
            const deferred = deferredByCell.get(c.cell) ?? 0n;
            const liveBalance = c.balance_atoms - deferred;
            const liveReserved = c.reserved_atoms - deferred;
            return liveBalance < 0n || liveBalance < liveReserved;
          });

        if (breached && accountState === "ACTIVE") {
          await tx`
            UPDATE public.upstream_accounts
            SET state = 'RECONCILE_REQUIRED', revision = revision + 1, updated_at = now()
            WHERE id = ${input.account_id}::uuid AND state = 'ACTIVE'
          `;
          const [ep] = await tx<{ version: number }[]>`
            UPDATE public.config_epoch SET version = version + 1, updated_at = now()
            WHERE id = 1
            RETURNING version::int AS version
          `;
          accountState = "RECONCILE_REQUIRED";
          bumpedEpoch = ep?.version;
        }

        const opId = crypto.randomUUID();
        await insertJournalLines(tx, [
          {
            journal_id: opId,
            ordinal: 0,
            side: "SUPPLIER",
            entity_id: input.account_id,
            request_id: null,
            reference: `supplier_adjust:${input.external_ref}`,
            kind: "SUPPLIER_ADJUST",
            currency: input.currency,
            account: "upstream:available",
            atoms: delta,
          },
          {
            journal_id: opId,
            ordinal: 1,
            side: "SUPPLIER",
            entity_id: input.account_id,
            request_id: null,
            reference: `supplier_adjust:${input.external_ref}`,
            kind: "SUPPLIER_ADJUST",
            currency: input.currency,
            account: "upstream:adjustment",
            atoms: -delta,
          },
        ]);

        const payload = {
          actor: input.actor ?? null,
          allow_negative: allowNegative,
        };
        const receipt = {
          delta_atoms: delta.toString(),
          total_balance_atoms: totalBalance.toString(),
          reserved_atoms: totalReserved.toString(),
          account_state: accountState,
        };
        await tx`
          INSERT INTO public.funding_operations (
            id, side, entity_id, kind, external_ref, fingerprint,
            amount_atoms, currency, reason, payload, receipt
          )
          VALUES (
            ${opId}::uuid, 'SUPPLIER', ${input.account_id}::uuid, 'SUPPLIER_ADJUST',
            ${input.external_ref}, ${fingerprint}, ${delta.toString()}::numeric,
            ${input.currency}, ${input.reason}, ${payload}::jsonb, ${receipt}::jsonb
          )
        `;

        return {
          operation_id: opId,
          replayed: false,
          delta_atoms: delta.toString(),
          total_balance_atoms: totalBalance.toString(),
          reserved_atoms: totalReserved.toString(),
          account_state: accountState,
          ...(bumpedEpoch !== undefined ? { config_epoch: bumpedEpoch } : {}),
        };
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
      const [row] = await db<
        {
          root_bal: string;
          root_spent: string;
          cell_bal: string;
          cell_res: string;
          cell_spent: string;
          lease_spent: string;
        }[]
      >`
        SELECT
          coalesce((
            SELECT r.balance_atoms
            FROM public.supplier_root_balances r
            WHERE r.account_id = ${accountId}::uuid AND r.currency = ${currency}
          ), 0)::text AS root_bal,
          coalesce((
            SELECT r.spent_atoms
            FROM public.supplier_root_balances r
            WHERE r.account_id = ${accountId}::uuid AND r.currency = ${currency}
          ), 0)::text AS root_spent,
          coalesce(c.bal, 0)::text AS cell_bal,
          coalesce(c.res, 0)::text AS cell_res,
          coalesce(c.spent, 0)::text AS cell_spent,
          coalesce((
            SELECT sum(sl.spent_atoms)
            FROM public.supplier_leases sl
            WHERE sl.account_id = ${accountId}::uuid
              AND sl.currency = ${currency}
              AND sl.state = 'ACTIVE'
          ), 0)::text AS lease_spent
        FROM (
          SELECT
            coalesce(sum(balance_atoms), 0) AS bal,
            coalesce(sum(reserved_atoms), 0) AS res,
            coalesce(sum(spent_atoms), 0) AS spent
          FROM public.supplier_cells
          WHERE account_id = ${accountId}::uuid AND currency = ${currency}
        ) c
      `;
      const rootBal = BigInt(row?.root_bal ?? "0");
      const rootSpent = BigInt(row?.root_spent ?? "0");
      const cellBal = BigInt(row?.cell_bal ?? "0");
      const cellRes = BigInt(row?.cell_res ?? "0");
      const cellSpent = BigInt(row?.cell_spent ?? "0");
      const leaseSpent = BigInt(row?.lease_spent ?? "0");
      return {
        balance_atoms: (rootBal + cellBal - leaseSpent).toString(),
        reserved_atoms: (cellRes - leaseSpent).toString(),
        spent_atoms: (rootSpent + cellSpent + leaseSpent).toString(),
      };
    },
  };
}
