import type { SQL } from "bun";
import { and, asc, eq, inArray, lte, sql } from "drizzle-orm";
import { detachHoldFromLeaseTx } from "./leases.ts";
import { billingHolds, type LedgerDrizzle } from "./schema.ts";
import type {
  HoldLotAllocation,
  SupplierHoldEntry,
  SwitchSupplierInput,
  SwitchSupplierResult,
} from "./types.ts";
import type { LeaseManager } from "./lease-manager.ts";
import {
  flushDirtyKeySpends,
  releaseKeyReservationInTx,
  rolloverKeySpendInMemory,
  toKeySpendRow,
  type KeySpendRow,
} from "./key-spend.ts";
import {
  parseAtoms,
  computeFreezeReason,
  sortLots,
  lotEligible,
  preferredCell,
  cellKey,
  toWalletRow,
  toLotRow,
  toCellRow,
  insertJournalLines,
  flushDirtyWallets,
  flushDirtyLots,
  flushDirtyCells,
  expireLotsInMemory,
  type LotRow,
  type SupplierCellRow,
  type JournalLine,
} from "./funds.ts";

type HoldRelease = {
  released: boolean;
  detachment?: {
    wlId: string | null;
    heldCust: bigint;
    slId: string | null;
    supAtoms: bigint;
  };
};

export async function releaseHoldInTx(tx: SQL, requestId: string): Promise<HoldRelease> {
  let detachment: HoldRelease["detachment"];
  const nowMs = Date.now();
  const rows = await tx`
          SELECT request_id, wallet_id, api_key_id, key_reserved_atoms::text, held_atoms::text, credit_atoms::text, lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state
          FROM public.billing_holds WHERE request_id = ${requestId} FOR NO KEY UPDATE
        `;
  if (!rows.length) return { released: false };
  const hold = rows[0]!;
  if (hold.state === "RELEASED") return { released: true };
  if (hold.state !== "HELD" && hold.state !== "UNKNOWN") return { released: false };

  const heldAtoms = BigInt(String(hold.held_atoms));
  const creditAtoms = BigInt(String(hold.credit_atoms));
  const holdApiKeyId = hold.api_key_id ? String(hold.api_key_id) : null;
  const keyReservedAtoms = BigInt(String(hold.key_reserved_atoms ?? "0"));
  let lotAllocs = (hold.lot_allocations ?? []) as HoldLotAllocation[];
  const supHolds = (hold.supplier_holds ?? []) as SupplierHoldEntry[];
  if (hold.wallet_lease_id || hold.supplier_lease_id) {
    const det = await detachHoldFromLeaseTx(tx, {
      request_id: String(hold.request_id),
      held_atoms: heldAtoms,
      wallet_lease_id: hold.wallet_lease_id ? String(hold.wallet_lease_id) : null,
      supplier_lease_id: hold.supplier_lease_id ? String(hold.supplier_lease_id) : null,
      lot_allocations: lotAllocs,
      supplier_holds: supHolds,
    });
    lotAllocs = det.lot_allocations;
    detachment = {
      wlId: det.detachedWalletLeaseId,
      heldCust: heldAtoms,
      slId: det.detachedSupplierLeaseId,
      supAtoms: det.detachedSupplierAtoms,
    };
  }

  await releaseKeyReservationInTx(tx, holdApiKeyId, keyReservedAtoms);
  const wRows = await tx`
          SELECT id, project_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets WHERE id = ${hold.wallet_id} FOR NO KEY UPDATE
        `;
  const wallet = toWalletRow(wRows[0]!);
  const lotIds = lotAllocs.map((a) => a.lot_id);
  const lotRowsRaw = lotIds.length
    ? await tx`
              SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
                issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
              FROM public.wallet_lots WHERE id = ANY(${lotIds}::uuid[]) ORDER BY id FOR NO KEY UPDATE
            `
    : [];
  const lotsById = new Map<string, LotRow>();
  for (const r of lotRowsRaw) {
    const l = toLotRow(r);
    lotsById.set(l.id, l);
  }

  const journals: JournalLine[] = [];
  let returnedAvail = 0n;
  let expiredHold = 0n;
  for (const alloc of lotAllocs) {
    const lot = lotsById.get(alloc.lot_id)!;
    const amt = BigInt(alloc.amount_atoms);
    lot.reserved_atoms -= amt;
    if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs) {
      lot.expired_atoms += amt;
      expiredHold += amt;
      const jid = crypto.randomUUID();
      journals.push(
        {
          journal_id: jid,
          ordinal: 0,
          side: "CUSTOMER",
          entity_id: wallet.id,
          request_id: requestId,
          reference: `release_expire:${requestId}:${lot.id}`,
          kind: "EXPIRE",
          currency: wallet.currency,
          account: `lot:${lot.id}:available`,
          atoms: -amt,
        },
        {
          journal_id: jid,
          ordinal: 1,
          side: "CUSTOMER",
          entity_id: wallet.id,
          request_id: requestId,
          reference: `release_expire:${requestId}:${lot.id}`,
          kind: "EXPIRE",
          currency: wallet.currency,
          account: `lot:${lot.id}:expired`,
          atoms: amt,
        },
      );
    } else {
      lot.available_atoms += amt;
      returnedAvail += amt;
    }
    lot.dirty = true;
  }

  wallet.reserved_atoms -= heldAtoms - creditAtoms;
  wallet.credit_reserved_atoms -= creditAtoms;
  wallet.available_atoms += returnedAvail;
  wallet.expired_atoms += expiredHold;
  wallet.freeze_reason = computeFreezeReason(wallet);
  wallet.dirty = true;

  const supAccts = [...new Set(supHolds.map((s) => s.account_id))].sort();
  const cellsRaw = supAccts.length
    ? await tx`
              SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
              FROM public.supplier_cells WHERE account_id = ANY(${supAccts}::uuid[])
              ORDER BY account_id, currency, cell FOR NO KEY UPDATE
            `
    : [];
  const cells = cellsRaw.map((r: Record<string, unknown>) => toCellRow(r));
  const cellsByKey = new Map<string, SupplierCellRow>();
  for (const c of cells) cellsByKey.set(cellKey(c.account_id, c.currency, c.cell), c);
  const updatedSup = supHolds.map((sh) => {
    if (sh.state === "HELD" || sh.state === "UNKNOWN") {
      const c = cellsByKey.get(cellKey(sh.account_id, sh.currency, sh.cell));
      if (c) {
        c.reserved_atoms -= BigInt(sh.held_atoms);
        c.revision += 1n;
        c.dirty = true;
      }
      return { ...sh, state: "RELEASED" as const };
    }
    return sh;
  });

  await flushDirtyWallets(tx, [wallet]);
  await flushDirtyLots(tx, lotsById.values());
  await flushDirtyCells(tx, cells);
  await insertJournalLines(tx, journals);
  await tx`
          UPDATE public.billing_holds
          SET state = 'RELEASED', supplier_holds = ${updatedSup}::jsonb, closed_at = now()
          WHERE request_id = ${requestId}
        `;
  return { released: true, ...(detachment ? { detachment } : {}) };
}

export function notifyHoldRelease(
  notifyDetachedLocal: LeaseManager["notifyDetachedLocal"],
  requestId: string,
  { released, detachment }: HoldRelease,
): void {
  if (released) {
    notifyDetachedLocal(
      requestId,
      detachment?.wlId ?? null,
      detachment?.heldCust ?? 0n,
      detachment?.slId ?? null,
      detachment?.supAtoms ?? 0n,
    );
  }
}

class SupplierSwitchRejected extends Error {
  constructor(readonly result: Extract<SwitchSupplierResult, { ok: false }>) {
    super(result.message);
  }
}

export function createHoldTransitions(
  db: SQL,
  orm: LedgerDrizzle,
  notifyDetachedLocal: LeaseManager["notifyDetachedLocal"],
) {
  return {
    async switchSupplierAttempt(input: SwitchSupplierInput): Promise<SwitchSupplierResult> {
      const nextSup = parseAtoms(input.next_supplier_atoms);
      if (nextSup === null) {
        return {
          ok: false,
          code: "supplier_insufficient_balance",
          message: "Invalid supplier atoms",
        };
      }
      const reqCust =
        input.required_customer_atoms !== undefined
          ? parseAtoms(input.required_customer_atoms)
          : null;
      let detached: {
        wlId: string | null;
        heldCust: bigint;
        slId: string | null;
        supAtoms: bigint;
      } | null = null;
      const result = await db
        .begin(async (tx): Promise<SwitchSupplierResult> => {
          const nowMs = Date.now();
          const holdRows = await tx`
          SELECT request_id, project_id, wallet_id, api_key_id, key_reserved_atoms::text, operation, model, held_atoms::text, credit_atoms::text,
            lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state
          FROM public.billing_holds WHERE request_id = ${input.request_id} FOR NO KEY UPDATE
        `;
          if (!holdRows.length || holdRows[0]!.state !== "HELD") {
            throw new SupplierSwitchRejected({
              ok: false,
              code: "hold_not_active",
              message: "Billing hold is no longer active",
            });
          }
          const hold = holdRows[0]!;
          let heldAtoms = BigInt(String(hold.held_atoms));
          let creditAtoms = BigInt(String(hold.credit_atoms));
          const holdApiKeyId = hold.api_key_id ? String(hold.api_key_id) : null;
          let keyReservedAtoms = BigInt(String(hold.key_reserved_atoms ?? "0"));
          let lotAllocs = [...((hold.lot_allocations ?? []) as HoldLotAllocation[])];
          const supplierHolds = [...((hold.supplier_holds ?? []) as SupplierHoldEntry[])];
          if (hold.wallet_lease_id || hold.supplier_lease_id) {
            const det = await detachHoldFromLeaseTx(tx, {
              request_id: String(hold.request_id),
              held_atoms: heldAtoms,
              wallet_lease_id: hold.wallet_lease_id ? String(hold.wallet_lease_id) : null,
              supplier_lease_id: hold.supplier_lease_id ? String(hold.supplier_lease_id) : null,
              lot_allocations: lotAllocs,
              supplier_holds: supplierHolds,
            });
            lotAllocs = det.lot_allocations;
            detached = {
              wlId: det.detachedWalletLeaseId,
              heldCust: heldAtoms,
              slId: det.detachedSupplierLeaseId,
              supAtoms: det.detachedSupplierAtoms,
            };
          }

          let lockedKeySpend: KeySpendRow | null = null;
          if (reqCust !== null && reqCust > heldAtoms && holdApiKeyId && keyReservedAtoms > 0n) {
            const kRows = await tx`
              SELECT id, project_id, spend_limit_atoms::text, spend_limit_window,
                spend_window_start, spent_atoms::text, reserved_atoms::text
              FROM public.api_keys WHERE id = ${holdApiKeyId} FOR NO KEY UPDATE
            `;
            if (kRows.length > 0) {
              lockedKeySpend = toKeySpendRow(kRows[0]!);
              rolloverKeySpendInMemory(lockedKeySpend, nowMs);
            }
          }

          const walletId = String(hold.wallet_id);
          const wRows = await tx`
          SELECT id, project_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets WHERE id = ${walletId} FOR NO KEY UPDATE
        `;
          const wallet = toWalletRow(wRows[0]!);
          const journals: JournalLine[] = [];

          if (reqCust !== null && reqCust > heldAtoms) {
            const freeze = wallet.freeze_reason ?? computeFreezeReason(wallet);
            if (freeze !== null) {
              throw new SupplierSwitchRejected({
                ok: false,
                code: "wallet_frozen",
                message: `Wallet frozen: ${freeze}`,
              });
            }
            const delta = reqCust - heldAtoms;
            if (
              lockedKeySpend &&
              lockedKeySpend.spend_limit_atoms !== null &&
              lockedKeySpend.spent_atoms + lockedKeySpend.reserved_atoms + delta >
                lockedKeySpend.spend_limit_atoms
            ) {
              throw new SupplierSwitchRejected({
                ok: false,
                code: "key_spend_limit_exceeded",
                message: "API key spend limit exceeded on fallback route top-up",
              });
            }
            const lotRowsRaw = await tx`
            SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
              issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
            FROM public.wallet_lots WHERE wallet_id = ${walletId} AND available_atoms > 0
            ORDER BY id FOR NO KEY UPDATE
          `;
            const lots = sortLots(
              lotRowsRaw.map((r: Record<string, unknown>) => toLotRow(r)),
              wallet.policy,
            );
            expireLotsInMemory(wallet, lots, nowMs, journals);
            let rem = delta;
            const addPlan: { lot: LotRow; take: bigint }[] = [];
            for (const lot of lots) {
              if (rem === 0n) break;
              if (!lotEligible(lot, String(hold.model), String(hold.operation), nowMs)) continue;
              const take = rem < lot.available_atoms ? rem : lot.available_atoms;
              rem -= take;
              addPlan.push({ lot, take });
            }
            const addCredit = rem;
            const addFunded = delta - addCredit;
            if (
              wallet.debt_atoms + wallet.credit_reserved_atoms + addCredit >
              wallet.credit_limit_atoms
            ) {
              throw new SupplierSwitchRejected({
                ok: false,
                code: "insufficient_balance",
                message: "Insufficient balance for fallback route top-up",
              });
            }
            for (const step of addPlan) {
              step.lot.available_atoms -= step.take;
              step.lot.reserved_atoms += step.take;
              step.lot.dirty = true;
              const existing = lotAllocs.find((a) => a.lot_id === step.lot.id);
              if (existing) {
                existing.amount_atoms = (BigInt(existing.amount_atoms) + step.take).toString();
              } else {
                lotAllocs.push({ lot_id: step.lot.id, amount_atoms: step.take.toString() });
              }
            }
            wallet.available_atoms -= addFunded;
            wallet.reserved_atoms += addFunded;
            wallet.credit_reserved_atoms += addCredit;
            wallet.dirty = true;
            heldAtoms += delta;
            creditAtoms += addCredit;
            if (lockedKeySpend && lockedKeySpend.spend_limit_atoms !== null) {
              lockedKeySpend.reserved_atoms += delta;
              lockedKeySpend.dirty = true;
              keyReservedAtoms += delta;
              await flushDirtyKeySpends(tx, [lockedKeySpend]);
            }
            await flushDirtyLots(tx, lots);
            await flushDirtyWallets(tx, [wallet]);
            await insertJournalLines(tx, journals);
          }

          const acctRows = await tx`
          SELECT id, state FROM public.upstream_accounts
          WHERE id = ${input.next_account_id} FOR SHARE
        `;
          if (!acctRows.length || acctRows[0]!.state !== "ACTIVE") {
            throw new SupplierSwitchRejected({
              ok: false,
              code: "supplier_unavailable",
              message: "Fallback supplier account is not active",
            });
          }

          const touchedAccounts = [
            ...new Set([...supplierHolds.map((s) => s.account_id), input.next_account_id]),
          ].sort();
          const cellRowsRaw = await tx`
          SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
          FROM public.supplier_cells
          WHERE account_id = ANY(${touchedAccounts}::uuid[])
          ORDER BY account_id, currency, cell
          FOR NO KEY UPDATE
        `;
          const cells: SupplierCellRow[] = cellRowsRaw.map((r: Record<string, unknown>) =>
            toCellRow(r),
          );
          const cellsByKey = new Map<string, SupplierCellRow>();
          for (const c of cells) cellsByKey.set(cellKey(c.account_id, c.currency, c.cell), c);

          for (let idx = 0; idx < supplierHolds.length; idx++) {
            const sh = supplierHolds[idx]!;
            if (sh.attempt_id === input.failed_attempt_id && sh.state === "HELD") {
              if (input.failed_outcome === "FAILED") {
                const oldCell = cellsByKey.get(cellKey(sh.account_id, sh.currency, sh.cell));
                if (oldCell) {
                  oldCell.reserved_atoms -= BigInt(sh.held_atoms);
                  oldCell.revision += 1n;
                  oldCell.dirty = true;
                }
                supplierHolds[idx] = { ...sh, state: "RELEASED" };
              } else {
                supplierHolds[idx] = { ...sh, state: "UNKNOWN" };
              }
            }
          }

          const nextAcctCells = cells.filter(
            (c) => c.account_id === input.next_account_id && c.currency === input.next_currency,
          );
          let chosen: SupplierCellRow | undefined;
          if (nextAcctCells.length > 0) {
            const start = preferredCell(input.next_attempt_id);
            for (let offset = 0; offset < nextAcctCells.length; offset++) {
              const c = nextAcctCells[(start + offset) % nextAcctCells.length]!;
              if (c.balance_atoms - c.reserved_atoms >= nextSup) {
                chosen = c;
                break;
              }
            }
          }
          if (!chosen) {
            throw new SupplierSwitchRejected({
              ok: false,
              code: "supplier_insufficient_balance",
              message: "Fallback supplier account lacks balance",
            });
          }
          chosen.reserved_atoms += nextSup;
          chosen.revision += 1n;
          chosen.dirty = true;

          supplierHolds.push({
            attempt_id: input.next_attempt_id,
            ...(input.next_supplier_cost_version_id
              ? { cost_version_id: input.next_supplier_cost_version_id }
              : {}),
            account_id: input.next_account_id,
            channel_id: input.next_channel_id,
            currency: input.next_currency,
            cell: chosen.cell,
            held_atoms: nextSup.toString(),
            state: "HELD",
          });

          await flushDirtyCells(tx, cells);
          await tx`
          UPDATE public.billing_holds
          SET held_atoms = ${heldAtoms.toString()}::numeric,
              credit_atoms = ${creditAtoms.toString()}::numeric,
              key_reserved_atoms = ${keyReservedAtoms.toString()}::numeric,
              lot_allocations = ${lotAllocs}::jsonb,
              supplier_holds = ${supplierHolds}::jsonb
          WHERE request_id = ${input.request_id}
        `;

          return { ok: true, next_cell: chosen.cell, held_atoms: heldAtoms.toString() };
        })
        .catch((error: unknown) => {
          if (error instanceof SupplierSwitchRejected) return error.result;
          throw error;
        });
      if (result.ok && detached) {
        const d = detached as {
          wlId: string | null;
          heldCust: bigint;
          slId: string | null;
          supAtoms: bigint;
        };
        notifyDetachedLocal(input.request_id, d.wlId, d.heldCust, d.slId, d.supAtoms);
      } else if (result.ok) {
        notifyDetachedLocal(input.request_id, null, 0n, null, 0n);
      }
      return result;
    },

    async releaseHold(requestId: string): Promise<boolean> {
      const result = await db.begin((tx) => releaseHoldInTx(tx, requestId));
      notifyHoldRelease(notifyDetachedLocal, requestId, result);
      return result.released;
    },

    async markUnknown(requestId: string): Promise<boolean> {
      let detached: {
        wlId: string | null;
        heldCust: bigint;
        slId: string | null;
        supAtoms: bigint;
      } | null = null;
      const marked = await db.begin(async (tx) => {
        const rows = await tx`
          SELECT request_id, held_atoms::text, lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state
          FROM public.billing_holds WHERE request_id = ${requestId} FOR NO KEY UPDATE
        `;
        if (!rows.length || rows[0]!.state !== "HELD") return false;
        const h = rows[0]!;
        const heldAtoms = BigInt(String(h.held_atoms));
        const supHolds = (h.supplier_holds ?? []) as SupplierHoldEntry[];
        if (h.wallet_lease_id || h.supplier_lease_id) {
          const det = await detachHoldFromLeaseTx(tx, {
            request_id: String(h.request_id),
            held_atoms: heldAtoms,
            wallet_lease_id: h.wallet_lease_id ? String(h.wallet_lease_id) : null,
            supplier_lease_id: h.supplier_lease_id ? String(h.supplier_lease_id) : null,
            lot_allocations: (h.lot_allocations ?? []) as HoldLotAllocation[],
            supplier_holds: supHolds,
          });
          detached = {
            wlId: det.detachedWalletLeaseId,
            heldCust: heldAtoms,
            slId: det.detachedSupplierLeaseId,
            supAtoms: det.detachedSupplierAtoms,
          };
        }
        await tx`
          UPDATE public.billing_holds SET state = 'UNKNOWN'
          WHERE request_id = ${requestId} AND state = 'HELD'
        `;
        return true;
      });
      if (detached) {
        const d = detached as {
          wlId: string | null;
          heldCust: bigint;
          slId: string | null;
          supAtoms: bigint;
        };
        notifyDetachedLocal(requestId, d.wlId, d.heldCust, d.slId, d.supAtoms);
      } else if (marked) {
        notifyDetachedLocal(requestId, null, 0n, null, 0n);
      }
      return marked;
    },

    async sweepTimedOutHolds(limit = 100): Promise<number> {
      const expired = orm
        .select({ requestId: billingHolds.requestId })
        .from(billingHolds)
        .where(and(eq(billingHolds.state, "HELD"), lte(billingHolds.expiresAt, sql`now()`)))
        .orderBy(asc(billingHolds.expiresAt))
        .limit(limit)
        .for("no key update", { skipLocked: true });
      const rows = await orm
        .update(billingHolds)
        .set({ state: "UNKNOWN" })
        .where(
          and(
            eq(billingHolds.state, "HELD"),
            lte(billingHolds.expiresAt, sql`now()`),
            inArray(billingHolds.requestId, expired),
          ),
        )
        .returning({ requestId: billingHolds.requestId });
      return rows.length;
    },
  };
}
