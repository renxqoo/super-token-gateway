import type {
  SwitchSupplierInput,
  SwitchSupplierResult,
  HoldLotAllocation,
  SupplierHoldEntry,
} from "./types.ts";
import { lockExactCells, type CellTarget } from "./supplier-cell-locks.ts";
import type { HoldTransitionLeaseHooks } from "./hold-transitions.ts";
import { detachHoldFromLeaseTx } from "./leases.ts";
import {
  type KeySpendRow,
  toKeySpendRow,
  rolloverKeySpendInMemory,
  flushDirtyKeySpends,
} from "./key-spend.ts";
import {
  toWalletRow,
  type JournalLine,
  sortLots,
  toLotRow,
  expireLotsInMemory,
  walletAdmissionError,
  loadDeferredWalletLeaseSpend,
  type LotRow,
  lotEligible,
  canReserveWalletCredit,
  flushDirtyLots,
  flushDirtyWallets,
  insertJournalLines,
  CELL_COUNT,
  preferredCell,
  type SupplierCellRow,
  cellKey,
  toCellRow,
  flushDirtyCells,
} from "./funds.ts";
import type { SQL } from "bun";

class SupplierSwitchRejected extends Error {
  constructor(readonly result: Extract<SwitchSupplierResult, { ok: false }>) {
    super(result.message);
  }
}

class RetryWithAllCells extends Error {
  constructor() {
    super("retry_all_cells");
  }
}

export function createSupplierSwitch({
  db,
  leases,
}: {
  db: SQL;
  leases: HoldTransitionLeaseHooks;
}) {
  const executeSingleSwitch = async (
    input: SwitchSupplierInput,
    nextSup: bigint,
    reqCust: bigint | null,
  ): Promise<SwitchSupplierResult> => {
    const executeSwitch = async (lockAllNextCells: boolean): Promise<SwitchSupplierResult> => {
      let reboundAttempt = false;
      let detached: {
        wlId: string | null;
        heldCust: bigint;
        slId: string | null;
        supAtoms: bigint;
        keptWalletLease: boolean;
      } | null = null;
      const result = await db
        .begin(async (tx): Promise<SwitchSupplierResult> => {
          const holdRows = await tx`
            SELECT request_id, billing_account_id, wallet_id, api_key_id, key_reserved_atoms::text, operation, model, held_atoms::text, credit_atoms::text,
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
          const needsCustTopUp = reqCust !== null && reqCust > heldAtoms;
          const failedIdx = supplierHolds.findIndex(
            (sh) => sh.attempt_id === input.failed_attempt_id && sh.state === "HELD",
          );

          if (!needsCustTopUp && input.failed_outcome === "FAILED" && failedIdx >= 0) {
            const oldSh = supplierHolds[failedIdx]!;
            if (
              oldSh.account_id === input.next_account_id &&
              oldSh.currency === input.next_currency &&
              BigInt(oldSh.held_atoms) === nextSup
            ) {
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
              supplierHolds[failedIdx] = { ...oldSh, state: "RELEASED" };
              supplierHolds.push({
                attempt_id: input.next_attempt_id,
                ...(input.next_supplier_cost_version_id
                  ? { cost_version_id: input.next_supplier_cost_version_id }
                  : {}),
                ...(input.next_customer_price_version_id
                  ? {
                      customer_price_version_id: input.next_customer_price_version_id,
                      ...(input.next_customer_price_adjustment
                        ? { customer_price_adjustment: input.next_customer_price_adjustment }
                        : {}),
                    }
                  : {}),
                ...(input.next_customer_pricing_snapshot
                  ? { customer_pricing_snapshot: input.next_customer_pricing_snapshot }
                  : {}),
                ...(input.next_supplier_pricing_snapshot
                  ? { supplier_pricing_snapshot: input.next_supplier_pricing_snapshot }
                  : {}),
                account_id: input.next_account_id,
                channel_id: input.next_channel_id,
                currency: input.next_currency,
                cell: oldSh.cell,
                held_atoms: nextSup.toString(),
                state: "HELD",
              });
              await tx`
                UPDATE public.billing_holds
                SET supplier_holds = ${supplierHolds}::jsonb,
                    expires_at = GREATEST(
                      expires_at,
                      COALESCE(${input.expires_at ?? null}::timestamptz, expires_at)
                    )
                WHERE request_id = ${input.request_id}
              `;
              reboundAttempt = true;
              return { ok: true, next_cell: oldSh.cell, held_atoms: heldAtoms.toString() };
            }
          }

          const hasWalletLease = hold.wallet_lease_id !== null;
          const hasSupplierLease = hold.supplier_lease_id !== null;
          const detachWallet = needsCustTopUp && hasWalletLease;
          if (detachWallet || hasSupplierLease) {
            const det = await detachHoldFromLeaseTx(
              tx,
              {
                request_id: String(hold.request_id),
                held_atoms: heldAtoms,
                wallet_lease_id: hold.wallet_lease_id ? String(hold.wallet_lease_id) : null,
                supplier_lease_id: hold.supplier_lease_id ? String(hold.supplier_lease_id) : null,
                lot_allocations: lotAllocs,
                supplier_holds: supplierHolds,
              },
              { detachWallet, detachSupplier: hasSupplierLease },
            );
            lotAllocs = det.lot_allocations;
            detached = {
              wlId: det.detachedWalletLeaseId,
              heldCust: heldAtoms,
              slId: det.detachedSupplierLeaseId,
              supAtoms: det.detachedSupplierAtoms,
              keptWalletLease: hasWalletLease && !detachWallet,
            };
          }

          if (needsCustTopUp) {
            let lockedKeySpend: KeySpendRow | null = null;
            if (holdApiKeyId) {
              const [pgNowRow] =
                await tx`SELECT floor(extract(epoch from now()) * 1000)::bigint AS ms`;
              const pgNowMs = pgNowRow ? Number(pgNowRow.ms) : Date.now();
              const kRows = await tx`
                  SELECT id, billing_account_id, spend_limit_atoms::text, spend_limit_window,
                    spend_window_start, spent_atoms::text, reserved_atoms::text
                  FROM public.api_keys
                  WHERE id = ${holdApiKeyId} AND (spend_limit_atoms IS NOT NULL OR reserved_atoms > 0)
                  FOR NO KEY UPDATE
                `;
              if (kRows.length > 0) {
                lockedKeySpend = toKeySpendRow(kRows[0]!);
                rolloverKeySpendInMemory(lockedKeySpend, pgNowMs);
              }
            }

            const walletId = String(hold.wallet_id);
            const wRows = await tx`
                SELECT id, billing_account_id, currency, policy,
                  available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
                  debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
                  credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
                FROM public.wallets WHERE id = ${walletId} FOR NO KEY UPDATE
              `;
            const wallet = toWalletRow(wRows[0]!);
            const journals: JournalLine[] = [];

            const delta = reqCust! - heldAtoms;
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
            const nowMs = Date.now();
            expireLotsInMemory(wallet, lots, nowMs, journals);
            const deferredSpend =
              wallet.debt_atoms > 0n
                ? ((await loadDeferredWalletLeaseSpend(tx, [wallet.id])).get(wallet.id) ?? 0n)
                : 0n;
            const admissionError = walletAdmissionError(wallet, deferredSpend);
            if (admissionError) throw new SupplierSwitchRejected({ ok: false, ...admissionError });
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
            if (!canReserveWalletCredit(wallet, addCredit, creditAtoms)) {
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

          const oldRelease =
            input.failed_outcome === "FAILED" && failedIdx >= 0 ? supplierHolds[failedIdx]! : null;
          const cellTargets: CellTarget[] = [];
          if (oldRelease) {
            cellTargets.push({
              account_id: oldRelease.account_id,
              currency: oldRelease.currency,
              cell: oldRelease.cell,
            });
          }
          if (lockAllNextCells) {
            for (let c = 0; c < CELL_COUNT; c++) {
              cellTargets.push({
                account_id: input.next_account_id,
                currency: input.next_currency,
                cell: c,
              });
            }
          } else {
            cellTargets.push({
              account_id: input.next_account_id,
              currency: input.next_currency,
              cell: preferredCell(input.next_attempt_id),
            });
          }

          const cells = await lockExactCells(tx, cellTargets);
          const cellsByKey = new Map<string, SupplierCellRow>();
          for (const c of cells) cellsByKey.set(cellKey(c.account_id, c.currency, c.cell), c);

          if (failedIdx >= 0) {
            const sh = supplierHolds[failedIdx]!;
            if (input.failed_outcome === "FAILED") {
              const oldCell = cellsByKey.get(cellKey(sh.account_id, sh.currency, sh.cell));
              if (oldCell) {
                oldCell.reserved_atoms -= BigInt(sh.held_atoms);
                oldCell.revision += 1n;
                oldCell.dirty = true;
              }
              supplierHolds[failedIdx] = { ...sh, state: "RELEASED" };
            } else {
              supplierHolds[failedIdx] = {
                ...sh,
                ...(input.failed_provider_request_id
                  ? { provider_request_id: input.failed_provider_request_id }
                  : {}),
                state: "UNKNOWN",
              };
            }
          }

          const pickFromNext = (): SupplierCellRow | undefined => {
            const nextAcctCells = cells.filter(
              (c) => c.account_id === input.next_account_id && c.currency === input.next_currency,
            );
            if (nextAcctCells.length === 0) return undefined;
            const start = preferredCell(input.next_attempt_id);
            for (let offset = 0; offset < nextAcctCells.length; offset++) {
              const c = nextAcctCells[(start + offset) % nextAcctCells.length]!;
              if (c.balance_atoms - c.reserved_atoms >= nextSup) return c;
            }
            return undefined;
          };

          let chosen = pickFromNext();
          if (!chosen && !lockAllNextCells) {
            const alreadyLocked = cells
              .filter(
                (c) => c.account_id === input.next_account_id && c.currency === input.next_currency,
              )
              .map((c) => c.cell);
            const extraRows = await tx`
                SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
                FROM public.supplier_cells
                WHERE account_id = ${input.next_account_id}
                  AND currency = ${input.next_currency}
                  AND cell <> ALL(${alreadyLocked}::smallint[])
                ORDER BY cell
                FOR NO KEY UPDATE SKIP LOCKED
              `;
            for (const r of extraRows) {
              const c = toCellRow(r);
              cells.push(c);
              cellsByKey.set(cellKey(c.account_id, c.currency, c.cell), c);
            }
            chosen = pickFromNext();
            const inspectedCount = cells.filter(
              (c) => c.account_id === input.next_account_id && c.currency === input.next_currency,
            ).length;
            if (!chosen && inspectedCount < CELL_COUNT) {
              throw new RetryWithAllCells();
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
            ...(input.next_customer_price_version_id
              ? {
                  customer_price_version_id: input.next_customer_price_version_id,
                  ...(input.next_customer_price_adjustment
                    ? { customer_price_adjustment: input.next_customer_price_adjustment }
                    : {}),
                }
              : {}),
            ...(input.next_customer_pricing_snapshot
              ? { customer_pricing_snapshot: input.next_customer_pricing_snapshot }
              : {}),
            ...(input.next_supplier_pricing_snapshot
              ? { supplier_pricing_snapshot: input.next_supplier_pricing_snapshot }
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
                  supplier_holds = ${supplierHolds}::jsonb,
                  expires_at = GREATEST(
                    expires_at,
                    COALESCE(${input.expires_at ?? null}::timestamptz, expires_at)
                  )
              WHERE request_id = ${input.request_id}
            `;

          return { ok: true, next_cell: chosen.cell, held_atoms: heldAtoms.toString() };
        })
        .catch((error: unknown) => {
          if (error instanceof SupplierSwitchRejected) return error.result;
          throw error;
        });
      if (result.ok) {
        if (reboundAttempt) {
          leases.rebindHoldAttempt(input.request_id, input.next_attempt_id, input.next_channel_id);
        } else if (detached) {
          const d = detached as {
            wlId: string | null;
            heldCust: bigint;
            slId: string | null;
            supAtoms: bigint;
            keptWalletLease: boolean;
          };
          if (d.keptWalletLease) {
            leases.notifySupplierDetachedLocal(input.request_id, d.slId, d.supAtoms);
          } else {
            leases.notifyDetachedLocal(input.request_id, d.wlId, d.heldCust, d.slId, d.supAtoms);
          }
        } else {
          leases.notifyDetachedLocal(input.request_id, null, 0n, null, 0n);
        }
      }
      return result;
    };

    try {
      return await executeSwitch(false);
    } catch (error) {
      if (error instanceof RetryWithAllCells) {
        return executeSwitch(true);
      }
      throw error;
    }
  };

  return { executeSingleSwitch };
}
