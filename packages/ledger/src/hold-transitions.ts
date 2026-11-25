import type { SQL } from "bun";
import { detachHoldFromLeaseTx } from "./leases.ts";
import { MicroBatchQueue } from "./micro-batch.ts";
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
  CELL_COUNT,
  isTransientPgConcurrencyError,
  parseAtoms,
  withPgConcurrencyRetry,
  computeFreezeReason,
  walletAdmissionError,
  canReserveWalletCredit,
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

type CellTarget = {
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

async function lockExactCells(tx: SQL, targets: CellTarget[]): Promise<SupplierCellRow[]> {
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

export async function releaseHoldInTx(
  tx: SQL,
  requestId: string,
  scope: "ALL" | "CUSTOMER" = "ALL",
): Promise<HoldRelease> {
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
          SELECT id, billing_account_id, currency, policy,
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

  const cellTargets =
    scope === "ALL" ? supHolds.filter((sh) => sh.state === "HELD" || sh.state === "UNKNOWN") : [];
  const cells = await lockExactCells(tx, cellTargets);
  const cellsByKey = new Map<string, SupplierCellRow>();
  for (const c of cells) cellsByKey.set(cellKey(c.account_id, c.currency, c.cell), c);
  const updatedSup = supHolds.map((sh) => {
    if (sh.state === "HELD" || sh.state === "UNKNOWN") {
      if (scope === "CUSTOMER") return { ...sh, state: "UNKNOWN" as const };
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

class RetryWithAllCells extends Error {
  constructor() {
    super("retry_all_cells");
  }
}

type HoldTransitionLeaseHooks = Pick<
  LeaseManager,
  "notifyDetachedLocal" | "notifySupplierDetachedLocal" | "rebindHoldAttempt"
>;

export function createHoldTransitions(
  db: SQL,
  leases: HoldTransitionLeaseHooks,
  batchWindowMs = 2,
  maxBatchSize = 256,
) {
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
                account_id: input.next_account_id,
                channel_id: input.next_channel_id,
                currency: input.next_currency,
                cell: oldSh.cell,
                held_atoms: nextSup.toString(),
                state: "HELD",
              });
              await tx`
                UPDATE public.billing_holds
                SET supplier_holds = ${supplierHolds}::jsonb
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
            const admissionError = walletAdmissionError(wallet);
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

  type FastSwitchJob = { input: SwitchSupplierInput; nextSup: bigint };
  const fastSwitchQueue = new MicroBatchQueue<FastSwitchJob, SwitchSupplierResult>(
    async (jobs) => {
      const out = Array.from<SwitchSupplierResult | Promise<SwitchSupplierResult>>({
        length: jobs.length,
      });
      const seenReq = new Set<string>();
      const uniqueJobs: { index: number; job: FastSwitchJob }[] = [];
      const fallbackIndexes: number[] = [];
      for (let i = 0; i < jobs.length; i++) {
        const job = jobs[i]!;
        if (seenReq.has(job.input.request_id)) {
          fallbackIndexes.push(i);
          continue;
        }
        seenReq.add(job.input.request_id);
        uniqueJobs.push({ index: i, job });
      }

      const reqIds = uniqueJobs.map((u) => u.job.input.request_id).sort();
      const acctIds = [...new Set(uniqueJobs.map((u) => u.job.input.next_account_id))].sort();
      const rebound: { requestId: string; nextAttemptId: string; nextChannelId: string }[] = [];

      if (uniqueJobs.length > 0) {
        await db.begin(async (tx) => {
          const [snap] = await tx<
            {
              holds: {
                request_id: string;
                held_atoms: string;
                supplier_holds: SupplierHoldEntry[];
                state: string;
              }[];
              accounts: { id: string; state: string }[];
            }[]
          >`
            WITH
              h AS MATERIALIZED (
                SELECT request_id, held_atoms::text, supplier_holds, state
                FROM public.billing_holds
                WHERE request_id = ANY(${reqIds}::uuid[])
                ORDER BY request_id
                FOR NO KEY UPDATE
              ),
              a AS MATERIALIZED (
                SELECT id, state
                FROM public.upstream_accounts
                WHERE (SELECT count(*) FROM h) >= 0 AND id = ANY(${acctIds}::uuid[])
                ORDER BY id
                FOR SHARE
              )
            SELECT
              COALESCE((SELECT jsonb_agg(h) FROM h), '[]'::jsonb) AS holds,
              COALESCE((SELECT jsonb_agg(a) FROM a), '[]'::jsonb) AS accounts
          `;
          const holdByReq = new Map((snap?.holds ?? []).map((h) => [String(h.request_id), h]));
          const activeAccounts = new Set(
            (snap?.accounts ?? []).filter((a) => a.state === "ACTIVE").map((a) => String(a.id)),
          );
          const updates: { request_id: string; supplier_holds: string }[] = [];
          for (const { index, job } of uniqueJobs) {
            const { input, nextSup } = job;
            const hold = holdByReq.get(input.request_id);
            if (!hold || hold.state !== "HELD") {
              out[index] = {
                ok: false,
                code: "hold_not_active",
                message: "Billing hold is no longer active",
              };
              continue;
            }
            const supplierHolds = [...((hold.supplier_holds ?? []) as SupplierHoldEntry[])];
            const failedIdx = supplierHolds.findIndex(
              (sh) => sh.attempt_id === input.failed_attempt_id && sh.state === "HELD",
            );
            if (failedIdx < 0) {
              fallbackIndexes.push(index);
              continue;
            }
            const oldSh = supplierHolds[failedIdx]!;
            if (
              oldSh.account_id !== input.next_account_id ||
              oldSh.currency !== input.next_currency ||
              BigInt(oldSh.held_atoms) !== nextSup
            ) {
              fallbackIndexes.push(index);
              continue;
            }
            if (!activeAccounts.has(input.next_account_id)) {
              out[index] = {
                ok: false,
                code: "supplier_unavailable",
                message: "Fallback supplier account is not active",
              };
              continue;
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
              account_id: input.next_account_id,
              channel_id: input.next_channel_id,
              currency: input.next_currency,
              cell: oldSh.cell,
              held_atoms: nextSup.toString(),
              state: "HELD",
            });
            updates.push({
              request_id: input.request_id,
              supplier_holds: JSON.stringify(supplierHolds),
            });
            rebound.push({
              requestId: input.request_id,
              nextAttemptId: input.next_attempt_id,
              nextChannelId: input.next_channel_id,
            });
            out[index] = {
              ok: true,
              next_cell: oldSh.cell,
              held_atoms: String(hold.held_atoms),
            };
          }
          if (updates.length > 0) {
            await tx`
              UPDATE public.billing_holds AS h
              SET supplier_holds = u.supplier_holds::jsonb
              FROM unnest(
                ${updates.map((u) => u.request_id)}::uuid[],
                ${updates.map((u) => u.supplier_holds)}::text[]
              ) AS u(request_id, supplier_holds)
              WHERE h.request_id = u.request_id
            `;
          }
        });
        for (const r of rebound) {
          leases.rebindHoldAttempt(r.requestId, r.nextAttemptId, r.nextChannelId);
        }
      }

      for (const idx of fallbackIndexes) {
        const job = jobs[idx]!;
        out[idx] = executeSingleSwitch(job.input, job.nextSup, null);
      }
      return out;
    },
    Math.max(maxBatchSize, 256),
    batchWindowMs,
    4,
  );

  const unknownSwitchQueue = new MicroBatchQueue<FastSwitchJob, SwitchSupplierResult>(
    async (jobs) => {
      const out = Array.from<SwitchSupplierResult | Promise<SwitchSupplierResult>>({
        length: jobs.length,
      });
      const seenReq = new Set<string>();
      const uniqueJobs: { index: number; job: FastSwitchJob }[] = [];
      const fallbackIndexes: number[] = [];
      for (let i = 0; i < jobs.length; i++) {
        const job = jobs[i]!;
        if (seenReq.has(job.input.request_id)) {
          fallbackIndexes.push(i);
          continue;
        }
        seenReq.add(job.input.request_id);
        uniqueJobs.push({ index: i, job });
      }

      const reqIds = uniqueJobs.map((u) => u.job.input.request_id).sort();
      const acctIds = [...new Set(uniqueJobs.map((u) => u.job.input.next_account_id))].sort();
      const detachedSuppliers: { requestId: string; slId: string | null; supAtoms: bigint }[] = [];

      if (uniqueJobs.length > 0) {
        await db.begin(async (tx) => {
          const holdRows = await tx`
            SELECT request_id, held_atoms::text, supplier_holds, supplier_lease_id, state
            FROM public.billing_holds
            WHERE request_id = ANY(${reqIds}::uuid[])
            ORDER BY request_id
            FOR NO KEY UPDATE
          `;
          const holdByReq = new Map<
            string,
            {
              request_id: string;
              held_atoms: string;
              supplier_holds: SupplierHoldEntry[];
              supplier_lease_id: string | null;
              state: string;
            }
          >();
          const slIdSet = new Set<string>();
          for (const r of holdRows) {
            const slId = r.supplier_lease_id ? String(r.supplier_lease_id) : null;
            if (r.state === "HELD" && slId) slIdSet.add(slId);
            holdByReq.set(String(r.request_id), {
              request_id: String(r.request_id),
              held_atoms: String(r.held_atoms),
              supplier_holds: (r.supplier_holds ?? []) as SupplierHoldEntry[],
              supplier_lease_id: slId,
              state: String(r.state),
            });
          }

          const slIds = [...slIdSet].sort();
          const slById = new Map<
            string,
            { id: string; remaining: bigint; state: string; dirty: boolean }
          >();
          if (slIds.length > 0) {
            const slRows = await tx`
              SELECT id, remaining_atoms::text, state
              FROM public.supplier_leases
              WHERE id = ANY(${slIds}::uuid[])
              ORDER BY id
              FOR NO KEY UPDATE
            `;
            for (const r of slRows) {
              slById.set(String(r.id), {
                id: String(r.id),
                remaining: BigInt(String(r.remaining_atoms)),
                state: String(r.state),
                dirty: false,
              });
            }
          }

          const acctRows = await tx`
            SELECT id, state FROM public.upstream_accounts
            WHERE id = ANY(${acctIds}::uuid[])
            ORDER BY id
            FOR SHARE
          `;
          const activeAccounts = new Set<string>();
          for (const r of acctRows) {
            if (r.state === "ACTIVE") activeAccounts.add(String(r.id));
          }

          const cellRowsRaw = await tx`
            SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
            FROM public.supplier_cells
            WHERE account_id = ANY(${acctIds}::uuid[])
            ORDER BY account_id, currency, cell
            FOR NO KEY UPDATE
          `;
          const cellsByAcctCur = new Map<string, SupplierCellRow[]>();
          const allCells: SupplierCellRow[] = [];
          for (const r of cellRowsRaw) {
            const c = toCellRow(r);
            allCells.push(c);
            const k = `${c.account_id}:${c.currency}`;
            const list = cellsByAcctCur.get(k);
            if (list) list.push(c);
            else cellsByAcctCur.set(k, [c]);
          }

          const holdUpdates: { request_id: string; supplier_holds: string; clear_sl: boolean }[] =
            [];
          for (const { index, job } of uniqueJobs) {
            const { input, nextSup } = job;
            const hold = holdByReq.get(input.request_id);
            if (!hold || hold.state !== "HELD") {
              out[index] = {
                ok: false,
                code: "hold_not_active",
                message: "Billing hold is no longer active",
              };
              continue;
            }
            if (!activeAccounts.has(input.next_account_id)) {
              out[index] = {
                ok: false,
                code: "supplier_unavailable",
                message: "Fallback supplier account is not active",
              };
              continue;
            }
            const supplierHolds = [...hold.supplier_holds];
            const failedIdx = supplierHolds.findIndex(
              (sh) => sh.attempt_id === input.failed_attempt_id && sh.state === "HELD",
            );
            const acctCells =
              cellsByAcctCur.get(`${input.next_account_id}:${input.next_currency}`) ?? [];
            const start = preferredCell(input.next_attempt_id);
            let chosen: SupplierCellRow | undefined;
            for (let offset = 0; offset < acctCells.length; offset++) {
              const cand = acctCells[(start + offset) % acctCells.length]!;
              if (cand.balance_atoms - cand.reserved_atoms >= nextSup) {
                chosen = cand;
                break;
              }
            }
            if (!chosen) {
              out[index] = {
                ok: false,
                code: "supplier_insufficient_balance",
                message: "Fallback supplier lacks sufficient balance",
              };
              continue;
            }

            let clearSl = false;
            if (hold.supplier_lease_id) {
              const sl = slById.get(hold.supplier_lease_id);
              if (sl && sl.state === "ACTIVE") {
                const supAtoms = supplierHolds.reduce(
                  (sum, s) =>
                    s.state === "HELD" || s.state === "UNKNOWN" ? sum + BigInt(s.held_atoms) : sum,
                  0n,
                );
                sl.remaining = sl.remaining >= supAtoms ? sl.remaining - supAtoms : 0n;
                sl.dirty = true;
                clearSl = true;
                detachedSuppliers.push({
                  requestId: input.request_id,
                  slId: sl.id,
                  supAtoms,
                });
              } else {
                detachedSuppliers.push({
                  requestId: input.request_id,
                  slId: null,
                  supAtoms: 0n,
                });
              }
            } else {
              detachedSuppliers.push({
                requestId: input.request_id,
                slId: null,
                supAtoms: 0n,
              });
            }

            if (failedIdx >= 0) {
              supplierHolds[failedIdx] = {
                ...supplierHolds[failedIdx]!,
                ...(input.failed_provider_request_id
                  ? { provider_request_id: input.failed_provider_request_id }
                  : {}),
                state: "UNKNOWN",
              };
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
              account_id: input.next_account_id,
              channel_id: input.next_channel_id,
              currency: input.next_currency,
              cell: chosen.cell,
              held_atoms: nextSup.toString(),
              state: "HELD",
            });
            holdUpdates.push({
              request_id: input.request_id,
              supplier_holds: JSON.stringify(supplierHolds),
              clear_sl: clearSl,
            });
            out[index] = {
              ok: true,
              next_cell: chosen.cell,
              held_atoms: hold.held_atoms,
            };
          }

          const dirtySl = [...slById.values()].filter((s) => s.dirty);
          const dirtyCells = allCells.filter((c) => c.dirty);
          if (dirtySl.length > 0 || dirtyCells.length > 0 || holdUpdates.length > 0) {
            await tx`
              WITH
                upd_sl AS (
                  UPDATE public.supplier_leases AS l SET
                    remaining_atoms = u.remaining_atoms,
                    updated_at = now()
                  FROM unnest(
                    ${dirtySl.map((s) => s.id)}::uuid[],
                    ${dirtySl.map((s) => s.remaining.toString())}::numeric[]
                  ) AS u(id, remaining_atoms)
                  WHERE l.id = u.id
                ),
                upd_c AS (
                  UPDATE public.supplier_cells AS c SET
                    reserved_atoms = u.reserved_atoms,
                    revision = u.revision
                  FROM unnest(
                    ${dirtyCells.map((c) => c.account_id)}::uuid[],
                    ${dirtyCells.map((c) => c.currency)}::text[],
                    ${dirtyCells.map((c) => c.cell)}::smallint[],
                    ${dirtyCells.map((c) => c.reserved_atoms.toString())}::numeric[],
                    ${dirtyCells.map((c) => c.revision.toString())}::bigint[]
                  ) AS u(account_id, currency, cell, reserved_atoms, revision)
                  WHERE c.account_id = u.account_id AND c.currency = u.currency AND c.cell = u.cell
                ),
                upd_h AS (
                  UPDATE public.billing_holds AS h SET
                    supplier_holds = u.supplier_holds::jsonb,
                    supplier_lease_id = CASE WHEN u.clear_sl THEN NULL ELSE h.supplier_lease_id END
                  FROM unnest(
                    ${holdUpdates.map((h) => h.request_id)}::uuid[],
                    ${holdUpdates.map((h) => h.supplier_holds)}::text[],
                    ${holdUpdates.map((h) => h.clear_sl)}::boolean[]
                  ) AS u(request_id, supplier_holds, clear_sl)
                  WHERE h.request_id = u.request_id
                )
              SELECT 1
            `;
          }
        });
        for (const d of detachedSuppliers) {
          leases.notifySupplierDetachedLocal(d.requestId, d.slId, d.supAtoms);
        }
      }

      for (const idx of fallbackIndexes) {
        const job = jobs[idx]!;
        out[idx] = executeSingleSwitch(job.input, job.nextSup, null);
      }
      return out;
    },
    Math.max(maxBatchSize, 256),
    batchWindowMs,
    1,
  );

  return {
    switchSupplierAttempt(input: SwitchSupplierInput): Promise<SwitchSupplierResult> {
      const nextSup = parseAtoms(input.next_supplier_atoms);
      if (nextSup === null) {
        return Promise.resolve({
          ok: false,
          code: "supplier_insufficient_balance",
          message: "Invalid supplier atoms",
        });
      }
      const reqCust =
        input.required_customer_atoms !== undefined
          ? parseAtoms(input.required_customer_atoms)
          : null;
      if (reqCust === null && input.failed_outcome === "FAILED") {
        return fastSwitchQueue.enqueue({ input, nextSup });
      }
      if (reqCust === null && input.failed_outcome === "UNKNOWN") {
        return unknownSwitchQueue.enqueue({ input, nextSup });
      }
      return executeSingleSwitch(input, nextSup, reqCust);
    },
    async drain(): Promise<void> {
      await Promise.all([fastSwitchQueue.drain(), unknownSwitchQueue.drain()]);
    },

    async releaseHold(requestId: string): Promise<boolean> {
      const result = await withPgConcurrencyRetry(() =>
        db.begin((tx) => releaseHoldInTx(tx, requestId)),
      );
      if (result.released) {
        notifyHoldRelease(leases.notifyDetachedLocal, requestId, result);
      } else {
        leases.notifyDetachedLocal(requestId, null, 0n, null, 0n);
      }
      return result.released;
    },

    async rememberProviderRequestId(
      requestId: string,
      attemptId: string,
      providerId: string,
    ): Promise<void> {
      await db`UPDATE public.billing_holds h
        SET supplier_holds=(SELECT jsonb_agg(
            CASE WHEN a.entry->>'attempt_id'=${attemptId} AND a.entry->>'provider_request_id' IS NULL
              THEN a.entry || jsonb_build_object('provider_request_id', ${providerId}::text) ELSE a.entry END
            ORDER BY a.ordinal)
          FROM jsonb_array_elements(h.supplier_holds) WITH ORDINALITY a(entry, ordinal))
        WHERE h.request_id=${requestId} AND h.state IN ('HELD','UNKNOWN')
          AND h.supplier_holds @> jsonb_build_array(jsonb_build_object('attempt_id', ${attemptId}::text))`;
    },

    async markUnknown(
      requestId: string,
      providerRequestIds?: ReadonlyMap<string, string>,
    ): Promise<boolean> {
      let detached: {
        wlId: string | null;
        heldCust: bigint;
        slId: string | null;
        supAtoms: bigint;
      } | null = null;
      const marked = await withPgConcurrencyRetry(() =>
        db.begin(async (tx) => {
          detached = null;
          const rows = await tx`
            SELECT request_id, held_atoms::text, lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state
            FROM public.billing_holds WHERE request_id = ${requestId} FOR NO KEY UPDATE
          `;
          if (
            !rows.length ||
            (rows[0]!.state !== "HELD" &&
              (rows[0]!.state !== "UNKNOWN" || !providerRequestIds?.size))
          )
            return false;
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
          const unknown = supHolds.map((attempt) => {
            if (attempt.state !== "HELD" && attempt.state !== "UNKNOWN") return attempt;
            const providerRequestId = providerRequestIds?.get(attempt.attempt_id);
            return {
              ...attempt,
              ...(providerRequestId && !attempt.provider_request_id
                ? { provider_request_id: providerRequestId }
                : {}),
              state: "UNKNOWN",
            };
          });
          await tx`
            UPDATE public.billing_holds SET state = 'UNKNOWN', supplier_holds = ${unknown}::jsonb,
              reconciliation_started_at = COALESCE(reconciliation_started_at, now())
            WHERE request_id = ${requestId} AND state IN ('HELD','UNKNOWN')
          `;
          return true;
        }),
      );
      if (detached) {
        const d = detached as {
          wlId: string | null;
          heldCust: bigint;
          slId: string | null;
          supAtoms: bigint;
        };
        leases.notifyDetachedLocal(requestId, d.wlId, d.heldCust, d.slId, d.supAtoms);
      } else {
        leases.notifyDetachedLocal(requestId, null, 0n, null, 0n);
      }
      return marked;
    },

    async sweepTimedOutHolds(limit = 100): Promise<number> {
      const unleasedRows = await db<{ request_id: string }[]>`
        WITH expired AS MATERIALIZED (
          SELECT request_id
          FROM public.billing_holds
          WHERE state = 'HELD'
            AND expires_at <= now()
            AND wallet_lease_id IS NULL
            AND supplier_lease_id IS NULL
          ORDER BY expires_at
          LIMIT ${limit}
          FOR NO KEY UPDATE SKIP LOCKED
        )
        UPDATE public.billing_holds AS h
        SET state = 'UNKNOWN', reconciliation_started_at = now()
        FROM expired
        WHERE h.request_id = expired.request_id AND h.state = 'HELD'
        RETURNING h.request_id
      `;
      let swept = unleasedRows.length;
      if (swept >= limit) return swept;

      const deadline = performance.now() + 750;
      const scanCap = Math.min(Math.max((limit - swept) * 4, 200), 2000);
      let scanned = 0;
      let cursorExpires: string | null = null;
      let cursorReqId: string | null = null;

      while (swept < limit && scanned < scanCap && performance.now() < deadline) {
        const pageSize = Math.min(100, scanCap - scanned);
        const page: { request_id: string; expires_at: string }[] =
          cursorExpires !== null && cursorReqId !== null
            ? await db<{ request_id: string; expires_at: string }[]>`
                SELECT request_id, expires_at::text AS expires_at
                FROM public.billing_holds
                WHERE state = 'HELD'
                  AND expires_at <= now()
                  AND (wallet_lease_id IS NOT NULL OR supplier_lease_id IS NOT NULL)
                  AND (expires_at, request_id) > (${cursorExpires}::timestamptz, ${cursorReqId}::uuid)
                ORDER BY expires_at, request_id
                LIMIT ${pageSize}
              `
            : await db<{ request_id: string; expires_at: string }[]>`
                SELECT request_id, expires_at::text AS expires_at
                FROM public.billing_holds
                WHERE state = 'HELD'
                  AND expires_at <= now()
                  AND (wallet_lease_id IS NOT NULL OR supplier_lease_id IS NOT NULL)
                ORDER BY expires_at, request_id
                LIMIT ${pageSize}
              `;
        if (page.length === 0) break;
        for (const row of page) {
          scanned++;
          cursorExpires = String(row.expires_at);
          cursorReqId = String(row.request_id);
          if (swept >= limit || performance.now() >= deadline) break;
          const requestId = cursorReqId;
          let detached: {
            wlId: string | null;
            heldCust: bigint;
            slId: string | null;
            supAtoms: bigint;
          } | null = null;
          const marked = await db
            .begin(async (tx) => {
              const rows = await tx`
                SELECT request_id, held_atoms::text, lot_allocations, supplier_holds,
                  wallet_lease_id, supplier_lease_id, state
                FROM public.billing_holds
                WHERE request_id = ${requestId}
                  AND state = 'HELD'
                  AND expires_at <= now()
                FOR NO KEY UPDATE SKIP LOCKED
              `;
              if (!rows.length) return false;
              const h = rows[0]!;
              const heldAtoms = BigInt(String(h.held_atoms));
              const supHolds = (h.supplier_holds ?? []) as SupplierHoldEntry[];
              if (h.wallet_lease_id || h.supplier_lease_id) {
                const det = await detachHoldFromLeaseTx(
                  tx,
                  {
                    request_id: String(h.request_id),
                    held_atoms: heldAtoms,
                    wallet_lease_id: h.wallet_lease_id ? String(h.wallet_lease_id) : null,
                    supplier_lease_id: h.supplier_lease_id ? String(h.supplier_lease_id) : null,
                    lot_allocations: (h.lot_allocations ?? []) as HoldLotAllocation[],
                    supplier_holds: supHolds,
                  },
                  { nowait: true },
                );
                detached = {
                  wlId: det.detachedWalletLeaseId,
                  heldCust: heldAtoms,
                  slId: det.detachedSupplierLeaseId,
                  supAtoms: det.detachedSupplierAtoms,
                };
              }
              await tx`
                UPDATE public.billing_holds
                SET state = 'UNKNOWN', reconciliation_started_at = now()
                WHERE request_id = ${requestId} AND state = 'HELD'
              `;
              return true;
            })
            .catch((error: unknown) => {
              if (isTransientPgConcurrencyError(error)) return false;
              throw error;
            });
          if (marked) {
            swept++;
            if (detached) {
              const d = detached as {
                wlId: string | null;
                heldCust: bigint;
                slId: string | null;
                supAtoms: bigint;
              };
              leases.notifyDetachedLocal(requestId, d.wlId, d.heldCust, d.slId, d.supAtoms);
            } else {
              leases.notifyDetachedLocal(requestId, null, 0n, null, 0n);
            }
          }
        }
        if (page.length < pageSize) break;
      }
      return swept;
    },
  };
}
