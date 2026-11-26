import type { SQL } from "bun";
import { earliestExpiryOf, type ActiveSupplierLease, type ActiveWalletLease } from "./leases.ts";
import type {
  AcquireHoldInput,
  AcquireHoldResult,
  HoldLotAllocation,
  LeaseLotAllocation,
  SupplierHoldEntry,
} from "./types.ts";
import type { LeaseManager } from "./lease-manager.ts";
import { flushDirtyKeySpends } from "./key-spend.ts";
import {
  MAX_ATOMS,
  CELL_COUNT,
  SETTLEMENT_LANES,
  SUPPLIER_CELLS_PER_LANE,
  walletLane,
  parseAtoms,
  walletAdmissionError,
  canReserveWalletCredit,
  lotEligible,
  preferredCell,
  walletKey,
  supplierKey,
  toCellRow,
  insertJournalLines,
  holdTemporarilyUnavailable,
  classifyLedgerStatementCancellation,
  isRetryableLedgerTransactionError,
  expireLotsInMemory,
  type WalletRow,
  type LotRow,
  type SupplierCellRow,
  type JournalLine,
} from "./funds.ts";

import { lockTransactionalHoldState, matchesHoldFingerprint } from "./transactional-hold-state.ts";

const DEMAND_WINDOW_MS = 2000;
const LEASE_MIN_MULTIPLIER = 64n;
const LEASE_TARGET_MULTIPLIER = 4096n;
const LEASE_SUPPLIER_TARGET_MULTIPLIER = 32_768n;
const LEASE_MIN_WALLET_CHUNK = 50_000_000n;
const LEASE_MIN_SUPPLIER_CHUNK = 2_000_000_000n;

function recordHit(
  map: Map<string, { count: number; windowStart: number }>,
  key: string,
  nowMs: number,
  windowMs: number,
): number {
  const prev = map.get(key);
  if (!prev || nowMs - prev.windowStart > windowMs) {
    map.set(key, { count: 1, windowStart: nowMs });
    return 1;
  }
  prev.count++;
  return prev.count;
}

export function createTransactionalHoldBatch(
  db: SQL,
  leases: LeaseManager,
  enqueueContended?: (input: AcquireHoldInput) => Promise<AcquireHoldResult>,
) {
  const cellOffsets = new Bun.CryptoHasher("sha256").update(leases.nodeId).digest();
  let batchCellCursor = 0;
  const recentWalletHits = new Map<string, { count: number; windowStart: number }>();
  const recentSupplierHits = new Map<string, { count: number; windowStart: number }>();
  const demandWindowMs =
    leases.walletIdleReleaseMs <= 0
      ? 0
      : Math.max(250, Math.min(DEMAND_WINDOW_MS, Math.floor(leases.walletIdleReleaseMs * 0.9)));

  let pruneDemandAt = 0;

  async function acquireBatch(items: AcquireHoldInput[], retryContended: boolean) {
    const retry: number[] = [];
    const results = Array.from<AcquireHoldResult>({ length: items.length });
    const parsed = items.map((item) => ({
      item,
      customerAtoms: parseAtoms(item.customer_atoms),
      supplierAtoms: parseAtoms(item.supplier_atoms),
    }));

    const billingAccountIds = [...new Set(items.map((i) => i.billing_account_id))];
    const limitedKeyIds = [
      ...new Set(
        items.filter((i) => i.enforce_key_spend_limit && i.api_key_id).map((i) => i.api_key_id!),
      ),
    ].sort();
    const supplierAccounts = [...new Set(items.map((i) => i.supplier_account_id))];
    const idempotencyKeys = items
      .filter((i) => i.idempotency_key !== null)
      .map((i) => i.idempotency_key!);

    const createdWalletLeases: ActiveWalletLease[] = [];
    const createdSupplierLeases: ActiveSupplierLease[] = [];

    const apply = async (tx: SQL) => {
      const batchStartedAt = Date.now();
      if (batchStartedAt >= pruneDemandAt) {
        for (const [key, hit] of recentWalletHits) {
          if (batchStartedAt - hit.windowStart > demandWindowMs) recentWalletHits.delete(key);
        }
        for (const [key, hit] of recentSupplierHits) {
          if (batchStartedAt - hit.windowStart > DEMAND_WINDOW_MS) recentSupplierHits.delete(key);
        }
        pruneDemandAt = batchStartedAt + Math.max(demandWindowMs, 1000);
      }

      const {
        walletsById,
        lotsByWallet,
        walletsByKey,
        activeAccounts,
        keySpendsById,
        cellsByAcctCur,
        targetCells,
        allCells,
        allLots,
        existingIdem,
      } = await lockTransactionalHoldState({
        limitedKeyIds,
        tx,
        batchStartedAt,
        billingAccountIds,
        supplierAccounts,
        batchCellCursor: () => batchCellCursor++,
        cellOffsets,
        items,
        retryContended,
        parsed,
        idempotencyKeys,
      });

      const nowMs = Date.now();
      const journals: JournalLine[] = [];
      const smallestUnfundedQuote = new Map<string, bigint>();
      for (const w of walletsById.values()) {
        const wLots = lotsByWallet.get(w.id);
        if (wLots) expireLotsInMemory(w, wLots, nowMs, journals);
      }

      const newHolds: {
        request_id: string;
        billing_account_id: string;
        wallet_id: string;
        api_key_id: string | null;
        key_reserved_atoms: string;
        pricing_override_id: string | null;
        discount_numerator: string | null;
        discount_denominator: string | null;
        operation: string;
        model: string;
        idempotency_key: string | null;
        fingerprint: string;
        held_atoms: string;
        credit_atoms: string;
        lot_allocations: HoldLotAllocation[];
        supplier_holds: SupplierHoldEntry[];
        expires_at: string;
      }[] = [];

      const walletLeaseDemand = new Map<
        string,
        { wallet: WalletRow; maxQuote: bigint; count: number }
      >();
      const supplierLeaseDemand = new Map<
        string,
        {
          accountCurrency: string;
          lane: number;
          cell: SupplierCellRow;
          maxQuote: bigint;
          count: number;
        }
      >();

      for (let i = 0; i < parsed.length; i++) {
        const { item, customerAtoms, supplierAtoms } = parsed[i]!;
        if (customerAtoms === null || supplierAtoms === null) {
          results[i] = { ok: false, code: "invalid_amount", message: "Invalid reservation atoms" };
          continue;
        }
        if (item.idempotency_key !== null) {
          const ik = `${item.billing_account_id}:${item.operation}:${item.idempotency_key}`;
          const prev = existingIdem.get(ik);
          if (prev) {
            if (!matchesHoldFingerprint(prev.fingerprint, item)) {
              results[i] = {
                ok: false,
                code: "idempotency_conflict",
                message: "Idempotency key reused with different request fingerprint",
              };
            } else {
              results[i] = {
                ok: true,
                replayed: true,
                request_id: prev.request_id,
                wallet_id: prev.wallet_id,
                held_atoms: prev.held_atoms,
                credit_atoms: prev.credit_atoms,
                supplier_cell: prev.supplier_holds[0]?.cell ?? 0,
                existing_state: prev.state,
              };
            }
            continue;
          }
        }

        const wk = walletKey(item.billing_account_id, item.currency);
        const wallet = walletsByKey.get(wk);
        if (!wallet) {
          results[i] = {
            ok: false,
            code: "wallet_not_found",
            message: "BillingAccount wallet not found",
          };
          continue;
        }
        const admissionError = walletAdmissionError(wallet);
        if (admissionError) {
          results[i] = { ok: false, ...admissionError };
          continue;
        }

        if (!activeAccounts.has(item.supplier_account_id)) {
          results[i] = {
            ok: false,
            code: "supplier_unavailable",
            message: "Supplier account is not active",
          };
          continue;
        }
        if (
          item.enforce_key_spend_limit &&
          item.api_key_id &&
          !keySpendsById.has(item.api_key_id)
        ) {
          results[i] = holdTemporarilyUnavailable();
          continue;
        }

        const acctCurKey = supplierKey(item.supplier_account_id, item.supplier_currency);
        const lane = walletLane(item.billing_account_id);
        const firstCell = lane * SUPPLIER_CELLS_PER_LANE;
        const sameLane = (cell: SupplierCellRow) =>
          cell.cell >= firstCell && cell.cell < firstCell + SUPPLIER_CELLS_PER_LANE;
        const acctCells = cellsByAcctCur.get(acctCurKey) ?? [];
        const pickCell = (list: SupplierCellRow[]): SupplierCellRow | undefined => {
          if (list.length === 0) return undefined;
          const start = preferredCell(item.attempt_id);
          for (let offset = 0; offset < list.length; offset++) {
            const candidate = list[(start + offset) % list.length]!;
            if (candidate.balance_atoms - candidate.reserved_atoms >= supplierAtoms) {
              return candidate;
            }
          }
          return undefined;
        };
        let chosenCell = pickCell(acctCells.filter(sameLane)) ?? pickCell(acctCells);
        const priorUnfundedQuote = smallestUnfundedQuote.get(acctCurKey);
        if (
          !chosenCell &&
          !retryContended &&
          (priorUnfundedQuote === undefined || supplierAtoms < priorUnfundedQuote)
        ) {
          const [extra] = await tx`
            SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
            FROM public.supplier_cells
            WHERE account_id = ${item.supplier_account_id} AND currency = ${item.supplier_currency}
              AND cell <> ALL(${[...targetCells, ...acctCells.map((cell) => cell.cell)]}::smallint[])
              AND balance_atoms - reserved_atoms >= ${supplierAtoms.toString()}::numeric
            ORDER BY (cell < ${firstCell} OR cell >= ${firstCell + SUPPLIER_CELLS_PER_LANE}), cell
            LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED
          `;
          if (extra) {
            chosenCell = toCellRow(extra);
            allCells.push(chosenCell);
            acctCells.push(chosenCell);
            cellsByAcctCur.set(acctCurKey, acctCells);
          } else {
            smallestUnfundedQuote.set(acctCurKey, supplierAtoms);
          }
        }
        if (!chosenCell) {
          if (!retryContended && acctCells.length < CELL_COUNT) retry.push(i);
          results[i] = {
            ok: false,
            code: "supplier_insufficient_balance",
            message: "Supplier funding cells lack sufficient balance",
          };
          continue;
        }

        const walletLots = lotsByWallet.get(wallet.id) ?? [];
        let remaining = customerAtoms;
        const plan: { lot: LotRow; take: bigint }[] = [];
        for (const lot of walletLots) {
          if (remaining === 0n) break;
          if (!lotEligible(lot, item.model, item.operation, nowMs)) continue;
          const take = remaining < lot.available_atoms ? remaining : lot.available_atoms;
          remaining -= take;
          plan.push({ lot, take });
        }
        const creditPart = remaining;
        const fundedPart = customerAtoms - creditPart;
        if (!canReserveWalletCredit(wallet, creditPart)) {
          results[i] = {
            ok: false,
            code: "insufficient_balance",
            message: "Insufficient wallet balance and credit limit",
          };
          continue;
        }
        if (
          wallet.reserved_atoms + fundedPart > MAX_ATOMS ||
          wallet.credit_reserved_atoms + creditPart > MAX_ATOMS
        ) {
          results[i] = {
            ok: false,
            code: "invalid_amount",
            message: "Wallet reservation exceeds NUMERIC(38,0)",
          };
          continue;
        }

        const keySpendRow =
          item.enforce_key_spend_limit && item.api_key_id
            ? keySpendsById.get(item.api_key_id)
            : undefined;
        if (
          keySpendRow &&
          keySpendRow.spend_limit_atoms !== null &&
          keySpendRow.spent_atoms + keySpendRow.reserved_atoms + customerAtoms >
            keySpendRow.spend_limit_atoms
        ) {
          results[i] = {
            ok: false,
            code: "key_spend_limit_exceeded",
            message: "API key spend limit exceeded",
          };
          continue;
        }

        const allocations: HoldLotAllocation[] = [];
        for (const step of plan) {
          step.lot.available_atoms -= step.take;
          step.lot.reserved_atoms += step.take;
          step.lot.dirty = true;
          allocations.push({ lot_id: step.lot.id, amount_atoms: step.take.toString() });
        }
        wallet.available_atoms -= fundedPart;
        wallet.reserved_atoms += fundedPart;
        wallet.credit_reserved_atoms += creditPart;
        wallet.dirty = true;

        let keyReservedAtoms = 0n;
        if (keySpendRow && keySpendRow.spend_limit_atoms !== null && customerAtoms > 0n) {
          keySpendRow.reserved_atoms += customerAtoms;
          keySpendRow.dirty = true;
          keyReservedAtoms = customerAtoms;
        }

        chosenCell.reserved_atoms += supplierAtoms;
        chosenCell.revision += 1n;
        chosenCell.dirty = true;

        const supplierHold: SupplierHoldEntry = {
          attempt_id: item.attempt_id,
          ...(item.supplier_cost_version_id
            ? { cost_version_id: item.supplier_cost_version_id }
            : {}),
          ...(item.customer_price_version_id
            ? {
                customer_price_version_id: item.customer_price_version_id,
                customer_price_adjustment: {
                  override_id: item.pricing_override_id ?? null,
                  discount_numerator: item.discount_numerator ?? null,
                  discount_denominator: item.discount_denominator ?? null,
                },
              }
            : {}),
          account_id: item.supplier_account_id,
          channel_id: item.supplier_channel_id,
          currency: item.supplier_currency,
          cell: chosenCell.cell,
          held_atoms: supplierAtoms.toString(),
          state: "HELD",
        };

        newHolds.push({
          request_id: item.request_id,
          billing_account_id: item.billing_account_id,
          wallet_id: wallet.id,
          api_key_id: item.api_key_id ?? null,
          key_reserved_atoms: keyReservedAtoms.toString(),
          pricing_override_id: item.pricing_override_id ?? null,
          discount_numerator: item.discount_numerator ?? null,
          discount_denominator: item.discount_denominator ?? null,
          operation: item.operation,
          model: item.model,
          idempotency_key: item.idempotency_key,
          fingerprint: item.fingerprint,
          held_atoms: customerAtoms.toString(),
          credit_atoms: creditPart.toString(),
          lot_allocations: allocations,
          supplier_holds: [supplierHold],
          expires_at: item.expires_at,
        });

        if (item.idempotency_key !== null) {
          existingIdem.set(`${item.billing_account_id}:${item.operation}:${item.idempotency_key}`, {
            request_id: item.request_id,
            wallet_id: wallet.id,
            fingerprint: item.fingerprint,
            held_atoms: customerAtoms.toString(),
            credit_atoms: creditPart.toString(),
            state: "HELD",
            supplier_holds: [supplierHold],
          });
        } else if (
          leases.canAcquire() &&
          !item.enforce_key_spend_limit &&
          creditPart === 0n &&
          customerAtoms > 0n
        ) {
          const wHits = recordHit(recentWalletHits, wk, nowMs, demandWindowMs);
          const demandKey = `${acctCurKey}:${lane}`;
          const sHits =
            supplierAtoms > 0n && sameLane(chosenCell)
              ? recordHit(recentSupplierHits, demandKey, nowMs, DEMAND_WINDOW_MS)
              : 0;
          const highLoadSupplier = sHits >= 32 || leases.hasSupplier(acctCurKey, lane);
          const walletHot = wHits >= 16 || (highLoadSupplier && wHits >= 2);
          if (!leases.hasWallet(wk)) {
            const d = walletLeaseDemand.get(wk);
            if (d) {
              if (customerAtoms > d.maxQuote) d.maxQuote = customerAtoms;
              d.count++;
            } else {
              walletLeaseDemand.set(wk, { wallet, maxQuote: customerAtoms, count: 1 });
            }
          }
          if (walletHot && !leases.hasWallet(wk)) {
            const d = walletLeaseDemand.get(wk);
            if (d && d.count < 4) d.count = 4;
          }
          if (supplierAtoms > 0n && sameLane(chosenCell) && !leases.hasSupplier(acctCurKey, lane)) {
            const demand = supplierLeaseDemand.get(demandKey);
            if (demand) {
              if (supplierAtoms > demand.maxQuote) demand.maxQuote = supplierAtoms;
              demand.count++;
              if (sHits >= 16 && demand.count < 4) demand.count = 4;
            } else {
              supplierLeaseDemand.set(demandKey, {
                accountCurrency: acctCurKey,
                lane,
                cell: chosenCell,
                maxQuote: supplierAtoms,
                count: sHits >= 16 ? 4 : 1,
              });
            }
          }
        }

        results[i] = {
          ok: true,
          replayed: false,
          request_id: item.request_id,
          wallet_id: wallet.id,
          held_atoms: customerAtoms.toString(),
          credit_atoms: creditPart.toString(),
          supplier_cell: chosenCell.cell,
        };
      }

      const newWalletLeaseRows: {
        id: string;
        wallet_id: string;
        currency: string;
        lot_allocations: LeaseLotAllocation[];
        remaining_atoms: string;
        earliest_lot_expires_at: string | null;
        expires_at: string;
      }[] = [];

      for (const [wk, demand] of walletLeaseDemand) {
        if (demand.count < 4 || leases.hasWallet(wk)) continue;
        const w = demand.wallet;
        if (w.freeze_reason !== null || w.debt_atoms > 0n || w.credit_reserved_atoms > 0n) continue;
        if (w.available_atoms < demand.maxQuote * LEASE_MIN_MULTIPLIER) continue;
        const wLots = lotsByWallet.get(w.id) ?? [];
        const activeLots = wLots.filter((l) => l.available_atoms > 0n);
        if (activeLots.length === 0) continue;
        if (activeLots.some((l) => l.models.length > 0 || l.operations.length > 0)) continue;
        const target = demand.maxQuote * LEASE_TARGET_MULTIPLIER;
        const desired = target > LEASE_MIN_WALLET_CHUNK ? target : LEASE_MIN_WALLET_CHUNK;
        const halfAvail = w.available_atoms / 2n;
        const chunk = desired < halfAvail ? desired : halfAvail;
        if (chunk < demand.maxQuote * 8n) continue;

        let rem = chunk;
        const leaseLots: LeaseLotAllocation[] = [];
        for (const lot of activeLots) {
          if (rem === 0n) break;
          if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs + 5000) continue;
          const take = rem < lot.available_atoms ? rem : lot.available_atoms;
          if (take <= 0n) continue;
          rem -= take;
          lot.available_atoms -= take;
          lot.reserved_atoms += take;
          lot.dirty = true;
          leaseLots.push({
            lot_id: lot.id,
            amount_atoms: take.toString(),
            expires_at_ms: lot.expires_at_ms,
          });
        }
        const reserved = chunk - rem;
        if (reserved < demand.maxQuote * 8n) {
          for (const alloc of leaseLots) {
            const lot = activeLots.find((l) => l.id === alloc.lot_id)!;
            const amt = BigInt(alloc.amount_atoms);
            lot.reserved_atoms -= amt;
            lot.available_atoms += amt;
          }
          continue;
        }
        w.available_atoms -= reserved;
        w.reserved_atoms += reserved;
        w.dirty = true;
        const leaseId = crypto.randomUUID();
        const earliestMs = earliestExpiryOf(leaseLots);
        const expiresAtMs = nowMs + leases.ttlMs;
        newWalletLeaseRows.push({
          id: leaseId,
          wallet_id: w.id,
          currency: w.currency,
          lot_allocations: leaseLots,
          remaining_atoms: reserved.toString(),
          earliest_lot_expires_at: earliestMs === null ? null : new Date(earliestMs).toISOString(),
          expires_at: new Date(expiresAtMs).toISOString(),
        });
        createdWalletLeases.push({
          leaseId,
          walletId: w.id,
          billingAccountId: w.billing_account_id,
          currency: w.currency,
          remainingInPg: reserved,
          freeInMemory: reserved,
          earliestLotExpiresAtMs: earliestMs,
          expiresAtMs,
          inFlightHolds: 0,
          closing: false,
          lastUsedAtMs: nowMs,
        });
      }

      const newSupplierLeaseRows: {
        id: string;
        account_id: string;
        currency: string;
        cell: number;
        remaining_atoms: string;
        expires_at: string;
      }[] = [];

      const hasAnyWalletLease = leases.hasAnyWallet() || createdWalletLeases.length > 0;
      for (const demand of supplierLeaseDemand.values()) {
        const { accountCurrency, lane } = demand;
        if (!hasAnyWalletLease || demand.count < 4 || leases.hasSupplier(accountCurrency, lane))
          continue;
        const cell = demand.cell;
        const free = cell.balance_atoms - cell.reserved_atoms;
        if (free < demand.maxQuote * LEASE_MIN_MULTIPLIER) continue;
        const budget = BigInt(SETTLEMENT_LANES);
        const target = (demand.maxQuote * LEASE_SUPPLIER_TARGET_MULTIPLIER) / budget;
        const minimum = LEASE_MIN_SUPPLIER_CHUNK / budget;
        const desired = target > minimum ? target : minimum;
        const halfFree = free / 2n;
        const chunk = desired < halfFree ? desired : halfFree;
        if (chunk < demand.maxQuote * 8n) continue;
        cell.reserved_atoms += chunk;
        cell.revision += 1n;
        cell.dirty = true;
        const leaseId = crypto.randomUUID();
        const expiresAtMs = nowMs + leases.ttlMs;
        newSupplierLeaseRows.push({
          id: leaseId,
          account_id: cell.account_id,
          currency: cell.currency,
          cell: cell.cell,
          remaining_atoms: chunk.toString(),
          expires_at: new Date(expiresAtMs).toISOString(),
        });
        createdSupplierLeases.push({
          lane,
          leaseId,
          accountId: cell.account_id,
          currency: cell.currency,
          cell: cell.cell,
          remainingInPg: chunk,
          freeInMemory: chunk,
          expiresAtMs,
          inFlightHolds: 0,
          closing: false,
          lastUsedAtMs: nowMs,
        });
      }

      await flushDirtyKeySpends(tx, keySpendsById.values());
      const dirtyWallets = [...walletsById.values()].filter((w) => w.dirty);
      const dirtyLots = allLots.filter((l) => l.dirty);
      const dirtyCells = allCells.filter((c) => c.dirty);
      if (
        dirtyWallets.length > 0 ||
        dirtyLots.length > 0 ||
        dirtyCells.length > 0 ||
        newHolds.length > 0
      ) {
        await tx`
          WITH
            upd_w AS (
              UPDATE public.wallets AS w SET
                available_atoms = u.available_atoms,
                reserved_atoms = u.reserved_atoms,
                spent_atoms = u.spent_atoms,
                expired_atoms = u.expired_atoms,
                debt_atoms = u.debt_atoms,
                credit_reserved_atoms = u.credit_reserved_atoms,
                freeze_reason = u.freeze_reason
              FROM unnest(
                ${dirtyWallets.map((w) => w.id)}::uuid[],
                ${dirtyWallets.map((w) => w.available_atoms.toString())}::numeric[],
                ${dirtyWallets.map((w) => w.reserved_atoms.toString())}::numeric[],
                ${dirtyWallets.map((w) => w.spent_atoms.toString())}::numeric[],
                ${dirtyWallets.map((w) => w.expired_atoms.toString())}::numeric[],
                ${dirtyWallets.map((w) => w.debt_atoms.toString())}::numeric[],
                ${dirtyWallets.map((w) => w.credit_reserved_atoms.toString())}::numeric[],
                ${dirtyWallets.map((w) => w.freeze_reason)}::text[]
              ) AS u(id, available_atoms, reserved_atoms, spent_atoms, expired_atoms, debt_atoms, credit_reserved_atoms, freeze_reason)
              WHERE w.id = u.id
            ),
            upd_l AS (
              UPDATE public.wallet_lots AS l SET
                available_atoms = u.available_atoms,
                reserved_atoms = u.reserved_atoms,
                spent_atoms = u.spent_atoms,
                expired_atoms = u.expired_atoms
              FROM unnest(
                ${dirtyLots.map((l) => l.id)}::uuid[],
                ${dirtyLots.map((l) => l.available_atoms.toString())}::numeric[],
                ${dirtyLots.map((l) => l.reserved_atoms.toString())}::numeric[],
                ${dirtyLots.map((l) => l.spent_atoms.toString())}::numeric[],
                ${dirtyLots.map((l) => l.expired_atoms.toString())}::numeric[]
              ) AS u(id, available_atoms, reserved_atoms, spent_atoms, expired_atoms)
              WHERE l.id = u.id
            ),
            upd_c AS (
              UPDATE public.supplier_cells AS c SET
                balance_atoms = u.balance_atoms,
                reserved_atoms = u.reserved_atoms,
                spent_atoms = u.spent_atoms,
                revision = u.revision
              FROM unnest(
                ${dirtyCells.map((c) => c.account_id)}::uuid[],
                ${dirtyCells.map((c) => c.currency)}::text[],
                ${dirtyCells.map((c) => c.cell)}::smallint[],
                ${dirtyCells.map((c) => c.balance_atoms.toString())}::numeric[],
                ${dirtyCells.map((c) => c.reserved_atoms.toString())}::numeric[],
                ${dirtyCells.map((c) => c.spent_atoms.toString())}::numeric[],
                ${dirtyCells.map((c) => c.revision.toString())}::bigint[]
              ) AS u(account_id, currency, cell, balance_atoms, reserved_atoms, spent_atoms, revision)
              WHERE c.account_id = u.account_id AND c.currency = u.currency AND c.cell = u.cell
            ),
            ins_h AS (
              INSERT INTO public.billing_holds (
                request_id, billing_account_id, wallet_id, api_key_id, key_reserved_atoms,
                pricing_override_id, discount_numerator, discount_denominator,
                operation, model, idempotency_key, fingerprint,
                held_atoms, credit_atoms, lot_allocations, supplier_holds, state, expires_at
              )
              SELECT
                u.request_id, u.billing_account_id, u.wallet_id, u.api_key_id, u.key_reserved_atoms,
                u.pricing_override_id, u.discount_numerator, u.discount_denominator,
                u.operation, u.model, u.idempotency_key, u.fingerprint,
                u.held_atoms, u.credit_atoms, u.lot_allocations::jsonb, u.supplier_holds::jsonb, 'HELD', u.expires_at
              FROM unnest(
                ${newHolds.map((h) => h.request_id)}::uuid[],
                ${newHolds.map((h) => h.billing_account_id)}::uuid[],
                ${newHolds.map((h) => h.wallet_id)}::uuid[],
                ${newHolds.map((h) => h.api_key_id)}::uuid[],
                ${newHolds.map((h) => h.key_reserved_atoms)}::numeric[],
                ${newHolds.map((h) => h.pricing_override_id)}::uuid[],
                ${newHolds.map((h) => h.discount_numerator)}::numeric[],
                ${newHolds.map((h) => h.discount_denominator)}::numeric[],
                ${newHolds.map((h) => h.operation)}::text[],
                ${newHolds.map((h) => h.model)}::text[],
                ${newHolds.map((h) => h.idempotency_key)}::text[],
                ${newHolds.map((h) => h.fingerprint)}::text[],
                ${newHolds.map((h) => h.held_atoms)}::numeric[],
                ${newHolds.map((h) => h.credit_atoms)}::numeric[],
                ${newHolds.map((h) => JSON.stringify(h.lot_allocations))}::text[],
                ${newHolds.map((h) => JSON.stringify(h.supplier_holds))}::text[],
                ${newHolds.map((h) => h.expires_at)}::timestamptz[]
              ) AS u(
                request_id, billing_account_id, wallet_id, api_key_id, key_reserved_atoms,
                pricing_override_id, discount_numerator, discount_denominator,
                operation, model, idempotency_key, fingerprint,
                held_atoms, credit_atoms, lot_allocations, supplier_holds, expires_at
              )
            )
          SELECT 1
        `;
      }
      await insertJournalLines(tx, journals);

      if (newWalletLeaseRows.length > 0) {
        await tx`
          INSERT INTO public.wallet_leases (
            id, node_id, wallet_id, currency, lot_allocations, remaining_atoms, earliest_lot_expires_at, state, expires_at
          )
          SELECT
            u.id, ${leases.nodeId}, u.wallet_id, u.currency, u.lot_allocations::jsonb, u.remaining_atoms,
            u.earliest_lot_expires_at, 'ACTIVE', u.expires_at
          FROM unnest(
            ${newWalletLeaseRows.map((l) => l.id)}::uuid[],
            ${newWalletLeaseRows.map((l) => l.wallet_id)}::uuid[],
            ${newWalletLeaseRows.map((l) => l.currency)}::text[],
            ${newWalletLeaseRows.map((l) => JSON.stringify(l.lot_allocations))}::text[],
            ${newWalletLeaseRows.map((l) => l.remaining_atoms)}::numeric[],
            ${newWalletLeaseRows.map((l) => l.earliest_lot_expires_at)}::timestamptz[],
            ${newWalletLeaseRows.map((l) => l.expires_at)}::timestamptz[]
          ) AS u(id, wallet_id, currency, lot_allocations, remaining_atoms, earliest_lot_expires_at, expires_at)
        `;
      }

      if (newSupplierLeaseRows.length > 0) {
        await tx`
          INSERT INTO public.supplier_leases (
            id, node_id, account_id, currency, cell, remaining_atoms, state, expires_at
          )
          SELECT
            u.id, ${leases.nodeId}, u.account_id, u.currency, u.cell, u.remaining_atoms, 'ACTIVE', u.expires_at
          FROM unnest(
            ${newSupplierLeaseRows.map((l) => l.id)}::uuid[],
            ${newSupplierLeaseRows.map((l) => l.account_id)}::uuid[],
            ${newSupplierLeaseRows.map((l) => l.currency)}::text[],
            ${newSupplierLeaseRows.map((l) => l.cell)}::smallint[],
            ${newSupplierLeaseRows.map((l) => l.remaining_atoms)}::numeric[],
            ${newSupplierLeaseRows.map((l) => l.expires_at)}::timestamptz[]
          ) AS u(id, account_id, currency, cell, remaining_atoms, expires_at)
        `;
      }
    };
    await db.begin(classifyLedgerStatementCancellation(apply));

    for (const lease of createdWalletLeases) leases.registerWallet(lease);
    for (const lease of createdSupplierLeases) leases.registerSupplier(lease);
    return { results, retry };
  }

  return async (
    items: AcquireHoldInput[],
  ): Promise<(AcquireHoldResult | Promise<AcquireHoldResult>)[]> => {
    try {
      const { results, retry } = await acquireBatch(items, !enqueueContended);
      if (!enqueueContended || !retry.length) return results;
      const deferred = new Set(retry);
      return results.map((result, index) =>
        deferred.has(index) ? enqueueContended(items[index]!) : result,
      );
    } catch (error) {
      if (!isRetryableLedgerTransactionError(error)) throw error;
      return items.map((item) =>
        enqueueContended ? enqueueContended(item) : holdTemporarilyUnavailable(),
      );
    }
  };
}
