import type { CommitTerminalInput, HoldLotAllocation, SupplierHoldEntry } from "./types.ts";
import { detachHoldFromLeaseTx } from "./leases.ts";
import { type KeySpendRow, toKeySpendRow, rolloverKeySpendInMemory } from "./key-spend.ts";
import {
  type WalletRow,
  toWalletRow,
  type LotRow,
  toLotRow,
  sortLots,
  cellKey,
  type SupplierCellRow,
  toCellRow,
} from "./funds.ts";
import type { SQL } from "bun";

export type DirectSettlementEntry = {
  index: number;
  item: CommitTerminalInput;
  previouslySettledSupplierJournal?: string | null;
};
export type DetachedHold = {
  requestId: string;
  wlId: string | null;
  heldCust: bigint;
  slId: string | null;
  supAtoms: bigint;
};

export async function loadDirectSettlementState({
  tx,
  entries,
  detachedNotifications,
  nowMs,
}: {
  tx: SQL;
  entries: readonly DirectSettlementEntry[];
  detachedNotifications: DetachedHold[];
  nowMs: number;
}) {
  const requestIds = [...new Set(entries.map(({ item }) => item.request_id))].sort();
  const holdRows = await tx`
        SELECT request_id, billing_account_id, wallet_id, api_key_id, key_reserved_atoms::text,
          pricing_override_id, discount_numerator::text, discount_denominator::text,
          operation, model, held_atoms::text, credit_atoms::text,
          lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state, created_at
        FROM public.billing_holds
        WHERE request_id = ANY(${requestIds}::uuid[])
        ORDER BY request_id
        FOR NO KEY UPDATE
      `;
  const holdsByReq = new Map<
    string,
    {
      request_id: string;
      billing_account_id: string;
      wallet_id: string;
      api_key_id: string | null;
      key_reserved_atoms: bigint;
      pricing_override_id: string | null;
      discount_numerator: string | null;
      discount_denominator: string | null;
      operation: string;
      model: string;
      held_atoms: bigint;
      credit_atoms: bigint;
      lot_allocations: HoldLotAllocation[];
      supplier_holds: SupplierHoldEntry[];
      wallet_lease_id: string | null;
      supplier_lease_id: string | null;
      state: string;
      created_at_ms: number;
    }
  >();
  for (const r of holdRows) {
    holdsByReq.set(String(r.request_id), {
      request_id: String(r.request_id),
      billing_account_id: String(r.billing_account_id),
      wallet_id: String(r.wallet_id),
      api_key_id: r.api_key_id ? String(r.api_key_id) : null,
      key_reserved_atoms: BigInt(String(r.key_reserved_atoms ?? "0")),
      pricing_override_id: r.pricing_override_id ? String(r.pricing_override_id) : null,
      discount_numerator: r.discount_numerator ? String(r.discount_numerator) : null,
      discount_denominator: r.discount_denominator ? String(r.discount_denominator) : null,
      operation: String(r.operation),
      model: String(r.model),
      held_atoms: BigInt(String(r.held_atoms)),
      credit_atoms: BigInt(String(r.credit_atoms)),
      lot_allocations: (r.lot_allocations ?? []) as HoldLotAllocation[],
      supplier_holds: (r.supplier_holds ?? []) as SupplierHoldEntry[],
      wallet_lease_id: r.wallet_lease_id ? String(r.wallet_lease_id) : null,
      supplier_lease_id: r.supplier_lease_id ? String(r.supplier_lease_id) : null,
      state: String(r.state),
      created_at_ms: Date.parse(String(r.created_at)),
    });
  }

  const selectedAttempts = new Map<DirectSettlementEntry, SupplierHoldEntry>();
  const approvedRequests = new Set<string>();
  for (const entry of entries) {
    const { item, previouslySettledSupplierJournal } = entry;
    const hold = holdsByReq.get(item.request_id);
    if (hold?.state !== "HELD" && hold?.state !== "UNKNOWN") continue;
    const attempt = hold.supplier_holds.find(
      (supplier) =>
        supplier.attempt_id === item.selected_attempt_id &&
        supplier.channel_id === item.channel_id &&
        supplier.currency === item.supplier_currency,
    );
    if (!attempt) continue;
    const settleable =
      attempt.state === "HELD" ||
      (hold.state === "UNKNOWN" &&
        (attempt.state === "UNKNOWN" ||
          (attempt.state === "SETTLED" && previouslySettledSupplierJournal !== undefined)));
    if (settleable) {
      selectedAttempts.set(entry, attempt);
      approvedRequests.add(item.request_id);
    }
  }

  const approvedHolds = [...holdsByReq.values()].filter((h) => approvedRequests.has(h.request_id));
  const holdsNeedingDetach = approvedHolds.filter(
    (h) => h.wallet_lease_id !== null || h.supplier_lease_id !== null,
  );
  if (holdsNeedingDetach.length > 0) {
    const wlIds = [
      ...new Set(
        holdsNeedingDetach.map((h) => h.wallet_lease_id).filter((x): x is string => x !== null),
      ),
    ].sort();
    const slIds = [
      ...new Set(
        holdsNeedingDetach.map((h) => h.supplier_lease_id).filter((x): x is string => x !== null),
      ),
    ].sort();
    if (wlIds.length > 0) {
      await tx`SELECT id FROM public.wallet_leases WHERE id = ANY(${wlIds}::uuid[]) ORDER BY id FOR NO KEY UPDATE`;
    }
    if (slIds.length > 0) {
      await tx`SELECT id FROM public.supplier_leases WHERE id = ANY(${slIds}::uuid[]) ORDER BY id FOR NO KEY UPDATE`;
    }
    for (const h of holdsNeedingDetach) {
      const det = await detachHoldFromLeaseTx(tx, h);
      h.lot_allocations = det.lot_allocations;
      h.wallet_lease_id = null;
      h.supplier_lease_id = null;
      detachedNotifications.push({
        requestId: h.request_id,
        wlId: det.detachedWalletLeaseId,
        heldCust: h.held_atoms,
        slId: det.detachedSupplierLeaseId,
        supAtoms: det.detachedSupplierAtoms,
      });
    }
  }

  const mayHaveExistingCharge =
    holdsByReq.size < requestIds.length || [...holdsByReq.values()].some((h) => h.state !== "HELD");
  const existingChargesRaw = mayHaveExistingCharge
    ? await tx`
        SELECT id, request_id, outcome, customer_atoms::text, funded_atoms::text, debt_atoms::text, supplier_cost_atoms::text
        FROM public.billing_charges
        WHERE request_id = ANY(${requestIds}::uuid[])
      `
    : [];
  const existingCharges = new Map<
    string,
    {
      id: string;
      outcome: "SETTLED" | "RELEASED" | "DEBT" | "MODERATION_BLOCKED" | "UNKNOWN_RESOLVED";
      customer_atoms: string;
      funded_atoms: string;
      debt_atoms: string;
      supplier_cost_atoms: string;
    }
  >();
  for (const r of existingChargesRaw) {
    existingCharges.set(String(r.request_id), {
      id: String(r.id),
      outcome: r.outcome as
        "SETTLED" | "RELEASED" | "DEBT" | "MODERATION_BLOCKED" | "UNKNOWN_RESOLVED",
      customer_atoms: String(r.customer_atoms),
      funded_atoms: String(r.funded_atoms),
      debt_atoms: String(r.debt_atoms),
      supplier_cost_atoms: String(r.supplier_cost_atoms),
    });
  }

  const keyIdsWithTracking = [
    ...new Set(approvedHolds.filter((h) => h.api_key_id !== null).map((h) => h.api_key_id!)),
  ].sort();
  const keySpendsById = new Map<string, KeySpendRow>();
  if (keyIdsWithTracking.length > 0) {
    const lockedKeys = await tx`
      SELECT id, billing_account_id, spend_limit_atoms::text, spend_limit_window,
        spend_window_start, spent_atoms::text, reserved_atoms::text,
        floor(extract(epoch from now()) * 1000)::bigint AS pg_now_ms
      FROM public.api_keys
      WHERE id = ANY(${keyIdsWithTracking}::uuid[])
        AND (spend_limit_atoms IS NOT NULL OR reserved_atoms > 0)
      ORDER BY id
      FOR NO KEY UPDATE
    `;
    for (const r of lockedKeys) {
      const row = toKeySpendRow(r);
      const pgNowMs = r.pg_now_ms !== undefined ? Number(r.pg_now_ms) : nowMs;
      rolloverKeySpendInMemory(row, pgNowMs);
      keySpendsById.set(row.id, row);
    }
  }

  const walletIds = [...new Set(approvedHolds.map((h) => h.wallet_id))].sort();
  const lockedWallets = walletIds.length
    ? await tx`
            SELECT id, billing_account_id, currency, policy,
              available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
              debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
              credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
            FROM public.wallets
            WHERE id = ANY(${walletIds}::uuid[])
            ORDER BY id
            FOR NO KEY UPDATE
          `
    : [];
  const walletsById = new Map<string, WalletRow>();
  for (const r of lockedWallets) {
    const w = toWalletRow(r);
    walletsById.set(w.id, w);
  }

  const lotRowsRaw = walletIds.length
    ? await tx`
            SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
              issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
            FROM public.wallet_lots
            WHERE wallet_id = ANY(${walletIds}::uuid[]) AND (available_atoms > 0 OR reserved_atoms > 0)
            ORDER BY id
            FOR NO KEY UPDATE
          `
    : [];
  const lotsById = new Map<string, LotRow>();
  const lotsByWallet = new Map<string, LotRow[]>();
  for (const r of lotRowsRaw) {
    const l = toLotRow(r);
    lotsById.set(l.id, l);
    const list = lotsByWallet.get(l.wallet_id);
    if (list) list.push(l);
    else lotsByWallet.set(l.wallet_id, [l]);
  }
  for (const [wid, arr] of lotsByWallet) {
    const w = walletsById.get(wid)!;
    lotsByWallet.set(wid, sortLots(arr, w.policy));
  }

  const neededCellMap = new Map<string, { account_id: string; currency: string; cell: number }>();
  for (const h of approvedHolds) {
    for (const s of h.supplier_holds) {
      const k = cellKey(s.account_id, s.currency, s.cell);
      if (!neededCellMap.has(k)) {
        neededCellMap.set(k, {
          account_id: s.account_id,
          currency: s.currency,
          cell: s.cell,
        });
      }
    }
  }
  const neededCells = [...neededCellMap.values()].sort((a, b) =>
    a.account_id !== b.account_id
      ? a.account_id.localeCompare(b.account_id)
      : a.currency !== b.currency
        ? a.currency.localeCompare(b.currency)
        : a.cell - b.cell,
  );
  const cellRowsRaw = neededCells.length
    ? await tx`
            SELECT c.account_id, c.currency, c.cell, c.balance_atoms::text, c.reserved_atoms::text, c.spent_atoms::text, c.revision::text
            FROM public.supplier_cells c
            JOIN unnest(
              ${neededCells.map((x) => x.account_id)}::uuid[],
              ${neededCells.map((x) => x.currency)}::text[],
              ${neededCells.map((x) => x.cell)}::smallint[]
            ) AS k(account_id, currency, cell)
              ON c.account_id = k.account_id AND c.currency = k.currency AND c.cell = k.cell
            ORDER BY c.account_id, c.currency, c.cell
            FOR NO KEY UPDATE
          `
    : [];
  const cellsByKey = new Map<string, SupplierCellRow>();
  for (const r of cellRowsRaw) {
    const c = toCellRow(r);
    cellsByKey.set(cellKey(c.account_id, c.currency, c.cell), c);
  }

  return {
    holdsByReq,
    selectedAttempts,
    existingCharges,
    keySpendsById,
    walletsById,
    lotsById,
    lotsByWallet,
    cellsByKey,
  };
}
