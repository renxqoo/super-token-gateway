import type { SQL } from "bun";
import type { HoldLotAllocation, LeaseLotAllocation, SupplierHoldEntry } from "./types.ts";
import {
  computeFreezeReason,
  flushDirtyLots,
  flushDirtyWallets,
  insertJournalLines,
  postgresState,
  toLotRow,
  toWalletRow,
  type JournalLine,
  type LotRow,
} from "./funds.ts";

export type ActiveWalletLease = {
  leaseId: string;
  walletId: string;
  billingAccountId: string;
  currency: string;
  remainingInPg: bigint;
  freeInMemory: bigint;
  earliestLotExpiresAtMs: number | null;
  expiresAtMs: number;
  inFlightHolds: number;
  closing: boolean;
  lastUsedAtMs: number;
};

export type ActiveSupplierLease = {
  lane: number;
  leaseId: string;
  accountId: string;
  currency: string;
  cell: number;
  remainingInPg: bigint;
  freeInMemory: bigint;
  expiresAtMs: number;
  inFlightHolds: number;
  closing: boolean;
  lastUsedAtMs: number;
};

export type LeasedHoldMeta = {
  requestId: string;
  walletLeaseId: string;
  supplierLeaseId: string;
  walletId: string;
  billingAccountId: string;
  apiKeyId: string | null;
  pricingOverrideId: string | null;
  discountNumerator: string | null;
  discountDenominator: string | null;
  currency: string;
  operation: string;
  model: string;
  heldCustomerAtoms: bigint;
  heldSupplierAtoms: bigint;
  attemptId: string;
  accountId: string;
  channelId: string;
  supplierCurrency: string;
  cell: number;
};

export function carveFromLeaseLots(
  leaseLots: LeaseLotAllocation[],
  amount: bigint,
): { carved: HoldLotAllocation[]; remaining: LeaseLotAllocation[]; taken: bigint } {
  let rem = amount;
  const carved: HoldLotAllocation[] = [];
  const remaining: LeaseLotAllocation[] = [];
  for (const entry of leaseLots) {
    const avail = BigInt(entry.amount_atoms);
    if (avail <= 0n) continue;
    if (rem > 0n) {
      const take = rem < avail ? rem : avail;
      rem -= take;
      if (take > 0n) {
        carved.push({ lot_id: entry.lot_id, amount_atoms: take.toString() });
      }
      const left = avail - take;
      if (left > 0n) {
        remaining.push({
          lot_id: entry.lot_id,
          amount_atoms: left.toString(),
          expires_at_ms: entry.expires_at_ms,
        });
      }
    } else {
      remaining.push({
        lot_id: entry.lot_id,
        amount_atoms: avail.toString(),
        expires_at_ms: entry.expires_at_ms,
      });
    }
  }
  return { carved, remaining, taken: amount - rem };
}

export function earliestExpiryOf(lots: readonly LeaseLotAllocation[]): number | null {
  let min: number | null = null;
  for (const l of lots) {
    if (BigInt(l.amount_atoms) <= 0n) continue;
    if (l.expires_at_ms !== null && (min === null || l.expires_at_ms < min)) {
      min = l.expires_at_ms;
    }
  }
  return min;
}

export async function detachHoldFromLeaseTx(
  tx: SQL,
  hold: {
    request_id: string;
    held_atoms: bigint;
    wallet_lease_id: string | null;
    supplier_lease_id: string | null;
    lot_allocations: HoldLotAllocation[];
    supplier_holds: { held_atoms: string; state: string }[];
  },
  options: { detachWallet?: boolean; detachSupplier?: boolean; nowait?: boolean } = {},
): Promise<{
  lot_allocations: HoldLotAllocation[];
  detachedWalletLeaseId: string | null;
  detachedSupplierLeaseId: string | null;
  detachedSupplierAtoms: bigint;
}> {
  const detachWallet = options.detachWallet ?? true;
  const detachSupplier = options.detachSupplier ?? true;
  const nowait = options.nowait ?? false;
  let lotAllocations = hold.lot_allocations;
  let detachedWalletLeaseId: string | null = null;
  let detachedSupplierLeaseId: string | null = null;
  let detachedSupplierAtoms = 0n;

  if (detachWallet && hold.wallet_lease_id) {
    const wlRows = nowait
      ? await tx`
          SELECT id, lot_allocations, remaining_atoms::text, state
          FROM public.wallet_leases
          WHERE id = ${hold.wallet_lease_id}
          FOR NO KEY UPDATE NOWAIT
        `
      : await tx`
          SELECT id, lot_allocations, remaining_atoms::text, state
          FROM public.wallet_leases
          WHERE id = ${hold.wallet_lease_id}
          FOR NO KEY UPDATE
        `;
    if (wlRows.length > 0 && wlRows[0]!.state === "ACTIVE") {
      const leaseLots = (wlRows[0]!.lot_allocations ?? []) as LeaseLotAllocation[];
      const leaseRem = BigInt(String(wlRows[0]!.remaining_atoms));
      const { carved, remaining, taken } = carveFromLeaseLots(leaseLots, hold.held_atoms);
      if (taken !== hold.held_atoms) {
        throw new Error(`Wallet lease ${hold.wallet_lease_id} lacks atoms to detach hold`);
      }
      const nextRem = leaseRem - taken;
      const nextEarliest = earliestExpiryOf(remaining);
      await tx`
        UPDATE public.wallet_leases
        SET lot_allocations = ${remaining}::jsonb,
            remaining_atoms = ${nextRem.toString()}::numeric,
            earliest_lot_expires_at = ${nextEarliest === null ? null : new Date(nextEarliest).toISOString()}::timestamptz,
            updated_at = now()
        WHERE id = ${hold.wallet_lease_id}
      `;
      lotAllocations = carved;
      detachedWalletLeaseId = hold.wallet_lease_id;
    }
  }

  if (detachSupplier && hold.supplier_lease_id) {
    const slRows = nowait
      ? await tx`
          SELECT id, remaining_atoms::text, state
          FROM public.supplier_leases
          WHERE id = ${hold.supplier_lease_id}
          FOR NO KEY UPDATE NOWAIT
        `
      : await tx`
          SELECT id, remaining_atoms::text, state
          FROM public.supplier_leases
          WHERE id = ${hold.supplier_lease_id}
          FOR NO KEY UPDATE
        `;
    if (slRows.length > 0 && slRows[0]!.state === "ACTIVE") {
      const supAtoms = hold.supplier_holds.reduce(
        (sum, s) =>
          s.state === "HELD" || s.state === "UNKNOWN" ? sum + BigInt(s.held_atoms) : sum,
        0n,
      );
      const slRem = BigInt(String(slRows[0]!.remaining_atoms));
      const nextRem = slRem >= supAtoms ? slRem - supAtoms : 0n;
      await tx`
        UPDATE public.supplier_leases
        SET remaining_atoms = ${nextRem.toString()}::numeric,
            updated_at = now()
        WHERE id = ${hold.supplier_lease_id}
      `;
      detachedSupplierLeaseId = hold.supplier_lease_id;
      detachedSupplierAtoms = supAtoms;
    }
  }

  if (detachedWalletLeaseId && detachedSupplierLeaseId) {
    await tx`
      UPDATE public.billing_holds
      SET lot_allocations = ${lotAllocations}::jsonb,
          wallet_lease_id = NULL,
          supplier_lease_id = NULL
      WHERE request_id = ${hold.request_id}
    `;
  } else if (detachedWalletLeaseId) {
    await tx`
      UPDATE public.billing_holds
      SET lot_allocations = ${lotAllocations}::jsonb,
          wallet_lease_id = NULL
      WHERE request_id = ${hold.request_id}
    `;
  } else if (detachedSupplierLeaseId) {
    await tx`
      UPDATE public.billing_holds
      SET supplier_lease_id = NULL
      WHERE request_id = ${hold.request_id}
    `;
  }

  return {
    lot_allocations: lotAllocations,
    detachedWalletLeaseId,
    detachedSupplierLeaseId,
    detachedSupplierAtoms,
  };
}

export async function closeWalletLease(
  db: SQL,
  leaseId: string,
  expiredOnly: boolean,
): Promise<boolean | null> {
  return db
    .begin(async (tx) => {
      const wlRows = await tx`
          SELECT id, wallet_id, currency, lot_allocations, remaining_atoms::text, state
          FROM public.wallet_leases
          WHERE id = ${leaseId}
            AND state = 'ACTIVE'
            AND (NOT ${expiredOnly}::boolean OR expires_at <= clock_timestamp())
          FOR UPDATE
        `;
      if (!wlRows.length) return false;
      const wl = wlRows[0]!;
      const openHolds = await tx`
          SELECT request_id, held_atoms::text
          FROM public.billing_holds
          WHERE wallet_lease_id = ${leaseId} AND state IN ('HELD', 'UNKNOWN')
          ORDER BY request_id
          FOR NO KEY UPDATE NOWAIT
        `;
      let leaseLots = (wl.lot_allocations ?? []) as LeaseLotAllocation[];
      let remainingAtoms = BigInt(String(wl.remaining_atoms));

      for (const oh of openHolds) {
        const amt = BigInt(String(oh.held_atoms));
        const { carved, remaining, taken } = carveFromLeaseLots(leaseLots, amt);
        leaseLots = remaining;
        remainingAtoms -= taken;
        await tx`
            UPDATE public.billing_holds
            SET lot_allocations = ${carved}::jsonb, wallet_lease_id = NULL
            WHERE request_id = ${oh.request_id}
          `;
      }

      const nowMs = Date.now();
      const wRows = await tx`
          SELECT id, billing_account_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets WHERE id = ${wl.wallet_id} FOR NO KEY UPDATE
        `;
      const wallet = toWalletRow(wRows[0]!);
      const lotIds = [...new Set(leaseLots.map((l) => l.lot_id))].sort();
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
      let expiredAmt = 0n;
      for (const entry of leaseLots) {
        const amt = BigInt(entry.amount_atoms);
        if (amt <= 0n) continue;
        const lot = lotsById.get(entry.lot_id);
        if (!lot) continue;
        lot.reserved_atoms -= amt;
        if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs) {
          lot.expired_atoms += amt;
          expiredAmt += amt;
          const jid = crypto.randomUUID();
          journals.push(
            {
              journal_id: jid,
              ordinal: 0,
              side: "CUSTOMER",
              entity_id: wallet.id,
              request_id: null,
              reference: `lease_expire:${leaseId}:${lot.id}`,
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
              request_id: null,
              reference: `lease_expire:${leaseId}:${lot.id}`,
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

      if (remainingAtoms > 0n) {
        wallet.reserved_atoms -= remainingAtoms;
        wallet.available_atoms += returnedAvail;
        wallet.expired_atoms += expiredAmt;
        wallet.freeze_reason = computeFreezeReason(wallet);
        wallet.dirty = true;
      }

      await flushDirtyWallets(tx, [wallet]);
      await flushDirtyLots(tx, lotsById.values());
      await insertJournalLines(tx, journals);
      await tx`
          UPDATE public.wallet_leases
          SET state = 'CLOSED', remaining_atoms = 0, lot_allocations = '[]'::jsonb, updated_at = now()
          WHERE id = ${leaseId}
        `;
      return true;
    })
    .catch((error: unknown) => {
      if (postgresState(error) === "55P03") return null;
      throw error;
    });
}

export async function closeSupplierLease(
  db: SQL,
  leaseId: string,
  expiredOnly: boolean,
): Promise<boolean | null> {
  return db
    .begin(async (tx) => {
      const slRows = await tx`
          SELECT id, account_id, currency, cell, remaining_atoms::text, state
          FROM public.supplier_leases
          WHERE id = ${leaseId}
            AND state = 'ACTIVE'
            AND (NOT ${expiredOnly}::boolean OR expires_at <= clock_timestamp())
          FOR UPDATE
        `;
      if (!slRows.length) return false;
      const sl = slRows[0]!;
      const openHolds = await tx`
          SELECT request_id, supplier_holds
          FROM public.billing_holds
          WHERE supplier_lease_id = ${leaseId} AND state IN ('HELD', 'UNKNOWN')
          ORDER BY request_id
          FOR NO KEY UPDATE NOWAIT
        `;
      let remainingAtoms = BigInt(String(sl.remaining_atoms));
      for (const oh of openHolds) {
        const holds = (oh.supplier_holds ?? []) as SupplierHoldEntry[];
        const heldSum = holds.reduce(
          (acc, s) =>
            s.state === "HELD" || s.state === "UNKNOWN" ? acc + BigInt(s.held_atoms) : acc,
          0n,
        );
        remainingAtoms = remainingAtoms >= heldSum ? remainingAtoms - heldSum : 0n;
        await tx`
            UPDATE public.billing_holds SET supplier_lease_id = NULL WHERE request_id = ${oh.request_id}
          `;
      }
      if (remainingAtoms > 0n) {
        await tx`
            UPDATE public.supplier_cells
            SET reserved_atoms = reserved_atoms - ${remainingAtoms.toString()}::numeric,
                revision = revision + 1
            WHERE account_id = ${sl.account_id} AND currency = ${sl.currency} AND cell = ${Number(sl.cell)}
          `;
      }
      await tx`
          UPDATE public.supplier_leases
          SET state = 'CLOSED', remaining_atoms = 0, updated_at = now()
          WHERE id = ${leaseId}
        `;
      return true;
    })
    .catch((error: unknown) => {
      if (postgresState(error) === "55P03") return null;
      throw error;
    });
}
