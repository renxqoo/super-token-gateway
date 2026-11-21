import type { SQL } from "bun";
import type { HoldLotAllocation, LeaseLotAllocation } from "./types.ts";

export type ActiveWalletLease = {
  leaseId: string;
  walletId: string;
  projectId: string;
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
  projectId: string;
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
): Promise<{
  lot_allocations: HoldLotAllocation[];
  detachedWalletLeaseId: string | null;
  detachedSupplierLeaseId: string | null;
  detachedSupplierAtoms: bigint;
}> {
  let lotAllocations = hold.lot_allocations;
  let detachedWalletLeaseId: string | null = null;
  let detachedSupplierLeaseId: string | null = null;
  let detachedSupplierAtoms = 0n;

  if (hold.wallet_lease_id) {
    const wlRows = await tx`
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

  if (hold.supplier_lease_id) {
    const slRows = await tx`
      SELECT id, remaining_atoms::text, state
      FROM public.supplier_leases
      WHERE id = ${hold.supplier_lease_id}
      FOR NO KEY UPDATE
    `;
    if (slRows.length > 0 && slRows[0]!.state === "ACTIVE") {
      const supAtoms = hold.supplier_holds.reduce((sum, s) => sum + BigInt(s.held_atoms), 0n);
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

  if (detachedWalletLeaseId || detachedSupplierLeaseId) {
    await tx`
      UPDATE public.billing_holds
      SET lot_allocations = ${lotAllocations}::jsonb,
          wallet_lease_id = NULL,
          supplier_lease_id = NULL
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
