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
  type WalletRow,
} from "./funds.ts";

export type PendingLeaseSpend = {
  id: string;
  spent_lot_id: string;
  spent_atoms: bigint;
};

export async function lockActiveWalletLeaseSpendTx(
  tx: SQL,
  walletId: string,
): Promise<PendingLeaseSpend[]> {
  const rows = await tx<{ id: string; spent_lot_id: string | null; spent_atoms: string }[]>`
    SELECT id::text AS id, spent_lot_id::text AS spent_lot_id, spent_atoms::text AS spent_atoms
    FROM public.wallet_leases
    WHERE wallet_id = ${walletId}::uuid AND state = 'ACTIVE'
    ORDER BY id
    FOR NO KEY UPDATE
  `;
  const pending: PendingLeaseSpend[] = [];
  for (const r of rows) {
    const spent = BigInt(r.spent_atoms);
    if (spent <= 0n) continue;
    if (!r.spent_lot_id) {
      throw new Error(`Wallet lease ${r.id} has spent_atoms without spent_lot_id`);
    }
    pending.push({
      id: r.id,
      spent_lot_id: r.spent_lot_id,
      spent_atoms: spent,
    });
  }
  return pending;
}

export async function flushActiveWalletLeaseSpendTx(
  tx: SQL,
  wallet: WalletRow,
  leases: readonly PendingLeaseSpend[],
): Promise<void> {
  if (leases.length === 0) return;
  const spentByLot = new Map<string, bigint>();
  let totalSpent = 0n;
  for (const l of leases) {
    totalSpent += l.spent_atoms;
    spentByLot.set(l.spent_lot_id, (spentByLot.get(l.spent_lot_id) ?? 0n) + l.spent_atoms);
  }
  const lotIds = [...spentByLot.keys()].sort();
  const lotSpents = lotIds.map((id) => spentByLot.get(id)!.toString());
  const leaseIds = leases.map((l) => l.id);

  await tx`
    SELECT id FROM public.wallet_lots
    WHERE id = ANY(${lotIds}::uuid[])
    ORDER BY id
    FOR NO KEY UPDATE
  `;
  await tx`
    UPDATE public.wallet_lots AS l
    SET reserved_atoms = l.reserved_atoms - u.spent,
        spent_atoms = l.spent_atoms + u.spent
    FROM unnest(${lotIds}::uuid[], ${lotSpents}::numeric[]) AS u(id, spent)
    WHERE l.id = u.id
  `;
  wallet.reserved_atoms -= totalSpent;
  wallet.spent_atoms += totalSpent;
  await tx`
    UPDATE public.wallets
    SET reserved_atoms = ${wallet.reserved_atoms.toString()}::numeric,
        spent_atoms = ${wallet.spent_atoms.toString()}::numeric
    WHERE id = ${wallet.id}::uuid
  `;
  await tx`
    UPDATE public.wallet_leases
    SET spent_atoms = 0,
        spent_lot_id = NULL,
        updated_at = now()
    WHERE id = ANY(${leaseIds}::uuid[])
  `;
}

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

export async function closeIdleWalletLeaseBatch(
  db: SQL,
  nodeId: string,
  leaseIds: string[],
): Promise<string[]> {
  return db.begin(async (tx) => {
    const locked = await tx<{ id: string }[]>`
      SELECT id FROM public.wallet_leases
      WHERE id = ANY(${leaseIds}::uuid[]) AND node_id = ${nodeId} AND state = 'ACTIVE'
        AND earliest_lot_expires_at IS NULL
        AND remaining_atoms >= 0
        AND (remaining_atoms > 0 OR spent_atoms > 0)
        AND (
          (remaining_atoms > 0 AND jsonb_array_length(lot_allocations) = 1
            AND (spent_atoms = 0 OR spent_lot_id = (lot_allocations->0->>'lot_id')::uuid))
          OR
          (remaining_atoms = 0 AND jsonb_array_length(lot_allocations) = 0 AND spent_lot_id IS NOT NULL)
        )
      ORDER BY id FOR UPDATE SKIP LOCKED
    `;
    if (locked.length === 0) return [];
    const closed = await tx<{ id: string }[]>`
      WITH open_wl AS MATERIALIZED (
        SELECT DISTINCT wallet_lease_id
        FROM public.billing_holds
        WHERE state IN ('HELD', 'UNKNOWN') AND wallet_lease_id IS NOT NULL
      ), idle AS MATERIALIZED (
        SELECT DISTINCT ON (l.wallet_id)
          l.id,
          l.wallet_id,
          l.remaining_atoms,
          l.spent_atoms,
          (l.remaining_atoms + l.spent_atoms) AS total_reserved,
          CASE WHEN l.spent_atoms > 0 THEN l.spent_lot_id ELSE (l.lot_allocations->0->>'lot_id')::uuid END AS lot_id
        FROM public.wallet_leases l
        LEFT JOIN open_wl o ON o.wallet_lease_id = l.id
        WHERE l.id = ANY(${locked.map(({ id }) => id)}::uuid[])
          AND (
            (l.remaining_atoms = 0 AND jsonb_array_length(l.lot_allocations) = 0)
            OR (l.remaining_atoms > 0 AND (l.lot_allocations->0->>'amount_atoms')::numeric = l.remaining_atoms)
          )
          AND o.wallet_lease_id IS NULL
        ORDER BY l.wallet_id, l.id
      ), wallets AS MATERIALIZED (
        SELECT i.id, i.wallet_id, i.lot_id, i.remaining_atoms, i.spent_atoms, i.total_reserved
        FROM idle i JOIN public.wallets w ON w.id = i.wallet_id
        WHERE w.reserved_atoms >= i.total_reserved
          AND w.freeze_reason IS NULL AND NOT w.manual_frozen AND NOT w.evidence_disputed
          AND w.debt_atoms + w.credit_reserved_atoms <= w.credit_limit_atoms
        ORDER BY w.id FOR NO KEY UPDATE OF w SKIP LOCKED
      ), releasable AS MATERIALIZED (
        SELECT w.id, w.wallet_id, w.lot_id, w.remaining_atoms, w.spent_atoms, w.total_reserved
        FROM wallets w JOIN public.wallet_lots l ON l.id = w.lot_id AND l.wallet_id = w.wallet_id
        WHERE (w.remaining_atoms = 0 OR l.expires_at IS NULL)
          AND l.reserved_atoms >= w.total_reserved
        ORDER BY l.id FOR NO KEY UPDATE OF l SKIP LOCKED
      ), credit_lots AS (
        UPDATE public.wallet_lots l
        SET available_atoms = l.available_atoms + r.remaining_atoms,
            reserved_atoms = l.reserved_atoms - r.total_reserved,
            spent_atoms = l.spent_atoms + r.spent_atoms
        FROM releasable r WHERE l.id = r.lot_id
      ), credit_wallets AS (
        UPDATE public.wallets w
        SET available_atoms = w.available_atoms + r.remaining_atoms,
            reserved_atoms = w.reserved_atoms - r.total_reserved,
            spent_atoms = w.spent_atoms + r.spent_atoms
        FROM releasable r WHERE w.id = r.wallet_id
      )
      UPDATE public.wallet_leases l
      SET state = 'CLOSED',
          remaining_atoms = 0,
          spent_atoms = 0,
          spent_lot_id = NULL,
          lot_allocations = '[]'::jsonb,
          updated_at = now()
      FROM releasable r WHERE l.id = r.id
      RETURNING l.id
    `;
    return closed.map(({ id }) => id);
  });
}

export async function closeWalletLease(
  db: SQL,
  leaseId: string,
  expiredOnly: boolean,
): Promise<boolean | null> {
  return db
    .begin(async (tx) => {
      const wlRows = await tx`
          SELECT id, wallet_id, currency, lot_allocations, remaining_atoms::text,
            spent_atoms::text, spent_lot_id, state
          FROM public.wallet_leases
          WHERE id = ${leaseId}
            AND state = 'ACTIVE'
            AND (NOT ${expiredOnly}::boolean OR expires_at <= clock_timestamp())
          FOR UPDATE
        `;
      if (!wlRows.length) return false;
      const wl = wlRows[0]!;
      const candidateHoldRows = await tx<{ request_id: string }[]>`
          SELECT request_id::text AS request_id
          FROM public.billing_holds
          WHERE state IN ('HELD', 'UNKNOWN') AND wallet_lease_id = ${leaseId}::uuid
          ORDER BY request_id
        `;
      const candidateIds = candidateHoldRows.map((r) => r.request_id);
      const openHolds = candidateIds.length
        ? await tx<{ request_id: string; held_atoms: string }[]>`
            SELECT request_id, held_atoms::text
            FROM public.billing_holds
            WHERE request_id = ANY(${candidateIds}::uuid[])
              AND wallet_lease_id = ${leaseId}::uuid
              AND state IN ('HELD', 'UNKNOWN')
            ORDER BY request_id
            FOR NO KEY UPDATE NOWAIT
          `
        : [];
      let leaseLots = (wl.lot_allocations ?? []) as LeaseLotAllocation[];
      let remainingAtoms = BigInt(String(wl.remaining_atoms));
      const spentAtoms = BigInt(String(wl.spent_atoms ?? "0"));
      const spentLotId = wl.spent_lot_id ? String(wl.spent_lot_id) : null;

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
      const lotIdSet = new Set(leaseLots.map((l) => l.lot_id));
      if (spentAtoms > 0n && spentLotId) lotIdSet.add(spentLotId);
      const lotIds = [...lotIdSet].sort();
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

      if (spentAtoms > 0n) {
        if (!spentLotId) throw new Error(`Wallet lease ${leaseId} missing spent_lot_id`);
        const spentLot = lotsById.get(spentLotId);
        if (!spentLot) throw new Error(`Wallet lease ${leaseId} missing spent lot ${spentLotId}`);
        spentLot.reserved_atoms -= spentAtoms;
        spentLot.spent_atoms += spentAtoms;
        spentLot.dirty = true;
      }

      if (remainingAtoms > 0n || spentAtoms > 0n) {
        wallet.reserved_atoms -= remainingAtoms + spentAtoms;
        wallet.available_atoms += returnedAvail;
        wallet.expired_atoms += expiredAmt;
        wallet.spent_atoms += spentAtoms;
        wallet.freeze_reason = computeFreezeReason(wallet);
        wallet.dirty = true;
      }

      await flushDirtyWallets(tx, [wallet]);
      await flushDirtyLots(tx, lotsById.values());
      await insertJournalLines(tx, journals);
      await tx`
          UPDATE public.wallet_leases
          SET state = 'CLOSED',
              remaining_atoms = 0,
              spent_atoms = 0,
              spent_lot_id = NULL,
              lot_allocations = '[]'::jsonb,
              updated_at = now()
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
          SELECT id, account_id, currency, cell, remaining_atoms::text, spent_atoms::text, state
          FROM public.supplier_leases
          WHERE id = ${leaseId}
            AND state = 'ACTIVE'
            AND (NOT ${expiredOnly}::boolean OR expires_at <= clock_timestamp())
          FOR UPDATE
        `;
      if (!slRows.length) return false;
      const sl = slRows[0]!;
      const candidateHoldRows = await tx<{ request_id: string }[]>`
          SELECT request_id::text AS request_id
          FROM public.billing_holds
          WHERE state IN ('HELD', 'UNKNOWN') AND supplier_lease_id = ${leaseId}::uuid
          ORDER BY request_id
        `;
      const candidateIds = candidateHoldRows.map((r) => r.request_id);
      const openHolds = candidateIds.length
        ? await tx<{ request_id: string; supplier_holds: SupplierHoldEntry[] }[]>`
            SELECT request_id, supplier_holds
            FROM public.billing_holds
            WHERE request_id = ANY(${candidateIds}::uuid[])
              AND supplier_lease_id = ${leaseId}::uuid
              AND state IN ('HELD', 'UNKNOWN')
            ORDER BY request_id
            FOR NO KEY UPDATE NOWAIT
          `
        : [];
      let remainingAtoms = BigInt(String(sl.remaining_atoms));
      const spentAtoms = BigInt(String(sl.spent_atoms ?? "0"));
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
      const totalReserved = remainingAtoms + spentAtoms;
      if (totalReserved > 0n) {
        await tx`
            UPDATE public.supplier_cells
            SET reserved_atoms = reserved_atoms - ${totalReserved.toString()}::numeric,
                balance_atoms = balance_atoms - ${spentAtoms.toString()}::numeric,
                spent_atoms = spent_atoms + ${spentAtoms.toString()}::numeric,
                revision = revision + 1
            WHERE account_id = ${sl.account_id} AND currency = ${sl.currency} AND cell = ${Number(sl.cell)}
          `;
      }
      await tx`
          UPDATE public.supplier_leases
          SET state = 'CLOSED', remaining_atoms = 0, spent_atoms = 0, updated_at = now()
          WHERE id = ${leaseId}
        `;
      return true;
    })
    .catch((error: unknown) => {
      if (postgresState(error) === "55P03") return null;
      throw error;
    });
}
