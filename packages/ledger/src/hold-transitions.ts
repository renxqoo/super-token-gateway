import type { SQL } from "bun";
import { detachHoldFromLeaseTx } from "./leases.ts";
import type {
  HoldLotAllocation,
  SupplierHoldEntry,
  SwitchSupplierInput,
  SwitchSupplierResult,
} from "./types.ts";
import type { LeaseManager } from "./lease-manager.ts";
import { releaseKeyReservationInTx } from "./key-spend.ts";
import {
  isTransientPgConcurrencyError,
  parseAtoms,
  withPgConcurrencyRetry,
  computeFreezeReason,
  cellKey,
  toWalletRow,
  toLotRow,
  insertJournalLines,
  flushDirtyWallets,
  flushDirtyLots,
  flushDirtyCells,
  type LotRow,
  type SupplierCellRow,
  type JournalLine,
} from "./funds.ts";
import { createSupplierSwitchQueues } from "./supplier-switch-queues.ts";
import { createSupplierSwitch } from "./supplier-switch.ts";
import { lockExactCells } from "./supplier-cell-locks.ts";

type HoldRelease = {
  released: boolean;
  detachment?: {
    wlId: string | null;
    heldCust: bigint;
    slId: string | null;
    supAtoms: bigint;
  };
};

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

export type HoldTransitionLeaseHooks = Pick<
  LeaseManager,
  "notifyDetachedLocal" | "notifySupplierDetachedLocal" | "rebindHoldAttempt"
>;

export function createHoldTransitions(
  db: SQL,
  leases: HoldTransitionLeaseHooks,
  batchWindowMs = 2,
  maxBatchSize = 256,
) {
  const { executeSingleSwitch } = createSupplierSwitch({ db, leases });

  const { fastSwitchQueue, unknownSwitchQueue } = createSupplierSwitchQueues({
    db,
    leases,
    executeSingleSwitch,
    maxBatchSize,
    batchWindowMs,
  });

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
