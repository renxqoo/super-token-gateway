import type { SQL } from "bun";
import type { AcquireHoldInput, AcquireHoldResult } from "./types.ts";
import type { LeaseManager, FastHoldRequest } from "./lease-manager.ts";
import { MicroBatchQueue } from "./micro-batch.ts";
import { createTransactionalHoldBatch } from "./transactional-holds.ts";
import { parseAtoms, SETTLEMENT_LANES, walletLane } from "./funds.ts";

export function createHoldAcquirer(
  db: SQL,
  leases: LeaseManager,
  batchWindowMs: number,
  maxBatchSize: number,
  _maxBatchConcurrency: number,
) {
  async function runFastHoldBatch(items: FastHoldRequest[]) {
    const batchPayload = JSON.stringify(
      items.map((x) => ({
        request_id: x.input.request_id,
        project_id: x.input.project_id,
        wallet_id: x.walletLease.walletId,
        operation: x.input.operation,
        model: x.input.model,
        fingerprint: x.input.fingerprint,
        held_atoms: x.customerAtoms.toString(),
        supplier_holds: [
          {
            attempt_id: x.input.attempt_id,
            ...(x.input.supplier_cost_version_id
              ? { cost_version_id: x.input.supplier_cost_version_id }
              : {}),
            account_id: x.input.supplier_account_id,
            channel_id: x.input.supplier_channel_id,
            currency: x.input.supplier_currency,
            cell: x.supplierLease.cell,
            held_atoms: x.supplierAtoms.toString(),
            state: "HELD",
          },
        ],
        wallet_lease_id: x.walletLease.leaseId,
        supplier_lease_id: x.supplierLease.leaseId,
        expires_at: x.input.expires_at,
      })),
    );

    const retireLeases = (item: FastHoldRequest) => {
      if (item.walletLease.inFlightHolds > 0) item.walletLease.inFlightHolds--;
      if (item.supplierLease.inFlightHolds > 0) item.supplierLease.inFlightHolds--;
      leases.retireWalletLease(item.walletLease);
      leases.retireSupplierLease(item.supplierLease);
    };
    let admitted: Set<string>;
    try {
      const rows = await db<{ request_id: string }[]>`
        WITH batch AS MATERIALIZED (
          SELECT * FROM jsonb_to_recordset(${batchPayload}::text::jsonb) AS u(
            request_id uuid,
            project_id uuid,
            wallet_id uuid,
            operation text,
            model text,
            fingerprint text,
            held_atoms numeric,
            supplier_holds jsonb,
            wallet_lease_id uuid,
            supplier_lease_id uuid,
            expires_at timestamptz
          )
        ), active_wallet_leases AS MATERIALIZED (
          SELECT l.id
          FROM public.wallet_leases l
          JOIN public.wallets w ON w.id = l.wallet_id
          WHERE l.id = ANY(ARRAY(SELECT DISTINCT wallet_lease_id FROM batch))
            AND l.node_id = ${leases.nodeId}
            AND l.state = 'ACTIVE' AND l.expires_at > clock_timestamp()
            AND (l.earliest_lot_expires_at IS NULL OR l.earliest_lot_expires_at > clock_timestamp())
            AND w.freeze_reason IS NULL
          ORDER BY l.id FOR KEY SHARE OF l
        ), active_supplier_leases AS MATERIALIZED (
          SELECT l.id
          FROM public.supplier_leases l
          JOIN public.upstream_accounts a ON a.id = l.account_id
          WHERE (SELECT count(*) FROM active_wallet_leases) > 0
            AND l.id = ANY(ARRAY(SELECT DISTINCT supplier_lease_id FROM batch))
            AND l.node_id = ${leases.nodeId}
            AND l.state = 'ACTIVE' AND l.expires_at > clock_timestamp() AND a.state = 'ACTIVE'
          ORDER BY l.id FOR KEY SHARE OF l
        )
        INSERT INTO public.billing_holds (
          request_id, project_id, wallet_id, operation, model, idempotency_key, fingerprint,
          held_atoms, credit_atoms, lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state, expires_at
        )
        SELECT
          u.request_id, u.project_id, u.wallet_id, u.operation, u.model, NULL, u.fingerprint,
          u.held_atoms, 0, '[]'::jsonb, u.supplier_holds, u.wallet_lease_id, u.supplier_lease_id, 'HELD', u.expires_at
        FROM batch u
        JOIN active_wallet_leases w ON w.id = u.wallet_lease_id
        JOIN active_supplier_leases s ON s.id = u.supplier_lease_id
        RETURNING request_id
      `;
      admitted = new Set(rows.map((row) => String(row.request_id)));
    } catch (err) {
      for (const item of items) retireLeases(item);
      throw err;
    }

    const out: (AcquireHoldResult | Promise<AcquireHoldResult>)[] = [];
    for (const x of items) {
      if (!admitted.has(x.input.request_id)) {
        retireLeases(x);
        out.push(acquireQueue.enqueue(x.input));
        continue;
      }
      leases.trackHold({
        requestId: x.input.request_id,
        walletLeaseId: x.walletLease.leaseId,
        supplierLeaseId: x.supplierLease.leaseId,
        walletId: x.walletLease.walletId,
        projectId: x.input.project_id,
        currency: x.input.currency,
        operation: x.input.operation,
        model: x.input.model,
        heldCustomerAtoms: x.customerAtoms,
        heldSupplierAtoms: x.supplierAtoms,
        attemptId: x.input.attempt_id,
        accountId: x.input.supplier_account_id,
        channelId: x.input.supplier_channel_id,
        supplierCurrency: x.input.supplier_currency,
        cell: x.supplierLease.cell,
      });
      out.push({
        ok: true,
        replayed: false,
        request_id: x.input.request_id,
        wallet_id: x.walletLease.walletId,
        held_atoms: x.customerAtoms.toString(),
        credit_atoms: "0",
        supplier_cell: x.supplierLease.cell,
      });
    }
    return out;
  }

  const acquireQueue = new MicroBatchQueue(
    createTransactionalHoldBatch(db, leases),
    maxBatchSize,
    batchWindowMs,
    1,
  );
  const fastHoldQueues = Array.from(
    { length: SETTLEMENT_LANES },
    () => new MicroBatchQueue(runFastHoldBatch, Math.max(maxBatchSize, 512), batchWindowMs, 1),
  );

  return {
    acquireHold(input: AcquireHoldInput): Promise<AcquireHoldResult> {
      if (leases.canAcquire() && input.idempotency_key === null) {
        const customerAtoms = parseAtoms(input.customer_atoms);
        const supplierAtoms = parseAtoms(input.supplier_atoms);
        if (customerAtoms !== null && supplierAtoms !== null) {
          const reserved = leases.reserveHold(input, customerAtoms, supplierAtoms);
          if (reserved) return fastHoldQueues[walletLane(input.project_id)]!.enqueue(reserved);
        }
      }
      return acquireQueue.enqueue(input);
    },
    async drain(): Promise<void> {
      await Promise.all(fastHoldQueues.map((queue) => queue.drain()));
      await acquireQueue.drain();
    },
  };
}
