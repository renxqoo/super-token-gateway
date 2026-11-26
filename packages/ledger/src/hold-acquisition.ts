import type { SQL } from "bun";
import type { AcquireHoldInput, AcquireHoldResult } from "./types.ts";
import type { LeaseManager, FastHoldRequest } from "./lease-manager.ts";
import { MicroBatchQueue } from "./micro-batch.ts";
import { createTransactionalHoldBatch } from "./transactional-holds.ts";
import {
  classifyLedgerStatementCancellation,
  holdTemporarilyUnavailable,
  isRetryableLedgerTransactionError,
  jsonNullable,
  jsonQuote,
  parseAtoms,
  SETTLEMENT_LANES,
  walletLane,
} from "./funds.ts";
import { matchesHoldFingerprint } from "./transactional-hold-state.ts";

function serializeFastHoldBatch(items: FastHoldRequest[]): string {
  let out = "[";
  for (let i = 0; i < items.length; i++) {
    if (i > 0) out += ",";
    const x = items[i]!;
    const inp = x.input;
    const custAtoms = x.customerAtoms.toString();
    const supAtoms = x.supplierAtoms.toString();
    const overrideId = jsonNullable(inp.pricing_override_id);
    const discNum = jsonNullable(inp.discount_numerator);
    const discDen = jsonNullable(inp.discount_denominator);
    let sh = `[{"attempt_id":"${inp.attempt_id}"`;
    if (inp.supplier_cost_version_id) sh += `,"cost_version_id":"${inp.supplier_cost_version_id}"`;
    if (inp.customer_price_version_id) {
      sh += `,"customer_price_version_id":"${inp.customer_price_version_id}","customer_price_adjustment":{"override_id":${overrideId},"discount_numerator":${discNum},"discount_denominator":${discDen}}`;
    }
    sh += `,"account_id":"${inp.supplier_account_id}","channel_id":"${inp.supplier_channel_id}","currency":${jsonQuote(inp.supplier_currency)},"cell":${x.supplierLease.cell},"held_atoms":"${supAtoms}","state":"HELD"}]`;
    out += `{"request_id":"${inp.request_id}","billing_account_id":"${inp.billing_account_id}","wallet_id":"${x.walletLease.walletId}","api_key_id":${jsonNullable(inp.api_key_id)},"pricing_override_id":${overrideId},"discount_numerator":${discNum},"discount_denominator":${discDen},"operation":${jsonQuote(inp.operation)},"model":${jsonQuote(inp.model)},"fingerprint":${inp.fingerprint.length === 0 ? '""' : JSON.stringify(inp.fingerprint)},"held_atoms":"${custAtoms}","supplier_holds":${sh},"wallet_lease_id":"${x.walletLease.leaseId}","supplier_lease_id":"${x.supplierLease.leaseId}","expires_at":${jsonQuote(inp.expires_at)}}`;
  }
  return `${out}]`;
}

export function createHoldAcquirer(
  db: SQL,
  leases: LeaseManager,
  batchWindowMs: number,
  maxBatchSize: number,
  maxBatchConcurrency: number,
) {
  async function runFastHoldBatch(
    items: FastHoldRequest[],
    retry = false,
    checkDebtGrants = false,
  ): Promise<(AcquireHoldResult | Promise<AcquireHoldResult>)[]> {
    const batchPayload = serializeFastHoldBatch(items);

    const retireLeases = (item: FastHoldRequest) => {
      if (item.walletLease.inFlightHolds > 0) item.walletLease.inFlightHolds--;
      if (item.supplierLease.inFlightHolds > 0) item.supplierLease.inFlightHolds--;
      leases.retireWalletLease(item.walletLease);
      leases.retireSupplierLease(item.supplierLease);
    };
    const restorePendingReservation = (item: FastHoldRequest) => {
      item.walletLease.inFlightHolds--;
      item.supplierLease.inFlightHolds--;
      item.walletLease.freeInMemory += item.customerAtoms;
      item.supplierLease.freeInMemory += item.supplierAtoms;
    };
    let admitted: Set<string>;
    try {
      const uniqueWalletLeaseIds = [...new Set(items.map((item) => item.walletLease.leaseId))];
      const uniqueSupplierLeaseIds = [...new Set(items.map((item) => item.supplierLease.leaseId))];
      const rows = await db.begin(
        classifyLedgerStatementCancellation(async (tx) => {
          const [locked] = await tx<{ wallet_lease_ids: string[]; supplier_lease_ids: string[] }[]>`
            WITH local_wallet_leases AS MATERIALIZED (
              SELECT l.id, l.wallet_id
              FROM public.wallet_leases l
              WHERE l.id = ANY(${uniqueWalletLeaseIds}::uuid[])
                AND l.node_id = ${leases.nodeId}
                AND l.state = 'ACTIVE' AND l.expires_at > clock_timestamp()
                AND (l.earliest_lot_expires_at IS NULL OR l.earliest_lot_expires_at > clock_timestamp())
              ORDER BY l.id FOR KEY SHARE OF l
            ), locked_supplier_leases AS MATERIALIZED (
              SELECT l.id
              FROM public.supplier_leases l
              WHERE EXISTS (SELECT 1 FROM local_wallet_leases)
                AND l.id = ANY(${uniqueSupplierLeaseIds}::uuid[])
                AND l.node_id = ${leases.nodeId}
                AND l.state = 'ACTIVE' AND l.expires_at > clock_timestamp()
              ORDER BY l.id FOR KEY SHARE OF l
            ), locked_wallet_leases AS MATERIALIZED (
              SELECT l.id
              FROM local_wallet_leases l
              JOIN public.wallets w ON w.id = l.wallet_id
              WHERE EXISTS (SELECT 1 FROM locked_supplier_leases)
              ORDER BY w.id, l.id FOR SHARE OF w
            )
            SELECT ARRAY(SELECT id FROM locked_wallet_leases) AS wallet_lease_ids,
                   ARRAY(SELECT id FROM locked_supplier_leases) AS supplier_lease_ids
          `;
          if (!locked?.wallet_lease_ids.length || !locked.supplier_lease_ids.length) return [];
          return tx<{ request_id: string }[]>`
            WITH active_wallet_leases AS MATERIALIZED (
              SELECT l.id
              FROM unnest(${locked.wallet_lease_ids}::uuid[]) AS acquired(id)
              CROSS JOIN LATERAL (
                SELECT wl.id, wl.wallet_id, wl.state, wl.expires_at, wl.earliest_lot_expires_at
                FROM public.wallet_leases wl
                WHERE wl.id = acquired.id
                LIMIT 1
              ) l
              JOIN public.wallets w ON w.id = l.wallet_id
              WHERE l.state = 'ACTIVE' AND l.expires_at > statement_timestamp()
                AND (l.earliest_lot_expires_at IS NULL OR l.earliest_lot_expires_at > statement_timestamp())
                AND w.freeze_reason IS NULL
                AND CASE WHEN w.reserved_atoms >= w.debt_atoms THEN true
                  WHEN ${checkDebtGrants}::boolean THEN w.available_atoms + w.reserved_atoms - w.debt_atoms >= (
                    SELECT COALESCE(SUM(lot.available_atoms), 0)
                    FROM public.wallet_lots lot
                    WHERE lot.wallet_id = w.id AND lot.available_atoms > 0
                      AND lot.expires_at <= statement_timestamp()
                  )
                  ELSE false
                END
            ), active_supplier_leases AS MATERIALIZED (
              SELECT l.id
              FROM public.supplier_leases l
              JOIN public.upstream_accounts a ON a.id = l.account_id
              WHERE l.id = ANY(${locked.supplier_lease_ids}::uuid[])
                AND l.state = 'ACTIVE' AND l.expires_at > statement_timestamp() AND a.state = 'ACTIVE'
            )
            INSERT INTO public.billing_holds (
              request_id, billing_account_id, wallet_id, api_key_id, key_reserved_atoms,
              pricing_override_id, discount_numerator, discount_denominator,
              operation, model, idempotency_key, fingerprint,
              held_atoms, credit_atoms, lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state, expires_at
            )
            SELECT
              u.request_id, u.billing_account_id, u.wallet_id, u.api_key_id, 0,
              u.pricing_override_id, u.discount_numerator, u.discount_denominator,
              u.operation, u.model, NULL, u.fingerprint,
              u.held_atoms, 0, '[]'::jsonb, u.supplier_holds, u.wallet_lease_id, u.supplier_lease_id, 'HELD', u.expires_at
            FROM jsonb_to_recordset(${batchPayload}::text::jsonb) AS u(
              request_id uuid,
              billing_account_id uuid,
              wallet_id uuid,
              api_key_id uuid,
              pricing_override_id uuid,
              discount_numerator numeric,
              discount_denominator numeric,
              operation text,
              model text,
              fingerprint text,
              held_atoms numeric,
              supplier_holds jsonb,
              wallet_lease_id uuid,
              supplier_lease_id uuid,
              expires_at timestamptz
            )
            JOIN active_wallet_leases awl ON awl.id = u.wallet_lease_id
            JOIN active_supplier_leases asl ON asl.id = u.supplier_lease_id
            RETURNING request_id
          `;
        }),
      );
      admitted = new Set(rows.map((row) => String(row.request_id)));
    } catch (err) {
      if (isRetryableLedgerTransactionError(err)) {
        if (!retry) return items.map((item) => contendedFastQueue.enqueue(item));
        return items.map((item) => {
          restorePendingReservation(item);
          return holdTemporarilyUnavailable();
        });
      }
      for (const item of items) retireLeases(item);
      throw err;
    }

    const out: (AcquireHoldResult | Promise<AcquireHoldResult>)[] = Array.from({
      length: items.length,
    });
    const rejectedIndices: number[] = [];
    const rejectedItems: FastHoldRequest[] = [];
    for (let i = 0; i < items.length; i++) {
      const x = items[i]!;
      if (!admitted.has(x.input.request_id)) {
        if (!checkDebtGrants) {
          rejectedIndices.push(i);
          rejectedItems.push(x);
        } else {
          retireLeases(x);
          out[i] = acquireQueues[walletLane(x.input.billing_account_id)]!.enqueue(x.input);
        }
        continue;
      }
      leases.trackHold({
        requestId: x.input.request_id,
        walletLeaseId: x.walletLease.leaseId,
        supplierLeaseId: x.supplierLease.leaseId,
        walletId: x.walletLease.walletId,
        billingAccountId: x.input.billing_account_id,
        apiKeyId: x.input.api_key_id ?? null,
        pricingOverrideId: x.input.pricing_override_id ?? null,
        discountNumerator: x.input.discount_numerator ?? null,
        discountDenominator: x.input.discount_denominator ?? null,
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
      out[i] = {
        ok: true,
        replayed: false,
        request_id: x.input.request_id,
        wallet_id: x.walletLease.walletId,
        held_atoms: x.customerAtoms.toString(),
        credit_atoms: "0",
        supplier_cell: x.supplierLease.cell,
      };
    }
    if (rejectedItems.length > 0) {
      const retried = await runFastHoldBatch(rejectedItems, retry, true).catch((err: unknown) =>
        rejectedItems.map(() => Promise.reject<AcquireHoldResult>(err)),
      );
      for (let i = 0; i < rejectedIndices.length; i++) {
        out[rejectedIndices[i]!] = retried[i]!;
      }
    }
    return out;
  }

  const contendedQueue = new MicroBatchQueue(
    createTransactionalHoldBatch(db, leases),
    1,
    batchWindowMs,
    maxBatchConcurrency,
  );
  const contendedFastQueue = new MicroBatchQueue<FastHoldRequest, AcquireHoldResult>(
    (items) => runFastHoldBatch(items, true),
    1,
    batchWindowMs,
    maxBatchConcurrency,
  );
  const acquireQueues = Array.from(
    { length: SETTLEMENT_LANES },
    () =>
      new MicroBatchQueue(
        createTransactionalHoldBatch(db, leases, (input) => contendedQueue.enqueue(input)),
        maxBatchSize,
        batchWindowMs,
        1,
        "collect-bursts",
      ),
  );
  const fastHoldQueue = new MicroBatchQueue(
    (items: FastHoldRequest[]) => runFastHoldBatch(items),
    Math.max(maxBatchSize, 1024),
    batchWindowMs,
    1,
    "collect-bursts",
  );

  return {
    async findIdempotentHold(
      billingAccountId: string,
      operation: AcquireHoldInput["operation"],
      key: string,
      fingerprints: Pick<AcquireHoldInput, "fingerprint" | "legacy_fingerprint">,
    ) {
      const [hold] = await db<{ request_id: string; fingerprint: string; state: string }[]>`
        SELECT request_id, fingerprint, state FROM public.billing_holds
        WHERE billing_account_id=${billingAccountId}::uuid AND operation=${operation} AND idempotency_key=${key}
      `;
      return hold
        ? {
            requestId: hold.request_id,
            state: hold.state,
            matches: matchesHoldFingerprint(hold.fingerprint, fingerprints),
          }
        : null;
    },
    acquireHold(input: AcquireHoldInput): Promise<AcquireHoldResult> {
      const lane = walletLane(input.billing_account_id);
      if (leases.canAcquire() && input.idempotency_key === null && !input.enforce_key_spend_limit) {
        const customerAtoms = parseAtoms(input.customer_atoms);
        const supplierAtoms = parseAtoms(input.supplier_atoms);
        if (customerAtoms !== null && supplierAtoms !== null) {
          const reserved = leases.reserveHold(input, customerAtoms, supplierAtoms);
          if (reserved) return fastHoldQueue.enqueue(reserved);
        }
      }
      return acquireQueues[lane]!.enqueue(input);
    },
    async drain(): Promise<void> {
      await fastHoldQueue.drain();
      await contendedFastQueue.drain();
      await Promise.all(acquireQueues.map((queue) => queue.drain()));
      await contendedQueue.drain();
    },
  };
}
