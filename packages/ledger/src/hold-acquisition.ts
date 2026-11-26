import type { SQL } from "bun";
import type { AcquireHoldInput, AcquireHoldResult } from "./types.ts";
import type { LeaseManager, FastHoldRequest } from "./lease-manager.ts";
import { MicroBatchQueue } from "./micro-batch.ts";
import { createTransactionalHoldBatch } from "./transactional-holds.ts";
import {
  holdTemporarilyUnavailable,
  isRetryableLedgerTransactionError,
  jsonNullable,
  jsonQuote,
  LedgerStatementCancelled,
  parseAtoms,
  postgresState,
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
      const [row] = await db<{ admitted: string[] | null }[]>`
        SELECT public.acquire_leased_holds(
          ${leases.nodeId},
          ${uniqueWalletLeaseIds}::uuid[],
          ${uniqueSupplierLeaseIds}::uuid[],
          ${batchPayload}::text::jsonb,
          ${checkDebtGrants}::boolean
        ) AS admitted
      `.catch((error: unknown) => {
        if (postgresState(error) === "57014") throw new LedgerStatementCancelled(error);
        throw error;
      });
      admitted = new Set((row?.admitted ?? []).map((id) => String(id)));
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
