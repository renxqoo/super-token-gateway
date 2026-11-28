import type { SQL } from "bun";
import type { CommitTerminalInput, CommitTerminalResult } from "./types.ts";
import type { LeaseManager } from "./lease-manager.ts";
import { MicroBatchQueue } from "./micro-batch.ts";
import { commitLeasedBatch, type FastCommitCandidate } from "./settlement-leased.ts";
import { commitDirectBatch } from "./settlement-direct.ts";
import {
  isRetryableLedgerTransactionError,
  isTransientPgConcurrencyError,
  LedgerStatementCancelled,
  postgresState,
  SETTLEMENT_LANES,
  walletLane,
  withPgConcurrencyRetry,
} from "./funds.ts";
import { priceSettlement } from "./settlement-pricing.ts";

export function createSettlementQueue(
  db: SQL,
  leases: LeaseManager,
  batchWindowMs: number,
  maxBatchSize: number,
) {
  async function runCommitBatch(items: CommitTerminalInput[]) {
    const results = Array.from<CommitTerminalResult>({ length: items.length });
    const fastCandidates: FastCommitCandidate[] = [];
    const directEntries: { index: number; item: CommitTerminalInput }[] = [];
    const taskEntries: typeof directEntries = [];
    const seenFastReqIds = new Set<string>();
    const nowMs = Date.now();

    for (let i = 0; i < items.length; i++) {
      const item = items[i]!;
      if (item.async_task) {
        taskEntries.push({ index: i, item });
        continue;
      }
      const meta = leases.holdById(item.request_id);
      if (
        meta &&
        !seenFastReqIds.has(item.request_id) &&
        !item.moderation_blocked &&
        !item.customer_pricing_snapshot &&
        !item.supplier_pricing_snapshot &&
        item.selected_attempt_id === meta.attemptId &&
        item.channel_id === meta.channelId &&
        item.billing_pricing.currency === meta.currency &&
        item.supplier_currency === meta.supplierCurrency
      ) {
        const wl = leases.walletById(meta.walletLeaseId);
        const sl = leases.supplierById(meta.supplierLeaseId);
        const lotValid =
          wl && (wl.earliestLotExpiresAtMs === null || wl.earliestLotExpiresAtMs > nowMs + 500);
        if (wl && sl && lotValid) {
          try {
            const { customerAtoms: custAtoms, supplierAtoms: supAtoms } = priceSettlement(item);
            if (custAtoms <= meta.heldCustomerAtoms && supAtoms <= meta.heldSupplierAtoms) {
              seenFastReqIds.add(item.request_id);
              fastCandidates.push({
                index: i,
                item,
                meta,
                customerAtoms: custAtoms,
                supplierAtoms: supAtoms,
              });
              continue;
            }
          } catch {
            void 0;
          }
        }
      }
      directEntries.push({ index: i, item });
    }

    const failures = new Map<number, unknown>();
    if (fastCandidates.length > 0) {
      try {
        const settle = () => commitLeasedBatch(db, leases, fastCandidates, results);
        const fallback = await settle().catch((error: unknown) => {
          if (!isTransientPgConcurrencyError(error)) throw error;
          return settle();
        });
        for (const entry of fallback) directEntries.push(entry);
      } catch (error) {
        const code = postgresState(error);
        const retryViaDirect =
          code?.startsWith("23") || code === "57014" || isTransientPgConcurrencyError(error);
        for (const candidate of fastCandidates) {
          const wl = leases.walletById(candidate.meta.walletLeaseId);
          if (wl) leases.retireWalletLease(wl);
          const sl = leases.supplierById(candidate.meta.supplierLeaseId);
          if (sl) leases.retireSupplierLease(sl);
          if (retryViaDirect) directEntries.push(candidate);
          else failures.set(candidate.index, error);
        }
      }
    }

    const bisectDeadline = performance.now() + 12_000;
    async function commitDirect(
      entries: typeof directEntries,
      parallelTimeoutSplit = true,
    ): Promise<void> {
      try {
        const run = () => commitDirectBatch(db, leases.notifyDetachedLocal, entries, results);
        if (entries.length === 1) {
          await withPgConcurrencyRetry(
            run,
            3,
            (error) =>
              isTransientPgConcurrencyError(error) ||
              (error instanceof LedgerStatementCancelled && performance.now() < bisectDeadline),
          );
        } else {
          await run().catch((error: unknown) => {
            const code = postgresState(error);
            if (code === "40P01" || code === "40001") return run();
            throw error;
          });
        }
        for (const { index, item } of entries) {
          if (results[index]?.ok) leases.notifyDetachedLocal(item.request_id, null, 0n, null, 0n);
        }
      } catch (error) {
        const code = postgresState(error);
        const canBisect =
          entries.length > 1 &&
          (code?.startsWith("23") ||
            (isRetryableLedgerTransactionError(error) && performance.now() < bisectDeadline));
        if (canBisect) {
          const half = Math.floor(entries.length / 2);
          const first = entries.slice(0, half);
          const second = entries.slice(half);
          if (parallelTimeoutSplit && error instanceof LedgerStatementCancelled) {
            await Promise.all([commitDirect(first, false), commitDirect(second, false)]);
          } else {
            await commitDirect(first, parallelTimeoutSplit);
            await commitDirect(second, parallelTimeoutSplit);
          }
        } else {
          for (const { index } of entries) failures.set(index, error);
        }
      }
    }

    if (directEntries.length > 0) await commitDirect(directEntries);
    for (const entry of taskEntries) await commitDirect([entry]);
    return results.map((result, index) =>
      failures.has(index) ? Promise.reject(failures.get(index)) : result,
    );
  }

  const batchCapacity = Math.max(maxBatchSize, 1024);
  const queues = Array.from(
    { length: SETTLEMENT_LANES },
    () => new MicroBatchQueue(runCommitBatch, batchCapacity, batchWindowMs, 1, "collect-bursts"),
  );
  const directQueues = Array.from(
    { length: SETTLEMENT_LANES },
    () => new MicroBatchQueue(runCommitBatch, batchCapacity, batchWindowMs, 1, "collect-bursts"),
  );
  const directFallback = new MicroBatchQueue(
    runCommitBatch,
    batchCapacity,
    batchWindowMs,
    2,
    "collect-bursts",
  );
  return {
    commitTerminal(input: CommitTerminalInput): Promise<CommitTerminalResult> {
      const hold = leases.holdById(input.request_id);
      if (hold) return queues[walletLane(hold.billingAccountId)]!.enqueue(input);
      if (input.billing_account_id)
        return directQueues[walletLane(input.billing_account_id)]!.enqueue(input);
      return directFallback.enqueue(input);
    },
    async drain(): Promise<void> {
      await Promise.all([
        ...queues.map((queue) => queue.drain()),
        ...directQueues.map((queue) => queue.drain()),
        directFallback.drain(),
      ]);
    },
  };
}
