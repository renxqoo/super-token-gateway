import type { SQL } from "bun";
import type { CommitTerminalInput, CommitTerminalResult } from "./types.ts";
import type { LeaseManager } from "./lease-manager.ts";
import { MicroBatchQueue } from "./micro-batch.ts";
import { commitLeasedBatch, type FastCommitCandidate } from "./settlement-leased.ts";
import { commitDirectBatch } from "./settlement-direct.ts";
import { postgresState } from "./funds.ts";
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
    const seenFastReqIds = new Set<string>();
    const nowMs = Date.now();

    for (let i = 0; i < items.length; i++) {
      const item = items[i]!;
      const meta = leases.holdById(item.request_id);
      if (
        meta &&
        !seenFastReqIds.has(item.request_id) &&
        !item.moderation_blocked &&
        item.selected_attempt_id === meta.attemptId &&
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
        const fallback = await commitLeasedBatch(db, leases, fastCandidates, results);
        for (const entry of fallback) directEntries.push(entry);
      } catch (error) {
        const retryIndividually = postgresState(error)?.startsWith("23");
        for (const candidate of fastCandidates) {
          const wl = leases.walletById(candidate.meta.walletLeaseId);
          if (wl) leases.retireWalletLease(wl);
          const sl = leases.supplierById(candidate.meta.supplierLeaseId);
          if (sl) leases.retireSupplierLease(sl);
          if (retryIndividually) directEntries.push(candidate);
          else failures.set(candidate.index, error);
        }
      }
    }

    async function commitDirect(entries: typeof directEntries): Promise<void> {
      try {
        await commitDirectBatch(db, leases.notifyDetachedLocal, entries, results);
        for (const { index, item } of entries) {
          if (results[index]?.ok) leases.notifyDetachedLocal(item.request_id, null, 0n, null, 0n);
        }
      } catch (error) {
        if (entries.length > 1 && postgresState(error)?.startsWith("23")) {
          const half = Math.floor(entries.length / 2);
          await commitDirect(entries.slice(0, half));
          await commitDirect(entries.slice(half));
        } else {
          for (const { index } of entries) failures.set(index, error);
        }
      }
    }

    if (directEntries.length > 0) await commitDirect(directEntries);
    return results.map((result, index) =>
      failures.has(index) ? Promise.reject(failures.get(index)) : result,
    );
  }

  const queue = new MicroBatchQueue(runCommitBatch, Math.max(maxBatchSize, 512), batchWindowMs, 2);
  return {
    commitTerminal: (input: CommitTerminalInput): Promise<CommitTerminalResult> =>
      queue.enqueue(input),
    drain: () => queue.drain(),
  };
}
