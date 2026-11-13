import type { SQL } from "bun";
import { UsageConsumer, evidenceChannel, type UsageQueueOptions } from "@gateway/runtime";
import { createSettlementStore } from "@gateway/storage/settlement";

export function createSettlementWorker(
  options: UsageQueueOptions & {
    db: SQL;
    concurrency?: number;
    onRejected?: (count: number) => void;
    onQuarantined?: (count: number) => void;
    onCommitted?: (groups: number, lagSeconds: number) => void;
  },
) {
  const store = createSettlementStore(options.db);
  const consumer = new UsageConsumer({
    ...options,
    channel: evidenceChannel,
    process: async (groups) => {
      const results = await store.persist(groups);
      const quarantined = results
        .flatMap((result) => (result.status === "COMMITTED" ? result.receipts : []))
        .filter((receipt) => receipt.outcome === "QUARANTINED" && !receipt.duplicate).length;
      if (quarantined > 0) options.onQuarantined?.(quarantined);
      if (results.some((result) => result.status === "ERROR"))
        throw new Error("evidence_group_retry");
      const rejected = results.filter((result) => result.status === "REJECTED").length;
      if (rejected > 0) options.onRejected?.(rejected);
      return results;
    },
  });
  return {
    ready: () => consumer.ready(),
    close: () => consumer.close(),
  };
}
