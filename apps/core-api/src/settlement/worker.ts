import type { SQL } from "bun";
import { UsageConsumer, evidenceChannel, type UsageQueueOptions } from "@gateway/runtime";
import { createSettlementStore } from "@gateway/storage/settlement";

export function createSettlementWorker(
  options: UsageQueueOptions & {
    db: SQL;
    concurrency?: number;
    onRejected?: (count: number) => void;
    onCommitted?: (groups: number, lagSeconds: number) => void;
  },
) {
  const store = createSettlementStore(options.db);
  const consumer = new UsageConsumer({
    ...options,
    channel: evidenceChannel,
    process: async (groups) => {
      const results = await store.persist(groups);
      const rejected = results.filter((result) => result.status === "REJECTED").length;
      if (rejected > 0) options.onRejected?.(rejected);
      if (results.some((result) => result.status === "ERROR"))
        throw new Error("evidence_group_retry");
      return results;
    },
  });
  return {
    ready: () => consumer.ready(),
    close: () => consumer.close(),
  };
}
