import type { SQL } from "bun";
import { UsageConsumer, evidenceChannel, type UsageQueueOptions } from "@gateway/runtime";
import { createSettlementStore } from "@gateway/storage/settlement";

type SettlementStore = ReturnType<typeof createSettlementStore>;
type GroupResult = Awaited<ReturnType<SettlementStore["persist"]>>[number];

const lockConflicts = new Set(["55P03", "40P01"]);
const lockRetryBudgetMs = 2000;

function lockConflict(result: GroupResult) {
  return result.status === "ERROR" && lockConflicts.has(result.error);
}

async function persistThroughLockConflicts(
  store: SettlementStore,
  groups: Parameters<SettlementStore["persist"]>[0],
) {
  const results = await store.persist(groups);
  const deadline = Date.now() + lockRetryBudgetMs;
  let pending = results.flatMap((result, index) => (lockConflict(result) ? [index] : []));
  for (let ceiling = 10; pending.length; ceiling = Math.min(ceiling * 2, 200)) {
    const delay = 1 + Math.floor(Math.random() * ceiling);
    if (Date.now() + delay >= deadline) break;
    await Bun.sleep(delay);
    const retried = await store.persist(pending.map((index) => groups[index]!));
    pending.forEach((index, at) => (results[index] = retried[at]!));
    pending = pending.filter((index) => lockConflict(results[index]!));
  }
  return results;
}

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
      const results = await persistThroughLockConflicts(store, groups);
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
    healthy: () => consumer.healthy(),
    close: () => consumer.close(),
  };
}
