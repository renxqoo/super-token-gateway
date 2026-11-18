import type { SQL } from "bun";
import { UsageConsumer, evidenceChannel, type UsageQueueOptions } from "@gateway/runtime";
import { createSettlementStore } from "@gateway/storage/settlement";
import { createSettlementProcessor } from "./processor.ts";

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
  const processor = createSettlementProcessor({
    persist: (groups) => store.persist(groups),
    ...(options.onRejected ? { onRejected: options.onRejected } : {}),
    ...(options.onQuarantined ? { onQuarantined: options.onQuarantined } : {}),
  });
  const consumer = new UsageConsumer({
    ...options,
    channel: evidenceChannel,
    process: (groups) => processor.process(groups),
  });
  let closing: Promise<void> | undefined;
  return {
    ready: () => consumer.ready(),
    healthy: () => consumer.healthy(),
    stats: () => processor.stats(),
    close: () => (closing ??= consumer.close().finally(() => processor.close())),
  };
}
