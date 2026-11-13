import { UsageConsumer, usageChannel } from "@gateway/runtime";
import type { Dependencies } from "./context.ts";
export class BillingWorker {
  readonly consumer: UsageConsumer;
  constructor(readonly deps: Dependencies) {
    this.consumer = new UsageConsumer({
      channel: usageChannel,
      url: deps.config.QUEUE_REDIS_URL,
      namespace: deps.config.REDIS_NAMESPACE,
      replicas: deps.config.QUEUE_REQUIRED_REPLICAS,
      timeoutMs: deps.config.QUEUE_ACK_TIMEOUT_MS,
      concurrency: deps.config.BILLING_CONCURRENCY,
      process: async (events) => {
        const counts = await deps.store.persistUsage(events);
        for (const [state, count] of Object.entries(counts))
          deps.metrics.increment(`billing_${state.toLowerCase()}_total`, count);
      },
      onCommitted: (events, lag) => deps.metrics.observeUsageLag(lag, events),
      onError: () => deps.metrics.increment("usage_consumer_errors_total"),
    });
  }
  async start(): Promise<void> {
    await this.consumer.ready();
  }
  async close(): Promise<void> {
    await this.consumer.close();
  }
}
