import type { TraceExportConfig, TraceWriterReply } from "./trace-queue.ts";
import { createTraceStore, gatewaySpans } from "./trace-storage.ts";
import type { TraceRecord } from "./trace-types.ts";

let config: TraceExportConfig;
let store: ReturnType<typeof createTraceStore>;
let activity: Promise<void> = Promise.resolve();
const sender = self as unknown as {
  postMessage: (reply: TraceWriterReply) => void;
};

function scheduleCleanup(delay: number) {
  setTimeout(() => {
    activity = activity.then(cleanup);
  }, delay);
}

async function cleanup() {
  const deadline = performance.now() + config.timeoutMs;
  let expired = 0;
  let removed = 0;
  let failed = false;
  try {
    do {
      removed = await store.expire(config.cleanupBatch);
      expired += removed;
    } while (removed === config.cleanupBatch && performance.now() < deadline);
  } catch {
    failed = true;
  }
  sender.postMessage({ type: "cleanup", expired, failed });
  scheduleCleanup(
    !failed && removed === config.cleanupBatch ? config.flushMs : config.cleanupIntervalMs,
  );
}

async function write(records: TraceRecord[]) {
  try {
    const spans = records.flatMap(gatewaySpans);
    for (let attempt = 0; ; attempt++) {
      try {
        await store.insert(spans, config.retentionSeconds);
        break;
      } catch (error) {
        if (attempt >= config.retryLimit) throw error;
        await Bun.sleep(config.flushMs);
      }
    }
    sender.postMessage({ type: "batch", written: true, spans: spans.length });
  } catch {
    sender.postMessage({ type: "batch", written: false, spans: 0 });
  }
}

self.onmessage = (
  event: MessageEvent<{ config: TraceExportConfig } | { records: TraceRecord[] }>,
) => {
  const data = event.data;
  if ("config" in data) {
    config = data.config;
    store = createTraceStore(config.databaseUrl, config.timeoutMs);
    scheduleCleanup(config.cleanupIntervalMs);
  } else activity = activity.then(() => write(data.records));
};
