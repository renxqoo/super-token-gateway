import type { TraceRecord } from "./trace-types.ts";

export type TraceExportConfig = {
  databaseUrl: string;
  sampleBasisPoints: number;
  slowFirstTextMs: number;
  maxAttempts: number;
  errorBodyMaxBytes?: number;
  maxRecords: number;
  maxBytes: number;
  maxPerProject: number;
  priorityReserve: number;
  batchSize: number;
  batchDeadlineMs: number;
  flushMs: number;
  timeoutMs: number;
  retryLimit: number;
  retentionSeconds: number;
  cleanupIntervalMs: number;
  cleanupBatch: number;
};

type PendingTrace = { record: TraceRecord; bytes: number; priority: boolean };
export type TraceWriterReply =
  | { type: "batch"; written: boolean; spans: number }
  | { type: "cleanup"; expired: number; failed: boolean };

export function createTraceQueue(config: TraceExportConfig, samplingKey: string) {
  const ordinary = new Set<PendingTrace>();
  const priority = new Set<PendingTrace>();
  const projectCounts = new Map<string, number>();
  const projectOrdinary = new Map<string, Set<PendingTrace>>();
  let inFlight: PendingTrace[] = [];
  let worker: Worker | undefined;
  let batchTimer: ReturnType<typeof setTimeout> | undefined;
  let closed = false;
  let queuedBytes = 0;
  let queuedCount = 0;
  let normalCount = 0;
  let written = 0;
  let failed = 0;
  let timeouts = 0;
  let cleanupFailed = 0;
  let expired = 0;
  let droppedOrdinary = 0;
  let droppedPriority = 0;
  const salt = Bun.hash(samplingKey);

  function dropped(critical: boolean) {
    if (critical) droppedPriority++;
    else droppedOrdinary++;
  }

  function discard(entry: PendingTrace) {
    queuedBytes -= entry.bytes;
    queuedCount--;
    if (!entry.priority) normalCount--;
    const project = entry.record.projectId;
    const count = projectCounts.get(project)! - 1;
    if (count === 0) projectCounts.delete(project);
    else projectCounts.set(project, count);
  }

  function finish(success: boolean, spans: number) {
    clearTimeout(batchTimer);
    batchTimer = undefined;
    if (!inFlight.length) return;
    written += spans;
    if (!success) failed++;
    for (const entry of inFlight) {
      discard(entry);
      if (!success) dropped(entry.priority);
    }
    inFlight = [];
  }

  function removeOrdinary(entry: PendingTrace) {
    ordinary.delete(entry);
    const project = entry.record.projectId;
    const waiting = projectOrdinary.get(project)!;
    waiting.delete(entry);
    if (!waiting.size) projectOrdinary.delete(project);
  }

  function evictOrdinary(entry: PendingTrace) {
    removeOrdinary(entry);
    discard(entry);
    dropped(false);
  }

  function stopWorker() {
    const stopped = worker;
    worker = undefined;
    stopped?.terminate();
    finish(false, 0);
  }

  function flush() {
    if (closed || inFlight.length) return;
    try {
      if (!worker) {
        const extension = import.meta.path.endsWith(".ts") ? "ts" : "js";
        const created = new Worker(
          new URL(`./trace-writer.${extension}`, import.meta.url).pathname,
        );
        worker = created;
        created.onmessage = (event: MessageEvent<TraceWriterReply>) => {
          if (worker !== created) return;
          if (event.data.type === "batch") finish(event.data.written, event.data.spans);
          else {
            expired += event.data.expired;
            if (event.data.failed) cleanupFailed++;
          }
        };
        created.onerror = (event) => {
          event.preventDefault();
          if (worker === created) stopWorker();
        };
        created.postMessage({ config });
      }
      for (const backlog of [priority, ordinary]) {
        for (const entry of backlog) {
          if (inFlight.length >= config.batchSize) break;
          if (entry.priority) priority.delete(entry);
          else removeOrdinary(entry);
          inFlight.push(entry);
        }
      }
      if (inFlight.length) {
        batchTimer = setTimeout(() => {
          timeouts++;
          stopWorker();
        }, config.batchDeadlineMs);
        batchTimer.unref();
        worker.postMessage({ records: inFlight.map((entry) => entry.record) });
      }
    } catch {
      stopWorker();
    }
  }

  const timer = setInterval(flush, config.flushMs);
  timer.unref();
  return {
    offer(record: TraceRecord) {
      const firstTextWait = (record.enqueued ?? record.end) - record.start;
      const critical =
        record.status >= 500 ||
        record.status === 499 ||
        record.failedAttempts > 0 ||
        record.attempts.length + record.omittedAttempts > 1 ||
        (record.firstTextTracked && firstTextWait >= config.slowFirstTextMs);
      if (
        !critical &&
        (config.sampleBasisPoints === 0 ||
          Bun.hash(record.traceId, salt) % 10_000n >= BigInt(config.sampleBasisPoints))
      )
        return;
      if (closed || (!critical && normalCount >= config.maxRecords - config.priorityReserve)) {
        dropped(critical);
        return;
      }
      const bytes =
        1536 +
        4 * (record.model?.length ?? 0) +
        record.attempts.reduce(
          (sum, attempt) =>
            sum +
            1536 +
            4 *
              (attempt.channel.length +
                (attempt.providerRequestId?.length ?? 0) +
                (attempt.providerError?.code.length ?? 0) +
                (attempt.providerError?.summary.length ?? 0)),
          0,
        );
      if (bytes > config.maxBytes) {
        dropped(critical);
        return;
      }
      if ((projectCounts.get(record.projectId) ?? 0) >= config.maxPerProject) {
        const evicted = projectOrdinary.get(record.projectId)?.values().next().value;
        if (!critical || !evicted) {
          dropped(critical);
          return;
        }
        evictOrdinary(evicted);
      }
      while (
        critical &&
        ordinary.size &&
        (queuedCount >= config.maxRecords || queuedBytes + bytes > config.maxBytes)
      ) {
        evictOrdinary(ordinary.values().next().value!);
      }
      if (queuedCount >= config.maxRecords || queuedBytes + bytes > config.maxBytes) {
        dropped(critical);
        return;
      }
      queuedCount++;
      queuedBytes += bytes;
      projectCounts.set(record.projectId, (projectCounts.get(record.projectId) ?? 0) + 1);
      const entry = { record, bytes, priority: critical };
      if (critical) priority.add(entry);
      else {
        ordinary.add(entry);
        const waiting = projectOrdinary.get(record.projectId) ?? new Set<PendingTrace>();
        waiting.add(entry);
        projectOrdinary.set(record.projectId, waiting);
        normalCount++;
      }
    },
    metrics() {
      let oldest = Math.min(
        ordinary.values().next().value?.record.end ?? Infinity,
        priority.values().next().value?.record.end ?? Infinity,
      );
      for (const entry of inFlight) oldest = Math.min(oldest, entry.record.end);
      return [
        `gateway_trace_queue_records ${queuedCount}`,
        `gateway_trace_queue_estimated_bytes ${queuedBytes}`,
        `gateway_trace_queue_oldest_age_seconds ${Math.max(0, performance.now() - oldest) / 1000}`,
        `gateway_trace_spans_written_total ${written}`,
        `gateway_trace_export_failed_batches_total ${failed}`,
        `gateway_trace_export_timed_out_batches_total ${timeouts}`,
        `gateway_trace_cleanup_failed_runs_total ${cleanupFailed}`,
        `gateway_trace_spans_expired_total ${expired}`,
        `gateway_trace_dropped_total{priority="ordinary"} ${droppedOrdinary}`,
        `gateway_trace_dropped_total{priority="critical"} ${droppedPriority}`,
      ];
    },
    async close() {
      clearInterval(timer);
      const deadline = performance.now() + config.timeoutMs;
      while (queuedCount && performance.now() < deadline) {
        flush();
        await Bun.sleep(Math.min(config.flushMs, Math.max(1, deadline - performance.now())));
      }
      closed = true;
      stopWorker();
      droppedOrdinary += normalCount;
      droppedPriority += queuedCount - normalCount;
      ordinary.clear();
      priority.clear();
      projectCounts.clear();
      projectOrdinary.clear();
      queuedCount = queuedBytes = normalCount = 0;
    },
  };
}
