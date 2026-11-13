import { boundedRelay } from "@gateway/runtime";
import type { Usage } from "@gateway/kernel";
import type { Dependencies } from "./context.ts";
import { publishEvidence } from "./metering.ts";

export function relayBinary(
  deps: Dependencies,
  raw: ReadableStream<Uint8Array>,
  attemptId: string,
  requestId: string,
  controller: AbortController,
  cleanup: () => Promise<void>,
  usage?: Usage,
): ReadableStream<Uint8Array> {
  let success = false;
  let settlement: Promise<void> | undefined;
  const reader = raw.getReader();
  const finish = (): Promise<void> => {
    if (!settlement)
      settlement = deps.lifecycle.track(
        (async () => {
          try {
            await publishEvidence(
              deps,
              attemptId,
              success ? "SUCCEEDED" : "UNKNOWN",
              success ? "FINAL" : "UNKNOWN",
              success ? { ...usage, requests: "1" } : undefined,
              success ? undefined : "binary_interrupted",
              undefined,
              success ? "SUCCEEDED" : "UNKNOWN",
            );
          } finally {
            await reader.cancel().catch(() => undefined);
            reader.releaseLock();
            await cleanup();
          }
        })(),
      );
    return settlement;
  };
  const input = new ReadableStream<Uint8Array>(
    {
      async pull(writer) {
        try {
          const value = await reader.read();
          if (value.done) {
            success = true;
            await finish();
            writer.close();
          } else writer.enqueue(value.value);
        } catch (error) {
          await finish().catch(() => deps.metrics.increment("binary_settlement_failures_total"));
          writer.error(error);
        }
      },
      async cancel() {
        controller.abort(new Error("Binary client disconnected"));
        await finish();
      },
    },
    { highWaterMark: 65536, size: (chunk) => chunk?.byteLength ?? 0 },
  );
  return boundedRelay(
    input,
    controller.signal,
    deps.config.DOWNSTREAM_STALL_MS,
    deps.config.TOTAL_TIMEOUT_MS,
    () => finish(),
  );
}
