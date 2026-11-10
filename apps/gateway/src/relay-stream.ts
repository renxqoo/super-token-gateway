import { GatewayError, SSEParser, encodeSSE } from "@gateway/kernel";
import type { SSEEvent } from "@gateway/kernel";
import type { StreamMeter } from "@gateway/connectors";
import type { Dependencies } from "./context.ts";
import { publishEvidence } from "./metering.ts";

export interface PreparedStream {
  reader: ReadableStreamDefaultReader<Uint8Array>;
  parser: SSEParser;
  events: SSEEvent[];
  reservedBytes: number;
}

export async function prepareStream(
  response: Response,
  deps: Dependencies,
  controller: AbortController,
  reserveMemory?: (bytes: number) => void,
): Promise<PreparedStream> {
  if (!response.body || !response.headers.get("content-type")?.includes("text/event-stream"))
    throw new GatewayError(502, "invalid_upstream", "Expected an SSE response");
  const reader = response.body.getReader();
  const parser = new SSEParser(deps.config.MAX_EVENT_BYTES);
  let reservedBytes = 0;
  const timeout = setTimeout(
    () => controller.abort(new Error("First upstream event deadline")),
    deps.config.STREAM_IDLE_MS,
  );
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done)
        throw new GatewayError(502, "stream_truncated", "Provider closed before the first event");
      reserveMemory?.(chunk.value.byteLength);
      reservedBytes += chunk.value.byteLength;
      const events = parser
        .feed(chunk.value)
        .filter((event) => event.data && event.event !== "ping");
      if (!events.length) continue;
      return { reader, parser, events, reservedBytes };
    }
  } catch (error) {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

export async function relayStream(
  deps: Dependencies,
  prepared: PreparedStream,
  translator: StreamMeter,
  attemptId: string,
  requestId: string,
  controller: AbortController,
  cleanup: () => Promise<void>,
  beforeOutput?: () => Promise<void>,
  reserveMemory?: (bytes: number) => void,
  afterCommit?: () => Promise<void>,
): Promise<ReadableStream<Uint8Array>> {
  const { reader, parser } = prepared;
  const events = [...prepared.events];
  const output: Uint8Array[] = [];
  const terminal: Uint8Array[] = [];
  let terminalBytes = 0;
  let ended = false;
  let terminalStarted = false;
  let settlement: Promise<void> | undefined;
  let idleTimer: ReturnType<typeof setTimeout> | undefined;
  let stallTimer: ReturnType<typeof setTimeout> | undefined;
  const done = (success: boolean, code?: string): Promise<void> => {
    if (settlement) return settlement;
    settlement = (async () => {
      clearTimeout(idleTimer);
      clearTimeout(stallTimer);
      try {
        await publishEvidence(
          deps,
          attemptId,
          translator.terminal && !translator.failed
            ? "SUCCEEDED"
            : translator.usage
              ? "PARTIAL"
              : "UNKNOWN",
          translator.terminal && !translator.failed && translator.usage
            ? "FINAL"
            : translator.usage
              ? "PARTIAL"
              : "UNKNOWN",
          translator.usage ? { ...translator.usage, requests: "1" } : undefined,
          code,
          undefined,
          success ? "SUCCEEDED" : "UNKNOWN",
        );
        await afterCommit?.();
      } finally {
        await reader.cancel().catch(() => undefined);
        reader.releaseLock();
        await cleanup();
      }
    })();
    return settlement;
  };
  const consume = async (event: SSEEvent) => {
    const released = Math.min(
      prepared.reservedBytes,
      new TextEncoder().encode(event.wire).byteLength,
    );
    reserveMemory?.(-released);
    prepared.reservedBytes -= released;
    const result = translator.consume(event);
    for (const chunk of result.chunks) reserveMemory?.(chunk.byteLength);
    await beforeOutput?.();
    if (result.terminal || translator.holdTerminal) terminalStarted = true;
    if (terminalStarted)
      for (const chunk of result.chunks) {
        terminalBytes += chunk.byteLength;
        if (terminalBytes > deps.config.MAX_EVENT_BYTES * 2)
          throw new GatewayError(502, "terminal_limit", "Terminal event window exceeds limit");
        terminal.push(chunk);
      }
    else output.push(...result.chunks);
  };
  const first = events.shift();
  if (first) {
    try {
      await consume(first);
    } catch (error) {
      await reader.cancel().catch(() => undefined);
      reader.releaseLock();
      throw error;
    }
  }
  const resetStall = () => {
    clearTimeout(stallTimer);
    stallTimer = setTimeout(
      () => controller.abort(new Error("Downstream write stalled")),
      deps.config.DOWNSTREAM_STALL_MS,
    );
  };
  controller.signal.addEventListener(
    "abort",
    () => {
      void reader.cancel().catch(() => undefined);
      if (!ended)
        deps.lifecycle
          .track(done(false, "stream_cancelled"))
          .catch(() => deps.metrics.increment("stream_cleanup_failures_total"));
    },
    { once: true },
  );
  resetStall();
  return new ReadableStream<Uint8Array>(
    {
      async pull(downstream) {
        clearTimeout(stallTimer);
        try {
          while (!output.length && !ended) {
            controller.signal.throwIfAborted();
            const event = events.shift();
            if (event) {
              await consume(event);
              continue;
            }
            idleTimer = setTimeout(
              () => controller.abort(new Error("Upstream stream idle")),
              deps.config.STREAM_IDLE_MS,
            );
            const chunk = await reader.read();
            clearTimeout(idleTimer);
            if (chunk.done) {
              parser.feed(new Uint8Array(), true);
              if (!translator.terminal || translator.failed || parser.incomplete)
                throw new GatewayError(
                  502,
                  "stream_truncated",
                  "Provider stream ended without a valid terminal event",
                );
              for (const chunk of translator.finalize()) {
                terminalBytes += chunk.byteLength;
                if (terminalBytes > deps.config.MAX_EVENT_BYTES * 2)
                  throw new GatewayError(
                    502,
                    "terminal_limit",
                    "Terminal event window exceeds limit",
                  );
                reserveMemory?.(chunk.byteLength);
                terminal.push(chunk);
              }
              await done(true);
              ended = true;
              output.push(...terminal);
            } else {
              if (chunk.value.byteLength > deps.config.MAX_EVENT_BYTES * 2)
                throw new GatewayError(502, "stream_chunk_limit", "Upstream chunk exceeds limit");
              reserveMemory?.(chunk.value.byteLength);
              prepared.reservedBytes += chunk.value.byteLength;
              events.push(...parser.feed(chunk.value));
            }
          }
          const next = output.shift();
          if (next) {
            reserveMemory?.(-next.byteLength);
            downstream.enqueue(next);
            if (!ended || output.length) resetStall();
          }
          if (ended && !output.length) downstream.close();
        } catch (error) {
          ended = true;
          controller.abort(error);
          await done(
            false,
            error instanceof GatewayError ? error.code : "stream_interrupted",
          ).catch(() => deps.metrics.increment("stream_cleanup_failures_total"));
          downstream.enqueue(
            encodeSSE(
              {
                error: {
                  type: "gateway_stream_error",
                  message: "Stream interrupted; usage may be partial",
                  request_id: requestId,
                },
              },
              translator.target === "messages" ? "error" : undefined,
            ),
          );
          downstream.close();
        }
      },
      async cancel() {
        ended = true;
        controller.abort(new Error("Client disconnected"));
        await done(false, "client_disconnected");
      },
    },
    { highWaterMark: 65536, size: (chunk) => chunk?.byteLength ?? 0 },
  );
}
