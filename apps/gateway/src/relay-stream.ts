import { GatewayError } from "@gateway/contracts";
import { SSEParser, encodeSSE } from "@gateway/connectors";
import type { SSEEvent } from "@gateway/connectors";
import type { StreamMeter } from "@gateway/connectors";
import type { Dependencies } from "./context.ts";
import { publishEvidence } from "./metering.ts";

const encoder = new TextEncoder();

export interface PreparedStream {
  reader: ReadableStreamDefaultReader<Uint8Array>;
  parser: SSEParser;
  events: (SSEEvent & { bytes: number })[];
  reservedBytes: number;
}

function parseChunk(
  prepared: PreparedStream,
  chunk: Uint8Array,
  prelude: boolean,
  reserveMemory?: (bytes: number) => void,
  flush = false,
): void {
  reserveMemory?.(chunk.byteLength);
  prepared.reservedBytes += chunk.byteLength;
  const frames = prepared.parser.feed(chunk, flush);
  let retained = prepared.parser.bufferedBytes;
  for (const frame of frames) {
    if (prelude && (!frame.data || frame.event === "ping")) continue;
    const bytes = encoder.encode(frame.wire).byteLength;
    retained += bytes;
    prepared.events.push({ ...frame, bytes });
  }
  reserveMemory?.(retained - prepared.reservedBytes);
  prepared.reservedBytes = retained;
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
  const prepared: PreparedStream = {
    reader,
    parser: new SSEParser(deps.config.MAX_EVENT_BYTES),
    events: [],
    reservedBytes: 0,
  };
  const cancelRead = () => void reader.cancel().catch(() => undefined);
  controller.signal.addEventListener("abort", cancelRead, { once: true });
  const timeout = setTimeout(
    () => controller.abort(new Error("First upstream event deadline")),
    deps.config.STREAM_IDLE_MS,
  );
  try {
    controller.signal.throwIfAborted();
    while (true) {
      const chunk = await reader.read();
      controller.signal.throwIfAborted();
      if (chunk.done)
        throw new GatewayError(502, "stream_truncated", "Provider closed before the first event");
      if (chunk.value.byteLength > deps.config.MAX_EVENT_BYTES * 2)
        throw new GatewayError(502, "stream_chunk_limit", "Upstream chunk exceeds limit");
      parseChunk(prepared, chunk.value, true, reserveMemory);
      if (prepared.events.length) return prepared;
    }
  } catch (error) {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
    throw error;
  } finally {
    clearTimeout(timeout);
    controller.signal.removeEventListener("abort", cancelRead);
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
  const { reader, parser, events } = prepared;
  const output: Uint8Array[] = [];
  const terminal: Uint8Array[] = [];
  let terminalBytes = 0;
  let ended = false;
  let eof = false;
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
        const usage =
          success || translator.usage ? { ...translator.usage, requests: "1" } : undefined;
        const completed =
          translator.terminal &&
          !translator.failed &&
          !parser.incomplete &&
          (success || (code === "client_disconnected" && translator.source !== "gemini"));
        await publishEvidence(
          deps,
          attemptId,
          completed ? "SUCCEEDED" : usage ? "PARTIAL" : "UNKNOWN",
          completed && usage ? "FINAL" : usage ? "PARTIAL" : "UNKNOWN",
          usage,
          code,
          undefined,
          success ? "SUCCEEDED" : "UNKNOWN",
        );
        if (success) await afterCommit?.();
      } finally {
        await reader.cancel().catch(() => undefined);
        reader.releaseLock();
        await cleanup();
      }
    })();
    return settlement;
  };
  const consume = async (event: SSEEvent & { bytes: number }) => {
    reserveMemory?.(-event.bytes);
    prepared.reservedBytes -= event.bytes;
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
            if (eof) {
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
              continue;
            }
            idleTimer = setTimeout(
              () => controller.abort(new Error("Upstream stream idle")),
              deps.config.STREAM_IDLE_MS,
            );
            const chunk = await reader.read();
            clearTimeout(idleTimer);
            if (chunk.done) {
              eof = true;
              parseChunk(prepared, new Uint8Array(), false, reserveMemory, true);
            } else {
              if (chunk.value.byteLength > deps.config.MAX_EVENT_BYTES * 2)
                throw new GatewayError(502, "stream_chunk_limit", "Upstream chunk exceeds limit");
              parseChunk(prepared, chunk.value, false, reserveMemory);
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
