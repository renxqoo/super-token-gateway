import { GatewayError } from "@gateway/contracts";

type StreamWriter = {
  enqueue(chunk: Uint8Array): Promise<void>;
  close(): void;
};

export function createStreamRelay(
  execute: (writer: StreamWriter) => Promise<void>,
  options: {
    signal: AbortSignal;
    cancel: () => void;
    fail: () => Promise<unknown>;
    abortedFrame: () => Uint8Array | undefined;
    finish: () => void;
  },
): ReadableStream<Uint8Array> {
  let resume: (() => void) | undefined;
  let cancelled = false;
  let running: Promise<void>;
  const wake = () => {
    const unblock = resume;
    resume = undefined;
    unblock?.();
  };
  return new ReadableStream<Uint8Array>(
    {
      start(controller) {
        options.signal.addEventListener("abort", wake, { once: true });
        let closed = false;
        const close = () => {
          if (cancelled || closed) return;
          closed = true;
          if (options.signal.aborted) {
            const failure = options.abortedFrame();
            if (failure && (controller.desiredSize ?? 0) <= 0) {
              controller.error(options.signal.reason);
              return;
            }
            if (failure) controller.enqueue(failure);
          }
          controller.close();
        };
        running = execute({
          async enqueue(chunk) {
            if (!cancelled && !options.signal.aborted && (controller.desiredSize ?? 0) <= 0) {
              await new Promise<void>((resolve) => {
                resume = resolve;
              });
            }
            if (cancelled || options.signal.aborted) {
              throw options.signal.reason ?? new Error("Client cancelled the stream");
            }
            controller.enqueue(chunk);
          },
          close,
        })
          .then(close)
          .catch(async (error: unknown) => {
            if (!cancelled && !closed) {
              if (options.signal.aborted) close();
              else controller.error(error);
            }
            await options.fail();
          })
          .finally(() => {
            options.signal.removeEventListener("abort", wake);
            options.finish();
          });
      },
      pull: wake,
      cancel() {
        cancelled = true;
        options.cancel();
        wake();
        return running;
      },
    },
    new ByteLengthQueuingStrategy({ highWaterMark: 64 * 1024 }),
  );
}

export function createTerminalBuffer() {
  const frames: Uint8Array[] = [];
  let size = 0;
  return {
    push(...chunks: Uint8Array[]) {
      for (const chunk of chunks) {
        size += chunk.byteLength;
        if (size > 2 * 1024 * 1024) {
          throw new GatewayError(
            502,
            "stream_interrupted",
            "Upstream terminal frames exceed limit",
          );
        }
        frames.push(chunk);
      }
    },
    get length() {
      return frames.length;
    },
    [Symbol.iterator]() {
      return frames.values();
    },
  };
}

export async function readStreamChunk(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  timeoutMs: number,
  onTimeout: () => void,
) {
  const timer = setTimeout(onTimeout, timeoutMs);
  timer.unref();
  try {
    return await reader.read();
  } finally {
    clearTimeout(timer);
  }
}

export async function peekStream<T>(
  source: ReadableStream<Uint8Array>,
  inspect: (chunk: Uint8Array) => T | undefined,
): Promise<{ body: ReadableStream<Uint8Array>; found: T | undefined }> {
  const reader = source.getReader();
  const buffered: Uint8Array[] = [];
  let total = 0;
  let found: T | undefined;
  let done = false;
  try {
    while (found === undefined) {
      const step = await reader.read();
      if (step.done) {
        done = true;
        break;
      }
      total += step.value.byteLength;
      if (total > 1024 * 1024) {
        throw new GatewayError(
          502,
          "upstream_stream_error",
          "Upstream stream prelude exceeds limit",
        );
      }
      buffered.push(step.value);
      found = inspect(step.value);
    }
  } catch (error) {
    await reader.cancel(error).catch(() => undefined);
    reader.releaseLock();
    throw error;
  }

  let offset = 0;
  let released = false;
  let cancelled = false;
  const release = () => {
    if (released) return;
    released = true;
    reader.releaseLock();
  };
  const body = new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        const initial = buffered[offset];
        if (initial) {
          offset++;
          controller.enqueue(initial);
          if (offset === buffered.length) {
            buffered.length = 0;
            offset = 0;
          }
          return;
        }
        try {
          if (!done) {
            const next = await reader.read();
            if (!next.done) {
              controller.enqueue(next.value);
              return;
            }
          }
          release();
          if (!cancelled) controller.close();
        } catch (error) {
          release();
          if (!cancelled) controller.error(error);
        }
      },
      async cancel(reason) {
        cancelled = true;
        await reader.cancel(reason).catch(() => undefined);
        release();
      },
    },
    { highWaterMark: 0 },
  );
  return { body, found };
}
