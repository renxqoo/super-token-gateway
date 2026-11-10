export function boundedRelay(
  body: ReadableStream<Uint8Array>,
  signal: AbortSignal,
  stallMs: number,
  totalMs: number,
  onClose: () => void | Promise<void> = () => undefined,
): ReadableStream<Uint8Array> {
  const reader = body.getReader();
  let writer: ReadableStreamDefaultController<Uint8Array>;
  let closed = false;
  let stall: ReturnType<typeof setTimeout>;
  let pendingClose: Promise<void> | undefined;
  const finish = (reason?: unknown): Promise<void> => {
    if (pendingClose) return pendingClose;
    closed = true;
    clearTimeout(stall);
    clearTimeout(total);
    signal.removeEventListener("abort", abort);
    pendingClose = (async () => {
      await reader.cancel(reason).catch(() => undefined);
      reader.releaseLock();
      await onClose();
    })();
    return pendingClose;
  };
  const fail = (reason: unknown) => {
    if (closed) return;
    writer.error(reason);
    void finish(reason).catch(() => undefined);
  };
  const abort = () => fail(signal.reason ?? new Error("Client disconnected"));
  const reset = () => {
    clearTimeout(stall);
    stall = setTimeout(() => fail(new Error("Relay made no progress")), stallMs);
  };
  const total = setTimeout(() => fail(new Error("Relay deadline reached")), totalMs);
  signal.addEventListener("abort", abort, { once: true });
  return new ReadableStream<Uint8Array>(
    {
      start(controller) {
        writer = controller;
        reset();
        if (signal.aborted) abort();
      },
      async pull(controller) {
        if (closed) return;
        reset();
        try {
          const value = await reader.read();
          if (closed) return;
          if (value.done) {
            await finish();
            controller.close();
          } else {
            controller.enqueue(value.value);
            reset();
          }
        } catch (error) {
          fail(error);
        }
      },
      async cancel(reason) {
        await finish(reason);
      },
    },
    { highWaterMark: 65536, size: (chunk) => chunk?.byteLength ?? 0 },
  );
}
