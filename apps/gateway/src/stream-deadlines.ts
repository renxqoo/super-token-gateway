export function idleGuard(
  body: ReadableStream<Uint8Array> | null,
  idleMs: number,
  expire: () => void,
): ReadableStream<Uint8Array> {
  const reader = body?.getReader();
  return new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        if (!reader) return controller.close();
        const timer = setTimeout(expire, idleMs);
        const next = await reader.read().finally(() => clearTimeout(timer));
        if (next.done) controller.close();
        else controller.enqueue(next.value);
      },
      cancel: (reason) => reader?.cancel(reason),
    },
    { highWaterMark: 0 },
  );
}

export function stallGuard(
  chunks: AsyncGenerator<Uint8Array>,
  first: Uint8Array | undefined,
  stallMs: number,
  stalled: () => void,
  signal?: AbortSignal,
): ReadableStream<Uint8Array> {
  let held = first,
    timer: ReturnType<typeof setTimeout> | undefined,
    onAbort: (() => void) | undefined,
    closed = false;
  function finish() {
    closed = true;
    held = undefined;
    clearTimeout(timer);
    if (onAbort) signal?.removeEventListener("abort", onAbort);
  }
  return new ReadableStream<Uint8Array>({
    start(controller) {
      if (!signal) return;
      onAbort = () => {
        if (closed) return;
        finish();
        controller.error(signal.reason);
        void chunks.return(undefined).catch(() => undefined);
      };
      if (signal.aborted) onAbort();
      else signal.addEventListener("abort", onAbort, { once: true });
    },
    async pull(controller) {
      clearTimeout(timer);
      let value = held;
      held = undefined;
      if (!value) {
        try {
          const next = await chunks.next();
          if (closed) return;
          if (next.done) {
            finish();
            return controller.close();
          }
          value = next.value;
        } catch (error) {
          finish();
          throw error;
        }
      }
      controller.enqueue(value);
      timer = setTimeout(() => {
        if (closed) return;
        finish();
        stalled();
        controller.error(new Error("Downstream stalled"));
        void chunks.return(undefined).catch(() => undefined);
      }, stallMs);
    },
    async cancel() {
      finish();
      await chunks.return(undefined);
    },
  });
}
