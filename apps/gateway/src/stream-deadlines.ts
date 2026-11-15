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
): ReadableStream<Uint8Array> {
  let held = first,
    timer: ReturnType<typeof setTimeout> | undefined;
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      clearTimeout(timer);
      let value = held;
      held = undefined;
      if (!value) {
        const next = await chunks.next();
        if (next.done) return controller.close();
        value = next.value;
      }
      controller.enqueue(value);
      timer = setTimeout(() => {
        stalled();
        controller.error(new Error("Downstream stalled"));
        void chunks.return(undefined);
      }, stallMs);
    },
    async cancel() {
      clearTimeout(timer);
      await chunks.return(undefined);
    },
  });
}
