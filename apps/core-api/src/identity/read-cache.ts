type Entry<T> = { value: T; expiresAt: number };
type Pending<T> = { promise: Promise<T | undefined>; controller: AbortController };

function waitForRead<T>(read: Pending<T>, signal?: AbortSignal): Promise<T | undefined> {
  return new Promise((resolve, reject) => {
    const signals = signal ? [signal, read.controller.signal] : [read.controller.signal];
    let finished = false;
    function complete(settle: () => void) {
      if (finished) return;
      finished = true;
      for (const source of signals) source.removeEventListener("abort", abort);
      settle();
    }
    function abort() {
      complete(() =>
        reject(
          signals.find((source) => source.aborted)?.reason ??
            new Error("identity_store_unavailable"),
        ),
      );
    }
    read.promise.then(
      (value) => complete(() => resolve(value)),
      (error) => complete(() => reject(error)),
    );
    for (const source of signals) source.addEventListener("abort", abort, { once: true });
    if (signals.some((source) => source.aborted)) abort();
  });
}

export function createIdentityReadCache<T>(options: {
  capacity: number;
  maxPending: number;
  ttlMs: number;
  load: (key: string, signal: AbortSignal) => Promise<T | undefined>;
  loadTimeoutMs?: number;
  now?: () => number;
}) {
  const entries = new Map<string, Entry<T>>();
  const pending = new Map<string, Pending<T>>();
  const now = options.now ?? (() => performance.now());
  let generation = 0;
  let closed = false;
  return {
    async get(key: string, signal?: AbortSignal): Promise<T | undefined> {
      if (closed) throw new Error("identity_store_unavailable");
      signal?.throwIfAborted();
      const cached = entries.get(key);
      if (cached && cached.expiresAt > now()) {
        entries.delete(key);
        entries.set(key, cached);
        return cached.value;
      }
      entries.delete(key);
      const existing = pending.get(key);
      if (existing) return waitForRead(existing, signal);
      if (pending.size >= options.maxPending) throw new Error("internal_capacity");
      const started = now();
      const version = generation;
      const controller = new AbortController();
      let read: Pending<T>;
      const timer = setTimeout(
        () => controller.abort(new Error("identity_store_unavailable")),
        options.loadTimeoutMs ?? 4000,
      );
      const loading = Promise.resolve()
        .then(() => options.load(key, controller.signal))
        .then((value) => {
          if (
            value !== undefined &&
            !controller.signal.aborted &&
            !closed &&
            version === generation &&
            started + options.ttlMs > now()
          ) {
            if (entries.size >= options.capacity) entries.delete(entries.keys().next().value!);
            entries.set(key, { value, expiresAt: started + options.ttlMs });
          }
          return value;
        })
        .finally(() => {
          clearTimeout(timer);
          if (pending.get(key) === read) pending.delete(key);
        });
      read = { promise: loading, controller };
      pending.set(key, read);
      return waitForRead(read, signal);
    },
    invalidate() {
      generation++;
      entries.clear();
    },
    close() {
      closed = true;
      generation++;
      entries.clear();
      for (const read of pending.values())
        read.controller.abort(new Error("identity_store_unavailable"));
    },
    stats: () => ({ entries: entries.size, pending: pending.size }),
  };
}
