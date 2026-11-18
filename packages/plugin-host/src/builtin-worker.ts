import { ownPluginModule, pluginModuleInfo, type PluginModule } from "@gateway/plugin-sdk";
import { PluginHostError } from "./supervisor.ts";

type Reply = { output: Uint8Array<ArrayBuffer>; state: Uint8Array<ArrayBuffer> };
type Job = {
  id: string;
  hash: string;
  input: Uint8Array;
  state: Uint8Array | undefined;
  bytes: number;
  deadline: number;
  timer: ReturnType<typeof setTimeout>;
  signal: AbortSignal | undefined;
  abort: () => void;
  resolve: (value: Reply) => void;
  reject: (error: PluginHostError) => void;
  slot?: Slot;
};
type Slot = {
  worker: Worker;
  ready: boolean;
  retiring: boolean;
  job?: Job;
  closed: Promise<void>;
  didClose: () => void;
};

export function createBuiltinWorkerExecutor(options: {
  code: Uint8Array;
  digest: string;
  modules: readonly string[];
  slots?: number;
  maxPending?: number;
  maxPendingBytes?: number;
  callMs?: number;
}) {
  const limits = {
    slots: options.slots ?? 4,
    maxPending: options.maxPending ?? 128,
    maxPendingBytes: options.maxPendingBytes ?? 16 << 20,
    callMs: options.callMs ?? 1000,
  };
  for (const [key, value] of Object.entries(limits))
    if (
      !Number.isSafeInteger(value) ||
      value < 1 ||
      value >
        {
          slots: 16,
          maxPending: 1024,
          maxPendingBytes: 256 << 20,
          callMs: 30000,
        }[key]!
    )
      throw new PluginHostError("CONFIG_INVALID");
  if (options.code.length > 16 << 20) throw new PluginHostError("WORKER_ARTIFACT_INVALID");
  const code = new Uint8Array(options.code);
  if (
    !/^[a-f0-9]{64}$/.test(options.digest) ||
    new Bun.CryptoHasher("sha256").update(code).digest("hex") !== options.digest ||
    !options.modules.length ||
    options.modules.length > 16 ||
    options.modules.some((hash) => !/^[a-f0-9]{64}$/.test(hash))
  )
    throw new PluginHostError("WORKER_ARTIFACT_INVALID");
  const modules = new Set(options.modules);
  const url = URL.createObjectURL(new Blob([code], { type: "text/javascript" }));
  const slots = new Set<Slot>();
  const pending = new Map<string, Job>();
  const queue: Job[] = [];
  const observers = new Set<(connection: string) => void>();
  const connection = crypto.randomUUID();
  const starts: number[] = [];
  let sequence = 0,
    started = 0,
    pendingBytes = 0,
    closed = false;
  let closing: Promise<void> | undefined;

  function settle(job: Job, error?: PluginHostError, value?: Reply) {
    if (!pending.delete(job.id)) return;
    pendingBytes -= job.bytes;
    clearTimeout(job.timer);
    job.signal?.removeEventListener("abort", job.abort);
    const at = queue.indexOf(job);
    if (at >= 0) queue.splice(at, 1);
    if (job.slot?.job === job) delete job.slot.job;
    job.input = new Uint8Array();
    job.state = undefined;
    if (error) job.reject(error);
    else job.resolve(value!);
  }

  function retire(slot: Slot, error: PluginHostError) {
    if (slot.retiring) return;
    slot.retiring = true;
    if (slot.job) settle(slot.job, error);
    slot.worker.terminate();
  }

  function createSlot(): Slot {
    const now = performance.now();
    while (starts.length && starts[0]! <= now - 1000) starts.shift();
    if (starts.length >= 16) throw new PluginHostError("RESTART_BUDGET");
    starts.push(now);
    started++;
    const worker = new Worker(url, { ref: true, smol: true, env: {} });
    const done = Promise.withResolvers<void>();
    const slot: Slot = {
      worker,
      ready: false,
      retiring: false,
      closed: done.promise,
      didClose: done.resolve,
    };
    slots.add(slot);
    const onClose = () => {
      if (slot.job) settle(slot.job, new PluginHostError("WORKER_EXITED"));
      slots.delete(slot);
      worker.onmessage = null;
      worker.onerror = null;
      worker.removeEventListener("messageerror", onMessageError);
      worker.removeEventListener("close", onClose);
      slot.didClose();
      pump();
    };
    const onMessageError = () => retire(slot, new PluginHostError("PROTOCOL_INVALID"));
    worker.addEventListener("close", onClose);
    worker.addEventListener("messageerror", onMessageError);
    worker.onerror = (event) => {
      event.preventDefault();
      retire(slot, new PluginHostError("WORKER_FAILED"));
    };
    worker.onmessage = ({ data }) => {
      if (slot.retiring) return;
      if (data?.version === 1 && data.event === "ready" && !slot.ready) {
        slot.ready = true;
        dispatch(slot);
        return;
      }
      const job = slot.job;
      if (!slot.ready || !job || data?.version !== 1 || data.id !== job.id) {
        retire(slot, new PluginHostError("PROTOCOL_INVALID"));
        return;
      }
      if (performance.now() >= job.deadline) {
        retire(slot, new PluginHostError("CALL_TIMEOUT"));
        return;
      }
      if (data.error === "GUEST_ERROR" || data.error === "OUTPUT_BUDGET") {
        settle(job, new PluginHostError(data.error));
      } else if (
        !(data.output instanceof Uint8Array) ||
        !(data.state instanceof Uint8Array) ||
        !(data.output.buffer instanceof ArrayBuffer) ||
        !(data.state.buffer instanceof ArrayBuffer) ||
        data.output.buffer.byteLength > 256 << 10 ||
        data.state.buffer.byteLength > 256 << 10 ||
        data.output.length > 256 << 10 ||
        data.state.length > 256 << 10 ||
        data.error !== undefined
      ) {
        retire(slot, new PluginHostError("PROTOCOL_INVALID"));
        return;
      } else {
        settle(job, undefined, {
          output:
            data.output.byteLength === data.output.buffer.byteLength
              ? data.output
              : new Uint8Array(data.output),
          state:
            data.state.byteLength === data.state.buffer.byteLength
              ? data.state
              : new Uint8Array(data.state),
        });
      }
      pump();
    };
    return slot;
  }

  function dispatch(slot: Slot) {
    const job = slot.job;
    if (!job || !slot.ready || slot.retiring) return;
    if (job.signal?.aborted || performance.now() >= job.deadline) {
      settle(job, new PluginHostError(job.signal?.aborted ? "CALL_CANCELLED" : "CALL_TIMEOUT"));
      pump();
      return;
    }
    try {
      slot.worker.postMessage({
        version: 1,
        id: job.id,
        module: job.hash,
        input: job.input,
        state: job.state,
      });
    } catch {
      retire(slot, new PluginHostError("WORKER_FAILED"));
    }
  }

  function pump() {
    if (closed) return;
    while (queue.length) {
      let slot = [...slots].find((item) => !item.retiring && !item.job);
      if (!slot && slots.size >= limits.slots) return;
      const job = queue.shift()!;
      if (job.signal?.aborted || performance.now() >= job.deadline) {
        settle(job, new PluginHostError(job.signal?.aborted ? "CALL_CANCELLED" : "CALL_TIMEOUT"));
        continue;
      }
      if (!slot) {
        try {
          slot = createSlot();
        } catch (error) {
          settle(
            job,
            error instanceof PluginHostError ? error : new PluginHostError("WORKER_FAILED"),
          );
          continue;
        }
      }
      slot.job = job;
      job.slot = slot;
      dispatch(slot);
    }
  }

  return {
    onDisconnect(listener: (value: string) => void) {
      if (closed) throw new PluginHostError("HOST_CLOSED");
      observers.add(listener);
      return () => observers.delete(listener);
    },
    async load(module: Uint8Array | PluginModule, signal?: AbortSignal) {
      if (closed) throw new PluginHostError("HOST_CLOSED");
      if (signal?.aborted) throw new PluginHostError("CALL_CANCELLED");
      if (module instanceof Uint8Array && module.length > 8 << 20)
        throw new PluginHostError("MODULE_BUDGET");
      const hash = pluginModuleInfo(
        module instanceof Uint8Array ? ownPluginModule(module) : module,
      ).hash;
      if (!modules.has(hash)) throw new PluginHostError("MODULE_MISSING");
      return { hash, connection };
    },
    call(
      hash: string,
      input: Uint8Array,
      state: Uint8Array | undefined,
      config: { signal?: AbortSignal; timeoutMs?: number; function?: string } = {},
    ) {
      if (closed) return Promise.reject(new PluginHostError("HOST_CLOSED"));
      if (config.signal?.aborted) return Promise.reject(new PluginHostError("CALL_CANCELLED"));
      if (!modules.has(hash)) return Promise.reject(new PluginHostError("MODULE_MISSING"));
      if (config.function !== undefined && config.function !== "invoke")
        return Promise.reject(new PluginHostError("COMMAND_MISSING"));
      if (input.length > 256 << 10 || (state?.length ?? 0) > 256 << 10)
        return Promise.reject(new PluginHostError("INPUT_BUDGET"));
      const timeoutMs = Math.min(config.timeoutMs ?? limits.callMs, limits.callMs);
      if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1)
        return Promise.reject(new PluginHostError("DEADLINE"));
      const bytes = input.length + (state?.length ?? 0);
      if (pending.size >= limits.maxPending || pendingBytes + bytes > limits.maxPendingBytes)
        return Promise.reject(new PluginHostError("PENDING_BUDGET"));
      return new Promise<Reply>((resolve, reject) => {
        const job: Job = {
          id: String(++sequence),
          hash,
          input: new Uint8Array(input),
          state: state ? new Uint8Array(state) : undefined,
          bytes,
          deadline: performance.now() + timeoutMs,
          signal: config.signal,
          resolve,
          reject,
          timer: setTimeout(() => cancel("CALL_TIMEOUT"), timeoutMs),
          abort: () => cancel("CALL_CANCELLED"),
        };
        function cancel(code: string) {
          if (!pending.has(job.id)) return;
          if (job.slot) retire(job.slot, new PluginHostError(code));
          else settle(job, new PluginHostError(code));
          pump();
        }
        pending.set(job.id, job);
        pendingBytes += bytes;
        config.signal?.addEventListener("abort", job.abort, { once: true });
        queue.push(job);
        pump();
      });
    },
    snapshot: () => ({
      started,
      workers: slots.size,
      retiring: [...slots].filter((slot) => slot.retiring).length,
      active: [...slots].filter((slot) => slot.job).length,
      queued: queue.length,
      pending: pending.size,
      pendingBytes,
      timers: pending.size,
      closed,
    }),
    close(): Promise<void> {
      if (closing) return closing;
      closed = true;
      const done = Promise.withResolvers<void>();
      closing = done.promise;
      for (const job of pending.values()) settle(job, new PluginHostError("HOST_CLOSED"));
      for (const observer of observers) {
        try {
          observer(connection);
        } catch {}
      }
      observers.clear();
      void (async () => {
        try {
          const retiring = [...slots];
          for (const slot of retiring) retire(slot, new PluginHostError("HOST_CLOSED"));
          await Promise.all(retiring.map((slot) => slot.closed));
        } finally {
          URL.revokeObjectURL(url);
        }
      })().then(done.resolve, done.reject);
      return closing;
    },
  };
}
