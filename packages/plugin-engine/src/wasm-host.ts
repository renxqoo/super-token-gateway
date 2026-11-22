import { dlopen, FFIType, ptr, toArrayBuffer } from "bun:ffi";
import { createPublicKey, verify, type KeyObject } from "node:crypto";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import type {
  WasmCallableHook,
  WasmExecutorStats,
  WasmInvokeOptions,
  WasmInvokePriority,
  WasmRuntimeExecutor,
} from "./types.ts";

const DEFAULT_MAX_MEMORY = 32 * 1024 * 1024;
const DEFAULT_FUEL = 2_000_000;
const WASM_PAGE_BYTES = 65_536;
const MAX_CACHED_MODULES = 64;
const MAX_CACHED_MODULE_BYTES = 64 * 1024 * 1024;
const DEFAULT_COLD_COMPILE_TIMEOUT_MS = 5000;
const DEFAULT_INGRESS_SHED_RATIO = 0.8;

export class WasmCapacityError extends Error {
  readonly code: "wasm_queue_full" | "wasm_ingress_shed" | "wasm_queue_timeout" | "wasm_pool_limit";
  constructor(
    code: "wasm_queue_full" | "wasm_ingress_shed" | "wasm_queue_timeout" | "wasm_pool_limit",
    message: string,
  ) {
    super(message);
    this.name = "WasmCapacityError";
    this.code = code;
  }
}

export class WasmPluginFaultError extends Error {
  readonly code:
    "wasm_execution_failed" | "wasm_timeout" | "wasm_compile_failed" | "wasm_memory_limit";
  constructor(
    code: "wasm_execution_failed" | "wasm_timeout" | "wasm_compile_failed" | "wasm_memory_limit",
    message: string,
  ) {
    super(message);
    this.name = "WasmPluginFaultError";
    this.code = code;
  }
}

function readLebU32(bytes: Uint8Array, offset: number): { value: number; next: number } {
  let value = 0;
  let shift = 0;
  let pos = offset;
  while (pos < bytes.length && shift <= 35) {
    const b = bytes[pos++]!;
    value |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) return { value: value >>> 0, next: pos };
    shift += 7;
  }
  throw new WasmPluginFaultError("wasm_compile_failed", "Malformed WASM LEB128");
}

function inspectWasmInitialMemoryBytes(bytes: Uint8Array): number {
  if (
    bytes.byteLength < 8 ||
    bytes[0] !== 0x00 ||
    bytes[1] !== 0x61 ||
    bytes[2] !== 0x73 ||
    bytes[3] !== 0x6d
  ) {
    throw new WasmPluginFaultError("wasm_compile_failed", "Invalid WASM magic header");
  }
  let pos = 8;
  let maxInitialPages = 0;
  while (pos < bytes.byteLength) {
    const sectionId = bytes[pos++]!;
    const size = readLebU32(bytes, pos);
    pos = size.next;
    const end = pos + size.value;
    if (end > bytes.byteLength) {
      throw new WasmPluginFaultError("wasm_compile_failed", "Truncated WASM section");
    }
    if (sectionId === 5) {
      let cur = pos;
      const count = readLebU32(bytes, cur);
      cur = count.next;
      for (let i = 0; i < count.value; i++) {
        const flags = bytes[cur++]!;
        const initial = readLebU32(bytes, cur);
        cur = initial.next;
        if (initial.value > maxInitialPages) maxInitialPages = initial.value;
        if ((flags & 0x01) !== 0) {
          const max = readLebU32(bytes, cur);
          cur = max.next;
        }
      }
    }
    pos = end;
  }
  return maxInitialPages * WASM_PAGE_BYTES;
}

function buildSelfCheckWasm(): Uint8Array {
  const reply = new TextEncoder().encode('{"action":"PASS"}');
  const replyOff = 64;
  const packed = (BigInt(replyOff) << 32n) | BigInt(reply.length);
  const lebU = (n: number): number[] => {
    const out: number[] = [];
    let v = n >>> 0;
    do {
      let b = v & 0x7f;
      v >>>= 7;
      if (v !== 0) b |= 0x80;
      out.push(b);
    } while (v !== 0);
    return out;
  };
  const lebS64 = (v: bigint): number[] => {
    const out: number[] = [];
    let cur = v;
    for (;;) {
      let b = Number(cur & 0x7fn);
      cur >>= 7n;
      const done = (cur === 0n && (b & 0x40) === 0) || (cur === -1n && (b & 0x40) !== 0);
      if (!done) b |= 0x80;
      out.push(b);
      if (done) break;
    }
    return out;
  };
  const sec = (id: number, body: number[]) => [id, ...lebU(body.length), ...body];
  const str = (s: string) => {
    const bytes = [...new TextEncoder().encode(s)];
    return [...lebU(bytes.length), ...bytes];
  };
  const types = sec(1, [2, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e]);
  const funcs = sec(3, [2, 0, 1]);
  const mem = sec(5, [1, 0x00, 1]);
  const exps = sec(7, [
    3,
    ...str("memory"),
    2,
    0,
    ...str("stg_alloc"),
    0,
    0,
    ...str("on_request"),
    0,
    1,
  ]);
  const b0 = [0, 0x41, ...lebS64(512n), 0x0b];
  const b1 = [0, 0x42, ...lebS64(packed), 0x0b];
  const code = sec(10, [2, ...lebU(b0.length), ...b0, ...lebU(b1.length), ...b1]);
  const data = sec(11, [
    1,
    0,
    0x41,
    ...lebS64(BigInt(replyOff)),
    0x0b,
    ...lebU(reply.length),
    ...reply,
  ]);
  return new Uint8Array([
    0x00,
    0x61,
    0x73,
    0x6d,
    1,
    0,
    0,
    0,
    ...types,
    ...funcs,
    ...mem,
    ...exps,
    ...code,
    ...data,
  ]);
}

const SELF_CHECK_WASM = buildSelfCheckWasm();
const SELF_CHECK_DIGEST = new Bun.CryptoHasher("sha256").update(SELF_CHECK_WASM).digest("hex");

type WorkerReply = {
  seq: number;
  ok: boolean;
  compiled?: boolean;
  compileFailed?: boolean;
  faultKind?: "host_capacity" | "plugin_fault";
  out?: Uint8Array;
  error?: string;
};

type PoolSlot = {
  worker: Worker;
  compiledDigests: Map<string, { digest: string; bytes: number }>;
  compiledBytes: number;
  onReply: ((data: WorkerReply) => void) | null;
  onFault: ((message: string) => void) | null;
  onClose: ((error: Error) => void) | null;
};

type QueueWaiter = {
  tenant: string;
  priority: WasmInvokePriority;
  serve: (slot: PoolSlot | Error) => void;
};

class FairTenantQueue {
  private readonly buckets = new Map<string, QueueWaiter[]>();
  private count = 0;

  get size(): number {
    return this.count;
  }

  enqueue(waiter: QueueWaiter): void {
    let list = this.buckets.get(waiter.tenant);
    if (!list) {
      list = [];
      this.buckets.set(waiter.tenant, list);
    }
    list.push(waiter);
    this.count++;
  }

  remove(waiter: QueueWaiter): boolean {
    const list = this.buckets.get(waiter.tenant);
    if (!list) return false;
    const idx = list.indexOf(waiter);
    if (idx < 0) return false;
    list.splice(idx, 1);
    this.count--;
    if (list.length === 0) this.buckets.delete(waiter.tenant);
    return true;
  }

  dequeue(): QueueWaiter | undefined {
    const first = this.buckets.entries().next().value;
    if (!first) return undefined;
    const [tenant, list] = first;
    const waiter = list.shift()!;
    this.count--;
    this.buckets.delete(tenant);
    if (list.length > 0) this.buckets.set(tenant, list);
    return waiter;
  }

  drainAll(error: Error): void {
    const pending: QueueWaiter[] = [];
    for (const list of this.buckets.values()) {
      pending.push(...list);
    }
    this.buckets.clear();
    this.count = 0;
    for (const waiter of pending) waiter.serve(error);
  }
}

export type WasmtimeExecutorOptions = {
  wasmtimeLibraryPath?: string;
  maxMemoryBytes?: number;
  poolSize?: number;
  maxPending?: number;
  minTimeoutMs?: number;
  maxTimeoutMs?: number;
  coldCompileTimeoutMs?: number;
  ingressShedRatio?: number;
};

export function createWasmtimeExecutor(options: WasmtimeExecutorOptions = {}): WasmRuntimeExecutor {
  const defaultMaxMemory = options.maxMemoryBytes ?? DEFAULT_MAX_MEMORY;
  const poolSize = Math.max(1, Math.min(16, options.poolSize ?? 4));
  const maxPending = Math.max(poolSize, options.maxPending ?? poolSize * 64);
  const minTimeoutMs = options.minTimeoutMs ?? 25;
  const maxTimeoutMs = options.maxTimeoutMs ?? 2000;
  const coldCompileTimeoutMs = Math.max(
    maxTimeoutMs,
    options.coldCompileTimeoutMs ?? DEFAULT_COLD_COMPILE_TIMEOUT_MS,
  );
  const ingressShedRatio = Math.max(
    0.1,
    Math.min(1, options.ingressShedRatio ?? DEFAULT_INGRESS_SHED_RATIO),
  );
  const resolvedPath = options.wasmtimeLibraryPath
    ? resolve(options.wasmtimeLibraryPath)
    : resolveWasmtimeLibraryPath();
  if (!resolvedPath || !existsSync(resolvedPath)) {
    throw new Error(
      "Native Wasmtime library not found; build native/plugin-wasmtime or set STG_WASMTIME_LIB",
    );
  }
  const nativeLibrary = resolvedPath;
  const nativeWorkerFile = [
    new URL("./wasmtime-worker.js", import.meta.url),
    new URL("./wasmtime-worker.ts", import.meta.url),
  ].find((file) => existsSync(file));
  if (!nativeWorkerFile) {
    throw new Error("Native Wasmtime worker file is missing from this build");
  }

  const modules = new Map<string, { digest: string; bytes: Uint8Array; initialMemBytes: number }>();
  let cachedModuleBytes = 0;
  const slots = new Set<PoolSlot>();
  const idleSlots: PoolSlot[] = [];
  const egressQueue = new FairTenantQueue();
  const ingressQueue = new FairTenantQueue();
  let activeCount = 0;
  let rejectedTotal = 0;
  let shedTotal = 0;
  let timeoutTotal = 0;
  let trapTotal = 0;
  let compileTotal = 0;
  let closed = false;
  let nextSeq = 1;

  function totalQueued(): number {
    return egressQueue.size + ingressQueue.size;
  }

  function dequeueWaiter(): QueueWaiter | undefined {
    return egressQueue.dequeue() ?? ingressQueue.dequeue();
  }

  function setWorkerRef(worker: Worker, active: boolean): void {
    const w = worker as Worker & { ref?: () => void; unref?: () => void };
    if (active) w.ref?.();
    else w.unref?.();
  }

  function touchSlotLru(slot: PoolSlot, pluginId: string): void {
    const existing = slot.compiledDigests.get(pluginId);
    if (!existing) return;
    slot.compiledDigests.delete(pluginId);
    slot.compiledDigests.set(pluginId, existing);
  }

  function spawnSlot(): PoolSlot {
    const worker = new Worker(nativeWorkerFile!, {
      argv: [nativeLibrary, String(defaultMaxMemory)],
      env: {},
    });
    const slot: PoolSlot = {
      worker,
      compiledDigests: new Map(),
      compiledBytes: 0,
      onReply: null,
      onFault: null,
      onClose: null,
    };
    worker.onmessage = (event) => {
      slot.onReply?.(event.data as WorkerReply);
    };
    worker.onerror = (err) => {
      slot.onFault?.(err.message || "WASM worker crashed");
    };
    slots.add(slot);
    return slot;
  }

  async function acquireSlot(
    timeoutMs: number,
    priority: WasmInvokePriority,
    tenant: string,
    signal?: AbortSignal,
  ): Promise<PoolSlot> {
    signal?.throwIfAborted();
    if (closed) throw new WasmCapacityError("wasm_queue_full", "WASM executor is closed");
    const slot = idleSlots.pop() ?? (slots.size < poolSize ? spawnSlot() : undefined);
    if (slot) {
      activeCount++;
      setWorkerRef(slot.worker, true);
      return slot;
    }
    const queued = totalQueued();
    if (queued >= maxPending) {
      rejectedTotal++;
      throw new WasmCapacityError("wasm_queue_full", "WASM executor queue is full");
    }
    if (
      priority === "ingress" &&
      queued >= Math.max(1, Math.floor(maxPending * ingressShedRatio))
    ) {
      rejectedTotal++;
      shedTotal++;
      throw new WasmCapacityError(
        "wasm_ingress_shed",
        "WASM executor shedding ingress load to protect in-flight responses",
      );
    }
    const queue = priority === "egress" ? egressQueue : ingressQueue;
    return new Promise((resolveSlot, rejectSlot) => {
      let done = false;
      const clear = () => {
        clearTimeout(timer);
        queue.remove(waiter);
        signal?.removeEventListener("abort", abort);
      };
      const abort = () => {
        if (done) return;
        done = true;
        clear();
        rejectSlot(signal?.reason);
      };
      const serve = (res: PoolSlot | Error) => {
        if (done) return;
        done = true;
        clear();
        if (res instanceof Error) rejectSlot(res);
        else {
          activeCount++;
          setWorkerRef(res.worker, true);
          resolveSlot(res);
        }
      };
      const waiter: QueueWaiter = { tenant, priority, serve };
      const timer = setTimeout(() => {
        timeoutTotal++;
        serve(
          new WasmCapacityError("wasm_queue_timeout", `WASM queue timed out after ${timeoutMs}ms`),
        );
      }, timeoutMs);
      queue.enqueue(waiter);
      signal?.addEventListener("abort", abort, { once: true });
    });
  }

  function releaseSlot(slot: PoolSlot, healthy: boolean): void {
    slot.onReply = null;
    slot.onFault = null;
    slot.onClose = null;
    if (activeCount > 0) activeCount--;
    if (closed || !healthy) {
      slots.delete(slot);
      try {
        slot.worker.terminate();
      } catch {
        void 0;
      }
      while (!closed && totalQueued() > 0) {
        const nextWaiter = dequeueWaiter();
        if (!nextWaiter) break;
        try {
          nextWaiter.serve(spawnSlot());
          break;
        } catch (error) {
          nextWaiter.serve(error instanceof Error ? error : new Error(String(error)));
        }
      }
      return;
    }
    const nextWaiter = dequeueWaiter();
    if (nextWaiter) {
      nextWaiter.serve(slot);
      return;
    }
    setWorkerRef(slot.worker, false);
    idleSlots.push(slot);
  }

  function closeSlot(slot: PoolSlot): void {
    let terminated = false;
    const finish = () => {
      if (terminated) return;
      terminated = true;
      clearTimeout(forceTimer);
      releaseSlot(slot, false);
    };
    const forceTimer = setTimeout(finish, 500);
    forceTimer.unref?.();
    slot.onReply = (data) => {
      if (data.seq === 0) finish();
    };
    slot.onFault = finish;
    slot.onClose = null;
    try {
      slot.worker.postMessage({ seq: 0 });
    } catch {
      finish();
    }
  }

  const api: WasmRuntimeExecutor = {
    runtimeKind: "wasmtime",

    compile(pluginId: string, digest: string, wasmBytes: Uint8Array): void {
      if (closed) throw new WasmCapacityError("wasm_queue_full", "WASM executor is closed");
      const existing = modules.get(pluginId);
      if (existing && existing.digest === digest) {
        modules.delete(pluginId);
        modules.set(pluginId, existing);
        return;
      }
      if (wasmBytes.byteLength > MAX_CACHED_MODULE_BYTES) {
        throw new WasmPluginFaultError("wasm_compile_failed", "WASM module exceeds cache budget");
      }
      const initialMemBytes = inspectWasmInitialMemoryBytes(wasmBytes);
      if (initialMemBytes > defaultMaxMemory) {
        throw new WasmPluginFaultError(
          "wasm_memory_limit",
          `WASM initial memory ${initialMemBytes} exceeds limit ${defaultMaxMemory}`,
        );
      }
      if (existing) {
        cachedModuleBytes -= existing.bytes.byteLength;
        modules.delete(pluginId);
      }
      while (
        modules.size >= MAX_CACHED_MODULES ||
        cachedModuleBytes + wasmBytes.byteLength > MAX_CACHED_MODULE_BYTES
      ) {
        const [expiredId, expired] = modules.entries().next().value!;
        cachedModuleBytes -= expired.bytes.byteLength;
        modules.delete(expiredId);
      }
      cachedModuleBytes += wasmBytes.byteLength;
      modules.set(pluginId, {
        digest,
        bytes: new Uint8Array(wasmBytes),
        initialMemBytes,
      });
    },

    async invoke(
      pluginId: string,
      hook: WasmCallableHook,
      input: Uint8Array,
      invokeOptions: WasmInvokeOptions,
    ): Promise<Uint8Array> {
      const { signal } = invokeOptions;
      signal?.throwIfAborted();
      const entry = modules.get(pluginId);
      if (!entry) {
        throw new WasmPluginFaultError(
          "wasm_compile_failed",
          `WASM plugin not compiled: ${pluginId}`,
        );
      }
      modules.delete(pluginId);
      modules.set(pluginId, entry);
      const effectiveMaxMem = Math.min(defaultMaxMemory, invokeOptions.maxMemoryBytes);
      if (entry.initialMemBytes > effectiveMaxMem) {
        throw new WasmPluginFaultError(
          "wasm_memory_limit",
          `WASM initial memory ${entry.initialMemBytes} exceeds limit`,
        );
      }
      const timeoutMs = Math.max(
        minTimeoutMs,
        Math.min(maxTimeoutMs, Math.ceil(invokeOptions.fuelBudget / 10_000)),
      );
      const priority = invokeOptions.priority ?? "ingress";
      const tenant = invokeOptions.tenantId ?? "default";
      const queuedAt = performance.now();
      const slot = await acquireSlot(timeoutMs, priority, tenant, signal);
      const previous = slot.compiledDigests.get(pluginId);
      const needsBytes = previous?.digest !== entry.digest;
      if (!needsBytes) touchSlotLru(slot, pluginId);
      const compileBudget = Math.max(
        timeoutMs,
        invokeOptions.compileTimeoutMs ?? coldCompileTimeoutMs,
      );
      const executionTimeoutMs = needsBytes ? compileBudget : timeoutMs;
      const timeoutError = new WasmPluginFaultError(
        "wasm_timeout",
        `WASM execution timed out after ${executionTimeoutMs}ms`,
      );
      const deadline = queuedAt + executionTimeoutMs;
      const remainingMs = Math.ceil(deadline - performance.now());
      if (closed || remainingMs <= 0 || signal?.aborted) {
        if (!closed) releaseSlot(slot, true);
        if (closed) throw new WasmCapacityError("wasm_queue_full", "WASM executor is closed");
        signal?.throwIfAborted();
        timeoutTotal++;
        throw timeoutError;
      }
      const seq = nextSeq++;
      const dropIds: string[] = [];
      if (needsBytes) {
        if (previous) {
          slot.compiledDigests.delete(pluginId);
          slot.compiledBytes -= previous.bytes;
        }
        while (
          slot.compiledDigests.size >= MAX_CACHED_MODULES ||
          slot.compiledBytes + entry.bytes.byteLength > MAX_CACHED_MODULE_BYTES
        ) {
          const [expiredId, expired] = slot.compiledDigests.entries().next().value!;
          slot.compiledDigests.delete(expiredId);
          slot.compiledBytes -= expired.bytes;
          dropIds.push(expiredId);
        }
      }

      return new Promise<Uint8Array>((resolveInvoke, rejectInvoke) => {
        let settled = false;
        const cancelWait = () => {
          clearTimeout(timer);
          signal?.removeEventListener("abort", abort);
        };
        const cleanupHandlers = () => {
          cancelWait();
          slot.onReply = null;
          slot.onFault = null;
          slot.onClose = null;
        };
        const interrupt = (reason: unknown, isTimeout: boolean) => {
          if (settled) return;
          settled = true;
          if (isTimeout) timeoutTotal++;
          cancelWait();
          rejectInvoke(reason);
        };
        const abort = () => interrupt(signal?.reason, false);
        const timer = setTimeout(() => interrupt(timeoutError, true), remainingMs);
        signal?.addEventListener("abort", abort, { once: true });

        slot.onReply = (data) => {
          if (data.seq !== seq) return;
          if (data.compiled) compileTotal++;
          if (needsBytes && !data.compileFailed) {
            slot.compiledDigests.set(pluginId, {
              digest: entry.digest,
              bytes: entry.bytes.byteLength,
            });
            slot.compiledBytes += entry.bytes.byteLength;
          } else if (data.compileFailed && !needsBytes && previous) {
            slot.compiledDigests.delete(pluginId);
            slot.compiledBytes -= previous.bytes;
          }
          if (settled) {
            releaseSlot(slot, true);
            return;
          }
          settled = true;
          cleanupHandlers();
          if (data.ok) {
            releaseSlot(slot, true);
            if (performance.now() >= deadline) {
              timeoutTotal++;
              rejectInvoke(timeoutError);
            } else {
              resolveInvoke(data.out ? new Uint8Array(data.out) : new Uint8Array(0));
            }
          } else {
            releaseSlot(slot, true);
            const message = data.error ?? "WASM invoke failed";
            if (data.faultKind === "host_capacity") {
              rejectedTotal++;
              rejectInvoke(new WasmCapacityError("wasm_pool_limit", message));
            } else if (data.compileFailed) {
              trapTotal++;
              rejectInvoke(new WasmPluginFaultError("wasm_compile_failed", message));
            } else if (message.includes("(-8)") || message.includes("memory limit")) {
              trapTotal++;
              rejectInvoke(new WasmPluginFaultError("wasm_memory_limit", message));
            } else {
              trapTotal++;
              rejectInvoke(new WasmPluginFaultError("wasm_execution_failed", message));
            }
          }
        };

        slot.onFault = (message) => {
          const alreadySettled = settled;
          settled = true;
          cleanupHandlers();
          trapTotal++;
          if (!alreadySettled) {
            rejectInvoke(new WasmPluginFaultError("wasm_execution_failed", message));
          }
          if (!closed) releaseSlot(slot, false);
        };

        slot.onClose = (closeError) => {
          const alreadySettled = settled;
          settled = true;
          cleanupHandlers();
          if (!alreadySettled) rejectInvoke(closeError);
        };

        try {
          slot.worker.postMessage({
            seq,
            id: pluginId,
            digest: entry.digest,
            dropIds,
            bytes: needsBytes ? entry.bytes : null,
            hook,
            input,
            fuelBudget: invokeOptions.fuelBudget,
            maxMemoryBytes: effectiveMaxMem,
          });
        } catch (error) {
          slot.onFault(error instanceof Error ? error.message : String(error));
        }
      });
    },

    async selfCheck(): Promise<void> {
      const checkId = "__stg_wasmtime_self_check__";
      api.compile(checkId, SELF_CHECK_DIGEST, SELF_CHECK_WASM);
      const out = await api.invoke(checkId, "on_request", new Uint8Array(), {
        fuelBudget: 100_000,
        maxMemoryBytes: 1024 * 1024,
        priority: "egress",
        compileTimeoutMs: coldCompileTimeoutMs,
      });
      const parsed = JSON.parse(new TextDecoder().decode(out)) as { action?: string };
      if (parsed.action !== "PASS") {
        throw new Error("Native Wasmtime self-check returned unexpected result");
      }
      modules.delete(checkId);
      cachedModuleBytes = Math.max(0, cachedModuleBytes - SELF_CHECK_WASM.byteLength);
    },

    stats(): WasmExecutorStats {
      return {
        runtime: "wasmtime",
        poolSize,
        active: activeCount,
        queued: totalQueued(),
        queuedIngress: ingressQueue.size,
        queuedEgress: egressQueue.size,
        rejectedTotal,
        shedTotal,
        timeoutTotal,
        trapTotal,
        compileTotal,
      };
    },

    close(): void {
      closed = true;
      const error = new WasmCapacityError("wasm_queue_full", "WASM executor is closed");
      egressQueue.drainAll(error);
      ingressQueue.drainAll(error);
      idleSlots.length = 0;
      for (const slot of slots) {
        slot.onClose?.(error);
        closeSlot(slot);
      }
      modules.clear();
      cachedModuleBytes = 0;
    },
  };

  return api;
}

const publicKeyCache = new Map<string, KeyObject>();

function cachedPublicKey(pem: string): KeyObject {
  const hit = publicKeyCache.get(pem);
  if (hit) return hit;
  if (publicKeyCache.size >= 32) publicKeyCache.clear();
  const key = createPublicKey(pem);
  publicKeyCache.set(pem, key);
  return key;
}

export function verifyPluginSignature(
  digestHex: string,
  signatureBase64: string | null,
  publicKeyPem: string | undefined,
): boolean {
  if (!publicKeyPem) return true;
  if (!signatureBase64) return false;
  try {
    const pub = cachedPublicKey(publicKeyPem);
    return verify(
      null,
      Buffer.from(digestHex, "utf8"),
      pub,
      Buffer.from(signatureBase64, "base64"),
    );
  } catch {
    return false;
  }
}

export function verifyPluginIntegrity(
  plugin: {
    digest: string;
    wasm_bytes: Uint8Array | null;
    signature: string | null;
  },
  publicKeyPem: string | undefined,
): { ok: true } | { ok: false; reason: "missing_bytes" | "digest_mismatch" | "signature_invalid" } {
  if (!plugin.wasm_bytes) return { ok: false, reason: "missing_bytes" };
  const actualDigest = new Bun.CryptoHasher("sha256").update(plugin.wasm_bytes).digest("hex");
  if (actualDigest !== plugin.digest) return { ok: false, reason: "digest_mismatch" };
  if (!verifyPluginSignature(plugin.digest, plugin.signature, publicKeyPem)) {
    return { ok: false, reason: "signature_invalid" };
  }
  return { ok: true };
}

export function resolveWasmtimeLibraryPath(explicitPath?: string): string | null {
  const candidates = [
    explicitPath,
    process.env.STG_WASMTIME_LIB,
    new URL("../native/libstg_plugin_wasmtime.dylib", import.meta.url).pathname,
    new URL("../native/libstg_plugin_wasmtime.so", import.meta.url).pathname,
    new URL(
      "../../../native/plugin-wasmtime/target/release/libstg_plugin_wasmtime.dylib",
      import.meta.url,
    ).pathname,
    new URL(
      "../../../native/plugin-wasmtime/target/release/libstg_plugin_wasmtime.so",
      import.meta.url,
    ).pathname,
    new URL("../../../dist/native/libstg_plugin_wasmtime.dylib", import.meta.url).pathname,
    new URL("../../../dist/native/libstg_plugin_wasmtime.so", import.meta.url).pathname,
  ];
  for (const candidate of candidates) {
    if (candidate && existsSync(candidate)) return candidate;
  }
  return null;
}

export function createFfiWasmtimeExecutor(options: {
  libraryPath: string;
  maxMemoryBytes?: number;
  defaultFuel?: number;
}): WasmRuntimeExecutor & { evict(pluginId: string): void } {
  const lib = dlopen(options.libraryPath, {
    stg_wasm_engine_new: { args: [FFIType.u64, FFIType.u64], returns: FFIType.ptr },
    stg_wasm_engine_free: { args: [FFIType.ptr], returns: FFIType.void },
    stg_wasm_module_compile: {
      args: [FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64],
      returns: FFIType.ptr,
    },
    stg_wasm_module_free: { args: [FFIType.ptr], returns: FFIType.void },
    stg_wasm_invoke: {
      args: [
        FFIType.ptr,
        FFIType.ptr,
        FFIType.ptr,
        FFIType.u64,
        FFIType.ptr,
        FFIType.u64,
        FFIType.u64,
        FFIType.u64,
        FFIType.ptr,
        FFIType.ptr,
        FFIType.ptr,
        FFIType.u64,
      ],
      returns: FFIType.i32,
    },
    stg_wasm_buf_free: { args: [FFIType.ptr, FFIType.u64], returns: FFIType.void },
  });

  const maxMem = BigInt(options.maxMemoryBytes ?? DEFAULT_MAX_MEMORY);
  const defFuel = BigInt(options.defaultFuel ?? DEFAULT_FUEL);
  const engine = lib.symbols.stg_wasm_engine_new(maxMem, defFuel);
  if (!engine) throw new Error("Failed to initialize Rust wasmtime engine");

  const compiled = new Map<string, { digest: string; handle: NonNullable<typeof engine> }>();
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  function readCString(buf: Uint8Array): string {
    const zero = buf.indexOf(0);
    return decoder.decode(zero >= 0 ? buf.subarray(0, zero) : buf);
  }

  return {
    runtimeKind: "wasmtime",

    evict(pluginId: string): void {
      const existing = compiled.get(pluginId);
      if (!existing) return;
      lib.symbols.stg_wasm_module_free(existing.handle);
      compiled.delete(pluginId);
    },

    compile(pluginId: string, digest: string, wasmBytes: Uint8Array): void {
      const existing = compiled.get(pluginId);
      if (existing && existing.digest === digest) return;
      if (existing) {
        lib.symbols.stg_wasm_module_free(existing.handle);
        compiled.delete(pluginId);
      }
      const errBuf = new Uint8Array(512);
      const handle = lib.symbols.stg_wasm_module_compile(
        engine,
        ptr(wasmBytes),
        BigInt(wasmBytes.byteLength),
        ptr(errBuf),
        BigInt(errBuf.byteLength),
      );
      if (!handle) {
        throw new WasmPluginFaultError(
          "wasm_compile_failed",
          `WASM compile failed for ${pluginId}: ${readCString(errBuf)}`,
        );
      }
      compiled.set(pluginId, { digest, handle });
    },

    async invoke(
      pluginId: string,
      hook: WasmCallableHook,
      input: Uint8Array,
      invokeOptions: WasmInvokeOptions,
    ): Promise<Uint8Array> {
      const mod = compiled.get(pluginId);
      if (!mod) {
        throw new WasmPluginFaultError(
          "wasm_compile_failed",
          `WASM plugin not compiled: ${pluginId}`,
        );
      }
      const hookBytes = encoder.encode(hook);
      const outPtrHolder = new BigUint64Array(1);
      const outLenHolder = new BigUint64Array(1);
      const errBuf = new Uint8Array(512);
      const rc = lib.symbols.stg_wasm_invoke(
        engine,
        mod.handle,
        ptr(hookBytes),
        BigInt(hookBytes.byteLength),
        input.byteLength > 0 ? ptr(input) : null,
        BigInt(input.byteLength),
        BigInt(invokeOptions.fuelBudget),
        BigInt(invokeOptions.maxMemoryBytes),
        ptr(outPtrHolder),
        ptr(outLenHolder),
        ptr(errBuf),
        BigInt(errBuf.byteLength),
      );
      if (rc !== 0) {
        const message = readCString(errBuf);
        if (rc === -3 && (message.includes("AllocationError") || message.includes("pooling"))) {
          throw new WasmCapacityError("wasm_pool_limit", `WASM invoke failed (${rc}): ${message}`);
        }
        if (rc === -8) {
          throw new WasmPluginFaultError(
            "wasm_memory_limit",
            `WASM invoke failed (${rc}): ${message}`,
          );
        }
        throw new WasmPluginFaultError(
          "wasm_execution_failed",
          `WASM invoke failed (${rc}): ${message}`,
        );
      }
      const rawPtr = Number(outPtrHolder[0]!);
      const rawLen = Number(outLenHolder[0]!);
      if (rawPtr === 0 || rawLen === 0) {
        if (rawPtr !== 0) lib.symbols.stg_wasm_buf_free(rawPtr as never, BigInt(rawLen));
        return new Uint8Array(0);
      }
      try {
        const view = new Uint8Array(toArrayBuffer(rawPtr as never, 0, rawLen));
        const copy = new Uint8Array(rawLen);
        copy.set(view);
        return copy;
      } finally {
        lib.symbols.stg_wasm_buf_free(rawPtr as never, BigInt(rawLen));
      }
    },

    close(): void {
      for (const mod of compiled.values()) {
        lib.symbols.stg_wasm_module_free(mod.handle);
      }
      compiled.clear();
      lib.symbols.stg_wasm_engine_free(engine);
      lib.close();
    },
  };
}
