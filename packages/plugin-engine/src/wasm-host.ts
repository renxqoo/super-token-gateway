import { dlopen, FFIType, ptr, toArrayBuffer } from "bun:ffi";
import { createPublicKey, verify } from "node:crypto";
import { existsSync } from "node:fs";
import type { PluginHook, WasmRuntimeExecutor } from "./types.ts";

const DEFAULT_MAX_MEMORY = 32 * 1024 * 1024;
const DEFAULT_FUEL = 2_000_000;
const WASM_PAGE_BYTES = 65_536;

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
  throw new Error("Malformed WASM LEB128");
}

function inspectWasmInitialMemoryBytes(bytes: Uint8Array): number {
  if (
    bytes.byteLength < 8 ||
    bytes[0] !== 0x00 ||
    bytes[1] !== 0x61 ||
    bytes[2] !== 0x73 ||
    bytes[3] !== 0x6d
  ) {
    throw new Error("Invalid WASM magic header");
  }
  let pos = 8;
  let maxInitialPages = 0;
  while (pos < bytes.byteLength) {
    const sectionId = bytes[pos++]!;
    const size = readLebU32(bytes, pos);
    pos = size.next;
    const end = pos + size.value;
    if (end > bytes.byteLength) throw new Error("Truncated WASM section");
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

const BUN_WASM_WORKER_SOURCE = `
const compiledModules = new Map();
self.onmessage = (event) => {
  const msg = event.data;
  try {
    let entry = compiledModules.get(msg.id);
    if (!entry || entry.digest !== msg.digest) {
      if (!msg.bytes) throw new Error("WASM module bytes missing");
      const mod = new WebAssembly.Module(msg.bytes);
      if (WebAssembly.Module.imports(mod).length > 0) {
        throw new Error("WASM plugin cannot declare host imports");
      }
      entry = { digest: msg.digest, mod };
      compiledModules.set(msg.id, entry);
    }
    const inst = new WebAssembly.Instance(entry.mod, {});
    const memory = inst.exports.memory;
    const alloc = inst.exports.stg_alloc;
    const hookFn = inst.exports[msg.hook];
    if (!(memory instanceof WebAssembly.Memory)) {
      throw new Error("missing exported memory");
    }
    if (typeof alloc !== "function") {
      throw new Error("missing stg_alloc");
    }
    if (typeof hookFn !== "function") {
      throw new Error("missing exported hook");
    }
    if (memory.buffer.byteLength > msg.maxMemoryBytes) {
      throw new Error("memory limit exceeded");
    }
    const input = msg.input;
    const inLen = input.byteLength;
    const inPtr = alloc(inLen) >>> 0;
    if (memory.buffer.byteLength > msg.maxMemoryBytes) {
      throw new Error("memory limit exceeded");
    }
    if (inPtr + inLen > memory.buffer.byteLength) {
      throw new Error("guest allocation out of bounds");
    }
    if (inLen > 0) {
      new Uint8Array(memory.buffer, inPtr, inLen).set(input);
    }
    const packed = BigInt.asUintN(64, BigInt(hookFn(inPtr, inLen)));
    if (memory.buffer.byteLength > msg.maxMemoryBytes) {
      throw new Error("memory limit exceeded");
    }
    const outPtr = Number(packed >> 32n);
    const outLen = Number(packed & 0xffffffffn);
    if (outLen > msg.maxMemoryBytes || outPtr + outLen > memory.buffer.byteLength) {
      throw new Error("output exceeds memory bounds");
    }
    const out = outLen > 0 ? new Uint8Array(memory.buffer, outPtr, outLen).slice() : new Uint8Array(0);
    self.postMessage({ seq: msg.seq, ok: true, out }, [out.buffer]);
  } catch (err) {
    self.postMessage({
      seq: msg.seq,
      ok: false,
      error: err instanceof Error ? err.message : String(err),
    });
  }
};
`;

type PoolSlot = {
  worker: Worker;
  busy: boolean;
  compiledDigests: Map<string, string>;
};

export function createBunWasmExecutor(options?: {
  maxMemoryBytes?: number;
  poolSize?: number;
  minTimeoutMs?: number;
  maxTimeoutMs?: number;
}): WasmRuntimeExecutor {
  const defaultMaxMemory = options?.maxMemoryBytes ?? DEFAULT_MAX_MEMORY;
  const poolSize = Math.max(1, Math.min(16, options?.poolSize ?? 4));
  const minTimeoutMs = options?.minTimeoutMs ?? 25;
  const maxTimeoutMs = options?.maxTimeoutMs ?? 2000;

  const modules = new Map<string, { digest: string; bytes: Uint8Array; initialMemBytes: number }>();
  const slots = new Set<PoolSlot>();
  const idleSlots: PoolSlot[] = [];
  const waiters: ((slot: PoolSlot | null) => void)[] = [];
  let workerBlobUrl: string | null = null;
  let closed = false;
  let nextSeq = 1;

  function spawnSlot(): PoolSlot {
    if (!workerBlobUrl) {
      workerBlobUrl = URL.createObjectURL(
        new Blob([BUN_WASM_WORKER_SOURCE], { type: "application/javascript" }),
      );
    }
    const worker = new Worker(workerBlobUrl);
    (worker as Worker & { unref?: () => void }).unref?.();
    const slot: PoolSlot = {
      worker,
      busy: false,
      compiledDigests: new Map(),
    };
    slots.add(slot);
    return slot;
  }

  function acquireSlot(): Promise<PoolSlot> {
    if (closed) return Promise.reject(new Error("WASM executor is closed"));
    const idle = idleSlots.pop();
    if (idle) {
      idle.busy = true;
      return Promise.resolve(idle);
    }
    if (slots.size < poolSize) {
      const created = spawnSlot();
      created.busy = true;
      return Promise.resolve(created);
    }
    return new Promise((resolve, reject) => {
      waiters.push((slot) => {
        if (!slot) reject(new Error("WASM executor is closed"));
        else resolve(slot);
      });
    });
  }

  function releaseSlot(slot: PoolSlot, healthy: boolean): void {
    if (!healthy) {
      slots.delete(slot);
      try {
        slot.worker.terminate();
      } catch {
        void 0;
      }
      if (!closed && waiters.length > 0) {
        const nextWaiter = waiters.shift()!;
        const replacement = spawnSlot();
        replacement.busy = true;
        nextWaiter(replacement);
      }
      return;
    }
    if (closed) {
      slots.delete(slot);
      try {
        slot.worker.terminate();
      } catch {
        void 0;
      }
      return;
    }
    if (waiters.length > 0) {
      const nextWaiter = waiters.shift()!;
      nextWaiter(slot);
      return;
    }
    slot.busy = false;
    idleSlots.push(slot);
  }

  return {
    compile(pluginId: string, digest: string, wasmBytes: Uint8Array): void {
      if (closed) throw new Error("WASM executor is closed");
      const existing = modules.get(pluginId);
      if (existing && existing.digest === digest) return;
      const initialMemBytes = inspectWasmInitialMemoryBytes(wasmBytes);
      if (initialMemBytes > defaultMaxMemory) {
        throw new Error(`WASM initial memory ${initialMemBytes} exceeds limit ${defaultMaxMemory}`);
      }
      const mod = new WebAssembly.Module(wasmBytes as BufferSource);
      if (WebAssembly.Module.imports(mod).length > 0) {
        throw new Error("WASM plugin cannot declare host imports");
      }
      const exportsList = WebAssembly.Module.exports(mod);
      if (!exportsList.some((e) => e.name === "memory" && e.kind === "memory")) {
        throw new Error("WASM plugin missing exported memory");
      }
      if (!exportsList.some((e) => e.name === "stg_alloc" && e.kind === "function")) {
        throw new Error("WASM plugin missing stg_alloc export");
      }
      modules.set(pluginId, {
        digest,
        bytes: new Uint8Array(wasmBytes),
        initialMemBytes,
      });
    },

    async invoke(
      pluginId: string,
      hook: PluginHook,
      input: Uint8Array,
      invokeOptions: { fuelBudget: number; maxMemoryBytes: number },
    ): Promise<Uint8Array> {
      const entry = modules.get(pluginId);
      if (!entry) throw new Error(`WASM plugin not compiled: ${pluginId}`);
      const effectiveMaxMem = Math.min(defaultMaxMemory, invokeOptions.maxMemoryBytes);
      if (entry.initialMemBytes > effectiveMaxMem) {
        throw new Error(`WASM initial memory ${entry.initialMemBytes} exceeds limit`);
      }
      const slot = await acquireSlot();
      const seq = nextSeq++;
      const timeoutMs = Math.max(
        minTimeoutMs,
        Math.min(maxTimeoutMs, Math.ceil(invokeOptions.fuelBudget / 10_000)),
      );
      const needsBytes = slot.compiledDigests.get(pluginId) !== entry.digest;

      return new Promise<Uint8Array>((resolve, reject) => {
        let settled = false;
        const cleanupHandlers = () => {
          clearTimeout(timer);
          slot.worker.onmessage = null;
          slot.worker.onerror = null;
        };
        const timer = setTimeout(() => {
          if (settled) return;
          settled = true;
          cleanupHandlers();
          releaseSlot(slot, false);
          reject(new Error(`WASM execution timed out after ${timeoutMs}ms`));
        }, timeoutMs);

        slot.worker.onmessage = (event) => {
          const data = event.data as {
            seq: number;
            ok: boolean;
            out?: Uint8Array;
            error?: string;
          };
          if (settled || data.seq !== seq) return;
          settled = true;
          cleanupHandlers();
          if (data.ok) {
            slot.compiledDigests.set(pluginId, entry.digest);
            releaseSlot(slot, true);
            resolve(data.out ? new Uint8Array(data.out) : new Uint8Array(0));
          } else {
            releaseSlot(slot, true);
            reject(new Error(data.error ?? "WASM invoke failed"));
          }
        };

        slot.worker.onerror = (err) => {
          if (settled) return;
          settled = true;
          cleanupHandlers();
          releaseSlot(slot, false);
          reject(new Error(err.message || "WASM worker crashed"));
        };

        slot.worker.postMessage({
          seq,
          id: pluginId,
          digest: entry.digest,
          bytes: needsBytes ? entry.bytes : null,
          hook,
          input,
          maxMemoryBytes: effectiveMaxMem,
        });
      });
    },

    close(): void {
      closed = true;
      while (waiters.length > 0) {
        waiters.shift()!(null);
      }
      idleSlots.length = 0;
      for (const slot of slots) {
        try {
          slot.worker.terminate();
        } catch {
          void 0;
        }
      }
      slots.clear();
      modules.clear();
      if (workerBlobUrl) {
        URL.revokeObjectURL(workerBlobUrl);
        workerBlobUrl = null;
      }
    },
  };
}

export function verifyPluginSignature(
  digestHex: string,
  signatureBase64: string | null,
  publicKeyPem: string | undefined,
): boolean {
  if (!publicKeyPem) return true;
  if (!signatureBase64) return false;
  const pub = createPublicKey(publicKeyPem);
  return verify(null, Buffer.from(digestHex, "utf8"), pub, Buffer.from(signatureBase64, "base64"));
}

export function resolveWasmtimeLibraryPath(explicitPath?: string): string | null {
  const candidates = [
    explicitPath,
    process.env.STG_WASMTIME_LIB,
    new URL("../native/libstg_plugin_wasmtime.dylib", import.meta.url).pathname,
    new URL("../native/libstg_plugin_wasmtime.so", import.meta.url).pathname,
    new URL("../../../dist/native/libstg_plugin_wasmtime.dylib", import.meta.url).pathname,
    new URL("../../../dist/native/libstg_plugin_wasmtime.so", import.meta.url).pathname,
    new URL(
      "../../../native/plugin-wasmtime/target/release/libstg_plugin_wasmtime.dylib",
      import.meta.url,
    ).pathname,
    new URL(
      "../../../native/plugin-wasmtime/target/release/libstg_plugin_wasmtime.so",
      import.meta.url,
    ).pathname,
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
}): WasmRuntimeExecutor {
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
        throw new Error(`WASM compile failed for ${pluginId}: ${readCString(errBuf)}`);
      }
      compiled.set(pluginId, { digest, handle });
    },

    async invoke(
      pluginId: string,
      hook: PluginHook,
      input: Uint8Array,
      invokeOptions: { fuelBudget: number; maxMemoryBytes: number },
    ): Promise<Uint8Array> {
      const mod = compiled.get(pluginId);
      if (!mod) throw new Error(`WASM plugin not compiled: ${pluginId}`);
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
        throw new Error(`WASM invoke failed (${rc}): ${readCString(errBuf)}`);
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
