import { dlopen, FFIType, ptr, toArrayBuffer } from "bun:ffi";
import { existsSync } from "node:fs";
import type { WasmCallableHook, WasmInvokeOptions, WasmRuntimeExecutor } from "./types.ts";
import {
  DEFAULT_MAX_MEMORY,
  DEFAULT_FUEL,
  WasmPluginFaultError,
  WasmCapacityError,
} from "./wasm-errors.ts";

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
  const outPtrHolder = new BigUint64Array(1);
  const outLenHolder = new BigUint64Array(1);
  const errBuf = new Uint8Array(512);
  const errBufLen = BigInt(errBuf.byteLength);
  const encodedHooks = new Map<WasmCallableHook, { bytes: Uint8Array; len: bigint }>();

  function getHookBytes(hook: WasmCallableHook): { bytes: Uint8Array; len: bigint } {
    let cached = encodedHooks.get(hook);
    if (!cached) {
      const bytes = encoder.encode(hook);
      cached = { bytes, len: BigInt(bytes.byteLength) };
      encodedHooks.set(hook, cached);
    }
    return cached;
  }

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
      errBuf[0] = 0;
      const handle = lib.symbols.stg_wasm_module_compile(
        engine,
        ptr(wasmBytes),
        BigInt(wasmBytes.byteLength),
        ptr(errBuf),
        errBufLen,
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
      const hookBuf = getHookBytes(hook);
      outPtrHolder[0] = 0n;
      outLenHolder[0] = 0n;
      errBuf[0] = 0;
      const rc = lib.symbols.stg_wasm_invoke(
        engine,
        mod.handle,
        ptr(hookBuf.bytes),
        hookBuf.len,
        input.byteLength > 0 ? ptr(input) : null,
        BigInt(input.byteLength),
        BigInt(invokeOptions.fuelBudget),
        BigInt(invokeOptions.maxMemoryBytes),
        ptr(outPtrHolder),
        ptr(outLenHolder),
        ptr(errBuf),
        errBufLen,
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
