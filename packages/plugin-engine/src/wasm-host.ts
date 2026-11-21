import { dlopen, FFIType, ptr, read, toArrayBuffer } from "bun:ffi";
import { createPublicKey, verify } from "node:crypto";
import { existsSync } from "node:fs";
import type { PluginHook, WasmRuntimeExecutor } from "./types.ts";

const DEFAULT_MAX_MEMORY = 32 * 1024 * 1024;
const DEFAULT_FUEL = 2_000_000;

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
        void read;
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
