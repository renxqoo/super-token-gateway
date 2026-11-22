import type { WasmCallableHook } from "./types.ts";
import { createFfiWasmtimeExecutor } from "./wasm-host.ts";

type InvokeMessage = {
  seq: number;
  id: string;
  digest: string;
  dropIds: string[];
  bytes: Uint8Array | null;
  hook: WasmCallableHook;
  input: Uint8Array;
  fuelBudget: number;
  maxMemoryBytes: number;
};

const [libraryPath, maxMemoryBytes] = Bun.argv.slice(-2);
let executor: ReturnType<typeof createFfiWasmtimeExecutor> | undefined;
const loadedDigests = new Map<string, string>();

globalThis.onmessage = async (event: MessageEvent<InvokeMessage>) => {
  const request = event.data;
  if (request.seq === 0) {
    executor?.close();
    executor = undefined;
    loadedDigests.clear();
    postMessage({ seq: 0, ok: true });
    return;
  }
  let compiledInCall = false;
  try {
    executor ??= createFfiWasmtimeExecutor({
      libraryPath: libraryPath!,
      maxMemoryBytes: Number(maxMemoryBytes),
    });
    for (const id of request.dropIds) {
      executor.evict(id);
      loadedDigests.delete(id);
    }
    if (request.bytes) {
      loadedDigests.delete(request.id);
      try {
        executor.compile(request.id, request.digest, request.bytes);
      } catch (compileErr) {
        executor.evict(request.id);
        loadedDigests.delete(request.id);
        postMessage({
          seq: request.seq,
          ok: false,
          compileFailed: true,
          error: compileErr instanceof Error ? compileErr.message : String(compileErr),
        });
        return;
      }
      loadedDigests.set(request.id, request.digest);
      compiledInCall = true;
    }
    if (loadedDigests.get(request.id) !== request.digest) {
      executor.evict(request.id);
      loadedDigests.delete(request.id);
      postMessage({
        seq: request.seq,
        ok: false,
        compileFailed: true,
        error: "WASM module revision is not loaded",
      });
      return;
    }
    const out = await executor.invoke(request.id, request.hook, request.input, {
      fuelBudget: request.fuelBudget,
      maxMemoryBytes: request.maxMemoryBytes,
    });
    postMessage({ seq: request.seq, ok: true, compiled: compiledInCall, out }, [
      out.buffer as ArrayBuffer,
    ]);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const capacity =
      message.includes("AllocationError") ||
      message.includes("maximum concurrent") ||
      message.includes("pooling");
    postMessage({
      seq: request.seq,
      ok: false,
      compileFailed: false,
      compiled: compiledInCall,
      faultKind: capacity ? "host_capacity" : "plugin_fault",
      error: message,
    });
  }
};
