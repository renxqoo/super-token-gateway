import type { WasmCallableHook } from "./types.ts";
import { createFfiWasmtimeExecutor } from "./wasm-host.ts";

type InvokeMessage = {
  seq: number;
  id: string;
  digest: string;
  bytes: Uint8Array | null;
  hook: WasmCallableHook;
  input: Uint8Array;
  fuelBudget: number;
  maxMemoryBytes: number;
};

const [libraryPath, maxMemoryBytes] = Bun.argv.slice(-2);
let executor: ReturnType<typeof createFfiWasmtimeExecutor> | undefined;

globalThis.onmessage = async (event: MessageEvent<InvokeMessage>) => {
  const request = event.data;
  if (request.seq === 0) {
    executor?.close();
    executor = undefined;
    postMessage({ seq: 0, ok: true });
    return;
  }
  try {
    executor ??= createFfiWasmtimeExecutor({
      libraryPath: libraryPath!,
      maxMemoryBytes: Number(maxMemoryBytes),
    });
    if (request.bytes) executor.compile(request.id, request.digest, request.bytes);
    const out = await executor.invoke(request.id, request.hook, request.input, {
      fuelBudget: request.fuelBudget,
      maxMemoryBytes: request.maxMemoryBytes,
    });
    postMessage({ seq: request.seq, ok: true, out }, [out.buffer as ArrayBuffer]);
  } catch (error) {
    postMessage({
      seq: request.seq,
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
};
