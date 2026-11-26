export const DEFAULT_MAX_MEMORY = 32 * 1024 * 1024;

export const DEFAULT_FUEL = 2_000_000;

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
