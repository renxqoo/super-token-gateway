import type { ChargePolicy, PluginAuditEntry } from "@gateway/ledger";

export type PluginHook = "on_request" | "on_response" | "on_stream_chunk" | "on_stream_end";

export type PluginDecision =
  | { action: "PASS" }
  | { action: "TRANSFORM"; text: string }
  | {
      action: "BLOCK";
      rule_code: string;
      http_status?: number;
      safe_message?: string;
    };

export type PluginHookInput = {
  hook: PluginHook;
  request_id: string;
  project_id: string;
  model: string;
  operation: string;
  text: string;
  final?: boolean;
};

export type WasmRuntimeExecutor = {
  compile(pluginId: string, digest: string, wasmBytes: Uint8Array): void;
  invoke(
    pluginId: string,
    hook: PluginHook,
    input: Uint8Array,
    options: { fuelBudget: number; maxMemoryBytes: number },
  ): Promise<Uint8Array>;
  close(): void;
};

export type PipelineExecutionResult = {
  blocked: boolean;
  text: string;
  rule_code?: string;
  http_status?: number;
  safe_message?: string;
  charge_policy: ChargePolicy;
  audit: PluginAuditEntry[];
};
