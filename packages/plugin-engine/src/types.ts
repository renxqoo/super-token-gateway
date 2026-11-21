import type { ChargePolicy, PluginAuditEntry } from "@gateway/ledger";

export type PluginHook = "on_request" | "on_response" | "on_stream_chunk" | "on_stream_end";
export type ProviderHook = "adapt_request" | "adapt_response" | "adapt_stream_event";
export type WasmCallableHook = PluginHook | ProviderHook;

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
    hook: WasmCallableHook,
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

export type ProviderRetryKind = "retryable" | "rate_limit" | "auth_error" | "client_error";

export type WasmProviderRequestInput = {
  operation: string;
  upstream_model: string;
  stream: boolean;
  client_protocol: string | null;
  body: Record<string, unknown>;
  headers: Record<string, string>;
};

export type WasmProviderRequestOutput = {
  path: string;
  query?: Record<string, string>;
  headers?: Record<string, string>;
  body: Record<string, unknown>;
};

export type WasmProviderResponseInput = {
  operation: string;
  upstream_model: string;
  client_protocol: string | null;
  status: number;
  headers: Record<string, string>;
  body: unknown;
};

export type WasmProviderResponseOutput = {
  status?: number;
  body?: Record<string, unknown>;
  usage?: Record<string, unknown>;
  retry_kind?: ProviderRetryKind;
  retry_after_ms?: number;
  error_message?: string;
};

export type WasmProviderStreamInput = {
  operation: string;
  upstream_model: string;
  client_protocol: string | null;
  event: string;
  data: string;
  state: string;
};

export type WasmProviderStreamOutput = {
  chunks?: Record<string, unknown>[];
  usage?: Record<string, unknown>;
  done?: boolean;
  state?: string;
  retry_kind?: ProviderRetryKind;
  retry_after_ms?: number;
  error_message?: string;
};
