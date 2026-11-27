import type { ChargePolicy, PluginAuditEntry } from "@gateway/ledger";

export type PluginHook = "on_request" | "on_response" | "on_stream_chunk" | "on_stream_end";
export type ProviderHook = "adapt_request" | "adapt_response" | "adapt_stream_event";
export type WasmCallableHook = PluginHook | ProviderHook;

export type PluginOutcomeKind = "content_decision" | "plugin_fault" | "host_capacity";

export type PluginDecision =
  | {
      action: "PASS";
      outcome_kind?: PluginOutcomeKind;
      fail_open_bypassed?: boolean;
      fault_code?: string;
    }
  | {
      action: "TRANSFORM";
      text: string;
      outcome_kind?: "content_decision";
    }
  | {
      action: "BLOCK";
      rule_code: string;
      http_status?: number;
      safe_message?: string;
      outcome_kind?: PluginOutcomeKind;
    };

export type PluginHookInput = {
  hook: PluginHook;
  request_id: string;
  billing_account_id: string;
  model: string;
  operation: string;
  text: string;
  final?: boolean;
};

export type WasmInvokePriority = "ingress" | "egress";

export type WasmInvokeOptions = {
  fuelBudget: number;
  maxMemoryBytes: number;
  signal?: AbortSignal;
  priority?: WasmInvokePriority;
  tenantId?: string;
  compileTimeoutMs?: number;
  transferInput?: boolean;
};

export type WasmExecutorStats = {
  runtime: "wasmtime" | "mock";
  poolSize: number;
  active: number;
  queued: number;
  queuedIngress: number;
  queuedEgress: number;
  rejectedTotal: number;
  shedTotal: number;
  timeoutTotal: number;
  trapTotal: number;
  compileTotal: number;
};

export type WasmRuntimeExecutor = {
  readonly runtimeKind: "wasmtime" | "mock";
  compile(pluginId: string, digest: string, wasmBytes: Uint8Array): void;
  invoke(
    pluginId: string,
    hook: WasmCallableHook,
    input: Uint8Array,
    options: WasmInvokeOptions,
  ): Promise<Uint8Array>;
  acceptsEgress?(): boolean;
  selfCheck?(): Promise<void>;
  stats?(): WasmExecutorStats;
  close(): void;
};

export type PipelineExecutionResult = {
  blocked: boolean;
  text: string;
  rule_code?: string;
  http_status?: number;
  safe_message?: string;
  charge_policy: ChargePolicy;
  outcome_kind: PluginOutcomeKind;
  fail_open_count: number;
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
