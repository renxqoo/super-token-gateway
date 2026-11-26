import type { Operation } from "@gateway/kernel";

export type WireDialect =
  "openai_chat" | "openai_responses" | "anthropic_messages" | "gemini" | "passthrough";

export type UsageDialect = "openai" | "anthropic" | "gemini" | "custom";

export type StreamFraming = "sse" | "bedrock_eventstream";

export type AuthSchemeConfig =
  | { scheme: "none" }
  | { scheme: "bearer" }
  | { scheme: "header"; header_name: string; prefix?: string }
  | { scheme: "query"; param_name: string }
  | { scheme: "azure_key"; default_api_version?: string }
  | { scheme: "aws_sigv4"; service: string }
  | { scheme: "gcp_vertex_oauth" }
  | {
      scheme: "hmac_sha256";
      signature_header: string;
      timestamp_header: string;
      key_id_header?: string;
      signature_prefix?: string;
      encoding?: "hex" | "base64";
    };

export type BodyTransformRule =
  | { op: "rename"; from: string; to: string; overwrite?: boolean }
  | { op: "drop"; path: string }
  | { op: "set_default"; path: string; value: unknown }
  | { op: "force_set"; path: string; value: unknown }
  | { op: "clamp"; path: string; min?: number; max?: number }
  | { op: "move_to"; from: string; to: string; overwrite?: boolean }
  | { op: "delete_if_equals"; path: string; value: unknown };

export type ResponseTransformRule =
  | { op: "rename"; from: string; to: string; overwrite?: boolean }
  | { op: "drop"; path: string }
  | { op: "set_default"; path: string; value: unknown };

export type ErrorMatchRule = {
  when_http_status?: number[];
  path: string;
  equals?: string | number | boolean;
  not_equals?: string | number | boolean;
  in?: (string | number)[];
  regex?: string;
  retry_kind: "retryable" | "rate_limit" | "auth_error" | "client_error";
  execution?: "NOT_EXECUTED";
  http_status?: number;
  message_path?: string;
  retry_after_path?: string;
};

export type ProviderErrorDiagnostics = {
  code_path?: string;
  code_header?: string;
  request_id_header?: string;
  summaries?: Record<string, string>;
};

export type UsageAliasTarget =
  | "input_tokens"
  | "output_tokens"
  | "cached_input_tokens"
  | "cache_write_tokens"
  | "cache_write_5m_tokens"
  | "cache_write_1h_tokens"
  | "reasoning_tokens"
  | "requests"
  | "images"
  | "audio_ms"
  | "audio_seconds"
  | "video_ms"
  | "video_seconds"
  | "music_ms"
  | "characters";

export type UsageAliasRule = {
  target: UsageAliasTarget;
  source_path: string;
};

export type EndpointTemplate = {
  path: string;
  stream_path?: string;
  query?: Record<string, string>;
  stream_query?: Record<string, string>;
};

export type SpendLimitWindow = "LIFETIME" | "DAILY" | "MONTHLY";

export type AsyncTaskProviderConfig = {
  query_path: string;
  query_param?: string;
  task_id_path?: string;
  status_path?: string;
  succeeded_values?: string[];
  failed_values?: string[];
  error_code_path?: string;
  error_message_path?: string;
  refundable_system_error_codes?: (string | number)[];
  non_refundable_error_codes?: (string | number)[];
  billed_indicator_path?: string;
};

export type MediaStreamTerminalRule =
  { event: string } | { path: string; equals: string | number | boolean };

export type DeclarativeProviderConfig = {
  base_dialect: WireDialect;
  usage_dialect: UsageDialect;
  stream_framing?: StreamFraming;
  auth: AuthSchemeConfig;
  endpoints: Partial<Record<Operation, EndpointTemplate>> & { default?: EndpointTemplate };
  extra_headers?: Record<string, string>;
  model_field?: string | null;
  stream_field?: string | null;
  include_stream_usage_option?: boolean;
  request_transforms?: BodyTransformRule[];
  response_transforms?: ResponseTransformRule[];
  error_rules?: ErrorMatchRule[];
  error_diagnostics?: ProviderErrorDiagnostics;
  usage_aliases?: UsageAliasRule[];
  media_stream_terminal?: MediaStreamTerminalRule;
  reasoning_field?: string;
  async_task_config?: AsyncTaskProviderConfig;
};

export type WasmProviderConfig = {
  base_dialect: WireDialect;
  usage_dialect: UsageDialect;
  stream_framing?: StreamFraming;
  auth: AuthSchemeConfig;
  extra_headers?: Record<string, string>;
  error_diagnostics?: ProviderErrorDiagnostics;
  hooks: ("adapt_request" | "adapt_response" | "adapt_stream_event")[];
  grants?: "extract_usage"[];
  fuel_budget?: number;
  max_memory_bytes?: number;
  failure_policy?: "FAIL_CLOSED";
};

export type ProviderPluginConfig = DeclarativeProviderConfig | WasmProviderConfig;

export type AdapterOverrides = {
  reconciliation_evidence_required?: boolean;
  reconciliation_response_id_header?: string;
  extra_headers?: Record<string, string>;
  query_params?: Record<string, string>;
  path_override?: string;
  request_transforms?: BodyTransformRule[];
  response_transforms?: ResponseTransformRule[];
  error_rules?: ErrorMatchRule[];
  error_diagnostics?: ProviderErrorDiagnostics;
  usage_aliases?: UsageAliasRule[];
  media_stream_terminal?: MediaStreamTerminalRule;
};
