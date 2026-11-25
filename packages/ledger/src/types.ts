import type { Operation, Pricing, Usage } from "@gateway/kernel";

export type RoutingStrategy = "WEIGHTED" | "ROUND_ROBIN" | "LEAST_LATENCY";
export type WalletPolicy = "GIFT_FIRST" | "CASH_FIRST" | "EARLIEST_EXPIRY";
export type LotKind = "CASH" | "GRANT";
export type PriceSide = "CUSTOMER" | "SUPPLIER";
export type PriceBasis = "PROVIDER_USAGE" | "REQUEST_COMPLETION" | "INPUT_CHARACTERS";
export type ChannelKind = string;
export type ChargePolicy = "CHARGE_CUSTOMER" | "PLATFORM_ABSORB";
export type PluginCapability = "CONTENT" | "PROVIDER";
export type PluginKind = "WASM" | "WEBHOOK" | "DECLARATIVE";
export type PluginAction = "PASS" | "TRANSFORM" | "BLOCK";

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

export type ProjectRecord = {
  id: string;
  external_ref: string | null;
  name: string;
  currency: string;
  rpm: number;
  tpm: number;
  concurrency: number;
  enabled: boolean;
};

export type ApiKeyRecord = {
  id: string;
  project_id: string;
  name: string;
  verifier: string;
  prefix: string;
  role: "ADMIN" | "BILLING" | "INVOKER" | "VIEWER";
  allowed_cidrs: string[];
  models: string[];
  operations: string[];
  enabled: boolean;
  expires_at: string | null;
  spend_limit_atoms: string | null;
  spend_limit_window: SpendLimitWindow;
};

export type ApiKeySpendSnapshot = {
  id: string;
  project_id: string;
  spend_limit_atoms: string | null;
  spend_limit_window: SpendLimitWindow;
  spend_window_start: string;
  spent_atoms: string;
  reserved_atoms: string;
  remaining_atoms: string | null;
};

export type ProjectPricingOverrideRecord = {
  id: string;
  project_id: string;
  model_name: string;
  operation: string;
  override_kind: "EXACT_PRICE" | "MULTIPLIER";
  billing_price_id: string | null;
  multiplier_numerator: string | null;
  multiplier_denominator: string | null;
  enabled: boolean;
};

export type AsyncTaskStatus = "QUEUED" | "PROCESSING" | "SUCCEEDED" | "FAILED" | "CANCELLED";
export type AsyncTaskFailureCategory =
  | "UPSTREAM_SYSTEM"
  | "CONTENT_POLICY"
  | "INVALID_INPUT"
  | "USER_CANCELLED"
  | "UPSTREAM_BILLED"
  | "TIMEOUT"
  | "UNKNOWN";
export type AsyncTaskRefundEligibility =
  | "PENDING"
  | "AUTO_REFUNDED"
  | "DENIED_USER_CANCELLED"
  | "DENIED_POLICY_OR_CLIENT"
  | "DENIED_UPSTREAM_BILLED"
  | "DENIED_ABUSE_LIMIT"
  | "MANUAL_REVIEW"
  | "NOT_APPLICABLE";

export type AsyncTaskRecord = {
  id: string;
  request_id: string;
  charge_id: string | null;
  project_id: string;
  api_key_id: string | null;
  channel_id: string;
  model: string;
  operation: string;
  upstream_task_id: string;
  status: AsyncTaskStatus;
  user_cancelled: boolean;
  upstream_billed: boolean;
  failure_category: AsyncTaskFailureCategory | null;
  refund_eligibility: AsyncTaskRefundEligibility;
  refund_id: string | null;
  result_json: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
};

export type UpstreamAccountRecord = {
  id: string;
  name: string;
  state: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED";
  revision: string;
};

export type ChannelRecord = {
  id: string;
  account_id: string;
  name: string;
  kind: ChannelKind;
  provider_plugin_id: string | null;
  base_url: string;
  secret_ciphertext: string;
  rpm: number;
  concurrency: number;
  enabled: boolean;
  routing_policy: Record<string, unknown>;
  adapter_config: AdapterOverrides;
  config_revision: number;
};

export type ModelRecord = {
  name: string;
  operations: Operation[];
  routing_strategy: RoutingStrategy;
  max_output_tokens: number;
  enabled: boolean;
};

export type PriceVersionRecord = {
  id: string;
  side: PriceSide;
  currency: string;
  basis: PriceBasis;
  rates: Pricing["rates"];
  ceilings: Record<string, string>;
  source_digest: string;
};

export type PluginPipelineStep = {
  plugin_id: string;
  priority: number;
  enabled?: boolean;
  charge_policy?: ChargePolicy;
};

export type RouteRetryPolicy = {
  max_attempts?: number;
  retry_on_ambiguous?: boolean;
  attempt_timeout_ms?: number;
};

export type RouteRecord = {
  id: string;
  model_name: string;
  channel_id: string;
  upstream_model: string;
  operations: Operation[];
  priority: number;
  weight: number;
  billing_price_id: string;
  cost_version_id: string;
  request_policy: Record<string, unknown>;
  adapter_overrides: AdapterOverrides;
  plugin_pipeline: PluginPipelineStep[];
  retry_policy: RouteRetryPolicy;
  enabled: boolean;
};

export type WebhookPluginConfig = {
  url: string;
  headers?: Record<string, string>;
  timeout_ms?: number;
  stream_mode?: "HOLD_UNTIL_APPROVED" | "STREAM_AND_CUTOFF";
  window_chars?: number;
  window_ms?: number;
  max_buffer_bytes?: number;
  max_concurrency?: number;
  max_response_bytes?: number;
  failure_policy?: "FAIL_CLOSED" | "FAIL_OPEN";
  charge_policy?: ChargePolicy;
  circuit_failure_threshold?: number;
  circuit_open_ms?: number;
};

export type WasmPluginManifest = {
  hooks: ("on_request" | "on_response" | "on_stream_chunk" | "on_stream_end")[];
  max_memory_bytes?: number;
  fuel_budget?: number;
  fuel_per_byte?: number;
  max_input_bytes?: number;
  deterministic_text_only?: boolean;
  window_bytes?: number;
  charge_policy?: ChargePolicy;
  failure_policy?: "FAIL_CLOSED" | "FAIL_OPEN";
};

export type PluginRecord = {
  id: string;
  name: string;
  version: string;
  capability: PluginCapability;
  kind: PluginKind;
  digest: string;
  wasm_bytes: Uint8Array | null;
  webhook_config: WebhookPluginConfig | null;
  provider_config: ProviderPluginConfig | null;
  signature: string | null;
  signing_key_id: string | null;
  manifest: Record<string, unknown>;
  enabled: boolean;
  builtin?: boolean;
  created_at?: string;
};

export type PluginReleaseTarget =
  | { channel_id: string; provider_plugin_id: string }
  | { route_id: string; plugin_pipeline: PluginPipelineStep[] }
  | { plugin_id: string; available: boolean };

export type PluginReleaseChange = {
  before: PluginReleaseTarget;
  after: PluginReleaseTarget;
};

export type EpochStamp = {
  version: number;
  lineageToken: string;
};

export type ConfigSnapshot = {
  epoch: number;
  lineageToken?: string;
  projects: Map<string, ProjectRecord>;
  keysByVerifier: Map<string, ApiKeyRecord>;
  accounts: Map<string, UpstreamAccountRecord>;
  channels: Map<string, ChannelRecord>;
  models: Map<string, ModelRecord>;
  prices: Map<string, PriceVersionRecord>;
  pricingOverridesByProject: Map<string, ProjectPricingOverrideRecord[]>;
  routesByModel: Map<string, RouteRecord[]>;
  plugins: Map<string, PluginRecord>;
  revokedPlugins: ReadonlySet<string>;
  providerPluginsByName: Map<string, PluginRecord>;
};

export type HoldLotAllocation = {
  lot_id: string;
  amount_atoms: string;
};

export type LeaseLotAllocation = {
  lot_id: string;
  amount_atoms: string;
  expires_at_ms: number | null;
};

export type ChargeLotAllocation = {
  lot_id: string;
  amount_atoms: string;
  refunded_atoms: string;
};

export type SupplierHoldEntry = {
  attempt_id: string;
  cost_version_id?: string;
  customer_price_version_id?: string;
  customer_price_adjustment?: {
    override_id: string | null;
    discount_numerator: string | null;
    discount_denominator: string | null;
  };
  provider_request_id?: string;
  account_id: string;
  channel_id: string;
  currency: string;
  cell: number;
  held_atoms: string;
  state: "HELD" | "RELEASED" | "UNKNOWN" | "SETTLED";
  resolution?: {
    action: "RELEASE" | "SETTLE";
    supplier_cost_atoms: string;
    evidence_ref: string;
    reason: string;
    actor: string;
  };
};

export type ResolveSupplierAttemptInput = {
  request_id: string;
  attempt_id: string;
  evidence_ref: string;
  reason: string;
  actor: string;
} & ({ action: "RELEASE" } | { action: "SETTLE"; supplier_cost_atoms: string });

export type ResolveSupplierAttemptResult =
  | { ok: true; replayed: boolean; state: "RELEASED" | "SETTLED" }
  | {
      ok: false;
      code: "supplier_attempt_not_found" | "supplier_attempt_conflict" | "invalid_supplier_cost";
    };

export type PluginAuditEntry = {
  plugin_id: string;
  version: string;
  hook: string;
  action: PluginAction;
  rule_code?: string;
  outcome_kind?: "content_decision" | "plugin_fault" | "host_capacity";
  fail_open?: boolean;
  latency_us: number;
  executions?: number;
};

export type AcquireHoldInput = {
  request_id: string;
  attempt_id: string;
  project_id: string;
  api_key_id?: string | null;
  enforce_key_spend_limit?: boolean;
  pricing_override_id?: string | null;
  discount_numerator?: string | null;
  discount_denominator?: string | null;
  currency: string;
  operation: Operation;
  model: string;
  idempotency_key: string | null;
  fingerprint: string;
  customer_atoms: string;
  supplier_account_id: string;
  supplier_cost_version_id?: string;
  customer_price_version_id?: string;
  supplier_channel_id: string;
  supplier_currency: string;
  supplier_atoms: string;
  expires_at: string;
};

export type AcquireHoldResult =
  | {
      ok: true;
      replayed: boolean;
      request_id: string;
      wallet_id: string;
      held_atoms: string;
      credit_atoms: string;
      supplier_cell: number;
      existing_state?: string;
    }
  | {
      ok: false;
      code:
        | "wallet_not_found"
        | "wallet_frozen"
        | "insufficient_balance"
        | "key_spend_limit_exceeded"
        | "idempotency_conflict"
        | "supplier_unavailable"
        | "supplier_insufficient_balance"
        | "ledger_busy"
        | "invalid_amount";
      message: string;
    };

export type SwitchSupplierInput = {
  request_id: string;
  failed_attempt_id: string;
  failed_outcome: "FAILED" | "UNKNOWN";
  next_attempt_id: string;
  next_account_id: string;
  next_channel_id: string;
  next_currency: string;
  next_supplier_atoms: string;
  next_supplier_cost_version_id?: string;
  next_customer_price_version_id?: string;
  next_customer_price_adjustment?: SupplierHoldEntry["customer_price_adjustment"];
  failed_provider_request_id?: string;
  required_customer_atoms?: string;
};

export type SwitchSupplierResult =
  | {
      ok: true;
      next_cell: number;
      held_atoms: string;
    }
  | {
      ok: false;
      code:
        | "hold_not_active"
        | "insufficient_balance"
        | "key_spend_limit_exceeded"
        | "wallet_frozen"
        | "supplier_unavailable"
        | "supplier_insufficient_balance";
      message: string;
    };

export type CommitTerminalInput = {
  request_id: string;
  project_id?: string;
  selected_attempt_id: string;
  channel_id: string;
  billing_price_id: string;
  cost_version_id: string;
  api_key_id?: string | null;
  pricing_override_id?: string | null;
  discount_numerator?: string | null;
  discount_denominator?: string | null;
  usage: Usage;
  billing_pricing: Pricing;
  billing_basis: PriceBasis;
  input_characters?: string;
  supplier_pricing: Pricing;
  supplier_basis?: PriceBasis;
  supplier_currency: string;
  supplier_cost_atoms_override?: string;
  plugin_audit?: PluginAuditEntry[];
  charge_policy?: ChargePolicy;
  moderation_blocked?: boolean;
};

export type CommitTerminalResult =
  | {
      ok: true;
      replayed: boolean;
      charge_id: string;
      outcome: "SETTLED" | "RELEASED" | "DEBT" | "MODERATION_BLOCKED" | "UNKNOWN_RESOLVED";
      customer_atoms: string;
      funded_atoms: string;
      debt_atoms: string;
      supplier_cost_atoms: string;
    }
  | {
      ok: false;
      code: "hold_not_found" | "invalid_state" | "pricing_error" | "currency_mismatch";
      message: string;
    };

export type CreditWalletInput = {
  project_id: string;
  currency: string;
  external_ref: string;
  amount_atoms: string;
  kind: LotKind;
  priority?: number;
  expires_at?: string | null;
  models?: string[];
  operations?: string[];
  reason?: string;
};

export type RefundChargeInput = {
  charge_id: string;
  external_ref: string;
  amount_atoms: string;
  reason: string;
};

export type AdjustChargeInput = {
  charge_id: string;
  external_ref: string;
  delta_atoms: string;
  reason: string;
};

export type AdjustWalletInput = {
  project_id: string;
  currency: string;
  external_ref: string;
  delta_atoms: string;
  lot_kind?: LotKind;
  force_debt?: boolean;
  reason: string;
  actor?: string;
};

export type AdjustWalletResult = {
  operation_id: string;
  wallet_id: string;
  replayed: boolean;
  delta_atoms: string;
  lot_id: string | null;
  funded_atoms: string;
  debt_delta_atoms: string;
  available_atoms: string;
  debt_atoms: string;
  freeze_reason: string | null;
  debt_collected_charges: number;
};

export type SetWalletFreezeInput = {
  project_id: string;
  currency: string;
  frozen: boolean;
  reason: string;
  actor?: string;
};

export type RechargeSupplierInput = {
  account_id: string;
  currency: string;
  external_ref: string;
  amount_atoms: string;
  reason?: string;
};

export type AdjustSupplierBalanceInput = {
  account_id: string;
  currency: string;
  external_ref: string;
  delta_atoms: string;
  allow_negative?: boolean;
  reason: string;
  actor?: string;
};

export type AdjustSupplierBalanceResult = {
  operation_id: string;
  replayed: boolean;
  delta_atoms: string;
  total_balance_atoms: string;
  reserved_atoms: string;
  account_state: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED";
  config_epoch?: number;
};

export type ResolveRequestInput = {
  request_id: string;
  action: "SETTLE" | "RELEASE";
  billing_price_id?: string;
  usage?: Usage;
  supplier_cost_atoms?: string;
  supplier_cost_version_id?: string;
  reason: string;
  actor: string;
};

export type ReconciliationReceipt = {
  request_id: string;
  attempt_id: string;
  account_id: string;
  channel_id: string;
  currency: string;
  provider_request_id: string;
  evidence_ref: string;
  match_reason?: string | undefined;
  actor: string;
} & (
  | { verdict: "NOT_EXECUTED" }
  | { verdict: "FINAL"; supplier_cost_atoms: string; customer_usage?: Usage | undefined }
);

export type ReconciliationPolicy = {
  project_id: string;
  channel_id: string;
  supplier_currency: string;
  enabled: boolean;
  include_historical: boolean;
  age_seconds: number;
  max_customer_atoms: string;
  limit_atoms: string;
};

export type WalletSnapshot = {
  id: string;
  project_id: string;
  currency: string;
  policy: WalletPolicy;
  available_atoms: string;
  reserved_atoms: string;
  spent_atoms: string;
  expired_atoms: string;
  debt_atoms: string;
  credit_limit_atoms: string;
  credit_reserved_atoms: string;
  credit_policy_version: string;
  manual_frozen: boolean;
  evidence_disputed: boolean;
  freeze_reason: string | null;
};

export type WalletLotSnapshot = {
  id: string;
  wallet_id: string;
  external_ref: string;
  kind: LotKind;
  priority: number;
  models: string[];
  operations: string[];
  expires_at: string | null;
  issued_atoms: string;
  available_atoms: string;
  reserved_atoms: string;
  spent_atoms: string;
  expired_atoms: string;
  created_at: string;
};

export type WalletWithLotsSnapshot = WalletSnapshot & {
  lots: WalletLotSnapshot[];
};

export type FundingOperationRecord = {
  id: string;
  side: "CUSTOMER" | "SUPPLIER";
  entity_id: string;
  project_id: string | null;
  wallet_id: string | null;
  charge_id: string | null;
  account_id: string | null;
  kind: "CREDIT" | "REFUND" | "ADJUST" | "SUPPLIER_RECHARGE" | "SUPPLIER_ADJUST";
  external_ref: string;
  amount_atoms: string;
  currency: string;
  reason: string;
  payload: Record<string, unknown>;
  receipt: Record<string, unknown>;
  created_at: string;
};

export type FinanceSummaryCurrencyRow = {
  currency: string;
  charge_count: number;
  moderation_absorbed_count: number;
  customer_billed_atoms: string;
  customer_funded_atoms: string;
  customer_debt_atoms: string;
  customer_refunded_atoms: string;
  net_customer_revenue_atoms: string;
  supplier_cost_atoms: string;
  gross_margin_atoms: string;
  cash_topup_atoms: string;
  grant_issued_atoms: string;
  customer_adjustment_net_atoms: string;
  supplier_recharge_atoms: string;
  supplier_adjustment_net_atoms: string;
};

export type FinanceSummaryDimensionRow = {
  key: string;
  currency: string;
  charge_count: number;
  customer_billed_atoms: string;
  customer_refunded_atoms: string;
  net_customer_revenue_atoms: string;
  supplier_cost_atoms: string;
  gross_margin_atoms: string;
};

export type FinanceSummaryReport = {
  from: string | null;
  to: string | null;
  project_id: string | null;
  currency: string | null;
  by_currency: FinanceSummaryCurrencyRow[];
  by_model: FinanceSummaryDimensionRow[];
  by_project: FinanceSummaryDimensionRow[];
  by_channel: FinanceSummaryDimensionRow[];
};

export type OutboxEventRecord = {
  seq: string;
  event_id: string;
  kind: string;
  request_id: string | null;
  project_id: string | null;
  payload: Record<string, unknown>;
  created_at: string;
};

export type ConservationAudit = {
  ok: boolean;
  unbalanced_journals: number;
  broken_lots: number;
  wallet_mismatches: number;
  reservation_mismatches: number;
  customer_ledger_mismatches: number;
  supplier_ledger_mismatches: number;
  open_holds: number;
  unknown_holds: number;
};
