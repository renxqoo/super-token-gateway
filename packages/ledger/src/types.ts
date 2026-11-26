import type { Operation, Pricing, Usage } from "@gateway/kernel";
import type { SpendLimitWindow, AdapterOverrides, ProviderPluginConfig } from "./provider-types.ts";
export type {
  WireDialect,
  UsageDialect,
  StreamFraming,
  AuthSchemeConfig,
  BodyTransformRule,
  ResponseTransformRule,
  ErrorMatchRule,
  ProviderErrorDiagnostics,
  UsageAliasTarget,
  UsageAliasRule,
  EndpointTemplate,
  SpendLimitWindow,
  AsyncTaskProviderConfig,
  MediaStreamTerminalRule,
  DeclarativeProviderConfig,
  WasmProviderConfig,
  ProviderPluginConfig,
  AdapterOverrides,
} from "./provider-types.ts";

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

export type BillingAccountRecord = {
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
  billing_account_id: string;
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
  billing_account_id: string;
  spend_limit_atoms: string | null;
  spend_limit_window: SpendLimitWindow;
  spend_window_start: string;
  spent_atoms: string;
  reserved_atoms: string;
  remaining_atoms: string | null;
};

export type BillingAccountPricingOverrideRecord = {
  id: string;
  billing_account_id: string;
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
  billing_account_id: string;
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
  billingAccounts: Map<string, BillingAccountRecord>;
  keysByVerifier: Map<string, ApiKeyRecord>;
  accounts: Map<string, UpstreamAccountRecord>;
  channels: Map<string, ChannelRecord>;
  models: Map<string, ModelRecord>;
  prices: Map<string, PriceVersionRecord>;
  pricingOverridesByBillingAccount: Map<string, BillingAccountPricingOverrideRecord[]>;
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
  billing_account_id: string;
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
  legacy_fingerprint?: string;
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
  billing_account_id?: string;
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
  billing_account_id: string;
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
  billing_account_id: string;
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
  billing_account_id: string;
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
  billing_account_id: string;
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
  billing_account_id: string;
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
  billing_account_id: string | null;
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
  billing_account_id: string | null;
  currency: string | null;
  by_currency: FinanceSummaryCurrencyRow[];
  by_model: FinanceSummaryDimensionRow[];
  by_billing_account: FinanceSummaryDimensionRow[];
  by_channel: FinanceSummaryDimensionRow[];
};

export type OutboxEventRecord = {
  seq: string;
  event_id: string;
  kind: string;
  request_id: string | null;
  billing_account_id: string | null;
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
