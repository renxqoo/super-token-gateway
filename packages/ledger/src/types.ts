import type { Operation, Pricing, Usage } from "@gateway/kernel";

export type RoutingStrategy = "WEIGHTED" | "ROUND_ROBIN" | "LEAST_LATENCY";
export type WalletPolicy = "GIFT_FIRST" | "CASH_FIRST" | "EARLIEST_EXPIRY";
export type LotKind = "CASH" | "GRANT";
export type PriceSide = "CUSTOMER" | "SUPPLIER";
export type PriceBasis = "PROVIDER_USAGE" | "REQUEST_COMPLETION" | "INPUT_CHARACTERS";
export type ChannelKind =
  | "openai"
  | "anthropic"
  | "gemini"
  | "azure"
  | "bedrock"
  | "vertex"
  | "deepseek"
  | "ollama"
  | "openai_compatible";
export type ChargePolicy = "CHARGE_CUSTOMER" | "PLATFORM_ABSORB";
export type PluginKind = "WASM" | "WEBHOOK";
export type PluginAction = "PASS" | "TRANSFORM" | "BLOCK";

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
  base_url: string;
  secret_ciphertext: string;
  rpm: number;
  concurrency: number;
  enabled: boolean;
  routing_policy: Record<string, unknown>;
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
  failure_policy?: "FAIL_CLOSED" | "FAIL_OPEN";
  charge_policy?: ChargePolicy;
  circuit_failure_threshold?: number;
  circuit_open_ms?: number;
};

export type WasmPluginManifest = {
  hooks: ("on_request" | "on_response" | "on_stream_chunk" | "on_stream_end")[];
  max_memory_bytes?: number;
  fuel_budget?: number;
  async_pool?: boolean;
  window_bytes?: number;
  charge_policy?: ChargePolicy;
  failure_policy?: "FAIL_CLOSED" | "FAIL_OPEN";
};

export type PluginRecord = {
  id: string;
  name: string;
  version: string;
  kind: PluginKind;
  digest: string;
  wasm_bytes: Uint8Array | null;
  webhook_config: WebhookPluginConfig | null;
  signature: string | null;
  signing_key_id: string | null;
  manifest: Record<string, unknown>;
  enabled: boolean;
};

export type ConfigSnapshot = {
  epoch: number;
  projects: Map<string, ProjectRecord>;
  keysByVerifier: Map<string, ApiKeyRecord>;
  accounts: Map<string, UpstreamAccountRecord>;
  channels: Map<string, ChannelRecord>;
  models: Map<string, ModelRecord>;
  prices: Map<string, PriceVersionRecord>;
  routesByModel: Map<string, RouteRecord[]>;
  plugins: Map<string, PluginRecord>;
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
  account_id: string;
  channel_id: string;
  currency: string;
  cell: number;
  held_atoms: string;
  state: "HELD" | "RELEASED" | "UNKNOWN" | "SETTLED";
};

export type PluginAuditEntry = {
  plugin_id: string;
  version: string;
  hook: string;
  action: PluginAction;
  rule_code?: string;
  latency_us: number;
};

export type AcquireHoldInput = {
  request_id: string;
  attempt_id: string;
  project_id: string;
  currency: string;
  operation: Operation;
  model: string;
  idempotency_key: string | null;
  fingerprint: string;
  customer_atoms: string;
  supplier_account_id: string;
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
        | "idempotency_conflict"
        | "supplier_unavailable"
        | "supplier_insufficient_balance"
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
        | "wallet_frozen"
        | "supplier_unavailable"
        | "supplier_insufficient_balance";
      message: string;
    };

export type CommitTerminalInput = {
  request_id: string;
  selected_attempt_id: string;
  channel_id: string;
  billing_price_id: string;
  cost_version_id: string;
  usage: Usage;
  billing_pricing: Pricing;
  billing_basis: PriceBasis;
  input_characters?: string;
  supplier_pricing: Pricing;
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

export type RechargeSupplierInput = {
  account_id: string;
  currency: string;
  external_ref: string;
  amount_atoms: string;
  reason?: string;
};

export type ResolveRequestInput = {
  request_id: string;
  action: "SETTLE" | "RELEASE";
  billing_price_id?: string;
  usage?: Usage;
  supplier_cost_atoms?: string;
  reason: string;
  actor: string;
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
  open_holds: number;
  unknown_holds: number;
};
