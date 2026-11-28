import type { Dimension, Operation, Pricing, Usage } from "../values.ts";

export type Rational = {
  numerator: string;
  denominator: string;
};

export type ExactRational = {
  numerator: bigint;
  denominator: bigint;
};

export type PricingSide = "CUSTOMER" | "SUPPLIER";
export type PricingBasis = "PROVIDER_USAGE" | "INPUT_CHARACTERS" | "REQUEST_COMPLETION";
export type ExecutionMode = "SYNC" | "BATCH";
export type AttrValue = string | number | boolean;

export type Quantizer = {
  min_units?: string;
  step?: string;
};

export type CliffTier = {
  min_driver: string;
  rates: Partial<Record<Dimension, Rational>>;
  reasoning_rate?: Rational;
};

export type CliffRule = {
  driver: readonly Dimension[];
  tiers: readonly CliffTier[];
};

export type ProgressiveBand = {
  up_to: string | null;
  rate: Rational;
};

export type MatrixEntry = {
  match: Record<string, AttrValue>;
  rates: Partial<Record<Dimension, Rational>>;
  quantizers?: Partial<Record<Dimension, Quantizer>>;
  reasoning_rate?: Rational;
  min_charge_atoms?: string;
  max_charge_atoms?: string;
};

export type PricingSpec = {
  quantizers?: Partial<Record<Dimension, Quantizer>> | undefined;
  cliff?: CliffRule | undefined;
  progressive?: Partial<Record<Dimension, readonly ProgressiveBand[]>> | undefined;
  matrix?: readonly MatrixEntry[] | undefined;
  reasoning_rate?: Rational | undefined;
  min_charge_atoms?: string | undefined;
  max_charge_atoms?: string | undefined;
};

export type RateCard = {
  id: string;
  side: PricingSide;
  currency: string;
  basis: PricingBasis;
  model?: string;
  operation?: Operation | "*";
  rates: Partial<Record<Dimension, Rational>>;
  ceilings: Partial<Record<Dimension, string>>;
  quantizers?: Partial<Record<Dimension, Quantizer>>;
  cliff?: CliffRule;
  progressive?: Partial<Record<Dimension, readonly ProgressiveBand[]>>;
  matrix?: readonly MatrixEntry[];
  reasoning_rate?: Rational;
  min_charge_atoms?: string;
  max_charge_atoms?: string;
};

export type TimeWindow = {
  timezone: string;
  days_of_week?: readonly number[];
  start_minute: number;
  end_minute: number;
};

export type ModifierLayer = "PRODUCT" | "COMMERCIAL";
export type ModifierKind = "MULTIPLIER" | "EXACT_RATE_CARD";
export type StackingPolicy = "MULTIPLY" | "BEST_FOR_CUSTOMER" | "EXCLUSIVE";

export type ModifierMatch = {
  side?: PricingSide;
  model?: string;
  operation?: Operation | "*";
  execution_mode?: ExecutionMode;
  service_tier?: string;
  region?: string;
  billing_account_id?: string;
  customer_group_id?: string;
  channel_id?: string;
  upstream_account_id?: string;
  attrs?: Record<string, AttrValue>;
};

export type PriceModifier = {
  id: string;
  layer: ModifierLayer;
  kind: ModifierKind;
  stacking: StackingPolicy;
  priority: number;
  enabled: boolean;
  effective_from?: string;
  effective_to?: string;
  time_windows?: readonly TimeWindow[];
  match: ModifierMatch;
  multiplier?: Rational;
  rate_card?: RateCard;
};

export type FxRateVersion = {
  id: string;
  base_currency: string;
  quote_currency: string;
  rate: Rational;
  effective_from?: string;
  effective_to?: string;
};

export type PricingContext = {
  side: PricingSide;
  model: string;
  operation: Operation;
  currency: string;
  rated_at: string;
  execution_mode?: ExecutionMode;
  service_tier?: string;
  region?: string;
  attrs?: Record<string, AttrValue>;
  billing_account_id?: string;
  customer_group_id?: string;
  channel_id?: string;
  upstream_account_id?: string;
};

export type AppliedModifier = {
  id: string;
  layer: ModifierLayer;
  kind: ModifierKind;
  stacking: StackingPolicy;
  multiplier?: Rational;
  rate_card_id?: string;
};

export type PricingSnapshot = {
  version: 1;
  rated_at: string;
  side: PricingSide;
  currency: string;
  base_currency: string;
  rate_card_id: string;
  basis: PricingBasis;
  effective_card: RateCard;
  matched_matrix_index: number | null;
  applied_modifiers: readonly AppliedModifier[];
  fx_version_id: string | null;
  fx_rate: Rational | null;
  combined_multiplier: Rational;
};

export type CostComponentBreakdown = {
  dimension: Dimension | "reasoning_tokens";
  curve: "FLAT" | "CLIFF" | "PROGRESSIVE" | "REASONING";
  raw_quantity: string;
  quantized_quantity: string;
  numerator: string;
  denominator: string;
};

export type CostBreakdown = {
  currency: string;
  rate_card_id: string;
  matched_cliff_tier_index: number | null;
  components: readonly CostComponentBreakdown[];
  pre_modifier_numerator: string;
  pre_modifier_denominator: string;
  combined_multiplier: Rational;
  raw_atoms: bigint;
  clamped_by: "MIN_CHARGE" | "MAX_CHARGE" | null;
  total_atoms: bigint;
};

export type HostedToolDimension =
  "web_search_calls" | "code_interpreter_sessions" | "file_search_calls";

export type SnapshotReservationMeasurement = {
  inputCharacters?: string | undefined;
  estimatedInputTokens?: number | undefined;
  images?: unknown;
  pages?: number | undefined;
  searchUnits?: number | undefined;
  maxToolCalls?: Partial<Record<HostedToolDimension, number>> | undefined;
  durationMs?: Partial<Record<"audio_ms" | "music_ms" | "video_ms", number>> | undefined;
  durationSeconds?: Partial<Record<"audio_seconds" | "video_seconds", string>> | undefined;
};

export type SnapshotReservationQuote = {
  currency: string;
  rate_card_id: string;
  amount_atoms: string;
  matched_cliff_tier_index: number | null;
  worst_case_usage: Usage;
};

export type MarginPolicy = {
  allow_negative_margin?: boolean;
  min_margin_bps?: bigint;
  max_loss_atoms?: bigint;
};

export type MarginEvaluation = {
  comparable: boolean;
  customer_currency: string;
  supplier_currency: string;
  customer_atoms: bigint;
  supplier_atoms: bigint;
  normalized_supplier_atoms: bigint | null;
  gross_margin_atoms: bigint | null;
  margin_bps: bigint | null;
  is_negative_margin: boolean | null;
  allowed: boolean;
  reason: string | null;
};

export type AccountPricingOverrideCandidate = {
  id: string;
  model_name: string;
  operation: string;
  override_kind: "EXACT_PRICE" | "MULTIPLIER";
  billing_price_id: string | null;
  multiplier_numerator: string | null;
  multiplier_denominator: string | null;
  enabled: boolean;
};

export type OverridePriceLookup = {
  side: PricingSide;
  currency: string;
  rates: Pricing["rates"];
};
