import {
  resolveAccountPricingOverride,
  type AccountPricingOverrideCandidate,
  type Pricing,
} from "@gateway/kernel";

export type MarketplaceOverrideRow = {
  id: string;
  modelName: string;
  operation: string;
  overrideKind: "EXACT_PRICE" | "MULTIPLIER";
  billingPriceId: string | null;
  multiplierNumerator: string | null;
  multiplierDenominator: string | null;
  enabled: boolean;
};

export type MarketplacePriceSnapshot = {
  side: "CUSTOMER" | "SUPPLIER";
  currency: string;
  rates: Pricing["rates"];
};

export type EffectiveMarketplacePricing = {
  currency: string;
  list_rates: Pricing["rates"];
  effective_rates: Pricing["rates"];
  override_applied: {
    id: string;
    override_kind: "EXACT_PRICE" | "MULTIPLIER";
    multiplier_numerator: string | null;
    multiplier_denominator: string | null;
    billing_price_id: string | null;
  } | null;
};

export function resolveMarketplacePricing(input: {
  modelName: string;
  operation: string;
  currency: string;
  listRates: Pricing["rates"];
  overrides: readonly MarketplaceOverrideRow[];
  exactPricesById: ReadonlyMap<string, MarketplacePriceSnapshot>;
}): EffectiveMarketplacePricing {
  const candidates: AccountPricingOverrideCandidate[] = input.overrides.map((o) => ({
    id: o.id,
    model_name: o.modelName,
    operation: o.operation,
    override_kind: o.overrideKind,
    billing_price_id: o.billingPriceId,
    multiplier_numerator: o.multiplierNumerator,
    multiplier_denominator: o.multiplierDenominator,
    enabled: o.enabled,
  }));
  const matched = resolveAccountPricingOverride({
    modelName: input.modelName,
    operation: input.operation,
    currency: input.currency,
    listRates: input.listRates,
    overrides: candidates,
    exactPricesById: input.exactPricesById,
    invalidPolicy: "SKIP",
  });

  if (matched.kind === "EXACT_PRICE") {
    return {
      currency: input.currency,
      list_rates: input.listRates,
      effective_rates: matched.rates,
      override_applied: {
        id: matched.override_id,
        override_kind: "EXACT_PRICE",
        multiplier_numerator: null,
        multiplier_denominator: null,
        billing_price_id: matched.billing_price_id,
      },
    };
  }
  if (matched.kind === "MULTIPLIER") {
    return {
      currency: input.currency,
      list_rates: input.listRates,
      effective_rates: matched.rates,
      override_applied: {
        id: matched.override_id,
        override_kind: "MULTIPLIER",
        multiplier_numerator: matched.multiplier_numerator,
        multiplier_denominator: matched.multiplier_denominator,
        billing_price_id: null,
      },
    };
  }
  return {
    currency: input.currency,
    list_rates: input.listRates,
    effective_rates: input.listRates,
    override_applied: null,
  };
}

export function buildSdkSnippets(
  gatewayPublicUrl: string,
  modelName: string,
  operations: string[],
) {
  const base = gatewayPublicUrl.replace(/\/+$/, "");
  const snippets: Record<string, string> = {};
  if (operations.includes("chat")) {
    snippets.curl = `curl ${base}/v1/chat/completions -H "Authorization: Bearer $GATEWAY_API_KEY" -H "Content-Type: application/json" -d '${JSON.stringify({ model: modelName, messages: [{ role: "user", content: "Hello" }] })}'`;
    snippets.openai_ts = `import OpenAI from "openai";\nconst client = new OpenAI({ baseURL: "${base}/v1", apiKey: process.env.GATEWAY_API_KEY });\nconst res = await client.chat.completions.create({ model: "${modelName}", messages: [{ role: "user", content: "Hello" }] });`;
  }
  if (operations.includes("messages")) {
    snippets.anthropic_curl = `curl ${base}/v1/messages -H "x-api-key: $GATEWAY_API_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" -d '${JSON.stringify({ model: modelName, max_tokens: 256, messages: [{ role: "user", content: "Hello" }] })}'`;
  }
  if (operations.includes("videos")) {
    snippets.video_curl = `curl ${base}/v1/videos -H "Authorization: Bearer $GATEWAY_API_KEY" -H "Content-Type: application/json" -d '${JSON.stringify({ model: modelName, prompt: "A serene lake at sunrise" })}'`;
  }
  return snippets;
}
