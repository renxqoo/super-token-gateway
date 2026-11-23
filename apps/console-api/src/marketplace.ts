import type { Pricing } from "@gateway/kernel";

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

function gcd(a: bigint, b: bigint): bigint {
  let x = a < 0n ? -a : a;
  let y = b < 0n ? -b : b;
  while (y !== 0n) {
    const t = x % y;
    x = y;
    y = t;
  }
  return x === 0n ? 1n : x;
}

function scaleRates(rates: Pricing["rates"], num: bigint, den: bigint): Pricing["rates"] | null {
  const out: Record<string, { numerator: string; denominator: string }> = {};
  for (const [metric, rate] of Object.entries(rates)) {
    if (!rate) continue;
    const rn = BigInt(rate.numerator) * num;
    const rd = BigInt(rate.denominator) * den;
    const g = gcd(rn, rd);
    const nStr = (rn / g).toString();
    const dStr = (rd / g).toString();
    if (nStr.length > 38 || dStr.length > 38) return null;
    out[metric] = { numerator: nStr, denominator: dStr };
  }
  return out as Pricing["rates"];
}

function overridePriority(o: MarketplaceOverrideRow, modelName: string, operation: string): number {
  if (!o.enabled) return 99;
  const em = o.modelName === modelName;
  const wm = o.modelName === "*";
  const eo = o.operation === operation;
  const wo = o.operation === "*";
  if (o.overrideKind === "EXACT_PRICE") {
    if (em && eo) return 1;
    if (em && wo) return 2;
    return 99;
  }
  if (em && eo) return 3;
  if (em && wo) return 4;
  if (wm && eo) return 5;
  if (wm && wo) return 6;
  return 99;
}

export function resolveMarketplacePricing(input: {
  modelName: string;
  operation: string;
  currency: string;
  listRates: Pricing["rates"];
  overrides: readonly MarketplaceOverrideRow[];
  exactPricesById: ReadonlyMap<string, MarketplacePriceSnapshot>;
}): EffectiveMarketplacePricing {
  let bestPrio = 99;
  let resolved: EffectiveMarketplacePricing = {
    currency: input.currency,
    list_rates: input.listRates,
    effective_rates: input.listRates,
    override_applied: null,
  };

  for (const o of input.overrides) {
    const p = overridePriority(o, input.modelName, input.operation);
    if (p >= bestPrio) continue;
    if (o.overrideKind === "EXACT_PRICE" && o.billingPriceId) {
      const snap = input.exactPricesById.get(o.billingPriceId);
      if (snap && snap.side === "CUSTOMER" && snap.currency === input.currency) {
        bestPrio = p;
        resolved = {
          currency: input.currency,
          list_rates: input.listRates,
          effective_rates: snap.rates,
          override_applied: {
            id: o.id,
            override_kind: "EXACT_PRICE",
            multiplier_numerator: null,
            multiplier_denominator: null,
            billing_price_id: o.billingPriceId,
          },
        };
      }
    } else if (
      o.overrideKind === "MULTIPLIER" &&
      o.multiplierNumerator &&
      o.multiplierDenominator
    ) {
      const num = BigInt(o.multiplierNumerator);
      const den = BigInt(o.multiplierDenominator);
      if (num > 0n && den > 0n) {
        const scaled = scaleRates(input.listRates, num, den);
        if (scaled) {
          bestPrio = p;
          resolved = {
            currency: input.currency,
            list_rates: input.listRates,
            effective_rates: scaled,
            override_applied: {
              id: o.id,
              override_kind: "MULTIPLIER",
              multiplier_numerator: o.multiplierNumerator,
              multiplier_denominator: o.multiplierDenominator,
              billing_price_id: null,
            },
          };
        }
      }
    }
  }

  return resolved;
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
