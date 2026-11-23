import { expect, test } from "bun:test";
import type { Pricing } from "@gateway/kernel";
import {
  buildSdkSnippets,
  resolveMarketplacePricing,
  type MarketplaceOverrideRow,
  type MarketplacePriceSnapshot,
} from "../marketplace.ts";

const listRates: Pricing["rates"] = {
  input_tokens: { numerator: "100", denominator: "1" },
  output_tokens: { numerator: "300", denominator: "1" },
};

test("resolveMarketplacePricing follows strict 6-level override priority on identical model and operation", () => {
  const exactOpRates: Pricing["rates"] = {
    input_tokens: { numerator: "11", denominator: "1" },
    output_tokens: { numerator: "22", denominator: "1" },
  };
  const exactWildcardOpRates: Pricing["rates"] = {
    input_tokens: { numerator: "33", denominator: "1" },
    output_tokens: { numerator: "44", denominator: "1" },
  };
  const exactPricesById = new Map<string, MarketplacePriceSnapshot>([
    ["p-exact-op", { side: "CUSTOMER", currency: "USD", rates: exactOpRates }],
    ["p-exact-any", { side: "CUSTOMER", currency: "USD", rates: exactWildcardOpRates }],
  ]);

  const rankedOverrides: MarketplaceOverrideRow[] = [
    {
      id: "prio-6",
      modelName: "*",
      operation: "*",
      overrideKind: "MULTIPLIER",
      billingPriceId: null,
      multiplierNumerator: "9",
      multiplierDenominator: "10",
      enabled: true,
    },
    {
      id: "prio-5",
      modelName: "*",
      operation: "chat",
      overrideKind: "MULTIPLIER",
      billingPriceId: null,
      multiplierNumerator: "8",
      multiplierDenominator: "10",
      enabled: true,
    },
    {
      id: "prio-4",
      modelName: "gpt-4o",
      operation: "*",
      overrideKind: "MULTIPLIER",
      billingPriceId: null,
      multiplierNumerator: "7",
      multiplierDenominator: "10",
      enabled: true,
    },
    {
      id: "prio-3",
      modelName: "gpt-4o",
      operation: "chat",
      overrideKind: "MULTIPLIER",
      billingPriceId: null,
      multiplierNumerator: "6",
      multiplierDenominator: "10",
      enabled: true,
    },
    {
      id: "prio-2",
      modelName: "gpt-4o",
      operation: "*",
      overrideKind: "EXACT_PRICE",
      billingPriceId: "p-exact-any",
      multiplierNumerator: null,
      multiplierDenominator: null,
      enabled: true,
    },
    {
      id: "prio-1",
      modelName: "gpt-4o",
      operation: "chat",
      overrideKind: "EXACT_PRICE",
      billingPriceId: "p-exact-op",
      multiplierNumerator: null,
      multiplierDenominator: null,
      enabled: true,
    },
  ];

  const expectedWinners = [
    { id: "prio-1", inputNum: "11" },
    { id: "prio-2", inputNum: "33" },
    { id: "prio-3", inputNum: "60" },
    { id: "prio-4", inputNum: "70" },
    { id: "prio-5", inputNum: "80" },
    { id: "prio-6", inputNum: "90" },
  ];

  let active = [...rankedOverrides];
  for (const expected of expectedWinners) {
    const res = resolveMarketplacePricing({
      modelName: "gpt-4o",
      operation: "chat",
      currency: "USD",
      listRates,
      overrides: active,
      exactPricesById,
    });
    expect(res.override_applied?.id).toBe(expected.id);
    expect(res.effective_rates.input_tokens).toEqual({
      numerator: expected.inputNum,
      denominator: "1",
    });
    active = active.filter((o) => o.id !== expected.id);
  }

  const fallback = resolveMarketplacePricing({
    modelName: "gpt-4o",
    operation: "chat",
    currency: "USD",
    listRates,
    overrides: active,
    exactPricesById,
  });
  expect(fallback.override_applied).toBeNull();
  expect(fallback.effective_rates).toEqual(listRates);
});

test("resolveMarketplacePricing ignores disabled, SUPPLIER-side, currency-mismatched, and 38-digit overflowing overrides", () => {
  const exactPricesById = new Map<string, MarketplacePriceSnapshot>([
    [
      "p-supplier",
      {
        side: "SUPPLIER",
        currency: "USD",
        rates: { input_tokens: { numerator: "1", denominator: "1" } },
      },
    ],
    [
      "p-eur",
      {
        side: "CUSTOMER",
        currency: "EUR",
        rates: { input_tokens: { numerator: "2", denominator: "1" } },
      },
    ],
  ]);

  const huge = "9".repeat(38);
  const overrides: MarketplaceOverrideRow[] = [
    {
      id: "disabled-exact",
      modelName: "gpt-4o",
      operation: "chat",
      overrideKind: "EXACT_PRICE",
      billingPriceId: "p-eur",
      multiplierNumerator: null,
      multiplierDenominator: null,
      enabled: false,
    },
    {
      id: "supplier-exact",
      modelName: "gpt-4o",
      operation: "chat",
      overrideKind: "EXACT_PRICE",
      billingPriceId: "p-supplier",
      multiplierNumerator: null,
      multiplierDenominator: null,
      enabled: true,
    },
    {
      id: "eur-exact",
      modelName: "gpt-4o",
      operation: "*",
      overrideKind: "EXACT_PRICE",
      billingPriceId: "p-eur",
      multiplierNumerator: null,
      multiplierDenominator: null,
      enabled: true,
    },
    {
      id: "overflow-mult",
      modelName: "gpt-4o",
      operation: "chat",
      overrideKind: "MULTIPLIER",
      billingPriceId: null,
      multiplierNumerator: huge,
      multiplierDenominator: "1",
      enabled: true,
    },
    {
      id: "valid-fallback",
      modelName: "*",
      operation: "*",
      overrideKind: "MULTIPLIER",
      billingPriceId: null,
      multiplierNumerator: "3",
      multiplierDenominator: "4",
      enabled: true,
    },
  ];

  const res = resolveMarketplacePricing({
    modelName: "gpt-4o",
    operation: "chat",
    currency: "USD",
    listRates,
    overrides,
    exactPricesById,
  });
  expect(res.override_applied?.id).toBe("valid-fallback");
  expect(res.effective_rates.input_tokens).toEqual({ numerator: "75", denominator: "1" });
  expect(res.effective_rates.output_tokens).toEqual({ numerator: "225", denominator: "1" });
});

test("buildSdkSnippets returns only snippets matching requested model operations", () => {
  const chatOnly = buildSdkSnippets("https://api.example.com/", "gpt-4o", ["chat"]);
  expect(Object.keys(chatOnly).sort()).toEqual(["curl", "openai_ts"]);
  expect(chatOnly.curl).toContain("https://api.example.com/v1/chat/completions");

  const messagesOnly = buildSdkSnippets("https://api.example.com", "claude-sonnet", ["messages"]);
  expect(Object.keys(messagesOnly)).toEqual(["anthropic_curl"]);
  expect(messagesOnly.anthropic_curl).toContain("https://api.example.com/v1/messages");

  const videoOnly = buildSdkSnippets("https://api.example.com", "sora-2", ["videos"]);
  expect(Object.keys(videoOnly)).toEqual(["video_curl"]);
  expect(videoOnly.video_curl).toContain("https://api.example.com/v1/videos");
});
