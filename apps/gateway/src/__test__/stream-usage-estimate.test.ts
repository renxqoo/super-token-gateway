import { expect, test } from "bun:test";
import type { Pricing } from "@gateway/kernel";
import type { PriceVersionRecord } from "@gateway/ledger";
import {
  buildEstimatedStreamUsage,
  buildModerationUsageFallback,
  canEstimateStreamSettlementError,
  observeClientChunkPlaintext,
} from "../inference/usage.ts";

function makePrice(
  side: "CUSTOMER" | "SUPPLIER",
  basis: PriceVersionRecord["basis"] = "PROVIDER_USAGE",
  rates: Pricing["rates"] = {
    input_tokens: { numerator: "1", denominator: "1" },
    output_tokens: { numerator: "1", denominator: "1" },
  },
  ceilings: PriceVersionRecord["ceilings"] = { input_tokens: "1000", output_tokens: "1000" },
): PriceVersionRecord {
  return {
    id: crypto.randomUUID(),
    side,
    currency: "USD",
    basis,
    rates,
    ceilings,
    source_digest: "test",
  };
}

function pricingOf(price: PriceVersionRecord): Pricing {
  return { currency: price.currency, rates: price.rates };
}

test("symptom: buildEstimatedStreamUsage returns null instead of fabricating debt when observed usage exceeds customer or supplier hold", () => {
  const customerPrice = makePrice("CUSTOMER");
  const supplierPrice = makePrice("SUPPLIER");
  const body = JSON.stringify({ messages: [{ role: "user", content: "hello" }] });

  expect(
    buildEstimatedStreamUsage({
      operation: "messages",
      customerPrice,
      supplierPrice,
      customerPricing: pricingOf(customerPrice),
      supplierPricing: pricingOf(supplierPrice),
      observedUsage: { input_tokens: "80" },
      preparedBody: body,
      inputChars: undefined,
      outputStats: { asciiChars: 40, nonAsciiChars: 0, chunks: 1 },
      streamCompleted: false,
      heldCustomerAtoms: 50n,
      heldSupplierAtoms: 500n,
    }),
  ).toBeNull();

  expect(
    buildEstimatedStreamUsage({
      operation: "messages",
      customerPrice,
      supplierPrice,
      customerPricing: pricingOf(customerPrice),
      supplierPricing: pricingOf(supplierPrice),
      observedUsage: { input_tokens: "80" },
      preparedBody: body,
      inputChars: undefined,
      outputStats: { asciiChars: 40, nonAsciiChars: 0, chunks: 1 },
      streamCompleted: false,
      heldCustomerAtoms: 500n,
      heldSupplierAtoms: 50n,
    }),
  ).toBeNull();

  expect(
    buildEstimatedStreamUsage({
      operation: "chat",
      customerPrice,
      supplierPrice,
      customerPricing: pricingOf(customerPrice),
      supplierPricing: pricingOf(supplierPrice),
      observedUsage: undefined,
      preparedBody: body,
      inputChars: undefined,
      outputStats: { asciiChars: 40, nonAsciiChars: 0, chunks: 1 },
      streamCompleted: false,
      heldCustomerAtoms: 0n,
      heldSupplierAtoms: 500n,
    }),
  ).toBeNull();

  expect(
    buildEstimatedStreamUsage({
      operation: "messages",
      customerPrice,
      supplierPrice,
      customerPricing: pricingOf(customerPrice),
      supplierPricing: pricingOf(supplierPrice),
      observedUsage: { input_tokens: "50" },
      preparedBody: body,
      inputChars: undefined,
      outputStats: { asciiChars: 40, nonAsciiChars: 0, chunks: 1 },
      streamCompleted: false,
      heldCustomerAtoms: 50n,
      heldSupplierAtoms: 500n,
    }),
  ).toBeNull();
});

test("symptom: interrupted non-PROVIDER_USAGE streams return null while completed streams settle normally", () => {
  const flatCustomer = makePrice(
    "CUSTOMER",
    "REQUEST_COMPLETION",
    { requests: { numerator: "100", denominator: "1" } },
    { requests: "1" },
  );
  const charCustomer = makePrice(
    "CUSTOMER",
    "INPUT_CHARACTERS",
    { characters: { numerator: "2", denominator: "1" } },
    { characters: "1000" },
  );
  const supplierPrice = makePrice("SUPPLIER");
  const body = JSON.stringify({ messages: [{ role: "user", content: "hello" }] });

  expect(
    buildEstimatedStreamUsage({
      operation: "chat",
      customerPrice: flatCustomer,
      supplierPrice,
      customerPricing: pricingOf(flatCustomer),
      supplierPricing: pricingOf(supplierPrice),
      observedUsage: undefined,
      preparedBody: body,
      inputChars: 5,
      outputStats: { asciiChars: 20, nonAsciiChars: 0, chunks: 1 },
      streamCompleted: false,
      heldCustomerAtoms: 100n,
      heldSupplierAtoms: 500n,
    }),
  ).toBeNull();

  expect(
    buildEstimatedStreamUsage({
      operation: "chat",
      customerPrice: charCustomer,
      supplierPrice,
      customerPricing: pricingOf(charCustomer),
      supplierPricing: pricingOf(supplierPrice),
      observedUsage: undefined,
      preparedBody: body,
      inputChars: 5,
      outputStats: { asciiChars: 20, nonAsciiChars: 0, chunks: 1 },
      streamCompleted: false,
      heldCustomerAtoms: 100n,
      heldSupplierAtoms: 500n,
    }),
  ).toBeNull();

  expect(
    buildEstimatedStreamUsage({
      operation: "chat",
      customerPrice: flatCustomer,
      supplierPrice,
      customerPricing: pricingOf(flatCustomer),
      supplierPricing: pricingOf(supplierPrice),
      observedUsage: undefined,
      preparedBody: body,
      inputChars: 5,
      outputStats: { asciiChars: 20, nonAsciiChars: 0, chunks: 1 },
      streamCompleted: true,
      heldCustomerAtoms: 100n,
      heldSupplierAtoms: 500n,
    }),
  ).toEqual({
    usage: { input_tokens: "12", output_tokens: "5" },
    estimated: true,
  });
});

test("buildEstimatedStreamUsage validates cache write TTL partitions, supplier clamping, and non-estimated passthrough", () => {
  const splitPrice = makePrice(
    "CUSTOMER",
    "PROVIDER_USAGE",
    {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "1", denominator: "1" },
      cache_write_5m_tokens: { numerator: "2", denominator: "1" },
      cache_write_1h_tokens: { numerator: "3", denominator: "1" },
    },
    { input_tokens: "1000", output_tokens: "1000" },
  );
  const supplierPrice = makePrice(
    "SUPPLIER",
    "PROVIDER_USAGE",
    {
      input_tokens: { numerator: "10", denominator: "1" },
      output_tokens: { numerator: "10", denominator: "1" },
    },
    { input_tokens: "1000", output_tokens: "1000" },
  );
  const body = JSON.stringify({ messages: [{ role: "user", content: "hello" }] });

  expect(
    buildEstimatedStreamUsage({
      operation: "messages",
      customerPrice: splitPrice,
      supplierPrice,
      customerPricing: pricingOf(splitPrice),
      supplierPricing: pricingOf(supplierPrice),
      observedUsage: { input_tokens: "10", cache_write_tokens: "20" },
      preparedBody: body,
      inputChars: undefined,
      outputStats: { asciiChars: 20, nonAsciiChars: 0, chunks: 1 },
      streamCompleted: false,
      heldCustomerAtoms: 1000n,
      heldSupplierAtoms: 1000n,
    }),
  ).toBeNull();

  const validCache = buildEstimatedStreamUsage({
    operation: "messages",
    customerPrice: splitPrice,
    supplierPrice,
    customerPricing: pricingOf(splitPrice),
    supplierPricing: pricingOf(supplierPrice),
    observedUsage: {
      input_tokens: "10",
      cache_write_tokens: "20",
      cache_write_5m_tokens: "12",
      cache_write_1h_tokens: "8",
    },
    preparedBody: body,
    inputChars: undefined,
    outputStats: { asciiChars: 400, nonAsciiChars: 0, chunks: 1 },
    streamCompleted: false,
    heldCustomerAtoms: 1000n,
    heldSupplierAtoms: 250n,
  });
  expect(validCache).not.toBeNull();
  expect(validCache!.estimated).toBe(true);
  expect(Number(validCache!.usage.output_tokens)).toBe(15);

  const exactObserved = buildEstimatedStreamUsage({
    operation: "chat",
    customerPrice: makePrice("CUSTOMER"),
    supplierPrice: makePrice("SUPPLIER"),
    customerPricing: pricingOf(makePrice("CUSTOMER")),
    supplierPricing: pricingOf(makePrice("SUPPLIER")),
    observedUsage: { input_tokens: "10", output_tokens: "12" },
    preparedBody: body,
    inputChars: undefined,
    outputStats: { asciiChars: 20, nonAsciiChars: 0, chunks: 1 },
    streamCompleted: false,
    heldCustomerAtoms: 100n,
    heldSupplierAtoms: 100n,
  });
  expect(exactObserved).toEqual({
    usage: { input_tokens: "10", output_tokens: "12" },
    estimated: false,
  });

  expect(
    buildEstimatedStreamUsage({
      operation: "chat",
      customerPrice: makePrice("CUSTOMER"),
      supplierPrice: makePrice("SUPPLIER"),
      customerPricing: pricingOf(makePrice("CUSTOMER")),
      supplierPricing: pricingOf(makePrice("SUPPLIER")),
      observedUsage: { input_tokens: "80", output_tokens: "30" },
      preparedBody: body,
      inputChars: undefined,
      outputStats: { asciiChars: 20, nonAsciiChars: 0, chunks: 1 },
      streamCompleted: false,
      heldCustomerAtoms: 100n,
      heldSupplierAtoms: 100n,
    }),
  ).toBeNull();
});

test("symptom: an insufficient hold can reduce provider-reported Gemini reasoning usage", () => {
  const customerPrice = makePrice("CUSTOMER");
  const supplierPrice = makePrice("SUPPLIER");
  const body = JSON.stringify({ contents: [{ parts: [{ text: "Hi" }] }] });

  const lifted = buildEstimatedStreamUsage({
    operation: "gemini",
    customerPrice,
    supplierPrice,
    customerPricing: pricingOf(customerPrice),
    supplierPricing: pricingOf(supplierPrice),
    observedUsage: { input_tokens: "10", reasoning_tokens: "40" },
    preparedBody: body,
    inputChars: undefined,
    outputStats: { asciiChars: 8, nonAsciiChars: 0, chunks: 1 },
    streamCompleted: true,
    heldCustomerAtoms: 100n,
    heldSupplierAtoms: 100n,
  });
  expect(lifted).toEqual({
    usage: { input_tokens: "10", output_tokens: "40", reasoning_tokens: "40" },
    estimated: true,
  });

  const clamped = buildEstimatedStreamUsage({
    operation: "gemini",
    customerPrice,
    supplierPrice,
    customerPricing: pricingOf(customerPrice),
    supplierPricing: pricingOf(supplierPrice),
    observedUsage: { input_tokens: "10", reasoning_tokens: "40" },
    preparedBody: body,
    inputChars: undefined,
    outputStats: { asciiChars: 8, nonAsciiChars: 0, chunks: 1 },
    streamCompleted: true,
    heldCustomerAtoms: 30n,
    heldSupplierAtoms: 100n,
  });
  expect(clamped).toBeNull();
});

test("symptom: moderation estimation miscounts CJK text or reduces provider-reported reasoning usage", () => {
  const customerPrice = makePrice(
    "CUSTOMER",
    "PROVIDER_USAGE",
    {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "2", denominator: "1" },
    },
    { input_tokens: "100", output_tokens: "100" },
  );
  const supplierPrice = makePrice(
    "SUPPLIER",
    "PROVIDER_USAGE",
    {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "5", denominator: "1" },
    },
    { input_tokens: "100", output_tokens: "100" },
  );
  const cjkBody = JSON.stringify({
    messages: [{ role: "user", content: "测试中文提示词内容".repeat(4) }],
  });

  const cjkUnclamped = buildModerationUsageFallback(undefined, 32, 20, {
    preparedBody: cjkBody,
    outputStats: { asciiChars: 0, nonAsciiChars: 20, chunks: 1 },
    customerPrice,
    supplierPrice,
    customerPricing: pricingOf(customerPrice),
    supplierPricing: pricingOf(supplierPrice),
    heldCustomerAtoms: 1000n,
    heldSupplierAtoms: 1000n,
  });
  expect(Number(cjkUnclamped!.input_tokens)).toBeGreaterThan(32);
  expect(cjkUnclamped!.output_tokens).toBe("20");

  const supplierClamped = buildModerationUsageFallback({ input_tokens: "10" }, 32, 20, {
    preparedBody: cjkBody,
    outputStats: { asciiChars: 0, nonAsciiChars: 20, chunks: 1 },
    customerPrice,
    supplierPrice,
    customerPricing: pricingOf(customerPrice),
    supplierPricing: pricingOf(supplierPrice),
    heldCustomerAtoms: 1000n,
    heldSupplierAtoms: 45n,
  });
  expect(supplierClamped!.input_tokens).toBe("10");
  expect(Number(supplierClamped!.output_tokens)).toBe(7);

  const ceilingClamped = buildModerationUsageFallback(undefined, 2000, 2000, {
    preparedBody: "中".repeat(500),
    outputStats: { asciiChars: 0, nonAsciiChars: 500, chunks: 1 },
    customerPrice,
    supplierPrice,
    customerPricing: pricingOf(customerPrice),
    supplierPricing: pricingOf(supplierPrice),
    heldCustomerAtoms: 10_000n,
    heldSupplierAtoms: 10_000n,
  });
  expect(ceilingClamped).toEqual({ input_tokens: "100", output_tokens: "100" });

  const reasoningLifted = buildModerationUsageFallback(
    { input_tokens: "10", reasoning_tokens: "40" },
    10,
    8,
    {
      preparedBody: cjkBody,
      outputStats: { asciiChars: 8, nonAsciiChars: 0, chunks: 1 },
      customerPrice,
      supplierPrice,
      customerPricing: pricingOf(customerPrice),
      supplierPricing: pricingOf(supplierPrice),
      heldCustomerAtoms: 1000n,
      heldSupplierAtoms: 1000n,
    },
  );
  expect(reasoningLifted).toEqual({
    input_tokens: "10",
    output_tokens: "40",
    reasoning_tokens: "40",
  });

  const reasoningClamped = buildModerationUsageFallback(
    { input_tokens: "10", reasoning_tokens: "40" },
    10,
    8,
    {
      preparedBody: cjkBody,
      outputStats: { asciiChars: 8, nonAsciiChars: 0, chunks: 1 },
      customerPrice,
      supplierPrice,
      customerPricing: pricingOf(customerPrice),
      supplierPricing: pricingOf(supplierPrice),
      heldCustomerAtoms: 50n,
      heldSupplierAtoms: 1000n,
    },
  );
  expect(reasoningClamped).toBeNull();

  const ttlSplitPrice = makePrice(
    "CUSTOMER",
    "PROVIDER_USAGE",
    {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "1", denominator: "1" },
      cache_write_5m_tokens: { numerator: "2", denominator: "1" },
      cache_write_1h_tokens: { numerator: "3", denominator: "1" },
    },
    { input_tokens: "100", output_tokens: "100" },
  );
  expect(
    buildModerationUsageFallback({ input_tokens: "10", cache_write_tokens: "20" }, 10, 8, {
      preparedBody: cjkBody,
      outputStats: { asciiChars: 8, nonAsciiChars: 0, chunks: 1 },
      customerPrice: ttlSplitPrice,
      supplierPrice,
      customerPricing: pricingOf(ttlSplitPrice),
      supplierPricing: pricingOf(supplierPrice),
      heldCustomerAtoms: 1000n,
      heldSupplierAtoms: 1000n,
    }),
  ).toBeNull();

  const legacyFallback = buildModerationUsageFallback(undefined, 40, 12);
  expect(legacyFallback).toEqual({ input_tokens: "10", output_tokens: "3" });
});

test("buildEstimatedStreamUsage preserves Anthropic input and cache counts on mid-stream disconnect and estimates tool/reasoning-only chunks", () => {
  const customerPrice = makePrice(
    "CUSTOMER",
    "PROVIDER_USAGE",
    {
      input_tokens: { numerator: "1", denominator: "1" },
      cached_input_tokens: { numerator: "1", denominator: "10" },
      cache_write_tokens: { numerator: "2", denominator: "1" },
      output_tokens: { numerator: "5", denominator: "1" },
    },
    {
      input_tokens: "10000",
      cached_input_tokens: "10000",
      cache_write_tokens: "1000",
      output_tokens: "1000",
    },
  );
  const supplierPrice = makePrice("SUPPLIER");
  const body = JSON.stringify({ messages: [{ role: "user", content: "hi" }] });

  const anthropicCut = buildEstimatedStreamUsage({
    operation: "messages",
    customerPrice,
    supplierPrice,
    customerPricing: pricingOf(customerPrice),
    supplierPricing: pricingOf(supplierPrice),
    observedUsage: {
      input_tokens: "30",
      cached_input_tokens: "2000",
      cache_write_tokens: "100",
    },
    preparedBody: body,
    inputChars: undefined,
    outputStats: { asciiChars: 40, nonAsciiChars: 0, chunks: 2 },
    streamCompleted: false,
    heldCustomerAtoms: 5000n,
    heldSupplierAtoms: 5000n,
  });
  expect(anthropicCut).toEqual({
    usage: {
      input_tokens: "30",
      cached_input_tokens: "2000",
      cache_write_tokens: "100",
      output_tokens: "10",
    },
    estimated: true,
  });

  const toolStats = { asciiChars: 0, nonAsciiChars: 0, chunks: 0 };
  observeClientChunkPlaintext(toolStats, "chat", {
    choices: [
      {
        delta: {
          tool_calls: [{ function: { name: "get_weather", arguments: '{"city":"Tokyo"}' } }],
        },
      },
    ],
  });
  observeClientChunkPlaintext(toolStats, "responses", {
    type: "response.output_item.added",
    item: { type: "function_call", name: "lookup" },
  });
  observeClientChunkPlaintext(toolStats, "responses", {
    type: "response.function_call_arguments.delta",
    delta: '{"id":42}',
  });
  observeClientChunkPlaintext(toolStats, "messages", {
    type: "content_block_delta",
    delta: { thinking: "reasoning silently" },
  });
  observeClientChunkPlaintext(toolStats, "gemini", {
    candidates: [
      { content: { parts: [{ functionCall: { name: "search", args: { q: "news" } } }] } },
    ],
  });
  expect(toolStats.chunks).toBeGreaterThan(0);
  expect(toolStats.asciiChars).toBeGreaterThan(40);

  const toolOnlyEst = buildEstimatedStreamUsage({
    operation: "chat",
    customerPrice: makePrice("CUSTOMER"),
    supplierPrice,
    customerPricing: pricingOf(makePrice("CUSTOMER")),
    supplierPricing: pricingOf(supplierPrice),
    observedUsage: undefined,
    preparedBody: body,
    inputChars: undefined,
    outputStats: toolStats,
    streamCompleted: false,
    heldCustomerAtoms: 1000n,
    heldSupplierAtoms: 1000n,
  });
  expect(toolOnlyEst).not.toBeNull();
  expect(Number(toolOnlyEst!.usage.output_tokens)).toBeGreaterThan(0);

  for (const estimable of [
    "stream_interrupted",
    "stream_truncated",
    "client_aborted",
    "upstream_timeout",
    "upstream_stream_idle_timeout",
  ]) {
    expect(canEstimateStreamSettlementError(estimable), estimable).toBe(true);
  }
  for (const nonEstimable of [
    "provider_stream_error",
    "invalid_upstream",
    "unexpected_trailing_event",
  ]) {
    expect(canEstimateStreamSettlementError(nonEstimable), nonEstimable).toBe(false);
  }
});

test("symptom: stream and moderation fallbacks fail closed when authorized hosted tools or priced non-token snapshot dimensions are unobserved", () => {
  const custPrice = makePrice("CUSTOMER");
  const supPrice = makePrice("SUPPLIER");
  const wsSnapshot = {
    version: 1 as const,
    rated_at: "2026-06-15T12:00:00Z",
    side: "CUSTOMER" as const,
    currency: "USD",
    base_currency: "USD",
    rate_card_id: custPrice.id,
    basis: "PROVIDER_USAGE" as const,
    effective_card: {
      id: custPrice.id,
      side: "CUSTOMER" as const,
      currency: "USD",
      basis: "PROVIDER_USAGE" as const,
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "2", denominator: "1" },
        web_search_calls: { numerator: "500", denominator: "1" },
      },
      ceilings: { input_tokens: "1000", output_tokens: "100", web_search_calls: "3" },
    },
    matched_matrix_index: null,
    applied_modifiers: [],
    fx_version_id: null,
    fx_rate: null,
    combined_multiplier: { numerator: "1", denominator: "1" },
  };
  const body = JSON.stringify({ model: "gpt-ws", input: "search" });
  const outputStats = { asciiChars: 40, nonAsciiChars: 0, chunks: 2 };

  expect(
    buildEstimatedStreamUsage({
      operation: "responses",
      customerPrice: custPrice,
      supplierPrice: supPrice,
      customerPricing: pricingOf(custPrice),
      supplierPricing: pricingOf(supPrice),
      customerSnapshot: wsSnapshot,
      authorizedHostedTools: new Set(["web_search_calls"]),
      observedUsage: undefined,
      preparedBody: body,
      inputChars: 6,
      outputStats,
      streamCompleted: false,
      heldCustomerAtoms: 5000n,
      heldSupplierAtoms: 5000n,
    }),
  ).toBeNull();

  expect(
    buildEstimatedStreamUsage({
      operation: "responses",
      customerPrice: custPrice,
      supplierPrice: supPrice,
      customerPricing: pricingOf(custPrice),
      supplierPricing: pricingOf(supPrice),
      customerSnapshot: wsSnapshot,
      observedUsage: undefined,
      preparedBody: body,
      inputChars: 6,
      outputStats,
      streamCompleted: true,
      heldCustomerAtoms: 5000n,
      heldSupplierAtoms: 5000n,
    }),
  ).toBeNull();

  expect(
    buildEstimatedStreamUsage({
      operation: "responses",
      customerPrice: custPrice,
      supplierPrice: supPrice,
      customerPricing: pricingOf(custPrice),
      supplierPricing: pricingOf(supPrice),
      customerSnapshot: wsSnapshot,
      authorizedHostedTools: new Set(),
      observedUsage: { input_tokens: "5" },
      preparedBody: body,
      inputChars: 6,
      outputStats,
      streamCompleted: true,
      heldCustomerAtoms: 5000n,
      heldSupplierAtoms: 5000n,
    }),
  ).toEqual({
    usage: { input_tokens: "5", output_tokens: "10", web_search_calls: "0" },
    estimated: true,
  });

  expect(
    buildModerationUsageFallback(undefined, 20, 40, {
      preparedBody: body,
      outputStats,
      customerPrice: custPrice,
      supplierPrice: supPrice,
      customerPricing: pricingOf(custPrice),
      supplierPricing: pricingOf(supPrice),
      customerSnapshot: wsSnapshot,
      authorizedHostedTools: new Set(["web_search_calls"]),
      heldCustomerAtoms: 5000n,
      heldSupplierAtoms: 5000n,
    }),
  ).toBeNull();

  const observedComplete = buildEstimatedStreamUsage({
    operation: "responses",
    customerPrice: custPrice,
    supplierPrice: supPrice,
    customerPricing: pricingOf(custPrice),
    supplierPricing: pricingOf(supPrice),
    customerSnapshot: wsSnapshot,
    authorizedHostedTools: new Set(["web_search_calls"]),
    observedUsage: { web_search_calls: "1" },
    preparedBody: body,
    inputChars: 6,
    outputStats,
    streamCompleted: false,
    heldCustomerAtoms: 5000n,
    heldSupplierAtoms: 5000n,
  });
  expect(observedComplete).not.toBeNull();
  expect(observedComplete!.usage.web_search_calls).toBe("1");
});
