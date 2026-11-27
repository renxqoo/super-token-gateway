import { expect, test } from "bun:test";
import type { Pricing } from "@gateway/kernel";
import type { PriceVersionRecord } from "@gateway/ledger";
import { buildEstimatedStreamUsage } from "../inference/usage.ts";

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

test("symptom: Gemini reasoning_tokens never exceed estimated or hold-clamped output_tokens", () => {
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
  expect(clamped).toEqual({
    usage: { input_tokens: "10", output_tokens: "20", reasoning_tokens: "20" },
    estimated: true,
  });
});
