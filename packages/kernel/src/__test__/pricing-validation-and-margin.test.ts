import { expect, test } from "bun:test";
import {
  evaluateMargin,
  evaluateSnapshotAtoms,
  resolveAccountPricingOverride,
  snapshotFromFlatPrice,
  validateFxRateVersion,
  validatePriceModifier,
  validateRateCard,
} from "../index.ts";

const R = (numerator: string, denominator = "1") => ({ numerator, denominator });

test("evaluateMargin computes exact same-currency and FX-normalized margin basis points and enforces policies", () => {
  const positive = evaluateMargin({
    customerCurrency: "USD",
    customerAtoms: 10_000n,
    supplierCurrency: "USD",
    supplierAtoms: 7_500n,
    policy: { allow_negative_margin: false, min_margin_bps: 2000n },
  });
  expect(positive.comparable).toBe(true);
  expect(positive.gross_margin_atoms).toBe(2_500n);
  expect(positive.margin_bps).toBe(2500n);
  expect(positive.is_negative_margin).toBe(false);
  expect(positive.allowed).toBe(true);

  const negativeBlocked = evaluateMargin({
    customerCurrency: "CNY",
    customerAtoms: 7_000n,
    supplierCurrency: "USD",
    supplierAtoms: 1_000n,
    supplierToCustomerFx: R("725", "100"),
    policy: { allow_negative_margin: false },
  });
  expect(negativeBlocked.normalized_supplier_atoms).toBe(7_250n);
  expect(negativeBlocked.gross_margin_atoms).toBe(-250n);
  expect(negativeBlocked.is_negative_margin).toBe(true);
  expect(negativeBlocked.allowed).toBe(false);
  expect(negativeBlocked.reason).toBe("negative_margin_blocked");

  const missingFx = evaluateMargin({
    customerCurrency: "CNY",
    customerAtoms: 7_000n,
    supplierCurrency: "USD",
    supplierAtoms: 1_000n,
    policy: { allow_negative_margin: false },
  });
  expect(missingFx.comparable).toBe(false);
  expect(missingFx.allowed).toBe(false);
  expect(missingFx.reason).toBe("fx_rate_unavailable_for_margin_guard");
});

test("static validators reject malformed cards, non-monotonic tiers, invalid timezones, and reversed windows", () => {
  expect(() =>
    validateRateCard({
      id: "bad-cliff",
      side: "CUSTOMER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: { input_tokens: R("1") },
      cliff: { driver: ["input_tokens"], tiers: [{ min_driver: "100", rates: {} }] },
      ceilings: { input_tokens: "1000" },
    }),
  ).toThrow("First cliff tier must start at min_driver 0");

  expect(() =>
    validateRateCard({
      id: "bad-prog",
      side: "CUSTOMER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: {},
      progressive: {
        input_tokens: [{ up_to: "100", rate: R("1") }],
      },
      ceilings: { input_tokens: "1000" },
    }),
  ).toThrow("up_to: null");

  expect(() =>
    validateRateCard({
      id: "bad-bounds",
      side: "CUSTOMER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: { input_tokens: R("1") },
      ceilings: { input_tokens: "1000" },
      min_charge_atoms: "500",
      max_charge_atoms: "400",
    }),
  ).toThrow("min_charge_atoms cannot exceed max_charge_atoms");

  expect(() =>
    validatePriceModifier({
      id: "bad-tz",
      layer: "PRODUCT",
      kind: "MULTIPLIER",
      stacking: "MULTIPLY",
      priority: 1,
      enabled: true,
      time_windows: [{ timezone: "Mars/Olympus", start_minute: 0, end_minute: 60 }],
      match: {},
      multiplier: R("1", "2"),
    }),
  ).toThrow("Invalid IANA timezone");

  expect(() =>
    validateFxRateVersion({
      id: "same-ccy",
      base_currency: "USD",
      quote_currency: "USD",
      rate: R("1"),
    }),
  ).toThrow("differ");
});

test("legacy snapshotFromFlatPrice and unified resolveAccountPricingOverride match STOP and SKIP policies", () => {
  const baseRates = { input_tokens: R("10"), output_tokens: R("30") };
  const snap = snapshotFromFlatPrice({
    id: "p1",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    rates: baseRates,
    ceilings: { input_tokens: "1000", output_tokens: "1000" },
    discount: { id: "d1", numerator: "3", denominator: "4" },
  });
  expect(evaluateSnapshotAtoms(snap, { input_tokens: "10", output_tokens: "5" })).toBe(188n);

  const overrides = [
    {
      id: "bad-exact",
      model_name: "m1",
      operation: "chat",
      override_kind: "EXACT_PRICE" as const,
      billing_price_id: "missing",
      multiplier_numerator: null,
      multiplier_denominator: null,
      enabled: true,
    },
    {
      id: "wild-half",
      model_name: "*",
      operation: "*",
      override_kind: "MULTIPLIER" as const,
      billing_price_id: null,
      multiplier_numerator: "1",
      multiplier_denominator: "2",
      enabled: true,
    },
  ];
  expect(
    resolveAccountPricingOverride({
      modelName: "m1",
      operation: "chat",
      currency: "USD",
      listRates: baseRates,
      overrides,
      exactPricesById: new Map(),
      invalidPolicy: "STOP",
    }),
  ).toEqual({ kind: "NONE" });
  expect(
    resolveAccountPricingOverride({
      modelName: "m1",
      operation: "chat",
      currency: "USD",
      listRates: baseRates,
      overrides,
      exactPricesById: new Map(),
      invalidPolicy: "SKIP",
    }),
  ).toMatchObject({ kind: "MULTIPLIER", override_id: "wild-half" });
});
