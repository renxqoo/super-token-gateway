import { expect, test } from "bun:test";
import {
  AMOUNT_DECIMALS,
  ATOMS_PER_UNIT,
  DomainError,
  calculateCompiledCost,
  calculateCompiledCostAtoms,
  calculateCost,
  compilePricing,
  formatAtomsToDecimal,
  parseDecimalToAtoms,
  quoteReservation,
} from "../index.ts";

test("fractional dimensions sum to one atom instead of rounding each third", () => {
  expect(
    calculateCost(
      { input_tokens: "1", output_tokens: "1" },
      {
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "3" },
          output_tokens: { numerator: "1", denominator: "3" },
        },
      },
    ),
  ).toBe("1");
});
test("125 music milliseconds at eight atoms per second cost exactly one atom", () => {
  expect(
    calculateCost(
      { music_ms: "125" },
      { currency: "USD", rates: { music_ms: { numerator: "8", denominator: "1000" } } },
    ),
  ).toBe("1");
});
test("large quantities stay exact and fractional music milliseconds are rejected", () => {
  expect(
    calculateCost(
      { music_ms: "9007199254740993" },
      { currency: "USD", rates: { music_ms: { numerator: "1", denominator: "1" } } },
    ),
  ).toBe("9007199254740993");
  expect(() => calculateCost({ music_ms: "0.125" }, { currency: "USD", rates: {} })).toThrow();
});
test("music reservations enforce an explicit music ceiling and request rate", () => {
  expect(
    quoteReservation(
      [
        {
          provider_id: "channel",
          id: "price",
          currency: "USD",
          basis: "PROVIDER_USAGE",
          rates: {
            music_ms: { numerator: "8", denominator: "1000" },
            requests: { numerator: "1", denominator: "3" },
          },
          ceilings: { music_ms: "125", requests: "1" },
        },
      ],
      "USD",
      {},
      0,
    ),
  ).toMatchObject({ amount_atoms: "2", prices: { channel: "price" } });
});

test("text reservations bound estimated input by the highest priced input dimension and fall back above ceilings", () => {
  const price = {
    provider_id: "channel",
    id: "price",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    rates: {
      input_tokens: { numerator: "2", denominator: "1" },
      cached_input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "3", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "100" },
  } as const;
  expect(quoteReservation([price], "USD", { estimatedInputTokens: 100 }, 10).amount_atoms).toBe(
    "230",
  );
  expect(quoteReservation([price], "USD", {}, 10).amount_atoms).toBe("3030");
  expect(quoteReservation([price], "USD", { estimatedInputTokens: 1001 }, 10).amount_atoms).toBe(
    "3030",
  );
  const premium = {
    ...price,
    rates: { ...price.rates, cached_input_tokens: { numerator: "5", denominator: "2" } },
  };
  expect(quoteReservation([premium], "USD", { estimatedInputTokens: 100 }, 10).amount_atoms).toBe(
    "280",
  );
  const cacheWrite = {
    ...price,
    rates: {
      ...price.rates,
      cache_write_tokens: { numerator: "3", denominator: "1" },
    },
    ceilings: {
      ...price.ceilings,
      cache_write_tokens: "200",
    },
  };
  expect(
    quoteReservation([cacheWrite], "USD", { estimatedInputTokens: 100 }, 10).amount_atoms,
  ).toBe("330");
  expect(
    quoteReservation([cacheWrite], "USD", { estimatedInputTokens: 201 }, 10).amount_atoms,
  ).toBe("3630");
  const ttlCacheWrite = {
    ...price,
    rates: {
      ...price.rates,
      cache_write_5m_tokens: { numerator: "5", denominator: "2" },
      cache_write_1h_tokens: { numerator: "4", denominator: "1" },
    },
    ceilings: {
      ...price.ceilings,
      cache_write_5m_tokens: "500",
      cache_write_1h_tokens: "500",
    },
  };
  expect(
    quoteReservation([ttlCacheWrite], "USD", { estimatedInputTokens: 100 }, 10).amount_atoms,
  ).toBe("430");
  expect(
    quoteReservation([ttlCacheWrite], "USD", { estimatedInputTokens: 501 }, 10).amount_atoms,
  ).toBe("6280");
});

test("integral tariff terms and fractional duration share one final rounding", () => {
  expect(
    calculateCost(
      { input_tokens: "1", output_tokens: "2", audio_seconds: "0.125" },
      {
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "3" },
          output_tokens: { numerator: "1", denominator: "1" },
          audio_seconds: { numerator: "1", denominator: "2" },
        },
      },
    ),
  ).toBe("3");
});

test("18-decimal fixed-point parsing and formatting preserve exact financial precision without float loss", () => {
  expect(AMOUNT_DECIMALS).toBe(18);
  expect(ATOMS_PER_UNIT).toBe(1_000_000_000_000_000_000n);

  expect(parseDecimalToAtoms("1")).toBe("1000000000000000000");
  expect(parseDecimalToAtoms("12.345")).toBe("12345000000000000000");
  expect(parseDecimalToAtoms("0.000000000000000001")).toBe("1");
  expect(parseDecimalToAtoms("99999999999999999999.999999999999999999")).toBe("9".repeat(38));
  expect(parseDecimalToAtoms("-0.000000000000000001", { allowNegative: true })).toBe("-1");
  expect(parseDecimalToAtoms("0", { allowZero: true })).toBe("0");

  expect(() => parseDecimalToAtoms("0")).toThrow();
  expect(() => parseDecimalToAtoms("-1")).toThrow();
  expect(() => parseDecimalToAtoms("-0", { allowNegative: true, allowZero: true })).toThrow();
  expect(() => parseDecimalToAtoms("0.0000000000000000001")).toThrow();
  expect(() => parseDecimalToAtoms("1e5")).toThrow();
  expect(() => parseDecimalToAtoms(" 1.0")).toThrow();
  expect(() => parseDecimalToAtoms("100000000000000000000")).toThrow();

  expect(formatAtomsToDecimal("12345000000000000000")).toBe("12.345000000000000000");
  expect(formatAtomsToDecimal("1")).toBe("0.000000000000000001");
  expect(formatAtomsToDecimal("-1")).toBe("-0.000000000000000001");
  expect(formatAtomsToDecimal("0")).toBe("0.000000000000000000");
  expect(formatAtomsToDecimal("9".repeat(38))).toBe("99999999999999999999.999999999999999999");
  expect(
    formatAtomsToDecimal("12345000000000000000", { trimTrailingZeros: true, minDecimals: 2 }),
  ).toBe("12.345");
  expect(
    formatAtomsToDecimal("12000000000000000000", { trimTrailingZeros: true, minDecimals: 2 }),
  ).toBe("12.00");
  expect(formatAtomsToDecimal("12345678900000000000", { fixedDecimals: 4 })).toBe("12.3456");
});

test("compilePricing and calculateCompiledCostAtoms match calculateCost across fast-path, BigInt, multi-dimension, zero-rate, and NUMERIC(38,0) boundaries", () => {
  const fastPricing = {
    currency: "USD",
    rates: {
      input_tokens: { numerator: "500000000", denominator: "1" },
      output_tokens: { numerator: "500000000", denominator: "1" },
    },
  } as const;
  const compiledFast = compilePricing(fastPricing);
  expect(compiledFast.allUnitSafe).toBe(true);

  const fastEdgeUsage = { input_tokens: "999999", output_tokens: "999999" };
  expect(calculateCompiledCost(fastEdgeUsage, compiledFast)).toBe("999999000000000");
  expect(calculateCompiledCost(fastEdgeUsage, compiledFast)).toBe(
    calculateCost(fastEdgeUsage, fastPricing),
  );

  const overflowFastLenUsage = { input_tokens: "1000000", output_tokens: "1" };
  expect(calculateCompiledCost(overflowFastLenUsage, compiledFast)).toBe("500000500000000");
  expect(calculateCompiledCost(overflowFastLenUsage, compiledFast)).toBe(
    calculateCost(overflowFastLenUsage, fastPricing),
  );

  const slowRatePricing = {
    currency: "USD",
    rates: {
      input_tokens: { numerator: "500000001", denominator: "1" },
      cached_input_tokens: { numerator: "1", denominator: "3" },
      cache_write_5m_tokens: { numerator: "5", denominator: "4" },
      cache_write_1h_tokens: { numerator: "7", denominator: "4" },
      output_tokens: { numerator: "11", denominator: "6" },
      characters: { numerator: "2", denominator: "5" },
      images: { numerator: "100", denominator: "1" },
      audio_seconds: { numerator: "3", denominator: "8" },
      video_seconds: { numerator: "7", denominator: "2" },
      music_ms: { numerator: "1", denominator: "125" },
      requests: { numerator: "1", denominator: "7" },
    },
  } as const;
  const compiledSlow = compilePricing(slowRatePricing);
  expect(compiledSlow.allUnitSafe).toBe(false);
  const multiDimUsage = {
    input_tokens: "100",
    cached_input_tokens: "7",
    cache_write_5m_tokens: "9",
    cache_write_1h_tokens: "11",
    output_tokens: "13",
    characters: "17",
    images: "2",
    audio_seconds: "1.25",
    video_seconds: "0.5",
    music_ms: "375",
    requests: "1",
  };
  expect(calculateCompiledCost(multiDimUsage, compiledSlow)).toBe(
    calculateCost(multiDimUsage, slowRatePricing),
  );

  const zeroPricing = {
    currency: "USD",
    rates: {
      input_tokens: { numerator: "0", denominator: "1" },
      output_tokens: { numerator: "0", denominator: "1" },
    },
  } as const;
  const hugeUsage = {
    input_tokens: "1000000000000000000000000000000",
    output_tokens: "1000000000000000000000000000000",
  };
  expect(calculateCost(hugeUsage, zeroPricing)).toBe("0");
  expect(calculateCompiledCostAtoms(hugeUsage, compilePricing(zeroPricing))).toBe(0n);

  const maxPricing = {
    currency: "USD",
    rates: { input_tokens: { numerator: "1", denominator: "1" } },
  } as const;
  const compiledMax = compilePricing(maxPricing);
  const maxAtomsStr = "9".repeat(38);
  expect(calculateCost({ input_tokens: maxAtomsStr }, maxPricing)).toBe(maxAtomsStr);
  expect(calculateCompiledCost({ input_tokens: maxAtomsStr }, compiledMax)).toBe(maxAtomsStr);

  const overflowPricing = {
    currency: "USD",
    rates: { input_tokens: { numerator: "10", denominator: "1" } },
  } as const;
  const compiledOverflow = compilePricing(overflowPricing);
  const justOverUsage = { input_tokens: `1${"0".repeat(37)}` };
  expect(() => calculateCost(justOverUsage, overflowPricing)).toThrow(RangeError);
  expect(() => calculateCompiledCostAtoms(justOverUsage, compiledOverflow)).toThrow(RangeError);
});

test("symptom: kernel calculateCost and calculateCompiledCostAtoms accept mismatched partial cache write partitions and reasoning_tokens above output_tokens", () => {
  const pricing = {
    currency: "USD",
    rates: {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "1", denominator: "1" },
      cache_write_5m_tokens: { numerator: "2", denominator: "1" },
      cache_write_1h_tokens: { numerator: "3", denominator: "1" },
    },
  } as const;
  const compiled = compilePricing(pricing);

  for (const invalidCacheUsage of [
    { cache_write_tokens: "10", cache_write_5m_tokens: "11" },
    { cache_write_tokens: "10", cache_write_1h_tokens: "11" },
    { cache_write_tokens: "9007199254740992", cache_write_1h_tokens: "9007199254740993" },
  ]) {
    expect(() => calculateCost(invalidCacheUsage, pricing)).toThrow(
      "Provider cache write partitions disagree",
    );
    expect(() => calculateCompiledCostAtoms(invalidCacheUsage, compiled)).toThrow(
      "Provider cache write partitions disagree",
    );
  }

  expect(calculateCost({ cache_write_tokens: "10", cache_write_5m_tokens: "10" }, pricing)).toBe(
    "20",
  );
  expect(
    calculateCompiledCostAtoms({ cache_write_tokens: "10", cache_write_1h_tokens: "4" }, compiled),
  ).toBe(12n);

  const badReasoning = { output_tokens: "5", reasoning_tokens: "6" };
  expect(() => calculateCost(badReasoning, pricing)).toThrow(
    "Reasoning usage exceeds total output",
  );
  expect(() => calculateCompiledCostAtoms(badReasoning, compiled)).toThrow(
    "Reasoning usage exceeds total output",
  );
  expect(calculateCost({ output_tokens: "5", reasoning_tokens: "5" }, pricing)).toBe("5");
  expect(calculateCompiledCostAtoms({ reasoning_tokens: "5" }, compiled)).toBe(0n);
});

test("quoteReservation selects maximum across eligible routes, supports zero-rate models, filters output ceilings, and rejects invalid ceilings", () => {
  const cheapRoute = {
    provider_id: "cheap",
    id: "p-cheap",
    currency: "USD",
    basis: "PROVIDER_USAGE" as const,
    rates: {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "2", denominator: "1" },
    },
    ceilings: { input_tokens: "100", output_tokens: "50" },
  };
  const priceyRoute = {
    provider_id: "pricey",
    id: "p-pricey",
    currency: "USD",
    basis: "PROVIDER_USAGE" as const,
    rates: {
      input_tokens: { numerator: "3", denominator: "1" },
      output_tokens: { numerator: "5", denominator: "1" },
    },
    ceilings: { input_tokens: "100", output_tokens: "200" },
  };

  expect(
    quoteReservation([cheapRoute, priceyRoute], "USD", { estimatedInputTokens: 20 }, 40),
  ).toEqual({
    currency: "USD",
    amount_atoms: "260",
    input_characters: "0",
    prices: { cheap: "p-cheap", pricey: "p-pricey" },
  });

  expect(
    quoteReservation([cheapRoute, priceyRoute], "USD", { estimatedInputTokens: 20 }, 100),
  ).toEqual({
    currency: "USD",
    amount_atoms: "560",
    input_characters: "0",
    prices: { pricey: "p-pricey" },
  });

  expect(() =>
    quoteReservation([cheapRoute, priceyRoute], "USD", { estimatedInputTokens: 20 }, 201),
  ).toThrow(expect.objectContaining({ code: "billing_ceiling" }));

  const freeRoute = {
    provider_id: "free",
    id: "p-free",
    currency: "USD",
    basis: "PROVIDER_USAGE" as const,
    rates: {
      input_tokens: { numerator: "0", denominator: "1" },
      output_tokens: { numerator: "0", denominator: "1" },
    },
    ceilings: { input_tokens: "100", output_tokens: "100" },
  };
  expect(quoteReservation([freeRoute], "USD", { estimatedInputTokens: 50 }, 50)).toEqual({
    currency: "USD",
    amount_atoms: "0",
    input_characters: "0",
    prices: { free: "p-free" },
  });

  const brokenRequestRoute = {
    provider_id: "bad-req",
    id: "p-bad",
    currency: "USD",
    basis: "REQUEST_COMPLETION" as const,
    rates: { requests: { numerator: "10", denominator: "1" } },
    ceilings: { requests: "0" },
  };
  expect(() => quoteReservation([brokenRequestRoute], "USD", {}, 0)).toThrow(DomainError);

  const imageRoute = {
    provider_id: "img",
    id: "p-img",
    currency: "USD",
    basis: "PROVIDER_USAGE" as const,
    rates: { images: { numerator: "25", denominator: "1" } },
    ceilings: { images: "4" },
  };
  expect(quoteReservation([imageRoute], "USD", { images: 3 }, 0).amount_atoms).toBe("75");
  for (const invalidImages of [0, -1, 1.5, 5]) {
    expect(() => quoteReservation([imageRoute], "USD", { images: invalidImages }, 0)).toThrow(
      expect.objectContaining({ code: "billing_ceiling" }),
    );
  }
});

test("symptom: calculateCompiledCostAtoms fast-path loses float64 precision across 21 unit-safe dimensions", () => {
  const unitDims = [
    "input_tokens",
    "output_tokens",
    "cached_input_tokens",
    "cache_write_5m_tokens",
    "cache_write_1h_tokens",
    "image_input_tokens",
    "image_output_tokens",
    "audio_input_tokens",
    "audio_output_tokens",
    "video_input_tokens",
    "requests",
    "images",
    "audio_ms",
    "music_ms",
    "video_ms",
    "characters",
    "web_search_calls",
    "code_interpreter_sessions",
    "file_search_calls",
    "search_units",
    "pages",
  ] as const;
  const rates: Record<string, { numerator: string; denominator: string }> = {};
  const usage: Record<string, string> = {};
  for (let i = 0; i < unitDims.length; i++) {
    const dim = unitDims[i]!;
    if (i < 19) {
      rates[dim] = { numerator: "500000000", denominator: "1" };
      usage[dim] = "999999";
    } else {
      rates[dim] = { numerator: "1", denominator: "1" };
      usage[dim] = "1";
    }
  }
  const pricing = { currency: "USD", rates };
  const exact = calculateCost(usage, pricing);
  const compiled = compilePricing(pricing);
  expect(compiled.allUnitSafe).toBe(false);
  expect(calculateCompiledCostAtoms(usage, compiled).toString()).toBe(exact);
  expect(exact).toBe("9499990500000002");
});
