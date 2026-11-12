import { expect, test } from "bun:test";
import { calculateUpstreamCost } from "../index.ts";
import type { Dimension, Pricing, UpstreamCostInput, Usage } from "../index.ts";

const unitPricing: Pricing = {
  currency: "USD",
  rates: {
    input_tokens: { numerator: "1", denominator: "1" },
    output_tokens: { numerator: "1", denominator: "1" },
  },
};

test("A nonfinal token observations never become expense before the final quantities arrive", () => {
  const pricing: Pricing = {
    currency: "USD",
    rates: {
      requests: { numerator: "2", denominator: "1" },
      input_tokens: { numerator: "1", denominator: "3" },
      output_tokens: { numerator: "1", denominator: "3" },
    },
  };
  const estimated_quantities = { requests: "1", input_tokens: "60", output_tokens: "30" };
  const partial = calculateUpstreamCost({
    pricing,
    estimated_quantities,
    head: { usage: { requests: "1", input_tokens: "30" }, final_dimensions: ["requests"] },
  });
  expect(partial.estimated_atoms).toBe("32");
  expect(partial.cost_atoms).toBe("2");
  expect(partial.remaining_reserved_atoms).toBe("30");
  expect(partial.complete).toBe(false);
  const final = calculateUpstreamCost({
    pricing,
    estimated_quantities,
    head: {
      usage: { requests: "1", input_tokens: "31", output_tokens: "2" },
      final_dimensions: ["requests", "input_tokens", "output_tokens"],
    },
  });
  expect(final.cost_atoms).toBe("13");
  expect(final.remaining_reserved_atoms).toBe("0");
  expect(final.complete).toBe(true);
});

test("B an over-ceiling final dimension cannot consume another unknown dimension's reservation", () => {
  const estimated_quantities = { input_tokens: "20", output_tokens: "20" };
  const partial = calculateUpstreamCost({
    pricing: unitPricing,
    estimated_quantities,
    head: { usage: { input_tokens: "30" }, final_dimensions: ["input_tokens"] },
  });
  expect(partial.estimated_atoms).toBe("40");
  expect(partial.cost_atoms).toBe("30");
  expect(partial.remaining_reserved_atoms).toBe("20");
  expect(partial.exposure_atoms).toBe("50");
  expect(partial.overrun).toBe(true);
  expect(partial.overrun_dimensions).toEqual(["input_tokens"]);
  const final = calculateUpstreamCost({
    pricing: unitPricing,
    estimated_quantities,
    head: {
      usage: { input_tokens: "30", output_tokens: "25" },
      final_dimensions: ["input_tokens", "output_tokens"],
    },
  });
  expect(final.cost_atoms).toBe("55");
  expect(final.remaining_reserved_atoms).toBe("0");
  expect(final.overrun).toBe(true);
  expect(final.overrun_dimensions).toEqual(["input_tokens", "output_tokens"]);
});

test("C an attempt without a head retains its original one hundred atom exposure", () => {
  const unknown = calculateUpstreamCost({
    pricing: { currency: "USD", rates: { requests: { numerator: "100", denominator: "1" } } },
    estimated_quantities: { requests: "1" },
  });
  expect(unknown.cost_atoms).toBe("0");
  expect(unknown.remaining_reserved_atoms).toBe("100");
  expect(unknown.complete).toBe(false);
});

test("C the other attempt posts cumulative twenty then fifty and releases only its own reserve", () => {
  const pricing: Pricing = {
    currency: "USD",
    rates: {
      requests: { numerator: "20", denominator: "1" },
      output_tokens: { numerator: "1", denominator: "1" },
    },
  };
  const estimated_quantities = { requests: "1", output_tokens: "60" };
  const partial = calculateUpstreamCost({
    pricing,
    estimated_quantities,
    head: { usage: { requests: "1" }, final_dimensions: ["requests"] },
  });
  expect(partial.estimated_atoms).toBe("80");
  expect(partial.cost_atoms).toBe("20");
  expect(partial.remaining_reserved_atoms).toBe("60");
  const final = calculateUpstreamCost({
    pricing,
    estimated_quantities,
    head: {
      usage: { requests: "1", output_tokens: "30" },
      final_dimensions: ["requests", "output_tokens"],
    },
  });
  expect(final.cost_atoms).toBe("50");
  expect(final.remaining_reserved_atoms).toBe("0");
  expect(final.complete).toBe(true);
});

test("D trusted nonfinal overrun reserves forty five without posting twenty five as expense", () => {
  const input = {
    pricing: unitPricing,
    estimated_quantities: { input_tokens: "20", output_tokens: "20" },
    trusted_partial_max: { input_tokens: "25" },
  };
  const partial = calculateUpstreamCost({
    ...input,
    head: { usage: { input_tokens: "25" }, final_dimensions: [] },
  });
  expect(partial.cost_atoms).toBe("0");
  expect(partial.remaining_reserved_atoms).toBe("45");
  expect(partial.partial_risk_dimensions).toEqual(["input_tokens"]);
  const final = calculateUpstreamCost({
    ...input,
    head: {
      usage: { input_tokens: "10", output_tokens: "15" },
      final_dimensions: ["input_tokens", "output_tokens"],
    },
  });
  expect(final.cost_atoms).toBe("25");
  expect(final.remaining_reserved_atoms).toBe("0");
  expect(final.partial_risk_dimensions).toEqual([]);
  expect(final.complete).toBe(true);
});

test("untrusted or estimated head observations cannot enlarge reserve or turn into expense", () => {
  const result = calculateUpstreamCost({
    pricing: unitPricing,
    estimated_quantities: { input_tokens: "20", output_tokens: "20" },
    head: { usage: { input_tokens: "25" }, final_dimensions: [] },
  });
  expect(result.cost_atoms).toBe("0");
  expect(result.remaining_reserved_atoms).toBe("40");
  expect(result.partial_risk_dimensions).toEqual([]);
});

test("fractional dimensions are summed before ceil and the known ceil is subtracted from joint exposure", () => {
  const pricing: Pricing = {
    currency: "USD",
    rates: {
      input_tokens: { numerator: "1", denominator: "3" },
      output_tokens: { numerator: "1", denominator: "3" },
    },
  };
  const unknown = calculateUpstreamCost({
    pricing,
    estimated_quantities: { input_tokens: "1", output_tokens: "1" },
  });
  expect(unknown.remaining_reserved_atoms).toBe("1");
  const partial = calculateUpstreamCost({
    pricing,
    estimated_quantities: { input_tokens: "1", output_tokens: "4" },
    head: { usage: { input_tokens: "1" }, final_dimensions: ["input_tokens"] },
  });
  expect(partial.cost_atoms).toBe("1");
  expect(partial.remaining_reserved_atoms).toBe("1");
  expect(partial.exposure_atoms).toBe("2");
});

test("rounding can leave zero remaining atoms while a priced dimension is still unknown", () => {
  const result = calculateUpstreamCost({
    pricing: {
      currency: "USD",
      rates: {
        input_tokens: { numerator: "1", denominator: "3" },
        output_tokens: { numerator: "1", denominator: "3" },
      },
    },
    estimated_quantities: { input_tokens: "1", output_tokens: "1" },
    head: { usage: { input_tokens: "1" }, final_dimensions: ["input_tokens"] },
  });
  expect(result.cost_atoms).toBe("1");
  expect(result.remaining_reserved_atoms).toBe("0");
  expect(result.complete).toBe(false);
});

test("125 milliseconds at eight atoms per second reserve and finalize exactly one atom", () => {
  const input = {
    pricing: { currency: "USD", rates: { music_ms: { numerator: "8", denominator: "1000" } } },
    estimated_quantities: { music_ms: "125" },
  };
  expect(calculateUpstreamCost(input).remaining_reserved_atoms).toBe("1");
  const final = calculateUpstreamCost({
    ...input,
    head: { usage: { music_ms: "125" }, final_dimensions: ["music_ms"] },
  });
  expect(final.cost_atoms).toBe("1");
  expect(final.remaining_reserved_atoms).toBe("0");
});

test("quantities above the safe integer limit retain exact cost and risk comparison", () => {
  const result = calculateUpstreamCost({
    pricing: { currency: "USD", rates: { input_tokens: { numerator: "1", denominator: "1" } } },
    estimated_quantities: { input_tokens: "9007199254740992" },
    head: { usage: { input_tokens: "9007199254740993" }, final_dimensions: ["input_tokens"] },
  });
  expect(result.cost_atoms).toBe("9007199254740993");
  expect(result.overrun_dimensions).toEqual(["input_tokens"]);
});

test("eighteen decimal place durations compare exactly and nonfinal risk remains reserve", () => {
  const input = {
    pricing: {
      currency: "USD",
      rates: { audio_seconds: { numerator: "1000000000000000000", denominator: "1" } },
    },
    estimated_quantities: { audio_seconds: "0.000000000000000001" },
    trusted_partial_max: { audio_seconds: "0.000000000000000002" },
  };
  const result = calculateUpstreamCost(input);
  expect(result.estimated_atoms).toBe("1");
  expect(result.cost_atoms).toBe("0");
  expect(result.remaining_reserved_atoms).toBe("2");
  expect(result.partial_risk_dimensions).toEqual(["audio_seconds"]);
  const final = calculateUpstreamCost({
    ...input,
    head: {
      usage: { audio_seconds: "0.000000000000000002" },
      final_dimensions: ["audio_seconds"],
    },
  });
  expect(final.cost_atoms).toBe("2");
  expect(final.remaining_reserved_atoms).toBe("0");
});

test("explicit zero rates require an estimate and final facts even when the charge is zero", () => {
  const pricing: Pricing = {
    currency: "USD",
    rates: { requests: { numerator: "0", denominator: "1" } },
  };
  expect(() => calculateUpstreamCost({ pricing, estimated_quantities: {} })).toThrow(TypeError);
  const unknown = calculateUpstreamCost({ pricing, estimated_quantities: { requests: "0" } });
  expect(unknown.remaining_reserved_atoms).toBe("0");
  expect(unknown.complete).toBe(false);
  const final = calculateUpstreamCost({
    pricing,
    estimated_quantities: { requests: "0" },
    head: { usage: { requests: "0" }, final_dimensions: ["requests"] },
  });
  expect(final.cost_atoms).toBe("0");
  expect(final.complete).toBe(true);
});

test("zero priced dimensions still report actual or trusted partial quantities above the captured bound", () => {
  const input = {
    pricing: { currency: "USD", rates: { requests: { numerator: "0", denominator: "1" } } },
    estimated_quantities: { requests: "0" },
  };
  expect(
    calculateUpstreamCost({ ...input, trusted_partial_max: { requests: "1" } })
      .partial_risk_dimensions,
  ).toEqual(["requests"]);
  const final = calculateUpstreamCost({
    ...input,
    head: { usage: { requests: "1" }, final_dimensions: ["requests"] },
  });
  expect(final.overrun).toBe(true);
  expect(final.cost_atoms).toBe("0");
});

test("a missing priced estimate or empty pricing cannot silently turn unknown liability into free service", () => {
  expect(() =>
    calculateUpstreamCost({ pricing: unitPricing, estimated_quantities: { input_tokens: "20" } }),
  ).toThrow(TypeError);
  expect(() =>
    calculateUpstreamCost({ pricing: { currency: "USD", rates: {} }, estimated_quantities: {} }),
  ).toThrow(TypeError);
});

test("a final dimension without an explicit value cannot become a zero-cost final fact", () => {
  expect(() =>
    calculateUpstreamCost({
      pricing: unitPricing,
      estimated_quantities: { input_tokens: "20", output_tokens: "20" },
      head: { usage: {}, final_dimensions: ["input_tokens"] },
    }),
  ).toThrow(TypeError);
});

test("unknown usage and invalid final dimensions are rejected instead of being ignored", () => {
  const input = {
    pricing: unitPricing,
    estimated_quantities: { input_tokens: "20", output_tokens: "20" },
  };
  expect(() =>
    calculateUpstreamCost({ ...input, trusted_partial_max: { tokens: "1" } as Usage }),
  ).toThrow(TypeError);
  expect(() =>
    calculateUpstreamCost({
      ...input,
      head: { usage: {}, final_dimensions: ["tokens" as Dimension] },
    }),
  ).toThrow(TypeError);
  expect(() =>
    calculateUpstreamCost({
      ...input,
      head: {
        usage: { reasoning_tokens: "1" },
        final_dimensions: ["reasoning_tokens" as Dimension],
      },
    }),
  ).toThrow(TypeError);
});

test("invalid exact quantities and rates retain kernel validation at the new public boundary", () => {
  for (const quantity of ["-1", "1.5", "01"]) {
    expect(() =>
      calculateUpstreamCost({
        pricing: unitPricing,
        estimated_quantities: { input_tokens: quantity, output_tokens: "0" },
      }),
    ).toThrow(TypeError);
  }
  expect(() =>
    calculateUpstreamCost({
      pricing: { currency: "USD", rates: { input_tokens: { numerator: "1", denominator: "0" } } },
      estimated_quantities: { input_tokens: "1" },
    }),
  ).toThrow(TypeError);
  expect(() =>
    calculateUpstreamCost({
      pricing: { currency: "USD", rates: { audio_seconds: { numerator: "1", denominator: "1" } } },
      estimated_quantities: { audio_seconds: "0.0000000000000000001" },
    }),
  ).toThrow(TypeError);
});

test("numeric rate values cannot silently pass through JavaScript integer rounding", () => {
  for (const rate of [
    { numerator: Number("9007199254740993"), denominator: "1" },
    { numerator: "1", denominator: 1 },
  ]) {
    expect(() =>
      calculateUpstreamCost({
        pricing: { currency: "USD", rates: { requests: rate } } as unknown as Pricing,
        estimated_quantities: { requests: "1" },
      }),
    ).toThrow(TypeError);
  }
});

test("larger unresolved disputes enlarge reserve without changing a complete head's posted cost", () => {
  const result = calculateUpstreamCost({
    pricing: { currency: "USD", rates: { input_tokens: { numerator: "1", denominator: "1" } } },
    estimated_quantities: { input_tokens: "50" },
    head: { usage: { input_tokens: "30" }, final_dimensions: ["input_tokens"] },
    unresolved_disputes: [
      { usage: { input_tokens: "40" }, final_dimensions: ["input_tokens"] },
      { usage: { input_tokens: "25" }, final_dimensions: ["input_tokens"] },
    ],
  });
  expect(result.cost_atoms).toBe("30");
  expect(result.remaining_reserved_atoms).toBe("10");
  expect(result.complete).toBe(true);
  expect(result.has_unresolved_disputes).toBe(true);
});

test("a smaller unresolved dispute cannot refund the selected head or release unknown exposure", () => {
  const input = {
    pricing: { currency: "USD", rates: { input_tokens: { numerator: "1", denominator: "1" } } },
    estimated_quantities: { input_tokens: "50" },
    unresolved_disputes: [
      { usage: { input_tokens: "25" }, final_dimensions: ["input_tokens" as const] },
    ],
  };
  const complete = calculateUpstreamCost({
    ...input,
    head: { usage: { input_tokens: "30" }, final_dimensions: ["input_tokens"] },
  });
  expect(complete.cost_atoms).toBe("30");
  expect(complete.remaining_reserved_atoms).toBe("0");
  expect(complete.has_unresolved_disputes).toBe(true);
  const unknown = calculateUpstreamCost(input);
  expect(unknown.cost_atoms).toBe("0");
  expect(unknown.remaining_reserved_atoms).toBe("50");
  expect(unknown.complete).toBe(false);
});

test("a disputed partial snapshot keeps its other unknown dimensions at the original captured bound", () => {
  const result = calculateUpstreamCost({
    pricing: unitPricing,
    estimated_quantities: { input_tokens: "20", output_tokens: "20" },
    head: {
      usage: { input_tokens: "10", output_tokens: "10" },
      final_dimensions: ["input_tokens", "output_tokens"],
    },
    unresolved_disputes: [
      { usage: { input_tokens: "30", output_tokens: "1" }, final_dimensions: ["input_tokens"] },
    ],
  });
  expect(result.cost_atoms).toBe("20");
  expect(result.exposure_atoms).toBe("50");
  expect(result.remaining_reserved_atoms).toBe("30");
});

test("risk and disputed exposures use their maximum rather than adding duplicate liabilities", () => {
  const result = calculateUpstreamCost({
    pricing: unitPricing,
    estimated_quantities: { input_tokens: "20", output_tokens: "20" },
    trusted_partial_max: { input_tokens: "25" },
    unresolved_disputes: [{ usage: { input_tokens: "30" }, final_dimensions: ["input_tokens"] }],
  });
  expect(result.cost_atoms).toBe("0");
  expect(result.remaining_reserved_atoms).toBe("50");
  expect(result.partial_risk_dimensions).toEqual(["input_tokens"]);
});

test("all thirty eight digit output atoms remain representable at the exact boundary", () => {
  const max = "99999999999999999999999999999999999999";
  const result = calculateUpstreamCost({
    pricing: { currency: "USD", rates: { input_tokens: { numerator: "1", denominator: "1" } } },
    estimated_quantities: { input_tokens: max },
    head: { usage: { input_tokens: max }, final_dimensions: ["input_tokens"] },
  });
  expect(result.cost_atoms).toBe("99999999999999999999999999999999999999");
  expect(result.remaining_reserved_atoms).toBe("0");
});

test("estimate final partial and disputed overflow fail explicitly instead of capping or returning zero", () => {
  const max = "99999999999999999999999999999999999999";
  const pricing: Pricing = {
    currency: "USD",
    rates: { input_tokens: { numerator: "2", denominator: "1" } },
  };
  const cases: UpstreamCostInput[] = [
    { pricing, estimated_quantities: { input_tokens: max } },
    {
      pricing,
      estimated_quantities: { input_tokens: "0" },
      head: { usage: { input_tokens: max }, final_dimensions: ["input_tokens"] },
    },
    {
      pricing,
      estimated_quantities: { input_tokens: "0" },
      trusted_partial_max: { input_tokens: max },
    },
    {
      pricing,
      estimated_quantities: { input_tokens: "0" },
      unresolved_disputes: [{ usage: { input_tokens: max }, final_dimensions: ["input_tokens"] }],
    },
  ];
  for (const input of cases) expect(() => calculateUpstreamCost(input)).toThrow(RangeError);
});

test("combined known cost and unknown exposure overflow even when each component fits separately", () => {
  expect(() =>
    calculateUpstreamCost({
      pricing: unitPricing,
      estimated_quantities: { input_tokens: "0", output_tokens: "1" },
      head: {
        usage: { input_tokens: "99999999999999999999999999999999999999" },
        final_dimensions: ["input_tokens"],
      },
    }),
  ).toThrow(RangeError);
});

test("cost and risk calculations leave captured pricing quantities and evidence inputs unchanged", () => {
  const input: UpstreamCostInput = {
    pricing: unitPricing,
    estimated_quantities: { input_tokens: "20", output_tokens: "20" },
    head: { usage: { input_tokens: "10" }, final_dimensions: ["input_tokens"] },
    trusted_partial_max: { output_tokens: "25" },
    unresolved_disputes: [{ usage: { input_tokens: "30" }, final_dimensions: ["input_tokens"] }],
  };
  const before = JSON.stringify(input);
  const result = calculateUpstreamCost(input);
  expect(result.cost_atoms).toBe("10");
  expect(result.remaining_reserved_atoms).toBe("40");
  expect(JSON.stringify(input)).toBe(before);
});
