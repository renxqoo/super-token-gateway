import { expect, test } from "bun:test";
import {
  AMOUNT_DECIMALS,
  ATOMS_PER_UNIT,
  calculateCost,
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
