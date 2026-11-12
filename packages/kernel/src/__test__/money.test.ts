import { expect, test } from "bun:test";
import { calculateCost, quoteReservation } from "../index.ts";

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
