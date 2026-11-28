import { expect, test } from "bun:test";
import {
  calculateCompiledCostAtoms,
  calculateCost,
  compilePricing,
  compileRatingPlan,
  evaluateSnapshotAtoms,
  quoteSnapshotReservation,
  resolvePricingSnapshot,
  type FxRateVersion,
  type PriceModifier,
  type RateCard,
  type Usage,
} from "../index.ts";

function makeRng(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function randInt(rng: () => number, min: number, max: number): number {
  return min + Math.floor(rng() * (max - min + 1));
}

function randRate(rng: () => number) {
  const denChoices = ["1", "2", "3", "4", "6", "10", "100", "1000", "1000000"];
  const den = denChoices[randInt(rng, 0, denChoices.length - 1)]!;
  const num = String(randInt(rng, 1, 5000));
  return { numerator: num, denominator: den };
}

test("property: across 5,000 randomized multi-curve/matrix/quantizer/TTL/modifier/FX scenarios, reservation >= actual, monotonic, and JSON snapshot round-trip is bit-exact", () => {
  const rng = makeRng(20261128);
  const scenarios = 5000;

  for (let i = 0; i < scenarios; i++) {
    const mode = i % 7;
    const useFx = i % 3 === 0;
    const useBounds = i % 4 === 0;
    const useReasoningRate = mode === 2 || mode === 5 || i % 7 === 0;
    const useTools = mode === 3 || i % 6 === 0;
    const useTtlCache = mode === 5 || i % 5 === 0;
    const useQuantizer = mode === 6;
    const useMatrix = i % 9 === 0;
    const useExactCardMod = i % 11 === 0;

    const inputCeil = randInt(rng, 500, 300_000);
    const outputCeil = randInt(rng, 100, 16_000);
    const outputBound = randInt(rng, 1, outputCeil);
    const estInput = randInt(rng, 1, inputCeil);
    const useTightEst = i % 2 === 0;

    const cacheRates = useTtlCache
      ? {
          cache_write_5m_tokens: randRate(rng),
          cache_write_1h_tokens: randRate(rng),
        }
      : {
          cache_write_tokens: randRate(rng),
        };
    const cacheCeilings = useTtlCache
      ? {
          cache_write_5m_tokens: String(inputCeil),
          cache_write_1h_tokens: String(inputCeil),
        }
      : {
          cache_write_tokens: String(inputCeil),
        };

    const card: RateCard = {
      id: `card-${i}`,
      side: "CUSTOMER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      model: `m-${i}`,
      operation: "chat",
      rates: {
        input_tokens: randRate(rng),
        cached_input_tokens: randRate(rng),
        ...cacheRates,
        output_tokens: randRate(rng),
        ...(useTools ? { web_search_calls: randRate(rng) } : {}),
      },
      ceilings: {
        input_tokens: String(inputCeil),
        cached_input_tokens: String(inputCeil),
        ...cacheCeilings,
        output_tokens: String(outputCeil),
        ...(useTools ? { web_search_calls: "5" } : {}),
      },
      ...(useReasoningRate ? { reasoning_rate: randRate(rng) } : {}),
      ...(useQuantizer
        ? {
            quantizers: {
              output_tokens: { min_units: "16", step: "32" },
            },
          }
        : {}),
      ...(mode === 1 || mode === 5
        ? {
            cliff: {
              driver:
                mode === 5
                  ? ["input_tokens"]
                  : useTtlCache
                    ? [
                        "input_tokens",
                        "cached_input_tokens",
                        "cache_write_5m_tokens",
                        "cache_write_1h_tokens",
                      ]
                    : ["input_tokens", "cached_input_tokens", "cache_write_tokens"],
              tiers: [
                { min_driver: "0", rates: {} },
                {
                  min_driver: String(Math.max(1, Math.floor(inputCeil / 2))),
                  rates: {
                    input_tokens: randRate(rng),
                    cached_input_tokens: randRate(rng),
                    ...(useTtlCache
                      ? {
                          cache_write_5m_tokens: randRate(rng),
                          cache_write_1h_tokens: randRate(rng),
                        }
                      : { cache_write_tokens: randRate(rng) }),
                    output_tokens: randRate(rng),
                  },
                  ...(useReasoningRate ? { reasoning_rate: randRate(rng) } : {}),
                },
              ],
            },
          }
        : {}),
      ...(mode === 4 || mode === 5
        ? {
            progressive: {
              input_tokens: [
                { up_to: String(Math.max(1, Math.floor(inputCeil / 3))), rate: randRate(rng) },
                {
                  up_to: String(Math.max(2, Math.floor((inputCeil * 2) / 3))),
                  rate: randRate(rng),
                },
                { up_to: null, rate: randRate(rng) },
              ],
              output_tokens: [
                { up_to: String(Math.max(1, Math.floor(outputCeil / 3))), rate: randRate(rng) },
                { up_to: null, rate: randRate(rng) },
              ],
            },
          }
        : {}),
      ...(useMatrix
        ? {
            matrix: [
              {
                match: { tier: "turbo" },
                rates: { output_tokens: randRate(rng) },
              },
            ],
          }
        : {}),
      ...(useBounds
        ? {
            min_charge_atoms: String(randInt(rng, 0, 500)),
            max_charge_atoms: String(randInt(rng, 500, 100_000_000)),
          }
        : {}),
    };

    const modifiers: PriceModifier[] = [
      {
        id: `prod-${i}`,
        layer: "PRODUCT",
        kind: "MULTIPLIER",
        stacking: "MULTIPLY",
        priority: 10,
        enabled: true,
        match: { model: `m-${i}` },
        multiplier: { numerator: String(randInt(rng, 5, 20)), denominator: "10" },
      },
      {
        id: `comm-a-${i}`,
        layer: "COMMERCIAL",
        kind: "MULTIPLIER",
        stacking: "BEST_FOR_CUSTOMER",
        priority: 20,
        enabled: true,
        match: { customer_group_id: "g1" },
        multiplier: { numerator: String(randInt(rng, 5, 10)), denominator: "10" },
      },
      {
        id: `comm-b-${i}`,
        layer: "COMMERCIAL",
        kind: "MULTIPLIER",
        stacking: "BEST_FOR_CUSTOMER",
        priority: 30,
        enabled: true,
        match: { model: `m-${i}` },
        multiplier: { numerator: String(randInt(rng, 5, 10)), denominator: "10" },
      },
      ...(useExactCardMod
        ? [
            {
              id: `exact-${i}`,
              layer: "COMMERCIAL" as const,
              kind: "EXACT_RATE_CARD" as const,
              stacking: "EXCLUSIVE" as const,
              priority: 1,
              enabled: true,
              match: { side: "CUSTOMER" as const, model: `m-${i}` },
              rate_card: {
                ...card,
                id: `exact-card-${i}`,
                rates: { ...card.rates, input_tokens: randRate(rng) },
              },
            },
          ]
        : []),
    ];
    const fxRates: FxRateVersion[] = useFx
      ? [
          {
            id: `fx-${i}`,
            base_currency: "USD",
            quote_currency: "CNY",
            rate: { numerator: "725", denominator: "100" },
          },
        ]
      : [];

    const plan = compileRatingPlan({
      rateCards: [card],
      modifiers,
      fxRates,
      requireCeilings: true,
    });
    const snap = resolvePricingSnapshot(plan, {
      side: "CUSTOMER",
      model: `m-${i}`,
      operation: "chat",
      currency: useFx ? "CNY" : "USD",
      rated_at: "2026-06-15T12:00:00Z",
      customer_group_id: "g1",
      ...(useMatrix ? { attrs: { tier: "turbo" } } : {}),
    });

    const maxToolCalls = useTools ? { web_search_calls: randInt(rng, 0, 5) } : undefined;
    const reserve = quoteSnapshotReservation(
      snap,
      {
        ...(useTightEst ? { estimatedInputTokens: estInput } : {}),
        ...(maxToolCalls ? { maxToolCalls } : {}),
      },
      outputBound,
    );

    const totalInputBudget = useTightEst ? estInput : inputCeil;
    const actualIn = randInt(rng, 0, totalInputBudget);
    const rem1 = totalInputBudget - actualIn;
    const actualCached = randInt(rng, 0, rem1);
    const rem2 = rem1 - actualCached;
    const actualWriteA = randInt(rng, 0, rem2);
    const actualWriteB = useTtlCache ? randInt(rng, 0, rem2 - actualWriteA) : 0;
    const actualOut = randInt(rng, 0, outputBound);
    const actualReason = useReasoningRate ? randInt(rng, 0, actualOut) : 0;
    const actualTools = maxToolCalls ? randInt(rng, 0, maxToolCalls.web_search_calls) : 0;

    const usage: Usage = {
      input_tokens: String(actualIn),
      cached_input_tokens: String(actualCached),
      ...(useTtlCache
        ? {
            cache_write_5m_tokens: String(actualWriteA),
            cache_write_1h_tokens: String(actualWriteB),
          }
        : { cache_write_tokens: String(actualWriteA) }),
      output_tokens: String(actualOut),
      ...(useReasoningRate ? { reasoning_tokens: String(actualReason) } : {}),
      ...(useTools ? { web_search_calls: String(actualTools) } : {}),
    };

    const actualAtoms = evaluateSnapshotAtoms(snap, usage);
    expect(BigInt(reserve.amount_atoms) >= actualAtoms, `scenario ${i} under-reserved`).toBe(true);

    const roundTripped = JSON.parse(JSON.stringify(snap));
    expect(evaluateSnapshotAtoms(roundTripped, usage)).toBe(actualAtoms);

    if (mode === 0 && !useFx && !useBounds && !useReasoningRate) {
      const bumpedUsage: Usage = {
        ...usage,
        input_tokens: String(actualIn + randInt(rng, 1, 50)),
      };
      expect(evaluateSnapshotAtoms(snap, bumpedUsage) >= actualAtoms).toBe(true);
    }
  }
});

test("property: flat snapshot evaluation matches legacy calculateCost and calculateCompiledCostAtoms across 1,000 random tariffs", () => {
  const rng = makeRng(99887766);
  for (let i = 0; i < 1000; i++) {
    const rates = {
      input_tokens: randRate(rng),
      output_tokens: randRate(rng),
      cached_input_tokens: randRate(rng),
      audio_seconds: randRate(rng),
      web_search_calls: randRate(rng),
    };
    const card: RateCard = {
      id: `flat-${i}`,
      side: "CUSTOMER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      model: "m",
      operation: "chat",
      rates,
      ceilings: {
        input_tokens: "100000",
        output_tokens: "100000",
        cached_input_tokens: "100000",
        audio_seconds: "1000",
        web_search_calls: "100",
      },
    };
    const plan = compileRatingPlan({ rateCards: [card] });
    const snap = resolvePricingSnapshot(plan, {
      side: "CUSTOMER",
      model: "m",
      operation: "chat",
      currency: "USD",
      rated_at: "2026-06-15T12:00:00Z",
    });
    const frac = String(randInt(rng, 0, 9999)).padStart(4, "0");
    const usage: Usage = {
      input_tokens: String(randInt(rng, 0, 100_000)),
      output_tokens: String(randInt(rng, 0, 100_000)),
      cached_input_tokens: String(randInt(rng, 0, 100_000)),
      audio_seconds: `${randInt(rng, 0, 500)}.${frac}`,
      web_search_calls: String(randInt(rng, 0, 20)),
    };
    const snapAtoms = evaluateSnapshotAtoms(snap, usage);
    const legacyStr = calculateCost(usage, { currency: "USD", rates });
    const compiledAtoms = calculateCompiledCostAtoms(
      usage,
      compilePricing({ currency: "USD", rates }),
    );
    expect(snapAtoms.toString()).toBe(legacyStr);
    expect(snapAtoms).toBe(compiledAtoms);

    const perDimRounded =
      BigInt(calculateCost({ input_tokens: usage.input_tokens! }, { currency: "USD", rates })) +
      BigInt(calculateCost({ output_tokens: usage.output_tokens! }, { currency: "USD", rates })) +
      BigInt(
        calculateCost(
          { cached_input_tokens: usage.cached_input_tokens! },
          { currency: "USD", rates },
        ),
      ) +
      BigInt(calculateCost({ audio_seconds: usage.audio_seconds! }, { currency: "USD", rates })) +
      BigInt(
        calculateCost({ web_search_calls: usage.web_search_calls! }, { currency: "USD", rates }),
      );
    expect(snapAtoms <= perDimRounded).toBe(true);
    expect(perDimRounded - snapAtoms < 5n).toBe(true);
  }
});
