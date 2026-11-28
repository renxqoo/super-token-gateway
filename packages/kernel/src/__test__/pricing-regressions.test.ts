import { expect, test } from "bun:test";
import {
  DomainError,
  compileRatingPlan,
  evaluateMargin,
  evaluateSnapshotAtoms,
  hasCompleteSnapshotUsage,
  quoteSnapshotReservation,
  resolvePricingSnapshot,
  validatePriceModifier,
  validateRateCard,
  type FxRateVersion,
  type PriceModifier,
  type RateCard,
} from "../index.ts";

const R = (numerator: string, denominator = "1") => ({ numerator, denominator });

test("symptom: progressive.output_tokens with reasoning_rate under-reserves interior breakpoint split", () => {
  const card: RateCard = {
    id: "prog-out-reason",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "m1",
    operation: "chat",
    rates: { input_tokens: R("1") },
    progressive: {
      output_tokens: [
        { up_to: "50", rate: R("100") },
        { up_to: null, rate: R("1") },
      ],
    },
    reasoning_rate: R("50"),
    ceilings: { input_tokens: "1000", output_tokens: "100" },
  };
  const plan = compileRatingPlan({ rateCards: [card], requireCeilings: true });
  const snap = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "m1",
    operation: "chat",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });
  const quote = quoteSnapshotReservation(snap, { estimatedInputTokens: 10 }, 100);
  const actual = evaluateSnapshotAtoms(snap, {
    input_tokens: "10",
    output_tokens: "100",
    reasoning_tokens: "50",
  });
  expect(actual).toBe(7510n);
  expect(BigInt(quote.amount_atoms)).toBe(7510n);
});

test("symptom: cliff + progressive.output_tokens + reasoning_rate flips curve precedence when reasoning_tokens > 0", () => {
  const card: RateCard = {
    id: "cliff-prog-reason",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "m2",
    operation: "chat",
    rates: { input_tokens: R("1"), output_tokens: R("10") },
    progressive: { output_tokens: [{ up_to: null, rate: R("100") }] },
    cliff: {
      driver: ["input_tokens"],
      tiers: [
        { min_driver: "0", rates: {} },
        {
          min_driver: "100",
          rates: { input_tokens: R("2"), output_tokens: R("10") },
          reasoning_rate: R("10"),
        },
      ],
    },
    reasoning_rate: R("10"),
    ceilings: { input_tokens: "1000", output_tokens: "100" },
  };
  const plan = compileRatingPlan({ rateCards: [card], requireCeilings: true });
  const snap = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "m2",
    operation: "chat",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });
  expect(
    evaluateSnapshotAtoms(snap, {
      input_tokens: "200",
      output_tokens: "100",
      reasoning_tokens: "0",
    }),
  ).toBe(1400n);
  expect(
    evaluateSnapshotAtoms(snap, {
      input_tokens: "200",
      output_tokens: "100",
      reasoning_tokens: "1",
    }),
  ).toBe(1400n);
});

test("symptom: tightened reservation under-reserves subset or unpriced cliff.driver dimensions", () => {
  const subsetDriverCard: RateCard = {
    id: "subset-driver",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "m3",
    operation: "messages",
    rates: { input_tokens: R("3"), cache_write_1h_tokens: R("6"), output_tokens: R("15") },
    cliff: {
      driver: ["input_tokens"],
      tiers: [
        { min_driver: "0", rates: {} },
        {
          min_driver: "200001",
          rates: {
            input_tokens: R("6"),
            cache_write_1h_tokens: R("12"),
            output_tokens: R("225", "10"),
          },
        },
      ],
    },
    ceilings: { input_tokens: "500000", cache_write_1h_tokens: "500000", output_tokens: "2000" },
  };
  const planA = compileRatingPlan({ rateCards: [subsetDriverCard], requireCeilings: true });
  const snapA = resolvePricingSnapshot(planA, {
    side: "CUSTOMER",
    model: "m3",
    operation: "messages",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });
  const reserveA = quoteSnapshotReservation(snapA, { estimatedInputTokens: 300_000 }, 1000);
  const actualA = evaluateSnapshotAtoms(snapA, {
    input_tokens: "200001",
    cache_write_1h_tokens: "99999",
    output_tokens: "1000",
  });
  expect(BigInt(reserveA.amount_atoms)).toBeGreaterThanOrEqual(actualA);

  const unpricedDriverCard: RateCard = {
    id: "unpriced-driver",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "m4",
    operation: "chat",
    rates: { input_tokens: R("1"), output_tokens: R("10") },
    cliff: {
      driver: ["cached_input_tokens"],
      tiers: [
        { min_driver: "0", rates: {} },
        { min_driver: "100", rates: { input_tokens: R("1"), output_tokens: R("1000") } },
      ],
    },
    ceilings: { input_tokens: "1000", output_tokens: "100" },
  };
  const planB = compileRatingPlan({ rateCards: [unpricedDriverCard], requireCeilings: true });
  const snapB = resolvePricingSnapshot(planB, {
    side: "CUSTOMER",
    model: "m4",
    operation: "chat",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });
  const reserveB = quoteSnapshotReservation(snapB, { estimatedInputTokens: 100 }, 100);
  const actualB = evaluateSnapshotAtoms(snapB, {
    input_tokens: "0",
    cached_input_tokens: "100",
    output_tokens: "100",
  });
  expect(BigInt(reserveB.amount_atoms)).toBeGreaterThanOrEqual(actualB);
});

test("symptom: cross-layer cache/bound conflicts, side leaks, and matrix-vs-progressive shadowing fail or misbill", () => {
  expect(() =>
    validateRateCard({
      id: "cross-cache-conflict",
      side: "CUSTOMER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: { cache_write_tokens: R("5") },
      cliff: {
        driver: ["input_tokens"],
        tiers: [
          { min_driver: "0", rates: {} },
          {
            min_driver: "100",
            rates: { cache_write_5m_tokens: R("6"), cache_write_1h_tokens: R("12") },
          },
        ],
      },
      ceilings: { cache_write_tokens: "1000" },
    }),
  ).toThrow("cache write");

  expect(() =>
    validateRateCard({
      id: "matrix-bound-conflict",
      side: "CUSTOMER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: { images: R("10") },
      min_charge_atoms: "500",
      matrix: [{ match: { size: "small" }, rates: { images: R("5") }, max_charge_atoms: "400" }],
      ceilings: { images: "4" },
    }),
  ).toThrow("min_charge_atoms");

  expect(() =>
    validatePriceModifier(
      {
        id: "side-mismatch",
        layer: "COMMERCIAL",
        kind: "EXACT_RATE_CARD",
        stacking: "EXCLUSIVE",
        priority: 1,
        enabled: true,
        match: { side: "SUPPLIER" },
        rate_card: {
          id: "cust-card",
          side: "CUSTOMER",
          currency: "USD",
          basis: "PROVIDER_USAGE",
          rates: { input_tokens: R("1") },
          ceilings: { input_tokens: "100" },
        },
      },
      { requireCeilings: true },
    ),
  ).toThrow("match.side");

  expect(() =>
    compileRatingPlan({
      rateCards: [
        {
          id: "base",
          side: "CUSTOMER",
          currency: "USD",
          basis: "PROVIDER_USAGE",
          model: "m",
          operation: "chat",
          rates: { input_tokens: R("1") },
          ceilings: { input_tokens: "100" },
        },
      ],
      modifiers: [
        {
          id: "missing-ceil-exact",
          layer: "COMMERCIAL",
          kind: "EXACT_RATE_CARD",
          stacking: "EXCLUSIVE",
          priority: 1,
          enabled: true,
          match: { model: "m" },
          rate_card: {
            id: "no-ceil",
            side: "CUSTOMER",
            currency: "USD",
            basis: "PROVIDER_USAGE",
            rates: { input_tokens: R("1") },
            ceilings: {},
          },
        },
      ],
      requireCeilings: true,
    }),
  ).toThrow("Missing reservation ceiling");

  const matrixOverProg: RateCard = {
    id: "matrix-over-prog",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "veo",
    operation: "videos",
    rates: {},
    progressive: {
      video_seconds: [
        { up_to: "5", rate: R("10") },
        { up_to: null, rate: R("20") },
      ],
    },
    matrix: [{ match: { res: "4k" }, rates: { video_seconds: R("100") } }],
    ceilings: { video_seconds: "10" },
  };
  const mPlan = compileRatingPlan({ rateCards: [matrixOverProg], requireCeilings: true });
  const mSnap = resolvePricingSnapshot(mPlan, {
    side: "CUSTOMER",
    model: "veo",
    operation: "videos",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
    attrs: { res: "4k" },
  });
  expect(evaluateSnapshotAtoms(mSnap, { video_seconds: "10" })).toBe(1000n);
});

test("symptom: progressive-only token/cache omissions, negative margin rounding, >38-digit modifiers, prototype attrs, and invalid calendar dates", () => {
  const progOnlyCard: RateCard = {
    id: "prog-only",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "mp",
    operation: "messages",
    rates: { output_tokens: R("10") },
    progressive: {
      input_tokens: [{ up_to: null, rate: R("5") }],
      cache_write_tokens: [{ up_to: null, rate: R("8") }],
    },
    ceilings: { input_tokens: "1000", output_tokens: "1000", cache_write_tokens: "1000" },
  };
  const pPlan = compileRatingPlan({ rateCards: [progOnlyCard], requireCeilings: true });
  const pSnap = resolvePricingSnapshot(pPlan, {
    side: "CUSTOMER",
    model: "mp",
    operation: "messages",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });
  expect(() => evaluateSnapshotAtoms(pSnap, { output_tokens: "10" })).toThrow(DomainError);
  expect(
    evaluateSnapshotAtoms(pSnap, {
      input_tokens: "0",
      output_tokens: "10",
      cache_write_5m_tokens: "100",
      cache_write_1h_tokens: "50",
    }),
  ).toBe(1300n);

  const tinyLoss = evaluateMargin({
    customerCurrency: "USD",
    customerAtoms: 100_000n,
    supplierCurrency: "USD",
    supplierAtoms: 100_001n,
    policy: { min_margin_bps: 0n },
  });
  expect(tinyLoss.margin_bps).toBe(-1n);
  expect(tinyLoss.allowed).toBe(false);

  const zeroRevLoss = evaluateMargin({
    customerCurrency: "USD",
    customerAtoms: 0n,
    supplierCurrency: "USD",
    supplierAtoms: 100n,
    policy: { min_margin_bps: -20_000n },
  });
  expect(zeroRevLoss.margin_bps).toBeNull();
  expect(zeroRevLoss.allowed).toBe(false);

  const deepMods: PriceModifier[] = [
    ["10000000007", "10000000019"],
    ["10000000033", "10000000061"],
    ["10000000069", "10000000097"],
    ["10000000103", "10000000121"],
  ].map(([n, d], idx) => ({
    id: `coprime-${idx}`,
    layer: "PRODUCT",
    kind: "MULTIPLIER",
    stacking: "MULTIPLY",
    priority: idx,
    enabled: true,
    match: { model: "deep" },
    multiplier: R(n!, d!),
  }));
  const deepCard: RateCard = {
    id: "deep-card",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "deep",
    operation: "chat",
    rates: { input_tokens: R("100"), output_tokens: R("100") },
    ceilings: { input_tokens: "1000", output_tokens: "1000" },
  };
  const deepPlan = compileRatingPlan({ rateCards: [deepCard], modifiers: deepMods });
  const deepSnap = resolvePricingSnapshot(deepPlan, {
    side: "CUSTOMER",
    model: "deep",
    operation: "chat",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });
  expect(deepSnap.combined_multiplier.numerator.length).toBeGreaterThan(38);
  expect(evaluateSnapshotAtoms(deepSnap, { input_tokens: "10", output_tokens: "0" })).toBe(1000n);

  const cnyCard: RateCard = {
    ...deepCard,
    id: "direct-cny",
    currency: "CNY",
  };
  const tieFx: FxRateVersion[] = [
    { id: "fx1", base_currency: "USD", quote_currency: "CNY", rate: R("725", "100") },
    { id: "fx2", base_currency: "USD", quote_currency: "CNY", rate: R("730", "100") },
  ];
  const orderPlan = compileRatingPlan({ rateCards: [deepCard, cnyCard], fxRates: tieFx });
  expect(
    resolvePricingSnapshot(orderPlan, {
      side: "CUSTOMER",
      model: "deep",
      operation: "chat",
      currency: "CNY",
      rated_at: "2026-06-15T12:00:00Z",
    }).rate_card_id,
  ).toBe("direct-cny");

  const protoAttrs = Object.create({ quality: "high" }) as Record<string, string>;
  const protoCard: RateCard = {
    id: "proto-card",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "img",
    operation: "images",
    rates: { images: R("10") },
    matrix: [{ match: { quality: "high" }, rates: { images: R("99") } }],
    ceilings: { images: "4" },
  };
  const protoSnap = resolvePricingSnapshot(compileRatingPlan({ rateCards: [protoCard] }), {
    side: "CUSTOMER",
    model: "img",
    operation: "images",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
    attrs: protoAttrs,
  });
  expect(protoSnap.matched_matrix_index).toBeNull();

  expect(() =>
    resolvePricingSnapshot(deepPlan, {
      side: "CUSTOMER",
      model: "deep",
      operation: "chat",
      currency: "USD",
      rated_at: "2026-02-31T00:00:00Z",
    }),
  ).toThrow("calendar");

  expect(() =>
    validatePriceModifier({
      id: "non-canonical-tz",
      layer: "PRODUCT",
      kind: "MULTIPLIER",
      stacking: "MULTIPLY",
      priority: 1,
      enabled: true,
      time_windows: [{ timezone: "aMeRiCa/nEw_YoRk", start_minute: 0, end_minute: 60 }],
      match: {},
      multiplier: R("1", "2"),
    }),
  ).toThrow("IANA timezone");

  expect(() => quoteSnapshotReservation(deepSnap, {}, -1)).toThrow(DomainError);
  expect(() => quoteSnapshotReservation(deepSnap, {}, 1.5)).toThrow(DomainError);

  const unpricedCacheCliffCard: RateCard = {
    id: "unpriced-cache-cliff",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "m-cache-driver",
    operation: "messages",
    rates: { input_tokens: R("2"), output_tokens: R("5") },
    cliff: {
      driver: [
        "input_tokens",
        "cache_write_tokens",
        "cache_write_5m_tokens",
        "cache_write_1h_tokens",
      ],
      tiers: [
        { min_driver: "0", rates: {} },
        { min_driver: "150000", rates: { input_tokens: R("4"), output_tokens: R("10") } },
      ],
    },
    ceilings: {
      input_tokens: "100000",
      cache_write_tokens: "100000",
      cache_write_5m_tokens: "100000",
      cache_write_1h_tokens: "100000",
      output_tokens: "1000",
    },
  };
  const cacheDriverSnap = resolvePricingSnapshot(
    compileRatingPlan({ rateCards: [unpricedCacheCliffCard], requireCeilings: true }),
    {
      side: "CUSTOMER",
      model: "m-cache-driver",
      operation: "messages",
      currency: "USD",
      rated_at: "2026-06-15T12:00:00Z",
    },
  );
  const cacheDriverQuote = quoteSnapshotReservation(cacheDriverSnap, {}, 1000);
  expect(cacheDriverQuote.matched_cliff_tier_index).toBe(1);
  expect(BigInt(cacheDriverQuote.amount_atoms)).toBe(410_000n);

  expect(() =>
    validateRateCard({
      id: "cross-tier-cache-conflict",
      side: "CUSTOMER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: { input_tokens: R("1") },
      cliff: {
        driver: ["input_tokens"],
        tiers: [
          { min_driver: "0", rates: { cache_write_tokens: R("2") } },
          {
            min_driver: "100",
            rates: { cache_write_5m_tokens: R("3"), cache_write_1h_tokens: R("4") },
          },
        ],
      },
      ceilings: {
        input_tokens: "1000",
        cache_write_tokens: "1000",
        cache_write_5m_tokens: "1000",
        cache_write_1h_tokens: "1000",
      },
    }),
  ).toThrow("cache write");

  const freeZeroMargin = evaluateMargin({
    customerCurrency: "USD",
    customerAtoms: 0n,
    supplierCurrency: "USD",
    supplierAtoms: 0n,
    policy: { min_margin_bps: 2000n },
  });
  expect(freeZeroMargin.margin_bps).toBe(0n);
  expect(freeZeroMargin.allowed).toBe(false);
  expect(freeZeroMargin.reason).toBe("margin_below_min_bps");
});

test("symptom: effective_card strips matrix, partial cache-write TTL ceilings fail closed, EXACT_RATE_CARD rejects BEST_FOR_CUSTOMER, and hasCompleteSnapshotUsage guards non-token & reasoning dimensions", () => {
  const matrixCard: RateCard = {
    id: "matrix-strip",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "m-mat",
    operation: "responses",
    rates: { input_tokens: R("1"), output_tokens: R("2") },
    matrix: [
      {
        match: { mode: "search" },
        rates: { input_tokens: R("1"), output_tokens: R("2"), web_search_calls: R("50") },
      },
    ],
    ceilings: { input_tokens: "100", output_tokens: "100", web_search_calls: "2" },
  };
  const mPlan = compileRatingPlan({ rateCards: [matrixCard], requireCeilings: true });
  const matchedSnap = resolvePricingSnapshot(mPlan, {
    side: "CUSTOMER",
    model: "m-mat",
    operation: "responses",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
    attrs: { mode: "search" },
  });
  expect(matchedSnap.matched_matrix_index).toBe(0);
  expect(matchedSnap.effective_card.matrix).toBeUndefined();
  expect(matchedSnap.effective_card.rates.web_search_calls).toEqual(R("50"));

  const fallbackSnap = resolvePricingSnapshot(mPlan, {
    side: "CUSTOMER",
    model: "m-mat",
    operation: "responses",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
    attrs: { mode: "plain" },
  });
  expect(fallbackSnap.matched_matrix_index).toBeNull();
  expect(fallbackSnap.effective_card.matrix).toBeUndefined();
  expect(fallbackSnap.effective_card.rates.web_search_calls).toBeUndefined();

  const partialCacheCard: RateCard = {
    id: "partial-cache-cliff",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "m-pcc",
    operation: "messages",
    rates: { input_tokens: R("1"), output_tokens: R("2") },
    cliff: {
      driver: ["input_tokens", "cache_write_tokens"],
      tiers: [
        { min_driver: "0", rates: {} },
        { min_driver: "2000", rates: { input_tokens: R("5") } },
      ],
    },
    ceilings: { input_tokens: "500", output_tokens: "100", cache_write_5m_tokens: "500" },
  };
  const partialSnap = resolvePricingSnapshot(compileRatingPlan({ rateCards: [partialCacheCard] }), {
    side: "CUSTOMER",
    model: "m-pcc",
    operation: "messages",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });
  const partialQuote = quoteSnapshotReservation(partialSnap, {}, 100);
  expect(partialQuote.matched_cliff_tier_index).toBe(1);
  expect(partialQuote.amount_atoms).toBe("2700");

  const bothTtlCard: RateCard = {
    ...partialCacheCard,
    id: "both-ttl-cliff",
    ceilings: {
      input_tokens: "500",
      output_tokens: "100",
      cache_write_5m_tokens: "300",
      cache_write_1h_tokens: "400",
    },
  };
  const bothTtlSnap = resolvePricingSnapshot(compileRatingPlan({ rateCards: [bothTtlCard] }), {
    side: "CUSTOMER",
    model: "m-pcc",
    operation: "messages",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });
  const bothTtlQuote = quoteSnapshotReservation(bothTtlSnap, {}, 100);
  expect(bothTtlQuote.matched_cliff_tier_index).toBe(0);
  expect(bothTtlQuote.amount_atoms).toBe("700");

  expect(() =>
    validatePriceModifier({
      id: "exact-best-invalid",
      layer: "COMMERCIAL",
      kind: "EXACT_RATE_CARD",
      stacking: "BEST_FOR_CUSTOMER",
      priority: 1,
      enabled: true,
      match: {},
      rate_card: matrixCard,
    }),
  ).toThrow("BEST_FOR_CUSTOMER");

  expect(hasCompleteSnapshotUsage(matchedSnap, { input_tokens: "10", output_tokens: "5" })).toBe(
    false,
  );
  expect(
    hasCompleteSnapshotUsage(matchedSnap, {
      input_tokens: "10",
      output_tokens: "5",
      web_search_calls: "0",
    }),
  ).toBe(true);

  const reasonCard: RateCard = {
    id: "reason-complete",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "m-rc",
    operation: "chat",
    rates: { input_tokens: R("1"), output_tokens: R("2") },
    reasoning_rate: R("10"),
    ceilings: { input_tokens: "100", output_tokens: "100" },
  };
  const reasonSnap = resolvePricingSnapshot(compileRatingPlan({ rateCards: [reasonCard] }), {
    side: "CUSTOMER",
    model: "m-rc",
    operation: "chat",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });
  expect(hasCompleteSnapshotUsage(reasonSnap, { input_tokens: "10", output_tokens: "5" })).toBe(
    false,
  );
  expect(
    hasCompleteSnapshotUsage(reasonSnap, {
      input_tokens: "10",
      output_tokens: "5",
      reasoning_tokens: "0",
    }),
  ).toBe(true);

  const cliffDriverCard: RateCard = {
    id: "cliff-driver-complete",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "m-cdc",
    operation: "chat",
    rates: { input_tokens: R("2"), output_tokens: R("4") },
    cliff: {
      driver: ["input_tokens", "cached_input_tokens"],
      tiers: [
        { min_driver: "0", rates: { cached_input_tokens: R("0") } },
        {
          min_driver: "200000",
          rates: { input_tokens: R("10"), output_tokens: R("20"), cached_input_tokens: R("1") },
        },
      ],
    },
    ceilings: { input_tokens: "500000", cached_input_tokens: "500000", output_tokens: "1000" },
  };
  const cliffDriverSnap = resolvePricingSnapshot(
    compileRatingPlan({ rateCards: [cliffDriverCard] }),
    {
      side: "CUSTOMER",
      model: "m-cdc",
      operation: "chat",
      currency: "USD",
      rated_at: "2026-06-15T12:00:00Z",
    },
  );
  expect(
    hasCompleteSnapshotUsage(cliffDriverSnap, { input_tokens: "100", output_tokens: "10" }),
  ).toBe(false);
  expect(
    hasCompleteSnapshotUsage(cliffDriverSnap, {
      input_tokens: "100",
      output_tokens: "10",
      cached_input_tokens: "0",
    }),
  ).toBe(true);
});
