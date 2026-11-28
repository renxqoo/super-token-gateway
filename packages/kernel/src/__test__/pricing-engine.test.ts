import { expect, test } from "bun:test";

import {
  DomainError,
  calculateCompiledCostAtoms,
  calculateCost,
  compilePricing,
  compileRatingPlan,
  evaluateSnapshotAtoms,
  evaluateSnapshotBreakdown,
  quoteSnapshotReservation,
  resolvePricingSnapshot,
  type FxRateVersion,
  type PriceModifier,
  type PricingContext,
  type RateCard,
} from "../index.ts";

const R = (numerator: string, denominator = "1") => ({ numerator, denominator });

test("symptom: TTL-only cliff driver under-reserves an aggregate cache write rate", () => {
  const card: RateCard = {
    id: "aggregate-cache-with-ttl-cliff",
    side: "CUSTOMER",
    basis: "PROVIDER_USAGE",
    currency: "USD",
    rates: { cache_write_tokens: { numerator: "1", denominator: "1" } },
    ceilings: { cache_write_tokens: "10", cache_write_5m_tokens: "10" },
    cliff: {
      driver: ["cache_write_5m_tokens"],
      tiers: [
        { min_driver: "0", rates: {} },
        { min_driver: "5", rates: { cache_write_tokens: { numerator: "10", denominator: "1" } } },
        { min_driver: "11", rates: { cache_write_tokens: { numerator: "25", denominator: "1" } } },
      ],
    },
  };
  const snapshot = resolvePricingSnapshot(compileRatingPlan({ rateCards: [card] }), {
    side: "CUSTOMER",
    currency: "USD",
    model: "cache-model",
    operation: "messages",
    rated_at: "2026-06-15T12:00:00Z",
  });
  expect(quoteSnapshotReservation(snapshot, {}, 0).amount_atoms).toBe("100");
  expect(() => evaluateSnapshotAtoms(snapshot, { cache_write_tokens: "10" })).toThrow(
    "Upstream omitted cache write usage required by pricing",
  );
  expect(
    evaluateSnapshotAtoms(snapshot, { cache_write_tokens: "10", cache_write_1h_tokens: "1" }),
  ).toBe(100n);
  expect(
    evaluateSnapshotAtoms(snapshot, { cache_write_tokens: "10", cache_write_5m_tokens: "0" }),
  ).toBe(10n);
});

test("symptom: unmatched matrix rejects a valid base reasoning rate", () => {
  const card: RateCard = {
    id: "reasoning-with-matrix-override",
    side: "CUSTOMER",
    basis: "PROVIDER_USAGE",
    currency: "USD",
    rates: {},
    reasoning_rate: R("4"),
    ceilings: { output_tokens: "10" },
    matrix: [{ match: { mode: "premium" }, rates: {}, reasoning_rate: R("9") }],
  };
  const snapshot = resolvePricingSnapshot(compileRatingPlan({ rateCards: [card] }), {
    side: "CUSTOMER",
    currency: "USD",
    model: "reasoning-model",
    operation: "chat",
    rated_at: "2026-06-15T12:00:00Z",
    attrs: { mode: "standard" },
  });
  expect(evaluateSnapshotAtoms(snapshot, { output_tokens: "5", reasoning_tokens: "2" })).toBe(8n);
});

test.each(["reasoning_tokens", "web_search_calls"] as const)(
  "symptom: a snapshot settles with missing %s usage",
  (missingDimension) => {
    const card: RateCard = {
      id: "reasoning-and-search",
      side: "CUSTOMER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: { input_tokens: R("2"), output_tokens: R("1"), web_search_calls: R("100") },
      reasoning_rate: R("20"),
      ceilings: { input_tokens: "10", output_tokens: "10", web_search_calls: "2" },
    };
    const snapshot = resolvePricingSnapshot(compileRatingPlan({ rateCards: [card] }), {
      side: "CUSTOMER",
      currency: "USD",
      model: "reasoning-model",
      operation: "responses",
      rated_at: "2026-06-15T12:00:00Z",
    });
    const explicit = {
      input_tokens: "3",
      output_tokens: "5",
      reasoning_tokens: "0",
      web_search_calls: "0",
    };
    const incomplete: Partial<typeof explicit> = { ...explicit };
    delete incomplete[missingDimension];
    expect(() => evaluateSnapshotAtoms(snapshot, incomplete)).toThrow(
      "Upstream omitted usage required by pricing",
    );
    expect(evaluateSnapshotAtoms(snapshot, explicit)).toBe(11n);
    expect(
      evaluateSnapshotAtoms(snapshot, {
        ...explicit,
        reasoning_tokens: "2",
        web_search_calls: "1",
      }),
    ).toBe(149n);
  },
);

test("all 24 standardized dimensions price accurately with single final ceiling rounding", () => {
  const card: RateCard = {
    id: "card-all-dims",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "omni-pro",
    operation: "responses",
    rates: {
      input_tokens: R("1", "3"),
      output_tokens: R("2", "3"),
      cached_input_tokens: R("1", "6"),
      cache_write_5m_tokens: R("5", "6"),
      cache_write_1h_tokens: R("7", "6"),
      image_input_tokens: R("3", "1"),
      image_output_tokens: R("10", "1"),
      audio_input_tokens: R("4", "1"),
      audio_output_tokens: R("12", "1"),
      video_input_tokens: R("5", "1"),
      requests: R("1", "2"),
      images: R("100", "1"),
      audio_seconds: R("2", "1"),
      music_ms: R("1", "100"),
      video_ms: R("3", "100"),
      characters: R("1", "10"),
      web_search_calls: R("1000", "1"),
      code_interpreter_sessions: R("3000", "1"),
      file_search_calls: R("250", "1"),
      search_units: R("50", "1"),
      pages: R("20", "1"),
    },
    ceilings: {
      input_tokens: "1000",
      output_tokens: "1000",
      cached_input_tokens: "1000",
      cache_write_5m_tokens: "1000",
      cache_write_1h_tokens: "1000",
      image_input_tokens: "1000",
      image_output_tokens: "1000",
      audio_input_tokens: "1000",
      audio_output_tokens: "1000",
      video_input_tokens: "1000",
      requests: "1",
      images: "4",
      audio_seconds: "60",
      music_ms: "10000",
      video_ms: "10000",
      characters: "10000",
      web_search_calls: "10",
      code_interpreter_sessions: "5",
      file_search_calls: "10",
      search_units: "10",
      pages: "100",
    },
  };
  const plan = compileRatingPlan({ rateCards: [card], requireCeilings: true });
  const snap = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "omni-pro",
    operation: "responses",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });
  const usage = {
    input_tokens: "1",
    output_tokens: "1",
    cached_input_tokens: "1",
    cache_write_5m_tokens: "1",
    cache_write_1h_tokens: "1",
    image_input_tokens: "2",
    image_output_tokens: "1",
    audio_input_tokens: "1",
    audio_output_tokens: "1",
    video_input_tokens: "2",
    requests: "1",
    images: "1",
    audio_seconds: "0.25",
    music_ms: "50",
    video_ms: "50",
    characters: "5",
    web_search_calls: "2",
    code_interpreter_sessions: "1",
    file_search_calls: "2",
    search_units: "3",
    pages: "4",
  };
  const expected = 5879n;
  expect(evaluateSnapshotAtoms(snap, usage)).toBe(expected);
  expect(calculateCost(usage, { currency: "USD", rates: card.rates })).toBe(expected.toString());
  expect(
    calculateCompiledCostAtoms(usage, compilePricing({ currency: "USD", rates: card.rates })),
  ).toBe(expected);
});

test("long-context CLIFF tiering switches entire request above 200K without double-counting cache write TTLs", () => {
  const claudeCard: RateCard = {
    id: "claude-sonnet-cliff",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "claude-sonnet-4-6",
    operation: "messages",
    rates: {
      input_tokens: R("3", "1"),
      cached_input_tokens: R("3", "10"),
      cache_write_5m_tokens: R("375", "100"),
      cache_write_1h_tokens: R("6", "1"),
      output_tokens: R("15", "1"),
    },
    cliff: {
      driver: [
        "input_tokens",
        "cached_input_tokens",
        "cache_write_tokens",
        "cache_write_5m_tokens",
        "cache_write_1h_tokens",
      ],
      tiers: [
        { min_driver: "0", rates: {} },
        {
          min_driver: "200001",
          rates: {
            input_tokens: R("6", "1"),
            cached_input_tokens: R("6", "10"),
            cache_write_5m_tokens: R("750", "100"),
            cache_write_1h_tokens: R("12", "1"),
            output_tokens: R("225", "10"),
          },
        },
      ],
    },
    ceilings: {
      input_tokens: "1000000",
      cached_input_tokens: "1000000",
      cache_write_5m_tokens: "1000000",
      cache_write_1h_tokens: "1000000",
      output_tokens: "64000",
    },
  };
  const plan = compileRatingPlan({ rateCards: [claudeCard], requireCeilings: true });
  const snap = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "claude-sonnet-4-6",
    operation: "messages",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });

  const atExactBoundary = evaluateSnapshotBreakdown(snap, {
    input_tokens: "100000",
    cached_input_tokens: "60000",
    cache_write_tokens: "40000",
    cache_write_5m_tokens: "40000",
    cache_write_1h_tokens: "0",
    output_tokens: "1000",
  });
  expect(atExactBoundary.matched_cliff_tier_index).toBe(0);
  expect(atExactBoundary.total_atoms).toBe(300_000n + 18_000n + 150_000n + 15_000n);

  const oneTokenOver = evaluateSnapshotBreakdown(snap, {
    input_tokens: "100001",
    cached_input_tokens: "60000",
    cache_write_tokens: "40000",
    cache_write_5m_tokens: "40000",
    cache_write_1h_tokens: "0",
    output_tokens: "1000",
  });
  expect(oneTokenOver.matched_cliff_tier_index).toBe(1);
  expect(oneTokenOver.total_atoms).toBe(600_006n + 36_000n + 300_000n + 22_500n);

  const tightBelowCliff = quoteSnapshotReservation(snap, { estimatedInputTokens: 100_000 }, 1000);
  expect(tightBelowCliff.matched_cliff_tier_index).toBe(0);
  expect(BigInt(tightBelowCliff.amount_atoms)).toBe(600_000n + 15_000n);

  const tightAboveCliff = quoteSnapshotReservation(snap, { estimatedInputTokens: 200_001 }, 1000);
  expect(tightAboveCliff.matched_cliff_tier_index).toBe(1);
  expect(BigInt(tightAboveCliff.amount_atoms)).toBe(2_400_012n + 22_500n);
  expect(BigInt(tightAboveCliff.amount_atoms)).toBeGreaterThanOrEqual(oneTokenOver.total_atoms);
});

test("PROGRESSIVE marginal brackets compute exact piecewise sums with single final rounding", () => {
  const card: RateCard = {
    id: "qwen-progressive",
    side: "CUSTOMER",
    currency: "CNY",
    basis: "PROVIDER_USAGE",
    model: "qwen-max",
    operation: "chat",
    rates: {
      input_tokens: R("1", "1"),
      output_tokens: R("10", "1"),
    },
    progressive: {
      input_tokens: [
        { up_to: "32000", rate: R("2", "1") },
        { up_to: "128000", rate: R("4", "1") },
        { up_to: null, rate: R("8", "1") },
      ],
    },
    ceilings: {
      input_tokens: "256000",
      output_tokens: "8000",
    },
  };
  const plan = compileRatingPlan({ rateCards: [card], requireCeilings: true });
  const snap = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "qwen-max",
    operation: "chat",
    currency: "CNY",
    rated_at: "2026-06-15T12:00:00Z",
  });

  expect(evaluateSnapshotAtoms(snap, { input_tokens: "10000", output_tokens: "100" })).toBe(
    20_000n + 1_000n,
  );
  expect(evaluateSnapshotAtoms(snap, { input_tokens: "50000", output_tokens: "100" })).toBe(
    64_000n + 72_000n + 1_000n,
  );
  expect(evaluateSnapshotAtoms(snap, { input_tokens: "150000", output_tokens: "100" })).toBe(
    64_000n + 384_000n + 176_000n + 1_000n,
  );

  const reserve = quoteSnapshotReservation(snap, { estimatedInputTokens: 150_000 }, 100);
  expect(BigInt(reserve.amount_atoms)).toBe(625_000n);
});

test("separate reasoning_rate splits output_tokens without double-billing and reserves worst case", () => {
  const card: RateCard = {
    id: "reasoning-split",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "reasoner-1",
    operation: "chat",
    rates: {
      input_tokens: R("2", "1"),
      output_tokens: R("10", "1"),
    },
    reasoning_rate: R("30", "1"),
    ceilings: {
      input_tokens: "10000",
      output_tokens: "4000",
    },
  };
  const plan = compileRatingPlan({ rateCards: [card], requireCeilings: true });
  const snap = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "reasoner-1",
    operation: "chat",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });

  const breakdown = evaluateSnapshotBreakdown(snap, {
    input_tokens: "100",
    output_tokens: "500",
    reasoning_tokens: "300",
  });
  expect(breakdown.total_atoms).toBe(200n + 9_000n + 2_000n);
  expect(breakdown.components.map((c) => c.dimension)).toEqual([
    "input_tokens",
    "reasoning_tokens",
    "output_tokens",
  ]);

  const quote = quoteSnapshotReservation(snap, { estimatedInputTokens: 100 }, 500);
  expect(BigInt(quote.amount_atoms)).toBe(200n + 15_000n);
});

test("media attribute matrix matches image size/quality and video resolution/audio and rejects ambiguous ties", () => {
  const videoCard: RateCard = {
    id: "veo-matrix",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "veo-3",
    operation: "videos",
    rates: {},
    matrix: [
      { match: { resolution: "720p" }, rates: { video_seconds: R("20", "1") } },
      {
        match: { resolution: "1080p", has_audio: false },
        rates: { video_seconds: R("40", "1") },
      },
      {
        match: { resolution: "1080p", has_audio: true },
        rates: { video_seconds: R("80", "1") },
        min_charge_atoms: "400",
      },
    ],
    ceilings: { video_seconds: "10" },
  };
  const plan = compileRatingPlan({ rateCards: [videoCard], requireCeilings: true });

  const withAudio = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "veo-3",
    operation: "videos",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
    attrs: { resolution: "1080p", has_audio: true },
  });
  expect(withAudio.matched_matrix_index).toBe(2);
  expect(evaluateSnapshotAtoms(withAudio, { video_seconds: "8" })).toBe(640n);
  expect(evaluateSnapshotAtoms(withAudio, { video_seconds: "3" })).toBe(400n);

  const noAudio = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "veo-3",
    operation: "videos",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
    attrs: { resolution: "1080p", has_audio: false },
  });
  expect(evaluateSnapshotAtoms(noAudio, { video_seconds: "8" })).toBe(320n);

  expect(() =>
    resolvePricingSnapshot(plan, {
      side: "CUSTOMER",
      model: "veo-3",
      operation: "videos",
      currency: "USD",
      rated_at: "2026-06-15T12:00:00Z",
      attrs: { resolution: "4k" },
    }),
  ).toThrow(DomainError);

  const ambiguousCard: RateCard = {
    id: "ambig-matrix",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "img-x",
    operation: "images",
    rates: {},
    matrix: [
      { match: { size: "1024x1024" }, rates: { images: R("10", "1") } },
      { match: { quality: "high" }, rates: { images: R("20", "1") } },
    ],
    ceilings: { images: "4" },
  };
  const ambigPlan = compileRatingPlan({ rateCards: [ambiguousCard] });
  expect(() =>
    resolvePricingSnapshot(ambigPlan, {
      side: "CUSTOMER",
      model: "img-x",
      operation: "images",
      currency: "USD",
      rated_at: "2026-06-15T12:00:00Z",
      attrs: { size: "1024x1024", quality: "high" },
    }),
  ).toThrow(expect.objectContaining({ code: "ambiguous_pricing" }));
});

test("quantizer enforces minimum duration and ceiling steps for ASR audio and token blocks", () => {
  const asrCard: RateCard = {
    id: "asr-step",
    side: "SUPPLIER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "whisper-pro",
    operation: "transcription",
    rates: { audio_seconds: R("10", "1") },
    quantizers: { audio_seconds: { min_units: "15", step: "60" } },
    ceilings: { audio_seconds: "600" },
  };
  const plan = compileRatingPlan({ rateCards: [asrCard], requireCeilings: true });
  const snap = resolvePricingSnapshot(plan, {
    side: "SUPPLIER",
    model: "whisper-pro",
    operation: "transcription",
    currency: "USD",
    rated_at: "2026-06-15T12:00:00Z",
  });
  expect(evaluateSnapshotAtoms(snap, { audio_seconds: "0" })).toBe(0n);
  expect(evaluateSnapshotAtoms(snap, { audio_seconds: "1.25" })).toBe(600n);
  expect(evaluateSnapshotAtoms(snap, { audio_seconds: "60" })).toBe(600n);
  expect(evaluateSnapshotAtoms(snap, { audio_seconds: "60.0001" })).toBe(1200n);
});

test("time-of-use peak/off-peak handles UTC, IANA timezones, DST transitions, and cross-midnight weekday rollover", () => {
  const card: RateCard = {
    id: "deepseek-base",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "deepseek-chat",
    operation: "chat",
    rates: { input_tokens: R("100", "1"), output_tokens: R("200", "1") },
    ceilings: { input_tokens: "10000", output_tokens: "4000" },
  };
  const modifiers: PriceModifier[] = [
    {
      id: "utc-offpeak",
      layer: "PRODUCT",
      kind: "MULTIPLIER",
      stacking: "MULTIPLY",
      priority: 10,
      enabled: true,
      time_windows: [
        {
          timezone: "UTC",
          days_of_week: [1, 2, 3, 4, 5],
          start_minute: 16 * 60 + 30,
          end_minute: 30,
        },
      ],
      match: { model: "deepseek-chat", region: "global" },
      multiplier: R("1", "2"),
    },
    {
      id: "ny-night",
      layer: "PRODUCT",
      kind: "MULTIPLIER",
      stacking: "MULTIPLY",
      priority: 10,
      enabled: true,
      time_windows: [
        {
          timezone: "America/New_York",
          start_minute: 22 * 60,
          end_minute: 6 * 60,
        },
      ],
      match: { model: "deepseek-chat", region: "us-east" },
      multiplier: R("3", "4"),
    },
  ];
  const plan = compileRatingPlan({ rateCards: [card], modifiers });
  const usage = { input_tokens: "10", output_tokens: "10" };

  const fridayLateUtc = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "deepseek-chat",
    operation: "chat",
    currency: "USD",
    region: "global",
    rated_at: "2026-06-19T23:59:00Z",
  });
  expect(evaluateSnapshotAtoms(fridayLateUtc, usage)).toBe(1500n);

  const saturdayEarlyUtc = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "deepseek-chat",
    operation: "chat",
    currency: "USD",
    region: "global",
    rated_at: "2026-06-20T00:15:00Z",
  });
  expect(evaluateSnapshotAtoms(saturdayEarlyUtc, usage)).toBe(1500n);

  const saturdayLateUtc = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "deepseek-chat",
    operation: "chat",
    currency: "USD",
    region: "global",
    rated_at: "2026-06-20T18:00:00Z",
  });
  expect(evaluateSnapshotAtoms(saturdayLateUtc, usage)).toBe(3000n);

  const nySummerNight = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "deepseek-chat",
    operation: "chat",
    currency: "USD",
    region: "us-east",
    rated_at: "2026-07-15T02:30:00Z",
  });
  expect(evaluateSnapshotAtoms(nySummerNight, usage)).toBe(2250n);

  const nyWinterSameUtc = resolvePricingSnapshot(plan, {
    side: "CUSTOMER",
    model: "deepseek-chat",
    operation: "chat",
    currency: "USD",
    region: "us-east",
    rated_at: "2026-01-15T02:30:00Z",
  });
  expect(evaluateSnapshotAtoms(nyWinterSameUtc, usage)).toBe(3000n);
});

test("layered modifiers enforce Product multiply, Commercial best-of-customer, Exclusive override, and FX conversion with single rounding", () => {
  const usdBase: RateCard = {
    id: "gpt-usd",
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    model: "gpt-5",
    operation: "chat",
    rates: { input_tokens: R("100", "1"), output_tokens: R("300", "1") },
    ceilings: { input_tokens: "10000", output_tokens: "4000" },
    min_charge_atoms: "100",
    max_charge_atoms: "50000",
  };
  const modifiers: PriceModifier[] = [
    {
      id: "priority-2x",
      layer: "PRODUCT",
      kind: "MULTIPLIER",
      stacking: "MULTIPLY",
      priority: 10,
      enabled: true,
      match: { service_tier: "priority" },
      multiplier: R("2", "1"),
    },
    {
      id: "batch-half",
      layer: "PRODUCT",
      kind: "MULTIPLIER",
      stacking: "MULTIPLY",
      priority: 10,
      enabled: true,
      match: { execution_mode: "BATCH" },
      multiplier: R("1", "2"),
    },
    {
      id: "vip-80",
      layer: "COMMERCIAL",
      kind: "MULTIPLIER",
      stacking: "BEST_FOR_CUSTOMER",
      priority: 20,
      enabled: true,
      match: { customer_group_id: "vip" },
      multiplier: R("8", "10"),
    },
    {
      id: "launch-70",
      layer: "COMMERCIAL",
      kind: "MULTIPLIER",
      stacking: "BEST_FOR_CUSTOMER",
      priority: 30,
      enabled: true,
      effective_from: "2026-06-01T00:00:00Z",
      effective_to: "2026-06-30T00:00:00Z",
      match: { model: "gpt-5" },
      multiplier: R("7", "10"),
    },
    {
      id: "coupon-extra-90",
      layer: "COMMERCIAL",
      kind: "MULTIPLIER",
      stacking: "MULTIPLY",
      priority: 40,
      enabled: true,
      match: { attrs: { coupon: "EXTRA10" } },
      multiplier: R("9", "10"),
    },
    {
      id: "enterprise-exclusive",
      layer: "COMMERCIAL",
      kind: "MULTIPLIER",
      stacking: "EXCLUSIVE",
      priority: 1,
      enabled: true,
      match: { billing_account_id: "acct-ent" },
      multiplier: R("6", "10"),
    },
  ];
  const fxRates: FxRateVersion[] = [
    {
      id: "fx-usd-cny-v1",
      base_currency: "USD",
      quote_currency: "CNY",
      rate: R("725", "100"),
      effective_from: "2026-01-01T00:00:00Z",
    },
  ];
  const plan = compileRatingPlan({ rateCards: [usdBase], modifiers, fxRates });

  const ctx: PricingContext = {
    side: "CUSTOMER",
    model: "gpt-5",
    operation: "chat",
    currency: "CNY",
    rated_at: "2026-06-15T12:00:00Z",
    service_tier: "priority",
    customer_group_id: "vip",
    attrs: { coupon: "EXTRA10" },
  };
  const snap = resolvePricingSnapshot(plan, ctx);
  expect(snap.fx_version_id).toBe("fx-usd-cny-v1");
  expect(snap.applied_modifiers.map((m) => m.id)).toEqual([
    "priority-2x",
    "coupon-extra-90",
    "launch-70",
  ]);
  expect(snap.combined_multiplier).toEqual(R("1827", "200"));
  expect(evaluateSnapshotAtoms(snap, { input_tokens: "1", output_tokens: "1" })).toBe(3654n);
  expect(evaluateSnapshotAtoms(snap, { input_tokens: "1", output_tokens: "0" })).toBe(914n);

  const floorHit = evaluateSnapshotBreakdown(snap, { input_tokens: "0", output_tokens: "0" });
  expect(floorHit.clamped_by).toBe("MIN_CHARGE");
  expect(floorHit.total_atoms).toBe(725n);

  const capHit = evaluateSnapshotBreakdown(snap, { input_tokens: "1000", output_tokens: "1000" });
  expect(capHit.clamped_by).toBe("MAX_CHARGE");
  expect(capHit.total_atoms).toBe(362_500n);

  const entSnap = resolvePricingSnapshot(plan, {
    ...ctx,
    billing_account_id: "acct-ent",
  });
  expect(entSnap.applied_modifiers.map((m) => m.id)).toEqual([
    "priority-2x",
    "enterprise-exclusive",
  ]);
  expect(entSnap.combined_multiplier).toEqual(R("87", "10"));
});
