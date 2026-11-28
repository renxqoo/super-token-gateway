import {
  calculateCompiledCostAtoms,
  compilePricing,
  compileRatingPlan,
  compileSnapshot,
  evaluateCompiledSnapshotAtoms,
  evaluateMargin,
  quoteCompiledSnapshotReservation,
  resolvePricingSnapshot,
  type FxRateVersion,
  type PriceModifier,
  type PricingContext,
  type RateCard,
  type Usage,
} from "@gateway/kernel";

type BenchSummary = {
  suite: string;
  ops: number;
  elapsed_ms: number;
  ops_per_sec: number;
  p50_us: number;
  p95_us: number;
  p99_us: number;
  max_us: number;
  violations: number;
};

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p));
  return Number(sorted[idx]!.toFixed(3));
}

function summarize(
  suite: string,
  ops: number,
  elapsedMs: number,
  samplesUs: number[],
  violations = 0,
): BenchSummary {
  samplesUs.sort((a, b) => a - b);
  return {
    suite,
    ops,
    elapsed_ms: Number(elapsedMs.toFixed(2)),
    ops_per_sec: Math.round((ops / elapsedMs) * 1000),
    p50_us: percentile(samplesUs, 0.5),
    p95_us: percentile(samplesUs, 0.95),
    p99_us: percentile(samplesUs, 0.99),
    max_us: percentile(samplesUs, 1),
    violations,
  };
}

function buildStressPlan() {
  const rateCards: RateCard[] = [];
  const modifiers: PriceModifier[] = [];
  const fxRates: FxRateVersion[] = [
    {
      id: "fx-usd-cny",
      base_currency: "USD",
      quote_currency: "CNY",
      rate: { numerator: "725", denominator: "100" },
      effective_from: "2026-01-01T00:00:00Z",
    },
    {
      id: "fx-usd-eur",
      base_currency: "USD",
      quote_currency: "EUR",
      rate: { numerator: "92", denominator: "100" },
      effective_from: "2026-01-01T00:00:00Z",
    },
  ];

  for (let m = 0; m < 100; m++) {
    const model = `model-${m}`;
    const mode = m % 5;
    const baseRates = {
      input_tokens: { numerator: String(100 + m), denominator: "1000000" },
      cached_input_tokens: { numerator: String(25 + (m % 10)), denominator: "1000000" },
      cache_write_5m_tokens: { numerator: String(125 + m), denominator: "1000000" },
      cache_write_1h_tokens: { numerator: String(200 + m), denominator: "1000000" },
      output_tokens: { numerator: String(400 + m * 2), denominator: "1000000" },
      web_search_calls: { numerator: "10000000000000000", denominator: "1000" },
    };
    const ceilings = {
      input_tokens: "1000000",
      cached_input_tokens: "1000000",
      cache_write_5m_tokens: "1000000",
      cache_write_1h_tokens: "1000000",
      output_tokens: "64000",
      web_search_calls: "10",
    };

    const card: RateCard = {
      id: `card-${m}`,
      side: "CUSTOMER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      model,
      operation: "chat",
      rates: baseRates,
      ceilings,
      ...(mode === 1
        ? {
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
                    input_tokens: { numerator: String((100 + m) * 2), denominator: "1000000" },
                    output_tokens: { numerator: String((400 + m * 2) * 3), denominator: "2000000" },
                  },
                },
              ],
            },
          }
        : {}),
      ...(mode === 2
        ? {
            progressive: {
              input_tokens: [
                { up_to: "32000", rate: { numerator: "100", denominator: "1000000" } },
                { up_to: "128000", rate: { numerator: "200", denominator: "1000000" } },
                { up_to: "256000", rate: { numerator: "400", denominator: "1000000" } },
                { up_to: null, rate: { numerator: "800", denominator: "1000000" } },
              ],
            },
          }
        : {}),
      ...(mode === 3
        ? {
            matrix: [
              {
                match: { quality: "high", size: "1024x1536" },
                rates: { images: { numerator: "80000000000000000", denominator: "1" } },
              },
              {
                match: { quality: "low", size: "1024x1024" },
                rates: { images: { numerator: "11000000000000000", denominator: "1" } },
              },
            ],
            ceilings: { ...ceilings, images: "4" },
          }
        : {}),
      ...(mode === 4
        ? {
            reasoning_rate: { numerator: String(800 + m * 2), denominator: "1000000" },
            min_charge_atoms: "1000000000000",
            max_charge_atoms: "50000000000000000000",
          }
        : {}),
    };
    rateCards.push(card);

    rateCards.push({
      id: `sup-${m}`,
      side: "SUPPLIER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      model,
      operation: "chat",
      rates: {
        input_tokens: { numerator: String(60 + m), denominator: "1000000" },
        output_tokens: { numerator: String(240 + m), denominator: "1000000" },
        web_search_calls: { numerator: "8000000000000000", denominator: "1000" },
      },
      ceilings: {
        input_tokens: "1000000",
        output_tokens: "64000",
        web_search_calls: "10",
      },
    });

    modifiers.push(
      {
        id: `tou-utc-${m}`,
        layer: "PRODUCT",
        kind: "MULTIPLIER",
        stacking: "MULTIPLY",
        priority: 10,
        enabled: true,
        time_windows: [{ timezone: "UTC", start_minute: 16 * 60 + 30, end_minute: 30 }],
        match: { side: "CUSTOMER", model },
        multiplier: { numerator: "1", denominator: "2" },
      },
      {
        id: `tou-sh-${m}`,
        layer: "PRODUCT",
        kind: "MULTIPLIER",
        stacking: "MULTIPLY",
        priority: 11,
        enabled: true,
        time_windows: [{ timezone: "Asia/Shanghai", start_minute: 0, end_minute: 8 * 60 }],
        match: { side: "CUSTOMER", model, region: "cn" },
        multiplier: { numerator: "8", denominator: "10" },
      },
      {
        id: `tier-prio-${m}`,
        layer: "PRODUCT",
        kind: "MULTIPLIER",
        stacking: "MULTIPLY",
        priority: 12,
        enabled: true,
        match: { side: "CUSTOMER", model, service_tier: "priority" },
        multiplier: { numerator: "2", denominator: "1" },
      },
      {
        id: `vip-${m}`,
        layer: "COMMERCIAL",
        kind: "MULTIPLIER",
        stacking: "BEST_FOR_CUSTOMER",
        priority: 20,
        enabled: true,
        match: { side: "CUSTOMER", model, customer_group_id: "vip" },
        multiplier: { numerator: "85", denominator: "100" },
      },
      {
        id: `promo-${m}`,
        layer: "COMMERCIAL",
        kind: "MULTIPLIER",
        stacking: "BEST_FOR_CUSTOMER",
        priority: 25,
        enabled: true,
        effective_from: "2026-01-01T00:00:00Z",
        effective_to: "2027-01-01T00:00:00Z",
        match: { side: "CUSTOMER", model },
        multiplier: { numerator: "9", denominator: "10" },
      },
    );
  }

  return compileRatingPlan({ rateCards, modifiers, fxRates, requireCeilings: true });
}

const compileStart = performance.now();
const plan = buildStressPlan();
const compileMs = Number((performance.now() - compileStart).toFixed(2));

const contexts: PricingContext[] = [];
const timestamps = [
  "2026-06-15T18:00:00Z",
  "2026-06-15T04:00:00Z",
  "2026-01-15T02:30:00Z",
  "2026-07-15T12:00:00Z",
];
const currencies = ["USD", "CNY", "EUR"] as const;
for (let i = 0; i < 1000; i++) {
  const m = i % 100;
  contexts.push({
    side: "CUSTOMER",
    model: `model-${m}`,
    operation: "chat",
    currency: currencies[i % currencies.length]!,
    rated_at: timestamps[i % timestamps.length]!,
    service_tier: i % 4 === 0 ? "priority" : "standard",
    region: i % 3 === 0 ? "cn" : "global",
    customer_group_id: i % 2 === 0 ? "vip" : "standard",
    ...(m % 5 === 3 ? { attrs: { quality: "high", size: "1024x1536" } } : {}),
  });
}

const results: BenchSummary[] = [];

{
  const ops = 200_000;
  const samples: number[] = [];
  const t0 = performance.now();
  for (let i = 0; i < ops; i++) {
    const s0 = i % 20 === 0 ? performance.now() : 0;
    resolvePricingSnapshot(plan, contexts[i % contexts.length]!);
    if (s0 > 0) samples.push((performance.now() - s0) * 1000);
  }
  results.push(
    summarize(
      "1. resolvePricingSnapshot (100 models x 500 mods x TZ x FX)",
      ops,
      performance.now() - t0,
      samples,
    ),
  );
}

const preSnapshots = contexts
  .slice(0, 100)
  .map((ctx) => compileSnapshot(resolvePricingSnapshot(plan, ctx)));
const preSupplierSnapshots = contexts.slice(0, 100).map((ctx) =>
  compileSnapshot(
    resolvePricingSnapshot(plan, {
      ...ctx,
      side: "SUPPLIER",
      currency: "USD",
    }),
  ),
);

const usages: Usage[] = [
  { input_tokens: "1200", output_tokens: "400" },
  { input_tokens: "180000", cached_input_tokens: "30000", output_tokens: "1200" },
  { input_tokens: "210000", output_tokens: "800" },
  { input_tokens: "4000", output_tokens: "1200", web_search_calls: "3", images: "2" },
  { input_tokens: "3000", output_tokens: "2000", reasoning_tokens: "1500" },
];

{
  const ops = 500_000;
  const samples: number[] = [];
  let sum = 0n;
  const t0 = performance.now();
  for (let i = 0; i < ops; i++) {
    const s0 = i % 50 === 0 ? performance.now() : 0;
    sum += evaluateCompiledSnapshotAtoms(
      preSnapshots[i % preSnapshots.length]!,
      usages[i % usages.length]!,
    );
    if (s0 > 0) samples.push((performance.now() - s0) * 1000);
  }
  if (sum <= 0n) throw new Error("unexpected zero sum");
  results.push(
    summarize(
      "2. evaluateCompiledSnapshotAtoms (Flat/Cliff/Progressive/Matrix/Reasoning/Tools/FX)",
      ops,
      performance.now() - t0,
      samples,
    ),
  );
}

{
  const legacyPricing = compilePricing({
    currency: "USD",
    rates: {
      input_tokens: { numerator: "100", denominator: "1" },
      output_tokens: { numerator: "400", denominator: "1" },
    },
  });
  const ops = 500_000;
  const samples: number[] = [];
  let sum = 0n;
  const t0 = performance.now();
  for (let i = 0; i < ops; i++) {
    const s0 = i % 50 === 0 ? performance.now() : 0;
    sum += calculateCompiledCostAtoms(
      { input_tokens: "1200", output_tokens: "400" },
      legacyPricing,
    );
    if (s0 > 0) samples.push((performance.now() - s0) * 1000);
  }
  if (sum <= 0n) throw new Error("unexpected zero sum");
  results.push(
    summarize(
      "3. calculateCompiledCostAtoms (legacy fast-path baseline)",
      ops,
      performance.now() - t0,
      samples,
    ),
  );
}

{
  const ops = 100_000;
  const samples: number[] = [];
  let violations = 0;
  const t0 = performance.now();
  for (let i = 0; i < ops; i++) {
    const idx = i % preSnapshots.length;
    const custSnap = preSnapshots[idx]!;
    const supSnap = preSupplierSnapshots[idx]!;
    const s0 = i % 20 === 0 ? performance.now() : 0;
    const estInput = idx % 5 === 1 ? 220_000 : 8_000;
    const outBound = 2_000;
    const quote = quoteCompiledSnapshotReservation(
      custSnap,
      {
        estimatedInputTokens: estInput,
        maxToolCalls: { web_search_calls: 3 },
        ...(idx % 5 === 3 ? { images: 2 } : {}),
      },
      outBound,
    );
    const actualUsage: Usage = {
      input_tokens: String(Math.floor(estInput * 0.7)),
      cached_input_tokens: String(Math.floor(estInput * 0.2)),
      output_tokens: "1500",
      ...(idx % 5 === 4 ? { reasoning_tokens: "1000" } : {}),
      web_search_calls: "2",
      ...(idx % 5 === 3 ? { images: "2" } : {}),
    };
    const custAtoms = evaluateCompiledSnapshotAtoms(custSnap, actualUsage);
    const supAtoms = evaluateCompiledSnapshotAtoms(supSnap, actualUsage);
    if (BigInt(quote.amount_atoms) < custAtoms) violations++;
    const margin = evaluateMargin({
      customerCurrency: custSnap.snapshot.currency,
      customerAtoms: custAtoms,
      supplierCurrency: supSnap.snapshot.currency,
      supplierAtoms: supAtoms,
      supplierToCustomerFx: custSnap.snapshot.fx_rate,
    });
    if (!margin.comparable) violations++;
    if (s0 > 0) samples.push((performance.now() - s0) * 1000);
  }
  results.push(
    summarize(
      "4. Full lifecycle (quoteReservation + evaluateCustomer + evaluateSupplier + evaluateMargin)",
      ops,
      performance.now() - t0,
      samples,
      violations,
    ),
  );
}

console.log(
  JSON.stringify(
    {
      compile_ms: compileMs,
      cards: 200,
      modifiers: 500,
      fx_rates: 2,
      suites: results,
      heap_mb: Number((process.memoryUsage().heapUsed / (1024 * 1024)).toFixed(2)),
    },
    null,
    2,
  ),
);
if (results.some((r) => r.violations > 0)) {
  process.exit(1);
}
