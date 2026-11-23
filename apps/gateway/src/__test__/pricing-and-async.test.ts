import { expect, test } from "bun:test";
import type {
  ConfigSnapshot,
  PriceVersionRecord,
  ProjectPricingOverrideRecord,
} from "@gateway/ledger";
import { computeSpendWindowStartMs, rolloverKeySpendInMemory } from "@gateway/ledger";
import { extractByDotPath, extractUpstreamTaskId } from "../async-tasks.ts";
import { resolveEffectiveCustomerPrice } from "../pricing.ts";

function makeBasePrice(id: string, currency = "USD"): PriceVersionRecord {
  return {
    id,
    side: "CUSTOMER",
    currency,
    basis: "PROVIDER_USAGE",
    rates: {
      input_tokens: { numerator: "10", denominator: "1" },
      output_tokens: { numerator: "30", denominator: "1" },
    },
    ceilings: { input_tokens: "10000", output_tokens: "10000" },
    source_digest: "digest-base",
  };
}

function makeSnap(
  prices: PriceVersionRecord[],
  overrides: ProjectPricingOverrideRecord[],
): ConfigSnapshot {
  const byProj = new Map<string, ProjectPricingOverrideRecord[]>();
  for (const o of overrides) {
    const list = byProj.get(o.project_id);
    if (list) list.push(o);
    else byProj.set(o.project_id, [o]);
  }
  return {
    epoch: 1,
    projects: new Map(),
    keysByVerifier: new Map(),
    accounts: new Map(),
    channels: new Map(),
    models: new Map(),
    prices: new Map(prices.map((p) => [p.id, p])),
    pricingOverridesByProject: byProj,
    routesByModel: new Map(),
    plugins: new Map(),
    revokedPlugins: new Set(),
    providerPluginsByName: new Map(),
  };
}

test("resolveEffectiveCustomerPrice obeys 6-level specificity order and exact rational GCD reduction", () => {
  const projId = "00000000-0000-4000-8000-000000000001";
  const basePrice = makeBasePrice("10000000-0000-4000-8000-000000000001");
  const exactOpPrice: PriceVersionRecord = {
    ...basePrice,
    id: "10000000-0000-4000-8000-000000000002",
    rates: { input_tokens: { numerator: "3", denominator: "1" } },
  };
  const exactWildOpPrice: PriceVersionRecord = {
    ...basePrice,
    id: "10000000-0000-4000-8000-000000000003",
    rates: { input_tokens: { numerator: "5", denominator: "1" } },
  };

  const snap = makeSnap(
    [basePrice, exactOpPrice, exactWildOpPrice],
    [
      {
        id: "ovr-wild-all",
        project_id: projId,
        model_name: "*",
        operation: "*",
        override_kind: "MULTIPLIER",
        billing_price_id: null,
        multiplier_numerator: "90",
        multiplier_denominator: "100",
        enabled: true,
      },
      {
        id: "ovr-wild-chat",
        project_id: projId,
        model_name: "*",
        operation: "chat",
        override_kind: "MULTIPLIER",
        billing_price_id: null,
        multiplier_numerator: "85",
        multiplier_denominator: "100",
        enabled: true,
      },
      {
        id: "ovr-model-wild-mult",
        project_id: projId,
        model_name: "gpt-4o",
        operation: "*",
        override_kind: "MULTIPLIER",
        billing_price_id: null,
        multiplier_numerator: "80",
        multiplier_denominator: "100",
        enabled: true,
      },
      {
        id: "ovr-model-op-mult",
        project_id: projId,
        model_name: "gpt-4o",
        operation: "responses",
        override_kind: "MULTIPLIER",
        billing_price_id: null,
        multiplier_numerator: "75",
        multiplier_denominator: "100",
        enabled: true,
      },
      {
        id: "ovr-exact-wild",
        project_id: projId,
        model_name: "claude-3",
        operation: "*",
        override_kind: "EXACT_PRICE",
        billing_price_id: exactWildOpPrice.id,
        multiplier_numerator: null,
        multiplier_denominator: null,
        enabled: true,
      },
      {
        id: "ovr-exact-op",
        project_id: projId,
        model_name: "claude-3",
        operation: "messages",
        override_kind: "EXACT_PRICE",
        billing_price_id: exactOpPrice.id,
        multiplier_numerator: null,
        multiplier_denominator: null,
        enabled: true,
      },
    ],
  );

  const r1 = resolveEffectiveCustomerPrice(snap, projId, "claude-3", "messages", basePrice);
  expect(r1.overrideId).toBe("ovr-exact-op");
  expect(r1.price.id).toBe(exactOpPrice.id);

  const r2 = resolveEffectiveCustomerPrice(snap, projId, "claude-3", "chat", basePrice);
  expect(r2.overrideId).toBe("ovr-exact-wild");
  expect(r2.price.id).toBe(exactWildOpPrice.id);

  const r3 = resolveEffectiveCustomerPrice(snap, projId, "gpt-4o", "responses", basePrice);
  expect(r3.overrideId).toBe("ovr-model-op-mult");
  expect(r3.price.rates.input_tokens).toEqual({ numerator: "15", denominator: "2" });
  expect(r3.price.rates.output_tokens).toEqual({ numerator: "45", denominator: "2" });

  const r4 = resolveEffectiveCustomerPrice(snap, projId, "gpt-4o", "chat", basePrice);
  expect(r4.overrideId).toBe("ovr-model-wild-mult");
  expect(r4.price.rates.input_tokens).toEqual({ numerator: "8", denominator: "1" });

  const r5 = resolveEffectiveCustomerPrice(snap, projId, "other-model", "chat", basePrice);
  expect(r5.overrideId).toBe("ovr-wild-chat");
  expect(r5.price.rates.input_tokens).toEqual({ numerator: "17", denominator: "2" });

  const r6 = resolveEffectiveCustomerPrice(snap, projId, "other-model", "embeddings", basePrice);
  expect(r6.overrideId).toBe("ovr-wild-all");
  expect(r6.price.rates.input_tokens).toEqual({ numerator: "9", denominator: "1" });
});

test("extractByDotPath blocks prototype pollution and extractUpstreamTaskId handles nested paths", () => {
  expect(extractByDotPath({ a: { b: 42 } }, "a.b")).toBe(42);
  expect(extractByDotPath({ a: {} }, "__proto__.polluted")).toBeUndefined();
  expect(extractByDotPath({ a: {} }, "a.constructor.name")).toBeUndefined();
  expect(extractUpstreamTaskId({ task_id: "mm_999" }, null)).toBe("mm_999");
  expect(
    extractUpstreamTaskId(
      { data: { job: { id: 123456 } } },
      { query_path: "/v1/q", task_id_path: "data.job.id" },
    ),
  ).toBe("123456");
});

test("key spend UTC DAILY and MONTHLY window rollover resets spent_atoms without touching reserved_atoms", () => {
  const jan15 = Date.UTC(2026, 0, 15, 10, 30, 0);
  const jan16 = Date.UTC(2026, 0, 16, 0, 0, 1);
  const feb01 = Date.UTC(2026, 1, 1, 0, 0, 1);

  expect(computeSpendWindowStartMs("DAILY", jan16, jan15)).toBe(Date.UTC(2026, 0, 16, 0, 0, 0));
  expect(computeSpendWindowStartMs("MONTHLY", feb01, jan15)).toBe(Date.UTC(2026, 1, 1, 0, 0, 0));
  expect(computeSpendWindowStartMs("LIFETIME", feb01, jan15)).toBe(jan15);

  const row = {
    id: "k1",
    project_id: "p1",
    spend_limit_atoms: 1000n,
    spend_limit_window: "DAILY" as const,
    spend_window_start_ms: Date.UTC(2026, 0, 15, 0, 0, 0),
    spent_atoms: 900n,
    reserved_atoms: 50n,
    dirty: false,
  };
  rolloverKeySpendInMemory(row, jan16);
  expect(row.spent_atoms).toBe(0n);
  expect(row.reserved_atoms).toBe(50n);
  expect(row.spend_window_start_ms).toBe(Date.UTC(2026, 0, 16, 0, 0, 0));
  expect(row.dirty).toBe(true);
});
