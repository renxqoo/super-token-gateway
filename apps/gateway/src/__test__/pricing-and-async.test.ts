import { expect, test } from "bun:test";
import type {
  ConfigSnapshot,
  PriceVersionRecord,
  BillingAccountPricingOverrideRecord,
} from "@gateway/ledger";
import { computeSpendWindowStartMs, rolloverKeySpendInMemory } from "@gateway/ledger";
import { extractByDotPath, extractUpstreamTaskId } from "../async-tasks.ts";
import { resolveEffectiveCustomerPrice, snapshotSupportsHostedTools } from "../pricing.ts";

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
  overrides: BillingAccountPricingOverrideRecord[],
): ConfigSnapshot {
  const byProj = new Map<string, BillingAccountPricingOverrideRecord[]>();
  for (const o of overrides) {
    const list = byProj.get(o.billing_account_id);
    if (list) list.push(o);
    else byProj.set(o.billing_account_id, [o]);
  }
  return {
    epoch: 1,
    billingAccounts: new Map(),
    keysByVerifier: new Map(),
    accounts: new Map(),
    channels: new Map(),
    models: new Map(),
    prices: new Map(prices.map((p) => [p.id, p])),
    pricingOverridesByBillingAccount: byProj,
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
        billing_account_id: projId,
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
        billing_account_id: projId,
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
        billing_account_id: projId,
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
        billing_account_id: projId,
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
        billing_account_id: projId,
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
        billing_account_id: projId,
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
    billing_account_id: "p1",
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

test("resolveEffectiveCustomerPrice skips disabled overrides, preserves zero rates with GCD reduction, and isolates snapshot cache keys", () => {
  const acctA = "00000000-0000-4000-8000-0000000000a1";
  const acctB = "00000000-0000-4000-8000-0000000000b2";
  const basePrice = makeBasePrice("20000000-0000-4000-8000-000000000001");
  const zeroBasePrice: PriceVersionRecord = {
    ...basePrice,
    id: "20000000-0000-4000-8000-000000000002",
    rates: {
      input_tokens: { numerator: "0", denominator: "1" },
      output_tokens: { numerator: "0", denominator: "1" },
    },
  };
  const freeExactPrice: PriceVersionRecord = {
    ...basePrice,
    id: "20000000-0000-4000-8000-000000000003",
    rates: {
      input_tokens: { numerator: "0", denominator: "1" },
      output_tokens: { numerator: "0", denominator: "1" },
    },
  };

  const snap = makeSnap(
    [basePrice, zeroBasePrice, freeExactPrice],
    [
      {
        id: "disabled-exact",
        billing_account_id: acctA,
        model_name: "gpt-4o",
        operation: "chat",
        override_kind: "EXACT_PRICE",
        billing_price_id: freeExactPrice.id,
        multiplier_numerator: null,
        multiplier_denominator: null,
        enabled: false,
      },
      {
        id: "wild-discount",
        billing_account_id: acctA,
        model_name: "*",
        operation: "*",
        override_kind: "MULTIPLIER",
        billing_price_id: null,
        multiplier_numerator: "50",
        multiplier_denominator: "100",
        enabled: true,
      },
      {
        id: "free-exact",
        billing_account_id: acctB,
        model_name: "gpt-4o",
        operation: "chat",
        override_kind: "EXACT_PRICE",
        billing_price_id: freeExactPrice.id,
        multiplier_numerator: null,
        multiplier_denominator: null,
        enabled: true,
      },
    ],
  );

  const disabledSkipped = resolveEffectiveCustomerPrice(snap, acctA, "gpt-4o", "chat", basePrice);
  expect(disabledSkipped.overrideId).toBe("wild-discount");
  expect(disabledSkipped.price.rates.input_tokens).toEqual({ numerator: "5", denominator: "1" });

  const zeroMultiplied = resolveEffectiveCustomerPrice(
    snap,
    acctA,
    "gpt-4o",
    "chat",
    zeroBasePrice,
  );
  expect(zeroMultiplied.overrideId).toBe("wild-discount");
  expect(zeroMultiplied.price.rates.input_tokens).toEqual({ numerator: "0", denominator: "1" });
  expect(zeroMultiplied.price.rates.output_tokens).toEqual({ numerator: "0", denominator: "1" });

  const exactFree = resolveEffectiveCustomerPrice(snap, acctB, "gpt-4o", "chat", basePrice);
  expect(exactFree.overrideId).toBe("free-exact");
  expect(exactFree.price.rates.input_tokens).toEqual({ numerator: "0", denominator: "1" });

  const otherOp = resolveEffectiveCustomerPrice(snap, acctB, "gpt-4o", "responses", basePrice);
  expect(otherOp.overrideId).toBeNull();
  expect(otherOp.price.id).toBe(basePrice.id);
});

test("symptom: resolveEffectiveCustomerPrice falls through to lower-priority wildcard override when higher-priority override is invalid or overflows", () => {
  const acct = "00000000-0000-4000-8000-0000000000c3";
  const baseUsd = makeBasePrice("30000000-0000-4000-8000-000000000001", "USD");
  const eurExact = makeBasePrice("30000000-0000-4000-8000-000000000002", "EUR");
  const supplierSide: PriceVersionRecord = {
    ...baseUsd,
    id: "30000000-0000-4000-8000-000000000003",
    side: "SUPPLIER",
  };
  const hugeBase: PriceVersionRecord = {
    ...baseUsd,
    id: "30000000-0000-4000-8000-000000000004",
    rates: {
      input_tokens: { numerator: `1${"0".repeat(37)}`, denominator: "1" },
    },
  };

  const snap = makeSnap(
    [baseUsd, eurExact, supplierSide, hugeBase],
    [
      {
        id: "wild-half-off",
        billing_account_id: acct,
        model_name: "*",
        operation: "*",
        override_kind: "MULTIPLIER",
        billing_price_id: null,
        multiplier_numerator: "50",
        multiplier_denominator: "100",
        enabled: true,
      },
      {
        id: "bad-currency-exact",
        billing_account_id: acct,
        model_name: "model-eur",
        operation: "chat",
        override_kind: "EXACT_PRICE",
        billing_price_id: eurExact.id,
        multiplier_numerator: null,
        multiplier_denominator: null,
        enabled: true,
      },
      {
        id: "bad-supplier-exact",
        billing_account_id: acct,
        model_name: "model-supplier",
        operation: "chat",
        override_kind: "EXACT_PRICE",
        billing_price_id: supplierSide.id,
        multiplier_numerator: null,
        multiplier_denominator: null,
        enabled: true,
      },
      {
        id: "missing-exact",
        billing_account_id: acct,
        model_name: "model-missing",
        operation: "chat",
        override_kind: "EXACT_PRICE",
        billing_price_id: "30000000-0000-4000-8000-999999999999",
        multiplier_numerator: null,
        multiplier_denominator: null,
        enabled: true,
      },
      {
        id: "overflow-mult",
        billing_account_id: acct,
        model_name: "model-overflow",
        operation: "chat",
        override_kind: "MULTIPLIER",
        billing_price_id: null,
        multiplier_numerator: "100",
        multiplier_denominator: "1",
        enabled: true,
      },
    ],
  );

  for (const [modelName, base] of [
    ["model-eur", baseUsd],
    ["model-supplier", baseUsd],
    ["model-missing", baseUsd],
    ["model-overflow", hugeBase],
  ] as const) {
    const resolved = resolveEffectiveCustomerPrice(snap, acct, modelName, "chat", base);
    expect(resolved.overrideId, modelName).toBeNull();
    expect(resolved.price.id, modelName).toBe(base.id);
  }
});

test("key spend rollover obeys UTC midnight, leap-year February, year boundary, and no-op within active window", () => {
  const dayStart = Date.UTC(2028, 1, 29, 0, 0, 0);
  const beforeMidnight = Date.UTC(2028, 1, 29, 23, 59, 59, 999);
  const afterMidnight = Date.UTC(2028, 2, 1, 0, 0, 0, 0);

  const dailyRow = {
    id: "k-leap",
    billing_account_id: "p1",
    spend_limit_atoms: 1000n,
    spend_limit_window: "DAILY" as const,
    spend_window_start_ms: dayStart,
    spent_atoms: 700n,
    reserved_atoms: 100n,
    dirty: false,
  };
  rolloverKeySpendInMemory(dailyRow, beforeMidnight);
  expect(dailyRow.spent_atoms).toBe(700n);
  expect(dailyRow.dirty).toBe(false);

  rolloverKeySpendInMemory(dailyRow, afterMidnight);
  expect(dailyRow.spent_atoms).toBe(0n);
  expect(dailyRow.reserved_atoms).toBe(100n);
  expect(dailyRow.spend_window_start_ms).toBe(afterMidnight);
  expect(dailyRow.dirty).toBe(true);

  const decRow = {
    id: "k-year",
    billing_account_id: "p1",
    spend_limit_atoms: 0n,
    spend_limit_window: "MONTHLY" as const,
    spend_window_start_ms: Date.UTC(2026, 11, 1, 0, 0, 0),
    spent_atoms: 500n,
    reserved_atoms: 25n,
    dirty: false,
  };
  rolloverKeySpendInMemory(decRow, Date.UTC(2026, 11, 31, 23, 59, 59, 999));
  expect(decRow.spent_atoms).toBe(500n);
  expect(decRow.dirty).toBe(false);

  rolloverKeySpendInMemory(decRow, Date.UTC(2027, 0, 1, 0, 0, 0, 0));
  expect(decRow.spent_atoms).toBe(0n);
  expect(decRow.reserved_atoms).toBe(25n);
  expect(decRow.spend_window_start_ms).toBe(Date.UTC(2027, 0, 1, 0, 0, 0));
  expect(decRow.dirty).toBe(true);
});

test("symptom: snapshotSupportsHostedTools fails closed on passthrough, custom/mismatched usage_dialect, and WASM lacking extract_usage or stream hook", () => {
  const custPrice: PriceVersionRecord = {
    ...makeBasePrice("40000000-0000-4000-8000-000000000001"),
    rates: {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "2", denominator: "1" },
      web_search_calls: { numerator: "500", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", output_tokens: "1000", web_search_calls: "3" },
  };
  const supPrice: PriceVersionRecord = {
    ...custPrice,
    id: "40000000-0000-4000-8000-000000000002",
    side: "SUPPLIER",
  };
  const buildSnapForPlugin = (
    pluginId: string,
    pluginKind: "DECLARATIVE" | "WASM",
    providerConfig: NonNullable<
      ConfigSnapshot["plugins"] extends Map<string, infer V> ? V : never
    >["provider_config"],
    channelAliases?: { target: "web_search_calls"; source_path: string }[],
  ): ConfigSnapshot => {
    const snap = makeSnap([custPrice, supPrice], []);
    snap.plugins.set(pluginId, {
      id: pluginId,
      name: `prov-${pluginId}`,
      version: "1.0.0",
      digest: "a".repeat(64),
      capability: "PROVIDER",
      kind: pluginKind,
      enabled: true,
      builtin: false,
      manifest: {},
      provider_config: providerConfig,
      wasm_bytes: null,
      webhook_config: null,
      signature: null,
      signing_key_id: null,
    });
    snap.channels.set("ch-1", {
      id: "ch-1",
      account_id: "acc-1",
      kind: "custom",
      provider_plugin_id: pluginId,
      name: "ch-1",
      base_url: "https://example.test",
      secret_ciphertext: "enc",
      enabled: true,
      rpm: 100,
      concurrency: 10,
      routing_policy: {},
      adapter_config: channelAliases ? { usage_aliases: channelAliases } : {},
      config_revision: 1,
    });
    snap.routesByModel.set("m-ws", [
      {
        id: "r-1",
        model_name: "m-ws",
        channel_id: "ch-1",
        upstream_model: "m-ws",
        priority: 1,
        weight: 1,
        enabled: true,
        operations: ["responses"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        retry_policy: {},
        plugin_pipeline: [],
        adapter_overrides: {},
        request_policy: {},
      },
    ]);
    return snap;
  };

  const tools = new Set<"web_search_calls">(["web_search_calls"]);

  const passthroughSnap = buildSnapForPlugin("p-pass", "DECLARATIVE", {
    base_dialect: "passthrough",
    usage_dialect: "openai",
    auth: { scheme: "bearer" },
    endpoints: { default: { path: "/v1/responses" } },
  });
  expect(snapshotSupportsHostedTools(passthroughSnap, "m-ws", "responses", tools, false)).toBe(
    false,
  );

  const customDialectSnap = buildSnapForPlugin("p-custom", "DECLARATIVE", {
    base_dialect: "openai_responses",
    usage_dialect: "custom",
    auth: { scheme: "bearer" },
    endpoints: { responses: { path: "/v1/responses" } },
  });
  expect(snapshotSupportsHostedTools(customDialectSnap, "m-ws", "responses", tools, false)).toBe(
    false,
  );

  const customWithAliasSnap = buildSnapForPlugin(
    "p-custom-alias",
    "DECLARATIVE",
    {
      base_dialect: "openai_responses",
      usage_dialect: "custom",
      auth: { scheme: "bearer" },
      endpoints: { responses: { path: "/v1/responses" } },
    },
    [{ target: "web_search_calls", source_path: "usage.searches" }],
  );
  expect(snapshotSupportsHostedTools(customWithAliasSnap, "m-ws", "responses", tools, false)).toBe(
    true,
  );

  const mismatchedDialectSnap = buildSnapForPlugin("p-mismatch", "DECLARATIVE", {
    base_dialect: "openai_responses",
    usage_dialect: "anthropic",
    auth: { scheme: "bearer" },
    endpoints: { responses: { path: "/v1/responses" } },
  });
  expect(
    snapshotSupportsHostedTools(mismatchedDialectSnap, "m-ws", "responses", tools, false),
  ).toBe(false);

  const wasmNoGrantSnap = buildSnapForPlugin("p-wasm-nogrant", "WASM", {
    base_dialect: "openai_responses",
    usage_dialect: "openai",
    auth: { scheme: "bearer" },
    hooks: ["adapt_request", "adapt_response", "adapt_stream_event"],
  });
  expect(snapshotSupportsHostedTools(wasmNoGrantSnap, "m-ws", "responses", tools, false)).toBe(
    false,
  );

  const wasmUnaryOnlySnap = buildSnapForPlugin("p-wasm-unary", "WASM", {
    base_dialect: "openai_responses",
    usage_dialect: "openai",
    auth: { scheme: "bearer" },
    hooks: ["adapt_request", "adapt_response"],
    grants: ["extract_usage"],
  });
  expect(snapshotSupportsHostedTools(wasmUnaryOnlySnap, "m-ws", "responses", tools, false)).toBe(
    true,
  );
  expect(snapshotSupportsHostedTools(wasmUnaryOnlySnap, "m-ws", "responses", tools, true)).toBe(
    false,
  );

  const wasmFullSnap = buildSnapForPlugin("p-wasm-full", "WASM", {
    base_dialect: "openai_responses",
    usage_dialect: "openai",
    auth: { scheme: "bearer" },
    hooks: ["adapt_request", "adapt_response", "adapt_stream_event"],
    grants: ["extract_usage"],
  });
  expect(snapshotSupportsHostedTools(wasmFullSnap, "m-ws", "responses", tools, true)).toBe(true);
});
