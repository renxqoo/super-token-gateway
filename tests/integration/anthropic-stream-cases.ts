import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { db, fakeUpstream, internalCall, makeConfig, setup } from "./smart-routing-fixture.ts";
import { buildRealMatchWasm } from "./gateway-wasm-fixture.ts";

const ceilings = {
  input_tokens: "100",
  output_tokens: "100",
  cached_input_tokens: "100",
};
const customerRates = {
  input_tokens: { numerator: "2", denominator: "1" },
  output_tokens: { numerator: "3", denominator: "1" },
  cached_input_tokens: { numerator: "1", denominator: "1" },
};
const supplierRates = {
  input_tokens: { numerator: "1", denominator: "1" },
  output_tokens: { numerator: "2", denominator: "1" },
  cached_input_tokens: { numerator: "1", denominator: "1" },
};

const ttlCeilings = {
  ...ceilings,
  cache_write_5m_tokens: "100",
  cache_write_1h_tokens: "100",
};

const ttlPrices = {
  customer: {
    ceilings: ttlCeilings,
    rates: {
      ...customerRates,
      cache_write_5m_tokens: { numerator: "4", denominator: "1" },
      cache_write_1h_tokens: { numerator: "5", denominator: "1" },
    },
  },
  supplier: {
    ceilings: ttlCeilings,
    rates: {
      ...supplierRates,
      cache_write_5m_tokens: { numerator: "2", denominator: "1" },
      cache_write_1h_tokens: { numerator: "3", denominator: "1" },
    },
  },
};

const aggregateCeilings = { ...ceilings, cache_write_tokens: "100" };
const aggregatePrices = {
  customer: {
    ceilings: aggregateCeilings,
    rates: { ...customerRates, cache_write_tokens: { numerator: "3", denominator: "1" } },
  },
  supplier: {
    ceilings: aggregateCeilings,
    rates: { ...supplierRates, cache_write_tokens: { numerator: "2", denominator: "1" } },
  },
};

function anthropicStream(finalUsage: Record<string, unknown>, reportCacheWrites = true) {
  const events = [
    {
      type: "message_start",
      message: {
        id: "msg_cache",
        type: "message",
        role: "assistant",
        model: "m",
        content: [],
        stop_reason: null,
        usage: {
          input_tokens: 8,
          output_tokens: 1,
          cache_read_input_tokens: 5,
          ...(reportCacheWrites
            ? {
                cache_creation_input_tokens: 7,
                cache_creation: { ephemeral_5m_input_tokens: 3, ephemeral_1h_input_tokens: 4 },
              }
            : {}),
        },
      },
    },
    { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
    {
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text: "anthropic answer" },
    },
    { type: "content_block_stop", index: 0 },
    { type: "message_delta", delta: { stop_reason: "end_turn" }, usage: finalUsage },
    { type: "message_stop" },
  ];
  return events.map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join("");
}

export function registerAnthropicStreamCases() {
  test("WASM reports individually valid cache counters whose combined values must be checked before charging", async () => {
    const hooks = ["adapt_request", "adapt_stream_event"];
    const input = { model: "m", stream: true, messages: [{ role: "user", content: "Hello" }] };
    const wasm = buildRealMatchWasm(
      hooks,
      "final-cache",
      { done: true, usage: { output_tokens: 4, cache_write_5m_tokens: 4 } },
      {
        path: "/v1/chat/completions",
        body: input,
        chunks: [{ id: "wasm-cache", choices: [{ index: 0, delta: { content: "wasm answer" } }] }],
        usage: { input_tokens: 3, cache_write_tokens: 2 },
      },
    );
    const upstream = fakeUpstream("split-wasm-cache", {
      streamFrames: 'data: {"phase":"start"}\n\ndata: {"phase":"final-cache"}\n\n',
    });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const created = await internalCall(runtime.internalApp, "PUT", "/internal/v1/plugins", {
        name: "split-wasm-cache",
        version: "1.0.0",
        kind: "WASM",
        capability: "PROVIDER",
        wasm_bytes_base64: Buffer.from(wasm).toString("base64"),
        provider_config: {
          base_dialect: "openai_chat",
          usage_dialect: "custom",
          auth: { scheme: "bearer" },
          hooks,
          grants: ["extract_usage"],
        },
      });
      expect(created.status, JSON.stringify(created.data)).toBe(200);
      const id = String(created.data.id);
      const released = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/plugin-releases",
        {
          id: crypto.randomUUID(),
          expected_epoch: runtime.snapshot.get().epoch,
          targets: [{ plugin_id: id }],
          samples: {
            [id]: { request: input, event: { name: "message", data: '{"phase":"final-cache"}' } },
          },
        },
      );
      expect(released.status, JSON.stringify(released.data)).toBe(200);
      const fixture = await setup(runtime, [upstream], {
        prices: aggregatePrices,
        providerPluginId: id,
      });
      try {
        const response = await fixture.chat(true);
        expect(response.status).toBe(200);
        const wire = await response.text();
        expect(wire).toContain('"content":"wasm answer"');
        expect(wire).toContain('"code":"usage_alias_conflict"');
        expect(wire).not.toContain("data: [DONE]");
        expect(upstream.calls()).toBe(1);
        expect(await fixture.charges()).toEqual({ n: 0, u: 0, atoms: "0" });
        expect(await fixture.holdsByState()).toEqual({ UNKNOWN: 1 });
      } finally {
        await fixture.resolveUnknownHolds();
      }
    } finally {
      upstream.server.stop(true);
      await runtime.close();
    }
  });

  test.each([
    ["consistent", 4],
    ["conflicting", 7],
  ] as const)(
    "SSE combines %s provider usage aliases from different events before settling",
    async (scenario, ttlTokens) => {
      const frames = [
        {
          id: "alias",
          choices: [{ index: 0, delta: { content: "aliased answer" }, finish_reason: null }],
          meter: { cache_total: 6 },
        },
        {
          id: "alias",
          choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
          usage: { prompt_tokens: 10, completion_tokens: 5 },
          meter: { cache_5m: ttlTokens },
        },
      ];
      const upstream = fakeUpstream("split-cache-aliases", {
        streamFrames:
          frames.map((frame) => `data: ${JSON.stringify(frame)}\n\n`).join("") + "data: [DONE]\n\n",
      });
      const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
      try {
        await runtime.start();
        const fixture = await setup(runtime, [upstream], {
          prices: aggregatePrices,
          adapterOverrides: {
            usage_aliases: [
              { target: "cache_write_tokens", source_path: "meter.cache_total" },
              { target: "cache_write_5m_tokens", source_path: "meter.cache_5m" },
            ],
          },
        });
        try {
          const response = await fixture.chat(true);
          expect(response.status).toBe(200);
          const wire = await response.text();
          expect(wire).toContain('"content":"aliased answer"');
          if (scenario === "conflicting") {
            expect(wire).toContain('"code":"usage_alias_conflict"');
            expect(wire).not.toContain("data: [DONE]");
            expect(await fixture.charges()).toEqual({ n: 0, u: 0, atoms: "0" });
            expect(await fixture.holdsByState()).toEqual({ UNKNOWN: 1 });
          } else {
            expect(wire.trimEnd()).toEndWith("data: [DONE]");
            const charge = await db`
              SELECT usage, customer_atoms::text, supplier_cost_atoms::text
              FROM public.billing_charges WHERE request_id = ${response.headers.get("x-request-id")}
            `;
            expect([...charge]).toEqual([
              {
                usage: {
                  input_tokens: "10",
                  output_tokens: "5",
                  cache_write_tokens: "6",
                  cache_write_5m_tokens: "4",
                },
                customer_atoms: "53",
                supplier_cost_atoms: "32",
              },
            ]);
          }
        } finally {
          await fixture.resolveUnknownHolds();
        }
      } finally {
        upstream.server.stop(true);
        await runtime.close();
      }
    },
  );

  for (const scenario of [
    {
      name: "converted Anthropic SSE retains opening cache counts when the final delta reports only output",
      finalUsage: { output_tokens: 9 },
      promptTokens: 20,
      cachedTokens: 5,
      charge: {
        usage: {
          input_tokens: "8",
          output_tokens: "9",
          cached_input_tokens: "5",
          cache_write_tokens: "7",
          cache_write_5m_tokens: "3",
          cache_write_1h_tokens: "4",
        },
        customer_atoms: "80",
        supplier_cost_atoms: "49",
      },
    },
    {
      name: "converted Anthropic SSE replaces opening cache counts with the final delta including an explicit zero",
      finalUsage: {
        output_tokens: 9,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 6,
        cache_creation: { ephemeral_5m_input_tokens: 4, ephemeral_1h_input_tokens: 2 },
      },
      promptTokens: 14,
      cachedTokens: 0,
      charge: {
        usage: {
          input_tokens: "8",
          output_tokens: "9",
          cached_input_tokens: "0",
          cache_write_tokens: "6",
          cache_write_5m_tokens: "4",
          cache_write_1h_tokens: "2",
        },
        customer_atoms: "69",
        supplier_cost_atoms: "40",
      },
    },
    {
      name: "converted Anthropic SSE uses the latest cache total with aggregate pricing and drops stale TTL details",
      finalUsage: { output_tokens: 9, cache_creation_input_tokens: 6 },
      prices: aggregatePrices,
      promptTokens: 19,
      cachedTokens: 5,
      charge: {
        usage: {
          input_tokens: "8",
          output_tokens: "9",
          cached_input_tokens: "5",
          cache_write_tokens: "6",
        },
        customer_atoms: "66",
        supplier_cost_atoms: "43",
      },
    },
    {
      name: "symptom: an aggregate tariff finalizes an undercharged stream with only one cache write TTL detail",
      finalUsage: { output_tokens: 9, cache_creation: { ephemeral_5m_input_tokens: 4 } },
      prices: aggregatePrices,
      error: "invalid_upstream_usage",
    },
    {
      name: "converted Anthropic SSE settles aggregate pricing from both TTL details without a reported total",
      finalUsage: {
        output_tokens: 9,
        cache_creation: { ephemeral_5m_input_tokens: 4, ephemeral_1h_input_tokens: 2 },
      },
      prices: aggregatePrices,
      promptTokens: 19,
      cachedTokens: 5,
      charge: {
        usage: {
          input_tokens: "8",
          output_tokens: "9",
          cached_input_tokens: "5",
          cache_write_tokens: "6",
          cache_write_5m_tokens: "4",
          cache_write_1h_tokens: "2",
        },
        customer_atoms: "66",
        supplier_cost_atoms: "43",
      },
    },
    {
      name: "symptom: a cache total without customer TTL details finalizes an undercharged Anthropic stream",
      finalUsage: { output_tokens: 9, cache_creation_input_tokens: 6 },
      prices: { customer: ttlPrices.customer, supplier: aggregatePrices.supplier },
      error: "invalid_upstream_usage",
    },
    {
      name: "symptom: one missing supplier TTL detail finalizes an underpriced Anthropic stream",
      finalUsage: {
        output_tokens: 9,
        cache_creation_input_tokens: 6,
        cache_creation: { ephemeral_5m_input_tokens: 4 },
      },
      prices: { customer: aggregatePrices.customer, supplier: ttlPrices.supplier },
      error: "invalid_upstream_usage",
    },
    {
      name: "symptom: an aggregate tariff settles a stream whose only cache TTL detail exceeds the total",
      finalUsage: {
        output_tokens: 9,
        cache_creation_input_tokens: 2,
        cache_creation: { ephemeral_5m_input_tokens: 4 },
      },
      prices: aggregatePrices,
      error: "usage_alias_conflict",
    },
    {
      name: "converted Anthropic SSE settles an aggregate tariff when a single TTL detail is within the total",
      finalUsage: {
        output_tokens: 9,
        cache_creation_input_tokens: 6,
        cache_creation: { ephemeral_5m_input_tokens: 4 },
      },
      prices: aggregatePrices,
      promptTokens: 19,
      cachedTokens: 5,
      charge: {
        usage: {
          input_tokens: "8",
          output_tokens: "9",
          cached_input_tokens: "5",
          cache_write_tokens: "6",
          cache_write_5m_tokens: "4",
        },
        customer_atoms: "66",
        supplier_cost_atoms: "43",
      },
    },
    {
      name: "converted Anthropic SSE reports conflicting cache totals and keeps the hold UNKNOWN",
      finalUsage: {
        output_tokens: 9,
        cache_creation_input_tokens: 7,
        cache_creation: { ephemeral_5m_input_tokens: 4, ephemeral_1h_input_tokens: 2 },
      },
      error: "usage_alias_conflict",
    },
    {
      name: "converted Anthropic SSE settles when a new cache total explicitly reports zero writes",
      finalUsage: { output_tokens: 9, cache_creation_input_tokens: 0 },
      promptTokens: 13,
      cachedTokens: 5,
      charge: {
        usage: {
          input_tokens: "8",
          output_tokens: "9",
          cached_input_tokens: "5",
          cache_write_tokens: "0",
        },
        customer_atoms: "48",
        supplier_cost_atoms: "31",
      },
    },
    {
      name: "converted Anthropic SSE can settle a response that reports no cache writes throughout",
      finalUsage: { output_tokens: 9 },
      reportCacheWrites: false,
      promptTokens: 13,
      cachedTokens: 5,
      charge: {
        usage: { input_tokens: "8", output_tokens: "9", cached_input_tokens: "5" },
        customer_atoms: "48",
        supplier_cost_atoms: "31",
      },
    },
  ] as const) {
    test(scenario.name, async () => {
      const upstream = fakeUpstream("anthropic-cache", {
        streamFrames: anthropicStream(
          scenario.finalUsage,
          "reportCacheWrites" in scenario ? scenario.reportCacheWrites : undefined,
        ),
      });
      const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
      try {
        await runtime.start();
        const fixture = await setup(runtime, [upstream], {
          providerKind: "anthropic",
          prices: "prices" in scenario ? scenario.prices : ttlPrices,
        });
        try {
          const response = await fixture.chat(true);
          expect(response.status).toBe(200);
          const wire = await response.text();
          expect(wire).toContain('"content":"anthropic answer"');
          const charges = await db`
            SELECT usage, customer_atoms::text, supplier_cost_atoms::text
            FROM public.billing_charges WHERE billing_account_id = ${fixture.billingAccountId}
          `;
          if ("error" in scenario) {
            expect(charges).toEqual([]);
            expect(wire).toContain("event: error");
            expect(wire).toContain(`"code":"${scenario.error}"`);
            expect(wire).not.toContain("data: [DONE]");
            expect(await fixture.holdsByState()).toEqual({ UNKNOWN: 1 });
          } else {
            const frames = wire.trimEnd().split("\n\n");
            expect(frames.at(-1)).toBe("data: [DONE]");
            expect(frames.at(-3)).toContain('"finish_reason":"stop"');
            expect(JSON.parse(frames.at(-2)!.slice("data: ".length))).toMatchObject({
              choices: [],
              usage: {
                prompt_tokens: scenario.promptTokens,
                completion_tokens: 9,
                total_tokens: scenario.promptTokens + 9,
                prompt_tokens_details: { cached_tokens: scenario.cachedTokens },
              },
            });
            expect(charges).toEqual([scenario.charge]);
            expect(await fixture.holdsByState()).toEqual({ SETTLED: 1 });
          }
        } finally {
          await fixture.resolveUnknownHolds();
        }
      } finally {
        upstream.server.stop(true);
        await runtime.close();
      }
    });
  }

  test("symptom: an Anthropic JSON response with missing priced cache TTL usage returns success", async () => {
    const upstream = fakeUpstream("anthropic-cache-json", {
      unaryBody: {
        id: "msg_cache",
        type: "message",
        role: "assistant",
        model: "m",
        content: [{ type: "text", text: "anthropic answer" }],
        stop_reason: "end_turn",
        usage: { input_tokens: 8, output_tokens: 9, cache_creation_input_tokens: 6 },
      },
    });
    const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
    try {
      await runtime.start();
      const fixture = await setup(runtime, [upstream], {
        providerKind: "anthropic",
        prices: ttlPrices,
      });
      try {
        const response = await fixture.chat();
        expect(response.status).toBe(502);
        expect(await response.json()).toMatchObject({ error: { code: "invalid_upstream_usage" } });
        expect(await fixture.charges()).toEqual({ n: 0, u: 0, atoms: "0" });
        expect(await fixture.holdsByState()).toEqual({ UNKNOWN: 1 });
      } finally {
        await fixture.resolveUnknownHolds();
      }
    } finally {
      upstream.server.stop(true);
      await runtime.close();
    }
  });
}
