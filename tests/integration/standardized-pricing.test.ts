import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { db, internalCall, makeConfig, registerGatewayFixture } from "./gateway-fixture.ts";
import { registerStandardizedPricingFailoverRbacCases } from "./standardized-pricing-failover-rbac-cases.ts";
import { registerStandardizedPricingFxRolesCases } from "./standardized-pricing-fx-roles-cases.ts";
import { registerStandardizedPricingStreamToolsCases } from "./standardized-pricing-stream-tools-cases.ts";

registerGatewayFixture();
registerStandardizedPricingFxRolesCases();
registerStandardizedPricingStreamToolsCases();
registerStandardizedPricingFailoverRbacCases();

async function adminCall(
  runtime: ReturnType<typeof createGatewayRuntime>,
  method: string,
  path: string,
  body?: unknown,
  expectedStatus?: number,
) {
  const res = await internalCall(runtime.internalApp, method, path, body);
  const want =
    expectedStatus ??
    (method === "POST" && (path === "/internal/v1/prices" || path === "/internal/v1/fx-rates")
      ? 201
      : 200);
  expect(res.status, `${method} ${path} -> ${JSON.stringify(res.data)}`).toBe(want);
  return res.data;
}

test("Standardized pricing E2E: CLIFF >200K + reasoning_rate + PROGRESSIVE + dry-run simulator parity", async () => {
  let nextUsage: Record<string, unknown> = {
    prompt_tokens: 1000,
    completion_tokens: 200,
    completion_tokens_details: { reasoning_tokens: 50 },
  };
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      await req.text();
      return Response.json({
        id: "chatcmpl-pricing",
        object: "chat.completion",
        model: "gpt-cliff",
        choices: [
          { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
        ],
        usage: nextUsage,
      });
    },
  });

  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const billingAccount = await adminCall(runtime, "PUT", "/internal/v1/billing-accounts", {
      name: "cliff-account",
      currency: "USD",
      rpm: 1000,
      tpm: 10_000_000,
      concurrency: 50,
    });
    await adminCall(runtime, "POST", `/internal/v1/wallets/${billingAccount.id}/credits`, {
      currency: "USD",
      external_ref: "cliff-fund-1",
      amount_atoms: "1000000000",
      kind: "CASH",
    });
    const key = await adminCall(runtime, "PUT", "/internal/v1/api-keys", {
      billing_account_id: billingAccount.id,
      name: "cliff-key",
    });
    const account = await adminCall(runtime, "PUT", "/internal/v1/upstream-accounts", {
      name: `cliff-sup-${crypto.randomUUID()}`,
    });
    await adminCall(runtime, "POST", `/internal/v1/upstream-accounts/${account.id}/recharges`, {
      currency: "USD",
      external_ref: "cliff-sup-fund-1",
      amount_atoms: "1000000000",
    });
    const channel = await adminCall(runtime, "PUT", "/internal/v1/channels", {
      account_id: account.id,
      kind: "openai",
      name: "cliff-chan",
      base_url: upstream.url.origin,
      api_key: "sk-cliff",
    });
    await adminCall(runtime, "PUT", "/internal/v1/models", {
      name: "gpt-cliff",
      operations: ["chat"],
      max_output_tokens: 4096,
    });
    await adminCall(runtime, "PUT", "/internal/v1/models", {
      name: "gpt-prog",
      operations: ["chat"],
      max_output_tokens: 4096,
    });

    const cliffCustomerPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
      side: "CUSTOMER",
      currency: "USD",
      rates: {},
      ceilings: {
        input_tokens: "500000",
        cached_input_tokens: "500000",
        cache_write_tokens: "500000",
        output_tokens: "4096",
      },
      pricing_spec: {
        cliff: {
          driver: ["input_tokens", "cached_input_tokens", "cache_write_tokens"],
          tiers: [
            {
              min_driver: "0",
              rates: {
                input_tokens: { numerator: "2", denominator: "1" },
                cached_input_tokens: { numerator: "1", denominator: "2" },
                cache_write_tokens: { numerator: "5", denominator: "2" },
                output_tokens: { numerator: "10", denominator: "1" },
              },
              reasoning_rate: { numerator: "20", denominator: "1" },
            },
            {
              min_driver: "200000",
              rates: {
                input_tokens: { numerator: "4", denominator: "1" },
                cached_input_tokens: { numerator: "1", denominator: "1" },
                cache_write_tokens: { numerator: "5", denominator: "1" },
                output_tokens: { numerator: "15", denominator: "1" },
              },
              reasoning_rate: { numerator: "30", denominator: "1" },
            },
          ],
        },
      },
    });
    const cliffSupplierPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
      side: "SUPPLIER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        cached_input_tokens: { numerator: "1", denominator: "4" },
        cache_write_tokens: { numerator: "5", denominator: "4" },
        output_tokens: { numerator: "5", denominator: "1" },
      },
      ceilings: {
        input_tokens: "500000",
        cached_input_tokens: "500000",
        cache_write_tokens: "500000",
        output_tokens: "4096",
      },
    });
    await adminCall(runtime, "PUT", "/internal/v1/routes", {
      model_name: "gpt-cliff",
      channel_id: channel.id,
      upstream_model: "gpt-cliff",
      operations: ["chat"],
      billing_price_id: cliffCustomerPrice.id,
      cost_version_id: cliffSupplierPrice.id,
    });

    const progCustomerPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
      side: "CUSTOMER",
      currency: "USD",
      rates: { output_tokens: { numerator: "5", denominator: "1" } },
      ceilings: { input_tokens: "100000", output_tokens: "4096" },
      pricing_spec: {
        progressive: {
          input_tokens: [
            { up_to: "1000", rate: { numerator: "10", denominator: "1" } },
            { up_to: "5000", rate: { numerator: "5", denominator: "1" } },
            { up_to: null, rate: { numerator: "2", denominator: "1" } },
          ],
        },
      },
    });
    await adminCall(runtime, "PUT", "/internal/v1/routes", {
      model_name: "gpt-prog",
      channel_id: channel.id,
      upstream_model: "gpt-prog",
      operations: ["chat"],
      billing_price_id: progCustomerPrice.id,
      cost_version_id: cliffSupplierPrice.id,
    });

    const simShort = await adminCall(runtime, "POST", "/internal/v1/pricing/simulate", {
      model: "gpt-cliff",
      operation: "chat",
      currency: "USD",
      billing_account_id: billingAccount.id,
      usage: { input_tokens: "1000", output_tokens: "200", reasoning_tokens: "50" },
      output_bound: 256,
      estimated_input_tokens: 1000,
    });
    expect((simShort.customer_breakdown as { total_atoms: string }).total_atoms).toBe("4500");
    expect((simShort.supplier_breakdown as { total_atoms: string }).total_atoms).toBe("2000");
    expect((simShort.margin as { gross_margin_atoms: string }).gross_margin_atoms).toBe("2500");

    const simLong = await adminCall(runtime, "POST", "/internal/v1/pricing/simulate", {
      model: "gpt-cliff",
      operation: "chat",
      currency: "USD",
      billing_account_id: billingAccount.id,
      usage: { input_tokens: "250000", output_tokens: "200", reasoning_tokens: "50" },
    });
    expect((simLong.customer_breakdown as { total_atoms: string }).total_atoms).toBe("1003750");

    const shortResp = await runtime.publicApp.fetch(
      new Request("http://public.local/v1/chat/completions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${key.key}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          model: "gpt-cliff",
          messages: [{ role: "user", content: "short prompt" }],
          max_tokens: 256,
        }),
      }),
    );
    expect(shortResp.status).toBe(200);
    const shortReqId = shortResp.headers.get("x-request-id")!;

    nextUsage = {
      prompt_tokens: 250000,
      completion_tokens: 200,
      completion_tokens_details: { reasoning_tokens: 50 },
    };
    const longResp = await runtime.publicApp.fetch(
      new Request("http://public.local/v1/chat/completions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${key.key}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          model: "gpt-cliff",
          messages: [{ role: "user", content: "x".repeat(900_000) }],
          max_tokens: 256,
        }),
      }),
    );
    expect(longResp.status).toBe(200);
    const longReqId = longResp.headers.get("x-request-id")!;

    nextUsage = { prompt_tokens: 6000, completion_tokens: 100 };
    const progResp = await runtime.publicApp.fetch(
      new Request("http://public.local/v1/chat/completions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${key.key}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          model: "gpt-prog",
          messages: [{ role: "user", content: "progressive test" }],
          max_tokens: 256,
        }),
      }),
    );
    expect(progResp.status).toBe(200);
    const progReqId = progResp.headers.get("x-request-id")!;

    const charges = await db<
      {
        request_id: string;
        customer_atoms: string;
        supplier_cost_atoms: string;
        pricing_snapshot: { customer?: { rate_card_id: string } } | null;
      }[]
    >`
      SELECT request_id::text, customer_atoms::text, supplier_cost_atoms::text, pricing_snapshot
      FROM public.billing_charges
      WHERE request_id = ANY(${[shortReqId, longReqId, progReqId]}::uuid[])
    `;
    const byReq = new Map(charges.map((c) => [c.request_id, c]));
    expect(byReq.get(shortReqId)!.customer_atoms).toBe("4500");
    expect(byReq.get(shortReqId)!.supplier_cost_atoms).toBe("2000");
    expect(byReq.get(shortReqId)!.pricing_snapshot?.customer?.rate_card_id).toBe(
      cliffCustomerPrice.id as string,
    );
    expect(byReq.get(longReqId)!.customer_atoms).toBe("1003750");
    expect(byReq.get(progReqId)!.customer_atoms).toBe("32500");

    const audit = await runtime.ledger.auditConservation();
    expect(audit.ok, JSON.stringify(audit)).toBe(true);
  } finally {
    await runtime.close();
    upstream.stop(true);
  }
});

test("Standardized pricing E2E: Media MATRIX + Quantizer + min_charge_atoms + unpriced matrix fail-closed", async () => {
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      if (url.pathname.endsWith("/images/generations")) {
        const body = (await req.json()) as { n?: number };
        const n = body.n ?? 1;
        return Response.json({
          created: 1,
          data: Array.from({ length: n }, (_, i) => ({
            url: `https://example.com/image-${i}.png`,
          })),
        });
      }
      if (url.pathname.endsWith("/audio/transcriptions")) {
        return Response.json({
          text: "hello world",
          usage: { type: "duration", seconds: 16.2 },
        });
      }
      return new Response("not found", { status: 404 });
    },
  });

  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const billingAccount = await adminCall(runtime, "PUT", "/internal/v1/billing-accounts", {
      name: "media-account",
      currency: "USD",
      rpm: 1000,
      tpm: 10_000_000,
      concurrency: 50,
    });
    await adminCall(runtime, "POST", `/internal/v1/wallets/${billingAccount.id}/credits`, {
      currency: "USD",
      external_ref: "media-fund-1",
      amount_atoms: "100000000",
      kind: "CASH",
    });
    const key = await adminCall(runtime, "PUT", "/internal/v1/api-keys", {
      billing_account_id: billingAccount.id,
      name: "media-key",
    });
    const account = await adminCall(runtime, "PUT", "/internal/v1/upstream-accounts", {
      name: `media-sup-${crypto.randomUUID()}`,
    });
    await adminCall(runtime, "POST", `/internal/v1/upstream-accounts/${account.id}/recharges`, {
      currency: "USD",
      external_ref: "media-sup-fund-1",
      amount_atoms: "100000000",
    });
    const channel = await adminCall(runtime, "PUT", "/internal/v1/channels", {
      account_id: account.id,
      kind: "openai",
      name: "media-chan",
      base_url: upstream.url.origin,
      api_key: "sk-media",
    });
    await adminCall(runtime, "PUT", "/internal/v1/models", {
      name: "gpt-image-matrix",
      operations: ["images"],
      max_output_tokens: 1,
    });
    await adminCall(runtime, "PUT", "/internal/v1/models", {
      name: "whisper-quant",
      operations: ["transcription"],
      max_output_tokens: 1,
    });

    const imageCustomerPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
      side: "CUSTOMER",
      currency: "USD",
      rates: {},
      ceilings: { images: "10" },
      pricing_spec: {
        matrix: [
          {
            match: { size: "1024x1024", quality: "standard" },
            rates: { images: { numerator: "4000", denominator: "1" } },
          },
          {
            match: { size: "1024x1024", quality: "hd" },
            rates: { images: { numerator: "8000", denominator: "1" } },
          },
          {
            match: { size: "1792x1024", quality: "hd" },
            rates: { images: { numerator: "12000", denominator: "1" } },
            min_charge_atoms: "15000",
          },
        ],
      },
    });
    const imageSupplierPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
      side: "SUPPLIER",
      currency: "USD",
      rates: { images: { numerator: "2000", denominator: "1" } },
      ceilings: { images: "10" },
    });
    await adminCall(runtime, "PUT", "/internal/v1/routes", {
      model_name: "gpt-image-matrix",
      channel_id: channel.id,
      upstream_model: "gpt-image-matrix",
      operations: ["images"],
      billing_price_id: imageCustomerPrice.id,
      cost_version_id: imageSupplierPrice.id,
    });

    const audioCustomerPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
      side: "CUSTOMER",
      currency: "USD",
      rates: { audio_seconds: { numerator: "100", denominator: "1" } },
      ceilings: { audio_seconds: "3600" },
      pricing_spec: {
        quantizers: {
          audio_seconds: { min_units: "15", step: "5" },
        },
      },
    });
    const audioSupplierPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
      side: "SUPPLIER",
      currency: "USD",
      rates: { audio_seconds: { numerator: "50", denominator: "1" } },
      ceilings: { audio_seconds: "3600" },
    });
    await adminCall(runtime, "PUT", "/internal/v1/routes", {
      model_name: "whisper-quant",
      channel_id: channel.id,
      upstream_model: "whisper-quant",
      operations: ["transcription"],
      billing_price_id: audioCustomerPrice.id,
      cost_version_id: audioSupplierPrice.id,
    });

    const imgHd = await runtime.publicApp.fetch(
      new Request("http://public.local/v1/images/generations", {
        method: "POST",
        headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "gpt-image-matrix",
          prompt: "cat",
          size: "1024x1024",
          quality: "hd",
          n: 2,
        }),
      }),
    );
    expect(imgHd.status).toBe(200);
    const imgHdReqId = imgHd.headers.get("x-request-id")!;

    const imgWide = await runtime.publicApp.fetch(
      new Request("http://public.local/v1/images/generations", {
        method: "POST",
        headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "gpt-image-matrix",
          prompt: "panorama",
          size: "1792x1024",
          quality: "hd",
          n: 1,
        }),
      }),
    );
    expect(imgWide.status).toBe(200);
    const imgWideReqId = imgWide.headers.get("x-request-id")!;

    const imgUnknown = await runtime.publicApp.fetch(
      new Request("http://public.local/v1/images/generations", {
        method: "POST",
        headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "gpt-image-matrix",
          prompt: "unknown",
          size: "9999x9999",
          quality: "ultra",
          n: 1,
        }),
      }),
    );
    expect(imgUnknown.status).toBe(422);

    const form = new FormData();
    form.append("model", "whisper-quant");
    form.append(
      "file",
      new File([new Uint8Array([1, 2, 3, 4])], "clip.wav", { type: "audio/wav" }),
    );
    const audioResp = await runtime.publicApp.fetch(
      new Request("http://public.local/v1/audio/transcriptions", {
        method: "POST",
        headers: { authorization: `Bearer ${key.key}` },
        body: form,
      }),
    );
    expect(audioResp.status).toBe(200);
    const audioReqId = audioResp.headers.get("x-request-id")!;

    const rows = await db<{ request_id: string; customer_atoms: string }[]>`
      SELECT request_id::text, customer_atoms::text
      FROM public.billing_charges
      WHERE request_id = ANY(${[imgHdReqId, imgWideReqId, audioReqId]}::uuid[])
    `;
    const byReq = new Map(rows.map((r) => [r.request_id, r.customer_atoms]));
    expect(byReq.get(imgHdReqId)).toBe("16000");
    expect(byReq.get(imgWideReqId)).toBe("15000");
    expect(byReq.get(audioReqId)).toBe("2000");

    const audit = await runtime.ledger.auditConservation();
    expect(audit.ok, JSON.stringify(audit)).toBe(true);
  } finally {
    await runtime.close();
    upstream.stop(true);
  }
});

test("Standardized pricing E2E: Modifiers (TOU + service_tier + customer_group_id + EXCLUSIVE) and hosted web_search_calls", async () => {
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      await req.text();
      if (url.pathname.endsWith("/responses")) {
        return Response.json({
          id: "resp_ws_1",
          object: "response",
          status: "completed",
          output: [
            {
              type: "message",
              role: "assistant",
              content: [{ type: "output_text", text: "searched result" }],
            },
          ],
          usage: { input_tokens: 100, output_tokens: 50, web_search_calls: 2 },
        });
      }
      return Response.json({
        id: "chatcmpl-mod",
        object: "chat.completion",
        model: "gpt-mod",
        choices: [
          { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
        ],
        usage: { prompt_tokens: 100, completion_tokens: 50 },
      });
    },
  });

  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const vipAccount = await adminCall(runtime, "PUT", "/internal/v1/billing-accounts", {
      name: "vip-mod-account",
      customer_group_id: "vip-enterprise",
      currency: "USD",
      rpm: 1000,
      tpm: 10_000_000,
      concurrency: 50,
    });
    await adminCall(runtime, "POST", `/internal/v1/wallets/${vipAccount.id}/credits`, {
      currency: "USD",
      external_ref: "vip-fund-1",
      amount_atoms: "100000000",
      kind: "CASH",
    });
    const key = await adminCall(runtime, "PUT", "/internal/v1/api-keys", {
      billing_account_id: vipAccount.id,
      name: "vip-key",
    });
    const account = await adminCall(runtime, "PUT", "/internal/v1/upstream-accounts", {
      name: `mod-sup-${crypto.randomUUID()}`,
    });
    await adminCall(runtime, "POST", `/internal/v1/upstream-accounts/${account.id}/recharges`, {
      currency: "USD",
      external_ref: "mod-sup-fund-1",
      amount_atoms: "100000000",
    });
    const channel = await adminCall(runtime, "PUT", "/internal/v1/channels", {
      account_id: account.id,
      kind: "openai",
      name: "mod-chan",
      base_url: upstream.url.origin,
      api_key: "sk-mod",
    });
    await adminCall(runtime, "PUT", "/internal/v1/models", {
      name: "gpt-mod",
      operations: ["chat", "responses"],
      max_output_tokens: 4096,
    });
    const baseCustPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
      side: "CUSTOMER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "10", denominator: "1" },
        output_tokens: { numerator: "20", denominator: "1" },
        web_search_calls: { numerator: "5000", denominator: "1" },
      },
      ceilings: {
        input_tokens: "10000",
        output_tokens: "4096",
        web_search_calls: "5",
      },
    });
    const baseSupPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
      side: "SUPPLIER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "5", denominator: "1" },
        output_tokens: { numerator: "10", denominator: "1" },
        web_search_calls: { numerator: "2500", denominator: "1" },
      },
      ceilings: {
        input_tokens: "10000",
        output_tokens: "4096",
        web_search_calls: "5",
      },
    });
    await adminCall(runtime, "PUT", "/internal/v1/routes", {
      model_name: "gpt-mod",
      channel_id: channel.id,
      upstream_model: "gpt-mod",
      operations: ["chat", "responses"],
      billing_price_id: baseCustPrice.id,
      cost_version_id: baseSupPrice.id,
    });

    await adminCall(runtime, "PUT", "/internal/v1/pricing-modifiers", {
      layer: "PRODUCT",
      kind: "MULTIPLIER",
      stacking: "MULTIPLY",
      priority: 10,
      match: { side: "CUSTOMER", model: "gpt-mod", service_tier: "priority" },
      multiplier_numerator: "200",
      multiplier_denominator: "100",
    });
    await adminCall(runtime, "PUT", "/internal/v1/pricing-modifiers", {
      layer: "PRODUCT",
      kind: "MULTIPLIER",
      stacking: "MULTIPLY",
      priority: 20,
      time_windows: [
        { timezone: "UTC", start_minute: 0, end_minute: 720 },
        { timezone: "UTC", start_minute: 720, end_minute: 0 },
      ],
      match: { side: "CUSTOMER", model: "gpt-mod", operation: "chat" },
      multiplier_numerator: "90",
      multiplier_denominator: "100",
    });
    await adminCall(runtime, "PUT", "/internal/v1/pricing-modifiers", {
      layer: "COMMERCIAL",
      kind: "MULTIPLIER",
      stacking: "BEST_FOR_CUSTOMER",
      priority: 10,
      match: { side: "CUSTOMER", model: "gpt-mod", customer_group_id: "vip-enterprise" },
      multiplier_numerator: "80",
      multiplier_denominator: "100",
    });
    await adminCall(runtime, "PUT", "/internal/v1/pricing-modifiers", {
      layer: "COMMERCIAL",
      kind: "MULTIPLIER",
      stacking: "BEST_FOR_CUSTOMER",
      priority: 20,
      match: { side: "CUSTOMER", model: "gpt-mod", billing_account_id: vipAccount.id },
      multiplier_numerator: "75",
      multiplier_denominator: "100",
    });

    const modsList = await adminCall(runtime, "GET", "/internal/v1/pricing-modifiers?enabled=true");
    const modItemsForModel = (modsList.items as { match?: { model?: string } }[]).filter(
      (m) => m.match?.model === "gpt-mod",
    );
    expect(modItemsForModel.length).toBe(4);

    const priorityRes = await runtime.publicApp.fetch(
      new Request("http://public.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "gpt-mod",
          service_tier: "priority",
          messages: [{ role: "user", content: "priority please" }],
          max_tokens: 100,
        }),
      }),
    );
    expect(priorityRes.status).toBe(200);
    const priorityReqId = priorityRes.headers.get("x-request-id")!;

    const unpricedTierRes = await runtime.publicApp.fetch(
      new Request("http://public.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "gpt-mod",
          service_tier: "unpriced_tier",
          messages: [{ role: "user", content: "nope" }],
          max_tokens: 100,
        }),
      }),
    );
    expect(unpricedTierRes.status).toBe(422);

    const webSearchRes = await runtime.publicApp.fetch(
      new Request("http://public.local/v1/responses", {
        method: "POST",
        headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "gpt-mod",
          input: "latest news",
          tools: [{ type: "web_search_preview" }],
          max_output_tokens: 100,
        }),
      }),
    );
    expect(webSearchRes.status).toBe(200);
    const webSearchReqId = webSearchRes.headers.get("x-request-id")!;

    const unpricedToolRes = await runtime.publicApp.fetch(
      new Request("http://public.local/v1/responses", {
        method: "POST",
        headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "gpt-mod",
          input: "run code",
          tools: [{ type: "code_interpreter" }],
          max_output_tokens: 100,
        }),
      }),
    );
    expect(unpricedToolRes.status).toBe(422);

    const exclusiveCard = await adminCall(runtime, "POST", "/internal/v1/prices", {
      side: "CUSTOMER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "3", denominator: "1" },
        output_tokens: { numerator: "6", denominator: "1" },
      },
      ceilings: { input_tokens: "10000", output_tokens: "4096" },
    });
    await adminCall(runtime, "PUT", "/internal/v1/pricing-modifiers", {
      layer: "COMMERCIAL",
      kind: "EXACT_RATE_CARD",
      stacking: "EXCLUSIVE",
      priority: 1,
      match: {
        side: "CUSTOMER",
        model: "gpt-mod",
        operation: "chat",
        billing_account_id: vipAccount.id,
      },
      rate_card_price_version_id: exclusiveCard.id,
    });
    const exclusiveRes = await runtime.publicApp.fetch(
      new Request("http://public.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "gpt-mod",
          messages: [{ role: "user", content: "exclusive contract" }],
          max_tokens: 100,
        }),
      }),
    );
    expect(exclusiveRes.status).toBe(200);
    const exclusiveReqId = exclusiveRes.headers.get("x-request-id")!;

    const rows = await db<
      { request_id: string; customer_atoms: string; supplier_cost_atoms: string }[]
    >`
      SELECT request_id::text, customer_atoms::text, supplier_cost_atoms::text
      FROM public.billing_charges
      WHERE request_id = ANY(${[priorityReqId, webSearchReqId, exclusiveReqId]}::uuid[])
    `;
    const byReq = new Map(rows.map((r) => [r.request_id, r]));
    expect(byReq.get(priorityReqId)!.customer_atoms).toBe("2700");
    expect(byReq.get(webSearchReqId)!.customer_atoms).toBe("9000");
    expect(byReq.get(webSearchReqId)!.supplier_cost_atoms).toBe("6000");
    expect(byReq.get(exclusiveReqId)!.customer_atoms).toBe("540");

    const audit = await runtime.ledger.auditConservation();
    expect(audit.ok, JSON.stringify(audit)).toBe(true);
  } finally {
    await runtime.close();
    upstream.stop(true);
  }
});
