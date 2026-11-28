import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { createLedgerQueries } from "@gateway/ledger";
import { db, internalCall, makeConfig } from "./gateway-fixture.ts";

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

export function registerStandardizedPricingStreamToolsCases() {
  test("Standardized pricing E2E: SSE streaming and interrupted stream fallback settle with FX + CLIFF snapshot", async () => {
    let truncateStream = false;
    const encoder = new TextEncoder();
    const upstream = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        await req.text();
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            const first = JSON.stringify({
              id: "chatcmpl-stream-pricing",
              object: "chat.completion.chunk",
              model: "gpt-stream-fx",
              choices: [
                { index: 0, delta: { content: "hello streaming world" }, finish_reason: null },
              ],
            });
            controller.enqueue(encoder.encode(`data: ${first}\n\n`));
            if (truncateStream) {
              controller.close();
              return;
            }
            const finish = JSON.stringify({
              id: "chatcmpl-stream-pricing",
              object: "chat.completion.chunk",
              model: "gpt-stream-fx",
              choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
              usage: { prompt_tokens: 100, completion_tokens: 20 },
            });
            controller.enqueue(encoder.encode(`data: ${finish}\n\n`));
            controller.enqueue(encoder.encode("data: [DONE]\n\n"));
            controller.close();
          },
        });
        return new Response(body, {
          status: 200,
          headers: { "content-type": "text/event-stream" },
        });
      },
    });

    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    const queries = createLedgerQueries(db);
    try {
      await runtime.start();
      const fx = await adminCall(runtime, "POST", "/internal/v1/fx-rates", {
        base_currency: "USD",
        quote_currency: "CNY",
        rate_numerator: "725",
        rate_denominator: "100",
        effective_from: "2024-01-01T00:00:00Z",
      });
      const acct = await adminCall(runtime, "PUT", "/internal/v1/billing-accounts", {
        name: "stream-fx-acct",
        currency: "CNY",
        rpm: 1000,
        tpm: 10_000_000,
        concurrency: 50,
      });
      await adminCall(runtime, "POST", `/internal/v1/wallets/${acct.id}/credits`, {
        currency: "CNY",
        external_ref: "stream-fx-fund-1",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      const key = await adminCall(runtime, "PUT", "/internal/v1/api-keys", {
        billing_account_id: acct.id,
        name: "stream-fx-key",
      });
      const sup = await adminCall(runtime, "PUT", "/internal/v1/upstream-accounts", {
        name: `stream-fx-sup-${crypto.randomUUID()}`,
      });
      await adminCall(runtime, "POST", `/internal/v1/upstream-accounts/${sup.id}/recharges`, {
        currency: "USD",
        external_ref: "stream-fx-sup-fund",
        amount_atoms: "100000000",
      });
      const chan = await adminCall(runtime, "PUT", "/internal/v1/channels", {
        account_id: sup.id,
        kind: "openai",
        name: "stream-fx-chan",
        base_url: upstream.url.origin,
        api_key: "sk-stream",
      });
      await adminCall(runtime, "PUT", "/internal/v1/models", {
        name: "gpt-stream-fx",
        operations: ["chat"],
        max_output_tokens: 100,
      });
      const custPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {},
        ceilings: { input_tokens: "1000", output_tokens: "100" },
        pricing_spec: {
          cliff: {
            driver: ["input_tokens"],
            tiers: [
              {
                min_driver: "0",
                rates: {
                  input_tokens: { numerator: "10", denominator: "1" },
                  output_tokens: { numerator: "20", denominator: "1" },
                },
              },
              {
                min_driver: "500",
                rates: {
                  input_tokens: { numerator: "30", denominator: "1" },
                  output_tokens: { numerator: "60", denominator: "1" },
                },
              },
            ],
          },
        },
      });
      const supPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "4", denominator: "1" },
          output_tokens: { numerator: "8", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", output_tokens: "100" },
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "gpt-stream-fx",
        channel_id: chan.id,
        upstream_model: "gpt-stream-fx",
        priority: 1,
        weight: 1,
        operations: ["chat"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
      });

      const okRes = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "gpt-stream-fx",
            stream: true,
            messages: [{ role: "user", content: "hi" }],
            max_tokens: 100,
          }),
        }),
      );
      expect(okRes.status).toBe(200);
      const okReqId = okRes.headers.get("x-request-id")!;
      await okRes.text();

      truncateStream = true;
      const truncRes = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "gpt-stream-fx",
            stream: true,
            messages: [{ role: "user", content: "truncated" }],
            max_tokens: 100,
          }),
        }),
      );
      expect(truncRes.status).toBe(200);
      const truncReqId = truncRes.headers.get("x-request-id")!;
      await truncRes.text();

      const okBilling = await queries.getRequestBilling(okReqId);
      expect(okBilling.charge?.currency).toBe("CNY");
      expect(okBilling.charge?.customerAtoms).toBe("10150");
      expect(okBilling.charge?.pricingSnapshot?.customer?.fx_version_id).toBe(fx.id as string);

      const truncBilling = await queries.getRequestBilling(truncReqId);
      expect(truncBilling.hold?.state).toBe("SETTLED");
      expect(truncBilling.charge?.currency).toBe("CNY");
      expect(truncBilling.charge?.outcome).toBe("SETTLED");
      expect(BigInt(truncBilling.charge!.customerAtoms)).toBeGreaterThan(0n);
      expect(BigInt(truncBilling.charge!.customerAtoms)).toBeLessThanOrEqual(
        BigInt(truncBilling.hold!.heldAtoms),
      );
      expect(truncBilling.charge?.pricingSnapshot?.customer?.fx_version_id).toBe(fx.id as string);

      const audit = await runtime.ledger.auditConservation();
      expect(audit.ok, JSON.stringify(audit)).toBe(true);
    } finally {
      await runtime.close();
      upstream.stop(true);
    }
  });

  test("Standardized pricing E2E: hosted tools native extraction, fail-closed guards, deepseek transforms, and videos ceiling", async () => {
    const upstream = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        const path = new URL(req.url).pathname;
        if (path === "/v1/responses") {
          const parsedReq = JSON.parse(await req.text()) as { stream?: boolean };
          if (parsedReq.stream) {
            const encoder = new TextEncoder();
            const streamBody = new ReadableStream<Uint8Array>({
              start(controller) {
                const delta = JSON.stringify({
                  type: "response.output_text.delta",
                  output_index: 0,
                  delta: "web search result preview",
                });
                controller.enqueue(
                  encoder.encode(`event: response.output_text.delta\ndata: ${delta}\n\n`),
                );
                controller.close();
              },
            });
            return new Response(streamBody, {
              status: 200,
              headers: { "content-type": "text/event-stream" },
            });
          }
          return Response.json({
            id: "resp-native-ws",
            object: "response",
            status: "completed",
            output: [
              { type: "web_search_call", status: "completed" },
              { type: "web_search_call", status: "completed" },
              {
                type: "message",
                role: "assistant",
                content: [{ type: "output_text", text: "searched" }],
              },
            ],
            usage: { input_tokens: 100, output_tokens: 20 },
          });
        }
        await req.text();
        if (path === "/v1/messages") {
          return Response.json({
            id: "msg-anthropic-tools",
            type: "message",
            role: "assistant",
            model: "claude-tools",
            content: [{ type: "text", text: "done" }],
            stop_reason: "end_turn",
            usage: {
              input_tokens: 100,
              output_tokens: 10,
              server_tool_use: { web_search_requests: 1, code_execution_requests: 2 },
            },
          });
        }
        if (path === "/v1/videos") {
          return Response.json({
            id: "vid-1",
            status: "completed",
            usage: { video_seconds: 1 },
          });
        }
        return Response.json({
          id: "chatcmpl-ds",
          object: "chat.completion",
          model: "deepseek-prio",
          choices: [
            { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
          ],
          usage: { prompt_tokens: 100, completion_tokens: 10 },
        });
      },
    });

    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    const queries = createLedgerQueries(db);
    try {
      await runtime.start();
      const acct = await adminCall(runtime, "PUT", "/internal/v1/billing-accounts", {
        name: "tools-edge-acct",
        currency: "USD",
        rpm: 1000,
        tpm: 10_000_000,
        concurrency: 50,
      });
      await adminCall(runtime, "POST", `/internal/v1/wallets/${acct.id}/credits`, {
        currency: "USD",
        external_ref: "tools-edge-fund",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      const key = await adminCall(runtime, "PUT", "/internal/v1/api-keys", {
        billing_account_id: acct.id,
        name: "tools-edge-key",
      });
      const sup = await adminCall(runtime, "PUT", "/internal/v1/upstream-accounts", {
        name: `tools-edge-sup-${crypto.randomUUID()}`,
      });
      await adminCall(runtime, "POST", `/internal/v1/upstream-accounts/${sup.id}/recharges`, {
        currency: "USD",
        external_ref: "tools-edge-sup-fund",
        amount_atoms: "100000000",
      });
      const openaiChan = await adminCall(runtime, "PUT", "/internal/v1/channels", {
        account_id: sup.id,
        kind: "openai",
        name: "tools-openai",
        base_url: upstream.url.origin,
        api_key: "sk-oai",
      });
      const anthropicChan = await adminCall(runtime, "PUT", "/internal/v1/channels", {
        account_id: sup.id,
        kind: "anthropic",
        name: "tools-anthropic",
        base_url: upstream.url.origin,
        api_key: "sk-ant",
      });
      const deepseekChan = await adminCall(runtime, "PUT", "/internal/v1/channels", {
        account_id: sup.id,
        kind: "deepseek",
        name: "tools-deepseek",
        base_url: upstream.url.origin,
        api_key: "sk-ds",
      });

      for (const [name, ops] of [
        ["oai-ws-native", ["responses", "chat"]],
        ["oai-ws-zero-ceil", ["responses"]],
        ["oai-ws-matrix", ["responses"]],
        ["claude-tools", ["messages"]],
        ["deepseek-prio", ["chat"]],
        ["vid-ceil-guard", ["videos"]],
      ] as const) {
        await adminCall(runtime, "PUT", "/internal/v1/models", {
          name,
          operations: [...ops],
          max_output_tokens: 1000,
        });
      }

      const wsCust = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: "2", denominator: "1" },
          web_search_calls: { numerator: "500", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000", web_search_calls: "5" },
      });
      const wsSup = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "2" },
          output_tokens: { numerator: "1", denominator: "1" },
          web_search_calls: { numerator: "200", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000", web_search_calls: "5" },
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "oai-ws-native",
        channel_id: openaiChan.id,
        upstream_model: "oai-ws-native",
        priority: 1,
        weight: 1,
        operations: ["responses", "chat"],
        billing_price_id: wsCust.id,
        cost_version_id: wsSup.id,
      });

      const zeroCeilCust = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: "2", denominator: "1" },
          web_search_calls: { numerator: "500", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000", web_search_calls: "0" },
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "oai-ws-zero-ceil",
        channel_id: openaiChan.id,
        upstream_model: "oai-ws-zero-ceil",
        priority: 1,
        weight: 1,
        operations: ["responses"],
        billing_price_id: zeroCeilCust.id,
        cost_version_id: wsSup.id,
      });

      const matCust = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: "2", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000", web_search_calls: "5" },
        pricing_spec: {
          matrix: [
            {
              match: { reasoning_effort: "high" },
              rates: {
                input_tokens: { numerator: "1", denominator: "1" },
                output_tokens: { numerator: "2", denominator: "1" },
                web_search_calls: { numerator: "500", denominator: "1" },
              },
            },
          ],
        },
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "oai-ws-matrix",
        channel_id: openaiChan.id,
        upstream_model: "oai-ws-matrix",
        priority: 1,
        weight: 1,
        operations: ["responses"],
        billing_price_id: matCust.id,
        cost_version_id: wsSup.id,
      });

      const antCust = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: "2", denominator: "1" },
          web_search_calls: { numerator: "300", denominator: "1" },
          code_interpreter_sessions: { numerator: "400", denominator: "1" },
        },
        ceilings: {
          input_tokens: "10000",
          output_tokens: "1000",
          web_search_calls: "5",
          code_interpreter_sessions: "5",
        },
      });
      const antSup = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "2" },
          output_tokens: { numerator: "1", denominator: "1" },
          web_search_calls: { numerator: "100", denominator: "1" },
          code_interpreter_sessions: { numerator: "150", denominator: "1" },
        },
        ceilings: {
          input_tokens: "10000",
          output_tokens: "1000",
          web_search_calls: "5",
          code_interpreter_sessions: "5",
        },
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "claude-tools",
        channel_id: anthropicChan.id,
        upstream_model: "claude-tools",
        priority: 1,
        weight: 1,
        operations: ["messages"],
        billing_price_id: antCust.id,
        cost_version_id: antSup.id,
      });

      const dsCust = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "10", denominator: "1" },
          output_tokens: { numerator: "20", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000" },
      });
      const dsSup = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "5", denominator: "1" },
          output_tokens: { numerator: "10", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000" },
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "deepseek-prio",
        channel_id: deepseekChan.id,
        upstream_model: "deepseek-prio",
        priority: 1,
        weight: 1,
        operations: ["chat"],
        billing_price_id: dsCust.id,
        cost_version_id: dsSup.id,
      });
      await adminCall(runtime, "PUT", "/internal/v1/pricing-modifiers", {
        layer: "PRODUCT",
        kind: "MULTIPLIER",
        stacking: "MULTIPLY",
        priority: 10,
        match: { side: "CUSTOMER", model: "deepseek-prio", service_tier: "priority" },
        multiplier_numerator: "150",
        multiplier_denominator: "100",
      });

      const vidCust = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: { video_seconds: { numerator: "1000", denominator: "1" } },
        ceilings: { video_seconds: "10" },
      });
      const vidSup = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "SUPPLIER",
        currency: "USD",
        rates: { video_seconds: { numerator: "500", denominator: "1" } },
        ceilings: { video_seconds: "10" },
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "vid-ceil-guard",
        channel_id: openaiChan.id,
        upstream_model: "vid-ceil-guard",
        priority: 1,
        weight: 1,
        operations: ["videos"],
        billing_price_id: vidCust.id,
        cost_version_id: vidSup.id,
      });

      const respWs = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/responses", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "oai-ws-native",
            input: "search news",
            tools: [{ type: "web_search" }],
            max_output_tokens: 100,
          }),
        }),
      );
      expect(respWs.status).toBe(200);
      const respWsBilling = await queries.getRequestBilling(respWs.headers.get("x-request-id")!);
      expect(respWsBilling.charge?.customerAtoms).toBe("1140");

      const respWsStreamTrunc = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/responses", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "oai-ws-native",
            input: "search stream truncated",
            stream: true,
            tools: [{ type: "web_search" }],
            max_output_tokens: 100,
          }),
        }),
      );
      expect(respWsStreamTrunc.status).toBe(200);
      const respWsStreamReqId = respWsStreamTrunc.headers.get("x-request-id")!;
      await respWsStreamTrunc.text();
      const respWsStreamBilling = await queries.getRequestBilling(respWsStreamReqId);
      expect(respWsStreamBilling.hold?.state).toBe("UNKNOWN");
      expect(respWsStreamBilling.charge).toBeNull();

      const chatWsRejected = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "oai-ws-native",
            messages: [{ role: "user", content: "search" }],
            web_search_options: { search_context_size: "low" },
            max_tokens: 100,
          }),
        }),
      );
      expect(chatWsRejected.status).toBe(422);

      const zeroCeilRejected = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/responses", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "oai-ws-zero-ceil",
            input: "search",
            tools: [{ type: "web_search" }],
            max_output_tokens: 100,
          }),
        }),
      );
      expect(zeroCeilRejected.status).toBe(422);

      const matrixUnmatchedRejected = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/responses", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "oai-ws-matrix",
            input: "search",
            tools: [{ type: "web_search" }],
            max_output_tokens: 100,
          }),
        }),
      );
      expect(matrixUnmatchedRejected.status).toBe(422);

      const antRes = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/messages", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "claude-tools",
            max_tokens: 100,
            messages: [{ role: "user", content: "search and run" }],
            tools: [
              { type: "web_search_20250305", name: "web_search" },
              { type: "code_execution_20250522", name: "code_execution" },
            ],
          }),
        }),
      );
      expect(antRes.status).toBe(200);
      const antBilling = await queries.getRequestBilling(antRes.headers.get("x-request-id")!);
      expect(antBilling.charge?.customerAtoms).toBe("1220");

      const dsRes = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "deepseek-prio",
            service_tier: "priority",
            messages: [{ role: "user", content: "fast" }],
            max_tokens: 100,
          }),
        }),
      );
      expect(dsRes.status).toBe(200);
      const dsBilling = await queries.getRequestBilling(dsRes.headers.get("x-request-id")!);
      expect(dsBilling.charge?.customerAtoms).toBe("1800");

      const tightAcct = await adminCall(runtime, "PUT", "/internal/v1/billing-accounts", {
        name: "vid-tight-acct",
        currency: "USD",
        rpm: 1000,
        tpm: 10_000_000,
        concurrency: 50,
      });
      await adminCall(runtime, "POST", `/internal/v1/wallets/${tightAcct.id}/credits`, {
        currency: "USD",
        external_ref: "vid-tight-fund",
        amount_atoms: "5000",
        kind: "CASH",
      });
      const tightKey = await adminCall(runtime, "PUT", "/internal/v1/api-keys", {
        billing_account_id: tightAcct.id,
        name: "vid-tight-key",
      });
      const vidRejected = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/videos", {
          method: "POST",
          headers: { authorization: `Bearer ${tightKey.key}`, "content-type": "application/json" },
          body: JSON.stringify({ model: "vid-ceil-guard", prompt: "cat", seconds: 1 }),
        }),
      );
      expect(vidRejected.status).toBe(402);

      const audit = await runtime.ledger.auditConservation();
      expect(audit.ok, JSON.stringify(audit)).toBe(true);
    } finally {
      await runtime.close();
      upstream.stop(true);
    }
  });
}
