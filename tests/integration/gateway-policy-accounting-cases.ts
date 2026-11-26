import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { makeConfig, db, internalCall, publishPlugins, textSample } from "./gateway-fixture.ts";

export function registerGatewayPolicyAccountingCases() {
  test("Hardened plugin billing, FAIL_OPEN audit, reasoning cross-protocol, compat dropped-fields, native errors, and epoch lineage", async () => {
    let upstreamMode: "reasoning" | "normal" | "gemini_single_sse" | "anthropic_thinking_sse" =
      "reasoning";
    const upstream = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req) {
        const url = new URL(req.url);
        if (url.pathname === "/webhook-down") {
          return new Response("down", { status: 503 });
        }
        if (upstreamMode === "gemini_single_sse") {
          const sse = `data: ${JSON.stringify({
            candidates: [
              {
                content: { role: "model", parts: [{ text: "Reach bob@example.com now" }] },
                finishReason: "STOP",
              },
            ],
            usageMetadata: { promptTokenCount: 5, candidatesTokenCount: 6 },
          })}\n\n`;
          return new Response(sse, {
            status: 200,
            headers: { "content-type": "text/event-stream; charset=utf-8" },
          });
        }
        if (upstreamMode === "anthropic_thinking_sse") {
          const events = [
            `event: message_start\ndata: ${JSON.stringify({
              type: "message_start",
              message: {
                id: "msg_ant_sse",
                type: "message",
                role: "assistant",
                model: "claude-sse-up",
                content: [],
                stop_reason: null,
                usage: { input_tokens: 8, output_tokens: 0 },
              },
            })}\n\n`,
            `event: content_block_start\ndata: ${JSON.stringify({
              type: "content_block_start",
              index: 0,
              content_block: { type: "thinking", thinking: "" },
            })}\n\n`,
            `event: content_block_delta\ndata: ${JSON.stringify({
              type: "content_block_delta",
              index: 0,
              delta: { type: "thinking_delta", thinking: "Internal note bob@example.com" },
            })}\n\n`,
            `event: content_block_stop\ndata: ${JSON.stringify({
              type: "content_block_stop",
              index: 0,
            })}\n\n`,
            `event: content_block_start\ndata: ${JSON.stringify({
              type: "content_block_start",
              index: 1,
              content_block: { type: "text", text: "" },
            })}\n\n`,
            `event: content_block_delta\ndata: ${JSON.stringify({
              type: "content_block_delta",
              index: 1,
              delta: { type: "text_delta", text: "Contact alice@example.com" },
            })}\n\n`,
            `event: content_block_stop\ndata: ${JSON.stringify({
              type: "content_block_stop",
              index: 1,
            })}\n\n`,
            `event: message_delta\ndata: ${JSON.stringify({
              type: "message_delta",
              delta: { stop_reason: "end_turn" },
              usage: { output_tokens: 12 },
            })}\n\n`,
            `event: message_stop\ndata: ${JSON.stringify({ type: "message_stop" })}\n\n`,
          ];
          return new Response(events.join(""), {
            status: 200,
            headers: { "content-type": "text/event-stream; charset=utf-8" },
          });
        }
        if (upstreamMode === "reasoning") {
          return Response.json({
            id: "chatcmpl_reason_1",
            object: "chat.completion",
            created: Math.floor(Date.now() / 1000),
            model: "reason-up",
            choices: [
              {
                index: 0,
                message: {
                  role: "assistant",
                  reasoning_content: "internal chain of thought",
                  content: "final answer 42",
                },
                finish_reason: "stop",
              },
            ],
            usage: { prompt_tokens: 10, completion_tokens: 20, total_tokens: 30 },
          });
        }
        return Response.json({
          id: "chatcmpl_norm_1",
          object: "chat.completion",
          created: Math.floor(Date.now() / 1000),
          model: "norm-up",
          choices: [
            {
              index: 0,
              message: { role: "assistant", content: "delivered despite fail-open" },
              finish_reason: "stop",
            },
          ],
          usage: { prompt_tokens: 5, completion_tokens: 5, total_tokens: 10 },
        });
      },
    });

    const runtime = createGatewayRuntime({
      config: makeConfig(),
      db,
    });
    await runtime.start();
    try {
      const billingAccount = (
        await internalCall(runtime.internalApp, "PUT", "/internal/v1/billing-accounts", {
          name: "hardened-proj",
          currency: "USD",
        })
      ).data;
      const key = (
        await internalCall(runtime.internalApp, "PUT", "/internal/v1/api-keys", {
          billing_account_id: billingAccount.id,
          name: "hardened-key",
        })
      ).data;
      await runtime.ledger.creditWallet({
        billing_account_id: String(billingAccount.id),
        currency: "USD",
        external_ref: "hardened-credit",
        amount_atoms: "1000000",
        kind: "CASH",
      });
      const account = (
        await internalCall(runtime.internalApp, "PUT", "/internal/v1/upstream-accounts", {
          name: "hardened-acct",
        })
      ).data;
      await runtime.ledger.rechargeSupplier({
        account_id: String(account.id),
        currency: "USD",
        external_ref: "hardened-sup",
        amount_atoms: "1000000",
      });
      const channel = (
        await internalCall(runtime.internalApp, "PUT", "/internal/v1/channels", {
          account_id: account.id,
          name: "openai-reason-ch",
          kind: "openai",
          base_url: `http://127.0.0.1:${upstream.port}`,
          api_key: "sk-test",
        })
      ).data;
      const custPrice = (
        await internalCall(runtime.internalApp, "POST", "/internal/v1/prices", {
          side: "CUSTOMER",
          currency: "USD",
          rates: {
            input_tokens: { numerator: "2", denominator: "1" },
            output_tokens: { numerator: "3", denominator: "1" },
          },
          ceilings: { input_tokens: "1000", output_tokens: "1000" },
        })
      ).data;
      const supPrice = (
        await internalCall(runtime.internalApp, "POST", "/internal/v1/prices", {
          side: "SUPPLIER",
          currency: "USD",
          rates: {
            input_tokens: { numerator: "1", denominator: "1" },
            output_tokens: { numerator: "1", denominator: "1" },
          },
          ceilings: { input_tokens: "1000", output_tokens: "1000" },
        })
      ).data;

      const failOpenPlugin = (
        await internalCall(runtime.internalApp, "PUT", "/internal/v1/plugins", {
          name: "fail-open-wh",
          version: "1.0.0",
          capability: "CONTENT",
          kind: "WEBHOOK",
          webhook_config: {
            url: `http://127.0.0.1:${upstream.port}/webhook-down`,
            failure_policy: "FAIL_OPEN",
            timeout_ms: 200,
          },
          manifest: { hooks: ["on_response"] },
        })
      ).data;
      await internalCall(runtime.internalApp, "POST", "/internal/v1/plugin-releases", {
        id: crypto.randomUUID(),
        expected_epoch: failOpenPlugin.config_epoch,
        targets: [{ plugin_id: failOpenPlugin.id }],
        samples: { [String(failOpenPlugin.id)]: { webhook_reply: { action: "PASS" } } },
      });

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "compat-reason",
        operations: ["chat", "messages", "gemini"],
        max_output_tokens: 128,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "compat-reason",
        channel_id: channel.id,
        upstream_model: "reason-up",
        operations: ["chat", "messages", "gemini"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        request_policy: { conversion_mode: "compat" },
      });

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "fail-open-model",
        operations: ["chat"],
        max_output_tokens: 128,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "fail-open-model",
        channel_id: channel.id,
        upstream_model: "norm-up",
        operations: ["chat"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        plugin_pipeline: [{ plugin_id: failOpenPlugin.id, priority: 1 }],
      });

      const epochBefore = await runtime.catalog.getEpochStamp();
      expect(epochBefore.lineageToken.length).toBeGreaterThan(10);

      const msgRes = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/messages", {
          method: "POST",
          headers: { "x-api-key": String(key.key), "content-type": "application/json" },
          body: JSON.stringify({
            model: "compat-reason",
            max_tokens: 64,
            metadata: { user_id: "u1" },
            messages: [
              {
                role: "user",
                content: [{ type: "text", text: "think", cache_control: { type: "ephemeral" } }],
              },
            ],
          }),
        }),
      );
      expect(msgRes.status).toBe(200);
      expect(msgRes.headers.get("x-gateway-dropped-fields")).toContain("metadata");
      expect(msgRes.headers.get("x-gateway-dropped-fields")).toContain("content.cache_control");
      const msgJson = (await msgRes.json()) as {
        type: string;
        content: { type: string; thinking?: string; text?: string }[];
      };
      expect(msgJson.type).toBe("message");
      expect(msgJson.content).toEqual([
        { type: "thinking", thinking: "internal chain of thought" },
        { type: "text", text: "final answer 42" },
      ]);

      const badAnthropic = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/messages", {
          method: "POST",
          headers: { "x-api-key": String(key.key), "content-type": "application/json" },
          body: JSON.stringify({ model: "missing-model", max_tokens: 10, messages: [] }),
        }),
      );
      expect(badAnthropic.status).toBe(404);
      expect(await badAnthropic.json()).toMatchObject({
        type: "error",
        error: { type: "not_found_error", code: "model_not_found" },
      });

      const badGemini = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1beta/models/missing-model:generateContent", {
          method: "POST",
          headers: { "x-goog-api-key": String(key.key), "content-type": "application/json" },
          body: JSON.stringify({ contents: [{ role: "user", parts: [{ text: "hi" }] }] }),
        }),
      );
      expect(badGemini.status).toBe(404);
      expect(await badGemini.json()).toMatchObject({
        error: { code: 404, status: "NOT_FOUND", gateway_code: "model_not_found" },
      });

      upstreamMode = "normal";
      const foRes = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "fail-open-model",
            messages: [{ role: "user", content: "hello" }],
            max_tokens: 32,
          }),
        }),
      );
      expect(foRes.status).toBe(200);
      const foReqId = foRes.headers.get("x-request-id")!;
      const foBilling = await runtime.queries.getRequestBilling(foReqId);
      expect(foBilling.charge?.customerAtoms).toBe("25");
      expect(foBilling.charge?.pluginAudit).toEqual(
        expect.arrayContaining([expect.objectContaining({ fail_open: true, hook: "on_response" })]),
      );

      const readyRes = await runtime.publicApp.fetch(new Request("http://gw.local/readyz"));
      expect(readyRes.status).toBe(200);
      expect(await readyRes.json()).toMatchObject({ status: "ready", wasm_runtime: "wasmtime" });

      const metricsText = await (await runtime.metricsResponse()).text();
      expect(metricsText).toContain('gateway_wasm_runtime_info{runtime="wasmtime"} 1');
      expect(metricsText).toContain("gateway_plugin_fail_open_total");
      expect(metricsText).toContain("gateway_protocol_field_dropped_total");

      const epochAfter = await runtime.catalog.upsertBillingAccount({
        id: String(billingAccount.id),
        name: "hardened-proj-2",
        rpm: 6000,
        tpm: 10_000_000,
        concurrency: 1000,
      });
      const stampAfter = await runtime.catalog.getEpochStamp();
      expect(stampAfter.version).toBe(epochAfter.config_epoch);
      expect(stampAfter.lineageToken).not.toBe(epochBefore.lineageToken);

      const piiInstalled = (
        await internalCall(
          runtime.internalApp,
          "POST",
          "/internal/v1/plugins/builtins/pii-guard/install",
          {},
        )
      ).data;
      await publishPlugins(runtime, [[piiInstalled.id, textSample]]);

      const chGemini = (
        await internalCall(runtime.internalApp, "PUT", "/internal/v1/channels", {
          account_id: account.id,
          name: "hardened-gemini-ch",
          kind: "gemini",
          base_url: `http://127.0.0.1:${upstream.port}`,
          api_key: "sk-gemini",
        })
      ).data;
      const chAnthropic = (
        await internalCall(runtime.internalApp, "PUT", "/internal/v1/channels", {
          account_id: account.id,
          name: "hardened-anthropic-ch",
          kind: "anthropic",
          base_url: `http://127.0.0.1:${upstream.port}`,
          api_key: "sk-anthropic",
        })
      ).data;
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "pii-gemini-stream",
        operations: ["gemini"],
        max_output_tokens: 128,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "pii-gemini-stream",
        channel_id: chGemini.id,
        upstream_model: "gemini-up",
        operations: ["gemini"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        plugin_pipeline: [{ plugin_id: piiInstalled.id, priority: 1 }],
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "pii-anthropic-thinking",
        operations: ["messages"],
        max_output_tokens: 128,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "pii-anthropic-thinking",
        channel_id: chAnthropic.id,
        upstream_model: "claude-sse-up",
        operations: ["messages"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        plugin_pipeline: [{ plugin_id: piiInstalled.id, priority: 1 }],
      });

      upstreamMode = "gemini_single_sse";
      const gemSseRes = await runtime.publicApp.fetch(
        new Request(
          "http://gw.local/v1beta/models/pii-gemini-stream:streamGenerateContent?alt=sse",
          {
            method: "POST",
            headers: { "x-goog-api-key": String(key.key), "content-type": "application/json" },
            body: JSON.stringify({ contents: [{ role: "user", parts: [{ text: "hello" }] }] }),
          },
        ),
      );
      expect(gemSseRes.status).toBe(200);
      const gemSseText = await gemSseRes.text();
      expect(gemSseText).toContain("[REDACTED_EMAIL]");
      expect(gemSseText).not.toContain("bob@example.com");
      expect(gemSseText).toContain('"finishReason":"STOP"');

      upstreamMode = "anthropic_thinking_sse";
      const antSseRes = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/messages", {
          method: "POST",
          headers: { "x-api-key": String(key.key), "content-type": "application/json" },
          body: JSON.stringify({
            model: "pii-anthropic-thinking",
            stream: true,
            max_tokens: 64,
            messages: [{ role: "user", content: "hello" }],
          }),
        }),
      );
      expect(antSseRes.status).toBe(200);
      const antSseText = await antSseRes.text();
      const antEvents = antSseText
        .split("\n\n")
        .filter((block) => block.includes("data: "))
        .map((block) => {
          const dataLine = block.split("\n").find((l) => l.startsWith("data: "));
          return dataLine ? (JSON.parse(dataLine.slice(6)) as Record<string, unknown>) : null;
        })
        .filter((ev): ev is Record<string, unknown> => ev !== null);
      const thinkingDeltaIdx = antEvents.findIndex(
        (ev) =>
          ev.type === "content_block_delta" &&
          ev.index === 0 &&
          (ev.delta as { type?: string; thinking?: string })?.type === "thinking_delta",
      );
      const textDeltaIdx = antEvents.findIndex(
        (ev) =>
          ev.type === "content_block_delta" &&
          ev.index === 1 &&
          (ev.delta as { type?: string; text?: string })?.type === "text_delta",
      );
      const textStopIdx = antEvents.findIndex(
        (ev) => ev.type === "content_block_stop" && ev.index === 1,
      );
      expect(thinkingDeltaIdx).toBeGreaterThanOrEqual(0);
      expect((antEvents[thinkingDeltaIdx]!.delta as { thinking?: string }).thinking).toBe(
        "Internal note bob@example.com",
      );
      expect(textDeltaIdx).toBeGreaterThan(thinkingDeltaIdx);
      expect((antEvents[textDeltaIdx]!.delta as { text?: string }).text).toContain(
        "[REDACTED_EMAIL]",
      );
      expect(antSseText).not.toContain("alice@example.com");
      expect(textStopIdx).toBeGreaterThan(textDeltaIdx);

      expect(await runtime.ledger.auditConservation()).toMatchObject({
        ok: true,
        open_holds: 0,
        unknown_holds: 0,
      });
    } finally {
      await runtime.close();
      await upstream.stop(true);
    }
  });
}
