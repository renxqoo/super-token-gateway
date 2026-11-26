import { expect, test } from "bun:test";
import { generateKeyPairSync, sign } from "node:crypto";
import { createGatewayRuntime } from "@gateway/gateway";
import {
  makeConfig,
  db,
  internalCall,
  publishPlugins,
  textSample,
  webhookSample,
} from "./gateway-fixture.ts";
import { buildRealMatchWasm, buildRealLoopWasm } from "./gateway-wasm-fixture.ts";

export function registerGatewayWasmCases() {
  test("G3.3 Full E2E real plugin mechanism (zero mocks): real Bun WASM Worker on_request/on_response/streaming/audit/loop-kill + real STREAM_AND_CUTOFF async abort settlement", async () => {
    const seenPrompts: string[] = [];
    let upstreamAbortedDuringCutoff = false;
    const openTimers = new Set<ReturnType<typeof setTimeout>>();

    const upstreamServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        const body = (await req.json()) as {
          model: string;
          stream?: boolean;
          messages: { role: string; content: string }[];
        };
        const prompt = body.messages[body.messages.length - 1]?.content ?? "";
        seenPrompts.push(prompt);
        if (body.stream && body.model === "cutoff-up") {
          const enc = new TextEncoder();
          const stream = new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(
                enc.encode(
                  `data: ${JSON.stringify({
                    id: "chatcmpl_cut",
                    object: "chat.completion.chunk",
                    model: "cutoff-up",
                    choices: [
                      { index: 0, delta: { role: "assistant", content: "Intro. ASYNC_BAN." } },
                    ],
                  })}\n\n`,
                ),
              );
              const t = setTimeout(() => {
                openTimers.delete(t);
                try {
                  controller.enqueue(
                    enc.encode(
                      `data: ${JSON.stringify({
                        id: "chatcmpl_cut",
                        object: "chat.completion.chunk",
                        model: "cutoff-up",
                        choices: [{ index: 0, delta: { content: " Should not arrive." } }],
                      })}\n\n`,
                    ),
                  );
                  controller.enqueue(
                    enc.encode(
                      `data: ${JSON.stringify({
                        id: "chatcmpl_cut",
                        object: "chat.completion.chunk",
                        model: "cutoff-up",
                        choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
                        usage: { prompt_tokens: 10, completion_tokens: 10, total_tokens: 20 },
                      })}\n\n`,
                    ),
                  );
                  controller.enqueue(enc.encode("data: [DONE]\n\n"));
                  controller.close();
                } catch {
                  void 0;
                }
              }, 300);
              openTimers.add(t);
              req.signal.addEventListener(
                "abort",
                () => {
                  upstreamAbortedDuringCutoff = true;
                  clearTimeout(t);
                  openTimers.delete(t);
                  try {
                    controller.close();
                  } catch {
                    void 0;
                  }
                },
                { once: true },
              );
            },
          });
          return new Response(stream, {
            status: 200,
            headers: { "content-type": "text/event-stream; charset=utf-8" },
          });
        }
        if (body.stream) {
          const enc = new TextEncoder();
          const sse =
            `data: ${JSON.stringify({
              id: "chatcmpl_wasm_sse",
              object: "chat.completion.chunk",
              model: body.model,
              choices: [{ index: 0, delta: { role: "assistant", content: "Here is LEAK_" } }],
            })}\n\n` +
            `data: ${JSON.stringify({
              id: "chatcmpl_wasm_sse",
              object: "chat.completion.chunk",
              model: body.model,
              choices: [{ index: 0, delta: { content: "RESP token." } }],
            })}\n\n` +
            `data: ${JSON.stringify({
              id: "chatcmpl_wasm_sse",
              object: "chat.completion.chunk",
              model: body.model,
              choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
              usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
            })}\n\n` +
            "data: [DONE]\n\n";
          return new Response(enc.encode(sse), {
            status: 200,
            headers: { "content-type": "text/event-stream; charset=utf-8" },
          });
        }
        const replyContent = prompt.includes("TRIGGER_RESP_MASK")
          ? "Reply with LEAK_RESP inside"
          : `Echo: ${prompt}`;
        return Response.json({
          id: "chatcmpl_real_wasm",
          object: "chat.completion",
          model: body.model,
          choices: [
            {
              index: 0,
              message: { role: "assistant", content: replyContent },
              finish_reason: "stop",
            },
          ],
          usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
        });
      },
    });

    const cutoffWebhookHooks: string[] = [];
    const cutoffWebhookServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        const body = (await req.json()) as { hook: string; text: string };
        cutoffWebhookHooks.push(body.hook);
        await Bun.sleep(25);
        if (body.text.includes("ASYNC_BAN")) {
          return Response.json({
            action: "BLOCK",
            rule_code: "ASYNC_CUTOFF_HIT",
            http_status: 400,
            safe_message: "Cutoff webhook aborted stream",
          });
        }
        return Response.json({ action: "PASS" });
      },
    });

    const { publicKey, privateKey } = generateKeyPairSync("ed25519");
    const pubPem = publicKey.export({ type: "spki", format: "pem" }).toString();
    const signWasm = (bytes: Uint8Array) => {
      const digest = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
      return sign(null, Buffer.from(digest, "utf8"), privateKey).toString("base64");
    };

    const runtime = createGatewayRuntime({
      config: makeConfig({ pluginSigningPublicKeyPem: pubPem }),
      db,
    });

    try {
      await runtime.start();
      const { data: billingAccount } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/billing-accounts",
        { name: "real-plugin-billingAccount", currency: "USD" },
      );
      const billingAccountId = String(billingAccount.id);
      const { data: keyRes } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/api-keys",
        { billing_account_id: billingAccountId, name: "real-plugin-key" },
      );
      const apiKey = String(keyRes.key);

      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/wallets/${billingAccountId}/credits`,
        {
          currency: "USD",
          external_ref: "credit-real-plugin-1",
          amount_atoms: "1000000",
          kind: "CASH",
        },
      );

      const { data: acc } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/upstream-accounts",
        { name: "real-plugin-acc" },
      );
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/upstream-accounts/${acc.id}/recharges`,
        { currency: "USD", external_ref: "sup-real-plugin-1", amount_atoms: "1000000" },
      );
      const { data: ch } = await internalCall(runtime.internalApp, "PUT", "/internal/v1/channels", {
        account_id: acc.id,
        name: "real-plugin-ch",
        kind: "openai",
        base_url: `http://127.0.0.1:${upstreamServer.port}`,
        api_key: "sk-real-plugin",
      });
      const { data: custPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "CUSTOMER",
          currency: "USD",
          rates: {
            input_tokens: { numerator: "10", denominator: "1" },
            output_tokens: { numerator: "20", denominator: "1" },
          },
          ceilings: { input_tokens: "1000", output_tokens: "100" },
        },
      );
      const { data: supPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "SUPPLIER",
          currency: "USD",
          rates: {
            input_tokens: { numerator: "2", denominator: "1" },
            output_tokens: { numerator: "4", denominator: "1" },
          },
          ceilings: { input_tokens: "1000", output_tokens: "100" },
        },
      );

      const reqBlockWasm = buildRealMatchWasm(["on_request"], "BAN_PROMPT", {
        action: "BLOCK",
        rule_code: "WASM_REQ_BLOCK",
        http_status: 400,
        safe_message: "Prompt blocked by real WASM",
      });
      const reqTransformWasm = buildRealMatchWasm(["on_request"], "REWRITE_REQ", {
        action: "TRANSFORM",
        text: "SANITIZED_PROMPT",
      });
      const respMaskWasm = buildRealMatchWasm(
        ["on_response", "on_stream_chunk", "on_stream_end"],
        "LEAK_RESP",
        { action: "TRANSFORM", text: "[MASKED_BY_REAL_WASM]" },
      );
      const loopWasm = buildRealLoopWasm();

      const { data: pReqBlock } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "wasm-req-block",
          version: "1.0.0",
          kind: "WASM",
          wasm_bytes_base64: Buffer.from(reqBlockWasm).toString("base64"),
          signature: signWasm(reqBlockWasm),
          signing_key_id: "k1",
          manifest: { hooks: ["on_request"] },
        },
      );
      const { data: pReqTransform } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "wasm-req-transform",
          version: "1.0.0",
          kind: "WASM",
          wasm_bytes_base64: Buffer.from(reqTransformWasm).toString("base64"),
          signature: signWasm(reqTransformWasm),
          signing_key_id: "k1",
          manifest: { hooks: ["on_request"] },
        },
      );
      const { data: pRespMask } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "wasm-resp-mask",
          version: "1.0.0",
          kind: "WASM",
          wasm_bytes_base64: Buffer.from(respMaskWasm).toString("base64"),
          signature: signWasm(respMaskWasm),
          signing_key_id: "k1",
          manifest: {
            hooks: ["on_response", "on_stream_chunk", "on_stream_end"],
            window_bytes: 16,
          },
        },
      );
      const { data: pLoop } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "wasm-loop-e2e",
          version: "1.0.0",
          kind: "WASM",
          wasm_bytes_base64: Buffer.from(loopWasm).toString("base64"),
          signature: signWasm(loopWasm),
          signing_key_id: "k1",
          manifest: { hooks: ["on_response"], fuel_budget: 500_000, failure_policy: "FAIL_CLOSED" },
        },
      );
      const { data: pCutoffWh } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "wh-cutoff-e2e",
          version: "1.0.0",
          kind: "WEBHOOK",
          webhook_config: {
            url: `http://127.0.0.1:${cutoffWebhookServer.port}/cutoff`,
            stream_mode: "STREAM_AND_CUTOFF",
            window_chars: 16,
            charge_policy: "PLATFORM_ABSORB",
          },
          manifest: { hooks: ["on_stream_chunk", "on_stream_end"] },
        },
      );

      await publishPlugins(runtime, [
        [pReqBlock.id, textSample],
        [pReqTransform.id, textSample],
        [pRespMask.id, textSample],
        [pCutoffWh.id, webhookSample],
      ]);
      const rejectedLoop = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/plugin-releases/validate",
        {
          id: crypto.randomUUID(),
          expected_epoch: runtime.snapshot.get().epoch,
          targets: [{ plugin_id: pLoop.id }],
          samples: { [String(pLoop.id)]: textSample },
        },
      );
      expect(rejectedLoop.status).toBe(422);
      await db`UPDATE public.plugins SET enabled = true WHERE id = ${pLoop.id}`;
      await runtime.snapshot.load();

      for (const m of ["real-wasm-model", "loop-wasm-model", "cutoff-wh-model"]) {
        await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
          name: m,
          operations: ["chat"],
          max_output_tokens: 100,
        });
      }

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "real-wasm-model",
        channel_id: ch.id,
        upstream_model: "real-wasm-up",
        operations: ["chat"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        plugin_pipeline: [
          { plugin_id: String(pReqBlock.id), priority: 1 },
          { plugin_id: String(pReqTransform.id), priority: 2 },
          { plugin_id: String(pRespMask.id), priority: 3 },
        ],
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "loop-wasm-model",
        channel_id: ch.id,
        upstream_model: "loop-wasm-up",
        operations: ["chat"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        plugin_pipeline: [{ plugin_id: String(pLoop.id), priority: 1 }],
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "cutoff-wh-model",
        channel_id: ch.id,
        upstream_model: "cutoff-up",
        operations: ["chat"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        plugin_pipeline: [{ plugin_id: String(pCutoffWh.id), priority: 1 }],
      });

      const blockedReq = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "real-wasm-model",
            messages: [{ role: "user", content: "please BAN_PROMPT now" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(blockedReq.status).toBe(400);
      const blockedReqJson = (await blockedReq.json()) as {
        error: { code: string; rule_code: string };
      };
      expect(blockedReqJson.error.code).toBe("content_blocked");
      expect(blockedReqJson.error.rule_code).toBe("WASM_REQ_BLOCK");
      expect(seenPrompts.length).toBe(0);

      const reqTransformResp = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "real-wasm-model",
            messages: [{ role: "user", content: "contains REWRITE_REQ secret" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(reqTransformResp.status).toBe(200);
      expect(seenPrompts[0]).toBe("SANITIZED_PROMPT");
      const reqTransformJson = (await reqTransformResp.json()) as {
        choices: { message: { content: string } }[];
      };
      expect(reqTransformJson.choices[0]!.message.content).toBe("Echo: SANITIZED_PROMPT");

      const respMaskResp = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "real-wasm-model",
            messages: [{ role: "user", content: "TRIGGER_RESP_MASK" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(respMaskResp.status).toBe(200);
      const respMaskJson = (await respMaskResp.json()) as {
        choices: { message: { content: string } }[];
      };
      expect(respMaskJson.choices[0]!.message.content).toBe("[MASKED_BY_REAL_WASM]");

      const streamMaskResp = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "real-wasm-model",
            stream: true,
            messages: [{ role: "user", content: "stream mask" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(streamMaskResp.status).toBe(200);
      const streamReqId = streamMaskResp.headers.get("x-request-id")!;
      const streamBody = await streamMaskResp.text();
      const streamDeltaText = streamBody
        .split("\n")
        .filter((l) => l.startsWith("data: ") && l !== "data: [DONE]")
        .map((l) => {
          const obj = JSON.parse(l.slice(6)) as {
            choices?: { delta?: { content?: string } }[];
          };
          return obj.choices?.[0]?.delta?.content ?? "";
        })
        .join("");
      expect(streamDeltaText).toBe("[MASKED_BY_REAL_WASM]");
      expect(streamBody).not.toContain("LEAK_RESP");
      expect(streamBody).toContain("[DONE]");

      const streamChargeRows = await db`
      SELECT plugin_audit FROM public.billing_charges WHERE request_id = ${streamReqId}
    `;
      const auditEntries = (streamChargeRows[0]!.plugin_audit ?? []) as {
        hook: string;
        action: string;
      }[];
      expect(
        auditEntries.some((a) => a.hook === "on_stream_chunk" && a.action === "TRANSFORM"),
      ).toBe(true);

      const { data: walletBeforeLoop } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/wallets/${billingAccountId}?currency=USD`,
      );
      const loopResp = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "loop-wasm-model",
            messages: [{ role: "user", content: "run infinite loop" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(loopResp.status).toBe(500);
      const loopReqId = loopResp.headers.get("x-request-id")!;
      const loopJson = (await loopResp.json()) as { error: { code: string; rule_code: string } };
      expect(loopJson.error.code).toBe("plugin_execution_failed");
      expect(loopJson.error.rule_code).toBe("wasm_execution_failed");
      const { data: walletAfterLoop } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/wallets/${billingAccountId}?currency=USD`,
      );
      expect(walletAfterLoop.available_atoms).toBe(walletBeforeLoop.available_atoms);
      const loopCharge = await runtime.queries.getRequestBilling(loopReqId);
      expect(loopCharge.charge?.customerAtoms).toBe("0");
      expect(loopCharge.charge?.chargePolicy).toBe("PLATFORM_ABSORB");

      const postLoopResp = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "real-wasm-model",
            messages: [{ role: "user", content: "still alive" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(postLoopResp.status).toBe(200);

      const { data: walletBeforeCutoff } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/wallets/${billingAccountId}?currency=USD`,
      );

      const cutoffResp = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "cutoff-wh-model",
            stream: true,
            messages: [{ role: "user", content: "test async cutoff" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(cutoffResp.status).toBe(200);
      const cutoffReqId = cutoffResp.headers.get("x-request-id")!;
      const cutoffText = await cutoffResp.text();
      expect(cutoffText).toContain("Intro. ASYNC_BAN.");
      expect(cutoffText).not.toContain("Should not arrive.");
      expect(cutoffText).toContain("content_blocked");
      expect(cutoffText).toContain("ASYNC_CUTOFF_HIT");
      expect(cutoffText).not.toContain("[DONE]");
      expect(cutoffWebhookHooks).not.toContain("on_request");
      expect(upstreamAbortedDuringCutoff).toBe(true);

      const cutoffHoldRows = await db`
      SELECT state FROM public.billing_holds WHERE request_id = ${cutoffReqId}
    `;
      expect(cutoffHoldRows[0]!.state).toBe("SETTLED");

      const chHealth = runtime.router.snapshot().find((s) => s.channel_id === String(ch.id));
      expect(chHealth?.state).toBe("CLOSED");
      expect(chHealth?.consecutive_failures ?? 0).toBe(0);

      const { data: walletAfterCutoff } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/wallets/${billingAccountId}?currency=USD`,
      );
      expect(walletAfterCutoff.spent_atoms).toBe(walletBeforeCutoff.spent_atoms);
      expect(walletAfterCutoff.reserved_atoms).toBe("0");

      const { data: audit } = await internalCall(
        runtime.internalApp,
        "GET",
        "/internal/v1/audit/conservation",
      );
      expect(audit.ok).toBe(true);
      expect(audit.open_holds).toBe(0);
      expect(audit.unknown_holds).toBe(0);
    } finally {
      for (const t of openTimers) clearTimeout(t);
      openTimers.clear();
      await runtime.close();
      await upstreamServer.stop(true);
      await cutoffWebhookServer.stop(true);
    }
  });
}
