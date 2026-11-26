import { expect, test } from "bun:test";
import { generateKeyPairSync, sign } from "node:crypto";
import type { WasmRuntimeExecutor } from "@gateway/plugin-engine";
import { createGatewayRuntime } from "@gateway/gateway";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";
import { getBuiltinPlugin } from "@gateway/plugin-engine";
import {
  makeConfig,
  db,
  internalCall,
  publishPlugins,
  textSample,
  webhookSample,
} from "./gateway-fixture.ts";

export function registerGatewayModerationCases() {
  test("G2.3 + G3.1 + G3.2 SSE streaming atomic [DONE], HOLD_UNTIL_APPROVED zero-leak Webhook + WASM redaction + CHARGE_CUSTOMER vs PLATFORM_ABSORB + zero plaintext DB", async () => {
    const mockControl = "mock-secret-ctrl";
    const mock = createMockApp(mockControl);
    const mockServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: mock.app.fetch,
    });

    const webhookServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        const body = (await req.json()) as { hook: string; text: string };
        if (body.text.includes("FORBIDDEN_SECRET")) {
          return Response.json({
            action: "BLOCK",
            rule_code: "DLP_SECRET_BLOCK",
            http_status: 400,
            safe_message: "Response blocked by DLP webhook",
          });
        }
        return Response.json({ action: "PASS" });
      },
    });

    const { publicKey, privateKey } = generateKeyPairSync("ed25519");
    const pubPem = publicKey.export({ type: "spki", format: "pem" }).toString();
    const guard = getBuiltinPlugin("pii-guard");
    if (guard?.capability !== "CONTENT") throw new Error("Missing content plugin fixture");
    const wasmBytes = guard.wasm_bytes;
    const digest = new Bun.CryptoHasher("sha256").update(wasmBytes).digest("hex");
    const signature = sign(null, Buffer.from(digest, "utf8"), privateKey).toString("base64");

    const mockWasm: WasmRuntimeExecutor = {
      runtimeKind: "mock",
      compile() {},
      async invoke(_id, _hook, inputBytes) {
        const payload = JSON.parse(new TextDecoder().decode(inputBytes)) as { text: string };
        if (payload.text.includes("PHONE_999")) {
          return new TextEncoder().encode(
            JSON.stringify({
              action: "TRANSFORM",
              text: payload.text.replaceAll("PHONE_999", "[MASKED_PHONE]"),
            }),
          );
        }
        return new TextEncoder().encode(JSON.stringify({ action: "PASS" }));
      },
      close() {},
    };

    const runtime = createGatewayRuntime({
      config: makeConfig({ pluginSigningPublicKeyPem: pubPem }),
      db,
      wasmExecutor: mockWasm,
    });

    try {
      await runtime.start();
      const { data: billingAccount } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/billing-accounts",
        {
          name: "moderation-billingAccount",
          currency: "USD",
        },
      );
      const billingAccountId = String(billingAccount.id);
      const { data: keyRes } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/api-keys",
        {
          billing_account_id: billingAccountId,
          name: "mod-key",
        },
      );
      const apiKey = String(keyRes.key);

      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/wallets/${billingAccountId}/credits`,
        {
          currency: "USD",
          external_ref: "credit-mod-1",
          amount_atoms: "1000000",
          kind: "CASH",
        },
      );

      const { data: acc } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/upstream-accounts",
        { name: "mock-acc" },
      );
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/upstream-accounts/${acc.id}/recharges`,
        { currency: "USD", external_ref: "sup-mod-1", amount_atoms: "1000000" },
      );

      const { data: ch } = await internalCall(runtime.internalApp, "PUT", "/internal/v1/channels", {
        account_id: acc.id,
        name: "mock-openai",
        kind: "openai",
        base_url: `http://127.0.0.1:${mockServer.port}/openai`,
        api_key: "sk-mock",
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
            cached_input_tokens: { numerator: "5", denominator: "1" },
            output_tokens: { numerator: "20", denominator: "1" },
          },
          ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "100" },
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
            input_tokens: { numerator: "3", denominator: "1" },
            cached_input_tokens: { numerator: "1", denominator: "1" },
            output_tokens: { numerator: "6", denominator: "1" },
          },
          ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "100" },
        },
      );

      const { data: wasmPlugin } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "pii-mask",
          version: "1.0.0",
          kind: "WASM",
          wasm_bytes_base64: Buffer.from(wasmBytes).toString("base64"),
          signature,
          signing_key_id: "key-1",
          manifest: {
            hooks: ["on_response", "on_stream_chunk", "on_stream_end"],
            window_bytes: 16,
          },
        },
      );
      const { data: whAbsorb } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "dlp-webhook-absorb",
          version: "1.0.0",
          kind: "WEBHOOK",
          webhook_config: {
            url: `http://127.0.0.1:${webhookServer.port}/check`,
            stream_mode: "HOLD_UNTIL_APPROVED",
            window_chars: 128,
            charge_policy: "PLATFORM_ABSORB",
          },
        },
      );
      const { data: whCharge } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "dlp-webhook-charge",
          version: "1.0.0",
          kind: "WEBHOOK",
          webhook_config: {
            url: `http://127.0.0.1:${webhookServer.port}/check`,
            stream_mode: "HOLD_UNTIL_APPROVED",
            window_chars: 128,
            charge_policy: "CHARGE_CUSTOMER",
          },
        },
      );

      await publishPlugins(runtime, [
        [wasmPlugin.id, textSample],
        [whAbsorb.id, webhookSample],
        [whCharge.id, webhookSample],
      ]);

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "mod-absorb",
        operations: ["chat", "messages"],
        max_output_tokens: 100,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "mod-charge",
        operations: ["chat"],
        max_output_tokens: 100,
      });

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "mod-absorb",
        channel_id: ch.id,
        upstream_model: "mod-absorb-up",
        operations: ["chat", "messages"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        plugin_pipeline: [
          { plugin_id: String(wasmPlugin.id), priority: 1 },
          { plugin_id: String(whAbsorb.id), priority: 2, charge_policy: "PLATFORM_ABSORB" },
        ],
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "mod-charge",
        channel_id: ch.id,
        upstream_model: "mod-charge-up",
        operations: ["chat"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        plugin_pipeline: [{ plugin_id: String(whCharge.id), priority: 1 }],
      });

      mock.scenarios.set("mod-absorb-up", { output: "Call PHONE_999 now" });
      const redactedResp = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "mod-absorb",
            messages: [{ role: "user", content: "PROMPT_MUST_NOT_PERSIST_123" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(redactedResp.status).toBe(200);
      const redactedJson = (await redactedResp.json()) as {
        choices: { message: { content: string } }[];
      };
      expect(redactedJson.choices[0]!.message.content).toBe("Call [MASKED_PHONE] now");

      const anthropicCross = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/messages", {
          method: "POST",
          headers: { "x-api-key": apiKey, "content-type": "application/json" },
          body: JSON.stringify({
            model: "mod-absorb",
            max_tokens: 50,
            messages: [{ role: "user", content: "cross protocol test" }],
          }),
        }),
      );
      expect(anthropicCross.status).toBe(200);
      const anthropicJson = (await anthropicCross.json()) as {
        type: string;
        content: { type: string; text: string }[];
      };
      expect(anthropicJson.type).toBe("message");
      expect(anthropicJson.content[0]!.text).toBe("Call [MASKED_PHONE] now");

      const { data: walletBeforeBlock } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/wallets/${billingAccountId}?currency=USD`,
      );

      mock.scenarios.set("mod-absorb-up", { output: "Here is FORBIDDEN_SECRET data" });
      const absorbStream = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "mod-absorb",
            stream: true,
            messages: [{ role: "user", content: "leak test" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(absorbStream.status).toBe(200);
      const sseText = await absorbStream.text();
      expect(sseText).not.toContain("FORBIDDEN_SECRET");
      expect(sseText).toContain("DLP_SECRET_BLOCK");
      expect(sseText).not.toContain("[DONE]");

      const { data: walletAfterAbsorb } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/wallets/${billingAccountId}?currency=USD`,
      );
      expect(walletAfterAbsorb.spent_atoms).toBe(walletBeforeBlock.spent_atoms);
      expect(walletAfterAbsorb.reserved_atoms).toBe("0");

      mock.scenarios.set("mod-charge-up", { output: "Blocked FORBIDDEN_SECRET payload" });
      const chargeBlock = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "mod-charge",
            messages: [{ role: "user", content: "charge block test" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(chargeBlock.status).toBe(400);
      const { data: walletAfterChargeBlock } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/wallets/${billingAccountId}?currency=USD`,
      );
      const expectedSingleCost = 7 * 10 + 3 * 5 + 5 * 20;
      expect(BigInt(String(walletAfterChargeBlock.spent_atoms))).toBe(
        BigInt(String(walletAfterAbsorb.spent_atoms)) + BigInt(expectedSingleCost),
      );

      mock.scenarios.set("mod-charge-up", { output: "Safe streaming answer", truncate: true });
      const truncatedStream = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "mod-charge",
            stream: true,
            messages: [{ role: "user", content: "truncate test" }],
            max_tokens: 50,
          }),
        }),
      );
      const truncText = await truncatedStream.text();
      expect(truncText).toContain("event: error");
      expect(truncText).not.toContain("[DONE]");

      const reqId = truncatedStream.headers.get("x-request-id")!;
      const holdRows = await db`SELECT state FROM public.billing_holds WHERE request_id = ${reqId}`;
      expect(holdRows[0]!.state).toBe("UNKNOWN");

      await internalCall(runtime.internalApp, "POST", `/internal/v1/requests/${reqId}/resolve`, {
        action: "RELEASE",
        reason: "truncated_upstream_stream",
      });

      const rawDump = await db`
      SELECT coalesce(string_agg(payload::text, ' '), '') AS outbox_txt FROM public.outbox_events
    `;
      const chargesDump = await db`
      SELECT coalesce(string_agg(plugin_audit::text, ' '), '') AS audit_txt FROM public.billing_charges
    `;
      expect(String(rawDump[0]!.outbox_txt)).not.toContain("PROMPT_MUST_NOT_PERSIST_123");
      expect(String(rawDump[0]!.outbox_txt)).not.toContain("FORBIDDEN_SECRET");
      expect(String(chargesDump[0]!.audit_txt)).not.toContain("FORBIDDEN_SECRET");

      const { data: audit } = await internalCall(
        runtime.internalApp,
        "GET",
        "/internal/v1/audit/conservation",
      );
      expect(audit.ok).toBe(true);
      expect(audit.open_holds).toBe(0);
      expect(audit.unknown_holds).toBe(0);
    } finally {
      await runtime.close();
      await mockServer.stop(true);
      await webhookServer.stop(true);
    }
  });
}
