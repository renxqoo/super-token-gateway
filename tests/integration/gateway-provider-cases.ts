import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { exerciseWasmProviderSetup } from "./gateway-provider-wasm-setup.ts";
import {
  exerciseProviderPluginReleases,
  type ProviderCapture,
} from "./gateway-provider-plugin-releases.ts";
import { makeConfig, db, internalCall, publishPlugins, textSample } from "./gateway-fixture.ts";

export function registerGatewayProviderCases() {
  test("G7.2-G7.7 Provider Plugin Architecture: 15 builtins, online unknown vendor, hot-override, fallback enforces its own input policy and output ceiling, fake-200 and SSE first-frame failover, real WASM proprietary provider, HMAC auth, and dry-run preview", async () => {
    const { createHmac } = await import("node:crypto");
    const { resolve } = await import("node:path");
    const wasmFixturePath = resolve(import.meta.dir, "../fixtures/example-provider.wasm");
    const wasmProviderBytes = new Uint8Array(await Bun.file(wasmFixturePath).arrayBuffer());

    const captures: ProviderCapture[] = [];
    let failoverPrimaryDown = true;
    let minimaxFake200Mode: "rate_limit" | "ok" = "rate_limit";
    let acmeWasmMode: "ok" | "evil_usage" = "ok";
    let acmeStreamTerminates = true;

    const upstream = Bun.serve({
      port: 0,
      async fetch(req) {
        const url = new URL(req.url);
        const rawBody = await req.text();
        const body = rawBody ? (JSON.parse(rawBody) as Record<string, unknown>) : {};
        const hdrs: Record<string, string> = {};
        req.headers.forEach((v, k) => {
          hdrs[k.toLowerCase()] = v;
        });
        captures.push({ path: url.pathname, query: url.search, headers: hdrs, body });

        if (url.pathname === "/v2/infer/chat") {
          expect(hdrs["x-acme-key"]).toBe("Key sk-acme-secret-1");
          expect(hdrs["x-client-app"]).toBe("super-token-gateway");
          expect(url.searchParams.get("api_ver")).toBe("2026");
          return Response.json({
            id: "acme-1",
            object: "chat.completion",
            model: String(body.model ?? "acme-m1"),
            choices: [
              {
                index: 0,
                message: { role: "assistant", content: "from-acme-declarative" },
                finish_reason: "stop",
              },
            ],
            usage: { prompt_tokens: 10, completion_tokens: 5, cache_hit_tokens: 4 },
          });
        }

        if (url.pathname === "/v1/hmac/chat") {
          const ts = hdrs["x-hmac-ts"] ?? "";
          const sig = hdrs["x-hmac-sig"] ?? "";
          const keyId = hdrs["x-hmac-key-id"] ?? "";
          const bodyHash = new Bun.CryptoHasher("sha256").update(rawBody).digest("hex");
          const expected = createHmac("sha256", "hmac-secret-xyz")
            .update(`POST\n${url.pathname}${url.search}\n${ts}\n${bodyHash}`)
            .digest("hex");
          expect(keyId).toBe("ak-99");
          expect(sig).toBe(`v1=${expected}`);
          return Response.json({
            id: "hmac-1",
            object: "chat.completion",
            model: "hmac-m",
            choices: [
              {
                index: 0,
                message: { role: "assistant", content: "hmac-verified" },
                finish_reason: "stop",
              },
            ],
            usage: { prompt_tokens: 4, completion_tokens: 2 },
          });
        }

        if (url.pathname === "/v1/primary-fail") {
          if (failoverPrimaryDown) {
            return Response.json({ error: { message: "primary busy" } }, { status: 503 });
          }
          return Response.json({
            id: "p1",
            object: "chat.completion",
            model: "o3-up",
            choices: [
              {
                index: 0,
                message: { role: "assistant", content: "from-primary" },
                finish_reason: "stop",
              },
            ],
            usage: { prompt_tokens: 5, completion_tokens: 3 },
          });
        }

        if (url.pathname === "/v1/text/chatcompletion_v2") {
          if (minimaxFake200Mode === "rate_limit") {
            if (body.stream === true) {
              return new Response(
                'data: {"base_resp":{"status_code":1002,"status_msg":"temporary vendor throttle"}}\n\n',
                {
                  headers: { "content-type": "text/event-stream" },
                },
              );
            }
            return Response.json(
              { base_resp: { status_code: 1002, status_msg: "minimax fake 200 rate limit" } },
              { status: 200 },
            );
          }
          return Response.json({
            id: "mm-1",
            object: "chat.completion",
            model: "MiniMax-Text-01",
            choices: [
              {
                index: 0,
                message: { role: "assistant", content: "from-minimax" },
                finish_reason: "stop",
              },
            ],
            usage: { prompt_tokens: 6, completion_tokens: 4 },
          });
        }

        if (url.pathname === "/v9/proprietary/generate") {
          expect(hdrs.authorization).toBe("Bearer sk-secret-acme-999");
          expect(hdrs["x-acme-proto"]).toBe("v9");
          expect(hdrs.host).not.toBe("spoofed.invalid");
          expect(body.secret_seen_in_wasm).toBe(false);
          expect(url.searchParams.get("fmt")).toBe("acme_v9");
          if (acmeWasmMode === "evil_usage") {
            return Response.json({ evil_usage: true });
          }
          return Response.json({
            answer: `wasm-echo:${String(body.prompt_text ?? "")}`,
            tok_in: 10,
            tok_out: 6,
            tok_cached: 4,
          });
        }

        if (url.pathname === "/v9/proprietary/stream") {
          expect(hdrs.authorization).toBe("Bearer sk-secret-acme-999");
          expect(hdrs["x-acme-proto"]).toBe("v9");
          expect(body.streaming).toBe(true);
          expect(body.secret_seen_in_wasm).toBe(false);
          const enc = new TextEncoder();
          const stream = new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(enc.encode(`data: ${JSON.stringify({ piece: "Hello " })}\n\n`));
              controller.enqueue(enc.encode(`data: ${JSON.stringify({ piece: "WASM!" })}\n\n`));
              controller.enqueue(
                enc.encode(
                  `data: ${JSON.stringify({ finish: acmeStreamTerminates, tok_in: 8, tok_out: 4 })}\n\n`,
                ),
              );
              controller.close();
            },
          });
          return new Response(stream, {
            status: 200,
            headers: { "content-type": "text/event-stream; charset=utf-8" },
          });
        }

        if (body.stream === true && body.model === "bak-mm-sse") {
          const frames = [
            {
              id: "bak-sse",
              object: "chat.completion.chunk",
              choices: [{ index: 0, delta: { content: "backup stream" }, finish_reason: null }],
            },
            {
              id: "bak-sse",
              object: "chat.completion.chunk",
              choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
            },
            {
              id: "bak-sse",
              object: "chat.completion.chunk",
              choices: [],
              usage: { prompt_tokens: 10, completion_tokens: 5 },
            },
          ];
          return new Response(
            frames.map((frame) => `data: ${JSON.stringify(frame)}\n\n`).join("") +
              "data: [DONE]\n\n",
            {
              headers: { "content-type": "text/event-stream" },
            },
          );
        }

        return Response.json({
          id: "std-1",
          object: "chat.completion",
          model: String(body.model ?? "std"),
          choices: [
            {
              index: 0,
              message: { role: "assistant", content: "from-backup" },
              finish_reason: "stop",
            },
          ],
          usage: { prompt_tokens: 10, completion_tokens: 5 },
        });
      },
    });

    const runtime = createGatewayRuntime({ config: makeConfig(), db });

    try {
      await runtime.start();

      const { acc, custPrice, supPrice, apiKey } = await exerciseProviderPluginReleases({
        runtime,
        upstream,
        captures,
      });

      const { data: backupContentPolicy } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/plugins/builtins/pii-guard/install",
      );
      if (!backupContentPolicy.enabled)
        await publishPlugins(runtime, [[backupContentPolicy.id, textSample]]);
      const { data: chPri } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/channels",
        {
          account_id: acc.id,
          name: "failover-pri",
          kind: "openai",
          base_url: `http://127.0.0.1:${upstream.port}`,
          api_key: "sk-pri",
          adapter_config: { path_override: "/v1/primary-fail" },
        },
      );
      const { data: chBak } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/channels",
        {
          account_id: acc.id,
          name: "failover-bak",
          kind: "openai_compatible",
          base_url: `http://127.0.0.1:${upstream.port}`,
          api_key: "sk-bak",
        },
      );
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "o3-failover",
        operations: ["chat"],
        max_output_tokens: 200,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "o3-failover",
        channel_id: chPri.id,
        upstream_model: "o3-up",
        operations: ["chat"],
        priority: 10,
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        adapter_overrides: {
          request_transforms: [
            { op: "drop", path: "temperature" },
            { op: "force_set", path: "a_flag", value: true },
          ],
        },
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "o3-failover",
        channel_id: chBak.id,
        upstream_model: "bak-up",
        operations: ["chat"],
        priority: 1,
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        plugin_pipeline: [{ plugin_id: backupContentPolicy.id, priority: 10 }],
        adapter_overrides: {
          request_transforms: [
            { op: "rename", from: "max_completion_tokens", to: "max_tokens", overwrite: false },
            { op: "force_set", path: "b_flag", value: true },
          ],
        },
      });

      const capIdxBeforeFo = captures.length;
      const foResp = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "o3-failover",
            messages: [{ role: "user", content: "isolate alice@example.com" }],
            temperature: 0.7,
            max_completion_tokens: 500,
          }),
        }),
      );
      expect(foResp.status).toBe(200);
      expect(foResp.headers.get("x-gateway-channel")).toBe("failover-bak");
      const priCap = captures[capIdxBeforeFo]!;
      const bakCap = captures[capIdxBeforeFo + 1]!;
      expect(priCap.body.temperature).toBeUndefined();
      expect(priCap.body.a_flag).toBe(true);
      expect(priCap.body.max_completion_tokens).toBe(200);
      expect(bakCap.body.temperature).toBe(0.7);
      expect(bakCap.body.a_flag).toBeUndefined();
      expect(bakCap.body.b_flag).toBe(true);
      expect(bakCap.body.max_tokens).toBe(200);
      expect(JSON.stringify(bakCap.body.messages)).not.toContain("alice@example.com");
      expect(bakCap.body.max_completion_tokens).toBeUndefined();
      failoverPrimaryDown = false;

      const { data: chMm } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/channels",
        {
          account_id: acc.id,
          name: "minimax-pri",
          kind: "minimax",
          base_url: `http://127.0.0.1:${upstream.port}`,
          api_key: "sk-mm",
        },
      );
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "mm-failover",
        operations: ["chat"],
        max_output_tokens: 200,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "mm-failover",
        channel_id: chMm.id,
        upstream_model: "MiniMax-Text-01",
        operations: ["chat"],
        priority: 10,
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "mm-failover",
        channel_id: chBak.id,
        upstream_model: "bak-mm",
        operations: ["chat"],
        priority: 1,
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
      });

      minimaxFake200Mode = "rate_limit";
      const mmFoResp = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "mm-failover",
            messages: [{ role: "user", content: "fake 200 failover" }],
          }),
        }),
      );
      expect(mmFoResp.status).toBe(200);
      expect(mmFoResp.headers.get("x-gateway-channel")).toBe("failover-bak");
      const mmHealth = (
        await internalCall(runtime.internalApp, "GET", "/internal/v1/health/channels")
      ).data.channels as { channel_id: string; state: string; cooldown_until_ms: number }[];
      expect(mmHealth.find((c) => c.channel_id === chMm.id)?.cooldown_until_ms).toBeGreaterThan(0);

      const { data: chMmSse } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/channels",
        {
          account_id: acc.id,
          name: "minimax-sse-pri",
          kind: "minimax",
          base_url: `http://127.0.0.1:${upstream.port}`,
          api_key: "sk-mm-sse",
        },
      );
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "mm-stream-failover",
        operations: ["chat"],
        max_output_tokens: 200,
      });
      for (const [channel, name, priority] of [
        [chMmSse, "MiniMax-Text-01", 10],
        [chBak, "bak-mm-sse", 1],
      ] as const) {
        await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
          model_name: "mm-stream-failover",
          channel_id: channel.id,
          upstream_model: name,
          operations: ["chat"],
          priority,
          billing_price_id: custPrice.id,
          cost_version_id: supPrice.id,
        });
      }
      const streamFailover = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "mm-stream-failover",
            stream: true,
            messages: [{ role: "user", content: "stream prelude" }],
          }),
        }),
      );
      const streamBody = await streamFailover.text();
      expect(streamFailover.headers.get("x-gateway-channel")).toBe("failover-bak");
      expect(streamBody).toContain("backup stream");
      expect(streamBody.trimEnd()).toEndWith("data: [DONE]");
      expect(streamBody).not.toContain("temporary vendor throttle");

      const { wasmRequestSample } = await exerciseWasmProviderSetup({
        runtime,
        wasmProviderBytes,
        acc,
        upstream,
        custPrice,
        supPrice,
        apiKey,
      });

      acmeStreamTerminates = false;
      const truncatedStream = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "acme-wasm-model",
            stream: true,
            messages: [{ role: "user", content: "metered stream without a terminal" }],
          }),
        }),
      );
      const truncatedWire = await truncatedStream.text();
      expect(truncatedWire).toContain("event: error");
      expect(truncatedWire).not.toContain("[DONE]");
      const { data: truncatedDetails } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/requests/${truncatedStream.headers.get("x-request-id")}`,
      );
      const truncatedHold = truncatedDetails.hold as { state: string; heldAtoms: string };
      const truncatedCharge = truncatedDetails.charge as { customerAtoms: string } | null;
      expect(truncatedHold.state).toBe("SETTLED");
      expect(truncatedCharge).not.toBeNull();
      expect(BigInt(truncatedCharge!.customerAtoms) > 0n).toBe(true);
      expect(BigInt(truncatedCharge!.customerAtoms) <= BigInt(truncatedHold.heldAtoms)).toBe(true);

      const capCountBeforeEscape = captures.length;
      const escapeResp = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "acme-wasm-model",
            messages: [{ role: "user", content: "TRIGGER_HOST_ESCAPE" }],
          }),
        }),
      );
      expect(escapeResp.status).toBe(502);
      expect(captures.length).toBe(capCountBeforeEscape);

      acmeWasmMode = "evil_usage";
      const evilResp = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "acme-wasm-model",
            messages: [{ role: "user", content: "evil usage" }],
          }),
        }),
      );
      expect(evilResp.status).toBe(502);
      const evilReqId = evilResp.headers.get("x-request-id")!;
      const { data: evilBill } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/requests/${evilReqId}`,
      );
      expect((evilBill.hold as { state: string }).state).toBe("UNKNOWN");
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/requests/${evilReqId}/resolve`,
        {
          action: "RELEASE",
          reason: "test cleanup after rejected negative usage",
        },
      );
      acmeWasmMode = "ok";

      const { status: ungrantedRegStatus, data: ungrantedWasm } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "acme-no-grant",
          version: "1.0.0",
          capability: "PROVIDER",
          kind: "WASM",
          wasm_bytes_base64: Buffer.from(wasmProviderBytes).toString("base64"),
          provider_config: {
            base_dialect: "passthrough",
            usage_dialect: "custom",
            auth: { scheme: "bearer" },
            hooks: ["adapt_request", "adapt_response"],
            grants: [],
          },
        },
      );
      expect(ungrantedRegStatus).toBe(200);
      await publishPlugins(runtime, [
        [
          ungrantedWasm.id,
          {
            request: wasmRequestSample,
            response: { err_code: 9002 },
          },
        ],
      ]);
      const { data: chUngranted } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/channels",
        {
          account_id: acc.id,
          name: "acme-ungranted-ch",
          kind: "acme-no-grant",
          provider_plugin_id: ungrantedWasm.id,
          base_url: `http://127.0.0.1:${upstream.port}`,
          api_key: "sk-secret-acme-999",
        },
      );
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "acme-ungranted-model",
        operations: ["chat"],
        max_output_tokens: 200,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "acme-ungranted-model",
        channel_id: chUngranted.id,
        upstream_model: "acme-engine-9",
        operations: ["chat"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
      });
      const ungrantedResp = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "acme-ungranted-model",
            messages: [{ role: "user", content: "no extract_usage grant" }],
          }),
        }),
      );
      expect(ungrantedResp.status).toBe(502);
      const ungrantedReqId = ungrantedResp.headers.get("x-request-id")!;
      const { data: ungrantedBill } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/requests/${ungrantedReqId}`,
      );
      expect((ungrantedBill.hold as { state: string }).state).toBe("UNKNOWN");
      const ungrantedHealth = runtime.router
        .snapshot()
        .find((s) => s.channel_id === String(chUngranted.id));
      expect(ungrantedHealth?.state ?? "CLOSED").toBe("CLOSED");
      expect(ungrantedHealth?.consecutive_failures ?? 0).toBe(0);
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/requests/${ungrantedReqId}/resolve`,
        {
          action: "RELEASE",
          reason: "test cleanup after ungranted usage rejection",
        },
      );

      const capCountBeforePreview = captures.length;
      const { status: prev1Status, data: prev1 } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/providers/preview",
        {
          provider: "deepseek",
          base_url: "https://api.deepseek.com",
          operation: "chat",
          upstream_model: "deepseek-reasoner",
          stream: false,
          client_body: {
            model: "ds-r1",
            messages: [{ role: "user", content: "hi" }],
            max_completion_tokens: 512,
            temperature: 1.5,
          },
          adapter_overrides: {
            request_transforms: [{ op: "drop", path: "temperature" }],
          },
          mock_upstream_response: {
            status: 200,
            body: {
              choices: [
                {
                  index: 0,
                  message: { role: "assistant", content: "ok" },
                  finish_reason: "stop",
                },
              ],
              usage: { prompt_tokens: 10, completion_tokens: 5, prompt_cache_hit_tokens: 4 },
            },
          },
        },
      );
      expect(prev1Status).toBe(200);
      expect((prev1.resolved_provider as { source: string }).source).toBe("db_override");
      expect(
        (prev1.prepared_request as { headers: Record<string, string> }).headers.authorization,
      ).toBe("Bearer <REDACTED>");
      expect((prev1.prepared_request as { body: Record<string, unknown> }).body.max_tokens).toBe(
        512,
      );
      expect(
        (prev1.prepared_request as { body: Record<string, unknown> }).body.temperature,
      ).toBeUndefined();
      expect(
        (prev1.decoded_response as { extracted_usage: Record<string, string> }).extracted_usage,
      ).toEqual({
        input_tokens: "6",
        output_tokens: "5",
        cached_input_tokens: "4",
      });

      const { status: prev2Status, data: prev2 } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/providers/preview",
        {
          provider: "minimax",
          base_url: "https://api.minimax.chat",
          operation: "chat",
          upstream_model: "MiniMax-Text-01",
          stream: false,
          client_body: {
            model: "mm",
            messages: [{ role: "user", content: "test" }],
          },
          mock_upstream_response: {
            status: 200,
            body: { base_resp: { status_code: 1002, status_msg: "throttled" } },
          },
        },
      );
      expect(prev2Status).toBe(200);
      expect((prev2.resolved_provider as { source: string }).source).toBe("builtin");
      expect((prev2.decoded_response as { retry_kind: string }).retry_kind).toBe("rate_limit");
      expect(captures.length).toBe(capCountBeforePreview);

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
      await upstream.stop(true);
    }
  });
}
