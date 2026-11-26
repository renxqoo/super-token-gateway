import { expect, test } from "bun:test";
import { getBuiltinPlugin, type WasmRuntimeExecutor } from "@gateway/plugin-engine";
import { createGatewayRuntime } from "@gateway/gateway";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";
import { makeConfig, db, internalCall } from "./cluster-and-media-fixture.ts";

export function registerMultimodalCases() {
  test("on_request TRANSFORM, multimodal endpoints (moderations, speech, transcriptions, images/edits), traceparent, paginated internal queries, and bounded multipart fingerprints", async () => {
    let lastUpstreamTraceparent: string | null = null;
    let lastUpstreamPrompt = "";
    const mock = createMockApp("media-ctrl");
    const mockServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        lastUpstreamTraceparent = req.headers.get("traceparent");
        if (req.url.endsWith("/openai/v1/chat/completions")) {
          const cloned = req.clone();
          const body = (await cloned.json()) as { messages?: { content?: string }[] };
          lastUpstreamPrompt = String(body.messages?.[0]?.content ?? "");
        }
        return mock.app.fetch(req);
      },
    });

    const mockWasm: WasmRuntimeExecutor = {
      runtimeKind: "mock",
      compile() {},
      async invoke(_id, hook, inputBytes) {
        const payload = JSON.parse(new TextDecoder().decode(inputBytes)) as { text: string };
        if (hook === "on_request" && payload.text.includes("alice@secret.io")) {
          return new TextEncoder().encode(
            JSON.stringify({
              action: "TRANSFORM",
              text: payload.text.replaceAll("alice@secret.io", "[REDACTED_EMAIL]"),
            }),
          );
        }
        return new TextEncoder().encode(JSON.stringify({ action: "PASS" }));
      },
      close() {},
    };

    const singleNodeCfg = makeConfig();
    delete singleNodeCfg.redisUrl;
    const runtime = createGatewayRuntime({
      config: singleNodeCfg,
      db,
      wasmExecutor: mockWasm,
    });

    try {
      await runtime.start();

      const badBillingAccount = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/billing-accounts",
        {
          name: "",
          currency: "invalid",
        },
      );
      expect(badBillingAccount.status).toBe(400);
      expect((badBillingAccount.data.error as { code: string }).code).toBe("validation_error");

      const { data: billingAccount } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/billing-accounts",
        {
          name: "media-proj",
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
          name: "media-key",
        },
      );
      const apiKey = String(keyRes.key);
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/wallets/${billingAccountId}/credits`,
        {
          currency: "USD",
          external_ref: "media-credit-1",
          amount_atoms: "1000000",
          kind: "CASH",
        },
      );
      const { data: acc } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/upstream-accounts",
        {
          name: "media-acc",
        },
      );
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/upstream-accounts/${acc.id}/recharges`,
        {
          currency: "USD",
          external_ref: "media-sup-1",
          amount_atoms: "1000000",
        },
      );
      const { data: ch } = await internalCall(runtime.internalApp, "PUT", "/internal/v1/channels", {
        account_id: acc.id,
        name: "media-openai",
        kind: "openai",
        base_url: `http://127.0.0.1:${mockServer.port}/openai`,
        api_key: "sk-media",
      });

      const builtin = getBuiltinPlugin("pii-guard");
      if (builtin?.capability !== "CONTENT") throw new Error("Missing content plugin fixture");
      const { data: plugin } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "req-redactor",
          version: "1.0.0",
          kind: "WASM",
          wasm_bytes_base64: Buffer.from(builtin.wasm_bytes).toString("base64"),
          manifest: { hooks: ["on_request"] },
        },
      );

      const activated = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/plugin-releases",
        {
          id: crypto.randomUUID(),
          expected_epoch: plugin.config_epoch,
          targets: [{ plugin_id: plugin.id }],
          samples: { [String(plugin.id)]: { text: "Email me at alice@secret.io please" } },
        },
      );
      expect(activated.status, JSON.stringify(activated.data)).toBe(200);

      const { data: tokenCustPrice } = await internalCall(
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
      const { data: tokenSupPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "SUPPLIER",
          currency: "USD",
          rates: {
            input_tokens: { numerator: "2", denominator: "1" },
            cached_input_tokens: { numerator: "1", denominator: "1" },
            output_tokens: { numerator: "4", denominator: "1" },
          },
          ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "100" },
        },
      );
      const { data: charCustPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "CUSTOMER",
          currency: "USD",
          basis: "INPUT_CHARACTERS",
          rates: { characters: { numerator: "3", denominator: "1" } },
          ceilings: { characters: "10000" },
        },
      );
      const { data: charSupPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "SUPPLIER",
          currency: "USD",
          basis: "INPUT_CHARACTERS",
          rates: { characters: { numerator: "1", denominator: "1" } },
          ceilings: { characters: "10000" },
        },
      );
      const { data: audioSecCustPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "CUSTOMER",
          currency: "USD",
          rates: { audio_seconds: { numerator: "80", denominator: "1" } },
          ceilings: { audio_seconds: "600" },
        },
      );
      const { data: audioSecSupPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "SUPPLIER",
          currency: "USD",
          rates: { audio_seconds: { numerator: "16", denominator: "1" } },
          ceilings: { audio_seconds: "600" },
        },
      );

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "omni-model",
        operations: ["chat", "moderations", "image_edits"],
        max_output_tokens: 100,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "tts-1",
        operations: ["speech"],
        max_output_tokens: 100,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "whisper-1",
        operations: ["transcription"],
        max_output_tokens: 100,
      });

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "omni-model",
        channel_id: ch.id,
        upstream_model: "omni-up",
        operations: ["chat", "moderations", "image_edits"],
        billing_price_id: tokenCustPrice.id,
        cost_version_id: tokenSupPrice.id,
        plugin_pipeline: [{ plugin_id: String(plugin.id), priority: 1 }],
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "tts-1",
        channel_id: ch.id,
        upstream_model: "tts-up",
        operations: ["speech"],
        billing_price_id: charCustPrice.id,
        cost_version_id: charSupPrice.id,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "whisper-1",
        channel_id: ch.id,
        upstream_model: "whisper-up",
        operations: ["transcription"],
        billing_price_id: audioSecCustPrice.id,
        cost_version_id: audioSecSupPrice.id,
      });

      const incomingTrace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
      const chatResp = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/chat/completions", {
          method: "POST",
          headers: {
            authorization: `Bearer ${apiKey}`,
            "content-type": "application/json",
            traceparent: incomingTrace,
          },
          body: JSON.stringify({
            model: "omni-model",
            messages: [{ role: "user", content: "Email me at alice@secret.io please" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(chatResp.status).toBe(200);
      expect(lastUpstreamPrompt).toBe("Email me at [REDACTED_EMAIL] please");
      expect(chatResp.headers.get("x-trace-id")).toBe("4bf92f3577b34da6a3ce929d0e0e4736");
      expect(lastUpstreamTraceparent).toStartWith("00-4bf92f3577b34da6a3ce929d0e0e4736-");
      const chatReqId = chatResp.headers.get("x-request-id")!;

      const modResp = await runtime.fetchPublic(
        new Request("http://gw.local/v1/moderations", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({ model: "omni-model", input: "safe text" }),
        }),
        { getPeerAddress: () => "127.0.0.1" },
      );
      expect(modResp.status).toBe(200);
      const livezResp = await runtime.fetchPublic(new Request("http://gw.local/livez"));
      expect(livezResp.status).toBe(200);

      const speechBin = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/audio/speech", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({ model: "tts-1", input: "Hello audio!" }),
        }),
      );
      expect(speechBin.status).toBe(200);
      expect(speechBin.headers.get("content-type")).toContain("audio/mpeg");
      expect(await speechBin.text()).toBe("mock-generated-audio-content");

      const form = new FormData();
      form.append("model", "whisper-1");
      form.append("stream", "true");
      form.append("file", new File(["fake-wav-bytes"], "sample.wav", { type: "audio/wav" }));
      const transcribeSse = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/audio/transcriptions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}` },
          body: form,
        }),
      );
      expect(transcribeSse.status).toBe(200);
      const sseBody = await transcribeSse.text();
      expect(sseBody).toContain("transcript.text.done");

      const editForm = new FormData();
      editForm.append("model", "omni-model");
      editForm.append("prompt", "make it brighter");
      editForm.append("image", new File(["png-bytes"], "in.png", { type: "image/png" }));
      const editResp = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/images/edits", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}` },
          body: editForm,
        }),
      );
      expect(editResp.status).toBe(200);
      const large = new FormData();
      large.append("model", "whisper-1");
      large.append("file", new File(["short"], "voice.wav", { type: "audio/wav" }));
      large.append("prompt", "x".repeat(2 * 1024 * 1024));
      const tooLarge = await runtime.publicApp.fetch(
        new Request("http://gw.local/v1/audio/transcriptions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}` },
          body: large,
        }),
      );
      expect(tooLarge.status).toBe(413);

      const metricsRes = await runtime.publicApp.fetch(new Request("http://gw.local/metrics"));
      const metricsTxt = await metricsRes.text();
      expect(metricsTxt).toContain("gateway_request_duration_ms_bucket");
      expect(metricsTxt).toContain("gateway_ttft_ms_bucket");
      expect(metricsTxt).toContain('gateway_http_requests_total{operation="chat"');
      expect(metricsTxt).toContain('gateway_http_requests_total{operation="speech"');

      const { data: chargesList } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/charges?billing_account_id=${billingAccountId}&limit=10`,
      );
      expect(chargesList.total).toBe(5);
      expect((chargesList.items as unknown[]).length).toBe(5);
      const transcribeWithKey = (bytes: string) => {
        const fileForm = new FormData();
        fileForm.append("model", "whisper-1");
        fileForm.append("file", new File([bytes], "voice.wav", { type: "audio/wav" }));
        return runtime.publicApp.fetch(
          new Request("http://gw.local/v1/audio/transcriptions", {
            method: "POST",
            headers: { authorization: `Bearer ${apiKey}`, "idempotency-key": "same-file-name" },
            body: fileForm,
          }),
        );
      };
      expect((await transcribeWithKey("voice A")).status).toBe(200);
      const differentFile = await transcribeWithKey("voice B");
      expect(differentFile.status).toBe(409);
      expect(await differentFile.json()).toMatchObject({ error: { code: "idempotency_conflict" } });

      const { data: reqDetail } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/requests/${chatReqId}`,
      );
      expect(reqDetail.hold).toBeTruthy();
      expect(reqDetail.charge).toBeTruthy();
      expect((reqDetail.entries as unknown[]).length).toBeGreaterThanOrEqual(4);

      const { data: entriesList } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/ledger-entries?request_id=${chatReqId}`,
      );
      expect(entriesList.total).toBeGreaterThanOrEqual(4);

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
    }
  });
}
