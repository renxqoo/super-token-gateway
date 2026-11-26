import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { makeConfig, db, internalCall } from "./cluster-and-media-fixture.ts";

export function registerMinimaxCases() {
  test("declarative MiniMax-style routes cover Anthropic, OpenAI, speech, transcription, images, music, videos, and HTTP-200 base_resp error hold release", async () => {
    const seen: { path: string; auth: string | null; xApiKey: string | null; model?: unknown }[] =
      [];
    const mockUpstream = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        const url = new URL(req.url);
        const auth = req.headers.get("authorization");
        const xApiKey = req.headers.get("x-api-key");
        const contentType = req.headers.get("content-type") ?? "";
        if (url.pathname === "/anthropic/v1/messages") {
          const body = (await req.json()) as Record<string, unknown>;
          seen.push({ path: url.pathname, auth, xApiKey, model: body.model });
          if (body.stream === true) {
            const sse = [
              `event: message_start\ndata: ${JSON.stringify({ type: "message_start", message: { id: "msg_mm_1", type: "message", role: "assistant", model: String(body.model), content: [], usage: { input_tokens: 12, output_tokens: 0 } } })}\n\n`,
              `event: content_block_start\ndata: ${JSON.stringify({ type: "content_block_start", index: 0, content_block: { type: "text", text: "" } })}\n\n`,
              `event: content_block_delta\ndata: ${JSON.stringify({ type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "ok-anth" } })}\n\n`,
              `event: content_block_stop\ndata: ${JSON.stringify({ type: "content_block_stop", index: 0 })}\n\n`,
              `event: message_delta\ndata: ${JSON.stringify({ type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 6 } })}\n\n`,
              `event: message_stop\ndata: ${JSON.stringify({ type: "message_stop" })}\n\n`,
            ].join("");
            return new Response(sse, { headers: { "content-type": "text/event-stream" } });
          }
          return Response.json({
            id: "msg_mm_1",
            type: "message",
            role: "assistant",
            model: String(body.model),
            content: [{ type: "text", text: "hello-from-minimax-anthropic" }],
            stop_reason: "end_turn",
            usage: { input_tokens: 12, output_tokens: 8 },
          });
        }
        if (url.pathname === "/v1/chat/completions") {
          const body = (await req.json()) as Record<string, unknown>;
          seen.push({ path: url.pathname, auth, xApiKey, model: body.model });
          if (
            Array.isArray(body.messages) &&
            String((body.messages[0] as { content?: string })?.content ?? "").includes(
              "TRIGGER_2049",
            )
          ) {
            return Response.json({
              base_resp: { status_code: 2049, status_msg: "invalid api key" },
            });
          }
          if (body.stream === true) {
            const sse = [
              `data: ${JSON.stringify({ id: "cmpl_mm_1", object: "chat.completion.chunk", model: String(body.model), choices: [{ index: 0, delta: { role: "assistant", content: "ok-oai" }, finish_reason: null }] })}\n\n`,
              `data: ${JSON.stringify({ id: "cmpl_mm_1", object: "chat.completion.chunk", model: String(body.model), choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 10, completion_tokens: 4, total_tokens: 14 } })}\n\n`,
              "data: [DONE]\n\n",
            ].join("");
            return new Response(sse, { headers: { "content-type": "text/event-stream" } });
          }
          return Response.json({
            id: "cmpl_mm_1",
            object: "chat.completion",
            model: String(body.model),
            choices: [
              {
                index: 0,
                message: { role: "assistant", content: "hello-from-minimax-openai" },
                finish_reason: "stop",
              },
            ],
            usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
            base_resp: { status_code: 0, status_msg: "success" },
          });
        }
        if (url.pathname === "/v1/t2a_v2") {
          const body = (await req.json()) as Record<string, unknown>;
          seen.push({ path: url.pathname, auth, xApiKey, model: body.model });
          return Response.json({
            data: { audio: "494433030000", status: 2 },
            extra_info: { audio_length: 2500, usage_characters: 11 },
            base_resp: { status_code: 0, status_msg: "success" },
          });
        }
        if (url.pathname === "/v1/speech_to_text") {
          const model = contentType.includes("multipart")
            ? String((await req.formData()).get("model"))
            : String(((await req.json()) as Record<string, unknown>).model);
          seen.push({ path: url.pathname, auth, xApiKey, model });
          return Response.json({
            text: "transcribed-by-minimax",
            extra_info: { audio_length: 3000 },
            base_resp: { status_code: 0, status_msg: "success" },
          });
        }
        if (url.pathname === "/v1/image_generation") {
          const body = (await req.json()) as Record<string, unknown>;
          seen.push({ path: url.pathname, auth, xApiKey, model: body.model });
          return Response.json({
            id: "img_mm_1",
            data: { image_urls: ["https://cdn.example/img1.png"] },
            metadata: { success_count: 2, failed_count: 0 },
            base_resp: { status_code: 0, status_msg: "success" },
          });
        }
        if (url.pathname === "/v1/music_generation") {
          const body = (await req.json()) as Record<string, unknown>;
          seen.push({ path: url.pathname, auth, xApiKey, model: body.model });
          return Response.json({
            data: { audio: "https://cdn.example/song.mp3", status: 2 },
            extra_info: { music_duration: 12500 },
            base_resp: { status_code: 0, status_msg: "success" },
          });
        }
        if (url.pathname === "/v2/video_generation") {
          const body = (await req.json()) as Record<string, unknown>;
          seen.push({ path: url.pathname, auth, xApiKey, model: body.model });
          return Response.json({
            task_id: "vid_task_12345",
            base_resp: { status_code: 0, status_msg: "success" },
          });
        }
        return Response.json({ error: `unexpected path ${url.pathname}` }, { status: 404 });
      },
    });

    const cfg = makeConfig();
    delete cfg.redisUrl;
    const runtime = createGatewayRuntime({ config: cfg, db, wasmExecutor: null });

    try {
      await runtime.start();
      const { data: billingAccount } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/billing-accounts",
        {
          name: "mm-mock-proj",
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
          name: "mm-mock-key",
        },
      );
      const apiKey = String(keyRes.key);
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/wallets/${billingAccountId}/credits`,
        {
          currency: "USD",
          external_ref: "mm-mock-credit",
          amount_atoms: "10000000",
          kind: "CASH",
        },
      );
      const { data: acc } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/upstream-accounts",
        { name: "mm-mock-acc" },
      );
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/upstream-accounts/${acc.id}/recharges`,
        { currency: "USD", external_ref: "mm-mock-sup", amount_atoms: "10000000" },
      );

      const mmErrorRules = [
        {
          when_http_status: [200],
          path: "base_resp.status_code",
          equals: 1002,
          retry_kind: "rate_limit",
          message_path: "base_resp.status_msg",
        },
        {
          when_http_status: [200],
          path: "base_resp.status_code",
          not_equals: 0,
          retry_kind: "client_error",
          http_status: 400,
          message_path: "base_resp.status_msg",
        },
      ];

      const { data: mmPlugin } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "minimax_suite",
          version: "1.0.0",
          capability: "PROVIDER",
          kind: "DECLARATIVE",
          provider_config: {
            base_dialect: "openai_chat",
            usage_dialect: "openai",
            stream_framing: "sse",
            auth: { scheme: "bearer" },
            include_stream_usage_option: true,
            error_rules: mmErrorRules,
            usage_aliases: [
              { target: "images", source_path: "metadata.success_count" },
              { target: "music_ms", source_path: "extra_info.music_duration" },
              { target: "characters", source_path: "extra_info.usage_characters" },
              { target: "audio_ms", source_path: "extra_info.audio_length" },
            ],
            endpoints: {
              chat: { path: "/v1/chat/completions" },
              speech: { path: "/v1/t2a_v2" },
              transcription: { path: "/v1/speech_to_text" },
              images: { path: "/v1/image_generation" },
              music: { path: "/v1/music_generation" },
              videos: { path: "/v2/video_generation" },
              default: { path: "/v1/chat/completions" },
            },
          },
        },
      );
      const rel = await internalCall(runtime.internalApp, "POST", "/internal/v1/plugin-releases", {
        id: crypto.randomUUID(),
        expected_epoch: mmPlugin.config_epoch,
        targets: [{ plugin_id: mmPlugin.id }],
      });
      expect(rel.status, JSON.stringify(rel.data)).toBe(200);

      const { data: chAnth } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/channels",
        {
          account_id: acc.id,
          name: "mm-anthropic",
          kind: "anthropic",
          base_url: `http://127.0.0.1:${mockUpstream.port}/anthropic`,
          api_key: "sk-mm-test",
        },
      );
      const { data: chSuite } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/channels",
        {
          account_id: acc.id,
          name: "mm-suite",
          kind: "minimax_suite",
          provider_plugin_id: mmPlugin.id,
          base_url: `http://127.0.0.1:${mockUpstream.port}`,
          api_key: "sk-mm-test",
        },
      );

      const mkPrice = async (
        side: "CUSTOMER" | "SUPPLIER",
        basis: "PROVIDER_USAGE" | "REQUEST_COMPLETION" | "INPUT_CHARACTERS",
        rates: Record<string, { numerator: string; denominator: string }>,
        ceilings: Record<string, string>,
      ) => {
        const { data } = await internalCall(runtime.internalApp, "POST", "/internal/v1/prices", {
          side,
          currency: "USD",
          basis,
          rates,
          ceilings,
        });
        return String(data.id);
      };

      const tokCust = await mkPrice(
        "CUSTOMER",
        "PROVIDER_USAGE",
        {
          input_tokens: { numerator: "10", denominator: "1" },
          output_tokens: { numerator: "20", denominator: "1" },
        },
        { input_tokens: "1000", output_tokens: "200" },
      );
      const tokSup = await mkPrice(
        "SUPPLIER",
        "PROVIDER_USAGE",
        {
          input_tokens: { numerator: "2", denominator: "1" },
          output_tokens: { numerator: "4", denominator: "1" },
        },
        { input_tokens: "1000", output_tokens: "200" },
      );
      const speechCust = await mkPrice(
        "CUSTOMER",
        "PROVIDER_USAGE",
        { characters: { numerator: "5", denominator: "1" } },
        { characters: "10000" },
      );
      const speechSup = await mkPrice(
        "SUPPLIER",
        "PROVIDER_USAGE",
        { characters: { numerator: "1", denominator: "1" } },
        { characters: "10000" },
      );
      const sttCust = await mkPrice(
        "CUSTOMER",
        "PROVIDER_USAGE",
        { audio_ms: { numerator: "1", denominator: "10" } },
        { audio_ms: "600000" },
      );
      const sttSup = await mkPrice(
        "SUPPLIER",
        "PROVIDER_USAGE",
        { audio_ms: { numerator: "1", denominator: "100" } },
        { audio_ms: "600000" },
      );
      const imgCust = await mkPrice(
        "CUSTOMER",
        "PROVIDER_USAGE",
        { images: { numerator: "500", denominator: "1" } },
        { images: "10" },
      );
      const imgSup = await mkPrice(
        "SUPPLIER",
        "PROVIDER_USAGE",
        { images: { numerator: "100", denominator: "1" } },
        { images: "10" },
      );
      const musCust = await mkPrice(
        "CUSTOMER",
        "PROVIDER_USAGE",
        { music_ms: { numerator: "8", denominator: "1000" } },
        { music_ms: "300000" },
      );
      const musSup = await mkPrice(
        "SUPPLIER",
        "PROVIDER_USAGE",
        { music_ms: { numerator: "2", denominator: "1000" } },
        { music_ms: "300000" },
      );
      const vidCust = await mkPrice(
        "CUSTOMER",
        "REQUEST_COMPLETION",
        { requests: { numerator: "1000", denominator: "1" } },
        { requests: "1" },
      );
      const vidSup = await mkPrice(
        "SUPPLIER",
        "REQUEST_COMPLETION",
        { requests: { numerator: "200", denominator: "1" } },
        { requests: "1" },
      );

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "MiniMax-M3.1-Flash",
        operations: ["chat", "messages"],
        max_output_tokens: 200,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "Speech-2.8-HD",
        operations: ["speech", "transcription"],
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "MiniMax-H3-Image",
        operations: ["images"],
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "music-3.0",
        operations: ["music"],
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "MiniMax-H3",
        operations: ["videos"],
      });

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "MiniMax-M3.1-Flash",
        channel_id: chAnth.id,
        upstream_model: "MiniMax-M3.1-Flash",
        operations: ["messages"],
        billing_price_id: tokCust,
        cost_version_id: tokSup,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "MiniMax-M3.1-Flash",
        channel_id: chSuite.id,
        upstream_model: "MiniMax-M3.1-Flash",
        operations: ["chat"],
        billing_price_id: tokCust,
        cost_version_id: tokSup,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "Speech-2.8-HD",
        channel_id: chSuite.id,
        upstream_model: "speech-2.8-hd",
        operations: ["speech"],
        billing_price_id: speechCust,
        cost_version_id: speechSup,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "Speech-2.8-HD",
        channel_id: chSuite.id,
        upstream_model: "speech-2.8-hd",
        operations: ["transcription"],
        billing_price_id: sttCust,
        cost_version_id: sttSup,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "MiniMax-H3-Image",
        channel_id: chSuite.id,
        upstream_model: "MiniMax-H3-Image",
        operations: ["images"],
        billing_price_id: imgCust,
        cost_version_id: imgSup,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "music-3.0",
        channel_id: chSuite.id,
        upstream_model: "music-3.0",
        operations: ["music"],
        billing_price_id: musCust,
        cost_version_id: musSup,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "MiniMax-H3",
        channel_id: chSuite.id,
        upstream_model: "MiniMax-H3",
        operations: ["videos"],
        billing_price_id: vidCust,
        cost_version_id: vidSup,
      });

      const postJson = (path: string, body: unknown, headers: Record<string, string> = {}) =>
        runtime.fetchPublic(
          new Request(`http://gw.local${path}`, {
            method: "POST",
            headers: {
              authorization: `Bearer ${apiKey}`,
              "content-type": "application/json",
              ...headers,
            },
            body: JSON.stringify(body),
          }),
        );

      const anthRes = await postJson(
        "/v1/messages",
        {
          model: "MiniMax-M3.1-Flash",
          max_tokens: 64,
          messages: [{ role: "user", content: "hi" }],
        },
        { "x-api-key": apiKey },
      );
      expect(anthRes.status).toBe(200);
      expect(await anthRes.json()).toMatchObject({
        content: [{ type: "text", text: "hello-from-minimax-anthropic" }],
      });

      const anthStream = await postJson("/v1/messages", {
        model: "MiniMax-M3.1-Flash",
        max_tokens: 64,
        stream: true,
        messages: [{ role: "user", content: "hi" }],
      });
      expect(anthStream.status).toBe(200);
      expect(await anthStream.text()).toContain("ok-anth");

      const oaiRes = await postJson("/v1/chat/completions", {
        model: "MiniMax-M3.1-Flash",
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 64,
      });
      expect(oaiRes.status).toBe(200);
      expect(await oaiRes.json()).toMatchObject({
        choices: [{ message: { content: "hello-from-minimax-openai" } }],
      });

      const oaiStream = await postJson("/v1/chat/completions", {
        model: "MiniMax-M3.1-Flash",
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 64,
        stream: true,
      });
      expect(oaiStream.status).toBe(200);
      expect(await oaiStream.text()).toContain("ok-oai");

      const speechRes = await postJson("/v1/audio/speech", {
        model: "Speech-2.8-HD",
        text: "Hello world",
        voice_setting: { voice_id: "English_expressive_narrator" },
      });
      expect(speechRes.status).toBe(200);
      expect(await speechRes.json()).toMatchObject({ data: { status: 2 } });

      const sttForm = new FormData();
      sttForm.append("model", "Speech-2.8-HD");
      sttForm.append("file", new File(["wav-bytes"], "clip.wav", { type: "audio/wav" }));
      const sttRes = await runtime.fetchPublic(
        new Request("http://gw.local/v1/audio/transcriptions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}` },
          body: sttForm,
        }),
      );
      expect(sttRes.status).toBe(200);
      expect(await sttRes.json()).toMatchObject({ text: "transcribed-by-minimax" });

      const imgRes = await postJson("/v1/images/generations", {
        model: "MiniMax-H3-Image",
        prompt: "orange cat",
        n: 2,
      });
      expect(imgRes.status).toBe(200);
      expect(await imgRes.json()).toMatchObject({ metadata: { success_count: 2 } });

      const musRes = await postJson("/v1/music/generations", {
        model: "music-3.0",
        prompt: "acoustic folk",
        lyrics: "[verse]\nhello",
      });
      expect(musRes.status).toBe(200);
      expect(await musRes.json()).toMatchObject({ extra_info: { music_duration: 12500 } });

      const vidRes = await postJson("/v1/videos", {
        model: "MiniMax-H3",
        prompt: "cinematic sunset",
      });
      expect(vidRes.status).toBe(200);
      expect(await vidRes.json()).toMatchObject({ task_id: "vid_task_12345" });

      const { data: walletBeforeErr } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/wallets/${billingAccountId}?currency=USD`,
      );

      const errRes = await postJson("/v1/chat/completions", {
        model: "MiniMax-M3.1-Flash",
        messages: [{ role: "user", content: "TRIGGER_2049" }],
        max_tokens: 64,
      });
      expect(errRes.status).toBe(400);
      expect(await errRes.json()).toMatchObject({
        error: { code: "upstream_client_error", message: "invalid api key" },
      });

      const { data: walletAfterErr } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/wallets/${billingAccountId}?currency=USD`,
      );
      expect(walletAfterErr.available_atoms).toBe(walletBeforeErr.available_atoms);
      expect(walletAfterErr.spent_atoms).toBe("3355");

      const { data: audit } = await internalCall(
        runtime.internalApp,
        "GET",
        "/internal/v1/audit/conservation",
      );
      expect(audit.ok).toBe(true);
      expect(audit.open_holds).toBe(0);
      expect(seen.map((s) => s.path)).toEqual([
        "/anthropic/v1/messages",
        "/anthropic/v1/messages",
        "/v1/chat/completions",
        "/v1/chat/completions",
        "/v1/t2a_v2",
        "/v1/speech_to_text",
        "/v1/image_generation",
        "/v1/music_generation",
        "/v2/video_generation",
        "/v1/chat/completions",
      ]);
    } finally {
      await runtime.close();
      await mockUpstream.stop(true);
    }
  });
}
