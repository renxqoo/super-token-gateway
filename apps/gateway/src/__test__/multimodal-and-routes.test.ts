import { expect, test } from "bun:test";
import type { Operation } from "@gateway/kernel";
import { getBuiltinProvider } from "@gateway/plugin-engine";
import { createKey } from "@gateway/runtime";
import { handleInferenceRequest } from "../proxy.ts";
import type { ProxyDependencies } from "../inference/types.ts";
import {
  makeApiKey,
  makeBillingAccount,
  makeConfig,
  makeFakeDeps,
  makeSnapshot,
} from "./proxy-fixture.ts";

function makeCtx(url: string, headers: Record<string, string>, body?: BodyInit) {
  return {
    req: {
      raw: new Request(url, {
        method: "POST",
        headers,
        ...(body === undefined ? {} : { body }),
      }),
      query: () => undefined,
    },
    json(data: unknown, status?: number, extra?: Record<string, string>) {
      return new Response(JSON.stringify(data), {
        status: status ?? 200,
        headers: { "content-type": "application/json", ...extra },
      });
    },
  };
}

test("multipart transcription preserves repeated bracket fields, audio MIME, text/srt/vtt responses, and multi-file image edits", async () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111151", 10);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const preset = getBuiltinProvider("openai")!;
  const pluginId = "88888888-8888-4888-8888-888888888851";
  snap.plugins.set(pluginId, {
    id: pluginId,
    name: preset.name,
    version: preset.version,
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    digest: preset.digest,
    wasm_bytes: null,
    webhook_config: null,
    provider_config: preset.provider_config,
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
    builtin: true,
  });

  const capturedTranscriptions: Array<{
    model: string | null;
    granularities: FormDataEntryValue[];
    includes: FormDataEntryValue[];
    chunking: FormDataEntryValue | null;
    responseFormat: FormDataEntryValue | null;
    fileName: string;
    fileType: string;
    fileText: string;
  }> = [];
  const capturedEdits: Array<{
    model: string | null;
    prompt: FormDataEntryValue | null;
    imageName: string;
    imageType: string;
    imageText: string;
    maskName: string;
    maskType: string;
    maskText: string;
  }> = [];

  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      const form = await req.formData();
      if (path === "/v1/audio/transcriptions") {
        const file = form.get("file");
        if (!(file instanceof File)) return new Response("missing file", { status: 400 });
        const fmt = form.get("response_format");
        capturedTranscriptions.push({
          model: typeof form.get("model") === "string" ? String(form.get("model")) : null,
          granularities: form.getAll("timestamp_granularities[]"),
          includes: form.getAll("include[]"),
          chunking: form.get("chunking_strategy[type]"),
          responseFormat: fmt,
          fileName: file.name,
          fileType: file.type,
          fileText: await file.text(),
        });
        if (fmt === "text") {
          return new Response("plain transcript\n", {
            headers: { "content-type": "text/plain; charset=utf-8" },
          });
        }
        if (fmt === "srt") {
          return new Response("1\n00:00:00,000 --> 00:00:01,000\nHello\n", {
            headers: { "content-type": "text/plain; charset=utf-8" },
          });
        }
        if (fmt === "vtt") {
          return new Response("WEBVTT\n\n00:00.000 --> 00:01.000\nHello\n", {
            headers: { "content-type": "text/vtt; charset=utf-8" },
          });
        }
        return Response.json({ text: "hello json", usage: { type: "duration", seconds: 2 } });
      }
      if (path === "/v1/images/edits") {
        const image = form.get("image");
        const mask = form.get("mask");
        if (!(image instanceof File) || !(mask instanceof File)) {
          return new Response("missing image or mask", { status: 400 });
        }
        capturedEdits.push({
          model: typeof form.get("model") === "string" ? String(form.get("model")) : null,
          prompt: form.get("prompt"),
          imageName: image.name,
          imageType: image.type,
          imageText: await image.text(),
          maskName: mask.name,
          maskType: mask.type,
          maskText: await mask.text(),
        });
        return Response.json({
          created: 1,
          data: [{ b64_json: "AAAA" }],
          usage: { input_tokens: 1, output_tokens: 1 },
        });
      }
      return new Response("not found", { status: 404 });
    },
  });

  const acctId = "44444444-4444-4444-8444-444444444451";
  const chId = "33333333-3333-4333-8333-333333333351";
  snap.accounts.set(acctId, { id: acctId, name: "acct-media", state: "ACTIVE", revision: "1" });
  snap.channels.set(chId, {
    id: chId,
    account_id: acctId,
    name: "ch-media",
    kind: "openai",
    provider_plugin_id: pluginId,
    base_url: `http://127.0.0.1:${upstream.port}`,
    secret_ciphertext: "",
    adapter_config: {},
    rpm: 10_000,
    concurrency: 100,
    enabled: true,
    routing_policy: {},
    config_revision: 1,
  });

  const reqCustPriceId = "66666666-6666-4666-8666-666666666651";
  const reqSupPriceId = "77777777-7777-4777-8777-777777777751";
  for (const [id, side] of [
    [reqCustPriceId, "CUSTOMER"],
    [reqSupPriceId, "SUPPLIER"],
  ] as const) {
    snap.prices.set(id, {
      id,
      side,
      currency: "USD",
      basis: "REQUEST_COMPLETION",
      rates: { requests: { numerator: "5", denominator: "1" } },
      ceilings: { requests: "1" },
      source_digest: "d-req",
    });
  }
  const usageCustPriceId = "66666666-6666-4666-8666-666666666652";
  const usageSupPriceId = "77777777-7777-4777-8777-777777777752";
  for (const [id, side] of [
    [usageCustPriceId, "CUSTOMER"],
    [usageSupPriceId, "SUPPLIER"],
  ] as const) {
    snap.prices.set(id, {
      id,
      side,
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: { audio_seconds: { numerator: "10", denominator: "1" } },
      ceilings: { audio_seconds: "60" },
      source_digest: "d-sec",
    });
  }

  snap.models.set("whisper-req", {
    name: "whisper-req",
    operations: ["transcription"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 100,
    enabled: true,
  });
  snap.models.set("whisper-sec", {
    name: "whisper-sec",
    operations: ["transcription"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 100,
    enabled: true,
  });
  snap.models.set("img-edit", {
    name: "img-edit",
    operations: ["image_edits"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 100,
    enabled: true,
  });
  snap.routesByModel.set("whisper-req", [
    {
      id: "r-w-req",
      model_name: "whisper-req",
      channel_id: chId,
      upstream_model: "whisper-upstream",
      operations: ["transcription"],
      billing_price_id: reqCustPriceId,
      cost_version_id: reqSupPriceId,
      priority: 10,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 1 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: {},
    },
  ]);
  snap.routesByModel.set("whisper-sec", [
    {
      id: "r-w-sec",
      model_name: "whisper-sec",
      channel_id: chId,
      upstream_model: "whisper-upstream",
      operations: ["transcription"],
      billing_price_id: usageCustPriceId,
      cost_version_id: usageSupPriceId,
      priority: 10,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 1 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: {},
    },
  ]);
  snap.routesByModel.set("img-edit", [
    {
      id: "r-edit",
      model_name: "img-edit",
      channel_id: chId,
      upstream_model: "gpt-image-edit-up",
      operations: ["image_edits"],
      billing_price_id: reqCustPriceId,
      cost_version_id: reqSupPriceId,
      priority: 10,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 1 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: {},
    },
  ]);

  const deps = makeFakeDeps(snap, makeConfig());
  const settled: string[] = [];
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "req", held_atoms: "1000" };
    },
    async commitTerminal() {
      settled.push("SETTLED");
      return {
        ok: true,
        replayed: false,
        charge_id: "chg",
        customer_atoms: "5",
        supplier_cost_atoms: "5",
      };
    },
    async releaseHold() {
      settled.push("RELEASED");
    },
    async markUnknown() {
      settled.push("UNKNOWN");
    },
  } as unknown as ProxyDependencies["ledger"];

  try {
    for (const [format, expectedContentType, expectedBody] of [
      ["text", "text/plain", "plain transcript\n"],
      ["srt", "text/plain", "1\n00:00:00,000 --> 00:00:01,000\nHello\n"],
      ["vtt", "text/vtt", "WEBVTT\n\n00:00.000 --> 00:01.000\nHello\n"],
    ] as const) {
      const form = new FormData();
      form.append("model", "whisper-req");
      form.append("response_format", format);
      form.append("timestamp_granularities[]", "word");
      form.append("timestamp_granularities[]", "segment");
      form.append("include[]", "logprobs");
      form.append("chunking_strategy[type]", "server_vad");
      form.append("file", new File(["mp3-raw-bytes"], "recording.mp3", { type: "audio/mpeg" }));

      const resp = await handleInferenceRequest(
        makeCtx(
          "http://127.0.0.1/v1/audio/transcriptions",
          { authorization: `Bearer ${rawKey}` },
          form,
        ),
        deps,
        "transcription",
      );
      expect(resp.status).toBe(200);
      expect(resp.headers.get("content-type")).toContain(expectedContentType);
      expect(await resp.text()).toBe(expectedBody);
    }

    expect(capturedTranscriptions).toHaveLength(3);
    expect(capturedTranscriptions[0]).toEqual({
      model: "whisper-upstream",
      granularities: ["word", "segment"],
      includes: ["logprobs"],
      chunking: "server_vad",
      responseFormat: "text",
      fileName: "recording.mp3",
      fileType: "audio/mpeg",
      fileText: "mp3-raw-bytes",
    });

    const unmeteredPlainForm = new FormData();
    unmeteredPlainForm.append("model", "whisper-sec");
    unmeteredPlainForm.append("response_format", "text");
    unmeteredPlainForm.append("file", new File(["wav"], "recording", { type: "audio/wav" }));
    const unmeteredPlain = await handleInferenceRequest(
      makeCtx(
        "http://127.0.0.1/v1/audio/transcriptions",
        { authorization: `Bearer ${rawKey}` },
        unmeteredPlainForm,
      ),
      deps,
      "transcription",
    );
    expect(unmeteredPlain.status).toBe(502);
    expect(await unmeteredPlain.json()).toMatchObject({
      error: { code: "invalid_upstream_usage" },
    });

    const editForm = new FormData();
    editForm.append("model", "img-edit");
    editForm.append("prompt", "add sun");
    editForm.append("image", new File(["png-image"], "base.png", { type: "image/png" }));
    editForm.append("mask", new File(["png-mask"], "mask.png", { type: "image/png" }));
    const editResp = await handleInferenceRequest(
      makeCtx("http://127.0.0.1/v1/images/edits", { authorization: `Bearer ${rawKey}` }, editForm),
      deps,
      "image_edits",
    );
    expect(editResp.status).toBe(200);
    expect(capturedEdits).toEqual([
      {
        model: "gpt-image-edit-up",
        prompt: "add sun",
        imageName: "base.png",
        imageType: "image/png",
        imageText: "png-image",
        maskName: "mask.png",
        maskType: "image/png",
        maskText: "png-mask",
      },
    ]);
    expect(settled).toEqual(["SETTLED", "SETTLED", "SETTLED", "UNKNOWN", "SETTLED"]);
  } finally {
    deps.state?.close();
    deps.transport.close();
    await upstream.stop(true);
  }
});

test("all 14 public inference POST operations reject missing model, malformed bodies, and invalid metered parameters with zero upstream calls and zero holds", async () => {
  const allOperations: readonly Operation[] = [
    "chat",
    "responses",
    "messages",
    "embeddings",
    "moderations",
    "completions",
    "images",
    "image_edits",
    "speech",
    "transcription",
    "translation",
    "music",
    "videos",
    "gemini",
  ];
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111152", 10);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  snap.models.set("test-model", {
    name: "test-model",
    operations: [...allOperations],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 100,
    enabled: true,
  });

  const deps = makeFakeDeps(snap, makeConfig());
  let holds = 0;
  let dispatches = 0;
  deps.ledger = {
    async acquireHold() {
      holds++;
      return { ok: true, replayed: false, request_id: "req", held_atoms: "100" };
    },
  } as unknown as ProxyDependencies["ledger"];
  deps.transport.fetchFast = async () => {
    dispatches++;
    throw new Error("should not dispatch");
  };

  try {
    for (const operation of allOperations) {
      const headers =
        operation === "messages"
          ? { "x-api-key": rawKey, "content-type": "application/json" }
          : operation === "gemini"
            ? { "x-goog-api-key": rawKey, "content-type": "application/json" }
            : { authorization: `Bearer ${rawKey}`, "content-type": "application/json" };

      const missingModel = await handleInferenceRequest(
        makeCtx(`http://127.0.0.1/v1/${operation}`, headers, JSON.stringify({})),
        deps,
        operation,
        operation === "gemini" ? { model: "", stream: false } : undefined,
      );
      expect(missingModel.status, `missing model: ${operation}`).toBe(400);

      const malformedJson = await handleInferenceRequest(
        makeCtx(`http://127.0.0.1/v1/${operation}`, headers, "{bad-json"),
        deps,
        operation,
        operation === "gemini" ? { model: "test-model", stream: false } : undefined,
      );
      expect(malformedJson.status, `malformed json: ${operation}`).toBe(400);
    }

    for (const multipartOp of ["transcription", "translation", "image_edits"] as const) {
      const badMultipart = await handleInferenceRequest(
        makeCtx(
          `http://127.0.0.1/v1/${multipartOp}`,
          {
            authorization: `Bearer ${rawKey}`,
            "content-type": "multipart/form-data; boundary=broken",
          },
          "not-valid-multipart-bytes",
        ),
        deps,
        multipartOp,
      );
      expect(badMultipart.status, `bad multipart: ${multipartOp}`).toBe(400);
    }

    const meteredCases: Array<{
      operation: Operation;
      body: Record<string, unknown>;
      code: string;
      pathOverride?: { model: string; stream: boolean };
    }> = [
      {
        operation: "chat",
        body: { model: "test-model", messages: [{ role: "user", content: "hi" }], n: 2 },
        code: "billing_ceiling",
      },
      {
        operation: "completions",
        body: { model: "test-model", prompt: "hi", best_of: 2 },
        code: "billing_ceiling",
      },
      {
        operation: "responses",
        body: { model: "test-model", input: "hi", conversation: "conv_1" },
        code: "unmetered_context",
      },
      {
        operation: "messages",
        body: {
          model: "test-model",
          messages: [{ role: "user", content: "hi" }],
          container: "ctr_1",
        },
        code: "unmetered_context",
      },
      {
        operation: "gemini",
        body: { contents: [{ parts: [{ text: "hi" }] }], cached_content: "cachedContents/1" },
        code: "unmetered_context",
        pathOverride: { model: "test-model", stream: false },
      },
      {
        operation: "gemini",
        body: {
          contents: [{ parts: [{ text: "hi" }] }],
          generation_config: { candidate_count: 2 },
        },
        code: "billing_ceiling",
        pathOverride: { model: "test-model", stream: false },
      },
    ];
    for (const item of meteredCases) {
      const headers =
        item.operation === "messages"
          ? { "x-api-key": rawKey, "content-type": "application/json" }
          : item.operation === "gemini"
            ? { "x-goog-api-key": rawKey, "content-type": "application/json" }
            : { authorization: `Bearer ${rawKey}`, "content-type": "application/json" };
      const resp = await handleInferenceRequest(
        makeCtx(`http://127.0.0.1/v1/${item.operation}`, headers, JSON.stringify(item.body)),
        deps,
        item.operation,
        item.pathOverride,
      );
      expect(resp.status, `${item.operation}:${item.code}`).toBe(422);
      expect(await resp.json()).toMatchObject({
        error: item.operation === "gemini" ? { gateway_code: item.code } : { code: item.code },
      });
    }

    expect(holds).toBe(0);
    expect(dispatches).toBe(0);
  } finally {
    deps.state?.close();
    deps.transport.close();
  }
});
