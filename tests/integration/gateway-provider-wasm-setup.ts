import type { createGatewayRuntime } from "@gateway/gateway";
import { internalCall, publishPlugins } from "./gateway-fixture.ts";
import { expect } from "bun:test";

export async function exerciseWasmProviderSetup({
  runtime,
  wasmProviderBytes,
  acc,
  upstream,
  custPrice,
  supPrice,
  apiKey,
}: {
  runtime: ReturnType<typeof createGatewayRuntime>;
  wasmProviderBytes: Uint8Array<ArrayBuffer>;
  acc: Record<string, unknown>;
  upstream: Bun.Server<undefined>;
  custPrice: Record<string, unknown>;
  supPrice: Record<string, unknown>;
  apiKey: string;
}) {
  const wasiBadBytes = new Uint8Array([
    0x00, 0x61, 0x73, 0x6d, 1, 0, 0, 0, 1, 4, 1, 0x60, 0, 0, 2, 36, 1, 21, 0x77, 0x61, 0x73, 0x69,
    0x5f, 0x73, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x5f, 0x70, 0x72, 0x65, 0x76, 0x69, 0x65,
    0x77, 0x31, 9, 0x70, 0x72, 0x6f, 0x63, 0x5f, 0x65, 0x78, 0x69, 0x74, 0x00, 0,
  ]);
  const { status: wasiRejectStatus } = await internalCall(
    runtime.internalApp,
    "PUT",
    "/internal/v1/plugins",
    {
      name: "bad-wasi-provider",
      version: "1.0.0",
      capability: "PROVIDER",
      kind: "WASM",
      wasm_bytes_base64: Buffer.from(wasiBadBytes).toString("base64"),
      provider_config: {
        base_dialect: "passthrough",
        usage_dialect: "custom",
        auth: { scheme: "bearer" },
        hooks: ["adapt_request"],
      },
    },
  );
  expect(wasiRejectStatus).toBe(400);

  const { status: wasmRegStatus, data: wasmProvPlugin } = await internalCall(
    runtime.internalApp,
    "PUT",
    "/internal/v1/plugins",
    {
      name: "acme-proprietary",
      version: "1.0.0",
      capability: "PROVIDER",
      kind: "WASM",
      wasm_bytes_base64: Buffer.from(wasmProviderBytes).toString("base64"),
      provider_config: {
        base_dialect: "passthrough",
        usage_dialect: "custom",
        auth: { scheme: "bearer" },
        hooks: ["adapt_request", "adapt_response", "adapt_stream_event"],
        grants: ["extract_usage"],
        fuel_budget: 5_000_000,
        max_memory_bytes: 16 * 1024 * 1024,
      },
    },
  );
  expect(wasmRegStatus).toBe(200);
  const wasmRequestSample = { messages: [{ role: "user", content: "validation" }] };
  await publishPlugins(runtime, [
    [
      wasmProvPlugin.id,
      {
        request: wasmRequestSample,
        response: { answer: "validation", tok_in: 4, tok_out: 2 },
        event: { name: "message", data: '{"finish":true,"tok_in":4,"tok_out":2}' },
      },
    ],
  ]);

  const { data: chWasm } = await internalCall(runtime.internalApp, "PUT", "/internal/v1/channels", {
    account_id: acc.id,
    name: "acme-wasm-ch",
    kind: "acme-proprietary",
    provider_plugin_id: wasmProvPlugin.id,
    base_url: `http://127.0.0.1:${upstream.port}`,
    api_key: "sk-secret-acme-999",
  });
  await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
    name: "acme-wasm-model",
    operations: ["chat"],
    max_output_tokens: 200,
  });
  await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
    model_name: "acme-wasm-model",
    channel_id: chWasm.id,
    upstream_model: "acme-engine-9",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });

  const wasmUnaryResp = await runtime.publicApp.fetch(
    new Request("http://gw.local/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "acme-wasm-model",
        messages: [{ role: "user", content: "hello proprietary wasm" }],
      }),
    }),
  );
  expect(wasmUnaryResp.status).toBe(200);
  const wasmUnaryJson = (await wasmUnaryResp.json()) as {
    model: string;
    choices: { message: { content: string } }[];
  };
  expect(wasmUnaryJson.model).toBe("acme-wasm-model");
  expect(wasmUnaryJson.choices[0]!.message.content).toBe("wasm-echo:hello proprietary wasm");
  const wasmUnaryReqId = wasmUnaryResp.headers.get("x-request-id")!;
  const { data: wasmUnaryBill } = await internalCall(
    runtime.internalApp,
    "GET",
    `/internal/v1/requests/${wasmUnaryReqId}`,
  );
  expect((wasmUnaryBill.charge as { usage: Record<string, string> }).usage).toEqual({
    input_tokens: "10",
    output_tokens: "6",
    cached_input_tokens: "4",
  });

  const wasmStreamResp = await runtime.publicApp.fetch(
    new Request("http://gw.local/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "acme-wasm-model",
        stream: true,
        messages: [{ role: "user", content: "stream proprietary" }],
      }),
    }),
  );
  expect(wasmStreamResp.status).toBe(200);
  const wasmStreamReqId = wasmStreamResp.headers.get("x-request-id")!;
  const wasmStreamWire = await wasmStreamResp.text();
  const streamedContent = wasmStreamWire
    .split("\n")
    .filter((l) => l.startsWith("data: ") && l !== "data: [DONE]")
    .map((l) => {
      const obj = JSON.parse(l.slice(6)) as { choices?: { delta?: { content?: string } }[] };
      return obj.choices?.[0]?.delta?.content ?? "";
    })
    .join("");
  expect(streamedContent).toBe("Hello WASM!");
  expect(wasmStreamWire).toContain("[DONE]");
  const { data: wasmStreamBill } = await internalCall(
    runtime.internalApp,
    "GET",
    `/internal/v1/requests/${wasmStreamReqId}`,
  );
  expect((wasmStreamBill.charge as { usage: Record<string, string> }).usage).toEqual({
    input_tokens: "8",
    output_tokens: "4",
  });

  return { wasmRequestSample };
}
