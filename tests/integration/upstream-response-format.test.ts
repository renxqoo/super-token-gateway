import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import {
  chatRequest,
  db,
  makeConfig,
  registerBillingFixture,
  seed,
  waitFor,
} from "./adversarial-billing-fixture.ts";

registerBillingFixture();

const sse =
  [
    'data: {"id":"format","model":"m","choices":[{"index":0,"delta":{"content":"completed output"},"finish_reason":null}]}',
    'data: {"id":"format","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}',
    "data: [DONE]",
  ].join("\n\n") + "\n\n";

async function chatWithUpstream(
  reply: () => Response,
  stream = false,
  inspectResponse?: (response: Response) => Promise<void>,
) {
  const requestedModes: boolean[] = [];
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const payload = (await request.json()) as { stream?: boolean };
      requestedModes.push(payload.stream === true);
      return reply();
    },
  });
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  let gateway: ReturnType<typeof Bun.serve> | undefined;
  try {
    await runtime.start();
    const fixture = await seed(runtime, upstream.url.origin);
    const model = `format-${crypto.randomUUID().slice(0, 8)}`;
    await fixture.call("PUT", "/internal/v1/models", {
      name: model,
      operations: ["chat"],
      max_output_tokens: 50,
    });
    await fixture.call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: fixture.channel.id,
      upstream_model: "m",
      operations: ["chat"],
      billing_price_id: fixture.customer.id,
      cost_version_id: fixture.supplier.id,
    });
    gateway = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch(req, server) {
        return runtime.fetchPublic(
          req,
          { getPeerAddress: () => server.requestIP(req)?.address },
          true,
        );
      },
    });
    const path = new URL("/v1/chat/completions", gateway.url);
    const response = await fetch(new Request(path, chatRequest(fixture.key, model, { stream })));
    await inspectResponse?.(response);
    const contentType = response.headers.get("content-type");
    const requestId = response.headers.get("x-request-id");
    const wire = await response.text();
    expect(requestedModes).toEqual([stream]);
    const billing = await fixture.call("GET", `/internal/v1/requests/${requestId}`);
    return { status: response.status, contentType, wire, billing };
  } finally {
    await gateway?.stop(true);
    upstream.stop(true);
    await runtime.close();
  }
}

test("symptom: invalid UTF-8 in an upstream JSON response returns internal_error", async () => {
  const result = await chatWithUpstream(
    () =>
      new Response(Uint8Array.of(0x7b, 0xff, 0x7d), {
        headers: { "content-type": "application/json" },
      }),
  );
  expect(result.status).toBe(502);
  expect(JSON.parse(result.wire)).toMatchObject({ error: { code: "invalid_upstream_json" } });
  expect(result.billing).toMatchObject({ hold: { state: "UNKNOWN" }, charge: null });
});

test("symptom: non-streaming chat returns SSE and settles when upstream ignores the stream flag", async () => {
  const result = await chatWithUpstream(
    () => new Response(sse, { headers: { "content-type": "text/event-stream" } }),
  );
  expect(result.status).toBe(502);
  expect(result.contentType).toStartWith("application/json");
  expect(JSON.parse(result.wire)).toMatchObject({ error: { code: "invalid_upstream" } });
  expect(result.billing).toMatchObject({ hold: { state: "UNKNOWN" }, charge: null });
});

test("chat closes an unexpected upstream SSE body before it ends", async () => {
  let upstreamCancelled = false;
  await chatWithUpstream(
    () =>
      new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(sse.slice(0, sse.indexOf("\n\n") + 2)));
          },
          cancel() {
            upstreamCancelled = true;
          },
        }),
        { headers: { "content-type": "text/event-stream" } },
      ),
    false,
    async (response) => {
      expect(response.status).toBe(502);
      await waitFor(async () => upstreamCancelled);
    },
  );
});

test("chat with stream enabled delivers the same SSE and settles the reported usage", async () => {
  const result = await chatWithUpstream(
    () => new Response(sse, { headers: { "content-type": "text/event-stream" } }),
    true,
  );
  expect(result.status).toBe(200);
  expect(result.contentType).toStartWith("text/event-stream");
  expect(result.wire).toContain("completed output");
  expect(result.wire.trimEnd()).toEndWith("data: [DONE]");
  expect(result.billing).toMatchObject({
    hold: { state: "SETTLED" },
    charge: { customerAtoms: "10" },
  });
});
