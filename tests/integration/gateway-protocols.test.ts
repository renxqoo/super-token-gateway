import { expect, test } from "bun:test";
import { anthropicMessagesReleaseId, geminiGenerateReleaseId } from "@gateway/plugin-sdk";
import { syntheticAdminKey } from "../support/catalog.ts";
import { settledCharge, withGatewayStack, type UpstreamCall } from "../support/gateway-process.ts";

type Stack = Parameters<Parameters<typeof withGatewayStack>[1]>[0];

const message = {
  id: "msg_synthetic",
  type: "message",
  role: "assistant",
  content: [{ type: "text", text: "合成回复" }],
  model: "synthetic",
  stop_reason: "end_turn",
  stop_sequence: null,
  usage: { input_tokens: 30, output_tokens: 15 },
};
const events: [string, Record<string, unknown>][] = [
  [
    "message_start",
    {
      type: "message_start",
      message: { ...message, content: [], usage: { input_tokens: 30, output_tokens: 1 } },
    },
  ],
  ["ping", { type: "ping" }],
  [
    "content_block_start",
    { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
  ],
  [
    "content_block_delta",
    { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "合成回复" } },
  ],
  ["content_block_stop", { type: "content_block_stop", index: 0 }],
  [
    "message_delta",
    {
      type: "message_delta",
      delta: { stop_reason: "end_turn", stop_sequence: null },
      usage: { output_tokens: 15 },
    },
  ],
  ["message_stop", { type: "message_stop" }],
];

function reply(call: UpstreamCall) {
  if (call.body.stream)
    return new Response(
      events.map(([name, data]) => `event: ${name}\ndata: ${JSON.stringify(data)}\n\n`).join(""),
      { headers: { "content-type": "text/event-stream" } },
    );
  return Response.json(message);
}

async function allowOperation(stack: Stack, operation: string) {
  const { db, setup } = stack;
  await db`UPDATE models SET operations=array_append(operations,${operation}) WHERE name=${setup.model}`;
  await db`UPDATE routes SET operations=array_append(operations,${operation}) WHERE model_name=${setup.model}`;
  await db`UPDATE api_keys SET operations=array_append(operations,${operation}) WHERE id=${setup.key.id}`;
  const tariff = await stack.f.request("/tariffs", {
    model: setup.model,
    operation,
    provider_id: setup.provider.id,
    pricing: { currency: "USD", rates: { requests: { numerator: "100", denominator: "1" } } },
    ceilings: { requests: "1" },
    basis: "REQUEST_COMPLETION",
  });
  expect(tariff.status).toBe(201);
}

async function bind(stack: Stack, releaseId: string, maxOutputTokens = 100) {
  const { f, setup } = stack;
  const installed = await f.catalog.application.installBundled(await f.catalog.actor(), {
    command_id: crypto.randomUUID(),
    release_id: releaseId,
    reason: "Synthetic native protocol installation",
  });
  const bound = await f.catalog.request(
    `/providers/${setup.provider.id}/plugin-binding`,
    {
      ...f.catalog.binding(installed.version_id),
      expected_revision: "1",
      account_id: setup.provider.account_id,
      brand_id: setup.provider.brand_id,
      secret_reference: `channel:${setup.provider.id}`,
      config: {
        model_map: { [setup.model]: "synthetic" },
        max_output_tokens: maxOutputTokens,
        max_input_tokens: 1000,
        auth: "api-key",
      },
    },
    syntheticAdminKey,
    "PUT",
  );
  expect(bound.status).toBe(201);
}

function sendMessages(stack: Stack, body: Record<string, unknown>) {
  return fetch(`${stack.gateway.origin}/v1/messages`, {
    method: "POST",
    headers: { "x-api-key": stack.setup.key.key, "content-type": "application/json" },
    body: JSON.stringify({ model: stack.setup.model, ...body }),
  });
}

test(
  "an Anthropic Messages client is served natively through /v1/messages with only the upstream model rewritten, JSON and SSE, and exact settlement",
  () =>
    withGatewayStack({ reply }, async (stack) => {
      await allowOperation(stack, "messages");
      await bind(stack, anthropicMessagesReleaseId);
      const request = {
        max_tokens: 30,
        system: [{ type: "text", text: "规则", cache_control: { type: "ephemeral" } }],
        messages: [{ role: "user", content: [{ type: "text", text: "你好" }] }],
        metadata: { user_id: "u1" },
      };
      const json = await sendMessages(stack, request);
      expect(json.status).toBe(200);
      expect(await json.json()).toEqual({ ...message, model: stack.setup.model });
      const call = stack.received.at(-1)!;
      expect(call.path).toBe("/v1/messages");
      expect(call.apiKey).toBeString();
      expect(call.headers["anthropic-version"]).toBe("2023-06-01");
      expect(call.body).toEqual({ ...request, model: "synthetic" });
      expect(await settledCharge(stack.db, json.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });

      const streamed = await sendMessages(stack, { ...request, stream: true });
      expect(streamed.status).toBe(200);
      const frames = (await streamed.text())
        .split("\n\n")
        .filter(Boolean)
        .map((frame) => {
          const [event, data] = frame.split("\n");
          return [event!.replace(/^event: /, ""), JSON.parse(data!.replace(/^data: /, ""))];
        });
      const start = events[0]![1] as { message: Record<string, unknown> };
      expect(frames).toEqual([
        ["message_start", { ...start, message: { ...start.message, model: stack.setup.model } }],
        ...events.slice(1),
      ]);
      expect(stack.received.at(-1)!.body).toEqual({ ...request, model: "synthetic", stream: true });
      expect(await settledCharge(stack.db, streamed.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });
    }),
  120000,
);

test(
  "a /v1/messages request for a model whose only channel runs a plugin without the messages operation is no_route and never reaches the upstream",
  () =>
    withGatewayStack({ reply }, async (stack) => {
      await allowOperation(stack, "messages");
      const response = await sendMessages(stack, {
        max_tokens: 30,
        messages: [{ role: "user", content: "你好" }],
      });
      expect(response.status).toBe(503);
      expect(((await response.json()) as { error: { code: string } }).error.code).toBe("no_route");
      expect(stack.received).toHaveLength(0);
    }),
  120000,
);

const geminiFrames = [
  {
    candidates: [{ content: { role: "model", parts: [{ text: "合成" }] } }],
    usageMetadata: { promptTokenCount: 30 },
    modelVersion: "synthetic-001",
  },
  {
    candidates: [{ content: { role: "model", parts: [{ text: "回复" }] }, finishReason: "STOP" }],
    usageMetadata: { promptTokenCount: 30, candidatesTokenCount: 15 },
    modelVersion: "synthetic-001",
  },
];
const geminiResponse = {
  candidates: [{ content: { role: "model", parts: [{ text: "合成回复" }] }, finishReason: "STOP" }],
  usageMetadata: { promptTokenCount: 30, candidatesTokenCount: 15 },
  modelVersion: "synthetic-001",
  responseId: "gem-1",
};

function geminiReply(call: UpstreamCall) {
  if (call.path.includes(":streamGenerateContent"))
    return new Response(
      geminiFrames.map((frame) => `data: ${JSON.stringify(frame)}\r\n\r\n`).join(""),
      {
        headers: { "content-type": "text/event-stream" },
      },
    );
  return Response.json(geminiResponse);
}

test(
  "a Gemini client is served natively through the generateContent paths with the model and stream taken from the URL, the configured output ceiling filled in, and exact settlement",
  () =>
    withGatewayStack({ reply: geminiReply }, async (stack) => {
      await allowOperation(stack, "gemini");
      await bind(stack, geminiGenerateReleaseId, 40);
      const request = {
        contents: [{ role: "user", parts: [{ text: "你好" }] }],
        generationConfig: { temperature: 0.5 },
      };
      const send = (method: string) =>
        fetch(`${stack.gateway.origin}/v1beta/models/${stack.setup.model}:${method}`, {
          method: "POST",
          headers: { "x-goog-api-key": stack.setup.key.key, "content-type": "application/json" },
          body: JSON.stringify(request),
        });
      const json = await send("generateContent");
      expect(json.status).toBe(200);
      expect(await json.json()).toEqual({ ...geminiResponse, modelVersion: stack.setup.model });
      const call = stack.received.at(-1)!;
      expect(call.path).toBe("/v1beta/models/synthetic:generateContent");
      expect(call.googKey).toBeString();
      expect(call.body).toEqual({
        ...request,
        generationConfig: { temperature: 0.5, maxOutputTokens: 40 },
      });
      expect(await settledCharge(stack.db, json.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });

      const streamed = await send("streamGenerateContent");
      expect(streamed.status).toBe(200);
      const frames = (await streamed.text())
        .split(/\r?\n\r?\n/)
        .filter(Boolean)
        .map((frame) => JSON.parse(frame.replace(/^data: /, "")));
      expect(frames).toEqual(
        geminiFrames.map((frame) => ({ ...frame, modelVersion: stack.setup.model })),
      );
      expect(stack.received.at(-1)!.path).toBe(
        "/v1beta/models/synthetic:streamGenerateContent?alt=sse",
      );
      expect(await settledCharge(stack.db, streamed.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });

      const unknown = await send("countTokens");
      expect(unknown.status).toBe(404);
      expect(stack.received).toHaveLength(2);
    }),
  120000,
);

const responsesUsage = {
  input_tokens: 30,
  input_tokens_details: { cached_tokens: 0 },
  output_tokens: 15,
  output_tokens_details: { reasoning_tokens: 0 },
  total_tokens: 45,
};
const responsesBody = {
  id: "resp_synthetic",
  object: "response",
  created_at: 1,
  status: "completed",
  error: null,
  model: "synthetic",
  output: [
    {
      type: "message",
      id: "msg_1",
      role: "assistant",
      content: [{ type: "output_text", text: "合成回复", annotations: [] }],
    },
  ],
  usage: responsesUsage,
};
const responsesEvents: Record<string, unknown>[] = [
  {
    type: "response.created",
    sequence_number: 0,
    response: { ...responsesBody, status: "in_progress", output: [], usage: null },
  },
  {
    type: "response.output_text.delta",
    sequence_number: 1,
    item_id: "msg_1",
    output_index: 0,
    content_index: 0,
    delta: "合成回复",
  },
  { type: "response.completed", sequence_number: 2, response: responsesBody },
];

function responsesReply(call: UpstreamCall) {
  if (call.body.stream)
    return new Response(
      responsesEvents
        .map((data) => `event: ${data.type}\ndata: ${JSON.stringify(data)}\n\n`)
        .join(""),
      { headers: { "content-type": "text/event-stream" } },
    );
  return Response.json(responsesBody);
}

test(
  "an OpenAI Responses client is served through /v1/responses with only the model rewritten, JSON and SSE, and exact settlement; previous_response_id is refused before reaching the upstream",
  () =>
    withGatewayStack({ reply: responsesReply }, async (stack) => {
      await allowOperation(stack, "responses");
      const send = (body: Record<string, unknown>) =>
        fetch(`${stack.gateway.origin}/v1/responses`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${stack.setup.key.key}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({ model: stack.setup.model, ...body }),
        });
      const request = { input: "你好", instructions: "简短", max_output_tokens: 30 };
      const json = await send(request);
      expect(json.status).toBe(200);
      expect(await json.json()).toEqual({ ...responsesBody, model: stack.setup.model });
      const call = stack.received.at(-1)!;
      expect(call.path).toBe("/v1/responses");
      expect(call.authorization).toStartWith("Bearer ");
      expect(call.body).toEqual({ ...request, model: "synthetic" });
      expect(await settledCharge(stack.db, json.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });

      const streamed = await send({ ...request, stream: true });
      expect(streamed.status).toBe(200);
      const frames = (await streamed.text())
        .split("\n\n")
        .filter(Boolean)
        .map((frame) => {
          const [event, data] = frame.split("\n");
          return [event!.replace(/^event: /, ""), JSON.parse(data!.replace(/^data: /, ""))];
        });
      const created = responsesEvents[0] as { response: Record<string, unknown> };
      expect(frames).toEqual([
        [
          "response.created",
          { ...created, response: { ...created.response, model: stack.setup.model } },
        ],
        ["response.output_text.delta", responsesEvents[1]],
        [
          "response.completed",
          { ...responsesEvents[2], response: { ...responsesBody, model: stack.setup.model } },
        ],
      ]);
      expect(await settledCharge(stack.db, streamed.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });

      const continued = await send({ input: "继续", previous_response_id: "resp_synthetic" });
      expect(continued.status).toBe(422);
      expect(stack.received).toHaveLength(2);
    }),
  120000,
);
