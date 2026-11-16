import { expect, test } from "bun:test";
import { anthropicMessagesReleaseId, geminiGenerateReleaseId } from "@gateway/plugin-sdk";
import { syntheticAdminKey } from "../support/catalog.ts";
import {
  allowOperation,
  chatReplies,
  settledCharge,
  withGatewayStack,
  type UpstreamCall,
} from "../support/gateway-process.ts";

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
  "a Messages client reaching a channel whose plugin only speaks chat is converted to chat upstream and back to Messages JSON and SSE, billed as messages",
  () =>
    withGatewayStack(
      { reply: (call) => (call.body.stream ? chatReplies.sse() : chatReplies.json()) },
      async (stack) => {
        await allowOperation(stack, "messages");
        const request = { max_tokens: 30, messages: [{ role: "user", content: "你好" }] };
        const json = await sendMessages(stack, request);
        expect(json.status).toBe(200);
        expect(await json.json()).toMatchObject({
          type: "message",
          role: "assistant",
          model: stack.setup.model,
          content: [{ type: "text", text: "ok" }],
          stop_reason: "end_turn",
          usage: { input_tokens: 30, output_tokens: 15 },
        });
        const call = stack.received.at(-1)!;
        expect(call.path).toBe("/v1/chat/completions");
        expect(call.body).toEqual({
          model: "synthetic",
          stream: false,
          messages: [{ role: "user", content: [{ type: "text", text: "你好" }] }],
          max_completion_tokens: 30,
        });
        const id = json.headers.get("x-request-id")!;
        expect(await settledCharge(stack.db, id)).toMatchObject({ state: "SUCCEEDED" });
        expect(
          (await stack.db`SELECT operation FROM gateway_requests WHERE id=${id}`)[0].operation,
        ).toBe("messages");

        const streamed = await sendMessages(stack, { ...request, stream: true });
        expect(streamed.status).toBe(200);
        const frames = (await streamed.text())
          .split("\n\n")
          .filter(Boolean)
          .map((frame) => {
            const lines = frame.split("\n");
            return {
              event: lines.find((l) => l.startsWith("event: "))?.slice(7),
              data: JSON.parse(lines.find((l) => l.startsWith("data: "))!.slice(6)),
            };
          });
        expect(frames.map((f) => f.event).filter((e) => e !== "ping")).toEqual([
          "message_start",
          "content_block_start",
          "content_block_delta",
          "content_block_delta",
          "content_block_stop",
          "message_delta",
          "message_stop",
        ]);
        expect(frames.every((f) => f.data.type === f.event)).toBe(true);
        expect(frames[0]!.data.message.model).toBe(stack.setup.model);
        expect(
          frames
            .filter((f) => f.event === "content_block_delta")
            .map((f) => f.data.delta.text)
            .join(""),
        ).toBe("ok");
        expect(frames.find((f) => f.event === "message_delta")!.data).toMatchObject({
          delta: { stop_reason: "end_turn" },
          usage: { output_tokens: 15 },
        });
        expect(stack.received.at(-1)!.body.stream).toBe(true);
        expect(await settledCharge(stack.db, streamed.headers.get("x-request-id")!)).toMatchObject({
          state: "SUCCEEDED",
        });

        const lossy = await sendMessages(stack, {
          max_tokens: 30,
          messages: [
            {
              role: "user",
              content: [{ type: "text", text: "你好", cache_control: { type: "ephemeral" } }],
            },
          ],
        });
        expect(lossy.status).toBe(422);
        expect(((await lossy.json()) as { error: { code: string } }).error.code).toBe(
          "conversion_loss",
        );
        expect(stack.received).toHaveLength(2);
      },
    ),
  120000,
);

test(
  "a Gemini client reaching a channel whose plugin only speaks chat is converted with the model and stream from the URL and answered in Gemini format",
  () =>
    withGatewayStack({ reply: () => chatReplies.json() }, async (stack) => {
      await allowOperation(stack, "gemini");
      const response = await fetch(
        `${stack.gateway.origin}/v1beta/models/${stack.setup.model}:generateContent`,
        {
          method: "POST",
          headers: { "x-goog-api-key": stack.setup.key.key, "content-type": "application/json" },
          body: JSON.stringify({
            contents: [{ role: "user", parts: [{ text: "你好" }] }],
            generationConfig: { maxOutputTokens: 30 },
          }),
        },
      );
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({
        candidates: [{ content: { role: "model", parts: [{ text: "ok" }] }, finishReason: "STOP" }],
        usageMetadata: { promptTokenCount: 30, candidatesTokenCount: 15 },
      });
      expect(stack.received.at(-1)!.body).toEqual({
        model: "synthetic",
        stream: false,
        messages: [{ role: "user", content: [{ type: "text", text: "你好" }] }],
        max_completion_tokens: 30,
      });
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
  const id = `resp_synthetic_${call.index}`;
  if (call.body.stream)
    return new Response(
      responsesEvents
        .map((data) =>
          "response" in data
            ? { ...data, response: { ...(data.response as Record<string, unknown>), id } }
            : data,
        )
        .map((data) => `event: ${data.type}\ndata: ${JSON.stringify(data)}\n\n`)
        .join(""),
      { headers: { "content-type": "text/event-stream" } },
    );
  return Response.json({ ...responsesBody, id });
}

test(
  "an OpenAI Responses client is served through /v1/responses with the model and response identity rewritten, JSON and SSE, and exact settlement; an upstream previous_response_id is refused before reaching the upstream",
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
      const publicId = (response: Response) =>
        `response_${response.headers.get("x-request-id")!.replaceAll("-", "")}`;
      expect(await json.json()).toEqual({
        ...responsesBody,
        id: publicId(json),
        model: stack.setup.model,
      });
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
      const identity = { id: publicId(streamed), model: stack.setup.model };
      expect(frames).toEqual([
        ["response.created", { ...created, response: { ...created.response, ...identity } }],
        ["response.output_text.delta", responsesEvents[1]],
        [
          "response.completed",
          { ...responsesEvents[2], response: { ...responsesBody, ...identity } },
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

const embedding = (model: string) =>
  `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.10000000000000001,-2]}],"model":${JSON.stringify(model)},"usage":{"prompt_tokens":4,"total_tokens":4}}`;
const moderation = { id: "modr-1", model: "synthetic-2025", results: [{ flagged: false }] };
const completion = {
  id: "cmpl-1",
  object: "text_completion",
  model: "synthetic-2025",
  choices: [{ text: "春眠", index: 0, finish_reason: "stop" }],
  usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 },
};

function nativeReply(call: UpstreamCall) {
  if (call.path === "/v1/embeddings")
    return new Response(embedding("synthetic-2025"), {
      headers: { "content-type": "application/json" },
    });
  if (call.path === "/v1/moderations") return Response.json(moderation);
  return Response.json(completion);
}

test(
  "embeddings, moderations and completions are served by the v2 Gateway with only the model rewritten; embeddings settle by input characters and streaming is refused before the upstream",
  () =>
    withGatewayStack({ reply: nativeReply }, async (stack) => {
      await allowOperation(stack, "embeddings", {
        pricing: { currency: "USD", rates: { characters: { numerator: "7", denominator: "1" } } },
        ceilings: { characters: "100" },
        basis: "INPUT_CHARACTERS",
      });
      await allowOperation(stack, "moderations");
      await allowOperation(stack, "completions");
      const send = (path: string, body: Record<string, unknown>) =>
        fetch(`${stack.gateway.origin}${path}`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${stack.setup.key.key}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({ model: stack.setup.model, ...body }),
        });

      const embedded = await send("/v1/embeddings", { input: ["你好", "世界"] });
      expect(embedded.status).toBe(200);
      expect(await embedded.text()).toBe(embedding(stack.setup.model));
      expect(stack.received.at(-1)).toMatchObject({
        path: "/v1/embeddings",
        body: { model: "synthetic", input: ["你好", "世界"] },
      });
      expect(await settledCharge(stack.db, embedded.headers.get("x-request-id")!)).toEqual({
        amount: "28",
        state: "SUCCEEDED",
        billing_outcome: expect.any(String),
      });

      const moderated = await send("/v1/moderations", { input: "检查" });
      expect(moderated.status).toBe(200);
      expect(await moderated.json()).toEqual({ ...moderation, model: stack.setup.model });
      expect(stack.received.at(-1)!.path).toBe("/v1/moderations");
      expect(await settledCharge(stack.db, moderated.headers.get("x-request-id")!)).toMatchObject({
        amount: "100",
        state: "SUCCEEDED",
      });

      const completed = await send("/v1/completions", { prompt: "写诗", max_tokens: 20 });
      expect(completed.status).toBe(200);
      expect(await completed.json()).toEqual({ ...completion, model: stack.setup.model });
      expect(stack.received.at(-1)).toMatchObject({
        path: "/v1/completions",
        body: { model: "synthetic", prompt: "写诗", max_tokens: 20 },
      });
      expect(await settledCharge(stack.db, completed.headers.get("x-request-id")!)).toMatchObject({
        amount: "100",
        state: "SUCCEEDED",
      });

      const calls = stack.received.length;
      const streamed = await send("/v1/embeddings", { input: "x", stream: true });
      expect(streamed.status).toBe(422);
      expect(stack.received).toHaveLength(calls);
    }),
  120000,
);

test(
  "completions on a character tariff are charged for the prompt's characters, for a string and a list of prompts, instead of settling to zero",
  () =>
    withGatewayStack({ reply: nativeReply }, async (stack) => {
      await allowOperation(stack, "completions", {
        pricing: { currency: "USD", rates: { characters: { numerator: "7", denominator: "1" } } },
        ceilings: { characters: "100" },
        basis: "INPUT_CHARACTERS",
      });
      for (const [prompt, amount] of [
        ["写诗", "14"],
        [["写诗", "作画吧"], "35"],
      ] as const) {
        const completed = await fetch(`${stack.gateway.origin}/v1/completions`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${stack.setup.key.key}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({ model: stack.setup.model, prompt, max_tokens: 20 }),
        });
        expect(completed.status).toBe(200);
        await completed.text();
        expect(await settledCharge(stack.db, completed.headers.get("x-request-id")!)).toMatchObject(
          { amount, state: "SUCCEEDED" },
        );
      }
    }),
  120000,
);

test(
  "server-executed tools without a billing dimension are refused with 422 unmetered_tool before Core or the upstream sees the request, while client-executed tools are served on every protocol (the v2 Gateway forwarded unmetered hosted tools)",
  () =>
    withGatewayStack(
      {
        reply: (call) =>
          call.path === "/v1/responses" ? responsesReply(call) : chatReplies.json(),
      },
      async (stack) => {
        for (const operation of ["responses", "messages", "gemini"] as const)
          await allowOperation(stack, operation);
        const send = (protocol: "chat" | "responses" | "messages" | "gemini", body: object) =>
          protocol === "gemini"
            ? fetch(`${stack.gateway.origin}/v1beta/models/${stack.setup.model}:generateContent`, {
                method: "POST",
                headers: {
                  "x-goog-api-key": stack.setup.key.key,
                  "content-type": "application/json",
                },
                body: JSON.stringify({
                  contents: [{ role: "user", parts: [{ text: "你好" }] }],
                  generationConfig: { maxOutputTokens: 30 },
                  ...body,
                }),
              })
            : fetch(
                `${stack.gateway.origin}/v1/${protocol === "chat" ? "chat/completions" : protocol}`,
                {
                  method: "POST",
                  headers: {
                    authorization: `Bearer ${stack.setup.key.key}`,
                    "content-type": "application/json",
                  },
                  body: JSON.stringify({
                    model: stack.setup.model,
                    ...(protocol === "responses"
                      ? { input: "你好", max_output_tokens: 30 }
                      : { messages: [{ role: "user", content: "你好" }], max_tokens: 30 }),
                    ...body,
                  }),
                },
              );
        const requests = async () =>
          (await stack.db`SELECT count(*)::integer n FROM gateway_requests`)[0].n as number;
        const before = await requests();
        for (const [protocol, body, code] of [
          ["chat", { tools: [{ type: "web_search" }] }, "unmetered_tool"],
          ["chat", { web_search_options: {} }, "unmetered_tool"],
          ["responses", { tools: [{ type: "web_search_preview" }] }, "unmetered_tool"],
          [
            "responses",
            { tools: [{ type: "code_interpreter", container: { type: "auto" } }] },
            "unmetered_tool",
          ],
          ["responses", { tools: [{ type: "file_search" }] }, "unmetered_tool"],
          [
            "responses",
            { tools: [{ type: "file_search", vector_store_ids: ["vs"] }] },
            "unmanaged_resource",
          ],
          [
            "messages",
            { tools: [{ type: "web_search_20250305", name: "web_search" }] },
            "unmetered_tool",
          ],
          [
            "messages",
            { tools: [{ type: "code_execution_20250522", name: "code_execution" }] },
            "unmetered_tool",
          ],
          ["gemini", { tools: [{ googleSearch: {} }] }, "unmetered_tool"],
          [
            "gemini",
            { tools: [{ functionDeclarations: [], codeExecution: {} }] },
            "unmetered_tool",
          ],
          ["gemini", { tools: [{ urlContext: {} }] }, "unmetered_tool"],
        ] as const) {
          const refused = await send(protocol, body);
          expect([protocol, refused.status]).toEqual([protocol, 422]);
          expect((await refused.json()).error.code).toBe(code);
        }
        expect(stack.received).toHaveLength(0);
        expect(await requests()).toBe(before);
        for (const [protocol, body] of [
          [
            "chat",
            {
              tools: [
                { type: "function", function: { name: "f", parameters: { type: "object" } } },
              ],
            },
          ],
          [
            "responses",
            { tools: [{ type: "function", name: "f", parameters: { type: "object" } }] },
          ],
          ["messages", { tools: [{ name: "f", input_schema: { type: "object" } }] }],
          [
            "gemini",
            { tools: [{ functionDeclarations: [{ name: "f", parameters: { type: "object" } }] }] },
          ],
        ] as const) {
          const served = await send(protocol, body);
          expect([protocol, served.status]).toEqual([protocol, 200]);
          await served.arrayBuffer();
        }
        expect(stack.received).toHaveLength(4);
      },
    ),
  120000,
);

test(
  "a key's CIDR allow-list is checked against the transport address on the chat and Gemini native paths, so a forged x-forwarded-for neither admits nor refuses it unless the peer is a trusted proxy",
  () =>
    withGatewayStack({ reply: () => chatReplies.json() }, async (stack) => {
      await allowOperation(stack, "gemini");
      const { f, setup, received } = stack;
      const restrict = async (allowed_cidrs: string[]) =>
        expect(
          (await f.request(`/keys/${setup.key.id}`, { allowed_cidrs }, undefined, "PATCH")).status,
        ).toBe(200);
      const answers = async (via: { origin: string }, forwarded: string) => {
        const results: string[] = [];
        for (const [path, headers, body] of [
          [
            "/v1/chat/completions",
            { authorization: `Bearer ${setup.key.key}` },
            {
              model: setup.model,
              messages: [{ role: "user", content: "合成" }],
              max_completion_tokens: 30,
            },
          ],
          [
            `/v1beta/models/${setup.model}:generateContent`,
            { "x-goog-api-key": setup.key.key },
            {
              contents: [{ role: "user", parts: [{ text: "合成" }] }],
              generationConfig: { maxOutputTokens: 30 },
            },
          ],
        ] as const) {
          const response = await fetch(`${via.origin}${path}`, {
            method: "POST",
            headers: {
              ...headers,
              "content-type": "application/json",
              "x-forwarded-for": forwarded,
            },
            body: JSON.stringify(body),
          });
          const text = await response.text();
          results.push(
            response.status === 200 ? "200" : `${response.status}:${JSON.parse(text).error.code}`,
          );
        }
        return results;
      };
      await restrict(["10.0.0.0/8"]);
      expect(await answers(stack.gateway, "10.1.2.3")).toEqual([
        "403:permission_denied",
        "403:permission_denied",
      ]);
      await restrict(["127.0.0.0/8"]);
      expect(await answers(stack.gateway, "10.1.2.3")).toEqual(["200", "200"]);
      const proxied = await stack.startGateway(
        { trusted_proxy_cidrs: ["127.0.0.0/8"] },
        "gatewayB",
      );
      expect(await answers(proxied, "10.1.2.3")).toEqual([
        "403:permission_denied",
        "403:permission_denied",
      ]);
      await restrict(["10.0.0.0/8"]);
      expect(await answers(proxied, "10.1.2.3")).toEqual(["200", "200"]);
      expect(await answers(stack.gateway, "10.1.2.3")).toEqual([
        "403:permission_denied",
        "403:permission_denied",
      ]);
      expect(received).toHaveLength(4);
    }),
  120000,
);
