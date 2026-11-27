import { expect, test } from "bun:test";
import type { AdapterOverrides } from "@gateway/ledger";
import { invokeInference, jsonBody } from "./proxy-response-harness.ts";

test("unary post-dispatch client disconnect settles within grace window and returns 499", async () => {
  const response = await invokeInference(jsonBody, { cancel: true, delayMs: 25 });
  expect({ status: response.status, finalized: response.finalized }, response.wire).toEqual({
    status: 499,
    finalized: ["SETTLED"],
  });
  expect(response.settledUsages).toEqual([{ input_tokens: "3", output_tokens: "2" }]);
  expect(response.transportCancelled).toBe(false);
});

test("symptom: a buffered response settles after the client disconnects when grace is disabled or exceeded", async () => {
  const disabled = await invokeInference(jsonBody, {
    cancel: true,
    configOverrides: { unaryClientAbortGraceMs: 0 },
  });
  expect({ status: disabled.status, finalized: disabled.finalized }, disabled.wire).toEqual({
    status: 499,
    finalized: ["UNKNOWN"],
  });
  expect(JSON.parse(disabled.wire)).toMatchObject({ error: { code: "client_aborted" } });
  expect(disabled.transportCancelled).toBe(true);

  const expired = await invokeInference(jsonBody, {
    cancel: true,
    delayMs: 80,
    configOverrides: { unaryClientAbortGraceMs: 15 },
  });
  expect({ status: expired.status, finalized: expired.finalized }, expired.wire).toEqual({
    status: 499,
    finalized: ["UNKNOWN"],
  });
  expect(expired.transportCancelled).toBe(true);

  const preDispatch = await invokeInference(jsonBody, { cancelBeforeDispatch: true });
  expect(preDispatch.dispatches).toBe(0);
  expect(preDispatch.status).toBe(499);
  expect(preDispatch.finalized).toEqual(["RELEASED"]);
});

test("symptom: a charged JSON response passes an upstream BOM to strict clients", async () => {
  const response = await invokeInference(`\uFEFF${jsonBody}`);
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["SETTLED"]);
  expect(JSON.parse(response.wire)).toMatchObject({
    usage: { prompt_tokens: 3, completion_tokens: 2 },
  });
});

test("symptom: a later SSE error in the same upstream read drops already validated text", async () => {
  const response = await invokeInference(
    'data: {"choices":[{"index":0,"delta":{"content":"valid answer prefix"}}]}\n\n' +
      'event: error\ndata: {"error":{"message":"Provider failed"}}\n\n',
    { stream: true },
  );
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["UNKNOWN"]);
  expect(response.wire).toContain("provider_stream_error");
  expect(response.wire).toContain("valid answer prefix");
  expect(response.wire.indexOf("valid answer prefix")).toBeLessThan(
    response.wire.indexOf("event: error"),
  );
});

test("fast-buffered SSE settles exact usage and returns complete SSE frames", async () => {
  const sseBody =
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"role":"assistant","content":"hello"}}]}\n\n' +
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2}}\n\n' +
    "data: [DONE]\n\n";
  const response = await invokeInference(sseBody, { stream: true, fast: true });
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["SETTLED"]);
  expect(response.wire).toContain('"content":"hello"');
  expect(response.wire).toContain("data: [DONE]\n\n");
});

test("fast-buffered SSE settles via plaintext estimate on incomplete trailing frame with output and stays unknown when disabled or zero output", async () => {
  const sseBody =
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"content":"partial answer text"}}]}\n\n' +
    'data: {"truncated":true';
  const estimated = await invokeInference(sseBody, { stream: true, fast: true });
  expect(estimated.status, estimated.wire).toBe(200);
  expect(estimated.finalized).toEqual(["SETTLED"]);
  expect(Number(estimated.settledUsages[0]?.output_tokens)).toBe(5);
  expect(estimated.wire).toContain('"content":"partial answer text"');
  expect(estimated.wire).toContain("stream_interrupted");
  expect(estimated.wire).not.toContain("data: [DONE]");

  const strict = await invokeInference(sseBody, {
    stream: true,
    fast: true,
    configOverrides: { streamEstimatedSettlement: false },
  });
  expect(strict.status, strict.wire).toBe(200);
  expect(strict.finalized).toEqual(["UNKNOWN"]);
  expect(strict.wire).toContain("stream_interrupted");

  const zeroOutput = await invokeInference(
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"role":"assistant"}}]}\n\n' +
      'data: {"truncated":true',
    { stream: true, fast: true },
  );
  expect(zeroOutput.status, zeroOutput.wire).toBe(200);
  expect(zeroOutput.finalized).toEqual(["UNKNOWN"]);
  expect(zeroOutput.wire).toContain("stream_interrupted");
});

test("fast-buffered SSE settles via plaintext estimate when usage metadata is missing and stays unknown when disabled", async () => {
  const sseBody =
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"content":"answer"}}]}\n\n' +
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n' +
    "data: [DONE]\n\n";
  const estimated = await invokeInference(sseBody, { stream: true, fast: true });
  expect(estimated.status, estimated.wire).toBe(200);
  expect(estimated.finalized).toEqual(["SETTLED"]);
  expect(Number(estimated.settledUsages[0]?.input_tokens)).toBeGreaterThan(0);
  expect(Number(estimated.settledUsages[0]?.output_tokens)).toBe(2);
  expect(estimated.estimatedStreamSettlements).toBe(1);
  expect(estimated.wire).toContain('"content":"answer"');
  expect(estimated.wire).toContain("data: [DONE]\n\n");

  const strict = await invokeInference(sseBody, {
    stream: true,
    fast: true,
    configOverrides: { streamEstimatedSettlement: false },
  });
  expect(strict.status, strict.wire).toBe(200);
  expect(strict.finalized).toEqual(["UNKNOWN"]);
  expect(strict.wire).toContain('"content":"answer"');
  expect(strict.wire).toContain("invalid_upstream_usage");
  expect(strict.wire).not.toContain("data: [DONE]");
});

test("symptom: fast-buffered SSE drops already validated text when a later event errors", async () => {
  const response = await invokeInference(
    'data: {"choices":[{"index":0,"delta":{"content":"valid fast prefix"}}]}\n\n' +
      'event: error\ndata: {"error":{"message":"Provider failed"}}\n\n',
    { stream: true, fast: true },
  );
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["UNKNOWN"]);
  expect(response.wire).toContain("provider_stream_error");
  expect(response.wire).toContain("valid fast prefix");
  expect(response.wire.indexOf("valid fast prefix")).toBeLessThan(
    response.wire.indexOf("event: error"),
  );
});

test.each([
  { operation: "chat" as const, fields: { n: 2 }, code: "billing_ceiling" },
  {
    operation: "completions" as const,
    fields: { prompt: ["a", "b"], best_of: 2 },
    code: "billing_ceiling",
  },
  {
    operation: "gemini" as const,
    fields: { contents: [{ parts: [{ text: "Hi" }] }], generationConfig: { candidateCount: 2 } },
    code: "billing_ceiling",
  },
  {
    operation: "responses" as const,
    fields: { input: "Hi", previous_response_id: "resp_previous" },
    code: "unmetered_context",
  },
  {
    operation: "responses" as const,
    fields: { input: [{ type: "item_reference", id: "item_old" }] },
    code: "unmetered_context",
  },
  {
    operation: "gemini" as const,
    fields: { contents: [{ parts: [{ text: "Hi" }] }], cachedContent: "cachedContents/existing" },
    code: "unmetered_context",
  },
  {
    operation: "responses" as const,
    fields: { input: "Hi", tools: [{ type: "shell", environment: { type: "container_auto" } }] },
    code: "unmetered_tool",
  },
])(
  "symptom: $operation forwards an unbounded request ($fields)",
  async ({ operation, fields, code }) => {
    const response = await invokeInference(jsonBody, { operation, fields });
    expect(response.dispatches, response.wire).toBe(0);
    expect(response.status, response.wire).toBe(422);
    expect(JSON.parse(response.wire)).toMatchObject({
      error: operation === "gemini" ? { gateway_code: code } : { code },
    });
    expect(response.finalized).not.toContain("SETTLED");
  },
);

test("symptom: a route transform adds a hosted tool after client validation", async () => {
  const response = await invokeInference(jsonBody, {
    overrides: {
      request_transforms: [{ op: "force_set", path: "web_search_options", value: {} }],
    },
  });
  expect(response.dispatches, response.wire).toBe(0);
  expect(response.status, response.wire).toBe(422);
  expect(JSON.parse(response.wire)).toMatchObject({ error: { code: "unmetered_tool" } });
  expect(response.holds).toBe(0);
  expect(response.finalized).toEqual([]);
});

test("symptom: JSON settles when the provider omits metered output tokens", async () => {
  const response = await invokeInference(jsonBody.replace(',"completion_tokens":2', ""));
  expect(response.status, response.wire).toBe(502);
  expect(response.finalized).toEqual(["UNKNOWN"]);
  expect(JSON.parse(response.wire)).toMatchObject({ error: { code: "invalid_upstream_usage" } });
});

const chatStreamUsageCases: {
  fast: boolean;
  opening: string;
  overrides: AdapterOverrides;
  evidence: string;
}[] = [
  { fast: false, opening: "", overrides: {}, evidence: "missing" },
  {
    fast: true,
    opening: '"usage":{"prompt_tokens":3,"completion_tokens":1}',
    overrides: {},
    evidence: "stale native",
  },
  {
    fast: true,
    opening: '"meter":{"output":1}',
    overrides: {
      usage_aliases: [{ target: "output_tokens", source_path: "meter.output" }],
    },
    evidence: "stale mapped",
  },
];
test.each(chatStreamUsageCases)(
  "symptom: Chat SSE strips $evidence output usage in strict mode and estimates plaintext instead of stale counts by default",
  async ({ fast, opening, overrides }) => {
    const streamPayload =
      (opening
        ? `data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"beginning"}}],${opening}}\n\n`
        : "") +
      'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3}}\n\n' +
      "data: [DONE]\n\n";
    const strict = await invokeInference(streamPayload, {
      stream: true,
      fast,
      overrides,
      configOverrides: { streamEstimatedSettlement: false },
    });
    expect(strict.status, strict.wire).toBe(200);
    expect(strict.finalized).toEqual(["UNKNOWN"]);
    expect(strict.wire).toContain("invalid_upstream_usage");
    expect(strict.wire).not.toContain("data: [DONE]");

    const estimated = await invokeInference(streamPayload, { stream: true, fast, overrides });
    expect(estimated.status, estimated.wire).toBe(200);
    expect(estimated.finalized).toEqual(["SETTLED"]);
    expect(estimated.settledUsages[0]?.input_tokens).toBe("3");
    expect(Number(estimated.settledUsages[0]?.output_tokens)).toBe(opening ? 4 : 2);
    expect(estimated.wire).toContain("data: [DONE]");
  },
);

test("an explicit zero metered output settles normally", async () => {
  const response = await invokeInference(
    jsonBody.replace('"completion_tokens":2', '"completion_tokens":0'),
    {
      fields: { n: 1 },
    },
  );
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["SETTLED"]);
});

test("symptom: Anthropic SSE ignores provisional output_tokens in strict mode and estimates plaintext output while keeping input_tokens by default", async () => {
  const anthropicPayload = [
    {
      type: "message_start",
      message: {
        id: "msg_1",
        model: "test-chat",
        role: "assistant",
        content: [],
        usage: { input_tokens: 10, output_tokens: 1 },
      },
    },
    { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
    {
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text: "An actual answer" },
    },
    { type: "content_block_stop", index: 0 },
    { type: "message_delta", delta: { stop_reason: "end_turn" } },
    { type: "message_stop" },
  ]
    .map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`)
    .join("");

  const strict = await invokeInference(anthropicPayload, {
    operation: "messages",
    stream: true,
    configOverrides: { streamEstimatedSettlement: false },
  });
  expect(strict.status, strict.wire).toBe(200);
  expect(strict.wire).toContain("An actual answer");
  expect(strict.wire).toContain("invalid_upstream_usage");
  expect(strict.wire).not.toContain("event: message_stop");
  expect(strict.finalized).toEqual(["UNKNOWN"]);

  const estimated = await invokeInference(anthropicPayload, {
    operation: "messages",
    stream: true,
  });
  expect(estimated.status, estimated.wire).toBe(200);
  expect(estimated.wire).toContain("event: message_stop");
  expect(estimated.finalized).toEqual(["SETTLED"]);
  expect(estimated.settledUsages).toEqual([{ input_tokens: "10", output_tokens: "4" }]);
});

test("symptom: Gemini SSE strips stale output usage in strict mode and estimates full plaintext output by default", async () => {
  const geminiPayload = [
    {
      candidates: [{ index: 0, content: { role: "model", parts: [{ text: "Beginning" }] } }],
      usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 1 },
    },
    {
      candidates: [
        {
          index: 0,
          content: { role: "model", parts: [{ text: "Rest of the answer" }] },
          finishReason: "STOP",
        },
      ],
      usageMetadata: { promptTokenCount: 10 },
    },
  ]
    .map((event) => `data: ${JSON.stringify(event)}\n\n`)
    .join("");

  const strict = await invokeInference(geminiPayload, {
    operation: "gemini",
    stream: true,
    fields: { contents: [{ parts: [{ text: "Hi" }] }] },
    configOverrides: { streamEstimatedSettlement: false },
  });
  expect(strict.status, strict.wire).toBe(200);
  expect(strict.wire).toContain("Beginning");
  expect(strict.wire).toContain("invalid_upstream_usage");
  expect(strict.wire).not.toContain('"finishReason":"STOP"');
  expect(strict.finalized).toEqual(["UNKNOWN"]);

  const estimated = await invokeInference(geminiPayload, {
    operation: "gemini",
    stream: true,
    fields: { contents: [{ parts: [{ text: "Hi" }] }] },
  });
  expect(estimated.status, estimated.wire).toBe(200);
  expect(estimated.wire).toContain('"finishReason":"STOP"');
  expect(estimated.finalized).toEqual(["SETTLED"]);
  expect(estimated.settledUsages).toEqual([{ input_tokens: "10", output_tokens: "7" }]);
});

test("stream plaintext estimation clamps estimated tokens to held funds so estimation never fabricates debt", async () => {
  const longChinese = "你好世界测试估算截断".repeat(20);
  const sseBody =
    `data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{"content":"${longChinese}"}}]}\n\n` +
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test-chat","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n' +
    "data: [DONE]\n\n";
  const response = await invokeInference(sseBody, {
    stream: true,
    fast: true,
    heldAtoms: "50",
  });
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["SETTLED"]);
  const usage = response.settledUsages[0]!;
  const total = Number(usage.input_tokens) + Number(usage.output_tokens);
  expect(total).toBeGreaterThanOrEqual(45);
  expect(total).toBeLessThanOrEqual(50);
  expect(Number(usage.output_tokens)).toBeGreaterThan(0);
  expect(response.estimatedStreamSettlements).toBe(1);
});

test("Responses accepts an explicitly local shell", async () => {
  const response = await invokeInference(
    '{"id":"resp_test","object":"response","model":"test-chat","status":"completed","output":[],"usage":{"input_tokens":3,"output_tokens":0}}',
    {
      operation: "responses",
      fields: { input: "Hi", tools: [{ type: "shell", environment: { type: "local" } }] },
    },
  );
  expect(response.status, response.wire).toBe(200);
  expect(response.finalized).toEqual(["SETTLED"]);
});
