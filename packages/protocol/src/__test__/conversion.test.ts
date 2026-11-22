import { expect, test } from "bun:test";
import {
  FIELD_CONVERSION_POLICY,
  StreamTranslator,
  decodeRequest,
  decodeResponse,
  encodeRequest,
  encodeResponse,
  formatProtocolError,
} from "../index.ts";
import type { Protocol } from "@gateway/kernel";

const convert = (body: Record<string, unknown>, source: Protocol, target: Protocol) =>
  encodeRequest(decodeRequest(body, source), target, "target");

test("cross-protocol conversion rejects effort instead of silently dropping it", () => {
  const body = { messages: [{ role: "user", content: "solve" }], output_config: { effort: "low" } };
  expect(() => convert(body, "messages", "gemini")).toThrow("output_config.effort");
});

test("cross-protocol conversion rejects audio and video instead of sending them as images", () => {
  for (const mimeType of ["audio/wav", "video/mp4"]) {
    for (const part of [
      { inlineData: { mimeType, data: "AAAA" } },
      { fileData: { mimeType, fileUri: "gs://media" } },
    ]) {
      const body = { contents: [{ role: "user", parts: [part] }] };
      expect(() => convert(body, "gemini", "messages")).toThrow("image media type");
    }
  }
});

test("cross-protocol conversion rejects strict output and nested tool controls instead of losing them", () => {
  expect(() =>
    convert(
      {
        messages: [],
        response_format: {
          type: "json_schema",
          json_schema: { name: "result", strict: true, schema: { type: "object" } },
        },
      },
      "chat",
      "messages",
    ),
  ).toThrow("strict");
  expect(() =>
    convert(
      {
        messages: [],
        tools: [
          {
            name: "lookup",
            input_schema: { type: "object" },
            cache_control: { type: "ephemeral" },
          },
        ],
      },
      "messages",
      "gemini",
    ),
  ).toThrow("cache_control");
});

test("cross-protocol conversion rejects versioned Anthropic tools instead of weakening their contracts", () => {
  const body = {
    messages: [{ role: "user", content: "run" }],
    tools: [{ type: "bash_20250124", name: "bash" }],
  };
  expect(() => convert(body, "messages", "chat")).toThrow("versioned tool");
});

test("cross-protocol requests preserve zero, ordered tools and results", () => {
  const request = decodeRequest(
    {
      messages: [
        { role: "system", content: "policy" },
        { role: "user", content: "weather?" },
        {
          role: "assistant",
          content: "checking",
          tool_calls: [
            {
              id: "call_1",
              type: "function",
              function: { name: "weather", arguments: '{"city":"北京"}' },
            },
          ],
        },
        { role: "tool", tool_call_id: "call_1", content: '{"degrees":0}' },
      ],
      temperature: 0,
      top_p: 0,
      tools: [{ type: "function", function: { name: "weather", parameters: { type: "object" } } }],
    },
    "chat",
  );
  expect(request.temperature).toBe(0);
  expect(request.topP).toBe(0);
  for (const target of ["chat", "responses", "messages", "gemini"] as Protocol[]) {
    const encoded = encodeRequest(request, target, "target-model");
    const serialized = JSON.stringify(encoded);
    expect(serialized).toContain("北京");
    expect(serialized).toContain("call_1");
    expect(serialized).toContain("degrees");
    expect(serialized).toContain("policy");
  }
});
test("unsupported semantics reject rather than silently vanish", () => {
  expect(() =>
    decodeRequest({ messages: [{ role: "user", content: "x" }], logprobs: true }, "chat"),
  ).toThrow("logprobs");
  expect(() =>
    decodeRequest({ messages: [], tools: [{ type: "web_search" }] }, "responses"),
  ).toThrow();
  expect(() =>
    decodeRequest(
      { contents: [{ role: "model", parts: [{ text: "thought", thoughtSignature: "signed" }] }] },
      "gemini",
    ),
  ).toThrow("thought/signature");
});
test("EOF cannot generate a successful converted terminal", () => {
  const translator = new StreamTranslator("gemini", "chat", "gemini", "logical");
  const result = translator.consume({
    event: "message",
    id: "",
    data: JSON.stringify({ candidates: [{ content: { parts: [{ text: "partial" }] } }] }),
    wire: "",
  });
  expect(translator.terminal).toBe(false);
  expect(new TextDecoder().decode(Buffer.concat(result.chunks))).not.toContain("[DONE]");
});
test("length produces incomplete, one terminal and no synthetic success after error", () => {
  const value = encodeResponse(
    { id: "id", parts: [{ type: "text", text: "limited" }], finish: "length", usage: undefined },
    "responses",
    "logical",
  );
  expect(value.status).toBe("incomplete");
  expect(value.incomplete_details).toEqual({ reason: "max_output_tokens" });
  const translator = new StreamTranslator("messages", "responses", "anthropic", "logical");
  expect(() =>
    translator.consume({ event: "error", id: "", data: '{"type":"error"}', wire: "" }),
  ).toThrow();
  expect(translator.terminal).toBe(false);
});

test("Gemini named tool restrictions and Anthropic error results cannot silently widen", () => {
  const decoded = decodeRequest(
    {
      contents: [{ role: "user", parts: [{ text: "run allowed" }] }],
      tools: [
        {
          functionDeclarations: [
            { name: "allowed", parameters: { type: "object" } },
            { name: "forbidden", parameters: { type: "object" } },
          ],
        },
      ],
      toolConfig: { functionCallingConfig: { mode: "ANY", allowedFunctionNames: ["allowed"] } },
    },
    "gemini",
  );
  const chat = encodeRequest(decoded, "chat", "target");
  expect(chat.tool_choice).toEqual({ type: "function", function: { name: "allowed" } });
  expect(JSON.stringify(chat.tools)).not.toContain("forbidden");
  expect(() =>
    decodeRequest(
      {
        messages: [
          {
            role: "user",
            content: [
              { type: "tool_result", tool_use_id: "call", is_error: true, content: "denied" },
            ],
          },
        ],
      },
      "messages",
    ),
  ).toThrow("error or structured tool result");
});

test("tool-only converted streams use contiguous stable target block identities", () => {
  const frames = [
    {
      choices: [
        {
          delta: {
            tool_calls: [
              { index: 0, id: "call_1", function: { name: "weather", arguments: '{"city":' } },
            ],
          },
          finish_reason: null,
        },
      ],
    },
    {
      choices: [
        {
          delta: { tool_calls: [{ index: 0, function: { arguments: '"北京"}' } }] },
          finish_reason: null,
        },
      ],
    },
    { choices: [{ delta: {}, finish_reason: "tool_calls" }] },
  ];
  for (const target of ["messages", "responses"] as const) {
    const translator = new StreamTranslator("chat", target, "openai", "logical");
    const chunks: Uint8Array[] = [];
    for (const frame of frames)
      chunks.push(
        ...translator.consume({ event: "message", data: JSON.stringify(frame), id: "", wire: "" })
          .chunks,
      );
    chunks.push(
      ...translator.consume({ event: "message", data: "[DONE]", id: "", wire: "" }).chunks,
      ...translator.finalize(),
    );
    const output = new TextDecoder().decode(Buffer.concat(chunks));
    if (target === "messages") {
      expect(output).toContain('"content_block_start","index":0');
      expect(output).not.toContain('"index":1');
    } else {
      expect(output).toContain('"item_id":"fc_0"');
      expect(output).not.toContain('"fc_1"');
      expect(output).toContain('"output_index":0');
    }
    expect(output).toContain("北京");
  }
});

test("cross-protocol reasoning parts convert cleanly in responses and streams without 502", () => {
  const fromChat = decodeResponse(
    {
      id: "chat_1",
      choices: [
        {
          index: 0,
          message: {
            role: "assistant",
            reasoning_content: "step by step",
            content: "42",
          },
          finish_reason: "stop",
        },
      ],
      usage: { prompt_tokens: 5, completion_tokens: 10, total_tokens: 15 },
    },
    "chat",
    "openai",
  );
  expect(fromChat.parts).toEqual([
    { type: "reasoning", text: "step by step" },
    { type: "text", text: "42" },
  ]);

  const toMessages = encodeResponse(fromChat, "messages", "m1");
  expect(toMessages.content).toEqual([
    { type: "thinking", thinking: "step by step" },
    { type: "text", text: "42" },
  ]);

  const toResponses = encodeResponse(fromChat, "responses", "m1");
  expect(toResponses.output).toEqual([
    { id: "rs_0", type: "reasoning", summary: [{ type: "summary_text", text: "step by step" }] },
    {
      id: "msg_1",
      type: "message",
      role: "assistant",
      status: "completed",
      content: [{ type: "output_text", text: "42", annotations: [] }],
    },
  ]);

  const toGemini = encodeResponse(fromChat, "gemini", "m1");
  expect((toGemini.candidates as { content: { parts: unknown[] } }[])[0]!.content.parts).toEqual([
    { text: "step by step", thought: true },
    { text: "42" },
  ]);

  const roundTripMessages = decodeResponse(toMessages, "messages", "anthropic");
  expect(encodeResponse(roundTripMessages, "chat", "m1")).toMatchObject({
    choices: [{ message: { reasoning_content: "step by step", content: "42" } }],
  });

  const roundTripGemini = decodeResponse(toGemini, "gemini", "gemini");
  expect(encodeResponse(roundTripGemini, "chat", "m1")).toMatchObject({
    choices: [{ message: { reasoning_content: "step by step", content: "42" } }],
  });

  const roundTripResponses = decodeResponse(toResponses, "responses", "openai");
  expect(encodeResponse(roundTripResponses, "chat", "m1")).toMatchObject({
    choices: [{ message: { reasoning_content: "step by step", content: "42" } }],
  });

  for (const target of ["messages", "responses", "gemini"] as const) {
    const translator = new StreamTranslator("chat", target, "openai", "m1");
    const chunks = [
      ...translator.consume({
        event: "message",
        id: "",
        wire: "",
        data: JSON.stringify({
          choices: [{ index: 0, delta: { reasoning_content: "think " }, finish_reason: null }],
        }),
      }).chunks,
      ...translator.consume({
        event: "message",
        id: "",
        wire: "",
        data: JSON.stringify({
          choices: [{ index: 0, delta: { content: "answer" }, finish_reason: null }],
        }),
      }).chunks,
      ...translator.consume({
        event: "message",
        id: "",
        wire: "",
        data: JSON.stringify({
          choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
          usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
        }),
      }).chunks,
      ...translator.consume({ event: "message", id: "", wire: "", data: "[DONE]" }).chunks,
      ...translator.finalize(),
    ];
    const wire = new TextDecoder().decode(Buffer.concat(chunks));
    expect(wire).toContain("think ");
    expect(wire).toContain("answer");
    if (target === "messages") expect(wire).toContain('"thinking_delta"');
    if (target === "responses") expect(wire).toContain("response.reasoning_summary_text.delta");
    if (target === "gemini") expect(wire).toContain('"thought":true');
  }

  const fromAnthropicHistory = decodeRequest(
    {
      messages: [
        { role: "user", content: "hi" },
        {
          role: "assistant",
          content: [
            { type: "thinking", thinking: "private thought" },
            { type: "text", text: "hello" },
          ],
        },
        { role: "user", content: "next" },
      ],
    },
    "messages",
  );
  const encodedChat = encodeRequest(fromAnthropicHistory, "chat", "m1");
  expect(JSON.stringify(encodedChat)).not.toContain("private thought");
  expect(JSON.stringify(encodedChat)).toContain("hello");
  const encodedAnthropicUnsigned = encodeRequest(fromAnthropicHistory, "messages", "m1");
  expect(JSON.stringify(encodedAnthropicUnsigned)).not.toContain("private thought");
});

test("field conversion policy table supports compat drop_hint, bidirectional parallel_tool_calls mapping, and strict reject", () => {
  expect(
    FIELD_CONVERSION_POLICY.some((e) => e.field === "cache_control" && e.action === "drop_hint"),
  ).toBe(true);
  expect(FIELD_CONVERSION_POLICY.some((e) => e.field === "logprobs" && e.action === "reject")).toBe(
    true,
  );

  const dropped: string[] = [];
  const compatReq = decodeRequest(
    {
      messages: [
        {
          role: "user",
          content: [{ type: "text", text: "hi", cache_control: { type: "ephemeral" } }],
        },
      ],
      tools: [
        {
          name: "lookup",
          input_schema: { type: "object" },
          cache_control: { type: "ephemeral" },
        },
      ],
      tool_choice: { type: "auto", disable_parallel_tool_use: true },
      top_k: 40,
      metadata: { user_id: "u1" },
      service_tier: "auto",
    },
    "messages",
    {
      mode: "compat",
      onFieldDropped: (field) => dropped.push(field),
    },
  );
  expect(compatReq.parallelToolCalls).toBe(false);
  expect(compatReq.topK).toBe(40);
  expect(dropped).toContain("metadata");
  expect(dropped).toContain("service_tier");
  expect(dropped).toContain("content.cache_control");
  expect(dropped).toContain("tools.cache_control");

  const finalDropped: string[] = [];
  const chatOut = encodeRequest(compatReq, "chat", "gpt-4o", {
    mode: "compat",
    droppedFields: finalDropped,
    onFieldDropped: (field) => dropped.push(field),
  });
  expect(chatOut.parallel_tool_calls).toBe(false);
  expect(finalDropped).toContain("top_k");
  expect(finalDropped).toContain("metadata");

  const rejected: string[] = [];
  expect(() =>
    decodeRequest({ messages: [{ role: "user", content: "x" }], logprobs: true }, "chat", {
      mode: "compat",
      onFieldRejected: (field) => rejected.push(field),
    }),
  ).toThrow("logprobs");
  expect(rejected).toEqual(["logprobs"]);

  expect(formatProtocolError("messages", 429, "rate_limit_rpm", "Too many", "req-1")).toEqual({
    type: "error",
    error: {
      type: "rate_limit_error",
      code: "rate_limit_rpm",
      message: "Too many",
      request_id: "req-1",
    },
  });
  expect(formatProtocolError("gemini", 503, "no_route", "No channel", "req-2")).toEqual({
    error: {
      code: 503,
      status: "UNAVAILABLE",
      gateway_code: "no_route",
      message: "No channel",
      request_id: "req-2",
    },
  });
  expect(
    formatProtocolError("chat", 400, "content_blocked", "Blocked", "req-3", { rule_code: "R1" }),
  ).toEqual({
    error: {
      type: "content_policy_violation",
      code: "content_blocked",
      message: "Blocked",
      request_id: "req-3",
      rule_code: "R1",
    },
  });

  const geminiDropped: string[] = [];
  const geminiCompat = decodeRequest(
    {
      contents: [
        {
          role: "model",
          parts: [
            { text: "visible answer", thoughtSignature: "sig_1" },
            {
              functionCall: { id: "c1", name: "lookup", args: { q: "x" } },
              thoughtSignature: "sig_2",
            },
            { text: "plain", thought: false },
          ],
        },
      ],
    },
    "gemini",
    { mode: "compat", onFieldDropped: (field) => geminiDropped.push(field) },
  );
  expect(geminiDropped).toEqual(["part.thoughtSignature"]);
  expect(geminiCompat.messages[0]!.parts).toEqual([
    { type: "text", text: "visible answer" },
    { type: "call", id: "c1", name: "lookup", arguments: '{"q":"x"}' },
    { type: "text", text: "plain" },
  ]);

  const respEmptySummary = decodeResponse(
    {
      id: "resp_r1",
      status: "completed",
      output: [
        {
          id: "rs_0",
          type: "reasoning",
          status: "completed",
          summary: [],
          content: [{ type: "reasoning_text", text: "deep thought" }],
        },
        {
          id: "msg_1",
          type: "message",
          content: [{ type: "output_text", text: "done" }],
        },
      ],
    },
    "responses",
    "openai",
  );
  expect(respEmptySummary.parts).toEqual([
    { type: "reasoning", text: "deep thought" },
    { type: "text", text: "done" },
  ]);

  const respRoundTripReq = decodeRequest(
    {
      input: [
        {
          id: "rs_0",
          type: "reasoning",
          status: "completed",
          summary: [],
          content: [{ type: "reasoning_text", text: "history thought" }],
        },
        { type: "message", role: "user", content: "follow up" },
      ],
    },
    "responses",
  );
  expect(respRoundTripReq.messages[0]!.parts).toEqual([
    { type: "reasoning", text: "history thought" },
  ]);

  const noToolsMsg = encodeRequest(
    decodeRequest(
      { messages: [{ role: "user", content: "hi" }], parallel_tool_calls: false },
      "chat",
    ),
    "messages",
    "claude",
  );
  expect(noToolsMsg.tool_choice).toBeUndefined();
  const noneChoiceMsg = encodeRequest(
    decodeRequest(
      {
        messages: [{ role: "user", content: "hi" }],
        tools: [{ type: "function", function: { name: "f", parameters: { type: "object" } } }],
        tool_choice: "none",
        parallel_tool_calls: false,
      },
      "chat",
    ),
    "messages",
    "claude",
  );
  expect(noneChoiceMsg.tool_choice).toEqual({ type: "none" });

  const interleaved = new StreamTranslator("chat", "messages", "openai", "m1");
  const interChunks = [
    ...interleaved.consume({
      event: "message",
      id: "",
      wire: "",
      data: JSON.stringify({
        choices: [{ index: 0, delta: { reasoning_content: "r1" }, finish_reason: null }],
      }),
    }).chunks,
    ...interleaved.consume({
      event: "message",
      id: "",
      wire: "",
      data: JSON.stringify({
        choices: [{ index: 0, delta: { content: "t1" }, finish_reason: null }],
      }),
    }).chunks,
    ...interleaved.consume({
      event: "message",
      id: "",
      wire: "",
      data: JSON.stringify({
        choices: [{ index: 0, delta: { reasoning_content: "r2" }, finish_reason: null }],
      }),
    }).chunks,
    ...interleaved.consume({
      event: "message",
      id: "",
      wire: "",
      data: JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }] }),
    }).chunks,
    ...interleaved.consume({ event: "message", id: "", wire: "", data: "[DONE]" }).chunks,
    ...interleaved.finalize(),
  ];
  const interWire = new TextDecoder().decode(Buffer.concat(interChunks));
  expect(interWire).toContain('"content_block_start","index":2,"content_block":{"type":"thinking"');
});

test("StreamTranslator falls back to top-level usage when message object is empty", () => {
  const translator = new StreamTranslator("messages", "messages", "anthropic", "claude-3");
  translator.consume({
    event: "message_delta",
    id: "",
    wire: "",
    data: JSON.stringify({
      type: "message_delta",
      message: {},
      delta: { stop_reason: "end_turn" },
      usage: { input_tokens: 12, output_tokens: 7 },
    }),
  });
  expect(translator.usage).toMatchObject({ input_tokens: "12", output_tokens: "7" });
});
