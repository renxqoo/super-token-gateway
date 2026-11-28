import { quoteReservation } from "@gateway/kernel";
import { requestCharacters } from "../characters.ts";
import { expect, test } from "bun:test";
import { calculateCost } from "@gateway/kernel";
import {
  extractUsage,
  parseClientJson,
  parseProviderJson,
  stringifyExact,
  StreamTranslator,
  validateUsageObject,
} from "../index.ts";
import type { Operation, ReservationPrice } from "@gateway/kernel";

test.each([
  ["audio_seconds", "invalid", "invalid_numeric_literal"],
  ["video_seconds", "1".repeat(201), "numeric_limit"],
  ["seconds", "invalid", "invalid_numeric_literal"],
])("malformed upstream %s retains its 502 protocol error", (field, value, code) => {
  expect(() => extractUsage({ usage: { type: "duration", [field]: value } }, "openai")).toThrow(
    expect.objectContaining({ status: 502, code }),
  );
});

test("oversized raw upstream integer usage retains its 502 numeric limit error", () => {
  const body = parseProviderJson(`{"usage":{"input_tokens":${"1".repeat(201)}}}`);
  expect(() => extractUsage(body, "openai")).toThrow(
    expect.objectContaining({ status: 502, code: "numeric_limit" }),
  );
});

test("exact upstream fractional seconds retain milliseconds without rounding", () => {
  expect(extractUsage({ usage: { audio_seconds: "0.125" } }, "openai")).toEqual({
    audio_seconds: "0.125",
    audio_ms: "125",
  });
});

test("mixed cache TTLs retain separately priced quantities instead of collapsing their costs", () => {
  const usage = extractUsage(
    {
      usage: {
        cache_creation_input_tokens: 200,
        cache_creation: { ephemeral_5m_input_tokens: 100, ephemeral_1h_input_tokens: 100 },
      },
    },
    "anthropic",
  );
  expect(usage).toEqual({
    cache_write_tokens: "200",
    cache_write_5m_tokens: "100",
    cache_write_1h_tokens: "100",
  });
  expect(
    calculateCost(usage!, {
      currency: "USD",
      rates: {
        cache_write_5m_tokens: { numerator: "5", denominator: "4" },
        cache_write_1h_tokens: { numerator: "2", denominator: "1" },
      },
    }),
  ).toBe("325");
});

test("new aggregate cache snapshots cannot reuse stale TTL quantities or invent missing quantities", () => {
  const previous = extractUsage(
    {
      usage: {
        cache_creation_input_tokens: 200,
        cache_creation: { ephemeral_5m_input_tokens: 100, ephemeral_1h_input_tokens: 100 },
      },
    },
    "anthropic",
  );
  expect(
    extractUsage({ usage: { cache_creation_input_tokens: 300 } }, "anthropic", previous),
  ).toEqual({ cache_write_tokens: "300" });
  expect(
    extractUsage(
      { usage: { cache_creation: { ephemeral_5m_input_tokens: 12 } } },
      "anthropic",
      previous,
    ),
  ).toEqual({ cache_write_5m_tokens: "12" });
  expect(
    extractUsage(
      {
        usage: { cache_creation: { ephemeral_5m_input_tokens: 12, ephemeral_1h_input_tokens: 3 } },
      },
      "bedrock",
    ),
  ).toEqual({ cache_write_tokens: "15", cache_write_5m_tokens: "12", cache_write_1h_tokens: "3" });
});

test("inconsistent cache partitions and duplicate aggregate pricing cannot produce a charge", () => {
  expect(() =>
    extractUsage(
      {
        usage: {
          cache_creation_input_tokens: 201,
          cache_creation: { ephemeral_5m_input_tokens: 100, ephemeral_1h_input_tokens: 100 },
        },
      },
      "anthropic",
    ),
  ).toThrow("cache write");
  expect(() =>
    calculateCost(
      { cache_write_tokens: "2", cache_write_5m_tokens: "1", cache_write_1h_tokens: "1" },
      {
        currency: "USD",
        rates: {
          cache_write_tokens: { numerator: "1", denominator: "1" },
          cache_write_5m_tokens: { numerator: "1", denominator: "1" },
        },
      },
    ),
  ).toThrow("cache write");
});

test("symptom: normalized usage accepts a single cache write TTL one token above the exact total", () => {
  expect(() =>
    validateUsageObject({
      cache_write_tokens: "9007199254740992",
      cache_write_1h_tokens: "9007199254740993",
    }),
  ).toThrow(expect.objectContaining({ status: 502, code: "usage_alias_conflict" }));
});

test("a single cache write TTL may equal the total beyond the JavaScript safe integer boundary", () => {
  expect(
    validateUsageObject({
      cache_write_tokens: "9007199254740993",
      cache_write_5m_tokens: "9007199254740993",
    }),
  ).toEqual({
    cache_write_tokens: "9007199254740993",
    cache_write_5m_tokens: "9007199254740993",
  });
});

test("JSON usage retains exact integers at and above the safe integer boundary", () => {
  for (const [literal, expected] of [
    ["9007199254740992", "9007199254740992"],
    ["9007199254740993", "9007199254740993"],
    ["1e21", "1000000000000000000000"],
    ["99999999999999999999999999999999999999", "99999999999999999999999999999999999999"],
  ] as const)
    expect(
      extractUsage(parseProviderJson(`{"usage":{"prompt_tokens":${literal}}}`), "openai"),
    ).toEqual({ input_tokens: expected });
});

test("metering precision preserves persisted request fingerprints for exact large client integers", () => {
  expect(stringifyExact(parseClientJson('{"value":9007199254740992}'))).toBe(
    '{"value":9007199254740992}',
  );
});

test("duration snapshots replace stale milliseconds and reject inconsistent unit aliases", () => {
  const previous = extractUsage({ usage: { audio_ms: 125, video_ms: 2345 } }, "openai");
  expect(previous).toEqual({
    audio_ms: "125",
    audio_seconds: "0.125",
    video_ms: "2345",
    video_seconds: "2.345",
  });
  expect(extractUsage({ usage: { audio_seconds: "0.1251" } }, "openai", previous)).toEqual({
    audio_seconds: "0.1251",
    video_ms: "2345",
    video_seconds: "2.345",
  });
  expect(() => extractUsage({ usage: { video_ms: 999, video_seconds: "1" } }, "openai")).toThrow(
    "duration usage aliases disagree",
  );
});

test("character quotes count direct Unicode text from each protocol without billing media URLs", () => {
  const tariff: ReservationPrice = {
    provider_id: "text",
    id: "character-price",
    currency: "USD",
    rates: { characters: { numerator: "7", denominator: "1" } },
    ceilings: { characters: "100" },
    basis: "INPUT_CHARACTERS",
  };
  const inputs: [Operation, Record<string, unknown>][] = [
    ["completions", { prompt: ["a", "界😀"] }],
    ["embeddings", { input: ["a", "界😀"] }],
    [
      "chat",
      {
        messages: [
          {
            role: "user",
            content: [
              { type: "text", text: "a界😀" },
              { type: "image_url", image_url: { url: "https://example.invalid/long" } },
            ],
          },
        ],
      },
    ],
    ["messages", { system: "a", messages: [{ role: "user", content: "界😀" }] }],
    [
      "messages",
      {
        messages: [
          {
            role: "user",
            content: [
              {
                type: "search_result",
                title: "uncounted",
                content: [{ type: "text", text: "a界😀" }],
              },
            ],
          },
        ],
      },
    ],
    [
      "responses",
      {
        instructions: "a",
        input: [{ role: "user", content: [{ type: "input_text", text: "界😀" }] }],
      },
    ],
    [
      "gemini",
      { systemInstruction: { parts: [{ text: "a" }] }, contents: [{ parts: [{ text: "界😀" }] }] },
    ],
    [
      "gemini",
      {
        system_instruction: { parts: [{ text: "a" }] },
        contents: [{ parts: [{ text: "界😀" }, { inline_data: { data: "aW1hZ2U=" } }] }],
      },
    ],
    ["responses", { previous_response_id: "response-1", instructions: "a界😀" }],
  ];
  for (const [operation, input] of inputs) {
    expect(quoteForRequest([tariff], "USD", operation, input, 0)).toMatchObject({
      input_characters: "3",
      amount_atoms: "21",
    });
  }

  expect(() => quoteForRequest([tariff], "USD", "embeddings", { input: [101, 102] }, 0)).toThrow(
    "Input character count cannot be determined",
  );
  expect(() =>
    quoteForRequest(
      [tariff],
      "USD",
      "gemini",
      {
        systemInstruction: { parts: [{ text: "a" }] },
        system_instruction: { parts: [{ text: "界😀" }] },
        contents: [{ parts: [{ text: "x" }] }],
      },
      0,
    ),
  ).toThrow("Input character count cannot be determined");
  const fallback: ReservationPrice = {
    ...tariff,
    provider_id: "per-request",
    id: "request-price",
    rates: { requests: { numerator: "9", denominator: "1" } },
    ceilings: { requests: "1" },
    basis: "REQUEST_COMPLETION",
  };
  expect(
    quoteForRequest([tariff, fallback], "USD", "embeddings", { input: [101, 102] }, 0),
  ).toMatchObject({ amount_atoms: "9", prices: { "per-request": "request-price" } });
});

test("Gemini tool-use prompt tokens are billed as input instead of being dropped", () => {
  expect(
    extractUsage(
      {
        usageMetadata: {
          promptTokenCount: 10,
          toolUsePromptTokenCount: 5,
          candidatesTokenCount: 3,
          cachedContentTokenCount: 4,
        },
      },
      "gemini",
    ),
  ).toMatchObject({ input_tokens: "11", cached_input_tokens: "4", output_tokens: "3" });
  expect(
    extractUsage(
      { usageMetadata: { toolUsePromptTokenCount: 6, candidatesTokenCount: 3 } },
      "vertex",
    ),
  ).toEqual({ output_tokens: "3" });
  const opening = extractUsage({ usageMetadata: { promptTokenCount: 10 } }, "gemini");
  const later = extractUsage(
    { usageMetadata: { toolUsePromptTokenCount: 5, candidatesTokenCount: 3 } },
    "gemini",
    opening,
  );
  expect(later).toEqual({ input_tokens: "15", output_tokens: "3" });
  expect(extractUsage({ usageMetadata: { toolUsePromptTokenCount: 6 } }, "gemini", later)).toEqual({
    input_tokens: "16",
    output_tokens: "3",
  });
});

function quoteForRequest(
  rows: ReservationPrice[],
  currency: string,
  operation: Operation,
  body: Record<string, unknown>,
  output: number,
) {
  return quoteReservation(
    rows,
    currency,
    {
      inputCharacters: requestCharacters(operation, body)?.toString(),
      images: operation === "images" || operation === "image_edits" ? (body.n ?? 1) : undefined,
    },
    output,
  );
}

test("symptom: Gemini loses previously metered thoughts when the final text frame omits them", () => {
  const translator = new StreamTranslator("gemini", "gemini", "gemini", "public-model");
  for (const body of [
    {
      candidates: [{ content: { parts: [{ text: "thinking", thought: true }] } }],
      usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 0, thoughtsTokenCount: 8 },
    },
    {
      candidates: [{ content: { parts: [{ text: "answer" }] }, finishReason: "STOP" }],
      usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 5 },
    },
  ]) {
    const data = JSON.stringify(body);
    translator.consume({ event: "message", id: "", data, wire: `data: ${data}\n\n` });
  }
  translator.assertComplete();
  expect(translator.usage).toMatchObject({
    input_tokens: "10",
    output_tokens: "13",
    reasoning_tokens: "8",
  });
});

test("Gemini explicit zero thoughtsTokenCount overwrites prior thoughts without inflating output_tokens", () => {
  const first = extractUsage(
    { usageMetadata: { promptTokenCount: 20, candidatesTokenCount: 0, thoughtsTokenCount: 100 } },
    "gemini",
  );
  expect(first).toEqual({ input_tokens: "20", output_tokens: "100", reasoning_tokens: "100" });

  const merged = extractUsage(
    { usageMetadata: { promptTokenCount: 20, candidatesTokenCount: 150, thoughtsTokenCount: 0 } },
    "gemini",
    first,
  );
  expect(merged).toEqual({ input_tokens: "20", output_tokens: "150", reasoning_tokens: "0" });

  const translator = new StreamTranslator("gemini", "gemini", "gemini", "public-model");
  for (const body of [
    {
      candidates: [{ content: { parts: [{ text: "plan", thought: true }] } }],
      usageMetadata: { promptTokenCount: 20, candidatesTokenCount: 0, thoughtsTokenCount: 100 },
    },
    {
      candidates: [{ content: { parts: [{ text: "final" }] }, finishReason: "STOP" }],
      usageMetadata: { promptTokenCount: 20, candidatesTokenCount: 150, thoughtsTokenCount: 0 },
    },
  ]) {
    const data = JSON.stringify(body);
    translator.consume({ event: "message", id: "", data, wire: `data: ${data}\n\n` });
  }
  translator.assertComplete();
  expect(translator.usage).toEqual({
    input_tokens: "20",
    output_tokens: "150",
    reasoning_tokens: "0",
  });
});

test("Anthropic cache_creation snapshot replaces prior 1h bucket cleanly instead of leaking stale TTL tokens", () => {
  const first = extractUsage(
    {
      usage: {
        input_tokens: 10,
        output_tokens: 1,
        cache_creation_input_tokens: 1000,
        cache_creation: { ephemeral_5m_input_tokens: 0, ephemeral_1h_input_tokens: 1000 },
      },
    },
    "anthropic",
  );
  expect(first).toEqual({
    input_tokens: "10",
    output_tokens: "1",
    cache_write_tokens: "1000",
    cache_write_5m_tokens: "0",
    cache_write_1h_tokens: "1000",
  });

  const replaced = extractUsage(
    {
      usage: {
        output_tokens: 40,
        cache_creation_input_tokens: 1000,
        cache_creation: { ephemeral_5m_input_tokens: 1000, ephemeral_1h_input_tokens: 0 },
      },
    },
    "anthropic",
    first,
  );
  expect(replaced).toEqual({
    input_tokens: "10",
    output_tokens: "40",
    cache_write_tokens: "1000",
    cache_write_5m_tokens: "1000",
    cache_write_1h_tokens: "0",
  });

  const partialReplace = extractUsage(
    {
      usage: {
        output_tokens: 45,
        cache_creation: { ephemeral_5m_input_tokens: 1000 },
      },
    },
    "anthropic",
    first,
  );
  expect(partialReplace).toEqual({
    input_tokens: "10",
    output_tokens: "45",
    cache_write_5m_tokens: "1000",
  });
  expect(partialReplace?.cache_write_1h_tokens).toBeUndefined();
});

test("Anthropic message_start zero/placeholder output and message_delta usage merge cleanly while Responses cache and reasoning overflows reject", () => {
  const translator = new StreamTranslator("messages", "messages", "anthropic", "claude-1");
  const startBody = JSON.stringify({
    type: "message_start",
    message: {
      id: "msg_1",
      type: "message",
      role: "assistant",
      model: "claude-1",
      content: [],
      usage: {
        input_tokens: 120,
        output_tokens: 1,
        cache_read_input_tokens: 500,
        cache_creation_input_tokens: 40,
      },
    },
  });
  const startStep = translator.consume({
    event: "message_start",
    id: "",
    data: startBody,
    wire: `event: message_start\ndata: ${startBody}\n\n`,
  });
  expect(startStep.outputUsageBoundary).toBe("ignore");
  expect(translator.usage).toEqual({
    input_tokens: "120",
    cached_input_tokens: "500",
    cache_write_tokens: "40",
  });

  const deltaText = JSON.stringify({
    type: "content_block_delta",
    index: 0,
    delta: { type: "text_delta", text: "Hello world" },
  });
  translator.consume({
    event: "content_block_delta",
    id: "",
    data: deltaText,
    wire: `event: content_block_delta\ndata: ${deltaText}\n\n`,
  });
  expect(translator.observedOutput.chunks).toBe(1);
  expect(translator.observedOutput.asciiChars).toBe(11);

  const msgDelta = JSON.stringify({
    type: "message_delta",
    delta: { stop_reason: "end_turn" },
    usage: { output_tokens: 45 },
  });
  translator.consume({
    event: "message_delta",
    id: "",
    data: msgDelta,
    wire: `event: message_delta\ndata: ${msgDelta}\n\n`,
  });
  expect(translator.usage).toEqual({
    input_tokens: "120",
    cached_input_tokens: "500",
    cache_write_tokens: "40",
    output_tokens: "45",
  });

  expect(
    extractUsage(
      {
        response: {
          usage: {
            input_tokens: 100,
            input_tokens_details: { cached_tokens: 40 },
            output_tokens: 30,
            output_tokens_details: { reasoning_tokens: 12 },
          },
        },
      },
      "openai",
    ),
  ).toEqual({
    input_tokens: "60",
    cached_input_tokens: "40",
    output_tokens: "30",
    reasoning_tokens: "12",
  });

  expect(() =>
    extractUsage(
      {
        response: {
          usage: {
            input_tokens: 100,
            input_tokens_details: { cached_tokens: 101 },
            output_tokens: 10,
          },
        },
      },
      "openai",
    ),
  ).toThrow("Provider cached usage exceeds total input");

  expect(() =>
    extractUsage(
      {
        response: {
          usage: {
            input_tokens: 100,
            output_tokens: 10,
            output_tokens_details: { reasoning_tokens: 11 },
          },
        },
      },
      "openai",
    ),
  ).toThrow("Reasoning usage exceeds total output");

  expect(() =>
    extractUsage(
      {
        usage: {
          prompt_tokens: 100,
          prompt_cache_hit_tokens: 40,
          prompt_cache_miss_tokens: 50,
          completion_tokens: 5,
        },
      },
      "openai",
    ),
  ).toThrow("Provider cache partition does not equal prompt usage");
});

test("symptom: hosted tool counts are extracted from OpenAI Responses output[] and Anthropic server_tool_use with alias conflict checks", () => {
  expect(
    extractUsage(
      {
        id: "resp_1",
        output: [
          { type: "web_search_call", status: "completed" },
          { type: "web_search_call", status: "completed" },
          { type: "code_interpreter_call", status: "completed" },
          { type: "file_search_call", status: "completed" },
          { type: "message", role: "assistant", content: [{ type: "output_text", text: "hi" }] },
        ],
        usage: { input_tokens: 50, output_tokens: 20 },
      },
      "openai",
    ),
  ).toEqual({
    input_tokens: "50",
    output_tokens: "20",
    web_search_calls: "2",
    code_interpreter_sessions: "1",
    file_search_calls: "1",
  });

  expect(
    extractUsage(
      {
        id: "resp_no_tools",
        output: [{ type: "message", role: "assistant", content: [] }],
        usage: { input_tokens: 10, output_tokens: 5 },
      },
      "openai",
    ),
  ).toEqual({
    input_tokens: "10",
    output_tokens: "5",
  });

  const translator = new StreamTranslator("responses", "responses", "openai", "gpt-5");
  const doneBody = {
    type: "response.completed",
    response: {
      id: "resp_stream",
      output: [{ type: "web_search_call" }, { type: "file_search_call" }],
      usage: { input_tokens: 30, output_tokens: 12 },
    },
  };
  translator.consume({
    event: "response.completed",
    id: "",
    data: JSON.stringify(doneBody),
    wire: `event: response.completed\ndata: ${JSON.stringify(doneBody)}\n\n`,
  });
  translator.assertComplete();
  expect(translator.usage).toEqual({
    input_tokens: "30",
    output_tokens: "12",
    web_search_calls: "1",
    file_search_calls: "1",
  });

  expect(() =>
    extractUsage(
      {
        output: [{ type: "web_search_call" }, { type: "web_search_call" }],
        usage: { input_tokens: 10, output_tokens: 5, web_search_calls: 1 },
      },
      "openai",
    ),
  ).toThrow("Provider web search usage aliases disagree");

  expect(
    extractUsage(
      {
        usage: {
          input_tokens: 100,
          output_tokens: 40,
          server_tool_use: { web_search_requests: 2, code_execution_requests: 3 },
        },
      },
      "anthropic",
    ),
  ).toEqual({
    input_tokens: "100",
    output_tokens: "40",
    web_search_calls: "2",
    code_interpreter_sessions: "3",
  });

  expect(() =>
    extractUsage(
      {
        usage: {
          input_tokens: 100,
          output_tokens: 40,
          code_interpreter_sessions: 1,
          server_tool_use: { code_execution_requests: 2 },
        },
      },
      "anthropic",
    ),
  ).toThrow("Provider code execution usage aliases disagree");
});
