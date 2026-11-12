import { canonicalExact } from "../json.ts";
import { quoteReservation } from "@gateway/kernel";
import { requestCharacters } from "../characters.ts";
import { expect, test } from "bun:test";
import { calculateCost } from "@gateway/kernel";
import { extractUsage, parseClientJson, parseProviderJson } from "../index.ts";
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
  expect(canonicalExact(parseClientJson('{"value":9007199254740992}'))).toBe(
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
    extractUsage({ usageMetadata: { toolUsePromptTokenCount: 6 } }, "vertex")?.input_tokens,
  ).toBe("6");
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
