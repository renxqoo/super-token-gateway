import { expect, test } from "bun:test";
import {
  canonical,
  extractUsage,
  parseClientJson,
  parseProviderJson,
  quoteReservation,
} from "../index.ts";
import type { Operation, ReservationPrice } from "../index.ts";

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
  expect(canonical(parseClientJson('{"value":9007199254740992}'))).toBe(
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
    expect(quoteReservation([tariff], "USD", operation, input, 0)).toMatchObject({
      input_characters: "3",
      amount_atoms: "21",
    });
  }

  expect(() => quoteReservation([tariff], "USD", "embeddings", { input: [101, 102] }, 0)).toThrow(
    "Input character count cannot be determined",
  );
  expect(() =>
    quoteReservation(
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
    quoteReservation([tariff, fallback], "USD", "embeddings", { input: [101, 102] }, 0),
  ).toMatchObject({ amount_atoms: "9", prices: { "per-request": "request-price" } });
});
