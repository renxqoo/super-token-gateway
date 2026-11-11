import { describe, expect, test } from "bun:test";
import {
  SSEParser,
  calculateCost,
  canonical,
  decrypt,
  encrypt,
  extractUsage,
  secureEqual,
  parseProviderJson,
  stringifyExact,
} from "../index.ts";

describe("precise cost and usage evidence", () => {
  test("uses original numeric literals rather than rounded JSON.parse quantities", () => {
    const value = parseProviderJson(
      '{"usage":{"input_tokens":9007199254740993,"output_tokens":2}}',
    );
    expect(extractUsage(value, "openai")?.input_tokens).toBe("9007199254740993");
    expect(stringifyExact(value)).toContain('"input_tokens":9007199254740993');
    expect(() =>
      extractUsage(
        parseProviderJson('{"usage":{"input_tokens":1.0000000000000001,"output_tokens":2}}'),
        "openai",
      ),
    ).toThrow("integer");
    expect(() =>
      extractUsage({ usage: { prompt_tokens: 10, input_tokens: 11 } }, "openai"),
    ).toThrow("aliases disagree");
    expect(canonical(parseProviderJson('{"values":[9007199254740993]}'))).not.toBe(
      canonical(parseProviderJson('{"values":[9007199254740992]}')),
    );
  });
  test("sums rational dimensions before a single rounding", () => {
    expect(
      calculateCost(
        { input_tokens: "1", output_tokens: "1", cached_input_tokens: "1" },
        {
          currency: "USD",
          rates: {
            input_tokens: { numerator: "1", denominator: "3" },
            output_tokens: { numerator: "1", denominator: "3" },
            cached_input_tokens: { numerator: "1", denominator: "3" },
          },
        },
      ),
    ).toBe("1");
  });
  test("preserves large integers and rejects floats/overflow", () => {
    expect(
      calculateCost(
        { input_tokens: "9007199254740993" },
        { currency: "USD", rates: { input_tokens: { numerator: "3", denominator: "1" } } },
      ),
    ).toBe("27021597764222979");
    expect(() => calculateCost({ input_tokens: "1.5" }, { currency: "USD", rates: {} })).toThrow();
    expect(() =>
      calculateCost(
        { input_tokens: "99999999999999999999999999999999999999" },
        { currency: "USD", rates: { input_tokens: { numerator: "2", denominator: "1" } } },
      ),
    ).toThrow();
  });
  test("cache and reasoning are dialect-aware without double counting", () => {
    expect(
      extractUsage(
        {
          usage: {
            prompt_tokens: 10,
            completion_tokens: 5,
            prompt_tokens_details: { cached_tokens: 3 },
            completion_tokens_details: { reasoning_tokens: 2 },
          },
        },
        "openai",
      ),
    ).toEqual({
      input_tokens: "7",
      output_tokens: "5",
      cached_input_tokens: "3",
      reasoning_tokens: "2",
    });
    expect(
      extractUsage(
        {
          usage: {
            input_tokens: 7,
            output_tokens: 5,
            cache_read_input_tokens: 3,
            cache_creation_input_tokens: 2,
          },
        },
        "anthropic",
      ),
    ).toEqual({
      input_tokens: "7",
      output_tokens: "5",
      cached_input_tokens: "3",
      cache_write_tokens: "2",
    });
    expect(
      extractUsage(
        {
          usageMetadata: {
            promptTokenCount: 10,
            candidatesTokenCount: 3,
            thoughtsTokenCount: 2,
            cachedContentTokenCount: 3,
          },
        },
        "gemini",
      ),
    ).toEqual({
      input_tokens: "7",
      output_tokens: "5",
      cached_input_tokens: "3",
      reasoning_tokens: "2",
    });
    expect(extractUsage({ content: "private output" }, "openai")).toBeUndefined();
  });
});

describe("bounded SSE parser", () => {
  test("handles every byte split, CRLF, Unicode, comments and multiline data", () => {
    const bytes = new TextEncoder().encode(
      ': comment\r\nevent: result\r\nid: 123\r\ndata: {"text":"北京🌍",\r\ndata: "n":1}\r\n\r\n',
    );
    for (let size = 1; size <= bytes.length; size++) {
      const parser = new SSEParser();
      const events = [];
      for (let offset = 0; offset < bytes.length; offset += size)
        events.push(...parser.feed(bytes.slice(offset, offset + size)));
      events.push(...parser.feed(new Uint8Array(), true));
      expect(events.length).toBe(1);
      expect(events[0]?.event).toBe("result");
      expect(events[0]?.id).toBe("123");
      expect(JSON.parse(events[0]?.data ?? "{}")).toEqual({ text: "北京🌍", n: 1 });
      expect(parser.incomplete).toBe(false);
    }
  });
  test("never turns incomplete EOF into a completed event", () => {
    const parser = new SSEParser();
    expect(parser.feed(new TextEncoder().encode("data: [DONE]\n"))).toEqual([]);
    expect(parser.feed(new Uint8Array(), true)).toEqual([]);
    expect(parser.incomplete).toBe(true);
    expect(() =>
      new SSEParser(16).feed(new TextEncoder().encode(`data: ${"x".repeat(40)}`)),
    ).toThrow();
  });
});

describe("secret and outbound boundaries", () => {
  test("authenticated encryption binds a provider identity", () => {
    const key = "11".repeat(32);
    const value = encrypt("provider-secret", key, "provider-1");
    expect(value).not.toContain("provider-secret");
    expect(decrypt(value, key, "provider-1")).toBe("provider-secret");
    expect(() => decrypt(value, key, "provider-2")).toThrow();
    expect(secureEqual("abc", "abc")).toBe(true);
    expect(secureEqual("abc", "ab")).toBe(false);
  });
  test("canonical fingerprints preserve array order and explicit zero/false", () => {
    expect(canonical({ b: false, a: [0, 1] })).toBe(canonical({ a: [0, 1], b: false }));
    expect(canonical({ a: [1, 0], b: false })).not.toBe(canonical({ a: [0, 1], b: false }));
  });
});

test("decimal durations price exactly without binary float drift or duplicate units", () => {
  const usage = extractUsage(
    parseProviderJson('{"usage":{"type":"duration","seconds":0.125}}'),
    "openai",
  );
  expect(usage).toEqual({ audio_seconds: "0.125", audio_ms: "125" });
  expect(
    calculateCost(usage ?? {}, {
      currency: "USD",
      rates: { audio_seconds: { numerator: "8", denominator: "1" } },
    }),
  ).toBe("1");
  expect(
    calculateCost(
      { audio_seconds: "0.000000000000000001" },
      {
        currency: "USD",
        rates: { audio_seconds: { numerator: "1000000000000000000", denominator: "1" } },
      },
    ),
  ).toBe("1");
  expect(() =>
    calculateCost(
      { audio_seconds: "1" },
      {
        currency: "USD",
        rates: {
          audio_ms: { numerator: "1", denominator: "1" },
          audio_seconds: { numerator: "1", denominator: "1" },
        },
      },
    ),
  ).toThrow("one duration unit");
});

test("JSON structure budgets reject nesting before recursive reviver and classify upstream corruption", () => {
  expect(() => parseProviderJson("[".repeat(65) + "0" + "]".repeat(65))).toThrow("nesting");
  expect(() => parseProviderJson("{ broken")).toThrow("malformed JSON");
  expect(parseProviderJson('{"text":"escaped \\\" [{braces}]","value":0}')).toEqual({
    text: 'escaped \\" [{braces}]'.replace('\\"', '"'),
    value: 0,
  });
});
