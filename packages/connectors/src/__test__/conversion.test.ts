import { expect, test } from "bun:test";
import {
  StreamTranslator,
  decodeRequest,
  encodeRequest,
  encodeResponse,
  imageCount,
} from "../index.ts";
import type { Protocol } from "@gateway/kernel";

test("image quantity follows payloads across object, split and metadata shapes", () => {
  expect(imageCount({ url: "image" })).toBe(1);
  expect(imageCount([{ url: "image" }, { b64_json: "image" }])).toBe(1);
  expect(imageCount([{ url: "a", b64_json: "a" }, { url: "b" }])).toBe(2);
  expect(imageCount([{ revised_prompt: "metadata" }, { b64_json: "image" }])).toBe(1);
  expect(imageCount([{ url: "", b64_json: "" }, null])).toBe(0);
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
