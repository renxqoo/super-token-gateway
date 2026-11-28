import { expect, test } from "bun:test";
import { SSEParser, StreamTranslator } from "../index.ts";

function parseTranslatedSse(chunks: Uint8Array[]) {
  const parser = new SSEParser();
  return parser.feed(Buffer.concat(chunks), true);
}

test("incomplete stream terminations never synthesize completion across protocols", () => {
  const doneWithoutFinish = new StreamTranslator("chat", "messages", "openai", "m1");
  const partial = doneWithoutFinish.consume({
    event: "message",
    id: "",
    wire: "",
    data: JSON.stringify({
      choices: [{ index: 0, delta: { content: "hi" }, finish_reason: null }],
    }),
  });
  expect(new TextDecoder().decode(Buffer.concat(partial.chunks))).not.toContain("message_stop");
  expect(() =>
    doneWithoutFinish.consume({ event: "message", id: "", wire: "", data: "[DONE]" }),
  ).toThrow(expect.objectContaining({ status: 502, code: "stream_truncated" }));
  expect(doneWithoutFinish.terminal).toBe(false);
  expect(() => doneWithoutFinish.finalize()).toThrow(
    expect.objectContaining({ status: 502, code: "stream_truncated" }),
  );

  const doneOnly = new StreamTranslator("chat", "chat", "openai", "m1");
  expect(() => doneOnly.consume({ event: "message", id: "", wire: "", data: "[DONE]" })).toThrow(
    expect.objectContaining({ status: 502, code: "stream_truncated" }),
  );

  for (const [source, target, kind, event, data] of [
    [
      "messages",
      "chat",
      "anthropic",
      "content_block_delta",
      JSON.stringify({
        type: "content_block_delta",
        index: 0,
        delta: { type: "text_delta", text: "hi" },
      }),
    ],
    [
      "responses",
      "chat",
      "openai",
      "response.output_text.delta",
      JSON.stringify({ type: "response.output_text.delta", output_index: 0, delta: "hi" }),
    ],
    [
      "gemini",
      "messages",
      "gemini",
      "message",
      JSON.stringify({
        candidates: [{ index: 0, content: { role: "model", parts: [{ text: "hi" }] } }],
      }),
    ],
  ] as const) {
    const t = new StreamTranslator(source, target, kind, "m1");
    const out = t.consume({ event, id: "", wire: "", data });
    const wire = new TextDecoder().decode(Buffer.concat(out.chunks));
    expect(wire).not.toContain("[DONE]");
    expect(wire).not.toContain("message_stop");
    expect(t.terminal).toBe(false);
    expect(() => t.assertComplete()).toThrow(
      expect.objectContaining({ status: 502, code: "stream_truncated" }),
    );
  }
});

test("same-protocol streams restamp logical model without leaking upstream prefixes or mutating Responses delta frames", () => {
  const chatT = new StreamTranslator("chat", "chat", "openai", "logical-chat");
  const chatRaw = JSON.stringify({
    id: "c1",
    object: "chat.completion.chunk",
    model: "openai/gpt-4o-internal",
    choices: [{ index: 0, delta: { content: "ok" }, finish_reason: null }],
  });
  const chatOut = chatT.consume({
    event: "message",
    id: "",
    data: chatRaw,
    wire: `data: ${chatRaw}\n\n`,
  });
  expect(JSON.parse(parseTranslatedSse(chatOut.chunks)[0]!.data)).toMatchObject({
    model: "logical-chat",
  });

  const msgT = new StreamTranslator("messages", "messages", "anthropic", "logical-claude");
  const msgRaw = JSON.stringify({
    type: "message_start",
    message: {
      id: "m1",
      type: "message",
      role: "assistant",
      model: "anthropic/claude-internal",
      content: [],
      usage: { input_tokens: 5, output_tokens: 1 },
    },
  });
  const msgOut = msgT.consume({
    event: "message_start",
    id: "",
    data: msgRaw,
    wire: `event: message_start\ndata: ${msgRaw}\n\n`,
  });
  expect(JSON.parse(parseTranslatedSse(msgOut.chunks)[0]!.data)).toMatchObject({
    message: { model: "logical-claude" },
  });

  const respT = new StreamTranslator("responses", "responses", "openai", "logical-resp");
  const respFrames: Array<[string, Record<string, unknown>]> = [
    [
      "response.created",
      {
        type: "response.created",
        response: { id: "r1", model: "vendor/internal-r", status: "in_progress" },
      },
    ],
    [
      "response.in_progress",
      {
        type: "response.in_progress",
        response: { id: "r1", model: "vendor/internal-r", status: "in_progress" },
      },
    ],
    [
      "response.output_text.delta",
      { type: "response.output_text.delta", output_index: 0, delta: "hello" },
    ],
    [
      "response.completed",
      {
        type: "response.completed",
        response: {
          id: "r1",
          model: "vendor/internal-r",
          status: "completed",
          usage: { input_tokens: 2, output_tokens: 1 },
        },
      },
    ],
  ];
  const respChunks: Uint8Array[] = [];
  for (const [ev, body] of respFrames) {
    const data = JSON.stringify(body);
    respChunks.push(
      ...respT.consume({ event: ev, id: "", data, wire: `event: ${ev}\ndata: ${data}\n\n` }).chunks,
    );
  }
  const respEmitted = parseTranslatedSse(respChunks).map((e) => ({
    event: e.event,
    body: JSON.parse(e.data) as Record<string, unknown>,
  }));
  for (const lifecycle of ["response.created", "response.in_progress", "response.completed"]) {
    expect(respEmitted.find((e) => e.event === lifecycle)?.body).toMatchObject({
      response: { model: "logical-resp" },
    });
  }
  const textDelta = respEmitted.find((e) => e.event === "response.output_text.delta")?.body;
  expect(textDelta).toEqual({
    type: "response.output_text.delta",
    output_index: 0,
    delta: "hello",
  });
  expect("response" in textDelta!).toBe(false);
});

test("Responses top-level error and response.failed events abort stream on first frame and after text with malformed error fields", () => {
  const errorPayloads: Array<[string, Record<string, unknown>]> = [
    ["error", { type: "error", error: null, code: 429, message: "rate limited" }],
    ["error", { type: "error", code: 500, message: "" }],
    ["error", { type: "error", code: "server_error", message: { nested: true } }],
    ["error", { type: "error", code: "overloaded", message: "x".repeat(5120) }],
    [
      "response.failed",
      {
        type: "response.failed",
        response: { id: "r_fail", status: "failed", error: { message: "boom" } },
      },
    ],
  ];
  for (const afterText of [false, true]) {
    for (const [evName, payload] of errorPayloads) {
      const translator = new StreamTranslator("responses", "responses", "openai", "m1");
      if (afterText) {
        translator.consume({
          event: "response.output_text.delta",
          id: "",
          wire: "",
          data: JSON.stringify({
            type: "response.output_text.delta",
            output_index: 0,
            delta: "prefix",
          }),
        });
      }
      expect(() =>
        translator.consume({ event: evName, id: "", wire: "", data: JSON.stringify(payload) }),
      ).toThrow(expect.objectContaining({ status: 502, code: "provider_stream_error" }));
      expect(translator.failed).toBe(true);
      expect(translator.terminal).toBe(false);
    }
  }
});
