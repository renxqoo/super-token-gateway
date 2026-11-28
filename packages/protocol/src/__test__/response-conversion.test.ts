import { expect, test } from "bun:test";
import {
  SSEParser,
  StreamTranslator,
  decodeResponse,
  encodeResponse,
  stringifyExact,
} from "../index.ts";

function parseTranslatedSse(chunks: Uint8Array[]) {
  const parser = new SSEParser();
  return parser.feed(Buffer.concat(chunks), true);
}

test("symptom: Responses incomplete with content_filter reason is misclassified as length in response and stream decoding", () => {
  const filtered = decodeResponse(
    {
      id: "resp_filter",
      status: "incomplete",
      incomplete_details: { reason: "content_filter" },
      output: [
        {
          id: "msg_0",
          type: "message",
          content: [{ type: "output_text", text: "blocked prefix" }],
        },
      ],
      usage: { input_tokens: 8, output_tokens: 3, total_tokens: 11 },
    },
    "responses",
    "openai",
  );
  expect(filtered.finish).toBe("content_filter");
  expect(encodeResponse(filtered, "chat", "m1")).toMatchObject({
    choices: [{ finish_reason: "content_filter" }],
  });
  expect(encodeResponse(filtered, "messages", "m1")).toMatchObject({
    stop_reason: "refusal",
  });
  expect(encodeResponse(filtered, "gemini", "m1")).toMatchObject({
    candidates: [{ finishReason: "SAFETY" }],
  });
  expect(encodeResponse(filtered, "responses", "m1")).toMatchObject({
    status: "incomplete",
    incomplete_details: { reason: "content_filter" },
  });

  const maxTokens = decodeResponse(
    {
      id: "resp_len",
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
      output: [{ id: "msg_0", type: "message", content: [{ type: "output_text", text: "cut" }] }],
    },
    "responses",
    "openai",
  );
  expect(maxTokens.finish).toBe("length");
  const missingDetails = decodeResponse(
    {
      id: "resp_default_len",
      status: "incomplete",
      output: [{ id: "msg_0", type: "message", content: [{ type: "output_text", text: "cut" }] }],
    },
    "responses",
    "openai",
  );
  expect(missingDetails.finish).toBe("length");
  expect(() =>
    decodeResponse(
      {
        id: "resp_unknown",
        status: "incomplete",
        incomplete_details: { reason: "vendor_custom_stop" },
        output: [],
      },
      "responses",
      "openai",
    ),
  ).toThrow(expect.objectContaining({ status: 502, code: "conversion_loss" }));

  for (const target of ["chat", "messages"] as const) {
    const translator = new StreamTranslator("responses", target, "openai", "m1");
    const chunks = [
      ...translator.consume({
        event: "response.output_text.delta",
        id: "",
        wire: "",
        data: JSON.stringify({ type: "response.output_text.delta", output_index: 0, delta: "hi" }),
      }).chunks,
      ...translator.consume({
        event: "response.incomplete",
        id: "",
        wire: "",
        data: JSON.stringify({
          type: "response.incomplete",
          response: {
            id: "resp_s",
            status: "incomplete",
            incomplete_details: { reason: "content_filter" },
            usage: { input_tokens: 4, output_tokens: 2, total_tokens: 6 },
          },
        }),
      }).chunks,
      ...translator.finalize(),
    ];
    const events = parseTranslatedSse(chunks);
    if (target === "chat") {
      const finishChunk = events
        .filter((e) => e.data !== "[DONE]")
        .map((e) => JSON.parse(e.data) as { choices?: Array<{ finish_reason?: string | null }> })
        .find((b) => b.choices?.[0]?.finish_reason);
      expect(finishChunk?.choices?.[0]?.finish_reason).toBe("content_filter");
    } else {
      const deltaEvent = events.find((e) => e.event === "message_delta");
      expect(JSON.parse(deltaEvent!.data)).toMatchObject({ delta: { stop_reason: "refusal" } });
    }
  }
});

test("symptom: non-streaming encodeResponse fails on empty tool arguments, accepts non-object tool args, and rounds exact large integers", () => {
  const fromArgs = (args: string) =>
    decodeResponse(
      {
        id: "chat_tool",
        choices: [
          {
            index: 0,
            message: {
              role: "assistant",
              content: null,
              tool_calls: [
                { id: "call_1", type: "function", function: { name: "lookup", arguments: args } },
              ],
            },
            finish_reason: "tool_calls",
          },
        ],
      },
      "chat",
      "openai",
    );

  for (const emptyArgs of ["", "   "]) {
    const completion = fromArgs(emptyArgs);
    expect(encodeResponse(completion, "messages", "claude-1")).toMatchObject({
      content: [{ type: "tool_use", id: "call_1", name: "lookup", input: {} }],
    });
    expect(encodeResponse(completion, "gemini", "gemini-1")).toMatchObject({
      candidates: [
        { content: { parts: [{ functionCall: { id: "call_1", name: "lookup", args: {} } }] } },
      ],
    });
  }

  const exactArgs =
    '{"id":9007199254740993,"ids":[18446744073709551615],"value":0.123456789012345678901}';
  const exactCompletion = fromArgs(exactArgs);
  const messagesJson = stringifyExact(encodeResponse(exactCompletion, "messages", "claude-1"));
  const geminiJson = stringifyExact(encodeResponse(exactCompletion, "gemini", "gemini-1"));
  for (const serialized of [messagesJson, geminiJson]) {
    expect(serialized).toContain("9007199254740993");
    expect(serialized).toContain("18446744073709551615");
    expect(serialized).toContain("0.123456789012345678901");
  }

  for (const badArgs of ["{", "null", '["x"]', '"x"', "42"]) {
    const badCompletion = fromArgs(badArgs);
    expect(() => encodeResponse(badCompletion, "messages", "claude-1")).toThrow(
      expect.objectContaining({ status: 502 }),
    );
    expect(() => encodeResponse(badCompletion, "gemini", "gemini-1")).toThrow(
      expect.objectContaining({ status: 502 }),
    );
  }
});

test("Gemini multi-part text and split JSON fragments concatenate verbatim without injected delimiters", () => {
  const parts = [
    { text: '{"word":"hel' },
    { text: "" },
    { text: 'lo\\nworld\\r\\n"' },
    { text: ',"slash":"a\\\\b"' },
    { text: ',"unicode":"北京🌍"}' },
  ];
  const expectedText = '{"word":"hello\\nworld\\r\\n","slash":"a\\\\b","unicode":"北京🌍"}';
  const completion = decodeResponse(
    {
      candidates: [{ index: 0, content: { role: "model", parts }, finishReason: "STOP" }],
      usageMetadata: { promptTokenCount: 4, candidatesTokenCount: 6, totalTokenCount: 10 },
    },
    "gemini",
    "gemini",
  );
  const chatResp = encodeResponse(completion, "chat", "m1") as {
    choices: Array<{ message: { content: string } }>;
  };
  expect(chatResp.choices[0]!.message.content).toBe(expectedText);
  expect(JSON.parse(chatResp.choices[0]!.message.content)).toEqual({
    word: "hello\nworld\r\n",
    slash: "a\\b",
    unicode: "北京🌍",
  });

  for (const target of ["chat", "messages", "responses"] as const) {
    const translator = new StreamTranslator("gemini", target, "gemini", "m1");
    const chunks = [
      ...translator.consume({
        event: "message",
        id: "",
        wire: "",
        data: JSON.stringify({
          candidates: [{ index: 0, content: { role: "model", parts }, finishReason: "STOP" }],
          usageMetadata: { promptTokenCount: 4, candidatesTokenCount: 6, totalTokenCount: 10 },
        }),
      }).chunks,
      ...translator.finalize(),
    ];
    const events = parseTranslatedSse(chunks);
    const stitched =
      target === "chat"
        ? events
            .filter((e) => e.data !== "[DONE]")
            .map(
              (e) =>
                (JSON.parse(e.data) as { choices?: Array<{ delta?: { content?: string } }> })
                  .choices?.[0]?.delta?.content ?? "",
            )
            .join("")
        : target === "messages"
          ? events
              .filter((e) => e.event === "content_block_delta")
              .map((e) => (JSON.parse(e.data) as { delta?: { text?: string } }).delta?.text ?? "")
              .join("")
          : events
              .filter((e) => e.event === "response.output_text.delta")
              .map((e) => (JSON.parse(e.data) as { delta?: string }).delta ?? "")
              .join("");
    expect(stitched).toBe(expectedText);
    expect(JSON.parse(stitched)).toEqual({
      word: "hello\nworld\r\n",
      slash: "a\\b",
      unicode: "北京🌍",
    });
  }
});

test("Gemini empty STOP completes cleanly while MALFORMED_FUNCTION_CALL rejects with conversion_loss", () => {
  for (const candidate of [
    { index: 0, finishReason: "STOP" },
    { index: 0, content: { role: "model", parts: [] }, finishReason: "STOP" },
  ]) {
    const decoded = decodeResponse(
      {
        candidates: [candidate],
        usageMetadata: { promptTokenCount: 5, candidatesTokenCount: 0, totalTokenCount: 5 },
      },
      "gemini",
      "gemini",
    );
    expect(decoded.finish).toBe("stop");
    expect(decoded.parts).toEqual([]);
    expect(encodeResponse(decoded, "chat", "m1")).toMatchObject({
      choices: [{ message: { role: "assistant", content: null }, finish_reason: "stop" }],
    });
    expect(encodeResponse(decoded, "messages", "m1")).toMatchObject({
      content: [],
      stop_reason: "end_turn",
    });

    for (const target of ["chat", "messages"] as const) {
      const translator = new StreamTranslator("gemini", target, "gemini", "m1");
      const chunks = [
        ...translator.consume({
          event: "message",
          id: "",
          wire: "",
          data: JSON.stringify({
            candidates: [candidate],
            usageMetadata: { promptTokenCount: 5, candidatesTokenCount: 0 },
          }),
        }).chunks,
        ...translator.finalize(),
      ];
      const wire = new TextDecoder().decode(Buffer.concat(chunks));
      expect(wire).toContain(
        target === "chat" ? '"finish_reason":"stop"' : '"stop_reason":"end_turn"',
      );
    }
  }

  const malformedBody = {
    candidates: [{ index: 0, finishReason: "MALFORMED_FUNCTION_CALL" }],
    usageMetadata: { promptTokenCount: 5, candidatesTokenCount: 0 },
  };
  expect(() => decodeResponse(malformedBody, "gemini", "gemini")).toThrow(
    expect.objectContaining({ status: 502, code: "conversion_loss" }),
  );
  for (const target of ["chat", "messages", "responses"] as const) {
    const translator = new StreamTranslator("gemini", target, "gemini", "m1");
    expect(() =>
      translator.consume({
        event: "message",
        id: "",
        wire: "",
        data: JSON.stringify(malformedBody),
      }),
    ).toThrow(expect.objectContaining({ status: 502, code: "conversion_loss" }));
  }
});
