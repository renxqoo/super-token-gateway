import { expect, test } from "bun:test";
import { SSEParser, StreamTranslator } from "../index.ts";

function parseTranslatedSse(chunks: Uint8Array[]) {
  const parser = new SSEParser();
  return parser.feed(Buffer.concat(chunks), true);
}

test("Responses to Anthropic Messages stream emits reasoning, text, and tool blocks once without duplicate done replay", () => {
  const translator = new StreamTranslator("responses", "messages", "openai", "claude-logical");
  const upstreamEvents: Array<[string, Record<string, unknown>]> = [
    [
      "response.created",
      {
        type: "response.created",
        response: {
          id: "resp_1",
          object: "response",
          model: "upstream-resp",
          status: "in_progress",
          output: [],
        },
      },
    ],
    [
      "response.output_item.added",
      {
        type: "response.output_item.added",
        output_index: 0,
        item: { id: "rs_0", type: "reasoning", summary: [], status: "in_progress" },
      },
    ],
    [
      "response.reasoning_summary_text.delta",
      {
        type: "response.reasoning_summary_text.delta",
        output_index: 0,
        item_id: "rs_0",
        summary_index: 0,
        delta: "check weather",
      },
    ],
    [
      "response.reasoning_summary_text.done",
      {
        type: "response.reasoning_summary_text.done",
        output_index: 0,
        item_id: "rs_0",
        summary_index: 0,
        text: "check weather",
      },
    ],
    [
      "response.output_item.done",
      {
        type: "response.output_item.done",
        output_index: 0,
        item: {
          id: "rs_0",
          type: "reasoning",
          summary: [{ type: "summary_text", text: "check weather" }],
          status: "completed",
        },
      },
    ],
    [
      "response.output_item.added",
      {
        type: "response.output_item.added",
        output_index: 1,
        item: {
          id: "msg_1",
          type: "message",
          role: "assistant",
          content: [],
          status: "in_progress",
        },
      },
    ],
    [
      "response.output_text.delta",
      {
        type: "response.output_text.delta",
        output_index: 1,
        item_id: "msg_1",
        content_index: 0,
        delta: "Calling tool.",
      },
    ],
    [
      "response.output_text.done",
      {
        type: "response.output_text.done",
        output_index: 1,
        item_id: "msg_1",
        content_index: 0,
        text: "Calling tool.",
      },
    ],
    [
      "response.content_part.done",
      {
        type: "response.content_part.done",
        output_index: 1,
        item_id: "msg_1",
        content_index: 0,
        part: { type: "output_text", text: "Calling tool.", annotations: [] },
      },
    ],
    [
      "response.output_item.done",
      {
        type: "response.output_item.done",
        output_index: 1,
        item: {
          id: "msg_1",
          type: "message",
          role: "assistant",
          content: [{ type: "output_text", text: "Calling tool.", annotations: [] }],
          status: "completed",
        },
      },
    ],
    [
      "response.output_item.added",
      {
        type: "response.output_item.added",
        output_index: 2,
        item: {
          id: "fc_2",
          type: "function_call",
          call_id: "call_w1",
          name: "lookup",
          arguments: "",
          status: "in_progress",
        },
      },
    ],
    [
      "response.function_call_arguments.delta",
      {
        type: "response.function_call_arguments.delta",
        output_index: 2,
        item_id: "fc_2",
        delta: '{"city":',
      },
    ],
    [
      "response.function_call_arguments.delta",
      {
        type: "response.function_call_arguments.delta",
        output_index: 2,
        item_id: "fc_2",
        delta: '"Tokyo"}',
      },
    ],
    [
      "response.function_call_arguments.done",
      {
        type: "response.function_call_arguments.done",
        output_index: 2,
        item_id: "fc_2",
        arguments: '{"city":"Tokyo"}',
      },
    ],
    [
      "response.output_item.done",
      {
        type: "response.output_item.done",
        output_index: 2,
        item: {
          id: "fc_2",
          type: "function_call",
          call_id: "call_w1",
          name: "lookup",
          arguments: '{"city":"Tokyo"}',
          status: "completed",
        },
      },
    ],
    [
      "response.completed",
      {
        type: "response.completed",
        response: {
          id: "resp_1",
          object: "response",
          model: "upstream-resp",
          status: "completed",
          output: [
            {
              id: "rs_0",
              type: "reasoning",
              summary: [{ type: "summary_text", text: "check weather" }],
            },
            {
              id: "msg_1",
              type: "message",
              role: "assistant",
              content: [{ type: "output_text", text: "Calling tool." }],
            },
            {
              id: "fc_2",
              type: "function_call",
              call_id: "call_w1",
              name: "lookup",
              arguments: '{"city":"Tokyo"}',
            },
          ],
          usage: { input_tokens: 14, output_tokens: 9, total_tokens: 23 },
        },
      },
    ],
  ];
  const chunks: Uint8Array[] = [];
  for (const [ev, body] of upstreamEvents) {
    chunks.push(
      ...translator.consume({ event: ev, id: "", data: JSON.stringify(body), wire: "" }).chunks,
    );
  }
  chunks.push(...translator.finalize());
  const emitted = parseTranslatedSse(chunks).map((e) => ({
    event: e.event,
    body: JSON.parse(e.data) as Record<string, unknown>,
  }));
  expect(emitted.filter((e) => e.event === "message_start")).toHaveLength(1);
  expect(emitted.filter((e) => e.event === "message_stop")).toHaveLength(1);
  const starts = emitted.filter((e) => e.event === "content_block_start");
  const stops = emitted.filter((e) => e.event === "content_block_stop");
  expect(starts.map((e) => e.body.index)).toEqual([0, 1, 2]);
  expect(stops.map((e) => e.body.index)).toEqual([0, 1, 2]);
  expect(starts.map((e) => (e.body.content_block as Record<string, unknown>).type)).toEqual([
    "thinking",
    "text",
    "tool_use",
  ]);
  const deltas = emitted.filter((e) => e.event === "content_block_delta");
  expect(
    deltas
      .filter((e) => (e.body.delta as Record<string, unknown>).type === "thinking_delta")
      .map((e) => (e.body.delta as Record<string, unknown>).thinking)
      .join(""),
  ).toBe("check weather");
  expect(
    deltas
      .filter((e) => (e.body.delta as Record<string, unknown>).type === "text_delta")
      .map((e) => (e.body.delta as Record<string, unknown>).text)
      .join(""),
  ).toBe("Calling tool.");
  const toolJson = deltas
    .filter((e) => (e.body.delta as Record<string, unknown>).type === "input_json_delta")
    .map((e) => (e.body.delta as Record<string, unknown>).partial_json)
    .join("");
  expect(JSON.parse(toolJson)).toEqual({ city: "Tokyo" });
  const msgDelta = emitted.find((e) => e.event === "message_delta");
  expect(msgDelta?.body).toMatchObject({
    delta: { stop_reason: "tool_use" },
    usage: { input_tokens: 14, output_tokens: 9 },
  });
});

test("Chat and Responses streams preserve full text, tools, content_filter incomplete_details, and single tool emission", () => {
  const toResp = new StreamTranslator("chat", "responses", "openai", "gpt-logical");
  const chatFrames = [
    { choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] },
    { choices: [{ index: 0, delta: { content: "Checking..." }, finish_reason: null }] },
    {
      choices: [
        {
          index: 0,
          delta: {
            tool_calls: [
              {
                index: 0,
                id: "call_1",
                type: "function",
                function: { name: "weather", arguments: "" },
              },
            ],
          },
          finish_reason: null,
        },
      ],
    },
    {
      choices: [
        {
          index: 0,
          delta: { tool_calls: [{ index: 0, function: { arguments: '{"city":"Paris"}' } }] },
          finish_reason: null,
        },
      ],
    },
    { choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] },
    { choices: [], usage: { prompt_tokens: 11, completion_tokens: 7, total_tokens: 18 } },
  ];
  const respChunks: Uint8Array[] = [];
  for (const frame of chatFrames) {
    respChunks.push(
      ...toResp.consume({ event: "message", id: "", wire: "", data: JSON.stringify(frame) }).chunks,
    );
  }
  respChunks.push(
    ...toResp.consume({ event: "message", id: "", wire: "", data: "[DONE]" }).chunks,
    ...toResp.finalize(),
  );
  const respEvents = parseTranslatedSse(respChunks);
  const completed = respEvents.find((e) => e.event === "response.completed");
  expect(completed).toBeDefined();
  expect(JSON.parse(completed!.data)).toMatchObject({
    type: "response.completed",
    response: {
      model: "gpt-logical",
      status: "completed",
      output: [
        { type: "message", content: [{ type: "output_text", text: "Checking..." }] },
        {
          type: "function_call",
          call_id: "call_1",
          name: "weather",
          arguments: '{"city":"Paris"}',
        },
      ],
      usage: { input_tokens: 11, output_tokens: 7, total_tokens: 18 },
    },
  });

  const filterToResp = new StreamTranslator("chat", "responses", "openai", "gpt-logical");
  const filterChunks = [
    ...filterToResp.consume({
      event: "message",
      id: "",
      wire: "",
      data: JSON.stringify({
        choices: [{ index: 0, delta: { content: "part" }, finish_reason: null }],
      }),
    }).chunks,
    ...filterToResp.consume({
      event: "message",
      id: "",
      wire: "",
      data: JSON.stringify({
        choices: [{ index: 0, delta: {}, finish_reason: "content_filter" }],
        usage: { prompt_tokens: 2, completion_tokens: 1, total_tokens: 3 },
      }),
    }).chunks,
    ...filterToResp.consume({ event: "message", id: "", wire: "", data: "[DONE]" }).chunks,
    ...filterToResp.finalize(),
  ];
  const filterEvents = parseTranslatedSse(filterChunks);
  const incompleteEvent = filterEvents.find((e) => e.event === "response.incomplete");
  expect(incompleteEvent).toBeDefined();
  expect(JSON.parse(incompleteEvent!.data)).toMatchObject({
    response: { status: "incomplete", incomplete_details: { reason: "content_filter" } },
  });

  const respToChat = new StreamTranslator("responses", "chat", "openai", "gpt-logical");
  const toChatChunks = [
    ...respToChat.consume({
      event: "response.output_item.added",
      id: "",
      wire: "",
      data: JSON.stringify({
        type: "response.output_item.added",
        output_index: 0,
        item: {
          id: "fc_0",
          type: "function_call",
          call_id: "call_9",
          name: "ping",
          arguments: "",
        },
      }),
    }).chunks,
    ...respToChat.consume({
      event: "response.function_call_arguments.delta",
      id: "",
      wire: "",
      data: JSON.stringify({
        type: "response.function_call_arguments.delta",
        output_index: 0,
        delta: '{"host":"a"}',
      }),
    }).chunks,
    ...respToChat.consume({
      event: "response.completed",
      id: "",
      wire: "",
      data: JSON.stringify({
        type: "response.completed",
        response: {
          id: "r9",
          status: "completed",
          output: [
            {
              id: "fc_0",
              type: "function_call",
              call_id: "call_9",
              name: "ping",
              arguments: '{"host":"a"}',
            },
          ],
          usage: { input_tokens: 3, output_tokens: 4, total_tokens: 7 },
        },
      }),
    }).chunks,
    ...respToChat.finalize(),
  ];
  const chatOut = parseTranslatedSse(toChatChunks)
    .filter((e) => e.data !== "[DONE]")
    .map(
      (e) =>
        JSON.parse(e.data) as {
          choices?: Array<{
            delta?: {
              tool_calls?: Array<{ id?: string; function?: { name?: string; arguments?: string } }>;
            };
          }>;
        },
    );
  const toolChunks = chatOut.flatMap((c) => c.choices?.[0]?.delta?.tool_calls ?? []);
  expect(toolChunks.map((t) => t.id ?? "").join("")).toBe("call_9");
  expect(toolChunks.map((t) => t.function?.name ?? "").join("")).toBe("ping");
  expect(toolChunks.map((t) => t.function?.arguments ?? "").join("")).toBe('{"host":"a"}');
});

test("symptom: fragmented streamed tool function names overwrite earlier fragments across protocols", () => {
  const frames = [
    {
      choices: [
        {
          index: 0,
          delta: {
            role: "assistant",
            tool_calls: [
              {
                index: 0,
                id: "call-add",
                type: "function",
                function: { name: "ad", arguments: "" },
              },
              {
                index: 1,
                id: "call-multiply",
                type: "function",
                function: { name: "multi", arguments: "" },
              },
            ],
          },
          finish_reason: null,
        },
      ],
    },
    {
      choices: [
        {
          index: 0,
          delta: {
            tool_calls: [
              { index: 1, function: { name: "ply", arguments: '{"x":3,' } },
              { index: 0, function: { arguments: '{"x":1,' } },
            ],
          },
          finish_reason: null,
        },
      ],
    },
    {
      choices: [
        {
          index: 0,
          delta: {
            tool_calls: [
              { index: 0, function: { name: "d", arguments: '"y":2}' } },
              { index: 1, function: { arguments: '"y":4}' } },
            ],
          },
          finish_reason: null,
        },
      ],
    },
    {
      choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }],
      usage: { prompt_tokens: 5, completion_tokens: 6, total_tokens: 11 },
    },
  ];

  for (const target of ["messages", "responses", "gemini"] as const) {
    const translator = new StreamTranslator("chat", target, "openai", "logical-model");
    const chunks: Uint8Array[] = [];
    for (const frame of frames) {
      chunks.push(
        ...translator.consume({
          event: "message",
          id: "",
          wire: "",
          data: JSON.stringify(frame),
        }).chunks,
      );
    }
    chunks.push(
      ...translator.consume({ event: "message", id: "", wire: "", data: "[DONE]" }).chunks,
      ...translator.finalize(),
    );
    const events = parseTranslatedSse(chunks);
    if (target === "messages") {
      const starts = events
        .filter((e) => e.event === "content_block_start")
        .map(
          (e) =>
            JSON.parse(e.data) as { index: number; content_block: { id: string; name: string } },
        );
      const deltas = events
        .filter((e) => e.event === "content_block_delta")
        .map((e) => JSON.parse(e.data) as { index: number; delta: { partial_json: string } });
      expect(starts.map((s) => ({ id: s.content_block.id, name: s.content_block.name }))).toEqual([
        { id: "call-add", name: "add" },
        { id: "call-multiply", name: "multiply" },
      ]);
      expect(
        JSON.parse(
          deltas
            .filter((d) => d.index === 0)
            .map((d) => d.delta.partial_json)
            .join(""),
        ),
      ).toEqual({
        x: 1,
        y: 2,
      });
      expect(
        JSON.parse(
          deltas
            .filter((d) => d.index === 1)
            .map((d) => d.delta.partial_json)
            .join(""),
        ),
      ).toEqual({
        x: 3,
        y: 4,
      });
    } else if (target === "responses") {
      const started = events
        .filter((event) => event.event === "response.output_item.added")
        .map((event) => JSON.parse(event.data)) as Array<{
        output_index: number;
        item: { id: string; call_id: string; name: string };
      }>;
      expect(started).toMatchObject([
        { output_index: 0, item: { id: "fc_0", call_id: "call-add", name: "add" } },
        { output_index: 1, item: { id: "fc_1", call_id: "call-multiply", name: "multiply" } },
      ]);
      for (const { output_index, item } of started) {
        const args = events
          .filter((event) => event.event === "response.function_call_arguments.delta")
          .map((event) => JSON.parse(event.data))
          .filter((event) => event.item_id === item.id && event.output_index === output_index)
          .map((event) => event.delta)
          .join("");
        expect(args).toBe(output_index === 0 ? '{"x":1,"y":2}' : '{"x":3,"y":4}');
      }
      const completed = events.find((e) => e.event === "response.completed");
      const output = (
        JSON.parse(completed!.data) as { response: { output: Array<Record<string, unknown>> } }
      ).response.output;
      expect(output).toMatchObject([
        { type: "function_call", call_id: "call-add", name: "add", arguments: '{"x":1,"y":2}' },
        {
          type: "function_call",
          call_id: "call-multiply",
          name: "multiply",
          arguments: '{"x":3,"y":4}',
        },
      ]);
    } else {
      const calls = events
        .map(
          (e) =>
            JSON.parse(e.data) as {
              candidates?: Array<{
                content?: { parts?: Array<{ functionCall?: Record<string, unknown> }> };
              }>;
            },
        )
        .flatMap((b) => b.candidates?.[0]?.content?.parts ?? [])
        .map((p) => p.functionCall)
        .filter(Boolean);
      expect(calls).toEqual([
        { id: "call-add", name: "add", args: { x: 1, y: 2 } },
        { id: "call-multiply", name: "multiply", args: { x: 3, y: 4 } },
      ]);
    }
  }
});

test("symptom: fragmented tool names can exceed the response conversion byte budget", () => {
  const translator = new StreamTranslator("chat", "responses", "openai", "model", 5);
  const consume = (name: string) =>
    translator.consume({
      event: "message",
      id: "",
      wire: "",
      data: JSON.stringify({ choices: [{ delta: { tool_calls: [{ function: { name } }] } }] }),
    });
  consume("雪");
  expect(() => consume("山")).toThrow("Converted response aggregate exceeds limit");
});

test("symptom: text after a deferred function call announces a later Responses item first", () => {
  const translator = new StreamTranslator("chat", "responses", "openai", "model");
  const consume = (data: string) =>
    translator.consume({ event: "message", id: "", wire: "", data });
  const choices = [
    { delta: { tool_calls: [{ id: "call_1", function: { name: "lo", arguments: "{" } }] } },
    { delta: { content: "answer" } },
    {
      delta: { tool_calls: [{ function: { name: "okup", arguments: "}" } }] },
      finish_reason: "tool_calls",
    },
  ];
  const chunks = [
    ...choices.flatMap((choice) => consume(JSON.stringify({ choices: [choice] })).chunks),
    ...consume("[DONE]").chunks,
    ...translator.finalize(),
  ];
  const events = parseTranslatedSse(chunks).map((event) => JSON.parse(event.data));
  expect(events.filter((event) => event.type === "response.output_item.added")).toMatchObject([
    { output_index: 0, item: { id: "msg_0" } },
    { output_index: 1, item: { id: "fc_1", name: "lookup" } },
  ]);
  expect(events.filter((event) => event.type === "response.output_text.delta")).toMatchObject([
    { output_index: 0, item_id: "msg_0", delta: "answer" },
  ]);
});

test("Gemini stream functionCall STOP, trailing usageMetadata absorption, and trailing content or error rejection", () => {
  for (const target of ["chat", "messages"] as const) {
    const callTranslator = new StreamTranslator("gemini", target, "gemini", "m1");
    const callChunks = [
      ...callTranslator.consume({
        event: "message",
        id: "",
        wire: "",
        data: JSON.stringify({
          candidates: [
            {
              index: 0,
              content: {
                role: "model",
                parts: [{ functionCall: { id: "fc_1", name: "lookup", args: { q: "a" } } }],
              },
              finishReason: "STOP",
            },
          ],
          usageMetadata: { promptTokenCount: 3, candidatesTokenCount: 2 },
        }),
      }).chunks,
      ...callTranslator.finalize(),
    ];
    const callWire = new TextDecoder().decode(Buffer.concat(callChunks));
    expect(callWire).toContain(
      target === "chat" ? '"finish_reason":"tool_calls"' : '"stop_reason":"tool_use"',
    );

    const usageTranslator = new StreamTranslator("gemini", target, "gemini", "m1");
    const usageChunks = [
      ...usageTranslator.consume({
        event: "message",
        id: "",
        wire: "",
        data: JSON.stringify({
          candidates: [
            {
              index: 0,
              content: { role: "model", parts: [{ text: "truncated" }] },
              finishReason: "MAX_TOKENS",
            },
          ],
        }),
      }).chunks,
      ...usageTranslator.consume({
        event: "message",
        id: "",
        wire: "",
        data: JSON.stringify({
          usageMetadata: { promptTokenCount: 12, candidatesTokenCount: 20, totalTokenCount: 32 },
        }),
      }).chunks,
      ...usageTranslator.finalize(),
    ];
    expect(usageTranslator.usage).toEqual({ input_tokens: "12", output_tokens: "20" });
    const usageWire = new TextDecoder().decode(Buffer.concat(usageChunks));
    expect(usageWire).toContain(
      target === "chat" ? '"finish_reason":"length"' : '"stop_reason":"max_tokens"',
    );
    expect(usageWire).toContain(
      target === "chat" ? '"completion_tokens":20' : '"output_tokens":20',
    );
  }

  for (const trailing of [
    {
      candidates: [{ index: 0, content: { role: "model", parts: [{ text: "late" }] } }],
      usageMetadata: { promptTokenCount: 1, candidatesTokenCount: 1 },
    },
    { error: { code: 500, message: "late failure" } },
  ]) {
    const t = new StreamTranslator("gemini", "chat", "gemini", "m1");
    t.consume({
      event: "message",
      id: "",
      wire: "",
      data: JSON.stringify({
        candidates: [
          { index: 0, content: { role: "model", parts: [{ text: "ok" }] }, finishReason: "STOP" },
        ],
      }),
    });
    expect(() =>
      t.consume({ event: "message", id: "", wire: "", data: JSON.stringify(trailing) }),
    ).toThrow(expect.objectContaining({ status: 502, code: "unexpected_trailing_event" }));
    expect(t.failed).toBe(true);
  }
});

test("leading empty choices filter frame and trailing usage-only frame bridge cleanly to Messages and Responses", () => {
  const frames = [
    {
      id: "az_1",
      choices: [],
      prompt_filter_results: [{ prompt_index: 0, content_filter_results: {} }],
    },
    {
      id: "az_1",
      choices: [{ index: 0, delta: { role: "assistant", content: "Hello" }, finish_reason: null }],
    },
    { id: "az_1", choices: [{ index: 0, delta: { content: " Azure" }, finish_reason: null }] },
    { id: "az_1", choices: [{ index: 0, delta: {}, finish_reason: "stop" }] },
    {
      id: "az_1",
      choices: [],
      usage: { prompt_tokens: 9, completion_tokens: 4, total_tokens: 13 },
    },
  ];
  for (const target of ["messages", "responses"] as const) {
    const translator = new StreamTranslator("chat", target, "openai", "m1");
    const chunks: Uint8Array[] = [];
    for (const frame of frames) {
      chunks.push(
        ...translator.consume({
          event: "message",
          id: "",
          wire: "",
          data: JSON.stringify(frame),
        }).chunks,
      );
    }
    chunks.push(
      ...translator.consume({ event: "message", id: "", wire: "", data: "[DONE]" }).chunks,
      ...translator.finalize(),
    );
    const events = parseTranslatedSse(chunks);
    if (target === "messages") {
      expect(events.filter((e) => e.event === "message_start")).toHaveLength(1);
      expect(events.filter((e) => e.event === "message_stop")).toHaveLength(1);
      expect(
        events
          .filter((e) => e.event === "content_block_delta")
          .map((e) => (JSON.parse(e.data) as { delta: { text: string } }).delta.text)
          .join(""),
      ).toBe("Hello Azure");
      expect(JSON.parse(events.find((e) => e.event === "message_delta")!.data)).toMatchObject({
        delta: { stop_reason: "end_turn" },
        usage: { input_tokens: 9, output_tokens: 4 },
      });
    } else {
      const completed = events.find((e) => e.event === "response.completed");
      expect(JSON.parse(completed!.data)).toMatchObject({
        response: {
          status: "completed",
          output: [{ type: "message", content: [{ type: "output_text", text: "Hello Azure" }] }],
          usage: { input_tokens: 9, output_tokens: 4, total_tokens: 13 },
        },
      });
    }
  }
});
