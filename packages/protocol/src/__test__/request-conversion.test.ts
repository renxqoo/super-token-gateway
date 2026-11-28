import { expect, test } from "bun:test";
import { FIELD_CONVERSION_POLICY, decodeRequest, encodeRequest, stringifyExact } from "../index.ts";

test("symptom: cross-protocol chat conversion rejects store: false instead of preserving stateless execution", () => {
  for (const storeValue of [false, null]) {
    const decoded = decodeRequest(
      {
        model: "gpt-6.1-terra",
        messages: [{ role: "user", content: "hi" }],
        stream: true,
        store: storeValue,
      },
      "chat",
    );
    const encodedResponses = encodeRequest(decoded, "responses", "gpt-6.1-terra");
    expect(encodedResponses.store).toBe(false);
    expect(encodedResponses.stream).toBe(true);
    const encodedMessages = encodeRequest(decoded, "messages", "claude-sonnet-5-5");
    expect(encodedMessages.messages).toEqual([
      { role: "user", content: [{ type: "text", text: "hi" }] },
    ]);
  }

  for (const source of ["chat", "responses"] as const) {
    const payload =
      source === "chat"
        ? { messages: [{ role: "user", content: "hi" }], store: true }
        : { input: "hi", store: true };
    expect(() => decodeRequest(payload, source, { mode: "strict" })).toThrow(
      "stored response continuation",
    );
    expect(() => decodeRequest(payload, source, { mode: "compat" })).toThrow(
      "stored response continuation",
    );
    const badStore =
      source === "chat"
        ? { messages: [{ role: "user", content: "hi" }], store: "false" }
        : { input: "hi", store: "false" };
    expect(() => decodeRequest(badStore, source, { mode: "compat" })).toThrow("store");
  }
});

test("symptom: cross-protocol chat conversion rejects reasoning_effort instead of mapping to responses and messages effort", () => {
  const fromChat = decodeRequest(
    {
      model: "gpt-6-2-sol",
      messages: [{ role: "user", content: "solve" }],
      stream: true,
      reasoning_effort: "high",
      response_format: {
        type: "json_schema",
        json_schema: { name: "answer", schema: { type: "object" } },
      },
    },
    "chat",
  );
  expect(fromChat.reasoningEffort).toBe("high");
  expect(encodeRequest(fromChat, "responses", "gpt-6-2-sol")).toMatchObject({
    model: "gpt-6-2-sol",
    stream: true,
    reasoning: { effort: "high", summary: "auto" },
    text: { format: { type: "json_schema", name: "response", schema: { type: "object" } } },
  });
  expect(
    encodeRequest(
      decodeRequest(
        { messages: [{ role: "user", content: "hi" }], reasoning_effort: "none" },
        "chat",
      ),
      "responses",
      "gpt-6-2-sol",
    ).reasoning,
  ).toEqual({ effort: "none" });
  expect(encodeRequest(fromChat, "chat", "gpt-6-2-sol")).toMatchObject({
    reasoning_effort: "high",
  });
  expect(encodeRequest(fromChat, "messages", "claude-opus-5-5")).toMatchObject({
    model: "claude-opus-5-5",
    stream: true,
    output_config: {
      effort: "high",
      format: { type: "json_schema", schema: { type: "object" } },
    },
  });
  expect(() => encodeRequest(fromChat, "gemini", "gemini-3-8-flash")).toThrow("reasoning_effort");
  expect(() => encodeRequest(fromChat, "gemini", "gemini-3-8-flash", { mode: "compat" })).toThrow(
    "reasoning_effort",
  );

  const fromChatNull = decodeRequest(
    {
      messages: [{ role: "user", content: "hi" }],
      reasoning_effort: null,
    },
    "chat",
  );
  expect(fromChatNull.reasoningEffort).toBeUndefined();
  expect(encodeRequest(fromChatNull, "responses", "gpt-6-2-sol").reasoning).toBeUndefined();
  expect(encodeRequest(fromChatNull, "messages", "claude-opus-5-5").output_config).toBeUndefined();

  const fromResponses = decodeRequest(
    {
      input: "solve",
      reasoning: { effort: "xhigh" },
    },
    "responses",
  );
  expect(fromResponses.reasoningEffort).toBe("xhigh");
  expect(encodeRequest(fromResponses, "chat", "gpt-5-2")).toMatchObject({
    reasoning_effort: "xhigh",
  });
  expect(encodeRequest(fromResponses, "responses", "gpt-6-2-sol")).toMatchObject({
    reasoning: { effort: "xhigh", summary: "auto" },
  });
  expect(encodeRequest(fromResponses, "messages", "claude-opus-5-5")).toMatchObject({
    output_config: { effort: "xhigh" },
  });
  expect(() => encodeRequest(fromResponses, "gemini", "gemini-3-8-flash")).toThrow(
    "reasoning_effort",
  );

  const fromMessages = decodeRequest(
    {
      messages: [{ role: "user", content: "solve" }],
      output_config: { effort: "medium" },
    },
    "messages",
  );
  expect(fromMessages.reasoningEffort).toBe("medium");
  expect(encodeRequest(fromMessages, "chat", "gpt-5-2")).toMatchObject({
    reasoning_effort: "medium",
  });
  expect(encodeRequest(fromMessages, "responses", "gpt-6-2-sol")).toMatchObject({
    reasoning: { effort: "medium", summary: "auto" },
  });
  expect(encodeRequest(fromMessages, "messages", "claude-opus-5-5")).toMatchObject({
    output_config: { effort: "medium" },
  });
  expect(() => encodeRequest(fromMessages, "gemini", "gemini-3-8-flash")).toThrow(
    "output_config.effort",
  );

  for (const emptyReasoning of [null, {}, { effort: null }]) {
    const decoded = decodeRequest({ input: "hi", reasoning: emptyReasoning }, "responses");
    expect(decoded.reasoningEffort).toBeUndefined();
    expect(encodeRequest(decoded, "chat", "gpt-5-2").reasoning_effort).toBeUndefined();
  }

  expect(() =>
    decodeRequest({ input: "hi", reasoning: { effort: "high", summary: "detailed" } }, "responses"),
  ).toThrow("reasoning.summary");
  expect(() => decodeRequest({ input: "hi", reasoning: "high" }, "responses")).toThrow("reasoning");
  expect(() => decodeRequest({ input: "hi", reasoning: { effort: "" } }, "responses")).toThrow(
    "reasoning.effort",
  );
  expect(() =>
    decodeRequest(
      { messages: [{ role: "user", content: "hi" }], output_config: { effort: "" } },
      "messages",
    ),
  ).toThrow("output_config.effort");
  expect(() =>
    decodeRequest({ messages: [{ role: "user", content: "hi" }], reasoning_effort: "" }, "chat"),
  ).toThrow("reasoning_effort");
  expect(() =>
    decodeRequest({ messages: [{ role: "user", content: "hi" }], reasoning_effort: 3 }, "chat"),
  ).toThrow("reasoning_effort");

  expect(
    FIELD_CONVERSION_POLICY.some(
      (e) =>
        e.field === "reasoning_effort" && e.action === "map" && e.protocols.includes("messages"),
    ),
  ).toBe(true);
});

test("Responses stateful continuation fields reject in both strict and compat conversion modes", () => {
  const statefulPayloads: Array<[string, Record<string, unknown>]> = [
    ["previous_response_id", { input: "hi", previous_response_id: "resp_prev" }],
    ["conversation", { input: "hi", conversation: "conv_1" }],
    ["prompt", { input: "hi", prompt: { id: "pmpt_1" } }],
    ["context_management", { input: "hi", context_management: [{ type: "compaction" }] }],
  ];
  for (const [field, payload] of statefulPayloads) {
    for (const mode of ["strict", "compat"] as const) {
      expect(() => decodeRequest(payload, "responses", { mode }), `${field}:${mode}`).toThrow(
        expect.objectContaining({ status: 422, code: "conversion_loss" }),
      );
    }
  }
});

test("Responses to Chat request conversion preserves developer role, scalar input, tool-only assistant message, and tool output", () => {
  const scalarChat = encodeRequest(
    decodeRequest({ input: "hello scalar" }, "responses"),
    "chat",
    "gpt-1",
  );
  expect(scalarChat.messages).toEqual([
    { role: "user", content: [{ type: "text", text: "hello scalar" }] },
  ]);

  const historyChat = encodeRequest(
    decodeRequest(
      {
        instructions: "top instruction",
        input: [
          { type: "message", role: "developer", content: "dev rule" },
          { type: "message", role: "user", content: "check weather" },
          {
            type: "function_call",
            call_id: "call_7",
            name: "weather",
            arguments: '{"city":"Paris"}',
          },
          {
            type: "function_call_output",
            call_id: "call_7",
            output: '{"temp":21}',
          },
        ],
      },
      "responses",
    ),
    "chat",
    "gpt-1",
  );
  expect(historyChat.messages).toEqual([
    { role: "system", content: [{ type: "text", text: "top instruction" }] },
    { role: "system", content: [{ type: "text", text: "dev rule" }] },
    { role: "user", content: [{ type: "text", text: "check weather" }] },
    {
      role: "assistant",
      content: null,
      tool_calls: [
        {
          id: "call_7",
          type: "function",
          function: { name: "weather", arguments: '{"city":"Paris"}' },
        },
      ],
    },
    { role: "tool", tool_call_id: "call_7", content: '{"temp":21}' },
  ]);
});

test("symptom: encodeRequest fails on empty tool arguments, accepts non-object tool args, and rounds large integers in tool history", () => {
  const fromHistoryArgs = (args: string) =>
    decodeRequest(
      {
        messages: [
          { role: "user", content: "run" },
          {
            role: "assistant",
            content: null,
            tool_calls: [
              { id: "call_1", type: "function", function: { name: "lookup", arguments: args } },
            ],
          },
        ],
      },
      "chat",
    );

  const emptyReq = fromHistoryArgs("");
  expect(encodeRequest(emptyReq, "messages", "claude-1")).toMatchObject({
    messages: [
      { role: "user", content: [{ type: "text", text: "run" }] },
      {
        role: "assistant",
        content: [{ type: "tool_use", id: "call_1", name: "lookup", input: {} }],
      },
    ],
  });
  expect(encodeRequest(emptyReq, "gemini", "gemini-1")).toMatchObject({
    contents: [
      { role: "user", parts: [{ text: "run" }] },
      { role: "model", parts: [{ functionCall: { id: "call_1", name: "lookup", args: {} } }] },
    ],
  });

  const exactReq = fromHistoryArgs('{"id":9007199254740993,"ids":[18446744073709551615]}');
  expect(stringifyExact(encodeRequest(exactReq, "messages", "claude-1"))).toContain(
    "9007199254740993",
  );
  expect(stringifyExact(encodeRequest(exactReq, "messages", "claude-1"))).toContain(
    "18446744073709551615",
  );
  expect(stringifyExact(encodeRequest(exactReq, "gemini", "gemini-1"))).toContain(
    "9007199254740993",
  );
  expect(stringifyExact(encodeRequest(exactReq, "gemini", "gemini-1"))).toContain(
    "18446744073709551615",
  );

  for (const badArgs of ["null", '["x"]', '"x"']) {
    const badReq = fromHistoryArgs(badArgs);
    expect(() => encodeRequest(badReq, "messages", "claude-1")).toThrow();
    expect(() => encodeRequest(badReq, "gemini", "gemini-1")).toThrow();
  }
});
