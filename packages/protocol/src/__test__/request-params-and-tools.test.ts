import { expect, test } from "bun:test";
import { decodeRequest, encodeRequest } from "../index.ts";

test("explicit zero sampling parameters and false booleans survive cross-protocol conversion while non-positive token limits reject", () => {
  const toolDef = {
    type: "function",
    function: { name: "ping", description: "p", parameters: { type: "object" } },
  };
  const chatReq = decodeRequest(
    {
      messages: [{ role: "user", content: "hi" }],
      temperature: 0,
      top_p: 0,
      stream: false,
      tools: [toolDef],
      parallel_tool_calls: false,
    },
    "chat",
  );
  expect(chatReq.temperature).toBe(0);
  expect(chatReq.topP).toBe(0);
  expect(chatReq.stream).toBe(false);
  expect(chatReq.parallelToolCalls).toBe(false);

  expect(encodeRequest(chatReq, "chat", "m")).toMatchObject({
    temperature: 0,
    top_p: 0,
    stream: false,
    parallel_tool_calls: false,
  });
  expect(encodeRequest(chatReq, "responses", "m")).toMatchObject({
    temperature: 0,
    top_p: 0,
    stream: false,
    parallel_tool_calls: false,
  });
  expect(encodeRequest(chatReq, "messages", "m")).toMatchObject({
    temperature: 0,
    top_p: 0,
    stream: false,
    tool_choice: { type: "auto", disable_parallel_tool_use: true },
  });
  expect(encodeRequest(chatReq, "gemini", "m", { mode: "compat" })).toMatchObject({
    generationConfig: { temperature: 0, topP: 0 },
  });

  const geminiZero = decodeRequest(
    {
      contents: [{ role: "user", parts: [{ text: "hi" }] }],
      generationConfig: { temperature: 0, topP: 0 },
    },
    "gemini",
  );
  expect(geminiZero.temperature).toBe(0);
  expect(geminiZero.topP).toBe(0);

  for (const [source, body] of [
    ["chat", { messages: [{ role: "user", content: "hi" }], max_tokens: 0 }],
    ["chat", { messages: [{ role: "user", content: "hi" }], max_completion_tokens: 0 }],
    ["responses", { input: "hi", max_output_tokens: 0 }],
    ["messages", { messages: [{ role: "user", content: "hi" }], top_k: 0 }],
    [
      "gemini",
      { contents: [{ role: "user", parts: [{ text: "hi" }] }], generationConfig: { topK: 0 } },
    ],
  ] as const) {
    expect(() => decodeRequest(body, source)).toThrow(
      expect.objectContaining({ status: 422, code: "conversion_loss" }),
    );
  }
});

test("symptom: encodeRequest emits unsupported stop field for responses target instead of rejecting in strict mode and dropping in compat mode", () => {
  const fromChatString = decodeRequest(
    { messages: [{ role: "user", content: "hi" }], stop: "END" },
    "chat",
  );
  expect(fromChatString.stop).toEqual(["END"]);
  expect(encodeRequest(fromChatString, "messages", "claude-1").stop_sequences).toEqual(["END"]);
  expect(
    (
      encodeRequest(fromChatString, "gemini", "gemini-1").generationConfig as Record<
        string,
        unknown
      >
    ).stopSequences,
  ).toEqual(["END"]);

  expect(() =>
    decodeRequest({ messages: [{ role: "user", content: "hi" }], stop: ["END", 42] }, "chat"),
  ).toThrow("stop");

  expect(() => encodeRequest(fromChatString, "responses", "gpt-5", { mode: "strict" })).toThrow(
    "stop",
  );
  const dropped: string[] = [];
  const compatResp = encodeRequest(fromChatString, "responses", "gpt-5", {
    mode: "compat",
    droppedFields: dropped,
  });
  expect(compatResp.stop).toBeUndefined();
  expect(dropped).toContain("stop");
  expect(() => decodeRequest(compatResp, "responses")).not.toThrow();

  const emptyStop = decodeRequest(
    { messages: [{ role: "user", content: "hi" }], stop: [] },
    "chat",
  );
  expect(encodeRequest(emptyStop, "responses", "gpt-5", { mode: "strict" }).stop).toBeUndefined();
});

test("symptom: cross-protocol tool turns split parallel function calls and tool outputs into consecutive same-role messages", () => {
  const fromResponses = decodeRequest(
    {
      input: [
        { type: "message", role: "user", content: "Check Tokyo and Paris" },
        {
          type: "message",
          role: "assistant",
          content: [{ type: "output_text", text: "Checking both." }],
        },
        {
          type: "function_call",
          call_id: "c1",
          name: "weather",
          arguments: '{"city":"Tokyo"}',
        },
        {
          type: "function_call",
          call_id: "c2",
          name: "weather",
          arguments: '{"city":"Paris"}',
        },
        { type: "function_call_output", call_id: "c1", output: "Sunny" },
        { type: "function_call_output", call_id: "c2", output: "Rainy" },
      ],
    },
    "responses",
  );

  expect(encodeRequest(fromResponses, "chat", "gpt-5").messages).toEqual([
    { role: "user", content: [{ type: "text", text: "Check Tokyo and Paris" }] },
    {
      role: "assistant",
      content: [{ type: "text", text: "Checking both." }],
      tool_calls: [
        {
          id: "c1",
          type: "function",
          function: { name: "weather", arguments: '{"city":"Tokyo"}' },
        },
        {
          id: "c2",
          type: "function",
          function: { name: "weather", arguments: '{"city":"Paris"}' },
        },
      ],
    },
    { role: "tool", tool_call_id: "c1", content: "Sunny" },
    { role: "tool", tool_call_id: "c2", content: "Rainy" },
  ]);

  expect(encodeRequest(fromResponses, "messages", "claude-1").messages).toEqual([
    { role: "user", content: [{ type: "text", text: "Check Tokyo and Paris" }] },
    {
      role: "assistant",
      content: [
        { type: "text", text: "Checking both." },
        { type: "tool_use", id: "c1", name: "weather", input: { city: "Tokyo" } },
        { type: "tool_use", id: "c2", name: "weather", input: { city: "Paris" } },
      ],
    },
    {
      role: "user",
      content: [
        { type: "tool_result", tool_use_id: "c1", content: "Sunny" },
        { type: "tool_result", tool_use_id: "c2", content: "Rainy" },
      ],
    },
  ]);

  expect(encodeRequest(fromResponses, "gemini", "gemini-1").contents).toEqual([
    { role: "user", parts: [{ text: "Check Tokyo and Paris" }] },
    {
      role: "model",
      parts: [
        { text: "Checking both." },
        { functionCall: { id: "c1", name: "weather", args: { city: "Tokyo" } } },
        { functionCall: { id: "c2", name: "weather", args: { city: "Paris" } } },
      ],
    },
    {
      role: "user",
      parts: [
        { functionResponse: { id: "c1", name: "weather", response: { result: "Sunny" } } },
        { functionResponse: { id: "c2", name: "weather", response: { result: "Rainy" } } },
      ],
    },
  ]);

  const fromChat = decodeRequest(
    {
      messages: [
        { role: "user", content: "Check two" },
        {
          role: "assistant",
          content: null,
          tool_calls: [
            { id: "t1", type: "function", function: { name: "alpha", arguments: '{"k":1}' } },
            { id: "t2", type: "function", function: { name: "beta", arguments: '{"k":2}' } },
          ],
        },
        { role: "tool", tool_call_id: "t1", content: "res-1" },
        { role: "tool", tool_call_id: "t2", content: "res-2" },
        { role: "user", content: "Compare them" },
      ],
    },
    "chat",
  );
  expect(encodeRequest(fromChat, "messages", "claude-1").messages).toEqual([
    { role: "user", content: [{ type: "text", text: "Check two" }] },
    {
      role: "assistant",
      content: [
        { type: "tool_use", id: "t1", name: "alpha", input: { k: 1 } },
        { type: "tool_use", id: "t2", name: "beta", input: { k: 2 } },
      ],
    },
    {
      role: "user",
      content: [
        { type: "tool_result", tool_use_id: "t1", content: "res-1" },
        { type: "tool_result", tool_use_id: "t2", content: "res-2" },
        { type: "text", text: "Compare them" },
      ],
    },
  ]);
  expect(encodeRequest(fromChat, "gemini", "gemini-1").contents).toEqual([
    { role: "user", parts: [{ text: "Check two" }] },
    {
      role: "model",
      parts: [
        { functionCall: { id: "t1", name: "alpha", args: { k: 1 } } },
        { functionCall: { id: "t2", name: "beta", args: { k: 2 } } },
      ],
    },
    {
      role: "user",
      parts: [
        { functionResponse: { id: "t1", name: "alpha", response: { result: "res-1" } } },
        { functionResponse: { id: "t2", name: "beta", response: { result: "res-2" } } },
        { text: "Compare them" },
      ],
    },
  ]);

  const orphanToolChat = decodeRequest(
    {
      messages: [
        { role: "user", content: "hi" },
        { role: "tool", tool_call_id: "missing_call", content: "res" },
      ],
    },
    "chat",
  );
  expect(() => encodeRequest(orphanToolChat, "gemini", "gemini-1")).toThrow(
    "tool result without function name",
  );

  const fromMessages = decodeRequest(
    {
      messages: [
        { role: "user", content: "lookup" },
        {
          role: "assistant",
          content: [{ type: "tool_use", id: "tu_1", name: "search", input: { q: "x" } }],
        },
        {
          role: "user",
          content: [{ type: "tool_result", tool_use_id: "tu_1", content: "found" }],
        },
      ],
    },
    "messages",
  );
  expect(encodeRequest(fromMessages, "gemini", "gemini-1").contents).toEqual([
    { role: "user", parts: [{ text: "lookup" }] },
    {
      role: "model",
      parts: [{ functionCall: { id: "tu_1", name: "search", args: { q: "x" } } }],
    },
    {
      role: "user",
      parts: [{ functionResponse: { id: "tu_1", name: "search", response: { result: "found" } } }],
    },
  ]);
});

test("symptom: tool_choice accepts contradictory type/name combinations while valid parallel_tool_calls and Gemini allowedFunctionNames map strictly", () => {
  const twoTools = [
    { type: "function", function: { name: "weather", parameters: { type: "object" } } },
    { type: "function", function: { name: "stock", parameters: { type: "object" } } },
  ];

  const noneWithNoParallel = decodeRequest(
    {
      messages: [{ role: "user", content: "hi" }],
      tools: twoTools,
      tool_choice: "none",
      parallel_tool_calls: false,
    },
    "chat",
  );
  expect(encodeRequest(noneWithNoParallel, "messages", "claude-1").tool_choice).toEqual({
    type: "none",
  });

  const noToolsNoParallel = decodeRequest(
    { messages: [{ role: "user", content: "hi" }], parallel_tool_calls: false },
    "chat",
  );
  expect(encodeRequest(noToolsNoParallel, "messages", "claude-1").tool_choice).toBeUndefined();
  expect(() => encodeRequest(noToolsNoParallel, "gemini", "gemini-1", { mode: "strict" })).toThrow(
    "parallel_tool_calls",
  );
  const geminiDropped: string[] = [];
  encodeRequest(noToolsNoParallel, "gemini", "gemini-1", {
    mode: "compat",
    droppedFields: geminiDropped,
  });
  expect(geminiDropped).toContain("parallel_tool_calls");

  const geminiSubset = decodeRequest(
    {
      contents: [{ role: "user", parts: [{ text: "hi" }] }],
      tools: [
        {
          functionDeclarations: [
            { name: "a", parameters: { type: "object" } },
            { name: "b", parameters: { type: "object" } },
            { name: "c", parameters: { type: "object" } },
          ],
        },
      ],
      toolConfig: {
        functionCallingConfig: { mode: "ANY", allowedFunctionNames: ["a", "c"] },
      },
    },
    "gemini",
  );
  expect(geminiSubset.tools.map((t) => t.name)).toEqual(["a", "c"]);
  expect(geminiSubset.toolChoice).toBe("required");

  for (const badGeminiConfig of [
    { mode: "AUTO", allowedFunctionNames: ["a"] },
    { mode: "ANY", allowedFunctionNames: [] },
    { mode: "ANY", allowedFunctionNames: ["unknown"] },
    { mode: "ANY", allowedFunctionNames: ["a"], allowed_function_names: ["a"] },
  ]) {
    expect(() =>
      decodeRequest(
        {
          contents: [{ role: "user", parts: [{ text: "hi" }] }],
          tools: [{ functionDeclarations: [{ name: "a", parameters: { type: "object" } }] }],
          toolConfig: { functionCallingConfig: badGeminiConfig },
        },
        "gemini",
      ),
    ).toThrow("allowedFunctionNames");
  }

  for (const badChoice of [
    { type: "allowed_tools", mode: "auto", tools: [{ type: "function", function: { name: "a" } }] },
    { type: "allowed_tools", mode: "AUTO" },
    { type: "none", name: "weather" },
    { type: "auto", function: { name: "weather" } },
    { type: "any", name: "weather" },
    { type: "function" },
  ]) {
    expect(() =>
      decodeRequest(
        {
          messages: [{ role: "user", content: "hi" }],
          tools: twoTools,
          tool_choice: badChoice,
        },
        "chat",
      ),
    ).toThrow(expect.objectContaining({ status: 422, code: "conversion_loss" }));
  }
});

test("symptom: response_format and Gemini responseMimeType silently accept invalid or schema-less formats", () => {
  const validJsonObj = decodeRequest(
    { messages: [{ role: "user", content: "hi" }], response_format: { type: "json_object" } },
    "chat",
  );
  expect(validJsonObj.jsonSchema).toEqual({ type: "object" });
  expect(
    decodeRequest(
      { messages: [{ role: "user", content: "hi" }], response_format: { type: "text" } },
      "chat",
    ).jsonSchema,
  ).toBeUndefined();
  expect(
    decodeRequest({ input: "hi", text: { format: { type: "text" } } }, "responses").jsonSchema,
  ).toBeUndefined();
  expect(
    decodeRequest(
      {
        contents: [{ role: "user", parts: [{ text: "hi" }] }],
        generationConfig: { responseMimeType: "text/plain" },
      },
      "gemini",
    ).jsonSchema,
  ).toBeUndefined();

  expect(() =>
    decodeRequest(
      {
        messages: [{ role: "user", content: "hi" }],
        response_format: {
          type: "json_schema",
          json_schema: { name: "ans", schema: { type: "object" }, strict: true },
        },
      },
      "chat",
    ),
  ).toThrow("strict output validation");

  for (const [source, body] of [
    ["chat", { messages: [{ role: "user", content: "hi" }], response_format: "json" }],
    ["chat", { messages: [{ role: "user", content: "hi" }], response_format: { type: "bogus" } }],
    [
      "chat",
      { messages: [{ role: "user", content: "hi" }], response_format: { type: "json_schema" } },
    ],
    ["responses", { input: "hi", text: { format: { type: "json_schema", name: "empty" } } }],
    [
      "messages",
      { messages: [{ role: "user", content: "hi" }], output_config: { format: { type: "bogus" } } },
    ],
    [
      "gemini",
      {
        contents: [{ role: "user", parts: [{ text: "hi" }] }],
        generationConfig: { responseMimeType: "text/x.enum" },
      },
    ],
  ] as const) {
    expect(() => decodeRequest(body, source), `${source}:${JSON.stringify(body)}`).toThrow(
      expect.objectContaining({ status: 422, code: "conversion_loss" }),
    );
  }
});

test("symptom: decodeRequest rejects JSON null optional parameters and silently masks conflicting max_tokens and max_completion_tokens", () => {
  const chatNulls = decodeRequest(
    {
      messages: [{ role: "user", content: "hi" }],
      n: null,
      temperature: null,
      top_p: null,
      max_tokens: null,
      max_completion_tokens: null,
      stop: null,
      parallel_tool_calls: null,
      tool_choice: null,
      response_format: null,
    },
    "chat",
  );
  expect(chatNulls.maxTokens).toBeUndefined();
  expect(chatNulls.temperature).toBeUndefined();
  expect(chatNulls.topP).toBeUndefined();
  expect(chatNulls.stop).toBeUndefined();
  expect(chatNulls.parallelToolCalls).toBeUndefined();
  expect(chatNulls.toolChoice).toBeUndefined();
  expect(chatNulls.jsonSchema).toBeUndefined();

  const responsesNulls = decodeRequest(
    {
      input: "hi",
      max_output_tokens: null,
      temperature: null,
      top_p: null,
      parallel_tool_calls: null,
      tool_choice: null,
    },
    "responses",
  );
  expect(responsesNulls.maxTokens).toBeUndefined();

  const messagesNulls = decodeRequest(
    {
      messages: [{ role: "user", content: "hi" }],
      max_tokens: null,
      temperature: null,
      top_p: null,
      top_k: null,
      stop_sequences: null,
      tool_choice: null,
    },
    "messages",
  );
  expect(messagesNulls.topK).toBeUndefined();
  expect(messagesNulls.stop).toBeUndefined();

  const geminiNulls = decodeRequest(
    {
      contents: [{ role: "user", parts: [{ text: "hi" }] }],
      generationConfig: null,
      generation_config: {
        max_output_tokens: null,
        temperature: null,
        top_p: null,
        top_k: null,
        stop_sequences: null,
        response_mime_type: null,
        response_schema: null,
      },
    },
    "gemini",
  );
  expect(geminiNulls.maxTokens).toBeUndefined();
  expect(geminiNulls.topK).toBeUndefined();

  expect(() =>
    decodeRequest(
      {
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 100,
        max_completion_tokens: 200,
      },
      "chat",
    ),
  ).toThrow(expect.objectContaining({ status: 422, code: "conversion_loss" }));
});
