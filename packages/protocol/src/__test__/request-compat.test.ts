import { expect, test } from "bun:test";
import { decodeRequest, encodeRequest } from "../index.ts";

test("symptom: Responses echoed input items with id/status and text-array function_call_output fail conversion", () => {
  const decoded = decodeRequest(
    {
      input: [
        {
          type: "message",
          id: "msg_1",
          status: "completed",
          role: "user",
          content: [{ type: "input_text", text: "Check weather" }],
        },
        {
          type: "function_call",
          id: "fc_1",
          status: "completed",
          call_id: "call_1",
          name: "weather",
          arguments: '{"city":"Tokyo"}',
        },
        {
          type: "function_call_output",
          id: "fco_1",
          status: "completed",
          call_id: "call_1",
          output: [
            { type: "input_text", text: "Sunny" },
            { type: "output_text", text: "22C" },
          ],
        },
      ],
    },
    "responses",
  );
  const chat = encodeRequest(decoded, "chat", "gpt-5");
  expect(chat.messages).toEqual([
    { role: "user", content: [{ type: "text", text: "Check weather" }] },
    {
      role: "assistant",
      content: null,
      tool_calls: [
        {
          id: "call_1",
          type: "function",
          function: { name: "weather", arguments: '{"city":"Tokyo"}' },
        },
      ],
    },
    { role: "tool", tool_call_id: "call_1", content: "Sunny\n22C" },
  ]);

  expect(() =>
    decodeRequest(
      {
        input: [
          {
            type: "function_call_output",
            call_id: "call_1",
            output: [{ type: "input_image", image_url: "https://example.com/a.png" }],
          },
        ],
      },
      "responses",
    ),
  ).toThrow("structured function_call_output");
});

test("symptom: Responses reasoning.summary fails in compat mode while remaining rejected in strict mode", () => {
  const dropped: string[] = [];
  const compat = decodeRequest(
    { input: "solve", reasoning: { effort: "high", summary: "auto" } },
    "responses",
    { mode: "compat", onFieldDropped: (f) => dropped.push(f) },
  );
  expect(compat.reasoningEffort).toBe("high");
  expect(dropped).toEqual(["reasoning.summary"]);

  expect(() =>
    decodeRequest({ input: "solve", reasoning: { effort: "high", summary: "auto" } }, "responses", {
      mode: "strict",
    }),
  ).toThrow("reasoning.summary");

  expect(() =>
    decodeRequest({ input: "solve", reasoning: { effort: "high", summary: 123 } }, "responses", {
      mode: "compat",
    }),
  ).toThrow("reasoning.summary");
});

test("symptom: Anthropic tool_result text-block arrays and compat is_error fail conversion", () => {
  const textBlocksReq = {
    messages: [
      {
        role: "assistant",
        content: [{ type: "tool_use", id: "tu_1", name: "search", input: { q: "a" } }],
      },
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            tool_use_id: "tu_1",
            content: [
              { type: "text", text: "first line" },
              { type: "text", text: "second line" },
            ],
          },
        ],
      },
    ],
  };
  const strictDecoded = decodeRequest(textBlocksReq, "messages", { mode: "strict" });
  const chatOut = encodeRequest(strictDecoded, "chat", "gpt-5");
  expect(chatOut.messages).toEqual([
    {
      role: "assistant",
      content: null,
      tool_calls: [
        { id: "tu_1", type: "function", function: { name: "search", arguments: '{"q":"a"}' } },
      ],
    },
    { role: "tool", tool_call_id: "tu_1", content: "first line\nsecond line" },
  ]);

  const errorToolReq = {
    messages: [
      {
        role: "user",
        content: [{ type: "tool_result", tool_use_id: "tu_1", is_error: true, content: "boom" }],
      },
    ],
  };
  expect(() => decodeRequest(errorToolReq, "messages", { mode: "strict" })).toThrow(
    "error or structured tool result",
  );
  const dropped: string[] = [];
  const compatError = decodeRequest(errorToolReq, "messages", {
    mode: "compat",
    onFieldDropped: (f) => dropped.push(f),
  });
  expect(dropped).toEqual(["tool_result.is_error"]);
  expect(compatError.messages[0]?.parts[0]).toEqual({
    type: "result",
    id: "tu_1",
    name: "",
    content: "boom",
  });

  expect(() =>
    decodeRequest(
      {
        messages: [
          {
            role: "user",
            content: [
              { type: "tool_result", tool_use_id: "tu_1", is_error: "yes", content: "boom" },
            ],
          },
        ],
      },
      "messages",
      { mode: "compat" },
    ),
  ).toThrow("error or structured tool result");
});

test("symptom: Anthropic thinking parameter fails cross-protocol conversion", () => {
  expect(
    decodeRequest(
      { messages: [{ role: "user", content: "hi" }], thinking: { type: "disabled" } },
      "messages",
    ).reasoningEffort,
  ).toBe("none");

  expect(
    decodeRequest(
      { messages: [{ role: "user", content: "hi" }], thinking: { type: "adaptive" } },
      "messages",
    ).reasoningEffort,
  ).toBe("medium");

  expect(() =>
    decodeRequest(
      {
        messages: [{ role: "user", content: "hi" }],
        thinking: { type: "enabled", budget_tokens: 4096 },
      },
      "messages",
      { mode: "strict" },
    ),
  ).toThrow("thinking.budget_tokens");

  const budgetCases: Array<[number, string]> = [
    [1024, "low"],
    [4096, "medium"],
    [16384, "high"],
  ];
  for (const [budget, expected] of budgetCases) {
    const dropped: string[] = [];
    const decoded = decodeRequest(
      {
        messages: [{ role: "user", content: "hi" }],
        thinking: { type: "enabled", budget_tokens: budget },
      },
      "messages",
      { mode: "compat", onFieldDropped: (f) => dropped.push(f) },
    );
    expect(decoded.reasoningEffort, String(budget)).toBe(expected);
    expect(dropped).toEqual(["thinking.budget_tokens"]);
  }

  const withExplicitEffort = decodeRequest(
    {
      messages: [{ role: "user", content: "hi" }],
      output_config: { effort: "low" },
      thinking: { type: "adaptive" },
    },
    "messages",
  );
  expect(withExplicitEffort.reasoningEffort).toBe("low");

  expect(() =>
    decodeRequest(
      {
        messages: [{ role: "user", content: "hi" }],
        output_config: { effort: "high" },
        thinking: { type: "disabled" },
      },
      "messages",
    ),
  ).toThrow("thinking");

  expect(() =>
    decodeRequest(
      {
        messages: [{ role: "user", content: "hi" }],
        output_config: { effort: "none" },
        thinking: { type: "adaptive" },
      },
      "messages",
    ),
  ).toThrow("thinking");

  expect(() =>
    decodeRequest(
      {
        messages: [{ role: "user", content: "hi" }],
        thinking: { type: "disabled", budget_tokens: 4096 },
      },
      "messages",
    ),
  ).toThrow("thinking.budget_tokens");

  expect(() =>
    decodeRequest(
      {
        messages: [{ role: "user", content: "hi" }],
        output_config: { effort: "low" },
        thinking: { type: "enabled", budget_tokens: 4096 },
      },
      "messages",
      { mode: "strict" },
    ),
  ).toThrow("thinking.budget_tokens");
  const conflictDrops: string[] = [];
  const conflictCompat = decodeRequest(
    {
      messages: [{ role: "user", content: "hi" }],
      output_config: { effort: "low" },
      thinking: { type: "enabled", budget_tokens: 16384 },
    },
    "messages",
    { mode: "compat", onFieldDropped: (f) => conflictDrops.push(f) },
  );
  expect(conflictCompat.reasoningEffort).toBe("low");
  expect(conflictDrops).toEqual(["thinking.budget_tokens"]);

  for (const badBudget of [0, -1, 1.5, "2048", Number.MAX_SAFE_INTEGER + 1, Number.NaN]) {
    for (const mode of ["strict", "compat"] as const) {
      expect(() =>
        decodeRequest(
          {
            messages: [{ role: "user", content: "hi" }],
            thinking: { type: "enabled", budget_tokens: badBudget },
          },
          "messages",
          { mode },
        ),
      ).toThrow("thinking.budget_tokens");
    }
  }
  expect(() =>
    decodeRequest(
      { messages: [{ role: "user", content: "hi" }], thinking: { type: "unknown" } },
      "messages",
      { mode: "compat" },
    ),
  ).toThrow("thinking");
  expect(() =>
    decodeRequest(
      { messages: [{ role: "user", content: "hi" }], output_config: { effort: "   " } },
      "messages",
      { mode: "compat" },
    ),
  ).toThrow("output_config.effort");
});

test("expanded compat top-level hint fields drop in compat mode and reject in strict mode", () => {
  const chatDrops: string[] = [];
  decodeRequest(
    {
      messages: [{ role: "user", content: "hi" }],
      frequency_penalty: 0.2,
      presence_penalty: 0.1,
      prompt_cache_key: "cache-1",
      reasoning_split: true,
    },
    "chat",
    { mode: "compat", onFieldDropped: (f) => chatDrops.push(f) },
  );
  expect(chatDrops).toEqual([
    "frequency_penalty",
    "presence_penalty",
    "prompt_cache_key",
    "reasoning_split",
  ]);
  expect(() =>
    decodeRequest({ messages: [{ role: "user", content: "hi" }], frequency_penalty: 0.2 }, "chat", {
      mode: "strict",
    }),
  ).toThrow("frequency_penalty");

  const respDrops: string[] = [];
  decodeRequest(
    {
      input: "hi",
      truncation: "auto",
      include: ["reasoning.encrypted_content"],
      prompt_cache_key: "cache-2",
    },
    "responses",
    { mode: "compat", onFieldDropped: (f) => respDrops.push(f) },
  );
  expect(respDrops).toEqual(["truncation", "include", "prompt_cache_key"]);

  const geminiDrops: string[] = [];
  decodeRequest(
    {
      contents: [{ role: "user", parts: [{ text: "hi" }] }],
      safety_settings: [{ category: "HARM_CATEGORY_HATE_SPEECH", threshold: "BLOCK_NONE" }],
    },
    "gemini",
    { mode: "compat", onFieldDropped: (f) => geminiDrops.push(f) },
  );
  expect(geminiDrops).toEqual(["safety_settings"]);
});

test("symptom: Gemini snake_case request fields fail decoding and duplicate camel/snake aliases silently collide", () => {
  const dropped: string[] = [];
  const decoded = decodeRequest(
    {
      system_instruction: { parts: [{ text: "Be helpful" }] },
      contents: [
        {
          role: "user",
          parts: [{ text: "describe" }, { inline_data: { mime_type: "image/png", data: "AAAA" } }],
        },
        {
          role: "model",
          parts: [
            { thought: true, text: "let me check", thought_signature: "sig-1" },
            { function_call: { id: "c1", name: "lookup", args: { q: "x" } } },
          ],
        },
        {
          role: "user",
          parts: [{ function_response: { id: "c1", name: "lookup", response: { val: 42 } } }],
        },
      ],
      tools: [
        {
          function_declarations: [
            { name: "lookup", description: "find", parameters_json_schema: { type: "object" } },
            { name: "unused", parameters: { type: "object" } },
          ],
        },
      ],
      tool_config: {
        function_calling_config: {
          mode: "ANY",
          allowed_function_names: ["lookup"],
        },
      },
      generation_config: {
        max_output_tokens: 256,
        temperature: 0.3,
        top_p: 0.9,
        top_k: 32,
        stop_sequences: ["END"],
        response_mime_type: "application/json",
        response_json_schema: { type: "object", properties: { ok: { type: "boolean" } } },
      },
    },
    "gemini",
    { mode: "compat", onFieldDropped: (f) => dropped.push(f) },
  );
  expect(dropped).toEqual(["part.thoughtSignature"]);
  expect(decoded.maxTokens).toBe(256);
  expect(decoded.temperature).toBe(0.3);
  expect(decoded.topP).toBe(0.9);
  expect(decoded.topK).toBe(32);
  expect(decoded.stop).toEqual(["END"]);
  expect(decoded.tools.map((t) => t.name)).toEqual(["lookup"]);
  expect(decoded.toolChoice).toEqual({ name: "lookup" });
  expect(decoded.jsonSchema).toEqual({
    type: "object",
    properties: { ok: { type: "boolean" } },
  });

  const chat = encodeRequest(decoded, "chat", "gpt-5", { mode: "compat" });
  expect(chat.max_completion_tokens).toBe(256);
  expect(chat.response_format).toEqual({
    type: "json_schema",
    json_schema: {
      name: "response",
      schema: { type: "object", properties: { ok: { type: "boolean" } } },
    },
  });

  expect(() =>
    decodeRequest(
      {
        contents: [{ role: "user", parts: [{ text: "hi" }] }],
        generationConfig: { maxOutputTokens: 10 },
        generation_config: { max_output_tokens: 20 },
      },
      "gemini",
    ),
  ).toThrow("generationConfig");

  expect(() =>
    decodeRequest(
      {
        contents: [{ role: "user", parts: [{ text: "hi" }] }],
        generation_config: { maxOutputTokens: 10, max_output_tokens: 20 },
      },
      "gemini",
    ),
  ).toThrow("generationConfig.maxOutputTokens");

  const nullAliases = decodeRequest(
    {
      contents: [{ role: "user", parts: [{ text: "hi", thought_signature: null }] }],
      generationConfig: null,
      generation_config: {
        max_output_tokens: null,
        temperature: null,
        top_p: null,
        top_k: null,
        stop_sequences: null,
      },
      serviceTier: "STANDARD",
    },
    "gemini",
    { mode: "compat" },
  );
  expect(nullAliases.maxTokens).toBeUndefined();
  expect(nullAliases.topP).toBeUndefined();
  expect(nullAliases.topK).toBeUndefined();
  expect(nullAliases.droppedFields).toContain("serviceTier");

  const nullResponsesMax = decodeRequest(
    { input: "hi", max_output_tokens: null, top_p: null },
    "responses",
  );
  expect(nullResponsesMax.maxTokens).toBeUndefined();
  expect(nullResponsesMax.topP).toBeUndefined();
});

test("encodeRequest honors defaultMaxTokens for messages target only when request.maxTokens is unset", () => {
  const withoutMax = decodeRequest({ messages: [{ role: "user", content: "hi" }] }, "chat");
  expect(
    encodeRequest(withoutMax, "messages", "claude-sonnet", { defaultMaxTokens: 65536 }).max_tokens,
  ).toBe(65536);

  const withMax = decodeRequest(
    { messages: [{ role: "user", content: "hi" }], max_tokens: 512 },
    "chat",
  );
  expect(
    encodeRequest(withMax, "messages", "claude-sonnet", { defaultMaxTokens: 65536 }).max_tokens,
  ).toBe(512);
});
