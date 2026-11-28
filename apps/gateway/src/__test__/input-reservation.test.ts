import { expect, test } from "bun:test";
import {
  estimateTextChatInput,
  estimateTextGeminiInput,
  estimateTextInputForProtocol,
  estimateTextMessagesInput,
  estimateTextResponsesInput,
} from "../upstream-input-estimate.ts";

test("estimateTextChatInput bounds ASCII and UTF-8 bytes, honors pre-serialized body, and rejects unsupported fields", () => {
  const cjkPayload = {
    model: "gpt-4o",
    messages: [{ role: "user", content: "你好世界🚀" }],
  };
  const serialized = JSON.stringify(cjkPayload);
  expect(Buffer.byteLength(serialized)).toBeGreaterThan(serialized.length);
  expect(estimateTextChatInput(cjkPayload)).toBe(Buffer.byteLength(serialized) + 160);
  expect(estimateTextChatInput(cjkPayload, `${serialized}   `)).toBe(
    Buffer.byteLength(serialized) + 163,
  );
  expect(estimateTextChatInput({ ...cjkPayload, n: 2 })).toBeUndefined();
  expect(
    estimateTextChatInput({
      model: "gpt-4o",
      messages: [
        {
          role: "user",
          content: [{ type: "image_url", image_url: { url: "https://example.com/a.png" } }],
        },
      ],
    }),
  ).toBeUndefined();
});

test("estimateTextResponsesInput bounds text, tools, and function call outputs while rejecting multimodal or stateful fields", () => {
  expect(estimateTextResponsesInput({ model: "m", input: "ab" })).toBe(186);
  expect(estimateTextResponsesInput({ model: "m", input: "你好" })).toBe(190);

  const stringPayload = {
    model: "gpt-6-2-sol",
    instructions: "Be concise",
    input: "你好世界",
    stream: true,
    store: false,
    reasoning: { effort: "medium", summary: "auto" },
  };
  expect(estimateTextResponsesInput(stringPayload)).toBe(311);

  const structuredPayload = {
    model: "gpt-6-2-sol",
    input: [
      { type: "message", role: "user", content: [{ type: "input_text", text: "hi" }] },
      { type: "function_call", call_id: "c1", name: "lookup", arguments: '{"q":"a"}' },
      {
        type: "function_call_output",
        call_id: "c1",
        output: [{ type: "output_text", text: "ok" }],
      },
    ],
    tools: [{ type: "function", name: "lookup", parameters: { type: "object" } }],
  };
  const structuredEstimate = estimateTextResponsesInput(structuredPayload)!;
  expect(structuredEstimate).toBeGreaterThan(500);
  expect(structuredEstimate).toBeLessThan(700);

  expect(
    estimateTextResponsesInput({
      ...stringPayload,
      previous_response_id: "resp_123",
    }),
  ).toBeUndefined();
  expect(
    estimateTextResponsesInput({
      model: "gpt-6-2-sol",
      input: [
        {
          type: "message",
          role: "user",
          content: [{ type: "input_image", image_url: "https://example.com/a.png" }],
        },
      ],
    }),
  ).toBeUndefined();
  expect(
    estimateTextResponsesInput({
      model: "gpt-6-2-sol",
      input: [
        {
          type: "function_call_output",
          call_id: "c1",
          output: [{ type: "input_image", image_url: "https://example.com/a.png" }],
        },
      ],
    }),
  ).toBeUndefined();
  expect(
    estimateTextResponsesInput({
      model: "gpt-6-2-sol",
      input: "hi",
      tools: [{ type: "web_search_preview" }],
    }),
  ).toBeUndefined();
  expect(
    estimateTextResponsesInput({
      model: "gpt-6-2-sol",
      input: [{ type: "reasoning", encrypted_content: "gAAAAAB..." }],
    }),
  ).toBeUndefined();
});

test("estimateTextMessagesInput accounts for Anthropic tool/thinking/output_config prompt overhead and rejects media or server tools", () => {
  const plainToolPayload = {
    model: "claude-sonnet-4-5",
    max_tokens: 1024,
    messages: [{ role: "user", content: "hi" }],
    tools: [{ name: "t", input_schema: { type: "object" } }],
  };
  const minimalToolEstimate = estimateTextMessagesInput(plainToolPayload)!;
  expect(minimalToolEstimate).toBeGreaterThan(1000);
  expect(minimalToolEstimate).toBe(1363);

  const thinkingAndStructured = estimateTextMessagesInput({
    ...plainToolPayload,
    thinking: { type: "enabled", budget_tokens: 1024 },
    output_config: { effort: "high" },
  })!;
  expect(thinkingAndStructured - minimalToolEstimate).toBeGreaterThan(768);

  const fullPayload = {
    model: "claude-sonnet-4-5",
    system: [{ type: "text", text: "你好", cache_control: { type: "ephemeral" } }],
    max_tokens: 1024,
    messages: [
      {
        role: "user",
        content: [{ type: "text", text: "Explain caching", cache_control: { type: "ephemeral" } }],
      },
      {
        role: "assistant",
        content: [
          { type: "thinking", thinking: "Step 1", signature: "sig123" },
          { type: "tool_use", id: "tu_1", name: "get_price", input: { sku: "a" } },
        ],
      },
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            tool_use_id: "tu_1",
            content: [{ type: "text", text: "42" }],
          },
        ],
      },
    ],
    tools: [{ name: "get_price", input_schema: { type: "object" } }],
  };
  const fullEstimate = estimateTextMessagesInput(fullPayload)!;
  expect(fullEstimate).toBeGreaterThan(1600);
  expect(fullEstimate).toBeLessThan(2100);

  expect(
    estimateTextMessagesInput({
      model: "claude-sonnet-4-5",
      max_tokens: 1024,
      messages: [
        {
          role: "user",
          content: [
            {
              type: "image",
              source: { type: "url", url: "https://example.com/a.png" },
            },
          ],
        },
      ],
    }),
  ).toBeUndefined();

  expect(
    estimateTextMessagesInput({
      model: "claude-sonnet-4-5",
      max_tokens: 1024,
      messages: [
        {
          role: "user",
          content: [
            {
              type: "tool_result",
              tool_use_id: "tu_1",
              content: [
                { type: "image", source: { type: "url", url: "https://example.com/a.png" } },
              ],
            },
          ],
        },
      ],
    }),
  ).toBeUndefined();

  expect(
    estimateTextMessagesInput({
      model: "claude-sonnet-4-5",
      max_tokens: 1024,
      messages: [{ role: "user", content: "hi" }],
      tools: [{ type: "web_search_20250305", name: "web_search" }],
    }),
  ).toBeUndefined();

  expect(
    estimateTextMessagesInput({
      model: "claude-sonnet-4-5",
      max_tokens: 1024,
      messages: [
        {
          role: "assistant",
          content: [{ type: "redacted_thinking", data: "opaque" }],
        },
      ],
    }),
  ).toBeUndefined();
});

test("estimateTextGeminiInput bounds camelCase and snake_case text payloads and rejects media, hosted tools, and multi-candidate configs", () => {
  const plainCamel = {
    contents: [{ role: "user", parts: [{ text: "你好世界🚀" }] }],
    systemInstruction: { parts: [{ text: "Be concise" }] },
    generationConfig: { maxOutputTokens: 128, temperature: 0.2 },
  };
  const serialized = JSON.stringify(plainCamel);
  expect(estimateTextGeminiInput(plainCamel)).toBe(Buffer.byteLength(serialized) + 128 + 32);
  expect(estimateTextGeminiInput(plainCamel, `${serialized}  `)).toBe(
    Buffer.byteLength(serialized) + 2 + 128 + 32,
  );

  const snakeWithToolsAndSchema = {
    system_instruction: { parts: [{ text: "Use tools" }] },
    contents: [
      { role: "user", parts: [{ text: "lookup" }] },
      {
        role: "model",
        parts: [
          { thought: true, text: "thinking", thought_signature: "sig" },
          { function_call: { name: "lookup", args: { q: "x" } } },
        ],
      },
      {
        role: "user",
        parts: [{ function_response: { name: "lookup", response: { ok: true } } }],
      },
    ],
    tools: [{ function_declarations: [{ name: "lookup", parameters: { type: "object" } }] }],
    generation_config: {
      max_output_tokens: 128,
      response_json_schema: { type: "object" },
    },
  };
  const snakeSerialized = JSON.stringify(snakeWithToolsAndSchema);
  expect(estimateTextGeminiInput(snakeWithToolsAndSchema)).toBe(
    Buffer.byteLength(snakeSerialized) + 128 + 32 * 4 + 512 + 256,
  );

  expect(
    estimateTextGeminiInput({
      contents: [
        { role: "user", parts: [{ inlineData: { mimeType: "image/png", data: "AAAA" } }] },
      ],
    }),
  ).toBeUndefined();
  expect(
    estimateTextGeminiInput({
      contents: [
        {
          role: "user",
          parts: [{ file_data: { mime_type: "image/png", file_uri: "gs://bucket/a.png" } }],
        },
      ],
    }),
  ).toBeUndefined();
  expect(
    estimateTextGeminiInput({
      cachedContent: "cachedContents/123",
      contents: [{ role: "user", parts: [{ text: "hi" }] }],
    }),
  ).toBeUndefined();
  expect(
    estimateTextGeminiInput({
      contents: [{ role: "user", parts: [{ text: "hi" }] }],
      tools: [{ googleSearch: {} }],
    }),
  ).toBeUndefined();
  expect(
    estimateTextGeminiInput({
      contents: [{ role: "user", parts: [{ text: "hi" }] }],
      generation_config: { candidate_count: 2 },
    }),
  ).toBeUndefined();
  expect(
    estimateTextGeminiInput({
      contents: [
        {
          role: "user",
          parts: [
            {
              functionResponse: {
                name: "screenshot",
                response: { ok: true },
                parts: [{ inlineData: { mimeType: "image/png", data: "AAAA" } }],
              },
            },
          ],
        },
      ],
    }),
  ).toBeUndefined();
  expect(
    estimateTextGeminiInput({
      contents: [{ role: "user", parts: [{ text: "hi" }] }],
      tools: [{ functionDeclarations: [], function_declarations: [{ name: "f" }] }],
    }),
  ).toBeUndefined();
});

test("estimateTextInputForProtocol dispatches only matching protocol and usage dialect pairs", () => {
  const chatPayload = { model: "m", messages: [{ role: "user", content: "hi" }] };
  const chatBody = JSON.stringify(chatPayload);
  expect(estimateTextInputForProtocol("chat", "openai", chatPayload, chatBody)).toBe(
    estimateTextChatInput(chatPayload, chatBody),
  );
  expect(estimateTextInputForProtocol("chat", "anthropic", chatPayload, chatBody)).toBeUndefined();
  expect(estimateTextInputForProtocol("gemini", "gemini", chatPayload, chatBody)).toBeUndefined();
  expect(estimateTextInputForProtocol(null, "openai", chatPayload, chatBody)).toBeUndefined();

  const respPayload = { model: "m", input: "hi" };
  const respBody = JSON.stringify(respPayload);
  expect(estimateTextInputForProtocol("responses", "openai", respPayload, respBody)).toBe(
    estimateTextResponsesInput(respPayload, respBody),
  );

  const msgPayload = { model: "m", max_tokens: 10, messages: [{ role: "user", content: "hi" }] };
  const msgBody = JSON.stringify(msgPayload);
  expect(estimateTextInputForProtocol("messages", "anthropic", msgPayload, msgBody)).toBe(
    estimateTextMessagesInput(msgPayload, msgBody),
  );
  expect(estimateTextInputForProtocol("messages", "openai", msgPayload, msgBody)).toBeUndefined();

  const geminiPayload = { contents: [{ role: "user", parts: [{ text: "hi" }] }] };
  const geminiBody = JSON.stringify(geminiPayload);
  expect(estimateTextInputForProtocol("gemini", "gemini", geminiPayload, geminiBody)).toBe(
    estimateTextGeminiInput(geminiPayload, geminiBody),
  );
  expect(
    estimateTextInputForProtocol("gemini", "openai", geminiPayload, geminiBody),
  ).toBeUndefined();
});
