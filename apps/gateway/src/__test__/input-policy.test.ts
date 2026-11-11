import { expect, test } from "bun:test";
import { applyOutputBound, rejectUnmeteredTools } from "../input-policy.ts";

test("a foreign output bound field cannot shrink the reservation below the native upstream bound", () => {
  for (const [operation, body] of [
    ["chat", { max_tokens: 32768, max_output_tokens: 1 }],
    ["chat", { max_completion_tokens: 32768, generationConfig: { maxOutputTokens: 1 } }],
    ["responses", { max_tokens: 1 }],
    ["responses", { max_output_tokens: 32768, max_completion_tokens: 1 }],
    ["messages", { max_tokens: 32768, max_completion_tokens: 1 }],
    ["messages", { max_tokens: 32768, max_output_tokens: 1 }],
    ["completions", { max_tokens: 32768, max_output_tokens: 1 }],
    ["gemini", { generationConfig: { maxOutputTokens: 32768 }, max_tokens: 1 }],
  ] as const)
    expect(() => applyOutputBound({ ...body }, operation, 65536)).toThrow(
      expect.objectContaining({ status: 400, code: "invalid_request" }),
    );
});

test("each protocol reserves for its own native output bound", () => {
  expect(applyOutputBound({ max_tokens: 7 }, "chat", 100)).toBe(7);
  expect(applyOutputBound({ max_completion_tokens: 8 }, "chat", 100)).toBe(8);
  expect(applyOutputBound({ max_output_tokens: 9 }, "responses", 100)).toBe(9);
  expect(applyOutputBound({ max_tokens: 10 }, "messages", 100)).toBe(10);
  expect(applyOutputBound({ max_tokens: 11 }, "completions", 100)).toBe(11);
  expect(applyOutputBound({ generationConfig: { maxOutputTokens: 12 } }, "gemini", 100)).toBe(12);
  const omitted: Record<string, unknown> = {};
  expect(applyOutputBound(omitted, "responses", 100)).toBe(100);
  expect(omitted.max_output_tokens).toBe(100);
});

test("server-executed tools without a billing dimension are rejected before admission", () => {
  for (const [operation, body] of [
    ["chat", { tools: [{ type: "web_search" }] }],
    ["chat", { web_search_options: {} }],
    ["responses", { tools: [{ type: "web_search_preview" }] }],
    ["responses", { tools: [{ type: "code_interpreter", container: { type: "auto" } }] }],
    ["responses", { tools: [{ type: "file_search", vector_store_ids: ["vs"] }] }],
    ["messages", { tools: [{ type: "web_search_20250305", name: "web_search" }] }],
    ["messages", { tools: [{ type: "code_execution_20250522", name: "code_execution" }] }],
    ["gemini", { tools: [{ googleSearch: {} }] }],
    ["gemini", { tools: [{ functionDeclarations: [], codeExecution: {} }] }],
    ["gemini", { tools: [{ urlContext: {} }] }],
  ] as const)
    expect(() => rejectUnmeteredTools(body, operation)).toThrow(
      expect.objectContaining({ status: 422, code: "unmetered_tool" }),
    );
});

test("client-executed tools billed only through tokens remain accepted", () => {
  rejectUnmeteredTools({ tools: [{ type: "function", function: { name: "f" } }] }, "chat");
  rejectUnmeteredTools({ tools: [{ type: "function", name: "f" }] }, "responses");
  rejectUnmeteredTools({ tools: [{ type: "custom", name: "f" }] }, "responses");
  rejectUnmeteredTools({ tools: [{ name: "f", input_schema: {} }] }, "messages");
  rejectUnmeteredTools({ tools: [{ type: "custom", name: "f", input_schema: {} }] }, "messages");
  rejectUnmeteredTools({ tools: [{ type: "bash_20250124", name: "bash" }] }, "messages");
  rejectUnmeteredTools({ tools: [{ functionDeclarations: [{ name: "f" }] }] }, "gemini");
  rejectUnmeteredTools({ input: "text" }, "embeddings");
});

test("an explicit null output bound means unset instead of an invalid limit", () => {
  const chat: Record<string, unknown> = { max_completion_tokens: null };
  expect(applyOutputBound(chat, "chat", 100)).toBe(100);
  expect(chat.max_completion_tokens).toBe(100);
  const legacy: Record<string, unknown> = { max_tokens: null };
  expect(applyOutputBound(legacy, "chat", 100)).toBe(100);
  expect("max_tokens" in legacy).toBe(false);
  const responses: Record<string, unknown> = { max_output_tokens: null };
  expect(applyOutputBound(responses, "responses", 100)).toBe(100);
  expect(responses.max_output_tokens).toBe(100);
  const gemini: Record<string, unknown> = { generationConfig: { maxOutputTokens: null } };
  expect(applyOutputBound(gemini, "gemini", 100)).toBe(100);
  expect(gemini.generationConfig).toEqual({ maxOutputTokens: 100 });
});

test("client-executed tool variants and null tool lists are not mistaken for unmetered hosted tools", () => {
  rejectUnmeteredTools({ tools: null }, "chat");
  rejectUnmeteredTools({ tools: [{ type: "custom", custom: { name: "f" } }] }, "chat");
  for (const type of ["computer_use_preview", "local_shell", "shell", "apply_patch"])
    rejectUnmeteredTools({ tools: [{ type }] }, "responses");
  rejectUnmeteredTools({ tools: [{ type: "memory_20250818", name: "memory" }] }, "messages");
  rejectUnmeteredTools({ tools: [{ function_declarations: [{ name: "f" }] }] }, "gemini");
  rejectUnmeteredTools(
    { tools: [{ computerUse: { environment: "ENVIRONMENT_BROWSER" } }] },
    "gemini",
  );
});
