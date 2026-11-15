import { expect, test } from "bun:test";
import { rejectUnmeteredTools } from "../tool-policy.ts";

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
    ["chat", { tools: { type: "function" } }],
    ["chat", { tools: ["function"] }],
    ["embeddings", { tools: [{ type: "function" }] }],
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

test("client-executed tool variants and null tool lists are not mistaken for unmetered hosted tools", () => {
  rejectUnmeteredTools({ tools: null, web_search_options: null }, "chat");
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
