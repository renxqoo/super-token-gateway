import { GatewayError } from "@gateway/contracts";
import type { Operation } from "@gateway/kernel";

const clientMessageTools = /^(?:custom|(?:bash|text_editor|computer|memory)_\d{8})$/;
const clientResponseTools = new Set([
  "function",
  "custom",
  "computer_use_preview",
  "local_shell",
  "shell",
  "apply_patch",
]);
const clientGeminiTools = new Set([
  "functionDeclarations",
  "function_declarations",
  "computerUse",
  "computer_use",
]);

function clientExecuted(value: unknown, operation: Operation): boolean {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const tool = value as Record<string, unknown>;
  if (operation === "gemini") return Object.keys(tool).every((key) => clientGeminiTools.has(key));
  if (operation === "chat") return tool.type === "function" || tool.type === "custom";
  if (operation === "responses") return clientResponseTools.has(String(tool.type));
  if (operation === "messages")
    return tool.type === undefined || clientMessageTools.test(String(tool.type));
  return false;
}

export function rejectUnmeteredTools(body: Record<string, unknown>, operation: Operation): void {
  if (
    (body.web_search_options !== undefined && body.web_search_options !== null) ||
    (body.tools !== undefined &&
      body.tools !== null &&
      (!Array.isArray(body.tools) || !body.tools.every((tool) => clientExecuted(tool, operation))))
  )
    throw new GatewayError(
      422,
      "unmetered_tool",
      "Server-executed tools without a billing dimension are not supported",
    );
}
