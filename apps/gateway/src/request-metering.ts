import { GatewayError, optionalObject } from "@gateway/contracts";
import type { Operation } from "@gateway/kernel";
import { numberLiteral } from "@gateway/protocol";

const clientMessageTools = /^(?:custom|(?:bash|text_editor|computer|memory)_\d{8})$/;
const clientResponseTools = new Set([
  "function",
  "custom",
  "computer_use_preview",
  "local_shell",
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
  if (operation === "responses") {
    if (tool.type === "shell") {
      return optionalObject(tool.environment).type === "local";
    }
    return clientResponseTools.has(String(tool.type));
  }
  if (operation === "messages") {
    return tool.type === undefined || clientMessageTools.test(String(tool.type));
  }
  return false;
}

function hasValue(value: unknown): boolean {
  return value !== undefined && value !== null;
}

function requestsMultipleCandidates(body: Record<string, unknown>, key: string): boolean {
  return hasValue(body[key]) && (body[key] !== 1 || numberLiteral(body, key) !== undefined);
}

export class MeteredRequestError extends GatewayError {
  constructor(code: "billing_ceiling" | "unmetered_context" | "unmetered_tool", message: string) {
    super(422, code, message);
  }
}

export function validateMeteredRequest(body: Record<string, unknown>, operation: Operation): void {
  if (
    ((operation === "chat" || operation === "completions") &&
      requestsMultipleCandidates(body, "n")) ||
    (operation === "completions" &&
      (requestsMultipleCandidates(body, "best_of") ||
        (Array.isArray(body.prompt) &&
          body.prompt.length > 1 &&
          body.prompt.some((prompt) => typeof prompt !== "number")))) ||
    (operation === "gemini" &&
      [body.generationConfig, body.generation_config].some((value) => {
        const config = optionalObject(value);
        return (
          requestsMultipleCandidates(config, "candidateCount") ||
          requestsMultipleCandidates(config, "candidate_count")
        );
      }))
  ) {
    throw new MeteredRequestError("billing_ceiling", "Only one generation candidate is supported");
  }

  if (
    (operation === "responses" &&
      (hasValue(body.previous_response_id) ||
        hasValue(body.conversation) ||
        hasValue(body.prompt) ||
        (Array.isArray(body.input) &&
          body.input.some((item) => optionalObject(item).type === "item_reference")))) ||
    (operation === "gemini" && (hasValue(body.cachedContent) || hasValue(body.cached_content))) ||
    (operation === "messages" && hasValue(body.container))
  ) {
    throw new MeteredRequestError(
      "unmetered_context",
      "Server-stored context without a verifiable reservation is not supported",
    );
  }

  if (
    (body.web_search_options !== undefined && body.web_search_options !== null) ||
    (body.tools !== undefined &&
      body.tools !== null &&
      (!Array.isArray(body.tools) || !body.tools.every((tool) => clientExecuted(tool, operation))))
  ) {
    throw new MeteredRequestError(
      "unmetered_tool",
      "Server-executed tools without a billing dimension are not supported",
    );
  }
}
