import { GatewayError, object, optionalObject } from "@gateway/kernel";
import type { Operation } from "@gateway/kernel";

const nativeOutputBounds: Partial<Record<Operation, readonly string[]>> = {
  chat: ["max_completion_tokens", "max_tokens"],
  responses: ["max_output_tokens"],
  messages: ["max_tokens"],
  completions: ["max_tokens"],
  gemini: ["maxOutputTokens"],
};

export function applyOutputBound(
  body: Record<string, unknown>,
  operation: Operation,
  modelLimit: number,
): number {
  const native = nativeOutputBounds[operation];
  if (!native) return 0;
  for (const field of ["max_completion_tokens", "max_output_tokens", "max_tokens"])
    if (body[field] === null) delete body[field];
  let generation = optionalObject(body.generationConfig);
  if (generation.maxOutputTokens === null) {
    const { maxOutputTokens: _, ...rest } = generation;
    generation = rest;
    body.generationConfig = rest;
  }
  const fields: Record<string, unknown> = {
    max_completion_tokens: body.max_completion_tokens,
    max_output_tokens: body.max_output_tokens,
    max_tokens: body.max_tokens,
    maxOutputTokens: generation.maxOutputTokens,
  };
  const present = Object.keys(fields).filter((field) => fields[field] !== undefined);
  if (present.length > 1 || present.some((field) => !native.includes(field)))
    throw new GatewayError(
      400,
      "invalid_request",
      "Specify one output token bound native to this operation",
    );
  const provided = present[0] === undefined ? undefined : fields[present[0]];
  const n = operation === "gemini" ? (generation.candidateCount ?? 1) : (body.n ?? 1);
  if (!Number.isSafeInteger(n) || Number(n) < 1 || Number(n) > 8)
    throw new GatewayError(400, "invalid_request", "n must be an integer from 1 to 8");
  if (
    provided !== undefined &&
    (!Number.isSafeInteger(provided) || Number(provided) < 1 || Number(provided) > modelLimit)
  )
    throw new GatewayError(422, "output_bound", "Output token limit exceeds model policy");
  const bound = provided === undefined ? modelLimit : Number(provided);
  if (provided === undefined) {
    if (operation === "gemini") body.generationConfig = { ...generation, maxOutputTokens: bound };
    else body[native[0]!] = bound;
  }
  return bound * Number(n);
}

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

function meteredTool(value: unknown, operation: Operation): boolean {
  const tool = object(value);
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
      (!Array.isArray(body.tools) || !body.tools.every((tool) => meteredTool(tool, operation))))
  )
    throw new GatewayError(
      422,
      "unmetered_tool",
      "Server-executed tools without a billing dimension are not supported",
    );
}
