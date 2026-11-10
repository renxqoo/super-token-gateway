import { GatewayError, optionalObject } from "@gateway/kernel";
import type { Operation } from "@gateway/kernel";

export function applyOutputBound(
  body: Record<string, unknown>,
  operation: Operation,
  modelLimit: number,
): number {
  const textOperation = ["chat", "responses", "messages", "gemini", "completions"].includes(
    operation,
  );
  if (!textOperation) return 0;
  const generation = optionalObject(body.generationConfig);
  const provided =
    body.max_completion_tokens ??
    body.max_output_tokens ??
    body.max_tokens ??
    generation.maxOutputTokens;
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
    else
      body[
        operation === "responses"
          ? "max_output_tokens"
          : operation === "chat"
            ? "max_completion_tokens"
            : "max_tokens"
      ] = bound;
  }
  if (
    operation === "chat" &&
    body.max_tokens !== undefined &&
    body.max_completion_tokens !== undefined
  )
    throw new GatewayError(400, "invalid_request", "Specify one output token bound");
  return bound * Number(n);
}
