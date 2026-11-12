import { extractUsage } from "./usage.ts";
import { copyExact } from "./json.ts";
import { optionalObject, GatewayError } from "@gateway/contracts";
import type { Operation, Protocol, ProviderKind, Usage } from "@gateway/kernel";
import { blockedPrompt } from "./finish.ts";
import { decodeResponse, encodeResponse } from "./response.ts";

export function imageCount(data: unknown): number {
  const entries = Array.isArray(data) ? data : [data];
  let urls = 0;
  let encoded = 0;
  for (const entry of entries) {
    const item = optionalObject(entry);
    if (typeof item.url === "string" && item.url.length) urls++;
    if (typeof item.b64_json === "string" && item.b64_json.length) encoded++;
  }
  return Math.max(urls, encoded);
}

export function measureResponse(
  value: Record<string, unknown>,
  kind: ProviderKind,
  operation: Operation,
): Usage {
  let usage = extractUsage(value, kind);
  if (operation === "images" || operation === "image_edits") {
    const images = imageCount(value.data);
    if (images) usage = { ...usage, images: String(images) };
  }
  return { ...usage, requests: "1" };
}

export function validateCompletion(
  value: Record<string, unknown>,
  protocol: Operation,
  expectedChoices = 1,
): void {
  if (value.error)
    throw new GatewayError(502, "provider_failed", "Provider returned an error response");
  const invalidChat =
    protocol === "chat" &&
    (!Array.isArray(value.choices) ||
      value.choices.length !== expectedChoices ||
      value.choices.some((raw) => {
        const choice = optionalObject(raw);
        const message = optionalObject(choice.message);
        return (
          !Object.keys(message).length ||
          !["content", "tool_calls", "function_call", "audio", "refusal"].some((field) =>
            Object.hasOwn(message, field),
          ) ||
          (Object.hasOwn(message, "content") &&
            message.content !== null &&
            typeof message.content !== "string" &&
            !Array.isArray(message.content)) ||
          typeof choice.finish_reason !== "string" ||
          !choice.finish_reason
        );
      }));
  if (
    invalidChat ||
    (protocol === "responses" && !Array.isArray(value.output)) ||
    (protocol === "messages" && !Array.isArray(value.content)) ||
    (protocol === "gemini" && !Array.isArray(value.candidates) && !blockedPrompt(value))
  )
    throw new GatewayError(502, "invalid_upstream", "Provider returned an invalid response shape");
}

export function presentResponse(
  value: Record<string, unknown>,
  source: Operation,
  target: Operation,
  kind: ProviderKind,
  model: string,
): Record<string, unknown> {
  if (["chat", "responses", "messages", "gemini"].includes(target) && source !== target)
    return encodeResponse(
      decodeResponse(value, source as Protocol, kind),
      target as Protocol,
      model,
    );
  return typeof value.model === "string" ? copyExact(value, { ...value, model }) : value;
}
