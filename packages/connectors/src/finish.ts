import { GatewayError, optionalObject } from "@gateway/kernel";
import type { Protocol } from "@gateway/kernel";
export type Finish = "stop" | "length" | "tool_calls" | "content_filter";
export function finishReason(protocol: Protocol, reason: unknown): Finish | undefined {
  if (reason === null || reason === undefined || reason === "") return undefined;
  const value = String(reason);
  if (protocol === "chat" && ["stop", "length", "tool_calls", "content_filter"].includes(value))
    return value as Finish;
  if (protocol === "messages") {
    if (["end_turn", "stop_sequence"].includes(value)) return "stop";
    if (["max_tokens", "model_context_window_exceeded"].includes(value)) return "length";
    if (value === "tool_use") return "tool_calls";
    if (value === "refusal") return "content_filter";
  }
  if (protocol === "gemini") {
    if (value === "STOP") return "stop";
    if (value === "MAX_TOKENS") return "length";
    if (
      [
        "SAFETY",
        "RECITATION",
        "BLOCKLIST",
        "PROHIBITED_CONTENT",
        "SPII",
        "IMAGE_SAFETY",
        "IMAGE_PROHIBITED_CONTENT",
      ].includes(value)
    )
      return "content_filter";
  }
  throw new GatewayError(
    502,
    "conversion_loss",
    "Provider termination reason cannot be represented",
  );
}

export function blockedPrompt(body: Record<string, unknown>): boolean {
  const reason = optionalObject(body.promptFeedback).blockReason;
  return typeof reason === "string" && reason.length > 0 && reason !== "BLOCK_REASON_UNSPECIFIED";
}
