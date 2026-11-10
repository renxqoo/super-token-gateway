import {
  GatewayError,
  extractUsage,
  providerObject as object,
  optionalObject,
  text,
  stringifyExact,
  parseProviderJson,
} from "@gateway/kernel";
import { finishReason, blockedPrompt } from "./finish.ts";
import type { Protocol, ProviderKind, Usage } from "@gateway/kernel";
import type { Part } from "./request.ts";

export interface Completion {
  id: string;
  parts: Part[];
  finish: "stop" | "length" | "tool_calls" | "content_filter";
  usage: Usage | undefined;
}

export function decodeResponse(value: unknown, source: Protocol, kind: ProviderKind): Completion {
  const body = object(value);
  const result: Completion = {
    id: text(body.id, `gen_${crypto.randomUUID()}`),
    parts: [],
    finish: "stop",
    usage: extractUsage(body, kind),
  };
  const addText = (value: unknown) => {
    if (typeof value === "string") result.parts.push({ type: "text", text: value });
  };
  const addCall = (id: unknown, name: unknown, args: unknown) =>
    result.parts.push({
      type: "call",
      id: text(id, `call_${result.parts.length}`),
      name: text(name),
      arguments: typeof args === "string" ? args : stringifyExact(args ?? {}),
    });
  if (source === "chat") {
    if (!Array.isArray(body.choices) || body.choices.length !== 1)
      throw new GatewayError(
        502,
        "conversion_loss",
        "Conversion requires exactly one completion choice",
      );
    const choice = object(body.choices[0]);
    const message = object(choice.message);
    for (const key of ["refusal", "reasoning_content", "audio"])
      if (message[key])
        throw new GatewayError(502, "conversion_loss", `Conversion cannot preserve ${key}`);
    if (typeof message.content === "string") addText(message.content);
    else if (Array.isArray(message.content))
      for (const raw of message.content) {
        const part = object(raw);
        if (part.type !== "text")
          throw new GatewayError(502, "conversion_loss", "Unsupported response part");
        addText(part.text);
      }
    if (Array.isArray(message.tool_calls))
      for (const raw of message.tool_calls) {
        const call = object(raw);
        const fn = object(call.function);
        addCall(call.id, fn.name, fn.arguments);
      }
    result.finish = finishReason("chat", choice.finish_reason) ?? result.finish;
    if (choice.finish_reason === "length") result.finish = "length";
    if (choice.finish_reason === "content_filter") result.finish = "content_filter";
    if (choice.finish_reason === "tool_calls") result.finish = "tool_calls";
  } else if (source === "responses") {
    if (body.status === "failed" || body.error)
      throw new GatewayError(502, "provider_failed", "Provider response failed");
    if (Array.isArray(body.output))
      for (const raw of body.output) {
        const item = object(raw);
        if (item.type === "function_call") addCall(item.call_id, item.name, item.arguments);
        else if (item.type === "message" && Array.isArray(item.content))
          for (const rawPart of item.content) {
            const part = object(rawPart);
            if (part.type !== "output_text")
              throw new GatewayError(502, "conversion_loss", "Unsupported Responses output part");
            addText(part.text);
          }
        else throw new GatewayError(502, "conversion_loss", "Unsupported Responses output item");
      }
    if (body.status === "incomplete") result.finish = "length";
  } else if (source === "messages") {
    if (!Array.isArray(body.content))
      throw new GatewayError(502, "invalid_upstream", "Missing message content");
    for (const raw of body.content) {
      const part = object(raw);
      if (part.type === "text") addText(part.text);
      else if (part.type === "tool_use") addCall(part.id, part.name, part.input);
      else throw new GatewayError(502, "conversion_loss", "Unsupported Anthropic response block");
    }
    result.finish = finishReason("messages", body.stop_reason) ?? result.finish;
    if (body.stop_reason === "max_tokens") result.finish = "length";
    if (body.stop_reason === "tool_use") result.finish = "tool_calls";
    if (body.stop_reason === "refusal") result.finish = "content_filter";
  } else {
    if (blockedPrompt(body)) {
      result.finish = "content_filter";
      return result;
    }
    if (!Array.isArray(body.candidates) || body.candidates.length !== 1)
      throw new GatewayError(502, "conversion_loss", "Conversion requires one Gemini candidate");
    const candidate = object(body.candidates[0]);
    const content = optionalObject(candidate.content);
    if (Array.isArray(content.parts))
      for (const raw of content.parts) {
        const part = object(raw);
        if (part.thought || part.thoughtSignature)
          throw new GatewayError(
            502,
            "conversion_loss",
            "Cannot preserve Gemini thought/signature",
          );
        if (typeof part.text === "string") addText(part.text);
        else if (part.functionCall) {
          const call = object(part.functionCall);
          addCall(call.id, call.name, call.args);
        } else throw new GatewayError(502, "conversion_loss", "Unsupported Gemini response part");
      }
    result.finish = finishReason("gemini", candidate.finishReason) ?? result.finish;
    if (candidate.finishReason === "MAX_TOKENS") result.finish = "length";
    if (
      ["SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT"].includes(
        text(candidate.finishReason),
      )
    )
      result.finish = "content_filter";
  }
  if (result.parts.some((p) => p.type === "call") && result.finish === "stop")
    result.finish = "tool_calls";
  return result;
}

export function publicUsage(usage: Usage | undefined, target: Protocol): Record<string, unknown> {
  if (!usage) return {};
  const value = (key: keyof Usage) => BigInt(usage[key] ?? "0");
  const number = (quantity: bigint): unknown => {
    if (quantity < 0n)
      throw new GatewayError(502, "invalid_upstream_usage", "Usage subtraction is negative");
    return quantity <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(quantity)
      : (JSON as JSON & { rawJSON: (source: string) => unknown }).rawJSON(quantity.toString());
  };
  const input = value("input_tokens"),
    output = value("output_tokens"),
    cached = value("cached_input_tokens"),
    write = value("cache_write_tokens"),
    reasoning = value("reasoning_tokens"),
    totalInput = input + cached + write;
  if (target === "messages")
    return {
      input_tokens: number(input),
      output_tokens: number(output),
      cache_read_input_tokens: number(cached),
      cache_creation_input_tokens: number(write),
    };
  if (target === "gemini")
    return {
      promptTokenCount: number(totalInput),
      candidatesTokenCount: number(output - reasoning),
      thoughtsTokenCount: number(reasoning),
      cachedContentTokenCount: number(cached),
      totalTokenCount: number(totalInput + output),
    };
  if (target === "responses")
    return {
      input_tokens: number(totalInput),
      output_tokens: number(output),
      total_tokens: number(totalInput + output),
      input_tokens_details: { cached_tokens: number(cached) },
      output_tokens_details: { reasoning_tokens: number(reasoning) },
    };
  return {
    prompt_tokens: number(totalInput),
    completion_tokens: number(output),
    total_tokens: number(totalInput + output),
    prompt_tokens_details: { cached_tokens: number(cached) },
    completion_tokens_details: { reasoning_tokens: number(reasoning) },
  };
}

export function encodeResponse(
  result: Completion,
  target: Protocol,
  model: string,
): Record<string, unknown> {
  const parts = result.parts;
  const calls = parts.flatMap((p) =>
    p.type === "call"
      ? [{ id: p.id, type: "function", function: { name: p.name, arguments: p.arguments } }]
      : [],
  );
  const content = parts.flatMap((p) => (p.type === "text" ? [p.text] : [])).join("");
  const usage = publicUsage(result.usage, target);
  if (target === "chat")
    return {
      id: result.id,
      object: "chat.completion",
      created: Math.floor(Date.now() / 1000),
      model,
      choices: [
        {
          index: 0,
          message: {
            role: "assistant",
            content: content || null,
            ...(calls.length ? { tool_calls: calls } : {}),
          },
          finish_reason: result.finish,
        },
      ],
      ...(result.usage ? { usage } : {}),
    };
  if (target === "responses")
    return {
      id: result.id,
      object: "response",
      created_at: Math.floor(Date.now() / 1000),
      model,
      status: ["length", "content_filter"].includes(result.finish) ? "incomplete" : "completed",
      incomplete_details:
        result.finish === "content_filter"
          ? { reason: "content_filter" }
          : result.finish === "length"
            ? { reason: "max_output_tokens" }
            : null,
      output: parts.map((part, index) =>
        part.type === "call"
          ? {
              id: `fc_${index}`,
              type: "function_call",
              call_id: part.id,
              name: part.name,
              arguments: part.arguments,
              status: "completed",
            }
          : {
              id: `msg_${index}`,
              type: "message",
              role: "assistant",
              status: "completed",
              content: [
                {
                  type: "output_text",
                  text: part.type === "text" ? part.text : "",
                  annotations: [],
                },
              ],
            },
      ),
      ...(result.usage ? { usage } : {}),
    };
  if (target === "messages")
    return {
      id: result.id,
      type: "message",
      role: "assistant",
      model,
      content: parts.map((part) =>
        part.type === "call"
          ? {
              type: "tool_use",
              id: part.id,
              name: part.name,
              input: parseProviderJson(part.arguments),
            }
          : { type: "text", text: part.type === "text" ? part.text : "" },
      ),
      stop_reason:
        result.finish === "content_filter"
          ? "refusal"
          : result.finish === "length"
            ? "max_tokens"
            : calls.length
              ? "tool_use"
              : "end_turn",
      stop_sequence: null,
      ...(result.usage ? { usage } : {}),
    };
  return {
    candidates: [
      {
        index: 0,
        content: {
          role: "model",
          parts: parts.map((part) =>
            part.type === "call"
              ? {
                  functionCall: {
                    id: part.id,
                    name: part.name,
                    args: parseProviderJson(part.arguments),
                  },
                }
              : { text: part.type === "text" ? part.text : "" },
          ),
        },
        finishReason:
          result.finish === "length"
            ? "MAX_TOKENS"
            : result.finish === "content_filter"
              ? "SAFETY"
              : "STOP",
      },
    ],
    ...(result.usage ? { usageMetadata: usage } : {}),
  };
}
