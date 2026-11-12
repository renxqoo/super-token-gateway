import { optionalObject } from "@gateway/contracts";
import type { Operation } from "@gateway/kernel";
function textCharacters(value: unknown): number | undefined {
  if (typeof value !== "string") return undefined;
  let characters = 0;
  for (const _ of value) characters++;
  return characters;
}

function sumCharacters(
  values: unknown,
  measure: (value: unknown) => number | undefined,
): number | undefined {
  if (!Array.isArray(values)) return undefined;
  let total = 0;
  for (const value of values) {
    const characters = measure(value);
    if (characters === undefined) return undefined;
    total += characters;
  }
  return total;
}

function contentCharacters(value: unknown): number | undefined {
  if (typeof value === "string") return textCharacters(value);
  return sumCharacters(value, (entry) => {
    if (typeof entry === "string") return textCharacters(entry);
    const part = optionalObject(entry);
    switch (part.type) {
      case "text":
      case "input_text":
      case "output_text":
        return textCharacters(part.text);
      case "refusal":
        return textCharacters(part.refusal);
      case "tool_result":
      case "search_result":
        return contentCharacters(part.content ?? "");
      case "image":
      case "image_url":
      case "input_image":
      case "input_audio":
      case "input_file":
      case "file":
      case "document":
      case "tool_use":
      case "thinking":
      case "redacted_thinking":
        return 0;
      default:
        return undefined;
    }
  });
}

function geminiCharacters(value: unknown): number | undefined {
  return sumCharacters(optionalObject(value).parts, (entry) => {
    const part = optionalObject(entry);
    if (typeof part.text === "string") return textCharacters(part.text);
    if (
      part.inlineData ||
      part.inline_data ||
      part.fileData ||
      part.file_data ||
      part.functionCall ||
      part.function_call ||
      part.functionResponse ||
      part.function_response
    )
      return 0;
    return undefined;
  });
}

export function requestCharacters(
  operation: Operation,
  body: Record<string, unknown>,
): number | undefined {
  switch (operation) {
    case "chat":
    case "messages": {
      const system = operation === "messages" ? contentCharacters(body.system ?? "") : 0;
      const messages = sumCharacters(body.messages, (value) => {
        const message = optionalObject(value);
        if (typeof message.role !== "string") return undefined;
        return contentCharacters(message.content ?? "");
      });
      return system === undefined || messages === undefined ? undefined : system + messages;
    }
    case "responses": {
      const instructions = textCharacters(body.instructions ?? "");
      const rawInput = body.input ?? "";
      const input =
        textCharacters(rawInput) ??
        sumCharacters(rawInput, (entry) => {
          const item = optionalObject(entry);
          if (item.type === "function_call_output") return contentCharacters(item.output);
          if (["function_call", "item_reference", "reasoning"].includes(String(item.type)))
            return 0;
          return contentCharacters(item.content);
        });
      return instructions === undefined || input === undefined ? undefined : instructions + input;
    }
    case "gemini": {
      if (Object.hasOwn(body, "systemInstruction") && Object.hasOwn(body, "system_instruction"))
        return undefined;
      const instruction = body.systemInstruction ?? body.system_instruction;
      const system = instruction === undefined ? 0 : geminiCharacters(instruction);
      const contents = sumCharacters(body.contents, geminiCharacters);
      return system === undefined || contents === undefined ? undefined : system + contents;
    }
    case "completions":
    case "images":
    case "image_edits":
    case "videos":
      return textCharacters(body.prompt) ?? sumCharacters(body.prompt, textCharacters);
    case "speech":
    case "embeddings":
      return textCharacters(body.input) ?? sumCharacters(body.input, textCharacters);
    case "moderations":
      return contentCharacters(body.input);
    case "transcription":
    case "translation":
      return textCharacters(body.prompt ?? "");
    default:
      return undefined;
  }
}
