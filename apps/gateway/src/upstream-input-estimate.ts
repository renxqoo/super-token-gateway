import { optionalObject } from "@gateway/contracts";
import type { Protocol } from "@gateway/kernel";
import { stringifyExact } from "@gateway/protocol";

const textChatFields = new Set([
  "model",
  "messages",
  "stream",
  "stream_options",
  "max_tokens",
  "max_completion_tokens",
  "temperature",
  "top_p",
  "stop",
  "n",
  "frequency_penalty",
  "presence_penalty",
  "seed",
  "tools",
  "tool_choice",
  "parallel_tool_calls",
  "reasoning_effort",
  "reasoning_split",
  "response_format",
  "user",
]);

const textMessageFields = new Set([
  "role",
  "content",
  "name",
  "tool_calls",
  "tool_call_id",
  "function_call",
  "reasoning_content",
]);

const textResponsesFields = new Set([
  "model",
  "input",
  "instructions",
  "stream",
  "store",
  "max_output_tokens",
  "temperature",
  "top_p",
  "stop",
  "tools",
  "tool_choice",
  "parallel_tool_calls",
  "reasoning",
  "text",
  "truncation",
  "metadata",
  "user",
  "prompt_cache_key",
  "service_tier",
  "include",
]);

const textResponsesMessageFields = new Set(["type", "role", "content", "id", "status"]);
const textResponsesFunctionCallFields = new Set([
  "type",
  "id",
  "call_id",
  "name",
  "arguments",
  "status",
]);
const textResponsesFunctionOutputFields = new Set(["type", "id", "call_id", "output", "status"]);

const textMessagesTopFields = new Set([
  "model",
  "messages",
  "system",
  "stream",
  "max_tokens",
  "temperature",
  "top_p",
  "top_k",
  "stop_sequences",
  "tools",
  "tool_choice",
  "thinking",
  "output_config",
  "metadata",
  "service_tier",
  "cache_control",
]);

const textAnthropicMessageFields = new Set(["role", "content"]);
const textAnthropicTextBlockFields = new Set(["type", "text", "cache_control"]);
const textAnthropicThinkingBlockFields = new Set(["type", "thinking", "signature"]);
const textAnthropicToolUseFields = new Set(["type", "id", "name", "input", "cache_control"]);
const textAnthropicToolResultFields = new Set([
  "type",
  "tool_use_id",
  "content",
  "is_error",
  "cache_control",
]);

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOnlyFields(record: Record<string, unknown>, allowed: ReadonlySet<string>): boolean {
  for (const key in record) {
    if (!allowed.has(key)) return false;
  }
  return true;
}

export function estimateTextChatInput(
  payload: Record<string, unknown>,
  body?: string,
): number | undefined {
  if (!Array.isArray(payload.messages) || (payload.n !== undefined && payload.n !== 1)) {
    return undefined;
  }
  if (!hasOnlyFields(payload, textChatFields)) return undefined;
  const tools = payload.tools;
  let toolsLen = 0;
  if (tools !== undefined) {
    if (!Array.isArray(tools)) return undefined;
    toolsLen = tools.length;
    for (let i = 0; i < toolsLen; i++) {
      if (optionalObject(tools[i]).type !== "function") return undefined;
    }
  }
  const messages = payload.messages;
  for (let i = 0; i < messages.length; i++) {
    const rawMsg = messages[i];
    if (!isPlainRecord(rawMsg)) return undefined;
    if (typeof rawMsg.role !== "string" || !hasOnlyFields(rawMsg, textMessageFields)) {
      return undefined;
    }
    const content = rawMsg.content;
    if (content === undefined || content === null || typeof content === "string") continue;
    if (!Array.isArray(content)) return undefined;
    for (let j = 0; j < content.length; j++) {
      const rawPart = content[j];
      if (!isPlainRecord(rawPart) || rawPart.type !== "text" || typeof rawPart.text !== "string") {
        return undefined;
      }
      let count = 0;
      for (const _ in rawPart) {
        if (++count > 2) return undefined;
      }
      if (count !== 2) return undefined;
    }
  }
  return (
    Buffer.byteLength(body ?? stringifyExact(payload)) + 128 + 32 * (messages.length + toolsLen)
  );
}

function isResponsesTextPart(rawPart: unknown): boolean {
  if (!isPlainRecord(rawPart)) return false;
  if (
    (rawPart.type !== "input_text" && rawPart.type !== "output_text" && rawPart.type !== "text") ||
    typeof rawPart.text !== "string"
  ) {
    return false;
  }
  let count = 0;
  for (const _ in rawPart) {
    if (++count > 2) return false;
  }
  return count === 2;
}

export function estimateTextResponsesInput(
  payload: Record<string, unknown>,
  body?: string,
): number | undefined {
  if (!hasOnlyFields(payload, textResponsesFields)) return undefined;
  if (payload.instructions !== undefined && typeof payload.instructions !== "string") {
    return undefined;
  }
  const tools = payload.tools;
  let toolsLen = 0;
  if (tools !== undefined) {
    if (!Array.isArray(tools)) return undefined;
    toolsLen = tools.length;
    for (let i = 0; i < toolsLen; i++) {
      if (optionalObject(tools[i]).type !== "function") return undefined;
    }
  }
  const input = payload.input;
  let itemCount = 1;
  if (typeof input === "string") {
    itemCount = 1;
  } else if (Array.isArray(input)) {
    itemCount = input.length;
    for (let i = 0; i < input.length; i++) {
      const item = input[i];
      if (!isPlainRecord(item)) return undefined;
      const itemType = item.type;
      if (itemType === undefined || itemType === "message") {
        if (!hasOnlyFields(item, textResponsesMessageFields) || typeof item.role !== "string") {
          return undefined;
        }
        const content = item.content;
        if (content === undefined || content === null || typeof content === "string") continue;
        if (!Array.isArray(content)) return undefined;
        for (let j = 0; j < content.length; j++) {
          if (!isResponsesTextPart(content[j])) return undefined;
        }
      } else if (itemType === "function_call") {
        if (
          !hasOnlyFields(item, textResponsesFunctionCallFields) ||
          typeof item.call_id !== "string" ||
          typeof item.name !== "string" ||
          typeof item.arguments !== "string"
        ) {
          return undefined;
        }
      } else if (itemType === "function_call_output") {
        if (
          !hasOnlyFields(item, textResponsesFunctionOutputFields) ||
          typeof item.call_id !== "string"
        ) {
          return undefined;
        }
        const output = item.output;
        if (typeof output === "string") continue;
        if (!Array.isArray(output)) return undefined;
        for (let j = 0; j < output.length; j++) {
          if (!isResponsesTextPart(output[j])) return undefined;
        }
      } else {
        return undefined;
      }
    }
  } else {
    return undefined;
  }
  return Buffer.byteLength(body ?? stringifyExact(payload)) + 128 + 32 * (itemCount + toolsLen);
}

function isAnthropicTextBlock(rawBlock: unknown): boolean {
  if (!isPlainRecord(rawBlock)) return false;
  if (rawBlock.type !== "text" || typeof rawBlock.text !== "string") return false;
  if (!hasOnlyFields(rawBlock, textAnthropicTextBlockFields)) return false;
  if (rawBlock.cache_control !== undefined && !isPlainRecord(rawBlock.cache_control)) return false;
  return true;
}

export function estimateTextMessagesInput(
  payload: Record<string, unknown>,
  body?: string,
): number | undefined {
  if (!Array.isArray(payload.messages) || !hasOnlyFields(payload, textMessagesTopFields)) {
    return undefined;
  }
  const system = payload.system;
  if (system !== undefined && typeof system !== "string") {
    if (!Array.isArray(system)) return undefined;
    for (let i = 0; i < system.length; i++) {
      if (!isAnthropicTextBlock(system[i])) return undefined;
    }
  }
  const tools = payload.tools;
  let toolsLen = 0;
  if (tools !== undefined) {
    if (!Array.isArray(tools)) return undefined;
    toolsLen = tools.length;
    for (let i = 0; i < toolsLen; i++) {
      const tool = tools[i];
      if (!isPlainRecord(tool)) return undefined;
      if (tool.type !== undefined && tool.type !== "custom") return undefined;
      if (typeof tool.name !== "string") return undefined;
    }
  }
  const messages = payload.messages;
  for (let i = 0; i < messages.length; i++) {
    const message = messages[i];
    if (
      !isPlainRecord(message) ||
      !hasOnlyFields(message, textAnthropicMessageFields) ||
      typeof message.role !== "string"
    ) {
      return undefined;
    }
    const content = message.content;
    if (typeof content === "string") continue;
    if (!Array.isArray(content)) return undefined;
    for (let j = 0; j < content.length; j++) {
      const block = content[j];
      if (!isPlainRecord(block) || typeof block.type !== "string") return undefined;
      if (block.type === "text") {
        if (!isAnthropicTextBlock(block)) return undefined;
      } else if (block.type === "thinking") {
        if (
          !hasOnlyFields(block, textAnthropicThinkingBlockFields) ||
          typeof block.thinking !== "string" ||
          (block.signature !== undefined && typeof block.signature !== "string")
        ) {
          return undefined;
        }
      } else if (block.type === "tool_use") {
        if (
          !hasOnlyFields(block, textAnthropicToolUseFields) ||
          typeof block.id !== "string" ||
          typeof block.name !== "string" ||
          (block.cache_control !== undefined && !isPlainRecord(block.cache_control))
        ) {
          return undefined;
        }
      } else if (block.type === "tool_result") {
        if (
          !hasOnlyFields(block, textAnthropicToolResultFields) ||
          typeof block.tool_use_id !== "string" ||
          (block.is_error !== undefined && typeof block.is_error !== "boolean") ||
          (block.cache_control !== undefined && !isPlainRecord(block.cache_control))
        ) {
          return undefined;
        }
        const toolContent = block.content;
        if (toolContent === undefined || typeof toolContent === "string") continue;
        if (!Array.isArray(toolContent)) return undefined;
        for (let k = 0; k < toolContent.length; k++) {
          if (!isAnthropicTextBlock(toolContent[k])) return undefined;
        }
      } else {
        return undefined;
      }
    }
  }
  const promptOverhead =
    (toolsLen > 0 ? 1024 : 0) +
    (payload.thinking !== undefined ? 256 : 0) +
    (payload.output_config !== undefined ? 512 : 0);
  return (
    Buffer.byteLength(body ?? stringifyExact(payload)) +
    128 +
    32 * (messages.length + toolsLen) +
    promptOverhead
  );
}

const textGeminiTopFields = new Set([
  "model",
  "contents",
  "systemInstruction",
  "system_instruction",
  "generationConfig",
  "generation_config",
  "tools",
  "toolConfig",
  "tool_config",
  "safetySettings",
  "safety_settings",
  "stream",
]);
const textGeminiContentFields = new Set(["role", "parts"]);
const textGeminiPartFields = new Set([
  "text",
  "thought",
  "thoughtSignature",
  "thought_signature",
  "functionCall",
  "function_call",
  "functionResponse",
  "function_response",
]);

const textGeminiFunctionCallFields = new Set(["id", "name", "args"]);
const textGeminiFunctionResponseFields = new Set(["id", "name", "response"]);

function isGeminiTextOnlyPart(rawPart: unknown): boolean {
  if (!isPlainRecord(rawPart) || !hasOnlyFields(rawPart, textGeminiPartFields)) return false;
  if (rawPart.text !== undefined && typeof rawPart.text !== "string") return false;
  if (rawPart.thought !== undefined && typeof rawPart.thought !== "boolean") return false;
  if (rawPart.thoughtSignature !== undefined && typeof rawPart.thoughtSignature !== "string") {
    return false;
  }
  if (rawPart.thought_signature !== undefined && typeof rawPart.thought_signature !== "string") {
    return false;
  }
  if (rawPart.functionCall !== undefined && rawPart.function_call !== undefined) return false;
  if (rawPart.functionResponse !== undefined && rawPart.function_response !== undefined) {
    return false;
  }
  for (const fc of [rawPart.functionCall, rawPart.function_call]) {
    if (
      fc !== undefined &&
      (!isPlainRecord(fc) || !hasOnlyFields(fc, textGeminiFunctionCallFields))
    ) {
      return false;
    }
  }
  for (const fr of [rawPart.functionResponse, rawPart.function_response]) {
    if (
      fr !== undefined &&
      (!isPlainRecord(fr) || !hasOnlyFields(fr, textGeminiFunctionResponseFields))
    ) {
      return false;
    }
  }
  return (
    rawPart.text !== undefined ||
    rawPart.functionCall !== undefined ||
    rawPart.function_call !== undefined ||
    rawPart.functionResponse !== undefined ||
    rawPart.function_response !== undefined
  );
}

export function estimateTextGeminiInput(
  payload: Record<string, unknown>,
  body?: string,
): number | undefined {
  if (!Array.isArray(payload.contents) || !hasOnlyFields(payload, textGeminiTopFields)) {
    return undefined;
  }
  if (
    payload.generationConfig !== undefined &&
    payload.generationConfig !== null &&
    payload.generation_config !== undefined &&
    payload.generation_config !== null
  ) {
    return undefined;
  }
  if (
    payload.systemInstruction !== undefined &&
    payload.systemInstruction !== null &&
    payload.system_instruction !== undefined &&
    payload.system_instruction !== null
  ) {
    return undefined;
  }
  let hasStructuredSchema = false;
  for (const cfgRaw of [payload.generationConfig, payload.generation_config]) {
    if (cfgRaw === undefined || cfgRaw === null) continue;
    if (!isPlainRecord(cfgRaw)) return undefined;
    if (cfgRaw.candidateCount !== undefined && cfgRaw.candidateCount !== 1) return undefined;
    if (cfgRaw.candidate_count !== undefined && cfgRaw.candidate_count !== 1) return undefined;
    if (
      cfgRaw.responseSchema !== undefined ||
      cfgRaw.response_schema !== undefined ||
      cfgRaw.responseJsonSchema !== undefined ||
      cfgRaw.response_json_schema !== undefined
    ) {
      hasStructuredSchema = true;
    }
  }
  for (const sysRaw of [payload.systemInstruction, payload.system_instruction]) {
    if (sysRaw === undefined || sysRaw === null) continue;
    if (!isPlainRecord(sysRaw) || !hasOnlyFields(sysRaw, textGeminiContentFields)) return undefined;
    if (!Array.isArray(sysRaw.parts)) return undefined;
    for (let i = 0; i < sysRaw.parts.length; i++) {
      const part = sysRaw.parts[i];
      if (!isPlainRecord(part) || typeof part.text !== "string") return undefined;
      for (const key in part) {
        if (key !== "text") return undefined;
      }
    }
  }
  const tools = payload.tools;
  let toolsLen = 0;
  if (tools !== undefined) {
    if (!Array.isArray(tools)) return undefined;
    for (let i = 0; i < tools.length; i++) {
      const tool = tools[i];
      if (!isPlainRecord(tool)) return undefined;
      for (const key in tool) {
        if (key !== "functionDeclarations" && key !== "function_declarations") return undefined;
      }
      if (tool.functionDeclarations !== undefined && tool.function_declarations !== undefined) {
        return undefined;
      }
      const decls = tool.functionDeclarations ?? tool.function_declarations;
      if (!Array.isArray(decls)) return undefined;
      toolsLen += decls.length;
    }
  }
  const contents = payload.contents;
  for (let i = 0; i < contents.length; i++) {
    const content = contents[i];
    if (!isPlainRecord(content) || !hasOnlyFields(content, textGeminiContentFields)) {
      return undefined;
    }
    if (!Array.isArray(content.parts)) return undefined;
    for (let j = 0; j < content.parts.length; j++) {
      if (!isGeminiTextOnlyPart(content.parts[j])) return undefined;
    }
  }
  return (
    Buffer.byteLength(body ?? stringifyExact(payload)) +
    128 +
    32 * (contents.length + toolsLen) +
    (toolsLen > 0 ? 512 : 0) +
    (hasStructuredSchema ? 256 : 0)
  );
}

export function estimateTextInputForProtocol(
  upstreamProtocol: Protocol | null,
  usageDialect: string,
  payload: Record<string, unknown>,
  body: string,
): number | undefined {
  if (upstreamProtocol === "chat" && usageDialect === "openai") {
    return estimateTextChatInput(payload, body);
  }
  if (upstreamProtocol === "responses" && usageDialect === "openai") {
    return estimateTextResponsesInput(payload, body);
  }
  if (upstreamProtocol === "messages" && usageDialect === "anthropic") {
    return estimateTextMessagesInput(payload, body);
  }
  if (upstreamProtocol === "gemini" && usageDialect === "gemini") {
    return estimateTextGeminiInput(payload, body);
  }
  return undefined;
}
