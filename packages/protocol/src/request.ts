import { GatewayError, object, optionalObject, text } from "@gateway/contracts";
import { stringifyExact } from "./json.ts";
import type { Protocol } from "@gateway/kernel";
import {
  decodeGeminiMessages,
  decodeGeminiTools,
  extractGeminiGenerationConfig,
  extractGeminiToolChoice,
} from "./request-gemini.ts";
import {
  type DecodeRequestOptions,
  type NormalRequest,
  type ConversionMode,
  type Part,
  COMPAT_DROP_TOP_LEVEL,
  type Message,
} from "./request-policy.ts";
export type {
  ConversionMode,
  FieldPolicyAction,
  FieldPolicyEntry,
  DecodeRequestOptions,
  EncodeRequestOptions,
  Part,
  Message,
  Tool,
  NormalRequest,
} from "./request-policy.ts";
export { FIELD_CONVERSION_POLICY } from "./request-policy.ts";
export { encodeRequest } from "./request-encode.ts";

const jsonArguments = (value: unknown): string =>
  typeof value === "string" ? value : stringifyExact(value ?? {});

export function decodeRequest(
  body: Record<string, unknown>,
  source: Protocol,
  options: DecodeRequestOptions = {},
): NormalRequest {
  const mode: ConversionMode = options.mode ?? "strict";
  const dropped = new Set<string>();
  const recordDrop = (field: string) => {
    if (!dropped.has(field)) {
      dropped.add(field);
      options.onFieldDropped?.(field);
    }
  };
  const unsupported = (field: string): never => {
    options.onFieldRejected?.(field);
    throw new GatewayError(
      422,
      "conversion_loss",
      `Cross-protocol conversion cannot preserve ${field}`,
    );
  };
  const list = (value: unknown): unknown[] => {
    if (!Array.isArray(value)) return unsupported("non-array content");
    return value;
  };
  const onlyKeys = (value: Record<string, unknown>, allowed: readonly string[], path: string) => {
    for (const key of Object.keys(value)) {
      if (!allowed.includes(key)) unsupported(`${path}.${key}`);
    }
  };
  const imageMime = (mime: unknown): string => {
    if (typeof mime !== "string" || !/^image\/(?:png|jpeg|gif|webp)$/.test(mime))
      return unsupported("image media type");
    return mime;
  };
  const imageFromData = (data: unknown, mime: unknown): Part => {
    if (typeof data !== "string" || typeof mime !== "string") return unsupported("image source");
    return { type: "image", url: `data:${imageMime(mime)};base64,${data}`, mime };
  };
  const openaiParts = (content: unknown): Part[] => {
    if (content === null || content === undefined) return [];
    if (typeof content === "string") return [{ type: "text", text: content }];
    return list(content).map((entry): Part => {
      const part = object(entry);
      onlyKeys(part, ["type", "text", "image_url", "detail"], "content");
      if (["text", "input_text", "output_text"].includes(text(part.type)))
        return { type: "text", text: text(part.text) };
      if (["image_url", "input_image"].includes(text(part.type))) {
        const image = optionalObject(part.image_url);
        onlyKeys(image, ["url", "detail"], "image_url");
        const url = text(image.url ?? part.image_url);
        if (
          !url ||
          (image.detail && image.detail !== "auto") ||
          (part.detail && part.detail !== "auto")
        )
          return unsupported("image detail/source");
        return { type: "image", url, mime: /^data:([^;]+)/.exec(url)?.[1] ?? "image/jpeg" };
      }
      return unsupported(`content type ${text(part.type)}`);
    });
  };
  const anthropicParts = (content: unknown): Part[] => {
    if (typeof content === "string") return [{ type: "text", text: content }];
    const parts: Part[] = [];
    for (const entry of list(content)) {
      const part = object(entry);
      if (part.cache_control !== undefined) {
        if (mode === "compat") recordDrop("content.cache_control");
        else unsupported("cache_control");
      }
      const fields: Record<string, string[]> = {
        text: ["type", "text", "cache_control"],
        thinking: ["type", "thinking", "signature"],
        redacted_thinking: ["type", "data"],
        image: ["type", "source", "cache_control"],
        tool_use: ["type", "id", "name", "input", "cache_control"],
        tool_result: ["type", "tool_use_id", "is_error", "content", "cache_control"],
      };
      onlyKeys(part, fields[text(part.type)] ?? ["type"], "content");
      switch (part.type) {
        case "text":
          parts.push({ type: "text", text: text(part.text) });
          break;
        case "thinking":
          parts.push({
            type: "reasoning",
            text: text(part.thinking),
            ...(typeof part.signature === "string" ? { signature: part.signature } : {}),
          });
          break;
        case "redacted_thinking":
          if (mode === "compat") recordDrop("content.redacted_thinking");
          else unsupported("content type redacted_thinking");
          break;
        case "image": {
          const sourceObj = object(part.source);
          onlyKeys(sourceObj, ["type", "data", "media_type", "url"], "image.source");
          if (sourceObj.type === "base64") {
            parts.push(imageFromData(sourceObj.data, sourceObj.media_type));
          } else if (sourceObj.type === "url") {
            parts.push({ type: "image", url: text(sourceObj.url), mime: "image/jpeg" });
          } else {
            unsupported("image source");
          }
          break;
        }
        case "tool_use":
          parts.push({
            type: "call",
            id: text(part.id),
            name: text(part.name),
            arguments: jsonArguments(part.input),
          });
          break;
        case "tool_result": {
          if (part.is_error !== undefined && typeof part.is_error !== "boolean") {
            unsupported("error or structured tool result");
          }
          if (part.is_error === true) {
            if (mode === "compat") recordDrop("tool_result.is_error");
            else unsupported("error or structured tool result");
          }
          let resultContent = "";
          if (typeof part.content === "string") {
            resultContent = part.content;
          } else if (part.content === undefined) {
            resultContent = "";
          } else if (Array.isArray(part.content)) {
            const textBlocks: string[] = [];
            for (const subEntry of part.content) {
              const sub = object(subEntry);
              if (sub.type !== "text" || typeof sub.text !== "string") {
                unsupported("error or structured tool result");
              }
              if (sub.cache_control !== undefined) {
                if (mode === "compat") recordDrop("content.cache_control");
                else unsupported("cache_control");
              }
              onlyKeys(sub, ["type", "text", "cache_control"], "content");
              textBlocks.push(sub.text as string);
            }
            resultContent = textBlocks.join("\n");
          } else {
            unsupported("error or structured tool result");
          }
          parts.push({
            type: "result",
            id: text(part.tool_use_id),
            name: "",
            content: resultContent,
          });
          break;
        }
        default:
          unsupported(`content type ${text(part.type)}`);
      }
    }
    return parts;
  };

  const allowed: Record<Protocol, Set<string>> = {
    chat: new Set([
      "model",
      "messages",
      "stream",
      "stream_options",
      "max_tokens",
      "max_completion_tokens",
      "temperature",
      "top_p",
      "stop",
      "tools",
      "tool_choice",
      "parallel_tool_calls",
      "response_format",
      "n",
      "store",
      "reasoning_effort",
    ]),
    responses: new Set([
      "model",
      "input",
      "instructions",
      "stream",
      "max_output_tokens",
      "temperature",
      "top_p",
      "tools",
      "tool_choice",
      "parallel_tool_calls",
      "text",
      "store",
      "reasoning",
    ]),
    messages: new Set([
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
    ]),
    gemini: new Set([
      "model",
      "contents",
      "systemInstruction",
      "system_instruction",
      "generationConfig",
      "generation_config",
      "tools",
      "toolConfig",
      "tool_config",
      "stream",
    ]),
  };
  for (const key of Object.keys(body)) {
    if (allowed[source].has(key)) continue;
    if (mode === "compat" && COMPAT_DROP_TOP_LEVEL[source].has(key)) {
      recordDrop(key);
      continue;
    }
    unsupported(key);
  }
  if (body.output_config)
    onlyKeys(object(body.output_config), ["format", "effort"], "output_config");
  if (body.text) onlyKeys(object(body.text), ["format"], "text");
  if (body.stream_options)
    onlyKeys(object(body.stream_options), ["include_usage"], "stream_options");
  if (body.n !== undefined && body.n !== null && body.n !== 1) unsupported("n > 1");
  if (body.store !== undefined && body.store !== null) {
    if (body.store === true) unsupported("stored response continuation");
    if (body.store !== false) unsupported("store");
  }

  const result: NormalRequest = { messages: [], tools: [], stream: body.stream === true };
  if (body.parallel_tool_calls !== undefined && body.parallel_tool_calls !== null) {
    if (typeof body.parallel_tool_calls !== "boolean") unsupported("parallel_tool_calls");
    result.parallelToolCalls = body.parallel_tool_calls as boolean;
  }
  if (source === "chat" && body.reasoning_effort !== undefined && body.reasoning_effort !== null) {
    if (typeof body.reasoning_effort !== "string" || body.reasoning_effort.trim().length === 0) {
      unsupported("reasoning_effort");
    }
    result.reasoningEffort = body.reasoning_effort as string;
    result.reasoningEffortField = "reasoning_effort";
  }
  if (source === "responses" && body.reasoning !== undefined && body.reasoning !== null) {
    if (typeof body.reasoning !== "object" || Array.isArray(body.reasoning)) {
      unsupported("reasoning");
    }
    const reasoning = body.reasoning as Record<string, unknown>;
    if (reasoning.summary !== undefined && reasoning.summary !== null) {
      if (mode === "compat") {
        if (typeof reasoning.summary !== "string") unsupported("reasoning.summary");
        recordDrop("reasoning.summary");
      } else {
        unsupported("reasoning.summary");
      }
    }
    onlyKeys(reasoning, ["effort", "summary"], "reasoning");
    if (reasoning.effort !== undefined && reasoning.effort !== null) {
      if (typeof reasoning.effort !== "string" || reasoning.effort.trim().length === 0) {
        unsupported("reasoning.effort");
      }
      result.reasoningEffort = reasoning.effort as string;
      result.reasoningEffortField = "reasoning_effort";
    }
  }
  if (source === "messages" && body.output_config !== undefined && body.output_config !== null) {
    const outputConfig = object(body.output_config);
    if (outputConfig.effort !== undefined && outputConfig.effort !== null) {
      if (typeof outputConfig.effort !== "string" || outputConfig.effort.trim().length === 0) {
        unsupported("output_config.effort");
      }
      result.reasoningEffort = outputConfig.effort as string;
      result.reasoningEffortField = "output_config.effort";
    }
  }
  if (source === "messages" && body.thinking !== undefined && body.thinking !== null) {
    const thinking = object(body.thinking);
    if (thinking.type === "disabled") {
      onlyKeys(thinking, ["type"], "thinking");
      if (result.reasoningEffort !== undefined && result.reasoningEffort !== "none") {
        unsupported("thinking");
      }
      result.reasoningEffort = "none";
      result.reasoningEffortField = "thinking";
    } else if (thinking.type === "adaptive") {
      onlyKeys(thinking, ["type"], "thinking");
      if (result.reasoningEffort === "none") {
        unsupported("thinking");
      }
      if (result.reasoningEffort === undefined) {
        result.reasoningEffort = "medium";
        result.reasoningEffortField = "thinking";
      }
    } else if (thinking.type === "enabled") {
      onlyKeys(thinking, ["type", "budget_tokens"], "thinking");
      if (result.reasoningEffort === "none") {
        unsupported("thinking");
      }
      const budget = thinking.budget_tokens;
      if (budget !== undefined) {
        if (typeof budget !== "number" || !Number.isSafeInteger(budget) || budget <= 0) {
          unsupported("thinking.budget_tokens");
        }
        if (mode !== "compat") unsupported("thinking.budget_tokens");
        recordDrop("thinking.budget_tokens");
      }
      if (result.reasoningEffort === undefined) {
        result.reasoningEffort =
          typeof budget === "number"
            ? budget <= 2048
              ? "low"
              : budget <= 8192
                ? "medium"
                : "high"
            : "medium";
        result.reasoningEffortField = "thinking";
      }
    } else {
      unsupported("thinking");
    }
  }

  const calls = new Map<string, string>();
  const add = (role: unknown, parts: Part[], coalesceMode?: "assistant" | "tool_results") => {
    if (!["user", "assistant", "system", "developer", "model", "tool"].includes(text(role)))
      unsupported("message role");
    const normalized = (
      role === "developer"
        ? "system"
        : role === "model"
          ? "assistant"
          : role === "tool"
            ? "user"
            : role
    ) as Message["role"];
    for (const part of parts) {
      if (part.type === "call") calls.set(part.id, part.name);
      if (part.type === "result" && !part.name && part.id) {
        part.name = calls.get(part.id) ?? "";
      }
    }
    const last = result.messages.at(-1);
    if (
      coalesceMode === "assistant" &&
      last &&
      last.role === "assistant" &&
      normalized === "assistant"
    ) {
      last.parts.push(...parts);
      return;
    }
    if (
      coalesceMode === "tool_results" &&
      last &&
      last.role === "user" &&
      normalized === "user" &&
      last.parts.length > 0 &&
      last.parts.every((p) => p.type === "result") &&
      parts.every((p) => p.type === "result")
    ) {
      last.parts.push(...parts);
      return;
    }
    result.messages.push({ role: normalized, parts });
  };

  if (source === "chat") {
    for (const entry of list(body.messages)) {
      const message = object(entry);
      for (const key of Object.keys(message))
        if (
          ![
            "role",
            "content",
            "tool_calls",
            "tool_call_id",
            "name",
            "reasoning_content",
            "reasoning",
          ].includes(key)
        )
          unsupported(`message.${key}`);
      if (message.name && message.role !== "tool") unsupported("message.name");
      const parts: Part[] = [];
      const reasoningRaw = message.reasoning_content ?? message.reasoning;
      if (reasoningRaw !== undefined && reasoningRaw !== null) {
        if (typeof reasoningRaw !== "string") unsupported("message.reasoning_content");
        if ((reasoningRaw as string).length > 0) {
          parts.push({ type: "reasoning", text: reasoningRaw as string });
        }
      }
      if (message.role === "tool") {
        parts.push({
          type: "result",
          id: text(message.tool_call_id),
          name: calls.get(text(message.tool_call_id)) ?? "",
          content: message.content,
        });
      } else {
        parts.push(...openaiParts(message.content));
      }
      if (message.tool_calls)
        for (const raw of list(message.tool_calls)) {
          const call = object(raw);
          const fn = object(call.function);
          onlyKeys(call, ["id", "type", "function"], "tool_call");
          onlyKeys(fn, ["name", "arguments"], "tool_call.function");
          if (call.type !== "function") unsupported("non-function tool");
          parts.push({
            type: "call",
            id: text(call.id),
            name: text(fn.name),
            arguments: jsonArguments(fn.arguments),
          });
        }
      add(message.role, parts, message.role === "tool" ? "tool_results" : undefined);
    }
  } else if (source === "responses") {
    if (body.instructions) add("system", [{ type: "text", text: text(body.instructions) }]);
    if (typeof body.input === "string") add("user", [{ type: "text", text: body.input }]);
    else
      for (const raw of list(body.input)) {
        const item = object(raw);
        onlyKeys(
          item,
          item.type === "function_call"
            ? ["type", "id", "call_id", "name", "arguments", "status"]
            : item.type === "function_call_output"
              ? ["type", "id", "call_id", "output", "status"]
              : item.type === "reasoning"
                ? ["type", "id", "summary", "content", "status"]
                : ["type", "id", "role", "content", "status"],
          "input",
        );
        if (item.type === "function_call")
          add(
            "assistant",
            [
              {
                type: "call",
                id: text(item.call_id),
                name: text(item.name),
                arguments: jsonArguments(item.arguments),
              },
            ],
            "assistant",
          );
        else if (item.type === "function_call_output") {
          let outputContent: unknown = item.output;
          if (Array.isArray(item.output)) {
            const parts = openaiParts(item.output);
            if (parts.some((p) => p.type !== "text")) {
              unsupported("structured function_call_output");
            }
            outputContent = parts.map((p) => (p.type === "text" ? p.text : "")).join("\n");
          }
          add(
            "user",
            [
              {
                type: "result",
                id: text(item.call_id),
                name: calls.get(text(item.call_id)) ?? "",
                content: outputContent,
              },
            ],
            "tool_results",
          );
        } else if (item.type === "reasoning") {
          const texts: string[] = [];
          const summaryList =
            Array.isArray(item.summary) && item.summary.length > 0
              ? item.summary
              : Array.isArray(item.content)
                ? item.content
                : [];
          for (const entry of summaryList) {
            const summaryObj = optionalObject(entry);
            if (typeof summaryObj.text === "string" && summaryObj.text.length > 0) {
              texts.push(summaryObj.text);
            }
          }
          if (texts.length > 0) {
            add("assistant", [{ type: "reasoning", text: texts.join("") }], "assistant");
          }
        } else if (!item.type || item.type === "message")
          add(
            item.role,
            openaiParts(item.content),
            item.role === "assistant" ? "assistant" : undefined,
          );
        else unsupported(`input type ${text(item.type)}`);
      }
  } else if (source === "messages") {
    if (body.system) add("system", anthropicParts(body.system));
    for (const raw of list(body.messages)) {
      const message = object(raw);
      onlyKeys(message, ["role", "content"], "message");
      add(message.role, anthropicParts(message.content));
    }
  } else {
    decodeGeminiMessages(body, {
      mode,
      recordDrop,
      unsupported,
      list,
      onlyKeys,
      imageMime,
      imageFromData,
      add,
      messageCount: () => result.messages.length,
    });
  }

  const config =
    source === "gemini" ? extractGeminiGenerationConfig(body, mode, recordDrop, unsupported) : body;
  if (
    source === "chat" &&
    config.max_tokens !== undefined &&
    config.max_tokens !== null &&
    config.max_completion_tokens !== undefined &&
    config.max_completion_tokens !== null
  ) {
    unsupported("conflicting output token limits");
  }
  const max =
    config.maxOutputTokens ??
    config.max_completion_tokens ??
    config.max_tokens ??
    config.max_output_tokens;
  if (max !== undefined && max !== null) {
    if (!Number.isSafeInteger(max) || Number(max) <= 0) unsupported("output token limit");
    result.maxTokens = Number(max);
  }
  const temperature = config.temperature;
  if (temperature !== undefined && temperature !== null) {
    if (typeof temperature !== "number" || !Number.isFinite(temperature))
      unsupported("temperature");
    result.temperature = Number(temperature);
  }
  const topP = config.topP ?? config.top_p;
  if (topP !== undefined && topP !== null) {
    if (typeof topP !== "number" || !Number.isFinite(topP)) unsupported("top_p");
    result.topP = Number(topP);
  }
  const topK = config.topK ?? config.top_k;
  if (topK !== undefined && topK !== null) {
    if (typeof topK !== "number" || !Number.isSafeInteger(topK) || topK <= 0) unsupported("top_k");
    result.topK = Number(topK);
  }
  const stop = config.stopSequences ?? config.stop_sequences ?? config.stop;
  if (stop !== undefined && stop !== null) {
    result.stop =
      typeof stop === "string"
        ? [stop]
        : list(stop).map((v) => {
            if (typeof v !== "string") return unsupported("stop");
            return v;
          });
  }
  if (body.tools) {
    if (source === "gemini") {
      result.tools.push(...decodeGeminiTools(body.tools, { list, onlyKeys, unsupported }));
    } else {
      for (const value of list(body.tools)) {
        const tool = object(value);
        const fn = source === "chat" ? object(tool.function) : tool;
        if (source === "chat") onlyKeys(tool, ["type", "function"], "tool");
        if (source === "messages" && fn.cache_control !== undefined) {
          if (mode === "compat") recordDrop("tools.cache_control");
          else unsupported("tool.cache_control");
        }
        onlyKeys(
          fn,
          [
            "type",
            "name",
            "description",
            "parameters",
            "input_schema",
            "strict",
            ...(source === "messages" && mode === "compat" ? ["cache_control"] : []),
          ],
          "tool",
        );
        if ((source === "chat" || source === "responses") && tool.type !== "function")
          unsupported("hosted tools");
        if (source === "messages" && tool.type !== undefined && tool.type !== "custom")
          unsupported("versioned tool");
        if (fn.strict === true) unsupported("strict tool validation");
        result.tools.push({
          name: text(fn.name),
          description: text(fn.description),
          schema: optionalObject(fn.parameters ?? fn.input_schema),
        });
      }
    }
  }
  const choice =
    source === "gemini" ? extractGeminiToolChoice(body, onlyKeys, unsupported) : body.tool_choice;
  if (typeof choice === "string") {
    if (!["auto", "none", "required"].includes(choice)) unsupported("tool_choice");
    result.toolChoice = choice as "auto" | "none" | "required";
  } else if (choice && Object.keys(optionalObject(choice)).length) {
    const c = object(choice);
    onlyKeys(
      c,
      [
        "type",
        "mode",
        "name",
        "function",
        "allowedFunctionNames",
        "allowed_function_names",
        "disable_parallel_tool_use",
      ],
      "tool_choice",
    );
    if (c.function) onlyKeys(object(c.function), ["name"], "tool_choice.function");
    if (c.disable_parallel_tool_use !== undefined) {
      if (typeof c.disable_parallel_tool_use !== "boolean")
        unsupported("disable_parallel_tool_use");
      result.parallelToolCalls = !(c.disable_parallel_tool_use as boolean);
    }
    if (c.type !== undefined && c.mode !== undefined) unsupported("tool_choice");
    const hasFn = c.function !== undefined && c.function !== null;
    const name = text(c.name ?? optionalObject(c.function).name);
    if (c.type === "none" || c.mode === "NONE") {
      if (name || hasFn) unsupported("tool_choice");
      result.toolChoice = "none";
    } else if (c.type === "auto" || c.mode === "AUTO") {
      if (name || hasFn) unsupported("tool_choice");
      result.toolChoice = "auto";
    } else if (c.type === "any" || c.mode === "ANY") {
      if (name || hasFn) unsupported("tool_choice");
      result.toolChoice = "required";
    } else if (c.type === "function" || c.type === "tool") {
      if (!name) unsupported("tool_choice");
      result.toolChoice = { name };
    } else {
      unsupported("tool_choice");
    }
    if (
      c.allowedFunctionNames !== undefined &&
      c.allowedFunctionNames !== null &&
      c.allowed_function_names !== undefined &&
      c.allowed_function_names !== null
    ) {
      unsupported("allowedFunctionNames");
    }
    const allowedNames = c.allowedFunctionNames ?? c.allowed_function_names;
    if (source === "gemini" && allowedNames !== undefined && allowedNames !== null) {
      const names = list(allowedNames);
      if (
        c.mode !== "ANY" ||
        !names.length ||
        names.some(
          (name) => typeof name !== "string" || !result.tools.some((tool) => tool.name === name),
        )
      )
        unsupported("allowedFunctionNames");
      result.tools = result.tools.filter((tool) => names.includes(tool.name));
      if (names.length === 1) result.toolChoice = { name: String(names[0]) };
    }
  }
  const rawFormat =
    source === "responses"
      ? optionalObject(body.text).format
      : source === "messages"
        ? optionalObject(body.output_config).format
        : body.response_format;
  if (
    rawFormat !== undefined &&
    rawFormat !== null &&
    (typeof rawFormat !== "object" || Array.isArray(rawFormat))
  ) {
    unsupported("response_format");
  }
  const format = optionalObject(rawFormat);
  onlyKeys(format, ["type", "json_schema", "name", "schema", "strict"], "format");
  if (
    format.json_schema !== undefined &&
    format.json_schema !== null &&
    (typeof format.json_schema !== "object" || Array.isArray(format.json_schema))
  ) {
    unsupported("format.json_schema");
  }
  const schemaFormat = optionalObject(format.json_schema);
  onlyKeys(schemaFormat, ["name", "description", "schema", "strict"], "format.json_schema");
  if (format.strict === true || schemaFormat.strict === true)
    unsupported("strict output validation");
  if (Object.keys(format).length > 0) {
    if (format.type === "json_object") {
      result.jsonSchema = { type: "object" };
    } else if (format.type === "json_schema") {
      const rawSchema =
        format.json_schema !== undefined && format.json_schema !== null
          ? schemaFormat.schema
          : format.schema;
      if (
        rawSchema === undefined ||
        rawSchema === null ||
        typeof rawSchema !== "object" ||
        Array.isArray(rawSchema)
      ) {
        unsupported("format.schema");
      }
      result.jsonSchema = rawSchema as Record<string, unknown>;
    } else if (format.type !== "text") {
      unsupported("response_format");
    }
  }
  const geminiMime = config.responseMimeType ?? config.response_mime_type;
  const geminiJsonSchema = config.responseJsonSchema ?? config.response_json_schema;
  const geminiSchema = config.responseSchema ?? config.response_schema;
  if (geminiMime !== undefined && geminiMime !== null) {
    if (geminiMime === "application/json") {
      result.jsonSchema = { type: "object" };
    } else if (geminiMime !== "text/plain") {
      unsupported("generationConfig.responseMimeType");
    }
  }
  const rawGeminiSchema = geminiJsonSchema ?? geminiSchema;
  if (rawGeminiSchema !== undefined && rawGeminiSchema !== null) {
    result.jsonSchema = object(rawGeminiSchema);
  }
  if (dropped.size > 0) result.droppedFields = [...dropped];
  return result;
}
