import { GatewayError, object, optionalObject, text } from "@gateway/contracts";
import { stringifyExact, parseProviderJson } from "./json.ts";
import type { Protocol } from "@gateway/kernel";

export type ConversionMode = "strict" | "compat";
export type FieldPolicyAction = "preserve" | "map" | "drop_hint" | "reject";

export type FieldPolicyEntry = {
  field: string;
  protocols: readonly Protocol[];
  action: FieldPolicyAction;
  note: string;
};

export const FIELD_CONVERSION_POLICY: readonly FieldPolicyEntry[] = [
  {
    field: "messages",
    protocols: ["chat", "responses", "messages", "gemini"],
    action: "preserve",
    note: "Conversation turns and text/image/tool parts",
  },
  {
    field: "temperature",
    protocols: ["chat", "responses", "messages", "gemini"],
    action: "preserve",
    note: "Sampling temperature",
  },
  {
    field: "top_p",
    protocols: ["chat", "responses", "messages", "gemini"],
    action: "preserve",
    note: "Nucleus sampling parameter",
  },
  {
    field: "max_tokens",
    protocols: ["chat", "responses", "messages", "gemini"],
    action: "map",
    note: "Mapped across max_completion_tokens, max_output_tokens, max_tokens, maxOutputTokens",
  },
  {
    field: "parallel_tool_calls",
    protocols: ["chat", "responses", "messages"],
    action: "map",
    note: "Mapped bidirectionally with Anthropic tool_choice.disable_parallel_tool_use",
  },
  {
    field: "top_k",
    protocols: ["messages", "gemini"],
    action: "map",
    note: "Preserved between Messages and Gemini; dropped with hint on OpenAI in compat mode",
  },
  {
    field: "reasoning",
    protocols: ["chat", "responses", "messages", "gemini"],
    action: "map",
    note: "Preserved in responses and streams; unsigned history reasoning stripped across protocols",
  },
  {
    field: "metadata",
    protocols: ["chat", "responses", "messages"],
    action: "drop_hint",
    note: "Non-semantic caller tags dropped with header hint in compat mode",
  },
  {
    field: "user",
    protocols: ["chat", "responses"],
    action: "drop_hint",
    note: "Caller end-user identifier dropped with header hint in compat mode",
  },
  {
    field: "seed",
    protocols: ["chat", "gemini"],
    action: "drop_hint",
    note: "Best-effort sampling seed dropped with header hint in compat mode",
  },
  {
    field: "service_tier",
    protocols: ["chat", "responses", "messages"],
    action: "drop_hint",
    note: "Provider-specific scheduling tier dropped with header hint in compat mode",
  },
  {
    field: "cache_control",
    protocols: ["messages"],
    action: "drop_hint",
    note: "Anthropic prompt cache hints dropped with header hint in compat mode",
  },
  {
    field: "n",
    protocols: ["chat"],
    action: "reject",
    note: "n > 1 changes candidate count and billing multiplicity",
  },
  {
    field: "store",
    protocols: ["responses"],
    action: "reject",
    note: "store: true requires stateful server continuation",
  },
  {
    field: "logprobs",
    protocols: ["chat"],
    action: "reject",
    note: "Token log-probabilities cannot be synthesized cross-protocol",
  },
  {
    field: "strict",
    protocols: ["chat", "responses"],
    action: "reject",
    note: "Strict grammar-constrained schema validation cannot be weakened silently",
  },
  {
    field: "output_config.effort",
    protocols: ["messages"],
    action: "reject",
    note: "Explicit effort contract cannot be silently dropped",
  },
  {
    field: "hosted_tools",
    protocols: ["chat", "responses", "messages", "gemini"],
    action: "reject",
    note: "Provider-hosted or versioned built-in tools cannot be emulated cross-protocol",
  },
];

const COMPAT_DROP_TOP_LEVEL: Record<Protocol, ReadonlySet<string>> = {
  chat: new Set(["metadata", "user", "seed", "service_tier"]),
  responses: new Set(["metadata", "user", "service_tier"]),
  messages: new Set(["metadata", "service_tier", "cache_control"]),
  gemini: new Set(["seed"]),
};

export type DecodeRequestOptions = {
  mode?: ConversionMode;
  onFieldDropped?: (field: string) => void;
  onFieldRejected?: (field: string) => void;
};

export type EncodeRequestOptions = {
  mode?: ConversionMode;
  onFieldDropped?: (field: string) => void;
  onFieldRejected?: (field: string) => void;
  droppedFields?: string[];
};

export type Part =
  | { type: "text"; text: string }
  | { type: "reasoning"; text: string; signature?: string }
  | { type: "image"; url: string; mime: string }
  | { type: "call"; id: string; name: string; arguments: string }
  | { type: "result"; id: string; name: string; content: unknown };

export interface Message {
  role: "system" | "user" | "assistant";
  parts: Part[];
}

export interface Tool {
  name: string;
  description: string;
  schema: Record<string, unknown>;
}

export interface NormalRequest {
  messages: Message[];
  tools: Tool[];
  stream: boolean;
  maxTokens?: number;
  temperature?: number;
  topP?: number;
  topK?: number;
  stop?: string[];
  parallelToolCalls?: boolean;
  toolChoice?: "auto" | "none" | "required" | { name: string };
  jsonSchema?: Record<string, unknown>;
  droppedFields?: string[];
}

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
        case "tool_result":
          if (part.is_error === true || typeof part.content !== "string")
            unsupported("error or structured tool result");
          parts.push({
            type: "result",
            id: text(part.tool_use_id),
            name: "",
            content: part.content,
          });
          break;
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
      "output_config",
    ]),
    gemini: new Set([
      "model",
      "contents",
      "systemInstruction",
      "generationConfig",
      "tools",
      "toolConfig",
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
  if (body.output_config) onlyKeys(object(body.output_config), ["format"], "output_config");
  if (body.text) onlyKeys(object(body.text), ["format"], "text");
  if (body.stream_options)
    onlyKeys(object(body.stream_options), ["include_usage"], "stream_options");
  if (body.n !== undefined && body.n !== 1) unsupported("n > 1");
  if (body.store === true) unsupported("stored response continuation");

  const result: NormalRequest = { messages: [], tools: [], stream: body.stream === true };
  if (body.parallel_tool_calls !== undefined) {
    if (typeof body.parallel_tool_calls !== "boolean") unsupported("parallel_tool_calls");
    result.parallelToolCalls = body.parallel_tool_calls as boolean;
  }

  const calls = new Map<string, string>();
  const add = (role: unknown, parts: Part[]) => {
    if (!["user", "assistant", "system", "developer", "model", "tool"].includes(text(role)))
      unsupported("message role");
    const normalized =
      role === "developer"
        ? "system"
        : role === "model"
          ? "assistant"
          : role === "tool"
            ? "user"
            : role;
    result.messages.push({ role: normalized as Message["role"], parts });
    for (const part of parts) if (part.type === "call") calls.set(part.id, part.name);
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
      add(message.role, parts);
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
            ? ["type", "call_id", "name", "arguments"]
            : item.type === "function_call_output"
              ? ["type", "call_id", "output"]
              : item.type === "reasoning"
                ? ["type", "id", "summary", "content", "status"]
                : ["type", "role", "content"],
          "input",
        );
        if (item.type === "function_call")
          add("assistant", [
            {
              type: "call",
              id: text(item.call_id),
              name: text(item.name),
              arguments: jsonArguments(item.arguments),
            },
          ]);
        else if (item.type === "function_call_output")
          add("user", [
            {
              type: "result",
              id: text(item.call_id),
              name: calls.get(text(item.call_id)) ?? "",
              content: item.output,
            },
          ]);
        else if (item.type === "reasoning") {
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
            add("assistant", [{ type: "reasoning", text: texts.join("") }]);
          }
        } else if (!item.type || item.type === "message") add(item.role, openaiParts(item.content));
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
    const system = optionalObject(body.systemInstruction);
    onlyKeys(system, ["parts", "role"], "systemInstruction");
    if (system.parts)
      add(
        "system",
        list(system.parts).map((raw) => {
          const part = object(raw);
          onlyKeys(part, ["text"], "systemInstruction.parts");
          return { type: "text", text: text(part.text) };
        }),
      );
    for (const raw of list(body.contents)) {
      const message = object(raw);
      onlyKeys(message, ["role", "parts"], "content");
      const parts: Part[] = [];
      for (const value of list(message.parts)) {
        const part = object(value);
        const working: Record<string, unknown> = { ...part };
        if (working.thoughtSignature !== undefined) {
          if (mode === "compat") {
            recordDrop("part.thoughtSignature");
            delete working.thoughtSignature;
          } else {
            unsupported("Gemini thought/signature");
          }
        }
        if (working.thought !== undefined) {
          if (working.thought === true) {
            onlyKeys(working, ["thought", "text"], "part");
            if (typeof working.text !== "string") unsupported("Gemini thought/signature");
            parts.push({ type: "reasoning", text: working.text as string });
            continue;
          }
          if (working.thought === false) {
            delete working.thought;
          } else {
            unsupported("Gemini thought/signature");
          }
        }
        if (Object.keys(working).length === 0) continue;
        onlyKeys(
          working,
          ["text", "inlineData", "fileData", "functionCall", "functionResponse"],
          "part",
        );
        if (Object.keys(working).length !== 1) unsupported("combined Gemini part fields");
        if (typeof working.text === "string") {
          parts.push({ type: "text", text: working.text });
        } else if (working.inlineData) {
          const data = object(working.inlineData);
          onlyKeys(data, ["mimeType", "data"], "inlineData");
          parts.push(imageFromData(data.data, data.mimeType));
        } else if (working.fileData) {
          const data = object(working.fileData);
          onlyKeys(data, ["mimeType", "fileUri"], "fileData");
          parts.push({ type: "image", url: text(data.fileUri), mime: imageMime(data.mimeType) });
        } else if (working.functionCall) {
          const call = object(working.functionCall);
          onlyKeys(call, ["id", "name", "args"], "functionCall");
          parts.push({
            type: "call",
            id: text(call.id, `call_${result.messages.length}`),
            name: text(call.name),
            arguments: jsonArguments(call.args),
          });
        } else if (working.functionResponse) {
          const response = object(working.functionResponse);
          onlyKeys(response, ["id", "name", "response"], "functionResponse");
          parts.push({
            type: "result",
            id: text(response.id),
            name: text(response.name),
            content: response.response,
          });
        } else {
          unsupported("Gemini part");
        }
      }
      add(message.role ?? "user", parts);
    }
  }

  const config = source === "gemini" ? optionalObject(body.generationConfig) : body;
  if (source === "gemini")
    for (const key of Object.keys(config)) {
      if (
        [
          "maxOutputTokens",
          "temperature",
          "topP",
          "topK",
          "stopSequences",
          "responseMimeType",
          "responseSchema",
          "responseJsonSchema",
        ].includes(key)
      ) {
        continue;
      }
      if (mode === "compat" && key === "seed") {
        recordDrop("generationConfig.seed");
        continue;
      }
      unsupported(`generationConfig.${key}`);
    }
  const max =
    config.maxOutputTokens ??
    config.max_completion_tokens ??
    config.max_tokens ??
    config.max_output_tokens;
  if (max !== undefined) {
    if (!Number.isSafeInteger(max) || Number(max) <= 0) unsupported("output token limit");
    result.maxTokens = Number(max);
  }
  const temperature = config.temperature;
  if (temperature !== undefined) {
    if (typeof temperature !== "number" || !Number.isFinite(temperature))
      unsupported("temperature");
    result.temperature = Number(temperature);
  }
  const topP = config.topP ?? config.top_p;
  if (topP !== undefined) {
    if (typeof topP !== "number" || !Number.isFinite(topP)) unsupported("top_p");
    result.topP = Number(topP);
  }
  const topK = config.topK ?? config.top_k;
  if (topK !== undefined) {
    if (typeof topK !== "number" || !Number.isSafeInteger(topK) || topK <= 0) unsupported("top_k");
    result.topK = Number(topK);
  }
  const stop = config.stopSequences ?? config.stop_sequences ?? config.stop;
  if (stop !== undefined) {
    result.stop =
      typeof stop === "string"
        ? [stop]
        : list(stop).map((v) => {
            if (typeof v !== "string") return unsupported("stop");
            return v;
          });
  }
  if (body.tools) {
    for (const value of list(body.tools)) {
      const tool = object(value);
      if (source === "gemini") {
        onlyKeys(tool, ["functionDeclarations"], "tool");
        if (!tool.functionDeclarations) unsupported("hosted tools");
        for (const raw of list(tool.functionDeclarations)) {
          const fn = object(raw);
          onlyKeys(
            fn,
            ["name", "description", "parameters", "parametersJsonSchema"],
            "tool.functionDeclarations",
          );
          result.tools.push({
            name: text(fn.name),
            description: text(fn.description),
            schema: optionalObject(fn.parameters ?? fn.parametersJsonSchema),
          });
        }
      } else {
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
    source === "gemini"
      ? optionalObject(optionalObject(body.toolConfig).functionCallingConfig)
      : body.tool_choice;
  if (source === "gemini" && body.toolConfig)
    onlyKeys(object(body.toolConfig), ["functionCallingConfig"], "toolConfig");
  if (typeof choice === "string") {
    if (!["auto", "none", "required"].includes(choice)) unsupported("tool_choice");
    result.toolChoice = choice as "auto" | "none" | "required";
  } else if (choice && Object.keys(optionalObject(choice)).length) {
    const c = object(choice);
    onlyKeys(
      c,
      ["type", "mode", "name", "function", "allowedFunctionNames", "disable_parallel_tool_use"],
      "tool_choice",
    );
    if (c.function) onlyKeys(object(c.function), ["name"], "tool_choice.function");
    if (c.disable_parallel_tool_use !== undefined) {
      if (typeof c.disable_parallel_tool_use !== "boolean")
        unsupported("disable_parallel_tool_use");
      result.parallelToolCalls = !(c.disable_parallel_tool_use as boolean);
    }
    const name = text(c.name ?? optionalObject(c.function).name);
    if (name) result.toolChoice = { name };
    else if (c.type === "any" || c.mode === "ANY") result.toolChoice = "required";
    else if (c.type === "none" || c.mode === "NONE") result.toolChoice = "none";
    else if (c.type === "auto" || c.mode === "AUTO") result.toolChoice = "auto";
    else unsupported("tool_choice");
    if (source === "gemini" && c.allowedFunctionNames !== undefined) {
      const names = list(c.allowedFunctionNames);
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
  const format =
    source === "responses"
      ? optionalObject(optionalObject(body.text).format)
      : source === "messages"
        ? optionalObject(optionalObject(body.output_config).format)
        : optionalObject(body.response_format);
  onlyKeys(format, ["type", "json_schema", "name", "schema", "strict"], "format");
  const schemaFormat = optionalObject(format.json_schema);
  onlyKeys(schemaFormat, ["name", "description", "schema", "strict"], "format.json_schema");
  if (format.strict === true || schemaFormat.strict === true)
    unsupported("strict output validation");
  if (format.type === "json_object" || config.responseMimeType === "application/json")
    result.jsonSchema = { type: "object" };
  if (format.type === "json_schema")
    result.jsonSchema = optionalObject(optionalObject(format.json_schema).schema ?? format.schema);
  if (config.responseJsonSchema || config.responseSchema)
    result.jsonSchema = object(config.responseJsonSchema ?? config.responseSchema);
  if (dropped.size > 0) result.droppedFields = [...dropped];
  return result;
}

export function encodeRequest(
  request: NormalRequest,
  target: Protocol,
  model: string,
  options: EncodeRequestOptions = {},
): Record<string, unknown> {
  const mode: ConversionMode = options.mode ?? "strict";
  const dropped = new Set<string>(request.droppedFields ?? []);
  const recordDrop = (field: string) => {
    if (!dropped.has(field)) {
      dropped.add(field);
      options.onFieldDropped?.(field);
    }
    if (options.droppedFields && !options.droppedFields.includes(field)) {
      options.droppedFields.push(field);
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
  const imageSource = (part: Extract<Part, { type: "image" }>): unknown => {
    if (target === "chat") return { type: "image_url", image_url: { url: part.url } };
    if (target === "responses") return { type: "input_image", image_url: part.url };
    const data = /^data:([^;]+);base64,([\s\S]+)$/.exec(part.url);
    if (target === "messages")
      return {
        type: "image",
        source: data
          ? { type: "base64", media_type: data[1], data: data[2] }
          : { type: "url", url: part.url },
      };
    if (!data && !part.url.startsWith("gs://")) return unsupported("remote image URL for Gemini");
    return data
      ? { inlineData: { mimeType: data[1], data: data[2] } }
      : { fileData: { mimeType: part.mime, fileUri: part.url } };
  };

  if (options.droppedFields) {
    for (const field of dropped) {
      if (!options.droppedFields.includes(field)) options.droppedFields.push(field);
    }
  }

  const body: Record<string, unknown> = { model, stream: request.stream };
  if (request.temperature !== undefined) body.temperature = request.temperature;
  if (request.topP !== undefined) body.top_p = request.topP;
  const system: string[] = [];
  const messages: unknown[] = [];
  let sawConversation = false;
  for (const message of request.messages) {
    if (message.role === "system" && (target === "messages" || target === "gemini")) {
      if (sawConversation || message.parts.some((p) => p.type !== "text"))
        unsupported("non-prefix system message");
      system.push(...message.parts.flatMap((p) => (p.type === "text" ? [p.text] : [])));
      continue;
    }
    sawConversation = true;
    if (target === "chat") {
      let content: unknown[] = [];
      let calls: unknown[] = [];
      const flush = () => {
        if (content.length || calls.length)
          messages.push({
            role: message.role,
            content: content.length ? content : null,
            ...(calls.length ? { tool_calls: calls } : {}),
          });
        content = [];
        calls = [];
      };
      for (const part of message.parts) {
        if (part.type === "reasoning") continue;
        if (part.type === "text") content.push({ type: "text", text: part.text });
        if (part.type === "image") content.push(imageSource(part));
        if (part.type === "call")
          calls.push({
            id: part.id,
            type: "function",
            function: { name: part.name, arguments: part.arguments },
          });
        if (part.type === "result") {
          flush();
          if (!part.id) unsupported("tool result without call ID");
          messages.push({
            role: "tool",
            tool_call_id: part.id,
            content: typeof part.content === "string" ? part.content : stringifyExact(part.content),
          });
        }
      }
      flush();
    } else if (target === "responses") {
      let content: unknown[] = [];
      const flush = () => {
        if (content.length) messages.push({ type: "message", role: message.role, content });
        content = [];
      };
      for (const part of message.parts) {
        if (part.type === "reasoning") continue;
        if (part.type === "text")
          content.push({
            type: message.role === "assistant" ? "output_text" : "input_text",
            text: part.text,
          });
        if (part.type === "image") content.push(imageSource(part));
        if (part.type === "call") {
          flush();
          messages.push({
            type: "function_call",
            call_id: part.id,
            name: part.name,
            arguments: part.arguments,
          });
        }
        if (part.type === "result") {
          flush();
          messages.push({
            type: "function_call_output",
            call_id: part.id,
            output: typeof part.content === "string" ? part.content : stringifyExact(part.content),
          });
        }
      }
      flush();
    } else {
      const parts: unknown[] = [];
      for (const part of message.parts) {
        if (part.type === "reasoning") {
          if (target === "messages" && part.signature) {
            parts.push({ type: "thinking", thinking: part.text, signature: part.signature });
          }
          continue;
        }
        if (part.type === "text") {
          parts.push(
            target === "messages" ? { type: "text", text: part.text } : { text: part.text },
          );
        } else if (part.type === "image") {
          parts.push(imageSource(part));
        } else if (part.type === "call") {
          parts.push(
            target === "messages"
              ? {
                  type: "tool_use",
                  id: part.id,
                  name: part.name,
                  input: parseProviderJson(part.arguments),
                }
              : {
                  functionCall: {
                    id: part.id,
                    name: part.name,
                    args: parseProviderJson(part.arguments),
                  },
                },
          );
        } else {
          if (target === "gemini" && !part.name) unsupported("tool result without function name");
          parts.push(
            target === "messages"
              ? { type: "tool_result", tool_use_id: part.id, content: part.content }
              : {
                  functionResponse: {
                    id: part.id,
                    name: part.name,
                    response:
                      typeof part.content === "object" &&
                      part.content !== null &&
                      !Array.isArray(part.content)
                        ? part.content
                        : { result: part.content },
                  },
                },
          );
        }
      }
      if (parts.length > 0) {
        messages.push(
          target === "messages"
            ? { role: message.role, content: parts }
            : { role: message.role === "assistant" ? "model" : "user", parts },
        );
      }
    }
  }
  if (target === "chat") {
    body.messages = messages;
    if (request.maxTokens !== undefined) body.max_completion_tokens = request.maxTokens;
    if (request.stream) body.stream_options = { include_usage: true };
    if (request.parallelToolCalls !== undefined)
      body.parallel_tool_calls = request.parallelToolCalls;
    if (request.topK !== undefined) {
      if (mode === "compat") recordDrop("top_k");
      else unsupported("top_k");
    }
  } else if (target === "responses") {
    body.input = messages;
    body.store = false;
    if (request.maxTokens !== undefined) body.max_output_tokens = request.maxTokens;
    if (request.parallelToolCalls !== undefined)
      body.parallel_tool_calls = request.parallelToolCalls;
    if (request.topK !== undefined) {
      if (mode === "compat") recordDrop("top_k");
      else unsupported("top_k");
    }
  } else if (target === "messages") {
    body.messages = messages;
    if (system.length) body.system = system.join("\n\n");
    body.max_tokens = request.maxTokens ?? 32768;
    if (request.topK !== undefined) body.top_k = request.topK;
  } else {
    delete body.stream;
    delete body.temperature;
    delete body.top_p;
    body.contents = messages;
    if (system.length) body.systemInstruction = { parts: system.map((value) => ({ text: value })) };
    if (request.parallelToolCalls !== undefined) {
      if (mode === "compat") recordDrop("parallel_tool_calls");
      else unsupported("parallel_tool_calls");
    }
    body.generationConfig = {
      ...(request.maxTokens !== undefined ? { maxOutputTokens: request.maxTokens } : {}),
      ...(request.temperature !== undefined ? { temperature: request.temperature } : {}),
      ...(request.topP !== undefined ? { topP: request.topP } : {}),
      ...(request.topK !== undefined ? { topK: request.topK } : {}),
      ...(request.stop ? { stopSequences: request.stop } : {}),
      ...(request.jsonSchema
        ? { responseMimeType: "application/json", responseJsonSchema: request.jsonSchema }
        : {}),
    };
  }
  if (request.stop && target !== "gemini")
    body[target === "messages" ? "stop_sequences" : "stop"] = request.stop;
  if (request.jsonSchema && target !== "gemini") {
    const format = { type: "json_schema", name: "response", schema: request.jsonSchema };
    if (target === "chat")
      body.response_format = {
        type: "json_schema",
        json_schema: { name: "response", schema: request.jsonSchema },
      };
    if (target === "responses") body.text = { format };
    if (target === "messages")
      body.output_config = { format: { type: "json_schema", schema: request.jsonSchema } };
  }
  if (request.tools.length) {
    body.tools =
      target === "chat"
        ? request.tools.map((t) => ({
            type: "function",
            function: { name: t.name, description: t.description, parameters: t.schema },
          }))
        : target === "responses"
          ? request.tools.map((t) => ({
              type: "function",
              name: t.name,
              description: t.description,
              parameters: t.schema,
            }))
          : target === "messages"
            ? request.tools.map((t) => ({
                name: t.name,
                description: t.description,
                input_schema: t.schema,
              }))
            : [
                {
                  functionDeclarations: request.tools.map((t) => ({
                    name: t.name,
                    description: t.description,
                    parametersJsonSchema: t.schema,
                  })),
                },
              ];
  }
  if (
    request.toolChoice ||
    (target === "messages" && request.parallelToolCalls !== undefined && request.tools.length > 0)
  ) {
    const choice = request.toolChoice;
    if (target === "chat" && choice)
      body.tool_choice =
        typeof choice === "string" ? choice : { type: "function", function: { name: choice.name } };
    if (target === "responses" && choice)
      body.tool_choice =
        typeof choice === "string" ? choice : { type: "function", name: choice.name };
    if (target === "messages") {
      const baseChoice = !choice
        ? { type: "auto" }
        : typeof choice === "object"
          ? { type: "tool", name: choice.name }
          : { type: choice === "required" ? "any" : choice };
      body.tool_choice = {
        ...baseChoice,
        ...(request.parallelToolCalls !== undefined && baseChoice.type !== "none"
          ? { disable_parallel_tool_use: !request.parallelToolCalls }
          : {}),
      };
    }
    if (target === "gemini" && choice)
      body.toolConfig = {
        functionCallingConfig: {
          mode:
            typeof choice === "object" || choice === "required"
              ? "ANY"
              : choice === "none"
                ? "NONE"
                : "AUTO",
          ...(typeof choice === "object" ? { allowedFunctionNames: [choice.name] } : {}),
        },
      };
  }
  return body;
}
