import {
  GatewayError,
  object,
  optionalObject,
  text,
  stringifyExact,
  parseProviderJson,
  copyExact,
} from "@gateway/kernel";
import type { Protocol, ProviderKind, Operation, RequestPolicy } from "@gateway/kernel";
import { providerAdapter, providerProtocol } from "./adapters.ts";
import { validateRequestPolicy } from "./policy.ts";

export type Part =
  | { type: "text"; text: string }
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
  stop?: string[];
  toolChoice?: "auto" | "none" | "required" | { name: string };
  jsonSchema?: Record<string, unknown>;
}

const unsupported = (field: string): never => {
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
const jsonArguments = (value: unknown): string =>
  typeof value === "string" ? value : stringifyExact(value ?? {});

function onlyKeys(value: Record<string, unknown>, allowed: readonly string[], path: string): void {
  for (const key of Object.keys(value)) if (!allowed.includes(key)) unsupported(`${path}.${key}`);
}

function imageMime(mime: unknown): string {
  if (typeof mime !== "string" || !/^image\/(?:png|jpeg|gif|webp)$/.test(mime))
    return unsupported("image media type");
  return mime;
}

function imageFromData(data: unknown, mime: unknown): Part {
  if (typeof data !== "string" || typeof mime !== "string") return unsupported("image source");
  return { type: "image", url: `data:${imageMime(mime)};base64,${data}`, mime };
}

function openaiParts(content: unknown): Part[] {
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
}

function anthropicParts(content: unknown): Part[] {
  if (typeof content === "string") return [{ type: "text", text: content }];
  return list(content).map((entry): Part => {
    const part = object(entry);
    if (part.cache_control) return unsupported("cache_control");
    const fields: Record<string, string[]> = {
      text: ["type", "text"],
      image: ["type", "source"],
      tool_use: ["type", "id", "name", "input"],
      tool_result: ["type", "tool_use_id", "is_error", "content"],
    };
    onlyKeys(part, fields[text(part.type)] ?? ["type"], "content");
    switch (part.type) {
      case "text":
        return { type: "text", text: text(part.text) };
      case "image": {
        const source = object(part.source);
        onlyKeys(source, ["type", "data", "media_type", "url"], "image.source");
        if (source.type === "base64") return imageFromData(source.data, source.media_type);
        if (source.type === "url")
          return { type: "image", url: text(source.url), mime: "image/jpeg" };
        return unsupported("image source");
      }
      case "tool_use":
        return {
          type: "call",
          id: text(part.id),
          name: text(part.name),
          arguments: jsonArguments(part.input),
        };
      case "tool_result":
        if (part.is_error === true || typeof part.content !== "string")
          return unsupported("error or structured tool result");
        return { type: "result", id: text(part.tool_use_id), name: "", content: part.content };
      default:
        return unsupported(`content type ${text(part.type)}`);
    }
  });
}

export function decodeRequest(body: Record<string, unknown>, source: Protocol): NormalRequest {
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
  for (const key of Object.keys(body)) if (!allowed[source].has(key)) unsupported(key);
  if (body.output_config) onlyKeys(object(body.output_config), ["format"], "output_config");
  if (body.text) onlyKeys(object(body.text), ["format"], "text");
  if (body.stream_options)
    onlyKeys(object(body.stream_options), ["include_usage"], "stream_options");
  if (body.n !== undefined && body.n !== 1) unsupported("n > 1");
  if (body.store === true) unsupported("stored response continuation");
  const result: NormalRequest = { messages: [], tools: [], stream: body.stream === true };
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
        if (!["role", "content", "tool_calls", "tool_call_id", "name"].includes(key))
          unsupported(`message.${key}`);
      if (message.name && message.role !== "tool") unsupported("message.name");
      const parts =
        message.role === "tool"
          ? [
              {
                type: "result" as const,
                id: text(message.tool_call_id),
                name: calls.get(text(message.tool_call_id)) ?? "",
                content: message.content,
              },
            ]
          : openaiParts(message.content);
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
        else if (!item.type || item.type === "message") add(item.role, openaiParts(item.content));
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
      const parts = list(message.parts).map((value): Part => {
        const part = object(value);
        if (part.thought || part.thoughtSignature) unsupported("Gemini thought/signature");
        onlyKeys(
          part,
          ["text", "inlineData", "fileData", "functionCall", "functionResponse"],
          "part",
        );
        if (Object.keys(part).length !== 1) unsupported("combined Gemini part fields");
        if (typeof part.text === "string") return { type: "text", text: part.text };
        if (part.inlineData) {
          const data = object(part.inlineData);
          onlyKeys(data, ["mimeType", "data"], "inlineData");
          return imageFromData(data.data, data.mimeType);
        }
        if (part.fileData) {
          const data = object(part.fileData);
          onlyKeys(data, ["mimeType", "fileUri"], "fileData");
          return { type: "image", url: text(data.fileUri), mime: imageMime(data.mimeType) };
        }
        if (part.functionCall) {
          const call = object(part.functionCall);
          onlyKeys(call, ["id", "name", "args"], "functionCall");
          return {
            type: "call",
            id: text(call.id, `call_${result.messages.length}`),
            name: text(call.name),
            arguments: jsonArguments(call.args),
          };
        }
        if (part.functionResponse) {
          const response = object(part.functionResponse);
          onlyKeys(response, ["id", "name", "response"], "functionResponse");
          return {
            type: "result",
            id: text(response.id),
            name: text(response.name),
            content: response.response,
          };
        }
        return unsupported("Gemini part");
      });
      add(message.role ?? "user", parts);
    }
  }
  const config = source === "gemini" ? optionalObject(body.generationConfig) : body;
  if (source === "gemini")
    for (const key of Object.keys(config))
      if (
        ![
          "maxOutputTokens",
          "temperature",
          "topP",
          "stopSequences",
          "responseMimeType",
          "responseSchema",
          "responseJsonSchema",
        ].includes(key)
      )
        unsupported(`generationConfig.${key}`);
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
        onlyKeys(
          fn,
          ["type", "name", "description", "parameters", "input_schema", "strict"],
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
    if (c.disable_parallel_tool_use !== undefined) unsupported("disable_parallel_tool_use");
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
  return result;
}

function imageSource(part: Extract<Part, { type: "image" }>, target: Protocol): unknown {
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
}

export function encodeRequest(
  request: NormalRequest,
  target: Protocol,
  model: string,
): Record<string, unknown> {
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
        if (part.type === "text") content.push({ type: "text", text: part.text });
        if (part.type === "image") content.push(imageSource(part, target));
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
        if (part.type === "text")
          content.push({
            type: message.role === "assistant" ? "output_text" : "input_text",
            text: part.text,
          });
        if (part.type === "image") content.push(imageSource(part, target));
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
      const parts = message.parts.map((part) => {
        if (part.type === "text")
          return target === "messages" ? { type: "text", text: part.text } : { text: part.text };
        if (part.type === "image") return imageSource(part, target);
        if (part.type === "call")
          return target === "messages"
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
              };
        if (target === "gemini" && !part.name) unsupported("tool result without function name");
        return target === "messages"
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
            };
      });
      messages.push(
        target === "messages"
          ? { role: message.role, content: parts }
          : { role: message.role === "assistant" ? "model" : "user", parts },
      );
    }
  }
  if (target === "chat") {
    body.messages = messages;
    if (request.maxTokens !== undefined) body.max_completion_tokens = request.maxTokens;
    if (request.stream) body.stream_options = { include_usage: true };
  } else if (target === "responses") {
    body.input = messages;
    body.store = false;
    if (request.maxTokens !== undefined) body.max_output_tokens = request.maxTokens;
  } else if (target === "messages") {
    body.messages = messages;
    if (system.length) body.system = system.join("\n\n");
    body.max_tokens = request.maxTokens ?? 32768;
  } else {
    delete body.stream;
    delete body.temperature;
    delete body.top_p;
    body.contents = messages;
    if (system.length) body.systemInstruction = { parts: system.map((value) => ({ text: value })) };
    body.generationConfig = {
      ...(request.maxTokens !== undefined ? { maxOutputTokens: request.maxTokens } : {}),
      ...(request.temperature !== undefined ? { temperature: request.temperature } : {}),
      ...(request.topP !== undefined ? { topP: request.topP } : {}),
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
  if (request.toolChoice) {
    const choice = request.toolChoice;
    if (target === "chat")
      body.tool_choice =
        typeof choice === "string" ? choice : { type: "function", function: { name: choice.name } };
    if (target === "responses")
      body.tool_choice =
        typeof choice === "string" ? choice : { type: "function", name: choice.name };
    if (target === "messages")
      body.tool_choice =
        typeof choice === "object"
          ? { type: "tool", name: choice.name }
          : { type: choice === "required" ? "any" : choice };
    if (target === "gemini")
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

export function prepareRequest(
  body: Record<string, unknown>,
  source: Protocol,
  kind: ProviderKind,
  model: string,
): { body: Record<string, unknown>; protocol: Protocol } {
  const target = providerProtocol(kind, source);
  const outbound =
    target === source
      ? copyExact(body, { ...body, model })
      : encodeRequest(decodeRequest(body, source), target, model);
  return { body: providerAdapter(kind).prepare(outbound, target), protocol: target };
}

export function prepareOperation(
  body: Record<string, unknown>,
  operation: Operation,
  kind: ProviderKind,
  model: string,
  policy?: RequestPolicy,
): { body: Record<string, unknown>; protocol: Operation } {
  if (["chat", "responses", "messages", "gemini"].includes(operation)) {
    const prepared = prepareRequest(body, operation as Protocol, kind, model);
    validateRequestPolicy(prepared.body, policy);
    return prepared;
  }
  if (!providerAdapter(kind).operations.includes(operation))
    throw new GatewayError(
      422,
      "unsupported_capability",
      "This provider does not support the requested native operation",
    );
  let outbound: Record<string, unknown> = copyExact(body, { ...body, model });
  if (operation === "batches" || operation === "files") delete outbound.model;
  if (kind === "gemini" && operation === "videos") {
    if (
      Object.keys(body).some(
        (field) => !["model", "prompt", "seconds", "aspect_ratio", "size"].includes(field),
      )
    )
      throw new GatewayError(
        422,
        "conversion_loss",
        "This video adapter supports prompt, duration and aspect ratio; unsupported fields cannot be converted",
      );
    const size = body.size;
    const aspect = size === "1280x720" ? "16:9" : size === "720x1280" ? "9:16" : undefined;
    if (
      (size !== undefined && !aspect) ||
      (aspect && body.aspect_ratio && aspect !== body.aspect_ratio)
    )
      throw new GatewayError(
        422,
        "conversion_loss",
        "Video size cannot be represented by this provider",
      );
    outbound = {
      instances: [{ prompt: body.prompt }],
      parameters: {
        durationSeconds: Number(body.seconds ?? 8),
        sampleCount: 1,
        ...(aspect || body.aspect_ratio ? { aspectRatio: aspect ?? body.aspect_ratio } : {}),
      },
    };
  }
  validateRequestPolicy(outbound, policy);
  return { body: outbound, protocol: operation };
}
