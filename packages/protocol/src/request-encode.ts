import { GatewayError } from "@gateway/contracts";
import { stringifyExact, parseProviderJson } from "./json.ts";
import type { Protocol } from "@gateway/kernel";
import type {
  NormalRequest,
  EncodeRequestOptions,
  ConversionMode,
  Part,
} from "./request-policy.ts";

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
