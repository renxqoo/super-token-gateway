import { object, optionalObject, text } from "@gateway/contracts";
import { stringifyExact } from "./json.ts";
import type { ConversionMode, Part, Tool } from "./request-policy.ts";

export interface GeminiDecodeHelpers {
  mode: ConversionMode;
  recordDrop: (field: string) => void;
  unsupported: (field: string) => never;
  list: (value: unknown) => unknown[];
  onlyKeys: (value: Record<string, unknown>, allowed: readonly string[], path: string) => void;
  imageMime: (mime: unknown) => string;
  imageFromData: (data: unknown, mime: unknown) => Part;
  add: (role: unknown, parts: Part[]) => void;
  messageCount: () => number;
}

const jsonArguments = (value: unknown): string =>
  typeof value === "string" ? value : stringifyExact(value ?? {});

const isPresent = (value: unknown): boolean => value !== undefined && value !== null;

function pickAlias(
  obj: Record<string, unknown>,
  camel: string,
  snake: string,
  label: string,
  unsupported: (field: string) => never,
): unknown {
  if (isPresent(obj[camel]) && isPresent(obj[snake])) {
    unsupported(label);
  }
  return isPresent(obj[camel]) ? obj[camel] : isPresent(obj[snake]) ? obj[snake] : undefined;
}

export function decodeGeminiMessages(
  body: Record<string, unknown>,
  helpers: GeminiDecodeHelpers,
): void {
  const { mode, recordDrop, unsupported, list, onlyKeys, imageMime, imageFromData, add } = helpers;
  const sysRaw = pickAlias(
    body,
    "systemInstruction",
    "system_instruction",
    "systemInstruction",
    unsupported,
  );
  const system = optionalObject(sysRaw);
  onlyKeys(system, ["parts", "role"], "systemInstruction");
  if (system.parts) {
    add(
      "system",
      list(system.parts).map((raw) => {
        const part = object(raw);
        onlyKeys(part, ["text"], "systemInstruction.parts");
        return { type: "text", text: text(part.text) };
      }),
    );
  }
  for (const raw of list(body.contents)) {
    const message = object(raw);
    onlyKeys(message, ["role", "parts"], "content");
    const parts: Part[] = [];
    for (const value of list(message.parts)) {
      const part = object(value);
      const working: Record<string, unknown> = { ...part };
      if (isPresent(working.thoughtSignature) && isPresent(working.thought_signature)) {
        unsupported("Gemini thought/signature");
      }
      if (working.thoughtSignature === null) delete working.thoughtSignature;
      if (working.thought_signature === null) delete working.thought_signature;
      const sigKey =
        working.thoughtSignature !== undefined
          ? "thoughtSignature"
          : working.thought_signature !== undefined
            ? "thought_signature"
            : undefined;
      if (sigKey !== undefined) {
        if (mode === "compat") {
          recordDrop("part.thoughtSignature");
          delete working[sigKey];
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
        [
          "text",
          "inlineData",
          "inline_data",
          "fileData",
          "file_data",
          "functionCall",
          "function_call",
          "functionResponse",
          "function_response",
        ],
        "part",
      );
      if (Object.keys(working).length !== 1) unsupported("combined Gemini part fields");
      const inlineData = working.inlineData ?? working.inline_data;
      const fileData = working.fileData ?? working.file_data;
      const functionCall = working.functionCall ?? working.function_call;
      const functionResponse = working.functionResponse ?? working.function_response;
      if (typeof working.text === "string") {
        parts.push({ type: "text", text: working.text });
      } else if (inlineData) {
        const data = object(inlineData);
        onlyKeys(data, ["mimeType", "mime_type", "data"], "inlineData");
        const mime = pickAlias(data, "mimeType", "mime_type", "inlineData.mimeType", unsupported);
        parts.push(imageFromData(data.data, mime));
      } else if (fileData) {
        const data = object(fileData);
        onlyKeys(data, ["mimeType", "mime_type", "fileUri", "file_uri"], "fileData");
        const uri = pickAlias(data, "fileUri", "file_uri", "fileData.fileUri", unsupported);
        const mime = pickAlias(data, "mimeType", "mime_type", "fileData.mimeType", unsupported);
        parts.push({
          type: "image",
          url: text(uri),
          mime: imageMime(mime),
        });
      } else if (functionCall) {
        const call = object(functionCall);
        onlyKeys(call, ["id", "name", "args"], "functionCall");
        parts.push({
          type: "call",
          id: text(call.id, `call_${helpers.messageCount()}`),
          name: text(call.name),
          arguments: jsonArguments(call.args),
        });
      } else if (functionResponse) {
        const response = object(functionResponse);
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

const GEMINI_CONFIG_KEYS = new Set([
  "maxOutputTokens",
  "max_output_tokens",
  "temperature",
  "topP",
  "top_p",
  "topK",
  "top_k",
  "stopSequences",
  "stop_sequences",
  "responseMimeType",
  "response_mime_type",
  "responseSchema",
  "response_schema",
  "responseJsonSchema",
  "response_json_schema",
]);

const GEMINI_CONFIG_ALIAS_PAIRS: readonly [string, string][] = [
  ["maxOutputTokens", "max_output_tokens"],
  ["topP", "top_p"],
  ["topK", "top_k"],
  ["stopSequences", "stop_sequences"],
  ["responseMimeType", "response_mime_type"],
  ["responseSchema", "response_schema"],
  ["responseJsonSchema", "response_json_schema"],
];

export function extractGeminiGenerationConfig(
  body: Record<string, unknown>,
  mode: ConversionMode,
  recordDrop: (field: string) => void,
  unsupported: (field: string) => never,
): Record<string, unknown> {
  const raw = pickAlias(
    body,
    "generationConfig",
    "generation_config",
    "generationConfig",
    unsupported,
  );
  const config = optionalObject(raw);
  for (const key of Object.keys(config)) {
    if (GEMINI_CONFIG_KEYS.has(key)) continue;
    if (mode === "compat" && key === "seed") {
      recordDrop("generationConfig.seed");
      continue;
    }
    unsupported(`generationConfig.${key}`);
  }
  for (const [camel, snake] of GEMINI_CONFIG_ALIAS_PAIRS) {
    if (isPresent(config[camel]) && isPresent(config[snake])) {
      unsupported(`generationConfig.${camel}`);
    }
  }
  return config;
}

export function decodeGeminiTools(
  rawTools: unknown,
  helpers: Pick<GeminiDecodeHelpers, "list" | "onlyKeys" | "unsupported">,
): Tool[] {
  const { list, onlyKeys, unsupported } = helpers;
  const tools: Tool[] = [];
  for (const value of list(rawTools)) {
    const tool = object(value);
    onlyKeys(tool, ["functionDeclarations", "function_declarations"], "tool");
    const declarations = pickAlias(
      tool,
      "functionDeclarations",
      "function_declarations",
      "tool.functionDeclarations",
      unsupported,
    );
    if (!declarations) unsupported("hosted tools");
    for (const raw of list(declarations)) {
      const fn = object(raw);
      onlyKeys(
        fn,
        ["name", "description", "parameters", "parametersJsonSchema", "parameters_json_schema"],
        "tool.functionDeclarations",
      );
      const jsonSchema = pickAlias(
        fn,
        "parametersJsonSchema",
        "parameters_json_schema",
        "tool.functionDeclarations.parametersJsonSchema",
        unsupported,
      );
      tools.push({
        name: text(fn.name),
        description: text(fn.description),
        schema: optionalObject(fn.parameters ?? jsonSchema),
      });
    }
  }
  return tools;
}

export function extractGeminiToolChoice(
  body: Record<string, unknown>,
  onlyKeys: (value: Record<string, unknown>, allowed: readonly string[], path: string) => void,
  unsupported: (field: string) => never,
): Record<string, unknown> {
  const toolConfigRaw = pickAlias(body, "toolConfig", "tool_config", "toolConfig", unsupported);
  if (!toolConfigRaw) return {};
  const toolConfig = object(toolConfigRaw);
  onlyKeys(toolConfig, ["functionCallingConfig", "function_calling_config"], "toolConfig");
  const fcc = pickAlias(
    toolConfig,
    "functionCallingConfig",
    "function_calling_config",
    "toolConfig.functionCallingConfig",
    unsupported,
  );
  return optionalObject(fcc);
}
