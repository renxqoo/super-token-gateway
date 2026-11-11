import { copyExact, optionalObject } from "@gateway/kernel";
import type { Operation, Protocol, ProviderKind } from "@gateway/kernel";

interface ProviderAdapter {
  protocol: (requested: Protocol) => Protocol;
  operations: readonly Operation[];
  prepare: (body: Record<string, unknown>, protocol: Protocol) => Record<string, unknown>;
}

function openaiProtocol(requested: Protocol): Protocol {
  return requested === "responses" ? "responses" : "chat";
}

function nativeRequest(body: Record<string, unknown>, protocol: Protocol): Record<string, unknown> {
  const native = copyExact(body, { ...body });
  if (protocol === "chat" && native.stream === true)
    native.stream_options = { ...optionalObject(native.stream_options), include_usage: true };
  if (protocol === "gemini") {
    delete native.model;
    delete native.stream;
  }
  return native;
}

function compatibleChat(
  body: Record<string, unknown>,
  protocol: Protocol,
): Record<string, unknown> {
  const native = nativeRequest(body, protocol);
  if (protocol === "chat" && native.max_completion_tokens !== undefined) {
    native.max_tokens = native.max_completion_tokens;
    delete native.max_completion_tokens;
  }
  return native;
}

const chat: ProviderAdapter = { protocol: () => "chat", operations: [], prepare: compatibleChat };
const messages: ProviderAdapter = {
  protocol: () => "messages",
  operations: [],
  prepare: nativeRequest,
};
const gemini: ProviderAdapter = {
  protocol: () => "gemini",
  operations: [],
  prepare: nativeRequest,
};
const adapters: Record<ProviderKind, ProviderAdapter> = {
  openai: {
    protocol: openaiProtocol,
    operations: [
      "embeddings",
      "images",
      "image_edits",
      "speech",
      "transcription",
      "translation",
      "files",
      "batches",
      "videos",
      "moderations",
      "completions",
    ],
    prepare: nativeRequest,
  },
  anthropic: messages,
  gemini: { ...gemini, operations: ["videos"] },
  deepseek: chat,
  ollama: { ...chat, protocol: openaiProtocol, operations: ["embeddings"] },
  azure: { protocol: () => "chat", operations: ["embeddings"], prepare: nativeRequest },
  bedrock: messages,
  vertex: gemini,
};

export function providerAdapter(kind: ProviderKind): ProviderAdapter {
  return adapters[kind];
}

export function providerProtocol(kind: ProviderKind, requested: Protocol): Protocol {
  return adapters[kind].protocol(requested);
}
