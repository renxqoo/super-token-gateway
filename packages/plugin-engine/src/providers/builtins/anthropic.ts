import type { BuiltinProviderDefinition } from "./types.ts";

export const anthropic = {
  name: "anthropic",
  version: "1.0.0",
  description: "Anthropic Messages API provider with anthropic-version header.",
  provider_config: {
    base_dialect: "anthropic_messages",
    usage_dialect: "anthropic",
    stream_framing: "sse",
    auth: { scheme: "header", header_name: "x-api-key" },
    extra_headers: { "anthropic-version": "2023-06-01" },
    endpoints: {
      messages: { path: "/v1/messages" },
      chat: { path: "/v1/messages" },
      responses: { path: "/v1/messages" },
      gemini: { path: "/v1/messages" },
      default: { path: "/v1/messages" },
    },
  },
} satisfies BuiltinProviderDefinition;
