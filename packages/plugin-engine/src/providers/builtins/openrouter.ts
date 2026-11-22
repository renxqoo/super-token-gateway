import type { BuiltinProviderDefinition } from "./types.ts";

export const openrouter = {
  name: "openrouter",
  version: "1.0.0",
  description: "OpenRouter unified aggregator provider.",
  provider_config: {
    base_dialect: "openai_chat",
    usage_dialect: "openai",
    stream_framing: "sse",
    auth: { scheme: "bearer" },
    include_stream_usage_option: true,
    reasoning_field: "reasoning",
    endpoints: {
      chat: { path: "/api/v1/chat/completions" },
      embeddings: { path: "/api/v1/embeddings" },
      completions: { path: "/api/v1/completions" },
      default: { path: "/api/v1/chat/completions" },
    },
  },
} satisfies BuiltinProviderDefinition;
