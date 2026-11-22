import type { BuiltinProviderDefinition } from "./types.ts";

export const volcengine = {
  name: "volcengine",
  version: "1.0.0",
  description: "Volcengine Ark (Doubao) OpenAI-compatible provider.",
  provider_config: {
    base_dialect: "openai_chat",
    usage_dialect: "openai",
    stream_framing: "sse",
    auth: { scheme: "bearer" },
    include_stream_usage_option: true,
    reasoning_field: "reasoning_content",
    request_transforms: [
      { op: "rename", from: "max_completion_tokens", to: "max_tokens", overwrite: false },
      { op: "drop", path: "max_completion_tokens" },
    ],
    endpoints: {
      chat: { path: "/api/v3/chat/completions" },
      embeddings: { path: "/api/v3/embeddings" },
      default: { path: "/api/v3/chat/completions" },
    },
  },
} satisfies BuiltinProviderDefinition;
