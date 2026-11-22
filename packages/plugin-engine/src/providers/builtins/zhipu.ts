import type { BuiltinProviderDefinition } from "./types.ts";

export const zhipu = {
  name: "zhipu",
  version: "1.0.0",
  description:
    "Zhipu AI (GLM) OpenAI-compatible provider with temperature bounds and error mapping.",
  provider_config: {
    base_dialect: "openai_chat",
    usage_dialect: "openai",
    stream_framing: "sse",
    auth: { scheme: "bearer" },
    include_stream_usage_option: false,
    request_transforms: [
      { op: "rename", from: "max_completion_tokens", to: "max_tokens", overwrite: false },
      { op: "drop", path: "max_completion_tokens" },
      { op: "clamp", path: "temperature", min: 0.01, max: 0.99 },
    ],
    error_rules: [
      {
        when_http_status: [200],
        path: "error.code",
        in: ["1302", "1303", "1305"],
        retry_kind: "rate_limit",
        message_path: "error.message",
      },
      {
        when_http_status: [200],
        path: "error.code",
        in: ["1001", "1002", "1003", "1004", "1113"],
        retry_kind: "auth_error",
        message_path: "error.message",
      },
    ],
    endpoints: {
      chat: { path: "/api/paas/v4/chat/completions" },
      embeddings: { path: "/api/paas/v4/embeddings" },
      default: { path: "/api/paas/v4/chat/completions" },
    },
  },
} satisfies BuiltinProviderDefinition;
