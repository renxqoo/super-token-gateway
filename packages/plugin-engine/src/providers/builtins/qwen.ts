import type { BuiltinProviderDefinition } from "./types.ts";

export const qwen = {
  name: "qwen",
  version: "1.0.0",
  description: "Alibaba Cloud DashScope Qwen OpenAI-compatible provider.",
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
    error_rules: [
      {
        when_http_status: [200],
        path: "code",
        in: ["Throttling", "Throttling.RateQuota", "Throttling.AllocationQuota"],
        retry_kind: "rate_limit",
        message_path: "message",
      },
      {
        when_http_status: [200],
        path: "code",
        in: ["InvalidApiKey", "Arrearage"],
        retry_kind: "auth_error",
        message_path: "message",
      },
    ],
    endpoints: {
      chat: { path: "/compatible-mode/v1/chat/completions" },
      embeddings: { path: "/compatible-mode/v1/embeddings" },
      completions: { path: "/compatible-mode/v1/completions" },
      default: { path: "/compatible-mode/v1/chat/completions" },
    },
  },
} satisfies BuiltinProviderDefinition;
