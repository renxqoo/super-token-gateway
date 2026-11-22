import type { BuiltinProviderDefinition } from "./types.ts";

export const minimax = {
  name: "minimax",
  version: "1.0.0",
  description: "MiniMax chatcompletion_v2 provider with base_resp.status_code error detection.",
  provider_config: {
    base_dialect: "openai_chat",
    usage_dialect: "openai",
    stream_framing: "sse",
    auth: { scheme: "bearer" },
    include_stream_usage_option: false,
    request_transforms: [
      { op: "rename", from: "max_completion_tokens", to: "max_tokens", overwrite: false },
      { op: "drop", path: "max_completion_tokens" },
      { op: "clamp", path: "temperature", min: 0.01, max: 1 },
    ],
    error_rules: [
      {
        when_http_status: [200],
        path: "base_resp.status_code",
        equals: 1002,
        retry_kind: "rate_limit",
        message_path: "base_resp.status_msg",
      },
      {
        when_http_status: [200],
        path: "base_resp.status_code",
        in: [1004, 1008],
        retry_kind: "auth_error",
        message_path: "base_resp.status_msg",
      },
      {
        when_http_status: [200],
        path: "base_resp.status_code",
        in: [1000, 1001, 2013],
        retry_kind: "retryable",
        message_path: "base_resp.status_msg",
      },
    ],
    endpoints: {
      chat: { path: "/v1/text/chatcompletion_v2" },
      embeddings: { path: "/v1/embeddings" },
      default: { path: "/v1/text/chatcompletion_v2" },
    },
  },
} satisfies BuiltinProviderDefinition;
