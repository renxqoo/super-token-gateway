import { OPENAI_CHAT_ENDPOINTS } from "./openai-endpoints.ts";
import type { BuiltinProviderDefinition } from "./types.ts";

export const deepseek = {
  name: "deepseek",
  version: "1.0.0",
  description:
    "DeepSeek provider converting max_completion_tokens to max_tokens with cache hit usage mapping.",
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
    usage_aliases: [
      { target: "cached_input_tokens", source_path: "usage.prompt_cache_hit_tokens" },
    ],
    endpoints: OPENAI_CHAT_ENDPOINTS,
  },
} satisfies BuiltinProviderDefinition;
