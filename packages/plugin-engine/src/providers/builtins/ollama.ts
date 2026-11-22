import { OPENAI_STANDARD_ENDPOINTS } from "./openai-endpoints.ts";
import type { BuiltinProviderDefinition } from "./types.ts";

export const ollama = {
  name: "ollama",
  version: "1.0.0",
  description: "Ollama local/self-hosted OpenAI-compatible provider.",
  provider_config: {
    base_dialect: "openai_chat",
    usage_dialect: "openai",
    stream_framing: "sse",
    auth: { scheme: "bearer" },
    include_stream_usage_option: true,
    request_transforms: [
      { op: "rename", from: "max_completion_tokens", to: "max_tokens", overwrite: false },
      { op: "drop", path: "max_completion_tokens" },
    ],
    endpoints: OPENAI_STANDARD_ENDPOINTS,
  },
} satisfies BuiltinProviderDefinition;
