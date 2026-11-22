import { OPENAI_CHAT_ENDPOINTS } from "./openai-endpoints.ts";
import type { BuiltinProviderDefinition } from "./types.ts";

export const openaiCompatible = {
  name: "openai_compatible",
  version: "1.0.0",
  description: "Generic OpenAI-compatible HTTP provider template.",
  provider_config: {
    base_dialect: "openai_chat",
    usage_dialect: "openai",
    stream_framing: "sse",
    auth: { scheme: "bearer" },
    endpoints: OPENAI_CHAT_ENDPOINTS,
    include_stream_usage_option: true,
  },
} satisfies BuiltinProviderDefinition;
