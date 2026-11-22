import { OPENAI_CHAT_ENDPOINTS } from "./openai-endpoints.ts";
import type { BuiltinProviderDefinition } from "./types.ts";

export const moonshot = {
  name: "moonshot",
  version: "1.0.0",
  description: "Moonshot AI (Kimi) OpenAI-compatible provider with temperature clamp.",
  provider_config: {
    base_dialect: "openai_chat",
    usage_dialect: "openai",
    stream_framing: "sse",
    auth: { scheme: "bearer" },
    include_stream_usage_option: true,
    request_transforms: [
      { op: "rename", from: "max_completion_tokens", to: "max_tokens", overwrite: false },
      { op: "drop", path: "max_completion_tokens" },
      { op: "clamp", path: "temperature", min: 0, max: 1 },
    ],
    endpoints: OPENAI_CHAT_ENDPOINTS,
  },
} satisfies BuiltinProviderDefinition;
