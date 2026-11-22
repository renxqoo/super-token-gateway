import { OPENAI_STANDARD_ENDPOINTS } from "./openai-endpoints.ts";
import type { BuiltinProviderDefinition } from "./types.ts";

export const openai = {
  name: "openai",
  version: "1.0.0",
  description:
    "OpenAI official API provider supporting Chat, Responses, Embeddings, Images, Audio, and Moderations.",
  provider_config: {
    base_dialect: "openai_chat",
    usage_dialect: "openai",
    stream_framing: "sse",
    auth: { scheme: "bearer" },
    endpoints: OPENAI_STANDARD_ENDPOINTS,
    include_stream_usage_option: true,
  },
} satisfies BuiltinProviderDefinition;
