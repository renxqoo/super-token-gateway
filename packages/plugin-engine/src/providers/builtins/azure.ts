import type { BuiltinProviderDefinition } from "./types.ts";

export const azure = {
  name: "azure",
  version: "1.0.0",
  description: "Azure OpenAI Service provider with deployment routing and api-version query.",
  provider_config: {
    base_dialect: "openai_chat",
    usage_dialect: "openai",
    stream_framing: "sse",
    auth: { scheme: "azure_key", default_api_version: "2024-10-21" },
    include_stream_usage_option: true,
    endpoints: {
      embeddings: { path: "/openai/deployments/${encoded_model}/embeddings" },
      completions: { path: "/openai/deployments/${encoded_model}/completions" },
      default: { path: "/openai/deployments/${encoded_model}/chat/completions" },
    },
  },
} satisfies BuiltinProviderDefinition;
