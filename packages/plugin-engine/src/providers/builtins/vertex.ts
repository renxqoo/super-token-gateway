import type { BuiltinProviderDefinition } from "./types.ts";

export const vertex = {
  name: "vertex",
  version: "1.0.0",
  description: "Google Cloud Vertex AI Gemini provider with OAuth2 service-account token exchange.",
  provider_config: {
    base_dialect: "gemini",
    usage_dialect: "gemini",
    stream_framing: "sse",
    auth: { scheme: "gcp_vertex_oauth" },
    model_field: null,
    stream_field: null,
    endpoints: {
      default: {
        path: "/models/${encoded_model}:generateContent",
        stream_path: "/models/${encoded_model}:streamGenerateContent",
        stream_query: { alt: "sse" },
      },
    },
  },
} satisfies BuiltinProviderDefinition;
