import type { BuiltinProviderDefinition } from "./types.ts";

export const gemini = {
  name: "gemini",
  version: "1.0.0",
  description: "Google Gemini Developer API provider with URL-bound model and SSE query.",
  provider_config: {
    base_dialect: "gemini",
    usage_dialect: "gemini",
    stream_framing: "sse",
    auth: { scheme: "header", header_name: "x-goog-api-key" },
    model_field: null,
    stream_field: null,
    endpoints: {
      default: {
        path: "/v1beta/models/${encoded_model}:generateContent",
        stream_path: "/v1beta/models/${encoded_model}:streamGenerateContent",
        stream_query: { alt: "sse" },
      },
    },
  },
} satisfies BuiltinProviderDefinition;
