import type { BuiltinProviderDefinition } from "./types.ts";

export const bedrock = {
  name: "bedrock",
  version: "1.0.0",
  description: "AWS Bedrock Anthropic provider with SigV4 signing and binary eventstream framing.",
  provider_config: {
    base_dialect: "anthropic_messages",
    usage_dialect: "anthropic",
    stream_framing: "bedrock_eventstream",
    auth: { scheme: "aws_sigv4", service: "bedrock" },
    model_field: null,
    stream_field: null,
    request_transforms: [
      { op: "drop", path: "model" },
      { op: "drop", path: "stream" },
      { op: "force_set", path: "anthropic_version", value: "bedrock-2023-05-31" },
    ],
    endpoints: {
      default: {
        path: "/model/${encoded_model}/invoke",
        stream_path: "/model/${encoded_model}/invoke-with-response-stream",
      },
    },
  },
} satisfies BuiltinProviderDefinition;
