import type { DeclarativeProviderConfig } from "@gateway/ledger";

export type BuiltinProviderPreset = {
  name: string;
  version: string;
  capability: "PROVIDER";
  kind: "DECLARATIVE";
  description: string;
  digest: string;
  provider_config: DeclarativeProviderConfig;
};

const OPENAI_CHAT_ENDPOINTS: DeclarativeProviderConfig["endpoints"] = {
  chat: { path: "/v1/chat/completions" },
  embeddings: { path: "/v1/embeddings" },
  moderations: { path: "/v1/moderations" },
  completions: { path: "/v1/completions" },
  images: { path: "/v1/images/generations" },
  image_edits: { path: "/v1/images/edits" },
  speech: { path: "/v1/audio/speech" },
  transcription: { path: "/v1/audio/transcriptions" },
  translation: { path: "/v1/audio/translations" },
  default: { path: "/v1/chat/completions" },
};

const OPENAI_STANDARD_ENDPOINTS: DeclarativeProviderConfig["endpoints"] = {
  ...OPENAI_CHAT_ENDPOINTS,
  responses: { path: "/v1/responses" },
};

const RAW_PRESETS: {
  name: string;
  version: string;
  description: string;
  provider_config: DeclarativeProviderConfig;
}[] = [
  {
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
  },
  {
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
  },
  {
    name: "anthropic",
    version: "1.0.0",
    description: "Anthropic Messages API provider with anthropic-version header.",
    provider_config: {
      base_dialect: "anthropic_messages",
      usage_dialect: "anthropic",
      stream_framing: "sse",
      auth: { scheme: "header", header_name: "x-api-key" },
      extra_headers: { "anthropic-version": "2023-06-01" },
      endpoints: {
        messages: { path: "/v1/messages" },
        chat: { path: "/v1/messages" },
        responses: { path: "/v1/messages" },
        gemini: { path: "/v1/messages" },
        default: { path: "/v1/messages" },
      },
    },
  },
  {
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
  },
  {
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
  },
  {
    name: "bedrock",
    version: "1.0.0",
    description:
      "AWS Bedrock Anthropic provider with SigV4 signing and binary eventstream framing.",
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
  },
  {
    name: "vertex",
    version: "1.0.0",
    description:
      "Google Cloud Vertex AI Gemini provider with OAuth2 service-account token exchange.",
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
  },
  {
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
  },
  {
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
  },
  {
    name: "qwen",
    version: "1.0.0",
    description: "Alibaba Cloud DashScope Qwen OpenAI-compatible provider.",
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
      error_rules: [
        {
          when_http_status: [200],
          path: "code",
          in: ["Throttling", "Throttling.RateQuota", "Throttling.AllocationQuota"],
          retry_kind: "rate_limit",
          message_path: "message",
        },
        {
          when_http_status: [200],
          path: "code",
          in: ["InvalidApiKey", "Arrearage"],
          retry_kind: "auth_error",
          message_path: "message",
        },
      ],
      endpoints: {
        chat: { path: "/compatible-mode/v1/chat/completions" },
        embeddings: { path: "/compatible-mode/v1/embeddings" },
        completions: { path: "/compatible-mode/v1/completions" },
        default: { path: "/compatible-mode/v1/chat/completions" },
      },
    },
  },
  {
    name: "volcengine",
    version: "1.0.0",
    description: "Volcengine Ark (Doubao) OpenAI-compatible provider.",
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
      endpoints: {
        chat: { path: "/api/v3/chat/completions" },
        embeddings: { path: "/api/v3/embeddings" },
        default: { path: "/api/v3/chat/completions" },
      },
    },
  },
  {
    name: "zhipu",
    version: "1.0.0",
    description:
      "Zhipu AI (GLM) OpenAI-compatible provider with temperature bounds and error mapping.",
    provider_config: {
      base_dialect: "openai_chat",
      usage_dialect: "openai",
      stream_framing: "sse",
      auth: { scheme: "bearer" },
      include_stream_usage_option: false,
      request_transforms: [
        { op: "rename", from: "max_completion_tokens", to: "max_tokens", overwrite: false },
        { op: "drop", path: "max_completion_tokens" },
        { op: "clamp", path: "temperature", min: 0.01, max: 0.99 },
      ],
      error_rules: [
        {
          when_http_status: [200],
          path: "error.code",
          in: ["1302", "1303", "1305"],
          retry_kind: "rate_limit",
          message_path: "error.message",
        },
        {
          when_http_status: [200],
          path: "error.code",
          in: ["1001", "1002", "1003", "1004", "1113"],
          retry_kind: "auth_error",
          message_path: "error.message",
        },
      ],
      endpoints: {
        chat: { path: "/api/paas/v4/chat/completions" },
        embeddings: { path: "/api/paas/v4/embeddings" },
        default: { path: "/api/paas/v4/chat/completions" },
      },
    },
  },
  {
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
  },
  {
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
  },
  {
    name: "openrouter",
    version: "1.0.0",
    description: "OpenRouter unified aggregator provider.",
    provider_config: {
      base_dialect: "openai_chat",
      usage_dialect: "openai",
      stream_framing: "sse",
      auth: { scheme: "bearer" },
      include_stream_usage_option: true,
      reasoning_field: "reasoning",
      endpoints: {
        chat: { path: "/api/v1/chat/completions" },
        embeddings: { path: "/api/v1/embeddings" },
        completions: { path: "/api/v1/completions" },
        default: { path: "/api/v1/chat/completions" },
      },
    },
  },
];

function computeDigest(name: string, version: string, cfg: DeclarativeProviderConfig): string {
  return new Bun.CryptoHasher("sha256")
    .update(JSON.stringify({ name, version, capability: "PROVIDER", kind: "DECLARATIVE", cfg }))
    .digest("hex");
}

const BUILTIN_PROVIDERS: readonly BuiltinProviderPreset[] = RAW_PRESETS.map((p) => ({
  name: p.name,
  version: p.version,
  capability: "PROVIDER",
  kind: "DECLARATIVE",
  description: p.description,
  digest: computeDigest(p.name, p.version, p.provider_config),
  provider_config: p.provider_config,
}));

const BUILTIN_PROVIDER_MAP = new Map<string, BuiltinProviderPreset>(
  BUILTIN_PROVIDERS.map((p) => [p.name, p]),
);

export function listBuiltinProviders(): BuiltinProviderPreset[] {
  return BUILTIN_PROVIDERS.map((p) => ({
    ...p,
    provider_config: structuredClone(p.provider_config),
  }));
}

export function getBuiltinProvider(name: string): BuiltinProviderPreset | null {
  const hit = BUILTIN_PROVIDER_MAP.get(name);
  return hit ? { ...hit, provider_config: structuredClone(hit.provider_config) } : null;
}

export function getBuiltinProviderReadonly(name: string): Readonly<BuiltinProviderPreset> | null {
  return BUILTIN_PROVIDER_MAP.get(name) ?? null;
}
