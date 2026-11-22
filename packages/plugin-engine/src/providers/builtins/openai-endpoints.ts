import type { DeclarativeProviderConfig } from "@gateway/ledger";

export const OPENAI_CHAT_ENDPOINTS: DeclarativeProviderConfig["endpoints"] = {
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

export const OPENAI_STANDARD_ENDPOINTS: DeclarativeProviderConfig["endpoints"] = {
  ...OPENAI_CHAT_ENDPOINTS,
  responses: { path: "/v1/responses" },
};
