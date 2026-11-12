export const providerKinds = [
  "openai",
  "anthropic",
  "gemini",
  "deepseek",
  "ollama",
  "azure",
  "bedrock",
  "vertex",
] as const;
export type ProviderKind = (typeof providerKinds)[number];
export const protocols = ["chat", "responses", "messages", "gemini"] as const;
export type Protocol = (typeof protocols)[number];
export const operations = [
  ...protocols,
  "embeddings",
  "images",
  "image_edits",
  "speech",
  "transcription",
  "translation",
  "files",
  "batches",
  "videos",
  "music",
  "moderations",
  "completions",
] as const;
export type Operation = (typeof operations)[number];
export const dimensions = [
  "input_tokens",
  "output_tokens",
  "cached_input_tokens",
  "cache_write_tokens",
  "cache_write_5m_tokens",
  "cache_write_1h_tokens",
  "requests",
  "images",
  "audio_ms",
  "music_ms",
  "video_ms",
  "audio_seconds",
  "video_seconds",
  "characters",
] as const;
export type Dimension = (typeof dimensions)[number];
export type Usage = Partial<Record<Dimension | "reasoning_tokens", string>>;
export type Finality = "FINAL" | "PARTIAL" | "UNKNOWN" | "ESTIMATED";

export interface Pricing {
  currency: string;
  rates: Partial<Record<Dimension, { numerator: string; denominator: string }>>;
}
export interface BillingQuote {
  currency: string;
  amount_atoms: string;
  input_characters: string;
  prices: Record<string, string>;
}
