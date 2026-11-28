import type { Protocol } from "@gateway/kernel";

export type ConversionMode = "strict" | "compat";

export type FieldPolicyAction = "preserve" | "map" | "drop_hint" | "reject";

export type FieldPolicyEntry = {
  field: string;
  protocols: readonly Protocol[];
  action: FieldPolicyAction;
  note: string;
};

export const FIELD_CONVERSION_POLICY: readonly FieldPolicyEntry[] = [
  {
    field: "messages",
    protocols: ["chat", "responses", "messages", "gemini"],
    action: "preserve",
    note: "Conversation turns and text/image/tool parts",
  },
  {
    field: "temperature",
    protocols: ["chat", "responses", "messages", "gemini"],
    action: "preserve",
    note: "Sampling temperature",
  },
  {
    field: "top_p",
    protocols: ["chat", "responses", "messages", "gemini"],
    action: "preserve",
    note: "Nucleus sampling parameter",
  },
  {
    field: "max_tokens",
    protocols: ["chat", "responses", "messages", "gemini"],
    action: "map",
    note: "Mapped across max_completion_tokens, max_output_tokens, max_tokens, maxOutputTokens",
  },
  {
    field: "parallel_tool_calls",
    protocols: ["chat", "responses", "messages"],
    action: "map",
    note: "Mapped bidirectionally with Anthropic tool_choice.disable_parallel_tool_use",
  },
  {
    field: "top_k",
    protocols: ["messages", "gemini"],
    action: "map",
    note: "Preserved between Messages and Gemini; dropped with hint on OpenAI in compat mode",
  },
  {
    field: "stop",
    protocols: ["chat", "messages", "gemini"],
    action: "map",
    note: "Mapped across stop, stop_sequences, and stopSequences; dropped with hint on Responses in compat mode",
  },
  {
    field: "reasoning",
    protocols: ["chat", "responses", "messages", "gemini"],
    action: "map",
    note: "Preserved in responses and streams; unsigned history reasoning stripped across protocols",
  },
  {
    field: "reasoning_effort",
    protocols: ["chat", "responses", "messages"],
    action: "map",
    note: "Mapped bidirectionally across chat reasoning_effort, responses reasoning.effort, and messages output_config.effort; rejected on Gemini",
  },
  {
    field: "metadata",
    protocols: ["chat", "responses", "messages"],
    action: "drop_hint",
    note: "Non-semantic caller tags dropped with header hint in compat mode",
  },
  {
    field: "user",
    protocols: ["chat", "responses"],
    action: "drop_hint",
    note: "Caller end-user identifier dropped with header hint in compat mode",
  },
  {
    field: "seed",
    protocols: ["chat", "gemini"],
    action: "drop_hint",
    note: "Best-effort sampling seed dropped with header hint in compat mode",
  },
  {
    field: "service_tier",
    protocols: ["chat", "responses", "messages"],
    action: "drop_hint",
    note: "Provider-specific scheduling tier dropped with header hint in compat mode",
  },
  {
    field: "cache_control",
    protocols: ["messages"],
    action: "drop_hint",
    note: "Anthropic prompt cache hints dropped with header hint in compat mode",
  },
  {
    field: "n",
    protocols: ["chat"],
    action: "reject",
    note: "n > 1 changes candidate count and billing multiplicity",
  },
  {
    field: "store",
    protocols: ["chat", "responses"],
    action: "reject",
    note: "store: true requires stateful server continuation",
  },
  {
    field: "logprobs",
    protocols: ["chat"],
    action: "reject",
    note: "Token log-probabilities cannot be synthesized cross-protocol",
  },
  {
    field: "strict",
    protocols: ["chat", "responses"],
    action: "reject",
    note: "Strict grammar-constrained schema validation cannot be weakened silently",
  },
  {
    field: "hosted_tools",
    protocols: ["chat", "responses", "messages", "gemini"],
    action: "reject",
    note: "Provider-hosted or versioned built-in tools cannot be emulated cross-protocol",
  },
];

export const COMPAT_DROP_TOP_LEVEL: Record<Protocol, ReadonlySet<string>> = {
  chat: new Set([
    "metadata",
    "user",
    "seed",
    "service_tier",
    "frequency_penalty",
    "presence_penalty",
    "prompt_cache_key",
    "reasoning_split",
  ]),
  responses: new Set([
    "metadata",
    "user",
    "service_tier",
    "truncation",
    "include",
    "prompt_cache_key",
  ]),
  messages: new Set(["metadata", "service_tier", "cache_control"]),
  gemini: new Set(["seed", "safetySettings", "safety_settings", "serviceTier", "service_tier"]),
};

export type DecodeRequestOptions = {
  mode?: ConversionMode;
  onFieldDropped?: (field: string) => void;
  onFieldRejected?: (field: string) => void;
};

export type EncodeRequestOptions = {
  mode?: ConversionMode;
  onFieldDropped?: (field: string) => void;
  onFieldRejected?: (field: string) => void;
  droppedFields?: string[];
  defaultMaxTokens?: number;
};

export type Part =
  | { type: "text"; text: string }
  | { type: "reasoning"; text: string; signature?: string }
  | { type: "image"; url: string; mime: string }
  | { type: "call"; id: string; name: string; arguments: string }
  | { type: "result"; id: string; name: string; content: unknown };

export interface Message {
  role: "system" | "user" | "assistant";
  parts: Part[];
}

export interface Tool {
  name: string;
  description: string;
  schema: Record<string, unknown>;
}

export interface NormalRequest {
  messages: Message[];
  tools: Tool[];
  stream: boolean;
  maxTokens?: number;
  temperature?: number;
  topP?: number;
  topK?: number;
  stop?: string[];
  parallelToolCalls?: boolean;
  reasoningEffort?: string;
  reasoningEffortField?: string;
  toolChoice?: "auto" | "none" | "required" | { name: string };
  jsonSchema?: Record<string, unknown>;
  droppedFields?: string[];
}
