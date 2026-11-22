import type { DeclarativeProviderConfig } from "@gateway/ledger";
import { openai } from "./builtins/openai.ts";
import { openaiCompatible } from "./builtins/openai-compatible.ts";
import { anthropic } from "./builtins/anthropic.ts";
import { gemini } from "./builtins/gemini.ts";
import { azure } from "./builtins/azure.ts";
import { bedrock } from "./builtins/bedrock.ts";
import { vertex } from "./builtins/vertex.ts";
import { deepseek } from "./builtins/deepseek.ts";
import { ollama } from "./builtins/ollama.ts";
import { qwen } from "./builtins/qwen.ts";
import { volcengine } from "./builtins/volcengine.ts";
import { zhipu } from "./builtins/zhipu.ts";
import { moonshot } from "./builtins/moonshot.ts";
import { minimax } from "./builtins/minimax.ts";
import { openrouter } from "./builtins/openrouter.ts";
import type { BuiltinProviderDefinition } from "./builtins/types.ts";

export type BuiltinProviderPreset = BuiltinProviderDefinition & {
  capability: "PROVIDER";
  kind: "DECLARATIVE";
  digest: string;
};

const RAW_PRESETS: readonly BuiltinProviderDefinition[] = [
  openai,
  openaiCompatible,
  anthropic,
  gemini,
  azure,
  bedrock,
  vertex,
  deepseek,
  ollama,
  qwen,
  volcengine,
  zhipu,
  moonshot,
  minimax,
  openrouter,
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

export function builtinProviderRevisionId(digest: string): string {
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-5${digest.slice(13, 16)}-8${digest.slice(17, 20)}-${digest.slice(20, 32)}`;
}
