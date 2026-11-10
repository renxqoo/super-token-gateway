import type { ProviderKind, Usage } from "./contracts.ts";
import { GatewayError, optionalObject } from "./errors.ts";
import { count } from "./cost.ts";
import { exactInteger, normalizedNumber, numberLiteral } from "./json.ts";

function seconds(source: Record<string, unknown>, key: string): string | undefined {
  const raw = source[key];
  if (raw === undefined || raw === null) return undefined;
  if (typeof raw !== "number" && typeof raw !== "string")
    throw new GatewayError(
      502,
      "invalid_upstream_usage",
      "Duration must be an exact nonnegative quantity",
    );
  const value = normalizedNumber(numberLiteral(source, key) ?? String(raw));
  if (value.negative || value.exponent < -18 || value.digits.length + value.exponent > 38)
    throw new GatewayError(502, "invalid_upstream_usage", "Duration exceeds precision budget");
  if (value.exponent >= 0) return value.digits + "0".repeat(value.exponent);
  const digits = value.digits.padStart(1 - value.exponent, "0");
  return `${digits.slice(0, value.exponent)}.${digits.slice(value.exponent)}`;
}

function quantity(source: Record<string, unknown>, key: string): string | undefined {
  if (source[key] === undefined || source[key] === null) return undefined;
  const value = exactInteger(source, key) ?? count(source[key]);
  if (value === undefined)
    throw new GatewayError(
      502,
      "invalid_upstream_usage",
      "Provider usage must contain exact nonnegative integer quantities",
    );
  return value;
}
function aliases(source: Record<string, unknown>, keys: string[]): string | undefined {
  let selected: string | undefined;
  for (const key of keys) {
    const value = quantity(source, key);
    if (value === undefined) continue;
    if (selected !== undefined && selected !== value)
      throw new GatewayError(502, "usage_alias_conflict", "Provider usage aliases disagree");
    selected = value;
  }
  return selected;
}
export function extractUsage(
  body: unknown,
  kind: ProviderKind,
  previous?: Usage,
): Usage | undefined {
  const value = optionalObject(body);
  const response = optionalObject(value.response);
  const raw = optionalObject(
    kind === "gemini" ? value.usageMetadata : (value.usage ?? response.usage),
  );
  const details = optionalObject(
    raw.prompt_tokens_details ?? raw.input_tokens_details ?? raw.input_token_details,
  );
  const outputDetails = optionalObject(raw.completion_tokens_details ?? raw.output_tokens_details);
  const input = aliases(raw, ["prompt_tokens", "input_tokens", "promptTokenCount"]);
  const output = aliases(raw, ["completion_tokens", "output_tokens", "candidatesTokenCount"]);
  const cachedAliases = [
    aliases(details, ["cached_tokens"]),
    aliases(raw, ["cache_read_input_tokens", "cachedContentTokenCount", "prompt_cache_hit_tokens"]),
  ].filter((v): v is string => v !== undefined);
  if (new Set(cachedAliases).size > 1)
    throw new GatewayError(502, "usage_alias_conflict", "Provider cache usage aliases disagree");
  const observedCached = cachedAliases[0];
  const cacheMiss = quantity(raw, "prompt_cache_miss_tokens");
  if (
    cacheMiss !== undefined &&
    input !== undefined &&
    observedCached !== undefined &&
    BigInt(cacheMiss) + BigInt(observedCached) !== BigInt(input)
  )
    throw new GatewayError(
      502,
      "usage_alias_conflict",
      "Provider cache partition does not equal prompt usage",
    );
  const write = quantity(raw, "cache_creation_input_tokens");
  const observedReasoning =
    quantity(outputDetails, "reasoning_tokens") ?? quantity(raw, "thoughtsTokenCount");
  const audioSeconds =
    seconds(raw, "audio_seconds") ??
    (raw.type === "duration" ? seconds(raw, "seconds") : undefined);
  const videoSeconds = seconds(raw, "video_seconds");
  if (
    input === undefined &&
    output === undefined &&
    audioSeconds === undefined &&
    videoSeconds === undefined &&
    observedCached === undefined &&
    write === undefined &&
    observedReasoning === undefined
  )
    return undefined;
  const usage: Usage = { ...previous };
  const cached = observedCached ?? previous?.cached_input_tokens;
  const reasoning = observedReasoning ?? previous?.reasoning_tokens;
  const totalInput =
    input ??
    (previous?.input_tokens !== undefined && kind !== "anthropic"
      ? (BigInt(previous.input_tokens) + BigInt(previous.cached_input_tokens ?? "0")).toString()
      : undefined);
  const candidateOutput =
    output ??
    (kind === "gemini" && previous?.output_tokens !== undefined
      ? (BigInt(previous.output_tokens) - BigInt(previous.reasoning_tokens ?? "0")).toString()
      : undefined);
  if (audioSeconds !== undefined) usage.audio_seconds = audioSeconds;
  if (videoSeconds !== undefined) usage.video_seconds = videoSeconds;
  if (totalInput !== undefined || input !== undefined) {
    const uncached =
      BigInt(totalInput ?? input ?? "0") - (kind === "anthropic" ? 0n : BigInt(cached ?? "0"));
    if (uncached < 0n)
      throw new GatewayError(
        502,
        "invalid_upstream_usage",
        "Provider cached usage exceeds total input",
      );
    usage.input_tokens = uncached.toString();
  }
  if (candidateOutput !== undefined)
    usage.output_tokens = (
      BigInt(candidateOutput) + (kind === "gemini" ? BigInt(reasoning ?? "0") : 0n)
    ).toString();
  if (
    kind !== "gemini" &&
    reasoning !== undefined &&
    usage.output_tokens !== undefined &&
    BigInt(reasoning) > BigInt(usage.output_tokens)
  )
    throw new GatewayError(502, "invalid_upstream_usage", "Reasoning usage exceeds total output");
  if (cached !== undefined) usage.cached_input_tokens = cached;
  if (write !== undefined) usage.cache_write_tokens = write;
  if (reasoning !== undefined) usage.reasoning_tokens = reasoning;
  return usage;
}
export function mergeUsage(
  previous: Usage | undefined,
  next: Usage | undefined,
): Usage | undefined {
  return next ? { ...previous, ...next } : previous;
}
