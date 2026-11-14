import { numberLiteral, normalizedProviderNumber } from "./json.ts";
import type { ProviderKind, Usage } from "@gateway/kernel";
import { GatewayError, optionalObject } from "@gateway/contracts";
import { count } from "@gateway/kernel";
import { exactInteger } from "./json.ts";

function seconds(source: Record<string, unknown>, key: string): string | undefined {
  const raw = source[key];
  if (raw === undefined || raw === null) return undefined;
  if (typeof raw !== "number" && typeof raw !== "string")
    throw new GatewayError(
      502,
      "invalid_upstream_usage",
      "Duration must be an exact nonnegative quantity",
    );
  return decimalDuration(numberLiteral(source, key) ?? String(raw));
}

function decimalDuration(literal: string): string {
  const value = normalizedProviderNumber(literal);
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

function measureDuration(
  usage: Usage,
  source: Record<string, unknown>,
  media: "audio" | "video",
): boolean {
  const secondsKey = `${media}_seconds` as const;
  const millisecondsKey = `${media}_ms` as const;
  const reported = seconds(source, secondsKey);
  const alias =
    media === "audio" && source.type === "duration" ? seconds(source, "seconds") : undefined;
  const milliseconds = quantity(source, millisecondsKey);
  const fromMilliseconds =
    milliseconds === undefined ? undefined : decimalDuration(`${milliseconds}e-3`);
  const duration = reported ?? alias ?? fromMilliseconds;
  if (duration === undefined) return false;
  if (
    (alias !== undefined && alias !== duration) ||
    (fromMilliseconds !== undefined && fromMilliseconds !== duration)
  )
    throw new GatewayError(502, "usage_alias_conflict", "Provider duration usage aliases disagree");

  usage[secondsKey] = duration;
  const scaled = normalizedProviderNumber(`${duration}e3`);
  if (scaled.exponent >= 0 && scaled.digits.length + scaled.exponent <= 38)
    usage[millisecondsKey] = scaled.digits + "0".repeat(scaled.exponent);
  else delete usage[millisecondsKey];
  return true;
}

export function extractUsage(
  body: unknown,
  kind: ProviderKind,
  previous?: Usage,
): Usage | undefined {
  if (kind === "bedrock") kind = "anthropic";
  if (kind === "vertex") kind = "gemini";
  const value = optionalObject(body);
  const response = optionalObject(value.response);
  const raw = optionalObject(
    kind === "gemini" ? value.usageMetadata : (value.usage ?? response.usage),
  );
  const details = optionalObject(
    raw.prompt_tokens_details ?? raw.input_tokens_details ?? raw.input_token_details,
  );
  const outputDetails = optionalObject(raw.completion_tokens_details ?? raw.output_tokens_details);
  const prompt = aliases(raw, ["prompt_tokens", "input_tokens", "promptTokenCount"]);
  const toolUse = kind === "gemini" ? quantity(raw, "toolUsePromptTokenCount") : undefined;
  const input =
    toolUse === undefined ? prompt : (BigInt(prompt ?? "0") + BigInt(toolUse)).toString();
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
  const creation = optionalObject(raw.cache_creation);
  const write5m = quantity(creation, "ephemeral_5m_input_tokens");
  const write1h = quantity(creation, "ephemeral_1h_input_tokens");
  let write = quantity(raw, "cache_creation_input_tokens");
  if (write5m !== undefined && write1h !== undefined) {
    const total = (BigInt(write5m) + BigInt(write1h)).toString();
    if ((write !== undefined && write !== total) || total.length > 38)
      throw new GatewayError(
        502,
        "usage_alias_conflict",
        "Provider cache write partitions disagree",
      );
    write = total;
  }
  const observedReasoning =
    quantity(outputDetails, "reasoning_tokens") ?? quantity(raw, "thoughtsTokenCount");
  const usage: Usage = { ...previous };
  const audioDuration = measureDuration(usage, raw, "audio");
  const videoDuration = measureDuration(usage, raw, "video");
  if (
    input === undefined &&
    output === undefined &&
    !audioDuration &&
    !videoDuration &&
    observedCached === undefined &&
    write === undefined &&
    write5m === undefined &&
    write1h === undefined &&
    observedReasoning === undefined
  )
    return undefined;
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
  if (write !== undefined || write5m !== undefined || write1h !== undefined) {
    delete usage.cache_write_tokens;
    delete usage.cache_write_5m_tokens;
    delete usage.cache_write_1h_tokens;
    if (write !== undefined) usage.cache_write_tokens = write;
    if (write5m !== undefined) usage.cache_write_5m_tokens = write5m;
    if (write1h !== undefined) usage.cache_write_1h_tokens = write1h;
  }
  if (reasoning !== undefined) usage.reasoning_tokens = reasoning;
  return usage;
}
