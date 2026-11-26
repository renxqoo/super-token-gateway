import { exactInteger, hasExactLiterals, numberLiteral, normalizedProviderNumber } from "./json.ts";
import {
  cacheWritePartitionMatches,
  count,
  dimensions,
  type Dimension,
  type ProviderKind,
  type Usage,
} from "@gateway/kernel";
import { GatewayError } from "@gateway/contracts";

const allowedUsageKeys = new Set<string>([...dimensions, "reasoning_tokens"]);
const decimalDurationKeys = new Set<string>(["audio_seconds", "video_seconds"]);

export function readUsageQuantity(
  source: Record<string, unknown>,
  key: string,
  dimension: string,
): string | undefined {
  const value = source[key];
  if (value === undefined || value === null) return undefined;
  if (!allowedUsageKeys.has(dimension)) {
    throw new GatewayError(
      502,
      "invalid_upstream_usage",
      `Unsupported usage dimension ${dimension}`,
    );
  }
  if (decimalDurationKeys.has(dimension)) return seconds(source, key, hasExactLiterals(source));
  const quantity =
    typeof value === "number" &&
    !hasExactLiterals(source) &&
    Number.isSafeInteger(value) &&
    value >= 0
      ? String(value)
      : (exactInteger(source, key) ?? count(value));
  if (quantity === undefined) {
    throw new GatewayError(
      502,
      "invalid_upstream_usage",
      `Invalid nonnegative integer for usage.${dimension}`,
    );
  }
  return quantity;
}

export function validateUsageObject(raw: unknown): Usage {
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
    throw new GatewayError(502, "invalid_upstream_usage", "Usage payload must be an object");
  }
  const obj = raw as Record<string, unknown>;
  const out: Usage = {};
  let hasAny = false;
  for (const key of Object.keys(obj)) {
    const quantity = readUsageQuantity(obj, key, key);
    if (quantity === undefined) continue;
    out[key as Dimension | "reasoning_tokens"] = quantity;
    hasAny = true;
  }
  if (!hasAny) {
    throw new GatewayError(502, "invalid_upstream_usage", "Usage payload contains no dimensions");
  }
  if (
    out.reasoning_tokens !== undefined &&
    out.output_tokens !== undefined &&
    BigInt(out.reasoning_tokens) > BigInt(out.output_tokens)
  ) {
    throw new GatewayError(502, "invalid_upstream_usage", "Reasoning usage exceeds total output");
  }
  if (
    !cacheWritePartitionMatches(
      out.cache_write_tokens,
      out.cache_write_5m_tokens,
      out.cache_write_1h_tokens,
    )
  ) {
    throw new GatewayError(502, "usage_alias_conflict", "Provider cache write partitions disagree");
  }
  return out;
}

function seconds(source: Record<string, unknown>, key: string, exact: boolean): string | undefined {
  const raw = source[key];
  if (raw === undefined || raw === null) return undefined;
  if (typeof raw !== "number" && typeof raw !== "string")
    throw new GatewayError(
      502,
      "invalid_upstream_usage",
      "Duration must be an exact nonnegative quantity",
    );
  return decimalDuration((exact ? numberLiteral(source, key) : undefined) ?? String(raw));
}

function decimalDuration(literal: string): string {
  const value = normalizedProviderNumber(literal);
  if (value.negative || value.exponent < -18 || value.digits.length + value.exponent > 38)
    throw new GatewayError(502, "invalid_upstream_usage", "Duration exceeds precision budget");
  if (value.exponent >= 0) return value.digits + "0".repeat(value.exponent);
  const digits = value.digits.padStart(1 - value.exponent, "0");
  return `${digits.slice(0, value.exponent)}.${digits.slice(value.exponent)}`;
}

function quantity(
  source: Record<string, unknown>,
  key: string,
  exact: boolean,
): string | undefined {
  const raw = source[key];
  if (raw === undefined || raw === null) return undefined;
  if (typeof raw === "number" && !exact && Number.isSafeInteger(raw) && raw >= 0) {
    return String(raw);
  }
  const value = (exact ? exactInteger(source, key) : undefined) ?? count(raw);
  if (value === undefined)
    throw new GatewayError(
      502,
      "invalid_upstream_usage",
      "Provider usage must contain exact nonnegative integer quantities",
    );
  return value;
}

function alias3(
  source: Record<string, unknown>,
  k0: string,
  k1: string,
  k2: string,
  exact: boolean,
): string | undefined {
  let selected: string | undefined;
  const v0 = source[k0];
  if (v0 !== undefined && v0 !== null) {
    selected =
      typeof v0 === "number" && !exact && Number.isSafeInteger(v0) && v0 >= 0
        ? String(v0)
        : quantity(source, k0, exact);
  }
  const v1 = source[k1];
  if (v1 !== undefined && v1 !== null) {
    const val =
      typeof v1 === "number" && !exact && Number.isSafeInteger(v1) && v1 >= 0
        ? String(v1)
        : quantity(source, k1, exact);
    if (val !== undefined) {
      if (selected !== undefined && selected !== val)
        throw new GatewayError(502, "usage_alias_conflict", "Provider usage aliases disagree");
      selected = val;
    }
  }
  const v2 = source[k2];
  if (v2 !== undefined && v2 !== null) {
    const val =
      typeof v2 === "number" && !exact && Number.isSafeInteger(v2) && v2 >= 0
        ? String(v2)
        : quantity(source, k2, exact);
    if (val !== undefined) {
      if (selected !== undefined && selected !== val)
        throw new GatewayError(502, "usage_alias_conflict", "Provider usage aliases disagree");
      selected = val;
    }
  }
  return selected;
}

function measureDuration(
  usage: Usage,
  source: Record<string, unknown>,
  secondsKey: "audio_seconds" | "video_seconds",
  millisecondsKey: "audio_ms" | "video_ms",
  allowBareSeconds: boolean,
  exact: boolean,
): boolean {
  const hasBare = allowBareSeconds && source.type === "duration" && source.seconds !== undefined;
  if (source[secondsKey] === undefined && source[millisecondsKey] === undefined && !hasBare) {
    return false;
  }
  const reported = seconds(source, secondsKey, exact);
  const alias = hasBare ? seconds(source, "seconds", exact) : undefined;
  const milliseconds = quantity(source, millisecondsKey, exact);
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

function asRecord(v: unknown): Record<string, unknown> | undefined {
  return v !== null && typeof v === "object" && !Array.isArray(v)
    ? (v as Record<string, unknown>)
    : undefined;
}

export function extractUsage(
  body: unknown,
  kind: ProviderKind | "custom",
  previous?: Usage,
): Usage | undefined {
  if (kind === "bedrock") kind = "anthropic";
  if (kind === "vertex") kind = "gemini";
  if (kind === "custom") kind = "openai";
  const value = asRecord(body);
  if (!value) return undefined;
  const rawCandidate =
    kind === "gemini" ? value.usageMetadata : (value.usage ?? asRecord(value.response)?.usage);
  const raw = asRecord(rawCandidate);
  if (!raw) return undefined;
  const rawExact = hasExactLiterals(raw);
  const details = asRecord(
    raw.prompt_tokens_details ?? raw.input_tokens_details ?? raw.input_token_details,
  );
  const outputDetails = asRecord(raw.completion_tokens_details ?? raw.output_tokens_details);
  const prompt = alias3(raw, "prompt_tokens", "input_tokens", "promptTokenCount", rawExact);
  const toolUse =
    kind === "gemini" ? quantity(raw, "toolUsePromptTokenCount", rawExact) : undefined;
  const input =
    toolUse === undefined ? prompt : (BigInt(prompt ?? "0") + BigInt(toolUse)).toString();
  const output = alias3(
    raw,
    "completion_tokens",
    "output_tokens",
    "candidatesTokenCount",
    rawExact,
  );
  const cachedDetails =
    details && details.cached_tokens !== undefined
      ? quantity(details, "cached_tokens", hasExactLiterals(details))
      : undefined;
  const cachedRaw = alias3(
    raw,
    "cache_read_input_tokens",
    "cachedContentTokenCount",
    "prompt_cache_hit_tokens",
    rawExact,
  );
  if (cachedDetails !== undefined && cachedRaw !== undefined && cachedDetails !== cachedRaw)
    throw new GatewayError(502, "usage_alias_conflict", "Provider cache usage aliases disagree");
  const observedCached = cachedDetails ?? cachedRaw;
  const cacheMiss =
    raw.prompt_cache_miss_tokens !== undefined
      ? quantity(raw, "prompt_cache_miss_tokens", rawExact)
      : undefined;
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
  const creation = asRecord(raw.cache_creation);
  const creationExact = creation ? hasExactLiterals(creation) : false;
  const write5m =
    creation && creation.ephemeral_5m_input_tokens !== undefined
      ? quantity(creation, "ephemeral_5m_input_tokens", creationExact)
      : undefined;
  const write1h =
    creation && creation.ephemeral_1h_input_tokens !== undefined
      ? quantity(creation, "ephemeral_1h_input_tokens", creationExact)
      : undefined;
  const reportedWrite = quantity(raw, "cache_creation_input_tokens", rawExact);
  const write =
    reportedWrite ??
    (write5m !== undefined && write1h !== undefined
      ? (BigInt(write5m) + BigInt(write1h)).toString()
      : undefined);
  if (
    (write !== undefined && write.length > 38) ||
    !cacheWritePartitionMatches(reportedWrite, write5m, write1h)
  )
    throw new GatewayError(502, "usage_alias_conflict", "Provider cache write partitions disagree");
  const observedReasoning =
    (outputDetails && outputDetails.reasoning_tokens !== undefined
      ? quantity(outputDetails, "reasoning_tokens", hasExactLiterals(outputDetails))
      : undefined) ??
    (raw.thoughtsTokenCount !== undefined
      ? quantity(raw, "thoughtsTokenCount", rawExact)
      : undefined);
  const usage: Usage = previous ? { ...previous } : {};
  const audioDuration = measureDuration(usage, raw, "audio_seconds", "audio_ms", true, rawExact);
  const videoDuration = measureDuration(usage, raw, "video_seconds", "video_ms", false, rawExact);
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
      ? previous.cached_input_tokens === undefined
        ? previous.input_tokens
        : (BigInt(previous.input_tokens) + BigInt(previous.cached_input_tokens)).toString()
      : undefined);
  const candidateOutput =
    output ??
    (kind === "gemini" && previous?.output_tokens !== undefined
      ? previous.reasoning_tokens === undefined
        ? previous.output_tokens
        : (BigInt(previous.output_tokens) - BigInt(previous.reasoning_tokens)).toString()
      : undefined);
  if (totalInput !== undefined || input !== undefined) {
    const total = totalInput ?? input ?? "0";
    if (kind === "anthropic" || cached === undefined || cached === "0") {
      usage.input_tokens = total;
    } else {
      const uncached = BigInt(total) - BigInt(cached);
      if (uncached < 0n)
        throw new GatewayError(
          502,
          "invalid_upstream_usage",
          "Provider cached usage exceeds total input",
        );
      usage.input_tokens = uncached.toString();
    }
  }
  if (candidateOutput !== undefined) {
    usage.output_tokens =
      kind === "gemini" && reasoning !== undefined && reasoning !== "0"
        ? (BigInt(candidateOutput) + BigInt(reasoning)).toString()
        : candidateOutput;
  }
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
