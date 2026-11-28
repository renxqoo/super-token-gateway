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
const geminiInputs = new WeakMap<Usage, { prompt: string | undefined; tool: string | undefined }>();

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
  const previousGeminiInput =
    kind === "gemini" && previous ? geminiInputs.get(previous) : undefined;
  const geminiPrompt = prompt ?? previousGeminiInput?.prompt;
  const geminiTool = toolUse ?? previousGeminiInput?.tool;
  const input =
    kind === "gemini" && geminiPrompt !== undefined && geminiTool !== undefined
      ? (BigInt(geminiPrompt) + BigInt(geminiTool)).toString()
      : geminiPrompt;
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
  const serverToolUse = asRecord(raw.server_tool_use);
  const serverToolUseExact = serverToolUse ? hasExactLiterals(serverToolUse) : false;
  const outputArray =
    kind === "openai"
      ? Array.isArray(value.output)
        ? value.output
        : Array.isArray(asRecord(value.response)?.output)
          ? (asRecord(value.response)!.output as unknown[])
          : undefined
      : undefined;
  let outputWebSearchCount = 0;
  let outputCodeInterpreterCount = 0;
  let outputFileSearchCount = 0;
  if (outputArray) {
    for (let i = 0; i < outputArray.length; i++) {
      const item = asRecord(outputArray[i]);
      const type = item?.type;
      if (type === "web_search_call") outputWebSearchCount++;
      else if (type === "code_interpreter_call") outputCodeInterpreterCount++;
      else if (type === "file_search_call") outputFileSearchCount++;
    }
  }
  const outputWebSearch = outputWebSearchCount > 0 ? String(outputWebSearchCount) : undefined;
  const outputCodeInterpreter =
    outputCodeInterpreterCount > 0 ? String(outputCodeInterpreterCount) : undefined;
  const outputFileSearch = outputFileSearchCount > 0 ? String(outputFileSearchCount) : undefined;
  const webSearchRaw = quantity(raw, "web_search_calls", rawExact);
  const webSearchAnthropic =
    serverToolUse && serverToolUse.web_search_requests !== undefined
      ? quantity(serverToolUse, "web_search_requests", serverToolUseExact)
      : undefined;
  if (
    (webSearchRaw !== undefined &&
      webSearchAnthropic !== undefined &&
      webSearchRaw !== webSearchAnthropic) ||
    (webSearchRaw !== undefined &&
      outputWebSearch !== undefined &&
      webSearchRaw !== outputWebSearch) ||
    (webSearchAnthropic !== undefined &&
      outputWebSearch !== undefined &&
      webSearchAnthropic !== outputWebSearch)
  ) {
    throw new GatewayError(
      502,
      "usage_alias_conflict",
      "Provider web search usage aliases disagree",
    );
  }
  const webSearchCalls = webSearchRaw ?? webSearchAnthropic ?? outputWebSearch;
  const codeInterpreterRaw = quantity(raw, "code_interpreter_sessions", rawExact);
  const codeExecutionAnthropic =
    serverToolUse && serverToolUse.code_execution_requests !== undefined
      ? quantity(serverToolUse, "code_execution_requests", serverToolUseExact)
      : undefined;
  if (
    (codeInterpreterRaw !== undefined &&
      codeExecutionAnthropic !== undefined &&
      codeInterpreterRaw !== codeExecutionAnthropic) ||
    (codeInterpreterRaw !== undefined &&
      outputCodeInterpreter !== undefined &&
      codeInterpreterRaw !== outputCodeInterpreter) ||
    (codeExecutionAnthropic !== undefined &&
      outputCodeInterpreter !== undefined &&
      codeExecutionAnthropic !== outputCodeInterpreter)
  ) {
    throw new GatewayError(
      502,
      "usage_alias_conflict",
      "Provider code execution usage aliases disagree",
    );
  }
  const codeInterpreterSessions =
    codeInterpreterRaw ?? codeExecutionAnthropic ?? outputCodeInterpreter;
  const fileSearchRaw = quantity(raw, "file_search_calls", rawExact);
  if (
    fileSearchRaw !== undefined &&
    outputFileSearch !== undefined &&
    fileSearchRaw !== outputFileSearch
  ) {
    throw new GatewayError(
      502,
      "usage_alias_conflict",
      "Provider file search usage aliases disagree",
    );
  }
  const fileSearchCalls = fileSearchRaw ?? outputFileSearch;
  const searchUnits = quantity(raw, "search_units", rawExact);
  const pages = quantity(raw, "pages", rawExact);
  const imageInputTokens = quantity(raw, "image_input_tokens", rawExact);
  const imageOutputTokens = quantity(raw, "image_output_tokens", rawExact);
  const audioInputTokens = quantity(raw, "audio_input_tokens", rawExact);
  const audioOutputTokens = quantity(raw, "audio_output_tokens", rawExact);
  const videoInputTokens = quantity(raw, "video_input_tokens", rawExact);
  const hasExtendedUsage =
    webSearchCalls !== undefined ||
    codeInterpreterSessions !== undefined ||
    fileSearchCalls !== undefined ||
    searchUnits !== undefined ||
    pages !== undefined ||
    imageInputTokens !== undefined ||
    imageOutputTokens !== undefined ||
    audioInputTokens !== undefined ||
    audioOutputTokens !== undefined ||
    videoInputTokens !== undefined;
  if (
    input === undefined &&
    toolUse === undefined &&
    output === undefined &&
    !audioDuration &&
    !videoDuration &&
    !hasExtendedUsage &&
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
  if (webSearchCalls !== undefined) usage.web_search_calls = webSearchCalls;
  if (codeInterpreterSessions !== undefined) {
    usage.code_interpreter_sessions = codeInterpreterSessions;
  }
  if (fileSearchCalls !== undefined) usage.file_search_calls = fileSearchCalls;
  if (searchUnits !== undefined) usage.search_units = searchUnits;
  if (pages !== undefined) usage.pages = pages;
  if (imageInputTokens !== undefined) usage.image_input_tokens = imageInputTokens;
  if (imageOutputTokens !== undefined) usage.image_output_tokens = imageOutputTokens;
  if (audioInputTokens !== undefined) usage.audio_input_tokens = audioInputTokens;
  if (audioOutputTokens !== undefined) usage.audio_output_tokens = audioOutputTokens;
  if (videoInputTokens !== undefined) usage.video_input_tokens = videoInputTokens;
  if (kind === "gemini" && (geminiPrompt !== undefined || geminiTool !== undefined)) {
    geminiInputs.set(usage, { prompt: geminiPrompt, tool: geminiTool });
  }
  return usage;
}
