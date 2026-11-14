import {
  AdapterFailure,
  LosslessNumber,
  decode64,
  exactParse,
  exactStringify,
  integer,
  object,
  runGuest,
  smallInteger,
  type AdapterFailureCode,
  type GuestInput,
} from "@gateway/plugin-sdk/guest";
import manifest from "./package/manifest.json";
declare const Host: { inputString(): string; outputString(value: string): void };
type Usage = Record<string, string>;
type Fact = {
  kind: string;
  bytes: number;
  count?: number;
  empty?: boolean;
  literal?: string;
};
type DocumentInput = {
  document: { handle: string; bytes: number };
  inspection: Record<string, Fact>;
  protocol?: string;
  operation?: string;
  model?: string;
  stream?: boolean;
};
type FrameInput = DocumentInput & {
  frame: {
    handle: string;
    ordinal: number;
    event: { value?: string };
    data: { handle: string; bytes: number; preview?: string };
  };
};
type State = {
  session: string;
  sequence: number;
  model: string;
  lastFrame: number;
  id: string;
  tools: number;
  usage: Usage | null;
  done: boolean;
  native: boolean;
};
class Slot {
  constructor(readonly spec: Record<string, unknown>) {}
}
const copy = (from: string) => new Slot({ from, mode: "value" });
const parsed = (from: string) => new Slot({ from, mode: "parse" });
const serialized = (from: string) => new Slot({ from, mode: "stringify" });
const joined = (sources: string[]) => new Slot({ sources, mode: "concat" });
const number = (value: string | number) => new LosslessNumber(String(value));
function fail(code: AdapterFailureCode, status: 400 | 413 | 422 | 502 = 502): never {
  throw new AdapterFailure(status, code);
}
function compose(handle: string, tree: unknown) {
  const slots: Record<string, unknown>[] = [];
  function walk(node: unknown, path: string): unknown {
    if (node instanceof Slot) {
      slots.push({ at: path, ...node.spec });
      return null;
    }
    if (node instanceof LosslessNumber) return node;
    if (Array.isArray(node)) return node.map((item, i) => walk(item, `${path}/${i}`));
    if (node && typeof node === "object") {
      const out: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(node))
        out[key] = walk(value, `${path}/${key.replace(/~/g, "~0").replace(/\//g, "~1")}`);
      return out;
    }
    return node;
  }
  const template = exactStringify(walk(tree, ""));
  return { document: handle, compose: { template, slots } };
}
function fact(value: DocumentInput, path: string): Fact {
  return value.inspection[path] ?? { kind: "missing", bytes: 0 };
}
function scalar(value: DocumentInput, path: string, status: 400 | 502 = 502): unknown {
  const found = fact(value, path);
  if (found.kind === "missing") return undefined;
  if (found.literal === undefined) fail("scalar_budget", status);
  return exactParse(found.literal, status);
}
function keys(value: DocumentInput, path: string): string[] {
  const prefix = `${path}/`;
  return Object.keys(value.inspection)
    .filter(
      (key) =>
        key.startsWith(prefix) &&
        !key.slice(prefix.length).includes("/") &&
        value.inspection[key]!.kind !== "missing",
    )
    .map((key) => key.slice(prefix.length).replace(/~1/g, "/").replace(/~0/g, "~"));
}
function inspect(value: DocumentInput, pointers: string[]) {
  return { abi_version: 1, kind: "inspect", document: value.document.handle, pointers };
}
function count(value: DocumentInput, path: string): number {
  return smallInteger(fact(value, path).count ?? 0, 100000);
}
const requestKeys = [
  "model",
  "messages",
  "max_tokens",
  "max_completion_tokens",
  "stream",
  "stream_options",
  "temperature",
  "top_p",
  "stop",
  "tools",
  "tool_choice",
  "n",
  "parallel_tool_calls",
];
const firstRound = [
  "/*",
  "/messages/*/*",
  "/messages/*/content/*/type",
  "/messages/*/tool_calls/*/function/arguments",
  "/tools/*/type",
  "/tools/*/function/*",
  "/model",
  "/n",
  "/stream",
  "/max_tokens",
  "/max_completion_tokens",
  "/stream_options/include_usage",
  "/tool_choice",
  "/tool_choice/function/name",
  "/stop",
  "/parallel_tool_calls",
];
const secondRound = [
  "/tool_choice/type",
  "/messages/*/content/*/*",
  "/messages/*/tool_calls/*/type",
  "/messages/*/tool_calls/*/id",
  "/messages/*/tool_calls/*/function/name",
  "/stop/*",
];
function textPointers(value: DocumentInput, base: string) {
  const pointers: string[] = [];
  for (let p = 0; p < count(value, base); p++) {
    if (scalar(value, `${base}/${p}/type`, 400) !== "text") fail("unsupported_semantics", 422);
    if (fact(value, `${base}/${p}/text`).kind !== "string") fail("invalid_messages", 400);
    pointers.push(`${base}/${p}/text`);
  }
  return pointers;
}
function convertMessages(value: DocumentInput) {
  const system: unknown[] = [],
    contents: unknown[] = [],
    names: Record<string, string> = {};
  let responses: unknown[] | null = null,
    conversation = false;
  const total = count(value, "/messages");
  if (fact(value, "/messages").kind !== "array" || !total) fail("invalid_messages", 400);
  const allowed: Record<string, string[]> = {
    system: ["role", "content"],
    developer: ["role", "content"],
    user: ["role", "content"],
    assistant: ["role", "content", "tool_calls", "refusal"],
    tool: ["role", "content", "tool_call_id"],
  };
  for (let i = 0; i < total; i++) {
    const base = `/messages/${i}`,
      role = scalar(value, `${base}/role`, 400),
      content = fact(value, `${base}/content`);
    if (typeof role !== "string" || !allowed[role]) fail("invalid_messages", 400);
    if (keys(value, base).some((key) => !allowed[role]!.includes(key)))
      fail("unsupported_semantics", 422);
    if (role !== "tool") responses = null;
    if (role === "system" || role === "developer") {
      if (conversation) fail("unsupported_semantics", 422);
      if (content.kind === "string") system.push({ text: copy(`${base}/content`) });
      else if (content.kind === "array")
        system.push(...textPointers(value, `${base}/content`).map((p) => ({ text: copy(p) })));
      else fail("invalid_messages", 400);
      continue;
    }
    conversation = true;
    if (role === "user") {
      if (content.kind === "string")
        contents.push({ role: "user", parts: [{ text: copy(`${base}/content`) }] });
      else if (content.kind === "array")
        contents.push({
          role: "user",
          parts: textPointers(value, `${base}/content`).map((p) => ({ text: copy(p) })),
        });
      else fail("invalid_messages", 400);
      continue;
    }
    if (role === "tool") {
      const id = scalar(value, `${base}/tool_call_id`, 400);
      if (typeof id !== "string" || !Object.hasOwn(names, id)) fail("invalid_messages", 400);
      const result =
        content.kind === "string"
          ? copy(`${base}/content`)
          : content.kind === "array"
            ? joined(textPointers(value, `${base}/content`))
            : fail("invalid_messages", 400);
      const part = {
        functionResponse: {
          id: copy(`${base}/tool_call_id`),
          name: copy(names[id]!),
          response: { result },
        },
      };
      if (responses) responses.push(part);
      else {
        responses = [part];
        contents.push({ role: "user", parts: responses });
      }
      continue;
    }
    const refusal = fact(value, `${base}/refusal`);
    if (refusal.kind !== "missing" && refusal.kind !== "null") fail("unsupported_semantics", 422);
    const parts: unknown[] = [];
    if (content.kind === "string") {
      if (!content.empty) parts.push({ text: copy(`${base}/content`) });
    } else if (content.kind === "array")
      parts.push(...textPointers(value, `${base}/content`).map((p) => ({ text: copy(p) })));
    else if (content.kind !== "null" && content.kind !== "missing") fail("invalid_messages", 400);
    const calls = fact(value, `${base}/tool_calls`);
    if (calls.kind === "array")
      for (let t = 0; t < count(value, `${base}/tool_calls`); t++) {
        const call = `${base}/tool_calls/${t}`,
          id = scalar(value, `${call}/id`, 400);
        if (
          scalar(value, `${call}/type`, 400) !== "function" ||
          typeof id !== "string" ||
          !id ||
          fact(value, `${call}/function/name`).kind !== "string" ||
          fact(value, `${call}/function/arguments`).kind !== "string"
        )
          fail("invalid_tool_call", 400);
        names[id] = `${call}/function/name`;
        parts.push({
          functionCall: {
            id: copy(`${call}/id`),
            name: copy(`${call}/function/name`),
            args: fact(value, `${call}/function/arguments`).empty
              ? {}
              : parsed(`${call}/function/arguments`),
          },
        });
      }
    else if (calls.kind !== "missing" && calls.kind !== "null") fail("invalid_tool_call", 400);
    if (!parts.length) fail("invalid_messages", 400);
    contents.push({ role: "model", parts });
  }
  if (!contents.length) fail("invalid_messages", 400);
  return { system, contents };
}
function convertTools(value: DocumentInput) {
  const kind = fact(value, "/tools").kind;
  if (kind === "missing" || kind === "null") return undefined;
  if (kind !== "array") fail("invalid_request", 400);
  const declarations: unknown[] = [];
  for (let t = 0; t < count(value, "/tools"); t++) {
    const base = `/tools/${t}`;
    if (scalar(value, `${base}/type`, 400) !== "function") fail("unsupported_semantics", 422);
    if (
      keys(value, `${base}/function`).some(
        (key) => !["name", "description", "parameters", "strict"].includes(key),
      )
    )
      fail("unsupported_semantics", 422);
    if (fact(value, `${base}/function/name`).kind !== "string") fail("invalid_request", 400);
    if (scalar(value, `${base}/function/strict`, 400) === true) fail("unsupported_semantics", 422);
    const description = fact(value, `${base}/function/description`).kind,
      parameters = fact(value, `${base}/function/parameters`).kind;
    if (parameters !== "missing" && parameters !== "object") fail("invalid_request", 400);
    declarations.push({
      name: copy(`${base}/function/name`),
      ...(description === "string" ? { description: copy(`${base}/function/description`) } : {}),
      ...(parameters === "object"
        ? { parametersJsonSchema: copy(`${base}/function/parameters`) }
        : {}),
    });
  }
  return declarations.length ? [{ functionDeclarations: declarations }] : undefined;
}
function convertChoice(value: DocumentInput) {
  const choice = fact(value, "/tool_choice"),
    parallel = scalar(value, "/parallel_tool_calls", 400);
  if (parallel !== undefined && typeof parallel !== "boolean") fail("invalid_request", 400);
  if (parallel === false) fail("unsupported_semantics", 422);
  if (choice.kind === "missing" || choice.kind === "null") return undefined;
  if (choice.kind === "string") {
    const mode = scalar(value, "/tool_choice", 400);
    if (mode === "auto") return { functionCallingConfig: { mode: "AUTO" } };
    if (mode === "required") return { functionCallingConfig: { mode: "ANY" } };
    if (mode === "none") return { functionCallingConfig: { mode: "NONE" } };
    fail("unsupported_semantics", 422);
  }
  if (
    scalar(value, "/tool_choice/type", 400) !== "function" ||
    fact(value, "/tool_choice/function/name").kind !== "string"
  )
    fail("unsupported_semantics", 422);
  return {
    functionCallingConfig: {
      mode: "ANY",
      allowedFunctionNames: [copy("/tool_choice/function/name")],
    },
  };
}
function upstreamModel(input: GuestInput, value: DocumentInput) {
  const map = object(input.config.model_map, 400),
    upstream = map[value.model!];
  if (typeof upstream !== "string" || !/^[A-Za-z0-9._-]{1,128}$/.test(upstream))
    fail("model_unavailable", 422);
  return upstream;
}
function target(input: GuestInput, upstream: string, stream: boolean) {
  const auth =
    input.config.auth === "api-key"
      ? { type: "api-key", header: "x-goog-api-key" }
      : input.config.auth === "none"
        ? { type: "none" }
        : input.config.auth === "google-oauth"
          ? { type: "google-oauth" }
          : { type: "bearer" };
  const method = stream ? "streamGenerateContent?alt=sse" : "generateContent";
  let path = `/v1beta/models/${upstream}:${method}`;
  if (input.config.path_style === "vertex") {
    const project = input.config.project,
      location = input.config.location;
    if (
      typeof project !== "string" ||
      !/^[a-z][a-z0-9-]{4,61}[a-z0-9]$/.test(project) ||
      typeof location !== "string" ||
      !/^[a-z0-9-]{1,40}$/.test(location)
    )
      fail("unsupported_capability", 422);
    path = `/v1/projects/${project}/locations/${location}/publishers/google/models/${upstream}:${method}`;
  }
  return {
    path,
    auth,
    headers: {
      "content-type": "application/json",
      accept: stream ? "text/event-stream" : "application/json",
    },
  };
}
function prepareNative(input: GuestInput, value: DocumentInput) {
  if (typeof value.stream !== "boolean") fail("invalid_stream", 400);
  if (!Object.hasOwn(value.inspection, "/model"))
    return inspect(value, [
      "/*",
      "/model",
      "/generationConfig",
      "/generationConfig/maxOutputTokens",
      "/generationConfig/candidateCount",
    ]);
  if (fact(value, "/model").kind !== "missing") fail("unsupported_semantics", 422);
  const upstream = upstreamModel(input, value);
  const max = smallInteger(input.config.max_output_tokens, 1000000, 422),
    inputTokens = smallInteger(input.config.max_input_tokens, 16 << 20, 422);
  if (value.document.bytes > inputTokens) fail("input_ceiling", 413);
  const config = fact(value, "/generationConfig").kind;
  if (config !== "missing" && config !== "object") fail("invalid_request", 400);
  const candidates = scalar(value, "/generationConfig/candidateCount", 400);
  if (candidates !== undefined && smallInteger(candidates, 128, 422) !== 1)
    fail("unsupported_choice_count", 422);
  const requested = scalar(value, "/generationConfig/maxOutputTokens", 400);
  const output = requested === undefined ? max : smallInteger(requested, max, 422);
  if (output < 1) fail("invalid_output_ceiling", 422);
  const edits =
    requested !== undefined
      ? []
      : config === "object"
        ? [{ op: "set", pointer: "/generationConfig/maxOutputTokens", value: String(output) }]
        : [{ op: "set", pointer: "/generationConfig", value: `{"maxOutputTokens":${output}}` }];
  return {
    abi_version: 1,
    kind: "prepared",
    request: {
      method: "POST",
      ...target(input, upstream, value.stream),
      body: { document: value.document.handle, edits },
      manifest: {
        operation: "gemini",
        model: value.model,
        upstream_model: upstream,
        stream: value.stream,
        ceilings: {
          input_tokens: String(inputTokens),
          output_tokens: String(output),
          requests: "1",
        },
        input_bytes: value.document.bytes,
      },
    },
  };
}
function prepare(input: GuestInput, value: DocumentInput) {
  if (!value.document || !value.inspection || typeof value.model !== "string" || !value.model)
    fail("invalid_request", 400);
  if (value.protocol === "gemini" && value.operation === "gemini")
    return prepareNative(input, value);
  if (value.protocol !== "chat" || value.operation !== "chat") fail("unsupported_semantics", 422);
  if (!Object.hasOwn(value.inspection, "/model")) return inspect(value, firstRound);
  if (!Object.hasOwn(value.inspection, "/tool_choice/type")) return inspect(value, secondRound);
  if (keys(value, "").some((key) => !requestKeys.includes(key))) fail("unsupported_semantics", 422);
  const upstream = upstreamModel(input, value),
    model = scalar(value, "/model", 400);
  if (model !== undefined && model !== value.model) fail("model_unavailable", 422);
  const n = scalar(value, "/n", 400),
    stream = scalar(value, "/stream", 400);
  if (n !== undefined && smallInteger(n, 128, 422) !== 1) fail("unsupported_choice_count", 422);
  if (stream !== undefined && typeof stream !== "boolean") fail("invalid_stream", 400);
  if (stream === true && scalar(value, "/stream_options/include_usage", 400) === false)
    fail("usage_required", 422);
  const max = smallInteger(input.config.max_output_tokens, 1000000, 422),
    inputTokens = smallInteger(input.config.max_input_tokens, 16 << 20, 422);
  if (value.document.bytes > inputTokens) fail("input_ceiling", 413);
  const maxTokens = scalar(value, "/max_tokens", 400),
    maxCompletion = scalar(value, "/max_completion_tokens", 400);
  if (maxTokens !== undefined && maxCompletion !== undefined)
    fail("conflicting_output_ceiling", 422);
  const output = smallInteger(maxCompletion ?? maxTokens ?? max, max, 422);
  if (output < 1) fail("invalid_output_ceiling", 422);
  const { system, contents } = convertMessages(value),
    tools = convertTools(value),
    toolConfig = convertChoice(value);
  const generation: Record<string, unknown> = { maxOutputTokens: number(output) };
  for (const [name, target] of [
    ["temperature", "temperature"],
    ["top_p", "topP"],
  ] as const) {
    const kind = fact(value, `/${name}`).kind;
    if (kind === "number") generation[target] = copy(`/${name}`);
    else if (kind !== "missing" && kind !== "null") fail("invalid_request", 400);
  }
  const stop = fact(value, "/stop");
  if (stop.kind === "string") generation.stopSequences = [copy("/stop")];
  else if (stop.kind === "array") {
    for (let s = 0; s < count(value, "/stop"); s++)
      if (fact(value, `/stop/${s}`).kind !== "string") fail("invalid_request", 400);
    generation.stopSequences = copy("/stop");
  } else if (stop.kind !== "missing" && stop.kind !== "null") fail("invalid_request", 400);
  const body: Record<string, unknown> = { contents };
  if (system.length) body.systemInstruction = { parts: system };
  if (tools) body.tools = tools;
  if (toolConfig) body.toolConfig = toolConfig;
  body.generationConfig = generation;
  return {
    abi_version: 1,
    kind: "prepared",
    request: {
      method: "POST",
      ...target(input, upstream, stream === true),
      body: compose(value.document.handle, body),
      manifest: {
        operation: "chat",
        model: value.model,
        upstream_model: upstream,
        stream: stream === true,
        ceilings: {
          input_tokens: String(inputTokens),
          output_tokens: String(output),
          requests: "1",
        },
        input_bytes: value.document.bytes,
      },
    },
  };
}
function quantity(value: DocumentInput, path: string): bigint | undefined {
  const raw = scalar(value, path);
  return raw === undefined || raw === null ? undefined : BigInt(integer(raw));
}
function measure(value: DocumentInput, previous: Usage | null): Usage | null {
  const prompt = quantity(value, "/usageMetadata/promptTokenCount"),
    toolUse = quantity(value, "/usageMetadata/toolUsePromptTokenCount"),
    candidates = quantity(value, "/usageMetadata/candidatesTokenCount"),
    cached = quantity(value, "/usageMetadata/cachedContentTokenCount"),
    thoughts = quantity(value, "/usageMetadata/thoughtsTokenCount");
  if ([prompt, toolUse, candidates, cached, thoughts].every((v) => v === undefined))
    return previous;
  const next: Usage = { ...previous };
  const cachedTotal = cached ?? BigInt(previous?.cached_input_tokens ?? "0"),
    reasoning = thoughts ?? BigInt(previous?.reasoning_tokens ?? "0");
  if (cached !== undefined) next.cached_input_tokens = cached.toString();
  if (thoughts !== undefined) next.reasoning_tokens = thoughts.toString();
  if (prompt !== undefined || toolUse !== undefined) {
    const uncached = (prompt ?? 0n) + (toolUse ?? 0n) - cachedTotal;
    if (uncached < 0n) fail("invalid_upstream_usage");
    next.input_tokens = uncached.toString();
  }
  if (candidates !== undefined || thoughts !== undefined)
    next.output_tokens = ((candidates ?? 0n) + reasoning).toString();
  for (const [key, before] of Object.entries(previous ?? {}))
    if (BigInt(next[key]!) < BigInt(before)) fail("usage_regressed");
  return next;
}
function finality(usage: Usage | null) {
  return usage?.input_tokens !== undefined && usage.output_tokens !== undefined
    ? "FINAL"
    : usage
      ? "PARTIAL"
      : "UNKNOWN";
}
function finishReason(value: unknown, calls: boolean): string {
  if (value === "STOP") return calls ? "tool_calls" : "stop";
  if (value === "MAX_TOKENS") return "length";
  if (
    [
      "SAFETY",
      "RECITATION",
      "BLOCKLIST",
      "PROHIBITED_CONTENT",
      "SPII",
      "IMAGE_SAFETY",
      "IMAGE_PROHIBITED_CONTENT",
    ].includes(String(value))
  )
    return "content_filter";
  fail("invalid_finish_reason");
}
function clientUsage(usage: Usage | null) {
  const cached = BigInt(usage?.cached_input_tokens ?? "0"),
    prompt = BigInt(usage?.input_tokens ?? "0") + cached,
    completion = BigInt(usage?.output_tokens ?? "0");
  return {
    prompt_tokens: number(prompt.toString()),
    completion_tokens: number(completion.toString()),
    total_tokens: number((prompt + completion).toString()),
    prompt_tokens_details: { cached_tokens: number(cached.toString()) },
    completion_tokens_details: { reasoning_tokens: number(usage?.reasoning_tokens ?? "0") },
  };
}
const candidatePointers = [
  "/*",
  "/responseId",
  "/candidates",
  "/candidates/*/*",
  "/candidates/0/content/*",
  "/candidates/0/content/parts/*/*",
  "/candidates/0/content/parts/*/thought",
  "/candidates/0/content/parts/*/functionCall/*",
  "/candidates/0/finishReason",
  "/promptFeedback/blockReason",
  "/usageMetadata/promptTokenCount",
  "/usageMetadata/toolUsePromptTokenCount",
  "/usageMetadata/candidatesTokenCount",
  "/usageMetadata/cachedContentTokenCount",
  "/usageMetadata/thoughtsTokenCount",
];
function candidate(value: DocumentInput, firstTool: number) {
  if (fact(value, "/error").kind !== "missing") fail("upstream_error");
  const listed = fact(value, "/candidates");
  if (listed.kind === "missing") {
    const blocked = scalar(value, "/promptFeedback/blockReason");
    if (typeof blocked !== "string" || !blocked || blocked === "BLOCK_REASON_UNSPECIFIED")
      fail("invalid_choices");
    return { texts: [] as string[], calls: [] as unknown[], finish: "content_filter" };
  }
  if (listed.kind !== "array" || count(value, "/candidates") !== 1) fail("invalid_choices");
  const parts = fact(value, "/candidates/0/content/parts"),
    texts: string[] = [],
    calls: unknown[] = [];
  if (parts.kind === "array")
    for (let p = 0; p < count(value, "/candidates/0/content/parts"); p++) {
      const base = `/candidates/0/content/parts/${p}`,
        fields = keys(value, base).filter((key) => key !== "thoughtSignature");
      if (scalar(value, `${base}/thought`) === true) fail("unsupported_semantics");
      if (fields.length === 1 && fields[0] === "text") {
        if (fact(value, `${base}/text`).kind !== "string") fail("invalid_content");
        texts.push(`${base}/text`);
      } else if (fields.length === 1 && fields[0] === "functionCall") {
        if (fact(value, `${base}/functionCall/name`).kind !== "string") fail("invalid_tool_call");
        const args = fact(value, `${base}/functionCall/args`).kind;
        if (args !== "missing" && args !== "object") fail("invalid_tool_call");
        const id = fact(value, `${base}/functionCall/id`).kind;
        calls.push({
          index: firstTool + calls.length,
          id:
            id === "string" ? copy(`${base}/functionCall/id`) : `call_${firstTool + calls.length}`,
          type: "function",
          function: {
            name: copy(`${base}/functionCall/name`),
            arguments: args === "object" ? serialized(`${base}/functionCall/args`) : "{}",
          },
        });
      } else fail("unsupported_semantics");
    }
  else if (parts.kind !== "missing") fail("invalid_content");
  const reason = scalar(value, "/candidates/0/finishReason");
  return {
    texts,
    calls,
    finish:
      reason === undefined || reason === null
        ? ""
        : finishReason(reason, calls.length > 0 || firstTool > 0),
  };
}
const nativePointers = [
  "/*",
  "/candidates",
  "/candidates/0/finishReason",
  "/promptFeedback/blockReason",
  "/usageMetadata/promptTokenCount",
  "/usageMetadata/toolUsePromptTokenCount",
  "/usageMetadata/candidatesTokenCount",
  "/usageMetadata/cachedContentTokenCount",
  "/usageMetadata/thoughtsTokenCount",
];
function nativeFinish(value: DocumentInput) {
  if (fact(value, "/error").kind !== "missing") fail("upstream_error");
  const listed = fact(value, "/candidates");
  if (listed.kind === "missing") {
    const blocked = scalar(value, "/promptFeedback/blockReason");
    if (typeof blocked !== "string" || !blocked || blocked === "BLOCK_REASON_UNSPECIFIED")
      fail("invalid_choices");
    return "content_filter";
  }
  if (listed.kind !== "array" || count(value, "/candidates") !== 1) fail("invalid_choices");
  const reason = scalar(value, "/candidates/0/finishReason");
  return reason === undefined || reason === null ? "" : finishReason(reason, false);
}
function nativeBody(value: DocumentInput, model: string) {
  return {
    document: value.document.handle,
    edits:
      fact(value, "/modelVersion").kind === "missing"
        ? []
        : [{ op: "set", pointer: "/modelVersion", value: JSON.stringify(model) }],
  };
}
function responseNative(value: DocumentInput) {
  if (!Object.hasOwn(value.inspection, "/candidates")) return inspect(value, nativePointers);
  const finish = nativeFinish(value);
  if (!finish) fail("invalid_finish_reason");
  let usage = measure(value, null);
  if (usage?.input_tokens === undefined) fail("invalid_upstream_usage");
  if (usage.output_tokens === undefined) usage = { ...usage, output_tokens: "0" };
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: nativeBody(value, value.model!),
    usage,
    finality: finality(usage),
    terminal: {
      kind: ["length", "content_filter"].includes(finish) ? "incomplete" : "complete",
      bytes: "",
    },
    needs_input: false,
  };
}
function nativeFrame(state: State, value: FrameInput) {
  const frame = value.frame;
  if (!Object.hasOwn(value.inspection, "/candidates")) return inspect(value, nativePointers);
  state.lastFrame = frame.ordinal;
  if (frame.event.value !== "" && frame.event.value !== "message") fail("invalid_stream");
  const finish = nativeFinish(value);
  state.usage = measure(value, state.usage);
  const body = nativeBody(value, state.model);
  if (!finish)
    return output(state, {
      handle: frame.handle,
      disposition: "emit",
      ...(body.edits.length ? { body } : {}),
    });
  if (state.usage?.input_tokens === undefined) fail("usage_required");
  if (state.usage.output_tokens === undefined) state.usage = { ...state.usage, output_tokens: "0" };
  state.done = true;
  return output(
    state,
    { handle: frame.handle, disposition: "hold", ...(body.edits.length ? { body } : {}) },
    {
      kind: ["length", "content_filter"].includes(finish) ? "incomplete" : "complete",
      bytes: "",
    },
  );
}
function response(_input: GuestInput, value: DocumentInput) {
  if (!value.document || !value.inspection || typeof value.model !== "string" || !value.model)
    fail("invalid_request");
  if (!Object.hasOwn(value.inspection, "/responseId")) return inspect(value, candidatePointers);
  const { texts, calls, finish } = candidate(value, 0);
  if (!finish) fail("invalid_finish_reason");
  if (!texts.length && !calls.length && finish !== "content_filter") fail("empty_response");
  let usage = measure(value, null);
  if (usage?.input_tokens === undefined) fail("invalid_upstream_usage");
  if (usage.output_tokens === undefined) usage = { ...usage, output_tokens: "0" };
  const message: Record<string, unknown> = {
    role: "assistant",
    content: texts.length ? joined(texts) : null,
  };
  if (calls.length)
    message.tool_calls = calls.map((call) => {
      const { index: _index, ...rest } = call as Record<string, unknown>;
      return rest;
    });
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: compose(value.document.handle, {
      id: fact(value, "/responseId").kind === "string" ? copy("/responseId") : "chatcmpl-gemini",
      object: "chat.completion",
      created: 0,
      model: value.model,
      choices: [{ index: 0, message, finish_reason: finish }],
      usage: clientUsage(usage),
    }),
    usage,
    finality: finality(usage),
    terminal: {
      kind: ["length", "content_filter"].includes(finish) ? "incomplete" : "complete",
      bytes: "",
    },
    needs_input: false,
  };
}
function chunk(state: State, choices: unknown[], extra: Record<string, unknown> = {}) {
  return {
    id: state.id,
    object: "chat.completion.chunk",
    created: 0,
    model: state.model,
    choices,
    ...extra,
  };
}
function streamFrame(state: State, value: FrameInput) {
  const frame = value.frame;
  if (!frame || frame.data.handle !== value.document.handle) fail("stream_state");
  if (frame.ordinal !== Number(state.lastFrame) + 1) fail("stream_state");
  if (!frame.data.bytes) {
    state.lastFrame = frame.ordinal;
    return output(state, { handle: frame.handle, disposition: "drop" });
  }
  if (state.native) return nativeFrame(state, value);
  if (!Object.hasOwn(value.inspection, "/responseId")) return inspect(value, candidatePointers);
  state.lastFrame = frame.ordinal;
  if (frame.event.value !== "" && frame.event.value !== "message") fail("invalid_stream");
  const first = !state.id;
  if (first) {
    const id = scalar(value, "/responseId");
    state.id = typeof id === "string" && /^[\x20-\x7e]{1,200}$/.test(id) ? id : "chatcmpl-gemini";
  }
  const tools = Number(state.tools),
    { texts, calls, finish } = candidate(value, tools);
  state.tools = tools + calls.length;
  state.usage = measure(value, state.usage);
  const delta: Record<string, unknown> = first ? { role: "assistant" } : {};
  if (texts.length) delta.content = joined(texts);
  else if (first) delta.content = "";
  if (calls.length) delta.tool_calls = calls;
  const body = chunk(state, [{ index: 0, delta, finish_reason: finish || null }]);
  if (!finish)
    return output(state, {
      handle: frame.handle,
      disposition: "emit",
      body: compose(value.document.handle, body),
      data_only: true,
    });
  if (state.usage?.input_tokens === undefined) fail("usage_required");
  if (state.usage.output_tokens === undefined) state.usage = { ...state.usage, output_tokens: "0" };
  state.done = true;
  return output(
    state,
    {
      handle: frame.handle,
      disposition: "hold",
      body: compose(value.document.handle, body),
      data_only: true,
      extra: [exactStringify(chunk(state, [], { usage: clientUsage(state.usage) })), "[DONE]"],
    },
    {
      kind: ["length", "content_filter"].includes(finish) ? "incomplete" : "complete",
      bytes: "",
    },
  );
}
function output(state: State, frame?: Record<string, unknown>, terminal?: Record<string, unknown>) {
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    ...(state.usage ? { usage: state.usage } : {}),
    finality: terminal ? finality(state.usage) : state.usage ? "PARTIAL" : "UNKNOWN",
    ...(frame ? { frame } : {}),
    ...(terminal ? { terminal } : {}),
    needs_input: !!frame && !terminal,
  };
}
export function invoke() {
  runGuest(Host, (input, previous) => {
    const bytes = decode64(input.payload);
    const document = () => object(JSON.parse(new TextDecoder().decode(bytes)));
    if (input.hook === "describe")
      return {
        result: {
          abi_version: 1,
          kind: "description",
          sdk_version: "1.0.0",
          capabilities: manifest.capabilities,
        },
        state: null,
      };
    if (input.hook === "prepare")
      return { result: prepare(input, document() as DocumentInput), state: null };
    if (input.hook === "response") {
      const value = document() as DocumentInput;
      return {
        result: value.protocol === "gemini" ? responseNative(value) : response(input, value),
        state: null,
      };
    }
    if (input.hook === "error")
      return {
        result: { abi_version: 1, kind: "failure", status: 502, code: "upstream_error" },
        state: null,
      };
    if (input.hook === "stream_dispose")
      return { result: { abi_version: 1, kind: "disposed" }, state: null };
    if (input.hook === "stream_open") {
      const payload = object(exactParse(new TextDecoder().decode(bytes), 400));
      if (typeof payload.model !== "string" || !payload.model) fail("invalid_model", 400);
      if (
        payload.protocol !== undefined &&
        payload.protocol !== "chat" &&
        payload.protocol !== "gemini"
      )
        fail("unsupported_semantics", 422);
      const state: State = {
        native: payload.protocol === "gemini",
        session: input.session.id,
        sequence: input.session.sequence,
        model: payload.model,
        lastFrame: 0,
        id: "",
        tools: 0,
        usage: null,
        done: false,
      };
      return { result: { ...output(state), needs_input: true }, state };
    }
    if (!["stream_feed", "stream_drain", "stream_finish"].includes(input.hook))
      fail("unsupported_capability", 422);
    const state = previous as State;
    if (
      !state ||
      state.session !== input.session.id ||
      Number(state.sequence) + 1 !== input.session.sequence ||
      (state.done && input.hook !== "stream_finish")
    )
      fail("stream_state");
    state.sequence = input.session.sequence;
    if (input.hook === "stream_finish") {
      if (!state.done) fail("stream_truncated");
      return {
        result: {
          abi_version: 1,
          kind: "output",
          consumed: 0,
          chunks: [],
          ...(state.usage ? { usage: state.usage } : {}),
          finality: finality(state.usage),
          needs_input: false,
        },
        state: null,
      };
    }
    return { result: streamFrame(state, document() as FrameInput), state };
  });
}
