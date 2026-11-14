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
  blocks: Record<string, number>;
  tools: number;
  finish: string;
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
  "user",
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
  "/messages/*/content/*/image_url/url",
  "/messages/*/tool_calls/*/type",
  "/messages/*/tool_calls/*/id",
  "/messages/*/tool_calls/*/function/name",
  "/stop/*",
];
function textParts(value: DocumentInput, base: string, status: 400 | 422 = 422) {
  const blocks: unknown[] = [];
  for (let p = 0; p < count(value, base); p++) {
    if (scalar(value, `${base}/${p}/type`, 400) !== "text") fail("unsupported_semantics", status);
    if (fact(value, `${base}/${p}/text`).kind !== "string") fail("invalid_messages", 400);
    blocks.push({ type: "text", text: copy(`${base}/${p}/text`) });
  }
  return blocks;
}
function userParts(value: DocumentInput, base: string) {
  const blocks: unknown[] = [];
  for (let p = 0; p < count(value, base); p++) {
    const type = scalar(value, `${base}/${p}/type`, 400);
    if (type === "text") {
      if (fact(value, `${base}/${p}/text`).kind !== "string") fail("invalid_messages", 400);
      blocks.push({ type: "text", text: copy(`${base}/${p}/text`) });
      continue;
    }
    if (type !== "image_url") fail("unsupported_semantics", 422);
    const url = scalar(value, `${base}/${p}/image_url/url`, 400);
    if (typeof url !== "string" || !/^https?:\/\//.test(url)) fail("unsupported_semantics", 422);
    blocks.push({
      type: "image",
      source: { type: "url", url: copy(`${base}/${p}/image_url/url`) },
    });
  }
  return blocks;
}
function convertMessages(value: DocumentInput) {
  const system: unknown[] = [],
    messages: unknown[] = [];
  const total = count(value, "/messages");
  if (fact(value, "/messages").kind !== "array" || !total) fail("invalid_messages", 400);
  for (let i = 0; i < total; i++) {
    const base = `/messages/${i}`,
      role = scalar(value, `${base}/role`, 400),
      content = fact(value, `${base}/content`);
    const allowed: Record<string, string[]> = {
      system: ["role", "content"],
      developer: ["role", "content"],
      user: ["role", "content"],
      assistant: ["role", "content", "tool_calls", "refusal"],
      tool: ["role", "content", "tool_call_id"],
    };
    if (typeof role !== "string" || !allowed[role]) fail("invalid_messages", 400);
    if (keys(value, base).some((key) => !allowed[role]!.includes(key)))
      fail("unsupported_semantics", 422);
    if (role === "system" || role === "developer") {
      if (content.kind === "string") system.push({ type: "text", text: copy(`${base}/content`) });
      else if (content.kind === "array") system.push(...textParts(value, `${base}/content`));
      else fail("invalid_messages", 400);
      continue;
    }
    if (role === "user") {
      if (content.kind === "string")
        messages.push({ role: "user", content: copy(`${base}/content`) });
      else if (content.kind === "array")
        messages.push({ role: "user", content: userParts(value, `${base}/content`) });
      else fail("invalid_messages", 400);
      continue;
    }
    if (role === "tool") {
      if (fact(value, `${base}/tool_call_id`).kind !== "string") fail("invalid_messages", 400);
      const result =
        content.kind === "string"
          ? copy(`${base}/content`)
          : content.kind === "array"
            ? textParts(value, `${base}/content`)
            : fail("invalid_messages", 400);
      messages.push({
        role: "user",
        content: [
          { type: "tool_result", tool_use_id: copy(`${base}/tool_call_id`), content: result },
        ],
      });
      continue;
    }
    const refusal = fact(value, `${base}/refusal`);
    if (refusal.kind !== "missing" && refusal.kind !== "null") fail("unsupported_semantics", 422);
    const blocks: unknown[] = [];
    if (content.kind === "string") {
      if (!content.empty) blocks.push({ type: "text", text: copy(`${base}/content`) });
    } else if (content.kind === "array") blocks.push(...textParts(value, `${base}/content`));
    else if (content.kind !== "null" && content.kind !== "missing") fail("invalid_messages", 400);
    const calls = fact(value, `${base}/tool_calls`);
    if (calls.kind === "array")
      for (let t = 0; t < count(value, `${base}/tool_calls`); t++) {
        const call = `${base}/tool_calls/${t}`;
        if (
          scalar(value, `${call}/type`, 400) !== "function" ||
          fact(value, `${call}/id`).kind !== "string" ||
          fact(value, `${call}/function/name`).kind !== "string" ||
          fact(value, `${call}/function/arguments`).kind !== "string"
        )
          fail("invalid_tool_call", 400);
        blocks.push({
          type: "tool_use",
          id: copy(`${call}/id`),
          name: copy(`${call}/function/name`),
          input: fact(value, `${call}/function/arguments`).empty
            ? {}
            : parsed(`${call}/function/arguments`),
        });
      }
    else if (calls.kind !== "missing" && calls.kind !== "null") fail("invalid_tool_call", 400);
    if (!blocks.length) fail("invalid_messages", 400);
    messages.push({ role: "assistant", content: blocks });
  }
  if (!messages.length) fail("invalid_messages", 400);
  return { system, messages };
}
function convertTools(value: DocumentInput) {
  const kind = fact(value, "/tools").kind;
  if (kind === "missing" || kind === "null") return undefined;
  if (kind !== "array") fail("invalid_request", 400);
  const tools: unknown[] = [];
  for (let t = 0; t < count(value, "/tools"); t++) {
    const base = `/tools/${t}`;
    if (scalar(value, `${base}/type`, 400) !== "function") fail("unsupported_semantics", 422);
    const fields = keys(value, `${base}/function`);
    if (fields.some((key) => !["name", "description", "parameters", "strict"].includes(key)))
      fail("unsupported_semantics", 422);
    if (fact(value, `${base}/function/name`).kind !== "string") fail("invalid_request", 400);
    if (scalar(value, `${base}/function/strict`, 400) === true) fail("unsupported_semantics", 422);
    const description = fact(value, `${base}/function/description`).kind;
    const parameters = fact(value, `${base}/function/parameters`).kind;
    if (parameters !== "missing" && parameters !== "object") fail("invalid_request", 400);
    tools.push({
      name: copy(`${base}/function/name`),
      ...(description === "string" ? { description: copy(`${base}/function/description`) } : {}),
      input_schema:
        parameters === "object"
          ? copy(`${base}/function/parameters`)
          : { type: "object", properties: {} },
    });
  }
  return tools;
}
function convertChoice(value: DocumentInput) {
  const choice = fact(value, "/tool_choice"),
    parallel = scalar(value, "/parallel_tool_calls", 400);
  if (parallel !== undefined && typeof parallel !== "boolean") fail("invalid_request", 400);
  const disable = parallel === false ? { disable_parallel_tool_use: true } : {};
  if (choice.kind === "missing" || choice.kind === "null")
    return parallel === false ? { type: "auto", ...disable } : undefined;
  if (choice.kind === "string") {
    const mode = scalar(value, "/tool_choice", 400);
    if (mode === "auto") return { type: "auto", ...disable };
    if (mode === "required") return { type: "any", ...disable };
    if (mode === "none") return { type: "none" };
    fail("unsupported_semantics", 422);
  }
  if (
    scalar(value, "/tool_choice/type", 400) !== "function" ||
    fact(value, "/tool_choice/function/name").kind !== "string"
  )
    fail("unsupported_semantics", 422);
  return { type: "tool", name: copy("/tool_choice/function/name"), ...disable };
}
function prepareNative(input: GuestInput, value: DocumentInput, target: Target) {
  if (!Object.hasOwn(value.inspection, "/max_tokens"))
    return inspect(value, ["/*", "/model", "/max_tokens", "/stream", "/anthropic_version"]);
  const map = object(input.config.model_map, 400),
    upstream = map[value.model!],
    model = scalar(value, "/model", 400),
    stream = scalar(value, "/stream", 400);
  if (typeof upstream !== "string" || !upstream || model !== value.model)
    fail("model_unavailable", 422);
  if (stream !== undefined && typeof stream !== "boolean") fail("invalid_stream", 400);
  const max = smallInteger(input.config.max_output_tokens, 1000000, 422),
    inputTokens = smallInteger(input.config.max_input_tokens, 16 << 20, 422);
  if (value.document.bytes > inputTokens) fail("input_ceiling", 413);
  const requested = scalar(value, "/max_tokens", 400);
  if (requested === undefined) fail("invalid_request", 400);
  const output = smallInteger(requested, max, 422);
  if (output < 1) fail("invalid_output_ceiling", 422);
  if (fact(value, "/anthropic_version").kind !== "missing") fail("unsupported_semantics", 422);
  const edits: { op: string; pointer: string; value?: string }[] = [];
  if (target === "bedrock") {
    if (!/^[A-Za-z0-9._:-]{1,200}$/.test(upstream)) fail("model_unavailable", 422);
    edits.push({ op: "remove", pointer: "/model" });
    if (stream !== undefined) edits.push({ op: "remove", pointer: "/stream" });
    edits.push({ op: "set", pointer: "/anthropic_version", value: '"bedrock-2023-05-31"' });
  } else edits.push({ op: "set", pointer: "/model", value: JSON.stringify(upstream) });
  return prepared(input, value, target, upstream, stream === true, output, inputTokens, {
    document: value.document.handle,
    edits,
  });
}
function prepared(
  input: GuestInput,
  value: DocumentInput,
  target: Target,
  upstream: string,
  stream: boolean,
  output: number,
  inputTokens: number,
  body: unknown,
) {
  const auth =
    target === "bedrock"
      ? { type: "aws-sigv4", service: "bedrock" }
      : input.config.auth === "api-key"
        ? { type: "api-key", header: "x-api-key" }
        : input.config.auth === "none"
          ? { type: "none" }
          : { type: "bearer" };
  const path =
    target === "bedrock"
      ? `/model/${encodeURIComponent(upstream)}/${stream ? "invoke-with-response-stream" : "invoke"}`
      : "/v1/messages";
  const headers: Record<string, string> =
    target === "bedrock"
      ? {
          "content-type": "application/json",
          accept: "application/json",
          ...(stream ? { "x-amzn-bedrock-accept": "application/json" } : {}),
        }
      : {
          "content-type": "application/json",
          accept: stream ? "text/event-stream" : "application/json",
          "anthropic-version": "2023-06-01",
        };
  return {
    abi_version: 1,
    kind: "prepared",
    request: {
      method: "POST",
      path,
      headers,
      body,
      auth,
      manifest: {
        operation: value.operation,
        model: value.model,
        upstream_model: upstream,
        stream,
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
function prepare(input: GuestInput, value: DocumentInput, target: Target) {
  if (value.protocol === "messages" && value.operation === "messages" && value.model)
    return prepareNative(input, value, target);
  if (value.protocol !== "chat" || value.operation !== "chat") fail("unsupported_semantics", 422);
  if (!value.document || !value.inspection || typeof value.model !== "string")
    fail("invalid_request", 400);
  if (!Object.hasOwn(value.inspection, "/model")) return inspect(value, firstRound);
  if (!Object.hasOwn(value.inspection, "/tool_choice/type")) return inspect(value, secondRound);
  if (keys(value, "").some((key) => !requestKeys.includes(key))) fail("unsupported_semantics", 422);
  const map = object(input.config.model_map, 400),
    upstream = map[value.model],
    model = scalar(value, "/model", 400);
  if (typeof upstream !== "string" || !upstream || (model !== undefined && model !== value.model))
    fail("model_unavailable", 422);
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
  const { system, messages } = convertMessages(value),
    tools = convertTools(value),
    choice = convertChoice(value);
  const body: Record<string, unknown> =
    target === "bedrock"
      ? { anthropic_version: "bedrock-2023-05-31", max_tokens: number(output) }
      : { model: upstream, max_tokens: number(output) };
  if (system.length) body.system = system;
  body.messages = messages;
  for (const name of ["temperature", "top_p"]) {
    const kind = fact(value, `/${name}`).kind;
    if (kind === "number") body[name] = copy(`/${name}`);
    else if (kind !== "missing" && kind !== "null") fail("invalid_request", 400);
  }
  const stop = fact(value, "/stop");
  if (stop.kind === "string") body.stop_sequences = [copy("/stop")];
  else if (stop.kind === "array") {
    for (let s = 0; s < count(value, "/stop"); s++)
      if (fact(value, `/stop/${s}`).kind !== "string") fail("invalid_request", 400);
    body.stop_sequences = copy("/stop");
  } else if (stop.kind !== "missing" && stop.kind !== "null") fail("invalid_request", 400);
  if (tools) body.tools = tools;
  if (choice) body.tool_choice = choice;
  const user = fact(value, "/user").kind;
  if (user === "string") body.metadata = { user_id: copy("/user") };
  else if (user !== "missing") fail("invalid_request", 400);
  if (stream === true && target === "anthropic") body.stream = true;
  if (target === "bedrock" && !/^[A-Za-z0-9._:-]{1,200}$/.test(upstream))
    fail("model_unavailable", 422);
  return prepared(
    input,
    value,
    target,
    upstream,
    stream === true,
    output,
    inputTokens,
    compose(value.document.handle, body),
  );
}
function measure(value: DocumentInput, base: string, previous: Usage | null): Usage | null {
  const next: Usage = { ...previous };
  for (const [field, name] of [
    ["input_tokens", "input_tokens"],
    ["output_tokens", "output_tokens"],
    ["cache_read_input_tokens", "cached_input_tokens"],
    ["cache_creation_input_tokens", "cache_write_tokens"],
  ] as const) {
    const raw = scalar(value, `${base}/${field}`);
    if (raw === undefined || raw === null) continue;
    next[name] = integer(raw);
  }
  for (const [key, before] of Object.entries(previous ?? {}))
    if (BigInt(next[key]!) < BigInt(before)) fail("usage_regressed");
  return Object.keys(next).length ? next : null;
}
function finality(usage: Usage | null) {
  return usage?.input_tokens !== undefined && usage.output_tokens !== undefined
    ? "FINAL"
    : usage
      ? "PARTIAL"
      : "UNKNOWN";
}
function finishReason(value: unknown): string {
  if (value === "end_turn" || value === "stop_sequence") return "stop";
  if (value === "max_tokens" || value === "model_context_window_exceeded") return "length";
  if (value === "tool_use") return "tool_calls";
  if (value === "refusal") return "content_filter";
  fail("invalid_finish_reason");
}
function clientUsage(usage: Usage | null) {
  const prompt =
    BigInt(usage?.input_tokens ?? "0") +
    BigInt(usage?.cached_input_tokens ?? "0") +
    BigInt(usage?.cache_write_tokens ?? "0");
  const completion = BigInt(usage?.output_tokens ?? "0");
  return {
    prompt_tokens: number(prompt.toString()),
    completion_tokens: number(completion.toString()),
    total_tokens: number((prompt + completion).toString()),
    prompt_tokens_details: { cached_tokens: number(usage?.cached_input_tokens ?? "0") },
  };
}
const responsePointers = [
  "/*",
  "/type",
  "/id",
  "/content",
  "/content/*/type",
  "/content/*/text",
  "/content/*/id",
  "/content/*/name",
  "/content/*/input",
  "/stop_reason",
  "/usage/input_tokens",
  "/usage/output_tokens",
  "/usage/cache_read_input_tokens",
  "/usage/cache_creation_input_tokens",
];
function responseNative(value: DocumentInput) {
  if (!Object.hasOwn(value.inspection, "/type")) return inspect(value, responsePointers);
  if (scalar(value, "/type") === "error" || fact(value, "/error").kind !== "missing")
    fail("upstream_error");
  if (scalar(value, "/type") !== "message" || fact(value, "/id").kind !== "string")
    fail("invalid_object");
  if (fact(value, "/content").kind !== "array") fail("invalid_object");
  const finish = finishReason(scalar(value, "/stop_reason"));
  const usage = measure(value, "/usage", null);
  if (usage?.input_tokens === undefined || usage.output_tokens === undefined)
    fail("invalid_upstream_usage");
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: {
      document: value.document.handle,
      edits: [{ op: "set", pointer: "/model", value: JSON.stringify(value.model) }],
    },
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
  if (!frame.data.bytes) {
    state.lastFrame = frame.ordinal;
    return output(state, { handle: frame.handle, disposition: "drop" });
  }
  if (!Object.hasOwn(value.inspection, "/type")) return inspect(value, framePointers);
  const type = scalar(value, "/type");
  if (frame.event.value !== "" && frame.event.value !== type) fail("invalid_stream");
  state.lastFrame = frame.ordinal;
  const pass = (disposition = "emit", body?: unknown) =>
    output(state, { handle: frame.handle, disposition, ...(body ? { body } : {}) });
  if (type === "error") fail("upstream_error");
  if (type === "ping") return pass();
  if (state.finish && type !== "message_stop") fail("data_after_finish");
  if (type === "message_start") {
    if (state.id) fail("stream_state");
    const id = scalar(value, "/message/id");
    if (typeof id !== "string" || !id) fail("invalid_object");
    state.id = id;
    state.usage = measure(value, "/message/usage", null);
    return pass("emit", {
      document: value.document.handle,
      edits: [{ op: "set", pointer: "/message/model", value: JSON.stringify(state.model) }],
    });
  }
  if (!state.id) fail("stream_state");
  if (["content_block_start", "content_block_delta", "content_block_stop"].includes(String(type)))
    return pass();
  if (type === "message_delta") {
    if (state.finish) fail("duplicate_finish");
    state.finish = finishReason(scalar(value, "/delta/stop_reason"));
    state.usage = measure(value, "/usage", state.usage);
    return pass("hold");
  }
  if (type === "message_stop") {
    if (!state.finish) fail("premature_terminal");
    if (state.usage?.input_tokens === undefined || state.usage.output_tokens === undefined)
      fail("usage_required");
    state.done = true;
    return output(
      state,
      { handle: frame.handle, disposition: "hold" },
      {
        kind: ["length", "content_filter"].includes(state.finish) ? "incomplete" : "complete",
        bytes: "",
      },
    );
  }
  fail("invalid_stream");
}
function response(_input: GuestInput, value: DocumentInput) {
  if (!value.document || !value.inspection || typeof value.model !== "string" || !value.model)
    fail("invalid_request");
  if (!Object.hasOwn(value.inspection, "/type")) return inspect(value, responsePointers);
  if (scalar(value, "/type") === "error" || fact(value, "/error").kind !== "missing")
    fail("upstream_error");
  if (scalar(value, "/type") !== "message" || fact(value, "/id").kind !== "string")
    fail("invalid_object");
  if (fact(value, "/content").kind !== "array") fail("invalid_object");
  const texts: string[] = [],
    calls: unknown[] = [];
  for (let c = 0; c < count(value, "/content"); c++) {
    const type = scalar(value, `/content/${c}/type`);
    if (type === "text") {
      if (fact(value, `/content/${c}/text`).kind !== "string") fail("invalid_content");
      texts.push(`/content/${c}/text`);
    } else if (type === "tool_use") {
      if (
        fact(value, `/content/${c}/id`).kind !== "string" ||
        fact(value, `/content/${c}/name`).kind !== "string" ||
        fact(value, `/content/${c}/input`).kind !== "object"
      )
        fail("invalid_tool_call");
      calls.push({
        id: copy(`/content/${c}/id`),
        type: "function",
        function: {
          name: copy(`/content/${c}/name`),
          arguments: serialized(`/content/${c}/input`),
        },
      });
    } else fail("unsupported_semantics");
  }
  if (!texts.length && !calls.length) fail("empty_response");
  const finish = finishReason(scalar(value, "/stop_reason"));
  const usage = measure(value, "/usage", null);
  if (usage?.input_tokens === undefined || usage.output_tokens === undefined)
    fail("invalid_upstream_usage");
  const message: Record<string, unknown> = {
    role: "assistant",
    content: texts.length ? joined(texts) : null,
  };
  if (calls.length) message.tool_calls = calls;
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: compose(value.document.handle, {
      id: copy("/id"),
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
const framePointers = [
  "/type",
  "/index",
  "/message/id",
  "/message/usage/input_tokens",
  "/message/usage/output_tokens",
  "/message/usage/cache_read_input_tokens",
  "/message/usage/cache_creation_input_tokens",
  "/content_block/type",
  "/content_block/id",
  "/content_block/name",
  "/delta/type",
  "/delta/stop_reason",
  "/usage/output_tokens",
  "/usage/input_tokens",
  "/usage/cache_read_input_tokens",
  "/usage/cache_creation_input_tokens",
];
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
  if (state.native) return nativeFrame(state, value);
  if (!frame.data.bytes) {
    state.lastFrame = frame.ordinal;
    return output(state, { handle: frame.handle, disposition: "drop" });
  }
  if (!Object.hasOwn(value.inspection, "/type")) return inspect(value, framePointers);
  const type = scalar(value, "/type"),
    handle = value.document.handle;
  if (frame.event.value !== undefined && frame.event.value !== type) fail("invalid_stream");
  function emit(body: unknown, disposition = "emit") {
    state.lastFrame = frame.ordinal;
    return output(state, {
      handle: frame.handle,
      disposition,
      body: compose(handle, body),
      data_only: true,
    });
  }
  function drop() {
    state.lastFrame = frame.ordinal;
    return output(state, { handle: frame.handle, disposition: "drop" });
  }
  if (type === "error") fail("upstream_error");
  if (type === "ping") return drop();
  if (state.finish && type !== "message_stop") fail("data_after_finish");
  if (type === "message_start") {
    if (state.id) fail("stream_state");
    const id = scalar(value, "/message/id");
    if (typeof id !== "string" || !id) fail("invalid_object");
    state.id = id;
    state.usage = measure(value, "/message/usage", null);
    return emit(
      chunk(state, [{ index: 0, delta: { role: "assistant", content: "" }, finish_reason: null }]),
    );
  }
  if (!state.id) fail("stream_state");
  const index = String(smallInteger(scalar(value, "/index") ?? 0, 100000));
  if (type === "content_block_start") {
    const kind = scalar(value, "/content_block/type");
    if (kind === "text") {
      state.blocks[index] = -1;
      return drop();
    }
    if (kind !== "tool_use") fail("unsupported_semantics");
    const tool = Number(state.tools);
    state.tools = tool + 1;
    state.blocks[index] = tool;
    return emit(
      chunk(state, [
        {
          index: 0,
          delta: {
            tool_calls: [
              {
                index: tool,
                id: copy("/content_block/id"),
                type: "function",
                function: { name: copy("/content_block/name"), arguments: "" },
              },
            ],
          },
          finish_reason: null,
        },
      ]),
    );
  }
  if (type === "content_block_delta") {
    const block = state.blocks[index] === undefined ? undefined : Number(state.blocks[index]),
      kind = scalar(value, "/delta/type");
    if (block === undefined) fail("stream_state");
    if (kind === "text_delta" && block === -1)
      return emit(
        chunk(state, [{ index: 0, delta: { content: copy("/delta/text") }, finish_reason: null }]),
      );
    if (kind === "input_json_delta" && block >= 0)
      return emit(
        chunk(state, [
          {
            index: 0,
            delta: {
              tool_calls: [{ index: block, function: { arguments: copy("/delta/partial_json") } }],
            },
            finish_reason: null,
          },
        ]),
      );
    fail("unsupported_semantics");
  }
  if (type === "content_block_stop") {
    if (state.blocks[index] === undefined) fail("stream_state");
    return drop();
  }
  if (type === "message_delta") {
    if (state.finish) fail("duplicate_finish");
    state.finish = finishReason(scalar(value, "/delta/stop_reason"));
    state.usage = measure(value, "/usage", state.usage);
    return emit(chunk(state, [{ index: 0, delta: {}, finish_reason: state.finish }]), "hold");
  }
  if (type === "message_stop") {
    if (!state.finish) fail("premature_terminal");
    if (state.usage?.input_tokens === undefined || state.usage.output_tokens === undefined)
      fail("usage_required");
    state.done = true;
    state.lastFrame = frame.ordinal;
    return output(
      state,
      {
        handle: frame.handle,
        disposition: "hold",
        body: compose(handle, chunk(state, [], { usage: clientUsage(state.usage) })),
        data_only: true,
        extra: ["[DONE]"],
      },
      {
        kind: ["length", "content_filter"].includes(state.finish) ? "incomplete" : "complete",
        bytes: "",
      },
    );
  }
  fail("invalid_stream");
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
export type Target = "anthropic" | "bedrock";
export function runMessages(manifest: { capabilities: unknown }, target: Target) {
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
      return { result: prepare(input, document() as DocumentInput, target), state: null };
    if (input.hook === "response") {
      const value = document() as DocumentInput;
      return {
        result: value.protocol === "messages" ? responseNative(value) : response(input, value),
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
        payload.protocol !== "messages"
      )
        fail("unsupported_semantics", 422);
      const state: State = {
        native: payload.protocol === "messages",
        session: input.session.id,
        sequence: input.session.sequence,
        model: payload.model,
        lastFrame: 0,
        id: "",
        blocks: {},
        tools: 0,
        finish: "",
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
