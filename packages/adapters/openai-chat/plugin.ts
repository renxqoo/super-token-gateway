import {
  AdapterFailure,
  decode64,
  exactParse,
  integer,
  object,
  runGuest,
  smallInteger,
  type GuestInput,
  type AdapterFailureCode,
} from "@gateway/plugin-sdk/guest";
import manifest from "./package/manifest.json";
declare const Host: { inputString(): string; outputString(value: string): void };
type Usage = Record<string, string>;
type State = {
  session: string;
  sequence: number;
  model: string;
  lastFrame: number;
  active?: {
    handle: string;
    ordinal: number;
    phase: string;
    index: number;
    count: number;
    pending?: string;
    finish?: string;
    checks?: string[];
  };
  finished: boolean;
  finishKind: string;
  content: boolean;
  usage: Usage | null;
  tools: Record<string, { id: string; name: string; args?: string }>;
  done: boolean;
};
function fail(code: AdapterFailureCode): never {
  throw new AdapterFailure(502, code);
}
function quantity(source: Record<string, unknown>, names: string[]): string | undefined {
  let value: string | undefined;
  for (const name of names) {
    if (source[name] === undefined || source[name] === null) continue;
    const next = integer(source[name]);
    if (value !== undefined && value !== next) fail("usage_alias_conflict");
    value = next;
  }
  return value;
}
function usage(value: Record<string, unknown>, previous: Usage | null): Usage | null {
  if (value.usage === undefined || value.usage === null) return previous;
  const raw = object(value.usage),
    details = raw.prompt_tokens_details === undefined ? {} : object(raw.prompt_tokens_details),
    output =
      raw.completion_tokens_details === undefined ? {} : object(raw.completion_tokens_details);
  const prompt = quantity(raw, ["prompt_tokens", "input_tokens"]),
    completion = quantity(raw, ["completion_tokens", "output_tokens"]),
    cached = quantity(details, ["cached_tokens"]) ?? quantity(raw, ["prompt_cache_hit_tokens"]),
    reasoning = quantity(output, ["reasoning_tokens"]);
  const otherCached = quantity(raw, ["prompt_cache_hit_tokens"]);
  if (cached !== undefined && otherCached !== undefined && cached !== otherCached)
    fail("usage_alias_conflict");
  const next = { ...previous };
  if (cached !== undefined) next.cached_input_tokens = cached;
  if (reasoning !== undefined) next.reasoning_tokens = reasoning;
  const totalPrompt =
    prompt ??
    (previous?.input_tokens === undefined
      ? undefined
      : (BigInt(previous.input_tokens) + BigInt(previous.cached_input_tokens ?? "0")).toString());
  if (totalPrompt !== undefined) {
    const uncached = BigInt(totalPrompt) - BigInt(next.cached_input_tokens ?? "0");
    if (uncached < 0n) fail("invalid_upstream_usage");
    next.input_tokens = uncached.toString();
  }
  if (completion !== undefined) next.output_tokens = completion;
  const miss = quantity(raw, ["prompt_cache_miss_tokens"]);
  if (
    miss !== undefined &&
    totalPrompt !== undefined &&
    cached !== undefined &&
    BigInt(miss) + BigInt(cached) !== BigInt(totalPrompt)
  )
    fail("usage_alias_conflict");
  if (
    next.reasoning_tokens !== undefined &&
    next.output_tokens !== undefined &&
    BigInt(next.reasoning_tokens) > BigInt(next.output_tokens)
  )
    fail("invalid_upstream_usage");
  for (const [key, before] of Object.entries(previous ?? {}))
    if (next[key] !== undefined && BigInt(next[key]!) < BigInt(before)) fail("usage_regressed");
  return Object.keys(next).length ? next : null;
}
function finality(value: Usage | null) {
  return value?.input_tokens !== undefined && value.output_tokens !== undefined
    ? "FINAL"
    : value
      ? "PARTIAL"
      : "UNKNOWN";
}
function reason(value: unknown): string {
  if (
    typeof value !== "string" ||
    !["stop", "length", "tool_calls", "function_call", "content_filter"].includes(value)
  )
    fail("invalid_finish_reason");
  return value;
}
type Fact = {
  kind: string;
  bytes: number;
  count?: number;
  empty?: boolean;
  literal?: string;
  string_info?: { utf16_units: number; digest: string };
};
type DocumentInput = {
  document: { handle: string; bytes: number };
  inspection: Record<string, Fact>;
  protocol?: string;
  operation?: string;
  model?: string;
};
const preparePointers = [
  "",
  "/model",
  "/messages",
  "/n",
  "/stream",
  "/max_tokens",
  "/max_completion_tokens",
  "/stream_options",
  "/stream_options/include_usage",
];
const responsePointers = [
  "",
  "/error",
  "/choices",
  "/choices/0",
  "/choices/0/message",
  "/choices/0/message/content",
  "/choices/0/message/tool_calls",
  "/choices/0/finish_reason",
  "/usage",
  "/usage/prompt_tokens",
  "/usage/input_tokens",
  "/usage/completion_tokens",
  "/usage/output_tokens",
  "/usage/prompt_tokens_details/cached_tokens",
  "/usage/completion_tokens_details/reasoning_tokens",
  "/usage/prompt_cache_hit_tokens",
];
const responseExtra = [
  "/usage/prompt_cache_miss_tokens",
  "/choices/0/message/tool_calls/0/id",
  "/choices/0/message/tool_calls/0/type",
  "/choices/0/message/tool_calls/0/function/name",
  "/choices/0/message/tool_calls/0/function/arguments",
];
function fact(value: DocumentInput, path: string): Fact {
  const found = value.inspection[path];
  if (!found) fail("inspection_missing");
  return found;
}
function scalar(value: DocumentInput, path: string, status: 400 | 502 = 502): unknown {
  const found = fact(value, path);
  if (found.kind === "missing") return undefined;
  if (found.literal === undefined) throw new AdapterFailure(status, "scalar_budget");
  return exactParse(found.literal, status);
}
function inspect(value: DocumentInput, paths: string[]) {
  const missing = paths.filter((path) => !Object.hasOwn(value.inspection, path));
  return missing.length
    ? { abi_version: 1, kind: "inspect", document: value.document.handle, pointers: missing }
    : undefined;
}
function prepare(input: GuestInput, value: DocumentInput) {
  if (value.protocol !== "chat" || value.operation !== "chat")
    throw new AdapterFailure(422, "unsupported_semantics");
  if (!value.document || !value.inspection || typeof value.model !== "string")
    throw new AdapterFailure(400, "invalid_request");
  const query = inspect(value, preparePointers);
  if (query) return query;
  if (
    fact(value, "").kind !== "object" ||
    fact(value, "/messages").kind !== "array" ||
    !fact(value, "/messages").count
  )
    throw new AdapterFailure(400, "invalid_messages");
  const map = object(input.config.model_map, 400),
    upstream = map[value.model],
    model = scalar(value, "/model", 400);
  if (typeof upstream !== "string" || !upstream || (model !== undefined && model !== value.model))
    throw new AdapterFailure(422, "model_unavailable");
  const n = scalar(value, "/n", 400),
    stream = scalar(value, "/stream", 400);
  if (n !== undefined && smallInteger(n, 128, 422) !== 1)
    throw new AdapterFailure(422, "unsupported_choice_count");
  if (stream !== undefined && typeof stream !== "boolean")
    throw new AdapterFailure(400, "invalid_stream");
  const max = smallInteger(input.config.max_output_tokens, 1000000, 422),
    inputTokens = smallInteger(input.config.max_input_tokens, 16 << 20, 422);
  if (value.document.bytes > inputTokens) throw new AdapterFailure(413, "input_ceiling");
  const maxTokens = scalar(value, "/max_tokens", 400),
    maxCompletion = scalar(value, "/max_completion_tokens", 400);
  if (maxTokens !== undefined && maxCompletion !== undefined)
    throw new AdapterFailure(422, "conflicting_output_ceiling");
  const output = smallInteger(maxCompletion ?? maxTokens ?? max, max, 422);
  if (output < 1) throw new AdapterFailure(422, "invalid_output_ceiling");
  const edits = [{ op: "set", pointer: "/model", value: JSON.stringify(upstream) }];
  if (maxTokens === undefined && maxCompletion === undefined)
    edits.push({ op: "set", pointer: "/max_completion_tokens", value: String(output) });
  if (stream === true) {
    const options = fact(value, "/stream_options"),
      include = scalar(value, "/stream_options/include_usage", 400);
    if (include === false) throw new AdapterFailure(422, "usage_required");
    if (options.kind !== "object" && options.kind !== "missing")
      throw new AdapterFailure(400, "invalid_stream_options");
    if (options.kind === "missing")
      edits.push({ op: "set", pointer: "/stream_options", value: '{"include_usage":true}' });
    else edits.push({ op: "set", pointer: "/stream_options/include_usage", value: "true" });
  }
  const auth =
    input.config.auth === "api-key"
      ? { type: "api-key", header: "api-key" }
      : input.config.auth === "none"
        ? { type: "none" }
        : { type: "bearer" };
  return {
    abi_version: 1,
    kind: "prepared",
    request: {
      method: "POST",
      path: "/v1/chat/completions",
      headers: {
        "content-type": "application/json",
        accept: stream ? "text/event-stream" : "application/json",
      },
      body: { document: value.document.handle, edits },
      auth,
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
function response(_input: GuestInput, value: DocumentInput) {
  if (!value.document || !value.inspection || typeof value.model !== "string" || !value.model)
    fail("invalid_request");
  const first = inspect(value, responsePointers);
  if (first) return first;
  if (fact(value, "").kind !== "object") fail("invalid_object");
  if (fact(value, "/error").kind !== "missing") fail("upstream_error");
  if (
    fact(value, "/choices").kind !== "array" ||
    fact(value, "/choices").count !== 1 ||
    fact(value, "/choices/0").kind !== "object" ||
    fact(value, "/choices/0/message").kind !== "object"
  )
    fail("invalid_choices");
  const second = inspect(value, responseExtra);
  if (second) return second;
  const finish = reason(scalar(value, "/choices/0/finish_reason")),
    content = fact(value, "/choices/0/message/content"),
    tools = fact(value, "/choices/0/message/tool_calls");
  if (!(content.kind === "string" && content.empty === false)) {
    if (
      tools.kind !== "array" ||
      !tools.count ||
      fact(value, "/choices/0/message/tool_calls/0/id").kind !== "string" ||
      fact(value, "/choices/0/message/tool_calls/0/id").empty ||
      fact(value, "/choices/0/message/tool_calls/0/function/name").kind !== "string" ||
      fact(value, "/choices/0/message/tool_calls/0/function/name").empty ||
      fact(value, "/choices/0/message/tool_calls/0/function/arguments").kind !== "string"
    )
      fail("empty_response");
  }
  const raw: Record<string, unknown> = {},
    details: Record<string, unknown> = {},
    outputDetails: Record<string, unknown> = {};
  for (const name of [
    "prompt_tokens",
    "input_tokens",
    "completion_tokens",
    "output_tokens",
    "prompt_cache_hit_tokens",
    "prompt_cache_miss_tokens",
  ]) {
    const current = scalar(value, `/usage/${name}`);
    if (current !== undefined) raw[name] = current;
  }
  const cached = scalar(value, "/usage/prompt_tokens_details/cached_tokens"),
    reasoning = scalar(value, "/usage/completion_tokens_details/reasoning_tokens");
  if (cached !== undefined) details.cached_tokens = cached;
  if (reasoning !== undefined) outputDetails.reasoning_tokens = reasoning;
  raw.prompt_tokens_details = details;
  raw.completion_tokens_details = outputDetails;
  const usageFact = fact(value, "/usage");
  if (!["object", "missing", "null"].includes(usageFact.kind)) fail("invalid_object");
  const measured = usage({ usage: raw }, null);
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: {
      document: value.document.handle,
      edits: [{ op: "set", pointer: "/model", value: JSON.stringify(value.model) }],
    },
    ...(measured ? { usage: measured } : {}),
    finality: finality(measured),
    terminal: {
      kind: ["length", "content_filter"].includes(finish) ? "incomplete" : "complete",
      bytes: "",
    },
    needs_input: false,
  };
}
type FrameInput = DocumentInput & {
  frame: {
    handle: string;
    ordinal: number;
    event: { value?: string };
    data: { handle: string; bytes: number; preview?: string };
  };
  operation?: unknown;
};
const streamUsagePaths = [
  "/usage",
  "/usage/prompt_tokens",
  "/usage/input_tokens",
  "/usage/completion_tokens",
  "/usage/output_tokens",
  "/usage/prompt_tokens_details/cached_tokens",
  "/usage/completion_tokens_details/reasoning_tokens",
  "/usage/prompt_cache_hit_tokens",
  "/usage/prompt_cache_miss_tokens",
];
const streamPaths = [
  "",
  "/error",
  "/model",
  "/choices",
  "/choices/0",
  "/choices/0/index",
  "/choices/0/delta",
  "/choices/0/delta/content",
  "/choices/0/delta/tool_calls",
  "/choices/0/finish_reason",
];
function streamOutput(
  state: State,
  frame?: { handle: string; disposition: string; body?: unknown },
  terminal?: { kind: string; bytes: string },
) {
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
function streamUsage(value: FrameInput, state: State) {
  const current = fact(value, "/usage");
  if (!["missing", "null", "object"].includes(current.kind)) fail("invalid_object");
  if (current.kind !== "object") return;
  const raw: Record<string, unknown> = {};
  for (const name of [
    "prompt_tokens",
    "input_tokens",
    "completion_tokens",
    "output_tokens",
    "prompt_cache_hit_tokens",
    "prompt_cache_miss_tokens",
  ]) {
    const n = scalar(value, `/usage/${name}`);
    if (n !== undefined) raw[name] = n;
  }
  const cached = scalar(value, "/usage/prompt_tokens_details/cached_tokens"),
    reasoning = scalar(value, "/usage/completion_tokens_details/reasoning_tokens");
  if (cached !== undefined) raw.prompt_tokens_details = { cached_tokens: cached };
  if (reasoning !== undefined) raw.completion_tokens_details = { reasoning_tokens: reasoning };
  state.usage = usage({ usage: raw }, state.usage);
}
function streamFrame(state: State, value: FrameInput) {
  const frame = value.frame;
  if (!frame || frame.data.handle !== value.document.handle) fail("stream_state");
  if (!state.active) {
    if (frame.ordinal !== smallInteger(state.lastFrame, Number.MAX_SAFE_INTEGER) + 1)
      fail("stream_state");
    state.active = {
      handle: frame.handle,
      ordinal: frame.ordinal,
      phase: "usage",
      index: 0,
      count: 0,
    };
  }
  const active = state.active;
  if (
    active.handle !== frame.handle ||
    smallInteger(active.ordinal, Number.MAX_SAFE_INTEGER) !== frame.ordinal
  )
    fail("stream_state");
  if (frame.event.value === "error") fail("upstream_error");
  function complete(disposition: string, terminal?: { kind: string; bytes: string }) {
    state.lastFrame = frame.ordinal;
    delete state.active;
    const body =
      fact(value, "/model").kind === "missing"
        ? undefined
        : {
            document: value.document.handle,
            edits: [{ op: "set", pointer: "/model", value: JSON.stringify(state.model) }],
          };
    return streamOutput(
      state,
      { handle: frame.handle, disposition, ...(body ? { body } : {}) },
      terminal,
    );
  }
  if (!frame.data.bytes) {
    state.lastFrame = frame.ordinal;
    delete state.active;
    return streamOutput(state, { handle: frame.handle, disposition: "emit" });
  }
  if (frame.data.preview === "[DONE]") {
    if (!state.finished || !state.content) fail("premature_terminal");
    if (value.operation) {
      const checked = object(value.operation).checked;
      if (
        !Array.isArray(checked) ||
        checked.some((item) => {
          const c = object(item);
          return c.valid !== true || c.kind !== "object";
        })
      )
        fail("incomplete_tool_call");
      active.index = smallInteger(active.index, 128) + checked.length;
    }
    const tools = Object.values(state.tools);
    if (tools.some((t) => !t.id || !t.name || !t.args)) fail("incomplete_tool_call");
    const index = smallInteger(active.index, 128),
      handles = tools.slice(index, index + 16).map((t) => t.args!);
    if (handles.length) return { abi_version: 1, kind: "buffer_check", handles, seal: true };
    state.done = true;
    state.lastFrame = frame.ordinal;
    delete state.active;
    return streamOutput(
      state,
      { handle: frame.handle, disposition: "hold" },
      {
        kind: ["length", "content_filter"].includes(state.finishKind) ? "incomplete" : "complete",
        bytes: "",
      },
    );
  }
  if (active.phase === "usage") {
    const query = inspect(value, streamUsagePaths);
    if (query) return query;
    streamUsage(value, state);
    active.phase = "structure";
    return streamOutput(state);
  }
  if (active.phase === "structure") {
    const query = inspect(value, streamPaths);
    if (query) return query;
    if (fact(value, "").kind !== "object") fail("invalid_object");
    if (fact(value, "/error").kind !== "missing") fail("upstream_error");
    const choices = fact(value, "/choices");
    if (choices.kind !== "array" || (choices.count ?? 0) > 1) fail("invalid_choices");
    if (choices.count) {
      if (
        fact(value, "/choices/0").kind !== "object" ||
        !["missing", "object"].includes(fact(value, "/choices/0/delta").kind)
      )
        fail("invalid_choices");
      const index = scalar(value, "/choices/0/index");
      if (index !== undefined && smallInteger(index, 0) !== 0) fail("invalid_choices");
      const content = fact(value, "/choices/0/delta/content"),
        tools = fact(value, "/choices/0/delta/tool_calls");
      if (
        state.finished &&
        ((content.kind === "string" && !content.empty) || tools.kind !== "missing")
      )
        fail("data_after_finish");
      if (!["missing", "null", "string"].includes(content.kind)) fail("invalid_content");
      if (content.kind === "string" && !content.empty) state.content = true;
      if (!["missing", "array"].includes(tools.kind)) fail("invalid_tool_call");
      active.count = tools.count ?? 0;
      if (active.count > 128) fail("tool_budget");
      const finish = scalar(value, "/choices/0/finish_reason");
      if (finish !== undefined && finish !== null) {
        if (state.finished) fail("duplicate_finish");
        active.finish = reason(finish);
      }
    }
    active.phase = "tools";
    return streamOutput(state);
  }
  if (active.pending) {
    const appended = object(object(value.operation).appended);
    if (typeof appended.handle !== "string") fail("stream_state");
    state.tools[active.pending]!.args = appended.handle;
    delete active.pending;
    active.index = smallInteger(active.index, 128) + 1;
  }
  const i = smallInteger(active.index, 128),
    count = smallInteger(active.count, 128);
  if (i < count) {
    const base = `/choices/0/delta/tool_calls/${i}`,
      paths = [
        base,
        `${base}/index`,
        `${base}/id`,
        `${base}/type`,
        `${base}/function`,
        `${base}/function/name`,
        `${base}/function/arguments`,
      ];
    const query = inspect(value, paths);
    if (query) return query;
    if (
      fact(value, base).kind !== "object" ||
      !["missing", "object"].includes(fact(value, `${base}/function`).kind)
    )
      fail("invalid_tool_call");
    const index = String(smallInteger(scalar(value, `${base}/index`), 127)),
      tool = state.tools[index] ?? { id: "", name: "" };
    const id = fact(value, `${base}/id`),
      name = fact(value, `${base}/function/name`),
      type = scalar(value, `${base}/type`);
    if (type !== undefined && type !== "function") fail("invalid_tool_call");
    if (id.kind !== "missing") {
      if (id.kind !== "string" || !id.string_info || (tool.id && tool.id !== id.string_info.digest))
        fail("invalid_tool_call");
      tool.id = id.empty ? "" : id.string_info.digest;
    }
    if (name.kind !== "missing") {
      if (name.kind !== "string") fail("invalid_tool_call");
      if (!name.empty) tool.name = "present";
    }
    if (Object.keys(state.tools).length >= 128 && !state.tools[index]) fail("tool_budget");
    state.tools[index] = tool;
    state.content = true;
    const args = fact(value, `${base}/function/arguments`);
    if (args.kind !== "missing") {
      if (args.kind !== "string") fail("invalid_tool_call");
      active.pending = index;
      return {
        abi_version: 1,
        kind: "buffer_append",
        ...(tool.args ? { target: tool.args } : {}),
        document: value.document.handle,
        pointers: [`${base}/function/arguments`],
      };
    }
    active.index = i + 1;
    return streamOutput(state);
  }
  const query = inspect(value, ["/model"]);
  if (query) return query;
  if (active.finish) {
    state.finished = true;
    state.finishKind = active.finish;
  }
  return complete(state.finished ? "hold" : "emit");
}
export function invoke() {
  runGuest(Host, (input, previous) => {
    const bytes = decode64(input.payload);
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
      return {
        result: prepare(
          input,
          object(JSON.parse(new TextDecoder().decode(bytes))) as DocumentInput,
        ),
        state: null,
      };
    if (input.hook === "response")
      return {
        result: response(
          input,
          object(JSON.parse(new TextDecoder().decode(bytes))) as DocumentInput,
        ),
        state: null,
      };
    if (input.hook === "error")
      return {
        result: { abi_version: 1, kind: "failure", status: 502, code: "upstream_error" },
        state: null,
      };
    if (input.hook === "stream_dispose")
      return { result: { abi_version: 1, kind: "disposed" }, state: null };
    if (input.hook === "stream_open") {
      const payload = object(exactParse(new TextDecoder().decode(bytes), 400));
      if (typeof payload.model !== "string" || !payload.model)
        throw new AdapterFailure(400, "invalid_model");
      const state: State = {
        session: input.session.id,
        sequence: input.session.sequence,
        model: payload.model,
        lastFrame: 0,
        finishKind: "",
        finished: false,
        content: false,
        usage: null,
        tools: {},
        done: false,
      };
      return {
        result: {
          abi_version: 1,
          kind: "output",
          consumed: 0,
          chunks: [],
          finality: "UNKNOWN",
          needs_input: true,
        },
        state,
      };
    }
    if (!["stream_feed", "stream_drain", "stream_finish"].includes(input.hook))
      throw new AdapterFailure(422, "unsupported_capability");
    const state = previous as State;
    if (
      !state ||
      state.session !== input.session.id ||
      smallInteger(state.sequence, Number.MAX_SAFE_INTEGER) + 1 !== input.session.sequence ||
      (state.done && input.hook !== "stream_finish")
    )
      fail("stream_state");
    state.sequence = input.session.sequence;
    if (input.hook === "stream_finish") {
      if (!state.done || state.active) fail("stream_truncated");
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
    return {
      result: streamFrame(state, object(JSON.parse(new TextDecoder().decode(bytes))) as FrameInput),
      state,
    };
  });
}
