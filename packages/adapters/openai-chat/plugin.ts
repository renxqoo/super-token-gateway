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
  resourceId?: string;
  remoteId?: string;
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
  native: boolean;
  media?: string;
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
  return value?.images !== undefined ||
    (value?.input_tokens !== undefined && value.output_tokens !== undefined)
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
  content?: string;
  input_characters?: string;
  resource_id?: string;
  action?: string;
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
function authOf(input: GuestInput) {
  return input.config.auth === "api-key"
    ? { type: "api-key", header: "api-key" }
    : input.config.auth === "none"
      ? { type: "none" }
      : { type: "bearer" };
}
function prepareResponses(input: GuestInput, value: DocumentInput) {
  const query = inspect(value, ["", "/model", "/stream", "/max_output_tokens", "/background"]);
  if (query) return query;
  if (fact(value, "").kind !== "object") throw new AdapterFailure(400, "invalid_request");
  const map = object(input.config.model_map, 400),
    upstream = map[value.model!];
  if (typeof upstream !== "string" || !upstream || scalar(value, "/model", 400) !== value.model)
    throw new AdapterFailure(422, "model_unavailable");
  if (input.config.path_style === "azure") throw new AdapterFailure(422, "unsupported_capability");
  const stream = scalar(value, "/stream", 400),
    background = scalar(value, "/background", 400);
  if (stream !== undefined && typeof stream !== "boolean")
    throw new AdapterFailure(400, "invalid_stream");
  if (background !== undefined && background !== null && typeof background !== "boolean")
    throw new AdapterFailure(400, "invalid_request");
  if (background === true && stream === true)
    throw new AdapterFailure(422, "unsupported_semantics");
  const max = smallInteger(input.config.max_output_tokens, 1000000, 422),
    inputTokens = smallInteger(input.config.max_input_tokens, 16 << 20, 422);
  if (value.document.bytes > inputTokens) throw new AdapterFailure(413, "input_ceiling");
  const requested = scalar(value, "/max_output_tokens", 400),
    output = requested === undefined ? max : smallInteger(requested, max, 422);
  if (output < 1) throw new AdapterFailure(422, "invalid_output_ceiling");
  const edits: { op: string; pointer: string; value?: string }[] = [
    { op: "set", pointer: "/model", value: JSON.stringify(upstream) },
  ];
  if (requested === undefined)
    edits.push({ op: "set", pointer: "/max_output_tokens", value: String(output) });
  return {
    abi_version: 1,
    kind: "prepared",
    request: {
      method: "POST",
      path: "/v1/responses",
      headers: {
        "content-type": "application/json",
        accept: stream ? "text/event-stream" : "application/json",
      },
      body: { document: value.document.handle, edits },
      auth: authOf(input),
      manifest: {
        operation: "responses",
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
function responsesUsage(value: DocumentInput, base: string, previous: Usage | null) {
  const usageFact = fact(value, base);
  if (usageFact.kind !== "object") fail("invalid_upstream_usage");
  const raw: Record<string, unknown> = {};
  for (const name of ["input_tokens", "output_tokens"]) {
    const n = scalar(value, `${base}/${name}`);
    if (n !== undefined) raw[name] = n;
  }
  const cached = scalar(value, `${base}/input_tokens_details/cached_tokens`),
    reasoning = scalar(value, `${base}/output_tokens_details/reasoning_tokens`);
  if (cached !== undefined) raw.prompt_tokens_details = { cached_tokens: cached };
  if (reasoning !== undefined) raw.completion_tokens_details = { reasoning_tokens: reasoning };
  const measured = usage({ usage: raw }, previous);
  if (measured?.input_tokens === undefined || measured.output_tokens === undefined)
    fail("invalid_upstream_usage");
  return measured;
}
const responseUsagePaths = (base: string) => [
  base,
  `${base}/input_tokens`,
  `${base}/output_tokens`,
  `${base}/input_tokens_details/cached_tokens`,
  `${base}/output_tokens_details/reasoning_tokens`,
];
function responseStatus(status: unknown) {
  if (status === "completed") return "complete";
  if (status === "incomplete") return "incomplete";
  if (status === "failed" || status === "cancelled") fail("upstream_error");
  fail("invalid_object");
}
function modelEdit(value: DocumentInput, pointer: string, model: string) {
  return fact(value, pointer).kind === "missing"
    ? []
    : [{ op: "set", pointer, value: JSON.stringify(model) }];
}
function remoteIdentity(value: DocumentInput, pointer: string) {
  const id = scalar(value, pointer);
  if (typeof id !== "string" || !/^[A-Za-z0-9_.:-]{1,200}$/.test(id)) fail("invalid_object");
  return id;
}
function identityEdits(value: DocumentInput, pointer: string, resourceId: string | undefined) {
  if (!resourceId) return { edits: [] as { op: string; pointer: string; value: string }[] };
  const remote = remoteIdentity(value, pointer);
  return {
    edits: [{ op: "set", pointer, value: JSON.stringify(resourceId) }],
    resource: { kind: "response", remote_id: remote },
  };
}
function responsesResponse(value: DocumentInput) {
  const query = inspect(value, [
    "",
    "/id",
    "/error",
    "/object",
    "/status",
    "/model",
    ...responseUsagePaths("/usage"),
  ]);
  if (query) return query;
  if (fact(value, "").kind !== "object") fail("invalid_object");
  if (!["missing", "null"].includes(fact(value, "/error").kind)) fail("upstream_error");
  if (scalar(value, "/object") !== "response") fail("invalid_object");
  const status = scalar(value, "/status");
  if (status === "queued" || status === "in_progress") {
    const deferred = identityEdits(value, "/id", value.resource_id);
    if (!deferred.resource) fail("invalid_object");
    return {
      abi_version: 1,
      kind: "output",
      consumed: 0,
      chunks: [],
      body: {
        document: value.document.handle,
        edits: [...modelEdit(value, "/model", value.model!), ...deferred.edits],
      },
      finality: "UNKNOWN",
      terminal: { kind: "pending", bytes: "" },
      needs_input: false,
      resource: deferred.resource,
    };
  }
  const kind = responseStatus(status),
    measured = responsesUsage(value, "/usage", null),
    identity = identityEdits(value, "/id", value.resource_id);
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: {
      document: value.document.handle,
      edits: [...modelEdit(value, "/model", value.model!), ...identity.edits],
    },
    usage: measured,
    finality: finality(measured),
    terminal: { kind, bytes: "" },
    needs_input: false,
    ...(identity.resource ? { resource: identity.resource } : {}),
  };
}
function prepareFiles(input: GuestInput, value: DocumentInput) {
  if (value.content !== "multipart") throw new AdapterFailure(400, "invalid_request");
  const query = inspect(value, ["", "/purpose", "/file", "/model"]);
  if (query) return query;
  if (fact(value, "").kind !== "object" || fact(value, "/file").kind !== "object")
    throw new AdapterFailure(400, "invalid_request");
  const purpose = scalar(value, "/purpose", 400);
  if (typeof purpose !== "string" || !/^[a-z][a-z0-9_-]{0,63}$/.test(purpose))
    throw new AdapterFailure(400, "invalid_request");
  const map = object(input.config.model_map, 400),
    upstream = map[value.model!],
    model = fact(value, "/model");
  if (
    typeof upstream !== "string" ||
    !upstream ||
    (model.kind !== "missing" && scalar(value, "/model", 400) !== value.model)
  )
    throw new AdapterFailure(422, "model_unavailable");
  if (input.config.path_style === "azure") throw new AdapterFailure(422, "unsupported_capability");
  return {
    abi_version: 1,
    kind: "prepared",
    request: {
      method: "POST",
      path: "/v1/files",
      headers: { accept: "application/json" },
      body: {
        document: value.document.handle,
        edits: model.kind === "missing" ? [] : [{ op: "remove", pointer: "/model" }],
      },
      body_format: "multipart",
      auth: authOf(input),
      manifest: {
        operation: "files",
        model: value.model,
        upstream_model: upstream,
        stream: false,
        ceilings: { requests: "1" },
        input_bytes: value.document.bytes,
      },
    },
  };
}
function prepareBatches(input: GuestInput, value: DocumentInput) {
  const query = inspect(value, ["", "/input_file_id", "/endpoint", "/completion_window", "/model"]);
  if (query) return query;
  if (fact(value, "").kind !== "object" || fact(value, "/model").kind !== "missing")
    throw new AdapterFailure(400, "invalid_request");
  for (const pointer of ["/input_file_id", "/endpoint", "/completion_window"])
    if (typeof scalar(value, pointer, 400) !== "string")
      throw new AdapterFailure(400, "invalid_request");
  const upstream = object(input.config.model_map, 400)[value.model!];
  if (typeof upstream !== "string" || !upstream) throw new AdapterFailure(422, "model_unavailable");
  if (input.config.path_style === "azure") throw new AdapterFailure(422, "unsupported_capability");
  return {
    abi_version: 1,
    kind: "prepared",
    request: {
      method: "POST",
      path: "/v1/batches",
      headers: { accept: "application/json" },
      body: { document: value.document.handle, edits: [] },
      auth: authOf(input),
      manifest: {
        operation: "batches",
        model: value.model,
        upstream_model: upstream,
        stream: false,
        ceilings: { requests: "1" },
        input_bytes: value.document.bytes,
      },
    },
  };
}
function batchesResponse(value: DocumentInput) {
  const query = inspect(value, ["", "/id", "/object", "/error"]);
  if (query) return query;
  if (fact(value, "").kind !== "object") fail("invalid_object");
  if (!["missing", "null"].includes(fact(value, "/error").kind)) fail("upstream_error");
  if (scalar(value, "/object") !== "batch") fail("invalid_object");
  const identity = identityEdits(value, "/id", value.resource_id);
  if (!identity.resource) fail("invalid_object");
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: { document: value.document.handle, edits: identity.edits },
    finality: "UNKNOWN",
    terminal: { kind: "pending", bytes: "" },
    needs_input: false,
    resource: { kind: "batch", remote_id: identity.resource.remote_id },
  };
}
function filesResponse(value: DocumentInput) {
  const query = inspect(value, ["", "/id", "/object", "/error"]);
  if (query) return query;
  if (fact(value, "").kind !== "object") fail("invalid_object");
  if (!["missing", "null"].includes(fact(value, "/error").kind)) fail("upstream_error");
  if (scalar(value, "/object") !== "file") fail("invalid_object");
  const identity = identityEdits(value, "/id", value.resource_id);
  if (!identity.resource) fail("invalid_object");
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: { document: value.document.handle, edits: identity.edits },
    usage: { requests: "1" },
    finality: "FINAL",
    terminal: { kind: "complete", bytes: "" },
    needs_input: false,
    resource: { kind: "file", remote_id: identity.resource.remote_id },
  };
}
function responsesResource(value: DocumentInput) {
  const file = value.resource_id?.startsWith("file_") === true,
    batch = value.resource_id?.startsWith("batch_") === true;
  const objects: Record<string, string> = file
    ? { get: "file", delete: "file" }
    : batch
      ? { get: "batch", cancel: "batch" }
      : {
          get: "response",
          cancel: "response",
          delete: "response",
          input_items: "list",
        };
  const query = inspect(value, ["", "/id", "/object", "/model", "/error"]);
  if (query) return query;
  if (fact(value, "").kind !== "object") fail("invalid_object");
  if (!["missing", "null"].includes(fact(value, "/error").kind)) fail("upstream_error");
  if (!value.action || !value.resource_id || scalar(value, "/object") !== objects[value.action])
    fail("invalid_object");
  const edits =
    value.action === "input_items"
      ? []
      : [
          ...(value.model && !file && !batch ? modelEdit(value, "/model", value.model) : []),
          ...identityEdits(value, "/id", value.resource_id).edits,
        ];
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: { document: value.document.handle, edits },
    finality: "UNKNOWN",
    terminal: { kind: "complete", bytes: "" },
    needs_input: false,
  };
}
function resourceRequest(input: GuestInput, payload: Record<string, unknown>) {
  const remote = payload.remote_id,
    action = payload.action;
  if (
    !["response", "file", "batch"].includes(String(payload.kind)) ||
    typeof remote !== "string" ||
    !/^[A-Za-z0-9_.:-]{1,200}$/.test(remote) ||
    typeof action !== "string"
  )
    throw new AdapterFailure(422, "unsupported_capability");
  if (input.config.path_style === "azure") throw new AdapterFailure(422, "unsupported_capability");
  const suffix: Record<string, [string, string]> =
    payload.kind === "file"
      ? { get: ["GET", ""], delete: ["DELETE", ""], content: ["GET", "/content"] }
      : payload.kind === "batch"
        ? { get: ["GET", ""], cancel: ["POST", "/cancel"] }
        : {
            get: ["GET", ""],
            delete: ["DELETE", ""],
            cancel: ["POST", "/cancel"],
            input_items: ["GET", "/input_items"],
          };
  const route = suffix[action];
  if (!route) throw new AdapterFailure(422, "unsupported_capability");
  return {
    abi_version: 1,
    kind: "resource_request",
    request: {
      method: route[0],
      path: `/v1/${{ file: "files", batch: "batches" }[String(payload.kind)] ?? "responses"}/${remote}${route[1]}`,
      headers: { accept: action === "content" ? "*/*" : "application/json" },
      auth: authOf(input),
    },
  };
}
function responsesFrame(state: State, value: FrameInput) {
  const frame = value.frame;
  if (frame.ordinal !== smallInteger(state.lastFrame, Number.MAX_SAFE_INTEGER) + 1)
    fail("stream_state");
  if (!frame.data.bytes) {
    state.lastFrame = frame.ordinal;
    return streamOutput(state, { handle: frame.handle, disposition: "drop" });
  }
  const query = inspect(value, [
    "",
    "/type",
    "/response/id",
    "/response/model",
    "/response/status",
    ...responseUsagePaths("/response/usage"),
  ]);
  if (query) return query;
  const type = scalar(value, "/type");
  if (typeof type !== "string" || (frame.event.value !== "" && frame.event.value !== type))
    fail("invalid_stream");
  state.lastFrame = frame.ordinal;
  const edits: { op: string; pointer: string; value?: string }[] = modelEdit(
    value,
    "/response/model",
    state.model,
  );
  let declared: { kind: string; remote_id: string } | undefined;
  if (state.resourceId && fact(value, "/response/id").kind !== "missing") {
    const remote = remoteIdentity(value, "/response/id");
    if (state.remoteId && state.remoteId !== remote) fail("invalid_stream");
    if (!state.remoteId) declared = { kind: "response", remote_id: remote };
    state.remoteId = remote;
    edits.push({ op: "set", pointer: "/response/id", value: JSON.stringify(state.resourceId) });
  }
  const body = edits.length ? { body: { document: value.document.handle, edits } } : {};
  if (type === "error" || type === "response.failed") fail("upstream_error");
  if (type === "response.completed" || type === "response.incomplete") {
    const kind = responseStatus(scalar(value, "/response/status"));
    state.usage = responsesUsage(value, "/response/usage", state.usage);
    state.done = true;
    return {
      ...streamOutput(
        state,
        { handle: frame.handle, disposition: "hold", ...body },
        { kind, bytes: "" },
      ),
      ...(declared ? { resource: declared } : {}),
    };
  }
  if (!type.startsWith("response.")) fail("invalid_stream");
  return {
    ...streamOutput(state, { handle: frame.handle, disposition: "emit", ...body }),
    ...(declared ? { resource: declared } : {}),
  };
}
const mediaStreams: Record<string, [string, string]> = {
  images: ["image_generation.", "image_generation.completed"],
  image_edits: ["image_edit.", "image_edit.completed"],
  speech: ["speech.audio.", "speech.audio.done"],
  transcription: ["transcript.text.", "transcript.text.done"],
};
function mediaFrame(state: State, value: FrameInput) {
  const frame = value.frame;
  if (frame.ordinal !== smallInteger(state.lastFrame, Number.MAX_SAFE_INTEGER) + 1)
    fail("stream_state");
  if (!frame.data.bytes) {
    state.lastFrame = frame.ordinal;
    return streamOutput(state, { handle: frame.handle, disposition: "drop" });
  }
  const query = inspect(value, [
    "",
    "/type",
    "/error",
    "/b64_json",
    "/usage",
    "/usage/input_tokens",
    "/usage/output_tokens",
  ]);
  if (query) return query;
  state.lastFrame = frame.ordinal;
  const type = scalar(value, "/type");
  if (typeof type !== "string" || (frame.event.value !== "" && frame.event.value !== type))
    fail("invalid_stream");
  if (type === "error" || !["missing", "null"].includes(fact(value, "/error").kind))
    fail("upstream_error");
  const [prefix, terminal] = mediaStreams[state.media!]!;
  if (!type.startsWith(prefix)) fail("invalid_stream");
  if (type !== terminal) return streamOutput(state, { handle: frame.handle, disposition: "emit" });
  if (!["object", "missing", "null"].includes(fact(value, "/usage").kind)) fail("invalid_object");
  const raw: Record<string, unknown> = {};
  for (const name of ["input_tokens", "output_tokens"]) {
    const current = scalar(value, `/usage/${name}`);
    if (current !== undefined) raw[name] = current;
  }
  const image = fact(value, "/b64_json");
  state.usage = {
    ...usage({ usage: raw }, state.usage),
    ...(imageOperations.includes(state.media!) && image.kind === "string" && !image.empty
      ? { images: "1" }
      : {}),
  };
  state.done = true;
  return streamOutput(
    state,
    { handle: frame.handle, disposition: "hold" },
    { kind: "complete", bytes: "" },
  );
}
const imageOperations = ["images", "image_edits"];
function prepareImages(input: GuestInput, value: DocumentInput) {
  const operation = value.operation!,
    form = value.content === "multipart";
  const query = inspect(value, ["", "/model", "/n", "/stream"]);
  if (query) return query;
  if (fact(value, "").kind !== "object") throw new AdapterFailure(400, "invalid_request");
  const map = object(input.config.model_map, 400),
    upstream = map[value.model!];
  if (typeof upstream !== "string" || !upstream || scalar(value, "/model", 400) !== value.model)
    throw new AdapterFailure(422, "model_unavailable");
  let n = scalar(value, "/n", 400),
    stream = scalar(value, "/stream", 400);
  if (form) {
    if (n !== undefined && (typeof n !== "string" || !/^[0-9]{1,6}$/.test(n)))
      throw new AdapterFailure(400, "invalid_request");
    n = n === undefined ? undefined : Number(n);
    if (stream !== undefined && stream !== "true" && stream !== "false")
      throw new AdapterFailure(400, "invalid_stream");
    stream = stream === undefined ? undefined : stream === "true";
  }
  if (stream !== undefined && typeof stream !== "boolean")
    throw new AdapterFailure(400, "invalid_stream");
  const count = n === undefined ? 1 : smallInteger(n, 1000000, 400);
  if (count < 1 || count > 8) throw new AdapterFailure(422, "unsupported_choice_count");
  const inputTokens = smallInteger(input.config.max_input_tokens, 16 << 20, 422);
  if (value.document.bytes > inputTokens) throw new AdapterFailure(413, "input_ceiling");
  const route = operation === "images" ? "images/generations" : "images/edits";
  let path = `/v1/${route}`;
  if (input.config.path_style === "azure") {
    const version = input.config.api_version;
    if (typeof version !== "string" || !/^\d{4}-\d{2}-\d{2}(?:-preview)?$/.test(version))
      throw new AdapterFailure(422, "unsupported_capability");
    if (!/^[A-Za-z0-9._-]{1,64}$/.test(upstream))
      throw new AdapterFailure(422, "model_unavailable");
    path = `/openai/deployments/${upstream}/${route}?api-version=${version}`;
  }
  return {
    abi_version: 1,
    kind: "prepared",
    request: {
      method: "POST",
      path,
      headers: {
        "content-type": "application/json",
        accept: stream ? "text/event-stream" : "application/json",
      },
      body: {
        document: value.document.handle,
        edits: [{ op: "set", pointer: "/model", value: JSON.stringify(upstream) }],
      },
      ...(form ? { body_format: "multipart" } : {}),
      auth: authOf(input),
      manifest: {
        operation,
        model: value.model,
        upstream_model: upstream,
        stream: stream === true,
        ceilings: { input_tokens: String(inputTokens), images: String(count), requests: "1" },
        images: String(count),
        input_bytes: value.document.bytes,
      },
    },
  };
}
function imagesResponse(value: DocumentInput) {
  const query = inspect(value, ["", "/error", "/data"]);
  if (query) return query;
  if (fact(value, "").kind !== "object") fail("invalid_object");
  if (!["missing", "null"].includes(fact(value, "/error").kind)) fail("upstream_error");
  const data = fact(value, "/data");
  if (data.kind !== "array" || !data.count) fail("invalid_object");
  const tokens = inspect(value, ["/usage", "/usage/input_tokens", "/usage/output_tokens"]);
  if (tokens) return tokens;
  if (!["object", "missing", "null"].includes(fact(value, "/usage").kind)) fail("invalid_object");
  const raw: Record<string, unknown> = {};
  for (const name of ["input_tokens", "output_tokens"]) {
    const current = scalar(value, `/usage/${name}`);
    if (current !== undefined) raw[name] = current;
  }
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: { document: value.document.handle, edits: [] },
    usage: { ...usage({ usage: raw }, null), images: String(data.count) },
    finality: "FINAL",
    terminal: { kind: "complete", bytes: "" },
    needs_input: false,
  };
}
const audioOperations = ["speech", "transcription", "translation"];
const transcriptFormats = ["json", "verbose_json", "text", "srt", "vtt"];
function prepareAudio(input: GuestInput, value: DocumentInput) {
  const operation = value.operation!,
    form = value.content === "multipart";
  if (form !== (operation !== "speech")) throw new AdapterFailure(400, "invalid_request");
  const query = inspect(value, [
    "",
    "/model",
    "/input",
    "/file",
    "/stream",
    "/stream_format",
    "/response_format",
  ]);
  if (query) return query;
  if (fact(value, "").kind !== "object") throw new AdapterFailure(400, "invalid_request");
  const map = object(input.config.model_map, 400),
    upstream = map[value.model!];
  if (typeof upstream !== "string" || !upstream || scalar(value, "/model", 400) !== value.model)
    throw new AdapterFailure(422, "model_unavailable");
  const inputTokens = smallInteger(input.config.max_input_tokens, 16 << 20, 422);
  const ceilings: Usage = { input_tokens: String(inputTokens), requests: "1" };
  let stream = false,
    binary: { usage?: Usage } | undefined,
    quantities: Usage | undefined,
    route: string;
  if (operation === "speech") {
    route = "audio/speech";
    if (fact(value, "/input").kind !== "string" || !value.input_characters)
      throw new AdapterFailure(400, "invalid_request");
    if (scalar(value, "/stream", 400) !== undefined)
      throw new AdapterFailure(422, "unsupported_semantics");
    const format = scalar(value, "/stream_format", 400);
    if (format !== undefined && format !== "sse" && format !== "audio")
      throw new AdapterFailure(400, "invalid_stream");
    stream = format === "sse";
    ceilings.characters = value.input_characters;
    quantities = { characters: value.input_characters };
    if (!stream) binary = { usage: { characters: value.input_characters } };
  } else {
    route = operation === "transcription" ? "audio/transcriptions" : "audio/translations";
    if (fact(value, "/file").kind !== "object") throw new AdapterFailure(400, "invalid_request");
    const requested = scalar(value, "/stream", 400);
    if (requested !== undefined && requested !== "true" && requested !== "false")
      throw new AdapterFailure(400, "invalid_stream");
    stream = requested === "true";
    if (stream && operation === "translation")
      throw new AdapterFailure(422, "unsupported_semantics");
    const format = scalar(value, "/response_format", 400) ?? "json";
    if (typeof format !== "string" || !transcriptFormats.includes(format))
      throw new AdapterFailure(422, "unsupported_semantics");
    if (stream && format !== "json" && format !== "text")
      throw new AdapterFailure(422, "unsupported_semantics");
    if (!stream && !format.endsWith("json")) binary = {};
  }
  if (value.document.bytes > inputTokens && !form) throw new AdapterFailure(413, "input_ceiling");
  let path = `/v1/${route}`;
  if (input.config.path_style === "azure") {
    const version = input.config.api_version;
    if (typeof version !== "string" || !/^\d{4}-\d{2}-\d{2}(?:-preview)?$/.test(version))
      throw new AdapterFailure(422, "unsupported_capability");
    if (!/^[A-Za-z0-9._-]{1,64}$/.test(upstream))
      throw new AdapterFailure(422, "model_unavailable");
    path = `/openai/deployments/${upstream}/${route}?api-version=${version}`;
  }
  return {
    abi_version: 1,
    kind: "prepared",
    request: {
      method: "POST",
      path,
      headers: {
        "content-type": "application/json",
        accept: stream ? "text/event-stream" : "*/*",
      },
      body: {
        document: value.document.handle,
        edits: [{ op: "set", pointer: "/model", value: JSON.stringify(upstream) }],
      },
      ...(form ? { body_format: "multipart" } : {}),
      ...(binary ? { binary } : {}),
      auth: authOf(input),
      manifest: {
        operation,
        model: value.model,
        upstream_model: upstream,
        stream,
        ceilings,
        ...(quantities ? { quantities } : {}),
        input_bytes: value.document.bytes,
      },
    },
  };
}
function decimalSeconds(literal: string | undefined) {
  if (literal === undefined || !/^(0|[1-9][0-9]{0,15})(\.[0-9]{1,18})?$/.test(literal))
    fail("invalid_upstream_usage");
  const [whole, fraction = ""] = literal.split("."),
    trimmed = fraction.replace(/0+$/, ""),
    seconds = trimmed ? `${whole}.${trimmed}` : whole!;
  if (trimmed.length > 3) return { audio_seconds: seconds };
  const ms = BigInt(whole!) * 1000n + BigInt((trimmed + "000").slice(0, 3));
  return { audio_seconds: seconds, audio_ms: ms.toString() };
}
function transcriptResponse(value: DocumentInput) {
  const query = inspect(value, [
    "",
    "/error",
    "/text",
    "/usage",
    "/usage/type",
    "/usage/seconds",
    "/usage/input_tokens",
    "/usage/output_tokens",
  ]);
  if (query) return query;
  if (fact(value, "").kind !== "object") fail("invalid_object");
  if (!["missing", "null"].includes(fact(value, "/error").kind)) fail("upstream_error");
  if (fact(value, "/text").kind !== "string") fail("invalid_object");
  const usageFact = fact(value, "/usage");
  if (!["object", "missing", "null"].includes(usageFact.kind)) fail("invalid_object");
  let measured: Usage = { requests: "1" };
  if (scalar(value, "/usage/type") === "duration")
    measured = { ...measured, ...decimalSeconds(fact(value, "/usage/seconds").literal) };
  else {
    const raw: Record<string, unknown> = {};
    for (const name of ["input_tokens", "output_tokens"]) {
      const current = scalar(value, `/usage/${name}`);
      if (current !== undefined) raw[name] = current;
    }
    measured = { ...measured, ...usage({ usage: raw }, null) };
  }
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: { document: value.document.handle, edits: [] },
    usage: measured,
    finality: "FINAL",
    terminal: { kind: "complete", bytes: "" },
    needs_input: false,
  };
}
const nativeOperations = ["embeddings", "moderations", "completions"];
function prepareNative(input: GuestInput, value: DocumentInput) {
  const operation = value.operation!;
  const query = inspect(value, ["", "/model", "/stream", "/n", "/max_tokens"]);
  if (query) return query;
  if (fact(value, "").kind !== "object") throw new AdapterFailure(400, "invalid_request");
  const map = object(input.config.model_map, 400),
    upstream = map[value.model!];
  if (typeof upstream !== "string" || !upstream || scalar(value, "/model", 400) !== value.model)
    throw new AdapterFailure(422, "model_unavailable");
  const stream = scalar(value, "/stream", 400);
  if (stream !== undefined && typeof stream !== "boolean")
    throw new AdapterFailure(400, "invalid_stream");
  if (stream === true) throw new AdapterFailure(422, "unsupported_semantics");
  const inputTokens = smallInteger(input.config.max_input_tokens, 16 << 20, 422);
  if (value.document.bytes > inputTokens) throw new AdapterFailure(413, "input_ceiling");
  const edits: { op: string; pointer: string; value?: string }[] = [
    { op: "set", pointer: "/model", value: JSON.stringify(upstream) },
  ];
  const ceilings: Usage = { input_tokens: String(inputTokens), requests: "1" };
  if (operation === "completions") {
    const n = scalar(value, "/n", 400);
    if (n !== undefined && smallInteger(n, 128, 422) !== 1)
      throw new AdapterFailure(422, "unsupported_choice_count");
    const max = smallInteger(input.config.max_output_tokens, 1000000, 422),
      requested = scalar(value, "/max_tokens", 400),
      output = requested === undefined ? max : smallInteger(requested, max, 422);
    if (output < 1) throw new AdapterFailure(422, "invalid_output_ceiling");
    if (requested === undefined)
      edits.push({ op: "set", pointer: "/max_tokens", value: String(output) });
    ceilings.output_tokens = String(output);
  }
  let path = `/v1/${operation}`;
  if (input.config.path_style === "azure") {
    const version = input.config.api_version;
    if (typeof version !== "string" || !/^\d{4}-\d{2}-\d{2}(?:-preview)?$/.test(version))
      throw new AdapterFailure(422, "unsupported_capability");
    if (!/^[A-Za-z0-9._-]{1,64}$/.test(upstream))
      throw new AdapterFailure(422, "model_unavailable");
    path = `/openai/deployments/${upstream}/${operation}?api-version=${version}`;
  }
  return {
    abi_version: 1,
    kind: "prepared",
    request: {
      method: "POST",
      path,
      headers: { "content-type": "application/json", accept: "application/json" },
      body: { document: value.document.handle, edits },
      auth: authOf(input),
      manifest: {
        operation,
        model: value.model,
        upstream_model: upstream,
        stream: false,
        ceilings,
        input_bytes: value.document.bytes,
      },
    },
  };
}
const usageNames = [
  "prompt_tokens",
  "input_tokens",
  "completion_tokens",
  "output_tokens",
  "prompt_cache_hit_tokens",
  "prompt_cache_miss_tokens",
];
function inspectedUsage(value: DocumentInput) {
  const usageFact = fact(value, "/usage");
  if (!["object", "missing", "null"].includes(usageFact.kind)) fail("invalid_object");
  const raw: Record<string, unknown> = {},
    details: Record<string, unknown> = {},
    outputDetails: Record<string, unknown> = {};
  for (const name of usageNames) {
    const current = scalar(value, `/usage/${name}`);
    if (current !== undefined) raw[name] = current;
  }
  const cached = scalar(value, "/usage/prompt_tokens_details/cached_tokens"),
    reasoning = scalar(value, "/usage/completion_tokens_details/reasoning_tokens");
  if (cached !== undefined) details.cached_tokens = cached;
  if (reasoning !== undefined) outputDetails.reasoning_tokens = reasoning;
  raw.prompt_tokens_details = details;
  raw.completion_tokens_details = outputDetails;
  return usage({ usage: raw }, null);
}
function nativeResponse(value: DocumentInput) {
  const query = inspect(value, [
    "",
    "/error",
    "/model",
    "/object",
    "/data",
    "/results",
    "/choices",
    "/choices/0",
    "/choices/0/text",
    "/choices/0/finish_reason",
    "/usage",
  ]);
  if (query) return query;
  if (fact(value, "").kind !== "object") fail("invalid_object");
  if (!["missing", "null"].includes(fact(value, "/error").kind)) fail("upstream_error");
  if (value.operation !== "moderations") {
    const usageQuery = inspect(value, [
      "/usage/prompt_tokens_details/cached_tokens",
      "/usage/completion_tokens_details/reasoning_tokens",
      ...usageNames.map((name) => `/usage/${name}`),
    ]);
    if (usageQuery) return usageQuery;
  }
  let measured: Usage | null,
    kind = "complete";
  if (value.operation === "embeddings") {
    if (scalar(value, "/object") !== "list" || fact(value, "/data").kind !== "array")
      fail("invalid_object");
    measured = inspectedUsage(value);
    if (measured?.input_tokens !== undefined && measured.output_tokens === undefined)
      measured = { ...measured, output_tokens: "0" };
  } else if (value.operation === "moderations") {
    if (fact(value, "/results").kind !== "array" || !fact(value, "/results").count)
      fail("invalid_object");
    measured = { requests: "1" };
  } else {
    if (
      fact(value, "/choices").kind !== "array" ||
      fact(value, "/choices").count !== 1 ||
      fact(value, "/choices/0").kind !== "object" ||
      fact(value, "/choices/0/text").kind !== "string"
    )
      fail("invalid_choices");
    const finish = reason(scalar(value, "/choices/0/finish_reason"));
    if (!["stop", "length", "content_filter"].includes(finish)) fail("invalid_finish_reason");
    if (finish !== "stop") kind = "incomplete";
    measured = inspectedUsage(value);
  }
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: { document: value.document.handle, edits: modelEdit(value, "/model", value.model!) },
    ...(measured ? { usage: measured } : {}),
    finality: value.operation === "moderations" ? "FINAL" : finality(measured),
    terminal: { kind, bytes: "" },
    needs_input: false,
  };
}
function prepare(input: GuestInput, value: DocumentInput) {
  if (!value.document || !value.inspection || typeof value.model !== "string" || !value.model)
    throw new AdapterFailure(400, "invalid_request");
  if (
    value.content === "multipart" &&
    !imageOperations.includes(value.operation ?? "") &&
    !audioOperations.includes(value.operation ?? "") &&
    value.operation !== "files"
  )
    throw new AdapterFailure(422, "unsupported_semantics");
  if (value.protocol === undefined && value.operation === "files")
    return prepareFiles(input, value);
  if (
    value.protocol === undefined &&
    value.operation === "batches" &&
    value.content !== "multipart"
  )
    return prepareBatches(input, value);
  if (value.protocol === "responses" && value.operation === "responses")
    return prepareResponses(input, value);
  if (value.protocol === undefined && nativeOperations.includes(value.operation ?? ""))
    return prepareNative(input, value);
  if (value.protocol === undefined && imageOperations.includes(value.operation ?? ""))
    return prepareImages(input, value);
  if (value.protocol === undefined && audioOperations.includes(value.operation ?? ""))
    return prepareAudio(input, value);
  if (value.protocol !== "chat" || value.operation !== "chat")
    throw new AdapterFailure(422, "unsupported_semantics");
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
  const edits: { op: string; pointer: string; value?: string }[] = [
    { op: "set", pointer: "/model", value: JSON.stringify(upstream) },
  ];
  if (input.config.output_field === "max_tokens") {
    if (maxCompletion !== undefined)
      edits.push({ op: "remove", pointer: "/max_completion_tokens" });
    if (maxTokens === undefined)
      edits.push({ op: "set", pointer: "/max_tokens", value: String(output) });
  } else if (maxTokens === undefined && maxCompletion === undefined)
    edits.push({ op: "set", pointer: "/max_completion_tokens", value: String(output) });
  let path = "/v1/chat/completions";
  if (input.config.path_style === "azure") {
    const version = input.config.api_version;
    if (typeof version !== "string" || !/^\d{4}-\d{2}-\d{2}(?:-preview)?$/.test(version))
      throw new AdapterFailure(422, "unsupported_capability");
    if (!/^[A-Za-z0-9._-]{1,64}$/.test(upstream))
      throw new AdapterFailure(422, "model_unavailable");
    path = `/openai/deployments/${upstream}/chat/completions?api-version=${version}`;
  }
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
  const auth = authOf(input);
  return {
    abi_version: 1,
    kind: "prepared",
    request: {
      method: "POST",
      path,
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
  const measured = inspectedUsage(value);
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
  if (state.native) return responsesFrame(state, value);
  if (state.media) return mediaFrame(state, value);
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
    if (input.hook === "response") {
      const value = object(JSON.parse(new TextDecoder().decode(bytes))) as DocumentInput;
      return {
        result:
          value.protocol === "responses"
            ? responsesResponse(value)
            : value.action !== undefined
              ? fail("invalid_object")
              : value.protocol === undefined && value.operation === "files"
                ? filesResponse(value)
                : value.protocol === undefined && value.operation === "batches"
                  ? batchesResponse(value)
                  : value.protocol === undefined && nativeOperations.includes(value.operation ?? "")
                    ? nativeResponse(value)
                    : value.protocol === undefined &&
                        imageOperations.includes(value.operation ?? "")
                      ? imagesResponse(value)
                      : value.operation === "transcription" || value.operation === "translation"
                        ? transcriptResponse(value)
                        : value.operation === "speech"
                          ? fail("invalid_object")
                          : response(input, value),
        state: null,
      };
    }
    if (input.hook === "resource_prepare")
      return {
        result: resourceRequest(input, object(exactParse(new TextDecoder().decode(bytes), 400))),
        state: null,
      };
    if (input.hook === "resource_response")
      return {
        result: responsesResource(
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
      if (
        (payload.protocol !== undefined &&
          payload.protocol !== "chat" &&
          payload.protocol !== "responses") ||
        (payload.operation !== undefined &&
          (payload.protocol !== undefined ||
            !Object.hasOwn(mediaStreams, String(payload.operation))))
      )
        throw new AdapterFailure(422, "unsupported_semantics");
      if (
        payload.resource_id !== undefined &&
        (payload.protocol !== "responses" ||
          typeof payload.resource_id !== "string" ||
          !/^response_[0-9a-f]{32}$/.test(payload.resource_id))
      )
        throw new AdapterFailure(400, "invalid_request");
      const state: State = {
        ...(payload.resource_id === undefined ? {} : { resourceId: payload.resource_id as string }),
        native: payload.protocol === "responses",
        ...(payload.operation === undefined ? {} : { media: String(payload.operation) }),
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
