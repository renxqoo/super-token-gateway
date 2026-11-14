import {
  AdapterFailure,
  decode64,
  exactParse,
  object,
  runGuest,
  smallInteger,
  type GuestInput,
} from "@gateway/plugin-sdk/guest";
import manifest from "./package/manifest.json";
declare const Host: { inputString(): string; outputString(value: string): void };
type Fact = { kind: string; count?: number; empty?: boolean; literal?: string };
type DocumentInput = {
  document: { handle: string; bytes: number };
  inspection: Record<string, Fact>;
  protocol?: string;
  operation?: string;
  model?: string;
  content?: string;
};
const fields = [
  "/model",
  "/prompt",
  "/lyrics",
  "/stream",
  "/output_format",
  "/audio_setting",
  "/aigc_watermark",
  "/lyrics_optimizer",
  "/is_instrumental",
];
function fact(value: DocumentInput, path: string, status: 400 | 502): Fact {
  const found = value.inspection[path];
  if (!found) throw new AdapterFailure(status, "inspection_missing");
  return found;
}
function scalar(value: DocumentInput, path: string, status: 400 | 502): unknown {
  const found = fact(value, path, status);
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
  if (
    !value.document ||
    !value.inspection ||
    typeof value.model !== "string" ||
    !value.model ||
    value.protocol !== undefined ||
    value.operation !== "music"
  )
    throw new AdapterFailure(400, "invalid_request");
  if (value.content !== undefined) throw new AdapterFailure(422, "unsupported_semantics");
  const query = inspect(value, ["", ...fields]);
  if (query) return query;
  const root = fact(value, "", 400);
  if (root.kind !== "object") throw new AdapterFailure(400, "invalid_request");
  if (root.count !== fields.filter((path) => fact(value, path, 400).kind !== "missing").length)
    throw new AdapterFailure(422, "unsupported_semantics");
  const map = object(input.config.model_map, 400),
    upstream = map[value.model];
  if (typeof upstream !== "string" || !upstream || scalar(value, "/model", 400) !== value.model)
    throw new AdapterFailure(422, "model_unavailable");
  const stream = scalar(value, "/stream", 400),
    format = scalar(value, "/output_format", 400);
  if (stream !== undefined && typeof stream !== "boolean")
    throw new AdapterFailure(400, "invalid_stream");
  if (format !== undefined && format !== "hex" && format !== "url")
    throw new AdapterFailure(400, "invalid_request");
  if (stream === true || format === "url") throw new AdapterFailure(422, "unsupported_semantics");
  if (value.document.bytes > smallInteger(input.config.max_input_tokens, 16 << 20, 422))
    throw new AdapterFailure(413, "input_ceiling");
  return {
    abi_version: 1,
    kind: "prepared",
    request: {
      method: "POST",
      path: "/v1/music_generation",
      headers: { "content-type": "application/json", accept: "application/json" },
      body: {
        document: value.document.handle,
        edits: [{ op: "set", pointer: "/model", value: JSON.stringify(upstream) }],
      },
      auth: { type: "bearer" },
      manifest: {
        operation: "music",
        model: value.model,
        upstream_model: upstream,
        stream: false,
        ceilings: { requests: "1" },
        input_bytes: value.document.bytes,
      },
    },
  };
}
function response(value: DocumentInput) {
  if (!value.document || !value.inspection || value.operation !== "music")
    throw new AdapterFailure(502, "invalid_request");
  const query = inspect(value, [
    "",
    "/base_resp/status_code",
    "/data",
    "/data/status",
    "/data/audio",
    "/extra_info/music_duration",
  ]);
  if (query) return query;
  if (fact(value, "", 502).kind !== "object") throw new AdapterFailure(502, "invalid_object");
  if (
    fact(value, "/base_resp/status_code", 502).literal !== "0" ||
    fact(value, "/data/status", 502).literal !== "2"
  )
    throw new AdapterFailure(502, "upstream_error");
  const audio = fact(value, "/data/audio", 502);
  if (audio.kind !== "string" || audio.empty) throw new AdapterFailure(502, "empty_response");
  const duration = fact(value, "/extra_info/music_duration", 502);
  let measured: Record<string, string> = { requests: "1" },
    finality = "UNKNOWN";
  if (duration.kind !== "missing") {
    if (duration.kind !== "number" || !/^(0|[1-9][0-9]{0,15})$/.test(duration.literal ?? ""))
      throw new AdapterFailure(502, "invalid_upstream_usage");
    measured = { music_ms: duration.literal!, requests: "1" };
    finality = "FINAL";
  }
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    body: { document: value.document.handle, edits: [] },
    usage: measured,
    finality,
    terminal: { kind: "complete", bytes: "" },
    needs_input: false,
  };
}
export function invoke() {
  runGuest(Host, (input) => {
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
    if (input.hook === "error")
      return {
        result: { abi_version: 1, kind: "failure", status: 502, code: "upstream_error" },
        state: null,
      };
    if (input.hook !== "prepare" && input.hook !== "response")
      throw new AdapterFailure(422, "unsupported_capability");
    const value = object(JSON.parse(new TextDecoder().decode(bytes))) as DocumentInput;
    return {
      result: input.hook === "prepare" ? prepare(input, value) : response(value),
      state: null,
    };
  });
}
