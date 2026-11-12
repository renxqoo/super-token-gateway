import {
  runGuest,
  decode64,
  exactParse,
  exactStringify,
  encode64,
  object,
  type GuestInput,
} from "@gateway/plugin-sdk/guest";
declare const Host: { inputString(): string; outputString(value: string): void };
let hidden: unknown = null;
export function invoke() {
  runGuest(Host, (input: GuestInput, state) => {
    const payload = object(exactParse(new TextDecoder().decode(decode64(input.payload))));
    if (
      typeof input.config.fixture_mode === "string" &&
      input.config.fixture_mode.startsWith("stream_")
    ) {
      const mode = input.config.fixture_mode,
        previous = state ? object(state) : {};
      const output = (frame?: unknown) => ({
        abi_version: 1,
        kind: "output",
        consumed: 0,
        chunks: [],
        finality: "UNKNOWN",
        needs_input: !!frame,
        ...(frame ? { frame } : {}),
      });
      if (input.hook === "stream_open")
        return { result: { ...output(), needs_input: true }, state: {} };
      const frame = object(payload.frame),
        doc = object(payload.document),
        current = String(doc.handle),
        handle = String(frame.handle),
        foreign = String(input.config.foreign_document);
      if (mode === "stream_foreign_doc" || (mode === "stream_replay_doc" && previous.document))
        return {
          result: {
            abi_version: 1,
            kind: "inspect",
            document: mode === "stream_foreign_doc" ? foreign : String(previous.document),
            pointers: [""],
          },
          state: previous,
        };
      if (mode === "stream_foreign_buffer")
        return {
          result: {
            abi_version: 1,
            kind: "buffer_append",
            target: foreign,
            document: current,
            pointers: ["/text"],
          },
          state: previous,
        };
      if (mode === "stream_failure_usage")
        return {
          result: {
            abi_version: 1,
            kind: "failure",
            status: 502,
            code: "invalid_content",
            usage: { input_tokens: "999999" },
            finality: "FINAL",
          },
          state: null,
        };
      if (mode === "stream_replay_doc")
        return { result: output({ handle, disposition: "emit" }), state: { document: current } };
      if (!previous.appended || mode === "stream_repeat_append")
        return {
          result: { abi_version: 1, kind: "buffer_append", document: current, pointers: ["/text"] },
          state: { appended: true },
        };
      if (mode === "stream_append_trap") throw new Error("synthetic trap after append");
      if (mode === "stream_append_loop") while (true) {}
      const appended = object(object(payload.operation).appended);
      return {
        result: output({
          handle,
          disposition: "emit",
          body: {
            document: current,
            edits: [{ op: "set", pointer: "/handle", value: JSON.stringify(appended.handle) }],
          },
        }),
        state: {},
      };
    }
    if (typeof input.config.fixture_mode === "string") {
      const doc = object(payload.document),
        current = String(doc.handle),
        foreign = String(input.config.foreign_document),
        mode = input.config.fixture_mode;
      if (mode === "invalid_result")
        return {
          result: { abi_version: 1, kind: "disposed", "synthetic-sensitive-canary": true },
          state: null,
        };
      if (mode === "invalid_failure")
        return {
          result: {
            abi_version: 1,
            kind: "failure",
            status: 502,
            code: "synthetic_sensitive_canary",
          },
          state: null,
        };
      if (mode === "trap") throw new Error("synthetic trap");
      if (mode === "loop") while (true) {}
      if (mode === "inspect_foreign")
        return {
          result: { abi_version: 1, kind: "inspect", document: foreign, pointers: [""] },
          state: null,
        };
      if (mode === "repeat_inspection")
        return {
          result: { abi_version: 1, kind: "inspect", document: current, pointers: [""] },
          state: null,
        };
      let document = mode === "plan_foreign" ? foreign : current;
      if (mode === "replay" && state) document = String(object(state).document);
      const edits =
        mode === "overlap"
          ? [
              { op: "set", pointer: "/a", value: "{}" },
              { op: "set", pointer: "/a/b", value: "2" },
            ]
          : mode === "oversize"
            ? [{ op: "set", pointer: "/x", value: JSON.stringify("x".repeat(4096)) }]
            : [];
      return {
        result: {
          abi_version: 1,
          kind: "prepared",
          request: {
            method: "POST",
            path: "/v1/chat/completions",
            headers: { "content-type": "application/json" },
            body: { document, edits },
            auth: { type: "bearer" },
            manifest: {
              operation: "chat",
              model: "logical",
              upstream_model: "synthetic",
              stream: false,
              ceilings: { output_tokens: "1" },
              input_bytes: Number(doc.bytes),
            },
          },
        },
        state: { document: current },
      };
    }
    if (payload.mode === "measure") {
      let size = String(payload.source).length;
      if (payload.stage === "envelope") return { result: { size }, state: null };
      const bytes = decode64(String(payload.source));
      size = bytes.length;
      if (payload.stage === "decode") return { result: { size }, state: null };
      const value = exactParse(new TextDecoder().decode(bytes));
      if (payload.stage === "parse") return { result: { size }, state: null };
      const serialized = exactStringify(value);
      size = serialized.length;
      if (payload.stage === "serialize") return { result: { size }, state: null };
      const encoded = encode64(new TextEncoder().encode(serialized));
      size = encoded.length;
      if (payload.stage === "encode") return { result: { size }, state: null };
      size = encode64(new TextEncoder().encode(JSON.stringify({ body: encoded }))).length;
      return { result: { size }, state: null };
    }
    const previous = hidden;
    hidden = payload.value;
    return { result: { previous, value: payload.value, state }, state: payload.state ?? state };
  });
}
export function loop() {
  while (true) {}
}
export function trap() {
  throw new Error("synthetic-trap");
}
