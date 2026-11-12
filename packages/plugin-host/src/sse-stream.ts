import type { AbiResult, Hook, JSONDocumentDescriptor, OutputResult } from "@gateway/plugin-sdk";
import type { createJSONDocuments } from "./json-documents.ts";
import { createSSEFrames, type SSEFrame } from "./sse-frames.ts";
import { PluginHostError } from "./supervisor.ts";
type Documents = ReturnType<typeof createJSONDocuments>;
type Accumulator = { parts: string[]; metered: number; charge: number; sealed: boolean };
export type StreamResult = AbiResult & { output_id?: string };
export function createSSEStream(options: {
  owner: string;
  documents: Documents;
  call: (
    hook: Hook,
    payload: Uint8Array,
    signal?: AbortSignal,
    document?: JSONDocumentDescriptor,
    frame?: boolean,
  ) => Promise<AbiResult>;
  sequence: () => number;
  disposed: () => boolean;
  dispose: () => void;
  takeOutput: (bytes: number) => void;
  maxEventBytes?: number;
  maxInputBytes: number;
  maxOutputBytes: number;
}) {
  const { owner, documents } = options,
    adjust = (n: number) => documents.adjust(owner, n),
    framer = createSSEFrames(
      adjust,
      options.maxEventBytes === undefined ? {} : { maxEventBytes: options.maxEventBytes },
    );
  const buffers = new Map<string, Accumulator>(),
    appendTokens = new Set<string>(),
    leases = new Map<string, { charge: number; values: string[] }>();
  let finalized = false;
  let current: SSEFrame | undefined,
    indexed = false,
    inspection: Record<string, unknown> = {},
    operation: unknown,
    latest: OutputResult | undefined,
    held: Uint8Array[] = [],
    heldCharge = 0,
    heldBytes = 0,
    bufferMetered = 0,
    appendOps = 0,
    lastAppend = 0,
    tokenCharge = 0,
    ended = false,
    opened = false,
    running = false,
    closed = false,
    batchCharge = 0;
  const metadata = (value: string) => ({
    bytes: Buffer.byteLength(value),
    ...(Buffer.byteLength(value) <= 4096 ? { value } : {}),
  });
  function output(usage = latest?.usage, finality = latest?.finality ?? "UNKNOWN"): OutputResult {
    return {
      abi_version: 1,
      kind: "output",
      consumed: 0,
      chunks: [],
      ...(usage ? { usage } : {}),
      finality,
      needs_input: true,
    };
  }
  function clearFrame() {
    appendTokens.clear();
    if (tokenCharge) {
      adjust(-tokenCharge);
      tokenCharge = 0;
    }
    if (current) {
      documents.discard(owner, current.dataHandle);
      framer.consume(current);
    }
    current = undefined;
    indexed = false;
    inspection = {};
    operation = undefined;
  }
  function ensureIndex(frame: SSEFrame) {
    if (!indexed) {
      documents.register(owner, frame.data, frame.dataHandle);
      indexed = true;
    }
  }
  function append(frame: SSEFrame, result: Extract<AbiResult, { kind: "buffer_append" }>) {
    if (result.document !== frame.dataHandle || options.sequence() <= lastAppend)
      throw new PluginHostError("SSE_FRAME");
    ensureIndex(frame);
    if (++appendOps > 65536) throw new PluginHostError("STRING_OPERATION_BUDGET");
    let buffer = result.target ? buffers.get(result.target) : undefined;
    if (result.target && (!buffer || buffer.sealed)) throw new PluginHostError("STRING_HANDLE");
    const handle = result.target ?? crypto.randomUUID();
    if (!buffer) {
      if (buffers.size >= 128) throw new PluginHostError("STRING_COUNT_BUDGET");
      adjust(512);
      buffer = { parts: [], metered: 0, charge: 512, sealed: false };
      buffers.set(handle, buffer);
    }
    for (const path of result.pointers) {
      const token = `${frame.handle}:${path}`;
      if (appendTokens.has(token)) throw new PluginHostError("STRING_REPLAY");
      const fact = documents.inspect(owner, frame.dataHandle, [path])[path]!;
      if (fact.kind !== "string") throw new PluginHostError("STRING_POINTER");
      const guard = fact.bytes * 6 + 512;
      adjust(guard);
      let value: string;
      try {
        value = documents.stringValue(owner, frame.dataHandle, path);
      } catch (error) {
        adjust(-guard);
        throw error;
      }
      const size = Buffer.byteLength(value);
      if (bufferMetered + size > 1048576) {
        adjust(-guard);
        throw new PluginHostError("STRING_BYTE_BUDGET");
      }
      const charge = value.length * 4 + size * 2 + 256;
      adjust(charge - guard);
      buffer.charge += charge;
      buffer.parts.push(value);
      buffer.metered += size;
      bufferMetered += size;
      const tokenBytes = token.length * 2 + 256;
      adjust(tokenBytes);
      tokenCharge += tokenBytes;
      appendTokens.add(token);
    }
    lastAppend = options.sequence();
    return { appended: { handle, bytes: buffer.metered } };
  }
  function check(result: Extract<AbiResult, { kind: "buffer_check" }>) {
    const checked: Record<string, unknown>[] = [];
    for (const handle of result.handles) {
      const buffer = buffers.get(handle);
      if (!buffer) throw new PluginHostError("STRING_HANDLE");
      const units = buffer.parts.reduce((n, s) => n + s.length, 0),
        guard = units * 6 + 512;
      adjust(guard);
      let document: JSONDocumentDescriptor | undefined;
      try {
        const text = buffer.parts.join(""),
          raw = new TextEncoder().encode(text);
        document = documents.register(owner, raw);
        const fact = documents.inspect(owner, document.handle, [""])[""]!;
        checked.push({
          handle,
          valid: true,
          kind: fact.kind,
          metered_bytes: buffer.metered,
          utf8_bytes: raw.length,
          utf16_units: units,
        });
      } catch (error) {
        if (
          error instanceof PluginHostError &&
          ["JSON_INVALID", "JSON_DUPLICATE_KEY", "JSON_LIMIT", "JSON_NUMERIC_LIMIT"].includes(
            error.code,
          )
        )
          checked.push({ handle, valid: false, metered_bytes: buffer.metered, utf16_units: units });
        else throw error;
      } finally {
        if (document) documents.discard(owner, document.handle);
        adjust(-guard);
      }
      if (result.seal) buffer.sealed = true;
    }
    return { checked };
  }
  function render(frame: SSEFrame, body: Uint8Array) {
    const data = new TextDecoder("utf-8", { fatal: true })
        .decode(body)
        .split("\n")
        .map((line) => `data: ${line}`),
      lines: string[] = [];
    let inserted = false;
    for (const line of frame.lines) {
      const colon = line.indexOf(":"),
        name = colon < 0 ? line : line.slice(0, colon);
      if (name === "data") {
        if (!inserted) {
          lines.push(...data);
          inserted = true;
        }
      } else lines.push(line);
    }
    if (!inserted) throw new PluginHostError("SSE_FRAME");
    return new TextEncoder().encode(lines.join("\n") + "\n\n");
  }
  function hold(bytes: Uint8Array) {
    if (heldBytes + bytes.length > 8 << 20 || held.length >= 256)
      throw new PluginHostError("SSE_HELD_BUDGET");
    const charge = bytes.length * 8 + 256;
    adjust(charge);
    heldCharge += charge;
    heldBytes += bytes.length;
    held.push(bytes);
  }
  function encoded(bytes: Uint8Array) {
    const charge = bytes.length * 4 + 256;
    adjust(charge);
    batchCharge += charge;
    return Buffer.from(bytes).toString("base64");
  }
  function release(id: string) {
    const value = leases.get(id);
    if (!value) return false;
    leases.delete(id);
    adjust(-value.charge);
    return true;
  }
  async function invoke(
    hook: Hook,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<StreamResult> {
    if (closed || options.disposed()) throw new PluginHostError("ATTEMPT_CLOSED");
    if (running) throw new PluginHostError("BATCH_CONCURRENT");
    running = true;
    if (hook !== "stream_open" && hook !== "stream_dispose" && !opened) {
      running = false;
      throw new PluginHostError("STREAM_NOT_OPEN");
    }
    if (hook === "stream_open" && opened) {
      running = false;
      throw new PluginHostError("STREAM_ALREADY_OPEN");
    }
    let chunks: string[] = [],
      candidate: OutputResult["terminal"],
      consumed = 0;
    try {
      if (signal?.aborted) throw new PluginHostError("CALL_CANCELLED");
      if (payload.length > options.maxInputBytes) throw new PluginHostError("INPUT_BUDGET");
      if (finalized && hook !== "stream_dispose") throw new PluginHostError("STREAM_FINISHED");
      if (hook === "stream_open") {
        if (opened) throw new PluginHostError("STREAM_ALREADY_OPEN");
        const result = await options.call(hook, payload, signal);
        if (result.kind === "output") {
          latest = result;
          opened = true;
        }
        return result;
      }
      if (hook === "stream_dispose") {
        const result = await options.call(hook, payload, signal);
        options.dispose();
        return result;
      }
      if (!opened) throw new PluginHostError("STREAM_NOT_OPEN");
      if (hook === "stream_feed") {
        if (latest?.terminal) throw new PluginHostError("SSE_AFTER_TERMINAL");
        if (ended) throw new PluginHostError("STREAM_FINISHED");
        consumed = framer.feed(payload);
      } else if (payload.length) throw new PluginHostError("INPUT_BUDGET");
      if (hook === "stream_finish" && !ended) {
        framer.finish();
        ended = true;
      }
      let steps = 0,
        queries = 0,
        frames = 0;
      while (steps++ < 32 && frames < 128) {
        current ??= framer.peek();
        if (!current) break;
        const frame = current;
        const message = {
          frame: {
            handle: frame.handle,
            ordinal: frame.ordinal,
            event: metadata(frame.event),
            id: metadata(frame.id),
            data: {
              handle: frame.dataHandle,
              bytes: frame.data.length,
              ...(frame.data.length <= 4096
                ? { preview: new TextDecoder().decode(frame.data) }
                : {}),
            },
          },
          document: { handle: frame.dataHandle, bytes: frame.data.length },
          inspection,
          operation,
        };
        const result = await options.call(
          hook === "stream_feed" ? "stream_feed" : "stream_drain",
          new TextEncoder().encode(JSON.stringify(message)),
          signal,
          { handle: frame.dataHandle, bytes: frame.data.length },
          true,
        );
        operation = undefined;
        if (result.kind === "failure") {
          const enriched = {
            abi_version: result.abi_version,
            kind: result.kind,
            status: result.status,
            code: result.code,
            ...(latest?.usage ? { usage: latest.usage, finality: latest.finality } : {}),
          };
          options.dispose();
          return enriched;
        }
        if (result.kind === "inspect") {
          if (++queries > 4) throw new PluginHostError("JSON_INSPECTION_BUDGET");
          if (
            result.document !== frame.dataHandle ||
            result.pointers.some((p) => Object.hasOwn(inspection, p))
          )
            throw new PluginHostError("JSON_HANDLE");
          ensureIndex(frame);
          Object.assign(inspection, documents.inspect(owner, frame.dataHandle, result.pointers));
          continue;
        }
        if (result.kind === "buffer_append") {
          operation = append(frame, result);
          continue;
        }
        if (result.kind === "buffer_check") {
          operation = check(result);
          continue;
        }
        if (
          result.kind !== "output" ||
          result.chunks.length ||
          result.body ||
          result.consumed !== 0
        )
          throw new PluginHostError("ABI_OUTPUT");
        if (!result.frame) {
          if (result.needs_input || result.terminal) throw new PluginHostError("SSE_FRAME");
          latest = result;
          if (queries >= 3) {
            inspection = {};
            break;
          }
          continue;
        }
        if (
          result.frame.handle !== frame.handle ||
          (result.frame.body && result.frame.body.document !== frame.dataHandle)
        )
          throw new PluginHostError("SSE_FRAME");
        if (result.terminal && result.frame.disposition !== "hold")
          throw new PluginHostError("ABI_OUTPUT");
        if (
          result.terminal &&
          (result.terminal.bytes || framer.snapshot().ready > 1 || framer.snapshot().incomplete)
        )
          throw new PluginHostError("SSE_AFTER_TERMINAL");
        let bytes = frame.wire,
          materialized: ReturnType<Documents["materialize"]> | undefined,
          renderCharge = 0;
        try {
          if (result.frame.body) {
            ensureIndex(frame);
            materialized = documents.materialize(owner, result.frame.body);
            renderCharge = 8 * (materialized.bytes.length + frame.wire.length) + 512;
            adjust(renderCharge);
            bytes = render(frame, materialized.bytes);
          }
          if (bytes.length > 2 * (options.maxEventBytes ?? 1048576) + (64 << 10))
            throw new PluginHostError("SSE_OUTPUT_BUDGET");
          options.takeOutput(bytes.length);
          if (result.frame.disposition === "hold") hold(bytes);
          else chunks.push(encoded(bytes));
        } finally {
          materialized?.release();
          if (renderCharge) adjust(-renderCharge);
        }
        clearFrame();
        frames++;
        queries = 0;
        latest = result;
        if (result.terminal) {
          if (result.terminal.bytes || framer.snapshot().ready || framer.snapshot().incomplete)
            throw new PluginHostError("SSE_AFTER_TERMINAL");
          const joined = new Uint8Array(Buffer.concat(held, heldBytes));
          candidate = { kind: result.terminal.kind, bytes: encoded(joined) };
          adjust(-heldCharge);
          held = [];
          heldBytes = 0;
          heldCharge = 0;
          break;
        }
      }
      if (current) inspection = {};
      if (ended && !current && !framer.snapshot().ready) {
        const result = await options.call(
          "stream_finish",
          new Uint8Array(),
          signal,
          undefined,
          true,
        );
        if (result.kind === "failure") {
          options.dispose();
          return {
            abi_version: result.abi_version,
            kind: result.kind,
            status: result.status,
            code: result.code,
            ...(latest?.usage ? { usage: latest.usage, finality: latest.finality } : {}),
          };
        }
        if (result.kind !== "output") throw new PluginHostError("ABI_OUTPUT");
        latest = result;
        finalized = true;
        for (const b of buffers.values()) adjust(-b.charge);
        buffers.clear();
        appendTokens.clear();
        if (tokenCharge) {
          adjust(-tokenCharge);
          tokenCharge = 0;
        }
        bufferMetered = 0;
      }
      const result: StreamResult = {
        ...output(),
        consumed,
        chunks,
        ...(candidate ? { terminal: candidate } : {}),
        needs_input: !candidate && !current && framer.snapshot().ready === 0 && !ended,
      };
      const size =
        chunks.reduce((n, s) => n + Buffer.byteLength(s, "base64"), 0) +
        (candidate ? Buffer.byteLength(candidate.bytes, "base64") : 0);
      if (size > options.maxOutputBytes) throw new PluginHostError("OUTPUT_BUDGET");
      if (batchCharge && !options.disposed()) {
        const id = crypto.randomUUID();
        leases.set(id, {
          charge: batchCharge,
          values: [...chunks, ...(candidate ? [candidate.bytes] : [])],
        });
        batchCharge = 0;
        result.output_id = id;
      }
      return result;
    } catch (error) {
      options.dispose();
      if (error instanceof TypeError)
        return { abi_version: 1, kind: "failure", status: 502, code: "invalid_utf8" };
      if (error instanceof PluginHostError) {
        const code: Record<
          string,
          | "stream_frame_budget"
          | "stream_truncated"
          | "data_after_terminal"
          | "invalid_json"
          | "numeric_limit"
          | "tool_budget"
        > = {
          SSE_EVENT_BUDGET: "stream_frame_budget",
          SSE_WIRE_BUDGET: "stream_frame_budget",
          SSE_TRUNCATED: "stream_truncated",
          SSE_AFTER_TERMINAL: "data_after_terminal",
          JSON_INVALID: "invalid_json",
          JSON_DUPLICATE_KEY: "invalid_json",
          JSON_LIMIT: "invalid_json",
          JSON_NUMERIC_LIMIT: "numeric_limit",
          STRING_BYTE_BUDGET: "tool_budget",
        };
        if (code[error.code])
          return {
            abi_version: 1,
            kind: "failure",
            status: 502,
            code: code[error.code]!,
            ...(latest?.usage ? { usage: latest.usage, finality: latest.finality } : {}),
          };
      }
      throw error;
    } finally {
      running = false;
      if (batchCharge) {
        adjust(-batchCharge);
        batchCharge = 0;
      }
    }
  }
  return {
    invoke,
    release,
    snapshot: () => ({
      ...framer.snapshot(),
      buffers: buffers.size,
      bufferMetered,
      appendOps,
      heldBytes,
      outputs: leases.size,
    }),
    dispose() {
      if (closed) return;
      closed = true;
      framer.dispose();
      for (const b of buffers.values()) adjust(-b.charge);
      buffers.clear();
      appendTokens.clear();
      if (tokenCharge) {
        adjust(-tokenCharge);
        tokenCharge = 0;
      }
      bufferMetered = 0;
      for (const id of leases.keys()) release(id);
      adjust(-heldCharge);
      held = [];
      heldBytes = 0;
      heldCharge = 0;
      if (batchCharge) {
        adjust(-batchCharge);
        batchCharge = 0;
      }
      current = undefined;
      inspection = {};
      operation = undefined;
    },
  };
}
