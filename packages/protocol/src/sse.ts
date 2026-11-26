import { optionalObject } from "@gateway/contracts";
import type { Protocol } from "@gateway/kernel";
import { stringifyExact } from "./json.ts";

const encoder = new TextEncoder();
const SHARED_UTF8_DECODER = new TextDecoder("utf-8", { fatal: true });
const SHARED_UTF8_DECODER_KEEP_BOM = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const DONE_SSE_BYTES = encoder.encode("data: [DONE]\n\n");

export interface SSEEvent {
  event: string;
  data: string;
  id: string;
  wire: string;
  wireBytes?: Uint8Array;
}

export function hasSseText(event: SSEEvent, protocol: Protocol): boolean {
  if (!event.data || event.data === "[DONE]" || event.event === "error") return false;
  let body: Record<string, unknown>;
  try {
    body = optionalObject(JSON.parse(event.data));
  } catch {
    return false;
  }
  if (body.error || body.type === "error" || body.type === "response.failed") return false;
  const nonempty = (value: unknown) => typeof value === "string" && value.length > 0;
  const type = body.type ?? event.event;
  if (protocol === "chat")
    return (
      Array.isArray(body.choices) &&
      body.choices.some((choice) => {
        const delta = optionalObject(optionalObject(choice).delta);
        return nonempty(delta.content) || nonempty(delta.refusal);
      })
    );
  if (protocol === "messages") {
    const delta = optionalObject(body.delta);
    const block = optionalObject(body.content_block);
    return (
      (type === "content_block_delta" && delta.type === "text_delta" && nonempty(delta.text)) ||
      (type === "content_block_start" && block.type === "text" && nonempty(block.text))
    );
  }
  if (protocol === "responses")
    return (
      (type === "response.output_text.delta" || type === "response.refusal.delta") &&
      nonempty(body.delta)
    );
  return (
    Array.isArray(body.candidates) &&
    body.candidates.some((candidate) => {
      const parts = optionalObject(optionalObject(candidate).content).parts;
      return (
        Array.isArray(parts) &&
        parts.some((item) => {
          const part = optionalObject(item);
          return part.thought !== true && nonempty(part.text);
        })
      );
    })
  );
}

export class SSEParser {
  private streamDecoder: TextDecoder | undefined;
  private seenBytes = false;
  private buffer = "";
  private eventLines: string[] = [];
  private eventSize = 0;
  private trailingBytes = 0;
  constructor(private readonly maxBytes = 1_048_576) {}

  feed(chunk: Uint8Array, flush = false): SSEEvent[] {
    const hadBuffered = this.buffer.length > 0;
    let decoded: string;
    if (this.streamDecoder) {
      decoded = this.streamDecoder.decode(chunk, { stream: !flush });
    } else if (chunk.byteLength === 0) {
      decoded = "";
    } else if (flush || chunk[chunk.byteLength - 1]! < 0x80) {
      decoded = (this.seenBytes ? SHARED_UTF8_DECODER_KEEP_BOM : SHARED_UTF8_DECODER).decode(chunk);
      this.seenBytes = true;
    } else {
      this.streamDecoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: this.seenBytes });
      this.seenBytes = true;
      decoded = this.streamDecoder.decode(chunk, { stream: true });
    }
    const canSliceAscii =
      !hadBuffered && !this.streamDecoder && decoded.length === chunk.byteLength;
    if (decoded.length > 0) this.buffer = hadBuffered ? this.buffer + decoded : decoded;
    const buf = this.buffer;
    const len = buf.length;
    const events: SSEEvent[] = [];
    let start = 0;
    const hasCr = buf.indexOf("\r") !== -1;
    const sliceAscii = canSliceAscii && !hasCr;
    let singleLineStart = -1;
    while (start < len) {
      let i: number;
      if (!hasCr) {
        i = buf.indexOf("\n", start);
        if (i === -1) break;
      } else {
        const nl = buf.indexOf("\n", start);
        const cr = buf.indexOf("\r", start);
        if (nl === -1 && cr === -1) break;
        i = nl === -1 ? cr : cr === -1 ? nl : nl < cr ? nl : cr;
        if (i === cr && i + 1 === len && !flush) break;
      }
      const lineStart = start;
      const line = buf.slice(start, i);
      start = hasCr && buf.charCodeAt(i) === 13 && buf.charCodeAt(i + 1) === 10 ? i + 2 : i + 1;
      this.eventSize += sliceAscii ? line.length + 1 : Buffer.byteLength(line) + 1;
      if (this.eventSize > this.maxBytes) throw new Error("SSE event exceeds limit");
      if (line !== "") {
        if (sliceAscii && this.eventLines.length === 0) singleLineStart = lineStart;
        else singleLineStart = -1;
        this.eventLines.push(line);
        continue;
      }
      if (this.eventLines.length) {
        const lines = this.eventLines;
        this.eventLines = [];
        this.eventSize = 0;
        if (lines.length === 1 && lines[0]!.startsWith("data: ")) {
          const single = lines[0]!;
          const ev: SSEEvent = {
            event: "message",
            id: "",
            data: single.slice(6),
            wire: `${single}\n\n`,
          };
          if (singleLineStart >= 0) ev.wireBytes = chunk.subarray(singleLineStart, start);
          events.push(ev);
        } else {
          const data: string[] = [];
          let event = "message";
          let id = "";
          for (let j = 0; j < lines.length; j++) {
            const current = lines[j]!;
            const colon = current.indexOf(":");
            const name = colon < 0 ? current : current.slice(0, colon);
            const raw = colon < 0 ? "" : current.slice(colon + 1);
            const value = raw.startsWith(" ") ? raw.slice(1) : raw;
            if (name === "data") data.push(value);
            else if (name === "event") event = value;
            else if (name === "id" && !value.includes("\0")) id = value;
          }
          events.push({ event, id, data: data.join("\n"), wire: `${lines.join("\n")}\n\n` });
        }
        singleLineStart = -1;
      } else {
        this.eventSize = 0;
        singleLineStart = -1;
      }
    }
    this.buffer = start === 0 ? buf : start >= len ? "" : buf.slice(start);
    this.trailingBytes = this.buffer.length === 0 ? 0 : Buffer.byteLength(this.buffer);
    if (this.bufferedBytes > this.maxBytes) {
      throw new Error("SSE event exceeds limit");
    }
    return events;
  }
  get bufferedBytes(): number {
    return this.trailingBytes + this.eventSize;
  }
  get incomplete(): boolean {
    return (
      (this.buffer.length > 0 && !this.buffer.startsWith(":")) ||
      this.eventLines.some((line) => !line.startsWith(":"))
    );
  }
}

export const encodeSSE = (data: unknown, event?: string): Uint8Array =>
  !event && data === "[DONE]"
    ? DONE_SSE_BYTES.slice()
    : encoder.encode(
        `${event ? `event: ${event}\n` : ""}data: ${typeof data === "string" ? data : stringifyExact(data)}\n\n`,
      );
