const encoder = new TextEncoder();

export interface SSEEvent {
  event: string;
  data: string;
  id: string;
  wire: string;
}

export class SSEParser {
  private readonly decoder = new TextDecoder("utf-8", { fatal: true });
  private buffer = "";
  private eventLines: string[] = [];
  private eventSize = 0;
  private trailingBytes = 0;
  constructor(private readonly maxBytes = 1_048_576) {}

  feed(chunk: Uint8Array, flush = false): SSEEvent[] {
    this.buffer += this.decoder.decode(chunk, { stream: !flush });
    const events: SSEEvent[] = [];
    let start = 0;
    for (let i = 0; i < this.buffer.length; i++) {
      const ch = this.buffer[i];
      if (ch !== "\r" && ch !== "\n") continue;
      if (ch === "\r" && i + 1 === this.buffer.length && !flush) break;
      const line = this.buffer.slice(start, i);
      if (ch === "\r" && this.buffer[i + 1] === "\n") i++;
      start = i + 1;
      this.eventSize += encoder.encode(line).byteLength + 1;
      if (this.eventSize > this.maxBytes) throw new Error("SSE event exceeds limit");
      if (line !== "") {
        this.eventLines.push(line);
        continue;
      }
      if (this.eventLines.length) {
        const lines = this.eventLines;
        this.eventLines = [];
        this.eventSize = 0;
        const data: string[] = [];
        let event = "message";
        let id = "";
        for (const current of lines) {
          const colon = current.indexOf(":");
          const name = colon < 0 ? current : current.slice(0, colon);
          const raw = colon < 0 ? "" : current.slice(colon + 1);
          const value = raw.startsWith(" ") ? raw.slice(1) : raw;
          if (name === "data") data.push(value);
          if (name === "event") event = value;
          if (name === "id" && !value.includes("\0")) id = value;
        }
        events.push({ event, id, data: data.join("\n"), wire: `${lines.join("\n")}\n\n` });
      } else this.eventSize = 0;
    }
    this.buffer = this.buffer.slice(start);
    this.trailingBytes = encoder.encode(this.buffer).byteLength;
    if (this.bufferedBytes > this.maxBytes) {
      throw new Error("SSE event exceeds limit");
    }
    return events;
  }
  get bufferedBytes(): number {
    return this.trailingBytes + this.eventSize;
  }
  get incomplete(): boolean {
    return this.buffer.length > 0 || this.eventLines.length > 0;
  }
}

export const encodeSSE = (data: unknown, event?: string): Uint8Array =>
  new TextEncoder().encode(
    `${event ? `event: ${event}\n` : ""}data: ${typeof data === "string" ? data : stringifyExact(data)}\n\n`,
  );
import { stringifyExact } from "./json.ts";
