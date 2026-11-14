import { GatewayError, providerObject, text } from "@gateway/contracts";
import { encodeSSE } from "./sse.ts";
import { parseProviderJson } from "./json.ts";

const crcTable = Uint32Array.from({ length: 256 }, (_, index) => {
  let value = index;
  for (let bit = 0; bit < 8; bit++) value = value & 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
  return value >>> 0;
});
function eventCrc32(bytes: Uint8Array): number {
  let value = 0xffffffff;
  for (const byte of bytes) value = crcTable[(value ^ byte) & 255]! ^ (value >>> 8);
  return (value ^ 0xffffffff) >>> 0;
}
const corrupt = () =>
  new GatewayError(502, "invalid_eventstream", "Provider event stream is invalid or truncated");
class BedrockFrames {
  private buffer = new Uint8Array(0);
  private failed = false;
  constructor(private readonly maxBytes: number) {}
  get bufferedBytes(): number {
    return this.buffer.length;
  }
  feed(chunk: Uint8Array, final = false): Uint8Array[] {
    if (this.failed) throw corrupt();
    if (chunk.length > Math.max(65536, this.maxBytes * 2)) {
      const output: Uint8Array[] = [];
      try {
        for (let at = 0; at < chunk.length; at += 65536) {
          output.push(...this.feed(chunk.subarray(at, at + 65536)));
          if (this.failed) break;
        }
        if (final && !this.failed) output.push(...this.feed(new Uint8Array(), true));
      } catch (error) {
        if (!output.length) throw error;
        output.push(
          encodeSSE(
            {
              type: "error",
              error: { type: "provider_stream_error", message: "Provider event stream failed" },
            },
            "error",
          ),
        );
      }
      return output;
    }
    if (this.buffer.length + chunk.length > Math.max(65536, this.maxBytes * 2) + this.maxBytes)
      throw corrupt();
    const merged = new Uint8Array(this.buffer.length + chunk.length);
    merged.set(this.buffer);
    merged.set(chunk, this.buffer.length);
    this.buffer = merged;
    const output: Uint8Array[] = [];
    try {
      while (this.buffer.length >= 12) {
        const view = new DataView(
          this.buffer.buffer,
          this.buffer.byteOffset,
          this.buffer.byteLength,
        );
        const length = view.getUint32(0),
          headerLength = view.getUint32(4);
        if (
          length < 16 ||
          length > this.maxBytes ||
          headerLength > length - 16 ||
          eventCrc32(this.buffer.subarray(0, 8)) !== view.getUint32(8)
        )
          throw corrupt();
        if (this.buffer.length < length) break;
        if (eventCrc32(this.buffer.subarray(0, length - 4)) !== view.getUint32(length - 4))
          throw corrupt();
        const headers: Record<string, string> = {};
        let at = 12;
        while (at < 12 + headerLength) {
          const nameLength = this.buffer[at++]!;
          if (at + nameLength + 3 > 12 + headerLength) throw corrupt();
          const name = new TextDecoder("utf-8", { fatal: true }).decode(
            this.buffer.subarray(at, at + nameLength),
          );
          at += nameLength;
          if (this.buffer[at++] !== 7) throw corrupt();
          const size = view.getUint16(at);
          at += 2;
          if (at + size > 12 + headerLength || Object.hasOwn(headers, name)) throw corrupt();
          headers[name] = new TextDecoder("utf-8", { fatal: true }).decode(
            this.buffer.subarray(at, at + size),
          );
          at += size;
        }
        if (headers[":message-type"] !== "event" || headers[":event-type"] !== "chunk")
          throw new GatewayError(
            502,
            "provider_stream_error",
            "Provider event stream reported an exception",
          );
        const payload = providerObject(
          parseProviderJson(
            new TextDecoder("utf-8", { fatal: true }).decode(
              this.buffer.subarray(12 + headerLength, length - 4),
            ),
          ),
        );
        const encoded = text(payload.bytes);
        if (!encoded || encoded.length % 4 || !/^[A-Za-z0-9+/]*={0,2}$/.test(encoded))
          throw corrupt();
        const body = providerObject(
          parseProviderJson(Buffer.from(encoded, "base64").toString("utf8")),
        );
        output.push(encodeSSE(body, text(body.type) || undefined));
        this.buffer = this.buffer.slice(length);
      }
      if (final && this.buffer.length) throw corrupt();
    } catch (error) {
      this.failed = true;
      this.buffer = new Uint8Array(0);
      if (!output.length) throw error;
      output.push(
        encodeSSE(
          {
            type: "error",
            error: { type: "provider_stream_error", message: "Provider event stream failed" },
          },
          "error",
        ),
      );
    }
    return output;
  }
}
export function bedrockEventStream(
  body: ReadableStream<Uint8Array>,
  maxBytes: number,
  reserveMemory?: (bytes: number) => void,
): ReadableStream<Uint8Array> {
  const reader = body.getReader(),
    frames = new BedrockFrames(maxBytes);
  const pending: Uint8Array[] = [];
  let raw: Uint8Array | undefined,
    offset = 0,
    buffered = 0,
    ended = false,
    closed = false;
  const release = () => {
    if (closed) return;
    closed = true;
    reserveMemory?.(-buffered);
    buffered = 0;
    raw = undefined;
    pending.length = 0;
  };
  const account = (bytes: number) => {
    reserveMemory?.(bytes);
    buffered += bytes;
  };
  return new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        try {
          while (!pending.length && !ended) {
            if (!raw) {
              const result = await reader.read();
              if (result.done) {
                frames.feed(new Uint8Array(), true);
                ended = true;
                reader.releaseLock();
                break;
              }
              account(result.value.length);
              raw = result.value;
              offset = 0;
            }
            const before = frames.bufferedBytes;
            const end = Math.min(raw.length, offset + 65536);
            const output = frames.feed(raw.subarray(offset, end));
            account(
              frames.bufferedBytes -
                before +
                output.reduce((total, chunk) => total + chunk.length, 0),
            );
            pending.push(...output);
            offset = end;
            if (offset === raw.length) {
              account(-raw.length);
              raw = undefined;
            }
          }
          const next = pending.shift();
          if (next) {
            account(-next.length);
            controller.enqueue(next);
          }
          if (ended && !pending.length) {
            release();
            controller.close();
          }
        } catch (error) {
          await reader.cancel().catch(() => undefined);
          release();
          controller.error(error);
        }
      },
      async cancel(reason) {
        try {
          await reader.cancel(reason);
        } finally {
          release();
        }
      },
    },
    { highWaterMark: 0 },
  );
}
