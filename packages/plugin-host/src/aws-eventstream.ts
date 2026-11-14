import { PluginHostError } from "./supervisor.ts";

const crcTable = Uint32Array.from({ length: 256 }, (_, index) => {
  let value = index;
  for (let bit = 0; bit < 8; bit++) value = value & 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
  return value >>> 0;
});
function crc32(bytes: Uint8Array): number {
  let value = 0xffffffff;
  for (const byte of bytes) value = crcTable[(value ^ byte) & 255]! ^ (value >>> 8);
  return (value ^ 0xffffffff) >>> 0;
}
const invalid = () => new PluginHostError("EVENTSTREAM_INVALID");
const utf8 = new TextDecoder("utf-8", { fatal: true });

export function createAwsEventStreamDecoder(maxEventBytes = 1048576) {
  const maxMessage = maxEventBytes * 2 + 4096;
  let buffer = new Uint8Array(0);
  function message(bytes: Uint8Array, view: DataView, length: number, headerLength: number) {
    const headers: Record<string, string> = {};
    let at = 12;
    while (at < 12 + headerLength) {
      const nameLength = bytes[at++]!;
      if (!nameLength || at + nameLength + 3 > 12 + headerLength) throw invalid();
      const name = utf8.decode(bytes.subarray(at, at + nameLength));
      at += nameLength;
      if (bytes[at++] !== 7) throw invalid();
      const size = view.getUint16(at);
      at += 2;
      if (at + size > 12 + headerLength || Object.hasOwn(headers, name)) throw invalid();
      headers[name] = utf8.decode(bytes.subarray(at, at + size));
      at += size;
    }
    if (headers[":message-type"] !== "event" || headers[":event-type"] !== "chunk") throw invalid();
    const payload: unknown = JSON.parse(utf8.decode(bytes.subarray(12 + headerLength, length - 4)));
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw invalid();
    const fields = Object.keys(payload),
      encoded = (payload as { bytes?: unknown }).bytes;
    if (
      fields.length !== 1 ||
      typeof encoded !== "string" ||
      !encoded ||
      encoded.length % 4 ||
      !/^[A-Za-z0-9+/]*={0,2}$/.test(encoded)
    )
      throw invalid();
    const data = Buffer.from(encoded, "base64");
    if (data.length > maxEventBytes || data.includes(10) || data.includes(13)) throw invalid();
    const inner: unknown = JSON.parse(utf8.decode(data));
    const type =
      inner && typeof inner === "object" && !Array.isArray(inner)
        ? (inner as { type?: unknown }).type
        : undefined;
    const event = typeof type === "string" && /^[\x21-\x7e]{1,200}$/.test(type) ? type : "";
    const prefix = event ? `event: ${event}\n` : "";
    return Buffer.concat([Buffer.from(`${prefix}data: `), data, Buffer.from("\n\n")]);
  }
  return {
    feed(chunk: Uint8Array): Uint8Array {
      if (buffer.length + chunk.length > maxMessage * 2) throw invalid();
      const merged = new Uint8Array(buffer.length + chunk.length);
      merged.set(buffer);
      merged.set(chunk, buffer.length);
      const output: Uint8Array[] = [];
      let offset = 0;
      try {
        while (merged.length - offset >= 12) {
          const bytes = merged.subarray(offset),
            view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength),
            length = view.getUint32(0),
            headerLength = view.getUint32(4);
          if (
            length < 16 ||
            length > maxMessage ||
            headerLength > length - 16 ||
            crc32(bytes.subarray(0, 8)) !== view.getUint32(8)
          )
            throw invalid();
          if (bytes.length < length) break;
          if (crc32(bytes.subarray(0, length - 4)) !== view.getUint32(length - 4)) throw invalid();
          output.push(message(bytes, view, length, headerLength));
          offset += length;
        }
      } catch (error) {
        buffer = new Uint8Array(0);
        if (error instanceof PluginHostError) throw error;
        throw invalid();
      }
      buffer = merged.slice(offset);
      return Buffer.concat(output);
    },
    finish() {
      if (buffer.length) throw new PluginHostError("SSE_TRUNCATED");
    },
  };
}
