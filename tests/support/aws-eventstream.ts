export function eventStreamMessage(
  headers: Record<string, string>,
  payload: Uint8Array | string,
): Uint8Array {
  const body = typeof payload === "string" ? Buffer.from(payload) : Buffer.from(payload);
  const encoded = Buffer.concat(
    Object.entries(headers).map(([name, value]) => {
      const key = Buffer.from(name),
        text = Buffer.from(value),
        size = Buffer.alloc(2);
      size.writeUInt16BE(text.length);
      return Buffer.concat([Buffer.from([key.length]), key, Buffer.from([7]), size, text]);
    }),
  );
  const total = 16 + encoded.length + body.length,
    prelude = Buffer.alloc(12);
  prelude.writeUInt32BE(total, 0);
  prelude.writeUInt32BE(encoded.length, 4);
  prelude.writeUInt32BE(Bun.hash.crc32(prelude.subarray(0, 8)), 8);
  const message = Buffer.concat([prelude, encoded, body, Buffer.alloc(4)]);
  message.writeUInt32BE(Bun.hash.crc32(message.subarray(0, total - 4)), total - 4);
  return message;
}

export function bedrockChunk(event: unknown): Uint8Array {
  return eventStreamMessage(
    { ":event-type": "chunk", ":content-type": "application/json", ":message-type": "event" },
    JSON.stringify({ bytes: Buffer.from(JSON.stringify(event)).toString("base64") }),
  );
}
