import { expect, test } from "bun:test";
import { createAwsEventStreamDecoder } from "../aws-eventstream.ts";
import { bedrockChunk, eventStreamMessage } from "../../../../tests/support/aws-eventstream.ts";

const decode = (bytes: Uint8Array) => new TextDecoder().decode(bytes);

test("chunk messages split at every byte boundary decode to SSE events named by the inner type", () => {
  const first = { type: "message_start", message: { id: "m", content: "你好🌏" } },
    second = { type: "message_stop" };
  const wire = Buffer.concat([bedrockChunk(first), bedrockChunk(second)]);
  const expected = `event: message_start\ndata: ${JSON.stringify(first)}\n\nevent: message_stop\ndata: ${JSON.stringify(second)}\n\n`;
  for (let split = 0; split <= wire.length; split++) {
    const decoder = createAwsEventStreamDecoder();
    const out =
      decode(decoder.feed(wire.subarray(0, split))) + decode(decoder.feed(wire.subarray(split)));
    expect(out).toBe(expected);
    expect(() => decoder.finish()).not.toThrow();
  }
});

test("CRC corruption, exception messages, malformed envelopes and oversized events are invalid", () => {
  const good = bedrockChunk({ type: "ping" });
  const flipped = Buffer.from(good);
  flipped[flipped.length - 6]! ^= 1;
  const prelude = Buffer.from(good);
  prelude[9]! ^= 1;
  const cases = [
    flipped,
    prelude,
    eventStreamMessage(
      { ":message-type": "exception", ":exception-type": "throttlingException" },
      '{"message":"slow down"}',
    ),
    eventStreamMessage({ ":message-type": "event", ":event-type": "chunk" }, '{"bytes":"%%%%"}'),
    eventStreamMessage(
      { ":message-type": "event", ":event-type": "chunk" },
      '{"bytes":"e30=","extra":1}',
    ),
    eventStreamMessage(
      { ":message-type": "event", ":event-type": "chunk" },
      JSON.stringify({ bytes: Buffer.from('{\n"type":"ping"}').toString("base64") }),
    ),
  ];
  for (const wire of cases)
    expect(() => createAwsEventStreamDecoder().feed(wire)).toThrow("EVENTSTREAM_INVALID");
  expect(() =>
    createAwsEventStreamDecoder(8).feed(bedrockChunk({ type: "content_block_delta" })),
  ).toThrow("EVENTSTREAM_INVALID");
});

test("bytes left after the last complete message at EOF are a truncation", () => {
  const wire = bedrockChunk({ type: "ping" });
  const decoder = createAwsEventStreamDecoder();
  expect(decode(decoder.feed(wire.subarray(0, wire.length - 1)))).toBe("");
  expect(() => decoder.finish()).toThrow("SSE_TRUNCATED");
});
