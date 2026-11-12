import { expect, test } from "bun:test";
import { createSSEFrames } from "../sse-frames.ts";
const encode = (text: string) => new TextEncoder().encode(text);
function fixture(limits: Parameters<typeof createSSEFrames>[1] = {}) {
  let bytes = 0,
    peak = 0;
  const frames = createSSEFrames((n) => {
    bytes += n;
    peak = Math.max(peak, bytes);
    if (bytes < 0) throw new Error("negative budget");
  }, limits);
  return { frames, bytes: () => bytes, peak: () => peak };
}
test("standard SSE framing preserves multiline data and Unicode across every CRLF and UTF8 byte boundary", () => {
  const f = fixture(),
    source = encode(
      '\uFEFF: hello\r\nid: abc\r\nevent: update\r\ndata: {"x":\r\ndata: "你好🌏"}\r\n\r\n',
    );
  for (const byte of source) expect(f.frames.feed(new Uint8Array([byte]))).toBe(1);
  const frame = f.frames.peek()!;
  expect(frame.event).toBe("update");
  expect(frame.id).toBe("abc");
  expect(new TextDecoder().decode(frame.data)).toBe('{"x":\n"你好🌏"}');
  expect(new TextDecoder().decode(frame.wire)).toBe(
    ': hello\nid: abc\nevent: update\ndata: {"x":\ndata: "你好🌏"}\n\n',
  );
  f.frames.consume(frame);
  f.frames.finish();
  expect(f.bytes()).toBe(0);
  f.frames.dispose();
});
test("the legacy normalized event budget accepts exactly one MiB even when CRLF doubles the raw wire", () => {
  const max = 1048576,
    f = fixture();
  const source = encode(":\r\n".repeat((max - 2) / 2) + "\r\n");
  expect(source.length).toBe(1572863);
  for (let i = 0; i < source.length; i += 8192)
    expect(f.frames.feed(source.subarray(i, i + 8192))).toBe(Math.min(8192, source.length - i));
  const frame = f.frames.peek()!;
  expect(frame.rawBytes).toBe(source.length);
  expect(frame.lines.length).toBe(524287);
  f.frames.consume(frame);
  expect(f.bytes()).toBe(0);
  const g = fixture();
  expect(() => g.frames.feed(encode(":" + "x".repeat(max - 2) + "\n\n"))).toThrow(
    "SSE_EVENT_BUDGET",
  );
  g.frames.dispose();
  expect(g.bytes()).toBe(0);
});
test("a maximum-size data event is accepted and the next normalized byte is rejected without a leaked partial frame", () => {
  const max = 1048576,
    f = fixture(),
    source = encode("data: " + "x".repeat(max - 8) + "\n\n");
  expect(source.length).toBe(max);
  expect(f.frames.feed(source)).toBe(max);
  expect(f.frames.peek()?.data.length).toBe(max - 8);
  f.frames.dispose();
  expect(f.bytes()).toBe(0);
  const g = fixture();
  expect(() => g.frames.feed(encode("data: " + "x".repeat(max - 7) + "\n\n"))).toThrow(
    "SSE_EVENT_BUDGET",
  );
  g.frames.dispose();
  expect(g.bytes()).toBe(0);
});
test("ready frame limits return a consumed prefix and resume without retaining the caller input", () => {
  const f = fixture({ maxReadyFrames: 2 }),
    source = encode(":a\n\n:b\n\n:c\n\n");
  expect(f.frames.feed(source)).toBe(8);
  expect(f.frames.snapshot().ready).toBe(2);
  const first = f.frames.peek()!;
  f.frames.consume(first);
  expect(() => f.frames.consume(first)).toThrow("SSE_FRAME");
  expect(f.frames.feed(source.subarray(8))).toBe(4);
  expect(f.frames.snapshot().ready).toBe(2);
  f.frames.dispose();
  expect(f.bytes()).toBe(0);
});
test("truncated UTF8, unfinished events and raw queue overflow are rejected and all reservations are reclaimable", () => {
  for (const source of [new Uint8Array([0xe4]), encode("data: x\n"), encode("data: x")]) {
    const f = fixture();
    f.frames.feed(source);
    expect(() => f.frames.finish()).toThrow();
    f.frames.dispose();
    expect(f.bytes()).toBe(0);
  }
  const f = fixture({ maxReadyBytes: 4 });
  expect(() => f.frames.feed(encode(":abc\r\n\r\n"))).toThrow("SSE_QUEUE_BUDGET");
  f.frames.dispose();
  expect(f.bytes()).toBe(0);
  const g = fixture();
  g.frames.feed(encode("data: x\r\r"));
  g.frames.finish();
  expect(new TextDecoder().decode(g.frames.peek()!.data)).toBe("x");
  g.frames.dispose();
  expect(g.bytes()).toBe(0);
});
