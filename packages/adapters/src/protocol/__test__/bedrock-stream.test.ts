import { expect, test } from "bun:test";
import { bedrockEventStream } from "../bedrock-stream.ts";
import events from "./bedrock-events.json";

const stop = Buffer.from(events.stop.base64, "base64");
const usage = Buffer.from(events.usage.base64, "base64");
const delta = Buffer.from(events.delta.base64, "base64");

function source(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}

function corruptStop(): Uint8Array {
  const corrupt = Uint8Array.from(stop);
  corrupt[corrupt.length - 1] = corrupt[corrupt.length - 1]! ^ 1;
  return corrupt;
}

test("Bedrock emits the exact SSE event at every binary read boundary", async () => {
  for (let split = 0; split <= stop.length; split++) {
    const body = bedrockEventStream(source([stop.subarray(0, split), stop.subarray(split)]), 1024);
    expect(await new Response(body).text()).toBe(events.stop.sse);
  }
});

test.each([
  ["prelude", 8],
  ["message", stop.length - 1],
] as const)("Bedrock rejects a corrupt %s checksum", async (_name, at) => {
  const corrupt = Uint8Array.from(stop);
  corrupt[at] = corrupt[at]! ^ 1;
  await expect(
    new Response(bedrockEventStream(source([corrupt]), 1024)).text(),
  ).rejects.toMatchObject({
    status: 502,
    code: "invalid_eventstream",
  });
});

test("Bedrock rejects truncated EOF instead of completing a partial frame", async () => {
  const body = bedrockEventStream(source([stop.subarray(0, -1)]), 1024);
  await expect(new Response(body).text()).rejects.toMatchObject({
    status: 502,
    code: "invalid_eventstream",
  });
});

test("Bedrock rejects a frame exceeding the configured byte budget", async () => {
  const body = bedrockEventStream(source([stop]), 32);
  await expect(new Response(body).text()).rejects.toMatchObject({
    status: 502,
    code: "invalid_eventstream",
  });
});

test("Bedrock retains preceding usage before a later corrupt frame fails the stream", async () => {
  const reader = bedrockEventStream(
    source([Buffer.concat([usage, corruptStop()])]),
    1024,
  ).getReader();
  try {
    expect(new TextDecoder().decode((await reader.read()).value)).toBe(events.usage.sse);
    expect(new TextDecoder().decode((await reader.read()).value)).toBe(
      'event: error\ndata: {"type":"error","error":{"type":"provider_stream_error","message":"Provider event stream failed"}}\n\n',
    );
    await expect(reader.read()).rejects.toMatchObject({ code: "invalid_eventstream" });
  } finally {
    reader.releaseLock();
  }
});

test("Bedrock accepts many valid frames in one read larger than the frame budget", async () => {
  const body = bedrockEventStream(source([Buffer.concat(Array(1000).fill(delta))]), 1024);
  expect(await new Response(body).text()).toBe(events.delta.sse.repeat(1000));
});

test("Bedrock emits one ordered error and no later deltas after corruption in a large read", async () => {
  const reader = bedrockEventStream(
    source([Buffer.concat([usage, corruptStop(), ...Array(1000).fill(delta)])]),
    1024,
  ).getReader();
  const output: Uint8Array[] = [];
  try {
    let failure: unknown;
    try {
      while (true) {
        const result = await reader.read();
        if (result.done) break;
        output.push(result.value);
      }
    } catch (error) {
      failure = error;
    }
    const text = Buffer.concat(output).toString();
    expect(text.startsWith(events.usage.sse)).toBe(true);
    expect(text.match(/event: error/g)).toHaveLength(1);
    expect(text).not.toContain("content_block_delta");
    expect(failure).toMatchObject({ code: "invalid_eventstream" });
  } finally {
    reader.releaseLock();
  }
});

test("Bedrock releases every memory reservation after successful EOF", async () => {
  let reserved = 0,
    peak = 0;
  const body = bedrockEventStream(source([usage, stop]), 1024, (bytes) => {
    reserved += bytes;
    peak = Math.max(peak, reserved);
  });
  expect(await new Response(body).text()).toBe(events.usage.sse + events.stop.sse);
  expect(peak).toBeGreaterThan(0);
  expect(reserved).toBe(0);
});

test("Bedrock consumer cancellation propagates upstream and releases buffered memory", async () => {
  let reserved = 0,
    reason: unknown;
  const upstream = new ReadableStream<Uint8Array>(
    {
      start(controller) {
        controller.enqueue(Buffer.concat(Array(1000).fill(delta)));
      },
      cancel(value) {
        reason = value;
      },
    },
    { highWaterMark: 0 },
  );
  const reader = bedrockEventStream(upstream, 1024, (bytes) => {
    reserved += bytes;
  }).getReader();
  try {
    expect(new TextDecoder().decode((await reader.read()).value)).toBe(events.delta.sse);
    expect(reserved).toBeGreaterThan(0);
  } finally {
    await reader.cancel("consumer stopped");
    reader.releaseLock();
  }
  expect(reason).toBe("consumer stopped");
  expect(reserved).toBe(0);
});
