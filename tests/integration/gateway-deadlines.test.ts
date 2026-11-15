import { expect, test } from "bun:test";
import { connect } from "node:net";
import type { SQL } from "bun";
import {
  chatFrames,
  chatReplies,
  eventually,
  requestOutcome,
  settledCharge,
  withGatewayStack,
  type UpstreamCall,
} from "../support/gateway-process.ts";

const timeout = 90000;
const encoder = new TextEncoder();

const closedCalls = new WeakSet<UpstreamCall>();

function openStream(type: string, prefix: string, call: UpstreamCall) {
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode(prefix));
      },
      cancel() {
        closedCalls.add(call);
      },
    }),
    { headers: { "content-type": type } },
  );
}

function endless(call: UpstreamCall) {
  const frame = encoder.encode(
    `data: ${JSON.stringify({ id: "chain", model: "synthetic", choices: [{ index: 0, delta: { content: "x".repeat(4096) } }] })}\n\n`,
  );
  return new Response(
    new ReadableStream<Uint8Array>({
      pull(controller) {
        controller.enqueue(frame);
      },
      cancel() {
        closedCalls.add(call);
      },
    }),
    { headers: { "content-type": "text/event-stream" } },
  );
}

async function attempts(db: SQL, requestId: string, count: number) {
  await eventually(
    async () =>
      (
        await db`SELECT count(*)::integer n FROM attempts WHERE request_id=${requestId} AND evidence_id IS NOT NULL`
      )[0].n === count,
    "attempt evidence",
  );
  return db`SELECT ordinal,error_code FROM attempts WHERE request_id=${requestId} ORDER BY ordinal`;
}

async function upstreamClosed(check: () => boolean) {
  await eventually(async () => check(), "upstream connection closed", 5000);
}

test(
  "a streaming attempt whose upstream sends no response headers within the header deadline is abandoned as unknown and the request switches to the other channel",
  () =>
    withGatewayStack(
      {
        reply: async (call) => {
          if (call.index === 0) await Bun.sleep(4000);
          return chatReplies.sse();
        },
        channels: 2,
        gateway: { upstream_header_timeout_ms: 500 },
      },
      async ({ db, received, send }) => {
        const started = performance.now();
        const response = await send({ stream: true });
        expect(response.status).toBe(200);
        const text = await response.text();
        expect(performance.now() - started).toBeLessThan(3000);
        expect(text).toContain("[DONE]");
        expect(text).not.toContain("gateway_stream_error");
        expect(received).toHaveLength(2);
        await upstreamClosed(() => received[0]!.signal.aborted);
        const requestId = response.headers.get("x-request-id")!;
        expect(await settledCharge(db, requestId)).toMatchObject({
          amount: "100",
          state: "SUCCEEDED",
        });
        expect(await attempts(db, requestId, 2)).toEqual([
          { ordinal: 1, error_code: "upstream_header_timeout" },
          { ordinal: 2, error_code: null },
        ]);
      },
    ),
  timeout,
);

test(
  "a non-streaming attempt is not cut by the header deadline because its headers arrive with the finished generation",
  () =>
    withGatewayStack(
      {
        reply: async () => {
          await Bun.sleep(1500);
          return chatReplies.json();
        },
        channels: 2,
        gateway: { upstream_header_timeout_ms: 500 },
      },
      async ({ received, send }) => {
        const response = await send();
        expect(response.status).toBe(200);
        expect((await response.json()).choices[0].message.content).toBe("ok");
        expect(received).toHaveLength(1);
      },
    ),
  timeout,
);

test(
  "a streaming upstream that goes silent before completing its first event is abandoned at the idle deadline and the request switches to the other channel",
  () =>
    withGatewayStack(
      {
        reply: (call) =>
          call.index === 0
            ? openStream("text/event-stream", 'data: {"id":"chain",', call)
            : chatReplies.sse(),
        channels: 2,
        gateway: { upstream_idle_timeout_ms: 500 },
      },
      async ({ db, received, send }) => {
        const started = performance.now();
        const response = await send({ stream: true });
        expect(response.status).toBe(200);
        const text = await response.text();
        expect(performance.now() - started).toBeLessThan(3000);
        expect(text).toContain('"content":"o"');
        expect(text).toContain("[DONE]");
        expect(text).not.toContain("gateway_stream_error");
        expect(received).toHaveLength(2);
        await upstreamClosed(() => closedCalls.has(received[0]!));
        const requestId = response.headers.get("x-request-id")!;
        expect((await settledCharge(db, requestId)).state).toBe("SUCCEEDED");
        expect(await attempts(db, requestId, 2)).toEqual([
          { ordinal: 1, error_code: "upstream_idle_timeout" },
          { ordinal: 2, error_code: null },
        ]);
      },
    ),
  timeout,
);

test(
  "a streaming upstream that goes silent after visible output ends the stream with a gateway error at the idle deadline instead of holding it to the total deadline, and is never regenerated",
  () =>
    withGatewayStack(
      {
        reply: (call) =>
          openStream("text/event-stream", `data: ${JSON.stringify(chatFrames()[0])}\n\n`, call),
        channels: 2,
        gateway: { upstream_idle_timeout_ms: 500 },
      },
      async ({ db, received, send }) => {
        const started = performance.now();
        const response = await send({ stream: true });
        expect(response.status).toBe(200);
        const text = await response.text();
        expect(performance.now() - started).toBeLessThan(3000);
        expect(text).toContain('"content":"o"');
        expect(text).toContain("gateway_stream_error");
        expect(text).not.toContain("[DONE]");
        expect(received).toHaveLength(1);
        await upstreamClosed(() => closedCalls.has(received[0]!));
        const requestId = response.headers.get("x-request-id")!;
        expect(await requestOutcome(db, requestId)).toMatchObject({ state: "FAILED" });
        expect(await attempts(db, requestId, 1)).toEqual([
          { ordinal: 1, error_code: "upstream_idle_timeout" },
        ]);
      },
    ),
  timeout,
);

test(
  "a JSON upstream that sends headers and part of its body then goes silent is abandoned at the idle deadline and the request switches to the other channel",
  () =>
    withGatewayStack(
      {
        reply: (call) =>
          call.index === 0
            ? openStream("application/json", '{"id":"chain",', call)
            : chatReplies.json(),
        channels: 2,
        gateway: { upstream_idle_timeout_ms: 500 },
      },
      async ({ db, received, send }) => {
        const started = performance.now();
        const response = await send();
        expect(response.status).toBe(200);
        expect((await response.json()).choices[0].message.content).toBe("ok");
        expect(performance.now() - started).toBeLessThan(3000);
        expect(received).toHaveLength(2);
        await upstreamClosed(() => closedCalls.has(received[0]!));
        const requestId = response.headers.get("x-request-id")!;
        expect((await settledCharge(db, requestId)).state).toBe("SUCCEEDED");
        expect(await attempts(db, requestId, 2)).toEqual([
          { ordinal: 1, error_code: "upstream_idle_timeout" },
          { ordinal: 2, error_code: null },
        ]);
      },
    ),
  timeout,
);

test(
  "a streaming client that stops reading is cut at the downstream stall deadline, closing the upstream and the request instead of holding them until it disconnects",
  () =>
    withGatewayStack(
      {
        reply: endless,
        gateway: { downstream_stall_timeout_ms: 500 },
      },
      async ({ db, gateway, received, setup }) => {
        const body = JSON.stringify({
          model: setup.model,
          messages: [{ role: "user", content: "合成" }],
          max_completion_tokens: 30,
          stream: true,
        });
        const origin = new URL(gateway.origin);
        const socket = connect(Number(origin.port), origin.hostname);
        socket.on("error", () => undefined);
        let head = "";
        const headed = new Promise<void>((resolve) =>
          socket.on("data", (chunk: Buffer) => {
            if (head.includes("\r\n\r\n")) return;
            head += chunk.toString("latin1");
            if (head.includes("\r\n\r\n")) {
              socket.pause();
              resolve();
            }
          }),
        );
        try {
          socket.write(
            `POST /v1/chat/completions HTTP/1.1\r\nhost: ${origin.host}\r\nauthorization: Bearer ${setup.key.key}\r\ncontent-type: application/json\r\ncontent-length: ${Buffer.byteLength(body)}\r\n\r\n${body}`,
          );
          await headed;
          const requestId = /x-request-id: ([0-9a-f-]+)/i.exec(head)![1]!;
          expect(head).toStartWith("HTTP/1.1 200");
          expect(await requestOutcome(db, requestId)).toMatchObject({ state: "FAILED" });
          expect(await attempts(db, requestId, 1)).toEqual([
            { ordinal: 1, error_code: "downstream_stalled" },
          ]);
          await upstreamClosed(() => closedCalls.has(received[0]!));
        } finally {
          socket.destroy();
        }
      },
    ),
  timeout,
);

test(
  "a request that reaches its total deadline while the upstream is still working answers 504 request_deadline instead of reporting an unreachable upstream",
  () =>
    withGatewayStack(
      {
        reply: async (call) => {
          if (call.index > 0) await Bun.sleep(9000);
          return chatReplies.json();
        },
        gateway: { total_timeout_ms: 5000 },
      },
      async ({ db, received, send }) => {
        const warm = await send();
        expect(warm.status).toBe(200);
        await warm.arrayBuffer();
        const response = await send();
        expect(response.status).toBe(504);
        expect((await response.json()).error.code).toBe("request_deadline");
        expect(received).toHaveLength(2);
        await upstreamClosed(() => received[1]!.signal.aborted);
        expect(await requestOutcome(db, response.headers.get("x-request-id")!)).toMatchObject({
          state: "FAILED",
          charged: "0",
        });
      },
    ),
  timeout,
);

test(
  "a 200 JSON answer that is malformed or unusable before anything reaches the client switches to the other channel and charges once",
  async () => {
    for (const invalid of [
      () => new Response('{"id":"chain",', { headers: { "content-type": "application/json" } }),
      () =>
        Response.json({
          id: "chain",
          model: "synthetic",
          choices: [],
          usage: { prompt_tokens: 30, completion_tokens: 15 },
        }),
    ])
      await withGatewayStack(
        {
          reply: (call) => (call.index === 0 ? invalid() : chatReplies.json()),
          channels: 2,
        },
        async ({ db, received, send }) => {
          const response = await send();
          expect(response.status).toBe(200);
          expect((await response.json()).choices[0].message.content).toBe("ok");
          expect(received).toHaveLength(2);
          const requestId = response.headers.get("x-request-id")!;
          expect(await settledCharge(db, requestId)).toMatchObject({
            amount: "100",
            state: "SUCCEEDED",
          });
          expect(await attempts(db, requestId, 2)).toEqual([
            { ordinal: 1, error_code: "upstream_invalid" },
            { ordinal: 2, error_code: null },
          ]);
        },
      );
  },
  timeout * 2,
);

test(
  "a 200 stream that is invalid or ends before its first visible event switches to the other channel instead of answering with a stream error",
  async () => {
    for (const invalid of [
      () =>
        new Response("data: {not json\n\n", { headers: { "content-type": "text/event-stream" } }),
      () => chatReplies.sse([], true),
    ])
      await withGatewayStack(
        {
          reply: (call) => (call.index === 0 ? invalid() : chatReplies.sse()),
          channels: 2,
        },
        async ({ db, received, send }) => {
          const response = await send({ stream: true });
          expect(response.status).toBe(200);
          const text = await response.text();
          expect(text).toContain("[DONE]");
          expect(text).not.toContain("gateway_stream_error");
          expect(received).toHaveLength(2);
          const requestId = response.headers.get("x-request-id")!;
          expect((await settledCharge(db, requestId)).state).toBe("SUCCEEDED");
          expect(await attempts(db, requestId, 2)).toEqual([
            { ordinal: 1, error_code: "stream_interrupted" },
            { ordinal: 2, error_code: null },
          ]);
        },
      );
  },
  timeout * 2,
);
