import { expect, test } from "bun:test";
import {
  StreamTranslator,
  MediaStreamMeter,
  decodeRequest,
  encodeRequest,
  publicUsage,
} from "@gateway/connectors";
import { parseProviderJson, stringifyExact } from "@gateway/connectors";
import type { SSEEvent } from "@gateway/connectors";
import type { UsageEvidence } from "@gateway/contracts";
import { MemoryBudget } from "@gateway/runtime";
import { applyOutputBound } from "../input-policy.ts";
import { prepareStream, relayStream } from "../relay-stream.ts";
import { resolveFileReferences } from "../resources.ts";
import type { Dependencies } from "../context.ts";
const event = (value: unknown, type = "message"): SSEEvent => ({
  data: JSON.stringify(value),
  event: type,
  id: "",
  wire: `data: ${JSON.stringify(value)}\n\n`,
});

test("cumulative usage retains omitted cache and reasoning details before normalization", () => {
  const gemini = new StreamTranslator("gemini", "chat", "gemini", "model");
  gemini.consume(
    event({
      usageMetadata: { promptTokenCount: 10, cachedContentTokenCount: 3, thoughtsTokenCount: 2 },
      candidates: [],
    }),
  );
  gemini.consume(
    event({
      usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 3 },
      candidates: [{ index: 0, finishReason: "STOP" }],
    }),
  );
  expect(gemini.usage).toEqual({
    input_tokens: "7",
    cached_input_tokens: "3",
    reasoning_tokens: "2",
    output_tokens: "5",
  });
  const openai = new StreamTranslator("chat", "chat", "openai", "model");
  openai.consume(
    event({
      usage: { prompt_tokens: 10, prompt_tokens_details: { cached_tokens: 3 } },
      choices: [],
    }),
  );
  openai.consume(event({ usage: { prompt_tokens: 10, completion_tokens: 5 }, choices: [] }));
  expect(openai.usage?.input_tokens).toBe("7");
});
test("failed stream events retain the latest observed numeric evidence", () => {
  const meter = new StreamTranslator("responses", "chat", "openai", "model");
  meter.consume(
    event({ type: "response.created", response: { usage: { input_tokens: 2, output_tokens: 1 } } }),
  );
  expect(() =>
    meter.consume(
      event({
        type: "response.failed",
        response: { usage: { input_tokens: 7, output_tokens: 5 } },
      }),
    ),
  ).toThrow();
  expect(meter.usage?.input_tokens).toBe("7");
  expect(meter.usage?.output_tokens).toBe("5");
  expect(meter.failed).toBe(true);
  const media = new MediaStreamMeter("images");
  expect(() =>
    media.consume(event({ type: "error", usage: { input_tokens: 7, output_tokens: 5 } })),
  ).toThrow();
  expect(media.usage?.output_tokens).toBe("5");
});
test("first error SSE reaches the usage meter rather than being discarded by transport preflight", async () => {
  const value = event({
    type: "response.failed",
    response: { usage: { input_tokens: 7, output_tokens: 5 } },
  });
  const deps = {
    config: { MAX_EVENT_BYTES: 1024, STREAM_IDLE_MS: 100 },
  } as unknown as Dependencies;
  const prepared = await prepareStream(
    new Response(value.wire, { headers: { "content-type": "text/event-stream" } }),
    deps,
    new AbortController(),
  );
  const meter = new StreamTranslator("responses", "responses", "openai", "model");
  expect(() => meter.consume(prepared.events[0]!)).toThrow();
  expect(meter.usage?.input_tokens).toBe("7");
  await prepared.reader.cancel();
  prepared.reader.releaseLock();
});
test("discarded prelude SSE heartbeats do not exhaust the buffered-byte budget", async () => {
  const encoder = new TextEncoder();
  const heartbeat = encoder.encode(`: ${"p".repeat(200)}\r\n\r\n`);
  const content = encoder.encode('data: {"ok":true}\r\n\r\n');
  const budget = new MemoryBudget(512);
  const lease = budget.lease();
  let chunks = 0;
  const source = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (chunks++ < 20) controller.enqueue(heartbeat);
      else {
        controller.enqueue(content);
        controller.close();
      }
    },
  });
  try {
    const prepared = await prepareStream(
      new Response(source, { headers: { "content-type": "text/event-stream" } }),
      { config: { MAX_EVENT_BYTES: 512, STREAM_IDLE_MS: 100 } } as Dependencies,
      new AbortController(),
      lease.update,
    );
    expect(prepared.events.map((frame) => frame.data)).toEqual(['{"ok":true}']);
    expect(budget.bufferedBytes).toBe(19);
    await prepared.reader.cancel();
    prepared.reader.releaseLock();
  } finally {
    lease.close();
  }
});

for (const trailingError of [false, true])
  test(`SSE ${trailingError ? "malformed trailing data cannot certify final usage" : "preserves a CR-delimited terminal frame at EOF"}`, async () => {
    const frame = {
      choices: [{ index: 0, delta: { content: "ok" }, finish_reason: "stop" }],
      usage: { prompt_tokens: 3, completion_tokens: 1 },
    };
    const wire = trailingError
      ? `data: ${JSON.stringify(frame)}\n\ndata: [DONE]\n\ndata: {\n\n`
      : `data: ${JSON.stringify(frame)}\r\rdata: [DONE]\r\r`;
    const evidence: UsageEvidence[] = [];
    const deps = {
      config: { MAX_EVENT_BYTES: 1024, STREAM_IDLE_MS: 100, DOWNSTREAM_STALL_MS: 1000 },
      usage: {
        async publish(value: UsageEvidence) {
          evidence.push(value);
        },
      },
      metrics: { increment() {} },
    } as unknown as Dependencies;
    const controller = new AbortController();
    const prepared = await prepareStream(
      new Response(wire, { headers: { "content-type": "text/event-stream" } }),
      deps,
      controller,
    );
    const stream = await relayStream(
      deps,
      prepared,
      new StreamTranslator("chat", "chat", "openai", "model"),
      "attempt",
      "request",
      controller,
      async () => {},
    );
    const output = await new Response(stream).text();
    expect(output.includes("[DONE]")).toBe(!trailingError);
    expect(output.includes("gateway_stream_error")).toBe(trailingError);
    expect(evidence).toHaveLength(1);
    expect(evidence[0]).toMatchObject({
      finality: trailingError ? "PARTIAL" : "FINAL",
      usage: { input_tokens: "3", output_tokens: "1", requests: "1" },
    });
  });
test("public usage includes cache creation and preserves aggregate integers beyond IEEE754", () => {
  const usage = {
    input_tokens: "7",
    cached_input_tokens: "3",
    cache_write_tokens: "2",
    output_tokens: "5",
  };
  expect(publicUsage(usage, "chat").prompt_tokens).toBe(12);
  expect(publicUsage(usage, "responses").total_tokens).toBe(17);
  expect(publicUsage(usage, "gemini").promptTokenCount).toBe(12);
  expect(
    stringifyExact(
      publicUsage(
        { input_tokens: "9007199254740991", cached_input_tokens: "2", output_tokens: "1" },
        "chat",
      ),
    ),
  ).toContain('"prompt_tokens":9007199254740993');
});
test("Gemini candidateCount participates in the output admission ceiling", () => {
  expect(
    applyOutputBound(
      { generationConfig: { maxOutputTokens: 32, candidateCount: 8 } },
      "gemini",
      64,
    ),
  ).toBe(256);
  expect(() =>
    applyOutputBound({ generationConfig: { candidateCount: 9 } }, "gemini", 64),
  ).toThrow();
});
test("Gemini native multi-candidate completion waits for all candidate identities", () => {
  const meter = new StreamTranslator("gemini", "gemini", "gemini", "model", 1024, 2);
  meter.consume(
    event({
      candidates: [
        { index: 0, content: { role: "model", parts: [{ text: "first" }] }, finishReason: "STOP" },
      ],
    }),
  );
  expect(meter.terminal).toBe(false);
  meter.consume(
    event({
      candidates: [
        { index: 1, content: { role: "model", parts: [{ text: "second" }] }, finishReason: "STOP" },
      ],
    }),
  );
  expect(meter.terminal).toBe(true);
  expect(() => meter.finalize()).not.toThrow();
});
test("Gemini tool calls have stream-stable identities across chunks", () => {
  const meter = new StreamTranslator("gemini", "chat", "gemini", "model");
  const chunks: Uint8Array[] = [];
  for (const [id, name, args] of [
    ["call_A", "first", { a: 1 }],
    ["call_B", "second", { b: 2 }],
  ] as const)
    chunks.push(
      ...meter.consume(
        event({
          candidates: [{ index: 0, content: { parts: [{ functionCall: { id, name, args } }] } }],
        }),
      ).chunks,
    );
  const wire = chunks.map((c) => new TextDecoder().decode(c)).join("");
  expect(wire).toContain('"id":"call_A"');
  expect(wire).toContain('"id":"call_B"');
  expect(wire).toContain('"index":1');
});
test("resource ownership resolver leaves tool business payloads intact", async () => {
  let lookups = 0;
  const deps = {
    store: {
      resource: () => {
        lookups++;
        throw new Error("not a provider reference");
      },
    },
  } as unknown as Dependencies;
  const body = parseProviderJson(
    '{"contents":[{"parts":[{"functionCall":{"name":"lookup","args":{"conversation":"business","file_id":"invoice_42"}}},{"functionResponse":{"name":"lookup","response":{"previous_response_id":"business_1","file_id":"invoice_42"}}}]}]}',
  ) as Record<string, unknown>;
  const result = await resolveFileReferences(deps, "project", body);
  expect(lookups).toBe(0);
  expect(stringifyExact(result.body)).toContain("invoice_42");
});
test("Chat text-block tool results become a Gemini Struct, not a JSON array", () => {
  const request = decodeRequest(
    {
      messages: [
        {
          role: "assistant",
          tool_calls: [
            { id: "call_1", type: "function", function: { name: "lookup", arguments: "{}" } },
          ],
        },
        { role: "tool", tool_call_id: "call_1", content: [{ type: "text", text: "ok" }] },
      ],
    },
    "chat",
  );
  const body = encodeRequest(request, "gemini", "model");
  const parts = (body.contents as { parts: Record<string, unknown>[] }[]).flatMap(
    (row) => row.parts,
  );
  const fn = parts.find((part) => part.functionResponse)?.functionResponse as Record<
    string,
    unknown
  >;
  expect(Array.isArray(fn.response)).toBe(false);
  expect(JSON.stringify(fn.response)).toContain("ok");
});

test("chat success requires every requested choice to finish before the terminal sentinel", () => {
  for (const frames of [
    [],
    [event({ choices: [], usage: { prompt_tokens: 10, completion_tokens: 0 } })],
    [event({ choices: [{ index: 0, delta: { content: "partial" }, finish_reason: null }] })],
  ]) {
    const meter = new StreamTranslator("chat", "chat", "openai", "model");
    for (const frame of frames) meter.consume(frame);
    expect(() => meter.consume({ ...event(null), data: "[DONE]" })).toThrow();
    expect(meter.terminal).toBe(false);
  }
  const multi = new StreamTranslator("chat", "chat", "openai", "model", 1024, 2);
  multi.consume(event({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }] }));
  expect(() => multi.consume({ ...event(null), data: "[DONE]" })).toThrow();
  const valid = new StreamTranslator("chat", "chat", "openai", "model", 1024, 2);
  valid.consume(
    event({
      choices: [
        { index: 0, delta: {}, finish_reason: "stop" },
        { index: 1, delta: {}, finish_reason: "length" },
      ],
    }),
  );
  expect(valid.consume({ ...event(null), data: "[DONE]" }).terminal).toBe(true);
});
