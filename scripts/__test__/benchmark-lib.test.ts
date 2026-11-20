import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  createChatStreamReader,
  LatencyHistogram,
  validChat,
  passesCapacityTarget,
} from "../benchmark-lib.ts";
test("benchmark rejects empty/error/malformed Chat successes and invalid SSE completion", () => {
  expect(validChat('{"choices":[]}', false)).toBe(false);
  expect(validChat("not json", false)).toBe(false);
  expect(validChat('{"error":{},"choices":[{}]}', false)).toBe(false);
  const chunk = 'data: {"choices":[{"delta":{"content":"hello"},"finish_reason":"stop"}]}\n\n';
  expect(validChat(chunk + "data: [DONE]\n\n", true, "hello")).toBe(true);
  expect(validChat(chunk + 'data: {"error":{"message":"fail"}}\n\ndata: [DONE]\n\n', true)).toBe(
    false,
  );
  expect(validChat(chunk + "data: [DONE]\n\ndata: [DONE]\n\n", true)).toBe(false);
  expect(validChat("data: [DONE]\n\n", true)).toBe(false);
  expect(validChat(chunk, true)).toBe(false);
});
test("bounded histogram quantiles retain millisecond accuracy across many observations", () => {
  const h = new LatencyHistogram();
  for (let i = 0; i < 100000; i++) h.add((i % 100) + 1);
  expect(h.count).toBe(100000);
  expect(h.quantile(0.5)).toBeGreaterThanOrEqual(50);
  expect(h.quantile(0.5)).toBeLessThan(51);
  expect(h.quantile(0.99)).toBeGreaterThanOrEqual(99);
  expect(h.quantile(0.99)).toBeLessThan(100);
});

test("fixed SSE output rejects a missing repetition, extra text or a tool-only replacement", () => {
  function read(deltas: object[]) {
    const observations: string[] = [];
    const reader = createChatStreamReader({ exact: "你好你好" }, (event) =>
      observations.push(event),
    );
    const send = (delta: object, finish: string | null) =>
      reader.feed(
        Buffer.from(`data: ${JSON.stringify({ choices: [{ delta, finish_reason: finish }] })}\n\n`),
      );
    for (const delta of deltas) send(delta, null);
    send({}, "stop");
    reader.feed(Buffer.from("data: [DONE]\n\n"));
    return { valid: reader.finish(), observations };
  }
  expect(read([{ tool_calls: [{ index: 0 }] }]).valid).toBe(false);
  expect(read([{ content: "你" }, { content: "好你" }, { content: "好" }])).toEqual({
    valid: true,
    observations: ["firstFrame", "firstContent", "nextContent", "nextContent", "done"],
  });
  expect(read([{ content: "你好" }]).valid).toBe(false);
  expect(read([{ content: "前你好你好" }]).valid).toBe(false);
  expect(read([{ content: "你好你好!" }]).valid).toBe(false);
});

test("a fixed-text stream rejects content after stop, duplicate stops and unexpected finish reasons", () => {
  const delta = (content: string, reason: string | null) =>
    JSON.stringify({ choices: [{ index: 0, delta: { content }, finish_reason: reason }] });
  const text = delta("hello", null);
  const stop = delta("", "stop");
  function read(...frames: string[]) {
    const reader = createChatStreamReader({ exact: "hello" });
    for (const frame of [...frames, "[DONE]"]) reader.feed(Buffer.from(`data: ${frame}\n\n`));
    return reader.finish();
  }
  expect(read(stop, text)).toBe(false);
  expect(read(text, stop, stop)).toBe(false);
  expect(read(delta("hello", "oops"))).toBe(false);
  expect(read(text, stop, JSON.stringify({ choices: [], usage: { total_tokens: 1 } }))).toBe(true);
});

test("rounded 6000 RPS cannot pass when the unrounded completed rate is below target", () => {
  expect(passesCapacityTarget(true, { succeeded: 5999996, duration_seconds: 1000 }, 6000)).toBe(
    false,
  );
  expect(passesCapacityTarget(true, { succeeded: 6000000, duration_seconds: 1000 }, 6000)).toBe(
    true,
  );
  expect(passesCapacityTarget(false, { succeeded: 7000000, duration_seconds: 1000 }, 6000)).toBe(
    false,
  );
  expect(passesCapacityTarget(true, { succeeded: 0, duration_seconds: 0 }, 6000)).toBe(false);
});

test("the open-loop driver does not drop the planned arrival at the window boundary", async () => {
  const directory = await mkdtemp(join(tmpdir(), "gateway-bench-boundary-"));
  const output = join(directory, "http.json");
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      await request.arrayBuffer();
      await Bun.sleep(5);
      return Response.json(
        {
          choices: [{ message: { role: "assistant", content: "ok" }, finish_reason: "stop" }],
        },
        { headers: { "x-request-id": "fixture-request" } },
      );
    },
  });
  try {
    const child = Bun.spawn(
      [
        process.execPath,
        "--no-env-file",
        "scripts/bench.ts",
        "--url",
        server.url.href,
        "--model",
        "fixture-chat",
        "--seconds",
        "1",
        "--rate",
        "2",
        "--concurrency",
        "1",
        "--record-request-ids",
        "--output",
        output,
      ],
      { env: { ...process.env, BENCH_API_KEYS: "fixture" }, stdout: "ignore", stderr: "pipe" },
    );
    const stderr = await new Response(child.stderr).text();
    expect(await child.exited, stderr).toBe(0);
    const report = await Bun.file(output).json();
    const identities = (await Bun.file(report.identity_log).text()).trim().split("\n");
    expect(identities).toHaveLength(2);
    expect(
      identities.filter(
        (line) => Number(line.split("\t")[1]) < Date.parse(report.measurement_started_at) + 1000,
      ),
    ).toHaveLength(1);
    expect(report).toMatchObject({
      offered: 2,
      shed: 0,
      succeeded: 2,
      identity_log: `${output}.identities.tsv`,
    });
  } finally {
    await server.stop(true);
    await rm(directory, { recursive: true, force: true });
  }
});

test("the SSE driver counts observed frames on incomplete responses and separates DONE from valid completion", async () => {
  const directory = await mkdtemp(join(tmpdir(), "gateway-bench-sse-"));
  const output = join(directory, "http.json");
  const frame = (delta: object, finish: string | null = null) =>
    `data: ${JSON.stringify({ choices: [{ delta, finish_reason: finish }] })}\n\n`;
  const role = frame({ role: "assistant" });
  const content = frame({ content: "好" }, "stop");
  const done = "data: [DONE]\n\n";
  const lateError = 'data: {"error":{"message":"fixture"}}\n\n';
  const delayedDeadline = join(directory, "late-deadline.ts");
  await Bun.write(
    delayedDeadline,
    `
    const schedule = globalThis.setTimeout;
    globalThis.setTimeout = (fn, delay, ...args) =>
      schedule(fn, delay >= 1000 && delay <= 1200 ? 2400 : delay, ...args);
  `,
  );
  let requests = 0;
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      await request.arrayBuffer();
      const sequence = requests++;
      if (sequence === 0) await Bun.sleep(1100);
      const body =
        sequence === 2
          ? role + content
          : sequence === 3
            ? role + done + done + lateError
            : role + content + done;
      const bytes = new TextEncoder().encode(body);
      const contentStart = bytes.indexOf(new TextEncoder().encode("好")[0]!);
      const split = sequence === 4 || contentStart === -1 ? role.length : contentStart + 1;
      return new Response(
        new ReadableStream({
          async start(controller) {
            controller.enqueue(bytes.subarray(0, split));
            await Bun.sleep(sequence === 4 ? 1350 : 10);
            controller.enqueue(bytes.subarray(split));
            controller.close();
          },
        }),
        { headers: { "content-type": "text/event-stream; charset=utf-8" } },
      );
    },
  });
  try {
    const child = Bun.spawn(
      [
        process.execPath,
        "--no-env-file",
        "--preload",
        delayedDeadline,
        "scripts/bench.ts",
        "--stream",
        "--url",
        server.url.href,
        "--model",
        "fixture",
        "--expected-text",
        "好",
        "--seconds",
        "1",
        "--rate",
        "4",
        "--request-timeout-ms",
        "1200",
        "--concurrency",
        "10000",
        "--warmup-concurrency",
        "1",
        "--output",
        output,
      ],
      { env: { ...process.env, BENCH_API_KEYS: "fixture" }, stdout: "ignore", stderr: "pipe" },
    );
    const stderr = await new Response(child.stderr).text();
    expect(await child.exited, stderr).toBe(1);
    expect(await Bun.file(output).exists(), stderr).toBe(true);
    const report = await Bun.file(output).json();
    expect(report).toMatchObject({
      completed: 4,
      succeeded: 1,
      invalid: 3,
      transport_errors: { REQUEST_DEADLINE: 1 },
      sse: { response_headers: 4, first_frame: 4, first_content: 2, done: 2, error_frames: 1 },
    });
    expect(report.sse.first_frame_p99_ms).toBeGreaterThan(0);
    expect(report.sse.first_content_p99_ms).toBeGreaterThan(0);
  } finally {
    await server.stop(true);
    await rm(directory, { recursive: true, force: true });
  }
});
