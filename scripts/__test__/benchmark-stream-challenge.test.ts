import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";
import { createChatStreamReader } from "../benchmark-lib.ts";

test("paced upstream delivers text before termination and releases a cancelled stream", async () => {
  const mock = createMockApp("fixture");
  mock.scenarios.set("fixture", { streamDurationMs: 200, repeat: 3, output: "字" });
  const open = () =>
    mock.app.request("/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "fixture", stream: true }),
    });
  const started = performance.now();
  const response = await open();
  const reader = response.body!.getReader();
  const received = createChatStreamReader({ exact: "字字字" });
  received.feed((await reader.read()).value!);
  const firstContent = (await reader.read()).value!;
  expect(new TextDecoder().decode(firstContent)).toContain('"content":"字"');
  expect(mock.stats.active).toBe(1);
  received.feed(firstContent);
  for (let chunk = await reader.read(); !chunk.done; chunk = await reader.read()) {
    received.feed(chunk.value);
  }
  expect(performance.now() - started).toBeGreaterThanOrEqual(200);
  expect(received.finish()).toBe(true);
  expect(mock.stats).toMatchObject({ active: 0, cancellations: 0 });

  const cancelled = (await open()).body!.getReader();
  await cancelled.read();
  await cancelled.read();
  const pending = cancelled.read();
  await cancelled.cancel();
  expect(await pending).toEqual({ done: true, value: undefined });
  expect(mock.stats).toMatchObject({ active: 0, cancellations: 1 });
});

test("a deterministic SSE reply replayed for a different request fails even when provider call counts match", async () => {
  const directory = await mkdtemp(join(tmpdir(), "gateway-bench-challenge-"));
  const token = crypto.randomUUID();
  const mock = createMockApp(token).app;
  const control = await mock.request("/__control/scenarios", {
    method: "POST",
    headers: { authorization: "Bearer " + token, "content-type": "application/json" },
    body: JSON.stringify({
      model: "fixture",
      scenario: { output: "字", repeat: 2, echoRequestChallenge: true },
    }),
  });
  expect(control.status).toBe(200);
  let calls = 0;
  const challenges: unknown[] = [];
  let warmupBody = "";
  const server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    async fetch(req) {
      const i = ++calls;
      challenges.push((await req.clone().json()).user);
      const response = await mock.fetch(req);
      const body = await response.text();
      if (i === 1) {
        warmupBody = body;
        await Bun.sleep(1050);
      }
      return new Response(i === 3 ? warmupBody : body, {
        status: response.status,
        headers: { "content-type": "text/event-stream", "x-request-id": crypto.randomUUID() },
      });
    },
  });
  try {
    const reportPath = join(directory, "http.json");
    const child = Bun.spawn(
      [
        process.execPath,
        "--no-env-file",
        "scripts/bench.ts",
        "--stream",
        "--upstream-challenge",
        "--url",
        server.url.href,
        "--model",
        "fixture",
        "--expected-text",
        "字",
        "--expected-text-repetitions",
        "2",
        "--seconds",
        "1",
        "--rate",
        "2",
        "--concurrency",
        "1",
        "--warmup-concurrency",
        "1",
        "--output",
        reportPath,
      ],
      {
        env: { PATH: process.env.PATH, BENCH_API_KEYS: "fixture" },
        stdout: "ignore",
        stderr: "pipe",
      },
    );
    const stderr = await new Response(child.stderr).text();
    const exit = await child.exited;
    expect(await Bun.file(reportPath).exists(), stderr).toBe(true);
    const report = await Bun.file(reportPath).json();
    expect(calls).toBe(3);
    expect(report).toMatchObject({
      warmup_requests: 1,
      offered: 2,
      succeeded: 1,
      invalid: 1,
      statuses: { "200": 2 },
    });
    expect(exit).toBe(1);
    expect(challenges.every((v) => typeof v === "string" && v.length > 0)).toBe(true);
    expect(new Set(challenges).size).toBe(3);
  } finally {
    await server.stop(true);
    await rm(directory, { recursive: true, force: true });
  }
});
