import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LatencyHistogram, validChat, passesCapacityTarget } from "../../scripts/benchmark-lib.ts";
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
