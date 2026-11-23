import type { SQL } from "bun";
import { request, type Agent } from "node:http";
import { SSEParser } from "@gateway/protocol";

const contentFrames = 40;
const stages = [
  { concurrency: 500, durationMs: 45_000 },
  { concurrency: 1000, durationMs: 70_000 },
  { concurrency: 2000, durationMs: 90_000 },
  { concurrency: 4000, durationMs: 120_000 },
];

type BurstOptions = {
  agent: Agent;
  freshOnly: boolean;
  ports: [number, number];
  keys: string[];
  db: SQL;
  redis: Bun.RedisClient;
  quotaPattern: string;
  mockPort: number;
  mockToken: string;
  readGatewayRss: () => number | Promise<number>;
};

type StreamResult = {
  port: number;
  connection: "fresh" | "reused" | "unassigned";
  requestId: string | undefined;
  error: string | undefined;
  firstContentMs: number | undefined;
  elapsedMs: number;
};

function runStream(
  agent: Agent,
  port: number,
  key: string,
  payload: Buffer,
  durationMs: number,
): Promise<StreamResult> {
  return new Promise((resolve) => {
    const started = performance.now();
    const parser = new SSEParser();
    let connection: StreamResult["connection"] = "unassigned";
    let requestId: string | undefined;
    let error: string | undefined;
    let firstContentMs: number | undefined;
    let content = 0;
    let usage = 0;
    let finish = 0;
    let done = 0;
    const report = (reason?: string) =>
      resolve({
        port,
        connection,
        requestId,
        error: reason ?? error,
        firstContentMs,
        elapsedMs: performance.now() - started,
      });
    const req = request(
      {
        hostname: "127.0.0.1",
        port,
        path: "/v1/chat/completions",
        method: "POST",
        agent,
        headers: {
          authorization: `Bearer ${key}`,
          "content-type": "application/json",
          "content-length": payload.byteLength,
        },
        signal: AbortSignal.timeout(durationMs + 30_000),
      },
      (res) => {
        const id = res.headers["x-request-id"];
        if (typeof id === "string") requestId = id;
        const status = res.statusCode;
        if (status !== 200) error = `http_${status}`;
        else if (!res.headers["content-type"]?.startsWith("text/event-stream"))
          error = "invalid_content_type";
        res.on("data", (chunk: Buffer) => {
          if (error) return;
          try {
            for (const frame of parser.feed(chunk)) {
              if (frame.event === "error") {
                error = "stream_error";
                return;
              }
              if (!frame.data) continue;
              if (done) {
                error = "event_after_done";
                return;
              }
              if (frame.data === "[DONE]") {
                done++;
                continue;
              }
              const object = JSON.parse(frame.data);
              if (object.error) {
                error = "stream_error";
                return;
              }
              if (object.usage) usage++;
              for (const choice of object.choices ?? []) {
                if (choice.finish_reason) finish++;
                if (typeof choice.delta?.content === "string" && choice.delta.content.length) {
                  firstContentMs ??= performance.now() - started;
                  content++;
                }
              }
            }
          } catch {
            error = "invalid_sse";
          }
        });
        res.on("end", () => {
          if (error) return report();
          if (!requestId || !/^[0-9a-f-]{36}$/.test(requestId)) return report("missing_request_id");
          try {
            if (parser.feed(new Uint8Array(0), true).length > 0 || parser.incomplete) {
              return report("incomplete_sse_frame");
            }
          } catch {
            return report("invalid_sse_trailer");
          }
          if (performance.now() - started < durationMs) return report("stream_ended_early");
          if (content !== contentFrames || usage !== 1 || finish !== 1 || done !== 1) {
            return report(
              `unexpected_frames content=${content} usage=${usage} finish=${finish} done=${done}`,
            );
          }
          return report();
        });
        res.on("aborted", () => report("client_response_aborted"));
        res.on("error", (cause) => report(`response:${cause.message}`));
      },
    );
    req.on("socket", () => {
      connection = req.reusedSocket ? "reused" : "fresh";
    });
    req.on("error", (cause) => report(`request:${cause.message}`));
    req.end(payload);
  });
}

async function activeStreams(port: number): Promise<number> {
  const res = await fetch(`http://127.0.0.1:${port}/metrics`, {
    signal: AbortSignal.timeout(3000),
  });
  if (!res.ok) throw new Error(`Gateway metrics returned ${res.status}`);
  const streams = /^gateway_active_streams (\d+)$/m.exec(await res.text())?.[1];
  if (streams === undefined) throw new Error("Gateway active streams metric missing");
  return Number(streams);
}

async function mockStats(options: BurstOptions) {
  const res = await fetch(`http://127.0.0.1:${options.mockPort}/__control/stats`, {
    headers: { authorization: `Bearer ${options.mockToken}` },
    signal: AbortSignal.timeout(3000),
  });
  if (!res.ok) throw new Error(`Mock stats returned ${res.status}`);
  return (await res.json()) as {
    active: number;
    cancellations: number;
    attempts: Record<string, number>;
  };
}

async function sample(options: BurstOptions) {
  const [one, two, mock] = await Promise.all([
    activeStreams(options.ports[0]),
    activeStreams(options.ports[1]),
    mockStats(options),
  ]);
  if (!Number.isInteger(mock.active) || !Number.isInteger(mock.cancellations)) {
    throw new Error("Mock stream metrics missing");
  }
  return {
    node1: one,
    node2: two,
    mock: mock.active,
    cancellations: mock.cancellations,
    attempts: mock.attempts["bprod-long-up"] ?? 0,
    rss: await options.readGatewayRss(),
  };
}

async function quotaMembers(redis: Bun.RedisClient, quotaPattern: string): Promise<number> {
  const keys = (await redis.send("KEYS", [quotaPattern])) as string[];
  const counts = await Promise.all(keys.map((key) => redis.send("ZCARD", [key])));
  return counts.reduce<number>((sum, count) => sum + Number(count), 0);
}

async function configure(options: BurstOptions, durationMs: number): Promise<void> {
  const res = await fetch(`http://127.0.0.1:${options.mockPort}/__control/scenarios`, {
    method: "POST",
    headers: { authorization: `Bearer ${options.mockToken}`, "content-type": "application/json" },
    body: JSON.stringify({
      model: "bprod-long-up",
      scenario: { repeat: contentFrames, streamDurationMs: durationMs },
    }),
    signal: AbortSignal.timeout(3000),
  });
  if (!res.ok || !(await res.json()).configured) throw new Error("Mock long stream setup failed");
}

function p99(values: number[]): number {
  if (!values.length) return 0;
  values.sort((a, b) => a - b);
  return Number(values[Math.floor((values.length - 1) * 0.99)]?.toFixed(2));
}

async function runStage(options: BurstOptions, concurrency: number, durationMs: number) {
  await configure(options, durationMs);
  const initial = await sample(options);
  const payload = Buffer.from(
    JSON.stringify({
      model: "bprod-long",
      stream: true,
      stream_options: { include_usage: true },
      messages: [{ role: "user", content: "long-sse load fixture" }],
      max_tokens: 32,
    }),
  );
  const started = performance.now();
  let peak = 0;
  let mockPeak = 0;
  let rssPeak = initial.rss;
  let firstFull: number | undefined;
  let lastFull: number | undefined;
  let pending: Promise<void> | undefined;
  let nextProgress = 15_000;
  const samplingErrors: string[] = [];
  const timer = setInterval(() => {
    if (pending) return;
    pending = sample(options)
      .then((observation) => {
        const elapsed = performance.now() - started;
        const active = observation.node1 + observation.node2;
        peak = Math.max(peak, active);
        mockPeak = Math.max(mockPeak, observation.mock);
        rssPeak = Math.max(rssPeak, observation.rss);
        if (active === concurrency && observation.mock === concurrency) {
          firstFull ??= elapsed;
          lastFull = elapsed;
        }
        if (elapsed >= nextProgress) {
          console.log(
            JSON.stringify({
              stage_progress: concurrency,
              elapsed_ms: Math.round(elapsed),
              ...observation,
            }),
          );
          nextProgress += 15_000;
        }
      })
      .catch((error: unknown) => {
        if (samplingErrors.length < 5) samplingErrors.push(String(error));
      })
      .finally(() => {
        pending = undefined;
      });
  }, 1000);
  console.log(JSON.stringify({ stage_start: concurrency, duration_ms: durationMs, ramp_ms: 0 }));
  let outcomes: StreamResult[];
  try {
    outcomes = await Promise.all(
      Array.from({ length: concurrency }, (_, index) =>
        runStream(
          options.agent,
          options.ports[index % 2]!,
          options.keys[Math.floor(index / 2) % options.keys.length]!,
          payload,
          durationMs,
        ),
      ),
    );
  } finally {
    clearInterval(timer);
    await pending;
  }

  const successful = outcomes.filter((outcome) => !outcome.error);
  const failed = outcomes.filter((outcome) => outcome.error);
  const ids = successful.map((result) => result.requestId!);
  const [billing] = await options.db`
    SELECT count(*)::int AS total, count(DISTINCT request_id)::int AS distinct,
      (count(*) FILTER (WHERE outcome = 'SETTLED'))::int AS settled,
      (count(*) FILTER (WHERE customer_atoms <> 185 OR funded_atoms <> 185 OR supplier_cost_atoms <> 37))::int AS amount_mismatches,
      coalesce(sum(customer_atoms), 0)::text AS customer_atoms,
      coalesce(sum(supplier_cost_atoms), 0)::text AS supplier_atoms
    FROM public.billing_charges WHERE request_id = ANY(${ids}::uuid[])
  `;
  let final = await sample(options);
  let remainingPermits = await quotaMembers(options.redis, options.quotaPattern);
  for (
    let attempt = 0;
    attempt < 20 && (final.node1 || final.node2 || final.mock || remainingPermits);
    attempt++
  ) {
    await Bun.sleep(100);
    final = await sample(options);
    remainingPermits = await quotaMembers(options.redis, options.quotaPattern);
  }
  const fullMs = firstFull === undefined || lastFull === undefined ? 0 : lastFull - firstFull;
  const cancelled = final.cancellations - initial.cancellations;
  const summary = {
    phase: `sse_${concurrency}c_${durationMs / 1000}s`,
    concurrency,
    duration_ms: durationMs,
    ramp_ms: 0,
    completed: successful.length,
    errors: failed.length,
    fresh_connections_completed: successful.filter((result) => result.connection === "fresh")
      .length,
    reused_connections_completed: successful.filter((result) => result.connection === "reused")
      .length,
    connection_resets: failed.filter((result) =>
      result.error?.startsWith("request:connect ECONNRESET"),
    ).length,
    error_counts: Object.fromEntries(
      Map.groupBy(failed, (entry) => entry.error!)
        .entries()
        .map(([error, entries]) => [error, entries.length]),
    ),
    sample_errors: failed.slice(0, 4),
    sampling_errors: samplingErrors,
    client_ttft_p99_ms: p99(successful.map((result) => result.firstContentMs!)),
    client_total_p99_ms: p99(successful.map((result) => result.elapsedMs)),
    peak_active_streams: peak,
    peak_mock_active: mockPeak,
    full_concurrency_seconds: Number((fullMs / 1000).toFixed(2)),
    combined_gw_rss_mb_peak: Number(Math.max(rssPeak, final.rss).toFixed(1)),
    content_frames: successful.length * contentFrames,
    mock_attempts: final.attempts,
    mock_cancelled: cancelled,
    final_gateway_active: final.node1 + final.node2,
    final_mock_active: final.mock,
    final_redis_quota_members: remainingPermits,
    billing_charges: billing,
  };
  console.log(JSON.stringify({ stage_result: summary }));
  const complete =
    summary.completed === concurrency &&
    (!options.freshOnly || summary.fresh_connections_completed === concurrency) &&
    peak === concurrency &&
    mockPeak === concurrency &&
    fullMs >= durationMs * 0.9 &&
    summary.final_gateway_active === 0 &&
    final.mock === 0 &&
    final.attempts === concurrency &&
    cancelled === 0 &&
    remainingPermits === 0 &&
    billing?.total === concurrency &&
    billing?.distinct === concurrency &&
    billing?.settled === concurrency &&
    billing?.amount_mismatches === 0;
  return { ...summary, passed: complete };
}

export async function runSseBurst(options: BurstOptions) {
  const results = [];
  const selectedStages = options.freshOnly ? [stages.at(-1)!] : stages;
  for (const { concurrency, durationMs } of selectedStages) {
    const result = await runStage(options, concurrency, durationMs);
    results.push(result);
    if (!result.passed) break;
    await Bun.sleep(500);
  }
  return results;
}
