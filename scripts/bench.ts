import { LatencyHistogram, validChat } from "./benchmark-lib.ts";
import { mkdir } from "node:fs/promises";
import { Agent as HttpAgent, request as httpRequest } from "node:http";
import { Agent as HttpsAgent, request as httpsRequest } from "node:https";

const options = Bun.argv.slice(2);
const option = (name: string, fallback: string) => {
  const index = options.indexOf(name);
  return index >= 0 ? (options[index + 1] ?? fallback) : fallback;
};
let client: Record<string, string> = {};
const clientFile = new URL("../.local/client.env", import.meta.url);
if (await Bun.file(clientFile).exists())
  client = Object.fromEntries(
    (await Bun.file(clientFile).text())
      .trim()
      .split("\n")
      .map((line) => {
        const at = line.indexOf("=");
        return [line.slice(0, at), line.slice(at + 1)];
      }),
  );
const url = option("--url", client.GATEWAY_URL ?? "http://127.0.0.1:8088");
const tokens = (
  process.env.BENCH_API_KEYS ??
  process.env.BENCH_API_KEY ??
  client.GATEWAY_API_KEY ??
  ""
)
  .split(",")
  .filter(Boolean);
const token = tokens[0];
const urls = url.split(",");
let sequence = 0;
if (!token) throw new Error("BENCH_API_KEY or .local/client.env is required");
const duration = Number(option("--seconds", "15"));
const concurrency = Number(option("--concurrency", "32"));
const rate = Number(option("--rate", "0"));
if (!Number.isSafeInteger(rate) || rate < 0 || rate > 100000)
  throw new Error("Invalid offered RPS");
if (
  !Number.isSafeInteger(duration) ||
  duration < 1 ||
  duration > 3600 ||
  !Number.isSafeInteger(concurrency) ||
  concurrency < 1 ||
  concurrency > 2000
)
  throw new Error("Invalid benchmark bounds");
const model = option("--model", "demo");
const stream = options.includes("--stream");
const payload = JSON.stringify({
  model,
  messages: [{ role: "user", content: "benchmark input" }],
  max_completion_tokens: 32,
  stream,
});
// A bounded, explicit persistent pool avoids ephemeral-port churn on the load machine.
const poolOptions = {
  keepAlive: true,
  maxSockets: concurrency,
  maxTotalSockets: concurrency,
  maxFreeSockets: concurrency,
};
const httpAgent = new HttpAgent(poolOptions);
const httpsAgent = new HttpsAgent(poolOptions);
const call = (target: string, apiKey: string): Promise<{ status: number; body: string }> =>
  new Promise((resolve, reject) => {
    const secure = new URL(target).protocol === "https:";
    const request = (secure ? httpsRequest : httpRequest)(
      `${target.replace(/\/$/, "")}/v1/chat/completions`,
      {
        method: "POST",
        agent: secure ? httpsAgent : httpAgent,
        headers: {
          authorization: `Bearer ${apiKey}`,
          "content-type": "application/json",
          "content-length": Buffer.byteLength(payload),
        },
      },
      (response) => {
        const chunks: Buffer[] = [];
        let size = 0;
        response.on("data", (chunk: Buffer) => {
          size += chunk.byteLength;
          if (size > 1048576)
            request.destroy(
              Object.assign(new Error("Benchmark response budget exceeded"), {
                code: "RESPONSE_BUDGET",
              }),
            );
          else chunks.push(chunk);
        });
        response.once("error", reject);
        response.once("end", () =>
          resolve({
            status: response.statusCode ?? 0,
            body: Buffer.concat(chunks).toString("utf8"),
          }),
        );
      },
    );
    const deadline = setTimeout(
      () =>
        request.destroy(
          Object.assign(new Error("Benchmark request deadline"), { code: "REQUEST_DEADLINE" }),
        ),
      30000,
    );
    request.once("error", reject);
    request.once("close", () => clearTimeout(deadline));
    request.end(payload);
  });
const latencies = new LatencyHistogram();
const expectedText = option("--expected-text", "") || undefined;
const windows = new Map<number, { completed: number; succeeded: number; invalid: number }>();
const statuses = new Map<number, number>();
const transportErrors = new Map<string, number>();
const collectMetrics = options.includes("--metrics");
if (collectMetrics && !process.env.ADMIN_KEY)
  throw new Error("--metrics requires this Gateway's ADMIN_KEY");
const metrics = async (): Promise<Record<string, number>> => {
  const snapshots = await Promise.all(
    urls.map(async (url) => {
      const response = await fetch(`${url.replace(/\/$/, "")}/admin/metrics`, {
        headers: { authorization: `Bearer ${process.env.ADMIN_KEY}` },
        signal: AbortSignal.timeout(2000),
      });
      if (!response.ok) throw new Error("Unable to collect Gateway metrics");
      return Object.fromEntries(
        (await response.text()).split("\n").flatMap((line) => {
          const match = /^(gateway_[a-z_]+) ([0-9.e+-]+)$/.exec(line);
          return match ? [[match[1] ?? "", Number(match[2])]] : [];
        }),
      );
    }),
  );
  const total: Record<string, number> = {};
  for (const snapshot of snapshots)
    for (const [key, value] of Object.entries(snapshot)) total[key] = (total[key] ?? 0) + value;
  return total;
};
const metricsBefore = collectMetrics ? await metrics() : undefined;
let serverPeakRSS = 0;
let peakBufferedBytes = 0;
let collecting: Promise<void> | undefined;
let nextSample = 0;
const sampleServer = () => {
  if (Date.now() < nextSample) return;
  nextSample = Date.now() + 500;
  if (collectMetrics && !collecting)
    collecting = metrics()
      .then((value) => {
        serverPeakRSS = Math.max(serverPeakRSS, value.gateway_rss_bytes ?? 0);
        peakBufferedBytes = Math.max(peakBufferedBytes, value.gateway_buffered_bytes ?? 0);
      })
      .catch(() => undefined)
      .finally(() => {
        collecting = undefined;
      });
};
const startedAt = new Date().toISOString();
let warmupRequests = 0;
let succeeded = 0;
let invalid = 0;
let completed = 0;
let peakRSS = 0;
let measuring = false;
let measuredStart = 0;
let deadline = performance.now() + 1000;
let warmupFailures = 0;
let offered = 0;
let shed = 0;
const invoke = async () => {
  const started = performance.now();
  let status = 0;
  let valid = false;
  try {
    const index = sequence++;
    const target = urls[index % urls.length]!;
    const apiKey = tokens[index % tokens.length]!;
    const response = await call(target, apiKey);
    status = response.status;
    const body = response.body;
    valid = status === 200 && validChat(body, stream, expectedText);
  } catch (error) {
    const fault = error as { code?: unknown; name?: unknown };
    const raw = String(fault.code ?? fault.name ?? "unclassified_transport");
    const code = /^[A-Za-z0-9_]{1,64}$/.test(raw) ? raw : "unclassified_transport";
    const bucket = transportErrors.has(code) || transportErrors.size < 16 ? code : "other";
    transportErrors.set(bucket, (transportErrors.get(bucket) ?? 0) + 1);
  } finally {
    if (!measuring) {
      warmupRequests++;
      if (!valid) warmupFailures++;
    } else {
      completed++;
      if (valid) succeeded++;
      else invalid++;
      statuses.set(status, (statuses.get(status) ?? 0) + 1);
      const window = Math.floor((performance.now() - measuredStart) / 60000);
      let sample = windows.get(window);
      if (!sample) {
        sample = { completed: 0, succeeded: 0, invalid: 0 };
        windows.set(window, sample);
      }
      sample.completed++;
      if (valid) sample.succeeded++;
      else sample.invalid++;
      latencies.add(performance.now() - started);
    }
  }
};
await Promise.all(
  Array.from({ length: concurrency }, async () => {
    while (performance.now() < deadline) await invoke();
  }),
);
if (warmupFailures) throw new Error(`Warmup produced ${warmupFailures} invalid responses`);
measuring = true;
const measurementStartedAt = new Date().toISOString();
const started = performance.now();
measuredStart = started;
deadline = started + duration * 1000;
const sampler = setInterval(() => {
  peakRSS = Math.max(peakRSS, process.memoryUsage().rss);
  sampleServer();
}, 100);
if (rate > 0) {
  // Fixed offered arrivals do not slow down when the target slows down. Bound in-flight memory explicitly.
  const pending = new Set<Promise<void>>();
  while (performance.now() < deadline) {
    const due = Math.floor(((performance.now() - started) * rate) / 1000) - offered;
    offered += due;
    const admitted = Math.min(due, concurrency - pending.size);
    shed += due - admitted;
    for (let i = 0; i < admitted; i++) {
      const task = invoke();
      pending.add(task);
      void task.finally(() => pending.delete(task));
    }
    await Bun.sleep(2);
  }
  await Promise.all(pending);
} else {
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (performance.now() < deadline) {
        offered++;
        await invoke();
      }
    }),
  );
}
clearInterval(sampler);
httpAgent.destroy();
httpsAgent.destroy();
const elapsed = (performance.now() - started) / 1000;
await collecting;
const metricsAfter = collectMetrics ? await metrics() : undefined;
const quantile = (value: number) => latencies.quantile(value);
const artifacts = collectMetrics
  ? {
      gateway_sha256: new Bun.CryptoHasher("sha256")
        .update(await Bun.file(new URL("../dist/gateway/main.js", import.meta.url)).arrayBuffer())
        .digest("hex"),
      manifest_sha256: new Bun.CryptoHasher("sha256")
        .update(await Bun.file(new URL("../dist/manifest.json", import.meta.url)).arrayBuffer())
        .digest("hex"),
    }
  : undefined;
const report = {
  label: option("--label", "gateway"),
  ...(artifacts ? { artifacts } : {}),
  timestamp: new Date().toISOString(),
  started_at: startedAt,
  warmup_requests: warmupRequests,
  model,
  runtime: Bun.version,
  platform: `${process.platform}/${process.arch}`,
  workload: stream ? "chat-sse" : "chat-json",
  transport: "Bun node:http(s) with bounded persistent agents",
  transport_errors: Object.fromEntries(transportErrors),
  duration_seconds: elapsed,
  concurrency,
  measurement_started_at: measurementStartedAt,
  load_model: rate > 0 ? "open-loop" : "closed-loop",
  offered_rps: rate || undefined,
  offered,
  shed,
  completed,
  succeeded,
  invalid,
  successful_rps: Math.round((succeeded / elapsed) * 100) / 100,
  success_ratio: succeeded / Math.max(1, completed + shed),
  latency_ms: { p50: quantile(0.5), p95: quantile(0.95), p99: quantile(0.99) },
  statuses: Object.fromEntries(statuses),
  minute_windows: Object.fromEntries(windows),
  histogram_samples: latencies.count,
  load_generator_peak_rss_bytes: peakRSS,
  ...(metricsBefore && metricsAfter
    ? {
        server_metrics: {
          sampled_peak_rss_bytes: serverPeakRSS,
          sampled_peak_buffered_bytes: peakBufferedBytes,
          cpu_seconds_including_warmup:
            (metricsAfter.gateway_cpu_seconds_total ?? 0) -
            (metricsBefore.gateway_cpu_seconds_total ?? 0),
          usage_queue_accepted:
            (metricsAfter.gateway_usage_queue_accepted_total ?? 0) -
            (metricsBefore.gateway_usage_queue_accepted_total ?? 0),
          usage_queue_failures:
            (metricsAfter.gateway_usage_queue_failures_total ?? 0) -
            (metricsBefore.gateway_usage_queue_failures_total ?? 0),
        },
      }
    : {}),
};
await mkdir(new URL("../reports/", import.meta.url), { recursive: true });
const output = option(
  "--output",
  `reports/${report.label}-${stream ? "sse" : "json"}-${Date.now()}.json`,
);
await Bun.write(output, `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(report, null, 2));
if (report.success_ratio < 0.999) process.exitCode = 1;
