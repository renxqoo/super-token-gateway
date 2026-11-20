import { createChatStreamReader, LatencyHistogram, validChat } from "./benchmark-lib.ts";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { createIdentityWriter } from "./capacity-evidence.ts";
import { Agent as HttpAgent, request as httpRequest } from "node:http";
import { Agent as HttpsAgent, request as httpsRequest } from "node:https";

const options = Bun.argv.slice(2);
const recordIds = options.includes("--record-request-ids");
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
const url = option("--url", client.GATEWAY_URL ?? "");
if (!url) throw new Error("--url or .local/client.env is required");
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
const fillSeconds = Number(option("--fill-seconds", "0"));
const warmupConcurrency = Number(
  option("--warmup-concurrency", String(Math.min(256, concurrency))),
);
if (
  !Number.isSafeInteger(warmupConcurrency) ||
  warmupConcurrency < 1 ||
  warmupConcurrency > concurrency
)
  throw new Error("Invalid warmup concurrency");
const hotShare = Number(option("--hot-share", "0"));
if (!Number.isFinite(hotShare) || hotShare < 0 || hotShare > 1)
  throw new Error("Invalid hot wallet share");
const requestTimeoutMs = Number(option("--request-timeout-ms", "30000"));
if (!Number.isSafeInteger(requestTimeoutMs) || requestTimeoutMs < 1000 || requestTimeoutMs > 600000)
  throw new Error("Invalid request timeout");
const walletCalls = new Uint32Array(tokens.length);
if (!Number.isSafeInteger(rate) || rate < 0 || rate > 100000)
  throw new Error("Invalid offered RPS");
if (
  !Number.isSafeInteger(duration) ||
  duration < 1 ||
  duration > 3600 ||
  !Number.isSafeInteger(concurrency) ||
  concurrency < 1 ||
  concurrency > 10000
)
  throw new Error("Invalid benchmark bounds");
const output = option("--output", `reports/gateway-${Date.now()}.json`);
const model = option("--model", client.GATEWAY_MODEL ?? "");
if (!model) throw new Error("--model or .local/client.env is required");
const stream = options.includes("--stream");
if (
  !Number.isSafeInteger(fillSeconds) ||
  fillSeconds < 0 ||
  fillSeconds > 122 ||
  (fillSeconds > 0 && (!stream || !rate || !recordIds))
)
  throw new Error("A stream fill phase requires bounded open-loop arrivals and identity evidence");
const chatRequest = {
  model,
  messages: [{ role: "user", content: "benchmark input" }],
  max_completion_tokens: 32,
  stream,
};
const fixedPayload = JSON.stringify(chatRequest);
const upstreamChallenge = options.includes("--upstream-challenge");
const challengePrefix = upstreamChallenge ? crypto.randomUUID() : "";
let challengeSequence = 0;
const expectedText = option("--expected-text", "") || undefined;
const repetitions = options.includes("--expected-text-repetitions")
  ? Number(option("--expected-text-repetitions", ""))
  : undefined;
if (upstreamChallenge && (!stream || repetitions === undefined))
  throw new Error("An upstream challenge requires deterministic stream text");
const challengeBytes = upstreamChallenge ? 1 + challengePrefix.length + 8 : 0;
if (
  repetitions !== undefined &&
  (!stream ||
    !expectedText ||
    !Number.isSafeInteger(repetitions) ||
    repetitions < 1 ||
    repetitions * Buffer.byteLength(expectedText) + challengeBytes > 1048576)
)
  throw new Error("Invalid repeated stream text for the 1 MiB response budget");
const streamText =
  repetitions === undefined ? expectedText : { exact: expectedText!.repeat(repetitions) };
await mkdir(dirname(output), { recursive: true });
const identityPath = `${output}.identities.tsv`;
const identityWriter = recordIds ? await createIdentityWriter(identityPath) : undefined;
const dispatchPath = `${output}.dispatch.tsv`;
const dispatchWriter = stream && recordIds ? await createIdentityWriter(dispatchPath) : undefined;
const firstFrameLatency = new LatencyHistogram();
const firstContentLatency = new LatencyHistogram();
const contentGaps = new LatencyHistogram();
let sseHeaders = 0;
let sseDone = 0;
let sseErrorFrames = 0;
const poolOptions = {
  keepAlive: true,
  maxSockets: concurrency,
  maxTotalSockets: concurrency,
  maxFreeSockets: concurrency,
};
const httpAgent = new HttpAgent(poolOptions);
const httpsAgent = new HttpsAgent(poolOptions);
const call = (
  target: string,
  apiKey: string,
): Promise<{
  status: number;
  body: string;
  valid: boolean;
  requestId?: string;
  firstByteMs?: number;
}> =>
  new Promise((resolve, reject) => {
    const challenge = upstreamChallenge
      ? challengePrefix + (++challengeSequence).toString(16).padStart(8, "0")
      : undefined;
    const payload = challenge ? JSON.stringify({ ...chatRequest, user: challenge }) : fixedPayload;
    const expectedStream =
      challenge && typeof streamText === "object"
        ? { exact: streamText.exact + " " + challenge }
        : streamText;
    const began = performance.now();
    const expiresAt = began + requestTimeoutMs;
    function expire() {
      const error = Object.assign(new Error("Benchmark request deadline"), {
        code: "REQUEST_DEADLINE",
      });
      reject(error);
      request.destroy(error);
    }
    function expiredStream(observedAt: number) {
      if (!stream || observedAt < expiresAt) return false;
      expire();
      return true;
    }
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
        let observedAt = performance.now();
        response.once("error", reject);
        if (expiredStream(observedAt)) return;
        const isSse =
          stream &&
          response.statusCode === 200 &&
          response.headers["content-type"]?.split(";", 1)[0]?.trim().toLowerCase() ===
            "text/event-stream";
        if (isSse && measuring) sseHeaders++;
        let lastContentAt = began;
        const reader = isSse
          ? createChatStreamReader(expectedStream, (event) => {
              if (!measuring) return;
              if (event === "firstFrame") firstFrameLatency.add(observedAt - began);
              else if (event === "firstContent" || event === "nextContent") {
                if (event === "firstContent") firstContentLatency.add(observedAt - began);
                else contentGaps.add(observedAt - lastContentAt);
                lastContentAt = observedAt;
              } else if (event === "done") sseDone++;
              else sseErrorFrames++;
            })
          : undefined;
        const chunks: Buffer[] = [];
        let size = 0;
        let firstByteMs: number | undefined;
        response.on("data", (chunk: Buffer) => {
          observedAt = performance.now();
          if (expiredStream(observedAt)) return;
          firstByteMs ??= observedAt - began;
          size += chunk.byteLength;
          if (size > 1048576)
            request.destroy(
              Object.assign(new Error("Benchmark response budget exceeded"), {
                code: "RESPONSE_BUDGET",
              }),
            );
          else if (reader) reader.feed(chunk);
          else chunks.push(chunk);
        });
        response.once("end", () => {
          observedAt = performance.now();
          if (expiredStream(observedAt)) return;
          const body = reader ? "" : Buffer.concat(chunks).toString("utf8");
          resolve({
            status: response.statusCode ?? 0,
            body,
            valid: reader
              ? reader.finish()
              : !stream && response.statusCode === 200 && validChat(body, false, expectedText),
            ...(typeof response.headers["x-request-id"] === "string"
              ? { requestId: response.headers["x-request-id"] }
              : {}),
            ...(firstByteMs === undefined ? {} : { firstByteMs }),
          });
        });
      },
    );
    const deadline = setTimeout(
      expire,
      stream ? Math.max(0, Math.ceil(expiresAt - performance.now())) : requestTimeoutMs,
    );
    request.once("error", reject);
    request.once("close", () => clearTimeout(deadline));
    request.end(payload);
  });
const latencies = new LatencyHistogram();
const windows = new Map<
  number,
  { completed: number; succeeded: number; invalid: number; latency: LatencyHistogram }
>();
const latencyWindows = new Map<number, LatencyHistogram>();
const statuses = new Map<number, number>();
const firstByteLatency = new LatencyHistogram();
const schedulingGaps = new LatencyHistogram();
let peakDriverInflight = 0;
let inflightShed = 0;
let lastDispatchOverrunMs = 0;
const transportErrors = new Map<string, number>();
const gatewayErrors = new Map<string, number>();

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
const warmupErrors = new Map<string, number>();
let offered = 0;
let shed = 0;
const invoke = async (arrivalWindow?: number) => {
  const started = performance.now();
  const issuedAt = dispatchWriter ? Date.now() : undefined;
  const issuedWindow = arrivalWindow ?? Math.floor((started - measuredStart) / 60000);
  let status = 0;
  let valid = false;
  let requestId: string | undefined;
  try {
    const index = sequence++;
    const target = urls[index % urls.length]!;
    const wallet =
      tokens.length > 1 && hotShare > 0
        ? (Math.imul(index + 1, 2654435761) >>> 0) / 4294967296 < hotShare
          ? 0
          : 1 + (index % (tokens.length - 1))
        : index % tokens.length;
    const apiKey = tokens[wallet]!;
    walletCalls[wallet] = (walletCalls[wallet] ?? 0) + 1;
    const response = await call(target, apiKey);
    status = response.status;
    requestId = response.requestId;
    const body = response.body;
    valid = response.valid;
    if (measuring && valid && response.firstByteMs !== undefined)
      firstByteLatency.add(response.firstByteMs);
    if (status >= 400) {
      const errors = measuring ? gatewayErrors : warmupErrors;
      let code = "other";
      try {
        const value: unknown = JSON.parse(body);
        if (value && typeof value === "object" && "error" in value) {
          const error = value.error;
          if (error && typeof error === "object" && "code" in error) {
            if (
              typeof error.code === "string" &&
              /^[a-z][a-z0-9_]{0,63}$/.test(error.code) &&
              (errors.has(error.code) || errors.size < 32)
            )
              code = error.code;
          }
        }
      } catch {}
      errors.set(code, (errors.get(code) ?? 0) + 1);
    }
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
      const completedAt = Date.now();
      const latencyMs = performance.now() - started;
      completed++;
      if (valid) succeeded++;
      else invalid++;
      statuses.set(status, (statuses.get(status) ?? 0) + 1);
      const window = Math.floor((performance.now() - measuredStart) / 60000);
      let sample = windows.get(window);
      if (!sample) {
        sample = { completed: 0, succeeded: 0, invalid: 0, latency: new LatencyHistogram() };
        windows.set(window, sample);
      }
      sample.completed++;
      if (valid) sample.succeeded++;
      else sample.invalid++;
      latencies.add(latencyMs);
      sample.latency.add(latencyMs);
      let issuedLatency = latencyWindows.get(issuedWindow);
      if (!issuedLatency) {
        issuedLatency = new LatencyHistogram();
        latencyWindows.set(issuedWindow, issuedLatency);
      }
      issuedLatency.add(latencyMs);
      if (identityWriter && valid) {
        const ids = [requestId ?? ""];
        await Promise.all([
          identityWriter.write(ids, completedAt),
          issuedAt === undefined ? undefined : dispatchWriter?.write(ids, issuedAt),
        ]);
      }
    }
  }
};
await Promise.all(
  Array.from({ length: warmupConcurrency }, async () => {
    while (performance.now() < deadline) await invoke();
  }),
);
if (warmupFailures)
  throw new Error(
    `Warmup produced ${warmupFailures} invalid responses out of ${warmupRequests}: ${JSON.stringify({ gateway: Object.fromEntries(warmupErrors), transport: Object.fromEntries(transportErrors) })}`,
  );
measuring = true;
const arrivalsStartedAt = Date.now();
const measurementStartedAt = new Date(arrivalsStartedAt + fillSeconds * 1000).toISOString();
const started = performance.now();
const cpuStart = process.cpuUsage();
measuredStart = started + fillSeconds * 1000;
deadline = measuredStart + duration * 1000;
const sampler = setInterval(() => {
  peakRSS = Math.max(peakRSS, process.memoryUsage().rss);
}, 100);
if (rate > 0) {
  const pending = new Set<Promise<void>>();
  const planned = (duration + fillSeconds) * rate;
  let previousTick = started;
  while (offered < planned) {
    const now = performance.now();
    schedulingGaps.add(now - previousTick);
    previousTick = now;
    const due = Math.min(planned, Math.floor(((now - started) * rate) / 1000)) - offered;
    offered += due;
    const admitted = Math.min(due, concurrency - pending.size);
    inflightShed += due - admitted;
    for (let i = 0; i < admitted; i++) {
      const task = invoke(Math.floor((offered - due + i - fillSeconds * rate) / (rate * 60)));
      pending.add(task);
      void task.finally(() => pending.delete(task));
    }
    peakDriverInflight = Math.max(peakDriverInflight, pending.size);
    if (offered < planned) await Bun.sleep(Math.min(2, Math.max(0, deadline - performance.now())));
    else lastDispatchOverrunMs = Math.max(0, now - deadline);
  }
  shed = inflightShed;
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
const cpu = process.cpuUsage(cpuStart);
await Promise.all([identityWriter?.close(), dispatchWriter?.close()]);
const quantile = (value: number) => latencies.quantile(value);
const report = {
  label: option("--label", "gateway"),
  timestamp: new Date().toISOString(),
  started_at: startedAt,
  warmup_requests: warmupRequests,
  warmup_concurrency: warmupConcurrency,
  model,
  runtime: Bun.version,
  platform: `${process.platform}/${process.arch}`,
  workload: stream ? "chat-sse" : "chat-json",
  transport: "Bun node:http(s) with bounded persistent agents",
  transport_errors: Object.fromEntries(transportErrors),
  gateway_errors: Object.fromEntries(gatewayErrors),
  duration_seconds: elapsed,
  concurrency,
  measurement_started_at: measurementStartedAt,
  ...(fillSeconds ? { arrivals_started_at: new Date(arrivalsStartedAt).toISOString() } : {}),
  hot_wallet_share: hotShare,
  wallet_calls: Array.from(walletCalls),
  load_model: rate > 0 ? "open-loop" : "closed-loop",
  offered_rps: rate || undefined,
  offered,
  shed,
  inflight_shed: inflightShed,
  last_dispatch_overrun_ms: rate > 0 ? lastDispatchOverrunMs : undefined,
  load_generator_peak_inflight: rate > 0 ? peakDriverInflight : undefined,
  scheduler_gap_ms:
    rate > 0 ? { p99: schedulingGaps.quantile(0.99), max: schedulingGaps.quantile(1) } : undefined,
  load_generator_cpu_seconds: (cpu.user + cpu.system) / 1e6,
  ...(stream
    ? {
        sse: {
          response_headers: sseHeaders,
          ...(upstreamChallenge ? { upstream_challenge: true } : {}),
          first_frame: firstFrameLatency.count,
          first_content: firstContentLatency.count,
          done: sseDone,
          error_frames: sseErrorFrames,
          first_frame_p99_ms: firstFrameLatency.quantile(0.99),
          first_content_p99_ms: firstContentLatency.quantile(0.99),
          content_gap_samples: contentGaps.count,
          content_gap_p99_ms: contentGaps.count ? contentGaps.quantile(0.99) : null,
        },
      }
    : {}),
  first_body_byte_ms: {
    p50: firstByteLatency.quantile(0.5),
    p95: firstByteLatency.quantile(0.95),
    p99: firstByteLatency.quantile(0.99),
  },
  ...(recordIds ? { identity_log: identityPath } : {}),
  ...(dispatchWriter ? { dispatch_log: dispatchPath } : {}),
  completed,
  succeeded,
  invalid,
  successful_rps: Math.round((succeeded / elapsed) * 100) / 100,
  success_ratio: succeeded / Math.max(1, completed + shed),
  latency_ms: {
    p50: quantile(0.5),
    p95: quantile(0.95),
    p99: quantile(0.99),
    p999: quantile(0.999),
  },
  statuses: Object.fromEntries(statuses),
  minute_windows: Object.fromEntries(
    [...windows].map(([index, sample]) => [
      index,
      {
        completed: sample.completed,
        succeeded: sample.succeeded,
        invalid: sample.invalid,
        latency_ms: {
          p50: sample.latency.quantile(0.5),
          p95: sample.latency.quantile(0.95),
          p99: sample.latency.quantile(0.99),
          p999: sample.latency.quantile(0.999),
        },
      },
    ]),
  ),
  latency_windows: Object.fromEntries(
    [...latencyWindows].map(([index, latency]) => [
      index,
      {
        samples: latency.count,
        p50: latency.quantile(0.5),
        p95: latency.quantile(0.95),
        p99: latency.quantile(0.99),
        p999: latency.quantile(0.999),
      },
    ]),
  ),
  histogram_samples: latencies.count,
  load_generator_peak_rss_bytes: peakRSS,
};
await mkdir(new URL("../reports/", import.meta.url), { recursive: true });
await Bun.write(output, `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(report, null, 2));
if (report.success_ratio < 0.999) process.exitCode = 1;
