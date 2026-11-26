import {
  buildPostRequest,
  postJsonKeepAlive,
  warmHttpConnections,
  createFastHttp1BenchClient,
} from "./bench-http.ts";
import { Agent } from "node:http";

export function createProductionBenchmarkLoad({
  targets,
  walLsnBytes,
  readCombinedGatewayRss,
  ports,
  redisUrl,
  keys,
  fastClient,
  tenantAffinity,
  agent,
  gatewayNodes,
  percentile,
  warmLimitSockets,
  limitConcurrency,
  limitSeconds,
}: {
  targets: { host: string; port: number }[];
  walLsnBytes: () => Promise<bigint>;
  readCombinedGatewayRss: () => Promise<number>;
  ports: number[];
  redisUrl: string | undefined;
  keys: string[];
  fastClient: ReturnType<typeof createFastHttp1BenchClient> | undefined;
  tenantAffinity: boolean;
  agent: Agent;
  gatewayNodes: number;
  percentile: (sorted: number[], p: number) => number;
  warmLimitSockets: boolean;
  limitConcurrency: number[];
  limitSeconds: number;
}) {
  async function runTimedLoad(
    label: string,
    model: string,
    keyPool: string[],
    concurrency: number,
    seconds: number,
    stream: boolean,
  ) {
    let completed = 0;
    let errors = 0;
    const sampleErrors: string[] = [];
    const payload = Buffer.from(
      JSON.stringify({
        model,
        stream,
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 32,
      }),
    );
    const rawByTargetKey = fastClient
      ? targets.map((t) => keyPool.map((k) => buildPostRequest(t, k, payload)))
      : null;
    const latencies: number[] = [];
    const walBefore = await walLsnBytes();
    const rssBefore = await readCombinedGatewayRss();
    const started = performance.now();
    const stopAt = started + seconds * 1000;
    let cursor = 0;

    async function worker(workerIdx: number) {
      const rampDelay = Math.floor(workerIdx / 64) * 10;
      if (rampDelay > 0) await Bun.sleep(rampDelay);
      const targetIdx = workerIdx % ports.length;
      const affineStride = tenantAffinity && keyPool.length >= ports.length ? ports.length : 1;
      const affineCount = Math.floor((keyPool.length - 1 - targetIdx) / affineStride) + 1;
      while (performance.now() < stopAt) {
        const idx = cursor++;
        const keyIdx =
          affineStride > 1 ? targetIdx + (idx % affineCount) * affineStride : idx % keyPool.length;
        const t0 = performance.now();
        const result = fastClient
          ? await fastClient.sendRaw(targetIdx, rawByTargetKey![targetIdx]![keyIdx]!, stream)
          : await postJsonKeepAlive(agent, ports[targetIdx]!, keyPool[keyIdx]!, payload, stream);
        if (result.ok) completed++;
        else {
          errors++;
          if (sampleErrors.length < 5)
            sampleErrors.push(`${result.status} ${result.errText ?? ""}`);
        }
        latencies.push(performance.now() - t0);
      }
    }

    await Promise.all(Array.from({ length: concurrency }, (_, i) => worker(i)));
    const elapsedSec = (performance.now() - started) / 1000;
    const walAfter = await walLsnBytes();
    const rssAfter = await readCombinedGatewayRss();
    latencies.sort((a, b) => a - b);
    return {
      phase: label,
      gateway_nodes: gatewayNodes,
      redis_enabled: Boolean(redisUrl),
      concurrency,
      completed,
      errors,
      ...(sampleErrors.length ? { sample_errors: sampleErrors } : {}),
      elapsed_sec: Number(elapsedSec.toFixed(3)),
      rps: Number((completed / elapsedSec).toFixed(1)),
      p50_ms: percentile(latencies, 0.5),
      p95_ms: percentile(latencies, 0.95),
      p99_ms: percentile(latencies, 0.99),
      wal_bytes_per_req: completed > 0 ? Number((walAfter - walBefore) / BigInt(completed)) : 0,
      combined_gw_rss_mb_before: rssBefore,
      combined_gw_rss_mb_after: rssAfter,
    };
  }

  async function runFixedCountLoad(
    label: string,
    model: string,
    keyPool: string[],
    concurrency: number,
    totalRequests: number,
    stream: boolean,
  ) {
    let completed = 0;
    let errors = 0;
    const sampleErrors: string[] = [];
    const payload = Buffer.from(
      JSON.stringify({
        model,
        stream,
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 32,
      }),
    );
    const rawByTargetKey = fastClient
      ? targets.map((t) => keyPool.map((k) => buildPostRequest(t, k, payload)))
      : null;
    const latencies: number[] = [];
    const walBefore = await walLsnBytes();
    const rssBefore = await readCombinedGatewayRss();
    const started = performance.now();
    let cursor = 0;

    async function worker(workerIdx: number) {
      const targetIdx = workerIdx % ports.length;
      while (true) {
        const idx = cursor++;
        if (idx >= totalRequests) break;
        const keyIdx = idx % keyPool.length;
        const t0 = performance.now();
        const result = fastClient
          ? await fastClient.sendRaw(targetIdx, rawByTargetKey![targetIdx]![keyIdx]!, stream)
          : await postJsonKeepAlive(agent, ports[targetIdx]!, keyPool[keyIdx]!, payload, stream);
        if (result.ok) completed++;
        else {
          errors++;
          if (sampleErrors.length < 5)
            sampleErrors.push(`${result.status} ${result.errText ?? ""}`);
        }
        latencies.push(performance.now() - t0);
      }
    }

    await Promise.all(Array.from({ length: concurrency }, (_, i) => worker(i)));
    const elapsedSec = (performance.now() - started) / 1000;
    const walAfter = await walLsnBytes();
    const rssAfter = await readCombinedGatewayRss();
    latencies.sort((a, b) => a - b);
    return {
      phase: label,
      gateway_nodes: gatewayNodes,
      redis_enabled: Boolean(redisUrl),
      concurrency,
      completed,
      errors,
      ...(sampleErrors.length ? { sample_errors: sampleErrors } : {}),
      elapsed_sec: Number(elapsedSec.toFixed(3)),
      rps: Number((completed / elapsedSec).toFixed(1)),
      p50_ms: percentile(latencies, 0.5),
      p95_ms: percentile(latencies, 0.95),
      p99_ms: percentile(latencies, 0.99),
      wal_bytes_per_req: completed > 0 ? Number((walAfter - walBefore) / BigInt(completed)) : 0,
      combined_gw_rss_mb_before: rssBefore,
      combined_gw_rss_mb_after: rssAfter,
    };
  }

  async function runLimitPhases() {
    if (warmLimitSockets) {
      const target = Math.ceil(Math.max(...limitConcurrency) / ports.length);
      if (fastClient) await fastClient.warm(target);
      else await Promise.all(ports.map((port) => warmHttpConnections(agent, port, target)));
    }
    const results: Awaited<ReturnType<typeof runTimedLoad>>[] = [];
    for (const concurrency of limitConcurrency) {
      results.push(
        await runTimedLoad(
          `${gatewayNodes}node_limit_${limitSeconds}s_json_${concurrency}c`,
          "bprod-fast",
          keys,
          concurrency,
          limitSeconds,
          false,
        ),
      );
    }
    return results;
  }

  return { runFixedCountLoad, runLimitPhases, runTimedLoad };
}
