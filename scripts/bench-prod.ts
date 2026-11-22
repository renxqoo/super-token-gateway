import { spawn, spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { Agent } from "node:http";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import type { SQL } from "bun";
import { createDatabase } from "@gateway/ledger";
import { auditConservation } from "../packages/ledger/src/conservation.ts";
import {
  runMixedSoak,
  seedMixedPlugins,
  verifyMixedCharges,
  verifyMixedBalances,
  type MixedChargeCheck,
} from "./bench-mixed.ts";
import { benchmarkConcurrency, postJsonKeepAlive, warmHttpConnections } from "./bench-http.ts";
import { runSseBurst } from "./bench-sse-burst.ts";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const isStress = process.argv.includes("--stress");
const isLimit = process.argv.includes("--limit");
const isMixed = process.argv.includes("--mixed-soak");
const gatewayNodes = isMixed ? Number(process.env.BENCH_GATEWAY_NODES ?? 2) : 2;
const soakSeconds = Number(process.env.BENCH_SOAK_SECONDS ?? 1800);
const soakRps = Number(process.env.BENCH_SOAK_RPS ?? 500);
const soakConcurrency = Number(process.env.BENCH_SOAK_CONCURRENCY ?? 1024);
if (
  isMixed &&
  (![1, 2].includes(gatewayNodes) ||
    ![soakSeconds, soakRps, soakConcurrency].every((n) => Number.isSafeInteger(n) && n > 0))
)
  throw new Error(
    "Mixed soak requires one or two nodes and positive integer duration, rate and concurrency",
  );
const limitConcurrency = benchmarkConcurrency(process.env.BENCH_JSON_CONCURRENCY, [512, 1024]);
const isSseBurstFresh = process.argv.includes("--sse-burst-fresh");
const isSseBurst = isSseBurstFresh || process.argv.includes("--sse-burst");
const warmLimitSockets = isLimit && process.env.BENCH_LIMIT_COLD_CONNECT !== "1";
const withRedis = !process.argv.includes("--no-redis");
if (isSseBurst && !withRedis) throw new Error("--sse-burst requires Redis");
const durationSec = isStress ? 15 : 5;
const hotWalletSec = isStress ? 10 : 3;
const longStreamCount = isStress ? 1000 : 500;
const longStreamMs = isStress ? 5000 : 2500;

async function findFreePort(): Promise<number> {
  return new Promise((resolvePort, rejectPort) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      srv.close(() => resolvePort(port));
    });
    srv.on("error", rejectPort);
  });
}

async function waitForJson(url: string): Promise<Record<string, unknown>> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.ok) return (await res.json()) as Record<string, unknown>;
    } catch {
      void 0;
    }
    await Bun.sleep(100);
  }
  throw new Error(`Timed out waiting for ${url}`);
}

function runSync(cwd: string, args: string[], env: Record<string, string>): Promise<void> {
  return new Promise((done, fail) => {
    const proc = spawn(process.execPath, args, { cwd, env, stdio: "inherit" });
    proc.on("exit", (code) => {
      if (code === 0) done();
      else fail(new Error(`Exited ${code}: ${args.join(" ")}`));
    });
  });
}

function getRssMb(pid: number): number {
  if (process.platform === "linux") {
    const status = readFileSync(`/proc/${pid}/status`, "utf8");
    const kb = Number(/^VmRSS:\s+(\d+) kB$/m.exec(status)?.[1]);
    if (!Number.isFinite(kb)) throw new Error(`RSS unavailable for Gateway PID ${pid}`);
    return Number((kb / 1024).toFixed(1));
  }
  const out = spawnSync("ps", ["-o", "rss=", "-p", String(pid)], { encoding: "utf8" });
  const kb = Number(out.stdout?.trim());
  if (out.status !== 0 || !Number.isFinite(kb) || kb <= 0) {
    throw new Error(`RSS unavailable for Gateway PID ${pid}`);
  }
  return Number((kb / 1024).toFixed(1));
}

async function stopProcesses(procs: ReturnType<typeof spawn>[]): Promise<void> {
  const running = procs.filter(
    (proc) => proc.pid && proc.exitCode === null && proc.signalCode === null,
  );
  const exits = Promise.all(
    running.map((proc) => new Promise<void>((resolve) => proc.once("exit", () => resolve()))),
  );
  for (const proc of running) proc.kill("SIGTERM");
  await Promise.race([exits, sleep(12_000, undefined, { ref: false })]);
  for (const proc of running) {
    if (proc.exitCode === null && proc.signalCode === null) proc.kill("SIGKILL");
  }
  await exits;
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p));
  return Number(sorted[idx]!.toFixed(2));
}

const tempDir = await mkdtemp(join(tmpdir(), "stg-bench-prod-"));
const dbName = `gateway_v4_bprod_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const adminUrl = new URL(baseDbUrl);
adminUrl.pathname = "/postgres";
const targetUrl = new URL(baseDbUrl);
targetUrl.pathname = `/${dbName}`;

const adminDb = createDatabase(adminUrl.toString(), 2);
const procs: ReturnType<typeof spawn>[] = [];
const agentOptions = {
  keepAlive: true,
  maxSockets: Math.max(2048, ...limitConcurrency, isMixed ? soakConcurrency : 0),
  maxFreeSockets: isMixed
    ? Math.ceil(soakConcurrency / gatewayNodes)
    : warmLimitSockets
      ? Math.max(1024, Math.ceil(Math.max(...limitConcurrency) / gatewayNodes))
      : 1024,
};
const agent = new Agent(agentOptions);

try {
  await cp(resolve(process.env.BENCH_DIST_DIR ?? "dist"), join(tempDir, "dist"), {
    recursive: true,
  });
  await adminDb.unsafe(`CREATE DATABASE ${dbName}`);

  const [mockPort, gwPort1, intPort1, gwPort2, intPort2, workerPort, redisPort] = await Promise.all(
    [
      findFreePort(),
      findFreePort(),
      findFreePort(),
      findFreePort(),
      findFreePort(),
      findFreePort(),
      findFreePort(),
    ],
  );

  let redisUrl: string | undefined;
  if (withRedis) {
    const redisBin =
      process.env.DURABILITY_REDIS_SERVER ??
      new URL("../.local/toolchain/redis74/bin/redis-server", import.meta.url).pathname;
    const redisData = join(tempDir, "redis");
    await cp(resolve("infra"), redisData, { recursive: true }).catch(() => undefined);
    const rProc = spawn(
      redisBin,
      [
        "--bind",
        "127.0.0.1",
        "--port",
        String(redisPort),
        "--dir",
        tempDir,
        "--save",
        "",
        "--appendonly",
        "no",
      ],
      { stdio: "ignore" },
    );
    procs.push(rProc);
    redisUrl = `redis://127.0.0.1:${redisPort}`;
    const rDeadline = Date.now() + 5000;
    while (Date.now() < rDeadline) {
      try {
        const cli = new Bun.RedisClient(redisUrl, { connectionTimeout: 300, autoReconnect: false });
        const pong = await cli.send("PING", []);
        cli.close();
        if (pong === "PONG") break;
      } catch {
        await Bun.sleep(50);
      }
    }
  }

  const baseEnv: Record<string, string> = {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    BUN_CONFIG_MAX_HTTP_REQUESTS: "4096",
    DATABASE_URL: targetUrl.toString(),
    ...(redisUrl ? { REDIS_URL: redisUrl, REDIS_NAMESPACE: "stg_bench" } : {}),
    KEY_PEPPER: "bprod-pepper-0123456789abcdef0123",
    ENCRYPTION_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    ADMIN_TOKEN: "bprod-admin-token-0123456789abcdef",
    ALLOW_PRIVATE_UPSTREAMS: "true",
    ACCESS_LOG: "false",
    SNAPSHOT_POLL_MS: "2000",
    MAX_IN_FLIGHT: "5000",
    DB_MAX_CONNECTIONS: "32",
    MICRO_BATCH_WINDOW_MS: process.env.BENCH_BATCH_WINDOW_MS ?? "2",
    MICRO_BATCH_MAX_SIZE: process.env.BENCH_BATCH_MAX_SIZE ?? "256",
    MOCK_HOST: "127.0.0.1",
    MOCK_PORT: String(mockPort),
    MOCK_CONTROL_TOKEN: "bprod-mock-ctrl",
    WORKER_HEALTH_HOST: "127.0.0.1",
    WORKER_HEALTH_PORT: String(workerPort),
  };

  await runSync(tempDir, ["dist/tools/migrate.js"], baseEnv);

  const mockProc = spawn(process.execPath, ["dist/mock-providers/main.js"], {
    cwd: tempDir,
    env: baseEnv,
    stdio: "ignore",
  });
  const workerProc = spawn(process.execPath, ["dist/worker/main.js"], {
    cwd: tempDir,
    env: baseEnv,
    stdio: "ignore",
  });
  const ports = [gwPort1, gwPort2].slice(0, gatewayNodes);
  const internalPorts = [intPort1, intPort2].slice(0, gatewayNodes);
  const gateways = ports.map((port, index) =>
    spawn(process.execPath, ["dist/gateway/main.js", "--mode=gateway"], {
      cwd: tempDir,
      env: {
        ...baseEnv,
        GATEWAY_HOST: "127.0.0.1",
        GATEWAY_PORT: String(port),
        INTERNAL_HOST: "127.0.0.1",
        INTERNAL_PORT: String(internalPorts[index]),
      },
      stdio: "ignore",
    }),
  );
  procs.push(mockProc, workerProc, ...gateways);

  await Promise.all([
    waitForJson(`http://127.0.0.1:${mockPort}/healthz`),
    ...ports.map((port) => waitForJson(`http://127.0.0.1:${port}/readyz`)),
    waitForJson(`http://127.0.0.1:${workerPort}/readyz`),
  ]);

  const adminHeaders = {
    authorization: `Bearer ${baseEnv.ADMIN_TOKEN}`,
    "content-type": "application/json",
  };
  const callInternal = async (method: string, path: string, body?: unknown) => {
    const res = await fetch(`http://127.0.0.1:${intPort1}${path}`, {
      method,
      headers: adminHeaders,
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
    if (!res.ok) throw new Error(`Internal ${path} failed: ${res.status} ${await res.text()}`);
    return (await res.json()) as Record<string, unknown>;
  };

  const acc = await callInternal("PUT", "/internal/v1/upstream-accounts", { name: "bprod-acc" });
  await callInternal("POST", `/internal/v1/upstream-accounts/${acc.id}/recharges`, {
    currency: "USD",
    external_ref: "bprod-sup-1",
    amount_atoms: "100000000000000",
  });
  const chPrimary = await callInternal("PUT", "/internal/v1/channels", {
    account_id: acc.id,
    name: "bprod-primary",
    kind: "openai",
    base_url: `http://127.0.0.1:${mockPort}/openai`,
    api_key: "sk-bprod-p",
    rpm: 10_000_000,
    concurrency: 10_000,
  });
  const chBackup = await callInternal("PUT", "/internal/v1/channels", {
    account_id: acc.id,
    name: "bprod-backup",
    kind: "openai",
    base_url: `http://127.0.0.1:${mockPort}/openai`,
    api_key: "sk-bprod-b",
    rpm: 10_000_000,
    concurrency: 10_000,
  });

  for (const name of ["bprod-fast", "bprod-long", "bprod-failover"]) {
    await callInternal("PUT", "/internal/v1/models", {
      name,
      operations: ["chat"],
      max_output_tokens: 64,
    });
  }
  const custPrice = await callInternal("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    currency: "USD",
    rates: {
      input_tokens: { numerator: "10", denominator: "1" },
      cached_input_tokens: { numerator: "5", denominator: "1" },
      output_tokens: { numerator: "20", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "64" },
  });
  const supPrice = await callInternal("POST", "/internal/v1/prices", {
    side: "SUPPLIER",
    currency: "USD",
    rates: {
      input_tokens: { numerator: "2", denominator: "1" },
      cached_input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "4", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "64" },
  });

  await callInternal("PUT", "/internal/v1/routes", {
    model_name: "bprod-fast",
    channel_id: chPrimary.id,
    upstream_model: "bprod-fast-up",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });
  await callInternal("PUT", "/internal/v1/routes", {
    model_name: "bprod-long",
    channel_id: chPrimary.id,
    upstream_model: "bprod-long-up",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });
  await callInternal("PUT", "/internal/v1/routes", {
    model_name: "bprod-failover",
    channel_id: chPrimary.id,
    upstream_model: "bprod-broken-up",
    operations: ["chat"],
    priority: 10,
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
    retry_policy: { max_attempts: 2 },
  });
  await callInternal("PUT", "/internal/v1/routes", {
    model_name: "bprod-failover",
    channel_id: chBackup.id,
    upstream_model: "bprod-fast-up",
    operations: ["chat"],
    priority: 5,
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
    retry_policy: { max_attempts: 2 },
  });

  await fetch(`http://127.0.0.1:${mockPort}/__control/scenarios`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${baseEnv.MOCK_CONTROL_TOKEN}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "bprod-long-up",
      scenario: { repeat: 8, streamDurationMs: longStreamMs },
    }),
  });
  await fetch(`http://127.0.0.1:${mockPort}/__control/scenarios`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${baseEnv.MOCK_CONTROL_TOKEN}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "bprod-broken-up",
      scenario: { status: 503 },
    }),
  });

  const mixedPlugins = isMixed
    ? await seedMixedPlugins({
        callInternal,
        mockPort,
        mockToken: baseEnv.MOCK_CONTROL_TOKEN!,
        channelId: chPrimary.id,
        billingPriceId: custPrice.id,
        costVersionId: supPrice.id,
      })
    : null;

  const keys: string[] = [];
  for (let i = 0; i < 32; i++) {
    const p = await callInternal("PUT", "/internal/v1/projects", {
      name: `bprod-p-${i}`,
      currency: "USD",
      rpm: 10_000_000,
      tpm: 1_000_000_000,
      concurrency: 5000,
    });
    const k = await callInternal("PUT", "/internal/v1/api-keys", {
      project_id: p.id,
      name: `k-${i}`,
    });
    await callInternal("POST", `/internal/v1/wallets/${p.id}/credits`, {
      currency: "USD",
      external_ref: `bprod-c-${i}`,
      amount_atoms: "1000000000000",
      kind: "CASH",
    });
    keys.push(String(k.key));
  }

  await Bun.sleep(250);

  const db = createDatabase(targetUrl.toString(), 4);
  const walLsnBytes = async (): Promise<bigint> => {
    const rows =
      await db`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint::text AS bytes`;
    return BigInt(String(rows[0]!.bytes));
  };

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
    const latencies: number[] = [];
    const walBefore = await walLsnBytes();
    const rssBefore = gateways.reduce((total, process) => total + getRssMb(process.pid!), 0);
    const started = performance.now();
    const stopAt = started + seconds * 1000;
    let cursor = 0;

    async function worker() {
      while (performance.now() < stopAt) {
        const idx = cursor++;
        const port = ports[idx % ports.length]!;
        const key = keyPool[idx % keyPool.length]!;
        const t0 = performance.now();
        const result = await postJsonKeepAlive(agent, port, key, payload, stream);
        if (result.ok) completed++;
        else {
          errors++;
          if (sampleErrors.length < 5)
            sampleErrors.push(`${result.status} ${result.errText ?? ""}`);
        }
        latencies.push(performance.now() - t0);
      }
    }

    await Promise.all(Array.from({ length: concurrency }, () => worker()));
    const elapsedSec = (performance.now() - started) / 1000;
    const walAfter = await walLsnBytes();
    const rssAfter = gateways.reduce((total, process) => total + getRssMb(process.pid!), 0);
    latencies.sort((a, b) => a - b);
    return {
      phase: label,
      gateway_nodes: 2,
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
      combined_gw_rss_mb_before: Number(rssBefore.toFixed(1)),
      combined_gw_rss_mb_after: Number(rssAfter.toFixed(1)),
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
    const latencies: number[] = [];
    const walBefore = await walLsnBytes();
    const rssBefore = gateways.reduce((total, process) => total + getRssMb(process.pid!), 0);
    const started = performance.now();
    let cursor = 0;

    async function worker() {
      while (true) {
        const idx = cursor++;
        if (idx >= totalRequests) break;
        const port = ports[idx % ports.length]!;
        const key = keyPool[idx % keyPool.length]!;
        const t0 = performance.now();
        const result = await postJsonKeepAlive(agent, port, key, payload, stream);
        if (result.ok) completed++;
        else {
          errors++;
          if (sampleErrors.length < 5)
            sampleErrors.push(`${result.status} ${result.errText ?? ""}`);
        }
        latencies.push(performance.now() - t0);
      }
    }

    await Promise.all(Array.from({ length: concurrency }, () => worker()));
    const elapsedSec = (performance.now() - started) / 1000;
    const walAfter = await walLsnBytes();
    const rssAfter = gateways.reduce((total, process) => total + getRssMb(process.pid!), 0);
    latencies.sort((a, b) => a - b);
    return {
      phase: label,
      gateway_nodes: 2,
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
      combined_gw_rss_mb_before: Number(rssBefore.toFixed(1)),
      combined_gw_rss_mb_after: Number(rssAfter.toFixed(1)),
    };
  }

  async function runLimitPhases() {
    if (warmLimitSockets) {
      const target = Math.ceil(Math.max(...limitConcurrency) / ports.length);
      await Promise.all(ports.map((port) => warmHttpConnections(agent, port, target)));
    }
    const results: Awaited<ReturnType<typeof runTimedLoad>>[] = [];
    for (const concurrency of limitConcurrency) {
      results.push(
        await runTimedLoad(
          `2node_limit_8s_json_${concurrency}c`,
          "bprod-fast",
          keys,
          concurrency,
          8,
          false,
        ),
      );
    }
    return results;
  }

  try {
    const warmup = await runFixedCountLoad("warmup", "bprod-fast", keys, 32, 200, false);

    const redis = isSseBurst
      ? new Bun.RedisClient(redisUrl!, { connectionTimeout: 300, autoReconnect: false })
      : undefined;
    const burstAgent = isSseBurstFresh ? new Agent(agentOptions) : agent;
    if (mixedPlugins) {
      const target = Math.ceil(soakConcurrency / ports.length);
      await Promise.all(ports.map((port) => warmHttpConnections(agent, port, target)));
    }
    const mixedWalBefore = mixedPlugins ? await walLsnBytes() : null;
    const results = mixedPlugins
      ? [
          await runMixedSoak({
            agent,
            ports,
            keys,
            seconds: soakSeconds,
            targetRps: soakRps,
            concurrency: soakConcurrency,
            processes: {
              client: process.pid,
              mock: mockProc.pid!,
              worker: workerProc.pid!,
              ...(withRedis ? { redis: procs[0]!.pid! } : {}),
              ...Object.fromEntries(
                gateways.map((gateway, index) => [`gateway_${index + 1}`, gateway.pid!]),
              ),
            },
          }),
        ]
      : redis
        ? await runSseBurst({
            agent: burstAgent,
            freshOnly: isSseBurstFresh,
            ports: [gwPort1, gwPort2],
            keys,
            db,
            redis,
            quotaPattern: "stg_bench:rl:conc:*",
            mockPort,
            mockToken: baseEnv.MOCK_CONTROL_TOKEN!,
            readGatewayRss: () =>
              gateways.reduce((total, process) => total + getRssMb(process.pid!), 0),
          }).finally(() => {
            redis.close();
            if (burstAgent !== agent) burstAgent.destroy();
          })
        : isLimit
          ? await runLimitPhases()
          : [
              await runTimedLoad(
                `2node_sustained_${durationSec}s_json_128c`,
                "bprod-fast",
                keys,
                128,
                durationSec,
                false,
              ),
              await runTimedLoad(
                `2node_sustained_${durationSec}s_json_256c`,
                "bprod-fast",
                keys,
                256,
                durationSec,
                false,
              ),
              await runTimedLoad(
                `2node_sustained_${durationSec}s_sse_128c`,
                "bprod-fast",
                keys,
                128,
                durationSec,
                true,
              ),
              await runTimedLoad(
                `2node_hot_wallet_${hotWalletSec}s_128c`,
                "bprod-fast",
                [keys[0]!],
                128,
                hotWalletSec,
                false,
              ),
              await runFixedCountLoad(
                `2node_${longStreamCount}_concurrent_${longStreamMs / 1000}s_sse_streams`,
                "bprod-long",
                keys,
                longStreamCount,
                longStreamCount,
                true,
              ),
              await runFixedCountLoad(
                "2node_live_failover_503_to_backup_64c",
                "bprod-failover",
                keys,
                64,
                1000,
                false,
              ),
            ];

    const mixedWalBytes =
      mixedWalBefore === null ? null : ((await walLsnBytes()) - mixedWalBefore).toString();
    const idleStarted = performance.now();
    const leaseDrainBudgetMs = 5_000;
    await Bun.sleep(600);
    const metrics = await Promise.all(
      ports.map(async (port) => (await fetch(`http://127.0.0.1:${port}/metrics`)).text()),
    );
    const nodeMetrics = (pattern: RegExp, fallback: number) =>
      Object.fromEntries(
        metrics.map((body, index) => [
          `node${index + 1}`,
          Number(pattern.exec(body)?.[1] ?? fallback),
        ]),
      );
    const inFlight = nodeMetrics(/gateway_in_flight (\d+)/, -1);
    const failovers = nodeMetrics(/gateway_failovers_total (\d+)/, 0);
    async function countActiveLeases() {
      const [row] = await db<{ n: number }[]>`
          SELECT (
            (SELECT count(*) FROM public.wallet_leases WHERE state = 'ACTIVE') +
            (SELECT count(*) FROM public.supplier_leases WHERE state = 'ACTIVE')
          )::int AS n
        `;
      return row!.n;
    }
    const firstLeaseProbe = await countActiveLeases();
    let activeLeases = firstLeaseProbe;
    while (activeLeases > 0 && performance.now() - idleStarted < leaseDrainBudgetMs) {
      await Bun.sleep(100);
      activeLeases = await countActiveLeases();
    }
    const idleElapsedMs = Number((performance.now() - idleStarted).toFixed(2));
    const pgSettings = await db`
      SELECT name, setting FROM pg_settings
      WHERE name IN ('fsync', 'synchronous_commit', 'full_page_writes')
      ORDER BY name
    `;
    const chargeCounts = (
      reader: SQL,
    ) => reader`SELECT count(*) AS total, count(DISTINCT request_id) AS unique,
        count(*) FILTER (WHERE outcome = 'SETTLED') AS settled
      FROM public.billing_charges`;
    const verified = mixedPlugins
      ? await db.begin(async (tx) => {
          await tx`SET LOCAL statement_timeout = '90s'`;
          const counts = await chargeCounts(tx);
          const audit = await auditConservation(tx);
          const exact = await verifyMixedCharges(tx, mixedPlugins);
          const balances = await verifyMixedBalances(tx);
          const [storage] = await tx`
            SELECT pg_database_size(current_database())::text AS db_bytes, blks_read::text,
              blks_hit::text, temp_bytes::text, xact_commit::text
            FROM pg_stat_database WHERE datname = current_database()
          `;
          return { counts, audit, exact, balances, storage };
        })
      : {
          counts: await chargeCounts(db),
          audit: await callInternal("GET", "/internal/v1/audit/conservation"),
          exact: null,
          balances: null,
          storage: null,
        };
    const charges = verified.counts[0]!;
    const totalCharges = Number(charges.total);
    const uniqueCharges = Number(charges.unique);
    const totalSettledCharges = Number(charges.settled);
    const audit = verified.audit;
    const mixedReport = results.find((result) => "lanes" in result);
    const mixedChargeCounts = new Map(
      mixedReport?.lanes.map((lane) => [`mixed-${lane.name}`, lane.completed]),
    );
    const mixedIncorrect =
      verified.exact?.some(
        (row: MixedChargeCheck) =>
          row.wrong_atoms_or_usage !== 0 ||
          row.wrong_plugin !== 0 ||
          row.charged !==
            (row.model === "bprod-fast"
              ? warmup.completed
              : mixedChargeCounts.get(String(row.model))),
      ) ?? false;
    const allPhases = [warmup, ...results];
    const successfulResponses = allPhases.reduce((count, phase) => count + phase.completed, 0);
    const totalErrors = allPhases.reduce((count, phase) => count + phase.errors, 0);

    console.log(
      JSON.stringify(
        {
          bun_version: Bun.version,
          mode: isMixed
            ? "mixed-soak"
            : isSseBurstFresh
              ? "sse-burst-fresh"
              : isSseBurst
                ? "sse-burst"
                : isLimit
                  ? "limit"
                  : isStress
                    ? "stress"
                    : "standard",
          topology: `${gatewayNodes}x standalone dist/gateway + 1x standalone dist/worker + ${withRedis ? "1x Redis 7.4 + " : ""}1x standalone dist/mock-providers + separate load client + PG 17`,
          micro_batch: {
            window_ms: Number(baseEnv.MICRO_BATCH_WINDOW_MS),
            max_size: Number(baseEnv.MICRO_BATCH_MAX_SIZE),
          },
          ...(isLimit ? { connection_mode: warmLimitSockets ? "warm" : "legacy" } : {}),
          pg_durability: Object.fromEntries(
            pgSettings.map((r: Record<string, unknown>) => [String(r.name), String(r.setting)]),
          ),
          successful_responses: successfulResponses,
          total_errors: totalErrors,
          total_settled_charges: totalSettledCharges,
          duplicate_request_charges: totalCharges - uniqueCharges,
          active_leases_after_idle: activeLeases,
          lease_drain: {
            active_at_first_probe: firstLeaseProbe,
            elapsed_ms: idleElapsedMs,
            budget_ms: leaseDrainBudgetMs,
          },
          results,
          ...(mixedPlugins
            ? {
                exact_ledger: verified.exact,
                exact_balances: verified.balances,
                storage: { ...verified.storage, wal_bytes: mixedWalBytes },
              }
            : {}),
          failovers_executed: failovers,
          post_drain_in_flight: inFlight,
          conservation: audit,
        },
        null,
        2,
      ),
    );
    if (
      totalErrors !== 0 ||
      (isSseBurst && results.some((result) => "passed" in result && !result.passed)) ||
      totalSettledCharges !== successfulResponses ||
      totalSettledCharges !== totalCharges ||
      totalCharges !== uniqueCharges ||
      !audit.ok ||
      audit.open_holds !== 0 ||
      audit.unknown_holds !== 0 ||
      mixedIncorrect ||
      (verified.balances && Object.values(verified.balances).some((n) => n !== 0)) ||
      (mixedReport && mixedReport.last_dispatch_ms > soakSeconds * 1000 + 50) ||
      idleElapsedMs > leaseDrainBudgetMs ||
      Object.values(inFlight).some((count) => count !== 0) ||
      activeLeases !== 0
    ) {
      throw new Error("Production stress benchmark invariant check failed");
    }
  } finally {
    await db.close();
  }
} finally {
  agent.destroy();
  await stopProcesses(procs);
  await adminDb.unsafe(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${dbName}' AND pid <> pg_backend_pid()`,
  );
  await adminDb.unsafe(`DROP DATABASE IF EXISTS ${dbName}`);
  await adminDb.close();
  await rm(tempDir, { recursive: true, force: true });
}
