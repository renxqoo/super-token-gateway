import { spawn, spawnSync } from "node:child_process";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createDatabase } from "@gateway/ledger";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const isStress = process.argv.includes("--stress");
const withRedis = !process.argv.includes("--no-redis");
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
  const out = spawnSync("ps", ["-o", "rss=", "-p", String(pid)], { encoding: "utf8" });
  const kb = Number(out.stdout.trim());
  return Number.isFinite(kb) ? Number((kb / 1024).toFixed(1)) : 0;
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

try {
  await cp(resolve("dist"), join(tempDir, "dist"), { recursive: true });
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
    MICRO_BATCH_WINDOW_MS: "2",
    MICRO_BATCH_MAX_SIZE: "256",
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
  const gw1 = spawn(process.execPath, ["dist/gateway/main.js", "--mode=gateway"], {
    cwd: tempDir,
    env: {
      ...baseEnv,
      GATEWAY_HOST: "127.0.0.1",
      GATEWAY_PORT: String(gwPort1),
      INTERNAL_HOST: "127.0.0.1",
      INTERNAL_PORT: String(intPort1),
    },
    stdio: "ignore",
  });
  const gw2 = spawn(process.execPath, ["dist/gateway/main.js", "--mode=gateway"], {
    cwd: tempDir,
    env: {
      ...baseEnv,
      GATEWAY_HOST: "127.0.0.1",
      GATEWAY_PORT: String(gwPort2),
      INTERNAL_HOST: "127.0.0.1",
      INTERNAL_PORT: String(intPort2),
    },
    stdio: "ignore",
  });
  procs.push(mockProc, workerProc, gw1, gw2);

  await Promise.all([
    waitForJson(`http://127.0.0.1:${mockPort}/healthz`),
    waitForJson(`http://127.0.0.1:${gwPort1}/readyz`),
    waitForJson(`http://127.0.0.1:${gwPort2}/readyz`),
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
  const endpoints = [
    `http://127.0.0.1:${gwPort1}/v1/chat/completions`,
    `http://127.0.0.1:${gwPort2}/v1/chat/completions`,
  ];

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
    const latencies: number[] = [];
    const walBefore = await walLsnBytes();
    const rssBefore = getRssMb(gw1.pid!) + getRssMb(gw2.pid!);
    const started = performance.now();
    const stopAt = started + seconds * 1000;
    let cursor = 0;

    async function worker() {
      while (performance.now() < stopAt) {
        const idx = cursor++;
        const url = endpoints[idx % endpoints.length]!;
        const key = keyPool[idx % keyPool.length]!;
        const t0 = performance.now();
        try {
          const res = await fetch(url, {
            method: "POST",
            headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
            body: JSON.stringify({
              model,
              stream,
              messages: [{ role: "user", content: "hi" }],
              max_tokens: 32,
            }),
          });
          const bodyText = await res.text();
          if (res.status === 200 && (!stream || bodyText.includes("[DONE]"))) completed++;
          else errors++;
        } catch {
          errors++;
        }
        latencies.push(performance.now() - t0);
      }
    }

    await Promise.all(Array.from({ length: concurrency }, () => worker()));
    const elapsedSec = (performance.now() - started) / 1000;
    const walAfter = await walLsnBytes();
    const rssAfter = getRssMb(gw1.pid!) + getRssMb(gw2.pid!);
    latencies.sort((a, b) => a - b);
    return {
      phase: label,
      gateway_nodes: 2,
      redis_enabled: Boolean(redisUrl),
      concurrency,
      completed,
      errors,
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
    const latencies: number[] = [];
    const walBefore = await walLsnBytes();
    const rssBefore = getRssMb(gw1.pid!) + getRssMb(gw2.pid!);
    const started = performance.now();
    let cursor = 0;

    async function worker() {
      while (true) {
        const idx = cursor++;
        if (idx >= totalRequests) break;
        const url = endpoints[idx % endpoints.length]!;
        const key = keyPool[idx % keyPool.length]!;
        const t0 = performance.now();
        try {
          const res = await fetch(url, {
            method: "POST",
            headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
            body: JSON.stringify({
              model,
              stream,
              messages: [{ role: "user", content: "hi" }],
              max_tokens: 32,
            }),
          });
          const bodyText = await res.text();
          if (res.status === 200 && (!stream || bodyText.includes("[DONE]"))) completed++;
          else errors++;
        } catch {
          errors++;
        }
        latencies.push(performance.now() - t0);
      }
    }

    await Promise.all(Array.from({ length: concurrency }, () => worker()));
    const elapsedSec = (performance.now() - started) / 1000;
    const walAfter = await walLsnBytes();
    const rssAfter = getRssMb(gw1.pid!) + getRssMb(gw2.pid!);
    latencies.sort((a, b) => a - b);
    return {
      phase: label,
      gateway_nodes: 2,
      redis_enabled: Boolean(redisUrl),
      concurrency,
      completed,
      errors,
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

  try {
    await runFixedCountLoad("warmup", "bprod-fast", keys, 32, 200, false);

    const results = [
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

    await Bun.sleep(600);
    const m1 = await (await fetch(`http://127.0.0.1:${gwPort1}/metrics`)).text();
    const m2 = await (await fetch(`http://127.0.0.1:${gwPort2}/metrics`)).text();
    const inFlight1 = Number(/gateway_in_flight (\d+)/.exec(m1)?.[1] ?? -1);
    const inFlight2 = Number(/gateway_in_flight (\d+)/.exec(m2)?.[1] ?? -1);
    const failovers1 = Number(/gateway_failovers_total (\d+)/.exec(m1)?.[1] ?? 0);
    const failovers2 = Number(/gateway_failovers_total (\d+)/.exec(m2)?.[1] ?? 0);
    const activeLeases = Number(
      (
        await db`
          SELECT (
            (SELECT count(*) FROM public.wallet_leases WHERE state = 'ACTIVE') +
            (SELECT count(*) FROM public.supplier_leases WHERE state = 'ACTIVE')
          )::int AS n
        `
      )[0]!.n,
    );
    const pgSettings = await db`
      SELECT name, setting FROM pg_settings
      WHERE name IN ('fsync', 'synchronous_commit', 'full_page_writes')
      ORDER BY name
    `;
    const totalCharges = Number(
      (await db`SELECT count(*)::int AS n FROM public.billing_charges`)[0]!.n,
    );
    const audit = await callInternal("GET", "/internal/v1/audit/conservation");

    console.log(
      JSON.stringify(
        {
          bun_version: Bun.version,
          mode: isStress ? "stress" : "standard",
          topology:
            "2x standalone dist/gateway + 1x standalone dist/worker + 1x Redis 7.4 + 1x standalone dist/mock-providers + separate load client + PG 17",
          pg_durability: Object.fromEntries(
            pgSettings.map((r: Record<string, unknown>) => [String(r.name), String(r.setting)]),
          ),
          total_settled_charges: totalCharges,
          active_leases_after_idle: activeLeases,
          results,
          failovers_executed: { node1: failovers1, node2: failovers2 },
          post_drain_in_flight: { node1: inFlight1, node2: inFlight2 },
          conservation: audit,
        },
        null,
        2,
      ),
    );
    if (
      !audit.ok ||
      audit.open_holds !== 0 ||
      inFlight1 !== 0 ||
      inFlight2 !== 0 ||
      activeLeases !== 0
    ) {
      throw new Error("Production stress benchmark invariant check failed");
    }
  } finally {
    await db.close();
  }
} finally {
  for (const p of procs) {
    try {
      p.kill("SIGTERM");
    } catch {
      void 0;
    }
  }
  await Bun.sleep(300);
  await adminDb.unsafe(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${dbName}' AND pid <> pg_backend_pid()`,
  );
  await adminDb.unsafe(`DROP DATABASE IF EXISTS ${dbName}`);
  await adminDb.close();
  await rm(tempDir, { recursive: true, force: true });
}
