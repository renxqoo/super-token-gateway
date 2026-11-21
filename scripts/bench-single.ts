import { spawn, spawnSync } from "node:child_process";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { Agent, request as httpRequest } from "node:http";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createDatabase } from "@gateway/ledger";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const durationSec = Number(process.env.BENCH_SECONDS ?? 10);

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

function getProcStats(pid: number): { rssMb: number; cpuPct: number } {
  const out = spawnSync("ps", ["-o", "rss=,%cpu=", "-p", String(pid)], { encoding: "utf8" });
  const parts = out.stdout.trim().split(/\s+/);
  const rssKb = Number(parts[0] ?? 0);
  const cpuPct = Number(parts[1] ?? 0);
  return {
    rssMb: Number.isFinite(rssKb) ? Number((rssKb / 1024).toFixed(1)) : 0,
    cpuPct: Number.isFinite(cpuPct) ? Number(cpuPct.toFixed(1)) : 0,
  };
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p));
  return Number(sorted[idx]!.toFixed(2));
}

function postJsonKeepAlive(
  agent: Agent,
  port: number,
  apiKey: string,
  payload: Buffer,
  expectDone: boolean,
): Promise<{ ok: boolean; status: number }> {
  return new Promise((resolveDone) => {
    const req = httpRequest(
      {
        hostname: "127.0.0.1",
        port,
        path: "/v1/chat/completions",
        method: "POST",
        agent,
        headers: {
          authorization: `Bearer ${apiKey}`,
          "content-type": "application/json",
          "content-length": payload.byteLength,
        },
      },
      (res) => {
        const status = res.statusCode ?? 0;
        let sawDone = !expectDone;
        res.on("data", (chunk: Buffer) => {
          if (!sawDone && chunk.includes("[DONE]")) sawDone = true;
        });
        res.on("end", () => resolveDone({ ok: status === 200 && sawDone, status }));
        res.on("error", () => resolveDone({ ok: false, status }));
      },
    );
    req.on("error", () => resolveDone({ ok: false, status: 0 }));
    req.end(payload);
  });
}

const tempDir = await mkdtemp(join(tmpdir(), "stg-bench-single-"));
const dbName = `gateway_v4_bsingle_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const adminUrl = new URL(baseDbUrl);
adminUrl.pathname = "/postgres";
const targetUrl = new URL(baseDbUrl);
targetUrl.pathname = `/${dbName}`;

const adminDb = createDatabase(adminUrl.toString(), 2);
const procs: ReturnType<typeof spawn>[] = [];
const agent = new Agent({ keepAlive: true, maxSockets: 1024, maxFreeSockets: 512 });

try {
  await cp(resolve("dist"), join(tempDir, "dist"), { recursive: true });
  await adminDb.unsafe(`CREATE DATABASE ${dbName}`);

  const [mockPort, gwPort, intPort] = await Promise.all([
    findFreePort(),
    findFreePort(),
    findFreePort(),
  ]);

  const env: Record<string, string> = {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    DATABASE_URL: targetUrl.toString(),
    KEY_PEPPER: "bsingle-pepper-0123456789abcdef01",
    ENCRYPTION_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    ADMIN_TOKEN: "bsingle-admin-token-0123456789abc",
    ALLOW_PRIVATE_UPSTREAMS: "true",
    ACCESS_LOG: "false",
    SNAPSHOT_POLL_MS: "0",
    MAX_IN_FLIGHT: "4000",
    DB_MAX_CONNECTIONS: "32",
    MICRO_BATCH_WINDOW_MS: "3",
    MICRO_BATCH_MAX_SIZE: "128",
    GATEWAY_HOST: "127.0.0.1",
    GATEWAY_PORT: String(gwPort),
    INTERNAL_HOST: "127.0.0.1",
    INTERNAL_PORT: String(intPort),
    MOCK_HOST: "127.0.0.1",
    MOCK_PORT: String(mockPort),
    MOCK_CONTROL_TOKEN: "bsingle-mock-ctrl",
  };

  await runSync(tempDir, ["dist/tools/migrate.js"], env);

  const mockProc = spawn(process.execPath, ["dist/mock-providers/main.js"], {
    cwd: tempDir,
    env,
    stdio: "ignore",
  });
  const gwProc = spawn(process.execPath, ["dist/gateway/main.js", "--mode=gateway"], {
    cwd: tempDir,
    env,
    stdio: "ignore",
  });
  procs.push(mockProc, gwProc);

  await Promise.all([
    waitForJson(`http://127.0.0.1:${mockPort}/healthz`),
    waitForJson(`http://127.0.0.1:${gwPort}/readyz`),
  ]);

  const adminHeaders = {
    authorization: `Bearer ${env.ADMIN_TOKEN}`,
    "content-type": "application/json",
  };
  const callInternal = async (method: string, path: string, body?: unknown) => {
    const res = await fetch(`http://127.0.0.1:${intPort}${path}`, {
      method,
      headers: adminHeaders,
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
    if (!res.ok) throw new Error(`Internal ${path} failed: ${res.status} ${await res.text()}`);
    return (await res.json()) as Record<string, unknown>;
  };

  const acc = await callInternal("PUT", "/internal/v1/upstream-accounts", { name: "bsingle-acc" });
  await callInternal("POST", `/internal/v1/upstream-accounts/${acc.id}/recharges`, {
    currency: "USD",
    external_ref: "bsingle-sup-1",
    amount_atoms: "100000000000000",
  });
  const ch = await callInternal("PUT", "/internal/v1/channels", {
    account_id: acc.id,
    name: "bsingle-ch",
    kind: "openai",
    base_url: `http://127.0.0.1:${mockPort}/openai`,
    api_key: "sk-bsingle",
    rpm: 10_000_000,
    concurrency: 10_000,
  });
  await callInternal("PUT", "/internal/v1/models", {
    name: "bsingle-gpt",
    operations: ["chat"],
    max_output_tokens: 64,
  });
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
    model_name: "bsingle-gpt",
    channel_id: ch.id,
    upstream_model: "bsingle-up",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });

  const keys: string[] = [];
  for (let i = 0; i < 32; i++) {
    const p = await callInternal("PUT", "/internal/v1/projects", {
      name: `bsingle-p-${i}`,
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
      external_ref: `bsingle-c-${i}`,
      amount_atoms: "1000000000000",
      kind: "CASH",
    });
    keys.push(String(k.key));
  }

  const db = createDatabase(targetUrl.toString(), 4);
  const walLsnBytes = async (): Promise<bigint> => {
    const rows =
      await db`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint::text AS bytes`;
    return BigInt(String(rows[0]!.bytes));
  };

  const jsonPayload = Buffer.from(
    JSON.stringify({
      model: "bsingle-gpt",
      stream: false,
      messages: [{ role: "user", content: "hi" }],
      max_tokens: 32,
    }),
  );
  const ssePayload = Buffer.from(
    JSON.stringify({
      model: "bsingle-gpt",
      stream: true,
      messages: [{ role: "user", content: "hi" }],
      max_tokens: 32,
    }),
  );

  async function runPhase(
    label: string,
    keyPool: string[],
    concurrency: number,
    seconds: number,
    stream: boolean,
  ) {
    let completed = 0;
    let errors = 0;
    const latencies: number[] = [];
    const payload = stream ? ssePayload : jsonPayload;
    const walBefore = await walLsnBytes();
    const started = performance.now();
    const stopAt = started + seconds * 1000;
    let cursor = 0;

    async function worker() {
      while (performance.now() < stopAt) {
        const idx = cursor++;
        const key = keyPool[idx % keyPool.length]!;
        const t0 = performance.now();
        const res = await postJsonKeepAlive(agent, gwPort, key, payload, stream);
        if (res.ok) completed++;
        else errors++;
        latencies.push(performance.now() - t0);
      }
    }

    await Promise.all(Array.from({ length: concurrency }, () => worker()));
    const elapsedSec = (performance.now() - started) / 1000;
    const walAfter = await walLsnBytes();
    const stats = getProcStats(gwProc.pid!);
    latencies.sort((a, b) => a - b);
    return {
      phase: label,
      concurrency,
      completed,
      errors,
      elapsed_sec: Number(elapsedSec.toFixed(3)),
      rps: Number((completed / elapsedSec).toFixed(1)),
      p50_ms: percentile(latencies, 0.5),
      p95_ms: percentile(latencies, 0.95),
      p99_ms: percentile(latencies, 0.99),
      wal_bytes_per_req: completed > 0 ? Number((walAfter - walBefore) / BigInt(completed)) : 0,
      gw_rss_mb: stats.rssMb,
      gw_cpu_pct: stats.cpuPct,
    };
  }

  try {
    await runPhase("warmup", keys, 32, 2, false);

    const results = [
      await runPhase(`1node_json_64c_${durationSec}s`, keys, 64, durationSec, false),
      await runPhase(`1node_json_128c_${durationSec}s`, keys, 128, durationSec, false),
      await runPhase(`1node_json_256c_${durationSec}s`, keys, 256, durationSec, false),
      await runPhase(`1node_json_512c_${durationSec}s`, keys, 512, durationSec, false),
      await runPhase(`1node_sse_256c_${durationSec}s`, keys, 256, durationSec, true),
      await runPhase(`1node_hot_wallet_256c_${durationSec}s`, [keys[0]!], 256, durationSec, false),
    ];

    const metricsTxt = await (await fetch(`http://127.0.0.1:${gwPort}/metrics`)).text();
    const inFlight = Number(/gateway_in_flight (\d+)/.exec(metricsTxt)?.[1] ?? -1);
    const totalCharges = Number(
      (await db`SELECT count(*)::int AS n FROM public.billing_charges`)[0]!.n,
    );
    const pgSettings = await db`
      SELECT name, setting FROM pg_settings
      WHERE name IN ('fsync', 'synchronous_commit', 'full_page_writes')
      ORDER BY name
    `;
    const audit = await callInternal("GET", "/internal/v1/audit/conservation");

    console.log(
      JSON.stringify(
        {
          bun_version: Bun.version,
          topology:
            "1x standalone dist/gateway + 1x standalone dist/mock-providers + separate keep-alive load client (maxSockets=1024) + PG 17",
          micro_batch: { window_ms: 3, max_size: 128 },
          pg_durability: Object.fromEntries(
            pgSettings.map((r: Record<string, unknown>) => [String(r.name), String(r.setting)]),
          ),
          total_settled_charges: totalCharges,
          results,
          post_drain_in_flight: inFlight,
          conservation: audit,
        },
        null,
        2,
      ),
    );
    if (!audit.ok || audit.open_holds !== 0 || inFlight !== 0) {
      throw new Error("Single-node benchmark invariant check failed");
    }
  } finally {
    await db.close();
  }
} finally {
  agent.destroy();
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
