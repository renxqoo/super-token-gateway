import { spawn, spawnSync } from "node:child_process";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createDatabase } from "@gateway/ledger";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

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

  const [mockPort, gwPort1, intPort1, gwPort2, intPort2] = await Promise.all([
    findFreePort(),
    findFreePort(),
    findFreePort(),
    findFreePort(),
    findFreePort(),
  ]);

  const baseEnv: Record<string, string> = {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    DATABASE_URL: targetUrl.toString(),
    KEY_PEPPER: "bprod-pepper-0123456789abcdef0123",
    ENCRYPTION_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    ADMIN_TOKEN: "bprod-admin-token-0123456789abcdef",
    ALLOW_PRIVATE_UPSTREAMS: "true",
    ACCESS_LOG: "false",
    SNAPSHOT_POLL_MS: "200",
    MAX_IN_FLIGHT: "4000",
    DB_MAX_CONNECTIONS: "32",
    MICRO_BATCH_WINDOW_MS: "3",
    MICRO_BATCH_MAX_SIZE: "64",
    MOCK_HOST: "127.0.0.1",
    MOCK_PORT: String(mockPort),
    MOCK_CONTROL_TOKEN: "bprod-mock-ctrl",
  };

  await runSync(tempDir, ["dist/tools/migrate.js"], baseEnv);

  const mockProc = spawn(process.execPath, ["dist/mock-providers/main.js"], {
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
  procs.push(mockProc, gw1, gw2);

  await Promise.all([
    waitForJson(`http://127.0.0.1:${mockPort}/healthz`),
    waitForJson(`http://127.0.0.1:${gwPort1}/readyz`),
    waitForJson(`http://127.0.0.1:${gwPort2}/readyz`),
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
    amount_atoms: "10000000000000",
  });
  const ch = await callInternal("PUT", "/internal/v1/channels", {
    account_id: acc.id,
    name: "bprod-ch",
    kind: "openai",
    base_url: `http://127.0.0.1:${mockPort}/openai`,
    api_key: "sk-bprod",
    rpm: 10_000_000,
    concurrency: 10_000,
  });
  await callInternal("PUT", "/internal/v1/models", {
    name: "bprod-fast",
    operations: ["chat"],
    max_output_tokens: 64,
  });
  await callInternal("PUT", "/internal/v1/models", {
    name: "bprod-long",
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
    model_name: "bprod-fast",
    channel_id: ch.id,
    upstream_model: "bprod-fast-up",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });
  await callInternal("PUT", "/internal/v1/routes", {
    model_name: "bprod-long",
    channel_id: ch.id,
    upstream_model: "bprod-long-up",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });

  await fetch(`http://127.0.0.1:${mockPort}/__control/scenarios`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${baseEnv.MOCK_CONTROL_TOKEN}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "bprod-long-up",
      scenario: { repeat: 6, streamDurationMs: 2500 },
    }),
  });

  const keys: string[] = [];
  for (let i = 0; i < 32; i++) {
    const p = await callInternal("PUT", "/internal/v1/projects", {
      name: `bprod-p-${i}`,
      currency: "USD",
      rpm: 10_000_000,
      tpm: 1_000_000_000,
      concurrency: 1000,
    });
    const k = await callInternal("PUT", "/internal/v1/api-keys", {
      project_id: p.id,
      name: `k-${i}`,
    });
    await callInternal("POST", `/internal/v1/wallets/${p.id}/credits`, {
      currency: "USD",
      external_ref: `bprod-c-${i}`,
      amount_atoms: "100000000000",
      kind: "CASH",
    });
    keys.push(String(k.key));
  }

  await Bun.sleep(400);

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

  async function runLoad(
    label: string,
    model: string,
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
        const key = keys[idx % keys.length]!;
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
      concurrency,
      total_requests: totalRequests,
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
    await runLoad("warmup", "bprod-fast", 32, 200, false);

    const results = [
      await runLoad("2node_short_chat_json", "bprod-fast", 128, 5000, false),
      await runLoad("2node_short_chat_sse", "bprod-fast", 128, 5000, true),
      await runLoad("2node_500_concurrent_2.5s_sse_streams", "bprod-long", 500, 500, true),
    ];

    const m1 = await (await fetch(`http://127.0.0.1:${gwPort1}/metrics`)).text();
    const m2 = await (await fetch(`http://127.0.0.1:${gwPort2}/metrics`)).text();
    const inFlight1 = Number(/gateway_in_flight (\d+)/.exec(m1)?.[1] ?? -1);
    const inFlight2 = Number(/gateway_in_flight (\d+)/.exec(m2)?.[1] ?? -1);
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
            "2x standalone dist/gateway + 1x standalone dist/mock-providers + separate load client + PG 17",
          pg_durability: Object.fromEntries(
            pgSettings.map((r: Record<string, unknown>) => [String(r.name), String(r.setting)]),
          ),
          results,
          post_drain_in_flight: { node1: inFlight1, node2: inFlight2 },
          conservation: audit,
        },
        null,
        2,
      ),
    );
    if (!audit.ok || audit.open_holds !== 0 || inFlight1 !== 0 || inFlight2 !== 0) {
      throw new Error("Multi-process benchmark invariant check failed");
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
