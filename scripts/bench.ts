import { createDatabase, migrate } from "@gateway/ledger";
import { createGatewayRuntime, type GatewayConfig } from "@gateway/gateway";
import { createMockApp } from "../apps/mock-providers/src/app.ts";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const dbName = `gateway_v4_bench_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const adminUrl = new URL(baseDbUrl);
adminUrl.pathname = "/postgres";
const targetUrl = new URL(baseDbUrl);
targetUrl.pathname = `/${dbName}`;

const adminDb = createDatabase(adminUrl.toString(), 2);
await adminDb.unsafe(`CREATE DATABASE ${dbName}`);
const db = createDatabase(targetUrl.toString(), 32);
await migrate(db);

const mock = createMockApp("bench-ctrl");
const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });

const config: GatewayConfig = {
  mode: "gateway",
  host: "127.0.0.1",
  port: 0,
  internalHost: "127.0.0.1",
  internalPort: 0,
  databaseUrl: targetUrl.toString(),
  dbMaxConnections: 32,
  keyPepper: "bench-pepper-0123456789abcdef0123",
  encryptionKey: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  adminToken: "bench-admin-token-0123456789abcdef",
  allowPrivateUpstreams: true,
  privateUpstreamCidrs: ["127.0.0.0/8"],
  trustedProxyCidrs: [],
  maxInFlight: 2000,
  maxBodyBytes: 2 * 1024 * 1024,
  holdTtlMs: 60_000,
  attemptTimeoutMs: 15_000,
  snapshotPollMs: 0,
  microBatchWindowMs: 3,
  microBatchMaxSize: 64,
};

const runtime = createGatewayRuntime({ config, db });
await runtime.start();
const publicServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: runtime.publicApp.fetch });
const internalServer = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  fetch: runtime.internalApp.fetch,
});

const adminHeaders = {
  authorization: `Bearer ${config.adminToken}`,
  "content-type": "application/json",
};
async function postInternal(method: string, path: string, body?: unknown) {
  const res = await fetch(`http://127.0.0.1:${internalServer.port}${path}`, {
    method,
    headers: adminHeaders,
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
  return (await res.json()) as Record<string, unknown>;
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p));
  return Number(sorted[idx]!.toFixed(2));
}

async function walLsnBytes(): Promise<bigint> {
  const rows = await db`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint::text AS bytes`;
  return BigInt(String(rows[0]!.bytes));
}

async function runPhase(
  label: string,
  keys: string[],
  concurrency: number,
  totalRequests: number,
  stream: boolean,
) {
  let completed = 0;
  let errors = 0;
  const latencies: number[] = [];
  const walBefore = await walLsnBytes();
  const started = performance.now();
  let nextIdx = 0;

  async function worker() {
    while (true) {
      const i = nextIdx++;
      if (i >= totalRequests) break;
      const key = keys[i % keys.length]!;
      const t0 = performance.now();
      try {
        const res = await fetch(`http://127.0.0.1:${publicServer.port}/v1/chat/completions`, {
          method: "POST",
          headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "bench-gpt",
            stream,
            messages: [{ role: "user", content: "hi" }],
            max_tokens: 32,
          }),
        });
        await res.text();
        if (res.status === 200) completed++;
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
  latencies.sort((a, b) => a - b);
  return {
    phase: label,
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
  };
}

try {
  const acc = await postInternal("PUT", "/internal/v1/upstream-accounts", { name: "bench-acc" });
  await postInternal("POST", `/internal/v1/upstream-accounts/${acc.id}/recharges`, {
    currency: "USD",
    external_ref: "bench-sup-1",
    amount_atoms: "1000000000000",
  });
  const ch = await postInternal("PUT", "/internal/v1/channels", {
    account_id: acc.id,
    name: "bench-ch",
    kind: "openai",
    base_url: `http://127.0.0.1:${mockServer.port}/openai`,
    api_key: "sk-bench",
    rpm: 10_000_000,
    concurrency: 10_000,
  });
  await postInternal("PUT", "/internal/v1/models", {
    name: "bench-gpt",
    operations: ["chat"],
    max_output_tokens: 64,
  });
  const custPrice = await postInternal("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    currency: "USD",
    rates: {
      input_tokens: { numerator: "10", denominator: "1" },
      cached_input_tokens: { numerator: "5", denominator: "1" },
      output_tokens: { numerator: "20", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "64" },
  });
  const supPrice = await postInternal("POST", "/internal/v1/prices", {
    side: "SUPPLIER",
    currency: "USD",
    rates: {
      input_tokens: { numerator: "2", denominator: "1" },
      cached_input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "4", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "64" },
  });
  await postInternal("PUT", "/internal/v1/routes", {
    model_name: "bench-gpt",
    channel_id: ch.id,
    upstream_model: "bench-up",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });

  const multiKeys: string[] = [];
  for (let i = 0; i < 32; i++) {
    const p = await postInternal("PUT", "/internal/v1/projects", {
      name: `bench-p-${i}`,
      currency: "USD",
      rpm: 10_000_000,
      tpm: 1_000_000_000,
      concurrency: 1000,
    });
    const k = await postInternal("PUT", "/internal/v1/api-keys", {
      project_id: p.id,
      name: `k-${i}`,
    });
    await postInternal("POST", `/internal/v1/wallets/${p.id}/credits`, {
      currency: "USD",
      external_ref: `bench-c-${i}`,
      amount_atoms: "100000000000",
      kind: "CASH",
    });
    multiKeys.push(String(k.key));
  }

  const hotKey = multiKeys[0]!;
  await runPhase("warmup", multiKeys, 16, 100, false);

  const results = [
    await runPhase("short_chat_json_32_wallets", multiKeys, 64, 2000, false),
    await runPhase("short_chat_sse_32_wallets", multiKeys, 64, 2000, true),
    await runPhase("single_hot_wallet_100c", [hotKey], 100, 1000, false),
  ];

  const pgSettings = await db`
    SELECT name, setting FROM pg_settings
    WHERE name IN ('fsync', 'synchronous_commit', 'full_page_writes')
    ORDER BY name
  `;
  const audit = await postInternal("GET", "/internal/v1/audit/conservation");

  console.log(
    JSON.stringify(
      {
        bun_version: Bun.version,
        pg_durability: Object.fromEntries(
          pgSettings.map((r: Record<string, unknown>) => [String(r.name), String(r.setting)]),
        ),
        results,
        conservation: audit,
      },
      null,
      2,
    ),
  );
  if (!audit.ok || audit.open_holds !== 0) {
    throw new Error("Benchmark conservation check failed");
  }
} finally {
  await publicServer.stop(true);
  await internalServer.stop(true);
  await runtime.close();
  await mockServer.stop(true);
  await db.close();
  await adminDb.unsafe(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${dbName}' AND pid <> pg_backend_pid()`,
  );
  await adminDb.unsafe(`DROP DATABASE IF EXISTS ${dbName}`);
  await adminDb.close();
}
