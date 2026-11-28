import { localInternalCredentials } from "./local-internal-auth.ts";
import { createDatabase, migrate } from "@gateway/ledger";
import { createGatewayRuntime, type GatewayConfig } from "@gateway/gateway";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const dbName = `gateway_v4_pricing_bench_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const adminUrl = new URL(baseDbUrl);
adminUrl.pathname = "/postgres";
const targetUrl = new URL(baseDbUrl);
targetUrl.pathname = `/${dbName}`;

const adminDb = createDatabase(adminUrl.toString(), 2);
await adminDb.unsafe(`CREATE DATABASE ${dbName}`);
const db = createDatabase(targetUrl.toString(), 32);
await migrate(db);

const mockServer = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  async fetch(req) {
    const body = (await req.json()) as { messages?: { content?: string }[] };
    const promptLen = body.messages?.[0]?.content?.length ?? 4;
    const isLong = promptLen > 1000;
    return Response.json({
      id: "chatcmpl-bench",
      object: "chat.completion",
      model: "gpt-pricing-bench",
      choices: [{ index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" }],
      usage: {
        prompt_tokens: isLong ? 220_000 : 1200,
        completion_tokens: 180,
        completion_tokens_details: { reasoning_tokens: 40 },
      },
    });
  },
});

const operatorToken = "bench-pricing-token-0123456789abcdef01234567";
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
  internalCredentials: localInternalCredentials(operatorToken),
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
  authorization: `Bearer ${operatorToken}`,
  "content-type": "application/json",
};

async function callInternal(method: string, path: string, body?: unknown) {
  const res = await fetch(`http://127.0.0.1:${internalServer.port}${path}`, {
    method,
    headers: adminHeaders,
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
  const data = (await res.json()) as Record<string, unknown>;
  if (!res.ok) throw new Error(`${method} ${path} failed (${res.status}): ${JSON.stringify(data)}`);
  return data;
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p));
  return Number(sorted[idx]!.toFixed(2));
}

try {
  const acc = await callInternal("PUT", "/internal/v1/upstream-accounts", {
    name: "pricing-bench-acc",
  });
  await callInternal("POST", `/internal/v1/upstream-accounts/${acc.id}/recharges`, {
    currency: "USD",
    external_ref: "pricing-bench-sup-1",
    amount_atoms: "1000000000000000",
  });
  const ch = await callInternal("PUT", "/internal/v1/channels", {
    account_id: acc.id,
    name: "pricing-bench-ch",
    kind: "openai",
    base_url: mockServer.url.origin,
    api_key: "sk-bench",
    rpm: 10_000_000,
    concurrency: 10_000,
  });
  await callInternal("PUT", "/internal/v1/models", {
    name: "gpt-pricing-bench",
    operations: ["chat"],
    max_output_tokens: 512,
  });

  const custPrice = await callInternal("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    currency: "USD",
    rates: {},
    ceilings: { input_tokens: "300000", output_tokens: "512" },
    pricing_spec: {
      cliff: {
        driver: ["input_tokens"],
        tiers: [
          {
            min_driver: "0",
            rates: {
              input_tokens: { numerator: "3", denominator: "1" },
              output_tokens: { numerator: "12", denominator: "1" },
            },
            reasoning_rate: { numerator: "24", denominator: "1" },
          },
          {
            min_driver: "200000",
            rates: {
              input_tokens: { numerator: "6", denominator: "1" },
              output_tokens: { numerator: "18", denominator: "1" },
            },
            reasoning_rate: { numerator: "36", denominator: "1" },
          },
        ],
      },
    },
  });
  const supPrice = await callInternal("POST", "/internal/v1/prices", {
    side: "SUPPLIER",
    currency: "USD",
    rates: {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "4", denominator: "1" },
    },
    ceilings: { input_tokens: "300000", output_tokens: "512" },
  });
  await callInternal("PUT", "/internal/v1/routes", {
    model_name: "gpt-pricing-bench",
    channel_id: ch.id,
    upstream_model: "gpt-pricing-bench",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });
  await callInternal("POST", "/internal/v1/fx-rates", {
    base_currency: "USD",
    quote_currency: "CNY",
    rate_numerator: "725",
    rate_denominator: "100",
    effective_from: "2020-01-01T00:00:00.000Z",
  });
  await callInternal("PUT", "/internal/v1/pricing-modifiers", {
    layer: "PRODUCT",
    kind: "MULTIPLIER",
    stacking: "MULTIPLY",
    priority: 10,
    time_windows: [
      { timezone: "Asia/Shanghai", start_minute: 0, end_minute: 720 },
      { timezone: "Asia/Shanghai", start_minute: 720, end_minute: 0 },
    ],
    match: { side: "CUSTOMER", model: "gpt-pricing-bench", service_tier: "priority" },
    multiplier_numerator: "150",
    multiplier_denominator: "100",
  });
  await callInternal("PUT", "/internal/v1/pricing-modifiers", {
    layer: "COMMERCIAL",
    kind: "MULTIPLIER",
    stacking: "BEST_FOR_CUSTOMER",
    priority: 10,
    match: { side: "CUSTOMER", model: "gpt-pricing-bench", customer_group_id: "enterprise-bench" },
    multiplier_numerator: "85",
    multiplier_denominator: "100",
  });

  const keys: string[] = [];
  for (let i = 0; i < 8; i++) {
    const ba = await callInternal("PUT", "/internal/v1/billing-accounts", {
      name: `pricing-bench-acct-${i}`,
      customer_group_id: "enterprise-bench",
      currency: "CNY",
      rpm: 10_000_000,
      tpm: 10_000_000_000,
      concurrency: 1000,
    });
    await callInternal("POST", `/internal/v1/wallets/${ba.id}/credits`, {
      currency: "CNY",
      external_ref: `pricing-bench-credit-${i}`,
      amount_atoms: "1000000000000000",
      kind: "CASH",
    });
    const k = await callInternal("PUT", "/internal/v1/api-keys", {
      billing_account_id: ba.id,
      name: `pricing-bench-key-${i}`,
    });
    keys.push(String(k.key));
  }

  const runLoad = async (label: string, concurrency: number, totalRequests: number) => {
    let completed = 0;
    let errors = 0;
    const latencies: number[] = [];
    let nextIdx = 0;
    const started = performance.now();
    async function worker() {
      while (true) {
        const idx = nextIdx++;
        if (idx >= totalRequests) break;
        const key = keys[idx % keys.length]!;
        const t0 = performance.now();
        try {
          const res = await fetch(`http://127.0.0.1:${publicServer.port}/v1/chat/completions`, {
            method: "POST",
            headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
            body: JSON.stringify({
              model: "gpt-pricing-bench",
              service_tier: "priority",
              messages: [{ role: "user", content: "standardized pricing bench" }],
              max_tokens: 256,
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
    };
  };

  await runLoad("warmup", 16, 200);
  const phase1 = await runLoad("standardized_pricing_e2e_c64", 64, 2000);
  const phase2 = await runLoad("standardized_pricing_e2e_c128", 128, 3000);

  const [snapshotCountRow] = await db<{ total: number; with_snapshot: number }[]>`
    SELECT count(*)::int AS total, count(pricing_snapshot)::int AS with_snapshot
    FROM public.billing_charges
  `;
  const audit = await runtime.ledger.auditConservation();

  console.log(
    JSON.stringify(
      {
        phases: [phase1, phase2],
        charges: snapshotCountRow,
        conservation: audit,
      },
      null,
      2,
    ),
  );
  if (
    phase1.errors > 0 ||
    phase2.errors > 0 ||
    !audit.ok ||
    snapshotCountRow!.total !== snapshotCountRow!.with_snapshot
  ) {
    process.exitCode = 1;
  }
} finally {
  publicServer.stop(true);
  internalServer.stop(true);
  mockServer.stop(true);
  await runtime.close();
  await db.close({ timeout: 1 });
  await adminDb.unsafe(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`);
  await adminDb.close({ timeout: 1 });
}
