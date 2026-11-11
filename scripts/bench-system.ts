/** Isolated, paid-path benchmark. Creates only its own disposable fixture DB and Redis namespace. */
import { mkdir } from "node:fs/promises";
import { cleanupCapacityNamespaces } from "./capacity-cleanup.ts";
import {
  auditCapacityIdentities,
  createIdentityWriter,
  capacityStorageBudget,
} from "./capacity-evidence.ts";
import { passesCapacityTarget, passesOnlineCapacity } from "./benchmark-lib.ts";
import { createDatabase, migrate, Store, FundsStore } from "@gateway/storage";
import { createKey, verifier, encrypt } from "@gateway/kernel";
import { RedisClient } from "bun";
const args = Bun.argv.slice(2);
const option = (name: string, fallback: string) => {
  const at = args.indexOf(name);
  return at >= 0 ? (args[at + 1] ?? fallback) : fallback;
};
const targetRps = Number(option("--target-rps", "0"));
if (!Number.isSafeInteger(targetRps) || targetRps < 0 || targetRps > 100000)
  throw new Error("Invalid target RPS");
const seconds = Number(option("--seconds", "10"));
const instances = Number(option("--instances", "1"));
const wallets = Number(option("--wallets", "1"));
const concurrency = Number(option("--concurrency", "64"));
const pool = Number(option("--pool", "16"));
const stream = args.includes("--stream");
const observe = args.includes("--observe");
const requireOnlineCapacity = args.includes("--online-capacity");
if (requireOnlineCapacity && (seconds < 60 || seconds % 60 !== 0))
  throw new Error("Online capacity acceptance requires complete 60-second windows");
const maxP99Ms = Number(option("--max-p99-ms", "1500"));
if (!Number.isFinite(maxP99Ms) || maxP99Ms <= 0) throw new Error("Invalid P99 budget");
const lz4 = args.includes("--wal-lz4");
const providerKind = option("--provider", "openai");
if (providerKind !== "openai" && providerKind !== "anthropic")
  throw new Error("Unsupported fixture provider");
const scenario = JSON.parse(option("--scenario", "{}"));
const upstreamMode = option("--upstream", "standard");
if (!["standard", "precomputed"].includes(upstreamMode))
  throw new Error("Invalid benchmark upstream");
if (
  upstreamMode === "precomputed" &&
  (stream || providerKind !== "openai" || Object.keys(scenario).length)
)
  throw new Error("Precomputed upstream requires plain OpenAI JSON");
const upstreamStats = async (action = "stats") => {
  const response = await fetch(`http://127.0.0.1:19088/__control/${action}`, {
    method: action === "stats" ? "GET" : "POST",
    headers: { authorization: `Bearer control-${id}`, "content-type": "application/json" },
    ...(action === "scenarios" ? { body: JSON.stringify({ model: "demo", scenario }) } : {}),
    signal: AbortSignal.timeout(2000),
  });
  if (!response.ok) throw new Error("Mock telemetry unavailable");
  return response.json();
};
const billingMode = option("--billing-mode", "paid");
if (!["paid", "free"].includes(billingMode)) throw new Error("Invalid billing mode");
const expectedCharge = billingMode === "paid" ? "59" : "0";
const workerMode = option("--worker-mode", "live");
if (!["live", "deferred"].includes(workerMode)) throw new Error("Invalid worker mode");
const drainSeconds = Number(option("--drain-seconds", "60"));
if (!Number.isSafeInteger(drainSeconds) || drainSeconds < 1 || drainSeconds > 600)
  throw new Error("Invalid drain budget");
const consumerMode = option("--consumer-mode", "live");
if (!["live", "none"].includes(consumerMode)) throw new Error("Invalid consumer mode");
if (requireOnlineCapacity && (workerMode !== "live" || consumerMode !== "live"))
  throw new Error("Online capacity requires live settlement and consumer ACK");
const hotShare = Number(option("--hot-share", "0"));
if (!Number.isFinite(hotShare) || hotShare < 0 || hotShare > 1)
  throw new Error("Invalid hot wallet share");
if (
  ![seconds, instances, wallets, concurrency, pool].every(
    (v) => Number.isSafeInteger(v) && v > 0,
  ) ||
  seconds > 3600 ||
  instances > 8 ||
  wallets > 1000 ||
  concurrency > 8192 ||
  instances * pool + 16 > 90
)
  throw new Error("Invalid capacity bounds");
const base = process.env.DATABASE_URL;
if (!base || !process.env.REDIS_URL || !process.env.QUEUE_REDIS_URL)
  throw new Error("Local PG, admission Redis and durable queue Redis URLs are required");
if (requireOnlineCapacity && Number(process.env.QUEUE_REQUIRED_REPLICAS ?? "0") < 1)
  throw new Error("Online capacity requires an acknowledged AOF replica");
const storageBudget = await capacityStorageBudget(
  process.cwd(),
  seconds * Number(option("--rate", String(targetRps || 6000))),
);
if (requireOnlineCapacity && !storageBudget.sufficient)
  throw new Error(
    `Capacity storage budget insufficient: ${storageBudget.available} bytes available, ${storageBudget.estimated + storageBudget.reserve} required; no fixture created`,
  );
const id = crypto.randomUUID().replaceAll("-", "");
const database = `gateway_bench_${id}`;
const namespace = `bench:${id}`;
const directory = `.local/phase2/bench-${id}`;
await mkdir(directory, { recursive: true });
const owner = createDatabase(base, 2);
await owner.unsafe(`CREATE DATABASE ${database}`);
if (lz4) await owner.unsafe(`ALTER DATABASE ${database} SET wal_compression=lz4`);
const url = new URL(base);
url.pathname = `/${database}`;
const db = createDatabase(url.toString(), 4);
const processes: ReturnType<typeof Bun.spawn>[] = [];
let telemetryTimer: ReturnType<typeof setInterval> | undefined;
let sampling: Promise<void> | undefined;
let consuming = false;
let consume: Promise<void> | undefined;
let ackWriter: Awaited<ReturnType<typeof createIdentityWriter>> | undefined;
let loadProcess: ReturnType<typeof Bun.spawn> | undefined;
let lastStorageCheck = 0;
let telemetryErrors = 0;
const env = {
  ...process.env,
  NODE_ENV: "test",
  QUEUE_REQUIRED_REPLICAS: process.env.QUEUE_REQUIRED_REPLICAS ?? "0",
  DATABASE_URL: url.toString(),
  REDIS_URL: process.env.REDIS_URL!,
  REDIS_EXTRA_URLS: process.env.REDIS_EXTRA_URLS ?? "",
  REDIS_NAMESPACE: namespace,
  ADMIN_KEY: `bench-${id}`,
  ENCRYPTION_KEY: "22".repeat(32),
  KEY_PEPPER: `pepper-${id}`,
  ALLOW_PRIVATE_UPSTREAM: "true",
  HOST: "127.0.0.1",
  TOTAL_TIMEOUT_MS: "30000",
  DB_POOL_SIZE: String(pool),
  MAX_INFLIGHT: "10000",
  QUEUE_BATCH_SIZE: option("--batch", "64"),
  QUEUE_BATCH_DELAY_MS: process.env.QUEUE_BATCH_DELAY_MS ?? "1",
  QUEUE_MAX_PENDING_JOBS: process.env.QUEUE_MAX_PENDING_JOBS ?? "1000",
  BILLING_CONCURRENCY: option("--worker-concurrency", "16"),
  WORKER_PORT: "18099",
  MOCK_CONTROL_TOKEN: `control-${id}`,
};
function spawn(entry: string, extra: Record<string, string>, label: string) {
  const output = Bun.file(`${directory}/${label}.log`);
  const child = Bun.spawn([process.execPath, entry], {
    env: { ...env, ...extra },
    stdout: output,
    stderr: output,
  });
  processes.push(child);
  return child;
}
const wait = async (port: number) => {
  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) {
    try {
      if ((await fetch(`http://127.0.0.1:${port}/${port === 19088 ? "healthz" : "livez"}`)).ok)
        return;
    } catch {}
    await Bun.sleep(25);
  }
  throw new Error(`Process ${port} did not become ready`);
};
try {
  const redisSettings: Record<string, unknown> = {};
  for (const [name, redisURL] of [
    ["admission", process.env.REDIS_URL],
    ...(process.env.REDIS_EXTRA_URLS ?? "")
      .split(",")
      .map((url) => url.trim())
      .filter(Boolean)
      .map((url, index) => [`admission-${index + 2}`, url] as const),
    ["evidence", process.env.QUEUE_REDIS_URL],
  ] as const) {
    const redis = new RedisClient(redisURL);
    try {
      await redis.connect();
      redisSettings[name] = await redis.send("CONFIG", [
        "GET",
        "appendonly",
        "appendfsync",
        "no-appendfsync-on-rewrite",
        "aof-load-truncated",
        "maxmemory-policy",
        "auto-aof-rewrite-min-size",
      ]);
    } finally {
      redis.close();
    }
  }
  await migrate(db);
  const databaseSettings = await db<
    { name: string; setting: string; unit: string | null }[]
  >`SELECT name,setting,unit FROM pg_settings WHERE name IN ('server_version','shared_buffers','effective_cache_size','work_mem','max_wal_size','checkpoint_timeout','wal_compression','fsync','full_page_writes','synchronous_commit','random_page_cost','autovacuum_naptime','track_functions','track_wal_io_timing','jit') ORDER BY name`;
  for (const name of ["fsync", "full_page_writes", "synchronous_commit"])
    if (!databaseSettings.some((row) => row.name === name && row.setting === "on"))
      throw new Error(`Financial capacity measurement requires ${name}=on`);
  if (args.includes("--profile")) {
    await db`CREATE EXTENSION pg_stat_statements`;
    await db.unsafe(`ALTER DATABASE ${database} SET pg_stat_statements.track='all'`);
    await db.unsafe(`ALTER DATABASE ${database} SET track_functions='all'`);
    await db.unsafe(`ALTER DATABASE ${database} SET track_wal_io_timing=on`);
  }
  const walBefore = (
    await db`SELECT wal_records::text,wal_fpi::text,wal_bytes::text,wal_sync,wal_write,wal_sync_time,wal_write_time FROM pg_stat_wal`
  )[0];
  const checkpoint = async () =>
    (
      await db`SELECT num_timed,num_requested,restartpoints_timed,write_time,sync_time,buffers_written FROM pg_stat_checkpointer`
    )[0];
  const checkpointBefore = await checkpoint();
  const admissionInfo = async () => {
    const redis = new RedisClient(env.REDIS_URL);
    try {
      await redis.connect();
      return String(await redis.send("INFO", ["persistence"]));
    } finally {
      redis.close();
    }
  };
  const admissionBefore = await admissionInfo();
  const store = new Store(db);
  const funds = new FundsStore(db);
  const provider = crypto.randomUUID();
  await store.createProvider(
    provider,
    {
      name: "benchmark",
      kind: providerKind,
      base_url: `http://127.0.0.1:19088/${providerKind}/v1`,
      concurrency: 100000,
      rpm: 10000000,
    },
    encrypt("benchmark-key", env.ENCRYPTION_KEY, provider),
  );
  await store.createModel({ name: "bench", operations: ["chat"], max_output_tokens: 4096 });
  await store.createRoute("bench", {
    provider_id: provider,
    upstream_model: "demo",
    operations: ["chat"],
    priority: 10,
    weight: 1,
    pricing: {
      currency: "USD",
      rates: {
        input_tokens: { numerator: "3", denominator: "1" },
        cached_input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "7", denominator: "1" },
      },
    },
  });
  await funds.setTariff({
    model: "bench",
    operation: "chat",
    pricing: {
      currency: "USD",
      rates: {
        input_tokens: { numerator: billingMode === "paid" ? "3" : "0", denominator: "1" },
        cached_input_tokens: { numerator: billingMode === "paid" ? "1" : "0", denominator: "1" },
        output_tokens: { numerator: billingMode === "paid" ? "7" : "0", denominator: "1" },
      },
    },
    ceilings: { input_tokens: "4096", cached_input_tokens: "4096", output_tokens: "4096" },
    basis: "PROVIDER_USAGE",
  });
  const keys: string[] = [];
  const projects: string[] = [];
  for (let i = 0; i < wallets; i++) {
    const project = await store.createProject({
      name: `wallet-${i}`,
      rpm: 10000000,
      tpm: 1000000000,
      concurrency: 100000,
    });
    projects.push(project);
    await funds.credit(project, {
      currency: "USD",
      external_ref: "benchmark-credit",
      amount_atoms: "1000000000000000000",
      kind: "CASH",
      priority: 100,
      models: [],
      operations: [],
    });
    const key = createKey();
    keys.push(key);
    await store.createCredential(
      project,
      { name: "benchmark", models: [], operations: ["chat"] },
      verifier(key, env.KEY_PEPPER),
      key.slice(0, 12),
    );
  }
  // Do not ANALYZE empty hot tables: caching zero-row FK plans would distort their initial growth.
  for (const table of [
    "projects",
    "api_keys",
    "providers",
    "models",
    "routes",
    "cost_versions",
    "billing_prices",
    "billing_tariffs",
    "wallets",
    "wallet_lots",
  ])
    await db.unsafe(`ANALYZE ${table}`);
  const consumer =
    consumerMode === "live" ? await store.createConsumer(`bench-${id}`, false) : undefined;
  const mock = spawn(
    upstreamMode === "precomputed" ? "scripts/bench-upstream.ts" : "dist/mock-providers/main.js",
    { MOCK_PORT: "19088" },
    "mock",
  );
  await wait(19088);
  if (Object.keys(scenario).length) await upstreamStats("scenarios");
  const ports = Array.from({ length: instances }, (_, i) => 18081 + i);
  for (const port of ports)
    spawn("dist/gateway/main.js", { PORT: String(port) }, `gateway-${port}`);
  for (const port of ports) await wait(port);
  let workerStarted = false;
  const startWorker = async () => {
    spawn("dist/gateway/worker-main.js", { DB_POOL_SIZE: "16" }, "worker");
    await wait(18099);
    workerStarted = true;
  };
  if (workerMode === "live") await startWorker();
  const timeline: {
    at: number;
    queue: Record<string, number>;
    worker_metrics: string;
    lock_waiters: number;
    active_queries: number;
    upstream_active: number;
    completed_at?: number;
    external_acked?: number;
    receipt_confirmed_acks?: number;
    waits: { wait_event_type: string; wait_event: string; count: number }[] | undefined;
    checkpoint?: unknown;
  }[] = [];
  const samplingStart = Date.now();
  let delivered = 0;
  let receiptConfirmedAcks = 0;
  const ackPath = `${directory}/ack.identities.tsv`;
  ackWriter = requireOnlineCapacity ? await createIdentityWriter(ackPath) : undefined;
  let unconfirmedReceipts = 0;
  telemetryTimer = setInterval(
    () => {
      if (sampling) return;
      sampling = (async () => {
        if (requireOnlineCapacity && Date.now() - lastStorageCheck >= 1000) {
          lastStorageCheck = Date.now();
          const storage = await capacityStorageBudget(process.cwd(), 0);
          if (storage.available < storage.reserve) {
            await Bun.write(
              `${directory}/abort.json`,
              JSON.stringify({
                reason: "storage_reserve_exhausted",
                storage,
                at: new Date().toISOString(),
              }),
            );
            loadProcess?.kill("SIGTERM");
            throw new Error("Capacity disk reserve exhausted");
          }
        }
        const headers = { authorization: `Bearer ${env.ADMIN_KEY}` };
        const [queue, worker, upstream] = await Promise.all([
          fetch(`http://127.0.0.1:${ports[0]}/admin/queue`, {
            headers,
            signal: AbortSignal.timeout(2000),
          }),
          workerStarted
            ? fetch("http://127.0.0.1:18099/metrics", {
                headers,
                signal: AbortSignal.timeout(2000),
              })
            : Promise.resolve(new Response("")),
          upstreamStats(),
        ]);
        if (!queue.ok || !worker.ok) throw new Error("Capacity telemetry unavailable");
        const activity = (
          await db`SELECT count(*) FILTER(WHERE wait_event_type='Lock')::int AS locks,count(*) FILTER(WHERE state='active')::int AS active FROM pg_stat_activity WHERE datname=current_database()`
        )[0];
        const waits = observe
          ? await db<
              { wait_event_type: string; wait_event: string; count: number }[]
            >`SELECT coalesce(wait_event_type,'CPU') AS wait_event_type,coalesce(wait_event,'CPU') AS wait_event,count(*)::int FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND pid<>pg_backend_pid() GROUP BY 1,2 ORDER BY 3 DESC`
          : undefined;
        const workerMetrics = await worker.text();
        const sampleTime = Date.now();
        const externalAcked = delivered;
        const receiptSnapshot = receiptConfirmedAcks;
        timeline.push({
          waits,
          checkpoint: observe ? await checkpoint() : undefined,
          completed_at: sampleTime,
          external_acked: externalAcked,
          receipt_confirmed_acks: receiptSnapshot,
          lock_waiters: activity.locks,
          active_queries: activity.active,
          upstream_active: upstream.active,
          at: Date.now() - samplingStart,
          queue: (await queue.json()) as Record<string, number>,
          worker_metrics: workerMetrics,
        });
      })()
        .catch(() => {
          telemetryErrors++;
        })
        .finally(() => {
          sampling = undefined;
        });
    },
    requireOnlineCapacity ? 250 : 1000,
  );
  consuming = true;
  consume = (async () => {
    while (consuming && consumer) {
      try {
        const page = await store.claimEvents(consumer, 500);
        if (page.events.length) {
          const receipts = requireOnlineCapacity
            ? (
                await db<{ n: number }[]>`SELECT count(*)::int AS n
            FROM unnest(${page.events.map((event) => event.request_id)}::uuid[],${page.events.map((event) => event.id)}::uuid[]) expected(request_id,evidence_id)
            JOIN billing_charges c ON c.request_id=expected.request_id AND c.evidence_id=expected.evidence_id
            WHERE c.amount_atoms=${expectedCharge}::numeric AND c.funded_atoms=${expectedCharge}::numeric
            AND c.currency='USD' AND c.debt_atoms=0 AND c.refunded_atoms=0`
              )[0]!.n
            : page.events.length;
          const numericEvidence =
            !requireOnlineCapacity ||
            page.events.every(
              (event) =>
                event.finality === "FINAL" &&
                event.currency === "USD" &&
                event.cost_complete &&
                event.cost_atoms === "59" &&
                event.usage.input_tokens === "7" &&
                event.usage.cached_input_tokens === "3" &&
                event.usage.output_tokens === "5",
            );
          const acknowledged = await store.ackEvents(
            consumer,
            page.lease_token,
            page.events.map((event) => event.id),
          );
          delivered += acknowledged;
          if (requireOnlineCapacity && receipts === page.events.length && numericEvidence) {
            receiptConfirmedAcks += acknowledged;
            if (acknowledged === page.events.length) {
              await ackWriter!.write(
                page.events.map((event) => event.request_id),
                Date.now(),
              );
            }
          } else if (requireOnlineCapacity) unconfirmedReceipts += acknowledged;
        } else await Bun.sleep(10);
      } catch {
        await Bun.sleep(50);
      }
    }
  })();
  const reportPath = `${directory}/http.json`;
  const load = Bun.spawn(
    [
      process.execPath,
      "scripts/bench.ts",
      "--url",
      ports.map((p) => `http://127.0.0.1:${p}`).join(","),
      "--model",
      "bench",
      "--seconds",
      String(seconds),
      "--concurrency",
      String(concurrency),
      "--warmup-concurrency",
      option("--warmup-concurrency", String(Math.min(256, concurrency))),
      "--metrics",
      ...(requireOnlineCapacity ? ["--record-request-ids"] : []),
      "--output",
      reportPath,
      "--hot-share",
      String(hotShare),
      ...(stream ? ["--stream"] : []),
      ...(args.includes("--rate") ? ["--rate", option("--rate", "0")] : []),
    ],
    {
      env: { ...env, BENCH_API_KEYS: keys.join(",") },
      stdout: Bun.file(`${directory}/load.log`),
      stderr: Bun.file(`${directory}/load.log`),
    },
  );
  loadProcess = load;
  processes.push(load);
  const exit = await load.exited;
  if (!(await Bun.file(reportPath).exists()))
    throw new Error(
      `Load generator exited ${exit} without a report; inspect ${directory}/load.log`,
    );
  const drainStart = Date.now();
  if (workerMode === "deferred") await startWorker();
  let pending = 1;
  while (Date.now() - drainStart < drainSeconds * 1000) {
    const rows =
      await db`SELECT EXISTS(SELECT 1 FROM billing_holds WHERE state NOT IN ('SETTLED','RELEASED')) AS waiting`;
    pending = rows[0].waiting ? 1 : 0;
    if (!pending) break;
    await Bun.sleep(100);
  }
  while (Date.now() - drainStart < drainSeconds * 1000) {
    const rows =
      await db`SELECT EXISTS(SELECT 1 FROM event_deliveries WHERE state<>'ACKED') AS waiting`;
    if (!rows[0].waiting) break;
    await Bun.sleep(25);
  }
  consuming = false;
  await consume;
  await ackWriter?.close();
  let queueStats: Record<string, number> = {};
  do {
    const response = await fetch(`http://127.0.0.1:${ports[0]}/admin/queue`, {
      headers: { authorization: `Bearer ${env.ADMIN_KEY}` },
    });
    if (!response.ok) throw new Error("Queue audit unavailable");
    queueStats = (await response.json()) as Record<string, number>;
    if (Object.values(queueStats).every((n) => n === 0)) break;
    await Bun.sleep(25);
  } while (Date.now() - drainStart < drainSeconds * 1000);
  const serviceDrainedAt = Date.now();
  clearInterval(telemetryTimer);
  pending = Number(
    (
      await db`SELECT count(*)::int AS n FROM billing_holds WHERE state NOT IN ('SETTLED','RELEASED')`
    )[0].n,
  );
  const accounting = await db.begin(async (audit) => {
    await audit`SET LOCAL statement_timeout=120000`;
    return audit`SELECT (SELECT count(*)::int FROM gateway_requests) AS requests,(SELECT count(*)::int FROM attempts) AS attempts,(SELECT count(*)::int FROM usage_events WHERE finality='FINAL' AND usage->>'input_tokens'='7' AND usage->>'cached_input_tokens'='3' AND usage->>'output_tokens'='5' AND cost_atoms=59 AND cost_complete) AS precise_usage,(SELECT count(*)::int FROM billing_charges WHERE amount_atoms=${expectedCharge}::numeric AND funded_atoms=${expectedCharge}::numeric AND debt_atoms=0 AND refunded_atoms=0) AS precise_charges,(SELECT count(*)::int FROM billing_holds WHERE state='SETTLED') AS settled_holds,(SELECT sum(reserved_atoms)::text FROM wallets) AS reserved,(SELECT sum(spent_atoms)::text FROM wallets) AS spent,(SELECT sum(debt_atoms)::text FROM wallets) AS debt,(SELECT count(*)::int FROM event_deliveries WHERE state='ACKED') AS acknowledged,(SELECT count(*)::int FROM money_journal WHERE kind='SETTLE') AS settlement_journals,(SELECT count(*)::int FROM money_journal j WHERE (SELECT sum((v->>'atoms')::numeric) FROM jsonb_array_elements(j.lines)v)<>0) AS unbalanced_journals`;
  });
  const check = accounting[0];
  let valid =
    exit === 0 &&
    !pending &&
    check.requests === check.attempts &&
    check.requests === check.precise_usage &&
    check.requests === check.precise_charges &&
    check.requests === check.settled_holds &&
    (consumerMode === "live" ? check.requests : 0) === check.acknowledged &&
    (billingMode === "paid" ? check.requests : 0) === check.settlement_journals &&
    check.reserved === "0" &&
    check.debt === "0" &&
    BigInt(check.spent) === BigInt(check.requests) * BigInt(expectedCharge) &&
    !check.unbalanced_journals;
  const http = await Bun.file(reportPath).json();
  const ownership = (
    await db`SELECT count(*)::int AS mismatches FROM billing_charges c
    JOIN billing_holds h ON h.id=c.hold_id JOIN wallets w ON w.id=h.wallet_id
    JOIN gateway_requests q ON q.id=c.request_id LEFT JOIN usage_event_index e ON e.id=c.evidence_id
    WHERE h.request_id<>q.id OR w.project_id<>q.project_id OR c.currency<>w.currency
    OR (e.id IS NOT NULL AND e.request_id<>q.id)`
  )[0]!.mismatches;
  const walletMismatches = (
    await db`SELECT count(*)::int AS mismatches
    FROM unnest(${projects}::uuid[],${http.wallet_calls}::bigint[]) expected(project_id,calls)
    LEFT JOIN wallets w ON w.project_id=expected.project_id AND w.currency='USD'
    WHERE w.id IS NULL OR w.reserved_atoms<>0 OR w.debt_atoms<>0
    OR w.spent_atoms<>expected.calls*${expectedCharge}::numeric
    OR w.available_atoms<>1000000000000000000::numeric-expected.calls*${expectedCharge}::numeric`
  )[0]!.mismatches;
  valid = valid && ownership === 0 && walletMismatches === 0;
  valid = valid && http.invalid === 0 && !(http.shed ?? 0) && http.completed === http.succeeded;
  if (args.includes("--rate")) {
    const planned = seconds * Number(option("--rate", "0"));
    valid = valid && http.offered === planned && http.completed + http.shed === planned;
  }
  valid = valid && check.requests === http.succeeded + http.warmup_requests;
  const profile =
    await db`SELECT funcname,calls,total_time,self_time FROM pg_stat_user_functions ORDER BY total_time DESC LIMIT 25`;
  valid = valid && Object.values(queueStats).every((n) => n === 0);
  clearInterval(telemetryTimer);
  await sampling;
  valid = valid && telemetryErrors === 0 && timeline.length >= Math.max(1, Math.floor(seconds / 2));
  const windowStart = Date.parse(http.measurement_started_at);
  const windowEnd = windowStart + seconds * 1000;
  const identityAudit = requireOnlineCapacity
    ? await auditCapacityIdentities(http.identity_log, ackPath, windowStart, seconds)
    : undefined;
  valid =
    valid &&
    (!identityAudit ||
      (identityAudit.matchedRequests === http.succeeded &&
        identityAudit.httpOnly === 0 &&
        identityAudit.ackOnly === http.warmup_requests));
  const withinWindow = timeline
    .filter((sample) => (sample.completed_at ?? Infinity) <= windowEnd)
    .at(-1);
  const settledByWindow =
    Number(
      /^gateway_billing_settled_total (\d+)$/m.exec(withinWindow?.worker_metrics ?? "")?.[1] ?? 0,
    ) - http.warmup_requests;
  const ackedByWindow = (withinWindow?.external_acked ?? 0) - http.warmup_requests;
  const receiptAcksByWindow = (withinWindow?.receipt_confirmed_acks ?? 0) - http.warmup_requests;
  valid =
    valid &&
    (!requireOnlineCapacity ||
      (unconfirmedReceipts === 0 &&
        receiptConfirmedAcks === (consumerMode === "live" ? check.requests : 0)));
  const onlineCapacity = requireOnlineCapacity
    ? {
        ...identityAudit!,
        maxP99Ms,
        drainAfterWindowMs: Math.max(0, serviceDrainedAt - windowEnd),
        receiptAcksByWindow,
        settledByWindow,
        ackedByWindow,
        sampledBeforeWindowEndMs: windowEnd - (withinWindow?.completed_at ?? 0),
        httpByWindow: http.minute_windows?.["0"]?.succeeded ?? 0,
      }
    : undefined;
  const result = {
    storageBudget,
    ownershipMismatches: ownership,
    walletMismatches,
    receiptConfirmedAcks,
    unconfirmedReceipts,
    onlineCapacity,
    databaseSettings,
    redisSettings,
    queue: {
      batchSize: Number(env.QUEUE_BATCH_SIZE),
      batchDelayMs: Number(env.QUEUE_BATCH_DELAY_MS ?? 1),
      maxPendingJobs: Number(env.QUEUE_MAX_PENDING_JOBS ?? 1000),
      requiredReplicas: Number(env.QUEUE_REQUIRED_REPLICAS ?? 0),
      workerConcurrency: Number(env.BILLING_CONCURRENCY),
    },
    telemetryErrors,
    timeline,
    queueStats,
    admissionBefore,
    admissionAfter: await admissionInfo(),
    checkpointBefore,
    checkpointAfter: await checkpoint(),
    walBefore,
    walAfter: (
      await db`SELECT wal_records::text,wal_fpi::text,wal_bytes::text,wal_sync,wal_write,wal_sync_time,wal_write_time FROM pg_stat_wal`
    )[0],
    profile,
    attemptOutcomes:
      await db`SELECT state,error_code,count(*)::int AS count FROM attempts GROUP BY state,error_code ORDER BY count(*) DESC`,
    statements: args.includes("--profile")
      ? await db`SELECT query,calls::text,round(total_exec_time::numeric,2)::text AS total_ms,round(mean_exec_time::numeric,3)::text AS mean_ms,rows::text,shared_blks_hit::text,shared_blks_read::text,wal_bytes::text FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40`
      : undefined,
    relations:
      await db`SELECT relname,n_tup_ins::text,n_tup_upd::text,n_tup_hot_upd::text,n_dead_tup::text FROM pg_stat_user_tables ORDER BY n_tup_upd DESC LIMIT 15`,
    id,
    valid,
    targetRps,
    capacityPassed:
      targetRps > 0
        ? passesCapacityTarget(valid, http, targetRps) &&
          (!onlineCapacity ||
            passesOnlineCapacity(
              valid,
              http,
              targetRps,
              seconds,
              onlineCapacity.minuteCompletions,
              maxP99Ms,
              onlineCapacity.drainAfterWindowMs,
            ))
        : undefined,
    seconds,
    instances,
    wallets,
    hotShare,
    pool,
    concurrency,
    stream,
    billingMode,
    providerKind,
    upstreamMode,
    admissionEndpoints: [
      env.REDIS_URL,
      ...(env.REDIS_EXTRA_URLS ?? "")
        .split(",")
        .map((url) => url.trim())
        .filter(Boolean),
    ].length,
    scenario,
    upstream: await upstreamStats(),
    consumerMode,
    workerMode,
    drainSeconds,
    expectedChargeAtoms: expectedCharge,
    expectedUpstreamCostAtoms: "59",
    http,
    accounting: check,
    pending,
    delivered,
    drain_ms: serviceDrainedAt - drainStart,
    audit_ms: Date.now() - serviceDrainedAt,
    financial_duration_seconds: (serviceDrainedAt - samplingStart) / 1000,
    financial_completed_rps:
      Math.round((check.requests * 100000) / (serviceDrainedAt - samplingStart)) / 100,
    database_bytes: Number(
      (await db`SELECT pg_database_size(current_database())::text AS size`)[0].size,
    ),
  };
  await Bun.write(`${directory}/audit.json`, JSON.stringify(result, null, 2));
  console.log(
    JSON.stringify({
      directory,
      billingMode,
      consumerMode,
      workerMode,
      drainSeconds,
      valid,
      rps: http.successful_rps,
      targetRps,
      capacityPassed: result.capacityPassed,
      financial_completed_rps: result.financial_completed_rps,
      invalid: http.invalid,
      accounting: check,
      pending,
      drain_ms: result.drain_ms,
    }),
  );
  if (!valid || result.capacityPassed === false) process.exitCode = 1;
  void mock;
} finally {
  consuming = false;
  clearInterval(telemetryTimer);
  for (const child of processes) {
    if (child.exitCode === null) child.kill("SIGTERM");
  }
  await Promise.allSettled(processes.map((p) => p.exited));
  await Promise.allSettled([consume, sampling]);
  await ackWriter?.close().catch(() => undefined);
  await db.close();
  await owner.unsafe(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
  await owner.close();
  const cleanup = await cleanupCapacityNamespaces(
    [
      process.env.REDIS_URL!,
      ...(process.env.REDIS_EXTRA_URLS ?? "")
        .split(",")
        .map((url) => url.trim())
        .filter(Boolean),
      process.env.QUEUE_REDIS_URL!,
    ],
    namespace,
  );
  await Bun.write(`${directory}/cleanup.json`, JSON.stringify(cleanup) + "\n");
  if (cleanup.failed) {
    process.exitCode = 1;
    console.error(
      `Capacity fixture cleanup could not reach ${cleanup.failed} endpoint(s); other endpoints were still cleaned`,
    );
  }
}
