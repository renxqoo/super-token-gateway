/** Isolated, paid-path benchmark. Creates only its own disposable fixture DB and Redis namespace. */
import { mkdir } from "node:fs/promises";
import { createDatabase, migrate, Store, FundsStore } from "@gateway/storage";
import { createKey, verifier, encrypt } from "@gateway/kernel";
import { RedisClient } from "bun";
const args = Bun.argv.slice(2);
const option = (name: string, fallback: string) => {
  const at = args.indexOf(name);
  return at >= 0 ? (args[at + 1] ?? fallback) : fallback;
};
const seconds = Number(option("--seconds", "10"));
const instances = Number(option("--instances", "1"));
const wallets = Number(option("--wallets", "1"));
const concurrency = Number(option("--concurrency", "64"));
const pool = Number(option("--pool", "16"));
const stream = args.includes("--stream");
if (
  ![seconds, instances, wallets, concurrency, pool].every(
    (v) => Number.isSafeInteger(v) && v > 0,
  ) ||
  seconds > 3600 ||
  instances > 8 ||
  wallets > 1000 ||
  instances * pool + 16 > 90
)
  throw new Error("Invalid capacity bounds");
const base = process.env.DATABASE_URL;
if (!base || !process.env.REDIS_URL || !process.env.QUEUE_REDIS_URL)
  throw new Error("Local PG, admission Redis and durable queue Redis URLs are required");
const id = crypto.randomUUID().replaceAll("-", "");
const database = `gateway_bench_${id}`;
const namespace = `bench:${id}`;
const directory = `.local/phase2/bench-${id}`;
await mkdir(directory, { recursive: true });
const owner = createDatabase(base, 2);
await owner.unsafe(`CREATE DATABASE ${database}`);
const url = new URL(base);
url.pathname = `/${database}`;
const db = createDatabase(url.toString(), 4);
const processes: ReturnType<typeof Bun.spawn>[] = [];
let telemetryTimer: ReturnType<typeof setInterval> | undefined;
let telemetryErrors = 0;
const env = {
  ...process.env,
  NODE_ENV: "test",
  QUEUE_REQUIRED_REPLICAS: process.env.QUEUE_REQUIRED_REPLICAS ?? "0",
  DATABASE_URL: url.toString(),
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
  await migrate(db);
  const databaseSettings = await db<
    { name: string; setting: string; unit: string | null }[]
  >`SELECT name,setting,unit FROM pg_settings WHERE name IN ('server_version','shared_buffers','effective_cache_size','work_mem','max_wal_size','checkpoint_timeout','wal_compression','fsync','full_page_writes','synchronous_commit','random_page_cost','autovacuum_naptime') ORDER BY name`;
  for (const name of ["fsync", "full_page_writes", "synchronous_commit"])
    if (!databaseSettings.some((row) => row.name === name && row.setting === "on"))
      throw new Error(`Financial capacity measurement requires ${name}=on`);
  if (args.includes("--profile")) {
    await db`CREATE EXTENSION pg_stat_statements`;
    await db.unsafe(`ALTER DATABASE ${database} SET pg_stat_statements.track='all'`);
  }
  await db.unsafe(`ALTER DATABASE ${database} SET track_functions='all'`);
  await db.unsafe(`ALTER DATABASE ${database} SET track_wal_io_timing=on`);
  const walBefore = (
    await db`SELECT wal_records::text,wal_bytes::text,wal_sync,wal_write,wal_sync_time,wal_write_time FROM pg_stat_wal`
  )[0];
  const store = new Store(db);
  const funds = new FundsStore(db);
  const provider = crypto.randomUUID();
  await store.createProvider(
    provider,
    {
      name: "benchmark",
      kind: "openai",
      base_url: "http://127.0.0.1:19088/openai/v1",
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
        input_tokens: { numerator: "3", denominator: "1" },
        cached_input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "7", denominator: "1" },
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
  const consumer = await store.createConsumer(`bench-${id}`, false);
  const mock = spawn("dist/mock-providers/main.js", { MOCK_PORT: "19088" }, "mock");
  await wait(19088);
  const ports = Array.from({ length: instances }, (_, i) => 18081 + i);
  for (const port of ports)
    spawn("dist/gateway/main.js", { PORT: String(port) }, `gateway-${port}`);
  for (const port of ports) await wait(port);
  spawn("dist/gateway/worker-main.js", { DB_POOL_SIZE: "16" }, "worker");
  await wait(18099);
  const timeline: { at: number; queue: Record<string, number>; worker_metrics: string }[] = [];
  const samplingStart = Date.now();
  let sampling: Promise<void> | undefined;
  telemetryTimer = setInterval(() => {
    if (sampling) return;
    sampling = (async () => {
      const headers = { authorization: `Bearer ${env.ADMIN_KEY}` };
      const [queue, worker] = await Promise.all([
        fetch(`http://127.0.0.1:${ports[0]}/admin/queue`, {
          headers,
          signal: AbortSignal.timeout(2000),
        }),
        fetch("http://127.0.0.1:18099/metrics", { headers, signal: AbortSignal.timeout(2000) }),
      ]);
      if (!queue.ok || !worker.ok) throw new Error("Capacity telemetry unavailable");
      timeline.push({
        at: Date.now() - samplingStart,
        queue: (await queue.json()) as Record<string, number>,
        worker_metrics: await worker.text(),
      });
    })()
      .catch(() => {
        telemetryErrors++;
      })
      .finally(() => {
        sampling = undefined;
      });
  }, 1000);
  let consuming = true;
  let delivered = 0;
  const consume = (async () => {
    while (consuming) {
      try {
        const page = await store.claimEvents(consumer, 500);
        if (page.events.length) {
          await store.ackEvents(
            consumer,
            page.lease_token,
            page.events.map((e) => e.id),
          );
          delivered += page.events.length;
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
      "--metrics",
      "--output",
      reportPath,
      ...(stream ? ["--stream"] : []),
      ...(args.includes("--rate") ? ["--rate", option("--rate", "0")] : []),
    ],
    {
      env: { ...env, BENCH_API_KEYS: keys.join(",") },
      stdout: Bun.file(`${directory}/load.log`),
      stderr: Bun.file(`${directory}/load.log`),
    },
  );
  processes.push(load);
  const exit = await load.exited;
  const drainStart = Date.now();
  let pending = 1;
  while (Date.now() - drainStart < 60000) {
    const rows =
      await db`SELECT count(*)::int AS n FROM billing_holds WHERE state NOT IN ('SETTLED','RELEASED')`;
    pending = rows[0].n;
    if (!pending) break;
    await Bun.sleep(100);
  }
  while (Date.now() - drainStart < 60000) {
    const rows = await db`SELECT count(*)::int AS n FROM event_deliveries WHERE state<>'ACKED'`;
    if (!rows[0].n) break;
    await Bun.sleep(25);
  }
  consuming = false;
  await consume;
  let queueStats: Record<string, number> = {};
  do {
    const response = await fetch(`http://127.0.0.1:${ports[0]}/admin/queue`, {
      headers: { authorization: `Bearer ${env.ADMIN_KEY}` },
    });
    if (!response.ok) throw new Error("Queue audit unavailable");
    queueStats = (await response.json()) as Record<string, number>;
    if (Object.values(queueStats).every((n) => n === 0)) break;
    await Bun.sleep(25);
  } while (Date.now() - drainStart < 60000);
  const serviceDrainedAt = Date.now();
  clearInterval(telemetryTimer);
  const accounting =
    await db`SELECT (SELECT count(*)::int FROM gateway_requests) AS requests,(SELECT count(*)::int FROM attempts) AS attempts,(SELECT count(*)::int FROM usage_events WHERE finality='FINAL' AND usage->>'input_tokens'='7' AND usage->>'cached_input_tokens'='3' AND usage->>'output_tokens'='5' AND cost_atoms=59 AND cost_complete) AS precise_usage,(SELECT count(*)::int FROM billing_charges WHERE amount_atoms=59 AND funded_atoms=59 AND debt_atoms=0 AND refunded_atoms=0) AS precise_charges,(SELECT count(*)::int FROM billing_holds WHERE state='SETTLED') AS settled_holds,(SELECT sum(reserved_atoms)::text FROM wallets) AS reserved,(SELECT sum(spent_atoms)::text FROM wallets) AS spent,(SELECT sum(debt_atoms)::text FROM wallets) AS debt,(SELECT count(*)::int FROM event_deliveries WHERE state='ACKED') AS acknowledged,(SELECT count(*)::int FROM money_journal WHERE kind='SETTLE') AS settlement_journals,(SELECT count(*)::int FROM money_journal j WHERE (SELECT sum((v->>'atoms')::numeric) FROM jsonb_array_elements(j.lines)v)<>0) AS unbalanced_journals`;
  const check = accounting[0];
  let valid =
    exit === 0 &&
    !pending &&
    check.requests === check.precise_usage &&
    check.requests === check.precise_charges &&
    check.requests === check.settled_holds &&
    check.requests === check.acknowledged &&
    check.requests === check.settlement_journals &&
    check.reserved === "0" &&
    check.debt === "0" &&
    BigInt(check.spent) === BigInt(check.requests) * 59n &&
    !check.unbalanced_journals;
  const http = await Bun.file(reportPath).json();
  valid = valid && http.invalid === 0 && !(http.shed ?? 0) && http.completed === http.succeeded;
  const profile =
    await db`SELECT funcname,calls,total_time,self_time FROM pg_stat_user_functions ORDER BY total_time DESC LIMIT 25`;
  valid = valid && Object.values(queueStats).every((n) => n === 0);
  clearInterval(telemetryTimer);
  await sampling;
  valid = valid && telemetryErrors === 0 && timeline.length >= Math.max(1, Math.floor(seconds / 2));
  const result = {
    databaseSettings,
    queue: {
      batchSize: Number(env.QUEUE_BATCH_SIZE),
      requiredReplicas: Number(env.QUEUE_REQUIRED_REPLICAS ?? 0),
      workerConcurrency: Number(env.BILLING_CONCURRENCY),
    },
    telemetryErrors,
    timeline,
    queueStats,
    walBefore,
    walAfter: (
      await db`SELECT wal_records::text,wal_bytes::text,wal_sync,wal_write,wal_sync_time,wal_write_time FROM pg_stat_wal`
    )[0],
    profile,
    statements: args.includes("--profile")
      ? await db`SELECT query,calls::text,round(total_exec_time::numeric,2)::text AS total_ms,round(mean_exec_time::numeric,3)::text AS mean_ms,rows::text,shared_blks_hit::text,shared_blks_read::text,wal_bytes::text FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40`
      : undefined,
    relations:
      await db`SELECT relname,n_tup_ins::text,n_tup_upd::text,n_tup_hot_upd::text,n_dead_tup::text FROM pg_stat_user_tables ORDER BY n_tup_upd DESC LIMIT 15`,
    id,
    valid,
    seconds,
    instances,
    wallets,
    pool,
    concurrency,
    stream,
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
      valid,
      rps: http.successful_rps,
      financial_completed_rps: result.financial_completed_rps,
      invalid: http.invalid,
      accounting: check,
      pending,
      drain_ms: result.drain_ms,
    }),
  );
  if (!valid) process.exitCode = 1;
  void mock;
} finally {
  clearInterval(telemetryTimer);
  for (const child of processes) {
    if (child.exitCode === null) child.kill("SIGTERM");
  }
  await Promise.allSettled(processes.map((p) => p.exited));
  await db.close();
  await owner.unsafe(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
  await owner.close();
  for (const redisURL of [process.env.REDIS_URL, process.env.QUEUE_REDIS_URL]) {
    const redis = new RedisClient(redisURL);
    try {
      let cursor = "0";
      do {
        const result = (await redis.send("SCAN", [
          cursor,
          "MATCH",
          `*${namespace}*`,
          "COUNT",
          "1000",
        ])) as [string, string[]];
        cursor = result[0];
        if (result[1].length) await redis.send("UNLINK", result[1]);
      } while (cursor !== "0");
    } finally {
      redis.close();
    }
  }
}
