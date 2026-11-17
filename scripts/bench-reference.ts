import { SQL, RedisClient } from "bun";
import { mkdir, rm } from "node:fs/promises";
import { cpus, release, totalmem } from "node:os";
import { resolve, join } from "node:path";
import { createServer } from "node:net";
import {
  createDatabaseMetrics,
  startProcessSampler,
  summarizeStatements,
} from "./benchmark-metrics.ts";
import {
  referenceOptions,
  auditReferencePreConsumption,
  referenceVerdict,
  type ReferenceAccounting,
  type ReferenceHttp,
} from "./bench-reference-lib.ts";

const options = referenceOptions(Bun.argv.slice(2));
const binary = resolve(options.binary),
  redisServer = resolve(options.redisServer);
for (const file of [binary, redisServer, "dist/mock-providers/main.js"])
  if (!(await Bun.file(file).exists())) throw new Error(`Missing benchmark artifact: ${file}`);
if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL required");
const base = new URL(process.env.DATABASE_URL);
if (!["localhost", "127.0.0.1"].includes(base.hostname))
  throw new Error("Reference benchmark requires local PostgreSQL");
const id = crypto.randomUUID().replaceAll("-", "");
const directory = resolve(options.output ?? `.local/capacity/reference-${id}`);
const runtime = join(directory, "runtime");
await mkdir(runtime, { recursive: true, mode: 0o700 });
const database = `newapi_p0_${id}`;
const owner = new SQL(base.toString(), { max: 1 });
base.pathname = `/${database}`;
const db = new SQL(base.toString(), { max: 1 });
const children: Bun.Subprocess[] = [];
let created = false,
  sampler: ReturnType<typeof startProcessSampler> | undefined,
  metrics: Awaited<ReturnType<typeof createDatabaseMetrics>> | undefined;
let result: Record<string, unknown> = {
  id,
  database,
  options: { ...options, binary },
  started_at: new Date().toISOString(),
};
let cleanupFailed = false;
async function port() {
  const server = createServer();
  await new Promise<void>((done, fail) => {
    server.once("error", fail);
    server.listen(0, "127.0.0.1", done);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Port allocation failed");
  const value = address.port;
  await new Promise<void>((done, fail) => server.close((error) => (error ? fail(error) : done())));
  return value;
}
async function stop(child: Bun.Subprocess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGTERM");
  const timer = setTimeout(() => child.kill("SIGKILL"), 5000);
  try {
    await child.exited;
  } finally {
    clearTimeout(timer);
  }
}
function spawn(
  command: string[],
  env: Record<string, string | undefined>,
  label: string,
  cwd?: string,
) {
  if (interrupted) throw new Error("Reference benchmark interrupted");
  const log = Bun.file(join(directory, `${label}.log`));
  const child = Bun.spawn(command, {
    env: { PATH: process.env.PATH, ...env },
    ...(cwd ? { cwd } : {}),
    stdin: "ignore",
    stdout: log,
    stderr: log,
  });
  children.push(child);
  return child;
}
async function ready(url: string, child: Bun.Subprocess) {
  const deadline = Date.now() + 60000;
  while (Date.now() < deadline) {
    if (interrupted) throw new Error("Reference benchmark interrupted");
    if (child.exitCode !== null)
      throw new Error(`Process ${child.pid} exited ${child.exitCode} before readiness`);
    try {
      if ((await fetch(url, { signal: AbortSignal.timeout(1000) })).ok) return;
    } catch {}
    await Bun.sleep(50);
  }
  throw new Error(`Readiness deadline: ${url}`);
}
let interrupted = false;
const interrupt = () => {
  interrupted = true;
  for (const child of children) if (child.exitCode === null) child.kill("SIGTERM");
};
process.on("SIGINT", interrupt);
process.on("SIGTERM", interrupt);
try {
  await owner.unsafe(`CREATE DATABASE ${database}`);
  created = true;
  metrics = await createDatabaseMetrics(base.toString());
  for (const name of ["fsync", "synchronous_commit", "full_page_writes"])
    if (!metrics.settings.some((row) => row.name === name && row.setting === "on"))
      throw new Error(`Reference requires ${name}=on`);
  const redisPort = await port(),
    apiPort = await port(),
    mockPort = await port();
  const redis = spawn(
    [
      redisServer,
      "--bind",
      "127.0.0.1",
      "--port",
      String(redisPort),
      "--save",
      "",
      "--appendonly",
      "no",
      "--dir",
      runtime,
    ],
    {},
    "redis",
  );
  const redisUrl = `redis://127.0.0.1:${redisPort}`;
  const client = new RedisClient(redisUrl, { autoReconnect: false, connectionTimeout: 1000 });
  try {
    const deadline = Date.now() + 5000;
    for (;;) {
      try {
        await client.connect();
        await client.send("PING", []);
        break;
      } catch (error) {
        if (Date.now() > deadline) throw error;
        await Bun.sleep(50);
      }
    }
  } finally {
    client.close();
  }
  const mock = spawn(
    [process.execPath, resolve("dist/mock-providers/main.js")],
    { MOCK_PORT: String(mockPort), MOCK_CONTROL_TOKEN: `control-${id}` },
    "mock",
  );
  await ready(`http://127.0.0.1:${mockPort}/healthz`, mock);
  const newApiEnv = {
    SQL_DSN: base.toString(),
    REDIS_CONN_STRING: redisUrl,
    PORT: String(apiPort),
    HOST: "127.0.0.1",
    GIN_MODE: "release",
    SESSION_SECRET: crypto.randomUUID(),
    CRYPTO_SECRET: crypto.randomUUID(),
    SQL_MAX_OPEN_CONNS: "64",
    SQL_MAX_IDLE_CONNS: "64",
    TZ: "UTC",
    TMPDIR: runtime,
    TRUSTED_PROXIES: "none",
  };
  let api = spawn([binary], newApiEnv, "new-api-init", runtime);
  await ready(`http://127.0.0.1:${apiPort}/api/status`, api);
  await stop(api);
  const keys = Array.from({ length: options.wallets }, () =>
    crypto.randomUUID().replaceAll("-", ""),
  );
  await db.begin(async (tx) => {
    for (let i = 0; i < options.wallets; i++) {
      const user = 10000 + i;
      await tx`INSERT INTO users(id,username,password,display_name,role,status,quota,used_quota,request_count,"group",setting) VALUES(${user},${`p0_${i}`},'unused-benchmark-password',${`p0_${i}`},1,1,1000000000,0,0,'default','{}')`;
      await tx`INSERT INTO tokens(id,user_id,key,status,name,created_time,accessed_time,expired_time,remain_quota,unlimited_quota,used_quota,"group",model_limits_enabled,allow_ips) VALUES(${user},${user},${keys[i]!},1,${`p0_${i}`},0,0,-1,1000000000,false,0,'default',false,'')`;
    }
    await tx`INSERT INTO channels(id,type,key,status,name,base_url,models,"group",used_quota,priority,weight,channel_info,setting,settings) VALUES(1,1,'benchmark-upstream',1,'p0',${`http://127.0.0.1:${mockPort}/openai`},'gpt-4o-mini','default',0,0,1,'{}','{}','{}')`;
    await tx`INSERT INTO abilities("group",model,channel_id,enabled,priority,weight) VALUES('default','gpt-4o-mini',1,true,0,1)`;
    if (options.trustQuotaUsd !== undefined)
      await tx`INSERT INTO options(key,value) VALUES('quota_setting.trust_quota_usd',${String(options.trustQuotaUsd)}) ON CONFLICT(key) DO UPDATE SET value=excluded.value`;
  });
  if (options.preConsumeMultiplier !== undefined)
    await db`INSERT INTO options(key,value) VALUES('quota_setting.pre_consume_multiplier',${String(options.preConsumeMultiplier)}) ON CONFLICT(key) DO UPDATE SET value=excluded.value`;
  api = spawn([binary], newApiEnv, "new-api", runtime);
  await ready(`http://127.0.0.1:${apiPort}/api/status`, api);
  await metrics.resetOwned();
  const before = await metrics.snapshot();
  sampler = startProcessSampler(async () => ({
    new_api: [api.pid],
    redis: [redis.pid],
    upstream: [mock.pid],
    ...(await metrics!.roots()),
  }));
  await sampler.start();
  const load = spawn(
    [
      process.execPath,
      "scripts/bench.ts",
      "--url",
      `http://127.0.0.1:${apiPort}`,
      "--model",
      "gpt-4o-mini",
      "--seconds",
      String(options.seconds),
      "--rate",
      String(options.rate),
      "--concurrency",
      String(options.concurrency),
      "--warmup-concurrency",
      String(Math.min(64, options.concurrency)),
      "--output",
      join(directory, "http.json"),
    ],
    { BENCH_API_KEYS: keys.join(",") },
    "load",
  );
  const loadExit = await load.exited;
  if (!(await Bun.file(join(directory, "http.json")).exists()))
    throw new Error(`Load generator exited ${loadExit} without HTTP audit`);
  const http = (await Bun.file(join(directory, "http.json")).json()) as ReferenceHttp;
  const requests = http.succeeded + http.warmup_requests;
  const windowEnd = Date.parse(http.measurement_started_at) + options.seconds * 1000;
  let accounting: ReferenceAccounting;
  for (;;) {
    accounting = (
      await db<
        ReferenceAccounting[]
      >`SELECT (SELECT count(*)::float8 FROM logs WHERE type=2) AS logs,(SELECT coalesce(sum(quota),0)::float8 FROM logs WHERE type=2) AS log_quota,(SELECT coalesce(sum(used_quota),0)::float8 FROM users WHERE id>=10000) AS users_used_quota,(SELECT coalesce(sum(used_quota),0)::float8 FROM tokens WHERE id>=10000) AS tokens_used_quota,(SELECT used_quota::float8 FROM channels WHERE id=1) AS channel_used_quota,(SELECT coalesce(sum(request_count),0)::float8 FROM users WHERE id>=10000) AS user_requests,(SELECT count(*)::float8 FROM logs WHERE type=2 AND (quota<>2 OR prompt_tokens<>10 OR completion_tokens<>5)) AS invalid_logs,0::float8 AS wallet_mismatches`
    )[0]!;
    if (
      (accounting.logs === requests &&
        accounting.users_used_quota === requests * 2 &&
        accounting.tokens_used_quota === requests * 2 &&
        accounting.channel_used_quota === requests * 2) ||
      Date.now() - windowEnd >= 120000 ||
      interrupted
    )
      break;
    await Bun.sleep(100);
  }
  const drainedAt = Date.now();
  const processCpu = await sampler.stop(Math.max(1, requests));
  const pgStatements = summarizeStatements(before, await metrics.snapshot(), Math.max(1, requests));
  const users = await db<
    {
      id: number;
      used: number;
      quota: number;
      requests: number;
      token_used: number;
      remain: number;
      logs: number;
      log_quota: number;
    }[]
  >`SELECT u.id,u.used_quota::float8 AS used,u.quota::float8,u.request_count::float8 AS requests,t.used_quota::float8 AS token_used,t.remain_quota::float8 AS remain,count(l.id)::float8 AS logs,coalesce(sum(l.quota),0)::float8 AS log_quota FROM users u JOIN tokens t ON t.user_id=u.id LEFT JOIN logs l ON l.user_id=u.id AND l.type=2 WHERE u.id>=10000 GROUP BY u.id,t.id ORDER BY u.id`;
  accounting.wallet_mismatches = users.filter((u, i) => {
    const calls = http.wallet_calls[i] ?? 0;
    return (
      u.requests !== calls ||
      u.logs !== calls ||
      u.log_quota !== calls * 2 ||
      u.used !== calls * 2 ||
      u.token_used !== calls * 2 ||
      u.quota !== 1e9 - calls * 2 ||
      u.remain !== 1e9 - calls * 2
    );
  }).length;
  const preConsumption = await auditReferencePreConsumption(join(directory, "new-api.log"));
  const verdict = referenceVerdict(
    http,
    accounting,
    options.rate,
    options.seconds,
    options.targetRps,
    Math.max(0, drainedAt - windowEnd),
    options.preConsumeMultiplier !== undefined && options.trustQuotaUsd === 0
      ? preConsumption
      : undefined,
  );
  if (loadExit !== 0) verdict.failures.push("load_generator_exit");
  if (processCpu.errors) verdict.failures.push("process_sampling");
  if (!pgStatements.valid) verdict.failures.push("pg_statistics");
  if (interrupted) verdict.failures.push("interrupted");
  verdict.valid = verdict.failures.length === 0;
  verdict.capacityPassed &&= verdict.valid;
  const artifacts: Record<string, string> = {};
  for (const path of [binary, "dist/mock-providers/main.js", "scripts/bench.ts"])
    artifacts[path] = new Bun.CryptoHasher("sha256")
      .update(await Bun.file(path).bytes())
      .digest("hex");
  result = {
    ...result,
    ...verdict,
    environment: {
      platform: process.platform,
      arch: process.arch,
      os_release: release(),
      cpu_model: cpus()[0]?.model,
      cpu_count: cpus().length,
      memory_bytes: totalmem(),
      bun: Bun.version,
      artifacts,
    },
    databaseSettings: metrics.settings,
    redisSettings: { appendonly: "no", purpose: "new-api cache only" },
    trustQuotaUsd: options.trustQuotaUsd ?? 10,
    preConsumeMultiplier: options.preConsumeMultiplier ?? 1,
    preConsumption,
    http,
    accounting,
    processes: processCpu.processes,
    processCpu,
    pgStatements,
    drain_after_window_ms: Math.max(0, drainedAt - windowEnd),
    semantics:
      "No external ACK. HTTP completion windows plus final consumption logs, each wallet and token balance, channel counters; quota equivalents divide native quota by 2. This is not a request-ID intersection with external ACK.",
  };
  if (!verdict.capacityPassed) process.exitCode = 1;
} catch (error) {
  result = { ...result, error: String(error), valid: false, capacityPassed: false };
  process.exitCode = 1;
} finally {
  const cleanup: {
    processes: { pid: number; stopped: boolean }[];
    database_dropped: boolean;
    runtime_removed: boolean;
    errors: string[];
  } = { processes: [], database_dropped: false, runtime_removed: false, errors: [] };
  await sampler?.close();
  for (const child of [...children].reverse()) {
    try {
      await stop(child);
      cleanup.processes.push({
        pid: child.pid,
        stopped: child.exitCode !== null || child.signalCode !== null,
      });
    } catch (error) {
      cleanup.errors.push(String(error));
    }
  }
  await metrics?.close();
  await db.close();
  try {
    if (created) {
      await owner.unsafe(`DROP DATABASE ${database} WITH (FORCE)`);
      cleanup.database_dropped = true;
    }
  } catch (error) {
    cleanup.errors.push(String(error));
  }
  await owner.close();
  try {
    await rm(runtime, { recursive: true, force: true });
    cleanup.runtime_removed = true;
  } catch (error) {
    cleanup.errors.push(String(error));
  }
  cleanupFailed = cleanup.errors.length > 0;
  result.cleanup = cleanup;
  await Bun.write(join(directory, "audit.json"), JSON.stringify(result, null, 2) + "\n");
  process.off("SIGINT", interrupt);
  process.off("SIGTERM", interrupt);
}
if (cleanupFailed) process.exitCode = 1;
console.log(
  JSON.stringify({
    directory,
    valid: result.valid,
    capacityPassed: result.capacityPassed,
    error: result.error,
    cleanup: result.cleanup,
  }),
);
