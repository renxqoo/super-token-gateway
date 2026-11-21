import { createServer, connect, type Socket } from "node:net";
import { once } from "node:events";
import { appendFile, cp, mkdir, mkdtemp, rm } from "node:fs/promises";
import { join, resolve } from "node:path";
import { createDatabase, createLedgerEngine, migrate } from "@gateway/ledger";
import { createGatewayRuntime } from "../apps/gateway/src/app.ts";
import { loadConfigFromEnv } from "../apps/gateway/src/config.ts";
import { createWorkerRunner } from "../apps/worker/src/runner.ts";

const pgBin = process.env.PG_TOOLS_DIRECTORY ?? resolve(".local/toolchain/pg17/bin");
const redisBin =
  process.env.DURABILITY_REDIS_SERVER ?? resolve(".local/toolchain/redis74/bin/redis-server");
const root = await mkdtemp("/tmp/stg-production-");
const popen: Bun.Subprocess[] = [];
const pgClusters = new Set<string>();
const pools: ReturnType<typeof createDatabase>[] = [];
const roles = ["catalog", "security", "finance", "auditor", "events", "ops"] as const;
const tokens = Object.fromEntries(
  roles.map((role) => [
    role,
    Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("base64url"),
  ]),
);
const pgUrl = (p: number, account = "drill_admin", database = "stg_drill") =>
  `postgres://${account}@127.0.0.1:${p}/${database}`;
const ms = (elapsed: number) => Number(elapsed.toFixed(2));
const database = (url: string) => {
  const db = createDatabase(url, 4);
  pools.push(db);
  return db;
};
function verify(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

async function until<T>(
  condition: () => Promise<T | undefined>,
  label: string,
  budget = 20_000,
): Promise<T> {
  const deadline = performance.now() + budget;
  let lastError: unknown;
  while (performance.now() < deadline) {
    try {
      const result = await condition();
      if (result !== undefined) return result;
    } catch (error) {
      lastError = error;
    }
    await Bun.sleep(25);
  }
  throw new Error(`${label} exceeded ${budget} ms`, { cause: lastError });
}

async function port() {
  const server = createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  verify(address && typeof address === "object", "Missing allocated local port");
  await new Promise<void>((done) => server.close(() => done()));
  return address.port;
}

async function runPg(name: string, args: string[]) {
  const proc = Bun.spawn([join(pgBin, name), ...args], { stdout: "pipe", stderr: "pipe" });
  const [status, stdout, stderr] = await Promise.all([
    proc.exited,
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);
  verify(status === 0, `${name} failed: ${stdout} ${stderr}`);
  return stdout;
}

async function startPg(directory: string, listenPort: number) {
  pgClusters.add(directory);
  await runPg("pg_ctl", [
    "-w",
    "-t",
    "15",
    "-D",
    directory,
    "-l",
    `${directory}.log`,
    "-o",
    `-p ${listenPort}`,
    "start",
  ]);
}
async function stopPg(directory: string, mode: "fast" | "immediate") {
  await runPg("pg_ctl", ["-w", "-t", "15", "-D", directory, "-m", mode, "stop"]);
  pgClusters.delete(directory);
}

async function tcpWriter(first: number) {
  let target: number | undefined = first;
  const sockets = new Set<Socket>();
  const server = createServer((socket) => {
    if (target === undefined) return socket.destroy();
    const upstream = connect(target, "127.0.0.1");
    sockets.add(socket);
    sockets.add(upstream);
    socket.on("error", () => upstream.destroy());
    upstream.on("error", () => socket.destroy());
    socket.on("close", () => {
      sockets.delete(socket);
      upstream.destroy();
    });
    upstream.on("close", () => {
      sockets.delete(upstream);
      socket.destroy();
    });
    socket.pipe(upstream).pipe(socket);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  verify(address && typeof address === "object", "Missing writer entry port");
  const switchTo = (next?: number) => {
    target = next;
    for (const socket of sockets) socket.destroy();
  };
  return {
    port: address.port,
    switchTo,
    async close() {
      switchTo();
      await new Promise<void>((done) => server.close(() => done()));
    },
  };
}

async function redisCommand(redisPort: number, command: string, args: string[] = []) {
  const client = new Bun.RedisClient(`redis://127.0.0.1:${redisPort}`, {
    connectionTimeout: 500,
    autoReconnect: false,
    enableOfflineQueue: false,
  });
  try {
    await client.connect();
    return await client.send(command, args);
  } finally {
    client.close();
  }
}

async function redisProcess(name: string, args: string[]) {
  const dir = join(root, name);
  await mkdir(dir, { recursive: true });
  const log = Bun.file(join(dir, "server.log"));
  const proc = Bun.spawn([redisBin, ...args], { cwd: dir, stdout: log, stderr: log });
  popen.push(proc);
  return proc;
}

let gateway: ReturnType<typeof createGatewayRuntime> | undefined;
let writer: Awaited<ReturnType<typeof tcpWriter>> | undefined;
let upstream: ReturnType<typeof Bun.serve> | undefined;
const workers: ReturnType<typeof createWorkerRunner>[] = [];
let passed = false;
try {
  const [primaryPort, standbyPort, restoredPort, redisPort, replicaPort, ...sentinelPorts] =
    (await Promise.all(Array.from({ length: 8 }, () => port()))) as [
      number,
      number,
      number,
      number,
      number,
      number,
      number,
      number,
    ];
  const primaryDir = join(root, "primary");
  const standbyDir = join(root, "standby");
  const backupDir = join(root, "backup");
  const restoreDir = join(root, "restore");
  const archiveDir = join(root, "wal");
  await mkdir(archiveDir);
  await runPg("initdb", [
    "-D",
    primaryDir,
    "-U",
    "drill_admin",
    "-A",
    "trust",
    "--no-locale",
    "--data-checksums",
  ]);
  await appendFile(
    join(primaryDir, "postgresql.conf"),
    `
listen_addresses = '127.0.0.1'
unix_socket_directories = '${root}'
wal_level = replica
archive_mode = on
archive_command = 'cp %p ${archiveDir}/%f.tmp && mv ${archiveDir}/%f.tmp ${archiveDir}/%f'
fsync = on
full_page_writes = on
synchronous_commit = on
`,
  );
  await startPg(primaryDir, primaryPort);
  const bootstrap = database(pgUrl(primaryPort, "drill_admin", "postgres"));
  await bootstrap`CREATE ROLE drill_migration LOGIN`;
  await bootstrap`CREATE DATABASE stg_drill OWNER drill_migration`;
  const owner = database(pgUrl(primaryPort));
  await migrate(owner);
  await runPg("psql", [
    "-X",
    "-d",
    pgUrl(primaryPort),
    "-v",
    "migration_role=drill_migration",
    "-f",
    resolve("infra/postgres/adopt-ownership.sql"),
  ]);
  await migrate(database(pgUrl(primaryPort, "drill_migration")));
  verify(
    (
      await owner`SELECT pg_get_userbyid(proowner) AS owner FROM pg_proc
    WHERE oid = 'public.prepare_ledger_month(date)'::regprocedure`
    )[0]?.owner === "drill_migration",
    "Partition definer must belong to the dedicated migration login",
  );
  await runPg("psql", ["-X", "-d", pgUrl(primaryPort), "-f", resolve("infra/postgres/roles.sql")]);
  await owner.unsafe(`
    CREATE ROLE drill_data LOGIN IN ROLE stg_data;
    CREATE ROLE drill_control LOGIN IN ROLE stg_control;
    CREATE ROLE drill_worker LOGIN IN ROLE stg_worker;
    CREATE ROLE drill_audit LOGIN IN ROLE stg_audit;
  `);

  const redisPrimary = await redisProcess("redis-primary", [
    "--bind",
    "127.0.0.1",
    "--port",
    String(redisPort),
    "--save",
    "",
  ]);
  const redisReplica = await redisProcess("redis-replica", [
    "--bind",
    "127.0.0.1",
    "--port",
    String(replicaPort),
    "--save",
    "",
    "--replicaof",
    "127.0.0.1",
    String(redisPort),
  ]);
  for (const [index, sentinelPort] of sentinelPorts.entries()) {
    const conf = join(root, `sentinel-${index}.conf`);
    await Bun.write(
      conf,
      `bind 127.0.0.1\nport ${sentinelPort}\nsentinel monitor stg 127.0.0.1 ${redisPort} 2\nsentinel down-after-milliseconds stg 800\nsentinel failover-timeout stg 5000\n`,
    );
    await redisProcess(`sentinel-${index}`, [conf, "--sentinel"]);
  }
  await until(async () => {
    for (const sentinelPort of sentinelPorts) {
      const reply = await redisCommand(sentinelPort, "SENTINEL", ["CKQUORUM", "stg"]);
      if (!String(reply).includes("3 usable Sentinels")) return;
    }
    const [role, , replicas] = (await redisCommand(redisPort, "ROLE")) as unknown[];
    return role === "master" && Array.isArray(replicas) && replicas.length > 0 ? true : undefined;
  }, "Sentinel quorum and replica link");

  writer = await tcpWriter(primaryPort);
  const authFile = join(root, "credentials.json");
  await Bun.write(
    authFile,
    JSON.stringify(
      roles.map((role) => ({
        id: `drill-${role}`,
        roles: [role],
        token_sha256: new Bun.CryptoHasher("sha256").update(tokens[role]!).digest("hex"),
        ...(role === "events" ? { consumers: ["drill-consumer"] } : {}),
      })),
    ),
  );
  const config = loadConfigFromEnv(
    {
      NODE_ENV: "production",
      GATEWAY_MODE: "gateway",
      DATABASE_URL: pgUrl(writer.port, "drill_data"),
      INTERNAL_DATABASE_URL: pgUrl(writer.port, "drill_control"),
      INTERNAL_AUTH_FILE: authFile,
      KEY_PEPPER: "drill-local-key-pepper",
      ENCRYPTION_KEY: "ab".repeat(32),
      REDIS_URL: `redis://127.0.0.1:${redisPort}`,
      REDIS_SENTINEL_URLS: sentinelPorts.map((p) => `redis://127.0.0.1:${p}`).join(","),
      REDIS_SENTINEL_MASTER: "stg",
      ALLOW_PRIVATE_UPSTREAMS: "true",
      PRIVATE_UPSTREAM_CIDRS: "127.0.0.0/8",
      SNAPSHOT_POLL_MS: "200",
      ACCESS_LOG: "false",
    },
    [],
  );
  gateway = createGatewayRuntime({ config });
  await gateway.start();
  const gw = gateway;
  const api = (role: string, route: string, method = "GET", body?: unknown) =>
    gw.internalApp.request(`/internal/v1/${route}`, {
      method,
      headers: { authorization: `Bearer ${tokens[role]}`, "content-type": "application/json" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
  async function admin(role: string, route: string, method = "GET", body?: unknown) {
    const response = await api(role, route, method, body);
    const result = (await response.json()) as Record<string, unknown>;
    verify(
      response.ok,
      `Admin ${method} ${route} failed: ${response.status} ${JSON.stringify(result)}`,
    );
    return result;
  }
  const project = await admin("catalog", "projects", "PUT", { name: "drill", currency: "USD" });
  const creditPath = `wallets/${project.id}/credits`;
  const credit = (ref: string, amount = "1000") =>
    admin("finance", creditPath, "POST", { external_ref: ref, amount_atoms: amount });
  verify(
    (await api("catalog", creditPath, "POST", { amount_atoms: "1000" })).status === 403,
    "Catalog role changed funds",
  );
  verify(
    (await api("events", "events/claim", "POST", { consumer: "other-consumer" })).status === 403,
    "Consumer could claim another cursor",
  );
  verify(
    (await api("finance", "plugins/builtins")).status === 403,
    "Finance role accessed plugin registry",
  );
  verify((await api("anonymous", "health")).status === 401, "Unauthenticated management route");
  verify(
    (await gw.publicApp.request("/metrics")).status === 404,
    "Production metrics are publicly accessible",
  );
  await credit("initial-wallet", "100000");
  const [auditRow] = await owner`SELECT actor, target FROM public.admin_audit_events
    WHERE phase = 'COMPLETED' AND action = 'POST /internal/v1/wallets/:projectId/credits' AND status = 200`;
  verify(auditRow?.actor === "drill-finance", "Audit actor is not the credential identity");
  verify(
    auditRow?.target === `/internal/v1/${creditPath}`,
    "Audit did not retain the affected wallet",
  );
  verify(
    (await owner`SELECT count(*)::int AS n FROM public.admin_audit_pending`)[0]?.n === 0,
    "Completed management operations are still marked pending",
  );

  const data = database(pgUrl(writer.port, "drill_data"));
  const auditDb = database(pgUrl(writer.port, "drill_audit"));
  const limitedWorker = database(pgUrl(writer.port, "drill_worker"));
  const privilegeChecks = await Promise.all(
    [
      data`UPDATE public.config_epoch SET version = version`,
      data`CREATE TABLE public.unauthorized (id integer)`,
      auditDb`SELECT verifier FROM public.api_keys`,
      limitedWorker`DELETE FROM public.ledger_entries`,
      limitedWorker`UPDATE public.billing_charges SET customer_atoms = 3`,
      data`UPDATE public.wallets SET credit_limit_atoms = 99`,
      data`SELECT public.prepare_ledger_month(date_trunc('month', now())::date)`,
    ].map(async (query) => {
      try {
        await query;
        return false;
      } catch (error) {
        return (error as { errno?: string }).errno === "42501";
      }
    }),
  );
  verify(privilegeChecks.every(Boolean), "A least-privilege denial failed");
  await auditDb`SELECT count(*) FROM public.admin_audit_events`;

  const account = await admin("catalog", "upstream-accounts", "PUT", { name: "drill-upstream" });
  await admin("finance", `upstream-accounts/${account.id}/recharges`, "POST", {
    external_ref: "recharge",
    amount_atoms: "100000",
  });
  const key = await admin("security", "api-keys", "PUT", { name: "drill", project_id: project.id });
  upstream = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch: () =>
      Response.json({
        choices: [
          { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
        ],
        usage: { prompt_tokens: 2, completion_tokens: 3, total_tokens: 5 },
      }),
  });
  const channel = await admin("catalog", "channels", "PUT", {
    name: "drill",
    account_id: account.id,
    kind: "openai",
    base_url: `http://127.0.0.1:${upstream.port}`,
    api_key: "drill-upstream",
  });
  await admin("catalog", "models", "PUT", {
    name: "drill-model",
    operations: ["chat"],
    max_output_tokens: 32,
  });
  const tariff = {
    currency: "USD",
    rates: {
      input_tokens: { numerator: "2", denominator: "1" },
      output_tokens: { numerator: "3", denominator: "1" },
    },
    ceilings: { input_tokens: "128", output_tokens: "32" },
  };
  const customerPrice = await admin("finance", "prices", "POST", { ...tariff, side: "CUSTOMER" });
  const supplierPrice = await admin("finance", "prices", "POST", { ...tariff, side: "SUPPLIER" });
  await admin("catalog", "routes", "PUT", {
    model_name: "drill-model",
    operations: ["chat"],
    upstream_model: "upstream",
    channel_id: channel.id,
    billing_price_id: customerPrice.id,
    cost_version_id: supplierPrice.id,
  });
  const infer = () =>
    gw.publicApp.request("/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "drill-model",
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 8,
      }),
    });
  const inference = await infer();
  verify(
    inference.status === 200,
    `Data-role inference failed: ${inference.status} ${await inference.text()}`,
  );
  const [write] =
    await auditDb`SELECT customer_atoms::text AS atoms FROM public.billing_charges WHERE outcome = 'SETTLED'`;
  verify(write?.atoms === "13", "Restricted data role did not settle exact charge");
  const claim = await admin("events", "events/claim", "POST", {
    consumer: "drill-consumer",
    limit: 1,
  });
  const claimed = claim.events as { seq: string }[];
  verify(claimed.length === 1, "Scoped consumer could not claim committed event");
  verify(
    (
      await admin("events", "events/ack", "POST", {
        consumer: "drill-consumer",
        lease_token: claim.lease_token,
        through_seq: claimed[0]!.seq,
      })
    ).ok,
    "Scoped consumer could not ACK",
  );
  for (const db of [limitedWorker, database(pgUrl(writer.port, "drill_worker"))]) {
    const worker = createWorkerRunner({ db, sweepIntervalMs: 200, monitorIntervalMs: 200 });
    workers.push(worker);
    await worker.start();
  }
  verify(workers.filter((item) => item.isLeader()).length === 1, "Expected one Worker leader");
  await until(
    async () =>
      workers.some((worker) =>
        /stg_worker_operations_sample_timestamp_seconds [1-9]/.test(worker.metrics()),
      )
        ? true
        : undefined,
    "Worker monitoring sample",
  );
  await until(
    async () =>
      (
        await owner`SELECT count(*) AS count FROM pg_inherits WHERE inhparent = 'public.ledger_entries'::regclass`
      )[0]?.count > 1
        ? true
        : undefined,
    "Worker partition maintenance",
  );

  await runPg("pg_basebackup", [
    "-h",
    "127.0.0.1",
    "-p",
    String(primaryPort),
    "-U",
    "drill_admin",
    "-D",
    backupDir,
    "-X",
    "stream",
    "--checkpoint=fast",
  ]);
  await runPg("pg_verifybackup", [backupDir]);
  await credit("pitr-visible", "101");
  const [cutoff] =
    await owner`SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') || '+00' AS target`;
  const targetTime = String(cutoff!.target);
  await Bun.sleep(80);
  await credit("pitr-excluded", "37");
  const [fault] =
    await owner`SELECT (extract(epoch FROM clock_timestamp()) * 1000)::float8 AS time_ms,
    pg_walfile_name(pg_current_wal_insert_lsn()) AS segment`;
  const restoreStart = performance.now();
  await owner`SELECT pg_switch_wal()`;
  await until(
    async () =>
      (await Bun.file(join(archiveDir, String(fault!.segment))).exists()) ? true : undefined,
    "WAL archive",
  );
  await cp(backupDir, restoreDir, { recursive: true });
  await appendFile(
    join(restoreDir, "postgresql.auto.conf"),
    `
archive_mode = off
restore_command = 'cp ${archiveDir}/%f %p'
recovery_target_time = '${targetTime}'
recovery_target_action = 'promote'
`,
  );
  await Bun.write(join(restoreDir, "recovery.signal"), "");
  await startPg(restoreDir, restoredPort);
  const restoredOwner = database(pgUrl(restoredPort));
  await until(
    async () =>
      (await restoredOwner`SELECT NOT pg_is_in_recovery() AS writable`)[0]?.writable
        ? true
        : undefined,
    "PITR promotion",
  );
  const restoredAdmin = database(pgUrl(restoredPort, "drill_control"));
  const restoreLedger = createLedgerEngine(restoredAdmin, { enableLeases: false });
  const restoredCredits =
    await restoredOwner`SELECT external_ref, amount_atoms::text AS atoms FROM public.funding_operations
     WHERE external_ref IN ('pitr-visible', 'pitr-excluded') ORDER BY external_ref`;
  verify(
    restoredCredits.length === 1 &&
      restoredCredits[0]?.external_ref === "pitr-visible" &&
      restoredCredits[0]?.atoms === "101",
    "PITR cutoff does not match confirmed credits",
  );
  verify((await restoreLedger.auditConservation()).ok, "Restored ledger conservation failed");
  const restoreWrite = await restoreLedger.creditWallet({
    project_id: String(project.id),
    currency: "USD",
    kind: "CASH",
    external_ref: "pitr-write-probe",
    amount_atoms: "1",
  });
  verify(restoreWrite, "PITR writer probe failed");
  const pitr = {
    target: targetTime,
    recovery_window_ms: ms(Number(fault!.time_ms) - Date.parse(targetTime)),
    lost_confirmed_before_target: 0,
    rto_ms: ms(performance.now() - restoreStart),
  };

  await runPg("pg_basebackup", [
    "-h",
    "127.0.0.1",
    "-p",
    String(primaryPort),
    "-U",
    "drill_admin",
    "-D",
    standbyDir,
    "-X",
    "stream",
    "--checkpoint=fast",
    "-R",
  ]);
  await appendFile(join(standbyDir, "postgresql.auto.conf"), "\narchive_mode = off\n");
  await startPg(standbyDir, standbyPort);
  await owner`ALTER SYSTEM SET synchronous_standby_names = '*'`;
  await owner`ALTER SYSTEM SET synchronous_commit = 'remote_apply'`;
  await owner`SELECT pg_reload_conf()`;
  await until(async () => {
    const [state] = await owner`SELECT current_setting('synchronous_commit') AS commit,
      EXISTS (SELECT FROM pg_stat_replication WHERE state = 'streaming' AND sync_state = 'sync') AS synced`;
    return state?.commit === "remote_apply" && state.synced ? true : undefined;
  }, "Synchronous remote_apply");
  const syncCount = 8;
  for (let i = 0; i < syncCount; i++) await credit(`pg-ha-${i}`);
  const pgFailure = performance.now();
  const pgFailureWall = Date.now();
  writer.switchTo();
  await stopPg(primaryDir, "immediate");
  const statusDuring = (await gw.publicApp.request("/readyz")).status;
  verify(statusDuring === 503, "Gateway accepted traffic during writer outage");
  await runPg("pg_ctl", ["-D", standbyDir, "-w", "-t", "15", "promote"]);
  writer.switchTo(standbyPort);
  await until(
    async () => ((await gw.publicApp.request("/readyz")).status === 200 ? true : undefined),
    "Gateway writer readiness",
  );
  await credit("pg-after-failover", "29");
  const pgRto = ms(performance.now() - pgFailure);
  verify((await infer()).status === 200, "Inference failed after writer promotion");
  const pgInferenceRto = ms(performance.now() - pgFailure);
  const promoted = database(pgUrl(standbyPort));
  const [retained] =
    await promoted`SELECT count(*)::int AS n FROM public.funding_operations WHERE external_ref LIKE 'pg-ha-%'`;
  verify(retained?.n === syncCount, "Lost confirmed PostgreSQL credits");
  await until(async () => {
    const leaders = workers.filter((item) => item.isLeader());
    if (
      leaders.length !== 1 ||
      Date.parse(leaders[0]!.status().last_successful_sweep ?? "") <= pgFailureWall
    )
      return;
    const [lock] = await promoted`SELECT count(*)::int AS n FROM pg_locks
        WHERE locktype = 'advisory' AND classid = 0 AND objid = 81442599 AND objsubid = 1 AND granted`;
    if (!leaders[0]!.status().last_successful_sweep || lock?.n !== 1) return;
    return true;
  }, "Worker leader after promotion");
  const workerRto = ms(performance.now() - pgFailure);
  verify((await admin("auditor", "audit/conservation")).ok, "Promoted ledger conservation failed");

  await redisCommand(redisPort, "SET", ["drill:replicated", "retained"]);
  await until(
    async () =>
      (await redisCommand(replicaPort, "GET", ["drill:replicated"])) === "retained"
        ? true
        : undefined,
    "Initial Redis marker replication",
  );
  const retainedAt = performance.now();
  redisReplica.kill("SIGSTOP");
  verify(
    Number(await redisCommand(redisPort, "CLIENT", ["KILL", "TYPE", "replica"])) === 1,
    "Redis replication link was not cut before fault injection",
  );
  const permit = await gw.cluster.acquireRateLimit(
    "drill-rpo",
    { concurrency: 1, rpm: 10, tpm: 10 },
    1,
  );
  verify(permit.ok, "Could not acquire Redis permit before failover");
  const quotaAcknowledgedAt = performance.now();
  await Bun.sleep(100);
  const redisFailure = performance.now();
  redisPrimary.kill("SIGKILL");
  await redisPrimary.exited;
  redisReplica.kill("SIGCONT");
  await until(
    async () => (!gw.cluster.isRedisConnected() ? true : undefined),
    "Redis permit revocation after primary loss",
  );
  verify(permit.signal?.aborted, "Old primary permit remained active");
  await until(async () => {
    const primary = (await redisCommand(sentinelPorts[0]!, "SENTINEL", [
      "get-master-addr-by-name",
      "stg",
    ])) as string[];
    if (
      Number(primary?.[1]) !== replicaPort ||
      (await gw.publicApp.request("/readyz")).status !== 200
    )
      return;
    const probe = await gw.cluster.acquireRateLimit(
      "drill-restored",
      { concurrency: 1, rpm: 10, tpm: 10 },
      1,
    );
    if (probe.ok) {
      probe.release();
      return true;
    }
  }, "Sentinel promotion and Gateway quota write");
  const redisQuotaRto = ms(performance.now() - redisFailure);
  verify(
    (await redisCommand(replicaPort, "GET", ["drill:replicated"])) === "retained",
    "Redis lost replicated marker",
  );
  const lostQuota =
    Number(await redisCommand(replicaPort, "EXISTS", ["stg:rl:rpm:drill-rpo"])) === 0;
  verify(lostQuota, "The unreplicated test permit unexpectedly survived the cut replication link");
  verify(
    (await gw.publicApp.request("/readyz")).status === 200,
    "Gateway did not recover after Sentinel failover",
  );
  const redisRto = ms(performance.now() - redisFailure);
  const afterRedis = await infer();
  verify(
    afterRedis.status === 200,
    `Gateway failed charging after Redis failover: ${afterRedis.status} ${await afterRedis.text()}`,
  );

  console.log(
    JSON.stringify(
      {
        verified: true,
        bun: Bun.version,
        postgresql: (await runPg("postgres", ["--version"])).trim(),
        authorization: {
          negative_db_checks: privilegeChecks.length,
          scoped_consumers: true,
          audit_actor: true,
          data_role_atoms: "13",
        },
        postgres_failover: {
          synchronous_commit: "remote_apply",
          confirmed_transactions: syncCount,
          lost_confirmed: 0,
          rpo_ms: 0,
          rto_ms: pgRto,
          inference_rto_ms: pgInferenceRto,
          worker_rto_ms: workerRto,
        },
        redis_sentinel_failover: {
          sentinels: sentinelPorts.length,
          replicated_marker_preserved: true,
          lost_confirmed_quota_updates: Number(lostQuota),
          rpo_ms: ms(redisFailure - retainedAt),
          oldest_lost_ack_age_ms: ms(redisFailure - quotaAcknowledgedAt),
          quota_rto_ms: redisQuotaRto,
          rto_ms: redisRto,
        },
        postgres_pitr: pitr,
      },
      null,
      2,
    ),
  );
  passed = true;
} catch (error) {
  console.error(`Production drill evidence directory: ${root}`);
  throw error;
} finally {
  await Promise.allSettled(workers.map((worker) => worker.stop()));
  if (gateway) await gateway.close().catch(() => undefined);
  if (upstream) await upstream.stop(true);
  await Promise.allSettled(pools.map((db) => db.close()));
  if (writer) await writer.close();
  for (const proc of popen) {
    if (proc.exitCode === null && proc.signalCode === null) {
      proc.kill("SIGCONT");
      proc.kill("SIGKILL");
    }
  }
  await Promise.allSettled(popen.map((proc) => proc.exited));
  await Promise.allSettled([...pgClusters].map((dir) => stopPg(dir, "immediate")));
  if (passed && !process.env.KEEP_DRILL_FILES) await rm(root, { recursive: true, force: true });
}
