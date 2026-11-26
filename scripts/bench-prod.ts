import { writeLocalInternalAuth } from "./local-internal-auth.ts";
import { spawn } from "node:child_process";
import { appendFile, cp, mkdtemp, rm } from "node:fs/promises";
import { Agent } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { SQL } from "bun";
import { createDatabase } from "@gateway/ledger";
import { auditConservation } from "../packages/ledger/src/conservation.ts";
import {
  runMixedSoak,
  verifyMixedCharges,
  verifyMixedBalances,
  type MixedChargeCheck,
} from "./bench-mixed.ts";
import {
  benchmarkConcurrency,
  benchmarkIngressEnvironment,
  collectProvenance,
  createFastHttp1BenchClient,
  diffCheckpointerStats,
  readCheckpointerStats,
  readProcessStats,
  warmHttpConnections,
} from "./bench-http.ts";
import { runSseBurst } from "./bench-sse-burst.ts";
import { createProductionBenchmarkLoad } from "./bench-prod-load.ts";
import { seedProductionBenchmarkCatalog } from "./bench-prod-catalog.ts";
import {
  findFreePort,
  runPgBin,
  pgBin,
  runSync,
  waitForJson,
  percentile,
  stopProcesses,
} from "./bench-prod-runtime.ts";

const syncStandby =
  process.env.BENCH_SYNC_STANDBY === "1" || process.env.BENCH_SYNC_STANDBY === "true";

const syncCommitMode = process.env.BENCH_SYNC_COMMIT ?? "on";

const baseDbUrl = process.env.DATABASE_URL;

if (!baseDbUrl && !syncStandby) throw new Error("DATABASE_URL is required");

const isStress = process.argv.includes("--stress");

const isLimit = process.argv.includes("--limit");

const isMixed = process.argv.includes("--mixed-soak");

const gatewayNodes = Number(process.env.BENCH_GATEWAY_NODES ?? 2);

if (!Number.isSafeInteger(gatewayNodes) || gatewayNodes < 1 || gatewayNodes > 8) {
  throw new Error("BENCH_GATEWAY_NODES must be an integer from 1 to 8");
}

const mockNodes = Number(
  process.env.BENCH_MOCK_NODES ?? (isLimit ? Math.min(4, Math.max(2, gatewayNodes)) : 1),
);

if (!Number.isSafeInteger(mockNodes) || mockNodes < 1 || mockNodes > 8) {
  throw new Error("BENCH_MOCK_NODES must be an integer from 1 to 8");
}

const billingAccountCount = Number(process.env.BENCH_BILLING_ACCOUNTS ?? 32);

if (
  !Number.isSafeInteger(billingAccountCount) ||
  billingAccountCount < 1 ||
  billingAccountCount > 5000
) {
  throw new Error("BENCH_BILLING_ACCOUNTS must be an integer from 1 to 5000");
}

const soakSeconds = Number(process.env.BENCH_SOAK_SECONDS ?? 1800);

const soakRps = Number(process.env.BENCH_SOAK_RPS ?? 500);

const soakConcurrency = Number(process.env.BENCH_SOAK_CONCURRENCY ?? 1024);

if (
  isMixed &&
  ![soakSeconds, soakRps, soakConcurrency].every((n) => Number.isSafeInteger(n) && n > 0)
) {
  throw new Error("Mixed soak requires positive integer duration, rate and concurrency");
}

const limitConcurrency = benchmarkConcurrency(process.env.BENCH_JSON_CONCURRENCY, [512, 1024]);

const limitSeconds = Number(process.env.BENCH_LIMIT_SECONDS ?? 8);

const isSseBurstFresh = process.argv.includes("--sse-burst-fresh");

const isSseBurst = isSseBurstFresh || process.argv.includes("--sse-burst");

const warmLimitSockets = isLimit && process.env.BENCH_LIMIT_COLD_CONNECT !== "1";

const withRedis = !process.argv.includes("--no-redis");

if (isSseBurst && !withRedis) throw new Error("--sse-burst requires Redis");

const durationSec = isStress ? 15 : 5;

const hotWalletSec = isStress ? 10 : 3;

const longStreamCount = isStress ? 1000 : 500;

const longStreamMs = isStress ? 5000 : 2500;

const maxInFlight = Number(process.env.BENCH_MAX_IN_FLIGHT ?? Math.max(5000, ...limitConcurrency));

const upstreamDelayMs = Number(process.env.BENCH_UPSTREAM_DELAY_MS ?? 0);

const accessLog = process.env.BENCH_ACCESS_LOG === "1" || process.env.BENCH_ACCESS_LOG === "true";

const verifyOutboxAck =
  process.env.BENCH_VERIFY_OUTBOX_ACK === "1" || process.env.BENCH_VERIFY_OUTBOX_ACK === "true";

const tenantAffinity =
  process.env.BENCH_TENANT_AFFINITY === "1" || process.env.BENCH_TENANT_AFFINITY === "true";

const useFastClient = process.env.BENCH_FAST_CLIENT !== "0";

const ingressEnv = benchmarkIngressEnvironment();

const tempDir = await mkdtemp(join(tmpdir(), "stg-bench-prod-"));

const distSrc = resolve(process.env.BENCH_DIST_DIR ?? "dist");

const dbName = `gateway_v4_bprod_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;

const pgClustersToStop: string[] = [];

let adminDb: ReturnType<typeof createDatabase> | undefined;

let targetDbUrl: string;

let replicationState: Record<string, unknown> | null = null;

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

let fastClient: ReturnType<typeof createFastHttp1BenchClient> | undefined;

try {
  await cp(distSrc, join(tempDir, "dist"), { recursive: true });

  if (syncStandby) {
    const [primaryPort, standbyPort] = await Promise.all([findFreePort(), findFreePort()]);
    const primaryDir = join(tempDir, "pg-primary");
    const standbyDir = join(tempDir, "pg-standby");
    await runPgBin(pgBin, "initdb", [
      "-D",
      primaryDir,
      "-U",
      "bench_pg",
      "-A",
      "trust",
      "--no-locale",
    ]);
    await appendFile(
      join(primaryDir, "postgresql.conf"),
      `\nlisten_addresses = '127.0.0.1'\nunix_socket_directories = '${tempDir}'\nwal_level = replica\nmax_wal_senders = 4\nshared_buffers = 512MB\nmax_wal_size = 4GB\ncheckpoint_completion_target = 0.9\nfsync = on\nfull_page_writes = on\nsynchronous_commit = ${syncCommitMode}\nmax_connections = 256\n`,
    );
    pgClustersToStop.push(primaryDir);
    await runPgBin(pgBin, "pg_ctl", [
      "-w",
      "-t",
      "15",
      "-D",
      primaryDir,
      "-l",
      join(tempDir, "pg-primary.log"),
      "-o",
      `-p ${primaryPort}`,
      "start",
    ]);

    await runPgBin(pgBin, "pg_basebackup", [
      "-h",
      "127.0.0.1",
      "-p",
      String(primaryPort),
      "-U",
      "bench_pg",
      "-D",
      standbyDir,
      "-X",
      "stream",
      "--checkpoint=fast",
      "-R",
    ]);
    pgClustersToStop.push(standbyDir);
    await runPgBin(pgBin, "pg_ctl", [
      "-w",
      "-t",
      "15",
      "-D",
      standbyDir,
      "-l",
      join(tempDir, "pg-standby.log"),
      "-o",
      `-p ${standbyPort}`,
      "start",
    ]);

    adminDb = createDatabase(`postgres://bench_pg@127.0.0.1:${primaryPort}/postgres`, 2);
    await adminDb`ALTER SYSTEM SET synchronous_standby_names = '*'`;
    await adminDb`SELECT pg_reload_conf()`;
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline) {
      const [rep] = await adminDb<Record<string, unknown>[]>`
        SELECT application_name, state, sync_state, sent_lsn::text, flush_lsn::text, replay_lsn::text
        FROM pg_stat_replication WHERE state = 'streaming' AND sync_state = 'sync'
        LIMIT 1
      `;
      if (rep) {
        replicationState = rep;
        break;
      }
      await Bun.sleep(100);
    }
    if (!replicationState)
      throw new Error("Synchronous standby did not reach streaming sync state");
    await adminDb.unsafe(`CREATE DATABASE ${dbName}`);
    targetDbUrl = `postgres://bench_pg@127.0.0.1:${primaryPort}/${dbName}`;
  } else {
    const adminUrl = new URL(baseDbUrl!);
    adminUrl.pathname = "/postgres";
    const targetUrl = new URL(baseDbUrl!);
    targetUrl.pathname = `/${dbName}`;
    adminDb = createDatabase(adminUrl.toString(), 2);
    await adminDb.unsafe(`CREATE DATABASE ${dbName}`);
    targetDbUrl = targetUrl.toString();
  }

  const mockPorts = await Promise.all(Array.from({ length: mockNodes }, () => findFreePort()));
  const ports = await Promise.all(Array.from({ length: gatewayNodes }, () => findFreePort()));
  const internalPorts = await Promise.all(
    Array.from({ length: gatewayNodes }, () => findFreePort()),
  );
  const [workerPort, redisPort] = await Promise.all([findFreePort(), findFreePort()]);

  let redisUrl: string | undefined;
  let redisProc: ReturnType<typeof spawn> | undefined;
  if (withRedis) {
    const redisBin =
      process.env.DURABILITY_REDIS_SERVER ??
      new URL("../.local/toolchain/redis74/bin/redis-server", import.meta.url).pathname;
    redisProc = spawn(
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
    procs.push(redisProc);
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

  const operatorToken = crypto.randomUUID() + crypto.randomUUID();
  const internalAuthFile = join(tempDir, "internal-auth.json");
  await writeLocalInternalAuth(internalAuthFile, operatorToken);
  const baseEnv: Record<string, string> = {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    BUN_CONFIG_MAX_HTTP_REQUESTS: String(Math.max(4096, maxInFlight)),
    DATABASE_URL: targetDbUrl,
    ...(redisUrl ? { REDIS_URL: redisUrl, REDIS_NAMESPACE: "stg_bench" } : {}),
    KEY_PEPPER: "bprod-pepper-0123456789abcdef0123",
    ENCRYPTION_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    INTERNAL_AUTH_FILE: internalAuthFile,
    ALLOW_PRIVATE_UPSTREAMS: "true",
    ACCESS_LOG: accessLog ? "true" : "false",
    SNAPSHOT_POLL_MS: "2000",
    MAX_IN_FLIGHT: String(maxInFlight),
    DB_MAX_CONNECTIONS: process.env.BENCH_DB_MAX_CONNECTIONS ?? "32",
    ...ingressEnv,
    MICRO_BATCH_WINDOW_MS: process.env.BENCH_BATCH_WINDOW_MS ?? "2",
    MICRO_BATCH_MAX_SIZE: process.env.BENCH_BATCH_MAX_SIZE ?? "256",
    MOCK_HOST: "127.0.0.1",
    MOCK_PORT: String(mockPorts[0]),
    MOCK_CONTROL_TOKEN: "bprod-mock-ctrl",
    WORKER_HEALTH_HOST: "127.0.0.1",
    WORKER_HEALTH_PORT: String(workerPort),
  };

  await runSync(tempDir, ["dist/tools/migrate.js"], baseEnv);

  const mockProcs = mockPorts.map((port) =>
    spawn(process.execPath, ["dist/mock-providers/main.js"], {
      cwd: tempDir,
      env: { ...baseEnv, MOCK_PORT: String(port) },
      stdio: "ignore",
    }),
  );
  const workerProc = spawn(process.execPath, ["dist/worker/main.js"], {
    cwd: tempDir,
    env: baseEnv,
    stdio: "ignore",
  });
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
  procs.push(...mockProcs, workerProc, ...gateways);

  await Promise.all([
    ...mockPorts.map((port) => waitForJson(`http://127.0.0.1:${port}/healthz`)),
    ...ports.map((port) => waitForJson(`http://127.0.0.1:${port}/readyz`)),
    waitForJson(`http://127.0.0.1:${workerPort}/readyz`),
  ]);

  const adminHeaders = {
    authorization: `Bearer ${operatorToken}`,
    "content-type": "application/json",
  };
  const { keys, callInternal, mixedPlugins } = await seedProductionBenchmarkCatalog({
    internalPorts,
    adminHeaders,
    mockPorts,
    baseEnv,
    maxInFlight,
    mockNodes,
    upstreamDelayMs,
    longStreamMs,
    isMixed,
    billingAccountCount,
  });

  await Bun.sleep(withRedis ? 250 : 2200);

  const db = createDatabase(targetDbUrl, 4);
  const walLsnBytes = async (): Promise<bigint> => {
    const rows =
      await db`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint::text AS bytes`;
    return BigInt(String(rows[0]!.bytes));
  };

  const readCombinedGatewayRss = async (): Promise<number> => {
    const pidMap = Object.fromEntries(gateways.map((g, i) => [`g${i}`, g.pid!]));
    const stats = await readProcessStats(pidMap);
    return Number(
      Object.values(stats)
        .reduce((sum, s) => sum + s.rssMb, 0)
        .toFixed(1),
    );
  };

  const targets = ports.map((port) => ({ host: "127.0.0.1", port }));
  if (useFastClient && !isMixed && !isSseBurst) {
    fastClient = createFastHttp1BenchClient(
      targets,
      Math.max(1024, Math.ceil(Math.max(...limitConcurrency, 2048) / targets.length) + 256),
    );
  }

  const { runFixedCountLoad, runLimitPhases, runTimedLoad } = createProductionBenchmarkLoad({
    targets,
    walLsnBytes,
    readCombinedGatewayRss,
    ports,
    redisUrl,
    keys,
    fastClient,
    tenantAffinity,
    agent,
    gatewayNodes,
    percentile,
    warmLimitSockets,
    limitConcurrency,
    limitSeconds,
  });

  let outboxStop = false;
  let outboxAcked = 0;
  let outboxBatches = 0;
  let outboxTask: Promise<void> | undefined;
  const outboxConsumerName = "bench-outbox-consumer";

  const pumpOutboxOnce = async (): Promise<number> => {
    const claimed = (await callInternal("POST", "/internal/v1/events/claim", {
      consumer: outboxConsumerName,
      limit: 500,
    })) as { lease_token: string; events: { seq: string }[] };
    if (claimed.events.length === 0) return 0;
    const throughSeq = claimed.events.at(-1)!.seq;
    const ackRes = await callInternal("POST", "/internal/v1/events/ack", {
      consumer: outboxConsumerName,
      lease_token: claimed.lease_token,
      through_seq: throughSeq,
    });
    if (!ackRes.ok) throw new Error("Outbox ACK returned ok=false");
    outboxAcked += claimed.events.length;
    outboxBatches++;
    return claimed.events.length;
  };

  try {
    const provenance = await collectProvenance(db, distSrc);
    const ckptBefore = await readCheckpointerStats(db);

    let outboxError: unknown = null;
    if (verifyOutboxAck) {
      outboxTask = (async () => {
        while (!outboxStop) {
          const n = await pumpOutboxOnce();
          if (n === 0) await Bun.sleep(20);
        }
      })().catch((err) => {
        outboxError = err;
      });
    }

    const warmup = await runFixedCountLoad("warmup", "bprod-fast", keys, 32, 200, false);

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
              mock: mockProcs[0]!.pid!,
              worker: workerProc.pid!,
              ...(redisProc ? { redis: redisProc.pid! } : {}),
              ...Object.fromEntries(
                gateways.map((gateway, index) => [`gateway_${index + 1}`, gateway.pid!]),
              ),
            },
          }),
        ]
      : isSseBurst
        ? await (async () => {
            const redis = new Bun.RedisClient(redisUrl!, {
              connectionTimeout: 300,
              autoReconnect: false,
            });
            const burstAgent = isSseBurstFresh ? new Agent(agentOptions) : agent;
            try {
              return await runSseBurst({
                agent: burstAgent,
                freshOnly: isSseBurstFresh,
                ports: [ports[0]!, ports[1] ?? ports[0]!],
                keys,
                db,
                redis,
                quotaPattern: "stg_bench:rl:conc:*",
                mockPort: mockPorts[0]!,
                mockToken: baseEnv.MOCK_CONTROL_TOKEN!,
                readGatewayRss: readCombinedGatewayRss,
              });
            } finally {
              redis.close();
              if (burstAgent !== agent) burstAgent.destroy();
            }
          })()
        : isLimit
          ? await runLimitPhases()
          : [
              await runTimedLoad(
                `${gatewayNodes}node_sustained_${durationSec}s_json_128c`,
                "bprod-fast",
                keys,
                128,
                durationSec,
                false,
              ),
              await runTimedLoad(
                `${gatewayNodes}node_sustained_${durationSec}s_json_256c`,
                "bprod-fast",
                keys,
                256,
                durationSec,
                false,
              ),
              await runTimedLoad(
                `${gatewayNodes}node_sustained_${durationSec}s_sse_128c`,
                "bprod-fast",
                keys,
                128,
                durationSec,
                true,
              ),
              await runTimedLoad(
                `${gatewayNodes}node_hot_wallet_${hotWalletSec}s_128c`,
                "bprod-fast",
                [keys[0]!],
                128,
                hotWalletSec,
                false,
              ),
              await runFixedCountLoad(
                `${gatewayNodes}node_${longStreamCount}_concurrent_${longStreamMs / 1000}s_sse_streams`,
                "bprod-long",
                keys,
                longStreamCount,
                longStreamCount,
                true,
              ),
              await runFixedCountLoad(
                `${gatewayNodes}node_live_failover_503_to_backup_64c`,
                "bprod-failover",
                keys,
                64,
                1000,
                false,
              ),
            ];

    let outboxBacklog = 0;
    let outboxDrainMs = 0;
    if (verifyOutboxAck && outboxTask) {
      outboxStop = true;
      await outboxTask;
      if (outboxError) throw outboxError;
      const drainStart = performance.now();
      while (performance.now() - drainStart < 120_000) {
        const n = await pumpOutboxOnce();
        if (n === 0) break;
      }
      outboxDrainMs = Number((performance.now() - drainStart).toFixed(2));
      const [rem] = await db<{ backlog: number }[]>`
        SELECT count(*)::int AS backlog
        FROM public.outbox_events e
        JOIN public.outbox_consumer_shards cs
          ON cs.name = ${outboxConsumerName} AND cs.shard = e.shard
        WHERE e.seq > cs.acked_seq
      `;
      outboxBacklog = rem?.backlog ?? -1;
    }

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
    const ckptAfter = await readCheckpointerStats(db);
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
          provenance,
          pg_checkpointer_delta: diffCheckpointerStats(ckptBefore, ckptAfter),
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
          topology: `${gatewayNodes}x standalone dist/gateway + 1x standalone dist/worker + ${withRedis ? "1x Redis 7.4 + " : ""}${mockNodes}x standalone dist/mock-providers + ${fastClient ? "Bun native HTTP/1.1" : "node:http"} load client + PG 17${syncStandby ? " (primary + sync standby)" : ""}`,
          options: {
            gateway_nodes: gatewayNodes,
            mock_nodes: mockNodes,
            billingAccounts: billingAccountCount,
            max_in_flight: maxInFlight,
            upstream_delay_ms: upstreamDelayMs,
            access_log: accessLog,
            sync_standby: syncStandby,
            verify_outbox_ack: verifyOutboxAck,
            tenant_affinity: tenantAffinity,
            db_max_connections_per_gateway: Number(baseEnv.DB_MAX_CONNECTIONS),
            ingress: ingressEnv,
          },
          ...(replicationState ? { pg_sync_replication: replicationState } : {}),
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
          ...(verifyOutboxAck
            ? {
                outbox_ack: {
                  consumer: outboxConsumerName,
                  acked_events: outboxAcked,
                  batches: outboxBatches,
                  backlog_after_drain: outboxBacklog,
                  drain_ms: outboxDrainMs,
                },
              }
            : {}),
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
      activeLeases !== 0 ||
      (verifyOutboxAck && (outboxBacklog !== 0 || outboxAcked < totalSettledCharges))
    ) {
      throw new Error("Production stress benchmark invariant check failed");
    }
  } finally {
    outboxStop = true;
    await outboxTask?.catch(() => undefined);
    await db.close();
  }
} finally {
  fastClient?.close();
  agent.destroy();
  await stopProcesses(procs);
  if (adminDb) {
    await adminDb
      .unsafe(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${dbName}' AND pid <> pg_backend_pid()`,
      )
      .catch(() => undefined);
    await adminDb.unsafe(`DROP DATABASE IF EXISTS ${dbName}`).catch(() => undefined);
    await adminDb.close().catch(() => undefined);
  }
  for (const dir of pgClustersToStop.reverse()) {
    await runPgBin(pgBin, "pg_ctl", ["-w", "-t", "15", "-D", dir, "-m", "immediate", "stop"]).catch(
      () => undefined,
    );
  }
  await rm(tempDir, { recursive: true, force: true });
}
