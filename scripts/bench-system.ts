import { execFileSync } from "node:child_process";
import { mkdir, rm } from "node:fs/promises";
import { cpus, release, totalmem } from "node:os";
import { resolve } from "node:path";
import { SQL, RedisClient } from "bun";
import {
  UsageProducer,
  evidenceChannel,
  streamTerminalRecoveryChannel,
  terminalRecoveryChannel,
} from "@gateway/runtime";
import { canonical } from "@gateway/kernel";
import type { createCoreManagementApplication } from "../apps/core-api/src/platform/management.ts";
import { withFinancial } from "../tests/support/financial-execution.ts";
import { syntheticAdminKey } from "../tests/support/catalog.ts";
import { createNativeFixture, nativeFixtureOptions } from "../tests/support/native-fixture.ts";
import {
  startCoreWorker,
  startGateway,
  withSettlementRole,
} from "../tests/support/gateway-process.ts";
import { confirmedStreamRequests, verifiedStreamProgress } from "./capacity-stream-completion.ts";
import { createCapacityAckIndex } from "./capacity-ack-index.ts";
import { cleanupCapacityNamespaces } from "./capacity-cleanup.ts";
import { approvePluginRuntime } from "./runtime-approval.ts";
import {
  auditCapacityIdentities,
  createIdentityWriter,
  capacityStorageBudget,
} from "./capacity-evidence.ts";
import {
  CONTRACT_MAX_P99_MS,
  passesCapacityTarget,
  passesLongStreamCapacity,
  passesOnlineCapacity,
} from "./benchmark-lib.ts";

import {
  createDatabaseMetrics,
  discoverRedisProcesses,
  startProcessSampler,
  summarizeStatements,
} from "./benchmark-metrics.ts";

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
const concurrency = Number(option("--concurrency", "8"));
const offeredRps = Number(option("--rate", "0"));
const workerConcurrency = Number(option("--worker-concurrency", "8"));
const gatewayInflight = Number(option("--gateway-inflight", "10000"));
const stream = args.includes("--stream");
const streamDurationSeconds = Number(option("--stream-duration-seconds", "0"));
const fillSeconds = streamDurationSeconds ? streamDurationSeconds + 2 : 0;
const replicatedShortChat = args.includes("--replicated-short-chat");
const replicatedStreamTerminal = args.includes("--replicated-stream-terminal");
if (replicatedStreamTerminal && (!replicatedShortChat || !stream))
  throw new Error("Replicated stream terminal requires --replicated-short-chat --stream");
const projectCoreRouting = args.includes("--project-core-routing");
if (projectCoreRouting && (!replicatedShortChat || (stream && !replicatedStreamTerminal)))
  throw new Error("Project Core routing benchmark requires replicated Chat terminal settlement");
const requestTimeoutMs = Number(option("--request-timeout-ms", "30000"));
if (!Number.isSafeInteger(requestTimeoutMs) || requestTimeoutMs < 1000 || requestTimeoutMs > 600000)
  throw new Error("Invalid request timeout");
const observe = args.includes("--observe");
const requireOnlineCapacity = args.includes("--online-capacity");
if (requireOnlineCapacity && (seconds < 60 || seconds % 60 !== 0))
  throw new Error("Online capacity acceptance requires complete 60-second windows");
const maxP99Ms = Number(option("--max-p99-ms", String(CONTRACT_MAX_P99_MS)));
if (!Number.isFinite(maxP99Ms) || maxP99Ms <= 0 || maxP99Ms > CONTRACT_MAX_P99_MS)
  throw new Error(`P99 budget must be within the ${CONTRACT_MAX_P99_MS} ms capacity contract`);
const scenario = JSON.parse(option("--scenario", "{}")) as Record<string, unknown>;
if ("streamDurationMs" in scenario)
  throw new Error("Use --stream-duration-seconds for a paced stream workload");
if (args.includes("--stream-duration-seconds")) {
  if (
    !stream ||
    !replicatedStreamTerminal ||
    !requireOnlineCapacity ||
    !targetRps ||
    !Number.isSafeInteger(offeredRps) ||
    offeredRps < targetRps ||
    offeredRps > 100000 ||
    ![5, 30, 120].includes(streamDurationSeconds) ||
    requestTimeoutMs <= streamDurationSeconds * 1000 ||
    Object.keys(scenario).length
  )
    throw new Error(
      "Long stream capacity requires fixed arrivals, replicated stream terminal, and a request timeout above a 5, 30, or 120 second stream",
    );
  scenario.streamDurationMs = streamDurationSeconds * 1000;
  scenario.repeat = 5;
}
const fixedStreamText = stream && scenario.tools !== true;
const upstreamChallenge = fixedStreamText && requireOnlineCapacity;
if (upstreamChallenge) scenario.echoRequestChallenge = true;
const upstreamMode = option("--upstream", "standard");
if (!["standard", "precomputed"].includes(upstreamMode))
  throw new Error("Invalid benchmark upstream");
if (upstreamMode === "precomputed" && (stream || Object.keys(scenario).length))
  throw new Error("Precomputed upstream requires plain JSON without scenarios");
const workerMode = option("--worker-mode", "live");
if (!["live", "deferred"].includes(workerMode)) throw new Error("Invalid worker mode");
const consumerMode = option("--consumer-mode", "live");
if (!["live", "none"].includes(consumerMode)) throw new Error("Invalid consumer mode");
if (requireOnlineCapacity && (workerMode !== "live" || consumerMode !== "live"))
  throw new Error("Online capacity requires live settlement and consumer ACK");
const drainSeconds = Number(option("--drain-seconds", "60"));
if (!Number.isSafeInteger(drainSeconds) || drainSeconds < 1 || drainSeconds > 600)
  throw new Error("Invalid drain budget");
const hotShare = Number(option("--hot-share", "0"));
const supplierAccounts = Number(option("--supplier-accounts", "1"));
const supplierConcurrency = 100000;
const upstreamConnectionsPerGateway = 1024;
const pluginConcurrentAttempts = replicatedShortChat ? 512 : 128;
const pluginRuntime = (() => {
  const value = option("--plugin-runtime", "builtin-worker");
  if (value !== "wasm" && value !== "builtin-worker") throw new Error("Invalid plugin runtime");
  return value;
})();
if (!Number.isSafeInteger(supplierAccounts) || supplierAccounts < 1 || supplierAccounts > 64)
  throw new Error("Invalid supplier account count");
if (!Number.isFinite(hotShare) || hotShare < 0 || hotShare > 1)
  throw new Error("Invalid hot wallet share");
if (
  ![seconds, instances, wallets, concurrency, workerConcurrency, gatewayInflight].every(
    (v) => Number.isSafeInteger(v) && v > 0,
  ) ||
  seconds > 3600 ||
  instances > 8 ||
  wallets > 1000 ||
  concurrency > 10000 ||
  workerConcurrency > 64 ||
  gatewayInflight > 100000
)
  throw new Error("Invalid capacity bounds");
if (streamDurationSeconds) {
  const inFlight = offeredRps * streamDurationSeconds;
  const limits = {
    driver: concurrency,
    gateway: instances * gatewayInflight,
    plugin: instances * pluginConcurrentAttempts,
    upstreamConnections: instances * upstreamConnectionsPerGateway,
    supplier: supplierAccounts * supplierConcurrency,
  };
  if (Object.values(limits).some((limit) => limit < inFlight))
    throw new Error(
      `Long stream requires at least ${inFlight} in-flight requests: ${JSON.stringify(limits)}; no fixture created`,
    );
}
if (
  !replicatedShortChat &&
  (!process.env.DATABASE_URL || !process.env.REDIS_URL || !process.env.QUEUE_REDIS_URL)
)
  throw new Error("Local PG, admission Redis and durable queue Redis URLs are required");
const requiredReplicas = replicatedShortChat
  ? 1
  : Number(process.env.QUEUE_REQUIRED_REPLICAS ?? "0");
if (!Number.isSafeInteger(requiredReplicas) || requiredReplicas < 0 || requiredReplicas > 5)
  throw new Error("Invalid QUEUE_REQUIRED_REPLICAS");
if (requireOnlineCapacity && requiredReplicas < 1)
  throw new Error("Online capacity requires an acknowledged AOF replica");
const artifacts: Record<string, string> = {};
for (const artifact of [
  "dist/core-api/main.js",
  "dist/core-api/platform/management.js",
  "dist/gateway/main.js",
  "dist/core-worker/main.js",
  "dist/native/plugin-host",
  "dist/native/builtin-adapters/worker.js",
  "dist/native/builtin-adapters/registry.json",
  "dist/mock-providers/main.js",
  "dist/manifest.json",
]) {
  const file = Bun.file(artifact);
  if (!(await file.exists())) throw new Error(`${artifact} is missing; run bun run build first`);
  artifacts[artifact] = new Bun.CryptoHasher("sha256").update(await file.bytes()).digest("hex");
}
const environment = {
  platform: process.platform,
  arch: process.arch,
  os_release: release(),
  cpu_model: cpus()[0]?.model ?? "unknown",
  cpu_count: cpus().length,
  memory_bytes: totalmem(),
  bun: Bun.version,
  artifacts,
};
const storageBudget = await capacityStorageBudget(
  process.cwd(),
  (seconds + fillSeconds) * (args.includes("--rate") ? offeredRps : targetRps || 6000),
  replicatedShortChat ? 80000 : undefined,
);
if (requireOnlineCapacity && !storageBudget.sufficient)
  throw new Error(
    `Capacity storage budget insufficient: ${storageBudget.available} bytes available, ${storageBudget.estimated + storageBudget.reserve} required; no fixture created`,
  );

const expectedUsage = {
  input_tokens: "7",
  cached_input_tokens: "3",
  output_tokens: "5",
  ...(upstreamMode === "standard" ? { reasoning_tokens: "2" } : {}),
};
const expectedCost = "59";
const expectedCharge = "100";
const supplierCredit = "1000000000000000000";
const walletCredit = "1000000000000000000";
const upstreamPort = 19088;
const id = crypto.randomUUID().replaceAll("-", "");
const namespace = `bench:${id}`;
const directory = `.local/capacity/bench-${id}`;
await mkdir(directory, { recursive: true, mode: 0o700 });
const controlToken = `control-${id}`;
const upstreamCertificate = resolve(directory, "upstream.pem"),
  upstreamKey = resolve(directory, "upstream.key");
const upstreamTls = { ca: "" };
const compiled = (await import(resolve("dist/core-api/platform/management.js"))) as {
  createCoreManagementApplication: typeof createCoreManagementApplication;
};

type Fixture = Parameters<Parameters<typeof withFinancial>[0]>[0];
type UsageEvent = {
  id: string;
  attempt_id: string;
  request_id: string;
  finality: string;
  currency: string;
  cost_complete: boolean;
  cost_atoms: string | null;
  usage: Record<string, string>;
};

async function upstreamStats(action = "stats") {
  const response = await fetch(`https://127.0.0.1:${upstreamPort}/__control/${action}`, {
    tls: upstreamTls,
    method: action === "stats" ? "GET" : "POST",
    headers: { authorization: `Bearer ${controlToken}`, "content-type": "application/json" },
    ...(action === "scenarios" ? { body: JSON.stringify({ model: "synthetic", scenario }) } : {}),
    signal: AbortSignal.timeout(2000),
  });
  if (!response.ok) throw new Error("Mock telemetry unavailable");
  return (await response.json()) as { active?: number; requests?: number; cancellations?: number };
}

async function created(
  pending: Promise<{ status: number; value: Record<string, unknown> }>,
  label: string,
  status = 201,
) {
  const response = await pending;
  if (response.status !== status)
    throw new Error(`${label} failed with ${response.status}: ${JSON.stringify(response.value)}`);
  return response.value;
}

const supplierRates = {
  input_tokens: { numerator: "3", denominator: "1" },
  cached_input_tokens: { numerator: "1", denominator: "1" },
  output_tokens: { numerator: "7", denominator: "1" },
};
const supplierCeilings = {
  input_tokens: "4096",
  cached_input_tokens: "4096",
  output_tokens: "4096",
};

async function provision(f: Fixture) {
  const setup = await f.bootstrap(
    "USD",
    supplierCredit,
    100,
    "REQUEST_COMPLETION",
    { rates: supplierRates, ceilings: supplierCeilings },
    { baseUrl: `https://127.0.0.1:${upstreamPort}`, maxInputTokens: 4096 },
  );
  const runtimeApproval = await approvePluginRuntime(f.db, {
    packageDigest: String(f.catalog.release.descriptor.package_digest),
    kind: pluginRuntime,
    policyFile: f.catalog.policyPath,
    helperExecutable: resolve("dist/native/plugin-host"),
    builtinDirectory: resolve("dist/native/builtin-adapters"),
    actor: "capacity-fixture-owner",
    reason: "Exact measured artifact execution approval",
  });
  await created(
    f.request(
      `/providers/${setup.provider.id}`,
      { concurrency: supplierConcurrency, rpm: 10000000 },
      undefined,
      "PATCH",
    ),
    "Provider limits",
    200,
  );
  const projects = [setup.project.id],
    keys = [setup.key.key],
    credited = new Map([[setup.project.id, BigInt(walletCredit) + 1000n]]);
  for (let index = 1; index < wallets; index++) {
    const project = (await created(
      f.request("/projects", {
        name: `capacity-${index}`,
        billing_currency: "USD",
        rpm: 10000000,
        tpm: 1000000000,
        concurrency: 100000,
      }),
      "Project",
    )) as { id: string };
    const key = (await created(
      f.request(`/projects/${project.id}/keys`, {
        name: "capacity",
        models: [setup.model],
        operations: ["chat"],
      }),
      "Key",
    )) as { key: string };
    projects.push(project.id);
    keys.push(key.key);
    credited.set(project.id, BigInt(walletCredit));
  }
  await created(
    f.request(
      `/projects/${setup.project.id}`,
      { rpm: 10000000, tpm: 1000000000, concurrency: 100000 },
      undefined,
      "PATCH",
    ),
    "Project limits",
    200,
  );
  for (const project of projects)
    await created(
      f.request(`/projects/${project}/credits`, {
        external_ref: `capacity-${project}`,
        currency: "USD",
        amount_atoms: walletCredit,
        kind: "CASH",
      }),
      "Customer credit",
    );
  const accounts = [setup.provider.account_id];
  for (let index = 1; index < supplierAccounts; index++) {
    const provider = (await created(
      f.request("/providers", {
        name: `capacity-supplier-${index}`,
        kind: "openai",
        base_url: `https://127.0.0.1:${upstreamPort}`,
        api_key: `capacity-upstream-${index}`,
        concurrency: supplierConcurrency,
        rpm: 10000000,
      }),
      "Supplier provider",
    )) as { id: string; account_id: string; brand_id: string };
    await created(
      f.request(`/models/${setup.model}/routes`, {
        provider_id: provider.id,
        upstream_model: "synthetic",
        operations: ["chat"],
        pricing: { currency: "USD", rates: supplierRates },
        ceilings: supplierCeilings,
      }),
      "Supplier route",
    );
    await created(
      f.request("/tariffs", {
        model: setup.model,
        operation: "chat",
        provider_id: provider.id,
        pricing: { currency: "USD", rates: { requests: { numerator: "100", denominator: "1" } } },
        ceilings: { requests: "1" },
        basis: "REQUEST_COMPLETION",
      }),
      "Supplier tariff",
    );
    await f.catalog.application.bind(await f.catalog.actor(), provider.id, {
      ...f.catalog.binding(setup.publication.version_id),
      account_id: provider.account_id,
      brand_id: provider.brand_id,
      secret_reference: `channel:${provider.id}`,
      config: {
        model_map: { [setup.model]: "synthetic" },
        max_output_tokens: 100,
        max_input_tokens: 4096,
        auth: "bearer",
      },
    });
    const voucherBytes = Buffer.from(`%PDF-capacity-supplier-${index}`);
    const voucher = (await created(
      f.request("/financial-vouchers", {
        content_base64: voucherBytes.toString("base64"),
        content_type: "application/pdf",
        digest: new Bun.CryptoHasher("sha256").update(voucherBytes).digest("hex"),
        expires_at: new Date(Date.now() + 3600000).toISOString(),
      }),
      "Supplier voucher",
    )) as { id: string; digest: string };
    await created(
      f.request(`/upstream-accounts/${provider.account_id}/recharges`, {
        ...setup.rechargeInput,
        external_ref: `capacity-supplier-${index}-${provider.id}`,
        credited_atoms: supplierCredit,
        voucher_id: voucher.id,
        voucher_digest: voucher.digest,
      }),
      "Supplier recharge",
    );
    accounts.push(provider.account_id);
  }
  return { setup, projects, keys, credited, accounts, runtimeApproval };
}

async function confirmedRequests(db: SQL, requests: string[]) {
  const rows = await db<{ request_id: string }[]>`SELECT c.request_id FROM billing_charges c
    WHERE c.request_id=ANY(${requests}::uuid[]) AND c.amount_atoms=${expectedCharge}::numeric
    AND c.funded_atoms=${expectedCharge}::numeric AND c.debt_atoms=0 AND c.refunded_atoms=0 AND c.currency='USD'`;
  return new Set(rows.map((row) => row.request_id));
}

function preciseEvidence(event: UsageEvent) {
  return (
    event.finality === "FINAL" &&
    event.currency === "USD" &&
    event.cost_complete &&
    event.cost_atoms === expectedCost &&
    JSON.stringify(Object.entries(event.usage).sort()) ===
      JSON.stringify(Object.entries(expectedUsage).sort())
  );
}

async function measure(f: Fixture, db: SQL, workerDatabaseUrl: string) {
  const spawned: Bun.Subprocess[] = [];
  const services: { alive(): boolean; kill(): void; stop(): Promise<unknown> }[] = [];
  let telemetryTimer: ReturnType<typeof setInterval> | undefined;
  let sampling: Promise<void> | undefined;
  let consuming = false;
  let consume: Promise<void> | undefined;
  let ackIndex: Awaited<ReturnType<typeof createCapacityAckIndex>> | undefined;
  let ackWriter: Awaited<ReturnType<typeof createIdentityWriter>> | undefined;
  let loadProcess: Bun.Subprocess | undefined;
  let databaseMetrics: Awaited<ReturnType<typeof createDatabaseMetrics>> | undefined;
  let processSampler: ReturnType<typeof startProcessSampler> | undefined;
  let consumerServer: ReturnType<typeof Bun.serve> | undefined;
  const queue = new UsageProducer({
    channel: evidenceChannel,
    url: process.env.QUEUE_REDIS_URL!,
    namespace,
    replicas: 0,
    timeoutMs: 2000,
  });
  const recoveryQueue = replicatedShortChat
    ? new UsageProducer({
        channel: terminalRecoveryChannel,
        url: process.env.QUEUE_REDIS_URL!,
        namespace,
        replicas: 0,
        timeoutMs: 1000,
      })
    : undefined;
  const streamRecoveryQueue = replicatedStreamTerminal
    ? new UsageProducer({
        channel: streamTerminalRecoveryChannel,
        url: process.env.QUEUE_REDIS_URL!,
        namespace,
        replicas: 0,
        timeoutMs: 1000,
      })
    : undefined;
  try {
    const databaseSettings = await db<
      { name: string; setting: string; unit: string | null }[]
    >`SELECT name,setting,unit FROM pg_settings WHERE name IN ('server_version','shared_buffers','effective_cache_size','work_mem','max_wal_size','checkpoint_timeout','wal_compression','fsync','full_page_writes','synchronous_commit','synchronous_standby_names','max_connections') ORDER BY name`;
    for (const name of ["fsync", "full_page_writes", "synchronous_commit"])
      if (!databaseSettings.some((row) => row.name === name && row.setting === "on"))
        throw new Error(`Financial capacity measurement requires ${name}=on`);
    const redisSettings: Record<string, unknown> = {};
    for (const [name, url] of [
      ["admission", process.env.REDIS_URL!],
      ["evidence", process.env.QUEUE_REDIS_URL!],
    ] as const) {
      const redis = new RedisClient(url, { autoReconnect: false, connectionTimeout: 1000 });
      try {
        await redis.connect();
        redisSettings[name] = await redis.send("CONFIG", [
          "GET",
          "appendonly",
          "appendfsync",
          "aof-load-truncated",
          "maxmemory-policy",
        ]);
      } finally {
        redis.close();
      }
    }
    const output = Bun.file(`${directory}/upstream.log`);
    const upstream = Bun.spawn(
      [
        process.execPath,
        upstreamMode === "precomputed"
          ? "scripts/bench-upstream.ts"
          : "dist/mock-providers/main.js",
      ],
      {
        env: {
          PATH: process.env.PATH,
          MOCK_PORT: String(upstreamPort),
          MOCK_CONTROL_TOKEN: controlToken,
          MOCK_TLS_KEY_FILE: upstreamKey,
          MOCK_TLS_CERT_FILE: upstreamCertificate,
        },
        stdout: output,
        stderr: output,
      },
    );
    spawned.push(upstream);
    const upstreamDeadline = Date.now() + 10000;
    for (;;) {
      try {
        if ((await fetch(`https://127.0.0.1:${upstreamPort}/healthz`, { tls: upstreamTls })).ok)
          break;
      } catch {}
      if (Date.now() > upstreamDeadline) throw new Error("Benchmark upstream did not start");
      await Bun.sleep(25);
    }
    if (Object.keys(scenario).length) await upstreamStats("scenarios");
    const { setup, projects, keys, credited, accounts, runtimeApproval } = await provision(f);
    if (consumerMode === "live")
      consumerServer = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        fetch: (request) => f.management.fetch(request),
      });
    async function consumerRequest(path: string, body: unknown) {
      const response = await fetch(`http://127.0.0.1:${consumerServer!.port}${path}`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${syntheticAdminKey}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(4000),
      });
      return { status: response.status, value: (await response.json()) as Record<string, unknown> };
    }
    const consumer =
      consumerMode === "live"
        ? (
            (await created(
              consumerRequest("/consumers", { name: `bench-${id}`, replay: false }),
              "Consumer",
            )) as {
              id: string;
            }
          ).id
        : undefined;
    let worker: Awaited<ReturnType<typeof startCoreWorker>> | undefined;
    const startWorker = async () => {
      worker = await startCoreWorker(namespace, {
        databaseUrl: workerDatabaseUrl,
        identity: f.identity,
        entry: "dist/core-worker/main.js",
        env: { SETTLEMENT_CONCURRENCY: String(workerConcurrency) },
      });
      services.push(worker);
    };
    if (workerMode === "live") await startWorker();
    const names = [
      "gateway",
      "gatewayB",
      ...Array.from({ length: 6 }, (_, i) => `gateway${i + 3}`),
    ];
    const gateways = [];
    for (const instance of names.slice(0, instances)) {
      const gateway = await startGateway(f, {
        namespace,
        upstreamCertificate,
        instance,
        ...(replicatedShortChat && gateways.length % 2 === 1 ? { core: "coreB" } : {}),
        ...(replicatedShortChat ? { coreConnections: 4, pluginConcurrentAttempts } : {}),
        ...(projectCoreRouting
          ? ({ shortChatPeerParity: gateways.length % 2 === 0 ? 1 : 0 } as const)
          : {}),
        entry: "dist/gateway/main.js",
        overrides: {
          total_timeout_ms: requestTimeoutMs,
          shutdown_grace_ms: 20000,
          resource_poll_ms: 5000,
          max_inflight: gatewayInflight,
          ...(streamDurationSeconds ? { upstream_pool_size: upstreamConnectionsPerGateway } : {}),
          commit_short_chat: replicatedShortChat,
          ...(replicatedStreamTerminal ? { commit_short_chat_sse: true } : {}),
          ...(replicatedShortChat
            ? {
                terminal_recovery_queue: {
                  url: process.env.QUEUE_REDIS_URL,
                  namespace,
                  required_replicas: requiredReplicas,
                },
              }
            : {}),
          queue: {
            url: process.env.QUEUE_REDIS_URL,
            namespace,
            required_replicas: requiredReplicas,
          },
        },
      });
      services.push(gateway);
      gateways.push(gateway);
    }
    for (const table of [
      "projects",
      "api_keys",
      "channels",
      "models",
      "routes",
      "wallets",
      "wallet_lots",
      "upstream_account_balances",
    ])
      await db.unsafe(`ANALYZE ${table}`);
    const roots = {
      gateway: gateways.map((gateway) => gateway.pid),
      core_api:
        replicatedShortChat && instances > 1
          ? [f.identity.a.pid, f.identity.b.pid]
          : [f.identity.a.pid],
      core_worker: worker ? [worker.pid] : [],
      upstream: [upstream.pid],
    };
    const databaseUrl = new URL(process.env.DATABASE_URL!);
    databaseUrl.pathname = `/${(await db`SELECT current_database() AS name`)[0].name}`;
    databaseMetrics = await createDatabaseMetrics(databaseUrl.toString());
    const redisRoots = await discoverRedisProcesses({
      redis_admission: process.env.REDIS_URL!,
      redis_evidence: process.env.QUEUE_REDIS_URL!,
    });
    processSampler = startProcessSampler(async () => ({
      ...roots,
      ...redisRoots,
      ...(await databaseMetrics!.roots()),
    }));
    await databaseMetrics.resetOwned();
    const statementsBefore = await databaseMetrics.snapshot();
    await processSampler.start();
    const timeline: Record<string, unknown>[] = [];
    const samplingStart = Date.now();
    ackIndex = consumer ? await createCapacityAckIndex(directory) : undefined;
    const ackedRequests = ackIndex;
    let delivered = 0,
      receiptConfirmedAcks = 0,
      imprecise = 0,
      expiredLeases = 0,
      ackShortfall = 0,
      consumerErrors = 0,
      telemetryErrors = 0,
      lastStorageCheck = 0;
    const ackPath = `${directory}/ack.identities.tsv`;
    ackWriter = requireOnlineCapacity ? await createIdentityWriter(ackPath) : undefined;
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
                JSON.stringify({ reason: "storage_reserve_exhausted", storage }),
              );
              loadProcess?.kill("SIGTERM");
              throw new Error("Capacity disk reserve exhausted");
            }
          }
          const [stats, workerState, upstreamState, activity] = await Promise.all([
            queue.stats(),
            worker
              ? fetch(`http://127.0.0.1:${worker.port}/readyz`, {
                  signal: AbortSignal.timeout(2000),
                }).then(async (response) => ({
                  status: response.status,
                  ...((await response.json()) as Record<string, unknown>),
                }))
              : Promise.resolve(undefined),
            upstreamStats(),
            db<
              { locks: number; active: number }[]
            >`SELECT count(*) FILTER(WHERE wait_event_type='Lock')::int AS locks,count(*) FILTER(WHERE state='active')::int AS active FROM pg_stat_activity WHERE datname=current_database()`,
          ]);
          const waits = observe
            ? await db`SELECT coalesce(wait_event_type,'CPU') AS wait_event_type,coalesce(wait_event,'CPU') AS wait_event,count(*)::int FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND pid<>pg_backend_pid() GROUP BY 1,2 ORDER BY 3 DESC`
            : undefined;
          const at = Date.now();
          timeline.push({
            at: at - samplingStart,
            completed_at: at,
            queue: stats,
            worker: workerState,
            upstream_active: upstreamState.active ?? 0,
            lock_waiters: activity[0]!.locks,
            active_queries: activity[0]!.active,
            external_acked: delivered,
            receipt_confirmed_acks: receiptConfirmedAcks,
            acked_requests: ackedRequests?.size ?? 0,
            ...(waits ? { waits: [...waits] } : {}),
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
    const strictStream = stream && requireOnlineCapacity;
    const expectation = { charge: expectedCharge, cost: expectedCost, usage: expectedUsage };
    let acknowledgedProgress = 0;
    consuming = true;
    consume = (async () => {
      let leases: { token: string; at: number; checkAfter: number; events: UsageEvent[] }[] = [];
      while (consuming && consumer && ackedRequests) {
        try {
          const page = (await created(
            consumerRequest(`/consumers/${consumer}/claim`, { limit: 500 }),
            "Event claim",
            200,
          )) as { lease_token: string; events: UsageEvent[] };
          if (page.events.length)
            leases.push({
              token: page.lease_token,
              at: Date.now(),
              checkAfter: 0,
              events: page.events,
            });
          let progressed = false;
          const kept: typeof leases = [];
          for (const lease of leases) {
            if (lease.checkAfter > Date.now()) {
              kept.push(lease);
              continue;
            }
            const progress = strictStream
              ? await verifiedStreamProgress(db, lease.events)
              : new Set<string>();
            const terminalEvents = lease.events.filter((event) => !progress.has(event.id));
            const requestIds = terminalEvents.map((event) => event.request_id);
            const confirmed =
              strictStream && requestIds.length
                ? new Set(
                    (
                      await confirmedStreamRequests(db, requestIds, consumer, false, expectation)
                    ).map((row) => row.request_id),
                  )
                : await confirmedRequests(db, requestIds);
            const ready = lease.events.filter(
              (event) =>
                progress.has(event.id) ||
                (confirmed.has(event.request_id) && preciseEvidence(event)),
            );
            imprecise += terminalEvents.filter(
              (event) => confirmed.has(event.request_id) && !preciseEvidence(event),
            ).length;
            const remaining = terminalEvents.filter((event) => !confirmed.has(event.request_id));
            if (ready.length) {
              const { acknowledged } = (await created(
                consumerRequest(`/consumers/${consumer}/ack`, {
                  lease_token: lease.token,
                  event_ids: ready.map((event) => event.id),
                }),
                "Event ACK",
                200,
              )) as { acknowledged: number };
              delivered += acknowledged;
              if (acknowledged === ready.length) {
                acknowledgedProgress += progress.size;
                receiptConfirmedAcks += acknowledged;
                const touched = ackedRequests.unseen(ready.map((event) => event.request_id));
                const completed = !touched.length
                  ? []
                  : strictStream
                    ? await confirmedStreamRequests(db, touched, consumer, true, expectation)
                    : await db<{ request_id: string }[]>`SELECT a.request_id FROM attempts a
                      JOIN usage_event_index i ON i.attempt_id=a.id AND i.record_kind<>'RAW'
                      LEFT JOIN event_deliveries d ON d.consumer_id=${consumer} AND d.event_id=i.id
                      WHERE a.request_id=ANY(${touched}::uuid[]) GROUP BY a.request_id
                      HAVING bool_and(d.state IS NOT DISTINCT FROM 'ACKED')`;
                if (completed.length) {
                  const at = Date.now();
                  const fresh = ackedRequests.add(
                    completed.map((row) => row.request_id),
                    at,
                  );
                  if (fresh.length) await ackWriter?.write(fresh, at);
                }
              } else ackShortfall += ready.length - acknowledged;
              progressed = true;
            }
            lease.events = remaining;
            if (remaining.length && Date.now() - lease.at <= 25000) {
              lease.checkAfter = Date.now() + 50;
              kept.push(lease);
            } else expiredLeases += remaining.length;
          }
          leases = kept;
          if (!page.events.length && !progressed) await Bun.sleep(10);
        } catch {
          consumerErrors++;
          await Bun.sleep(50);
        }
      }
    })();
    const reportPath = `${directory}/http.json`;
    const loadOutput = Bun.file(`${directory}/load.log`);
    loadProcess = Bun.spawn(
      [
        process.execPath,
        "scripts/bench.ts",
        "--url",
        gateways.map((gateway) => gateway.origin).join(","),
        "--model",
        setup.model,
        "--seconds",
        String(seconds),
        ...(fillSeconds ? ["--fill-seconds", String(fillSeconds)] : []),
        "--concurrency",
        String(concurrency),
        "--warmup-concurrency",
        option("--warmup-concurrency", String(Math.min(256, concurrency))),
        ...(requireOnlineCapacity ? ["--record-request-ids"] : []),
        "--output",
        reportPath,
        "--hot-share",
        String(hotShare),
        ...(stream ? ["--stream"] : []),
        ...(upstreamChallenge ? ["--upstream-challenge"] : []),
        ...(fixedStreamText
          ? [
              "--expected-text",
              String(scenario.output ?? "Hello, 世界 🌍"),
              "--expected-text-repetitions",
              String(scenario.repeat ?? 1),
            ]
          : []),
        "--request-timeout-ms",
        String(requestTimeoutMs),
        ...(args.includes("--rate") ? ["--rate", option("--rate", "0")] : []),
      ],
      {
        env: { PATH: process.env.PATH, BENCH_API_KEYS: keys.join(",") },
        stdout: loadOutput,
        stderr: loadOutput,
      },
    );
    spawned.push(loadProcess);
    const exit = await loadProcess.exited;
    if (!(await Bun.file(reportPath).exists()))
      throw new Error(
        `Load generator exited ${exit} without a report; inspect ${directory}/load.log`,
      );
    const drainStart = Date.now();
    if (workerMode === "deferred") {
      await startWorker();
      roots.core_worker = worker ? [worker.pid] : [];
    }
    const withinDrain = () => Date.now() - drainStart < drainSeconds * 1000;
    while (withinDrain()) {
      const rows =
        await db`SELECT EXISTS(SELECT 1 FROM billing_holds WHERE state NOT IN ('SETTLED','RELEASED')) AS waiting`;
      if (!rows[0].waiting) break;
      await Bun.sleep(100);
    }
    while (consumer && withinDrain()) {
      const rows =
        await db`SELECT EXISTS(SELECT 1 FROM event_deliveries WHERE consumer_id=${consumer} AND state<>'ACKED') AS waiting`;
      if (!rows[0].waiting) break;
      await Bun.sleep(25);
    }
    let queueStats: Record<string, number> = {};
    let recoveryStats: Record<string, number> = {};
    let streamRecoveryStats: Record<string, number> = {};
    do {
      queueStats = await queue.stats();
      recoveryStats = (await recoveryQueue?.stats()) ?? {};
      streamRecoveryStats = (await streamRecoveryQueue?.stats()) ?? {};
      if (
        [
          ...Object.values(queueStats),
          ...Object.values(recoveryStats),
          ...Object.values(streamRecoveryStats),
        ].every((n) => n === 0)
      )
        break;
      await Bun.sleep(25);
    } while (withinDrain());
    const serviceDrainedAt = Date.now();
    consuming = false;
    await consume;
    await ackWriter?.close();
    clearInterval(telemetryTimer);
    await sampling;
    const measuredRequests = (await db`SELECT count(*)::int AS count FROM gateway_requests`)[0]
      .count as number;
    const processCpu = await processSampler.stop(Math.max(1, measuredRequests));
    const pgStatements = summarizeStatements(
      statementsBefore,
      await databaseMetrics.snapshot(),
      Math.max(1, measuredRequests),
    );
    const check = (
      await db.begin(async (audit) => {
        await audit`SET LOCAL statement_timeout=120000`;
        return audit`SELECT (SELECT count(*)::int FROM gateway_requests) AS requests,
          (SELECT count(DISTINCT a.request_id)::int FROM attempts a JOIN execution_permits p ON p.attempt_id=a.id JOIN execution_contexts c ON c.id=p.context_id WHERE c.snapshot?'candidate_digests') AS projected_requests,
          (SELECT count(*)::int FROM attempts) AS attempts,
          (SELECT count(*)::int FROM attempts WHERE state='SUCCEEDED') AS succeeded_attempts,
          (SELECT count(*)::int FROM usage_events) AS usage_events,
          (SELECT count(DISTINCT request_id)::int FROM usage_events) AS usage_requests,
          (SELECT count(*)::int FROM core_evidence_receipts WHERE outcome NOT IN ('ACCEPTED','STALE')) AS unexpected_receipts,
          (SELECT count(*)::int FROM usage_events WHERE finality='FINAL' AND usage=${expectedUsage}::jsonb AND cost_atoms=${expectedCost}::numeric AND cost_complete) AS precise_usage,
          (SELECT count(*)::int FROM billing_charges WHERE amount_atoms=${expectedCharge}::numeric AND funded_atoms=${expectedCharge}::numeric AND debt_atoms=0 AND refunded_atoms=0) AS precise_charges,
          (SELECT count(*)::int FROM billing_holds WHERE state='SETTLED') AS settled_holds,
          (SELECT sum(reserved_atoms)::text FROM wallets) AS reserved,
          (SELECT sum(spent_atoms)::text FROM wallets) AS spent,
          (SELECT sum(debt_atoms)::text FROM wallets) AS debt,
          (SELECT count(*)::int FROM event_deliveries WHERE state='ACKED') AS acknowledged,
          (SELECT count(*)::int FROM money_journal WHERE kind='SETTLE') AS settlement_journals,
          (SELECT count(*)::int FROM money_journal j WHERE (SELECT sum((v->>'atoms')::numeric) FROM jsonb_array_elements(j.lines) v)<>0) AS unbalanced_journals,
          (SELECT count(*)::int FROM upstream_cost_heads) AS supplier_cost_heads,
          (SELECT count(*)::int FROM gateway_requests q
            JOIN attempts a ON (a.id,a.request_id)=(q.selected_attempt_id,q.id)
            JOIN upstream_holds h ON h.attempt_id=a.id
            JOIN upstream_cost_heads ch ON ch.attempt_id=a.id
            JOIN upstream_costs c ON (c.attempt_id,c.revision)=(ch.attempt_id,ch.revision)
            JOIN (SELECT j.attempt_id,sum(e.atoms) AS spent,
                bool_and(j.account_id=owner.account_id AND j.channel_id IS NOT DISTINCT FROM owner.channel_id AND j.currency=owner.currency AND e.currency=owner.currency) AS owned
              FROM upstream_journals j JOIN upstream_entries e ON e.journal_id=j.id
              JOIN upstream_holds owner ON owner.attempt_id=j.attempt_id
              WHERE e.account='upstream:expense' GROUP BY j.attempt_id) ledger ON ledger.attempt_id=a.id
            JOIN usage_event_index evidence ON evidence.id=c.evidence_id AND evidence.attempt_id=a.id AND evidence.request_id=q.id
            WHERE h.state='SETTLED' AND h.currency='USD' AND h.current_reserved_atoms=0 AND h.final_cost_atoms=${expectedCost}::numeric
              AND c.complete AND NOT c.disputed AND c.cost_version_id=h.cost_version_id AND c.currency=h.currency
              AND c.cost_atoms=h.final_cost_atoms AND ledger.owned AND ledger.spent=c.cost_atoms
          ) AS exact_supplier_postings,
          (SELECT count(*)::int FROM usage_conflicts) AS conflicts,
          (SELECT count(*)::int FROM upstream_holds h WHERE h.current_reserved_atoms<>(SELECT coalesce(sum(e.atoms),0) FROM upstream_journals j JOIN upstream_entries e ON e.journal_id=j.id WHERE j.attempt_id=h.attempt_id AND e.account='upstream:reserved')) AS inconsistent_hold_reserves,
          (SELECT count(*)::int FROM core_upstream_balances_v1 b WHERE (core_funding.audit_v1(b.account_id,b.currency)->>'cells_conserved')::boolean IS DISTINCT FROM true) AS inconsistent_funding_budgets,
          (SELECT count(*)::int FROM core_funding.balance_cells c WHERE c.reserved_atoms<>(SELECT coalesce(sum(h.current_reserved_atoms),0) FROM upstream_holds h WHERE (h.account_id,h.currency,h.funding_cell)=(c.account_id,c.currency,c.cell))) AS inconsistent_cell_reserves,
          (SELECT count(*)::int FROM (SELECT journal_id FROM upstream_entries GROUP BY journal_id HAVING sum(atoms)<>0) j) AS unbalanced_supplier_journals`;
      })
    )[0] as Record<string, number | string>;
    const requests = Number(check.requests);
    const supplier = (
      await db<
        { balance: string; reserved: string; spent: string; accounts: number }[]
      >`SELECT sum(balance_atoms)::text AS balance,sum(reserved_atoms)::text AS reserved,sum(spent_atoms)::text AS spent,count(*)::int AS accounts FROM core_upstream_balances_v1 WHERE account_id=ANY(${accounts}::uuid[]) AND currency='USD'`
    )[0];
    const supplierSpent = BigInt(requests) * BigInt(expectedCost);
    const supplierExact =
      Number(supplier?.accounts) === accounts.length &&
      supplier?.reserved === "0" &&
      supplier.spent === String(supplierSpent) &&
      BigInt(supplier.balance) === BigInt(supplierCredit) * BigInt(accounts.length) - supplierSpent;
    const permits = [
      ...(await db`SELECT state,count(*)::int AS count FROM execution_permits GROUP BY state ORDER BY state`),
    ] as { state: string; count: number }[];
    const attemptFailures = [
      ...(await db`SELECT state,error_code,count(*)::int AS count FROM attempts
        WHERE error_code IS NOT NULL GROUP BY state,error_code ORDER BY count DESC,state,error_code LIMIT 25`),
    ];
    const executionRuntimes = [
      ...(await db`SELECT snapshot->'dispatch_projection'->'execution' execution,count(*)::int count FROM execution_permits GROUP BY 1 ORDER BY 1`),
    ];
    const usageShapes = [
      ...(await db`SELECT record_kind,snapshot_revision::text AS revision,finality,usage,cost_atoms::text AS cost,cost_complete,count(*)::int AS count FROM usage_events GROUP BY 1,2,3,4,5,6 ORDER BY 7 DESC LIMIT 20`),
    ];
    const receipts = [
      ...(await db`SELECT outcome,count(*)::int AS count FROM core_evidence_receipts GROUP BY outcome ORDER BY outcome`),
    ] as { outcome: string; count: number }[];
    const http = await Bun.file(reportPath).json();
    const ownership = (
      await db`SELECT count(*)::int AS mismatches FROM billing_charges c
      JOIN billing_holds h ON h.id=c.hold_id JOIN wallets w ON w.id=h.wallet_id
      JOIN gateway_requests q ON q.id=c.request_id LEFT JOIN usage_event_index e ON e.id=c.evidence_id
      WHERE h.request_id<>q.id OR w.project_id<>q.project_id OR c.currency<>w.currency
      OR (e.id IS NOT NULL AND e.request_id<>q.id)`
    )[0]!.mismatches as number;
    const walletMismatches = (
      await db`SELECT count(*)::int AS mismatches
      FROM unnest(${projects}::uuid[],${http.wallet_calls}::bigint[],${projects.map((project) => String(credited.get(project)))}::numeric[]) expected(project_id,calls,credited)
      LEFT JOIN wallets w ON w.project_id=expected.project_id AND w.currency='USD'
      WHERE w.id IS NULL OR w.reserved_atoms<>0 OR w.debt_atoms<>0
      OR w.spent_atoms<>expected.calls*${expectedCharge}::numeric
      OR w.available_atoms<>expected.credited-expected.calls*${expectedCharge}::numeric`
    )[0]!.mismatches as number;
    const admittedWalletMismatches = (
      await db`WITH admitted AS (
        SELECT project_id,count(*) AS requests FROM gateway_requests GROUP BY project_id
      ) SELECT count(*)::int AS mismatches
      FROM unnest(${projects}::uuid[],${projects.map((project) => String(credited.get(project)))}::numeric[]) expected(project_id,credited)
      LEFT JOIN admitted a USING(project_id)
      LEFT JOIN wallets w ON w.project_id=expected.project_id AND w.currency='USD'
      WHERE w.id IS NULL OR w.reserved_atoms<>0 OR w.debt_atoms<>0
       OR w.spent_atoms<>coalesce(a.requests,0)*${expectedCharge}::numeric
       OR w.available_atoms<>expected.credited-coalesce(a.requests,0)*${expectedCharge}::numeric`
    )[0]!.mismatches as number;
    const windowStart = Date.parse(http.measurement_started_at);
    const windowEnd = windowStart + seconds * 1000;
    const fillSample = streamDurationSeconds
      ? timeline.findLast((sample) => Number(sample.completed_at) <= windowStart)
      : undefined;
    const upstreamAtWindowStart = Number(fillSample?.upstream_active ?? 0);
    const fillSampleAgeMs = windowStart - Number(fillSample?.completed_at ?? 0);
    const identityAudit = requireOnlineCapacity
      ? await auditCapacityIdentities(
          http.identity_log,
          ackPath,
          windowStart,
          seconds,
          stream ? http.dispatch_log : undefined,
          maxP99Ms,
          streamDurationSeconds
            ? {
                arrivalsStartedAt: Date.parse(http.arrivals_started_at),
                minimumStreamMs: streamDurationSeconds * 1000,
              }
            : undefined,
        )
      : undefined;
    const expectedAcks = consumerMode === "live" ? Number(check.usage_events) : 0;
    const upstreamResult = await upstreamStats();
    const failures: string[] = [];
    const require = (condition: boolean, label: string) => {
      if (!condition) failures.push(label);
    };
    require(exit === 0, "load_generator_exit");
    require(Number(check.attempts) === requests, "extra_or_missing_attempts");
    require(Number(check.succeeded_attempts) === requests, "unsuccessful_attempts");
    require(Number(check.usage_requests) === requests, "requests_without_usage");
    require(Number(check.precise_usage) + acknowledgedProgress ===
      Number(check.usage_events), "imprecise_usage");
    require(Number(check.unexpected_receipts) === 0, "unexpected_evidence_receipts");
    require(Number(check.precise_charges) === requests, "imprecise_customer_charges");
    require(Number(check.settled_holds) === requests, "unsettled_holds");
    require(Number(check.settlement_journals) === requests, "customer_settlement_journals");
    require(Number(check.acknowledged) === expectedAcks, "external_acks");
    require(check.reserved === "0" && check.debt === "0", "customer_reservations_or_debt");
    require(BigInt(check.spent ?? "0") ===
      BigInt(requests) * BigInt(expectedCharge), "customer_spent");
    require(Number(check.unbalanced_journals) === 0, "unbalanced_customer_journals");
    require(Number(check.supplier_cost_heads) === requests, "supplier_cost_heads");
    require(Number(check.exact_supplier_postings) === requests, "per_request_supplier_postings");
    require(supplierExact, "supplier_ledger");
    require(Number(check.inconsistent_hold_reserves) === 0, "supplier_hold_reserves");
    require(Number(check.inconsistent_funding_budgets) === 0, "supplier_budget_transfers");
    require(Number(check.inconsistent_cell_reserves) === 0, "supplier_cell_reserves");
    require(Number(check.unbalanced_supplier_journals) === 0, "unbalanced_supplier_journals");
    require(Number(check.conflicts) === 0, "evidence_conflicts");
    require(ownership === 0, "charge_ownership");
    require(executionRuntimes.length === 1 &&
      canonical(executionRuntimes[0]!.execution) ===
        canonical(runtimeApproval.execution), "execution_runtime_binding");
    require(walletMismatches === 0, "wallet_balances");
    require(admittedWalletMismatches === 0, "admitted_wallet_balances");
    require(http.invalid === 0 &&
      !(http.shed ?? 0) &&
      http.completed === http.succeeded, "http_validity");
    if (stream) {
      if (requireOnlineCapacity)
        require(!!identityAudit?.completionLatencyMs, "stream_dispatch_evidence");
      require(upstreamResult.requests ===
        http.succeeded + http.warmup_requests, "upstream_request_count");
      require((!upstreamChallenge || http.sse.upstream_challenge === true) &&
        http.sse.response_headers === http.offered &&
        http.sse.first_frame === http.offered &&
        http.sse.done === http.offered &&
        http.sse.error_frames === 0 &&
        (!fixedStreamText || http.sse.first_content === http.offered), "sse_completion");
    }
    if (streamDurationSeconds) {
      require(fillSampleAgeMs <= 1000 &&
        upstreamAtWindowStart >= targetRps * streamDurationSeconds, "stream_fill_upstream");
      require(identityAudit?.longStream?.shortStreams === 0, "shortened_streams");
      require(upstreamResult.active === 0 &&
        upstreamResult.cancellations === 0, "upstream_stream_cleanup");
    }
    if (args.includes("--rate")) {
      const planned = (seconds + fillSeconds) * offeredRps;
      require(http.offered === planned &&
        http.completed + (http.shed ?? 0) === planned, "planned_arrivals");
    }
    require(requests === http.succeeded + http.warmup_requests, "request_count");
    require(Object.values(queueStats).every((n) => n === 0), "evidence_queue_backlog");
    if (replicatedStreamTerminal)
      require(Object.values(streamRecoveryStats).every(
        (n) => n === 0,
      ), "stream_terminal_recovery_backlog");
    require(Object.values(recoveryStats).every((n) => n === 0), "terminal_recovery_backlog");
    require(telemetryErrors === 0 &&
      timeline.length >= Math.max(1, Math.floor(seconds / 2)), "telemetry");
    require(consumerErrors === 0 &&
      imprecise === 0 &&
      expiredLeases === 0 &&
      ackShortfall === 0 &&
      receiptConfirmedAcks === expectedAcks &&
      (ackedRequests?.size ?? 0) ===
        (consumerMode === "live" ? requests : 0), "receipt_confirmed_acks");
    if (identityAudit)
      require(identityAudit.matchedRequests === http.succeeded &&
        identityAudit.httpOnly === 0 &&
        identityAudit.ackOnly === http.warmup_requests, "identity_intersection");
    require(processCpu.errors === 0, "process_cpu_sampling");
    require(pgStatements.valid, "pg_statement_statistics");
    const valid = failures.length === 0;
    const withinWindow = timeline.findLast(
      (sample) => Number(sample.completed_at ?? Infinity) <= windowEnd,
    );
    const onlineCapacity = identityAudit
      ? {
          ...identityAudit,
          maxP99Ms,
          ...(streamDurationSeconds ? { upstreamAtWindowStart, fillSampleAgeMs } : {}),
          drainAfterWindowMs: Math.max(0, serviceDrainedAt - windowEnd),
          receiptAckedRequestsByWindow:
            Number(withinWindow?.acked_requests ?? 0) -
            Number(fillSample?.acked_requests ?? http.warmup_requests),
          ackedEventsByWindow:
            Number(withinWindow?.external_acked ?? 0) - Number(fillSample?.external_acked ?? 0),
          sampledBeforeWindowEndMs: windowEnd - Number(withinWindow?.completed_at ?? 0),
          httpByWindow: http.minute_windows?.["0"]?.succeeded ?? 0,
        }
      : undefined;
    let capacityPassed: boolean | undefined;
    if (streamDurationSeconds) {
      capacityPassed =
        !!onlineCapacity &&
        passesLongStreamCapacity(
          valid,
          targetRps,
          seconds,
          streamDurationSeconds,
          onlineCapacity,
          onlineCapacity.drainAfterWindowMs,
        );
    } else if (targetRps > 0) {
      capacityPassed = onlineCapacity
        ? passesOnlineCapacity(
            valid,
            http,
            targetRps,
            seconds,
            onlineCapacity.minuteCompletions,
            maxP99Ms,
            onlineCapacity.drainAfterWindowMs,
            onlineCapacity.completionLatencyMs,
          )
        : passesCapacityTarget(valid, http, targetRps);
    }
    const result = {
      id,
      scope: "v2-three-process",
      environment,
      valid,
      failures,
      capacityPassed,
      targetRps,
      seconds,
      instances,
      wallets,
      hotShare,
      supplierAccounts,
      pluginRuntime,
      runtimeApproval,
      executionRuntimes,
      concurrency,
      stream,
      ...(streamDurationSeconds ? { streamDurationSeconds, fillSeconds } : {}),
      requestTimeoutMs,
      upstreamMode,
      scenario,
      workerMode,
      workerConcurrency,
      consumerMode,
      gatewayInflight,
      requiredReplicas,
      replicatedShortChat,
      replicatedStreamTerminal,
      projectCoreRouting,
      coreConnectionsPerGateway: replicatedShortChat ? 4 : 1,
      pluginConcurrentAttempts,
      recoveryStats,
      ...(replicatedStreamTerminal ? { streamRecoveryStats } : {}),
      drainSeconds,
      expectedChargeAtoms: expectedCharge,
      expectedSupplierCostAtoms: expectedCost,
      storageBudget,
      databaseSettings: [...databaseSettings],
      redisSettings,
      accounting: check,
      supplier,
      permits,
      attemptFailures,
      receipts,
      usageShapes,
      ownershipMismatches: ownership,
      walletMismatches,
      admittedWalletMismatches,
      delivered,
      receiptConfirmedAcks,
      ackedRequests: ackedRequests?.size ?? 0,
      imprecise,
      ...(strictStream ? { verified_progress_acks: acknowledgedProgress } : {}),
      expiredLeases,
      ackShortfall,
      consumerErrors,
      telemetryErrors,
      processes: processCpu.processes,
      processCpu,
      pgStatements,
      resources: processCpu.samples,
      onlineCapacity,
      queueStats,
      timeline,
      upstream: upstreamResult,
      http,
      drain_ms: serviceDrainedAt - drainStart,
      financial_duration_seconds: (serviceDrainedAt - samplingStart) / 1000,
      financial_completed_rps:
        Math.round((requests * 100000) / (serviceDrainedAt - samplingStart)) / 100,
      database: String((await db`SELECT current_database() AS name`)[0].name),
      database_bytes: Number(
        (await db`SELECT pg_database_size(current_database())::text AS size`)[0].size,
      ),
    };
    for (const service of [...services].reverse()) if (service.alive()) await service.stop();
    return result;
  } finally {
    consuming = false;
    clearInterval(telemetryTimer);
    try {
      await processSampler?.close();
      await databaseMetrics?.close();
      for (const child of spawned) if (child.exitCode === null) child.kill("SIGTERM");
      for (const service of services) service.kill();
      await Promise.allSettled(spawned.map((child) => child.exited));
      await Promise.allSettled([consume, sampling]);
      await consumerServer?.stop(true);
      await ackWriter?.close().catch(() => undefined);
      queue.close();
      recoveryQueue?.close();
      streamRecoveryQueue?.close();
    } finally {
      await ackIndex?.close();
    }
  }
}

let result: Awaited<ReturnType<typeof measure>> | undefined;
const native = replicatedShortChat
  ? await createNativeFixture({
      ...nativeFixtureOptions(),
      diskBudgetBytes: Math.max(8 * 1024 ** 3, storageBudget.estimated),
      truncateProcessOutput: true,
    })
  : undefined;
try {
  let standbyUrl: string | undefined;
  let witnessUrl: string | undefined;
  if (native) {
    const primary = await native.startPostgres(true);
    standbyUrl = (await native.startPostgresStandby(primary)).url;
    const admission = await native.startRedis("capacity-admission", undefined, true);
    const evidence = await native.startRedis("capacity-evidence", undefined, true);
    const replica = await native.startRedis("capacity-evidence-replica", evidence, true);
    await native.waitFor(async () =>
      (await replica.command("INFO", "replication")).includes("master_link_status:up"),
    );
    process.env.DATABASE_URL = primary.url;
    process.env.REDIS_URL = admission.url;
    process.env.QUEUE_REDIS_URL = evidence.url;
    const metrics = new SQL(primary.url, { max: 1 });
    try {
      await metrics`CREATE EXTENSION pg_stat_statements`;
    } finally {
      await metrics.close({ timeout: 1 });
    }
  }
  execFileSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-days",
      "1",
      "-subj",
      "/CN=127.0.0.1",
      "-addext",
      "subjectAltName=IP:127.0.0.1",
      "-keyout",
      upstreamKey,
      "-out",
      upstreamCertificate,
    ],
    { stdio: "pipe" },
  );
  upstreamTls.ca = await Bun.file(upstreamCertificate).text();
  await withFinancial(
    (f, db) =>
      withSettlementRole(db, async (workerDatabaseUrl) => {
        result = await measure(f, db, workerDatabaseUrl);
      }),
    {
      entry: "dist/core-api/main.js",
      managementFactory: compiled.createCoreManagementApplication,
      gateways: Math.max(2, instances),
      ...(standbyUrl
        ? {
            managementStandby: () => new SQL(witnessUrl!, { max: 4, connectionTimeout: 2 }),
            async terminalSettlement(db: SQL) {
              const [{ name }] = await db`SELECT current_database() AS name`;
              await db.unsafe(await Bun.file("infra/core-terminal-settlement-roles.sql").text());
              const password = crypto.randomUUID().replaceAll("-", "");
              await db.unsafe(
                `CREATE ROLE capacity_terminal_write LOGIN PASSWORD '${password}' IN ROLE core_terminal_writer`,
              );
              await db.unsafe(
                `CREATE ROLE capacity_terminal_read LOGIN PASSWORD '${password}' IN ROLE core_terminal_confirmation`,
              );
              const writer = new URL(process.env.DATABASE_URL!);
              writer.pathname = `/${name}`;
              writer.username = "capacity_terminal_write";
              writer.password = password;
              const reader = new URL(standbyUrl);
              reader.pathname = writer.pathname;
              reader.username = "capacity_terminal_read";
              reader.password = password;
              witnessUrl = reader.toString();
              return {
                database_url: writer.toString(),
                standby_url: witnessUrl,
                short_chat_funding_epoch: 1,
                ...(replicatedStreamTerminal ? { commit_short_chat_sse: true } : {}),
                recovery_queue: {
                  url: process.env.QUEUE_REDIS_URL!,
                  namespace,
                  required_replicas: 1,
                  ack_timeout_ms: 1000,
                },
              };
            },
          }
        : {}),
    },
  );
} catch (error) {
  native?.assertHealthy();
  throw error;
} finally {
  await rm(upstreamKey, { force: true });
  const cleanup = await cleanupCapacityNamespaces(
    process.env.QUEUE_REDIS_URL ? [process.env.QUEUE_REDIS_URL] : [],
    namespace,
  ).finally(() => native?.close());
  await Bun.write(`${directory}/cleanup.json`, JSON.stringify(cleanup) + "\n");
  if (cleanup.failed) {
    process.exitCode = 1;
    console.error("Capacity fixture cleanup could not reach the evidence queue");
  }
}
if (!result) throw new Error("Capacity run produced no audit");
await Bun.write(`${directory}/audit.json`, JSON.stringify(result, null, 2));
console.log(
  JSON.stringify({
    directory,
    scope: result.scope,
    valid: result.valid,
    failures: result.failures,
    rps: result.http.successful_rps,
    targetRps,
    capacityPassed: result.capacityPassed,
    financial_completed_rps: result.financial_completed_rps,
    accounting: result.accounting,
    supplier: result.supplier,
    drain_ms: result.drain_ms,
  }),
);
if (!result.valid || result.capacityPassed === false) process.exitCode = 1;
