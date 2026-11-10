/** Destructive faults target only disposable containers, volumes, databases and namespaces owned by this run. */
import assert from "node:assert/strict";
import { mkdir, rm } from "node:fs/promises";
import { RedisClient } from "bun";
import { UsageProducer, UsageConsumer } from "@gateway/runtime";
import { createDatabase, migrate, Store, FundsStore } from "@gateway/storage";
import type { Credential, UsageEvidence } from "@gateway/kernel";

const until = async (predicate: () => Promise<boolean>, timeout = 15000) => {
  const end = Date.now() + timeout;
  while (!(await predicate())) {
    if (Date.now() > end) throw new Error("Durability fault condition timed out");
    await Bun.sleep(25);
  }
};
if (process.env.DURABILITY_CHILD === "true") {
  const db = createDatabase(process.env.DATABASE_URL!, 2);
  const store = new Store(db);
  const consumer = new UsageConsumer({
    url: process.env.QUEUE_REDIS_URL!,
    namespace: process.env.REDIS_NAMESPACE!,
    replicas: 1,
    timeoutMs: 1000,
    concurrency: 1,
    lockDurationMs: 1000,
    stalledIntervalMs: 1000,
    process: async (events) => {
      if (process.env.DURABILITY_PHASE === "BEFORE_COMMIT") {
        await db.begin(async (tx) => {
          const rows = await tx`SELECT persist_usage_batch(${events}::jsonb) AS result`;
          assert(
            Object.values(rows[0].result as Record<string, { status: string }>).every(
              (r) => r.status !== "ERROR",
            ),
          );
          await Bun.write(
            process.env.DURABILITY_MARKER!,
            "uncommitted evidence and ledger written",
          );
          await new Promise(() => {});
        });
      }
      await store.persistUsage(events);
      await Bun.write(process.env.DURABILITY_MARKER!, "reached");
      await new Promise(() => {});
    },
  });
  await consumer.ready();
  await new Promise(() => {});
}
const base = process.env.DATABASE_URL;
if (!base) throw new Error("DATABASE_URL is required");
const id = crypto.randomUUID().replaceAll("-", "");
const database = `gateway_durability_${id}`;
const label = `gateway-durability-${id}`;
const directory = new URL(`../.local/${label}/`, import.meta.url);
await mkdir(directory, { recursive: true });
const freePort = async () => {
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response() });
  const port = server.port!;
  await server.stop(true);
  return port;
};
const [primaryPort, replicaPort] = await Promise.all([freePort(), freePort()]);
const primaryURL = `redis://127.0.0.1:${primaryPort}`;
const replicaURL = `redis://127.0.0.1:${replicaPort}`;
const image =
  "redis:7-alpine@sha256:7b1753b4c5ad8458906c272a8132b8c15f5557b000379eb115fb239706a384b7";
const docker = async (...args: string[]) => {
  const child = Bun.spawn(["docker", ...args], { stdout: "pipe", stderr: "pipe" });
  const output = new Response(child.stdout).text();
  const errors = new Response(child.stderr).text();
  const code = await child.exited;
  await errors;
  if (code !== 0) throw new Error(`Fixture Docker command failed: ${args[0]}`);
  return (await output).trim();
};
const native = Bun.argv.includes("--native");
const nodes = new Map<
  string,
  { args: string[]; child: ReturnType<typeof Bun.spawn>; starts: number; stopped: boolean }
>();
const primary = `${label}-primary`;
const replica = `${label}-replica`;
async function redisCommand(name: string, ...args: string[]): Promise<string> {
  if (!native) return docker("exec", name, "redis-cli", ...args);
  const node = nodes.get(name);
  if (!node || node.stopped || node.child.exitCode !== null || node.child.signalCode !== null)
    throw new Error("Fixture Redis is not running");
  const client = new RedisClient(name === primary ? primaryURL : replicaURL, {
    autoReconnect: false,
    connectionTimeout: 500,
  });
  try {
    await client.connect();
    const info = String(await client.send("INFO", ["server"]));
    if (Number(/^process_id:(\d+)\r?$/m.exec(info)?.[1]) !== node.child.pid)
      throw new Error("Fixture Redis ownership mismatch");
    return String(await client.send(args[0]!, args.slice(1)));
  } finally {
    client.close();
  }
}
async function startNode(name: string): Promise<void> {
  if (!native) {
    await docker("start", name);
    return;
  }
  const node = nodes.get(name);
  if (!node || !node.stopped) throw new Error("Fixture Redis cannot restart");
  node.starts++;
  const log = Bun.file(new URL(`${name}-${node.starts}.log`, directory));
  node.child = Bun.spawn(node.args, { stdout: log, stderr: log });
  node.stopped = false;
}
async function killNode(name: string): Promise<void> {
  if (!native) {
    await docker("kill", name);
    return;
  }
  const node = nodes.get(name);
  if (!node || node.stopped) throw new Error("Fixture Redis cannot be killed");
  node.child.kill("SIGKILL");
  await node.child.exited;
  node.stopped = true;
}
const waitRedis = async (name: string) =>
  until(async () => {
    try {
      return (await redisCommand(name, "PING")) === "PONG";
    } catch {
      return false;
    }
  });
let checkpoint = "fixture initialization";
const owner = createDatabase(base, 2);
let db: ReturnType<typeof createDatabase> | undefined;
let created = false;
let consumer: UsageConsumer | undefined;
let producer: UsageProducer | undefined;
let child: ReturnType<typeof Bun.spawn> | undefined;
const checks: string[] = [];
let failureCause: unknown;
const cleanupErrors: unknown[] = [];
let outcome: string | undefined;
try {
  if (!native) await docker("network", "create", label);
  const common = [
    "redis-server",
    "--save",
    "",
    "--appendonly",
    "yes",
    "--appendfsync",
    "always",
    "--aof-load-truncated",
    "no",
    "--maxmemory-policy",
    "noeviction",
    "--repl-diskless-sync-delay",
    "0",
  ];
  for (const [name, port, extra] of [
    [primary, primaryPort, []],
    [replica, replicaPort, ["--replicaof", primary, "6379"]],
  ] as const) {
    if (native) {
      const data = new URL(`${name}/`, directory);
      await mkdir(data, { recursive: true });
      const command = [
        ...common,
        "--bind",
        "127.0.0.1",
        "--port",
        String(port),
        "--dir",
        data.pathname,
        ...(name === replica ? ["--replicaof", "127.0.0.1", String(primaryPort)] : []),
      ];
      const log = Bun.file(new URL(`${name}-0.log`, directory));
      nodes.set(name, {
        args: command,
        child: Bun.spawn(command, { stdout: log, stderr: log }),
        starts: 0,
        stopped: false,
      });
    } else {
      await docker(
        "run",
        "-d",
        "--name",
        name,
        "--network",
        label,
        "-p",
        `127.0.0.1:${port}:6379`,
        "-v",
        `${name}:/data`,
        image,
        ...common,
        ...extra,
      );
    }
  }
  await waitRedis(primary);
  await waitRedis(replica);
  await until(async () => {
    try {
      return (await redisCommand(replica, "INFO", "replication")).includes("master_link_status:up");
    } catch {
      return false;
    }
  });
  const strictProbe = new UsageProducer({
    url: primaryURL,
    namespace: `${label}:topology`,
    replicas: 1,
    timeoutMs: 1000,
    requireAllReplicas: true,
  });
  checkpoint = "strict production topology bootstrap";
  await until(async () => {
    try {
      await strictProbe.ready();
      return true;
    } catch {
      return false;
    }
  });
  checkpoint = "live durability configuration drift";
  await redisCommand(primary, "CONFIG", "SET", "maxmemory-policy", "allkeys-lru");
  await Bun.sleep(110);
  await assert.rejects(strictProbe.ready());
  strictProbe.close();
  await redisCommand(primary, "CONFIG", "SET", "maxmemory-policy", "noeviction");
  const unacknowledgedCandidate = new UsageProducer({
    url: primaryURL,
    namespace: `${label}:topology`,
    replicas: 0,
    timeoutMs: 1000,
    requireAllReplicas: true,
  });
  await assert.rejects(unacknowledgedCandidate.ready());
  unacknowledgedCandidate.close();
  checks.push(
    "production topology acknowledges every connected candidate and rejects live persistence drift",
  );
  await owner.unsafe(`CREATE DATABASE ${database}`);
  created = true;
  const url = new URL(base);
  url.pathname = `/${database}`;
  db = createDatabase(url.toString(), 4);
  await migrate(db);
  const store = new Store(db);
  const funds = new FundsStore(db);
  const project = await store.createProject({
    name: id,
    rpm: 1000,
    tpm: 1000000,
    concurrency: 100,
  });
  const key = await store.createCredential(
    project,
    { name: "faults", models: [], operations: ["chat"] },
    id,
    "fixture",
  );
  await funds.credit(project, {
    currency: "USD",
    external_ref: "fixture",
    amount_atoms: "10000",
    kind: "CASH",
    priority: 100,
    models: [],
    operations: [],
  });
  const provider = crypto.randomUUID();
  await store.createProvider(
    provider,
    {
      name: id,
      kind: "openai",
      base_url: "https://fixture.invalid/v1",
      rpm: 1000,
      concurrency: 100,
    },
    "fixture",
  );
  await store.createModel({ name: id, operations: ["chat"], max_output_tokens: 100 });
  const rates = {
    input_tokens: { numerator: "1", denominator: "1" },
    output_tokens: { numerator: "1", denominator: "1" },
  };
  await store.createRoute(id, {
    provider_id: provider,
    upstream_model: id,
    operations: ["chat"],
    priority: 1,
    weight: 1,
    pricing: { currency: "USD", rates },
  });
  const route = (await store.routes(id, "chat"))[0]!;
  const tariff = await funds.setTariff({
    model: id,
    operation: "chat",
    basis: "PROVIDER_USAGE",
    pricing: { currency: "USD", rates },
    ceilings: { input_tokens: "100", output_tokens: "100" },
  });
  const evidence = async (): Promise<UsageEvidence> => {
    const admitted = await store.admitInitial(
      crypto.randomUUID(),
      { id: key, project_id: project } as Credential,
      "chat",
      id,
      null,
      "fixture",
      route,
      {
        currency: "USD",
        amount_atoms: "100",
        input_characters: "0",
        prices: { [provider]: tariff.pricing_id },
      },
    );
    return {
      attemptId: admitted.attempt_id!,
      source: "execution",
      state: "SUCCEEDED",
      finality: "FINAL",
      usage: { input_tokens: "7", output_tokens: "5" },
      requestState: "SUCCEEDED",
    };
  };
  let expected = 0;
  const consume = async (namespace: string, endpoint: string) => {
    consumer = new UsageConsumer({
      url: endpoint,
      namespace,
      replicas: 0,
      timeoutMs: 1000,
      concurrency: 1,
      lockDurationMs: 1000,
      stalledIntervalMs: 1000,
      process: (events) => store.persistUsage(events),
    });
    await consumer.ready();
    await until(
      async () =>
        Number(
          (
            await db!`SELECT count(*)::int AS n FROM billing_charges WHERE amount_atoms=12 AND funded_atoms=12`
          )[0].n,
        ) === expected,
    );
    const inspector = new UsageProducer({ url: endpoint, namespace, replicas: 0, timeoutMs: 1000 });
    await until(async () => {
      const counts = await inspector.stats();
      return Object.values(counts).every((n) => n === 0);
    });
    inspector.close();
    await consumer.close();
    consumer = undefined;
  };
  // Deterministic SIGKILL at both transaction/queue acknowledgement boundaries.
  for (const phase of ["BEFORE_COMMIT", "AFTER_COMMIT"]) {
    checkpoint = phase;
    const namespace = `${label}:${phase}`;
    const marker = new URL(`${phase}.marker`, directory).pathname;
    producer = new UsageProducer({ url: primaryURL, namespace, replicas: 1, timeoutMs: 1000 });
    child = Bun.spawn([process.execPath, import.meta.path], {
      env: {
        ...process.env,
        DURABILITY_CHILD: "true",
        DURABILITY_PHASE: phase,
        DURABILITY_MARKER: marker,
        DATABASE_URL: url.toString(),
        QUEUE_REDIS_URL: primaryURL,
        REDIS_NAMESPACE: namespace,
      },
      stdout: Bun.file(new URL(`${phase}.log`, directory)),
      stderr: "ignore",
    });
    await producer.publish(await evidence());
    await until(async () => Bun.file(marker).exists());
    child.kill("SIGKILL");
    await child.exited;
    child = undefined;
    expected++;
    if (phase === "AFTER_COMMIT")
      assert.equal(
        Number((await db`SELECT count(*)::int AS n FROM billing_charges`)[0].n),
        expected,
      );
    await consume(namespace, primaryURL);
    producer.close();
    producer = undefined;
    assert.equal(
      Number((await db`SELECT count(*)::int AS n FROM money_journal WHERE kind='SETTLE'`)[0].n),
      expected,
    );
    checks.push(`SIGKILL ${phase}: recovered exactly once`);
  }
  const namespace = `${label}:AOF`;
  producer = new UsageProducer({ url: primaryURL, namespace, replicas: 1, timeoutMs: 1000 });
  await producer.publish(await evidence());
  expected++;
  assert.equal((await producer.stats()).waiting, 1);
  producer.close();
  producer = undefined;
  // Both copies are killed without a graceful save, then loaded from their own AOF volumes.
  await Promise.all([killNode(primary), killNode(replica)]);
  // Recover each copy independently: the replica must never be able to refill from the primary.
  checkpoint = "primary AOF restart";
  await startNode(primary);
  await waitRedis(primary);
  producer = new UsageProducer({ url: primaryURL, namespace, replicas: 0, timeoutMs: 1000 });
  assert.equal((await producer.stats()).waiting, 1);
  producer.close();
  producer = undefined;
  checks.push("primary hard restart retains acknowledged evidence from its AOF");
  await killNode(primary);
  checkpoint = "replica AOF promotion";
  await startNode(replica);
  await waitRedis(replica);
  await redisCommand(replica, "REPLICAOF", "NO", "ONE");
  await consume(namespace, replicaURL);
  checks.push("acknowledged replica promotion retains evidence and exact settlement");
  checkpoint = "unsafe ingress rejection";
  producer = new UsageProducer({ url: replicaURL, namespace, replicas: 1, timeoutMs: 200 });
  await assert.rejects(producer.ready());
  producer.close();
  producer = undefined;
  checks.push("promoted node without required durable replica rejects admission");
  await redisCommand(replica, "CONFIG", "SET", "appendfsync", "everysec");
  producer = new UsageProducer({ url: replicaURL, namespace, replicas: 0, timeoutMs: 200 });
  await assert.rejects(producer.ready());
  producer.close();
  producer = undefined;
  await redisCommand(replica, "CONFIG", "SET", "appendfsync", "always");
  checks.push("unsafe persistence configuration rejects admission");
  producer = new UsageProducer({ url: replicaURL, namespace, replicas: 0, timeoutMs: 1000 });
  await assert.rejects(
    producer.publish({
      ...(await evidence()),
      body: "PRIVATE_LLM_CONTENT_SENTINEL",
    } as UsageEvidence),
  );
  producer.close();
  producer = undefined;
  const wallet = (
    await db`SELECT available_atoms::text,reserved_atoms::text,spent_atoms::text,debt_atoms::text FROM wallets`
  )[0];
  assert.equal(wallet.spent_atoms, String(expected * 12));
  assert.equal(wallet.debt_atoms, "0");
  assert.equal(
    wallet.reserved_atoms,
    "100",
    "Invalid evidence cannot silently release an already-created hold",
  );
  assert.equal(Number((await db`SELECT count(*)::int AS n FROM usage_events`)[0].n), expected);
  assert.equal(
    Number((await db`SELECT count(*)::int AS n FROM money_journal WHERE kind='SETTLE'`)[0].n),
    expected,
  );
  checks.push("strict usage payload rejects body; all monetary journals remain singular");
  outcome = JSON.stringify({
    durability: "pass",
    fixture: native ? "native" : "docker",
    checks,
    charges: expected,
    wallet,
  });
} catch (error) {
  failureCause = new Error(`Durability fixture failed at ${checkpoint}`, { cause: error });
} finally {
  async function clean(action: () => unknown | Promise<unknown>) {
    try {
      await action();
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
  await clean(async () => {
    if (child && child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
  });
  await clean(() => consumer?.close());
  await clean(() => producer?.close());
  await clean(() => db?.close());
  await clean(async () => {
    if (created) await owner.unsafe(`DROP DATABASE ${database} WITH (FORCE)`);
  });
  await clean(() => owner.close());
  if (native) {
    for (const node of nodes.values())
      await clean(async () => {
        if (node.child.exitCode === null && node.child.signalCode === null)
          node.child.kill("SIGKILL");
        await node.child.exited;
        node.stopped = true;
      });
  } else {
    for (const name of [replica, primary]) {
      await docker("rm", "-f", name).catch(() => undefined);
      await docker("volume", "rm", name).catch(() => undefined);
    }
    await docker("network", "rm", label).catch(() => undefined);
  }
  if (!native || [...nodes.values()].every((node) => node.stopped))
    await clean(() => rm(directory, { recursive: true, force: true }));
}

if (failureCause || cleanupErrors.length)
  throw new AggregateError(
    failureCause ? [failureCause, ...cleanupErrors] : cleanupErrors,
    "Durability fixture failed",
  );
if (!outcome) throw new Error("Durability fixture completed without evidence");
console.log(outcome);
