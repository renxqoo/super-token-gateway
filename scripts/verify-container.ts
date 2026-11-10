/** Runs the same compiled gateway, worker and mock in separate least-privilege Linux containers. */
import { createDatabase, migrate, Store, FundsStore } from "@gateway/storage";
import { createKey, encrypt, verifier } from "@gateway/kernel";
import { mkdir, unlink, chmod } from "node:fs/promises";
import { RedisClient } from "bun";
import assert from "node:assert/strict";
const base = process.env.DATABASE_URL ?? "";
const url = new URL(base);
if (!["localhost", "127.0.0.1"].includes(url.hostname) || url.port !== "55442")
  throw new Error("Container verification requires the dedicated local PostgreSQL fixture");
if (!process.env.REDIS_URL || !process.env.QUEUE_REDIS_URL)
  throw new Error("Both Redis endpoints are required");
const suffix = crypto.randomUUID().replaceAll("-", "");
const database = `gateway_linux_${suffix}`;
const namespace = `linux:${suffix}`;
const network = `gateway-linux-${suffix}`;
const mockName = `${network}-mock`;
const owner = createDatabase(base, 2);
await owner.unsafe(`CREATE DATABASE ${database}`);
url.pathname = `/${database}`;
const db = createDatabase(url.toString(), 4);
const admin = crypto.randomUUID() + crypto.randomUUID();
const encryption = "44".repeat(32);
const pepper = suffix;
const remote = (value: string) => {
  const result = new URL(value);
  result.hostname = "host.docker.internal";
  return result.toString();
};
await mkdir(new URL("../.local/", import.meta.url), { recursive: true });
const path = new URL(`../.local/${network}.env`, import.meta.url);
const freePort = async () => {
  const s = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response() });
  const p = s.port!;
  await s.stop(true);
  return p;
};
const [gatewayPort, workerPort] = await Promise.all([freePort(), freePort()]);
await Bun.write(
  path,
  Object.entries({
    DATABASE_URL: remote(url.toString()),
    REDIS_URL: remote(process.env.REDIS_URL),
    QUEUE_REDIS_URL: remote(process.env.QUEUE_REDIS_URL),
    QUEUE_REQUIRED_REPLICAS: "0",
    ADMIN_KEY: admin,
    KEY_PEPPER: pepper,
    ENCRYPTION_KEY: encryption,
    REDIS_NAMESPACE: namespace,
    NODE_ENV: "test",
    ALLOW_PRIVATE_UPSTREAM: "true",
    HOST: "0.0.0.0",
    PORT: "8088",
    WORKER_PORT: "8099",
    MOCK_PORT: "9088",
    MOCK_HOST: "0.0.0.0",
    MOCK_CONTROL_TOKEN: suffix,
  })
    .map(([k, v]) => `${k}=${v}`)
    .join("\n"),
);
await chmod(path, 0o600);
const run = async (args: string[]) => {
  const child = Bun.spawn(["docker", ...args], { stdout: "pipe", stderr: "pipe" });
  const output = new Response(child.stdout).text();
  const errors = new Response(child.stderr).text();
  const code = await child.exited;
  await errors;
  assert.equal(code, 0, `Fixture Docker ${args[0]}`);
  return output;
};
const names: string[] = [];
async function until(predicate: () => Promise<boolean>) {
  const end = Date.now() + 15000;
  while (!(await predicate())) {
    if (Date.now() > end) throw new Error("Linux fixture timed out");
    await Bun.sleep(50);
  }
}
try {
  await migrate(db);
  const store = new Store(db);
  const funds = new FundsStore(db);
  const project = await store.createProject({
    name: suffix,
    rpm: 1000,
    tpm: 1000000,
    concurrency: 100,
  });
  const key = createKey();
  await store.createCredential(
    project,
    { name: "linux", models: [], operations: ["chat"] },
    verifier(key, pepper),
    key.slice(0, 12),
  );
  await funds.credit(project, {
    currency: "USD",
    external_ref: "linux",
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
      name: suffix,
      kind: "openai",
      base_url: `http://${mockName}:9088/openai/v1`,
      rpm: 1000,
      concurrency: 100,
    },
    encrypt("fixture", encryption, provider),
  );
  const rates = {
    input_tokens: { numerator: "1", denominator: "1" },
    output_tokens: { numerator: "1", denominator: "1" },
  };
  await store.createModel({ name: suffix, operations: ["chat"], max_output_tokens: 100 });
  await store.createRoute(suffix, {
    provider_id: provider,
    upstream_model: suffix,
    operations: ["chat"],
    priority: 1,
    weight: 1,
    pricing: { currency: "USD", rates },
  });
  await funds.setTariff({
    model: suffix,
    operation: "chat",
    basis: "PROVIDER_USAGE",
    pricing: { currency: "USD", rates },
    ceilings: { input_tokens: "100", output_tokens: "100" },
  });
  await run(["network", "create", network]);
  for (const [kind, name, port] of [
    ["mock", mockName, 0],
    ["worker", `${network}-worker`, workerPort],
    ["gateway", `${network}-gateway`, gatewayPort],
  ] as const) {
    await run([
      "run",
      "-d",
      "--name",
      name,
      "--network",
      network,
      "--read-only",
      "--tmpfs",
      "/tmp:size=64m",
      "--cap-drop",
      "ALL",
      "--security-opt",
      "no-new-privileges",
      "--memory",
      "512m",
      "--env-file",
      path.pathname,
      ...(port ? ["-p", `127.0.0.1:${port}:${kind === "worker" ? 8099 : 8088}`] : []),
      "super-token-gateway:validation",
      "bun",
      kind === "mock"
        ? "dist/mock-providers/main.js"
        : kind === "worker"
          ? "dist/gateway/worker-main.js"
          : "dist/gateway/main.js",
    ]);
    names.push(name);
  }
  const origin = `http://127.0.0.1:${gatewayPort}`;
  for (const port of [gatewayPort, workerPort])
    await until(async () => {
      try {
        return (
          await fetch(`http://127.0.0.1:${port}/readyz`, { signal: AbortSignal.timeout(1000) })
        ).ok;
      } catch {
        return false;
      }
    });
  await until(async () => {
    try {
      return (
        (
          await run([
            "exec",
            `${network}-gateway`,
            "bun",
            "-e",
            `const r=await fetch('http://${mockName}:9088/healthz');if(!r.ok)process.exit(1);console.log('ready')`,
          ])
        ).trim() === "ready"
      );
    } catch {
      return false;
    }
  });
  for (const stream of [false, true]) {
    const response = await fetch(origin + "/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: suffix,
        messages: [{ role: "user", content: "linux fixture" }],
        max_completion_tokens: 32,
        stream,
      }),
    });
    if (response.status !== 200) throw new Error(`Linux inference: ${await response.text()}`);
    const body = await response.text();
    assert(body.includes("世界"));
    if (stream) assert(body.includes("[DONE]"));
  }
  await until(
    async () =>
      Number(
        (
          await db`SELECT count(*)::int AS n FROM billing_charges WHERE amount_atoms=12 AND funded_atoms=12 AND debt_atoms=0`
        )[0].n,
      ) === 2,
  );
  assert.deepEqual(
    (await db`SELECT spent_atoms::text,reserved_atoms::text,debt_atoms::text FROM wallets`)[0],
    { spent_atoms: "24", reserved_atoms: "0", debt_atoms: "0" },
  );
  for (const name of names) {
    assert(!(await run(["logs", name])).includes("Hello, 世界"));
    await run(["stop", "-t", "45", name]);
  }
  names.length = 0;
  console.log(
    JSON.stringify({
      linux_container: "pass",
      runtime: "Bun 1.4.4",
      nonroot: true,
      read_only: true,
      checks: [
        "independent gateway, worker and mock",
        "PG and two Redis dependencies",
        "Chat JSON and SSE",
        "two exact charges and balanced wallet",
        "SIGTERM clean shutdown",
      ],
    }),
  );
} finally {
  for (const name of [`${network}-gateway`, `${network}-worker`, mockName])
    await run(["rm", "-f", name]).catch(() => undefined);
  await run(["network", "rm", network]).catch(() => undefined);
  for (const endpoint of [process.env.REDIS_URL, process.env.QUEUE_REDIS_URL]) {
    const redis = new RedisClient(endpoint);
    try {
      let cursor = "0";
      do {
        const [next, keys] = (await redis.send("SCAN", [
          cursor,
          "MATCH",
          `*${namespace}*`,
          "COUNT",
          "1000",
        ])) as [string, string[]];
        cursor = next;
        if (keys.length) await redis.send("UNLINK", keys);
      } while (cursor !== "0");
    } finally {
      redis.close();
    }
  }
  await db.close();
  await owner.unsafe(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
  await owner.close();
  await unlink(path).catch(() => undefined);
}
