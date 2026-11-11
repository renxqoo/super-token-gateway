import assert from "node:assert/strict";
import { mkdir, rm } from "node:fs/promises";
import { createDatabase } from "@gateway/storage";
import { RedisClient } from "bun";

const base = process.env.DATABASE_URL;
const redisURL = process.env.REDIS_URL;
const queueURL = process.env.QUEUE_REDIS_URL;
if (!base || !redisURL || !queueURL)
  throw new Error("Artifact verification requires PG, admission Redis and durable queue Redis");
const suffix = crypto.randomUUID().replaceAll("-", "");
const name = `gateway_artifact_${suffix}`;
const namespace = `artifact:${suffix}`;
const root = new URL("../", import.meta.url);
const cwd = new URL(`../.local/artifact-${suffix}/`, import.meta.url);
await mkdir(cwd, { recursive: true });
const owner = createDatabase(base);
await owner.unsafe(`CREATE DATABASE ${name}`);
const url = new URL(base);
url.pathname = `/${name}`;
const db = createDatabase(url.toString());
const admin = crypto.randomUUID() + crypto.randomUUID();
const control = crypto.randomUUID();
const freePort = async () => {
  const temporary = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response() });
  const port = temporary.port;
  await temporary.stop(true);
  return port;
};
const gatewayPort = await freePort();
const mockPort = await freePort();
const workerPort = await freePort();
const origin = `http://127.0.0.1:${gatewayPort}`;
const mockOrigin = `http://127.0.0.1:${mockPort}`;
const logs: string[] = [];
const children = new Set<ReturnType<typeof Bun.spawn>>();
const env = {
  ...process.env,
  DATABASE_URL: url.toString(),
  REDIS_URL: redisURL,
  QUEUE_REDIS_URL: queueURL,
  QUEUE_REQUIRED_REPLICAS: "0",
  REDIS_NAMESPACE: namespace,
  NODE_ENV: "test",
  ADMIN_KEY: admin,
  KEY_PEPPER: "artifact-" + suffix,
  ENCRYPTION_KEY: "33".repeat(32),
  ALLOW_PRIVATE_UPSTREAM: "true",
  HOST: "127.0.0.1",
  PORT: String(gatewayPort),
  MOCK_PORT: String(mockPort),
  WORKER_PORT: String(workerPort),
  MOCK_CONTROL_TOKEN: control,
  TOTAL_TIMEOUT_MS: "5000",
  SHUTDOWN_GRACE_MS: "1000",
  STREAM_IDLE_MS: "1000",
  DOWNSTREAM_STALL_MS: "1000",
};
const start = (path: string, args: string[] = []) => {
  const process = Bun.spawn(["bun", new URL(path, root).pathname, ...args], {
    cwd: cwd.pathname,
    env,
    stdout: "pipe",
    stderr: "pipe",
  });
  children.add(process);
  void new Response(process.stdout).text().then((value) => logs.push(value));
  void new Response(process.stderr).text().then((value) => logs.push(value));
  return process;
};
async function until(predicate: () => Promise<boolean>, ms = 5000) {
  const deadline = Date.now() + ms;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error("Artifact condition timed out");
    await Bun.sleep(20);
  }
}
const health = async (origin: string, path: string) =>
  until(async () => {
    try {
      return (await fetch(origin + path, { signal: AbortSignal.timeout(500) })).ok;
    } catch {
      return false;
    }
  });
const api = async (path: string, body: unknown, token = admin) => {
  const response = await fetch(origin + path, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  assert(response.ok, `Artifact API ${path}: ${response.status}`);
  return (await response.json()) as Record<string, unknown>;
};
try {
  assert.equal(
    await start("dist/tools/migrate.js").exited,
    0,
    "Compiled migrator must find both SQL assets from an empty working directory",
  );
  const archivePath = new URL("numeric-archive/", cwd).pathname;
  assert.equal(
    await start("dist/tools/archive.js", ["--month", "2025-01", "--output", archivePath]).exited,
    0,
  );
  assert.equal(
    await start("dist/tools/archive.js", ["--verify", archivePath + "/manifest.json"]).exited,
    0,
  );
  let server = start("dist/gateway/main.js");
  start("dist/mock-providers/main.js");
  await Promise.all([health(origin, "/readyz"), health(mockOrigin, "/healthz")]);
  const project = await api("/admin/projects", {
    name: "artifact",
    rpm: 10000,
    tpm: 10000000,
    concurrency: 100,
  });
  const key = await api(`/admin/projects/${project.id}/keys`, { name: "artifact" });
  const provider = await api("/admin/providers", {
    name: "artifact",
    kind: "openai",
    api_key: "fixture-only",
    base_url: mockOrigin + "/openai/v1",
  });
  const model = "artifact-model";
  await api("/admin/models", { name: model, operations: ["chat"] });
  await api(`/admin/models/${model}/routes`, {
    provider_id: provider.id,
    upstream_model: model,
    operations: ["chat"],
    pricing: {
      currency: "USD",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "1", denominator: "1" },
      },
    },
  });
  await api(`/admin/projects/${project.id}/credits`, {
    currency: "USD",
    external_ref: "artifact-credit",
    amount_atoms: "1000000",
    kind: "CASH",
  });
  await api("/admin/tariffs", {
    model,
    operation: "chat",
    basis: "PROVIDER_USAGE",
    pricing: {
      currency: "USD",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "1", denominator: "1" },
      },
    },
    ceilings: { input_tokens: "4096", output_tokens: "4096" },
  });
  const body = {
    model,
    messages: [{ role: "user", content: "artifact input" }],
    max_completion_tokens: 32,
  };
  await api("/v1/chat/completions", body, String(key.key));
  assert.equal(
    (await db`SELECT id FROM usage_events`).length,
    0,
    "HTTP completion must not depend on the PG worker",
  );
  assert.equal((await db`SELECT state FROM billing_holds`)[0].state, "HELD");
  start("dist/gateway/worker-main.js");
  await health(`http://127.0.0.1:${workerPort}`, "/readyz");
  await until(
    async () =>
      (await db`SELECT id FROM billing_charges WHERE amount_atoms=12 AND funded_atoms=12`)
        .length === 1,
  );
  const scenario = async (value: unknown) => {
    const response = await fetch(mockOrigin + "/__control/scenarios", {
      method: "POST",
      headers: { authorization: `Bearer ${control}`, "content-type": "application/json" },
      body: JSON.stringify({ model, scenario: value }),
    });
    assert(response.ok);
  };
  await scenario({ headerDelayMs: 1500 });
  const intent = crypto.randomUUID();
  const pending = fetch(origin + "/v1/chat/completions", {
    method: "POST",
    headers: {
      authorization: `Bearer ${key.key}`,
      "content-type": "application/json",
      "idempotency-key": intent,
    },
    body: JSON.stringify(body),
  })
    .then((response) => response.arrayBuffer())
    .catch(() => undefined);
  await until(
    async () => (await db`SELECT id FROM attempts WHERE state='DISPATCHING'`).length === 1,
  );
  server.kill("SIGKILL");
  await server.exited;
  await pending;
  await db`UPDATE attempts SET started_at=now()-interval '10 minutes' WHERE state='DISPATCHING'`;
  await db`UPDATE gateway_requests SET updated_at=now()-interval '10 minutes' WHERE state='RUNNING'`;
  server = start("dist/gateway/main.js");
  await health(origin, "/readyz");
  await api("/admin/recover", {});
  await until(
    async () => (await db`SELECT state FROM gateway_requests WHERE state='UNKNOWN'`).length === 1,
  );
  const duplicate = await fetch(origin + "/v1/chat/completions", {
    method: "POST",
    headers: {
      authorization: `Bearer ${key.key}`,
      "content-type": "application/json",
      "idempotency-key": intent,
    },
    body: JSON.stringify(body),
  });
  assert.equal(duplicate.status, 409);
  await duplicate.arrayBuffer();
  assert.equal(
    (await db`SELECT id FROM attempts`).length,
    2,
    "Crash recovery must never replay an upstream call",
  );
  await scenario({ repeat: 5000, chunkBytes: 128, chunkDelayMs: 2 });
  const stream = await fetch(origin + "/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
    body: JSON.stringify({ ...body, stream: true }),
  });
  const reader = stream.body?.getReader();
  assert(reader);
  await reader.read();
  server.kill("SIGTERM");
  await Promise.race([
    server.exited,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error("Graceful shutdown exceeded 5 seconds")), 5000),
    ),
  ]);
  await reader.cancel().catch(() => undefined);
  await until(
    async () => (await db`SELECT id FROM attempts WHERE state='DISPATCHING'`).length === 0,
  );
  // A non-streaming body remains a live request until it is fully read and durably accepted.
  server = start("dist/gateway/main.js");
  await health(origin, "/readyz");
  await scenario({ jsonChunkDelayMs: 20, jsonChunkBytes: 64 });
  const slow = fetch(origin + "/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  await until(
    async () => (await db`SELECT id FROM attempts WHERE state='DISPATCHING'`).length === 1,
  );
  await Bun.sleep(40);
  server.kill("SIGTERM");
  const slowResponse = await slow;
  assert.equal(slowResponse.status, 200);
  const slowJSON = (await slowResponse.json()) as { choices: unknown[] };
  assert(slowJSON.choices.length > 0);
  await server.exited;
  await until(
    async () =>
      (await db`SELECT id FROM billing_charges WHERE amount_atoms=12 AND funded_atoms=12`)
        .length === 2,
  );
  const attempts = await db`SELECT state FROM attempts`;
  assert.equal(attempts.length, 4);
  assert(attempts.every((row: { state: string }) => row.state !== "DISPATCHING"));
  assert.equal((await db`SELECT id FROM usage_events`).length, 4);
  const balance = (
    await db`SELECT reserved_atoms::text,spent_atoms::text,debt_atoms::text FROM wallets`
  )[0];
  assert.equal(balance.spent_atoms, "24");
  assert.equal(balance.debt_atoms, "0");
  assert(
    BigInt(balance.reserved_atoms) > 0n,
    "Interrupted requests must retain reconciliation holds",
  );
  assert(!logs.join("").includes("Hello, 世界"));
  console.log(
    JSON.stringify({
      artifact: "pass",
      checks: [
        "standalone bundle with empty working directory",
        "compiled migration SQL assets and archive CLI",
        "real HTTP readiness and inference",
        "strict queue success while worker stopped, then exact settlement",
        "SIGKILL plus restart and UNKNOWN recovery",
        "idempotent replay after crash",
        "SIGTERM stream settlement before exit",
        "SIGTERM during slow JSON completes within grace",
      ],
      attempts: 4,
      usage_events: 4,
    }),
  );
} finally {
  for (const child of children) if (child.exitCode === null) child.kill("SIGKILL");
  await Promise.allSettled([...children].map((child) => child.exited));
  for (const endpoint of [
    redisURL,
    ...(process.env.REDIS_EXTRA_URLS ?? "")
      .split(",")
      .map((url) => url.trim())
      .filter(Boolean),
    queueURL,
  ]) {
    const redis = new RedisClient(endpoint);
    try {
      await redis.connect();
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
        if (result[1].length) await redis.send("DEL", result[1]);
      } while (cursor !== "0");
    } finally {
      redis.close();
    }
  }
  await db.close();
  await owner.unsafe(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
  await owner.close();
  await rm(cwd, { recursive: true, force: true });
}
