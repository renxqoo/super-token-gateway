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
const root = new URL("../", import.meta.url);
const cwd = new URL(`../.local/artifact-${suffix}/`, import.meta.url);
await mkdir(cwd, { recursive: true });
const owner = createDatabase(base);
await owner.unsafe(`CREATE DATABASE ${name}`);
const url = new URL(base);
url.pathname = `/${name}`;
const env = { ...process.env, DATABASE_URL: url.toString(), NODE_ENV: "test" };
const children = new Set<ReturnType<typeof Bun.spawn>>();
const start = (path: string, args: string[] = []) => {
  const process = Bun.spawn(["bun", new URL(path, root).pathname, ...args], {
    cwd: cwd.pathname,
    env,
    stdout: "ignore",
    stderr: "inherit",
  });
  children.add(process);
  return process;
};
try {
  const benchmark = Bun.spawn(
    [
      process.execPath,
      "scripts/bench-system.ts",
      "--seconds",
      "1",
      "--rate",
      "1",
      "--concurrency",
      "1",
      "--warmup-concurrency",
      "1",
      "--instances",
      "1",
      "--wallets",
      "1",
      "--worker-concurrency",
      "1",
      "--drain-seconds",
      "10",
      "--scenario",
      JSON.stringify({ headerDelayMs: 50 }),
    ],
    { cwd: root.pathname, env, stdout: "pipe", stderr: "pipe" },
  );
  children.add(benchmark);
  const [benchmarkOutput, benchmarkErrors, benchmarkExit] = await Promise.all([
    new Response(benchmark.stdout).text(),
    new Response(benchmark.stderr).text(),
    benchmark.exited,
  ]);
  assert.equal(
    benchmarkExit,
    0,
    `Fresh-schema benchmark must initialize, settle and clean up: ${benchmarkOutput}${benchmarkErrors}`,
  );
  const benchmarkResult = JSON.parse(benchmarkOutput.trim()) as {
    directory: string;
    valid: boolean;
  };
  assert.equal(benchmarkResult.valid, true);
  const benchmarkDirectory = new URL(benchmarkResult.directory + "/", root);
  const audit = await Bun.file(new URL("audit.json", benchmarkDirectory)).json();
  for (const [path, digest] of Object.entries(audit.environment.artifacts))
    assert.equal(
      digest,
      new Bun.CryptoHasher("sha256")
        .update(await Bun.file(new URL(path, root)).bytes())
        .digest("hex"),
    );
  assert.deepEqual(Object.keys(audit.environment.artifacts).sort(), [
    "dist/core-api/main.js",
    "dist/core-api/platform/management.js",
    "dist/core-worker/main.js",
    "dist/gateway/main.js",
    "dist/manifest.json",
    "dist/mock-providers/main.js",
    "dist/native/builtin-adapters/registry.json",
    "dist/native/builtin-adapters/worker.js",
    "dist/native/plugin-host",
  ]);
  assert.equal(audit.http.offered, 1);
  assert.equal(audit.http.succeeded, 1);
  assert.equal(audit.http.invalid, 0);
  assert(audit.accounting.requests > 1);
  assert.equal(audit.accounting.precise_charges, audit.accounting.requests);
  assert.equal(audit.accounting.acknowledged, audit.accounting.usage_events);
  assert.equal(audit.ackedRequests, audit.accounting.requests);
  assert.equal(audit.supplier.spent, String(audit.accounting.requests * 59));
  assert(Object.values(audit.queueStats).every((count) => count === 0));
  assert.equal((await Bun.file(new URL("cleanup.json", benchmarkDirectory)).json()).failed, 0);
  assert.equal(
    (await owner`SELECT count(*)::int AS n FROM pg_database WHERE datname=${audit.database}`)[0].n,
    0,
  );
  for (const endpoint of [redisURL, queueURL]) {
    const redis = new RedisClient(endpoint);
    try {
      await redis.connect();
      let cursor = "0";
      do {
        const page = (await redis.send("SCAN", [
          cursor,
          "MATCH",
          `*bench:${audit.id}*`,
          "COUNT",
          "1000",
        ])) as [string, string[]];
        cursor = page[0];
        assert.equal(page[1].length, 0, "Benchmark must remove its own Redis namespace");
      } while (cursor !== "0");
    } finally {
      redis.close();
    }
  }
  assert.equal(
    await start("dist/tools/migrate.js", ["--database", name]).exited,
    0,
    "Compiled migrator must find versioned SQL assets from an empty working directory",
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
  console.log(
    JSON.stringify({
      artifact: "pass",
      checks: [
        "fresh-schema benchmark initialization, exact settlement, ACK, cleanup and tested artifact digests",
        "compiled migration SQL assets and archive CLI from an empty working directory",
      ],
    }),
  );
} finally {
  for (const child of children) if (child.exitCode === null) child.kill("SIGKILL");
  await Promise.allSettled([...children].map((child) => child.exited));
  await owner.unsafe(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
  await owner.close();
  await rm(cwd, { recursive: true, force: true });
}
