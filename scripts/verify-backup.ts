import assert from "node:assert/strict";
import { join } from "node:path";
import { chmod, mkdir } from "node:fs/promises";
import { createDatabase, SCHEMA_VERSION } from "@gateway/storage";
import { UsageProducer } from "@gateway/runtime";
import { loadConfig } from "../apps/gateway/src/config.ts";
if (!Bun.argv.includes("--quiesced"))
  throw new Error(
    "Stop all admission/producer processes and drain the queue, then pass --quiesced. A PG-only snapshot of an active queue is incomplete.",
  );
const config = loadConfig();
const queue = new UsageProducer({
  url: config.QUEUE_REDIS_URL,
  namespace: config.REDIS_NAMESPACE,
  replicas: config.QUEUE_REQUIRED_REPLICAS,
  timeoutMs: config.QUEUE_ACK_TIMEOUT_MS,
});
const assertDrained = async () => {
  const counts = await queue.stats();
  assert(
    Object.values(counts).every((n) => n === 0),
    "Queue must have no waiting, active, delayed or failed evidence",
  );
};

const connection = process.env.DATABASE_URL;
if (!connection) throw new Error("DATABASE_URL is required");
const url = new URL(connection);
const container = process.env.PG_CONTAINER ?? "super-token-gateway-postgres-1";
if (!/^[a-zA-Z0-9_.-]+$/.test(container)) throw new Error("Invalid explicit PG_CONTAINER");
if (!["localhost", "127.0.0.1"].includes(url.hostname) || url.port !== "55442")
  throw new Error(
    "This verifier only targets the dedicated local PostgreSQL fixture on port 55442",
  );
const source = url.pathname.slice(1);
if (!/^[a-zA-Z0-9_]+$/.test(source)) throw new Error("Invalid source database identifier");
const restore = `gateway_restore_${crypto.randomUUID().replaceAll("-", "")}`;
const directory = new URL("../.local/backups/", import.meta.url);
await mkdir(directory, { recursive: true });
const path = new URL(`gateway-${Date.now()}.dump`, directory);
await Bun.write(path, new Uint8Array());
await chmod(path, 0o600);
const run = async (args: string[], input?: URL, output?: URL) => {
  const native = process.env.PG_TOOLS_DIRECTORY;
  const command = native
    ? [join(native, args[0]!), ...args.slice(1)]
    : ["docker", "exec", ...(input ? ["-i"] : []), container, ...args];
  const child = Bun.spawn(command, {
    ...(native
      ? {
          env: {
            ...process.env,
            PGHOST: url.hostname,
            PGPORT: url.port,
            PGPASSWORD: decodeURIComponent(url.password),
          },
        }
      : {}),
    stdin: input ? Bun.file(input) : "ignore",
    stdout: output ? Bun.file(output) : "pipe",
    stderr: "pipe",
  });
  const stderr = new Response(child.stderr).text();
  const code = await child.exited;
  await stderr;
  if (code !== 0)
    throw new Error(
      `PostgreSQL backup command failed (${code}); sensitive diagnostic output withheld`,
    );
};
const owner = createDatabase(connection);
let destination: ReturnType<typeof createDatabase> | undefined;
try {
  await assertDrained();
  assert.equal((await owner`SELECT version FROM schema_migrations`)[0].version, SCHEMA_VERSION);
  assert.equal(
    (await owner`SELECT count(*)::int AS n FROM attempts WHERE state='DISPATCHING'`)[0].n,
    0,
    "No producer may still be executing",
  );
  const counters = async (connection: typeof owner) =>
    (
      await connection`SELECT
    (SELECT count(*)::text FROM attempts) AS attempts,
    (SELECT count(*)::text FROM usage_events) AS usage,
    (SELECT count(*)::text FROM money_journal) AS journals,
    (SELECT count(*)::text FROM billing_holds) AS holds,
    (SELECT count(*)::text FROM billing_charges) AS charges,
    (SELECT count(*)::text FROM wallet_lots) AS lots,
    (SELECT jsonb_agg(v ORDER BY currency) FROM(SELECT currency,sum(available_atoms)::text AS available,sum(reserved_atoms)::text AS reserved,sum(spent_atoms)::text AS spent,sum(expired_atoms)::text AS expired,sum(debt_atoms)::text AS debt FROM wallets GROUP BY currency)v) AS balances,
    (SELECT count(*)::int FROM money_journal j WHERE (SELECT sum((v->>'atoms')::numeric) FROM jsonb_array_elements(j.lines)v)<>0) AS unbalanced,
    (SELECT count(*)::int FROM wallets w WHERE (w.available_atoms,w.reserved_atoms,w.spent_atoms,w.expired_atoms) IS DISTINCT FROM (SELECT ROW(coalesce(sum(l.available_atoms),0),coalesce(sum(l.reserved_atoms),0),coalesce(sum(l.spent_atoms),0),coalesce(sum(l.expired_atoms),0)) FROM wallet_lots l WHERE l.wallet_id=w.id)) AS inconsistent_wallets
  `
    )[0];
  let before: Record<string, unknown> = {};
  await owner.begin(async (tx) => {
    await tx`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`;
    const snapshot = String((await tx`SELECT pg_export_snapshot() AS id`)[0].id);
    // Use the same exported MVCC snapshot for counters and pg_dump.
    before = await counters(tx as unknown as typeof owner);
    await run(
      [
        "pg_dump",
        "-U",
        decodeURIComponent(url.username),
        "-d",
        source,
        "--format=custom",
        "--no-owner",
        "--no-acl",
        `--snapshot=${snapshot}`,
      ],
      undefined,
      path,
    );
  });
  await assertDrained();
  assert(Bun.file(path).size > 0);
  await owner.unsafe(`CREATE DATABASE ${restore}`);
  await run(
    [
      "pg_restore",
      "-U",
      decodeURIComponent(url.username),
      "-d",
      restore,
      "--no-owner",
      "--no-acl",
      "--exit-on-error",
    ],
    path,
  );
  const restored = new URL(url);
  restored.pathname = `/${restore}`;
  destination = createDatabase(restored.toString());
  // --no-acl restores functions with default PUBLIC execute. Remove it before granting the two roles.
  await destination`REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC`;
  await destination.unsafe(
    await Bun.file(new URL("../infra/runtime-role.sql", import.meta.url)).text(),
  );
  const after = await counters(destination);
  assert.deepEqual(after, before);
  assert.equal(after.unbalanced, 0);
  assert.equal(after.inconsistent_wallets, 0);
  assert.equal(
    (await destination`SELECT version FROM schema_migrations`)[0].version,
    SCHEMA_VERSION,
  );
  assert.equal((await destination`SELECT to_regclass('public.billing_work') AS old`)[0].old, null);
  const first = (await destination`SELECT id FROM usage_events LIMIT 1`)[0];
  if (first)
    await assert.rejects(destination`UPDATE usage_events SET cost_atoms=0 WHERE id=${first.id}`);
  const hash = new Bun.CryptoHasher("sha256")
    .update(await Bun.file(path).arrayBuffer())
    .digest("hex");
  console.log(
    JSON.stringify({
      backup_restore: "pass",
      file: path.pathname,
      bytes: Bun.file(path).size,
      sha256: hash,
      ...after,
      queue: "quiesced and drained",
      immutable_trigger: first ? "verified" : "no usage fixture",
      current_roles: "reapplied",
    }),
  );
} finally {
  queue.close();
  await destination?.close();
  await owner.unsafe(`DROP DATABASE IF EXISTS ${restore} WITH (FORCE)`);
  await owner.close();
}
