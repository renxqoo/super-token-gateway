import assert from "node:assert/strict";
import { isAbsolute, join } from "node:path";
import { chmod, mkdir, rm } from "node:fs/promises";
import { createDatabase, SCHEMA_VERSION } from "@gateway/storage";
import { UsageProducer, evidenceChannel } from "@gateway/runtime";
import { loadCoreWorkerConfig } from "../apps/core-worker/src/config.ts";
if (!Bun.argv.includes("--quiesced"))
  throw new Error(
    "Stop all admission/producer processes and drain the queue, then pass --quiesced. A PG-only snapshot of an active queue is incomplete.",
  );
const config = loadCoreWorkerConfig();
const queue = new UsageProducer({
  channel: evidenceChannel,
  url: config.QUEUE_REDIS_URL,
  namespace: config.REDIS_NAMESPACE,
  replicas: 0,
  timeoutMs: config.QUEUE_ACK_TIMEOUT_MS,
});
const assertDrained = async () => {
  const counts = await queue.stats();
  assert(
    Object.values(counts).every((n) => n === 0),
    `Queues must have no waiting, active, delayed or failed evidence: ${evidenceChannel.name} ${JSON.stringify(counts)}`,
  );
};
const roles = [
  "core-identity-role.sql",
  "core-catalog-role.sql",
  "core-execution-role.sql",
  "core-management-role.sql",
  "core-public-role.sql",
  "core-settlement-role.sql",
];
const digest = (table: string) =>
  `(SELECT md5(coalesce(string_agg(h,'' ORDER BY h),'')) FROM (SELECT md5(to_jsonb(t)::text) AS h FROM ${table} t) rows)`;

const connection = process.env.DATABASE_URL;
if (!connection) throw new Error("DATABASE_URL is required");
const url = new URL(connection);
const tools = process.env.PG_TOOLS_DIRECTORY;
if (!tools || !isAbsolute(tools))
  throw new Error("PG_TOOLS_DIRECTORY must be the absolute PostgreSQL 17 tools directory");
if (!["localhost", "127.0.0.1"].includes(url.hostname))
  throw new Error("This verifier only targets a loopback PostgreSQL server");
const source = url.pathname.slice(1);
if (!/^[a-zA-Z0-9_]+$/.test(source)) throw new Error("Invalid source database identifier");
const restore = `gateway_restore_${crypto.randomUUID().replaceAll("-", "")}`;
const directory = new URL("../.local/backups/", import.meta.url);
await mkdir(directory, { recursive: true });
const path = new URL(`gateway-${Date.now()}.dump`, directory);
await Bun.write(path, new Uint8Array());
await chmod(path, 0o600);
const run = async (args: string[], input?: URL, output?: URL) => {
  const child = Bun.spawn([join(tools, args[0]!), ...args.slice(1)], {
    env: {
      PATH: "/usr/bin:/bin",
      PGHOST: url.hostname,
      PGPORT: url.port,
      PGPASSWORD: decodeURIComponent(url.password),
    },
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
let passed = false;
try {
  await assertDrained();
  assert.equal(
    (await owner`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`)[0].version,
    SCHEMA_VERSION,
  );
  assert.equal(
    (await owner`SELECT count(*)::int AS n FROM attempts WHERE state='DISPATCHING'`)[0].n,
    0,
    "No producer may still be executing",
  );
  const counters = async (connection: typeof owner) =>
    (
      await connection.unsafe(`SELECT
    (SELECT count(*)::text FROM attempts) AS attempts,
    (SELECT count(*)::text FROM usage_events) AS usage,
    (SELECT count(*)::text FROM money_journal) AS journals,
    (SELECT count(*)::text FROM billing_holds) AS holds,
    (SELECT count(*)::text FROM billing_charges) AS charges,
    (SELECT count(*)::text FROM wallet_lots) AS lots,
    (SELECT count(*)::text FROM wallet_credit_policies) AS credit_policies,
    (SELECT jsonb_agg(v ORDER BY currency) FROM(SELECT currency,sum(available_atoms)::text AS available,sum(reserved_atoms)::text AS reserved,sum(spent_atoms)::text AS spent,sum(expired_atoms)::text AS expired,sum(debt_atoms)::text AS debt,sum(credit_limit_atoms)::text AS credit_limit,sum(credit_reserved_atoms)::text AS credit_reserved,sum(credit_policy_version)::text AS credit_versions FROM wallets GROUP BY currency)v) AS balances,
    (SELECT count(*)::int FROM wallets w WHERE w.credit_reserved_atoms<>(SELECT coalesce(sum(h.credit_atoms),0) FROM billing_holds h WHERE h.wallet_id=w.id AND h.state IN ('HELD','RECONCILING'))) AS inconsistent_credit,
    (SELECT count(*)::int FROM money_journal j WHERE (SELECT sum((v->>'atoms')::numeric) FROM jsonb_array_elements(j.lines)v)<>0) AS unbalanced,
    (SELECT count(*)::int FROM wallets w WHERE (w.available_atoms,w.reserved_atoms,w.spent_atoms,w.expired_atoms) IS DISTINCT FROM (SELECT ROW(coalesce(sum(l.available_atoms),0),coalesce(sum(l.reserved_atoms),0),coalesce(sum(l.spent_atoms),0),coalesce(sum(l.expired_atoms),0)) FROM wallet_lots l WHERE l.wallet_id=w.id)) AS inconsistent_wallets,
    (SELECT jsonb_agg(v ORDER BY currency) FROM(SELECT currency,sum(balance_atoms)::text AS balance,sum(reserved_atoms)::text AS reserved,sum(spent_atoms)::text AS spent FROM upstream_account_balances GROUP BY currency)v) AS upstream_balances,
    (SELECT count(*)::int FROM upstream_account_balances b WHERE (b.balance_atoms,b.reserved_atoms,b.spent_atoms) IS DISTINCT FROM (SELECT ROW(coalesce(sum(e.atoms) FILTER (WHERE e.account IN ('upstream:available','upstream:reserved')),0),coalesce(sum(e.atoms) FILTER (WHERE e.account='upstream:reserved'),0),coalesce(sum(e.atoms) FILTER (WHERE e.account='upstream:expense'),0)) FROM upstream_entries e JOIN upstream_journals j ON j.id=e.journal_id WHERE j.account_id=b.account_id AND e.currency=b.currency)) AS inconsistent_upstream,
    (SELECT count(*)::int FROM (SELECT journal_id FROM upstream_entries GROUP BY journal_id HAVING sum(atoms)<>0) j) AS unbalanced_upstream,
    (SELECT jsonb_object_agg(record_kind,n) FROM (SELECT record_kind,count(*)::text AS n FROM usage_event_index GROUP BY record_kind) k) AS evidence_kinds,
    (SELECT jsonb_object_agg(state,n) FROM (SELECT state,count(*)::text AS n FROM event_deliveries GROUP BY state) d) AS deliveries,
    ${digest("event_consumers")} AS consumers_digest,
    ${digest("event_deliveries")} AS deliveries_digest,
    ${digest("core_evidence_receipts")} AS receipts_digest,
    ${digest("upstream_entries")} AS upstream_entries_digest,
    ${digest("plugin_packages")} AS plugin_packages_digest,
    ${digest("plugin_versions")} AS plugin_versions_digest,
    ${digest("channel_plugin_bindings")} AS plugin_bindings_digest,
    ${digest("resources")} AS resources_digest
  `)
    )[0] as Record<string, unknown>;
  let before: Record<string, unknown> = {};
  await owner.begin(async (tx) => {
    await tx`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`;
    const snapshot = String((await tx`SELECT pg_export_snapshot() AS id`)[0].id);
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
  await destination`REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC`;
  for (const role of roles)
    await destination.unsafe(await Bun.file(new URL(`../infra/${role}`, import.meta.url)).text());
  const after = await counters(destination);
  assert.deepEqual(after, before);
  assert.equal(after.unbalanced, 0);
  assert.equal(after.inconsistent_wallets, 0);
  assert.equal(after.inconsistent_credit, 0);
  assert.equal(after.inconsistent_upstream, 0);
  assert.equal(after.unbalanced_upstream, 0);
  assert.equal(
    (await destination`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`)[0]
      .version,
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
  passed = true;
} finally {
  queue.close();
  if (!passed) await rm(path, { force: true });
  await destination?.close();
  await owner.unsafe(`DROP DATABASE IF EXISTS ${restore} WITH (FORCE)`);
  await owner.close();
}
