import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { createReadStream } from "node:fs";
import { appendFile, mkdir, rm } from "node:fs/promises";
import { createGunzip } from "node:zlib";
import {
  createCustomerFundsStore,
  createDatabase,
  migrate,
  prepareEvidenceMonth,
} from "@gateway/storage";
import { exportEvidenceArchive, verifyEvidenceArchive } from "../../scripts/evidence-archive.ts";
import { chatReplies, settledCharge, withGatewayStack } from "../support/gateway-process.ts";

async function backdate(db: SQL, at: string) {
  await db.begin(async (tx) => {
    await tx`ALTER TABLE usage_payloads ALTER CONSTRAINT usage_payloads_id_created_at_fkey DEFERRABLE INITIALLY DEFERRED`;
    await tx`ALTER TABLE journal_payloads ALTER CONSTRAINT journal_payloads_id_created_at_fkey DEFERRABLE INITIALLY DEFERRED`;
    for (const [table, trigger] of [
      ["usage_event_index", "immutable_usage"],
      ["usage_payloads", "immutable_usage_payload"],
      ["money_entries", "immutable_journal"],
      ["journal_payloads", "immutable_journal_payload"],
      ["core_evidence_receipts", "immutable_core_receipts"],
      ["upstream_journals", "immutable_upstream_journals"],
    ])
      await tx.unsafe(`ALTER TABLE ${table} DISABLE TRIGGER ${trigger}`);
    await tx`SET CONSTRAINTS ALL DEFERRED`;
    await tx`CREATE TEMP TABLE archive_dates ON COMMIT DROP AS SELECT id,${at}::timestamptz+row_number() OVER(ORDER BY seq)*interval '1 microsecond' AS at FROM usage_event_index`;
    await tx`UPDATE usage_event_index e SET created_at=d.at FROM archive_dates d WHERE e.id=d.id`;
    await tx`UPDATE usage_payloads e SET created_at=d.at FROM archive_dates d WHERE e.id=d.id`;
    await tx`UPDATE money_entries SET created_at=${at}::timestamptz`;
    await tx`UPDATE journal_payloads SET created_at=${at}::timestamptz`;
    await tx`UPDATE core_evidence_receipts SET created_at=${at}::timestamptz`;
    await tx`UPDATE upstream_journals SET created_at=${at}::timestamptz`;
    await tx`SET CONSTRAINTS ALL IMMEDIATE`;
    for (const [table, trigger] of [
      ["usage_event_index", "immutable_usage"],
      ["usage_payloads", "immutable_usage_payload"],
      ["money_entries", "immutable_journal"],
      ["journal_payloads", "immutable_journal_payload"],
      ["core_evidence_receipts", "immutable_core_receipts"],
      ["upstream_journals", "immutable_upstream_journals"],
    ])
      await tx.unsafe(`ALTER TABLE ${table} ENABLE TRIGGER ${trigger}`);
    await tx`ALTER TABLE usage_payloads ALTER CONSTRAINT usage_payloads_id_created_at_fkey NOT DEFERRABLE`;
    await tx`ALTER TABLE journal_payloads ALTER CONSTRAINT journal_payloads_id_created_at_fkey NOT DEFERRABLE`;
  });
}

async function lines(path: string) {
  let text = "";
  for await (const chunk of createReadStream(path).pipe(createGunzip())) text += chunk;
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

test(
  "a monthly archive of v2 traffic keeps raw signed evidence, evidence receipts and upstream ledger lines",
  () =>
    withGatewayStack({ reply: () => chatReplies.json("机密回答") }, async (stack) => {
      const { db } = stack;
      const response = await stack.send({ content: "机密提示" });
      expect(response.status).toBe(200);
      const requestId = response.headers.get("x-request-id")!;
      await settledCharge(db, requestId);
      await stack.worker.stop();
      await stack.gateway.stop();
      await backdate(db, "2025-03-10T00:00:00Z");
      const counts = (
        await db`SELECT
          (SELECT count(*)::int FROM usage_event_index) AS usage,
          (SELECT count(*)::int FROM usage_event_index WHERE record_kind='RAW') AS raw,
          (SELECT count(*)::int FROM money_journal) AS journal,
          (SELECT count(*)::int FROM core_evidence_receipts) AS receipts,
          (SELECT count(*)::int FROM upstream_journals WHERE created_at<'2025-04-01') AS upstream`
      )[0] as { usage: number; raw: number; journal: number; receipts: number; upstream: number };
      expect(counts.raw).toBeGreaterThan(0);
      expect(counts.receipts).toBeGreaterThan(0);
      expect(counts.upstream).toBeGreaterThan(0);
      const directory = new URL(`../../.local/archive-${crypto.randomUUID()}/`, import.meta.url)
        .pathname;
      await mkdir(directory, { recursive: true });
      try {
        const manifest = await exportEvidenceArchive(db, "2025-03", directory);
        const verified = await verifyEvidenceArchive(manifest);
        expect(Object.fromEntries(verified.files.map((file) => [file.kind, file.rows]))).toEqual({
          usage: counts.usage,
          journal: counts.journal,
          receipts: counts.receipts,
          upstream: counts.upstream,
        });
        const usage = await lines(`${directory}/usage.ndjson.gz`);
        expect(usage.filter((row) => row.record_kind === "RAW")).toHaveLength(counts.raw);
        expect(usage.some((row) => row.record_kind === "CANONICAL")).toBe(true);
        expect(usage.every((row) => typeof row.usage === "object")).toBe(true);
        const receipts = await lines(`${directory}/receipts.ndjson.gz`);
        expect(new Set(receipts.map((row) => row.attempt_id))).toEqual(
          new Set(
            (await db`SELECT id FROM attempts WHERE request_id=${requestId}::uuid`).map(
              (row: { id: string }) => row.id,
            ),
          ),
        );
        const upstream = await lines(`${directory}/upstream.ndjson.gz`);
        for (const journal of upstream) {
          const entries = journal.entries as { atoms: string }[];
          expect(entries.length).toBeGreaterThan(1);
          expect(entries.reduce((sum, entry) => sum + BigInt(entry.atoms), 0n)).toBe(0n);
        }
        const text = [usage, receipts, upstream].map((rows) => JSON.stringify(rows)).join("");
        expect(text).not.toContain("机密");
        await appendFile(`${directory}/receipts.ndjson.gz`, "corrupt");
        await expect(verifyEvidenceArchive(manifest)).rejects.toThrow("checksum");
      } finally {
        await rm(directory, { recursive: true, force: true });
      }
    }),
  180000,
);

test("a failed monthly partition migration rolls back moved fallback evidence and restores its guards", async () => {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL required");
  const name = `gateway_test_archive_${crypto.randomUUID().replaceAll("-", "")}`;
  const owner = createDatabase(base, 1);
  await owner.unsafe(`CREATE DATABASE ${name}`);
  const url = new URL(base);
  url.pathname = `/${name}`;
  const db = createDatabase(url.toString(), 1);
  try {
    await migrate(db);
    const project = crypto.randomUUID();
    await db`INSERT INTO projects(id,name,rpm,tpm,concurrency) VALUES(${project},'archive',1000,100000,10)`;
    await createCustomerFundsStore(db).credit(project, {
      currency: "USD",
      external_ref: "archive-credit",
      amount_atoms: "100",
      kind: "CASH",
      priority: 100,
      models: [],
      operations: [],
    });
    await db`INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines,created_at) SELECT ${crypto.randomUUID()},id,'fallback','ADJUSTMENT',currency,'[{"account":"system:funding","atoms":"0"},{"account":"system:revenue","atoms":"0"}]','2024-02-02T00:00:00Z' FROM wallets LIMIT 1`;
    const guards = () => db`SELECT
        (SELECT count(*)::int FROM journal_payloads_default WHERE created_at<'2024-03-01') AS fallback,
        to_regclass('public.usage_payloads_202402')::text AS usage_partition,
        to_regclass('public.journal_payloads_202402')::text AS journal_partition,
        EXISTS(SELECT 1 FROM pg_constraint WHERE conname='journal_index_payload') AS constraint_present,
        (SELECT tgenabled FROM pg_trigger WHERE tgname='immutable_journal_payload' AND tgrelid='public.journal_payloads'::regclass) AS trigger_state`;
    const before = (await guards())[0];
    expect(before).toEqual({
      fallback: 1,
      usage_partition: null,
      journal_partition: null,
      constraint_present: true,
      trigger_state: "O",
    });
    await db`CREATE DOMAIN journal_payloads_202402 AS integer`;
    await expect(prepareEvidenceMonth(db, "2024-02", true)).rejects.toThrow();
    expect((await guards())[0]).toEqual(before);
    await expect(
      db`UPDATE journal_payloads SET lines='[]' WHERE created_at<'2024-03-01'`,
    ).rejects.toThrow("immutable");
    await db`DROP DOMAIN journal_payloads_202402`;
    await prepareEvidenceMonth(db, "2024-02", true);
    expect((await guards())[0]).toEqual({
      fallback: 0,
      usage_partition: "usage_payloads_202402",
      journal_partition: "journal_payloads_202402",
      constraint_present: true,
      trigger_state: "O",
    });
    expect((await db`SELECT count(*)::int AS n FROM journal_payloads_202402`)[0].n).toBe(1);
  } finally {
    await db.close();
    await owner.unsafe(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await owner.close();
  }
}, 60000);
