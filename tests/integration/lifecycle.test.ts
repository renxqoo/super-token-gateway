import { test, expect } from "bun:test";
import {
  createCatalogStore,
  createDatabase,
  migrate,
  createCustomerFundsStore,
  prepareEvidenceMonth,
  SCHEMA_VERSION,
  moveEvidenceMonth,
} from "@gateway/storage";
import { exportEvidenceArchive, verifyEvidenceArchive } from "../../scripts/evidence-archive.ts";
import { mkdir, rm, appendFile } from "node:fs/promises";

test("baseline replay preserves funded wallets and permits concurrent foreign-key readers", async () => {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL required");
  const name = `gateway_test_upgrade_${crypto.randomUUID().replaceAll("-", "")}`;
  const owner = createDatabase(base, 1);
  await owner.unsafe(`CREATE DATABASE ${name}`);
  const url = new URL(base);
  url.pathname = `/${name}`;
  let db = createDatabase(url.toString(), 1);
  try {
    await migrate(db);
    let funds = createCustomerFundsStore(db);
    const project = await createCatalogStore(db).createProject({
      name: "function-upgrade",
      rpm: 1000,
      tpm: 10000,
      concurrency: 100,
    });
    const input = {
      currency: "USD",
      external_ref: "original",
      amount_atoms: "1000",
      kind: "CASH" as const,
      priority: 100,
      models: [],
      operations: [],
    };
    await funds.credit(project, input);
    const snapshot = () =>
      db`SELECT to_jsonb(w)::text AS row FROM wallets w WHERE project_id=${project}`;
    const before = await snapshot();
    await migrate(db);
    expect(
      (await db`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`)[0].version,
    ).toBe(SCHEMA_VERSION);
    expect(await snapshot()).toEqual(before);
    await db.close();
    db = createDatabase(url.toString(), 2);
    funds = createCustomerFundsStore(db);
    await db.begin(async (tx) => {
      await tx`SELECT id FROM wallets WHERE project_id=${project} FOR KEY SHARE`;
      await funds.credit(project, { ...input, external_ref: "after-upgrade", amount_atoms: "1" });
    });
    expect((await funds.wallets(project))[0]).toMatchObject({
      available_atoms: "1001",
      reserved_atoms: "0",
      debt_atoms: "0",
    });
    expect(await funds.journal(project, "USD")).toHaveLength(2);
  } finally {
    await db.close();
    await owner.unsafe(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await owner.close();
  }
});

test("monthly evidence partitions enforce immutability, quiesced moves and verified archives", async () => {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL required");
  const name = `gateway_test_lifecycle_${crypto.randomUUID().replaceAll("-", "")}`;
  const owner = createDatabase(base, 1);
  await owner.unsafe(`CREATE DATABASE ${name}`);
  const url = new URL(base);
  url.pathname = `/${name}`;
  const db = createDatabase(url.toString(), 1);
  const directory = new URL(`../../.local/${name}/`, import.meta.url);
  try {
    await migrate(db);
    const funds = createCustomerFundsStore(db);
    const project = await createCatalogStore(db).createProject({
      name: "lifecycle-partitions",
      rpm: 1000,
      tpm: 100000,
      concurrency: 100,
    });
    await funds.credit(project, {
      currency: "USD",
      external_ref: "credit",
      amount_atoms: "1000",
      kind: "CASH",
      priority: 100,
      models: [],
      operations: [],
    });
    await db`INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines,created_at) SELECT ${crypto.randomUUID()},id,'jan-2025','ADJUSTMENT',currency,'[{"account":"system:funding","atoms":"0"},{"account":"system:revenue","atoms":"0"}]','2025-01-15T00:00:00Z' FROM wallets LIMIT 1`;
    await db`INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines,created_at) SELECT ${crypto.randomUUID()},id,'fallback','ADJUSTMENT',currency,'[{"account":"system:funding","atoms":"0"},{"account":"system:revenue","atoms":"0"}]','2024-01-02T00:00:00Z' FROM wallets LIMIT 1`;
    expect((await db`SELECT count(*)::int AS n FROM journal_payloads_default`)[0].n).toBe(2);
    await expect(prepareEvidenceMonth(db, "2025-01")).rejects.toThrow("--quiesced");
    await prepareEvidenceMonth(db, "2025-01", true);
    expect((await db`SELECT count(*)::int AS n FROM journal_payloads_202501`)[0].n).toBe(1);
    await moveEvidenceMonth(db, "2025-01", "pg_default");
    await expect(db`UPDATE journal_payloads SET lines='[]'`).rejects.toThrow("immutable");
    await expect(prepareEvidenceMonth(db, "2024-01")).rejects.toThrow("--quiesced");
    await prepareEvidenceMonth(db, "2024-01", true);
    expect((await db`SELECT count(*)::int AS n FROM journal_payloads_default`)[0].n).toBe(0);
    expect((await db`SELECT count(*)::int AS n FROM journal_payloads_202401`)[0].n).toBe(1);
    await expect(db`DELETE FROM journal_payloads_202401`).rejects.toThrow("immutable");
    await mkdir(directory, { recursive: true });
    const archive = await exportEvidenceArchive(db, "2025-01", directory.pathname);
    const verified = await verifyEvidenceArchive(archive);
    expect(verified.files.find((file) => file.kind === "journal")?.rows).toBe(1);
    await appendFile(new URL("journal.ndjson.gz", directory), "corrupt");
    await expect(verifyEvidenceArchive(archive)).rejects.toThrow("checksum");
  } finally {
    await db.close();
    await owner.unsafe(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await owner.close();
    await rm(directory, { recursive: true, force: true });
  }
}, 30000);
