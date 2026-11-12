import { expect, test } from "bun:test";
import { createRepositories, migrate, SCHEMA_VERSION, Store } from "@gateway/storage";
import { supplierBrands, upstreamAccounts, upstreamAccountBalances } from "@gateway/storage/schema";
import { withDatabase } from "../support/database.ts";

test("new v2 baseline initializes once without fabricated upstream funding", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const before =
      await db`SELECT oid::text,relname FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid`;
    await migrate(db);
    expect(
      await db`SELECT oid::text,relname FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid`,
    ).toEqual(before);
    expect(
      (await db`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`)[0].version,
    ).toBe(SCHEMA_VERSION);
    expect(
      (
        await db`SELECT (SELECT count(*)::int FROM upstream_account_balances) AS balances,(SELECT count(*)::int FROM upstream_funding_operations) AS funding`
      )[0],
    ).toEqual({ balances: 0, funding: 0 });
  }));
test("nonempty unknown databases are refused without altering their data", () =>
  withDatabase(async (db) => {
    await db`CREATE TABLE existing_data(id integer PRIMARY KEY,value text NOT NULL)`;
    await db`INSERT INTO existing_data VALUES(1,'retained')`;
    await expect(migrate(db)).rejects.toThrow("nonempty");
    expect(await db`SELECT * FROM existing_data`).toEqual([{ id: 1, value: "retained" }]);
    expect((await db`SELECT to_regclass('schema_migrations') AS relation`)[0].relation).toBeNull();
    await db`DROP TABLE existing_data`;
    await db`CREATE SCHEMA unrelated`;
    await expect(migrate(db)).rejects.toThrow("nonempty");
    expect(
      (await db`SELECT count(*)::int AS n FROM pg_namespace WHERE nspname='unrelated'`)[0].n,
    ).toBe(1);
  }));
test("historical and incorrect schema versions are refused without an implicit upgrade", () =>
  withDatabase(async (db) => {
    await db`CREATE TABLE schema_migrations(version integer PRIMARY KEY,applied_at timestamptz DEFAULT now())`;
    await db`INSERT INTO schema_migrations(version) VALUES(2026111201)`;
    await expect(migrate(db)).rejects.toThrow("new empty v2 database");
    expect(
      (await db`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`)[0].version,
    ).toBe(2026111201);
    expect(
      (
        await db`SELECT count(*)::int AS n FROM information_schema.columns WHERE table_name='schema_migrations' AND column_name='checksum'`
      )[0].n,
    ).toBe(0);
  }));
test("changed applied manifest checksums refuse replay without touching funds", () =>
  withDatabase(async (db) => {
    await migrate(db);
    await db`UPDATE schema_migrations SET checksum=repeat('0',64)`;
    await expect(migrate(db)).rejects.toThrow("checksum mismatch");
    expect((await db`SELECT count(*)::int AS n FROM upstream_funding_operations`)[0].n).toBe(0);
  }));
test("interrupted baseline DDL rolls back all tables and can be retried", () =>
  withDatabase(async (db) => {
    await db`SET statement_timeout=1`;
    await expect(migrate(db)).rejects.toMatchObject({ errno: "57014" });
    await db`SET statement_timeout=8000`;
    expect(
      (
        await db`SELECT count(*)::int AS n FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind IN ('r','p','S','v')`
      )[0].n,
    ).toBe(0);
    await migrate(db);
    expect(
      (await db`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`)[0].version,
    ).toBe(SCHEMA_VERSION);
  }));
test("Drizzle and Bun.SQL preserve atoms above Number precision in each currency", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const repo = createRepositories(db);
    const brand = crypto.randomUUID(),
      account = crypto.randomUUID();
    await repo.transaction(async (tx) => {
      await tx.insert(supplierBrands).values({ id: brand, name: "typed-test" });
      await tx.insert(upstreamAccounts).values({ id: account, brandId: brand, name: "isolated" });
      await tx.insert(upstreamAccountBalances).values(
        ["CNY", "USD", "HKD"].map((currency) => ({
          accountId: account,
          currency,
          balanceAtoms: "9007199254740993",
          revision: "9007199254740993",
        })),
      );
    });
    const balances = await repo.select().from(upstreamAccountBalances);
    expect(balances.map((row) => [row.currency, row.balanceAtoms, row.revision]).sort()).toEqual([
      ["CNY", "9007199254740993", "9007199254740993"],
      ["HKD", "9007199254740993", "9007199254740993"],
      ["USD", "9007199254740993", "9007199254740993"],
    ]);
    expect(
      (await db`SELECT sum(balance_atoms)::text AS total FROM upstream_account_balances`)[0].total,
    ).toBe("27021597764222979");
    await expect(
      repo.transaction(async (tx) => {
        await tx.insert(supplierBrands).values({ id: crypto.randomUUID(), name: "rolled-back" });
        throw new Error("rollback typed transaction");
      }),
    ).rejects.toThrow("rollback typed transaction");
    expect((await db`SELECT count(*)::int AS n FROM supplier_brands`)[0].n).toBe(1);
  }));
test("SQL prices thirds, milliseconds and large integer music usage independently", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const quote = async (
      usage: Record<string, string>,
      rates: Record<string, { numerator: string; denominator: string }>,
    ) =>
      (await db`SELECT exact_billing_price(${usage}::jsonb,${rates}::jsonb)::text AS atoms`)[0]
        .atoms;
    expect(
      await quote(
        { input_tokens: "1", output_tokens: "1" },
        {
          input_tokens: { numerator: "1", denominator: "3" },
          output_tokens: { numerator: "1", denominator: "3" },
        },
      ),
    ).toBe("1");
    expect(
      await quote({ audio_ms: "125" }, { audio_ms: { numerator: "8", denominator: "1000" } }),
    ).toBe("1");
    expect(
      await quote({ music_ms: "125" }, { music_ms: { numerator: "8", denominator: "1000" } }),
    ).toBe("1");
    expect(
      await quote(
        { music_ms: "9007199254740993" },
        { music_ms: { numerator: "1", denominator: "1" } },
      ),
    ).toBe("9007199254740993");
  }));
test("upstream journals reject unbalanced commits and retain immutable balanced entries", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const brand = crypto.randomUUID(),
      account = crypto.randomUUID();
    await db`INSERT INTO supplier_brands(id,name) VALUES(${brand},'ledger-test')`;
    await db`INSERT INTO upstream_accounts(id,brand_id,name) VALUES(${account},${brand},'ledger-test')`;
    const journal = crypto.randomUUID();
    await expect(
      db.begin(async (tx) => {
        await tx`INSERT INTO upstream_journals(id,account_id,currency,kind,reference) VALUES(${journal},${account},'USD','TEST_OPENING','unbalanced')`;
        await tx`INSERT INTO upstream_entries VALUES(${journal},0,'USD','upstream:balance',100)`;
      }),
    ).rejects.toThrow("unbalanced upstream journal");
    expect((await db`SELECT count(*)::int AS n FROM upstream_journals`)[0].n).toBe(0);
    await db.begin(async (tx) => {
      await tx`INSERT INTO upstream_journals(id,account_id,currency,kind,reference) VALUES(${journal},${account},'USD','TEST_OPENING','balanced')`;
      await tx`INSERT INTO upstream_entries VALUES(${journal},0,'USD','upstream:balance',100),(${journal},1,'USD','external:funding',-100)`;
    });
    await expect(
      db`UPDATE upstream_entries SET atoms=0 WHERE journal_id=${journal}`,
    ).rejects.toThrow("immutable");
    expect(
      (
        await db`SELECT sum(atoms)::text AS total FROM upstream_entries WHERE journal_id=${journal}`
      )[0].total,
    ).toBe("0");
  }));
test("package and financial voucher bytes are bounded", () =>
  withDatabase(async (db) => {
    await migrate(db);
    await expect(
      db`INSERT INTO financial_vouchers(id,digest,content_type,content,actor,expires_at) VALUES(${crypto.randomUUID()},repeat('a',64),'application/pdf',convert_to(repeat('x',5242881),'UTF8'),'test',now())`,
    ).rejects.toMatchObject({ errno: "23514" });
    await expect(
      db`INSERT INTO plugin_packages(digest,package_bytes,manifest,signature,signing_key_id,uploaded_by) VALUES(repeat('a',64),''::bytea,'{}','signature','test','test')`,
    ).rejects.toMatchObject({ errno: "23514" });
    expect((await db`SELECT count(*)::int AS n FROM financial_vouchers`)[0].n).toBe(0);
  }));

test("resource URLs and arbitrary metadata are rejected before persistence", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const store = new Store(db);
    const project = await store.createProject({
      name: "resource-validation",
      rpm: 100,
      tpm: 1000,
      concurrency: 10,
    });
    const channel = crypto.randomUUID();
    await store.createProvider(
      channel,
      {
        name: "resource-validation",
        kind: "openai",
        base_url: "http://fixture.invalid/v1",
        rpm: 100,
        concurrency: 10,
      },
      "encrypted-fixture",
    );
    for (const remote of ["https://media.example/file", "file?signed=token"])
      await expect(
        store.saveResource(project, channel, remote, "file", null),
      ).rejects.toMatchObject({ status: 502, code: "invalid_upstream" });
    const invalidBatch = {
      endpoint: "/v1/embeddings",
      upstreamModel: "model",
      manifest: {
        items: 1,
        idsSha256: "a".repeat(64),
        download_url: "https://media.example/?signed=token",
      },
    };
    await expect(
      store.saveResource(
        project,
        channel,
        "files/stable",
        "file",
        null,
        null,
        undefined,
        invalidBatch,
      ),
    ).rejects.toMatchObject({ status: 502, code: "invalid_upstream" });
    expect((await db`SELECT count(*)::int AS n FROM resources`)[0].n).toBe(0);
    const id = await store.saveResource(project, channel, "files/stable", "file", null);
    expect((await store.resource(id, project)).remote_id).toBe("files/stable");
    expect((await db`SELECT binding_id FROM resources WHERE id=${id}`)[0].binding_id).toBeNull();
    await expect(
      db`UPDATE resources SET binding_id=${crypto.randomUUID()} WHERE id=${id}`,
    ).rejects.toMatchObject({ errno: "23503" });
    await db`INSERT INTO cost_versions(id,currency,rates) VALUES(${crypto.randomUUID()},'USD','{}')`;
    expect((await db`SELECT ceilings FROM cost_versions`)[0].ceilings).toEqual({});
    await expect(
      db`INSERT INTO cost_versions(id,currency,rates,ceilings) VALUES(${crypto.randomUUID()},'USD','{}','[]')`,
    ).rejects.toMatchObject({ errno: "23514" });

    await expect(
      db`INSERT INTO resources(id,project_id,provider_id,remote_id,kind) VALUES('unsafe',${project},${channel},'https://media.example/file','file')`,
    ).rejects.toMatchObject({ errno: "23514" });
  }));

test("public enum or domain-only databases are refused without creating the baseline", async () => {
  for (const definition of [
    "CREATE TYPE public.existing_type AS ENUM ('retained')",
    "CREATE DOMAIN public.existing_type AS text CHECK(length(VALUE)>0)",
  ]) {
    await withDatabase(async (db) => {
      await db.unsafe(definition);
      await expect(migrate(db)).rejects.toThrow("nonempty");
      expect((await db`SELECT to_regtype('public.existing_type')::text AS name`)[0].name).toBe(
        "existing_type",
      );
      expect(
        (await db`SELECT to_regclass('public.schema_migrations') AS relation`)[0].relation,
      ).toBeNull();
    });
  }
});

test("an additional extension without public relations is refused while default plpgsql remains allowed", () =>
  withDatabase(async (db) => {
    await db`CREATE EXTENSION btree_gin WITH SCHEMA pg_catalog`;
    await expect(migrate(db)).rejects.toThrow("nonempty");
    expect(
      (
        await db`SELECT count(*)::int AS n FROM pg_extension WHERE extname IN ('btree_gin','plpgsql')`
      )[0].n,
    ).toBe(2);
    expect(
      (await db`SELECT to_regclass('public.schema_migrations') AS relation`)[0].relation,
    ).toBeNull();
  }));

test("RECHARGE cannot persist NULL payment atoms while adjustments retain optional payment facts", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const brand = crypto.randomUUID(),
      account = crypto.randomUUID(),
      voucher = crypto.randomUUID();
    await db`INSERT INTO supplier_brands(id,name) VALUES(${brand},'recharge-facts')`;
    await db`INSERT INTO upstream_accounts(id,brand_id,name) VALUES(${account},${brand},'recharge-facts')`;
    await db`INSERT INTO financial_vouchers(id,digest,content_type,content,actor,expires_at) VALUES(${voucher},${new Bun.CryptoHasher("sha256").update("synthetic").digest("hex")},'application/pdf',${Buffer.from("synthetic")},'test',now()+interval '1 hour')`;
    const valid = {
      payment: "100",
      currency: "CNY",
      credited: "25",
      voucher,
      digest: new Bun.CryptoHasher("sha256").update("synthetic").digest("hex"),
    };
    for (const invalid of [
      { ...valid, payment: null },
      { ...valid, payment: "0" },
      { ...valid, payment: "-1" },
      { ...valid, currency: null },
      { ...valid, credited: "0" },
      { ...valid, voucher: null },
      { ...valid, digest: null },
    ])
      await expect(
        db`INSERT INTO upstream_funding_operations(id,account_id,kind,external_ref,payload_digest,payment_currency,payment_atoms,credited_currency,credited_atoms,voucher_id,voucher_digest,completed_at,actor,reason,receipt) VALUES(${crypto.randomUUID()},${account},'RECHARGE',${crypto.randomUUID()},${"b".repeat(64)},${invalid.currency},${invalid.payment},'USD',${invalid.credited},${invalid.voucher},${invalid.digest},now(),'test','synthetic recharge','{}')`,
      ).rejects.toMatchObject({ errno: "23514" });
    await db`INSERT INTO upstream_funding_operations(id,account_id,kind,external_ref,payload_digest,payment_currency,payment_atoms,credited_currency,credited_atoms,voucher_id,voucher_digest,completed_at,actor,reason,receipt) VALUES(${crypto.randomUUID()},${account},'RECHARGE','complete',${"b".repeat(64)},'CNY',100,'USD',25,${voucher},${valid.digest},now(),'test','synthetic recharge','{}')`;
    await db`INSERT INTO upstream_funding_operations(id,account_id,kind,external_ref,payload_digest,credited_currency,credited_atoms,completed_at,actor,reason,receipt) VALUES(${crypto.randomUUID()},${account},'ADJUSTMENT','adjust',${"c".repeat(64)},'USD',-1,now(),'test','synthetic adjustment','{}')`;
    expect(
      await db`SELECT kind,payment_atoms::text,credited_atoms::text FROM upstream_funding_operations ORDER BY kind`,
    ).toEqual([
      { kind: "ADJUSTMENT", payment_atoms: null, credited_atoms: "-1" },
      { kind: "RECHARGE", payment_atoms: "100", credited_atoms: "25" },
    ]);
  }));

test("resource manifests reject JSON null and wrong field types instead of CHECK unknown bypass", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const store = new Store(db);
    const project = await store.createProject({
      name: "batch-facts",
      rpm: 100,
      tpm: 1000,
      concurrency: 2,
    });
    const channel = crypto.randomUUID();
    await store.createProvider(
      channel,
      {
        name: "batch-facts",
        kind: "openai",
        base_url: "http://fixture.invalid/v1",
        rpm: 100,
        concurrency: 2,
      },
      "synthetic-encrypted",
    );
    const valid = { items: 1, idsSha256: "a".repeat(64) };
    const insert = (manifest: string | null) => {
      const id = crypto.randomUUID();
      return db`INSERT INTO resources(id,project_id,provider_id,remote_id,kind,batch_manifest) VALUES(${id},${project},${channel},${id},'batch',${manifest}::text::jsonb)`;
    };
    for (const invalid of [
      { ...valid, idsSha256: null },
      { ...valid, images: null },
      { ...valid, items: null },
      { items: 1 },
      { idsSha256: valid.idsSha256 },
      { ...valid, items: "1" },
      { ...valid, images: "1" },
      { ...valid, images: false },
      { ...valid, items: 0 },
      { ...valid, items: 1.5 },
      { ...valid, items: 9007199254740992 },
      { ...valid, images: 0 },
      { ...valid, images: 1.5 },
      { ...valid, images: 9007199254740992 },
      { ...valid, extra: "arbitrary metadata" },
      null,
      [],
    ])
      await expect(insert(JSON.stringify(invalid))).rejects.toMatchObject({ errno: "23514" });
    await expect(insert(`{"items":1,"idsSha256":${"1".repeat(64)}}`)).rejects.toMatchObject({
      errno: "23514",
    });
    await insert(null);
    await insert(JSON.stringify(valid));
    await insert(JSON.stringify({ ...valid, items: 9007199254740991, images: 9007199254740991 }));
    expect((await db`SELECT count(*)::int AS n FROM resources`)[0].n).toBe(3);
    expect(
      (await db`SELECT count(*)::int AS n FROM resources WHERE batch_manifest IS NULL`)[0].n,
    ).toBe(1);
  }));
