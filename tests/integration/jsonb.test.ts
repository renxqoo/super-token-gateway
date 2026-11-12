import { expect, test } from "bun:test";
import {
  createRepositories,
  eq,
  jsonNull,
  migrate,
  placeholder,
  sql,
  Store,
} from "@gateway/storage";
import {
  costVersions,
  models,
  pluginPackages,
  pluginVersions,
  routes,
} from "@gateway/storage/schema";
import type { SQL } from "bun";
import { withDatabase } from "../support/database.ts";

function isolated(run: (db: SQL) => Promise<void>, controlledMutation = false) {
  return withDatabase(
    async (db) => {
      await migrate(db);
      if (controlledMutation)
        await db.begin(async (tx) => {
          await tx`SELECT set_config('gateway.catalog_mutation','on',true)`;
          await run(tx as unknown as SQL);
        });
      else await run(db);
    },
    { registry: ".local/T03-jsonb-resources.jsonl" },
  );
}

function packageInput() {
  const packageBytes = Buffer.from("synthetic-jsonb-regression");
  return {
    digest: new Bun.CryptoHasher("sha256").update(packageBytes).digest("hex"),
    packageBytes,
    manifest: { abi_version: 1, quantities: { atoms: "99999999999999999999999999999999999999" } },
    signature: "synthetic-signature",
    signingKeyId: "synthetic-key",
    uploadedBy: "synthetic-jsonb-test",
  };
}

function versionInput(version: string, validationResult: unknown) {
  return {
    id: crypto.randomUUID(),
    name: "jsonb-regression",
    version,
    packageDigest: packageInput().digest,
    abiVersion: 1,
    configSchemaDigest: "a".repeat(64),
    validationResult,
  };
}

const vectors = [
  { value: {}, expected: {}, kind: "object" },
  {
    value: { nested: { atoms: "9007199254740993", enabled: false }, list: [1, null, "42"] },
    expected: { nested: { atoms: "9007199254740993", enabled: false }, list: [1, null, "42"] },
    kind: "object",
  },
  { value: [], expected: [], kind: "array" },
  {
    value: ["9007199254740993", { count: 2 }],
    expected: ["9007199254740993", { count: 2 }],
    kind: "array",
  },
  { value: "plain text", expected: "plain text", kind: "string" },
  { value: "42", expected: "42", kind: "string" },
  { value: "false", expected: "false", kind: "string" },
  { value: "null", expected: "null", kind: "string" },
  { value: "{}", expected: "{}", kind: "string" },
  { value: "[]", expected: "[]", kind: "string" },
  { value: "9007199254740993", expected: "9007199254740993", kind: "string" },
  {
    value: "99999999999999999999999999999999999999",
    expected: "99999999999999999999999999999999999999",
    kind: "string",
  },
  { value: 0, expected: 0, kind: "number" },
  { value: 42, expected: 42, kind: "number" },
  { value: 1.25, expected: 1.25, kind: "number" },
  { value: true, expected: true, kind: "boolean" },
  { value: false, expected: false, kind: "boolean" },
  { value: null, expected: null, kind: null },
];

test("typed JSONB manifest is not double encoded into a string rejected by plugin_manifest", () =>
  isolated(async (db) => {
    const repo = createRepositories(db);
    const [row] = await repo.insert(pluginPackages).values(packageInput()).returning();
    expect(row?.manifest).toEqual({
      abi_version: 1,
      quantities: { atoms: "99999999999999999999999999999999999999" },
    });
    expect((await db`SELECT jsonb_typeof(manifest) AS kind FROM plugin_packages`)[0].kind).toBe(
      "object",
    );
  }));

test("typed JSONB CRUD preserves nested values, scalar types and exact atoms strings", () =>
  isolated(async (db) => {
    const repo = createRepositories(db);
    await repo.insert(pluginPackages).values(packageInput());
    for (const [index, vector] of vectors.entries()) {
      const input = versionInput(String(index), vector.value);
      const [inserted] = await repo.insert(pluginVersions).values(input).returning();
      expect(inserted?.validationResult).toEqual(vector.expected);
      const [selected] = await repo
        .select()
        .from(pluginVersions)
        .where(eq(pluginVersions.id, input.id));
      expect(selected?.validationResult).toEqual(vector.expected);
      await repo
        .update(pluginVersions)
        .set({ validationResult: "temporary-update" })
        .where(eq(pluginVersions.id, input.id));
      const [updated] = await repo
        .update(pluginVersions)
        .set({ validationResult: vector.value })
        .where(eq(pluginVersions.id, input.id))
        .returning();
      expect(updated?.validationResult).toEqual(vector.expected);
      await repo
        .update(pluginVersions)
        .set({ validationResult: "temporary-upsert" })
        .where(eq(pluginVersions.id, input.id));
      const [upserted] = await repo
        .insert(pluginVersions)
        .values({ ...input, validationResult: "replaced" })
        .onConflictDoUpdate({ target: pluginVersions.id, set: { validationResult: vector.value } })
        .returning();
      expect(upserted?.validationResult).toEqual(vector.expected);
      const [raw] = await db`SELECT jsonb_typeof(validation_result) AS kind,
        validation_result IS NULL AS sql_null FROM plugin_versions WHERE id=${input.id}`;
      expect(raw.kind).toBe(vector.kind);
      expect(raw.sql_null).toBe(vector.kind === null);
    }
  }, true));

test("prepared JSONB insert, update and encoded where preserve scalar and structured values", () =>
  isolated(async (db) => {
    const repo = createRepositories(db);
    await repo.insert(pluginPackages).values(packageInput());
    const insert = repo
      .insert(pluginVersions)
      .values({
        ...versionInput("unused", null),
        id: placeholder("id"),
        version: placeholder("version"),
        validationResult: placeholder("value"),
      })
      .returning()
      .prepare("jsonb_insert");
    const update = repo
      .update(pluginVersions)
      .set({ validationResult: placeholder("value") })
      .where(eq(pluginVersions.id, placeholder("id")))
      .returning()
      .prepare("jsonb_update");
    const select = repo
      .select()
      .from(pluginVersions)
      .where(
        eq(
          pluginVersions.validationResult,
          sql.param(placeholder("value"), pluginVersions.validationResult),
        ),
      )
      .prepare("jsonb_encoded_where");
    for (const [index, vector] of vectors.entries()) {
      const id = crypto.randomUUID();
      const [inserted] = await insert.execute({ id, version: String(index), value: vector.value });
      expect(inserted?.validationResult).toEqual(vector.expected);
      await update.execute({ id, value: "temporary" });
      const [updated] = await update.execute({ id, value: vector.value });
      expect(updated?.validationResult).toEqual(vector.expected);
      const matches = await select.execute({ value: vector.value });
      expect(matches.map((row) => row.id)).toEqual(vector.kind === null ? [] : [id]);
    }
  }, true));

test("JSON null and SQL NULL retain distinct database semantics through typed writes", () =>
  isolated(async (db) => {
    const repo = createRepositories(db);
    await repo.insert(pluginPackages).values(packageInput());
    const json = versionInput("json-null", jsonNull);
    const nullable = versionInput("sql-null", null);
    const omitted = versionInput("omitted", undefined);
    await repo.insert(pluginVersions).values([json, nullable, omitted]);
    expect(
      await repo.select({ value: pluginVersions.validationResult }).from(pluginVersions),
    ).toEqual([{ value: null }, { value: null }, { value: null }]);
    expect(
      await db`SELECT version,jsonb_typeof(validation_result) AS kind,validation_result IS NULL AS sql_null
      FROM plugin_versions ORDER BY version`,
    ).toEqual([
      { version: "json-null", kind: "null", sql_null: false },
      { version: "omitted", kind: null, sql_null: true },
      { version: "sql-null", kind: null, sql_null: true },
    ]);
    const [updated] = await repo
      .update(pluginVersions)
      .set({ validationResult: jsonNull })
      .where(eq(pluginVersions.id, nullable.id))
      .returning();
    expect(updated?.validationResult).toBeNull();
    expect(
      (
        await db`SELECT validation_result IS NULL AS sql_null FROM plugin_versions WHERE id=${nullable.id}`
      )[0].sql_null,
    ).toBe(false);
  }, true));

test("JSONB transaction commit and rollback use the explicit business pool", () =>
  isolated(async (db) => {
    const repo = createRepositories(db);
    const [identity] = await db`SELECT current_database() AS name, pg_backend_pid() AS pid`;
    await repo.transaction(async (tx) => {
      const identityInTransaction = await tx.execute(
        sql`SELECT current_database() AS name, pg_backend_pid() AS pid`,
      );
      expect(identityInTransaction[0]).toEqual(identity);
      await tx.insert(pluginPackages).values(packageInput());
      const [row] = await tx
        .insert(pluginVersions)
        .values(versionInput("committed", { atoms: "9007199254740993" }))
        .returning();
      expect(row?.validationResult).toEqual({ atoms: "9007199254740993" });
    });
    await expect(
      repo.transaction(async (tx) => {
        await tx.execute(sql`SELECT set_config('gateway.catalog_mutation','on',true)`);
        await tx.update(pluginVersions).set({ validationResult: { atoms: "1" } });
        await tx.insert(pluginVersions).values(versionInput("rolled-back", false));
        throw new Error("rollback JSONB transaction");
      }),
    ).rejects.toThrow("rollback JSONB transaction");
    expect(
      await repo
        .select({ version: pluginVersions.version, result: pluginVersions.validationResult })
        .from(pluginVersions),
    ).toEqual([{ version: "committed", result: { atoms: "9007199254740993" } }]);
  }));

test("current JSONB object defaults retain their checks and typed default writes", () =>
  isolated(async (db) => {
    const repo = createRepositories(db);
    const store = new Store(db);
    const provider = crypto.randomUUID();
    await store.createProvider(
      provider,
      {
        name: "jsonb-default",
        kind: "openai",
        base_url: "http://fixture.invalid/v1",
        rpm: 10,
        concurrency: 1,
      },
      "synthetic-secret",
    );
    await repo
      .insert(models)
      .values({ name: "jsonb-default", operations: ["chat"], maxOutputTokens: 10 });
    const price = crypto.randomUUID();
    const [cost] = await repo
      .insert(costVersions)
      .values({
        id: price,
        currency: "USD",
        rates: { input_tokens: { numerator: "1", denominator: "3" } },
      })
      .returning();
    expect(cost?.ceilings).toEqual({});
    const [route] = await repo
      .insert(routes)
      .values({
        id: crypto.randomUUID(),
        modelName: "jsonb-default",
        providerId: provider,
        upstreamModel: "synthetic",
        operations: ["chat"],
        priority: 0,
        weight: 1,
        pricingId: price,
      })
      .returning();
    expect(route?.requestPolicy).toEqual({});
    await expect(
      repo
        .insert(costVersions)
        .values({ id: crypto.randomUUID(), currency: "USD", rates: {}, ceilings: "{}" })
        .execute(),
    ).rejects.toMatchObject({
      cause: { errno: "23514", constraint: "cost_versions_ceilings_check" },
    });
    await expect(
      repo.update(routes).set({ requestPolicy: "{}" }).where(eq(routes.id, route!.id)).execute(),
    ).rejects.toMatchObject({
      cause: { errno: "23514", constraint: "routes_request_policy_check" },
    });
  }));

test("JSONB mapping preserves real plugin manifest and package byte constraints", () =>
  isolated(async (db) => {
    const repo = createRepositories(db);
    await expect(
      repo
        .insert(pluginPackages)
        .values({ ...packageInput(), manifest: "{}" })
        .execute(),
    ).rejects.toMatchObject({ cause: { errno: "23514", constraint: "plugin_manifest" } });
    await expect(
      repo
        .insert(pluginPackages)
        .values({ ...packageInput(), packageBytes: Buffer.alloc(0) })
        .execute(),
    ).rejects.toMatchObject({ cause: { errno: "23514", constraint: "plugin_package_size" } });
    expect((await db`SELECT count(*)::int AS n FROM plugin_packages`)[0].n).toBe(0);
  }));

test("stored JSON looking scalar strings are not parsed a second time on typed reads", () =>
  isolated(async (db) => {
    const repo = createRepositories(db);
    await repo
      .insert(pluginPackages)
      .values({ ...packageInput(), manifest: sql`'{"abi_version":1}'::jsonb` });
    for (const [index, text] of ["42", "false", "null", "{}", "9007199254740993"].entries()) {
      const id = crypto.randomUUID();
      await db`INSERT INTO plugin_versions(id,name,version,package_digest,abi_version,config_schema_digest,validation_result)
        VALUES(${id},'scalar-read',${String(index)},${packageInput().digest},1,repeat('a',64),${db.json(text)})`;
      const [row] = await repo.select().from(pluginVersions).where(eq(pluginVersions.id, id));
      expect(row?.validationResult).toBe(text);
    }
  }));
