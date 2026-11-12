import { expect, test } from "bun:test";

test("workspace dependencies stay acyclic and commercial capabilities stay outside gateway", async () => {
  const allowed: Record<string, string[]> = {
    kernel: [],
    contracts: ["kernel"],
    connectors: ["kernel", "contracts"],
    runtime: ["kernel", "contracts"],
    "core-client": ["kernel", "contracts", "runtime/crypto", "runtime/h2"],
    storage: ["kernel", "contracts", "runtime/batcher", "runtime/crypto"],
  };
  for (const [name, imports] of Object.entries(allowed)) {
    for await (const file of new Bun.Glob("**/*.ts").scan(`packages/${name}/src`)) {
      const source = await Bun.file(`packages/${name}/src/${file}`).text();
      for (const match of source.matchAll(/(?:from\s+|import\s*)["']@gateway\/([^"']+)/g))
        expect(imports).toContain(match[1] ?? "");
      expect(source).not.toMatch(/from\s+["'][^"']*(?:apps\/|wallet|payments|business-system)/);
      if (name === "kernel" && !file.includes("__test__/"))
        expect(source).not.toMatch(
          /(?:from\s*["'](?:node:|bun)|\b(?:fetch|setTimeout|setInterval|Bun|process)\b)/,
        );
    }
  }
  const mock = (await Bun.file("apps/mock-providers/package.json").json()) as {
    dependencies: Record<string, string>;
  };
  expect(Object.keys(mock.dependencies)).toEqual(["hono"]);
  for await (const file of new Bun.Glob("**/*.ts").scan("apps/gateway/src")) {
    const source = await Bun.file(`apps/gateway/src/${file}`).text();
    if (["app.ts", "admin.ts"].includes(file)) expect(source).not.toMatch(/@gateway\/storage/);
    expect(source).not.toMatch(/\.db\s*(?:`|<|\.)|\.unsafe\s*\(/);
    expect(source).not.toMatch(
      /(?:Bun\.write|writeFile|appendFile|console\.(?:debug|error|log)\([^\n]*(?:body|content|response))/,
    );
  }
});

test("every schema artifact has one checksummed migration owner and no compatibility outbox", async () => {
  const root = "packages/storage/src/";
  const manifest = (await Bun.file(`${root}migrations/manifest.json`).json()) as {
    migrations: { files: { path: string; checksum: string }[] }[];
  };
  const reachable = new Set<string>();
  for (const entry of manifest.migrations.flatMap((migration) => migration.files)) {
    expect(reachable.has(entry.path)).toBe(false);
    reachable.add(entry.path);
    const source = await Bun.file(
      new URL(entry.path, new URL(`../../${root}database.ts`, import.meta.url)),
    ).text();
    expect(new Bun.CryptoHasher("sha256").update(source).digest("hex")).toBe(entry.checksum);
    expect(source).not.toMatch(
      /admit_initial_request|billing_work|record_gateway_usage|LEGACY|meter_gateway_batch|settle_request\(|reconcile_known_holds|reserve_wallet\(/,
    );
  }
  const sql = await Array.fromAsync(new Bun.Glob("**/*.sql").scan(root));
  expect(sql.map((path) => `./${path}`).sort()).toEqual([...reachable].sort());
  expect(await Bun.file("packages/runtime/src/billing-queue.ts").exists()).toBe(false);
  const migrator = await Bun.file(`${root}database.ts`).text();
  expect(migrator).not.toMatch(/\.match\(|CREATE OR REPLACE FUNCTION|supported upgrades/);
});

test("Drizzle declarations and the versioned ordinary schema snapshot cannot drift", async () => {
  const { generateDrizzleJson, generateMigration } = await import("drizzle-kit/api");
  const schema = await import("@gateway/storage/schema");
  const journal = await Bun.file("packages/storage/src/migrations/meta/_journal.json").json();
  const index = String(journal.entries.at(-1).idx).padStart(4, "0");
  const snapshot = await Bun.file(
    `packages/storage/src/migrations/meta/${index}_snapshot.json`,
  ).json();
  expect(await generateMigration(snapshot, generateDrizzleJson(schema, snapshot.id))).toEqual([]);
});
