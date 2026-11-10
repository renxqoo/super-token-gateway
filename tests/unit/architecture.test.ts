import { expect, test } from "bun:test";

test("workspace dependencies stay acyclic and commercial capabilities stay outside gateway", async () => {
  const allowed: Record<string, string[]> = {
    kernel: [],
    connectors: ["kernel"],
    runtime: ["kernel"],
    storage: ["kernel"],
  };
  for (const [name, imports] of Object.entries(allowed)) {
    for await (const file of new Bun.Glob("**/*.ts").scan(`packages/${name}/src`)) {
      const source = await Bun.file(`packages/${name}/src/${file}`).text();
      for (const match of source.matchAll(/(?:from\s+|import\s*)["']@gateway\/([^"']+)/g))
        expect(imports).toContain(match[1] ?? "");
      expect(source).not.toMatch(/from\s+["'][^"']*(?:apps\/|wallet|payments|business-system)/);
    }
  }
  const mock = (await Bun.file("apps/mock-providers/package.json").json()) as {
    dependencies: Record<string, string>;
  };
  expect(Object.keys(mock.dependencies)).toEqual(["hono"]);
  for await (const file of new Bun.Glob("**/*.ts").scan("apps/gateway/src")) {
    const source = await Bun.file(`apps/gateway/src/${file}`).text();
    expect(source).not.toMatch(
      /(?:Bun\.write|writeFile|appendFile|console\.(?:debug|error|log)\([^\n]*(?:body|content|response))/,
    );
  }
});

test("the sole current implementation has no PG usage outbox or compatibility schema", async () => {
  const schemas = await Array.fromAsync(new Bun.Glob("schema*.sql").scan("packages/storage/src"));
  expect(schemas).toEqual(["schema.sql"]);
  const schema = await Bun.file("packages/storage/src/schema.sql").text();
  expect(schema).not.toMatch(
    /admit_initial_request|billing_work|record_gateway_usage|LEGACY|meter_gateway_batch|settle_request\(|reconcile_known_holds|reserve_wallet\(/,
  );
  expect(await Bun.file("packages/runtime/src/billing-queue.ts").exists()).toBe(false);
});
