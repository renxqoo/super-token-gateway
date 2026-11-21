import { expect, test } from "bun:test";
import { readdir } from "node:fs/promises";

test("workspace dependencies remain strictly acyclic with zero legacy dual-track modules", async () => {
  const apps = (await readdir("apps")).filter((name) => !name.startsWith(".")).sort();
  expect(apps).toEqual(["gateway", "mock-providers", "worker"]);

  const packages = (await readdir("packages")).filter((name) => !name.startsWith(".")).sort();
  expect(packages).toEqual([
    "contracts",
    "kernel",
    "ledger",
    "plugin-engine",
    "protocol",
    "runtime",
  ]);

  const allowed: Record<string, string[]> = {
    kernel: [],
    contracts: ["kernel"],
    protocol: ["kernel", "contracts"],
    runtime: ["kernel", "contracts"],
    ledger: ["kernel"],
    "plugin-engine": ["ledger"],
  };

  for (const [name, imports] of Object.entries(allowed)) {
    const root = `packages/${name}/src`;
    for await (const file of new Bun.Glob("**/*.ts").scan(root)) {
      const source = await Bun.file(`${root}/${file}`).text();
      for (const match of source.matchAll(/(?:from\s+|import\s*)["']@gateway\/([^"']+)/g)) {
        expect(imports).toContain(match[1] ?? "");
      }
      expect(source).not.toMatch(
        /@gateway\/(?:storage|core-client|plugin-host|plugin-sdk|adapters)/,
      );
      if (name === "kernel" && !file.includes("__test__/")) {
        expect(source).not.toMatch(
          /(?:from\s*["'](?:node:|bun)|\b(?:fetch|setTimeout|setInterval|Bun|process)\b)/,
        );
      }
    }
  }

  const mock = (await Bun.file("apps/mock-providers/package.json").json()) as {
    dependencies: Record<string, string>;
  };
  expect(Object.keys(mock.dependencies)).toEqual(["hono"]);
});

test("apps keep strict responsibility boundaries and never reference legacy artifacts", async () => {
  const forbidden: Record<string, RegExp> = {
    gateway: /@gateway\/(?:storage|core-client|plugin-host|plugin-sdk|adapters)|drizzle-orm/,
    worker: /@gateway\/(?:gateway|protocol|plugin-engine|storage|core-client)|drizzle-orm/,
    "mock-providers": /@gateway\//,
  };
  for (const [app, pattern] of Object.entries(forbidden)) {
    for await (const file of new Bun.Glob("**/*.ts").scan(`apps/${app}/src`)) {
      const source = await Bun.file(`apps/${app}/src/${file}`).text();
      for (const match of source.matchAll(/(?:from\s+|import\s*\(?\s*)["']([^"']+)["']/g)) {
        expect(match[1] ?? "").not.toMatch(pattern);
      }
    }
  }
});

test("ledger schema baseline has checksummed single-owner manifest and matches disk SQL", async () => {
  const root = "packages/ledger/src/";
  const manifest = (await Bun.file(`${root}migrations/manifest.json`).json()) as {
    migrations: { version: number; files: { path: string; checksum: string }[] }[];
  };
  const reachable = new Set<string>();
  for (const entry of manifest.migrations.flatMap((m) => m.files)) {
    expect(reachable.has(entry.path)).toBe(false);
    reachable.add(entry.path);
    const source = await Bun.file(
      new URL(entry.path, new URL(`../../${root}database.ts`, import.meta.url)),
    ).text();
    expect(new Bun.CryptoHasher("sha256").update(source).digest("hex")).toBe(entry.checksum);
  }
  const sqlFiles = await Array.fromAsync(new Bun.Glob("**/*.sql").scan(root));
  expect(sqlFiles.map((p) => `./${p}`).sort()).toEqual([...reachable].sort());
});

test("deployment units match build script targets and expose zero internal admin routes via Caddy", async () => {
  const build = await Bun.file("scripts/build.ts").text();
  const units: Record<string, Record<string, string[]>> = {};
  for await (const file of new Bun.Glob("*.service").scan("infra")) {
    const fields: Record<string, string[]> = {};
    for (const line of (await Bun.file(`infra/${file}`).text()).split("\n")) {
      const match = /^([A-Za-z]+)=(.*)$/.exec(line);
      if (match) (fields[match[1]!] ??= []).push(match[2]!);
    }
    units[file] = fields;
    const entry = /^\/usr\/local\/bin\/bun (dist\/([a-z-]+)\/([a-z-]+)\.js)(?:\s|$)/.exec(
      fields.ExecStart?.[0] ?? "",
    );
    expect(entry).not.toBeNull();
    const source = `apps/${entry![2]}/src/${entry![3]}.ts`;
    expect(await Bun.file(source).exists()).toBe(true);
    expect(build).toContain(`"${source}"`);
    expect(build).toContain(`"dist/${entry![2]}"`);
    expect(fields.User?.[0]).not.toBe("root");
    expect(fields.NoNewPrivileges).toEqual(["true"]);
    expect(fields.ProtectSystem).toEqual(["strict"]);
  }
  expect(Object.keys(units).sort()).toEqual(["gateway@.service", "worker.service"]);
  const caddy = await Bun.file("infra/Caddyfile").text();
  expect(caddy).not.toMatch(/\/internal|\/admin|8081/);
  expect(caddy).toContain("/v1/chat/completions");
  expect(caddy).toContain("flush_interval -1");
});
