import { expect, test } from "bun:test";
import { readdir } from "node:fs/promises";
import { posix } from "node:path";

test("workspace dependencies remain strictly acyclic with zero legacy dual-track modules", async () => {
  const apps = (await readdir("apps")).filter((name) => !name.startsWith(".")).sort();
  expect(apps).toEqual(["console-api", "gateway", "mock-providers", "worker"]);

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
    "plugin-engine": ["contracts", "ledger", "protocol"],
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
    "console-api":
      /@gateway\/(?:ledger|gateway|worker|plugin-engine|protocol|runtime|storage|core-client)/,
    gateway:
      /@gateway\/(?:console-api|storage|core-client|plugin-host|plugin-sdk|adapters)|drizzle-orm/,
    worker:
      /@gateway\/(?:console-api|gateway|protocol|plugin-engine|storage|core-client)|drizzle-orm/,
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

test("console-api schema baseline has checksummed single-owner manifest and matches disk SQL", async () => {
  const root = "apps/console-api/src/db/";
  const manifest = (await Bun.file(`${root}migrations/manifest.json`).json()) as {
    migrations: { version: number; path: string; checksum: string }[];
  };
  const reachable = new Set<string>();
  for (const entry of manifest.migrations) {
    expect(reachable.has(entry.path)).toBe(false);
    reachable.add(entry.path);
    const source = await Bun.file(
      new URL(entry.path, new URL(`../../${root}migrate.ts`, import.meta.url)),
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
    const entry =
      /^\/usr\/local\/bin\/bun (?:--no-env-file )?(dist\/([a-z-]+)\/([a-z-]+)\.js)(?:\s|$)/.exec(
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
  expect(Object.keys(units).sort()).toEqual([
    "console-api@.service",
    "gateway@.service",
    "worker.service",
  ]);
  const caddy = await Bun.file("infra/Caddyfile").text();
  expect(caddy).not.toMatch(/\/internal|\/admin|8081|8093/);
  expect(caddy).toContain("/v1/chat/completions");
  expect(caddy).toContain("flush_interval -1");
  const consoleCaddy = await Bun.file("infra/console.Caddyfile").text();
  expect(consoleCaddy).toMatch(/path \/api\/v1\/\* \/livez \/readyz/);
  expect(consoleCaddy).not.toMatch(/\/internal|\/admin|8093/);
  const consoleCompose = await Bun.file("infra/console.compose.yml").text();
  expect(consoleCompose).toMatch(/127\.0\.0\.1:.*:8092/);
  expect(consoleCompose).toMatch(/127\.0\.0\.1:.*:8093/);
});

test("G7.1 provider plugin architecture eliminates hardcoded vendor enums and channel.kind switches", async () => {
  const baselineSql = await Bun.file(
    "packages/ledger/src/migrations/0000_billing_accounts.sql",
  ).text();
  expect(baselineSql).not.toMatch(/CHECK\s*\(.*\bkind\b.*(?:IN|ANY).*'openai'/i);
  expect(baselineSql).toMatch(
    /FOREIGN KEY \(provider_plugin_id\) REFERENCES public\.plugins\(id\)/,
  );
  expect(baselineSql).toMatch(/adapter_config jsonb DEFAULT '{}'::jsonb NOT NULL/);
  expect(baselineSql).toMatch(/adapter_overrides jsonb DEFAULT '{}'::jsonb NOT NULL/);

  const root = "apps/gateway/src";
  const upstreamFiles = new Bun.Glob("{upstream,upstream-*,provider-request-preview}.ts");
  for await (const file of upstreamFiles.scan(root)) {
    const source = await Bun.file(`${root}/${file}`).text();
    expect(source, file).not.toMatch(/switch\s*\(\s*channel\.kind\s*\)/);
    expect(source, file).not.toMatch(
      /channel\.kind\s*===\s*["'](?:deepseek|ollama|openai|anthropic|gemini|azure|bedrock|vertex)["']/,
    );
  }

  const internalFiles = new Bun.Glob("internal-*.ts");
  for await (const file of internalFiles.scan(root)) {
    const source = await Bun.file(`${root}/${file}`).text();
    expect(source, file).not.toMatch(/channelKindEnum/);
    expect(source, file).not.toMatch(/kind\s*===\s*["'](?:azure|bedrock|vertex)["']/);
  }
});

test("inference stages are acyclic and ledger writes and identity reads have designated owners", async () => {
  const root = "apps/gateway/src";
  const stages = await Array.fromAsync(new Bun.Glob("inference/*.ts").scan(root));
  const files = ["proxy.ts", ...stages];
  const graph = new Map<string, string[]>();
  for (const file of files) {
    const source = await Bun.file(`${root}/${file}`).text();
    expect(source).not.toMatch(/from ["']\.\.\/(?:app|index|proxy)\.ts["']/);
    if (file !== "inference/accounting.ts") {
      expect(source, file).not.toMatch(
        /\bledger\s*\.\s*(?:acquireHold|switchSupplierAttempt|rememberProviderRequestId|commitTerminal|releaseHold|markUnknown)\s*\(/,
      );
    }
    if (file !== "inference/request-identity.ts") {
      expect(source, file).not.toMatch(/\bledger\s*\.\s*findIdempotentHold\s*\(/);
    }
    graph.set(
      file,
      [...source.matchAll(/from ["'](\.{1,2}\/[^"']+\.ts)["']/g)].map((match) =>
        posix.normalize(posix.join(posix.dirname(file), match[1]!)),
      ),
    );
  }

  const checked = new Set<string>();
  const visiting = new Set<string>();
  function visit(file: string) {
    if (checked.has(file)) return;
    expect(visiting.has(file), `Cyclic inference dependency involving ${file}`).toBe(false);
    visiting.add(file);
    for (const dependency of graph.get(file) ?? []) visit(dependency);
    visiting.delete(file);
    checked.add(file);
  }
  for (const file of graph.keys()) visit(file);
});
