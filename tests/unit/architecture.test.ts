import { expect, test } from "bun:test";

test("workspace dependencies stay acyclic and commercial capabilities stay outside gateway", async () => {
  const allowed: Record<string, string[]> = {
    kernel: [],
    contracts: ["kernel"],
    "adapters/src/protocol": ["kernel", "contracts"],
    runtime: ["kernel", "contracts"],
    "core-client": ["kernel", "contracts", "runtime/crypto", "runtime/h2"],
    storage: ["kernel", "contracts", "runtime/batcher", "runtime/crypto"],
  };
  for (const [name, imports] of Object.entries(allowed)) {
    const root = name.includes("/") ? `packages/${name}` : `packages/${name}/src`;
    for await (const file of new Bun.Glob("**/*.ts").scan(root)) {
      const source = await Bun.file(`${root}/${file}`).text();
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
  for await (const file of new Bun.Glob("**/*.ts").scan("apps/legacy-gateway/src")) {
    const source = await Bun.file(`apps/legacy-gateway/src/${file}`).text();
    if (["app.ts", "admin.ts"].includes(file)) expect(source).not.toMatch(/@gateway\/storage/);
    expect(source).not.toMatch(/\.db\s*(?:`|<|\.)|\.unsafe\s*\(/);
    expect(source).not.toMatch(
      /(?:Bun\.write|writeFile|appendFile|console\.(?:debug|error|log)\([^\n]*(?:body|content|response))/,
    );
  }
});

test("Core processes never reach into the transitional Gateway or each other's private modules", async () => {
  const forbidden: Record<string, RegExp> = {
    "core-api": /legacy-gateway|core-worker|@gateway\/(?:legacy-gateway|adapters\/protocol)/,
    "core-worker": /legacy-gateway|core-api|@gateway\/(?:legacy-gateway|core-api|plugin-host)/,
    "mock-providers": /legacy-gateway|core-api|core-worker/,
    gateway:
      /legacy-gateway|core-api|core-worker|@gateway\/(?:storage|legacy-gateway|core-api|core-worker)|drizzle|^postgres/,
  };
  for (const [app, pattern] of Object.entries(forbidden))
    for await (const file of new Bun.Glob("**/*.ts").scan(`apps/${app}/src`)) {
      const source = await Bun.file(`apps/${app}/src/${file}`).text();
      for (const match of source.matchAll(/(?:from\s+|import\s*\(?\s*)["']([^"']+)["']/g))
        expect(match[1] ?? "").not.toMatch(pattern);
    }
});

test("the v2 Gateway has no database path and reads only its config file from the environment", async () => {
  const closure = new Set<string>();
  const pending = ["apps/gateway"];
  while (pending.length) {
    const directory = pending.pop()!;
    const manifest = (await Bun.file(`${directory}/package.json`).json()) as {
      dependencies?: Record<string, string>;
    };
    for (const name of Object.keys(manifest.dependencies ?? {})) {
      if (closure.has(name)) continue;
      closure.add(name);
      if (name.startsWith("@gateway/")) pending.push(`packages/${name.slice(9)}`);
    }
  }
  for (const name of [
    "@gateway/storage",
    "@gateway/core-api",
    "@gateway/legacy-gateway",
    "drizzle-orm",
    "postgres",
  ])
    expect(closure.has(name)).toBe(false);
  const reads = new Set<string>();
  for await (const file of new Bun.Glob("**/*.ts").scan("apps/gateway/src")) {
    if (file.includes("__test__")) continue;
    const source = await Bun.file(`apps/gateway/src/${file}`).text();
    expect(source).not.toMatch(/DATABASE_URL|KEY_PEPPER|Bun\.env|new SQL\b/);
    for (const match of source.matchAll(/process\.env(?:\.([A-Z_]+)|\[)/g))
      reads.add(match[1] ?? "[dynamic]");
  }
  expect([...reads]).toEqual(["GATEWAY_CONFIG_FILE"]);
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

test("deployment units start built entries, keep v2 processes under separate users and route Chat to the v2 Gateway", async () => {
  const build = await Bun.file("scripts/build.ts").text();
  const units: Record<string, Record<string, string[]>> = {};
  for await (const file of new Bun.Glob("*.service").scan("infra")) {
    const fields: Record<string, string[]> = {};
    for (const line of (await Bun.file(`infra/${file}`).text()).split("\n")) {
      const match = /^([A-Za-z]+)=(.*)$/.exec(line);
      if (match) (fields[match[1]!] ??= []).push(match[2]!);
    }
    units[file] = fields;
    const entry = /^\/usr\/local\/bin\/bun (dist\/([a-z-]+)\/([a-z-]+)\.js)$/.exec(
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
    "core-api.service",
    "core-worker.service",
    "gateway@.service",
    "legacy-gateway-worker.service",
    "legacy-gateway@.service",
  ]);
  const gateway = units["gateway@.service"]!;
  expect(gateway.ExecStart).toEqual(["/usr/local/bin/bun dist/gateway/main.js"]);
  expect(gateway.EnvironmentFile).toBeUndefined();
  expect(gateway.Environment).toEqual(["GATEWAY_CONFIG_FILE=/etc/llm-gateway/gateway-%i.json"]);
  const users = Object.values(units).map((unit) => unit.User![0]);
  expect(new Set(users).size).toBe(users.length);
  const example = (await Bun.file("infra/gateway.example.json").json()) as { port: number };
  const caddy = await Bun.file("infra/Caddyfile").text();
  const chat = caddy.indexOf("handle @chat"),
    legacy = caddy.indexOf("handle @legacy");
  expect(caddy).toContain(
    "@chat {\n\t\tmethod POST\n\t\tpath /v1/chat/completions /v1/messages /v1beta/models/*\n\t}\n",
  );
  expect(chat).toBeGreaterThan(-1);
  expect(legacy).toBeGreaterThan(chat);
  expect(caddy.slice(chat, legacy)).toContain(`127.0.0.1:${example.port}`);
  expect(caddy.slice(legacy)).not.toContain(`127.0.0.1:${example.port}`);
  expect(caddy).not.toMatch(/\/admin|\/_internal/);
  const account = caddy.indexOf("handle @account"),
    request = caddy.indexOf("handle @request");
  expect(caddy).toContain(
    "@account path /v1/wallets /v1/models /v1beta/models /v1/usage /v1/project/keys /v1/project/keys/*\n",
  );
  expect(caddy).toContain("@request path_regexp ^/v1/requests/[^/]+(/billing)?$\n");
  for (const handle of [account, request]) {
    expect(handle).toBeGreaterThan(-1);
    expect(handle).toBeLessThan(legacy);
  }
  expect(caddy.slice(legacy)).not.toContain("127.0.0.1:8095");
});
