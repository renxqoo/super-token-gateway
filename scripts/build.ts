import { chmod, mkdir, rm } from "node:fs/promises";
if (Bun.version !== "1.4.4") throw new Error("Build requires Bun 1.4.4");
await rm(new URL("../dist/", import.meta.url), { recursive: true, force: true });
for (const [entries, directory] of [
  [["apps/gateway/src/main.ts", "apps/gateway/src/worker-main.ts"], "dist/gateway"],
  [["apps/mock-providers/src/main.ts"], "dist/mock-providers"],
  [
    ["apps/core-api/src/identity-main.ts", "apps/core-api/src/platform/management.ts"],
    "dist/core-api",
  ],
  [
    [
      "scripts/migrate.ts",
      "scripts/archive.ts",
      "scripts/sign-bundled-release.ts",
      "scripts/install-bundled-release.ts",
      "scripts/identity-provision.ts",
      "scripts/identity-revoke.ts",
    ],
    "dist/tools",
  ],
  [["packages/plugin-host/src/index.ts"], "dist/plugin-host"],
  [["packages/adapters/src/index.ts"], "dist/adapters"],
] as const) {
  const result = await Bun.build({
    entrypoints: [...entries],
    outdir: directory,
    target: "bun",
    minify: false,
  });
  if (!result.success)
    throw new Error(`Build failed: ${result.logs.map((log) => log.message).join(", ")}`);
}
for (const pattern of ["migrations/*.sql", "migrations/manifest.json", "sql/*.sql"])
  for await (const file of new Bun.Glob(pattern).scan("packages/storage/src"))
    await Bun.write(`dist/tools/${file}`, Bun.file(`packages/storage/src/${file}`));
const hashes: Record<string, string> = {};
for (const pattern of [
  "apps/**/*.ts",
  "packages/**/*.ts",
  "packages/**/*.sql",
  "packages/storage/src/migrations/**/*.json",
  "packages/plugin-host/native/**/*.go",
  "packages/plugin-host/native/go.mod",
  "packages/plugin-host/native/go.sum",
  "packages/adapters/openai-chat/package/**/*",
  "scripts/plugin-tools.json",
  "scripts/plugin-builder/*",
  "scripts/setup-plugin-tools.py",
  "scripts/**/*.ts",
  "**/package.json",
  "bun.lock",
]) {
  for await (const file of new Bun.Glob(pattern).scan({ cwd: process.cwd(), onlyFiles: true })) {
    if (file.startsWith("node_modules/") || file.startsWith(".local/") || file.startsWith("dist/"))
      continue;
    hashes[file] = new Bun.CryptoHasher("sha256")
      .update(await Bun.file(file).arrayBuffer())
      .digest("hex");
  }
}
await mkdir("dist", { recursive: true });
await Bun.write("dist/native/plugin-host", Bun.file(".local/plugin-host"));
await chmod("dist/native/plugin-host", 0o755);
for await (const path of new Bun.Glob("**/*").scan({
  cwd: "packages/adapters/openai-chat/package",
  onlyFiles: true,
}))
  await Bun.write(
    `dist/openai-chat/package/${path}`,
    Bun.file(`packages/adapters/openai-chat/package/${path}`),
  );
await Bun.write(
  "dist/manifest.json",
  JSON.stringify(
    {
      runtime: Bun.version,
      files: Object.fromEntries(Object.entries(hashes).sort(([a], [b]) => a.localeCompare(b))),
    },
    null,
    2,
  ) + "\n",
);
console.log("Built gateway, worker, Core identity, migration, mock and isolated plugin artifacts");
