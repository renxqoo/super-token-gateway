import { mkdir, rm } from "node:fs/promises";
if (Bun.version !== "1.4.4") throw new Error("Build requires Bun 1.4.4");
await rm(new URL("../dist/", import.meta.url), { recursive: true, force: true });
for (const [entries, directory] of [
  [["apps/gateway/src/main.ts", "apps/gateway/src/worker-main.ts"], "dist/gateway"],
  [["apps/mock-providers/src/main.ts"], "dist/mock-providers"],
  [["scripts/migrate.ts"], "dist/tools"],
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
for await (const file of new Bun.Glob("*.sql").scan("packages/storage/src"))
  await Bun.write(`dist/tools/${file}`, Bun.file(`packages/storage/src/${file}`));
const hashes: Record<string, string> = {};
for (const pattern of [
  "apps/**/*.ts",
  "packages/**/*.ts",
  "packages/**/*.sql",
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
console.log("Built standalone gateway, worker, migration and independent mock artifacts");
