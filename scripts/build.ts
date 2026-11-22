import { existsSync } from "node:fs";
import { cp, mkdir, rm } from "node:fs/promises";
import { join } from "node:path";
import { SCHEMA_VERSION } from "@gateway/ledger";

const distDir = "dist";
await rm(distDir, { recursive: true, force: true });
await mkdir(distDir, { recursive: true });

const targets = [
  { entry: "apps/gateway/src/main.ts", outdir: "dist/gateway", name: "gateway" },
  {
    entry: "packages/plugin-engine/src/wasmtime-worker.ts",
    outdir: "dist/gateway",
    name: "wasmtime-worker",
  },
  { entry: "apps/worker/src/main.ts", outdir: "dist/worker", name: "worker" },
  {
    entry: "apps/mock-providers/src/main.ts",
    outdir: "dist/mock-providers",
    name: "mock-providers",
  },
  { entry: "scripts/migrate.ts", outdir: "dist/tools", name: "migrate" },
  { entry: "scripts/archive.ts", outdir: "dist/tools", name: "archive" },
];

const outputs: { name: string; file: string; sha256: string; bytes: number }[] = [];

for (const target of targets) {
  const res = await Bun.build({
    entrypoints: [target.entry],
    outdir: target.outdir,
    target: "bun",
    format: "esm",
    minify: false,
    sourcemap: "none",
  });
  if (!res.success) {
    for (const log of res.logs) console.error(log);
    throw new Error(`Build failed for ${target.entry}`);
  }
  for (const artifact of res.outputs) {
    const bytes = new Uint8Array(await artifact.arrayBuffer());
    const sha256 = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
    outputs.push({
      name: target.name,
      file: artifact.path.replace(`${process.cwd()}/`, ""),
      sha256,
      bytes: bytes.byteLength,
    });
  }
}

await cp("packages/ledger/src/migrations", join(distDir, "tools/migrations"), { recursive: true });

let bundledNative = false;
for (const nativeName of ["libstg_plugin_wasmtime.dylib", "libstg_plugin_wasmtime.so"]) {
  const srcPath = join("native/plugin-wasmtime/target/release", nativeName);
  if (existsSync(srcPath)) {
    const nativeDir = join(distDir, "native");
    await mkdir(nativeDir, { recursive: true });
    const destPath = join(nativeDir, nativeName);
    await cp(srcPath, destPath);
    const bytes = new Uint8Array(await Bun.file(destPath).arrayBuffer());
    const sha256 = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
    outputs.push({
      name: "plugin-wasmtime",
      file: destPath,
      sha256,
      bytes: bytes.byteLength,
    });
    bundledNative = true;
  }
}
if (!bundledNative) {
  throw new Error("Native Wasmtime library missing; run bun run build:wasmtime first");
}

const manifest = {
  bun_version: Bun.version,
  schema_version: SCHEMA_VERSION,
  built_at: new Date().toISOString(),
  outputs,
};
await Bun.write(join(distDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
console.log(
  JSON.stringify({ built: true, artifacts: outputs.length, schema_version: SCHEMA_VERSION }),
);
