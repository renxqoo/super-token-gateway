import { mkdir } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { join } from "node:path";

const crateManifest = "native/builtin-pii-guard/Cargo.toml";
const build = spawnSync(
  "cargo",
  ["build", "--release", "--target", "wasm32-unknown-unknown", "--manifest-path", crateManifest],
  { stdio: "inherit" },
);
if (build.status !== 0) {
  throw new Error(`cargo build failed with status ${build.status ?? -1}`);
}

const wasmPath =
  "native/builtin-pii-guard/target/wasm32-unknown-unknown/release/stg_builtin_pii_guard.wasm";
const wasmBytes = new Uint8Array(await Bun.file(wasmPath).arrayBuffer());
const mod = new WebAssembly.Module(wasmBytes as BufferSource);
const imports = WebAssembly.Module.imports(mod);
if (imports.length > 0) {
  throw new Error(`Built-in WASM plugin must have zero imports, got: ${JSON.stringify(imports)}`);
}
const exportsList = WebAssembly.Module.exports(mod).map((e) => `${e.name}:${e.kind}`);
for (const req of [
  "memory:memory",
  "stg_alloc:function",
  "on_request:function",
  "on_response:function",
  "on_stream_chunk:function",
  "on_stream_end:function",
]) {
  if (!exportsList.includes(req)) {
    throw new Error(`Built-in WASM plugin missing export ${req}`);
  }
}

const digest = new Bun.CryptoHasher("sha256").update(wasmBytes).digest("hex");
const b64 = Buffer.from(wasmBytes).toString("base64");
const outDir = "packages/plugin-engine/src/builtins";
await mkdir(outDir, { recursive: true });
await Bun.write(join(outDir, "pii-guard.wasm"), wasmBytes);
const tsSource = `export const PII_GUARD_WASM_BASE64 =\n  "${b64}";\n\nexport const PII_GUARD_WASM_DIGEST = "${digest}";\nexport const PII_GUARD_WASM_BYTES_LENGTH = ${wasmBytes.byteLength};\n`;
await Bun.write(join(outDir, "pii-guard-data.ts"), tsSource);
console.log(
  JSON.stringify({
    built: true,
    plugin: "pii-guard",
    bytes: wasmBytes.byteLength,
    digest,
    exports: exportsList,
  }),
);
