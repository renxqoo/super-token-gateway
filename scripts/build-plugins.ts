import { mkdir, chmod, copyFile, mkdtemp, rm } from "node:fs/promises";
import { gunzipSync } from "node:zlib";
import { build, version } from "esbuild";
import { fileURLToPath } from "node:url";
import lock from "./plugin-tools.json";

const root = fileURLToPath(new URL("../", import.meta.url));
const cache = `${root}.local/plugin-tools/`;
const sha = (bytes: Uint8Array) => new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
if (`${process.platform}-${process.arch}` !== lock.platform || version !== lock.esbuild)
  throw new Error("PLUGIN_TOOLCHAIN_UNSUPPORTED");
await mkdir(`${cache}vendor`, { recursive: true });
if (process.argv.includes("--setup-tools")) {
  const child = Bun.spawn(
    [
      "/usr/bin/python3",
      `${root}scripts/setup-plugin-tools.py`,
      ...process.argv.slice(2).filter((arg) => arg !== "--setup-tools"),
    ],
    { env: { PATH: "/usr/bin:/bin" }, stdout: "inherit", stderr: "inherit" },
  );
  if ((await child.exited) !== 0) throw new Error("PLUGIN_SETUP_FAILED");
  process.exit(0);
}
const compilerPath = `${cache}extism-js-deterministic`;
const receipt = (await Bun.file(`${cache}builder-receipt.json`).json()) as {
  format: number;
  origin: string;
  binarySha256: string;
  builder: typeof lock.builder;
};
if (
  receipt.format !== 1 ||
  JSON.stringify(receipt.builder) !== JSON.stringify(lock.builder) ||
  !["verified-reference-binary", "verified-source-build"].includes(receipt.origin)
)
  throw new Error("PLUGIN_BUILDER_RECEIPT");
if (
  receipt.origin === "verified-reference-binary" &&
  receipt.binarySha256 !== lock.builder.referenceBinarySha256
)
  throw new Error("PLUGIN_BUILDER_HASH");
if (
  Bun.file(compilerPath).size > 64 << 20 ||
  sha(await Bun.file(compilerPath).bytes()) !== receipt.binarySha256 ||
  sha(await Bun.file(`${cache}engine.wasm`).bytes()) !== lock.builder.engine.sha256
)
  throw new Error("PLUGIN_BUILDER_HASH");
for (const [path, expected] of [
  ["scripts/plugin-builder/deterministic-preinit.patch", lock.builder.patchSha256],
  ["scripts/plugin-builder/upstream-Cargo.lock", lock.builder.upstreamLockSha256],
  ["scripts/plugin-builder/Cargo.lock", lock.builder.lockSha256],
] as const)
  if (sha(await Bun.file(`${root}${path}`).bytes()) !== expected)
    throw new Error("PLUGIN_BUILDER_SOURCE_HASH");
async function archive(name: string, pin: { url: string; sha256: string }) {
  const file = Bun.file(`${cache}vendor/${name}`);
  if (!(await file.exists())) {
    if (!process.argv.includes("--fetch-tools"))
      throw new Error("PLUGIN_TOOL_MISSING: explicitly run build:plugins --fetch-tools");
    const response = await fetch(pin.url, { signal: AbortSignal.timeout(30000) });
    if (!response.ok || Number(response.headers.get("content-length") ?? 0) > 32 << 20)
      throw new Error("PLUGIN_TOOL_DOWNLOAD");
    const reader = response.body?.getReader();
    if (!reader) throw new Error("PLUGIN_TOOL_DOWNLOAD");
    const chunks: Uint8Array[] = [];
    let total = 0;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        total += value.length;
        if (total > 32 << 20) throw new Error("PLUGIN_TOOL_BUDGET");
        chunks.push(value);
      }
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
    const bytes = new Uint8Array(Buffer.concat(chunks, total));
    if (bytes.length > 32 << 20 || sha(bytes) !== pin.sha256) throw new Error("PLUGIN_TOOL_HASH");
    await Bun.write(file, bytes);
  }
  const bytes = new Uint8Array(await file.arrayBuffer());
  if (sha(bytes) !== pin.sha256) throw new Error("PLUGIN_TOOL_HASH");
  return bytes;
}
const compiler = gunzipSync(await archive("extism-js.gz", lock["extism-js"]));
if (sha(compiler) !== lock.builder.officialBinarySha256)
  throw new Error("PLUGIN_OFFICIAL_BINARY_HASH");
await Bun.write(`${cache}extism-js-official`, compiler);
await chmod(`${cache}extism-js-official`, 0o700);
await archive("binaryen.tar.gz", lock.binaryen);
const unpack = Bun.spawn(["/usr/bin/tar", "-xzf", `${cache}vendor/binaryen.tar.gz`, "-C", cache], {
  env: { PATH: "/usr/bin:/bin" },
  stdout: "pipe",
  stderr: "pipe",
});
if ((await unpack.exited) !== 0) throw new Error("PLUGIN_TOOL_UNPACK");
const reported = Bun.spawn([compilerPath, "--version"], {
  env: { PATH: "/usr/bin:/bin" },
  stdout: "pipe",
  stderr: "pipe",
});
if (
  !(await new Response(reported.stdout).text()).includes(lock["extism-js"].reported) ||
  (await reported.exited) !== 0
)
  throw new Error("PLUGIN_TOOL_VERSION");
const output = `${root}packages/adapters/openai-chat/package/`;
await build({
  absWorkingDir: root,
  entryPoints: ["packages/adapters/openai-chat/plugin.ts"],
  bundle: true,
  format: "cjs",
  target: "es2020",
  minify: true,
  outfile: `${cache}openai-chat.js`,
});
await copyFile(`${root}packages/adapters/openai-chat/plugin.d.ts`, `${cache}openai-chat.d.ts`);
async function compileOnce(name: string, cwd: string, output: string) {
  const child = Bun.spawn([compilerPath, `${name}.js`, "-i", `${name}.d.ts`, "-o", output], {
    cwd,
    timeout: 30000,
    env: { PATH: `${cache}binaryen-version_133/bin:/usr/bin:/bin` },
    stdout: "pipe",
    stderr: "pipe",
  });
  const diagnostic = await new Response(child.stderr).text();
  if ((await child.exited) !== 0) throw new Error(`PLUGIN_COMPILE: ${diagnostic.slice(0, 1000)}`);
  return sha(await Bun.file(`${cwd}/${output}`).bytes());
}
const matrix: Record<string, string[]> = {};
async function reproducible(name: string) {
  const first = await compileOnce(name, cache, `${name}.wasm`),
    second = await compileOnce(name, cache, `${name}.second.wasm`);
  const relocated = await mkdtemp(`${cache}repro-`);
  try {
    for (const suffix of ["js", "d.ts"])
      await copyFile(`${cache}${name}.${suffix}`, `${relocated}/${name}.${suffix}`);
    const third = await compileOnce(name, relocated, `${name}.wasm`);
    matrix[name] = [first, second, third];
    if (first !== second || first !== third) throw new Error("PLUGIN_REPRODUCIBILITY");
  } finally {
    await rm(relocated, { recursive: true, force: true });
  }
}
await reproducible("openai-chat");
await build({
  absWorkingDir: root,
  entryPoints: ["tests/support/plugins/isolation.ts"],
  bundle: true,
  format: "cjs",
  target: "es2020",
  minify: true,
  outfile: `${cache}isolation.js`,
});
await copyFile(`${root}tests/support/plugins/isolation.d.ts`, `${cache}isolation.d.ts`);
await reproducible("isolation");
await copyFile(`${cache}openai-chat.wasm`, `${output}module.wasm`);
const sources: Record<string, { bytes: number; sha256: string }> = {};
for (const path of [
  "packages/adapters/openai-chat/plugin.ts",
  "packages/adapters/openai-chat/plugin.d.ts",
  "packages/plugin-sdk/src/guest.ts",
  "packages/adapters/openai-chat/package/module.wasm",
  "packages/adapters/openai-chat/package/manifest.json",
  "packages/adapters/openai-chat/package/config.schema.json",
  "packages/adapters/openai-chat/package/fixtures/cases.json",
  "scripts/plugin-tools.json",
  "scripts/build-plugins.ts",
  "scripts/setup-plugin-tools.py",
  "scripts/plugin-builder/deterministic-preinit.patch",
  "scripts/plugin-builder/upstream-Cargo.lock",
  "scripts/plugin-builder/Cargo.lock",
]) {
  const bytes = new Uint8Array(await Bun.file(`${root}${path}`).arrayBuffer());
  sources[path] = { bytes: bytes.length, sha256: sha(bytes) };
}
await Bun.write(
  `${output}provenance.json`,
  JSON.stringify(
    {
      source: "packages/adapters/openai-chat/plugin.ts",
      tools: {
        bun: Bun.version,
        esbuild: lock.esbuild,
        "lossless-json": lock["lossless-json"],
        "extism-js-release": lock["extism-js"].release,
        "extism-js-reported": lock["extism-js"].reported,
        "extism-js-url": lock["extism-js"].url,
        "extism-js-sha256": lock["extism-js"].sha256,
        "deterministic-builder-version": lock.builder.version,
        "deterministic-builder-binary-sha256": receipt.binarySha256,
        "deterministic-builder-source-url": lock.builder.source.url,
        "deterministic-builder-source-sha256": lock.builder.source.sha256,
        "deterministic-builder-commit": lock.builder.source.commit,
        "deterministic-builder-patch-sha256": lock.builder.patchSha256,
        "deterministic-builder-lock-sha256": lock.builder.lockSha256,
        "deterministic-builder-upstream-lock-sha256": lock.builder.upstreamLockSha256,
        "engine-bootstrap-sha256": lock.builder.engine.sha256,
        "engine-source-built": "false; exact verified official CLI slice",
        "compiler-binary-reproducibility": "not verified",
        "platforms-verified": lock.builder.platformsVerified.join(","),
        "preinit-random-clock": "0x5a, epoch 0, resolution 1ns; builder only",
        "rust-compiler": lock.builder.rustVersion,
        "rust-profile": "release opt-level=0/debug=0/jobs=2",
        "binaryen-release": lock.binaryen.release,
        "binaryen-url": lock.binaryen.url,
        "binaryen-sha256": lock.binaryen.sha256,
        "pdk-native-base64-source":
          "https://github.com/extism/js-pdk/blob/v1.7.0/crates/core/src/prelude/src/atob-btoa.ts",
        "quickjs-license-source":
          "rquickjs-sys 0.12.0 crate, SHA-256 698077537c286a169de8693b216672bcef148bf2e2e112ebf50758c68e9afa09",
      },
      licenses: [
        "MIT: adapter, lossless-json, QuickJS",
        "BSD-3-Clause: Extism JS-PDK",
        "Apache-2.0: Binaryen build tool",
        "Apache-2.0 WITH LLVM-exception: Wizer/Wasmtime build tools",
      ],
    },
    null,
    2,
  ) + "\n",
);
await Bun.write(`${output}source-hashes.json`, JSON.stringify(sources, null, 2) + "\n");
await copyFile(`${cache}isolation.wasm`, `${root}tests/support/plugins/isolation.wasm`);
await Bun.write(
  `${root}tests/support/plugins/provenance.json`,
  JSON.stringify(
    {
      purpose: "Synthetic SDK isolation fixture; no production trust anchor",
      source: "isolation.ts",
      sdk: {
        path: "packages/plugin-sdk/src/guest.ts",
        ...sources["packages/plugin-sdk/src/guest.ts"]!,
      },
      tools: lock,
      files: Object.fromEntries(
        await Promise.all(
          ["isolation.ts", "isolation.d.ts", "isolation.wasm"].map(async (path) => {
            const bytes = new Uint8Array(
              await Bun.file(`${root}tests/support/plugins/${path}`).arrayBuffer(),
            );
            return [path, { bytes: bytes.length, sha256: sha(bytes) }];
          }),
        ),
      ),
    },
    null,
    2,
  ) + "\n",
);
await Bun.write(
  `${cache}reproducibility.json`,
  JSON.stringify({ builder: receipt.binarySha256, matrix }, null, 2) + "\n",
);
const format = Bun.spawn(
  [process.execPath, "fmt", "--no-script", `${root}tests/support/plugins/provenance.json`],
  { cwd: root, env: { PATH: "/usr/bin:/bin" }, stdout: "pipe", stderr: "pipe" },
);
if ((await format.exited) !== 0) throw new Error("PLUGIN_PROVENANCE_FORMAT");
console.log(
  `Built OpenAI Chat WASM ${sources["packages/adapters/openai-chat/package/module.wasm"]!.sha256}`,
);
