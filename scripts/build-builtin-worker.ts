import { chmod, mkdir, mkdtemp, rename, rm } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { build, version } from "esbuild";
import { bundledPackageFiles } from "@gateway/adapters";
import {
  bundledReleaseIds,
  packageDigest,
  builtinWorkerArtifactDigest,
  builtinWorkerProofSchema,
  parseBuiltinWorkerArtifact,
} from "@gateway/plugin-sdk";
import { createBuiltinBuildSnapshot } from "./builtin-build-snapshot.ts";
import lock from "./plugin-tools.json";

const root = fileURLToPath(new URL("../", import.meta.url));
const hash = (bytes: Uint8Array) => new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
const adapters = [
  "openai-chat",
  "anthropic-messages",
  "gemini-generate",
  "bedrock-anthropic",
  "minimax-music",
];
const proofPath = resolve(root, "packages/adapters/builtin/proof.json");

function entrySource(records: readonly { module_hash: string }[]) {
  return (
    adapters
      .map(
        (name, n) => `import { invoke as p${n} } from "../../packages/adapters/${name}/plugin.ts";`,
      )
      .join("\n") +
    `\nimport { startBuiltinWorker } from "../../packages/adapters/src/builtin-worker-entry.ts";\nstartBuiltinWorker({${records.map((record, n) => `${JSON.stringify(record.module_hash)}:p${n}`).join(",")}});\n`
  );
}

export function builtinProofInputPath(path: string): string {
  if (!path.startsWith("$dependency/")) return resolve(root, path);
  const components = path.slice("$dependency/".length).split("/");
  const nameLength = components[0]?.startsWith("@") ? 2 : 1;
  const name = components.slice(0, nameLength).join("/");
  const entry = fileURLToPath(
    import.meta.resolve(name, resolve(root, "packages/plugin-sdk/src/guest.ts")),
  );
  const marker = `/node_modules/${name}/`;
  const at = entry.lastIndexOf(marker);
  if (at < 0) throw new Error("BUILTIN_DEPENDENCY_LAYOUT");
  return resolve(entry.slice(0, at + marker.length), ...components.slice(nameLength));
}

export async function buildBuiltinWorker(directory: string) {
  if (Bun.version !== "1.4.4") throw new Error("BUILTIN_RUNTIME_UNSUPPORTED");
  if (Bun.file(proofPath).size > 24 << 20) throw new Error("BUILTIN_PROOF_INVALID");
  const proof = builtinWorkerProofSchema.parse(await Bun.file(proofPath).json());
  const { worker_base64: encoded, ...metadata } = proof;
  const code = new Uint8Array(Buffer.from(encoded, "base64"));
  if (Buffer.from(code).toString("base64") !== encoded || hash(code) !== proof.worker_digest)
    throw new Error("BUILTIN_PROOF_INVALID");
  const checked = new Map<string, string>();
  for (const sources of [proof.sources, ...proof.records.map((record) => record.sources)])
    for (const [path, expected] of Object.entries(sources)) {
      if (path === "$generated-entry") {
        if (hash(new TextEncoder().encode(entrySource(proof.records))) !== expected)
          throw new Error("BUILTIN_PROOF_STALE");
        continue;
      }
      let actual = checked.get(path);
      if (!actual) {
        actual = hash(await Bun.file(builtinProofInputPath(path)).bytes());
        checked.set(path, actual);
      }
      if (actual !== expected) throw new Error(`BUILTIN_PROOF_STALE:${path}`);
    }
  if (proof.records.length !== adapters.length) throw new Error("BUILTIN_PROOF_INVALID");
  for (const [n, name] of adapters.entries()) {
    const id = bundledReleaseIds.find((release) => release.startsWith(`gateway.${name}-`))!;
    const record = proof.records[n]!;
    const files = await bundledPackageFiles(id);
    if (
      record.release_id !== id ||
      record.package_digest !== packageDigest(files) ||
      record.module_hash !== hash(files.find((file) => file.path === "module.wasm")!.bytes) ||
      record.manifest_digest !== hash(files.find((file) => file.path === "manifest.json")!.bytes) ||
      record.config_schema_digest !==
        hash(files.find((file) => file.path === "config.schema.json")!.bytes)
    )
      throw new Error(`BUILTIN_PROOF_STALE:${name}`);
  }
  const runtimeDigest = hash(await Bun.file(process.execPath).bytes());
  const registry = parseBuiltinWorkerArtifact(
    {
      ...metadata,
      runtime_digest: runtimeDigest,
      artifact_digest: builtinWorkerArtifactDigest(proof.worker_digest, runtimeDigest),
    },
    code,
    runtimeDigest,
  );
  await mkdir(directory, { recursive: true });
  await Bun.write(resolve(directory, "worker.js"), code);
  await Bun.write(resolve(directory, "registry.json"), JSON.stringify(registry, null, 2) + "\n");
  return registry;
}

export async function proveBuiltinWorker() {
  if (
    Bun.version !== "1.4.4" ||
    version !== lock.esbuild ||
    `${process.platform}-${process.arch}` !== lock.platform
  )
    throw new Error("BUILTIN_TOOLCHAIN_UNSUPPORTED");
  const snapshot = createBuiltinBuildSnapshot(root);
  for (const path of [
    "bun.lock",
    "tsconfig.json",
    "package.json",
    "packages/plugin-sdk/package.json",
    "scripts/plugin-tools.json",
    "scripts/build-builtin-worker.ts",
    "scripts/builtin-build-snapshot.ts",
  ])
    await snapshot.capture(path);
  const tsconfig = new TextDecoder().decode(await snapshot.capture("tsconfig.json"));
  if (
    JSON.stringify(
      JSON.parse(new TextDecoder().decode(await snapshot.capture("scripts/plugin-tools.json"))),
    ) !== JSON.stringify(lock)
  )
    throw new Error("BUILTIN_BUILD_INPUT_CHANGED");
  const packages = [];
  for (const name of adapters) {
    const id = bundledReleaseIds.find((release) => release.startsWith(`gateway.${name}-`))!;
    const files = await bundledPackageFiles(id);
    for (const file of files)
      snapshot.seed(`packages/adapters/${name}/package/${file.path}`, file.bytes);
    packages.push({ id, files });
  }
  const cache = resolve(root, ".local/plugin-tools");
  const compiler = resolve(cache, "extism-js-deterministic");
  if (Bun.file(compiler).size > 64 << 20) throw new Error("BUILTIN_BUILDER_INVALID");
  const compilerBytes = new Uint8Array(await Bun.file(compiler).bytes());
  const receipt = await Bun.file(resolve(cache, "builder-receipt.json")).json();
  if (
    receipt.format !== 1 ||
    JSON.stringify(receipt.builder) !== JSON.stringify(lock.builder) ||
    !["verified-reference-binary", "verified-source-build"].includes(receipt.origin) ||
    (receipt.origin === "verified-reference-binary" &&
      receipt.binarySha256 !== lock.builder.referenceBinarySha256) ||
    hash(compilerBytes) !== receipt.binarySha256 ||
    hash(await Bun.file(resolve(cache, "engine.wasm")).bytes()) !== lock.builder.engine.sha256
  )
    throw new Error("BUILTIN_BUILDER_INVALID");
  for (const [path, expected] of [
    ["scripts/plugin-builder/deterministic-preinit.patch", lock.builder.patchSha256],
    ["scripts/plugin-builder/upstream-Cargo.lock", lock.builder.upstreamLockSha256],
    ["scripts/plugin-builder/Cargo.lock", lock.builder.lockSha256],
  ])
    if (hash(await Bun.file(resolve(root, path!)).bytes()) !== expected)
      throw new Error("BUILTIN_BUILDER_INVALID");
  const binaryen = resolve(cache, "vendor/binaryen.tar.gz");
  if (Bun.file(binaryen).size > 128 << 20) throw new Error("BUILTIN_BUILDER_INVALID");
  const binaryenBytes = new Uint8Array(await Bun.file(binaryen).bytes());
  if (hash(binaryenBytes) !== lock.binaryen.sha256) throw new Error("BUILTIN_BUILDER_INVALID");
  const temporary = await mkdtemp(resolve(root, ".local/builtin-build-"));
  let child: Bun.Subprocess | undefined;
  let aborted = false;
  function stop() {
    aborted = true;
    if (child) {
      try {
        process.kill(-child.pid, "SIGKILL");
      } catch {}
    }
  }
  function check() {
    if (aborted) throw new Error("BUILTIN_BUILD_CANCELLED");
  }
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  async function run(command: string[], cwd: string, path: string) {
    check();
    const process_ = Bun.spawn(command, {
      cwd,
      env: { PATH: path },
      detached: true,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    });
    child = process_;
    const timer = setTimeout(() => {
      try {
        process.kill(-process_.pid, "SIGKILL");
      } catch {}
    }, 30000);
    try {
      const [code, stdout, stderr] = await Promise.all([
        process_.exited,
        new Response(process_.stdout).text(),
        new Response(process_.stderr).text(),
      ]);
      if (code !== 0)
        throw new Error(`BUILTIN_BUILD_FAILED:${code}:${(stdout + stderr).slice(0, 1000)}`);
      check();
    } finally {
      clearTimeout(timer);
      try {
        process.kill(-process_.pid, "SIGKILL");
      } catch {}
      child = undefined;
    }
  }
  try {
    const ownedCompiler = resolve(temporary, "compiler");
    const ownedArchive = resolve(temporary, "binaryen.tar.gz");
    await Bun.write(ownedCompiler, compilerBytes);
    await chmod(ownedCompiler, 0o700);
    await Bun.write(ownedArchive, binaryenBytes);
    await run(["/usr/bin/tar", "-xzf", ownedArchive, "-C", temporary], temporary, "/usr/bin:/bin");
    const records = [];
    const permittedInputs = new Set([
      resolve(root, "packages/adapters/src/builtin-host.ts"),
      resolve(root, "packages/adapters/src/builtin-worker-entry.ts"),
    ]);
    for (const [n, name] of adapters.entries()) {
      const target = resolve(temporary, name);
      await mkdir(target);
      const bundled = await build({
        absWorkingDir: root,
        entryPoints: [`packages/adapters/${name}/plugin.ts`],
        bundle: true,
        format: "cjs",
        target: "es2020",
        tsconfigRaw: tsconfig,
        minify: true,
        metafile: true,
        outfile: resolve(target, `${name}.js`),
        plugins: [snapshot.plugin],
      });
      for (const path of Object.keys(bundled.metafile.inputs))
        permittedInputs.add(resolve(root, path));
      await Bun.write(
        resolve(target, `${name}.d.ts`),
        await snapshot.capture(`packages/adapters/${name}/plugin.d.ts`),
      );
      const { id, files } = packages[n]!;
      const moduleHash = hash(files.find((file) => file.path === "module.wasm")!.bytes);
      const runs: [string, string, string] = ["", "", ""];
      for (let attempt = 0; attempt < 3; attempt++) {
        const output = `${name}.${attempt}.wasm`;
        await run(
          [ownedCompiler, `${name}.js`, "-i", `${name}.d.ts`, "-o", output],
          target,
          `${temporary}/binaryen-version_133/bin:/usr/bin:/bin`,
        );
        const actual = hash(await Bun.file(resolve(target, output)).bytes());
        if (actual !== moduleHash) throw new Error(`BUILTIN_WASM_SOURCE_MISMATCH:${name}`);
        runs[attempt] = actual;
      }
      records.push({
        release_id: id,
        package_digest: packageDigest(files),
        module_hash: moduleHash,
        manifest_digest: hash(files.find((file) => file.path === "manifest.json")!.bytes),
        config_schema_digest: hash(files.find((file) => file.path === "config.schema.json")!.bytes),
        sources: await snapshot.hashes(bundled.metafile, undefined, [
          `packages/adapters/${name}/plugin.d.ts`,
        ]),
        verification_runs: runs,
      });
    }
    const entry = resolve(temporary, "worker-entry.ts");
    await Bun.write(entry, entrySource(records));
    const built = await build({
      absWorkingDir: root,
      entryPoints: [entry],
      bundle: true,
      format: "esm",
      platform: "node",
      target: "es2020",
      tsconfigRaw: tsconfig,
      minify: true,
      metafile: true,
      write: false,
      plugins: [snapshot.plugin],
      inject: [resolve(root, "packages/adapters/src/builtin-host.ts")],
    });
    for (const path of Object.keys(built.metafile.inputs))
      if (resolve(root, path) !== entry && !permittedInputs.has(resolve(root, path)))
        throw new Error("BUILTIN_GRAPH_MISMATCH");
    const code = new Uint8Array(built.outputFiles[0]!.contents);
    const proof = builtinWorkerProofSchema.parse({
      format: 1,
      abi_version: 1,
      state_version: 1,
      runtime: Bun.version,
      worker_digest: hash(code),
      worker_base64: Buffer.from(code).toString("base64"),
      sources: await snapshot.hashes(
        built.metafile,
        entry,
        adapters.map((name) => `packages/adapters/${name}/plugin.d.ts`),
      ),
      compiler_digest: receipt.binarySha256,
      binaryen_archive_digest: lock.binaryen.sha256,
      records,
    });
    check();
    const staging = resolve(temporary, "proof.json");
    await Bun.write(staging, JSON.stringify(proof, null, 2) + "\n");
    check();
    await mkdir(resolve(root, "packages/adapters/builtin"), { recursive: true });
    check();
    await rename(staging, proofPath);
    check();
    return proof;
  } finally {
    process.removeListener("SIGTERM", stop);
    process.removeListener("SIGINT", stop);
    await rm(temporary, { recursive: true, force: true });
  }
}

if (import.meta.main) {
  if (process.argv.slice(2).some((arg) => arg !== "--prove"))
    throw new Error("BUILTIN_BUILD_ARGUMENT");
  if (process.argv.includes("--prove")) await proveBuiltinWorker();
  const registry = await buildBuiltinWorker(resolve(root, ".local/builtin-adapters"));
  console.log(
    JSON.stringify({
      artifact_digest: registry.artifact_digest,
      packages: registry.records.length,
      source_proof: process.argv.includes("--prove") ? "rebuilt" : "verified",
      wasm_rebuilds: process.argv.includes("--prove") ? 15 : 0,
    }),
  );
}
