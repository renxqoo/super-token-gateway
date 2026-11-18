import { readFile } from "node:fs/promises";
import { realpathSync } from "node:fs";
import { resolve, relative, extname } from "node:path";
import { type Metafile, type Plugin, type ResolveResult } from "esbuild";

export function createBuiltinBuildSnapshot(root: string) {
  const directory = realpathSync(root);
  const files = new Map<string, Uint8Array>();
  const reading = new Map<string, Promise<Uint8Array>>();
  const resolutions = new Map<string, Promise<ResolveResult>>();
  async function file(path: string) {
    const absolute = resolve(directory, path);
    let bytes = files.get(absolute);
    if (!bytes) {
      let pending = reading.get(absolute);
      if (!pending) {
        pending = readFile(absolute).then((value) => {
          const owned = new Uint8Array(value);
          files.set(absolute, owned);
          return owned;
        });
        reading.set(absolute, pending);
      }
      bytes = await pending;
    }
    return bytes;
  }
  const plugin: Plugin = {
    name: "builtin-input-snapshot",
    setup(build) {
      build.onResolve({ filter: /.*/ }, async (args) => {
        if (args.pluginData === "builtin-snapshot-resolving") return;
        const key = JSON.stringify([
          args.importer,
          args.path,
          args.kind,
          args.namespace,
          args.resolveDir,
        ]);
        let result = resolutions.get(key);
        if (!result) {
          result = build.resolve(args.path, {
            kind: args.kind,
            importer: args.importer,
            namespace: args.namespace,
            resolveDir: args.resolveDir,
            pluginData: "builtin-snapshot-resolving",
          });
          resolutions.set(key, result);
        }
        const loaded = await result;
        if (loaded.errors.length) return { errors: loaded.errors, warnings: loaded.warnings };
        return {
          path: loaded.path,
          namespace: loaded.namespace,
          external: loaded.external,
          sideEffects: loaded.sideEffects,
        };
      });
      build.onLoad({ filter: /.*/, namespace: "file" }, async (args) => {
        const extension = extname(args.path);
        const loader =
          extension === ".ts" || extension === ".mts" || extension === ".cts"
            ? "ts"
            : extension === ".tsx"
              ? "tsx"
              : extension === ".jsx"
                ? "jsx"
                : extension === ".json"
                  ? "json"
                  : extension === ".js" || extension === ".mjs" || extension === ".cjs"
                    ? "js"
                    : undefined;
        if (!loader) throw new Error("BUILTIN_SOURCE_TYPE");
        return { contents: await file(args.path), loader };
      });
    },
  };
  return {
    plugin,
    seed(path: string, bytes: Uint8Array) {
      const absolute = resolve(directory, path);
      if (reading.has(absolute)) throw new Error("BUILTIN_INPUT_CHANGED");
      const prior = files.get(absolute);
      if (prior && !Buffer.from(prior).equals(bytes)) throw new Error("BUILTIN_INPUT_CHANGED");
      files.set(absolute, new Uint8Array(bytes));
    },
    async capture(path: string) {
      return new Uint8Array(await file(path));
    },
    async hashes(meta: Metafile, generated?: string, additional: readonly string[] = []) {
      const result: Record<string, string> = {};
      for (const input of [
        ...Object.keys(meta.inputs),
        ...additional,
        "bun.lock",
        "tsconfig.json",
        "package.json",
        "packages/plugin-sdk/package.json",
        "scripts/plugin-tools.json",
        "scripts/build-builtin-worker.ts",
        "scripts/builtin-build-snapshot.ts",
      ]) {
        const absolute = resolve(directory, input);
        let key = relative(directory, absolute);
        const dependency = absolute.lastIndexOf("/node_modules/");
        if (dependency >= 0)
          key = `$dependency/${absolute.slice(dependency + "/node_modules/".length)}`;
        if (absolute === generated) key = "$generated-entry";
        result[key] = new Bun.CryptoHasher("sha256").update(await file(absolute)).digest("hex");
      }
      return Object.fromEntries(Object.entries(result).sort(([a], [b]) => a.localeCompare(b)));
    },
  };
}
