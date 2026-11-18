import {
  closeSync,
  constants,
  fstatSync,
  lstatSync,
  openSync,
  readSync,
  realpathSync,
} from "node:fs";
import { chmod, link, mkdir, open, unlink } from "node:fs/promises";
import { basename, dirname, join } from "node:path";

function bytes(path: string) {
  const descriptor = openSync(path, constants.O_RDONLY | constants.O_NONBLOCK);
  try {
    const state = fstatSync(descriptor);
    if (!state.isFile() || state.size < 1 || state.size > 64 << 20)
      throw new Error("PLUGIN_EXECUTABLE_INVALID");
    const result = new Uint8Array(state.size);
    let at = 0;
    while (at < result.length) {
      const count = readSync(descriptor, result, at, result.length - at, at);
      if (count === 0) throw new Error("PLUGIN_EXECUTABLE_CHANGED");
      at += count;
    }
    if (readSync(descriptor, new Uint8Array(1), 0, 1, at) !== 0)
      throw new Error("PLUGIN_EXECUTABLE_CHANGED");
    return result;
  } finally {
    closeSync(descriptor);
  }
}

export function pluginExecutableDigest(path: string): string {
  return new Bun.CryptoHasher("sha256").update(bytes(path)).digest("hex");
}

function published(path: string, digest: string) {
  const actual = join(realpathSync(dirname(path)), basename(path));
  const state = lstatSync(actual);
  if (
    basename(actual) !== "plugin-host" ||
    basename(dirname(actual)) !== digest ||
    !state.isFile() ||
    (state.mode & 0o222) !== 0 ||
    pluginExecutableDigest(actual) !== digest
  )
    throw new Error("PLUGIN_EXECUTABLE_CHANGED");
  return { executable: actual, digest };
}

function declaredExecutable(path: string) {
  const digest = basename(dirname(path));
  if (basename(path) === "plugin-host" && /^[a-f0-9]{64}$/.test(digest))
    return published(path, digest);
}

export function resolvePluginExecutable(source: string) {
  const declared = declaredExecutable(source);
  if (declared) return declared;
  const actual = realpathSync(source);
  const canonical = declaredExecutable(actual);
  if (canonical) return canonical;
  const digest = pluginExecutableDigest(actual);
  const path = join(dirname(actual), "executables", digest, "plugin-host");
  return published(path, digest);
}

export async function publishPluginExecutable(source: string) {
  const declared = declaredExecutable(source);
  if (declared) return declared;
  const actual = realpathSync(source);
  const canonical = declaredExecutable(actual);
  if (canonical) return canonical;
  const data = bytes(actual);
  const digest = new Bun.CryptoHasher("sha256").update(data).digest("hex");
  const directory = join(dirname(actual), "executables", digest);
  await mkdir(directory, { recursive: true, mode: 0o755 });
  const target = join(directory, "plugin-host");
  try {
    return published(target, digest);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const temporary = join(directory, `staging-${process.pid}-${crypto.randomUUID()}`);
  const file = await open(temporary, "wx", 0o500);
  try {
    try {
      await file.writeFile(data);
      await file.sync();
    } finally {
      await file.close();
    }
    await chmod(temporary, 0o555);
    try {
      await link(temporary, target);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    return published(target, digest);
  } finally {
    await unlink(temporary);
  }
}
