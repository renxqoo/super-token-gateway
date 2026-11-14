import type { KeyObject } from "node:crypto";
import { verifyPackage, type VerifiedPackage } from "@gateway/plugin-host";
import { packageTrust, releasePolicySchema } from "@gateway/plugin-sdk";

export type PackageTrust = ((digest: string) => ReadonlyMap<string, KeyObject>) & {
  admits(pkg: VerifiedPackage): boolean;
};

export async function loadPackageTrust(path: string, now = Date.now()): Promise<PackageTrust> {
  return packageTrust(releasePolicySchema.parse(await Bun.file(path).json()), now);
}

export function createPackageCache(
  download: (digest: string, deadlineAt: string) => Promise<Uint8Array>,
  trust: PackageTrust,
  capacity = 8,
) {
  const verified = new Map<string, VerifiedPackage>();
  const pending = new Map<string, Promise<VerifiedPackage>>();
  async function fetchVerified(digest: string, deadlineAt: string) {
    const keys = trust(digest);
    if (keys.size === 0) throw new Error("Plugin package is not trusted by this deployment");
    const pkg = verifyPackage(await download(digest, deadlineAt), keys);
    if (pkg.digest !== digest) throw new Error("Plugin package digest mismatch");
    if (!trust.admits(pkg)) throw new Error("Plugin package is not trusted by this deployment");
    if (verified.size >= capacity) verified.delete(verified.keys().next().value!);
    verified.set(digest, pkg);
    return pkg;
  }
  return {
    get(digest: string, deadlineAt: string): Promise<VerifiedPackage> {
      const cached = verified.get(digest);
      if (cached) {
        verified.delete(digest);
        verified.set(digest, cached);
        return Promise.resolve(cached);
      }
      let task = pending.get(digest);
      if (!task) {
        task = fetchVerified(digest, deadlineAt).finally(() => pending.delete(digest));
        pending.set(digest, task);
      }
      return task;
    },
    size: () => verified.size,
  };
}
