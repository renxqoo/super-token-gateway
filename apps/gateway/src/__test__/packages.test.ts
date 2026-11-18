import { expect, test } from "bun:test";
import { generateKeyPairSync, type KeyObject } from "node:crypto";
import { openAIChatPackageFiles } from "@gateway/adapters";
import { syntheticSignedPackage } from "../../../../tests/support/plugin-package.ts";
import { createPackageCache } from "../packages.ts";

const key = generateKeyPairSync("ed25519");
const files = await openAIChatPackageFiles();
const first = syntheticSignedPackage(files, key);
const second = syntheticSignedPackage(
  files.map((file) =>
    file.path === "provenance.json"
      ? {
          path: file.path,
          bytes: new TextEncoder().encode(
            JSON.stringify({
              ...JSON.parse(new TextDecoder().decode(file.bytes)),
              tools: { variant: "second" },
            }),
          ),
        }
      : file,
  ),
  key,
);
const keys = new Map([["synthetic-test-only", key.publicKey]]);
const trustAll = Object.assign((): ReadonlyMap<string, KeyObject> => keys, {
  admits: () => true,
  isPlatform: () => false,
});
const deadline = new Date(Date.now() + 60000).toISOString();

function downloads(served: Record<string, Uint8Array>) {
  const calls: string[] = [];
  return {
    calls,
    download: async (digest: string) => {
      calls.push(digest);
      const bytes = served[digest];
      if (!bytes) throw new Error("unavailable");
      return bytes;
    },
  };
}

test("concurrent requests for one digest share a single verified download and later hits stay local", async () => {
  const source = downloads({ [first.pkg.digest]: first.bytes });
  const cache = createPackageCache(source.download, trustAll);
  const results = await Promise.all([1, 2, 3].map(() => cache.get(first.pkg.digest, deadline)));
  expect(results.map((p) => p.digest)).toEqual([
    first.pkg.digest,
    first.pkg.digest,
    first.pkg.digest,
  ]);
  await cache.get(first.pkg.digest, deadline);
  expect(source.calls).toEqual([first.pkg.digest]);
});

test("a digest outside the deployment trust is refused without downloading", async () => {
  const source = downloads({ [first.pkg.digest]: first.bytes });
  const cache = createPackageCache(
    source.download,
    Object.assign(() => new Map<string, KeyObject>(), {
      admits: () => true,
      isPlatform: () => false,
    }),
  );
  await expect(cache.get(first.pkg.digest, deadline)).rejects.toThrow("not trusted");
  expect(source.calls).toEqual([]);
});

test("Core serving a different validly signed package for the requested digest is refused and not cached", async () => {
  const source = downloads({ [first.pkg.digest]: second.bytes });
  const cache = createPackageCache(source.download, trustAll);
  await expect(cache.get(first.pkg.digest, deadline)).rejects.toThrow("digest mismatch");
  expect(cache.size()).toBe(0);
});

test("a validly signed package the deployment does not admit for its signer is refused and not cached", async () => {
  const source = downloads({ [first.pkg.digest]: first.bytes });
  const cache = createPackageCache(
    source.download,
    Object.assign((): ReadonlyMap<string, KeyObject> => keys, {
      admits: () => false,
      isPlatform: () => false,
    }),
  );
  await expect(cache.get(first.pkg.digest, deadline)).rejects.toThrow("not trusted");
  expect(cache.size()).toBe(0);
});

test("a failed download is retried on the next request instead of being remembered", async () => {
  const served: Record<string, Uint8Array> = {};
  const source = downloads(served);
  const cache = createPackageCache(source.download, trustAll);
  await expect(cache.get(first.pkg.digest, deadline)).rejects.toThrow("unavailable");
  served[first.pkg.digest] = first.bytes;
  expect((await cache.get(first.pkg.digest, deadline)).digest).toBe(first.pkg.digest);
  expect(source.calls).toEqual([first.pkg.digest, first.pkg.digest]);
});

test("the cache holds at most its capacity and evicts the least recently used package", async () => {
  const source = downloads({ [first.pkg.digest]: first.bytes, [second.pkg.digest]: second.bytes });
  const cache = createPackageCache(source.download, trustAll, 1);
  await cache.get(first.pkg.digest, deadline);
  await cache.get(second.pkg.digest, deadline);
  expect(cache.size()).toBe(1);
  await cache.get(first.pkg.digest, deadline);
  expect(source.calls).toEqual([first.pkg.digest, second.pkg.digest, first.pkg.digest]);
});
