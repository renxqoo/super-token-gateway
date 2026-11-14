import { afterAll, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { loadCoreApiConfig } from "../config.ts";

const directory = await mkdtemp(resolve(tmpdir(), "core-api-config-"));
afterAll(() => rm(directory, { recursive: true, force: true }));

const listener = {
  database_url: "postgres://core@127.0.0.1/db",
  audience: "synthetic",
  hostname: "127.0.0.1",
  servername: "localhost",
  port: 0,
  key_file: "core.key",
  cert_file: "core.pem",
  ca_file: "ca.pem",
  client_key_file: "core.key",
  client_cert_file: "core.pem",
};
const admin = {
  database_url: "postgres://admin@127.0.0.1/db",
  hostname: "127.0.0.1",
  port: 0,
  admin_key: "synthetic-platform-admin-key-0123456789",
};
const execution = {
  pepper: "synthetic-platform-pepper-0123456789",
  encryptionKey: "44".repeat(32),
  redisUrls: ["redis://127.0.0.1:6379"],
  quotaNamespace: "core",
  quotaGeneration: "v1",
};

async function load(value: unknown) {
  const path = resolve(directory, `${crypto.randomUUID()}.json`);
  await writeFile(path, JSON.stringify(value));
  return loadCoreApiConfig(path);
}

test("the admin listener is refused without the execution secrets it authenticates with", async () => {
  await expect(load({ ...listener, admin })).rejects.toThrow(
    "Core API admin listener requires the execution configuration",
  );
  expect((await load({ ...listener, admin, execution })).admin?.port).toBe(0);
});

test("an unknown admin field and a short bootstrap admin key are refused", async () => {
  await expect(
    load({ ...listener, execution, admin: { ...admin, extra: true } }),
  ).rejects.toThrow();
  await expect(
    load({ ...listener, execution, admin: { ...admin, admin_key: "short" } }),
  ).rejects.toThrow();
});

test("a missing config file path is refused before reading anything", async () => {
  await expect(loadCoreApiConfig("")).rejects.toThrow("CORE_API_CONFIG_FILE required");
});

test("the public account listener requires the execution pepper and valid trusted proxy ranges", async () => {
  const publicListener = {
    database_url: "postgres://public@127.0.0.1/db",
    hostname: "127.0.0.1",
    port: 0,
  };
  await expect(load({ ...listener, public: publicListener })).rejects.toThrow(
    "Core API public listener requires the execution configuration",
  );
  await expect(
    load({
      ...listener,
      execution,
      public: { ...publicListener, trusted_proxy_cidrs: ["10.0.0.0/33"] },
    }),
  ).rejects.toThrow();
  expect(
    (await load({ ...listener, execution, public: publicListener })).public?.trusted_proxy_cidrs,
  ).toEqual([]);
});
