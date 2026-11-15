import { expect, test } from "bun:test";
import { loadCoreWorkerConfig } from "../config.ts";

const valid = {
  DATABASE_URL: "postgres://worker:secret-password@127.0.0.1/db",
  QUEUE_REDIS_URL: "redis://127.0.0.1:6380",
};

test("a worker without durable endpoints is refused by field name without echoing secrets", () => {
  expect(() =>
    loadCoreWorkerConfig({ DATABASE_URL: "mysql://worker:secret-password@host/db" }),
  ).toThrow(/^Invalid configuration fields: DATABASE_URL, QUEUE_REDIS_URL$/);
});

test("a queue namespace that could escape its key prefix and unbounded settlement concurrency are refused", () => {
  expect(() => loadCoreWorkerConfig({ ...valid, REDIS_NAMESPACE: "stg}*" })).toThrow(
    "REDIS_NAMESPACE",
  );
  expect(() => loadCoreWorkerConfig({ ...valid, SETTLEMENT_CONCURRENCY: "65" })).toThrow(
    "SETTLEMENT_CONCURRENCY",
  );
  expect(
    loadCoreWorkerConfig({ ...valid, SETTLEMENT_CONCURRENCY: "64" }).SETTLEMENT_CONCURRENCY,
  ).toBe(64);
});

const core = {
  CORE_ORIGIN: "https://127.0.0.1:9443",
  CORE_SERVERNAME: "localhost",
  CORE_AUDIENCE: "synthetic/core-api",
  CORE_PEER_SHA256: "a".repeat(64),
  CORE_SUBJECT_ALTNAME: "URI:spiffe://synthetic/core-api/coreA",
  CORE_KEY_FILE: "worker.key",
  CORE_CERT_FILE: "worker.pem",
  CORE_CA_FILE: "ca.pem",
};

test("a partial Core recovery client is refused by the missing field names", () => {
  expect(() =>
    loadCoreWorkerConfig({ ...valid, CORE_ORIGIN: core.CORE_ORIGIN, CORE_KEY_FILE: "worker.key" }),
  ).toThrow(
    /^Invalid configuration fields: CORE_SERVERNAME, CORE_AUDIENCE, CORE_PEER_SHA256, CORE_SUBJECT_ALTNAME, CORE_CERT_FILE, CORE_CA_FILE$/,
  );
  expect(loadCoreWorkerConfig({ ...valid, ...core }).CORE_ORIGIN).toBe(core.CORE_ORIGIN);
});

test("production refuses a worker that cannot recover expired reservations through Core", () => {
  expect(() => loadCoreWorkerConfig({ ...valid, NODE_ENV: "production" })).toThrow("CORE_ORIGIN");
  expect(loadCoreWorkerConfig({ ...valid, NODE_ENV: "production", ...core }).NODE_ENV).toBe(
    "production",
  );
  expect(loadCoreWorkerConfig(valid).CORE_ORIGIN).toBeUndefined();
});

test("maintenance batches beyond what one settlement or recovery call accepts are refused", () => {
  expect(() => loadCoreWorkerConfig({ ...valid, RECONCILE_BATCH: "257" })).toThrow(
    "RECONCILE_BATCH",
  );
  expect(() => loadCoreWorkerConfig({ ...valid, RECOVERY_BATCH: "101" })).toThrow("RECOVERY_BATCH");
});

test("the shipped production example loads with a complete Core recovery client over TLS", async () => {
  const text = await Bun.file(
    new URL("../../../../infra/core-worker.example.env", import.meta.url),
  ).text();
  const env = Object.fromEntries(
    text
      .split("\n")
      .filter(Boolean)
      .map((line) => [line.slice(0, line.indexOf("=")), line.slice(line.indexOf("=") + 1)]),
  );
  const config = loadCoreWorkerConfig(env);
  expect(config.NODE_ENV).toBe("production");
  expect(config.CORE_ORIGIN?.startsWith("https://")).toBe(true);
  expect(config.QUEUE_REDIS_URL.startsWith("rediss://")).toBe(true);
  expect(new URL(config.DATABASE_URL).searchParams.get("sslmode")).toBe("verify-full");
  expect(config.HOST).toBe("127.0.0.1");
});
