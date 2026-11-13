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
