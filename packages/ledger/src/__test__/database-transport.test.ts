import { expect, test } from "bun:test";
import { assertProductionPostgresTransport } from "../database.ts";

test("production PostgreSQL configuration requires remote certificate and hostname verification", () => {
  expect(() =>
    assertProductionPostgresTransport("postgres://runtime@db.example.test/ledger", undefined),
  ).toThrow("sslmode=verify-full");
  expect(() =>
    assertProductionPostgresTransport(
      "postgres://runtime@db.example.test/ledger?sslmode=verify-full",
      "0",
    ),
  ).toThrow("certificate verification");
  expect(() =>
    assertProductionPostgresTransport(
      "postgres://runtime@db.example.test/ledger?sslmode=verify-full",
      undefined,
    ),
  ).not.toThrow();
  expect(() =>
    assertProductionPostgresTransport("postgres://runtime@127.0.0.1/ledger", undefined),
  ).not.toThrow();
});
