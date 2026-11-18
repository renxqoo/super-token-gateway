import { expect, test } from "bun:test";
import { SQL } from "bun";
import { runDatabaseQuery, withDatabaseTransaction } from "@gateway/storage";
import { withDatabase } from "../support/database.ts";
import { createCommitAckProxy } from "../support/commit-ack-proxy.ts";

test("canceling an owner transaction in the pool queue cannot execute a late mutation after a connection is released", () =>
  withDatabase(async (db, name) => {
    await db`CREATE TABLE owner_transaction_probe(id integer PRIMARY KEY)`;
    const url = new URL(process.env.DATABASE_URL!);
    url.pathname = `/${name}`;
    const pool = new SQL(url, { max: 1 });
    const held = await pool.reserve();
    let invoked = false;
    const controller = new AbortController(),
      reason = new Error("pool canceled");
    const timer = setTimeout(() => controller.abort(reason), 30);
    try {
      const error = await withDatabaseTransaction(
        pool,
        { signal: controller.signal, deadline: performance.now() + 1000 },
        async (tx) => {
          invoked = true;
          await tx`INSERT INTO owner_transaction_probe VALUES(1)`;
        },
      ).catch((failure: unknown) => failure);
      expect(error).toBe(reason);
      expect(invoked).toBe(false);
      await held.release();
      expect((await pool`SELECT count(*)::integer n FROM owner_transaction_probe`)[0].n).toBe(0);
      expect(invoked).toBe(false);
    } finally {
      clearTimeout(timer);
      await held.release();
      await pool.close();
    }
  }));

test("several individually short SQL statements cannot exceed the owner's total transaction deadline or commit", () =>
  withDatabase(async (db) => {
    await db`CREATE TABLE owner_transaction_probe(id integer PRIMARY KEY)`;
    const controller = new AbortController(),
      until = performance.now() + 200;
    const error = await withDatabaseTransaction(
      db,
      { signal: controller.signal, deadline: until },
      async (tx) => {
        await runDatabaseQuery(
          tx`INSERT INTO owner_transaction_probe VALUES(1)`,
          controller.signal,
        );
        await runDatabaseQuery(tx`SELECT pg_sleep(0.15)`, controller.signal);
        await runDatabaseQuery(tx`SELECT pg_sleep(0.15)`, controller.signal);
      },
    ).catch((failure: unknown) => failure);
    expect(error).toMatchObject({ message: "DATABASE_OPERATION_DEADLINE" });
    expect((await db`SELECT count(*)::integer n FROM owner_transaction_probe`)[0].n).toBe(0);
    expect(
      (
        await db`SELECT count(*)::integer n FROM pg_stat_activity WHERE datname=current_database() AND state LIKE 'idle in transaction%'`
      )[0].n,
    ).toBe(0);
  }));

test("cancellation between owner SQL statements rolls back instead of sending COMMIT after the callback returns", () =>
  withDatabase(async (db) => {
    await db`CREATE TABLE owner_transaction_probe(id integer PRIMARY KEY)`;
    const controller = new AbortController(),
      reason = new Error("between statements canceled");
    const until = performance.now() + 1000;
    const error = await withDatabaseTransaction(
      db,
      { signal: controller.signal, deadline: until },
      async (tx) => {
        await runDatabaseQuery(
          tx`INSERT INTO owner_transaction_probe VALUES(1)`,
          controller.signal,
        );
        controller.abort(reason);
        await Bun.sleep(20);
      },
    ).catch((failure: unknown) => failure);
    expect(error).toBe(reason);
    expect((await db`SELECT count(*)::integer n FROM owner_transaction_probe`)[0].n).toBe(0);
    expect(
      (
        await db`SELECT count(*)::integer n FROM pg_stat_activity WHERE datname=current_database() AND state LIKE 'idle in transaction%'`
      )[0].n,
    ).toBe(0);
  }));

test(
  "a lost owner COMMIT reply reports an unknown outcome while preserving the actual committed mutation",
  () =>
    withDatabase(async (db, name) => {
      await db`CREATE TABLE owner_transaction_probe(id integer PRIMARY KEY)`;
      const proxy = await createCommitAckProxy();
      const url = new URL(process.env.DATABASE_URL!);
      url.pathname = `/${name}`;
      url.port = String(proxy.port);
      url.searchParams.set("sslmode", "disable");
      const pool = new SQL(url, { max: 1 });
      const controller = new AbortController(),
        until = performance.now() + 3000;
      let operation: Promise<unknown> | undefined;
      try {
        const held = proxy.arm([], 3000, "acknowledgement");
        operation = withDatabaseTransaction(
          pool,
          { signal: controller.signal, deadline: until },
          async (tx) => {
            await runDatabaseQuery(
              tx`INSERT INTO owner_transaction_probe VALUES(1)`,
              controller.signal,
            );
          },
        ).catch((failure: unknown) => failure);
        await held;
        expect((await db`SELECT count(*)::integer n FROM owner_transaction_probe`)[0].n).toBe(1);
        controller.abort(new Error("canceled after commit"));
        let watchdog: ReturnType<typeof setTimeout> | undefined;
        let error: unknown;
        try {
          error = await Promise.race([
            operation,
            new Promise((resolve) => {
              watchdog = setTimeout(() => resolve(new Error("OWNER_COMMIT_DID_NOT_SETTLE")), 500);
            }),
          ]);
        } finally {
          clearTimeout(watchdog);
        }
        expect(error).toMatchObject({ message: "DATABASE_COMMIT_OUTCOME_UNKNOWN" });
        expect((await db`SELECT count(*)::integer n FROM owner_transaction_probe`)[0].n).toBe(1);
      } finally {
        controller.abort();
        await proxy.close();
        await operation;
        await pool.close({ timeout: 1 });
      }
    }),
  30000,
);
