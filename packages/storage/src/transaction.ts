import type { SQL } from "bun";

export class DatabaseCommitUnknown extends Error {
  constructor(cause: unknown) {
    super("DATABASE_COMMIT_OUTCOME_UNKNOWN", { cause });
  }
}

export class DatabaseDeadlineExceeded extends Error {
  constructor(cause?: unknown) {
    super("DATABASE_OPERATION_DEADLINE", { cause });
  }
}

type DatabaseLimits = { deadline: number; signal?: AbortSignal };

async function databaseOperation<T>(
  limits: DatabaseLimits,
  action: (signal: AbortSignal) => Promise<T>,
) {
  limits.signal?.throwIfAborted();
  const remaining = limits.deadline - performance.now();
  if (!Number.isFinite(remaining) || remaining > 60000)
    throw new TypeError("DATABASE_DEADLINE_INVALID");
  if (!(remaining > 0)) throw new DatabaseDeadlineExceeded();
  const controller = new AbortController();
  const cancel = () => controller.abort(limits.signal?.reason);
  const timer = setTimeout(() => controller.abort(new DatabaseDeadlineExceeded()), remaining);
  limits.signal?.addEventListener("abort", cancel, { once: true });
  try {
    if (limits.signal?.aborted) cancel();
    return await action(controller.signal);
  } catch (error) {
    if (error instanceof DatabaseCommitUnknown) throw error;
    controller.signal.throwIfAborted();
    if ((error as { errno?: string } | null)?.errno === "25P04")
      throw new DatabaseDeadlineExceeded(error);
    throw error;
  } finally {
    clearTimeout(timer);
    limits.signal?.removeEventListener("abort", cancel);
  }
}

export async function withDatabaseConnection<T>(
  db: SQL,
  limits: DatabaseLimits,
  action: (connection: SQL, signal: AbortSignal, close: () => Promise<void>) => Promise<T>,
) {
  return databaseOperation(limits, async (signal) => {
    const connection = await db.reserve({ signal });
    let closing: Promise<void> | undefined;
    const close = () =>
      (closing ??= Promise.resolve()
        .then(() => connection.close({ timeout: 0 }))
        .catch(() => {}));
    const cancel = () => void close();
    signal.addEventListener("abort", cancel, { once: true });
    if (signal.aborted) cancel();
    try {
      signal.throwIfAborted();
      return await action(connection, signal, close);
    } finally {
      signal.removeEventListener("abort", cancel);
      if (closing) await closing;
      else await connection.release();
    }
  });
}

export async function runDatabaseQuery<T>(query: PromiseLike<T>, signal: AbortSignal) {
  signal.throwIfAborted();
  return await query;
}

export async function withDatabaseSavepoint<T>(tx: SQL, action: (tx: SQL) => Promise<T>) {
  const name = `stg_${crypto.randomUUID().replaceAll("-", "")}`;
  await tx.unsafe(`SAVEPOINT ${name}`);
  try {
    const result = await action(tx);
    await tx.unsafe(`RELEASE SAVEPOINT ${name}`);
    return result;
  } catch (error) {
    await tx.unsafe(`ROLLBACK TO SAVEPOINT ${name}`);
    await tx.unsafe(`RELEASE SAVEPOINT ${name}`);
    throw error;
  }
}

export async function withDatabaseTransaction<T>(
  db: SQL,
  limits: DatabaseLimits & { synchronousCommit?: "remote_apply" },
  action: (tx: SQL, signal: AbortSignal) => Promise<T>,
) {
  return withDatabaseConnection(db, limits, async (connection, signal, close) => {
    let committing = false;
    try {
      await runDatabaseQuery(connection`BEGIN`, signal);
      const remaining = Math.floor(limits.deadline - performance.now());
      if (remaining < 1) throw new DatabaseDeadlineExceeded();
      const requestedCommit = limits.synchronousCommit;
      const [setup] = await runDatabaseQuery(
        connection<{ ready: boolean }[]>`SELECT
          set_config('transaction_timeout',${String(remaining)},true),
          set_config('statement_timeout',${String(remaining)},true),
          set_config('lock_timeout',${String(remaining)},true),
          CASE WHEN ${requestedCommit ?? null}::text IS NOT NULL
            THEN set_config('synchronous_commit',${requestedCommit ?? null},true) END,
          CASE WHEN ${requestedCommit ?? null}::text IS NULL THEN true ELSE NOT pg_is_in_recovery()
            AND current_setting('fsync')='on' AND current_setting('full_page_writes')='on'
            AND current_setting('synchronous_standby_names')<>'' END AS ready`,
        signal,
      );
      if (!setup?.ready) throw new Error("DATABASE_REPLICATION_UNAVAILABLE");
      const result = await action(connection, signal);
      signal.throwIfAborted();
      if (!(limits.deadline > performance.now())) throw new DatabaseDeadlineExceeded();
      committing = true;
      await runDatabaseQuery(connection`COMMIT`, signal);
      return result;
    } catch (error) {
      const cleanup = new AbortController();
      const timer = setTimeout(() => {
        cleanup.abort();
        void close();
      }, 1000);
      try {
        await runDatabaseQuery(connection`ROLLBACK`, cleanup.signal);
      } catch {
        await close();
      } finally {
        clearTimeout(timer);
      }
      if (committing) throw new DatabaseCommitUnknown(error);
      throw error;
    }
  });
}
