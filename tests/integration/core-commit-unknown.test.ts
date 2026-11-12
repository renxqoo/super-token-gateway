import { expect, test } from "bun:test";
import { CoreRpcError } from "@gateway/core-client";
import type { ExecutionReply } from "@gateway/contracts";
import { withFinancial, prepareFinancialAdmission } from "../support/financial-execution.ts";
import { createCommitAckProxy } from "../support/commit-ack-proxy.ts";
test("lost PostgreSQL COMMIT acknowledgement never emits a begin grant and replay only recovers safe status", async () => {
  const proxy = await createCommitAckProxy();
  try {
    await withFinancial(
      async (f, db) => {
        const setup = await f.bootstrap(),
          owner = f.client();
        await owner.startup();
        const prepared = await prepareFinancialAdmission(setup, owner),
          admitted = await prepared.admit(),
          command = crypto.randomUUID(),
          observed = proxy.arm();
        const pending = owner
          .execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
            commandId: command,
            deadlineAt: prepared.deadline,
          })
          .catch((error) => error as Error);
        await observed;
        const failure = await pending;
        expect(failure).toBeInstanceOf(CoreRpcError);
        expect((failure as CoreRpcError).detail.command_state).toBe("UNKNOWN");
        expect((failure as CoreRpcError).detail.retryability).toBe("STATUS_ONLY");
        expect(
          (await db`SELECT state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`)[0]
            .state,
        ).toBe("SEND_GRANTED");
        const replay = (await owner.execution(
          "execution_begin_v1",
          prepared.beginData(admitted.receipt),
          { commandId: command, deadlineAt: prepared.deadline },
        )) as ExecutionReply;
        expect(replay.receipt.state).toBe("SEND_GRANTED");
        expect(replay.transient_send).toBeUndefined();
        expect(
          (
            await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
          )[0].reserved_atoms,
        ).toBe("30");
      },
      { databasePort: proxy.port },
    );
  } finally {
    await proxy.close();
  }
}, 30000);

test("lost recovery COMMIT acknowledgement recovers the same abort receipt without a second release journal", async () => {
  const proxy = await createCommitAckProxy();
  try {
    await withFinancial(
      async (f, db) => {
        const setup = await f.bootstrap(),
          owner = f.client(),
          worker = f.client(f.identity.b.port, "worker");
        await owner.startup();
        await worker.startup();
        const prepared = await prepareFinancialAdmission(setup, owner);
        await prepared.admit();
        await db`UPDATE api_keys SET enabled=false WHERE id=${setup.key.id}`;
        const command = crypto.randomUUID(),
          deadline = new Date(Date.now() + 25000).toISOString(),
          observed = proxy.arm(),
          pending = worker
            .execution(
              "execution_abort_recovery_v1",
              { attempt_id: prepared.attemptId },
              { commandId: command, deadlineAt: deadline },
            )
            .catch((error) => error as Error);
        await observed;
        const failure = await pending;
        expect(failure).toBeInstanceOf(CoreRpcError);
        expect((failure as CoreRpcError).detail.command_state).toBe("UNKNOWN");
        const replay = (await worker.execution(
          "execution_abort_recovery_v1",
          { attempt_id: prepared.attemptId },
          { commandId: command, deadlineAt: deadline },
        )) as ExecutionReply;
        expect(replay.receipt.state).toBe("ABORTED");
        expect(
          (await db`SELECT count(*)::integer n FROM upstream_journals WHERE kind='RELEASE'`)[0].n,
        ).toBe(1);
        expect(
          (
            await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`
          )[0].reserved_atoms,
        ).toBe("0");
      },
      { databasePort: proxy.port },
    );
  } finally {
    await proxy.close();
  }
}, 30000);
