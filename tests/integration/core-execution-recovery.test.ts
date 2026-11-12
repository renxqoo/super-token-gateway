import { financialRejection } from "../support/financial-rejection.ts";
import type { ExecutionReply } from "@gateway/contracts";
import { expect, test } from "bun:test";
import { withFinancial, prepareFinancialAdmission } from "../support/financial-execution.ts";

test(
  "healthy unexpired RESERVED refuses technical abort and Gateway cannot invoke worker recovery",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        owner = f.client(),
        worker = f.client(f.identity.b.port, "worker");
      await owner.startup();
      await worker.startup();
      const prepared = await prepareFinancialAdmission(setup, owner);
      await prepared.admit();
      const command = crypto.randomUUID(),
        deadline = new Date(Date.now() + 20000).toISOString(),
        data = { attempt_id: prepared.attemptId };
      const rejected = (await financialRejection(
        worker.execution("execution_abort_recovery_v1", data, {
          commandId: command,
          deadlineAt: deadline,
        }),
        409,
      )) as ExecutionReply;
      expect(rejected.receipt).toEqual({
        command_state: "REJECTED",
        code: "recovery_not_eligible",
      });
      expect(
        await financialRejection(
          worker.execution("execution_abort_recovery_v1", data, {
            commandId: command,
            deadlineAt: deadline,
          }),
          409,
        ),
      ).toEqual(rejected);
      const query = (await worker.execution(
        "execution_query_v1",
        { kind: "execution_abort_recovery_v1", command_id: command },
        { deadlineAt: deadline },
      )) as ExecutionReply;
      expect(query.receipt.command_state).toBe("REJECTED");
      await expect(
        owner.execution("execution_abort_recovery_v1", data, { deadlineAt: deadline }),
      ).rejects.toThrow("internal_peer_forbidden");
      expect(
        (
          await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
        )[0].reserved_atoms,
      ).toBe("30");
    }),
  30000,
);

for (const mode of ["deadline", "customer", "instance", "certificate", "session"] as const)
  test(
    `${mode} recovery aborts only never-granted RESERVED and closes the last customer hold atomically`,
    () =>
      withFinancial(async (f, db) => {
        const setup = await f.bootstrap(),
          owner = f.client(),
          worker = f.client(f.identity.b.port, "worker");
        const session = await owner.startup();
        await worker.startup();
        const deadline = new Date(Date.now() + (mode === "deadline" ? 300 : 20000)).toISOString(),
          prepared = await prepareFinancialAdmission(setup, owner, { deadline });
        await prepared.admit();
        if (mode === "deadline")
          await Bun.sleep(Math.max(0, Date.parse(deadline) - Date.now() + 20));
        if (mode === "customer")
          await db`UPDATE api_keys SET enabled=false WHERE id=${setup.key.id}`;
        if (mode === "instance")
          await db`SELECT internal_identity_change_v1('gateway','gatewayA',NULL,NULL,'SECURITY_REVOKE')`;
        if (mode === "certificate")
          await db`SELECT internal_identity_change_v1('gateway','gatewayA',${session.certificate_sha256},NULL,'SECURITY_REVOKE')`;
        if (mode === "session")
          await db`SELECT internal_identity_change_v1('gateway','gatewayA',NULL,${session.session_id}::uuid,'SECURITY_REVOKE')`;
        const command = crypto.randomUUID(),
          commandDeadline = new Date(Date.now() + 25000).toISOString(),
          result = (await worker.execution(
            "execution_abort_recovery_v1",
            { attempt_id: prepared.attemptId },
            { commandId: command, deadlineAt: commandDeadline },
          )) as ExecutionReply;
        expect(result.receipt).toMatchObject({
          command_state: "COMMITTED",
          state: "ABORTED",
          attempt_id: prepared.attemptId,
        });
        expect(result.transient_send).toBeUndefined();
        expect(
          (
            await db`SELECT balance_atoms::text,reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
          )[0],
        ).toEqual({ balance_atoms: "1000", reserved_atoms: "0" });
        expect(
          (
            await db`SELECT available_atoms::text,reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`
          )[0],
        ).toEqual({ available_atoms: "1000", reserved_atoms: "0" });
        expect(
          (
            await db`SELECT execution_closed_at IS NOT NULL closed FROM gateway_requests WHERE id=${prepared.requestId}`
          )[0].closed,
        ).toBe(true);
        const release = (await worker.execution("execution_quota_recovery_v1", { limit: 100 })) as {
          released: number;
        };
        expect(release.released).toBe(3);
        expect(
          (
            await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE state='RELEASED'`
          )[0].n,
        ).toBe(3);
        expect(
          (await db`SELECT count(*)::integer n FROM upstream_journals WHERE kind='RELEASE'`)[0].n,
        ).toBe(1);
      }),
    30000,
  );

test(
  "security revocation never lets recovery release SEND_GRANTED money and current owner can close after key revocation",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        owner = f.client(),
        worker = f.client(f.identity.b.port, "worker");
      const session = await owner.startup();
      await worker.startup();
      const prepared = await prepareFinancialAdmission(setup, owner),
        admitted = await prepared.admit();
      await owner.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
        deadlineAt: prepared.deadline,
      });
      await db`UPDATE api_keys SET enabled=false WHERE id=${setup.key.id}`;
      const close = (await owner.execution(
        "execution_quota_v1",
        { attempt_id: prepared.attemptId, permit_epoch: "1", action: "CLOSE", reason: "DRAINED" },
        { deadlineAt: prepared.deadline },
      )) as { receipt: ExecutionReply["receipt"]; state: string };
      expect(close.receipt.code).toBe("execution_closed");
      expect(close.state).toBe("RELEASED");
      await db`SELECT internal_identity_change_v1('gateway','gatewayA',NULL,${session.session_id}::uuid,'SECURITY_REVOKE')`;
      const recovery = (await financialRejection(
        worker.execution("execution_abort_recovery_v1", {
          attempt_id: prepared.attemptId,
        }),
        409,
      )) as ExecutionReply;
      expect(recovery.receipt).toEqual({
        command_state: "REJECTED",
        code: "recovery_not_reserved",
      });
      expect(
        (
          await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
        )[0].reserved_atoms,
      ).toBe("30");
      expect(
        (await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`)[0]
          .reserved_atoms,
      ).toBe("100");
    }),
  30000,
);

test(
  "quota recovery converges when Redis adoption committed but both PG ACTIVE and post-close RELEASED replies failed",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        owner = f.client(),
        worker = f.client(f.identity.b.port, "worker");
      await owner.startup();
      await worker.startup();
      const prepared = await prepareFinancialAdmission(setup, owner),
        admitted = await prepared.admit();
      await db.unsafe(
        `CREATE FUNCTION quota_projection_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state IN ('ACTIVE','RELEASED') THEN RAISE EXCEPTION 'synthetic quota projection failure';END IF;RETURN NEW;END $$;CREATE TRIGGER quota_projection_failure BEFORE UPDATE ON execution_quota_leases FOR EACH ROW EXECUTE FUNCTION quota_projection_failure()`,
      );
      const beginFailure = await owner
        .execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
          deadlineAt: prepared.deadline,
        })
        .catch((error) => error as Error);
      expect(beginFailure).toBeInstanceOf(Error);
      expect(
        (
          await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE state='ADOPTION_PENDING'`
        )[0].n,
      ).toBe(3);
      const closeFailure = await owner
        .execution(
          "execution_quota_v1",
          { attempt_id: prepared.attemptId, permit_epoch: "1", action: "CLOSE", reason: "DRAINED" },
          { deadlineAt: prepared.deadline },
        )
        .catch((error) => error as Error);
      expect(closeFailure).toBeInstanceOf(Error);
      expect(
        (
          await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE state='RELEASE_PENDING'`
        )[0].n,
      ).toBe(3);
      await db.unsafe(
        "DROP TRIGGER quota_projection_failure ON execution_quota_leases;DROP FUNCTION quota_projection_failure()",
      );
      const recovered = (await worker.execution("execution_quota_recovery_v1", { limit: 100 })) as {
        released: number;
      };
      expect(recovered.released).toBe(3);
      expect(
        (await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE state='RELEASED'`)[0]
          .n,
      ).toBe(3);
      expect(
        (
          await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
        )[0].reserved_atoms,
      ).toBe("30");
    }),
  30000,
);

import { financialGate, financialBlockedBy } from "../support/financial-execution.ts";
for (const first of ["begin", "recovery"] as const)
  test(
    `${first} locks first and the competing begin or technical abort cannot cross its uncommitted transition`,
    () =>
      withFinancial(async (f, db) => {
        const setup = await f.bootstrap(),
          owner = f.client(),
          worker = f.client(f.identity.b.port, "worker");
        await owner.startup();
        await worker.startup();
        const prepared = await prepareFinancialAdmission(setup, owner),
          admitted = await prepared.admit();
        if (first === "recovery")
          await db`UPDATE channels SET enabled=false WHERE id=${setup.provider.id}`;
        const gate = await financialGate(db),
          action = first === "begin" ? "execution_begin_v1" : "execution_abort_recovery_v1";
        await db.unsafe(
          `CREATE FUNCTION recovery_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='${action}' THEN PERFORM pg_advisory_xact_lock(${gate.key}::bigint);END IF;RETURN NEW;END $$;CREATE TRIGGER recovery_gate BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION recovery_gate()`,
        );
        const command = crypto.randomUUID(),
          deadline = prepared.deadline;
        const pending =
          first === "begin"
            ? owner.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
                deadlineAt: deadline,
              })
            : worker.execution(
                "execution_abort_recovery_v1",
                { attempt_id: prepared.attemptId },
                { commandId: command, deadlineAt: deadline },
              );
        try {
          await financialBlockedBy(db, gate.pid);
          const competing = await (
            first === "begin"
              ? worker.execution(
                  "execution_abort_recovery_v1",
                  { attempt_id: prepared.attemptId },
                  { commandId: command, deadlineAt: deadline },
                )
              : owner.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
                  deadlineAt: deadline,
                })
          ).catch((error) => error as Error);
          expect(competing).toBeInstanceOf(Error);
          expect(["identity_busy", "core_busy"]).toContain((competing as Error).message);
          expect(
            (
              await db`SELECT state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`
            )[0].state,
          ).toBe("RESERVED");
        } finally {
          await gate.release();
        }
        const result = (await pending) as ExecutionReply;
        expect(result.receipt.state).toBe(first === "begin" ? "SEND_GRANTED" : "ABORTED");
        expect(Boolean(result.transient_send)).toBe(first === "begin");
        const retryCall = worker.execution(
          "execution_abort_recovery_v1",
          { attempt_id: prepared.attemptId },
          { commandId: command, deadlineAt: deadline },
        );
        const retry = (
          first === "begin" ? await financialRejection(retryCall, 409) : await retryCall
        ) as ExecutionReply;
        expect(retry.receipt).toMatchObject(
          first === "begin"
            ? { command_state: "REJECTED", code: "recovery_not_reserved" }
            : { command_state: "COMMITTED", state: "ABORTED" },
        );
        expect(
          (
            await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
          )[0].reserved_atoms,
        ).toBe(first === "begin" ? "30" : "0");
      }),
    30000,
  );

test(
  "technical abort audit failure rolls back both releases closure and command receipt",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        owner = f.client(),
        worker = f.client(f.identity.b.port, "worker");
      await owner.startup();
      await worker.startup();
      const prepared = await prepareFinancialAdmission(setup, owner);
      await prepared.admit();
      await db`UPDATE api_keys SET enabled=false WHERE id=${setup.key.id}`;
      await db.unsafe(
        "CREATE FUNCTION recovery_audit_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='execution_abort_recovery_v1' THEN RAISE EXCEPTION 'synthetic recovery audit failure';END IF;RETURN NEW;END $$;CREATE TRIGGER recovery_audit_fail BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION recovery_audit_fail()",
      );
      const command = crypto.randomUUID(),
        deadlineAt = prepared.deadline;
      const result = await worker
        .execution(
          "execution_abort_recovery_v1",
          { attempt_id: prepared.attemptId },
          { commandId: command, deadlineAt },
        )
        .catch((error) => error as Error);
      expect(result).toBeInstanceOf(Error);
      expect(
        (await db`SELECT state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`)[0]
          .state,
      ).toBe("RESERVED");
      expect(
        (
          await db`SELECT execution_closed_at FROM gateway_requests WHERE id=${prepared.requestId}`
        )[0].execution_closed_at,
      ).toBeNull();
      expect(
        (await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`)[0]
          .reserved_atoms,
      ).toBe("100");
      expect(
        (
          await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
        )[0].reserved_atoms,
      ).toBe("30");
      expect(
        (
          await db`SELECT count(*)::integer n FROM execution_commands WHERE command_id=${command}`
        )[0].n,
      ).toBe(0);
      await db.unsafe(
        "DROP TRIGGER recovery_audit_fail ON audit_events;DROP FUNCTION recovery_audit_fail()",
      );
      expect(
        (
          (await worker.execution(
            "execution_abort_recovery_v1",
            { attempt_id: prepared.attemptId },
            { commandId: command, deadlineAt },
          )) as ExecutionReply
        ).receipt.state,
      ).toBe("ABORTED");
    }),
  30000,
);
