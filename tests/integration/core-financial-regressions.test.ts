import { expect, test } from "bun:test";
import { RedisClient } from "bun";
import { CoreRpcError } from "@gateway/core-client";
import type { ExecutionReply } from "@gateway/contracts";
import {
  withFinancial,
  prepareFinancialAdmission,
  financialRpcSession,
} from "../support/financial-execution.ts";
import { createCommitAckProxy } from "../support/commit-ack-proxy.ts";
import { sha256 } from "@gateway/runtime/crypto";

async function redisValues(namespace: string) {
  const redis = new RedisClient(process.env.REDIS_URL!);
  try {
    await redis.connect();
    const keys = ((await redis.send("KEYS", [`${namespace}:*`])) as string[]).sort();
    return await Promise.all(keys.map(async (key) => [key, await redis.send("DUMP", [key])]));
  } finally {
    redis.close();
  }
}

test(
  "wrong permit epochs cannot poison begin adopt renew release or close",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        rpc = await financialRpcSession(f),
        p = await prepareFinancialAdmission(setup, rpc),
        admitted = await p.admit();
      const before = await redisValues(f.namespace),
        leases =
          await db`SELECT state,revision::text,descriptor FROM execution_quota_leases ORDER BY lease_id`;
      const wrong = await rpc.send(
        "execution_begin_v1",
        { ...p.beginData(admitted.receipt), permit_epoch: "2" },
        { deadlineAt: p.deadline },
      );
      expect(wrong.status).toBe(403);
      for (const action of ["ADOPT", "RENEW", "RELEASE", "CLOSE"]) {
        const response = await rpc.send(
          "execution_quota_v1",
          {
            attempt_id: p.attemptId,
            permit_epoch: "2",
            action,
            ...(action === "CLOSE" ? { reason: "COMPLETED" } : {}),
          },
          { deadlineAt: p.deadline },
        );
        expect(response.status).toBe(403);
      }
      expect(await redisValues(f.namespace)).toEqual(before);
      expect(
        await db`SELECT state,revision::text,descriptor FROM execution_quota_leases ORDER BY lease_id`,
      ).toEqual(leases);
      const [correct, invalid] = await Promise.all([
        rpc.send("execution_begin_v1", p.beginData(admitted.receipt), { deadlineAt: p.deadline }),
        rpc.send(
          "execution_quota_v1",
          { attempt_id: p.attemptId, permit_epoch: "2", action: "RENEW" },
          { deadlineAt: p.deadline },
        ),
      ]);
      expect(correct.status).toBe(200);
      expect(correct.value.transient_send).toBeDefined();
      expect(invalid.status).toBe(403);
      expect(
        (await db`SELECT state FROM execution_permits WHERE attempt_id=${p.attemptId}`)[0].state,
      ).toBe("SEND_GRANTED");
      const closed = await rpc.send(
        "execution_quota_v1",
        { attempt_id: p.attemptId, permit_epoch: "1", action: "CLOSE", reason: "COMPLETED" },
        { deadlineAt: p.deadline },
      );
      expect(closed.status).toBe(200);
      const released = await redisValues(f.namespace);
      const renew = await rpc.send(
        "execution_quota_v1",
        { attempt_id: p.attemptId, permit_epoch: "1", action: "RENEW" },
        { deadlineAt: p.deadline },
      );
      expect(renew.status).toBe(409);
      expect(await redisValues(f.namespace)).toEqual(released);
    }),
  30000,
);

test(
  "persistent financial rejections expose 503 402 409 typed errors with immutable query receipts and no extra holds",
  () =>
    withFinancial(async (f, db) => {
      const rpc = await financialRpcSession(f),
        low = await f.bootstrap("USD", "1"),
        supplier = await prepareFinancialAdmission(low, rpc);
      const owner = f.client();
      await owner.startup();
      const setup = await f.bootstrap();
      await f.request("/billing/tariffs", {
        model: setup.model,
        operation: "chat",
        provider_id: setup.provider.id,
        pricing: { currency: "USD", rates: { requests: { numerator: "1000", denominator: "1" } } },
        ceilings: { requests: "1" },
        basis: "REQUEST_COMPLETION",
      });
      const first = await prepareFinancialAdmission(setup, owner);
      await first.admit();
      const customer = await prepareFinancialAdmission(setup, owner),
        conflict = await prepareFinancialAdmission(setup, owner, {
          intent: first.data.intent_digest!,
        });
      const raw = await rpc.send("execution_admit_v1", supplier.data, {
        commandId: supplier.command,
        deadlineAt: supplier.deadline,
      });
      expect(raw.status).toBe(503);
      expect(raw.value).toMatchObject({
        code: "upstream_funds_unavailable",
        command_state: "REJECTED",
        retryability: "NONE",
      });
      const supplierClient = await prepareFinancialAdmission(low, owner);
      const cases = [
        [supplierClient, 503, "upstream_funds_unavailable"],
        [customer, 402, "insufficient_funds"],
        [conflict, 409, "already_accepted"],
      ] as const;
      for (const [p, status, code] of cases) {
        for (let replay = 0; replay < 2; replay++) {
          const error = await p.admit().catch((error) => error);
          expect(error).toBeInstanceOf(CoreRpcError);
          expect((error as CoreRpcError).status).toBe(status);
          expect((error as CoreRpcError).detail).toMatchObject({
            code,
            command_state: "REJECTED",
            retryability: "NONE",
          });
        }
        const query = (await owner.execution(
          "execution_query_v1",
          { kind: "execution_admit_v1", command_id: p.command },
          { deadlineAt: p.deadline },
        )) as ExecutionReply;
        expect(query.receipt).toMatchObject({ command_state: "REJECTED", code });
        expect(query.transient_send).toBeUndefined();
        expect(
          (
            await db`SELECT state,rejection_code FROM execution_commands WHERE command_id=${p.command}`
          )[0],
        ).toEqual({ state: "REJECTED", rejection_code: code });
      }
      expect((await db`SELECT count(*)::integer n FROM billing_holds`)[0].n).toBe(1);
      expect((await db`SELECT count(*)::integer n FROM upstream_holds`)[0].n).toBe(1);
      expect(
        (
          await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${low.provider.account_id}`
        )[0].reserved_atoms,
      ).toBe("0");
      expect(
        (await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`)[0]
          .reserved_atoms,
      ).toBe("1000");
    }),
  30000,
);

test("unknown begin COMMIT tells callers status only and query or replay never grants again", async () => {
  const proxy = await createCommitAckProxy();
  try {
    await withFinancial(
      async (f, db) => {
        const setup = await f.bootstrap(),
          rpc = await financialRpcSession(f),
          p = await prepareFinancialAdmission(setup, rpc),
          admitted = await p.admit(),
          command = crypto.randomUUID(),
          ack = proxy.arm();
        const pending = rpc.send("execution_begin_v1", p.beginData(admitted.receipt), {
          commandId: command,
          deadlineAt: p.deadline,
        });
        await ack;
        const response = await pending;
        expect(response.status).toBe(503);
        expect(response.value).toMatchObject({
          command_state: "UNKNOWN",
          retryability: "STATUS_ONLY",
        });
        expect(response.value.transient_send).toBeUndefined();
        for (const result of [
          await rpc.send(
            "execution_query_v1",
            { kind: "execution_begin_v1", command_id: command },
            { deadlineAt: p.deadline },
          ),
          await rpc.send("execution_begin_v1", p.beginData(admitted.receipt), {
            commandId: command,
            deadlineAt: p.deadline,
          }),
        ]) {
          expect(result.status).toBe(200);
          expect(result.value.receipt).toMatchObject({
            command_state: "COMMITTED",
            state: "SEND_GRANTED",
          });
          expect(result.value.transient_send).toBeUndefined();
        }
        expect(
          (
            await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
          )[0].reserved_atoms,
        ).toBe("30");
        expect(
          (await db`SELECT count(*)::integer n FROM upstream_journals WHERE kind='RESERVE'`)[0].n,
        ).toBe(1);
      },
      { databasePort: proxy.port },
    );
  } finally {
    await proxy.close();
  }
}, 30000);

test(
  "committed begin response budget failure returns STATUS_ONLY without releasing holds or regranting send authority",
  () =>
    withFinancial(
      async (f, db) => {
        const setup = await f.bootstrap(),
          credential = "k".repeat(4096);
        expect(
          (
            await f.request(
              `/providers/${setup.provider.id}`,
              { api_key: credential },
              undefined,
              "PATCH",
            )
          ).status,
        ).toBe(200);
        const owner = f.client();
        await owner.startup();
        const p = await prepareFinancialAdmission(setup, owner),
          admitted = await p.admit(),
          command = crypto.randomUUID();
        expect(admitted.receipt).toMatchObject({
          customer_atoms: "100",
          supplier_atoms: "30",
          state: "RESERVED",
        });
        const failure = await owner
          .execution("execution_begin_v1", p.beginData(admitted.receipt), {
            commandId: command,
            deadlineAt: p.deadline,
          })
          .catch((error: unknown) => error);
        expect(failure).toBeInstanceOf(CoreRpcError);
        if (!(failure instanceof CoreRpcError)) throw new Error("Expected bounded RPC error");
        expect(failure.status).toBe(503);
        expect(failure.detail.command_state).toBe("UNKNOWN");
        expect(Object.keys(failure.detail).sort()).toEqual([
          "category",
          "code",
          "command_state",
          "message",
          "retryability",
        ]);
        expect(JSON.stringify(failure.detail)).not.toContain(credential);
        expect(
          (await db`SELECT state FROM execution_permits WHERE attempt_id=${p.attemptId}`)[0].state,
        ).toBe("SEND_GRANTED");
        expect(
          (await db`SELECT state FROM execution_commands WHERE command_id=${command}`)[0].state,
        ).toBe("COMMITTED");
        for (const result of [
          await owner.execution(
            "execution_query_v1",
            { kind: "execution_begin_v1", command_id: command },
            { deadlineAt: p.deadline },
          ),
          await owner.execution("execution_begin_v1", p.beginData(admitted.receipt), {
            commandId: command,
            deadlineAt: p.deadline,
          }),
        ] as ExecutionReply[]) {
          expect(result.receipt).toMatchObject({
            command_state: "COMMITTED",
            state: "SEND_GRANTED",
          });
          expect(result.transient_send).toBeUndefined();
          expect(JSON.stringify(result)).not.toContain(credential);
        }
        const newCommand = await owner
          .execution("execution_begin_v1", p.beginData(admitted.receipt), {
            deadlineAt: p.deadline,
          })
          .catch((error: unknown) => error);
        expect(newCommand).toBeInstanceOf(CoreRpcError);
        if (!(newCommand instanceof CoreRpcError))
          throw new Error("Expected stable begin rejection");
        expect(newCommand.status).toBe(409);
        expect(newCommand.detail).toMatchObject({
          code: "permit_not_reserved",
          command_state: "REJECTED",
          retryability: "NONE",
        });
        expect(JSON.stringify(newCommand.detail)).not.toContain(credential);
        expect(
          (
            await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id} AND currency='USD'`
          )[0].reserved_atoms,
        ).toBe("30");
        expect(
          (
            await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id} AND currency='USD'`
          )[0].reserved_atoms,
        ).toBe("100");
        expect(
          (
            await db`SELECT count(*)::integer n FROM upstream_journals WHERE attempt_id=${p.attemptId} AND kind='RELEASE'`
          )[0].n,
        ).toBe(0);
        expect(
          (
            await db`SELECT count(*)::integer n FROM upstream_journals WHERE attempt_id=${p.attemptId} AND kind='RESERVE'`
          )[0].n,
        ).toBe(1);
        expect((await db`SELECT count(*)::integer n FROM upstream_holds`)[0].n).toBe(1);
        expect((await db`SELECT count(*)::integer n FROM billing_holds`)[0].n).toBe(1);
        expect(failure.detail).toMatchObject({
          code: "identity_store_unavailable",
          category: "DEPENDENCY",
          command_state: "UNKNOWN",
          retryability: "STATUS_ONLY",
        });
      },
      { limits: { responseBytes: 4096 } },
    ),
  30000,
);

for (const mismatch of ["fingerprint", "resources"] as const)
  test(
    `next ${mismatch} mismatch cannot attach another logical request or alter original money and quota`,
    () =>
      withFinancial(async (f, db) => {
        const setup = await f.bootstrap(),
          rpc = await financialRpcSession(f),
          p = await prepareFinancialAdmission(setup, rpc),
          admitted = await p.admit();
        await rpc.execution("execution_begin_v1", p.beginData(admitted.receipt), {
          deadlineAt: p.deadline,
        });
        const before = (
          await db`SELECT billing_quote,execution_budget FROM gateway_requests WHERE id=${p.requestId}`
        )[0];
        const invalid = await prepareFinancialAdmission(setup, rpc, {
          requestId: p.requestId,
          deadline: p.deadline,
          ...(mismatch === "fingerprint"
            ? { fingerprint: sha256("different-customer-request") }
            : { resourceIds: [crypto.randomUUID()] }),
        });
        const {
          project_id: _project,
          key_id: _key,
          intent_digest: _intent,
          fingerprint: _fingerprint,
          customer_currency: _currency,
          ...dispatch
        } = invalid.data;
        const response = await rpc.send(
          "execution_next_v1",
          {
            ...dispatch,
            request_id: p.requestId,
            previous_attempt_id: p.attemptId,
            expected_ordinal: 2,
            visible_output: false,
          },
          { deadlineAt: p.deadline },
        );
        expect(response.status).toBe(409);
        expect(response.value).toMatchObject({
          command_state: "REJECTED",
          code: "request_mismatch",
          retryability: "NONE",
        });
        expect(
          (await db`SELECT count(*)::integer n FROM attempts WHERE request_id=${p.requestId}`)[0].n,
        ).toBe(1);
        expect(
          (await db`SELECT state FROM execution_permits WHERE attempt_id=${p.attemptId}`)[0].state,
        ).toBe("SEND_GRANTED");
        expect(
          (
            await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
          )[0].reserved_atoms,
        ).toBe("30");
        expect(
          (
            await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`
          )[0].reserved_atoms,
        ).toBe("100");
        expect(
          (
            await db`SELECT billing_quote,execution_budget FROM gateway_requests WHERE id=${p.requestId}`
          )[0],
        ).toEqual(before);
        expect(
          (
            await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE scope_kind<>'channel' AND state='ACTIVE'`
          )[0].n,
        ).toBe(2);
        const valid = await prepareFinancialAdmission(setup, rpc, {
            requestId: p.requestId,
            deadline: p.deadline,
          }),
          {
            project_id: _p,
            key_id: _k,
            intent_digest: _i,
            fingerprint: _f,
            customer_currency: _c,
            ...validDispatch
          } = valid.data;
        const next = (await rpc.execution(
          "execution_next_v1",
          {
            ...validDispatch,
            request_id: p.requestId,
            previous_attempt_id: p.attemptId,
            expected_ordinal: 2,
            visible_output: false,
          },
          { deadlineAt: p.deadline },
        )) as ExecutionReply;
        expect(next.receipt).toMatchObject({
          command_state: "COMMITTED",
          ordinal: 2,
          customer_atoms: "100",
        });
        expect(
          (await db`SELECT state FROM execution_permits WHERE attempt_id=${p.attemptId}`)[0].state,
        ).toBe("UNKNOWN");
        expect(
          (
            await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
          )[0].reserved_atoms,
        ).toBe("60");
        expect(
          (
            await db`SELECT billing_quote,execution_budget FROM gateway_requests WHERE id=${p.requestId}`
          )[0],
        ).toEqual(before);
      }),
    30000,
  );

test(
  "known precommit begin rollback stays retryable with the same command and grants only after a later commit",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        rpc = await financialRpcSession(f),
        p = await prepareFinancialAdmission(setup, rpc),
        admitted = await p.admit(),
        command = crypto.randomUUID();
      await db.unsafe(
        "CREATE FUNCTION begin_audit_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='execution_begin_v1' THEN RAISE EXCEPTION 'synthetic begin audit failure';END IF;RETURN NEW;END $$;CREATE TRIGGER begin_audit_fail BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION begin_audit_fail()",
      );
      const result = await rpc.send("execution_begin_v1", p.beginData(admitted.receipt), {
        commandId: command,
        deadlineAt: p.deadline,
      });
      expect(result.status).toBe(503);
      expect(result.value).toMatchObject({
        command_state: "UNKNOWN",
        retryability: "SAME_COMMAND",
      });
      expect(
        (
          await db`SELECT count(*)::integer n FROM execution_commands WHERE command_id=${command}`
        )[0].n,
      ).toBe(0);
      expect(
        (await db`SELECT state FROM execution_permits WHERE attempt_id=${p.attemptId}`)[0].state,
      ).toBe("RESERVED");
      await db.unsafe("DROP TRIGGER begin_audit_fail ON audit_events");
      const retry = await rpc.send("execution_begin_v1", p.beginData(admitted.receipt), {
        commandId: command,
        deadlineAt: p.deadline,
      });
      expect(retry.status).toBe(200);
      expect(retry.value.transient_send).toBeDefined();
    }),
  30000,
);

test(
  "begin client transport timeout exposes UNKNOWN STATUS_ONLY and later query has no transient authority",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        owner = f.client(f.identity.a.port, "gateway", { timeoutMs: 500 });
      await owner.startup();
      const p = await prepareFinancialAdmission(setup, owner),
        admitted = await p.admit(),
        command = crypto.randomUUID();
      await db.unsafe(
        "CREATE FUNCTION begin_audit_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='execution_begin_v1' THEN PERFORM pg_sleep(1);END IF;RETURN NEW;END $$;CREATE TRIGGER begin_audit_delay BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION begin_audit_delay()",
      );
      const failure = await owner
        .execution("execution_begin_v1", p.beginData(admitted.receipt), {
          commandId: command,
          deadlineAt: p.deadline,
        })
        .catch((error) => error);
      expect(failure).toBeInstanceOf(CoreRpcError);
      expect((failure as CoreRpcError).detail).toMatchObject({
        command_state: "UNKNOWN",
        retryability: "STATUS_ONLY",
      });
      await Bun.sleep(700);
      const query = (await owner.execution(
        "execution_query_v1",
        { kind: "execution_begin_v1", command_id: command },
        { deadlineAt: p.deadline },
      )) as ExecutionReply;
      expect(query.receipt).toMatchObject({ command_state: "COMMITTED", state: "SEND_GRANTED" });
      expect(query.transient_send).toBeUndefined();
      expect(
        (
          await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
        )[0].reserved_atoms,
      ).toBe("30");
    }),
  30000,
);
