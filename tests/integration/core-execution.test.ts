import { financialRejection } from "../support/financial-rejection.ts";
import { prepBindingV1, dispatchProjectionV1Schema } from "@gateway/contracts";
import type { ExecutionReply, ExecutionContextResponse } from "@gateway/contracts";
import { expect, test } from "bun:test";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime/crypto";
import { withFinancial } from "../support/financial-execution.ts";
const digest = (value: unknown) => sha256(canonical(value));
test(
  "admission captures 60/30 at one third as 30 and begin never reissues transient material",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap();
      const client = f.client();
      await client.startup();
      const deadline = new Date(Date.now() + 25000).toISOString();
      const context = await client.execution(
        "execution_context_v1",
        {
          customer_token: setup.key.key,
          ip: "127.0.0.1",
          operation: "chat",
          model: setup.model,
          fingerprint: sha256("synthetic-input"),
        },
        { deadlineAt: deadline },
      );
      expect((context as ExecutionContextResponse).candidates).toHaveLength(1);
      const c = (context as ExecutionContextResponse).candidates[0]!,
        request = crypto.randomUUID(),
        attempt = crypto.randomUUID();
      const manifest = {
        version: 1,
        operation: "chat",
        model: setup.model,
        upstream_model: "synthetic",
        stream: false,
        input_bytes: 100,
        quantities: {},
        resource_ids: [],
        ceilings: { input_tokens: "60", output_tokens: "30" },
      };
      const projection = {
        version: 1,
        proposed_request_id: request,
        proposed_attempt_id: attempt,
        origin: c.origin,
        method: "POST",
        operation: "chat",
        request_target_sha256: sha256("/v1/chat/completions"),
        final_headers_digest: sha256("synthetic-headers"),
        final_body_sha256: sha256("synthetic-input"),
        candidate_ref: c.ref,
        auth: { type: "bearer" },
        credential_ciphertext_sha256: c.credential_ciphertext_sha256,
        billable_manifest_sha256: digest(["stg.internal.billable-manifest", "1", manifest]),
        deadline_at: deadline,
      };
      const prepId = crypto.randomUUID();
      const data = {
        context_id: (context as ExecutionContextResponse).context_id,
        dispatch_projection: projection,
        dispatched_digest: digest(["stg.internal.dispatch-projection", "1", projection]),
        manifest,
        prep_session_id: prepId,
        prep_binding_digest: digest([
          "stg.internal.prep-binding",
          "1",
          prepBindingV1({
            owner: (context as ExecutionContextResponse).owner,
            project_id: setup.project.id,
            projection: dispatchProjectionV1Schema.parse(projection),
            prep_session_id: prepId,
            host_config_sha256: digest(c.config),
          }),
        ]),
        project_id: setup.project.id,
        key_id: setup.key.id,
        intent_digest: sha256("intent"),
        fingerprint: sha256("synthetic-input"),
        customer_currency: "USD",
      };
      const command = crypto.randomUUID();
      const admission = (await client.execution("execution_admit_v1", data, {
        commandId: command,
        deadlineAt: deadline,
      })) as ExecutionReply;
      expect(admission.receipt).toMatchObject({
        command_state: "COMMITTED",
        request_id: request,
        attempt_id: attempt,
        customer_atoms: "100",
        supplier_atoms: "30",
        state: "RESERVED",
      });
      expect(admission.transient_send).toBeUndefined();
      const replay = await client.execution("execution_admit_v1", data, {
        commandId: command,
        deadlineAt: deadline,
      });
      expect(replay).toEqual(admission);
      const hold = (
        await db`SELECT estimated_atoms::text,state,estimated_quantities FROM upstream_holds WHERE attempt_id=${attempt}`
      )[0];
      expect(hold).toEqual({
        estimated_atoms: "30",
        state: "HELD",
        estimated_quantities: { input_tokens: "60", output_tokens: "30" },
      });
      expect(
        financialBalances(
          (await f.request(`/upstream-accounts/${setup.provider.account_id}/balances`)).value,
        )[0],
      ).toMatchObject({ balance_atoms: "1000", reserved_atoms: "30", available_atoms: "970" });
      const beginData = {
        attempt_id: attempt,
        permit_epoch: "1",
        authorization_plan_digest: admission.receipt.authorization_plan_digest,
        dispatched_digest: data.dispatched_digest,
        prep_session_id: data.prep_session_id,
        prep_binding_digest: data.prep_binding_digest,
      };
      const beginCommand = crypto.randomUUID();
      const begin = (await client.execution("execution_begin_v1", beginData, {
        commandId: beginCommand,
        deadlineAt: deadline,
      })) as ExecutionReply;
      expect(begin.receipt.state).toBe("SEND_GRANTED");
      expect(begin.transient_send!.auth_material).toEqual({
        type: "bearer",
        value: "synthetic-upstream-secret",
      });
      expect(
        (
          (await client.execution("execution_begin_v1", beginData, {
            commandId: beginCommand,
            deadlineAt: deadline,
          })) as ExecutionReply
        ).transient_send,
      ).toBeUndefined();
      const query = (await client.execution(
        "execution_query_v1",
        { kind: "execution_begin_v1", command_id: beginCommand },
        { deadlineAt: deadline },
      )) as ExecutionReply;
      expect(query.transient_send).toBeUndefined();
      expect(query.receipt.state).toBe("SEND_GRANTED");
      const all = JSON.stringify(await db`SELECT to_jsonb(c) value FROM execution_commands c`);
      expect(all).not.toContain("synthetic-upstream-secret");
      expect(all).not.toContain("transient_send");
    }),
  30000,
);

import { financialBalances, prepareFinancialAdmission } from "../support/financial-execution.ts";
import { RedisClient } from "bun";
async function quotaCounts(namespace: string) {
  const redis = new RedisClient(process.env.REDIS_URL!);
  try {
    await redis.connect();
    const keys = (await redis.send("KEYS", [`${namespace}:*:rpm`])) as string[];
    return await Promise.all(
      keys.map(async (key) => ({ key, count: Number(await redis.send("ZCARD", [key])) })),
    );
  } finally {
    redis.close();
  }
}

test(
  "same project intent and proposed UUID conflicts never create a second customer hold",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        a = f.client(),
        b = f.client(f.identity.b.port, "gatewayB");
      await a.startup();
      await b.startup();
      const intent = sha256("same-project-intent"),
        one = await prepareFinancialAdmission(setup, a, { intent }),
        two = await prepareFinancialAdmission(setup, b, { intent });
      const admitted = await one.admit();
      expect(admitted.receipt.command_state).toBe("COMMITTED");
      expect((await financialRejection(two.admit(), 409)).receipt).toMatchObject({
        command_state: "REJECTED",
        code: "already_accepted",
        request_id: one.requestId,
      });
      const sameId = await prepareFinancialAdmission(setup, b, {
        requestId: one.requestId,
        intent: sha256("new-intent"),
      });
      expect((await financialRejection(sameId.admit(), 409)).receipt).toMatchObject({
        command_state: "REJECTED",
        code: "proposed_id_conflict",
      });
      expect((await db`SELECT count(*)::integer n FROM billing_holds`)[0].n).toBe(1);
      expect((await db`SELECT count(*)::integer n FROM upstream_holds`)[0].n).toBe(1);
      await expect(
        a.execution(
          "execution_admit_v1",
          { ...one.data, customer_currency: "HKD" },
          { commandId: one.command, deadlineAt: one.deadline },
        ),
      ).rejects.toThrow("command_conflict");
      const lookup = (await b.execution(
        "execution_query_v1",
        { kind: "execution_admit_v1", command_id: one.command },
        { deadlineAt: one.deadline },
      )) as ExecutionReply;
      expect(lookup.receipt.command_state).toBe("NOT_FOUND");
    }),
  30000,
);

test(
  "retry retains UNKNOWN supplier exposure and shares one customer RPM and concurrency lease",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        client = f.client();
      await client.startup();
      const first = await prepareFinancialAdmission(setup, client),
        admission = await first.admit();
      await client.execution("execution_begin_v1", first.beginData(admission.receipt), {
        deadlineAt: first.deadline,
      });
      const next = await prepareFinancialAdmission(setup, client, {
        requestId: first.requestId,
        deadline: first.deadline,
      });
      const data = {
        context_id: next.data.context_id,
        dispatch_projection: next.data.dispatch_projection,
        dispatched_digest: next.data.dispatched_digest,
        manifest: next.data.manifest,
        prep_session_id: next.data.prep_session_id,
        prep_binding_digest: next.data.prep_binding_digest,
        request_id: first.requestId,
        previous_attempt_id: first.attemptId,
        expected_ordinal: 2,
        visible_output: false,
      };
      const second = (await client.execution("execution_next_v1", data, {
        deadlineAt: first.deadline,
      })) as ExecutionReply;
      expect(second.receipt).toMatchObject({
        command_state: "COMMITTED",
        attempt_id: next.attemptId,
        ordinal: 2,
      });
      expect(
        (await db`SELECT state FROM execution_permits WHERE attempt_id=${first.attemptId}`)[0]
          .state,
      ).toBe("UNKNOWN");
      expect((await db`SELECT count(*)::integer n FROM billing_holds`)[0].n).toBe(1);
      expect(
        (
          await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id} AND currency='USD'`
        )[0].reserved_atoms,
      ).toBe("60");
      const scopes =
        await db`SELECT scope_kind,count(*)::integer n FROM execution_quota_leases GROUP BY scope_kind ORDER BY scope_kind`;
      expect(scopes).toEqual([
        { scope_kind: "channel", n: 2 },
        { scope_kind: "key", n: 1 },
        { scope_kind: "project", n: 1 },
      ]);
      const counts = await quotaCounts(f.namespace);
      expect(counts.map((x) => x.count).sort()).toEqual([1, 1, 2]);
      const begun = (await client.execution("execution_begin_v1", next.beginData(second.receipt), {
        deadlineAt: first.deadline,
      })) as ExecutionReply;
      expect(begun.transient_send).toBeDefined();
      await client.execution(
        "execution_quota_v1",
        { attempt_id: next.attemptId, permit_epoch: "1", action: "CLOSE", reason: "COMPLETED" },
        { deadlineAt: first.deadline },
      );
      expect(
        (
          await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id} AND currency='USD'`
        )[0].reserved_atoms,
      ).toBe("60");
      expect(
        (await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`)[0]
          .reserved_atoms,
      ).toBe("100");
      expect(
        (await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE state='RELEASED'`)[0]
          .n,
      ).toBe(4);
      const thirdPrepared = await prepareFinancialAdmission(setup, client, {
        requestId: first.requestId,
        deadline: first.deadline,
      });
      const third = (await financialRejection(
        client.execution(
          "execution_next_v1",
          {
            context_id: thirdPrepared.data.context_id,
            dispatch_projection: thirdPrepared.data.dispatch_projection,
            dispatched_digest: thirdPrepared.data.dispatched_digest,
            manifest: thirdPrepared.data.manifest,
            prep_session_id: thirdPrepared.data.prep_session_id,
            prep_binding_digest: thirdPrepared.data.prep_binding_digest,
            request_id: first.requestId,
            previous_attempt_id: next.attemptId,
            expected_ordinal: 3,
            visible_output: false,
          },
          { deadlineAt: first.deadline },
        ),
        409,
      )) as ExecutionReply;
      expect(third.receipt).toMatchObject({ command_state: "REJECTED", code: "execution_closed" });
      expect((await db`SELECT count(*)::integer n FROM attempts`)[0].n).toBe(2);
    }),
  30000,
);

test(
  "parallel begin and abort has one winner and never returns two grants",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        client = f.client();
      await client.startup();
      const prepared = await prepareFinancialAdmission(setup, client),
        admitted = await prepared.admit();
      const outcomes = await Promise.allSettled([
        client.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
          deadlineAt: prepared.deadline,
        }),
        client.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
          deadlineAt: prepared.deadline,
        }),
        client.execution(
          "execution_abort_v1",
          {
            attempt_id: prepared.attemptId,
            permit_epoch: "1",
            reason: "CANCELLED",
            close_request: true,
          },
          { deadlineAt: prepared.deadline },
        ),
      ]);
      const values = outcomes.flatMap((x) =>
        x.status === "fulfilled" ? [x.value as ExecutionReply] : [],
      );
      expect(values.filter((x) => x.transient_send).length).toBeLessThanOrEqual(1);
      const state = (
        await db`SELECT state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`
      )[0].state;
      expect(["SEND_GRANTED", "ABORTED"]).toContain(state);
      const balance = (
        await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id} AND currency='USD'`
      )[0];
      expect(balance.reserved_atoms).toBe(state === "ABORTED" ? "0" : "30");
      if (state === "ABORTED") {
        expect(
          (await db`SELECT state FROM billing_holds WHERE request_id=${prepared.requestId}`)[0]
            .state,
        ).toBe("RELEASED");
        expect(
          (
            await db`SELECT execution_closed_at IS NOT NULL closed FROM gateway_requests WHERE id=${prepared.requestId}`
          )[0].closed,
        ).toBe(true);
      }
    }),
  30000,
);

test(
  "credential rotation and committed key revocation block old unbegun snapshots without releasing funds",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        client = f.client();
      await client.startup();
      const prepared = await prepareFinancialAdmission(setup, client),
        admitted = await prepared.admit();
      expect(
        (
          await f.request(
            `/providers/${setup.provider.id}`,
            { api_key: "rotated-synthetic-secret" },
            undefined,
            "PATCH",
          )
        ).status,
      ).toBe(200);
      const begin = (await financialRejection(
        client.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
          deadlineAt: prepared.deadline,
        }),
        409,
      )) as ExecutionReply;
      expect(begin.receipt).toMatchObject({ command_state: "REJECTED", code: "candidate_stale" });
      expect(begin.transient_send).toBeUndefined();
      await db`UPDATE api_keys SET enabled=false WHERE id=${setup.key.id}`;
      await expect(
        client.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
          deadlineAt: prepared.deadline,
        }),
      ).rejects.toThrow("customer_forbidden");
      expect(
        (
          await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id} AND currency='USD'`
        )[0].reserved_atoms,
      ).toBe("30");
    }),
  30000,
);

test(
  "failed admission audit and deferred supplier journal constraint leave no receipt or monetary effects",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        client = f.client();
      await client.startup();
      for (const failure of ["audit", "journal"]) {
        const prepared = await prepareFinancialAdmission(setup, client);
        if (failure === "audit")
          await db.unsafe(
            `CREATE FUNCTION financial_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='execution_admit_v1' THEN RAISE EXCEPTION 'synthetic audit failure';END IF;RETURN NEW;END $$;CREATE TRIGGER financial_failure BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION financial_failure()`,
          );
        else
          await db.unsafe(
            `CREATE FUNCTION financial_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.account='upstream:reserved' THEN NEW.atoms=NEW.atoms+1;END IF;RETURN NEW;END $$;CREATE TRIGGER financial_failure BEFORE INSERT ON upstream_entries FOR EACH ROW EXECUTE FUNCTION financial_failure()`,
          );
        await expect(prepared.admit()).rejects.toThrow();
        expect(
          (
            await db`SELECT count(*)::integer n FROM execution_commands WHERE command_id=${prepared.command}`
          )[0].n,
        ).toBe(0);
        expect(
          (
            await db`SELECT count(*)::integer n FROM gateway_requests WHERE id=${prepared.requestId}`
          )[0].n,
        ).toBe(0);
        expect(
          (
            await db`SELECT available_atoms::text,reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`
          )[0],
        ).toEqual({ available_atoms: "1000", reserved_atoms: "0" });
        expect(
          (
            await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id} AND currency='USD'`
          )[0].reserved_atoms,
        ).toBe("0");
        await db.unsafe(
          `DROP TRIGGER financial_failure ON ${failure === "audit" ? "audit_events" : "upstream_entries"};DROP FUNCTION financial_failure()`,
        );
      }
    }),
  30000,
);

import {
  financialRpcSession,
  financialGate,
  financialBlockedBy,
} from "../support/financial-execution.ts";

test(
  "two Core same-command loser has no cleanup authority over the winner active common leases",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        rpc = await financialRpcSession(f),
        prepared = await prepareFinancialAdmission(setup, rpc),
        gate = await financialGate(db);
      await db.unsafe(
        `CREATE FUNCTION admission_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='execution_admit_v1' THEN PERFORM pg_advisory_xact_lock(${gate.key}::bigint);END IF;RETURN NEW;END $$;CREATE TRIGGER admission_gate BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION admission_gate()`,
      );
      const winner = prepared.admit();
      try {
        await financialBlockedBy(db, gate.pid);
        const loser = await rpc.send(
          "execution_admit_v1",
          prepared.data,
          { commandId: prepared.command, deadlineAt: prepared.deadline },
          true,
        );
        expect(loser.status).toBe(503);
        expect(loser.value.command_state).toBe("UNKNOWN");
        expect(
          (
            await db`SELECT count(*)::integer n FROM execution_commands WHERE command_id=${prepared.command}`
          )[0].n,
        ).toBe(0);
      } finally {
        await gate.release();
      }
      const accepted = await winner;
      expect(accepted.receipt.state).toBe("RESERVED");
      const begin = (await rpc.execution(
        "execution_begin_v1",
        prepared.beginData(accepted.receipt),
        { deadlineAt: prepared.deadline },
      )) as ExecutionReply;
      expect(begin.transient_send).toBeDefined();
      await Bun.sleep(50);
      expect(
        (
          await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE request_id=${prepared.requestId} AND state='ACTIVE'`
        )[0].n,
      ).toBe(3);
      expect((await quotaCounts(f.namespace)).map((x) => x.count)).toEqual([1, 1, 1]);
    }),
  30000,
);

test(
  "per-attempt abort preserves common leases and the customer hold for the next physical attempt",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        client = f.client();
      await client.startup();
      const first = await prepareFinancialAdmission(setup, client);
      await first.admit();
      const abort = (await client.execution(
        "execution_abort_v1",
        { attempt_id: first.attemptId, permit_epoch: "1", reason: "PREPARE_DISPOSED" },
        { deadlineAt: first.deadline },
      )) as ExecutionReply;
      expect(abort.receipt.state).toBe("ABORTED");
      expect(
        (await db`SELECT state FROM billing_holds WHERE request_id=${first.requestId}`)[0].state,
      ).toBe("HELD");
      const next = await prepareFinancialAdmission(setup, client, {
        requestId: first.requestId,
        deadline: first.deadline,
      });
      const second = (await client.execution(
        "execution_next_v1",
        {
          context_id: next.data.context_id,
          dispatch_projection: next.data.dispatch_projection,
          dispatched_digest: next.data.dispatched_digest,
          manifest: next.data.manifest,
          prep_session_id: next.data.prep_session_id,
          prep_binding_digest: next.data.prep_binding_digest,
          request_id: first.requestId,
          previous_attempt_id: first.attemptId,
          expected_ordinal: 2,
          visible_output: false,
        },
        { deadlineAt: first.deadline },
      )) as ExecutionReply;
      expect(second.receipt.state).toBe("RESERVED");
      const begun = (await client.execution("execution_begin_v1", next.beginData(second.receipt), {
        deadlineAt: first.deadline,
      })) as ExecutionReply;
      expect(begun.transient_send).toBeDefined();
      expect((await quotaCounts(f.namespace)).map((x) => x.count).sort()).toEqual([1, 1, 2]);
      expect((await db`SELECT count(*)::integer n FROM billing_holds`)[0].n).toBe(1);
    }),
  30000,
);

test(
  "begin holds authorization through outer commit and sends no transient material before commit",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        client = f.client();
      await client.startup();
      const prepared = await prepareFinancialAdmission(setup, client),
        admitted = await prepared.admit(),
        gate = await financialGate(db);
      await db.unsafe(
        `CREATE FUNCTION begin_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='execution_begin_v1' THEN PERFORM pg_advisory_xact_lock(${gate.key}::bigint);END IF;RETURN NEW;END $$;CREATE TRIGGER begin_gate BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION begin_gate()`,
      );
      let completed = false;
      const begin = client
        .execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
          deadlineAt: prepared.deadline,
        })
        .finally(() => (completed = true));
      const beginPid = await financialBlockedBy(db, gate.pid);
      let revoked = false;
      const revoke = db
        .begin(async (tx) => {
          await tx`UPDATE api_keys SET enabled=false WHERE id=${setup.key.id}`;
        })
        .then(() => (revoked = true));
      try {
        await financialBlockedBy(db, beginPid);
        expect(completed).toBe(false);
        expect(revoked).toBe(false);
        expect(
          (await db`SELECT state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`)[0]
            .state,
        ).toBe("RESERVED");
      } finally {
        await gate.release();
      }
      const result = (await begin) as ExecutionReply;
      expect(result.transient_send).toBeDefined();
      await revoke;
      expect(revoked).toBe(true);
    }),
  30000,
);

test(
  "lost begin response stays committed and a new startup session can only query safe status",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        rpc = await financialRpcSession(f, { timeoutMs: 100 }),
        prepared = await prepareFinancialAdmission(setup, rpc),
        admitted = await prepared.admit(),
        gate = await financialGate(db),
        command = crypto.randomUUID();
      await db.unsafe(
        `CREATE FUNCTION lost_begin_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='execution_begin_v1' THEN PERFORM pg_advisory_xact_lock(${gate.key}::bigint);END IF;RETURN NEW;END $$;CREATE TRIGGER lost_begin_gate BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION lost_begin_gate()`,
      );
      const lost = rpc.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
        commandId: command,
        deadlineAt: prepared.deadline,
      });
      try {
        await financialBlockedBy(db, gate.pid);
        await expect(lost).rejects.toThrow();
      } finally {
        await gate.release();
      }
      const limit = Date.now() + 2000;
      while (
        Date.now() < limit &&
        (await db`SELECT state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`)[0]
          .state === "RESERVED"
      )
        await Bun.sleep(5);
      expect(
        (await db`SELECT state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`)[0]
          .state,
      ).toBe("SEND_GRANTED");
      const replacement = f.client();
      await replacement.startup();
      const query = (await replacement.execution(
        "execution_query_v1",
        { kind: "execution_begin_v1", command_id: command },
        { deadlineAt: prepared.deadline },
      )) as ExecutionReply;
      expect(query.receipt.state).toBe("SEND_GRANTED");
      expect(query.transient_send).toBeUndefined();
      const forbidden = await replacement
        .execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
          deadlineAt: prepared.deadline,
        })
        .catch((e) => String(e));
      expect(forbidden).toContain("internal_owner_mismatch");
      expect(
        (
          await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id} AND currency='USD'`
        )[0].reserved_atoms,
      ).toBe("30");
    }),
  30000,
);

test(
  "logical CLOSE committed first blocks begin and next while its supplier and customer holds stay intact",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        client = f.client();
      await client.startup();
      const prepared = await prepareFinancialAdmission(setup, client),
        admitted = await prepared.admit(),
        close = crypto.randomUUID(),
        closeData = {
          attempt_id: prepared.attemptId,
          permit_epoch: "1",
          action: "CLOSE",
          reason: "CANCELLED",
        };
      const closed = await client.execution("execution_quota_v1", closeData, {
        commandId: close,
        deadlineAt: prepared.deadline,
      });
      expect(
        await client.execution("execution_quota_v1", closeData, {
          commandId: close,
          deadlineAt: prepared.deadline,
        }),
      ).toEqual(closed);
      const conflict = await client
        .execution(
          "execution_quota_v1",
          { ...closeData, reason: "DRAINED" },
          { commandId: close, deadlineAt: prepared.deadline },
        )
        .catch((error) => error as Error);
      expect(conflict).toBeInstanceOf(Error);
      expect((conflict as Error).message).toBe("command_conflict");
      const begin = (await financialRejection(
        client.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
          deadlineAt: prepared.deadline,
        }),
        422,
      )) as ExecutionReply;
      expect(begin.receipt).toMatchObject({
        command_state: "REJECTED",
        code: "begin_precondition",
      });
      expect(begin.transient_send).toBeUndefined();
      expect(
        (await db`SELECT state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`)[0]
          .state,
      ).toBe("RESERVED");
      expect(
        (await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`)[0]
          .reserved_atoms,
      ).toBe("100");
      expect(
        (
          await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
        )[0].reserved_atoms,
      ).toBe("30");
    }),
  30000,
);

test(
  "begin locked first forces concurrent CLOSE to retry then closes quota without releasing UNKNOWN money",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        client = f.client();
      await client.startup();
      const prepared = await prepareFinancialAdmission(setup, client),
        admitted = await prepared.admit(),
        gate = await financialGate(db);
      await db.unsafe(
        `CREATE FUNCTION close_race_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='execution_begin_v1' THEN PERFORM pg_advisory_xact_lock(${gate.key}::bigint);END IF;RETURN NEW;END $$;CREATE TRIGGER close_race_gate BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION close_race_gate()`,
      );
      const begin = client.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
        deadlineAt: prepared.deadline,
      });
      try {
        await financialBlockedBy(db, gate.pid);
        await expect(
          client.execution(
            "execution_quota_v1",
            {
              attempt_id: prepared.attemptId,
              permit_epoch: "1",
              action: "CLOSE",
              reason: "DRAINED",
            },
            { deadlineAt: prepared.deadline },
          ),
        ).rejects.toThrow("identity_busy");
      } finally {
        await gate.release();
      }
      expect(((await begin) as ExecutionReply).transient_send).toBeDefined();
      await client.execution(
        "execution_quota_v1",
        { attempt_id: prepared.attemptId, permit_epoch: "1", action: "CLOSE", reason: "DRAINED" },
        { deadlineAt: prepared.deadline },
      );
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

test(
  "a busy wallet group leaves no command result while an independent wallet proceeds in the same Core",
  () =>
    withFinancial(async (f, db) => {
      const a = await f.bootstrap(),
        b = await f.bootstrap(),
        client = f.client();
      await client.startup();
      const first = await prepareFinancialAdmission(a, client),
        second = await prepareFinancialAdmission(b, client);
      let release!: () => void, ready!: () => void;
      const gate = new Promise<void>((r) => (release = r)),
        locked = new Promise<void>((r) => (ready = r)),
        blocker = db.begin(async (tx) => {
          await tx`SELECT id FROM wallets WHERE project_id=${a.project.id} FOR NO KEY UPDATE`;
          ready();
          await gate;
        });
      await locked;
      let outcomes: PromiseSettledResult<ExecutionReply>[];
      try {
        outcomes = await Promise.allSettled([first.admit(), second.admit()]);
      } finally {
        release();
        await blocker;
      }
      expect(outcomes[0]!.status).toBe("rejected");
      expect(outcomes[1]!.status).toBe("fulfilled");
      expect(outcomes[1]!.status === "fulfilled" && outcomes[1]!.value.receipt.state).toBe(
        "RESERVED",
      );
      expect(
        (
          await db`SELECT count(*)::integer n FROM execution_commands WHERE command_id=${first.command}`
        )[0].n,
      ).toBe(0);
      expect((await db`SELECT count(*)::integer n FROM billing_holds`)[0].n).toBe(1);
    }),
  30000,
);

test(
  "strict projection unknown-field canaries reject at the real authenticated Core and never persist",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        rpc = await financialRpcSession(f),
        prepared = await prepareFinancialAdmission(setup, rpc),
        canary = "raw-target-header-body-canary";
      const response = await rpc.send(
        "execution_admit_v1",
        {
          ...prepared.data,
          dispatch_projection: {
            ...prepared.data.dispatch_projection,
            request_target: canary,
            headers: { "x-canary": canary },
            body: canary,
          },
        },
        { commandId: prepared.command, deadlineAt: prepared.deadline },
        false,
        true,
      );
      expect(response.status).toBe(400);
      expect(response.raw).not.toContain(canary);
      expect(
        (
          await db`SELECT count(*)::integer n FROM execution_commands WHERE command_id=${prepared.command}`
        )[0].n,
      ).toBe(0);
      expect(
        JSON.stringify(await db`SELECT to_jsonb(c) value FROM execution_contexts c`),
      ).not.toContain(canary);
    }),
  30000,
);
