import { expect, test } from "bun:test";
import type { SQL } from "bun";
import type { CoreVerifiedCallerProjection, ExecutionKind } from "@gateway/contracts";
import { createInternalIdentityStore } from "@gateway/storage/internal-identity";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime/crypto";
import {
  withFinancial,
  financialRpcSession,
  prepareFinancialAdmission,
} from "../support/financial-execution.ts";
import { supplier } from "../support/core-settlement.ts";
import { approvePluginRuntime } from "../../scripts/runtime-approval.ts";
import { catalogHelper } from "../support/catalog.ts";

type Fixture = Parameters<Parameters<typeof withFinancial>[0]>[0];
type Setup = Awaited<ReturnType<Fixture["bootstrap"]>>;

async function batchFixture(f: Fixture, db: SQL) {
  const rpc = await financialRpcSession(f);
  const directory = createInternalIdentityStore(db);
  const session = (await directory.readStartupSession(rpc.identity.receipt.session_id))!;
  const peer = (await directory.readPeerDirectory(session.certificate_sha256))!;
  async function prepare(setup: Setup, attemptId?: string) {
    const request = await prepareFinancialAdmission(setup, rpc, attemptId ? { attemptId } : {});
    const admission = await request.admit();
    await rpc.execution("execution_quota_v1", {
      action: "ADOPT",
      attempt_id: request.attemptId,
      permit_epoch: "1",
    });
    const leases = await db<
      { lease_id: string; revision: string; descriptor: { remaining_ms: number } }[]
    >`SELECT lease_id,revision::text revision,descriptor FROM execution_quota_leases WHERE request_id=${request.requestId} ORDER BY lease_id`;
    const remaining = Math.min(...leases.map((row) => Number(row.descriptor.remaining_ms)));
    expect(remaining).toBeGreaterThan(5000);
    function item(
      kind: ExecutionKind = "execution_begin_v1",
      data: unknown = request.beginData(admission.receipt),
      command = crypto.randomUUID(),
      proofRemaining = 5000,
    ) {
      const projection: CoreVerifiedCallerProjection = {
        service: session.service,
        instance_id: session.instance_id,
        certificate_sha256: session.certificate_sha256,
        instance_revision: peer.instance_revision,
        certificate_revision: peer.certificate_revision,
        session_id: session.session_id,
        incarnation_id: session.incarnation_id,
        key_sha256: session.public_key_sha256,
        session_revision: session.session_revision,
        command_type: kind,
        command_id: command,
        raw_body_sha256: sha256("independent BEGIN batch fixture"),
        method: "POST",
        normalized_path: `/_internal/core/v1/${kind === "execution_begin_v1" ? "begin" : "abort"}`,
        deadline_at: request.deadline,
        trace_id: "begin-batch",
      };
      return {
        projection,
        envelope: {
          command_id: command,
          caller_incarnation: projection.incarnation_id,
          deadline_at: projection.deadline_at,
          trace_id: projection.trace_id,
          contract_version: 1,
          body_digest: sha256(
            canonical({
              kind,
              contract_version: 1,
              owner: {
                service: projection.service,
                instance_id: projection.instance_id,
                session_id: projection.session_id,
                incarnation_id: projection.incarnation_id,
              },
              deadline_at: projection.deadline_at,
              data,
            }),
          ),
        },
        data,
        computed: {
          proofs: leases.map(({ lease_id, revision }) => ({ lease_id, revision })),
          proof_remaining_ms: proofRemaining,
        },
      };
    }
    return { request, item };
  }
  return { prepare };
}

test("BEGIN batches write permits, commands and audits once while replay never grants a second send", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const batch = await batchFixture(f, db);
    const first = await batch.prepare(setup);
    const second = await batch.prepare(setup);
    const items = [first.item(), second.item()];
    await db.unsafe(`
      CREATE TABLE public.begin_statement_trace(target text,rows integer);
      CREATE FUNCTION public.begin_statement_trace() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
      BEGIN INSERT INTO public.begin_statement_trace SELECT TG_TABLE_NAME,count(*) FROM newly_written;RETURN NULL;END $$;
      CREATE TRIGGER begin_statement_trace AFTER UPDATE ON public.execution_permits REFERENCING NEW TABLE AS newly_written FOR EACH STATEMENT EXECUTE FUNCTION public.begin_statement_trace();
      CREATE TRIGGER begin_statement_trace AFTER INSERT ON public.execution_commands REFERENCING NEW TABLE AS newly_written FOR EACH STATEMENT EXECUTE FUNCTION public.begin_statement_trace();
      CREATE TRIGGER begin_statement_trace AFTER INSERT ON public.audit_events REFERENCING NEW TABLE AS newly_written FOR EACH STATEMENT EXECUTE FUNCTION public.begin_statement_trace();
    `);
    const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
    for (const [index, result] of results.entries()) {
      expect(result).toMatchObject({
        fresh_transition: true,
        receipt: { state: "SEND_GRANTED", attempt_id: [first, second][index]!.request.attemptId },
      });
      expect(result.credential_ciphertext).toBeString();
    }
    expect(await db`SELECT target,rows FROM begin_statement_trace ORDER BY target`).toEqual([
      { target: "audit_events", rows: 2 },
      { target: "execution_commands", rows: 2 },
      { target: "execution_permits", rows: 2 },
    ]);
    const replayed = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
    for (const result of replayed) {
      expect(result.fresh_transition).not.toBe(true);
      expect(result).not.toHaveProperty("credential_ciphertext");
    }
    expect(await db`SELECT count(*)::integer n FROM begin_statement_trace`).toEqual([{ n: 3 }]);
    expect(
      await db`SELECT reserved_atoms::text reserved,spent_atoms::text spent FROM wallets`,
    ).toEqual([{ reserved: "200", spent: "0" }]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "1000",
      reserved: "60",
      spent: "0",
      state: "ACTIVE",
    });
  }));

test("BEGIN activates all three provisional leases together before returning its one send grant", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const client = f.client();
    await client.startup();
    const request = await prepareFinancialAdmission(setup, client);
    const admission = await request.admit();
    await db.unsafe(`
      CREATE TABLE quota_activation_rows(rows int);
      CREATE FUNCTION trace_quota_activation() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
      BEGIN INSERT INTO public.quota_activation_rows SELECT count(*) FROM touched WHERE state='ACTIVE';RETURN NULL;END $$;
      CREATE TRIGGER trace_quota_activation AFTER UPDATE ON execution_quota_leases REFERENCING NEW TABLE AS touched FOR EACH STATEMENT EXECUTE FUNCTION trace_quota_activation();
    `);
    const grant = await client.execution(
      "execution_begin_v1",
      request.beginData(admission.receipt),
    );
    expect(grant).toMatchObject({ receipt: { state: "SEND_GRANTED" } });
    expect(await db`SELECT rows FROM quota_activation_rows WHERE rows>0`).toEqual([{ rows: 3 }]);
    expect(
      await db`SELECT state,revision::text revision FROM execution_quota_leases WHERE request_id=${request.requestId} ORDER BY scope_kind`,
    ).toEqual([
      { state: "ACTIVE", revision: "1" },
      { state: "ACTIVE", revision: "1" },
      { state: "ACTIVE", revision: "1" },
    ]);
  }));

test("an invalid proof lifetime or lease revision cannot activate or corrupt any quota lease", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const batch = await batchFixture(f, db);
    const { item, request } = await batch.prepare(setup);
    const snapshot = () =>
      db<
        { lease_id: string; state: string; revision: string; descriptor: Record<string, unknown> }[]
      >`SELECT lease_id,state,revision::text revision,descriptor FROM execution_quota_leases WHERE request_id=${request.requestId} ORDER BY lease_id`;
    const before = await snapshot();
    const projection = item().projection;
    for (const invalid of [
      { remaining_ms: null },
      { remaining_ms: "NaN" },
      { revision: "NaN" },
      { revision: "1.5" },
    ]) {
      const descriptors = before.map((row, index) => ({
        ...row.descriptor,
        ...(index === 1 ? invalid : {}),
      }));
      await expect(
        db`SELECT core_quota_v1(${projection}::jsonb,${request.attemptId}::uuid,'ACTIVE',${descriptors}::jsonb)`,
      ).rejects.toMatchObject({ errno: "revision" in invalid ? "P0403" : "P0409" });
      expect(await snapshot()).toEqual(before);
    }
  }));

test("a balance write cannot stall an already reserved send, while a concurrent wallet freeze fences it", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const batch = await batchFixture(f, db);
    const first = await batch.prepare(setup);
    const second = await batch.prepare(setup);
    await db.begin(async (tx) => {
      await tx`UPDATE wallets SET available_atoms=available_atoms WHERE project_id=${setup.project.id}`;
      const [row] = await db`SELECT core_execution_batch_v1(${[first.item()]}::jsonb) decisions`;
      expect(row.decisions[0]).toMatchObject({
        fresh_transition: true,
        receipt: { state: "SEND_GRANTED" },
      });
    });
    const last = await batch.prepare(setup);
    await db.begin(async (grant) => {
      const [row] =
        await grant`SELECT core_execution_batch_v1(${[second.item()]}::jsonb) decisions`;
      expect(row.decisions[0].fresh_transition).toBe(true);
      const freeze = await db
        .begin(async (tx) => {
          await tx`SET LOCAL lock_timeout='150ms'`;
          await tx`UPDATE wallets SET manual_frozen=true WHERE project_id=${setup.project.id}`;
        })
        .catch((error: unknown) => error);
      expect(freeze).toMatchObject({ errno: "55P03" });
    });
    await db.begin(async (tx) => {
      await tx`UPDATE wallets SET manual_frozen=true WHERE project_id=${setup.project.id}`;
      const [row] = await db`SELECT core_execution_batch_v1(${[last.item()]}::jsonb) decisions`;
      expect(row.decisions[0]).toEqual({ error: "identity_busy" });
    });
    const [row] = await db`SELECT core_execution_batch_v1(${[last.item()]}::jsonb) decisions`;
    expect(row.decisions[0]).toMatchObject({
      receipt: { command_state: "REJECTED", code: "begin_precondition" },
    });
    expect(
      await db`SELECT state FROM execution_permits WHERE attempt_id=${last.request.attemptId}`,
    ).toEqual([{ state: "RESERVED" }]);
  }));

test("case variants of an attempt UUID cannot receive two fresh BEGIN grants in one batch", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const batch = await batchFixture(f, db);
    const prepared = await batch.prepare(setup, "abcdefab-cdef-4abc-8def-abcdefabcdef");
    const first = prepared.item();
    const second = prepared.item("execution_begin_v1", {
      ...(first.data as Record<string, unknown>),
      attempt_id: prepared.request.attemptId.toUpperCase(),
    });
    const results = (await db`SELECT core_execution_batch_v1(${[first, second]}::jsonb) value`)[0]
      .value;
    expect(results[0]).toMatchObject({ fresh_transition: true });
    expect(results[1]).toMatchObject({
      receipt: { command_state: "REJECTED", code: "permit_not_reserved" },
    });
    expect(results[1].fresh_transition).not.toBe(true);
    expect(results[1]).not.toHaveProperty("credential_ciphertext");
    expect(
      await db`SELECT count(*)::integer n FROM audit_events WHERE action='execution_begin_v1'`,
    ).toEqual([{ n: 1 }]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "1000",
      reserved: "30",
      spent: "0",
      state: "ACTIVE",
    });
  }));

test("a late BEGIN command failure preserves mixed-item order and retries only the ungranted attempt", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const batch = await batchFixture(f, db);
    const first = await batch.prepare(setup);
    const second = await batch.prepare(setup);
    const third = await batch.prepare(setup);
    const failed = second.item();
    const items = [
      first.item(),
      first.item("execution_abort_v1", {
        attempt_id: first.request.attemptId,
        permit_epoch: "1",
        close_request: false,
      }),
      failed,
      third.item(),
    ];
    await db.unsafe(`
      CREATE FUNCTION public.begin_command_failure() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
      BEGIN IF NEW.command_id='${failed.envelope.command_id}'::uuid THEN RAISE EXCEPTION 'synthetic BEGIN command failure' USING ERRCODE='P0B99';END IF;RETURN NEW;END $$;
      CREATE TRIGGER begin_command_failure BEFORE INSERT ON public.execution_commands FOR EACH ROW EXECUTE FUNCTION public.begin_command_failure();
    `);
    const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
    expect(results[0]).toMatchObject({ fresh_transition: true });
    expect(results[1]).toMatchObject({
      receipt: { command_state: "REJECTED", code: "permit_not_reserved" },
    });
    expect(results[2]).toEqual({ error: "identity_store_unavailable" });
    expect(results[3]).toMatchObject({ fresh_transition: true });
    expect(
      await db`SELECT state FROM execution_permits WHERE attempt_id=${second.request.attemptId}`,
    ).toEqual([{ state: "RESERVED" }]);
    expect(
      await db`SELECT count(*)::integer n FROM execution_commands WHERE kind='execution_begin_v1'`,
    ).toEqual([{ n: 2 }]);
    await db.unsafe("DROP TRIGGER begin_command_failure ON public.execution_commands");
    const retried = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
    expect(retried[0].fresh_transition).not.toBe(true);
    expect(retried[2]).toMatchObject({ fresh_transition: true });
    expect(retried[3].fresh_transition).not.toBe(true);
    expect(
      await db`SELECT count(*)::integer n FROM audit_events WHERE action='execution_begin_v1'`,
    ).toEqual([{ n: 3 }]);
    expect(await supplier(db, setup.provider.account_id)).toEqual({
      balance: "1000",
      reserved: "90",
      spent: "0",
      state: "ACTIVE",
    });
  }));

test.each(["key", "proof"] as const)(
  "BEGIN flush cannot renew a %s authority that expires during the batch",
  (mode) =>
    withFinancial(async (f, db) => {
      const firstSetup = await f.bootstrap();
      const secondSetup = await f.bootstrap();
      const batch = await batchFixture(f, db);
      const first = await batch.prepare(firstSetup);
      const second = await batch.prepare(secondSetup);
      const items = [
        first.item(undefined, undefined, undefined, mode === "proof" ? 1100 : 5000),
        second.item(),
      ];
      await db`ALTER FUNCTION public.core_digest_v1(jsonb) RENAME TO begin_original_digest_v1`;
      await db.unsafe(`
      CREATE FUNCTION public.core_digest_v1(v jsonb) RETURNS text LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
      BEGIN IF v->>'kind'='execution_begin_v1' AND v->'data'->>'attempt_id'='${second.request.attemptId}' THEN PERFORM pg_sleep(0.3);END IF;RETURN public.begin_original_digest_v1(v);END $$;
    `);
      if (mode === "key")
        await db`UPDATE api_keys SET expires_at=clock_timestamp()+interval '0.2 seconds' WHERE id=${firstSetup.key.id}`;
      const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      if (mode === "key") expect(results[0]).toEqual({ error: "customer_forbidden" });
      else
        expect(results[0]).toMatchObject({
          receipt: { command_state: "REJECTED", code: "begin_precondition" },
        });
      expect(results[1]).toMatchObject({ fresh_transition: true });
      expect(
        await db`SELECT state,granted_at FROM execution_permits WHERE attempt_id=${first.request.attemptId}`,
      ).toEqual([{ state: "RESERVED", granted_at: null }]);
      expect(await supplier(db, firstSetup.provider.account_id)).toEqual({
        balance: "1000",
        reserved: "30",
        spent: "0",
        state: "ACTIVE",
      });
    }),
);

test(
  "BEGIN flush cannot grant a Wasm build after its explicit approval expires during the batch",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap();
      const batch = await batchFixture(f, db);
      const first = await batch.prepare(setup);
      const second = await batch.prepare(setup);
      const items = [first.item(), second.item()];
      const packageDigest = first.request.candidate.ref.package_digest;
      const approved = await approvePluginRuntime(db, {
        packageDigest,
        kind: "wasm",
        policyFile: f.catalog.policyPath,
        helperExecutable: catalogHelper,
        actor: "integration-owner",
        reason: "BEGIN deferred approval expiry",
      });
      const [before] =
        await db`SELECT facts FROM core_execution_runtime.approvals WHERE package_digest=${packageDigest} AND kind='wasm' AND artifact_digest=${approved.execution.artifact_digest}`;
      await db`ALTER FUNCTION public.core_digest_v1(jsonb) RENAME TO begin_original_digest_v1`;
      await db.unsafe(`
      CREATE FUNCTION public.core_digest_v1(v jsonb) RETURNS text LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
      BEGIN IF v->>'kind'='execution_begin_v1' AND v->'data'->>'attempt_id'='${second.request.attemptId}' THEN PERFORM pg_sleep(0.3);END IF;RETURN public.begin_original_digest_v1(v);END $$;
    `);
      const facts = { ...before.facts, trust_until: new Date(Date.now() + 200).toISOString() };
      await db`SELECT core_execution_runtime.approve_v1(${packageDigest},${approved.execution}::jsonb,${facts}::jsonb,true,'integration-owner','Shorter verified trust boundary')`;
      const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      for (const result of results) {
        expect(result).toMatchObject({
          receipt: { command_state: "REJECTED", code: "candidate_stale" },
        });
        expect(result.fresh_transition).not.toBe(true);
        expect(result).not.toHaveProperty("credential_ciphertext");
      }
      expect(await db`SELECT state,granted_at FROM execution_permits ORDER BY attempt_id`).toEqual([
        { state: "RESERVED", granted_at: null },
        { state: "RESERVED", granted_at: null },
      ]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "1000",
        reserved: "60",
        spent: "0",
        state: "ACTIVE",
      });
    }),
  30000,
);
