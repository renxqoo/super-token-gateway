import { expect, test } from "bun:test";
import { SQL } from "bun";
import type { CoreVerifiedCallerProjection } from "@gateway/contracts";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime/crypto";
import { createInternalIdentityStore } from "@gateway/storage/internal-identity";
import { currentCommand, registeredIdentity, withIdentity } from "../support/internal-identity.ts";

type Fixture = Parameters<Parameters<typeof withIdentity>[0]>[0];

async function queryItems(fixture: Fixture, db: SQL) {
  const client = await fixture.connection();
  const identity = await registeredIdentity(client, fixture.a.port);
  const command = currentCommand(identity);
  const store = createInternalIdentityStore(db);
  const peer = (await store.readPeerDirectory(identity.receipt.certificate_sha256))!;
  const session = (await store.readStartupSession(identity.receipt.session_id))!;
  const projection: CoreVerifiedCallerProjection = {
    service: session.service,
    instance_id: session.instance_id,
    certificate_sha256: session.certificate_sha256,
    instance_revision: peer.instance_revision,
    certificate_revision: peer.certificate_revision,
    raw_body_sha256: command.proof.raw_body_sha256,
    session_id: session.session_id,
    incarnation_id: session.incarnation_id,
    key_sha256: session.public_key_sha256,
    session_revision: session.session_revision,
    command_type: "execution_query_v1",
    command_id: crypto.randomUUID(),
    method: "POST",
    normalized_path: "/_internal/core/v1/commands",
    deadline_at: new Date(Date.now() + 20000).toISOString(),
    trace_id: "batch-identity",
  };
  const data = { kind: "execution_begin_v1", command_id: crypto.randomUUID() };
  function item(value: unknown = data, changes: Partial<CoreVerifiedCallerProjection> = {}) {
    const p = { ...projection, command_id: crypto.randomUUID(), ...changes };
    const e = {
      command_id: p.command_id,
      caller_incarnation: p.incarnation_id,
      deadline_at: p.deadline_at,
      trace_id: p.trace_id,
      contract_version: 1,
      body_digest: sha256(
        canonical({
          kind: p.command_type,
          contract_version: 1,
          owner: {
            service: p.service,
            instance_id: p.instance_id,
            session_id: p.session_id,
            incarnation_id: p.incarnation_id,
          },
          deadline_at: p.deadline_at,
          data: value,
        }),
      ),
    };
    return { projection: p, envelope: e, data: value, computed: {} };
  }
  return { session, item, data };
}

test("stale revisions and mismatched authority fields cannot poison valid peers of the same session in one batch", () =>
  withIdentity(async (fixture, db) => {
    const prepared = await queryItems(fixture, db);
    const cases: Array<[Partial<CoreVerifiedCallerProjection>, string]> = [
      [{ session_revision: "2" }, "internal_session_invalid"],
      [{ instance_revision: "2" }, "internal_peer_forbidden"],
      [{ certificate_revision: "2" }, "internal_peer_forbidden"],
      [{ key_sha256: sha256("another-key") }, "internal_session_invalid"],
      [{ incarnation_id: crypto.randomUUID() }, "internal_session_invalid"],
      [{ certificate_sha256: sha256("another-certificate") }, "internal_peer_forbidden"],
      [{ instance_id: "another-instance" }, "internal_peer_forbidden"],
    ];
    const items = cases.flatMap(([changes]) => [
      prepared.item(prepared.data, changes),
      prepared.item(),
    ]);
    const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
    expect(results).toEqual(
      cases.flatMap(([, error]) => [{ error }, { receipt: { command_state: "NOT_FOUND" } }]),
    );
    expect((await db`SELECT count(*)::int n FROM execution_commands`)[0].n).toBe(0);
  }));

test("a failed item cannot release cached identity SHARE locks before the outer batch commits", () =>
  withIdentity(async (fixture, db) => {
    const prepared = await queryItems(fixture, db);
    const ready = Promise.withResolvers<void>(),
      release = Promise.withResolvers<void>();
    const transaction = db.begin(async (tx) => {
      const items = [
        prepared.item({ kind: "execution_begin_v1", command_id: "bad" }),
        prepared.item(),
      ];
      const results = (await tx`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      expect(results).toEqual([
        { error: "identity_store_unavailable" },
        { receipt: { command_state: "NOT_FOUND" } },
      ]);
      ready.resolve();
      await release.promise;
    });
    await Promise.race([ready.promise, transaction]);
    let done = false;
    const writer = Promise.resolve(
      db`SELECT internal_identity_change_v1('gateway','gatewayA',NULL,${prepared.session.session_id}::uuid,'DISABLE')`,
    ).then(() => {
      done = true;
    });
    try {
      let blocked = false;
      for (let n = 0; n < 100; n++) {
        const rows =
          await db`SELECT wait_event_type FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query LIKE 'SELECT internal_identity_change_v1%'`;
        if (rows.some((row: { wait_event_type: string }) => row.wait_event_type === "Lock")) {
          blocked = true;
          break;
        }
        await Bun.sleep(5);
      }
      expect(blocked).toBe(true);
      expect(done).toBe(false);
    } finally {
      release.resolve();
      await Promise.all([transaction, writer]);
    }
  }));

test("revocation committed before the batch denies every item without writing command receipts", () =>
  withIdentity(async (fixture, db) => {
    const prepared = await queryItems(fixture, db);
    await db`SELECT internal_identity_change_v1('gateway','gatewayA',NULL,${prepared.session.session_id}::uuid,'SECURITY_REVOKE')`;
    const items = [prepared.item(), prepared.item(), prepared.item()];
    const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
    expect(results).toEqual([
      { error: "internal_identity_revoked" },
      { error: "internal_identity_revoked" },
      { error: "internal_identity_revoked" },
    ]);
    expect((await db`SELECT count(*)::int n FROM execution_commands`)[0].n).toBe(0);
  }));

test("batch identity validation preserves the 128 item boundary and ordered independent results", () =>
  withIdentity(async (fixture, db) => {
    const prepared = await queryItems(fixture, db);
    const items = Array.from({ length: 128 }, (_, n) =>
      prepared.item(prepared.data, n % 2 ? {} : { session_revision: "2" }),
    );
    const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
    expect(results).toEqual(
      Array.from({ length: 128 }, (_, n) =>
        n % 2 ? { receipt: { command_state: "NOT_FOUND" } } : { error: "internal_session_invalid" },
      ),
    );
    await expect(
      db`SELECT core_execution_batch_v1(${[...items, prepared.item()]}::jsonb) value`,
    ).rejects.toMatchObject({ message: "internal_request_invalid" });
    expect((await db`SELECT count(*)::int n FROM execution_commands`)[0].n).toBe(0);
  }));

test("a later item's deadline expiring during a peer's work is rechecked instead of inherited from the authority representative", () =>
  withIdentity(async (fixture, db) => {
    const prepared = await queryItems(fixture, db);
    await db`ALTER FUNCTION public.core_digest_v1(jsonb) RENAME TO identity_deadline_digest_v1`;
    await db.unsafe(
      `CREATE FUNCTION public.core_digest_v1(v jsonb) RETURNS text LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$ BEGIN IF v->'data'->>'delay_probe'='yes' THEN PERFORM pg_sleep(0.25);END IF;RETURN public.identity_deadline_digest_v1(v);END $$`,
    );
    const items = [
      prepared.item({ ...prepared.data, delay_probe: "yes" }),
      prepared.item(prepared.data, { deadline_at: new Date(Date.now() + 150).toISOString() }),
    ];
    const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
    expect(results).toEqual([
      { receipt: { command_state: "NOT_FOUND" } },
      { error: "internal_deadline" },
    ]);
  }));

test("runtime cannot forge cached authority times by calling private batch kernels", () =>
  withIdentity(async (fixture, db) => {
    const role = new SQL(fixture.apiUrl);
    try {
      for (const query of [
        () =>
          role`SELECT core_execution_item_v2('{}'::jsonb,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb,'{"session_expires_at":"infinity"}'::jsonb)`,
        () => role`SELECT core_caller_projection_v2('{}'::jsonb)`,
      ])
        await expect(query()).rejects.toMatchObject({ errno: "42501" });
      expect(
        (
          await db`SELECT to_regprocedure('public.core_execution_v1(jsonb,jsonb,jsonb,jsonb)') value`
        )[0].value,
      ).toBeNull();
    } finally {
      await role.close();
    }
  }));
