import { expect, test } from "bun:test";
import { SQL } from "bun";
import { randomBytes, randomUUID } from "node:crypto";
import { createInternalIdentityStore } from "@gateway/storage/internal-identity";
import { sha256 } from "@gateway/runtime/crypto";
import {
  withIdentity,
  registeredIdentity,
  currentCommand,
  startupIdentity,
  challengeIdentity,
  identityAudience,
} from "../support/internal-identity.ts";

async function state(db: SQL) {
  return (
    await db`SELECT jsonb_build_object(
    'instances',(SELECT jsonb_agg(to_jsonb(i) ORDER BY service,instance_id) FROM internal_service_instances i),
    'certificates',(SELECT jsonb_agg(to_jsonb(c) ORDER BY certificate_sha256) FROM internal_service_certificates c),
    'registrations',(SELECT jsonb_agg(to_jsonb(r) ORDER BY challenge_id) FROM internal_startup_registrations r),
    'sessions',(SELECT jsonb_agg(to_jsonb(s) ORDER BY session_id) FROM internal_startup_sessions s),
    'audit',(SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM audit_events a)) AS value`
  )[0].value;
}
async function rejectUnchanged(
  db: SQL,
  failures: string[],
  label: string,
  run: () => Promise<unknown>,
) {
  const before = await state(db);
  try {
    await run();
    failures.push(`${label}: accepted`);
  } catch (error) {
    const e = error as Error & { errno: string };
    expect(e.errno).toMatch(/^P04[01389][01389]$/);
    expect(e.message).toMatch(
      /^internal_(request_invalid|peer_forbidden|session_invalid|proof_invalid|deadline|owner_mismatch)$|^registration_conflict$/,
    );
    expect(e.message).not.toContain("input-canary");
  }
  expect(await state(db)).toEqual(before);
}
async function caller(f: Parameters<Parameters<typeof withIdentity>[0]>[0], api: SQL) {
  const client = await f.connection(),
    identity = await registeredIdentity(client, f.a.port),
    command = currentCommand(identity),
    store = createInternalIdentityStore(api),
    peer = (await store.readPeerDirectory(identity.receipt.certificate_sha256))!,
    session = (await store.readStartupSession(identity.receipt.session_id))!;
  const base = {
    service: session.service,
    instance_id: session.instance_id,
    certificate_sha256: session.certificate_sha256,
    instance_revision: peer.instance_revision,
    certificate_revision: peer.certificate_revision,
    raw_body_sha256: command.proof.raw_body_sha256,
  };
  return {
    client,
    identity,
    store,
    base,
    current: {
      ...base,
      session_id: session.session_id,
      incarnation_id: session.incarnation_id,
      key_sha256: session.public_key_sha256,
      session_revision: session.session_revision,
      command_type: "identity_current_v1" as const,
      command_id: command.proof.command_id,
      method: "POST" as const,
      normalized_path: "/_internal/core/v1/identity/current-session",
      deadline_at: command.proof.deadline_at,
      trace_id: command.proof.trace_id,
    },
  };
}
function malformed(p: Record<string, unknown>) {
  const result: [string, unknown][] = [
    ["SQL null", null],
    ["array", []],
    ["scalar", "input-canary"],
  ];
  for (const field of Object.keys(p)) {
    const missing = { ...p };
    delete missing[field];
    result.push([`${field} missing`, missing]);
    for (const [mode, value] of [
      ["null", null],
      ["number", 1],
      ["boolean", true],
      ["array", []],
      ["object", {}],
      ["empty", ""],
      ["format", "input-canary !"],
    ] as const)
      result.push([`${field} ${mode}`, { ...p, [field]: value }]);
  }
  result.push(["unknown field", { ...p, input_canary: "input-canary" }]);
  return result;
}

test("missing null and nonstring command authority cannot bypass the final API-role current barrier", () =>
  withIdentity(async (f, db) => {
    const api = new SQL(f.apiUrl, { max: 1 });
    try {
      const c = await caller(f, api),
        failures: string[] = [];
      const role = (
        await api`SELECT rolsuper,has_table_privilege(current_user,'internal_startup_sessions','SELECT') AS direct_read FROM pg_roles WHERE rolname=current_user`
      )[0];
      expect(role.rolsuper).toBe(false);
      expect(role.direct_read).toBe(false);
      expect((await c.store.current(c.current)).current).toBe(true);
      for (const [label, p] of malformed(c.current))
        await rejectUnchanged(
          db,
          failures,
          label,
          async () => api`SELECT internal_identity_current_v1(${p}::jsonb)`,
        );
      for (const [field, value] of Object.entries({
        service: "core-api",
        instance_id: "gatewayB",
        certificate_sha256: "1".repeat(64),
        instance_revision: "2",
        certificate_revision: "2",
        session_revision: "2",
        key_sha256: "1".repeat(64),
        session_id: randomUUID(),
        incarnation_id: randomUUID(),
        command_type: "admit_v1",
        method: "GET",
        normalized_path: "/_internal/core/v1/identity/challenges",
        deadline_at: "2026-02-30T00:00:00.000Z",
        command_id: "00000000-0000-0000-0000-000000000000",
      }))
        await rejectUnchanged(
          db,
          failures,
          `${field} mismatch`,
          async () =>
            api`SELECT internal_identity_current_v1(${{ ...c.current, [field]: value }}::jsonb)`,
        );
      for (const deadline_at of ["2000-01-01T00:00:00.000Z", "2999-01-01T00:00:00.000Z"])
        await rejectUnchanged(
          db,
          failures,
          "deadline outside allowed window",
          async () =>
            api`SELECT internal_identity_current_v1(${{ ...c.current, deadline_at }}::jsonb)`,
        );
      const missingKey = { ...c.current } as Partial<typeof c.current>;
      delete missingKey.key_sha256;
      await rejectUnchanged(db, failures, "store current missing key", () =>
        c.store.current(missingKey as typeof c.current),
      );
      expect(
        (
          await c.store.current({
            ...c.current,
            deadline_at: new Date(Date.now() + 10000).toISOString(),
          })
        ).receipt,
      ).toEqual(c.identity.receipt);
      expect(failures).toEqual([]);
    } finally {
      await api.close();
    }
  }));

test("missing peer projection and nullable challenge scalars cannot extend or recover a challenge", () =>
  withIdentity(async (f, db) => {
    const api = new SQL(f.apiUrl, { max: 1 });
    try {
      const c = await caller(f, api),
        failures: string[] = [],
        identity = startupIdentity();
      const p = { ...c.base, endpoint: "challenges" as const };
      const args: unknown[] = [
        identity.registrationId,
        Buffer.from(identity.spki, "base64url"),
        Buffer.from(identity.boot, "base64url"),
        identityAudience,
        "2".repeat(64),
        randomBytes(32),
        300,
      ];
      const challenge = async (projection: unknown, values = args) =>
        (
          await api`SELECT internal_identity_challenge_v1(${projection}::jsonb,${values[0]}::uuid,${values[1]}::bytea,${values[2]}::bytea,${values[3]}::text,${values[4]}::text,${values[5]}::bytea,${values[6]}::integer) AS value`
        )[0].value;
      const original = await challenge(p);
      expect(
        (
          await db`SELECT challenge_expires_at<=clock_timestamp()+interval '300 seconds' AS bounded FROM internal_startup_registrations WHERE challenge_id=${original.challenge_id}::uuid`
        )[0].bounded,
      ).toBe(true);
      expect(await challenge(p, [...args.slice(0, 6), 1])).toEqual(original);
      for (const [label, projection] of malformed(p))
        await rejectUnchanged(db, failures, label, () => challenge(projection));
      for (const [index, bad] of [
        [0, null],
        [0, "00000000-0000-0000-0000-000000000000"],
        [1, null],
        [1, Buffer.alloc(43)],
        [1, Buffer.alloc(45)],
        [2, null],
        [2, Buffer.alloc(31)],
        [2, Buffer.alloc(33)],
        [3, null],
        [3, ""],
        [3, "other/core-api"],
        [4, null],
        [4, ""],
        [4, "A".repeat(64)],
        [4, "3".repeat(64)],
        [5, null],
        [5, Buffer.alloc(31)],
        [5, Buffer.alloc(33)],
        [6, null],
        [6, 0],
        [6, 301],
      ] as [number, unknown][]) {
        const values = [...args];
        values[index] = bad;
        await rejectUnchanged(db, failures, `challenge scalar ${index} ${String(bad)}`, () =>
          challenge(p, values),
        );
      }
      expect(await challenge(p)).toEqual(original);
      expect((await c.store.current(c.current)).current).toBe(true);
      expect(failures).toEqual([]);
    } finally {
      await api.close();
    }
  }));

test("nullable registration id digest and TTL cannot consume or replay a registration", () =>
  withIdentity(async (f, db) => {
    const api = new SQL(f.apiUrl, { max: 1 });
    try {
      const c = await caller(f, api),
        failures: string[] = [];
      const p = { ...c.base, endpoint: "registrations" };
      const existing = await c.store.readRegistration(String(c.identity.challenge.challenge_id));
      const replay = async (projection: unknown) =>
        api`SELECT internal_identity_register_v1(${projection}::jsonb,${existing!.challenge_id}::uuid,${existing!.registration_id}::uuid,${existing!.registration_digest},60)`;
      for (const [label, projection] of malformed(p))
        await rejectUnchanged(db, failures, label, () => replay(projection));
      for (const registered of [false, true]) {
        for (const [index, bad] of [
          [0, null],
          [0, "00000000-0000-0000-0000-000000000000"],
          [0, randomUUID()],
          [1, null],
          [1, "00000000-0000-0000-0000-000000000000"],
          [1, randomUUID()],
          [2, null],
          [2, ""],
          [2, "A".repeat(64)],
          [2, "3".repeat(64)],
          [3, null],
          [3, 0],
          [3, 86401],
        ] as [number, unknown][]) {
          const identity = startupIdentity(),
            challenge = await challengeIdentity(c.client, f.a.port, identity);
          expect(challenge.status).toBe(200);
          const row = (await c.store.readRegistration(String(challenge.value.challenge_id)))!;
          const args: unknown[] = [
            row.challenge_id,
            row.registration_id,
            row.registration_digest,
            60,
          ];
          const register = async (values = args) =>
            (
              await api`SELECT internal_identity_register_v1(${p}::jsonb,${values[0]}::uuid,${values[1]}::uuid,${values[2]}::text,${values[3]}::integer) AS value`
            )[0].value;
          let receipt: unknown;
          if (registered) receipt = (await register()).receipt;
          const values = [...args];
          values[index] = bad;
          await rejectUnchanged(
            db,
            failures,
            `register ${registered} scalar ${index} ${String(bad)}`,
            () => register(values),
          );
          const valid = await register();
          expect(valid.current).toBe(true);
          expect(valid.replayed).toBe(registered);
          if (registered) expect(valid.receipt).toEqual(receipt);
          expect((await register()).receipt).toEqual(valid.receipt);
        }
      }
      expect((await c.store.current(c.current)).current).toBe(true);
      expect(failures).toEqual([]);
    } finally {
      await api.close();
    }
  }));

test("nullable provision replays and malformed change actions reject before directory or audit edits", () =>
  withIdentity(async (f, db) => {
    const api = new SQL(f.apiUrl, { max: 1 }),
      deployer = new SQL(f.deployerUrl, { max: 1 });
    try {
      const c = await caller(f, api),
        failures: string[] = [],
        cert = f.certificates.get("gateway")!;
      const args: unknown[] = [
        "gateway",
        "gatewayA",
        sha256(cert.x509.raw),
        "spiffe://synthetic/gateway/gatewayA",
        cert.x509.subjectAltName,
        new Date(cert.x509.validFrom).toISOString(),
        new Date(cert.x509.validTo).toISOString(),
      ];
      const provision = async (v = args) =>
        deployer`SELECT internal_identity_provision_v1(${v[0]}::text,${v[1]}::text,${v[2]}::text,${v[3]}::text,${v[4]}::text,${v[5]}::timestamptz,${v[6]}::timestamptz)`;
      const extra = f.certificates.get("extra")!;
      const newCertificate: unknown[] = [
        "gateway",
        "extra",
        sha256(extra.x509.raw),
        "spiffe://synthetic/gateway/extra",
        extra.x509.subjectAltName,
        new Date(extra.x509.validFrom).toISOString(),
        new Date(extra.x509.validTo).toISOString(),
      ];
      for (const [scope, material] of [
        ["existing", args],
        ["new", newCertificate],
      ] as const) {
        for (let index = 0; index < material.length; index++) {
          const values = [...material];
          values[index] = null;
          await rejectUnchanged(db, failures, `provision ${scope} null ${index}`, () =>
            provision(values),
          );
        }
      }
      for (const [index, bad] of [
        [0, ""],
        [0, "other"],
        [1, ""],
        [1, "bad space"],
        [2, ""],
        [2, "A".repeat(64)],
        [3, ""],
        [3, "input-canary"],
        [4, ""],
        [4, "x".repeat(2001)],
        [5, "infinity"],
        [6, "-infinity"],
        [6, args[5]],
      ] as [number, unknown][]) {
        const values = [...args];
        values[index] = bad;
        await rejectUnchanged(db, failures, `provision invalid ${index}`, () => provision(values));
      }
      await provision();
      for (const values of [
        [null, "gatewayA", null, null, "DISABLE"],
        ["gateway", null, null, null, "DISABLE"],
        ["gateway", "gatewayA", "", null, "RETIRE"],
        ["gateway", "gatewayA", null, null, null],
        ["gateway", "gatewayA", null, null, ""],
        ["gateway", "gatewayA", null, null, "input-canary"],
        ["gateway", "gatewayA", null, c.identity.receipt.session_id, "RETIRE"],
        ["gateway", "gatewayA", args[2], null, "DISABLE"],
        ["gateway", "gatewayA", null, null, "RETIRE"],
      ])
        await rejectUnchanged(
          db,
          failures,
          "change invalid",
          () =>
            deployer`SELECT internal_identity_change_v1(${values[0]}::text,${values[1]}::text,${values[2]}::text,${values[3]}::uuid,${values[4]}::text)`,
        );
      expect((await c.store.current(c.current)).current).toBe(true);
      expect(failures).toEqual([]);
    } finally {
      await api.close();
      await deployer.close();
    }
  }));

test("legal nullable deployment selectors retain disable retire and security revoke scopes", () =>
  withIdentity(async (f, db) => {
    const api = new SQL(f.apiUrl, { max: 1 }),
      deployer = new SQL(f.deployerUrl, { max: 1 });
    try {
      const client = await f.connection(),
        a = await registeredIdentity(client, f.a.port),
        b = await registeredIdentity(client, f.a.port),
        hash = a.receipt.certificate_sha256;
      await deployer`SELECT internal_identity_change_v1('gateway','gatewayA',NULL,${a.receipt.session_id}::uuid,'DISABLE')`;
      await deployer`SELECT internal_identity_change_v1('gateway','gatewayA',${hash},${a.receipt.session_id}::uuid,'DISABLE')`;
      await deployer`SELECT internal_identity_change_v1('gateway','gatewayA',${hash},${b.receipt.session_id}::uuid,'SECURITY_REVOKE')`;
      await deployer`SELECT internal_identity_change_v1('gateway','gatewayA',NULL,${a.receipt.session_id}::uuid,'SECURITY_REVOKE')`;
      const sessions =
        await db`SELECT disabled_at,security_revoked_at FROM internal_startup_sessions WHERE service='gateway' ORDER BY registered_at`;
      expect(sessions[0].disabled_at).not.toBeNull();
      expect(sessions[0].security_revoked_at).not.toBeNull();
      expect(sessions[1].disabled_at).toBeNull();
      expect(sessions[1].security_revoked_at).not.toBeNull();
      const otherHash = sha256(f.certificates.get("gatewayB")!.x509.raw);
      await deployer`SELECT internal_identity_change_v1('gateway','gatewayB',${otherHash},NULL,'RETIRE')`;
      await deployer`SELECT internal_identity_change_v1('gateway','gatewayB',${otherHash},NULL,'SECURITY_REVOKE')`;
      await deployer`SELECT internal_identity_change_v1('core-worker','workerA',NULL,NULL,'DISABLE')`;
      await deployer`SELECT internal_identity_change_v1('core-worker','workerA',NULL,NULL,'SECURITY_REVOKE')`;
      expect(
        (
          await db`SELECT retired_at IS NOT NULL AND security_revoked_at IS NOT NULL AS both FROM internal_service_certificates WHERE certificate_sha256=${otherHash}`
        )[0].both,
      ).toBe(true);
      expect(
        (
          await db`SELECT NOT enabled AND security_revoked_at IS NOT NULL AS both FROM internal_service_instances WHERE service='core-worker'`
        )[0].both,
      ).toBe(true);
      const store = createInternalIdentityStore(api);
      expect(await store.readStartupSession(a.receipt.session_id)).toBeDefined();
      expect(await store.readPeerDirectory("0".repeat(64))).toBeUndefined();
      expect(await store.readStartupSession(randomUUID())).toBeUndefined();
      expect(await store.readRegistration(randomUUID())).toBeUndefined();
      expect(
        (
          await api`SELECT internal_identity_read_peer_v1(NULL) AS peer,internal_identity_read_session_v1(NULL) AS session,internal_identity_read_registration_v1(NULL) AS registration`
        )[0],
      ).toMatchObject({ peer: null, session: null, registration: null });
    } finally {
      await api.close();
      await deployer.close();
    }
  }));
