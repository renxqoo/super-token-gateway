import { connect as netConnect } from "node:net";
import { listenBoundedH2, h2JsonReply, readBoundedData } from "@gateway/runtime/h2";
import { appendFile } from "node:fs/promises";
import {
  openRawH2,
  h2Frame,
  identityHeaders,
  boundedClose,
  hpackValueLength,
  sendRaw,
} from "../support/raw-h2.ts";
import { request as httpsRequest } from "node:https";
import { connect as h2Connect } from "node:http2";
import { sha256 } from "@gateway/runtime/crypto";
import { SQL } from "bun";
import { createCoreRpcClient } from "@gateway/core-client";
import { createInternalIdentityStore } from "@gateway/storage/internal-identity";
import {
  startupChallengeSchema,
  registrationProofFields,
  commandProofFields,
  type CoreVerifiedCallerProjection,
} from "@gateway/contracts";
import { randomBytes, randomUUID, sign } from "node:crypto";
import { expect, test } from "bun:test";
import {
  identityRegistry,
  identityAudience,
  withIdentity,
  registeredIdentity,
  currentCommand,
  identityRequest,
  startupIdentity,
  challengeIdentity,
  registerIdentity,
} from "../support/internal-identity.ts";

test("two independent Core processes register and authenticate against shared PG", () =>
  withIdentity(async (f, db) => {
    expect(f.a.session.service).toBe("core-api");
    expect(f.b.session.service).toBe("core-api");
    expect(f.a.session.session_id).not.toBe(f.b.session.session_id);
    const client = await f.connection(),
      identity = await registeredIdentity(client, f.a.port),
      other = await f.connection("gateway", f.b.port),
      command = currentCommand(identity);
    const response = await identityRequest(
      other,
      f.b.port,
      "/_internal/core/v1/identity/current-session",
      command.body,
      command.header,
    );
    expect(response.status).toBe(200);
    expect(response.value.command_id).toBe(command.body.envelope.command_id);
    expect((response.value.receipt as { session_id: string }).session_id).toBe(
      identity.receipt.session_id,
    );
    expect((await db`SELECT count(*)::int AS n FROM internal_startup_sessions`)[0].n).toBe(3);
  }));

test("registration response loss across Core replays one immutable session and concurrent challenge does not refresh TTL", () =>
  withIdentity(async (f, db) => {
    const a = await f.connection(),
      b = await f.connection("gateway", f.b.port),
      identity = startupIdentity();
    const results = await Promise.all([
      challengeIdentity(a, f.a.port, identity),
      challengeIdentity(b, f.b.port, identity),
    ]);
    expect(results.some((r) => r.status === 200)).toBe(true);
    expect(results.every((r) => r.status === 200 || r.status === 503)).toBe(true);
    const first = results.find((r) => r.status === 200)!,
      again = await challengeIdentity(b, f.b.port, identity);
    expect(again.value).toEqual(first.value);
    const registers = await Promise.all([
      registerIdentity(a, f.a.port, identity, first.value),
      registerIdentity(b, f.b.port, identity, first.value),
    ]);
    expect(registers.some((r) => r.status === 201)).toBe(true);
    expect(registers.every((r) => [200, 201, 503].includes(r.status))).toBe(true);
    const receipt = registers.find((r) => r.status === 201)!.value.receipt;
    const recovered = await registerIdentity(b, f.b.port, identity, first.value);
    expect(recovered.status).toBe(200);
    expect(recovered.value.receipt).toEqual(receipt);
    expect(recovered.value.replayed).toBe(true);
    expect(
      (
        await db`SELECT count(*)::int AS n FROM internal_startup_sessions WHERE service='gateway'`
      )[0].n,
    ).toBe(1);
    const changed = { ...identity, boot: randomBytes(32).toString("base64url") };
    const conflict = await challengeIdentity(a, f.a.port, changed);
    expect(conflict.status).toBe(409);
    expect(conflict.value.code).toBe("registration_conflict");
  }));

test("global canonical key and instance boot conflicts never replace original owner", () =>
  withIdentity(async (f, db) => {
    const a = await f.connection(),
      identity = await registeredIdentity(a, f.a.port);
    const reused = {
      ...identity,
      registrationId: randomUUID(),
      boot: randomBytes(32).toString("base64url"),
    };
    const c = await challengeIdentity(a, f.a.port, reused),
      rejected = await registerIdentity(a, f.a.port, reused, c.value);
    expect(rejected.status).toBe(409);
    expect(rejected.value.code).toBe("startup_key_reused");
    const bootConflict = { ...startupIdentity(), boot: identity.boot },
      c2 = await challengeIdentity(a, f.a.port, bootConflict),
      r2 = await registerIdentity(a, f.a.port, bootConflict, c2.value);
    expect(r2.status).toBe(409);
    expect(r2.value.code).toBe("startup_boot_conflict");
    const next = await registeredIdentity(a, f.a.port);
    expect(next.receipt.incarnation_id).not.toBe(identity.receipt.incarnation_id);
    expect(
      (
        await db`SELECT count(*)::int AS n FROM internal_startup_sessions WHERE service='gateway'`
      )[0].n,
    ).toBe(2);
    const command = currentCommand(identity);
    expect(
      (
        await identityRequest(
          a,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          command.body,
          command.header,
        )
      ).status,
    ).toBe(200);
  }));

test("registration wrong signature and cross-instance peer cannot consume challenge", () =>
  withIdentity(async (f, db) => {
    const a = await f.connection(),
      other = await f.connection("gatewayB"),
      identity = startupIdentity(),
      challenge = await challengeIdentity(a, f.a.port, identity);
    const request = {
      challenge_id: challenge.value.challenge_id,
      registration_id: identity.registrationId,
      signature: Buffer.alloc(64).toString("base64url"),
      contract_version: 1,
    };
    expect(
      (await identityRequest(a, f.a.port, "/_internal/core/v1/identity/registrations", request))
        .status,
    ).toBe(401);
    const stolen = await registerIdentity(other, f.a.port, identity, challenge.value);
    expect(stolen.status).toBe(401);
    expect(
      (
        await db`SELECT count(*)::int AS n FROM internal_startup_sessions WHERE service='gateway'`
      )[0].n,
    ).toBe(0);
    expect((await registerIdentity(a, f.a.port, identity, challenge.value)).status).toBe(201);
  }));

test("signed command tampering and forged context never reaches current-session authority", () =>
  withIdentity(async (f) => {
    const a = await f.connection(),
      identity = await registeredIdentity(a, f.a.port),
      other = await f.connection("gatewayB"),
      cmd = currentCommand(identity);
    for (const [field, value] of Object.entries({
      session_id: randomUUID(),
      incarnation_id: randomUUID(),
      command_id: randomUUID(),
      trace_id: "changed",
      deadline_at: new Date(Date.now() + 20000).toISOString(),
      raw_body_sha256: "0".repeat(64),
      contract_version: 2,
      command_type: "admit_v1",
      signature: Buffer.alloc(64).toString("base64url"),
    })) {
      const header = Buffer.from(JSON.stringify({ ...cmd.proof, [field]: value })).toString(
        "base64url",
      );
      const response = await identityRequest(
        a,
        f.a.port,
        "/_internal/core/v1/identity/current-session",
        cmd.body,
        header,
      );
      expect(response.status).toBe(401);
    }
    expect(
      (
        await identityRequest(
          other,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          cmd.body,
          cmd.header,
        )
      ).status,
    ).toBe(401);
    for (const changedFields of [
      commandProofFields(
        "wrong/core-api",
        identity.receipt,
        cmd.proof,
        "POST",
        "/_internal/core/v1/identity/current-session",
      ),
      commandProofFields(
        identityAudience,
        identity.receipt,
        cmd.proof,
        "GET",
        "/_internal/core/v1/identity/current-session",
      ),
      commandProofFields(
        identityAudience,
        identity.receipt,
        cmd.proof,
        "POST",
        "/_internal/core/v1/identity/other",
      ),
      [
        "other.domain",
        ...commandProofFields(
          identityAudience,
          identity.receipt,
          cmd.proof,
          "POST",
          "/_internal/core/v1/identity/current-session",
        ).slice(1),
      ],
    ]) {
      const signature = sign(
        null,
        Buffer.from(JSON.stringify(changedFields)),
        identity.keys.privateKey,
      ).toString("base64url");
      const r = await identityRequest(
        a,
        f.a.port,
        "/_internal/core/v1/identity/current-session",
        cmd.body,
        Buffer.from(JSON.stringify({ ...cmd.proof, signature })).toString("base64url"),
      );
      expect(r.status).toBe(401);
    }
    const forgedBody = {
      ...cmd.body,
      context: { service: "gateway", session_id: identity.receipt.session_id },
    };
    const forgedRaw = Buffer.from(JSON.stringify(forgedBody));
    const forgedProof = { ...cmd.proof, raw_body_sha256: sha256(forgedRaw) };
    forgedProof.signature = sign(
      null,
      Buffer.from(
        JSON.stringify(
          commandProofFields(
            identityAudience,
            identity.receipt,
            forgedProof,
            "POST",
            "/_internal/core/v1/identity/current-session",
          ),
        ),
      ),
      identity.keys.privateKey,
    ).toString("base64url");
    const forged = await identityRequest(
      a,
      f.a.port,
      "/_internal/core/v1/identity/current-session",
      forgedBody,
      Buffer.from(JSON.stringify(forgedProof)).toString("base64url"),
    );
    expect(forged.status).toBe(400);
    const raw = Buffer.from(JSON.stringify(cmd.body, null, 2));
    expect(
      (
        await identityRequest(
          a,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          cmd.body,
          cmd.header,
          raw,
        )
      ).status,
    ).toBe(401);
    expect(
      (
        await identityRequest(
          a,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          { ...cmd.body, context: { service: "gateway", session_id: identity.receipt.session_id } },
          cmd.header,
        )
      ).status,
    ).toBe(401);
    for (const path of [
      "/_internal/core/v1/admit",
      "/_internal/core/v1/identity//current-session",
      "/_internal/core/v1/identity/current-session?x=BODY_CANARY",
      "/_internal/core/v1/identity/%63urrent-session",
      "/_internal/core/v1/identity/../current-session",
    ]) {
      const r = await identityRequest(a, f.a.port, path, cmd.body, cmd.header);
      expect(r.status).toBe(400);
      expect(r.raw).not.toContain("BODY_CANARY");
    }
    const expired = currentCommand(identity, { deadline_at: "2000-01-01T00:00:00.000Z" });
    expect(
      (
        await identityRequest(
          a,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          expired.body,
          expired.header,
        )
      ).status,
    ).toBe(408);
  }));

for (const layer of ["session", "certificate", "instance"] as const)
  test(`shared PG security revoke ${layer} rejects next stream on already connected Core A`, () =>
    withIdentity(async (f, db) => {
      const client = await f.connection(),
        identity = await registeredIdentity(client, f.a.port),
        before = currentCommand(identity);
      expect(
        (
          await identityRequest(
            client,
            f.a.port,
            "/_internal/core/v1/identity/current-session",
            before.body,
            before.header,
          )
        ).status,
      ).toBe(200);
      await db`SELECT internal_identity_change_v1('gateway','gatewayA',${layer === "instance" ? null : identity.receipt.certificate_sha256},${layer === "session" ? identity.receipt.session_id : null}::uuid,'SECURITY_REVOKE')`;
      const after = currentCommand(identity),
        r = await identityRequest(
          client,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          after.body,
          after.header,
        );
      expect(r.status).toBe(403);
      expect(r.value.code).toBe("internal_identity_revoked");
      const publicMaterial = (
        await db`SELECT internal_identity_read_session_v1(${identity.receipt.session_id}::uuid) AS value`
      )[0].value;
      expect(publicMaterial.public_key_sha256).toBe(identity.receipt.public_key_sha256);
    }));

test("normal certificate retirement and session disable retain historical public key without granting current authority", () =>
  withIdentity(async (f, db) => {
    const client = await f.connection(),
      identity = await registeredIdentity(client, f.a.port);
    await db`SELECT internal_identity_change_v1('gateway','gatewayA',${identity.receipt.certificate_sha256},${identity.receipt.session_id}::uuid,'DISABLE')`;
    const c = currentCommand(identity);
    expect(
      (
        await identityRequest(
          client,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          c.body,
          c.header,
        )
      ).status,
    ).toBe(401);
    const replay = await registerIdentity(client, f.a.port, identity, identity.challenge);
    expect(replay.status).toBe(200);
    expect(replay.value.current).toBe(false);
    expect(replay.value.receipt).toEqual(identity.receipt);
    await db`SELECT internal_identity_change_v1('gateway','gatewayA',${identity.receipt.certificate_sha256},NULL,'RETIRE')`;
    const material = (
      await db`SELECT internal_identity_read_session_v1(${identity.receipt.session_id}::uuid) AS v`
    )[0].v;
    expect(material.security_revoked_at).toBeNull();
    expect(material.public_key_sha256).toBe(identity.receipt.public_key_sha256);
    const cert = (
      await db`SELECT internal_identity_read_peer_v1(${identity.receipt.certificate_sha256}) AS v`
    )[0].v;
    expect(cert.security_revoked_at).toBeNull();
    expect(cert.retired_at).not.toBeNull();
  }));

test("workload cannot self-provision or mutate directory and worker verifier cannot register", () =>
  withIdentity(async (f, db) => {
    const api = new SQL(f.apiUrl, { max: 1 }),
      verifier = new SQL(f.verifierUrl, { max: 1 });
    try {
      for (const conn of [api, verifier]) {
        await expect(
          conn`UPDATE internal_service_instances SET enabled=false`,
        ).rejects.toMatchObject({ errno: "42501" });
        await expect(
          conn`SELECT internal_identity_provision_v1('gateway','forged',repeat('c',64),'URI:forged','URI:forged',clock_timestamp(),clock_timestamp()+interval '1 day')`,
        ).rejects.toMatchObject({ errno: "42501" });
        await expect(
          conn`SELECT set_config('role','core_identity_owner',false)`,
        ).rejects.toMatchObject({ errno: "42501" });
      }
      await expect(
        verifier`SELECT internal_identity_register_v1('{}',${randomUUID()}::uuid,${randomUUID()}::uuid,repeat('c',64),10)`,
      ).rejects.toMatchObject({ errno: "42501" });
      expect((await db`SELECT count(*)::int AS n FROM execution_commands`)[0].n).toBe(0);
    } finally {
      await api.close();
      await verifier.close();
    }
  }));

test("TLS rejects wrong CA hostname missing certificate and public certificate without matching private key", () =>
  withIdentity(async (f) => {
    for (const extra of [
      { ca: f.certificates.get("gateway")!.cert },
      { servername: "wrong.invalid" },
      { cert: "", key: "" },
      { cert: f.certificates.get("gatewayB")!.cert, key: f.certificates.get("gateway")!.key },
    ]) {
      let rejected = false;
      let connection: Awaited<ReturnType<typeof f.connection>> | undefined;
      try {
        connection = await f.connection("gateway", f.a.port, extra);
        await identityRequest(connection, f.a.port, "/_internal/core/v1/identity/challenges", {});
      } catch {
        rejected = true;
      } finally {
        await connection?.close();
      }
      expect(rejected).toBe(true);
    }
    const unknown = await f.connection("extra");
    const unregistered = await identityRequest(
      unknown,
      f.a.port,
      "/_internal/core/v1/identity/challenges",
      {},
    );
    expect(unregistered.status).toBe(403);
  }));

test("provisioned exact SAN mismatch rejects every stream without accepting a client-supplied identity", () =>
  withIdentity(async (f, db) => {
    const extra = f.certificates.get("extra")!;
    await db`SELECT internal_identity_provision_v1('gateway','extra',${sha256(extra.x509.raw)},'spiffe://synthetic/gateway/extra','URI:spiffe://synthetic/gateway/extra',${new Date(extra.x509.validFrom).toISOString()}::timestamptz,${new Date(extra.x509.validTo).toISOString()}::timestamptz)`;
    const client = await f.connection("extra"),
      request = startupIdentity();
    const result = await challengeIdentity(client, f.a.port, request);
    expect(result.status).toBe(403);
    expect(result.value.code).toBe("internal_peer_forbidden");
  }));

test("HTTP1 and cleartext h2 cannot enter the internal authority", () =>
  withIdentity(async (f) => {
    const cert = f.certificates.get("gateway")!;
    const http1 = await new Promise<boolean>((resolve) => {
      const requestOptions = {
        hostname: "127.0.0.1",
        port: String(f.a.port),
        servername: "localhost",
        ca: f.ca,
        key: cert.key,
        cert: cert.cert,
        agent: false,
        path: "/_internal/core/v1/identity/challenges",
        method: "POST",
        ALPNProtocols: ["http/1.1"],
      };
      const req = httpsRequest(requestOptions, (res) => {
        res.resume();
        resolve(false);
      });
      req.setTimeout(2000, () => req.destroy());
      req.on("error", () => resolve(true));
      req.end("{}");
    });
    expect(http1).toBe(true);
    const cleartext = await new Promise<boolean>((resolve) => {
      const session = h2Connect(`http://127.0.0.1:${f.a.port}`);
      session.on("error", () => {
        session.destroy();
        resolve(true);
      });
      const timer = setTimeout(() => {
        session.destroy();
        resolve(true);
      }, 1500);
      session.on("close", () => clearTimeout(timer));
      const req = session.request({
        ":path": "/_internal/core/v1/identity/challenges",
        ":method": "POST",
      });
      req.on("error", () => {
        session.destroy();
        resolve(true);
      });
      req.on("response", () => {
        session.destroy();
        resolve(false);
      });
      req.end("{}");
    });
    expect(cleartext).toBe(true);
  }));

test("body privacy and declared byte limits reject canaries without echo or persistence", () =>
  withIdentity(async (f, db) => {
    const client = await f.connection(),
      identity = startupIdentity();
    const canary = "BODY_CANARY_HEADER_and_QUERY_23910";
    const result = await identityRequest(
      client,
      f.a.port,
      "/_internal/core/v1/identity/challenges",
      {
        registration_id: identity.registrationId,
        public_key_spki: identity.spki,
        boot_nonce: identity.boot,
        contract_version: 1,
        headers: { "x-legitimate": canary },
        path: `/v1/chat/completions?x=${canary}`,
      },
    );
    expect(result.status).toBe(400);
    expect(result.raw).not.toContain(canary);
    const big = await identityRequest(client, f.a.port, "/_internal/core/v1/identity/challenges", {
      x: canary.repeat(400),
    });
    expect(big.status).toBe(413);
    expect(big.raw).not.toContain(canary);
    const invalidUtf8 = await identityRequest(
      client,
      f.a.port,
      "/_internal/core/v1/identity/challenges",
      {},
      undefined,
      Buffer.from([0x7b, 0xff, 0x7d]),
    );
    expect(invalidUtf8.status).toBe(400);
    expect(
      (
        await db`SELECT count(*)::int AS n FROM internal_startup_registrations WHERE service='gateway'`
      )[0].n,
    ).toBe(0);
    const rows = await db`SELECT to_jsonb(r) AS value FROM internal_startup_registrations r`;
    expect(JSON.stringify(rows)).not.toContain(canary);
  }));

test("expired startup sessions replay the original receipt but never renew or authorize", () =>
  withIdentity(
    async (f, db) => {
      const client = await f.connection(),
        identity = await registeredIdentity(client, f.a.port);
      await Bun.sleep(1100);
      const cmd = currentCommand(identity),
        r = await identityRequest(
          client,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          cmd.body,
          cmd.header,
        );
      expect(r.status).toBe(401);
      const recovered = await registerIdentity(client, f.a.port, identity, identity.challenge);
      expect(recovered.status).toBe(200);
      expect(recovered.value.receipt).toEqual(identity.receipt);
      expect(recovered.value.current).toBe(false);
      expect(
        (
          await db`SELECT count(*)::int AS n FROM internal_startup_sessions WHERE service='gateway'`
        )[0].n,
      ).toBe(1);
    },
    { sessionTtl: 1 },
  ));

test("expired unconsumed challenge fails without consuming a startup key", () =>
  withIdentity(
    async (f, db) => {
      const client = await f.connection(),
        identity = startupIdentity(),
        c = await challengeIdentity(client, f.a.port, identity);
      await Bun.sleep(1100);
      const r = await registerIdentity(client, f.a.port, identity, c.value);
      expect(r.status).toBe(408);
      expect(r.value.code).toBe("internal_deadline");
      expect(
        (
          await db`SELECT count(*)::int AS n FROM internal_startup_sessions WHERE service='gateway'`
        )[0].n,
      ).toBe(0);
      const again = await challengeIdentity(client, f.a.port, identity);
      expect(again.value).toEqual(c.value);
    },
    { challengeTtl: 1 },
  ));

test("all three startup service identities use actual core-client with concurrent initialization and cross-Core reconnect", () =>
  withIdentity(async (f, db) => {
    const worker = f.certificates.get("worker")!;
    const rpc = createCoreRpcClient({
      origin: `https://127.0.0.1:${f.a.port}`,
      servername: "localhost",
      key: worker.key,
      cert: worker.cert,
      ca: f.ca,
      audience: identityAudience,
      expectedPeerSha256: sha256(f.certificates.get("coreA")!.x509.raw),
      expectedSubjectAltname: f.certificates.get("coreA")!.x509.subjectAltName ?? "",
    });
    try {
      const receipts = await Promise.all(Array.from({ length: 8 }, () => rpc.startup()));
      expect(new Set(receipts.map((r) => r.session_id)).size).toBe(1);
      expect(receipts[0]!.service).toBe("core-worker");
      const replies = await Promise.all(Array.from({ length: 8 }, () => rpc.current()));
      expect(
        replies.every((r) => r.current && r.receipt.session_id === receipts[0]!.session_id),
      ).toBe(true);
      await rpc.reconnect({
        origin: `https://127.0.0.1:${f.b.port}`,
        expectedPeerSha256: sha256(f.certificates.get("coreB")!.x509.raw),
        expectedSubjectAltname: f.certificates.get("coreB")!.x509.subjectAltName ?? "",
      });
      expect((await rpc.current()).receipt.session_id).toBe(receipts[0]!.session_id);
      expect(
        (
          await db`SELECT count(*)::int AS n FROM internal_startup_sessions WHERE service='core-worker'`
        )[0].n,
      ).toBe(1);
    } finally {
      await rpc.close();
    }
    await expect(rpc.startup()).rejects.toThrow("Client closed");
  }));

test("registration response body discarded after commit recovers same receipt on the other Core", () =>
  withIdentity(async (f, db) => {
    const client = await f.connection(),
      identity = startupIdentity(),
      challenge = await challengeIdentity(client, f.a.port, identity),
      c = startupChallengeSchema.parse(challenge.value);
    const cert = f.certificates.get("gateway")!,
      signature = sign(
        null,
        Buffer.from(JSON.stringify(registrationProofFields(c))),
        identity.keys.privateKey,
      ).toString("base64url"),
      raw = Buffer.from(
        JSON.stringify({
          challenge_id: c.challenge_id,
          registration_id: identity.registrationId,
          signature,
          contract_version: 1,
        }),
      );
    await new Promise<void>((resolve, reject) => {
      const session = h2Connect(`https://127.0.0.1:${f.a.port}`, {
        servername: "localhost",
        ca: f.ca,
        key: cert.key,
        cert: cert.cert,
      });
      session.on("error", reject);
      const timer = setTimeout(() => {
        session.destroy();
        reject(new Error("Response loss test timeout"));
      }, 2000);
      const req = session.request({
        ":authority": `localhost:${f.a.port}`,
        ":method": "POST",
        ":path": "/_internal/core/v1/identity/registrations",
        "content-type": "application/json",
        "content-length": String(raw.length),
      });
      req.on("error", () => {});
      req.once("response", (headers) => {
        clearTimeout(timer);
        expect(headers[":status"]).toBe(201);
        req.close();
        session.destroy();
        resolve();
      });
      req.end(raw);
    });
    const persisted = (
      await db`SELECT receipt FROM internal_startup_registrations WHERE challenge_id=${c.challenge_id}::uuid`
    )[0].receipt;
    const other = await f.connection("gateway", f.b.port),
      recovered = await registerIdentity(other, f.b.port, identity, challenge.value);
    expect(recovered.status).toBe(200);
    expect(recovered.value.receipt).toEqual(persisted);
    expect(recovered.value.replayed).toBe(true);
  }));

for (const layer of ["session", "certificate", "instance"] as const)
  test(`current authorization and ${layer} revoke have both PG lock linearization orders`, () =>
    withIdentity(async (f, db) => {
      const client = await f.connection(),
        identity = await registeredIdentity(client, f.a.port),
        sid = identity.receipt.session_id,
        hash = identity.receipt.certificate_sha256;
      let release!: () => void, ready!: () => void;
      const held = new Promise<void>((r) => (ready = r)),
        gate = new Promise<void>((r) => (release = r));
      const reader = db.begin(async (tx) => {
        await tx`SELECT * FROM internal_service_instances WHERE service='gateway' AND instance_id='gatewayA' FOR SHARE NOWAIT`;
        await tx`SELECT * FROM internal_service_certificates WHERE certificate_sha256=${hash} FOR SHARE NOWAIT`;
        await tx`SELECT * FROM internal_startup_sessions WHERE session_id=${sid}::uuid FOR SHARE NOWAIT`;
        ready();
        await gate;
      });
      await held;
      const revoke = db`SELECT internal_identity_change_v1('gateway','gatewayA',${layer === "instance" ? null : hash},${layer === "session" ? sid : null}::uuid,'SECURITY_REVOKE')`;
      let revokeDone = false;
      const writer = Promise.resolve(revoke).then(() => {
        revokeDone = true;
      });
      try {
        let blocked = false;
        for (let i = 0; i < 100; i++) {
          const rows =
            await db`SELECT wait_event_type FROM pg_stat_activity WHERE datname=current_database() AND query LIKE 'SELECT internal_identity_change_v1%' AND pid<>pg_backend_pid()`;
          if (rows.some((r: { wait_event_type: string }) => r.wait_event_type === "Lock")) {
            blocked = true;
            break;
          }
          await Bun.sleep(5);
        }
        expect(blocked).toBe(true);
        expect(revokeDone).toBe(false);
        const cmd = currentCommand(identity);
        const before = await identityRequest(
          client,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          cmd.body,
          cmd.header,
        );
        expect(before.status).toBe(200);
      } finally {
        release();
        await reader;
        await writer;
      }
      const after = currentCommand(identity),
        r = await identityRequest(
          client,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          after.body,
          after.header,
        );
      expect(r.status).toBe(403);
      expect(r.value.code).toBe("internal_identity_revoked");
    }));

test("bounded in-flight streams reject excess work and cancellation leaves the same socket usable", () =>
  withIdentity(
    async (f) => {
      const cert = f.certificates.get("gateway")!,
        session = h2Connect(`https://127.0.0.1:${f.a.port}`, {
          servername: "localhost",
          ca: f.ca,
          key: cert.key,
          cert: cert.cert,
        });
      session.on("error", () => {});
      const held: ReturnType<typeof session.request>[] = [];
      function request(raw: Buffer) {
        return new Promise<{ status: number; body: string }>((resolve, reject) => {
          const s = session.request({
            ":authority": `localhost:${f.a.port}`,
            ":method": "POST",
            ":path": "/_internal/core/v1/identity/challenges",
            "content-type": "application/json",
            "content-length": String(raw.length),
          });
          let status = 0,
            body = "";
          s.on("response", (h) => (status = Number(h[":status"])));
          s.on("data", (c) => (body += c));
          s.once("end", () => resolve({ status, body }));
          s.once("error", reject);
          s.end(raw);
        });
      }
      try {
        await new Promise<void>((resolve, reject) => {
          session.once("connect", resolve);
          session.once("error", reject);
        });
        for (let i = 0; i < 4; i++) {
          const s = session.request({
            ":authority": `localhost:${f.a.port}`,
            ":method": "POST",
            ":path": "/_internal/core/v1/identity/challenges",
            "content-type": "application/json",
            "content-length": "100",
          });
          s.on("error", () => {});
          s.write("{");
          held.push(s);
        }
        await new Promise<void>((resolve, reject) =>
          session.ping((error) => (error ? reject(error) : resolve())),
        );
        const rejected = await request(Buffer.from("{}"));
        expect(rejected.status).toBe(429);
        expect(rejected.body).toContain("internal_capacity");
        for (const s of held) s.close();
        await new Promise<void>((resolve, reject) =>
          session.ping((error) => (error ? reject(error) : resolve())),
        );
        const identity = startupIdentity(),
          raw = Buffer.from(
            JSON.stringify({
              registration_id: identity.registrationId,
              public_key_spki: identity.spki,
              boot_nonce: identity.boot,
              contract_version: 1,
            }),
          );
        const recovered = await request(raw);
        expect(recovered.status).toBe(200);
      } finally {
        for (const s of held) s.close();
        session.destroy();
      }
    },
    { limits: { streams: 4 } },
  ));

test("header count byte and duplicate proof budgets fail safely without blocking later commands", () =>
  withIdentity(async (f) => {
    const cert = f.certificates.get("gateway")!,
      session = h2Connect(`https://127.0.0.1:${f.a.port}`, {
        servername: "localhost",
        ca: f.ca,
        key: cert.key,
        cert: cert.cert,
      });
    session.on("error", () => {});
    try {
      for (const extra of [
        { "x-large": "HEADER_BODY_CANARY".repeat(1200) },
        Object.fromEntries(Array.from({ length: 35 }, (_, i) => [`x-${i}`, "a"])),
        { "x-stg-proof": ["a", "b"] },
      ]) {
        const rejected = await new Promise<boolean>((resolve) => {
          const timer = setTimeout(() => resolve(true), 2000);
          try {
            const s = session.request({
              ":authority": `localhost:${f.a.port}`,
              ":method": "POST",
              ":path": "/_internal/core/v1/identity/current-session",
              "content-type": "application/json",
              ...extra,
            });
            let body = "",
              status = 0;
            s.on("response", (h) => (status = Number(h[":status"])));
            s.on("data", (c) => (body += c));
            s.on("error", () => {
              clearTimeout(timer);
              resolve(true);
            });
            s.on("end", () => {
              clearTimeout(timer);
              expect(body).not.toContain("HEADER_BODY_CANARY");
              resolve(status >= 400);
            });
            s.end("{}");
          } catch {
            clearTimeout(timer);
            resolve(true);
          }
        });
        expect(rejected).toBe(true);
      }
    } finally {
      session.destroy();
    }
    const client = await f.connection(),
      identity = await registeredIdentity(client, f.a.port),
      cmd = currentCommand(identity);
    expect(
      (
        await identityRequest(
          client,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          cmd.body,
          cmd.header,
        )
      ).status,
    ).toBe(200);
  }));

test("peer connection budget rejects a fifth connection and preserves the four established peers", () =>
  withIdentity(async (f) => {
    const clients: Awaited<ReturnType<typeof f.connection>>[] = [];
    try {
      for (let i = 0; i < 4; i++) {
        const c = await f.connection();
        clients.push(c);
        expect(
          (await identityRequest(c, f.a.port, "/_internal/core/v1/identity/challenges", {})).status,
        ).toBe(400);
      }
      let rejected = false;
      try {
        const fifth = await f.connection();
        clients.push(fifth);
        await identityRequest(fifth, f.a.port, "/_internal/core/v1/identity/challenges", {});
      } catch {
        rejected = true;
      }
      expect(rejected).toBe(true);
      expect(
        (await identityRequest(clients[0]!, f.a.port, "/_internal/core/v1/identity/challenges", {}))
          .status,
      ).toBe(400);
    } finally {
      for (const c of clients) await c.close();
    }
  }));

test("storage final current-state barrier holds SHARE locks to outer commit and rejects stale scalar revisions", () =>
  withIdentity(async (f, db) => {
    const client = await f.connection(),
      identity = await registeredIdentity(client, f.a.port),
      c = currentCommand(identity),
      store = createInternalIdentityStore(db),
      peer = (await store.readPeerDirectory(identity.receipt.certificate_sha256))!,
      session = (await store.readStartupSession(identity.receipt.session_id))!;
    const projection: CoreVerifiedCallerProjection = {
      service: session.service,
      instance_id: session.instance_id,
      certificate_sha256: session.certificate_sha256,
      instance_revision: peer.instance_revision,
      certificate_revision: peer.certificate_revision,
      raw_body_sha256: c.proof.raw_body_sha256,
      session_id: session.session_id,
      incarnation_id: session.incarnation_id,
      key_sha256: session.public_key_sha256,
      session_revision: session.session_revision,
      command_type: "identity_current_v1",
      command_id: c.proof.command_id,
      method: "POST",
      normalized_path: "/_internal/core/v1/identity/current-session",
      deadline_at: c.proof.deadline_at,
      trace_id: c.proof.trace_id,
    };
    let release!: () => void, ready!: () => void;
    const held = new Promise<void>((r) => (ready = r)),
      gate = new Promise<void>((r) => (release = r));
    const transaction = db.begin(async (tx) => {
      const result = await createInternalIdentityStore(tx).current(projection);
      expect(result.current).toBe(true);
      ready();
      await gate;
    });
    await held;
    let done = false;
    const writer = Promise.resolve(
      db`SELECT internal_identity_change_v1('gateway','gatewayA',NULL,${session.session_id}::uuid,'DISABLE')`,
    ).then(() => {
      done = true;
    });
    try {
      let blocked = false;
      for (let i = 0; i < 100; i++) {
        const rows =
          await db`SELECT wait_event_type FROM pg_stat_activity WHERE datname=current_database() AND query LIKE 'SELECT internal_identity_change_v1%' AND pid<>pg_backend_pid()`;
        if (rows.some((r: { wait_event_type: string }) => r.wait_event_type === "Lock")) {
          blocked = true;
          break;
        }
        await Bun.sleep(5);
      }
      expect(blocked).toBe(true);
      expect(done).toBe(false);
    } finally {
      release();
      await transaction;
      await writer;
    }
    await expect(store.current(projection)).rejects.toMatchObject({
      message: "internal_session_invalid",
    });
  }));

test("writer-held revocation returns bounded BUSY before commit then rejects security revoked identity", () =>
  withIdentity(async (f, db) => {
    const client = await f.connection(),
      identity = await registeredIdentity(client, f.a.port);
    let release!: () => void, ready!: () => void;
    const held = new Promise<void>((r) => (ready = r)),
      gate = new Promise<void>((r) => (release = r));
    const transaction = db.begin(async (tx) => {
      await tx`SELECT internal_identity_change_v1('gateway','gatewayA',NULL,${identity.receipt.session_id}::uuid,'SECURITY_REVOKE')`;
      ready();
      await gate;
    });
    await held;
    try {
      const command = currentCommand(identity),
        r = await identityRequest(
          client,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          command.body,
          command.header,
        );
      expect(r.status).toBe(503);
      expect(r.value.code).toBe("identity_busy");
    } finally {
      release();
      await transaction;
    }
    const after = currentCommand(identity),
      result = await identityRequest(
        client,
        f.a.port,
        "/_internal/core/v1/identity/current-session",
        after.body,
        after.header,
      );
    expect(result.status).toBe(403);
    expect(result.value.code).toBe("internal_identity_revoked");
  }));

test("raw whitespace needs a new signature while semantic read-only command identity remains the same", () =>
  withIdentity(async (f) => {
    const client = await f.connection(),
      identity = await registeredIdentity(client, f.a.port),
      c = currentCommand(identity),
      first = await identityRequest(
        client,
        f.a.port,
        "/_internal/core/v1/identity/current-session",
        c.body,
        c.header,
      );
    const raw = Buffer.from(JSON.stringify(c.body, null, 2)),
      proof = { ...c.proof, raw_body_sha256: sha256(raw) };
    proof.signature = sign(
      null,
      Buffer.from(
        JSON.stringify(
          commandProofFields(
            identityAudience,
            identity.receipt,
            proof,
            "POST",
            "/_internal/core/v1/identity/current-session",
          ),
        ),
      ),
      identity.keys.privateKey,
    ).toString("base64url");
    const second = await identityRequest(
      client,
      f.a.port,
      "/_internal/core/v1/identity/current-session",
      c.body,
      Buffer.from(JSON.stringify(proof)).toString("base64url"),
      raw,
    );
    expect(first.status).toBe(200);
    expect(second.status).toBe(200);
    expect(second.value).toEqual(first.value);
  }));

test("first HEADERS watchdog and partial later header blocks stay within measured idle and lifetime bounds", () =>
  withIdentity(
    async (f) => {
      const cert = f.certificates.get("gateway")!;
      const initial = await openRawH2({ port: f.a.port, ca: f.ca, key: cert.key, cert: cert.cert }),
        start = performance.now();
      try {
        await boundedClose(initial.closed, 2000);
        const elapsed = performance.now() - start;
        expect(elapsed).toBeLessThan(1500);
        await appendFile(
          identityRegistry,
          JSON.stringify({ event: "raw_first_headers_bound", elapsed_ms: elapsed }) + "\n",
        );
      } finally {
        initial.socket.destroy();
      }
      for (const drip of [false, true]) {
        const raw = await openRawH2({ port: f.a.port, ca: f.ca, key: cert.key, cert: cert.cert });
        let interval: ReturnType<typeof setInterval> | undefined;
        try {
          const identity = startupIdentity(),
            body = Buffer.from(
              JSON.stringify({
                registration_id: identity.registrationId,
                public_key_spki: identity.spki,
                boot_nonce: identity.boot,
                contract_version: 1,
              }),
            );
          raw.socket.write(
            Buffer.concat([h2Frame(1, 4, 1, identityHeaders(f.a.port)), h2Frame(0, 1, 1, body)]),
          );
          const valid = startupChallengeSchema.parse(await raw.response);
          expect(valid.instance_id).toBe("gatewayA");
          const begin = performance.now();
          raw.socket.write(h2Frame(1, 0, 3, Buffer.from([0x83])));
          if (drip)
            interval = setInterval(() => {
              if (!raw.socket.destroyed) raw.socket.write(h2Frame(9, 0, 3, Buffer.from([0])));
            }, 25);
          await boundedClose(raw.closed, 2500);
          const elapsed = performance.now() - begin;
          expect(elapsed).toBeLessThan(2200);
          await appendFile(
            identityRegistry,
            JSON.stringify({
              event: "raw_partial_headers_bound",
              drip,
              elapsed_ms: elapsed,
              frames: raw.frames,
            }) + "\n",
          );
        } finally {
          if (interval) clearInterval(interval);
          raw.socket.destroy();
        }
      }
    },
    { limits: { headersMs: 300, idleMs: 400, lifetimeMs: 1200, drainMs: 100 } },
  ));

test("native decoder rejects an oversized HPACK value before the full header block", () =>
  withIdentity(
    async (f) => {
      const cert = f.certificates.get("gateway")!,
        raw = await openRawH2({ port: f.a.port, ca: f.ca, key: cert.key, cert: cert.cert });
      try {
        const identity = startupIdentity(),
          body = Buffer.from(
            JSON.stringify({
              registration_id: identity.registrationId,
              public_key_spki: identity.spki,
              boot_nonce: identity.boot,
              contract_version: 1,
            }),
          );
        raw.socket.write(
          Buffer.concat([h2Frame(1, 4, 1, identityHeaders(f.a.port)), h2Frame(0, 1, 1, body)]),
        );
        startupChallengeSchema.parse(await raw.response);
        const length = 8 * 1024 * 1024,
          begin = performance.now();
        raw.socket.write(
          h2Frame(1, 0, 3, Buffer.concat([Buffer.from([0, 1, 0x78]), hpackValueLength(length)])),
        );
        let written = 0;
        for (let i = 0; i < length / 16384; i++) {
          if (!(await sendRaw(raw.socket, h2Frame(9, 0, 3, Buffer.alloc(16384, 0x61))))) break;
          written += 16384;
        }
        await boundedClose(raw.closed, 3000);
        const elapsed = performance.now() - begin;
        await appendFile(
          identityRegistry,
          JSON.stringify({
            event: "raw_hpack_oversize",
            elapsed_ms: elapsed,
            written,
            frames: raw.frames,
          }) + "\n",
        );
        expect(elapsed).toBeLessThan(3000);
        expect(
          raw.frames.some(
            (frame) => (frame.type === 7 || frame.type === 3) && frame.errorCode !== 0,
          ),
        ).toBe(true);
      } finally {
        raw.socket.destroy();
      }
    },
    { limits: { idleMs: 1000, lifetimeMs: 5000, drainMs: 100 } },
  ));

test("deployment-only CLI provisions and security revokes an actual peer with an audited database actor", () =>
  withIdentity(async (f, db) => {
    const command = (args: string[]) =>
      Bun.spawn(["bun", ...args], {
        env: {
          ...process.env,
          MINIMAX_API_KEY: "",
          INTERNAL_IDENTITY_DEPLOY_DATABASE_URL: f.deployerUrl,
        },
        stdout: "pipe",
        stderr: "pipe",
      });
    const provision = command([
      "scripts/identity-provision.ts",
      "--service",
      "gateway",
      "--instance",
      "extra",
      "--certificate",
      `${f.directory}/extra.pem`,
      "--uri-san",
      "spiffe://synthetic/gateway/extra",
      "--hostname",
      "localhost",
    ]);
    const out = new Response(provision.stdout).text(),
      err = new Response(provision.stderr).text();
    expect(await provision.exited).toBe(0);
    expect(await out).toContain("Provisioned internal certificate");
    expect(await err).toBe("");
    const client = await f.connection("extra"),
      identity = await registeredIdentity(client, f.a.port),
      before = currentCommand(identity);
    expect(
      (
        await identityRequest(
          client,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          before.body,
          before.header,
        )
      ).status,
    ).toBe(200);
    const revoke = command([
      "scripts/identity-revoke.ts",
      "--service",
      "gateway",
      "--instance",
      "extra",
      "--session",
      identity.receipt.session_id,
      "--action",
      "SECURITY_REVOKE",
    ]);
    const revokeOut = new Response(revoke.stdout).text(),
      revokeErr = new Response(revoke.stderr).text();
    expect(await revoke.exited).toBe(0);
    expect(await revokeOut).toContain("Changed internal identity authorization");
    expect(await revokeErr).toBe("");
    const after = currentCommand(identity);
    expect(
      (
        await identityRequest(
          client,
          f.a.port,
          "/_internal/core/v1/identity/current-session",
          after.body,
          after.header,
        )
      ).status,
    ).toBe(403);
    const audit =
      await db`SELECT action,actor FROM audit_events WHERE resource_id='gateway:extra' ORDER BY created_at`;
    expect(audit.map((a: { action: string }) => a.action)).toEqual([
      "internal_identity_provision",
      "internal_identity_security_revoke",
    ]);
    expect(audit.every((a: { actor: string }) => a.actor === new URL(f.deployerUrl).username)).toBe(
      true,
    );
  }));

test("slow body progress timeout closes only its stream and preserves later work", () =>
  withIdentity(
    async (f) => {
      const cert = f.certificates.get("gateway")!,
        session = h2Connect(`https://127.0.0.1:${f.a.port}`, {
          servername: "localhost",
          ca: f.ca,
          key: cert.key,
          cert: cert.cert,
        });
      session.on("error", () => {});
      try {
        const rejected = await new Promise<{ status: number; body: string }>((resolve) => {
          const request = session.request({
            ":authority": `localhost:${f.a.port}`,
            ":method": "POST",
            ":path": "/_internal/core/v1/identity/challenges",
            "content-type": "application/json",
            "content-length": "100",
          });
          let status = 0,
            body = "";
          request.on("error", () => {});
          request.on("response", (h) => (status = Number(h[":status"])));
          request.on("data", (c) => (body += c));
          request.on("end", () => resolve({ status, body }));
          request.write("{");
        });
        expect(rejected.status).toBe(400);
        expect(rejected.body).toContain("internal_request_invalid");
      } finally {
        session.destroy();
      }
      const c = await f.connection(),
        identity = await registeredIdentity(c, f.a.port),
        command = currentCommand(identity);
      expect(
        (
          await identityRequest(
            c,
            f.a.port,
            "/_internal/core/v1/identity/current-session",
            command.body,
            command.header,
          )
        ).status,
      ).toBe(200);
    },
    { limits: { progressMs: 100, bodyMs: 300 } },
  ));

test("native session memory credit refuses a second stream while the first bounded response is flow controlled", () =>
  withIdentity(async (f) => {
    const cert = f.certificates.get("coreA")!,
      gateway = f.certificates.get("gateway")!;
    let calls = 0;
    const server = await listenBoundedH2(
      {
        hostname: "127.0.0.1",
        port: 0,
        key: cert.key,
        cert: cert.cert,
        ca: f.ca,
        limits: { responseBytes: 8 * 1024 * 1024, drainMs: 100, idleMs: 2000, lifetimeMs: 5000 },
      },
      async (stream) => {
        calls++;
        h2JsonReply(stream, 200, { synthetic: "a".repeat(5 * 1024 * 1024) }, 8 * 1024 * 1024);
      },
    );
    const raw = await openRawH2({
      port: server.port,
      ca: f.ca,
      key: gateway.key,
      cert: gateway.cert,
    });
    try {
      raw.socket.write(
        Buffer.concat([
          h2Frame(1, 4, 1, identityHeaders(server.port)),
          h2Frame(0, 1, 1, Buffer.from("{}")),
        ]),
      );
      for (let i = 0; i < 100 && !raw.frames.some((f) => f.type === 0 && f.stream === 1); i++)
        await Bun.sleep(5);
      raw.socket.write(
        Buffer.concat([
          h2Frame(1, 4, 3, identityHeaders(server.port)),
          h2Frame(0, 1, 3, Buffer.from("{}")),
        ]),
      );
      for (let i = 0; i < 100 && !raw.frames.some((f) => f.stream === 3); i++) await Bun.sleep(5);
      expect(calls).toBe(1);
      expect(raw.frames.some((f) => f.type === 3 && f.stream === 3 && f.errorCode === 11)).toBe(
        true,
      );
    } finally {
      raw.socket.destroy();
      await boundedClose(raw.closed, 1000);
      await server.stop();
      for (const count of Object.values(server.stats())) expect(count).toBe(0);
    }
  }));

test("global connection budget includes sockets that have not started a TLS handshake", () =>
  withIdentity(
    async (f) => {
      const sockets: ReturnType<typeof netConnect>[] = [];
      try {
        for (let i = 0; i < 2; i++) {
          const socket = netConnect({ host: "127.0.0.1", port: f.a.port });
          socket.on("error", () => {});
          sockets.push(socket);
          await new Promise<void>((resolve, reject) => {
            socket.once("connect", resolve);
            socket.once("error", reject);
          });
        }
        let rejected = false;
        try {
          const excess = await f.connection();
          await identityRequest(excess, f.a.port, "/_internal/core/v1/identity/challenges", {});
        } catch {
          rejected = true;
        }
        expect(rejected).toBe(true);
      } finally {
        for (const socket of sockets) socket.destroy();
      }
      const client = await f.connection(),
        identity = await registeredIdentity(client, f.a.port),
        command = currentCommand(identity);
      expect(
        (
          await identityRequest(
            client,
            f.a.port,
            "/_internal/core/v1/identity/current-session",
            command.body,
            command.header,
          )
        ).status,
      ).toBe(200);
    },
    { limits: { connections: 3 } },
  ));

test("actual shared receive budget caps reserved bytes before stream count and returns all resources on drain", () =>
  withIdentity(async (f) => {
    const cert = f.certificates.get("coreA")!,
      gateway = f.certificates.get("gateway")!;
    const server = await listenBoundedH2(
      {
        hostname: "127.0.0.1",
        port: 0,
        key: cert.key,
        cert: cert.cert,
        ca: f.ca,
        limits: { streams: 4, receiveBytes: 16384, drainMs: 100 },
      },
      async (stream, headers, signal, limits) => {
        try {
          await readBoundedData(stream, headers, limits, signal);
          h2JsonReply(stream, 200, {});
        } catch {
          h2JsonReply(stream, 400, { error: "cancelled" });
        }
      },
    );
    const session = h2Connect(`https://127.0.0.1:${server.port}`, {
      servername: "localhost",
      ca: f.ca,
      key: gateway.key,
      cert: gateway.cert,
    });
    session.on("error", () => {});
    const held: ReturnType<typeof session.request>[] = [];
    try {
      await new Promise<void>((resolve, reject) => {
        session.once("connect", resolve);
        session.once("error", reject);
      });
      for (let i = 0; i < 2; i++) {
        const s = session.request({
          ":authority": `localhost:${server.port}`,
          ":method": "POST",
          ":path": "/bounded",
          "content-length": "100",
        });
        s.on("error", () => {});
        s.write("{");
        held.push(s);
      }
      for (let i = 0; i < 100 && server.stats().streams < 2; i++) await Bun.sleep(2);
      const peak = server.stats();
      expect(peak.streams).toBe(2);
      expect(peak.reservedBytes).toBe(16384);
      const rejected = await new Promise<number>((resolve, reject) => {
        const s = session.request({
          ":authority": `localhost:${server.port}`,
          ":method": "POST",
          ":path": "/bounded",
        });
        s.once("response", (h) => resolve(Number(h[":status"])));
        s.on("error", reject);
        s.resume();
        s.end("{}");
      });
      expect(rejected).toBe(429);
      expect(server.stats().reservedBytes).toBe(16384);
      await appendFile(
        identityRegistry,
        JSON.stringify({ event: "measured_receive_budget", peak }) + "\n",
      );
    } finally {
      for (const s of held) s.close();
      session.destroy();
      await server.stop();
      const after = server.stats();
      for (const count of Object.values(after)) expect(count).toBe(0);
      await appendFile(
        identityRegistry,
        JSON.stringify({ event: "measured_receive_budget_stopped", resources: after }) + "\n",
      );
    }
  }));

test("legacy runtime role grants cannot expose identity tables or registration functions", () =>
  withIdentity(async (_f, db) => {
    await db.unsafe(await Bun.file("infra/runtime-role.sql").text());
    for (const role of ["gateway_runtime", "gateway_worker"]) {
      const connection = await db.reserve();
      try {
        await connection.unsafe(`SET ROLE ${role}`);
        for (const table of [
          "internal_service_instances",
          "internal_service_certificates",
          "internal_startup_registrations",
          "internal_startup_sessions",
        ])
          await expect(connection.unsafe(`SELECT * FROM ${table}`)).rejects.toMatchObject({
            errno: "42501",
          });
        await expect(
          connection`SELECT internal_identity_read_peer_v1(repeat('a',64))`,
        ).rejects.toMatchObject({ errno: "42501" });
      } finally {
        await connection.unsafe("RESET ROLE");
        await connection.release();
      }
    }
  }));

test("Core RPC rejects a CA-valid hostname-valid server when the provisioned pin or complete SAN differs", () =>
  withIdentity(async (f) => {
    const gateway = f.certificates.get("gateway")!,
      core = f.certificates.get("coreA")!;
    for (const peer of [
      {
        expectedPeerSha256: "0".repeat(64),
        expectedSubjectAltname: core.x509.subjectAltName ?? "",
      },
      {
        expectedPeerSha256: sha256(core.x509.raw),
        expectedSubjectAltname: "URI:spiffe://synthetic/core-api/coreA",
      },
    ]) {
      const client = createCoreRpcClient({
        origin: `https://127.0.0.1:${f.a.port}`,
        servername: "localhost",
        key: gateway.key,
        cert: gateway.cert,
        ca: f.ca,
        audience: identityAudience,
        ...peer,
      });
      try {
        await expect(client.startup()).rejects.toThrow("Internal transport unavailable");
      } finally {
        await client.close();
      }
    }
  }));

test("another workload certificate from the same CA and DNS cannot receive even one Core command stream", () =>
  withIdentity(async (f) => {
    const gateway = f.certificates.get("gateway")!,
      core = f.certificates.get("coreA")!;
    let received = 0;
    const impostor = await listenBoundedH2(
      {
        hostname: "127.0.0.1",
        port: 0,
        key: gateway.key,
        cert: gateway.cert,
        ca: f.ca,
        limits: { drainMs: 100 },
      },
      async (stream) => {
        received++;
        h2JsonReply(stream, 400, { error: "unexpected_command" });
      },
    );
    try {
      for (const expectedPeerSha256 of [sha256(core.x509.raw), sha256(gateway.x509.raw)]) {
        const client = createCoreRpcClient({
          origin: `https://127.0.0.1:${impostor.port}`,
          servername: "localhost",
          key: gateway.key,
          cert: gateway.cert,
          ca: f.ca,
          audience: identityAudience,
          expectedPeerSha256,
          expectedSubjectAltname: core.x509.subjectAltName ?? "",
        });
        try {
          await expect(client.startup()).rejects.toThrow("Internal transport unavailable");
        } finally {
          await client.close();
        }
      }
    } finally {
      await impostor.stop();
    }
    expect(received).toBe(0);
    for (const value of Object.values(impostor.stats())) expect(value).toBe(0);
  }));
