import { expect, test } from "bun:test";
import { connect, constants } from "node:http2";
import { startCoreInternalListener } from "../../apps/core-api/src/identity/internal-listener.ts";
import { eventually } from "../support/gateway-process.ts";
import {
  currentCommand,
  identityAudience,
  identityRequest,
  registeredIdentity,
  withIdentity,
} from "../support/internal-identity.ts";

for (const layer of ["session", "certificate", "instance"] as const)
  test(`a cached internal identity still rejects ${layer} revocation when invalidation notifications are lost`, () =>
    withIdentity(async (fixture, db) => {
      for (const [table, trigger] of [
        ["internal_service_instances", "identity_instance_notify_v2"],
        ["internal_service_certificates", "identity_certificate_notify_v2"],
        ["internal_startup_sessions", "identity_session_notify_v2"],
      ])
        await db.unsafe(`ALTER TABLE public.${table} DISABLE TRIGGER ${trigger}`);
      const client = await fixture.connection();
      const identity = await registeredIdentity(client, fixture.a.port);
      const path = "/_internal/core/v1/identity/current-session";
      async function current() {
        const command = currentCommand(identity);
        return identityRequest(client, fixture.a.port, path, command.body, command.header);
      }
      expect((await current()).status).toBe(200);
      await db.unsafe(
        `CREATE OR REPLACE FUNCTION public.internal_identity_read_peer_v1(hash text) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$ BEGIN RAISE EXCEPTION 'identity_read_tripwire';END $$`,
      );
      await db.unsafe(
        `CREATE OR REPLACE FUNCTION public.internal_identity_read_session_v1(id uuid) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$ BEGIN RAISE EXCEPTION 'identity_read_tripwire';END $$`,
      );
      expect((await current()).status).toBe(200);
      await db`SELECT internal_identity_change_v1('gateway','gatewayA',${layer === "instance" ? null : identity.receipt.certificate_sha256},${layer === "session" ? identity.receipt.session_id : null}::uuid,'SECURITY_REVOKE')`;
      const revoked = await current();
      expect(revoked.status).toBe(403);
      expect(revoked.value.code).toBe("internal_identity_revoked");
      expect((await db`SELECT count(*)::int n FROM execution_commands`)[0].n).toBe(0);
    }));

test("cancelling a cold identity stream releases its budget while a shared PG lookup remains blocked for another stream", () =>
  withIdentity(async (fixture, db) => {
    const core = fixture.certificates.get("coreA")!;
    const gateway = fixture.certificates.get("gateway")!;
    const server = await startCoreInternalListener({
      databaseUrl: fixture.apiUrl,
      audience: identityAudience,
      hostname: "127.0.0.1",
      servername: "localhost",
      port: 0,
      key: core.key,
      cert: core.cert,
      ca: fixture.ca,
      clientKey: core.key,
      clientCert: core.cert,
    });
    const session = connect(`https://127.0.0.1:${server.port}`, {
      servername: "localhost",
      ca: fixture.ca,
      key: gateway.key,
      cert: gateway.cert,
    });
    session.on("error", () => undefined);
    const streams: ReturnType<typeof session.request>[] = [];
    const unlock = Promise.withResolvers<void>();
    let transaction: Promise<unknown> | undefined;
    try {
      const client = await fixture.connection("gateway", server.port);
      const identity = await registeredIdentity(client, server.port);
      const command = currentCommand(identity);
      const path = "/_internal/core/v1/identity/current-session";
      expect(
        (await identityRequest(client, server.port, path, command.body, command.header)).status,
      ).toBe(200);
      const ready = Promise.withResolvers<void>();
      transaction = db.begin(async (tx) => {
        await tx`LOCK TABLE internal_startup_sessions IN ACCESS EXCLUSIVE MODE`;
        ready.resolve();
        await unlock.promise;
      });
      await Promise.race([ready.promise, transaction]);
      const proof = Buffer.from(
        JSON.stringify({ ...command.proof, session_id: crypto.randomUUID() }),
      ).toString("base64url");
      const body = Buffer.from(JSON.stringify(command.body));
      function send() {
        const stream = session.request({
          ":authority": `localhost:${server.port}`,
          ":method": "POST",
          ":path": path,
          "content-type": "application/json",
          "content-length": String(body.length),
          "x-stg-proof": proof,
        });
        streams.push(stream);
        const response = new Promise<number>((resolve, reject) => {
          let status = 0;
          stream.once("response", (headers) => {
            status = Number(headers[":status"]);
          });
          stream.once("end", () => resolve(status));
          stream.once("error", reject);
          stream.once("close", () => {
            if (!stream.readableEnded) reject(new Error("stream cancelled"));
          });
        });
        stream.resume();
        stream.end(body);
        return { stream, response };
      }
      const first = send();
      const firstResponse = first.response.catch(() => 0);
      const second = send();
      const secondResponse = second.response.catch(() => 0);
      await eventually(
        async () => server.stats().streams === 2,
        "two identity streams active",
        1000,
      );
      await eventually(
        async () =>
          (
            await db`SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query LIKE 'SELECT internal_identity_read_session_v1%' AND wait_event_type='Lock'`
          ).length > 0,
        "shared identity lookup blocked",
        1000,
      );
      first.stream.close(constants.NGHTTP2_CANCEL);
      await firstResponse;
      await eventually(
        async () => server.stats().streams === 1 && server.stats().tasks === 1,
        "cancelled identity waiter released",
        500,
      );
      expect(server.stats().reservedBytes).toBeGreaterThan(0);
      unlock.resolve();
      await transaction;
      expect(await secondResponse).toBe(401);
      await eventually(
        async () => server.stats().streams === 0 && server.stats().tasks === 0,
        "identity stream budgets released",
        1000,
      );
      expect(server.stats().reservedBytes).toBe(0);
      await client.close();
    } finally {
      unlock.resolve();
      const results = await Promise.allSettled([
        transaction,
        Promise.resolve().then(() => {
          for (const stream of streams) stream.close();
          session.destroy();
        }),
        server.stop(),
      ]);
      expect(results.every((result) => result.status === "fulfilled")).toBe(true);
      for (const value of Object.values(server.stats())) expect(value).toBe(0);
    }
  }));
