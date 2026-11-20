import { expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { X509Certificate } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CoreRpcError, createCoreRpcClient } from "@gateway/core-client";
import { sha256 } from "@gateway/runtime/crypto";
import { h2ClientBudget, h2JsonReply, listenBoundedH2, readBoundedData } from "@gateway/runtime/h2";

test("cancelling before a Core grant is dispatched frees its queue and handshake positions while dispatched grants still return receipts", async () => {
  const directory = await mkdtemp(join(tmpdir(), "gateway-h2-dispatch-"));
  const held = Promise.withResolvers<void>();
  const releaseOne = Promise.withResolvers<void>();
  const releaseAll = Promise.withResolvers<void>();
  const received: string[] = [];
  const audience = "queue-test/core-api";
  let blockers = 0;
  let client: ReturnType<typeof createCoreRpcClient> | undefined;
  let server: Awaited<ReturnType<typeof listenBoundedH2>> | undefined;
  try {
    execFileSync(
      "openssl",
      [
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-subj",
        "/CN=localhost",
        "-addext",
        "subjectAltName=DNS:localhost",
        "-keyout",
        "key.pem",
        "-out",
        "cert.pem",
      ],
      { cwd: directory, stdio: "pipe" },
    );
    const key = await readFile(join(directory, "key.pem"));
    const cert = await readFile(join(directory, "cert.pem"));
    const x509 = new X509Certificate(cert);
    const owner = {
      service: "gateway",
      instance_id: "queue",
      certificate_sha256: sha256(x509.raw),
      public_key_sha256: "",
    };
    server = await listenBoundedH2(
      { hostname: "127.0.0.1", port: 0, key, cert, ca: cert },
      async (stream, headers, signal, limits) => {
        const path = String(headers[":path"]);
        const body = JSON.parse(
          (await readBoundedData(stream, headers, limits, signal)).toString(),
        );
        const expires = new Date(Date.now() + 30000).toISOString();
        if (path.endsWith("/identity/challenges")) {
          owner.public_key_sha256 = sha256(Buffer.from(body.public_key_spki, "base64url"));
          h2JsonReply(stream, 200, {
            ...owner,
            audience,
            challenge_id: crypto.randomUUID(),
            registration_id: body.registration_id,
            boot_nonce: body.boot_nonce,
            server_nonce: "A".repeat(43),
            challenge_expires_at: expires,
          });
          return;
        }
        if (path.endsWith("/identity/registrations")) {
          h2JsonReply(stream, 200, {
            receipt: {
              ...owner,
              session_id: crypto.randomUUID(),
              incarnation_id: crypto.randomUUID(),
              expires_at: expires,
            },
            current: true,
            replayed: false,
          });
          return;
        }
        const label = body.envelope.trace_id;
        if (label === "hold") {
          if (++blockers === h2ClientBudget.streams) held.resolve();
          await (blockers === h2ClientBudget.streams ? releaseOne : releaseAll).promise;
        } else received.push(label);
        h2JsonReply(stream, 400, {
          code: "internal_request_invalid",
          category: "VALIDATION",
          retryability: "NONE",
          command_state: "NOT_ATTEMPTED",
          message: "Synthetic reply",
        });
      },
    );
    const rpc = createCoreRpcClient({
      origin: `https://127.0.0.1:${server.port}`,
      servername: "localhost",
      key,
      cert,
      ca: cert,
      audience,
      expectedPeerSha256: owner.certificate_sha256,
      expectedSubjectAltname: x509.subjectAltName!,
    });
    client = rpc;
    await rpc.startup();
    const digest = "a".repeat(64);
    const permit = { attempt_id: crypto.randomUUID(), permit_epoch: "1" };
    const context = {
      customer_token: "synthetic",
      ip: "127.0.0.1",
      operation: "chat",
      model: "synthetic",
      fingerprint: digest,
    };
    const send = (traceId: string, kind: Parameters<typeof rpc.execution>[0], data: unknown) =>
      rpc
        .execution(kind, data, {
          traceId,
        })
        .catch((error: unknown) => {
          if (!(error instanceof CoreRpcError)) throw error;
          return error;
        });
    const active = new AbortController();
    const begin = {
      ...permit,
      prep_session_id: crypto.randomUUID(),
      prep_binding_digest: digest,
      dispatched_digest: digest,
      authorization_plan_digest: digest,
    };
    const cancellable = (traceId: string, signal: AbortSignal) =>
      rpc
        .execution("execution_begin_v1", begin, { traceId, dispatchSignal: signal })
        .catch((error: unknown) => error);
    const holding = Promise.all(
      Array.from({ length: h2ClientBudget.streams }, () => cancellable("hold", active.signal)),
    );
    try {
      await Promise.race([held.promise, holding]);
      active.abort();
      const queued = new AbortController();
      const waiting = cancellable("cancelled-before-grant", queued.signal);
      await Bun.sleep(0);
      queued.abort();
      const beforeRelease = await Promise.race([
        waiting,
        Bun.sleep(250).then(() => "still waiting"),
      ]);
      if (beforeRelease === "still waiting") {
        releaseOne.resolve();
        await waiting;
      }
      expect(received).not.toContain("cancelled-before-grant");
      expect(beforeRelease).toMatchObject({ name: "InternalTransportCancelled" });
      const replacement = send("replacement", "execution_context_v1", context);
      releaseOne.resolve();
      await expect(replacement).resolves.toMatchObject({ status: 400 });
      expect(received).toEqual(["replacement"]);
    } finally {
      releaseOne.resolve();
      releaseAll.resolve();
      expect(
        (await holding).every((reply) => reply instanceof CoreRpcError && reply.status === 400),
      ).toBe(true);
    }
    const silent = Bun.listen({ hostname: "127.0.0.1", port: 0, socket: { data() {} } });
    const stop = new AbortController();
    let pending: Promise<unknown>[] = [];
    try {
      await rpc.reconnect({
        origin: `https://127.0.0.1:${silent.port}`,
        expectedPeerSha256: owner.certificate_sha256,
        expectedSubjectAltname: x509.subjectAltName!,
      });
      pending = Array.from({ length: h2ClientBudget.requests }, () =>
        cancellable("handshake", stop.signal),
      );
      await expect(send("overflow", "execution_context_v1", context)).resolves.toMatchObject({
        detail: { code: "internal_capacity" },
      });
      stop.abort();
      expect(
        await Promise.race([
          Promise.all(pending).then((replies) =>
            replies.every((r) => r instanceof Error && r.name === "InternalTransportCancelled"),
          ),
          Bun.sleep(250).then(() => false),
        ]),
      ).toBe(true);
      const reuse = new AbortController();
      const replacement = cancellable("handshake-replacement", reuse.signal);
      reuse.abort();
      await expect(replacement).resolves.toMatchObject({ name: "InternalTransportCancelled" });
    } finally {
      silent.stop(true);
      await Promise.allSettled(pending);
    }
  } finally {
    await client?.close();
    await server?.stop();
    await rm(directory, { recursive: true, force: true });
  }
});
