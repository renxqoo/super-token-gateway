import { SQL } from "bun";
import { X509Certificate, randomBytes } from "node:crypto";
import type { ServerHttp2Stream, IncomingHttpHeaders } from "node:http2";
import {
  startupChallengeRequestSchema,
  startupRegistrationRequestSchema,
  internalProofSchema,
  internalCurrentRequestSchema,
  executionEnvelopeSchema,
  executionReceiptSchema,
  executionRejectionCodeSchema,
  executionRejectionStatus,
  internalErrorSchema,
  executionDataSchemas,
  type ExecutionKind,
  registrationProofFields,
  commandProofFields,
  type CoreVerifiedPeerProjection,
  type CoreVerifiedCallerProjection,
} from "@gateway/contracts";
import { canonical } from "@gateway/kernel";
import { ExecutionOutcomeUnknown } from "@gateway/storage/execution";
import { createInternalIdentityStore } from "@gateway/storage/internal-identity";
import {
  canonicalEd25519Spki,
  decodeCanonicalBase64url,
  sha256,
  verifyEd25519Proof,
} from "@gateway/runtime/crypto";
import {
  listenBoundedH2,
  readBoundedData,
  readH2Peer,
  h2JsonReply,
  type H2Limits,
} from "@gateway/runtime/h2";
import { createCoreRpcClient } from "@gateway/core-client";

import { createExecutionHandler, type ExecutionOptions } from "../execution/handler.ts";

type Options = {
  execution?: ExecutionOptions;
  databaseUrl: string;
  audience: string;
  hostname: string;
  servername: string;
  port: number;
  key: string | Buffer;
  cert: string | Buffer;
  ca: string | Buffer;
  clientKey: string | Buffer;
  clientCert: string | Buffer;
  limits?: Partial<H2Limits>;
  challengeTtlSeconds?: number;
  sessionTtlSeconds?: number;
};
function reject(code: string): never {
  throw new Error(code);
}
export async function startCoreInternalListener(options: Options) {
  if (!/^[A-Za-z0-9_.-]{1,100}\/core-api$/.test(options.audience))
    throw new Error("Invalid audience");
  const challengeTtl = options.challengeTtlSeconds ?? 30,
    sessionTtl = options.sessionTtlSeconds ?? 86400;
  if (
    !Number.isInteger(challengeTtl) ||
    challengeTtl < 1 ||
    challengeTtl > 300 ||
    !Number.isInteger(sessionTtl) ||
    sessionTtl < 1 ||
    sessionTtl > 86400
  )
    throw new Error("Invalid identity TTL");
  const db = new SQL(options.databaseUrl, {
    max: 4,
    connectionTimeout: 3,
    idleTimeout: 30,
    connection: {
      statement_timeout: 4000,
      lock_timeout: 1000,
      application_name: "core-internal-identity",
    },
  });
  const store = createInternalIdentityStore(db);
  type Peer = NonNullable<Awaited<ReturnType<typeof store.readPeerDirectory>>>;
  type Session = NonNullable<Awaited<ReturnType<typeof store.readStartupSession>>>;
  type Snapshot =
    | { scope: "peer"; peer: Peer; endpoint: "challenges" | "registrations"; rawDigest: string }
    | {
        scope: "command";
        peer: Peer;
        session: Session;
        proof: ReturnType<typeof internalProofSchema.parse>;
        rawDigest: string;
        endpoint: "current-session" | ExecutionKind;
      };
  const contexts = new WeakMap<object, Snapshot>();
  let activeContexts = 0;
  function mint(snapshot: Snapshot) {
    const context = Object.freeze({});
    contexts.set(context, snapshot);
    activeContexts++;
    return context;
  }
  function requirePeer(
    context: object,
    endpoint: "challenges" | "registrations",
  ): CoreVerifiedPeerProjection {
    const s = contexts.get(context);
    if (!s || s.scope !== "peer" || s.endpoint !== endpoint)
      return reject("internal_peer_forbidden");
    return {
      service: s.peer.service,
      instance_id: s.peer.instance_id,
      certificate_sha256: s.peer.certificate_sha256,
      instance_revision: s.peer.instance_revision,
      certificate_revision: s.peer.certificate_revision,
      raw_body_sha256: s.rawDigest,
      endpoint,
    };
  }
  function requireCaller(context: object): CoreVerifiedCallerProjection {
    const s = contexts.get(context);
    if (!s || s.scope !== "command" || s.scope !== "command")
      return reject("internal_peer_forbidden");
    return {
      service: s.peer.service,
      instance_id: s.peer.instance_id,
      certificate_sha256: s.peer.certificate_sha256,
      instance_revision: s.peer.instance_revision,
      certificate_revision: s.peer.certificate_revision,
      session_id: s.session.session_id,
      incarnation_id: s.session.incarnation_id,
      key_sha256: s.session.public_key_sha256,
      session_revision: s.session.session_revision,
      command_type: s.proof.command_type,
      command_id: s.proof.command_id,
      method: "POST",
      normalized_path:
        s.endpoint === "current-session"
          ? "/_internal/core/v1/identity/current-session"
          : executionPaths[s.endpoint],
      raw_body_sha256: s.rawDigest,
      deadline_at: s.proof.deadline_at,
      trace_id: s.proof.trace_id,
    };
  }
  const executionPaths: Record<ExecutionKind, string> = {
    execution_context_v1: "/_internal/core/v1/context",
    execution_admit_v1: "/_internal/core/v1/admissions",
    execution_next_v1: "/_internal/core/v1/attempts",
    execution_begin_v1: "/_internal/core/v1/begin",
    execution_abort_v1: "/_internal/core/v1/abort",
    execution_query_v1: "/_internal/core/v1/commands",
    execution_quota_v1: "/_internal/core/v1/quota",
    execution_quota_recovery_v1: "/_internal/core/v1/quota-recovery",
    execution_abort_recovery_v1: "/_internal/core/v1/abort-recovery",
  };
  const execution = options.execution
    ? createExecutionHandler(db, options.execution, requireCaller)
    : undefined;
  async function authenticatePeer(
    stream: ServerHttp2Stream,
    requestStore: ReturnType<typeof createInternalIdentityStore>,
  ) {
    let observed: ReturnType<typeof readH2Peer>;
    try {
      observed = readH2Peer(stream);
    } catch {
      return reject("internal_peer_forbidden");
    }
    const peer = await requestStore.readPeerDirectory(sha256(observed.leaf_der));
    if (
      !peer ||
      peer.subject_altname_exact !== observed.subject_altname_exact ||
      !observed.subject_altname_exact.split(", ").includes(`URI:${peer.uri_san}`) ||
      Date.parse(peer.not_before) !== observed.not_before ||
      Date.parse(peer.not_after) !== observed.not_after
    )
      return reject("internal_peer_forbidden");
    if (peer.security_revoked_at || peer.instance_security_revoked_at)
      return reject("internal_identity_revoked");
    if (!peer.instance_enabled || peer.retired_at || Date.parse(peer.not_after) <= Date.now())
      return reject("internal_peer_forbidden");
    return peer;
  }
  async function onStream(
    stream: ServerHttp2Stream,
    headers: IncomingHttpHeaders,
    signal: AbortSignal,
    limits: H2Limits,
  ) {
    let context: object | undefined,
      committedBeginResponse = false,
      raw: Buffer | undefined,
      reserved: Awaited<ReturnType<typeof db.reserve>> | undefined;
    try {
      const routes: Record<
        string,
        "challenges" | "registrations" | "current-session" | ExecutionKind
      > = {
        ...(execution
          ? Object.fromEntries(
              Object.entries(executionPaths).map(([kind, path]) => [path, kind as ExecutionKind]),
            )
          : {}),
        "/_internal/core/v1/identity/challenges": "challenges",
        "/_internal/core/v1/identity/registrations": "registrations",
        "/_internal/core/v1/identity/current-session": "current-session",
      };
      const endpoint = routes[String(headers[":path"])];
      const authority =
        options.port === 443 ? options.servername : `${options.servername}:${listener.port}`;
      if (
        !endpoint ||
        headers[":method"] !== "POST" ||
        headers[":scheme"] !== "https" ||
        headers[":authority"] !== authority ||
        headers["content-type"] !== "application/json" ||
        (headers["content-encoding"] !== undefined && headers["content-encoding"] !== "identity") ||
        Object.keys(headers).some(
          (name) =>
            ![
              ":method",
              ":scheme",
              ":authority",
              ":path",
              "content-type",
              "content-length",
              "content-encoding",
              "x-stg-proof",
            ].includes(name),
        )
      )
        return reject("internal_request_invalid");
      try {
        raw = await readBoundedData(stream, headers, limits, signal);
      } catch (error) {
        return reject(
          error instanceof Error && error.message === "body_budget"
            ? "internal_body_too_large"
            : "internal_request_invalid",
        );
      }
      if (signal.aborted) return reject("internal_deadline");
      reserved = await db.reserve({ signal });
      const requestStore = createInternalIdentityStore(reserved, signal);
      const peer = await authenticatePeer(stream, requestStore);
      const rawDigest = sha256(raw);
      let parsed: unknown;
      try {
        parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw));
      } catch {
        return reject("internal_request_invalid");
      }
      if (endpoint === "challenges") {
        if (headers["x-stg-proof"] !== undefined) return reject("internal_request_invalid");
        const request = startupChallengeRequestSchema.safeParse(parsed);
        if (!request.success) return reject("internal_request_invalid");
        let spki: Buffer, boot: Buffer;
        try {
          spki = canonicalEd25519Spki(request.data.public_key_spki);
          boot = decodeCanonicalBase64url(request.data.boot_nonce, 32);
        } catch {
          return reject("internal_request_invalid");
        }
        context = mint({ scope: "peer", peer, endpoint, rawDigest });
        const digest = sha256(
          JSON.stringify([
            "stg.internal.startup-request",
            "1",
            options.audience,
            peer.service,
            peer.instance_id,
            peer.certificate_sha256,
            request.data.registration_id,
            sha256(spki),
            request.data.boot_nonce,
          ]),
        );
        const result = await requestStore.challenge(
          requirePeer(context, endpoint),
          {
            registration_id: request.data.registration_id,
            public_key_spki: spki,
            boot_nonce: boot,
          },
          options.audience,
          digest,
          randomBytes(32),
          challengeTtl,
        );
        h2JsonReply(stream, 200, result, limits.responseBytes);
      } else if (endpoint === "registrations") {
        if (headers["x-stg-proof"] !== undefined) return reject("internal_request_invalid");
        const request = startupRegistrationRequestSchema.safeParse(parsed);
        if (!request.success) return reject("internal_request_invalid");
        const challenge = await requestStore.readRegistration(request.data.challenge_id);
        if (
          !challenge ||
          challenge.registration_id !== request.data.registration_id ||
          challenge.audience !== options.audience ||
          challenge.certificate_sha256 !== peer.certificate_sha256 ||
          challenge.service !== peer.service ||
          challenge.instance_id !== peer.instance_id ||
          !verifyEd25519Proof(
            Buffer.from(challenge.public_key_spki, "base64"),
            registrationProofFields(challenge),
            request.data.signature,
          )
        )
          return reject("internal_proof_invalid");
        context = mint({ scope: "peer", peer, endpoint, rawDigest });
        const result = await requestStore.register(
          requirePeer(context, endpoint),
          challenge.challenge_id,
          request.data.registration_id,
          challenge.registration_digest,
          sessionTtl,
        );
        h2JsonReply(stream, result.replayed ? 200 : 201, result, limits.responseBytes);
      } else {
        let proof: ReturnType<typeof internalProofSchema.parse>;
        try {
          if (typeof headers["x-stg-proof"] !== "string") return reject("internal_proof_invalid");
          proof = internalProofSchema.parse(
            JSON.parse(
              new TextDecoder("utf-8", { fatal: true }).decode(
                decodeCanonicalBase64url(headers["x-stg-proof"], 2048),
              ),
            ),
          );
        } catch {
          return reject("internal_proof_invalid");
        }
        const session = await requestStore.readStartupSession(proof.session_id);
        if (
          !session ||
          session.service !== peer.service ||
          session.instance_id !== peer.instance_id ||
          session.certificate_sha256 !== peer.certificate_sha256 ||
          session.incarnation_id !== proof.incarnation_id
        )
          return reject("internal_session_invalid");
        if (
          proof.raw_body_sha256 !== rawDigest ||
          !verifyEd25519Proof(
            Buffer.from(session.public_key_spki, "base64"),
            commandProofFields(options.audience, session, proof, "POST", String(headers[":path"])),
            proof.signature,
          )
        )
          return reject("internal_proof_invalid");
        if (
          Date.parse(proof.deadline_at) <= Date.now() ||
          Date.parse(proof.deadline_at) > Date.now() + 3600000
        )
          return reject("internal_deadline");
        if (
          proof.command_type !== (endpoint === "current-session" ? "identity_current_v1" : endpoint)
        )
          return reject("internal_proof_invalid");
        const request = (
          endpoint === "current-session"
            ? internalCurrentRequestSchema
            : (await import("zod")).z.strictObject({
                envelope: executionEnvelopeSchema,
                data: executionDataSchemas[endpoint],
              })
        ).safeParse(parsed);
        if (!request.success) return reject("internal_request_invalid");
        const e = request.data.envelope;
        if (
          e.command_id !== proof.command_id ||
          e.caller_incarnation !== proof.incarnation_id ||
          e.deadline_at !== proof.deadline_at ||
          e.trace_id !== proof.trace_id ||
          e.contract_version !== proof.contract_version
        )
          return reject("internal_proof_invalid");
        const expected = sha256(
          canonical({
            kind: proof.command_type,
            contract_version: 1,
            owner: {
              service: session.service,
              instance_id: session.instance_id,
              session_id: session.session_id,
              incarnation_id: session.incarnation_id,
            },
            deadline_at: proof.deadline_at,
            data: request.data.data,
          }),
        );
        if (expected !== e.body_digest) return reject("internal_proof_invalid");
        context = mint({ scope: "command", peer, session, proof, rawDigest, endpoint });
        if (endpoint !== "current-session") {
          await reserved.release();
          reserved = undefined;
        }
        const result =
          endpoint === "current-session"
            ? await requestStore.current(requireCaller(context))
            : await execution!.handle(context, endpoint, request.data);
        if (!signal.aborted) {
          const receipt = executionReceiptSchema.safeParse(
            (result as { receipt?: unknown }).receipt,
          );
          committedBeginResponse =
            endpoint === "execution_begin_v1" &&
            receipt.success &&
            receipt.data.command_state === "COMMITTED";
          if (
            endpoint !== "execution_query_v1" &&
            receipt.success &&
            receipt.data.command_state === "REJECTED"
          ) {
            const r = receipt.data,
              code = executionRejectionCodeSchema.parse(r.code);
            h2JsonReply(
              stream,
              executionRejectionStatus(code),
              internalErrorSchema.parse({
                code,
                category: code === "upstream_funds_unavailable" ? "DEPENDENCY" : "VALIDATION",
                retryability: "NONE",
                command_state: "REJECTED",
                ...(r.request_id ? { request_id: r.request_id } : {}),
                ...(r.attempt_id ? { attempt_id: r.attempt_id } : {}),
                message: "Internal request rejected",
              }),
              limits.responseBytes,
            );
          } else h2JsonReply(stream, 200, result, limits.responseBytes);
        }
      }
    } catch (caughtError) {
      const error = committedBeginResponse ? new ExecutionOutcomeUnknown() : caughtError;
      const message = error instanceof Error ? error.message : "";
      const codes: Record<string, number> = {
        internal_proof_invalid: 401,
        internal_session_invalid: 401,
        internal_peer_forbidden: 403,
        internal_owner_mismatch: 403,
        internal_identity_revoked: 403,
        internal_request_invalid: 400,
        registration_conflict: 409,
        startup_key_reused: 409,
        startup_boot_conflict: 409,
        internal_deadline: 408,
        internal_body_too_large: 413,
        internal_capacity: 429,
        identity_busy: 503,
        command_conflict: 409,
        quota_unavailable: 503,
        quota_release_denied: 409,
        candidate_stale: 409,
        cost_unbounded: 422,
        customer_forbidden: 403,
        context_invalid: 403,
      };
      const sqlcode = (error as { errno?: string })?.errno;
      const code = Object.hasOwn(codes, message)
        ? message
        : sqlcode === "55P03" || sqlcode === "P0503"
          ? "identity_busy"
          : error instanceof (await import("zod")).z.ZodError
            ? "internal_request_invalid"
            : "identity_store_unavailable";
      h2JsonReply(
        stream,
        codes[code] ?? 503,
        {
          code,
          category:
            code.includes("busy") || code.includes("unavailable")
              ? "DEPENDENCY"
              : code.includes("invalid") || code.includes("large")
                ? "VALIDATION"
                : "AUTHENTICATION",
          retryability:
            code.includes("busy") || code.includes("unavailable")
              ? error instanceof ExecutionOutcomeUnknown &&
                context &&
                requireCaller(context).command_type === "execution_begin_v1"
                ? "STATUS_ONLY"
                : "SAME_COMMAND"
              : "NONE",
          command_state:
            code.includes("busy") || code.includes("unavailable") ? "UNKNOWN" : "NOT_ATTEMPTED",
          message: "Internal request rejected",
        },
        limits.responseBytes,
      );
    } finally {
      raw?.fill(0);
      if (context && contexts.delete(context)) activeContexts--;
      await reserved?.release();
    }
  }
  let listener: Awaited<ReturnType<typeof listenBoundedH2>>;
  try {
    listener = await listenBoundedH2(
      {
        ...options,
        rejections: {
          capacity: {
            code: "internal_capacity",
            category: "DEPENDENCY",
            retryability: "SAME_COMMAND",
            command_state: "NOT_ATTEMPTED",
            message: "Internal request rejected",
          },
          headers: {
            code: "internal_request_invalid",
            category: "VALIDATION",
            retryability: "NONE",
            command_state: "NOT_ATTEMPTED",
            message: "Internal request rejected",
          },
          failure: {
            code: "identity_store_unavailable",
            category: "DEPENDENCY",
            retryability: "SAME_COMMAND",
            command_state: "UNKNOWN",
            message: "Internal request rejected",
          },
        },
      },
      onStream,
    );
  } catch (error) {
    execution?.close();
    await db.close();
    throw error;
  }
  const certificate = new X509Certificate(options.cert);
  const loopbackHost =
    options.hostname === "0.0.0.0"
      ? "127.0.0.1"
      : options.hostname === "::"
        ? "[::1]"
        : options.hostname.includes(":")
          ? `[${options.hostname}]`
          : options.hostname;
  const startup = createCoreRpcClient({
    origin: `https://${loopbackHost}:${listener.port}`,
    servername: options.servername,
    key: options.clientKey,
    cert: options.clientCert,
    ca: options.ca,
    expectedPeerSha256: sha256(certificate.raw),
    expectedSubjectAltname: certificate.subjectAltName ?? "",
    audience: options.audience,
  });
  try {
    const self = await startup.startup();
    if (self.service !== "core-api") throw new Error("Core startup role mismatch");
    return {
      port: listener.port,
      session: self,
      stats: () => ({ ...listener.stats(), contexts: activeContexts }),
      async stop() {
        await startup.close();
        await listener.stop();
        execution?.close();
        await db.close();
      },
    };
  } catch (error) {
    await startup.close();
    await listener.stop();
    execution?.close();
    await db.close();
    throw error;
  }
}
