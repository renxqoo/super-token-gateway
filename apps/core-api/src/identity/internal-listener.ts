import { SQL } from "bun";
import { createIdentityReadCache } from "./read-cache.ts";
import { X509Certificate, randomBytes } from "node:crypto";
import type { ServerHttp2Stream, IncomingHttpHeaders } from "node:http2";
import {
  startupChallengeRequestSchema,
  startupRegistrationRequestSchema,
  internalProofSchema,
  internalCurrentRequestSchema,
  executionEnvelopeSchema,
  routeTicketMaxBytes,
  routeTicketSchema,
  executionReceiptSchema,
  executionRejectionCodeSchema,
  executionRejectionStatus,
  internalErrorSchema,
  executionDataSchemas,
  terminalSettlementPath,
  terminalSettlementRequestSchema,
  type CoreEvidenceGroupV1,
  pluginPackageDataSchema,
  pluginPackageMaxBytes,
  executionContextResponseMaxBytes,
  type ExecutionKind,
  registrationProofFields,
  commandProofFields,
  type CoreVerifiedPeerProjection,
  type CoreVerifiedCallerProjection,
} from "@gateway/contracts";
import { canonical } from "@gateway/kernel";
import {
  createShortChatAdmissionStore,
  createShortChatFundingScheduler,
  ExecutionOutcomeUnknown,
} from "@gateway/storage/execution";
import { createInternalIdentityStore } from "@gateway/storage/internal-identity";
import {
  createReplicatedTerminalSettlementStore,
  TerminalSettlementRejected,
  StreamTerminalDisabled,
} from "@gateway/storage/settlement";
import {
  UsageConsumer,
  UsageProducer,
  streamTerminalRecoveryChannel,
  terminalRecoveryChannel,
} from "@gateway/runtime";
import {
  canonicalEd25519Spki,
  decodeCanonicalBase64url,
  sha256,
  verifyEd25519Proof,
} from "@gateway/runtime/crypto";
import {
  listenBoundedH2,
  boundedH2Defaults,
  readBoundedData,
  readH2Peer,
  h2JsonReply,
  h2BytesReply,
  type H2Limits,
} from "@gateway/runtime/h2";
import { createCoreRpcClient } from "@gateway/core-client";

import { createExecutionHandler, type ExecutionOptions } from "../execution/handler.ts";
import type { CoreApiConfig } from "../config.ts";

type Options = {
  execution?: ExecutionOptions;
  terminalSettlement?: NonNullable<CoreApiConfig["terminal_settlement"]>;
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
const capacityRejection = {
  code: "internal_capacity",
  category: "DEPENDENCY",
  retryability: "SAME_COMMAND",
  command_state: "NOT_ATTEMPTED",
  message: "Internal request rejected",
} as const;
const identityConcurrency = 16;
const routeTicketOverhead = Buffer.byteLength(',"route_ticket":""');
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
  const databaseOptions = {
    connectionTimeout: 3,
    idleTimeout: 30,
    connection: {
      statement_timeout: 4000,
      lock_timeout: 1000,
    },
  };
  const db = new SQL(options.databaseUrl, {
    ...databaseOptions,
    max: options.terminalSettlement?.short_chat_funding_epoch ? 10 : 4,
    connection: { ...databaseOptions.connection, application_name: "core-internal-identity" },
  });
  const store = createInternalIdentityStore(db);
  const terminalConnections = options.terminalSettlement && {
    primary: new SQL(options.terminalSettlement.database_url, {
      max: options.terminalSettlement.short_chat_funding_epoch ? 8 : 4,
      connectionTimeout: 3,
      idleTimeout: 30,
      connection: { application_name: "core-terminal-writer" },
    }),
    standby: new SQL(options.terminalSettlement.standby_url, {
      max: 4,
      connectionTimeout: 3,
      idleTimeout: 30,
    }),
  };
  const fundingSchedule = createShortChatFundingScheduler();
  const terminalSettlement =
    terminalConnections &&
    createReplicatedTerminalSettlementStore(
      terminalConnections.primary,
      terminalConnections.standby,
      options.terminalSettlement?.short_chat_funding_epoch,
      fundingSchedule,
      options.terminalSettlement?.commit_short_chat_sse,
    );
  const epoch = options.terminalSettlement?.short_chat_funding_epoch;
  const shortChat =
    terminalConnections && epoch !== undefined
      ? createShortChatAdmissionStore(db, terminalConnections.standby, epoch, fundingSchedule)
      : undefined;
  const recovery = options.terminalSettlement?.recovery_queue;
  const recoveryOptions = recovery && {
    channel: terminalRecoveryChannel,
    url: recovery.url,
    namespace: recovery.namespace,
    replicas: recovery.required_replicas,
    timeoutMs: recovery.ack_timeout_ms,
    requireAllReplicas: true,
  };
  const terminalRecovery =
    recoveryOptions && new UsageProducer({ ...recoveryOptions, batchSize: 1 });
  const streamRecoveryOptions =
    recoveryOptions && options.terminalSettlement?.commit_short_chat_sse !== undefined
      ? { ...recoveryOptions, channel: streamTerminalRecoveryChannel }
      : undefined;
  const streamRecovery =
    streamRecoveryOptions && new UsageProducer({ ...streamRecoveryOptions, batchSize: 1 });
  let streamConsumer: UsageConsumer<CoreEvidenceGroupV1> | undefined;
  let recoveryConsumer: { ready: () => Promise<void>; close: () => Promise<void> } | undefined;
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
        endpoint:
          "current-session" | "plugin_package_v1" | "terminal_settlement_v1" | ExecutionKind;
      };
  const peerReads = createIdentityReadCache({
    capacity: 4096,
    maxPending: 64,
    ttlMs: 1000,
    load: (hash, signal) => createInternalIdentityStore(db, signal).readPeerDirectory(hash),
  });
  const sessionReads = createIdentityReadCache({
    capacity: 4096,
    maxPending: 64,
    ttlMs: 1000,
    load: (id, signal) => createInternalIdentityStore(db, signal).readStartupSession(id),
  });
  function invalidateIdentityReads() {
    peerReads.invalidate();
    sessionReads.invalidate();
  }
  const contexts = new WeakMap<object, Snapshot>();
  let activeContexts = 0,
    identityStreams = 0;
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
    if (!s || s.scope !== "command") return reject("internal_peer_forbidden");
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
          : s.endpoint === "plugin_package_v1"
            ? "/_internal/core/v1/plugin-package"
            : s.endpoint === "terminal_settlement_v1"
              ? terminalSettlementPath
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
    execution_resource_commit_v1: "/_internal/core/v1/resources/commit",
    execution_resource_v1: "/_internal/core/v1/resource-permits",
    execution_resource_list_v1: "/_internal/core/v1/resources/list",
    execution_resource_claim_v1: "/_internal/core/v1/resources/claim",
    execution_resource_poll_v1: "/_internal/core/v1/resources/poll-permits",
    execution_resource_release_v1: "/_internal/core/v1/resources/release",
  };
  const preflightDb = options.execution
    ? new SQL(options.databaseUrl, {
        ...databaseOptions,
        max: 2,
        connection: { ...databaseOptions.connection, application_name: "core-execution-preflight" },
      })
    : undefined;
  const execution = options.execution
    ? createExecutionHandler(db, options.execution, requireCaller, shortChat, preflightDb)
    : undefined;
  async function authenticatePeer(
    stream: ServerHttp2Stream,
    requestStore: ReturnType<typeof createInternalIdentityStore>,
    cached: boolean,
    signal: AbortSignal,
  ) {
    let observed: ReturnType<typeof readH2Peer>;
    try {
      observed = readH2Peer(stream);
    } catch {
      return reject("internal_peer_forbidden");
    }
    const hash = sha256(observed.leaf_der);
    const peer = cached
      ? await peerReads.get(hash, signal)
      : await requestStore.readPeerDirectory(hash);
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
      combinedGrant = false,
      identityCounted = false,
      raw: Buffer | undefined,
      reserved: Awaited<ReturnType<typeof db.reserve>> | undefined;
    try {
      const routes: Record<
        string,
        | "challenges"
        | "registrations"
        | "current-session"
        | "plugin_package_v1"
        | "terminal_settlement_v1"
        | ExecutionKind
      > = {
        ...(execution
          ? {
              ...Object.fromEntries(
                Object.entries(executionPaths).map(([kind, path]) => [path, kind as ExecutionKind]),
              ),
              "/_internal/core/v1/plugin-package": "plugin_package_v1" as const,
            }
          : {}),
        "/_internal/core/v1/identity/challenges": "challenges",
        "/_internal/core/v1/identity/registrations": "registrations",
        "/_internal/core/v1/identity/current-session": "current-session",
        ...(terminalSettlement
          ? { [terminalSettlementPath]: "terminal_settlement_v1" as const }
          : {}),
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
      if (
        endpoint === "challenges" ||
        endpoint === "registrations" ||
        endpoint === "current-session"
      ) {
        if (identityStreams >= identityConcurrency) {
          h2JsonReply(stream, 429, capacityRejection, limits.responseBytes);
          stream.resume();
          return;
        }
        identityStreams++;
        identityCounted = true;
      }
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
      const startupRequest = endpoint === "challenges" || endpoint === "registrations";
      if (startupRequest) reserved = await db.reserve({ signal });
      const requestStore = createInternalIdentityStore(reserved ?? db, signal);
      const peer = await authenticatePeer(stream, requestStore, !startupRequest, signal);
      signal.throwIfAborted();
      const rawDigest = sha256(raw);
      let parsed: unknown;
      try {
        parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw));
      } catch {
        return reject("internal_request_invalid");
      }
      if (endpoint === "execution_admit_v1" || endpoint === "execution_next_v1") {
        const ticket =
          parsed && typeof parsed === "object" && "route_ticket" in parsed
            ? parsed.route_ticket
            : undefined;
        const allowance =
          typeof ticket === "string" && routeTicketSchema.safeParse(ticket).success
            ? ticket.length + routeTicketOverhead
            : 0;
        if (raw.length - allowance > (options.limits?.bodyBytes ?? boundedH2Defaults.bodyBytes))
          return reject("internal_body_too_large");
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
        const session = await sessionReads.get(proof.session_id, signal);
        signal.throwIfAborted();
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
                ...(endpoint === "execution_admit_v1" || endpoint === "execution_next_v1"
                  ? { route_ticket: routeTicketSchema.optional() }
                  : {}),
                data:
                  endpoint === "plugin_package_v1"
                    ? pluginPackageDataSchema
                    : endpoint === "terminal_settlement_v1"
                      ? terminalSettlementRequestSchema
                      : executionDataSchemas[endpoint],
              })
        ).safeParse(parsed);
        if (!request.success) return reject("internal_request_invalid");
        combinedGrant =
          endpoint === "execution_admit_v1" &&
          (request.data.data as { commit_send?: boolean }).commit_send === true;
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
        if (endpoint === "terminal_settlement_v1") {
          if (session.security_revoked_at) return reject("internal_identity_revoked");
          if (session.disabled_at || Date.parse(session.expires_at) <= Date.now())
            return reject("internal_session_invalid");
          const { stream_terminal_v1: streamTerminal, ...group } =
            terminalSettlementRequestSchema.parse(request.data.data);
          if (
            peer.service !== "gateway" ||
            group.events.some(
              (event) =>
                event.origin_identity !== `${peer.service}:${peer.instance_id}` ||
                event.origin_session_id !== session.session_id ||
                event.origin_incarnation !== session.incarnation_id,
            )
          )
            return reject("internal_owner_mismatch");
          try {
            const result = await terminalSettlement!.settle(
              group,
              {
                signal,
                deadline:
                  performance.now() + Math.min(3000, Date.parse(proof.deadline_at) - Date.now()),
              },
              streamTerminal ? "online" : undefined,
            );
            if (!signal.aborted) h2JsonReply(stream, 200, result, limits.responseBytes);
          } catch (error) {
            if (error instanceof StreamTerminalDisabled)
              return reject("internal_stream_terminal_disabled");
            if (error instanceof TerminalSettlementRejected)
              return reject("internal_request_invalid");
            const writer = streamTerminal ? streamRecovery : terminalRecovery;
            if (writer) {
              await writer.ready();
              await writer.publish(group);
            }
            throw error;
          }
          return;
        }
        context = mint({ scope: "command", peer, session, proof, rawDigest, endpoint });
        if (endpoint === "plugin_package_v1") {
          const bytes = await execution!.pluginPackage(
            context,
            pluginPackageDataSchema.parse(request.data.data).digest,
          );
          if (!signal.aborted) h2BytesReply(stream, bytes, pluginPackageMaxBytes);
          return;
        }
        const result =
          endpoint === "current-session"
            ? await requestStore.current(requireCaller(context))
            : await execution!.handle(context, endpoint, request.data, signal);
        if (!signal.aborted) {
          const receipt = executionReceiptSchema.safeParse(
            (result as { receipt?: unknown }).receipt,
          );
          committedBeginResponse =
            (endpoint === "execution_begin_v1" || combinedGrant) &&
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
          } else
            h2JsonReply(
              stream,
              200,
              combinedGrant &&
                (request.data.data as { manifest?: { stream?: boolean } }).manifest?.stream ===
                  true &&
                options.terminalSettlement?.commit_short_chat_sse &&
                receipt.success &&
                receipt.data.state === "SEND_GRANTED" &&
                "transient_send" in result
                ? { ...result, stream_terminal_v1: true }
                : result,
              endpoint === "execution_context_v1"
                ? executionContextResponseMaxBytes
                : limits.responseBytes,
            );
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
        internal_stream_terminal_disabled: 409,
        registration_conflict: 409,
        startup_key_reused: 409,
        startup_boot_conflict: 409,
        internal_deadline: 408,
        internal_body_too_large: 413,
        internal_capacity: 429,
        identity_busy: 503,
        command_conflict: 409,
        quota_unavailable: 503,
        quota_limited: 429,
        channel_capacity: 429,
        quota_release_denied: 409,
        candidate_stale: 409,
        cost_unbounded: 422,
        customer_forbidden: 403,
        context_invalid: 403,
        plugin_package_unavailable: 404,
        no_route: 503,
        resource_not_found: 404,
        resource_rate_limited: 429,
        resource_provider_mismatch: 422,
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
            code.includes("busy") ||
            code.includes("unavailable") ||
            code === "no_route" ||
            code === "internal_stream_terminal_disabled"
              ? "DEPENDENCY"
              : code.includes("invalid") || code.includes("large")
                ? "VALIDATION"
                : "AUTHENTICATION",
          retryability:
            code.includes("busy") || code.includes("unavailable")
              ? error instanceof ExecutionOutcomeUnknown &&
                context &&
                (requireCaller(context).command_type === "execution_begin_v1" || combinedGrant)
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
      if (identityCounted) identityStreams--;
      if (context && contexts.delete(context)) activeContexts--;
      await reserved?.release();
    }
  }
  let listener: Awaited<ReturnType<typeof listenBoundedH2>>;
  let invalidation: Awaited<ReturnType<typeof db.listen>> | undefined;
  async function closeDependencies() {
    peerReads.close();
    sessionReads.close();
    try {
      await invalidation?.unlisten();
    } finally {
      execution?.close();
      terminalRecovery?.close();
      streamRecovery?.close();
      try {
        await Promise.all([recoveryConsumer?.close(), streamConsumer?.close()]);
      } finally {
        await Promise.all([
          db.close({ timeout: 5 }),
          preflightDb?.close({ timeout: 5 }),
          terminalConnections?.primary.close({ timeout: 1 }),
          terminalConnections?.standby.close({ timeout: 1 }),
        ]);
      }
    }
  }
  try {
    invalidation = await db.listen(
      "stg_identity",
      invalidateIdentityReads,
      invalidateIdentityReads,
    );
    listener = await listenBoundedH2(
      {
        ...options,
        extraBodyBytesByPath: {
          [executionPaths.execution_admit_v1]: routeTicketMaxBytes + routeTicketOverhead,
          [executionPaths.execution_next_v1]: routeTicketMaxBytes + routeTicketOverhead,
        },
        rejections: {
          capacity: capacityRejection,
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
    await closeDependencies();
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
    if (terminalSettlement && recoveryOptions) {
      recoveryConsumer = new UsageConsumer({
        ...recoveryOptions,
        concurrency: 1,
        process: async (groups) => {
          if (groups.length !== 1) throw new Error("TERMINAL_RECOVERY_BATCH_INVALID");
          await terminalSettlement.settle(groups[0]!, { deadline: performance.now() + 3000 });
        },
      });
      await recoveryConsumer.ready();
      if (streamRecoveryOptions) {
        streamConsumer = new UsageConsumer({
          ...streamRecoveryOptions,
          concurrency: 1,
          process: async (groups) => {
            if (groups.length !== 1) throw new Error("TERMINAL_RECOVERY_BATCH_INVALID");
            await terminalSettlement.settle(
              groups[0]!,
              { deadline: performance.now() + 3000 },
              "recovery",
            );
          },
        });
        await Promise.all([streamConsumer.ready(), streamRecovery!.ready()]);
      }
    }
    return {
      port: listener.port,
      session: self,
      stats: () => ({ ...listener.stats(), contexts: activeContexts }),
      async stop() {
        await startup.close();
        await listener.stop();
        await closeDependencies();
      },
    };
  } catch (error) {
    await startup.close();
    await listener.stop();
    await closeDependencies();
    throw error;
  }
}
