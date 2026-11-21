import { generateKeyPairSync, randomBytes, randomUUID, sign } from "node:crypto";
import {
  executionDataSchemas,
  executionContextResponseSchema,
  resourceCommitResponseSchema,
  resourceListResponseSchema,
  resourceClaimResponseSchema,
  resourcePollResponseSchema,
  resourceReleaseResponseSchema,
  resourcePermitResponseSchema,
  pluginPackageDataSchema,
  pluginPackageMaxBytes,
  executionContextResponseMaxBytes,
  executionReceiptSchema,
  transientSendSchema,
  type ExecutionKind,
  startupChallengeSchema,
  startupRegistrationResponseSchema,
  internalCurrentResponseSchema,
  internalErrorSchema,
  registrationProofFields,
  commandProofFields,
  coreEvidenceEventV1Schema,
  coreEvidenceGroupV1Schema,
  shortChatFundingGroup,
  terminalSettlementPath,
  terminalSettlementResponseSchema,
  evidenceProofFields,
  type CoreEvidenceEventV1,
  type CoreEvidenceGroupV1,
  type SignedCoreEvidenceEventV1,
  type StartupReceipt,
} from "@gateway/contracts";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime/crypto";
import {
  boundedH2Defaults,
  createBoundedH2Client,
  h2ClientBudget,
  InternalTransportBusy,
  InternalTransportCancelled,
  InternalTransportDeadline,
  InternalTransportRefused,
} from "@gateway/runtime/h2";

export { InternalTransportCancelled as CoreRpcDispatchCancelled };

export class CoreRpcError extends Error {
  constructor(
    readonly detail: ReturnType<typeof internalErrorSchema.parse>,
    readonly status?: number,
  ) {
    super(detail.code);
    this.name = "CoreRpcError";
  }
  get retryableAdmissionBusy() {
    return (
      this.status !== undefined &&
      this.detail.retryability === "SAME_COMMAND" &&
      (this.detail.code === "internal_capacity" || this.detail.code === "identity_busy")
    );
  }
}
type CoreTarget = { origin: string; expectedPeerSha256: string; expectedSubjectAltname: string };
function requireTarget(target: CoreTarget) {
  if (
    typeof target.expectedPeerSha256 !== "string" ||
    !/^[a-f0-9]{64}$/.test(target.expectedPeerSha256) ||
    typeof target.expectedSubjectAltname !== "string" ||
    !target.expectedSubjectAltname ||
    target.expectedSubjectAltname.length > 2000
  )
    throw new Error("Core target pin required");
  const url = new URL(target.origin);
  if (url.protocol !== "https:" || url.origin !== target.origin)
    throw new Error("Invalid Core target");
}
export const coreAudiencePattern = /^[A-Za-z0-9_.-]{1,100}\/core-api$/;

export function createCoreRpcClient(
  options: Parameters<typeof createBoundedH2Client>[0] &
    CoreTarget & {
      audience: string;
      connections?: number;
      shortChatPeer?: CoreTarget & { servername: string; fundingGroupParity: 0 | 1 };
    },
) {
  requireTarget(options);
  if (!coreAudiencePattern.test(options.audience)) throw new Error("Invalid Core audience");
  const key = generateKeyPairSync("ed25519"),
    registrationId = randomUUID(),
    bootNonce = randomBytes(32).toString("base64url"),
    spki = key.publicKey.export({ format: "der", type: "spki" }).toString("base64url");
  type Transport = Awaited<ReturnType<typeof createBoundedH2Client>>;
  const count = options.connections ?? 1;
  if (!Number.isInteger(count) || count < 1 || count > 4)
    throw new Error("Invalid Core connection count");
  const peer = options.shortChatPeer;
  if (peer) {
    requireTarget(peer);
    if (
      count !== 4 ||
      peer.origin === options.origin ||
      !peer.servername ||
      (peer.fundingGroupParity !== 0 && peer.fundingGroupParity !== 1)
    )
      throw new Error("Invalid short Chat Core routing");
  }
  type Lane = {
    load: number;
    client?: Transport | undefined;
    connecting?: { task: Promise<Transport>; waiting: Set<() => void> } | undefined;
  };
  function pool(size: number) {
    const lanes = Array.from({ length: size }, (): Lane => ({ load: 0 }));
    return { lanes, ordinary: size > 1 ? lanes.slice(0, -1) : lanes, terminal: lanes[size - 1]! };
  }
  const primary = pool(peer ? 2 : count);
  const secondary = peer && pool(2);
  const transports = [...primary.lanes, ...(secondary?.lanes ?? [])];
  let receipt: StartupReceipt | undefined,
    challenge: ReturnType<typeof startupChallengeSchema.parse> | undefined;
  let starting: Promise<StartupReceipt> | undefined,
    closed = false,
    generation = 0;
  async function connection(
    lane: Lane,
    target: CoreTarget & { servername: string },
    dispatchDeadlineMs?: number,
    dispatchSignal?: AbortSignal,
  ) {
    if (closed) throw new Error("Client closed");
    if (lane.client?.stats().closed) {
      const previous = lane.client;
      lane.client = undefined;
      void previous.retire().catch(() => {});
    }
    if (lane.client) return lane.client;
    if (!lane.connecting) {
      const captured = generation;
      const waiting = new Set<() => void>();
      const task = createBoundedH2Client({ ...options, ...target })
        .then(async (client) => {
          if (closed || captured !== generation) {
            await client.close();
            throw new Error("Client closed");
          }
          lane.client = client;
          return client;
        })
        .finally(() => {
          lane.connecting = undefined;
          for (const resume of waiting) resume();
        });
      void task.catch(() => {});
      lane.connecting = { task, waiting };
    }
    const { task, waiting } = lane.connecting;
    if (dispatchDeadlineMs === undefined && !dispatchSignal) return task;
    return new Promise<Transport>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      function cleanup() {
        clearTimeout(timer);
        waiting.delete(resume);
        dispatchSignal?.removeEventListener("abort", cancel);
      }
      function resume() {
        cleanup();
        resolve(task);
      }
      function cancel() {
        cleanup();
        reject(new InternalTransportCancelled());
      }
      if (dispatchSignal?.aborted) return cancel();
      waiting.add(resume);
      dispatchSignal?.addEventListener("abort", cancel, { once: true });
      if (dispatchDeadlineMs !== undefined)
        timer = setTimeout(
          () => {
            cleanup();
            reject(new InternalTransportDeadline());
          },
          Math.max(0, dispatchDeadlineMs - Date.now()),
        );
    });
  }
  async function releaseConnections() {
    await Promise.all(
      transports.map(async (lane) => {
        const previous = lane.client;
        lane.client = undefined;
        await previous?.close();
        await lane.connecting?.task.catch(() => {});
      }),
    );
  }
  async function send(
    path: string,
    prepare: () => { body: Buffer; proof?: string | undefined },
    reply?: {
      maxBytes?: number;
      timeoutMs?: number;
      binary?: boolean;
      shortChatProject?: string | undefined;
      dispatchDeadlineMs?: number;
      dispatchSignal?: AbortSignal;
    },
  ) {
    const usePeer =
      peer &&
      secondary &&
      reply?.shortChatProject !== undefined &&
      shortChatFundingGroup(reply.shortChatProject) % 2 === peer.fundingGroupParity;
    const target = usePeer ? peer : options;
    const routes = usePeer ? secondary : primary;
    const port = new URL(target.origin).port;
    const authority = port ? `${target.servername}:${port}` : target.servername;
    const frame = () => {
      const { body, proof } = prepare();
      return {
        body,
        headers: {
          ":authority": authority,
          ":method": "POST",
          ":path": path,
          "content-type": "application/json",
          "content-length": String(body.length),
          ...(proof ? { "x-stg-proof": proof } : {}),
        },
      };
    };
    const dispatchDeadlineMs = reply?.dispatchDeadlineMs;
    for (let retried = false; ; retried = true) {
      try {
        if (reply?.dispatchSignal?.aborted) throw new InternalTransportCancelled();
        if (dispatchDeadlineMs !== undefined && Date.now() >= dispatchDeadlineMs)
          throw new InternalTransportDeadline();
        const lane =
          path === terminalSettlementPath
            ? routes.terminal
            : routes.ordinary.reduce((best, current) =>
                current.load < best.load ? current : best,
              );
        if (lane.load >= h2ClientBudget.requests) throw new InternalTransportBusy();
        lane.load++;
        try {
          return await (
            await connection(lane, target, dispatchDeadlineMs, reply?.dispatchSignal)
          ).request(frame, undefined, reply);
        } finally {
          lane.load--;
        }
      } catch (error) {
        if (error instanceof InternalTransportRefused && !retried) continue;
        if (error instanceof InternalTransportDeadline)
          throw new CoreRpcError({
            code: "internal_deadline",
            category: "DEPENDENCY",
            retryability: "NONE",
            command_state: "NOT_ATTEMPTED",
            message: "Internal request deadline elapsed before dispatch",
          });
        if (!(error instanceof InternalTransportBusy)) throw error;
        throw new CoreRpcError({
          code: "internal_capacity",
          category: "DEPENDENCY",
          retryability: "SAME_COMMAND",
          command_state: "NOT_ATTEMPTED",
          message: "Internal request rejected",
        });
      }
    }
  }
  function parseReply(response: { status: number; body: Buffer }) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(response.body));
    } catch {
      throw new Error("Invalid internal response");
    } finally {
      response.body.fill(0);
    }
    if (response.status >= 400) {
      const error = internalErrorSchema.safeParse(parsed);
      if (error.success) throw new CoreRpcError(error.data, response.status);
      throw new Error("Internal transport rejected request");
    }
    return parsed;
  }
  async function request(
    path: string,
    body: Buffer,
    proof?: string,
    reply?: { maxBytes?: number; dispatchDeadlineMs?: number },
  ) {
    return parseReply(await send(path, () => ({ body, proof }), reply));
  }
  function signed(
    kind: ExecutionKind | "identity_current_v1" | "plugin_package_v1" | "terminal_settlement_v1",
    normalizedPath: string,
    data: unknown,
    requestOptions: {
      commandId?: string;
      deadlineAt?: string;
      traceId?: string;
      routeTicket?: string;
    },
  ) {
    const owner = receipt;
    if (!owner) throw new Error("Startup required");
    const commandId = requestOptions.commandId ?? randomUUID(),
      deadline = requestOptions.deadlineAt ?? new Date(Date.now() + 30000).toISOString(),
      trace = requestOptions.traceId ?? randomUUID();
    const envelope = {
      command_id: commandId,
      body_digest: sha256(
        canonical({
          kind,
          contract_version: 1,
          owner: {
            service: owner.service,
            instance_id: owner.instance_id,
            session_id: owner.session_id,
            incarnation_id: owner.incarnation_id,
          },
          deadline_at: deadline,
          data,
        }),
      ),
      caller_incarnation: owner.incarnation_id,
      deadline_at: deadline,
      trace_id: trace,
      contract_version: 1,
    };
    const body = Buffer.from(
      JSON.stringify({
        envelope,
        data,
        ...(requestOptions.routeTicket ? { route_ticket: requestOptions.routeTicket } : {}),
      }),
    );
    const proof = {
      session_id: owner.session_id,
      incarnation_id: owner.incarnation_id,
      command_type: kind,
      command_id: commandId,
      deadline_at: deadline,
      trace_id: trace,
      contract_version: 1 as const,
      raw_body_sha256: sha256(body),
    };
    const signature = sign(
      null,
      Buffer.from(
        JSON.stringify(commandProofFields(options.audience, owner, proof, "POST", normalizedPath)),
      ),
      key.privateKey,
    ).toString("base64url");
    return {
      owner,
      body,
      deadline,
      proof: Buffer.from(JSON.stringify({ ...proof, signature })).toString("base64url"),
    };
  }
  async function register() {
    if (receipt) return receipt;
    if (!challenge) {
      challenge = startupChallengeSchema.parse(
        await request(
          "/_internal/core/v1/identity/challenges",
          Buffer.from(
            JSON.stringify({
              registration_id: registrationId,
              public_key_spki: spki,
              boot_nonce: bootNonce,
              contract_version: 1,
            }),
          ),
        ),
      );
      if (
        challenge.audience !== options.audience ||
        challenge.registration_id !== registrationId ||
        challenge.public_key_sha256 !== sha256(Buffer.from(spki, "base64url")) ||
        challenge.boot_nonce !== bootNonce
      )
        throw new Error("Registration binding mismatch");
    }
    const signature = sign(
      null,
      Buffer.from(JSON.stringify(registrationProofFields(challenge))),
      key.privateKey,
    ).toString("base64url");
    const result = startupRegistrationResponseSchema.parse(
      await request(
        "/_internal/core/v1/identity/registrations",
        Buffer.from(
          JSON.stringify({
            challenge_id: challenge.challenge_id,
            registration_id: registrationId,
            signature,
            contract_version: 1,
          }),
        ),
      ),
    );
    if (
      result.receipt.service !== challenge.service ||
      result.receipt.instance_id !== challenge.instance_id ||
      result.receipt.certificate_sha256 !== challenge.certificate_sha256 ||
      result.receipt.public_key_sha256 !== challenge.public_key_sha256 ||
      !result.current
    )
      throw new Error("Registration binding mismatch");
    receipt = Object.freeze(result.receipt);
    return receipt;
  }
  return {
    startup() {
      if (closed) return Promise.reject(new Error("Client closed"));
      if (!starting)
        starting = register().finally(() => {
          starting = undefined;
        });
      return starting;
    },
    async current(
      requestOptions: { commandId?: string; deadlineAt?: string; traceId?: string } = {},
    ) {
      const commandId = requestOptions.commandId ?? randomUUID();
      const deadlineAt = requestOptions.deadlineAt ?? new Date(Date.now() + 5000).toISOString();
      const { owner, body, proof } = signed(
        "identity_current_v1",
        "/_internal/core/v1/identity/current-session",
        {},
        { ...requestOptions, commandId, deadlineAt },
      );
      const reply = internalCurrentResponseSchema.parse(
        await request("/_internal/core/v1/identity/current-session", body, proof),
      );
      if (
        reply.command_id !== commandId ||
        reply.receipt.session_id !== owner.session_id ||
        reply.receipt.incarnation_id !== owner.incarnation_id ||
        reply.receipt.public_key_sha256 !== owner.public_key_sha256 ||
        reply.receipt.certificate_sha256 !== owner.certificate_sha256
      )
        throw new Error("Response binding mismatch");
      return reply;
    },
    async execution<Kind extends ExecutionKind>(
      kind: Kind,
      data: unknown,
      requestOptions: {
        commandId?: string;
        deadlineAt?: string;
        traceId?: string;
        routeTicket?: string;
        dispatchSignal?: AbortSignal;
        shortChatProject?: string | undefined;
      } = {},
    ) {
      if (requestOptions.commandId && !requestOptions.deadlineAt)
        throw new Error("Stable command deadline required");
      if (!receipt) throw new Error("Startup required");
      const parsedData = executionDataSchemas[kind].parse(data);
      const combined =
        kind === "execution_admit_v1" &&
        (parsedData as { commit_send?: boolean }).commit_send === true;
      const canGrant = kind === "execution_begin_v1" || combined;
      const path = (
        {
          execution_context_v1: "context",
          execution_admit_v1: "admissions",
          execution_next_v1: "attempts",
          execution_begin_v1: "begin",
          execution_abort_v1: "abort",
          execution_query_v1: "commands",
          execution_quota_v1: "quota",
          execution_quota_recovery_v1: "quota-recovery",
          execution_abort_recovery_v1: "abort-recovery",
          execution_resource_commit_v1: "resources/commit",
          execution_resource_v1: "resource-permits",
          execution_resource_list_v1: "resources/list",
          execution_resource_claim_v1: "resources/claim",
          execution_resource_poll_v1: "resources/poll-permits",
          execution_resource_release_v1: "resources/release",
        } as const
      )[kind];
      const normalizedPath = `/_internal/core/v1/${path}`;
      const owner = receipt;
      const deadline = requestOptions.deadlineAt ?? new Date(Date.now() + 30000).toISOString();
      const command = {
        ...requestOptions,
        commandId: requestOptions.commandId ?? randomUUID(),
        traceId: requestOptions.traceId ?? randomUUID(),
        deadlineAt: deadline,
      };
      let message: ReturnType<typeof signed> | undefined;
      try {
        const value = parseReply(
          await send(
            normalizedPath,
            () => (message ??= signed(kind, normalizedPath, parsedData, command)),
            {
              dispatchDeadlineMs: Date.parse(deadline),
              ...(requestOptions.dispatchSignal
                ? { dispatchSignal: requestOptions.dispatchSignal }
                : {}),
              shortChatProject: requestOptions.shortChatProject,
              ...(kind === "execution_context_v1"
                ? { maxBytes: executionContextResponseMaxBytes }
                : {}),
            },
          ),
        ) as Record<string, unknown>;
        if (kind === "execution_context_v1") return executionContextResponseSchema.parse(value);
        if (kind === "execution_resource_commit_v1")
          return resourceCommitResponseSchema.parse(value);
        if (kind === "execution_resource_v1") {
          const permit = resourcePermitResponseSchema.parse(value);
          if ((permit.state === "GRANTED") !== (permit.auth_material !== undefined))
            throw new Error("Unexpected send authority");
          return permit;
        }
        if (kind === "execution_resource_list_v1") return resourceListResponseSchema.parse(value);
        if (kind === "execution_resource_claim_v1") return resourceClaimResponseSchema.parse(value);
        if (kind === "execution_resource_poll_v1") return resourcePollResponseSchema.parse(value);
        if (kind === "execution_resource_release_v1")
          return resourceReleaseResponseSchema.parse(value);
        if (kind === "execution_quota_v1" || kind === "execution_quota_recovery_v1") return value;
        const safe = executionReceiptSchema.parse(value.receipt);
        if (value.transient_send !== undefined) {
          if (!canGrant || safe.state !== "SEND_GRANTED")
            throw new Error("Unexpected send authority");
          const transient = transientSendSchema.parse(value.transient_send);
          if (
            transient.owner_session_id !== owner.session_id ||
            transient.incarnation_id !== owner.incarnation_id ||
            transient.attempt_id !== safe.attempt_id ||
            transient.permit_epoch !== safe.permit_epoch ||
            transient.authorization_plan_digest !== safe.authorization_plan_digest
          )
            throw new Error("Response binding mismatch");
          return {
            receipt: safe,
            transient_send: transient,
            ...(combined && value.stream_terminal_v1 === true
              ? { stream_terminal_v1: true as const }
              : {}),
          };
        }
        return { receipt: safe };
      } catch (error) {
        if (
          canGrant &&
          !(error instanceof CoreRpcError) &&
          !(error instanceof InternalTransportCancelled)
        )
          throw new CoreRpcError({
            code: "identity_store_unavailable",
            category: "DEPENDENCY",
            retryability: "STATUS_ONLY",
            command_state: "UNKNOWN",
            message: "Internal request outcome unknown",
          });
        throw error;
      } finally {
        message?.body.fill(0);
      }
    },
    async pluginPackage(digest: string, requestOptions: { deadlineAt?: string } = {}) {
      const data = pluginPackageDataSchema.parse({ digest });
      const path = "/_internal/core/v1/plugin-package";
      const { body, proof } = signed("plugin_package_v1", path, data, requestOptions);
      try {
        const response = await send(path, () => ({ body, proof }), {
          maxBytes: pluginPackageMaxBytes,
          timeoutMs: 30000,
          binary: true,
        });
        if (response.status !== 200) parseReply(response);
        return new Uint8Array(response.body);
      } finally {
        body.fill(0);
      }
    },
    async settleTerminal(
      group: CoreEvidenceGroupV1,
      deadlineAt: string,
      shortChatProject?: string,
      streamTerminal = false,
    ) {
      const data = {
        ...coreEvidenceGroupV1Schema.parse(group),
        ...(streamTerminal ? { stream_terminal_v1: true } : {}),
      };
      const { body, proof } = signed("terminal_settlement_v1", terminalSettlementPath, data, {
        deadlineAt,
      });
      try {
        const remaining = Date.parse(deadlineAt) - Date.now();
        if (!(remaining > 0)) throw new Error("Terminal settlement deadline elapsed");
        return terminalSettlementResponseSchema.parse(
          parseReply(
            await send(terminalSettlementPath, () => ({ body, proof }), {
              maxBytes: boundedH2Defaults.responseBytes,
              shortChatProject,
              dispatchDeadlineMs: Date.parse(deadlineAt),
              timeoutMs: Math.min(5000, remaining),
            }),
          ),
        );
      } finally {
        body.fill(0);
      }
    },
    signEvidence(
      body: Omit<
        CoreEvidenceEventV1,
        "origin_identity" | "origin_incarnation" | "origin_session_id"
      >,
    ): SignedCoreEvidenceEventV1 {
      const owner = receipt;
      if (!owner) throw new Error("Startup required");
      if (owner.service !== "gateway") throw new Error("Only gateway produces evidence");
      const event = coreEvidenceEventV1Schema.parse({
        ...body,
        origin_identity: `${owner.service}:${owner.instance_id}`,
        origin_incarnation: owner.incarnation_id,
        origin_session_id: owner.session_id,
      });
      const signature = sign(
        null,
        Buffer.from(JSON.stringify(evidenceProofFields(event, sha256(canonical(event))))),
        key.privateKey,
      ).toString("base64url");
      return { ...event, signature };
    },
    async reconnect(target: CoreTarget) {
      if (peer) throw new Error("Project Core targets are static");
      requireTarget(target);
      generation++;
      await releaseConnections();
      options = { ...options, ...target };
    },
    async close() {
      closed = true;
      generation++;
      await releaseConnections();
      await starting?.catch(() => {});
      receipt = undefined;
      challenge = undefined;
    },
  };
}
