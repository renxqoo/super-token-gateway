import { generateKeyPairSync, randomBytes, randomUUID, sign } from "node:crypto";
import {
  executionDataSchemas,
  executionContextResponseSchema,
  executionReceiptSchema,
  transientSendSchema,
  type ExecutionKind,
  startupChallengeSchema,
  startupRegistrationResponseSchema,
  internalCurrentResponseSchema,
  internalErrorSchema,
  registrationProofFields,
  commandProofFields,
  type StartupReceipt,
} from "@gateway/contracts";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime/crypto";
import { createBoundedH2Client } from "@gateway/runtime/h2";

export class CoreRpcError extends Error {
  constructor(
    readonly detail: ReturnType<typeof internalErrorSchema.parse>,
    readonly status?: number,
  ) {
    super(detail.code);
    this.name = "CoreRpcError";
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
export function createCoreRpcClient(
  options: Parameters<typeof createBoundedH2Client>[0] & CoreTarget & { audience: string },
) {
  requireTarget(options);
  if (!/^[A-Za-z0-9_.-]{1,100}\/core-api$/.test(options.audience))
    throw new Error("Invalid Core audience");
  const key = generateKeyPairSync("ed25519"),
    registrationId = randomUUID(),
    bootNonce = randomBytes(32).toString("base64url"),
    spki = key.publicKey.export({ format: "der", type: "spki" }).toString("base64url");
  let transport: Awaited<ReturnType<typeof createBoundedH2Client>> | undefined,
    receipt: StartupReceipt | undefined,
    challenge: ReturnType<typeof startupChallengeSchema.parse> | undefined;
  let starting: Promise<StartupReceipt> | undefined,
    closed = false,
    generation = 0;
  let connecting: Promise<Awaited<ReturnType<typeof createBoundedH2Client>>> | undefined;
  async function connection() {
    if (closed) throw new Error("Client closed");
    if (transport) return transport;
    if (!connecting) {
      const captured = generation;
      connecting = createBoundedH2Client(options)
        .then(async (client) => {
          if (closed || captured !== generation) {
            await client.close();
            throw new Error("Client closed");
          }
          transport = client;
          return client;
        })
        .finally(() => {
          connecting = undefined;
        });
    }
    return connecting;
  }
  async function request(path: string, body: Buffer, proof?: string) {
    const c = await connection();
    const response = await c.request(
      {
        ":authority": new URL(options.origin).port
          ? `${options.servername}:${new URL(options.origin).port}`
          : options.servername,
        ":method": "POST",
        ":path": path,
        "content-type": "application/json",
        "content-length": String(body.length),
        ...(proof ? { "x-stg-proof": proof } : {}),
      },
      body,
    );
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
      const owner = receipt;
      if (!owner) throw new Error("Startup required");
      const commandId = requestOptions.commandId ?? randomUUID(),
        deadline = requestOptions.deadlineAt ?? new Date(Date.now() + 5000).toISOString(),
        trace = requestOptions.traceId ?? randomUUID();
      const data = {},
        bodyDigest = sha256(
          canonical({
            kind: "identity_current_v1",
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
        );
      const body = Buffer.from(
        JSON.stringify({
          envelope: {
            command_id: commandId,
            body_digest: bodyDigest,
            caller_incarnation: owner.incarnation_id,
            deadline_at: deadline,
            trace_id: trace,
            contract_version: 1,
          },
          data,
        }),
      );
      const proof = {
        session_id: owner.session_id,
        incarnation_id: owner.incarnation_id,
        command_type: "identity_current_v1" as const,
        command_id: commandId,
        deadline_at: deadline,
        trace_id: trace,
        contract_version: 1 as const,
        raw_body_sha256: sha256(body),
      };
      const signature = sign(
        null,
        Buffer.from(
          JSON.stringify(
            commandProofFields(
              options.audience,
              owner,
              proof,
              "POST",
              "/_internal/core/v1/identity/current-session",
            ),
          ),
        ),
        key.privateKey,
      ).toString("base64url");
      const reply = internalCurrentResponseSchema.parse(
        await request(
          "/_internal/core/v1/identity/current-session",
          body,
          Buffer.from(JSON.stringify({ ...proof, signature })).toString("base64url"),
        ),
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
      requestOptions: { commandId?: string; deadlineAt?: string; traceId?: string } = {},
    ) {
      if (requestOptions.commandId && !requestOptions.deadlineAt)
        throw new Error("Stable command deadline required");
      const owner = receipt;
      if (!owner) throw new Error("Startup required");
      const parsedData = executionDataSchemas[kind].parse(data);
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
        } as const
      )[kind];
      const normalizedPath = `/_internal/core/v1/${path}`;
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
            data: parsedData,
          }),
        ),
        caller_incarnation: owner.incarnation_id,
        deadline_at: deadline,
        trace_id: trace,
        contract_version: 1,
      };
      const body = Buffer.from(JSON.stringify({ envelope, data: parsedData }));
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
          JSON.stringify(
            commandProofFields(options.audience, owner, proof, "POST", normalizedPath),
          ),
        ),
        key.privateKey,
      ).toString("base64url");
      try {
        const value = (await request(
          normalizedPath,
          body,
          Buffer.from(JSON.stringify({ ...proof, signature })).toString("base64url"),
        )) as Record<string, unknown>;
        if (kind === "execution_context_v1") return executionContextResponseSchema.parse(value);
        if (kind === "execution_quota_v1" || kind === "execution_quota_recovery_v1") return value;
        const safe = executionReceiptSchema.parse(value.receipt);
        if (value.transient_send !== undefined) {
          if (kind !== "execution_begin_v1") throw new Error("Unexpected send authority");
          const transient = transientSendSchema.parse(value.transient_send);
          if (
            transient.owner_session_id !== owner.session_id ||
            transient.incarnation_id !== owner.incarnation_id ||
            transient.attempt_id !== safe.attempt_id ||
            transient.authorization_plan_digest !== safe.authorization_plan_digest
          )
            throw new Error("Response binding mismatch");
          return { receipt: safe, transient_send: transient };
        }
        return { receipt: safe };
      } catch (error) {
        if (kind === "execution_begin_v1" && !(error instanceof CoreRpcError))
          throw new CoreRpcError({
            code: "identity_store_unavailable",
            category: "DEPENDENCY",
            retryability: "STATUS_ONLY",
            command_state: "UNKNOWN",
            message: "Internal request outcome unknown",
          });
        throw error;
      } finally {
        body.fill(0);
      }
    },
    async reconnect(target: CoreTarget) {
      requireTarget(target);
      generation++;
      const previous = transport;
      transport = undefined;
      await previous?.close();
      options = { ...options, ...target };
    },
    async close() {
      closed = true;
      generation++;
      const previous = transport;
      transport = undefined;
      await previous?.close();
      await connecting?.catch(() => {});
      await starting?.catch(() => {});
      receipt = undefined;
      challenge = undefined;
    },
  };
}
