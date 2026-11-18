import {
  awsAmzDate,
  awsCredentials,
  awsSigningKey,
  Limits,
  orderRouteCandidates,
  VertexTokens,
} from "@gateway/runtime";
import type { SQL } from "bun";
import { z } from "zod";
import {
  executionEnvelopeSchema,
  executionContextResponseMaxBytes,
  routeTicketMaxBytes,
  routeTicketSchema,
  executionDataSchemas,
  candidateMetadataSchema,
  executionReceiptSchema,
  quotaRecoveryResultSchema,
  transientSendSchema,
  dispatchAuthSchema,
  hostAuthorizationPlanV2Schema,
  executionRuntimesSchema,
  type ExecutionKind,
  type CoreVerifiedCallerProjection,
  GatewayError,
} from "@gateway/contracts";
import {
  canonical,
  calculateCost,
  quoteReservation,
  DomainError,
  type Usage,
  type Pricing,
} from "@gateway/kernel";
import { decrypt, encrypt, sha256, verifier } from "@gateway/runtime/crypto";
import {
  createQuotaLeases,
  type QuotaDescriptor,
  type QuotaScope,
} from "@gateway/runtime/quota-leases";
import { createExecutionStore, ExecutionOutcomeUnknown } from "@gateway/storage/execution";
export type ExecutionOptions = {
  pepper: string;
  encryptionKey: string;
  redisUrls: readonly string[];
  quotaNamespace: string;
  quotaGeneration: string;
  maxAttempts?: number | undefined;
};
const leaseStateSchema = z.object({
  permit_epoch: z.string(),
  permit_state: z.string(),
  execution_closed: z.boolean(),
  leases: z.array(
    z.object({
      state: z.string(),
      descriptor: z.custom<QuotaDescriptor>(),
      attempt_id: z.string(),
      permit_epoch: z.string(),
    }),
  ),
});
const contextSchema = z.object({
  max_output_tokens: z.number().int().positive(),
  batch: z.object({ items: z.number().int().min(1).max(50000) }).optional(),
  routing_strategy: z.enum(["WEIGHTED", "ROUND_ROBIN", "LEAST_LATENCY"]),
  execution_budget: z.object({ max_attempts: z.number().int().min(1).max(5) }),
  project_id: z.string(),
  key_id: z.string(),
  customer_currency: z.string(),
  scopes: z.array(
    z.object({
      kind: z.enum(["organization", "principal", "project", "key", "channel"]),
      id: z.string(),
      rpm: z.number(),
      tpm: z.number(),
      concurrency: z.number(),
    }),
  ),
  candidates: z.array(candidateMetadataSchema),
});
const admissionCandidateSchema = z.object(
  candidateMetadataSchema.pick({
    ref: true,
    cost_currency: true,
    cost_rates: true,
    cost_ceilings: true,
    customer_currency: true,
    customer_rates: true,
    customer_ceilings: true,
    customer_basis: true,
    channel_limits: true,
  }).shape,
);
const admissionContextSchema = contextSchema.extend({
  candidates: z.array(admissionCandidateSchema),
});
const routeClaimsSchema = z.object({
  verifier: z.string(),
  ip: z.string(),
  model: z.string(),
  fingerprint: z.string(),
  expires_at: z.string(),
  candidate_digests: z.array(z.string()),
  execution_runtimes: executionRuntimesSchema,
  captured: admissionContextSchema,
});
function routeBinding(p: CoreVerifiedCallerProjection, id: string) {
  return JSON.stringify([
    "stg.chat.route.v1",
    p.service,
    p.instance_id,
    p.session_id,
    p.incarnation_id,
    id,
  ]);
}
function quantityExceeds(left: string, right: string) {
  const [a, af = ""] = left.split("."),
    [b, bf = ""] = right.split(".");
  return BigInt(a! + af) * 10n ** BigInt(bf.length) > BigInt(b! + bf) * 10n ** BigInt(af.length);
}
export function createExecutionHandler(
  db: SQL,
  options: ExecutionOptions,
  requireCaller: (context: object) => CoreVerifiedCallerProjection,
) {
  if (!/^[0-9a-f]{64}$/.test(options.encryptionKey) || !options.pepper)
    throw new Error("Execution secrets required");
  if (
    !Number.isInteger(options.maxAttempts ?? 3) ||
    (options.maxAttempts ?? 3) < 1 ||
    (options.maxAttempts ?? 3) > 5
  )
    throw new Error("Invalid attempt budget");
  const cloudTokens = new VertexTokens();
  async function grantMaterial(
    auth: z.infer<typeof dispatchAuthSchema>,
    grant: Record<string, unknown>,
    deadlineAt: string,
  ) {
    const secret =
      auth.type === "none"
        ? Buffer.alloc(0)
        : Buffer.from(
            decrypt(
              String(grant.credential_ciphertext),
              options.encryptionKey,
              String(grant.channel_id),
            ),
          );
    try {
      if (secret.length > 4096 || [...secret].some((byte) => byte < 33 || byte > 126))
        throw new Error("no_route");
      const authMaterial = await material(
        auth,
        secret,
        AbortSignal.timeout(Math.max(1, Date.parse(deadlineAt) - Date.now())),
      );
      if (!authMaterial) throw new Error("no_route");
      return authMaterial;
    } finally {
      secret.fill(0);
    }
  }
  async function material(
    auth: z.infer<typeof dispatchAuthSchema>,
    secret: Buffer,
    signal: AbortSignal,
  ) {
    if (auth.type === "none") return { type: "none" };
    if (auth.type === "aws-sigv4") {
      let credentials: ReturnType<typeof awsCredentials>;
      try {
        credentials = awsCredentials(secret.toString());
      } catch {
        return undefined;
      }
      const amzDate = awsAmzDate();
      return {
        type: "aws-sigv4",
        service: auth.service,
        access_key_id: credentials.accessKeyId,
        ...(credentials.sessionToken ? { session_token: credentials.sessionToken } : {}),
        region: credentials.region,
        amz_date: amzDate,
        signing_key: awsSigningKey(
          credentials.secretAccessKey,
          amzDate.slice(0, 8),
          credentials.region,
          auth.service,
        ).toString("hex"),
      };
    }
    if (auth.type !== "google-oauth") return { ...auth, value: secret.toString() };
    const encoded = secret.toString();
    if (!/^[A-Za-z0-9_-]+$/.test(encoded)) return undefined;
    const token = await cloudTokens
      .get(Buffer.from(encoded, "base64url").toString("utf8"), signal)
      .catch(() => undefined);
    return token ? { type: "bearer", value: token } : undefined;
  }
  const store = createExecutionStore(db),
    routingLimits = new Limits(options.redisUrls, options.quotaNamespace),
    quota = createQuotaLeases({
      urls: options.redisUrls,
      namespace: options.quotaNamespace,
      generation: options.quotaGeneration,
    });
  async function limitResource(scope: string, rpm: number) {
    await routingLimits
      .enter(`resource:${scope}`, crypto.randomUUID(), rpm, Number.MAX_SAFE_INTEGER, 1000)
      .catch((error: unknown) => {
        if (error instanceof GatewayError && error.code === "rate_limited")
          throw new Error("resource_rate_limited");
        throw error;
      });
  }
  async function synchronize(
    p: CoreVerifiedCallerProjection,
    attempt: string,
    epoch: string,
    release = false,
    leaseState?: unknown,
  ) {
    const current = leaseStateSchema.parse(leaseState ?? (await store.readLeaseState(p, attempt)));
    if (current.permit_epoch !== epoch) throw new Error("internal_owner_mismatch");
    if (!release && current.execution_closed)
      return { proofs: [], deadline: performance.now(), descriptors: [], closed: true };
    const proofs: { lease_id: string; revision: string }[] = [],
      descriptors: QuotaDescriptor[] = [];
    let deadline = Number.POSITIVE_INFINITY;
    const relevant = current.leases.filter((lease) =>
      release
        ? ["RELEASE_PENDING", "RELEASED"].includes(lease.state)
        : lease.descriptor.scope_kind !== "channel" || lease.attempt_id === attempt,
    );
    if (
      !release &&
      (!["RESERVED", "SEND_GRANTED", "UNKNOWN"].includes(current.permit_state) ||
        relevant.some((lease) => !["ADOPTION_PENDING", "ACTIVE"].includes(lease.state)))
    )
      throw new Error("quota_release_denied");
    for (const lease of relevant) {
      const d = lease.descriptor;
      const adoptedAttempt = lease.attempt_id;
      if (release) {
        descriptors.push(
          (
            await quota.release(d, {
              state: "RELEASE_PENDING",
              attempt_id: adoptedAttempt,
              permit_epoch: lease.permit_epoch,
            })
          ).descriptor,
        );
      } else {
        const operation = sha256(JSON.stringify([p.command_id, d.lease_id]));
        const result =
          lease.state === "ADOPTION_PENDING"
            ? await quota.adopt(d, adoptedAttempt, lease.permit_epoch, operation)
            : await quota.renew(d, adoptedAttempt, lease.permit_epoch, operation);
        descriptors.push(result.descriptor);
        proofs.push({ lease_id: d.lease_id, revision: result.descriptor.revision });
        deadline = Math.min(deadline, result.proofDeadline);
      }
    }
    if (!release && (!descriptors.length || deadline <= performance.now()))
      throw new Error("quota_unavailable");
    if (descriptors.length)
      await store.quota(p, attempt, release ? "RELEASED" : "ACTIVE", descriptors);
    return {
      proofs,
      deadline,
      descriptors,
      closed: false,
      retained: current.leases.filter(
        (lease) => !["RELEASE_PENDING", "RELEASED"].includes(lease.state),
      ).length,
    };
  }
  return {
    close() {
      quota.close();
      routingLimits.close();
    },
    async pluginPackage(context: object, digest: string) {
      return store.pluginPackage(requireCaller(context), digest);
    },
    async handle(context: object, kind: ExecutionKind, input: unknown, signal?: AbortSignal) {
      const p = requireCaller(context);
      if (p.command_type !== kind) throw new Error("internal_proof_invalid");
      const wire = z
        .strictObject({
          envelope: executionEnvelopeSchema,
          data: executionDataSchemas[kind],
          ...(kind === "execution_admit_v1" || kind === "execution_next_v1"
            ? { route_ticket: routeTicketSchema.optional() }
            : {}),
        })
        .parse(input);
      const e = wire.envelope,
        d = wire.data;
      if (kind === "execution_abort_recovery_v1") {
        return { receipt: executionReceiptSchema.parse(await store.recoverAbort(p, e, d)) };
      }
      if (kind === "execution_quota_recovery_v1") {
        const { limit } = executionDataSchemas.execution_quota_recovery_v1.parse(d),
          items = (await store.recoverQuota(p, limit)) as {
            attempt_id: string;
            descriptor: QuotaDescriptor;
          }[],
          completed = [];
        for (const item of items) {
          try {
            completed.push(
              (
                await quota.release(item.descriptor, {
                  state: "RELEASE_PENDING",
                  attempt_id: item.attempt_id,
                  permit_epoch: "1",
                })
              ).descriptor,
            );
          } catch {}
        }
        const result = await store.recoverQuota(p, completed);
        const recovery = await store.expireQuota(p, limit);
        return quotaRecoveryResultSchema.parse({
          released: result.released,
          pending: items.length,
          failed: items.length - completed.length,
          expired: recovery.expired,
          expiry_more: recovery.more,
        });
      }
      if (kind === "execution_resource_commit_v1")
        return store.resource(p, "core_resource_commit_v1", d as Record<string, unknown>);
      if (kind === "execution_resource_list_v1") {
        const { customer_token, ...data } =
          executionDataSchemas.execution_resource_list_v1.parse(d);
        const result = await store.resource(p, "core_resource_list_v1", {
          ...data,
          verifier: verifier(customer_token, options.pepper),
        });
        await limitResource(`key:${String(result.key_id)}`, Number(result.rpm));
        return { data: result.data };
      }
      if (kind === "execution_resource_v1") {
        const { customer_token, ...data } = executionDataSchemas.execution_resource_v1.parse(d);
        const result = await store.resource(p, "core_resource_permit_v1", {
          ...data,
          verifier: verifier(customer_token, options.pepper),
        });
        const limits = result.rpm as { key: number; channel: number },
          resolution = result.resolution as Record<string, unknown>;
        if (result.state === "RESOLVED") {
          await limitResource(`key:${String(result.key_id)}`, limits.key);
          await limitResource(`channel:${String(result.channel_id)}`, limits.channel);
        }
        if (data.action === "files")
          return { resolution, state: result.state, files: result.files };
        if (result.state !== "GRANTED" || !data.auth) return { resolution, state: result.state };
        return {
          resolution,
          state: result.state,
          auth_material: await grantMaterial(data.auth, result, String(resolution.deadline_at)),
        };
      }
      if (kind === "execution_resource_claim_v1")
        return store.resource(
          p,
          "core_resource_claim_v1",
          executionDataSchemas.execution_resource_claim_v1.parse(d),
        );
      if (kind === "execution_resource_release_v1")
        return store.resource(
          p,
          "core_resource_release_v1",
          executionDataSchemas.execution_resource_release_v1.parse(d),
        );
      if (kind === "execution_resource_poll_v1") {
        const data = executionDataSchemas.execution_resource_poll_v1.parse(d);
        const result = await store.resource(p, "core_resource_poll_v1", data);
        await limitResource(`channel:${String(result.channel_id)}`, Number(result.rpm));
        return {
          auth_material: await grantMaterial(data.auth, result, String(result.deadline_at)),
        };
      }
      if (kind === "execution_context_v1") {
        const data = executionDataSchemas.execution_context_v1.parse(d);
        const id = crypto.randomUUID();
        const project =
          data.route_projection === true && data.operation === "chat" && !data.resource_ids;
        const input = {
          verifier: verifier(data.customer_token, options.pepper),
          ip: data.ip,
          operation: data.operation,
          ...(data.model ? { model: data.model } : {}),
          fingerprint: data.fingerprint,
          max_attempts: options.maxAttempts ?? 3,
          ...(data.resource_ids ? { resource_ids: data.resource_ids } : {}),
        };
        let snapshot = await store.context(p, project ? null : id, input, signal);
        let parsed = contextSchema.parse(snapshot);
        let ticket: string | undefined;
        if (project) {
          const claims = JSON.stringify({
            verifier: input.verifier,
            ip: data.ip,
            model: data.model,
            fingerprint: data.fingerprint,
            expires_at: snapshot.expires_at,
            execution_runtimes: snapshot.execution_runtimes,
            captured: admissionContextSchema.parse(parsed),
            candidate_digests: snapshot.candidate_digests,
          });
          if (4 * Math.ceil((Buffer.byteLength(claims) + 28) / 3) <= routeTicketMaxBytes) {
            ticket = encrypt(claims, options.encryptionKey, routeBinding(p, id));
          }
          const allowed = executionRuntimesSchema.parse(snapshot.execution_runtimes);
          if (
            ticket &&
            Buffer.byteLength(
              JSON.stringify({
                ...snapshot,
                context_id: id,
                route_ticket: ticket,
                execution_runtimes: Object.fromEntries(
                  parsed.candidates.map((candidate) => [
                    candidate.ref.package_digest,
                    allowed[candidate.ref.package_digest] ?? [],
                  ]),
                ),
              }),
            ) > executionContextResponseMaxBytes
          )
            ticket = undefined;
          if (!ticket) {
            snapshot = await store.context(p, id, input, signal);
            parsed = contextSchema.parse(snapshot);
          }
        }
        delete snapshot.candidate_digests;
        const ordered = await orderRouteCandidates(
          parsed.candidates.map((candidate) => ({
            id: candidate.ref.route_id,
            provider_id: candidate.ref.channel_id,
            ...candidate.routing,
            candidate,
          })),
          parsed.routing_strategy,
          String(snapshot.model),
          routingLimits,
        ).catch((error: unknown) => {
          if (error instanceof GatewayError && error.code === "no_route")
            throw new Error("no_route");
          throw error;
        });
        const candidates = ordered
          .slice(0, parsed.execution_budget.max_attempts)
          .map((row) => row.candidate);
        const runtimes = executionRuntimesSchema.parse(snapshot.execution_runtimes);
        return {
          ...snapshot,
          context_id: id,
          ...(ticket ? { route_ticket: ticket } : {}),
          candidates,
          execution_runtimes: Object.fromEntries(
            candidates.map((candidate) => [
              candidate.ref.package_digest,
              runtimes[candidate.ref.package_digest] ?? [],
            ]),
          ),
        };
      }
      const admitting = kind === "execution_admit_v1" || kind === "execution_next_v1";
      let beginLeaseState: unknown;
      if (kind === "execution_begin_v1") {
        const data = executionDataSchemas.execution_begin_v1.parse(d);
        const prepared = await store.prepareBegin(p, e, data.attempt_id);
        if (prepared.receipt) return { receipt: executionReceiptSchema.parse(prepared.receipt) };
        beginLeaseState = prepared.leases;
      } else if (!admitting && kind !== "execution_query_v1" && kind !== "execution_quota_v1") {
        const previous = await store.lookup(p, e);
        if (previous) return { receipt: executionReceiptSchema.parse(previous) };
      }
      let route: Record<string, unknown> | undefined;
      const rejected = async (code: string) => ({
        receipt: executionReceiptSchema.parse(
          (await store.execute(p, e, d, { rejection: code, ...(route ? { route } : {}) })).receipt,
        ),
      });
      let computed: Record<string, unknown> = {},
        drafts: QuotaDescriptor[] = [],
        proofDeadline = Number.POSITIVE_INFINITY,
        health: { channel: string; attempt: string } | undefined;
      const releaseHealth = async () => {
        if (health)
          await routingLimits
            .settleHealth(health.channel, health.attempt, p.session_id)
            .catch(() => {});
      };
      if (admitting) {
        const data =
          kind === "execution_admit_v1"
            ? executionDataSchemas.execution_admit_v1.parse(d)
            : executionDataSchemas.execution_next_v1.parse(d);
        let claims: z.infer<typeof routeClaimsSchema> | undefined;
        if (typeof wire.route_ticket === "string") {
          try {
            claims = routeClaimsSchema.parse(
              JSON.parse(
                decrypt(wire.route_ticket, options.encryptionKey, routeBinding(p, data.context_id)),
              ),
            );
          } catch {
            throw new Error("context_invalid");
          }
        }
        const { receipt, context: snapshot } = await store.prepareAdmission(
          p,
          e,
          data.context_id,
          claims,
        );
        if (receipt !== null) return { receipt: executionReceiptSchema.parse(receipt) };
        if (!claims && snapshot && typeof snapshot === "object" && "candidate_digests" in snapshot)
          throw new Error("context_invalid");
        const captured = claims?.captured ?? admissionContextSchema.parse(snapshot);
        if (claims) {
          const {
            verifier,
            ip,
            model,
            fingerprint,
            expires_at,
            candidate_digests,
            execution_runtimes,
          } = claims;
          route = {
            verifier,
            ip,
            model,
            fingerprint,
            expires_at,
            snapshot: {
              candidate_digests,
              execution_runtimes,
              execution_budget: captured.execution_budget,
              scopes: captured.scopes,
              customer_currency: captured.customer_currency,
            },
          };
          const auth = z
            .object({
              project_id: z.string(),
              key_id: z.string(),
              customer_currency: z.string(),
              scopes: contextSchema.shape.scopes,
            })
            .parse(snapshot);
          if (
            auth.project_id !== captured.project_id ||
            auth.key_id !== captured.key_id ||
            auth.customer_currency !== captured.customer_currency ||
            canonical(auth.scopes) !== canonical(captured.scopes) ||
            Date.parse(expires_at) <= Date.now()
          )
            return rejected("context_stale");
        }
        const projection = data.dispatch_projection;
        const selected = captured.candidates.find(
          (c) => canonical(c.ref) === canonical(projection.candidate_ref),
        );
        if (!selected) return rejected("candidate_stale");
        if (
          sha256(canonical(["stg.internal.billable-manifest", "1", data.manifest])) !==
            projection.billable_manifest_sha256 ||
          sha256(canonical(["stg.internal.dispatch-projection", "2", projection])) !==
            data.dispatched_digest
        )
          throw new Error("internal_request_invalid");
        const output = data.manifest.ceilings.output_tokens;
        if (
          (["chat", "responses", "messages", "gemini", "completions"].includes(
            data.manifest.operation,
          ) &&
            output === undefined) ||
          (output !== undefined &&
            BigInt(output) >
              BigInt(captured.max_output_tokens) * BigInt(captured.batch?.items ?? 1))
        )
          return rejected("output_bound");
        if (
          ["chat", "responses", "messages", "gemini", "completions"].includes(
            data.manifest.operation,
          ) &&
          data.manifest.ceilings.input_tokens === undefined
        )
          return rejected("cost_unbounded");
        const estimated: Usage = {};
        for (const dimension of Object.keys(selected.cost_rates) as (keyof Usage)[]) {
          const bound = selected.cost_ceilings[dimension];
          if (bound === undefined) return rejected("cost_unbounded");
          const exact = data.manifest.quantities[dimension],
            hard = data.manifest.ceilings[dimension];
          if (exact !== undefined && hard !== undefined && quantityExceeds(exact, hard))
            return rejected("cost_ceiling");
          estimated[dimension] = exact ?? hard ?? bound;
        }
        let supplier: string;
        try {
          supplier = calculateCost(estimated, {
            currency: selected.cost_currency,
            rates: selected.cost_rates as Pricing["rates"],
          });
        } catch (error) {
          if (error instanceof RangeError) return rejected("amount_overflow");
          throw error;
        }
        const images = data.manifest.images;
        if (
          BigInt(output ?? "0") > BigInt(Number.MAX_SAFE_INTEGER) ||
          (images !== undefined && BigInt(images) > BigInt(Number.MAX_SAFE_INTEGER))
        )
          return rejected("cost_unbounded");
        let quote: ReturnType<typeof quoteReservation>;
        try {
          quote = quoteReservation(
            captured.candidates.map((c) => ({
              provider_id: c.ref.channel_id,
              id: c.ref.customer_price_id,
              currency: c.customer_currency,
              rates: c.customer_rates as Pricing["rates"],
              ceilings: c.customer_ceilings,
              basis: c.customer_basis,
            })),
            captured.customer_currency,
            {
              inputCharacters: data.manifest.input_characters,
              images: images === undefined ? undefined : Number(images),
            },
            Number(output ?? "0"),
          );
        } catch (error) {
          if (error instanceof RangeError) return rejected("amount_overflow");
          if (error instanceof DomainError) return rejected(error.code);
          throw error;
        }
        const channelScope: QuotaScope = {
          kind: "channel",
          id: selected.ref.channel_id,
          rpm: selected.channel_limits.rpm,
          tpm: Number.MAX_SAFE_INTEGER,
          concurrency: selected.channel_limits.concurrency,
        };
        const scopes =
          kind === "execution_admit_v1" ? [...captured.scopes, channelScope] : [channelScope];
        let tokenCount = 0n;
        for (const key of [
          "input_tokens",
          "output_tokens",
          "cached_input_tokens",
          "cache_write_tokens",
        ] as const)
          tokenCount += BigInt(data.manifest.ceilings[key] ?? "0");
        if (
          tokenCount * BigInt(captured.execution_budget.max_attempts) >
          BigInt(Number.MAX_SAFE_INTEGER)
        )
          return rejected("quota_budget_exceeded");
        const channel = selected.ref.channel_id,
          attempt = projection.proposed_attempt_id;
        if (
          !(await routingLimits.grantHealth(
            channel,
            attempt,
            p.session_id,
            Date.parse(e.deadline_at) - Date.now() + 10_000,
          ))
        )
          throw new Error("no_route");
        health = { channel, attempt };
        try {
          for (const scope of scopes) {
            const result = await quota
              .acquire({
                scope,
                caller: `${p.service}:${p.instance_id}`,
                kind,
                command_id: e.command_id,
                body_digest: e.body_digest,
                owner_session_id: p.session_id,
                deadline_ms: Date.parse(e.deadline_at),
                tokens:
                  Number(tokenCount) *
                  (scope.kind === "channel" ? 1 : captured.execution_budget.max_attempts),
              })
              .catch((error: unknown) => {
                if (error instanceof GatewayError && error.code === "rate_limited")
                  throw new Error(scope.kind === "channel" ? "channel_capacity" : "quota_limited");
                throw error;
              });
            drafts.push(result.descriptor);
          }
        } catch (error) {
          await releaseHealth();
          const message = error instanceof Error ? error.message : "";
          if (message === "channel_capacity" || message === "quota_limited")
            for (const draft of drafts)
              await quota
                .release(draft, {
                  state: "REJECTED",
                  command_id: e.command_id,
                  body_digest: e.body_digest,
                })
                .catch(() => {});
          throw error;
        }
        computed = {
          quote,
          estimated_quantities: estimated,
          supplier_atoms: supplier,
          ...(route ? { route } : {}),
          leases: drafts,
          token_count: tokenCount.toString(),
        };
      } else if (kind === "execution_begin_v1") {
        const data = executionDataSchemas.execution_begin_v1.parse(d),
          proof = await synchronize(p, data.attempt_id, data.permit_epoch, false, beginLeaseState);
        proofDeadline = proof.deadline;
        computed = {
          proofs: proof.proofs,
          proof_remaining_ms: Math.floor(proofDeadline - performance.now()),
        };
        if (!proof.closed && Number(computed.proof_remaining_ms) <= 1000)
          throw new Error("quota_unavailable");
      } else if (kind === "execution_quota_v1") {
        const data = executionDataSchemas.execution_quota_v1.parse(d);
        if (data.action === "OBSERVE") {
          const observation = data.observation!;
          await routingLimits.settleHealth(
            observation.channel_id,
            data.attempt_id,
            p.session_id,
            observation.outcome === "NOT_SENT"
              ? undefined
              : { failed: observation.outcome === "FAILURE", latency: observation.latency_ms },
          );
          return { state: "OBSERVED" };
        }
        if (data.action === "CLOSE") {
          if (!data.reason) throw new Error("internal_request_invalid");
          const receipt = await store.closeQuota(p, e, data);
          await synchronize(p, data.attempt_id, data.permit_epoch, true);
          return { receipt, state: "RELEASED" };
        }
        const synchronized = await synchronize(
          p,
          data.attempt_id,
          data.permit_epoch,
          data.action === "RELEASE",
        );
        if (synchronized.closed) throw new Error("quota_release_denied");
        if (data.action === "RELEASE" && !synchronized.descriptors.length)
          throw new Error("quota_release_denied");
        return {
          state:
            data.action === "RELEASE" ? (synchronized.retained ? "PARTIAL" : "RELEASED") : "ACTIVE",
          released: data.action === "RELEASE" ? synchronized.descriptors.length : 0,
        };
      }
      const result = await store.execute(p, e, d, computed);
      try {
        const receipt = executionReceiptSchema.parse(result.receipt);
        if (receipt.command_state === "REJECTED") {
          await releaseHealth();
          for (const draft of drafts)
            await quota
              .release(draft, {
                state: "REJECTED",
                command_id: e.command_id,
                body_digest: e.body_digest,
              })
              .catch(() => {});
        }
        if (kind === "execution_abort_v1" && receipt.state === "ABORTED")
          await synchronize(p, receipt.attempt_id!, receipt.permit_epoch!, true).catch(() => {});
        if (kind !== "execution_begin_v1" || result.fresh_transition !== true) return { receipt };
        const data = executionDataSchemas.execution_begin_v1.parse(d);
        const plan = hostAuthorizationPlanV2Schema.parse(result.snapshot);
        const remaining = Math.floor(
          Math.min(
            proofDeadline - performance.now(),
            Date.parse(receipt.deadline_at!) - Date.now(),
          ),
        );
        if (remaining <= 1000) return { receipt };
        let secret: Buffer | undefined;
        try {
          const auth = plan.dispatch_projection.auth;
          if (
            sha256(String(result.credential_ciphertext)) !==
            plan.dispatch_projection.credential_ciphertext_sha256
          )
            throw new Error("credential_changed");
          secret =
            auth.type === "none"
              ? Buffer.alloc(0)
              : Buffer.from(
                  decrypt(
                    String(result.credential_ciphertext),
                    options.encryptionKey,
                    String(result.channel_id),
                  ),
                );
          if (secret.length > 4096 || [...secret].some((byte) => byte < 33 || byte > 126))
            throw new Error("credential_invalid");
          const authMaterial = await material(auth, secret, AbortSignal.timeout(remaining));
          const left = Math.floor(
            Math.min(
              proofDeadline - performance.now(),
              Date.parse(receipt.deadline_at!) - Date.now(),
            ),
          );
          if (!authMaterial || left <= 1000) return { receipt };
          const transient = transientSendSchema.parse({
            grant_id: crypto.randomUUID(),
            owner_session_id: p.session_id,
            incarnation_id: p.incarnation_id,
            attempt_id: data.attempt_id,
            permit_epoch: data.permit_epoch,
            authorization_plan_digest: data.authorization_plan_digest,
            deadline_at: receipt.deadline_at,
            remaining_ms: left,
            auth_material: authMaterial,
          });
          if (proofDeadline <= performance.now() + 1000) return { receipt };
          return { receipt, transient_send: transient };
        } finally {
          secret?.fill(0);
          delete result.credential_ciphertext;
        }
      } catch (error) {
        if (kind === "execution_begin_v1") throw new ExecutionOutcomeUnknown();
        throw error;
      }
    },
  };
}
