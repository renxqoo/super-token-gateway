import { Limits, orderRouteCandidates } from "@gateway/runtime";
import type { SQL } from "bun";
import { z } from "zod";
import {
  executionEnvelopeSchema,
  executionDataSchemas,
  candidateMetadataSchema,
  executionReceiptSchema,
  transientSendSchema,
  hostAuthorizationPlanV1Schema,
  type ExecutionKind,
  type CoreVerifiedCallerProjection,
} from "@gateway/contracts";
import {
  canonical,
  calculateCost,
  quoteReservation,
  DomainError,
  type Usage,
  type Pricing,
} from "@gateway/kernel";
import { decrypt, sha256, verifier } from "@gateway/runtime/crypto";
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
const contextSchema = z.object({
  permit_epoch: z.string().nullable().optional(),
  permit_state: z.string().nullable().optional(),
  execution_closed: z.boolean().nullable().optional(),
  max_output_tokens: z.number().int().positive(),
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
  leases: z
    .array(
      z.object({
        state: z.string(),
        descriptor: z.custom<QuotaDescriptor>(),
        attempt_id: z.string(),
        permit_epoch: z.string(),
      }),
    )
    .optional(),
});
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
  const store = createExecutionStore(db),
    routingLimits = new Limits(options.redisUrls, options.quotaNamespace),
    quota = createQuotaLeases({
      urls: options.redisUrls,
      namespace: options.quotaNamespace,
      generation: options.quotaGeneration,
    });
  async function synchronize(
    p: CoreVerifiedCallerProjection,
    attempt: string,
    epoch: string,
    release = false,
  ) {
    const current = contextSchema.parse(await store.preflight(p, undefined, attempt));
    if (current.permit_epoch !== epoch) throw new Error("internal_owner_mismatch");
    if (!release && current.execution_closed)
      return { proofs: [], deadline: performance.now(), descriptors: [], closed: true };
    const proofs: { lease_id: string; revision: string }[] = [],
      descriptors: QuotaDescriptor[] = [];
    let deadline = Number.POSITIVE_INFINITY;
    const relevant = (current.leases ?? []).filter((lease) =>
      release
        ? ["RELEASE_PENDING", "RELEASED"].includes(lease.state)
        : lease.descriptor.scope_kind !== "channel" || lease.attempt_id === attempt,
    );
    if (
      !release &&
      (!["RESERVED", "SEND_GRANTED", "UNKNOWN"].includes(current.permit_state ?? "") ||
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
      retained: (current.leases ?? []).filter(
        (lease) => !["RELEASE_PENDING", "RELEASED"].includes(lease.state),
      ).length,
    };
  }
  return {
    close() {
      quota.close();
      routingLimits.close();
    },
    async handle(context: object, kind: ExecutionKind, input: unknown) {
      const p = requireCaller(context);
      if (p.command_type !== kind) throw new Error("internal_proof_invalid");
      const wire = z
        .strictObject({ envelope: executionEnvelopeSchema, data: executionDataSchemas[kind] })
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
        return store.recoverQuota(p, completed);
      }
      if (kind === "execution_context_v1") {
        const data = executionDataSchemas.execution_context_v1.parse(d);
        const snapshot = await store.context(p, crypto.randomUUID(), {
          verifier: verifier(data.customer_token, options.pepper),
          ip: data.ip,
          operation: data.operation,
          model: data.model,
          fingerprint: data.fingerprint,
          max_attempts: options.maxAttempts ?? 3,
        });
        const parsed = contextSchema.parse(snapshot);
        const ordered = await orderRouteCandidates(
          parsed.candidates.map((candidate) => ({
            id: candidate.ref.route_id,
            provider_id: candidate.ref.channel_id,
            ...candidate.routing,
            candidate,
          })),
          parsed.routing_strategy,
          data.model,
          routingLimits,
        );
        return { ...snapshot, candidates: ordered.map((row) => row.candidate) };
      }
      if (kind !== "execution_query_v1" && kind !== "execution_quota_v1") {
        const previous = await store.lookup(p, e);
        if (previous) return { receipt: executionReceiptSchema.parse(previous) };
      }
      const rejected = async (code: string) => ({
        receipt: executionReceiptSchema.parse(
          (await store.execute(p, e, d, { rejection: code })).receipt,
        ),
      });
      let computed: Record<string, unknown> = {},
        drafts: QuotaDescriptor[] = [],
        proofDeadline = Number.POSITIVE_INFINITY;
      if (kind === "execution_admit_v1" || kind === "execution_next_v1") {
        const data =
          kind === "execution_admit_v1"
            ? executionDataSchemas.execution_admit_v1.parse(d)
            : executionDataSchemas.execution_next_v1.parse(d);
        const captured = contextSchema.parse(await store.preflight(p, data.context_id));
        const projection = data.dispatch_projection;
        const selected = captured.candidates.find(
          (c) => canonical(c.ref) === canonical(projection.candidate_ref),
        );
        if (!selected) return rejected("candidate_stale");
        if (
          sha256(canonical(["stg.internal.billable-manifest", "1", data.manifest])) !==
            projection.billable_manifest_sha256 ||
          sha256(canonical(["stg.internal.dispatch-projection", "1", projection])) !==
            data.dispatched_digest
        )
          throw new Error("internal_request_invalid");
        const output = data.manifest.ceilings.output_tokens;
        if (
          (["chat", "responses", "messages", "gemini", "completions"].includes(
            data.manifest.operation,
          ) &&
            output === undefined) ||
          (output !== undefined && BigInt(output) > BigInt(captured.max_output_tokens))
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
        for (const scope of scopes) {
          const result = await quota.acquire({
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
          });
          drafts.push(result.descriptor);
        }
        computed = {
          quote,
          estimated_quantities: estimated,
          supplier_atoms: supplier,
          leases: drafts,
          token_count: tokenCount.toString(),
        };
      } else if (kind === "execution_begin_v1") {
        const data = executionDataSchemas.execution_begin_v1.parse(d),
          proof = await synchronize(p, data.attempt_id, data.permit_epoch);
        proofDeadline = proof.deadline;
        computed = {
          proofs: proof.proofs,
          proof_remaining_ms: Math.floor(proofDeadline - performance.now()),
        };
        if (!proof.closed && Number(computed.proof_remaining_ms) <= 1000)
          throw new Error("quota_unavailable");
      } else if (kind === "execution_quota_v1") {
        const data = executionDataSchemas.execution_quota_v1.parse(d);
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
        const plan = hostAuthorizationPlanV1Schema.parse(result.snapshot);
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
          const transient = transientSendSchema.parse({
            grant_id: crypto.randomUUID(),
            owner_session_id: p.session_id,
            incarnation_id: p.incarnation_id,
            attempt_id: data.attempt_id,
            permit_epoch: data.permit_epoch,
            authorization_plan_digest: data.authorization_plan_digest,
            deadline_at: receipt.deadline_at,
            remaining_ms: remaining,
            auth_material:
              auth.type === "none" ? { type: "none" } : { ...auth, value: secret.toString() },
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
