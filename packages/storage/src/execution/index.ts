import type { SQL } from "bun";
import type { CoreVerifiedCallerProjection, ExecutionEnvelope } from "@gateway/contracts";
import { Batcher } from "@gateway/runtime/batcher";
export class ExecutionOutcomeUnknown extends Error {
  constructor() {
    super("identity_store_unavailable");
    this.name = "ExecutionOutcomeUnknown";
  }
}
export function createExecutionStore(db: SQL) {
  const batch = new Batcher<
    {
      projection: CoreVerifiedCallerProjection;
      envelope: ExecutionEnvelope;
      data: unknown;
      computed: unknown;
    },
    Record<string, unknown>
  >(
    async (items) =>
      db
        .begin(
          async (tx) => (await tx`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value,
        )
        .catch(() => {
          throw new ExecutionOutcomeUnknown();
        }),
    128,
    3,
    4,
    2048,
  );
  const projection = (p: CoreVerifiedCallerProjection) => ({ ...p });
  return {
    async context(p: CoreVerifiedCallerProjection, id: string, input: Record<string, unknown>) {
      return (
        await db`SELECT core_context_v1(${projection(p)}::jsonb,${id}::uuid,${input}::jsonb) value`
      )[0].value;
    },
    async preflight(p: CoreVerifiedCallerProjection, contextId?: string, attemptId?: string) {
      return (
        await db`SELECT core_preflight_v1(${projection(p)}::jsonb,${contextId ?? null}::uuid,${attemptId ?? null}::uuid) value`
      )[0].value;
    },
    async lookup(p: CoreVerifiedCallerProjection, e: ExecutionEnvelope) {
      return (await db`SELECT core_lookup_v1(${projection(p)}::jsonb,${e}::jsonb) value`)[0].value;
    },
    async execute(
      p: CoreVerifiedCallerProjection,
      e: ExecutionEnvelope,
      data: unknown,
      computed: unknown,
    ) {
      const result = await batch.add({ projection: projection(p), envelope: e, data, computed });
      if (result.error) throw new Error(String(result.error));
      return result;
    },
    async recoverAbort(p: CoreVerifiedCallerProjection, e: ExecutionEnvelope, d: unknown) {
      return db.begin(
        async (tx) =>
          (
            await tx`SELECT core_abort_recovery_v1(${projection(p)}::jsonb,${e}::jsonb,${d}::jsonb) value`
          )[0].value,
      );
    },
    async recoverQuota(p: CoreVerifiedCallerProjection, items: unknown) {
      if (typeof items === "number")
        return (
          await db`SELECT core_quota_recovery_v1(${projection(p)}::jsonb,to_jsonb(${items}::integer)) value`
        )[0].value;
      return (
        await db`SELECT core_quota_recovery_v1(${projection(p)}::jsonb,${items}::jsonb) value`
      )[0].value;
    },
    async closeQuota(p: CoreVerifiedCallerProjection, e: ExecutionEnvelope, d: unknown) {
      return db.begin(
        async (tx) =>
          (
            await tx`SELECT core_quota_close_v1(${projection(p)}::jsonb,${e}::jsonb,${d}::jsonb) value`
          )[0].value,
      );
    },
    async quota(
      p: CoreVerifiedCallerProjection,
      attempt: string,
      action: "ACTIVE" | "RELEASED",
      descriptors: unknown,
    ) {
      return (
        await db`SELECT core_quota_v1(${projection(p)}::jsonb,${attempt}::uuid,${action},${descriptors}::jsonb) value`
      )[0].value;
    },
  };
}
