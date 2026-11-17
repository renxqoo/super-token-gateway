import type { SQL } from "bun";
import type { CoreVerifiedCallerProjection, ExecutionEnvelope } from "@gateway/contracts";
import { Batcher } from "@gateway/runtime/batcher";
export class ExecutionOutcomeUnknown extends Error {
  constructor() {
    super("identity_store_unavailable");
    this.name = "ExecutionOutcomeUnknown";
  }
}
const busyRetryMs = 1000;
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
    async context(
      p: CoreVerifiedCallerProjection,
      id: string,
      input: Record<string, unknown>,
      signal?: AbortSignal,
    ) {
      const until =
        performance.now() + Math.min(Date.parse(p.deadline_at) - Date.now(), busyRetryMs);
      let lastBusyError: unknown;
      for (let delay = 1; ; delay = Math.min(delay * 2, 16)) {
        signal?.throwIfAborted();
        if (lastBusyError !== undefined && !(performance.now() < until)) throw lastBusyError;
        const query = db`SELECT core_context_v1(${projection(p)}::jsonb,${id}::uuid,${input}::jsonb) value`;
        const cancel = () => {
          query.cancel();
        };
        signal?.addEventListener("abort", cancel, { once: true });
        if (signal?.aborted) cancel();
        try {
          const rows = await query;
          signal?.throwIfAborted();
          return rows[0].value;
        } catch (error) {
          signal?.throwIfAborted();
          const code = (error as { errno?: string } | null)?.errno;
          if ((code !== "55P03" && code !== "P0503") || !(performance.now() + delay < until))
            throw error;
          lastBusyError = error;
        } finally {
          signal?.removeEventListener("abort", cancel);
        }
        await Bun.sleep(delay);
      }
    },
    async preflight(p: CoreVerifiedCallerProjection, contextId?: string, attemptId?: string) {
      return (
        await db`SELECT core_preflight_v1(${projection(p)}::jsonb,${contextId ?? null}::uuid,${attemptId ?? null}::uuid) value`
      )[0].value;
    },
    async resource(
      p: CoreVerifiedCallerProjection,
      fn:
        | "core_resource_commit_v1"
        | "core_resource_permit_v1"
        | "core_resource_list_v1"
        | "core_resource_claim_v1"
        | "core_resource_poll_v1"
        | "core_resource_release_v1",
      data: Record<string, unknown>,
    ): Promise<Record<string, unknown>> {
      return db.begin(
        async (tx) =>
          (
            await tx`SELECT ${tx.unsafe(`public.${fn}`)}(${projection(p)}::jsonb,${data}::jsonb) value`
          )[0].value,
      );
    },
    async pluginPackage(p: CoreVerifiedCallerProjection, digest: string): Promise<Buffer> {
      return (await db`SELECT core_plugin_package_v1(${projection(p)}::jsonb,${digest}) value`)[0]
        .value;
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
      const until = Math.min(Date.parse(e.deadline_at), Date.now() + busyRetryMs);
      for (let delay = 1; ; delay = Math.min(delay * 2, 16)) {
        const result = await batch.add({ projection: projection(p), envelope: e, data, computed });
        if (result.error === "identity_busy" && Date.now() + delay < until) {
          await Bun.sleep(delay);
          continue;
        }
        if (result.error) throw new Error(String(result.error));
        return result;
      }
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
