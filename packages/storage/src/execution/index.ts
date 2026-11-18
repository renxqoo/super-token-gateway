import type { SQL } from "bun";
import type { CoreVerifiedCallerProjection, ExecutionEnvelope } from "@gateway/contracts";
import { Batcher } from "@gateway/runtime/batcher";
import { withDatabaseTransaction } from "../transaction.ts";
export class ExecutionOutcomeUnknown extends Error {
  constructor() {
    super("identity_store_unavailable");
    this.name = "ExecutionOutcomeUnknown";
  }
}
const busyRetryMs = 1000;
export function createExecutionStore(db: SQL) {
  let quotaCursor: unknown = null;
  let quotaCursorVersion = 0;
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
      id: string | null,
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
    async prepareAdmission(
      p: CoreVerifiedCallerProjection,
      e: ExecutionEnvelope,
      contextId: string,
      route?: { verifier: string; ip: string; model: string },
    ) {
      const caller = projection(p);
      const preflight = route
        ? db`core_route_authority_v1(${caller}::jsonb,${route.verifier},${route.ip},${route.model})`
        : db`core_preflight_v1(${caller}::jsonb,${contextId}::uuid,NULL)`;
      return (
        await db<{ receipt: unknown; context: unknown }[]>`
          WITH existing AS MATERIALIZED (SELECT core_lookup_v1(${caller}::jsonb,${e}::jsonb) receipt)
          SELECT receipt,CASE WHEN receipt IS NULL THEN ${preflight} END context FROM existing`
      )[0]!;
    },
    async prepareBegin(p: CoreVerifiedCallerProjection, e: ExecutionEnvelope, attemptId: string) {
      const caller = projection(p);
      return (
        await db<{ receipt: unknown; leases: unknown }[]>`
          WITH existing AS MATERIALIZED (SELECT core_lookup_v1(${caller}::jsonb,${e}::jsonb) receipt)
          SELECT receipt,CASE WHEN receipt IS NULL THEN core_preflight_v1(${caller}::jsonb,NULL,${attemptId}::uuid) END leases FROM existing`
      )[0]!;
    },
    async readLeaseState(p: CoreVerifiedCallerProjection, attemptId: string) {
      return (
        await db`SELECT core_preflight_v1(${projection(p)}::jsonb,NULL,${attemptId}::uuid) value`
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
      const until =
        performance.now() + Math.min(Date.parse(e.deadline_at) - Date.now(), busyRetryMs);
      const item = { projection: projection(p), envelope: e, data, computed };
      for (let delay = 1; ; delay = Math.min(delay * 2, 16)) {
        let result = await batch.add(item);
        if (result.error === "funding_slow") {
          try {
            result = await withDatabaseTransaction(
              db,
              { deadline: until },
              async (tx) =>
                (await tx`SELECT core_execution_slow_v1(${item}::jsonb) value`)[0].value,
            );
          } catch (error) {
            if (["55P03", "P0503", "40P01"].includes((error as { errno: string }).errno))
              result = { error: "identity_busy" };
            else throw error;
          }
        }
        if (result.error === "identity_busy" && performance.now() + delay < until) {
          await Bun.sleep(delay);
          continue;
        }
        if (result.error) throw new Error(String(result.error));
        return result;
      }
    },
    async recoverAbort(p: CoreVerifiedCallerProjection, e: ExecutionEnvelope, d: unknown) {
      const deadline = performance.now() + Math.min(Date.parse(p.deadline_at) - Date.now(), 8000);
      try {
        return await withDatabaseTransaction(
          db,
          { deadline },
          async (tx) =>
            (
              await tx`SELECT core_abort_recovery_v1(${projection(p)}::jsonb,${e}::jsonb,${d}::jsonb) value`
            )[0].value,
        );
      } catch (error) {
        if ((error as { errno: string }).errno !== "P0F01") throw error;
        return withDatabaseTransaction(
          db,
          { deadline },
          async (tx) =>
            (
              await tx`SELECT core_abort_recovery_slow_v1(${projection(p)}::jsonb,${e}::jsonb,${d}::jsonb) value`
            )[0].value,
        );
      }
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
    async expireQuota(p: CoreVerifiedCallerProjection, limit: number) {
      const version = quotaCursorVersion;
      const input = { limit, after: quotaCursor };
      const result = (
        await db`SELECT core_quota_recovery_v1(${projection(p)}::jsonb,${input}::jsonb) value`
      )[0].value;
      if (quotaCursorVersion === version) {
        quotaCursor = result.cursor;
        quotaCursorVersion++;
      }
      return result;
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
      const until =
        performance.now() + Math.min(Date.parse(p.deadline_at) - Date.now(), busyRetryMs);
      let lastBusy: unknown;
      for (let delay = 1; ; delay = Math.min(delay * 2, 16)) {
        if (lastBusy !== undefined && !(performance.now() < until)) throw lastBusy;
        try {
          return (
            await db`SELECT core_quota_v1(${projection(p)}::jsonb,${attempt}::uuid,${action},${descriptors}::jsonb) value`
          )[0].value;
        } catch (error) {
          const code = (error as { errno?: string } | null)?.errno;
          if ((code !== "55P03" && code !== "P0503") || !(performance.now() + delay < until))
            throw error;
          lastBusy = error;
        }
        await Bun.sleep(delay);
      }
    },
  };
}
