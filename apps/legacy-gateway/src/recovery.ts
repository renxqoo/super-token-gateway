import type { Dependencies } from "./context.ts";
import { queryResource } from "./resources.ts";

export async function recoveryTick(deps: Dependencies): Promise<void> {
  await recoverStale(deps);
  const batch = await deps.store.claimResources(4);
  const outcomes = await Promise.allSettled(
    batch.resources.map(async (resource) => {
      let failed = false;
      let release: (() => Promise<void>) | undefined;
      try {
        const provider = await deps.store.provider(resource.provider_id);
        release = await deps.limits.enter(
          `provider:${resource.provider_id}`,
          `poll:${resource.id}`,
          provider.rpm,
          provider.concurrency,
          20000,
        );
        const response = await queryResource(deps, resource, "get", AbortSignal.timeout(10000));
        await response.body?.cancel();
      } catch {
        failed = true;
        deps.metrics.increment("resource_poll_failures_total");
      } finally {
        await release?.();
        await deps.store.completePoll(resource.id, batch.token, failed);
      }
    }),
  );
  if (outcomes.some((result) => result.status === "rejected"))
    throw new Error("Recovery batch failed after all tasks completed");
}

export async function recoverStale(deps: Dependencies): Promise<number> {
  const attempts = await deps.store.staleAttempts(
    Math.ceil(deps.config.TOTAL_TIMEOUT_MS / 1000) + 60,
  );
  for (const attempt of attempts)
    await deps.usage.publish({
      attemptId: attempt.id,
      source: "recovery_interruption",
      state: "UNKNOWN",
      finality: "UNKNOWN",
      usage: {},
      code: "process_interrupted",
      requestState: "UNKNOWN",
    });
  return attempts.length;
}
