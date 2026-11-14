import type { SQL } from "bun";
import { CoreRpcError, type createCoreRpcClient } from "@gateway/core-client";

type CoreClient = Pick<ReturnType<typeof createCoreRpcClient>, "execution">;

export function startPeriodic(
  task: () => Promise<boolean>,
  intervalMs: number,
  onError: (error: unknown) => void,
) {
  let stopped = false;
  let wake: (() => void) | undefined;
  const loop = (async () => {
    while (!stopped) {
      let more = false;
      try {
        more = await task();
      } catch (error) {
        onError(error);
      }
      if (stopped || more) continue;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, intervalMs);
        wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
      wake = undefined;
    }
  })();
  return {
    async close() {
      stopped = true;
      wake?.();
      await loop;
    },
  };
}

export async function recoverExecutions(db: SQL, core: CoreClient, batch: number) {
  const released = (await core.execution("execution_quota_recovery_v1", { limit: batch })) as {
    released?: number;
  };
  const expired = await db<
    { attempt_id: string }[]
  >`SELECT attempt_id FROM execution_permits WHERE state='RESERVED' AND granted_at IS NULL AND expires_at<=now() ORDER BY expires_at,attempt_id LIMIT ${batch}`;
  let aborted = 0;
  for (const { attempt_id } of expired) {
    try {
      await core.execution(
        "execution_abort_recovery_v1",
        { attempt_id },
        { commandId: crypto.randomUUID(), deadlineAt: new Date(Date.now() + 10000).toISOString() },
      );
      aborted++;
    } catch (error) {
      if (!(error instanceof CoreRpcError) || error.status !== 409) throw error;
    }
  }
  return { released: released.released ?? 0, aborted, candidates: expired.length };
}
