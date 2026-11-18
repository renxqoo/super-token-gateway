import type { SQL } from "bun";
import { CoreRpcError, type createCoreRpcClient } from "@gateway/core-client";
import { executionReplySchema, quotaRecoveryResultSchema } from "@gateway/contracts";

type CoreClient = Pick<ReturnType<typeof createCoreRpcClient>, "execution">;

export function startPeriodic(
  task: () => Promise<boolean>,
  intervalMs: number,
  onError: (error: unknown) => void,
) {
  let stopped = false;
  let requested = false;
  let backingOff = false;
  let wake: (() => void) | undefined;
  const loop = (async () => {
    while (!stopped) {
      requested = false;
      backingOff = false;
      let more = false;
      try {
        more = await task();
      } catch (error) {
        backingOff = true;
        onError(error);
      }
      if (stopped || more || (!backingOff && requested)) continue;
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
    trigger() {
      if (stopped) return;
      requested = true;
      if (!backingOff) wake?.();
    },
    async close() {
      stopped = true;
      wake?.();
      await loop;
    },
  };
}

export async function recoverExecutions(db: SQL, core: CoreClient, batch: number) {
  const released = quotaRecoveryResultSchema.parse(
    await core.execution("execution_quota_recovery_v1", { limit: batch }),
  );
  const expired = await db<
    { attempt_id: string }[]
  >`SELECT attempt_id FROM execution_permits WHERE state='RESERVED' AND granted_at IS NULL AND expires_at<=now() ORDER BY expires_at,attempt_id LIMIT ${batch}`;
  let aborted = 0;
  for (const { attempt_id } of expired) {
    try {
      const { receipt } = executionReplySchema.parse(
        await core.execution(
          "execution_abort_recovery_v1",
          { attempt_id },
          {
            commandId: crypto.randomUUID(),
            deadlineAt: new Date(Date.now() + 10000).toISOString(),
          },
        ),
      );
      if (
        receipt.command_state === "COMMITTED" &&
        receipt.state === "ABORTED" &&
        receipt.attempt_id === attempt_id
      )
        aborted++;
    } catch (error) {
      if (!(error instanceof CoreRpcError) || error.status !== 409) throw error;
    }
  }
  const count = released.released;
  return {
    released: count,
    aborted,
    candidates: expired.length,
    blocked: released.failed > 0,
    expired: released.expired,
    more: released.failed === 0 && (count === batch || aborted === batch || released.expiry_more),
  };
}
