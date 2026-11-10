import type { UsageEvidence, Usage } from "@gateway/kernel";
import type { TokenSummary } from "@gateway/storage";
import type { Dependencies } from "./context.ts";
export async function publishEvidence(
  deps: Dependencies,
  attemptId: string,
  state: UsageEvidence["state"],
  finality: UsageEvidence["finality"],
  usage: Usage | undefined,
  code?: string,
  providerRequestId?: string,
  requestState?: UsageEvidence["requestState"],
): Promise<TokenSummary> {
  try {
    const result = await deps.usage.publish({
      attemptId,
      state,
      finality,
      usage: usage ?? {},
      source: "execution",
      ...(code ? { code } : {}),
      ...(providerRequestId ? { providerRequestId } : {}),
      ...(requestState ? { requestState } : {}),
    });
    deps.metrics.increment("usage_queue_accepted_total");
    return result;
  } catch (error) {
    deps.metrics.increment("usage_queue_failures_total");
    throw error;
  }
}
