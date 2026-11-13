import { canonical, dimensions } from "@gateway/kernel";
import type { AbiResult, Hook } from "@gateway/plugin-sdk";
import type { createPluginAttempts } from "./attempts.ts";

type PluginAttempt = ReturnType<ReturnType<typeof createPluginAttempts>["create"]>;
type Finality = "FINAL" | "PARTIAL" | "UNKNOWN" | "ESTIMATED";
export type ObservedEvidence = {
  event_id: string;
  attempt_id: string;
  permit_epoch: string;
  source: "execution";
  source_revision: string;
  finality: Finality;
  usage: Record<string, string>;
  final_dimensions: string[];
  attempt_state?: "SUCCEEDED" | "FAILED" | "UNKNOWN" | "PARTIAL";
  request_state?: "SUCCEEDED" | "FAILED" | "CANCELLED" | "UNKNOWN";
  error_code?: string;
  provider_request_id?: string;
};
export type AttemptOutcome = Pick<
  ObservedEvidence,
  "attempt_state" | "request_state" | "error_code" | "provider_request_id"
>;

function completeDimensions(usage: Record<string, string>): string[] {
  const partitioned =
    usage.cache_write_5m_tokens !== undefined && usage.cache_write_1h_tokens !== undefined;
  return Object.keys(usage)
    .filter((key) => (dimensions as readonly string[]).includes(key))
    .filter((key) => key !== "cache_write_tokens" || partitioned)
    .sort();
}

export function observeAttempt(
  attempt: PluginAttempt,
  binding: {
    attemptId: string;
    permitEpoch: string;
    record: (evidence: ObservedEvidence) => Promise<void>;
  },
) {
  let revision = 0n,
    usage: Record<string, string> = {},
    finality: Finality = "UNKNOWN",
    last = "",
    finished = false;
  async function emit(outcome: AttemptOutcome = {}) {
    revision++;
    await binding.record({
      event_id: crypto.randomUUID(),
      attempt_id: binding.attemptId,
      permit_epoch: binding.permitEpoch,
      source: "execution",
      source_revision: revision.toString(),
      finality,
      usage,
      final_dimensions: finality === "FINAL" ? completeDimensions(usage) : [],
      ...outcome,
    });
  }
  async function observe(result: AbiResult) {
    if (finished) throw new Error("Attempt evidence already finished");
    if (result.kind !== "output" && result.kind !== "failure") return;
    if (!result.usage) return;
    const next = {
      usage: { ...result.usage } as Record<string, string>,
      finality: result.kind === "output" ? result.finality : ("UNKNOWN" as const),
    };
    const key = canonical(next);
    if (key === last) return;
    usage = next.usage;
    finality = next.finality;
    await emit();
    last = key;
  }
  return {
    async invoke(hook: Hook, payload?: Uint8Array, signal?: AbortSignal) {
      const result = await attempt.invoke(hook, payload, signal);
      await observe(result);
      return result;
    },
    async transformJSON(
      hook: "prepare" | "response" | "error",
      metadata: Record<string, unknown>,
      body: Uint8Array,
      signal?: AbortSignal,
    ) {
      const transformed = await attempt.transformJSON(hook, metadata, body, signal);
      await observe(transformed.result);
      return transformed;
    },
    async finish(outcome: AttemptOutcome) {
      if (finished) throw new Error("Attempt evidence already finished");
      finished = true;
      await emit(outcome);
    },
    dispose: () => attempt.dispose(),
  };
}
