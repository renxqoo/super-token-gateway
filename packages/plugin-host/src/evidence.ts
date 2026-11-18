import { canonical, dimensions } from "@gateway/kernel";
import type { AbiResult, Hook } from "@gateway/plugin-sdk";
import type { createPluginAttempts } from "./attempts.ts";

type PluginAttempt = ReturnType<ReturnType<typeof createPluginAttempts>["create"]>;
type Finality = "FINAL" | "PARTIAL" | "UNKNOWN" | "ESTIMATED" | "NOT_EXECUTED";
export type ObservedEvidence = {
  event_id: string;
  attempt_id: string;
  permit_epoch: string;
  source: "execution";
  source_revision: string;
  finality: Finality;
  usage: Record<string, string>;
  final_dimensions: string[];
  attempt_state?: "SUCCEEDED" | "FAILED" | "UNKNOWN" | "PARTIAL" | "NOT_EXECUTED";
  request_state?: "SUCCEEDED" | "FAILED" | "CANCELLED" | "UNKNOWN";
  error_code?: string;
  provider_request_id?: string;
};
export type AttemptOutcome = Pick<
  ObservedEvidence,
  "request_state" | "error_code" | "provider_request_id"
> & { attempt_state?: "SUCCEEDED" | "FAILED" | "UNKNOWN" | "PARTIAL" };

export function pricedDimensions(usage: Record<string, string>): string[] {
  return Object.keys(usage)
    .filter((key) => (dimensions as readonly string[]).includes(key))
    .sort();
}

export function observeAttempt<Sealed>(
  attempt: PluginAttempt,
  binding: {
    attemptId: string;
    permitEpoch: string;
    delivery: "progressive" | "terminal-group";
    seal: (evidence: ObservedEvidence) => Sealed;
    record: (evidence: Sealed[]) => Promise<void>;
  },
) {
  let revision = 0n,
    usage: Record<string, string> = {},
    finality: Finality = "UNKNOWN",
    last = "",
    finished = false,
    disposed = false,
    active = false;
  let pending: { event: Sealed } | undefined;
  function open() {
    if (disposed) throw new Error("Attempt evidence disposed");
    if (finished) throw new Error("Attempt evidence already finished");
  }
  async function operation<T>(run: () => Promise<T>) {
    open();
    if (active) throw new Error("Attempt evidence operation already active");
    active = true;
    try {
      const result = await run();
      if (disposed) throw new Error("Attempt evidence disposed");
      return result;
    } finally {
      active = false;
    }
  }
  function seal(
    outcome: Pick<
      ObservedEvidence,
      "attempt_state" | "request_state" | "error_code" | "provider_request_id"
    > = {},
  ) {
    revision++;
    return binding.seal({
      event_id: crypto.randomUUID(),
      attempt_id: binding.attemptId,
      permit_epoch: binding.permitEpoch,
      source: "execution",
      source_revision: revision.toString(),
      finality,
      usage: { ...usage },
      final_dimensions: finality === "FINAL" ? pricedDimensions(usage) : [],
      ...outcome,
    });
  }
  async function flush() {
    if (!pending) return;
    const current = pending;
    await binding.record([current.event]);
    if (pending === current) pending = undefined;
  }
  async function emit(defer: boolean) {
    if (pending) await flush();
    open();
    const event = seal();
    open();
    if (defer && finality === "FINAL") pending = { event };
    else await binding.record([event]);
  }
  async function terminal(outcome: Parameters<typeof seal>[0], releasePlugin = false) {
    finished = true;
    try {
      const event = seal(outcome);
      if (disposed) throw new Error("Attempt evidence disposed");
      const delivery = binding.record(pending ? [pending.event, event] : [event]);
      if (
        releasePlugin &&
        pending &&
        binding.delivery === "terminal-group" &&
        outcome?.attempt_state === "SUCCEEDED" &&
        outcome.request_state === "SUCCEEDED"
      )
        attempt.dispose();
      await delivery;
    } finally {
      pending = undefined;
    }
  }
  async function observe(result: AbiResult, defer: boolean) {
    open();
    if (result.kind !== "output" && result.kind !== "failure") return;
    if (!result.usage) return;
    await update(
      {
        usage: { ...result.usage } as Record<string, string>,
        finality: result.kind === "output" ? result.finality : "UNKNOWN",
      },
      defer,
    );
  }
  async function update(
    next: { usage: Record<string, string>; finality: Finality },
    defer: boolean,
  ) {
    const key = canonical(next);
    if (key === last) return;
    usage = next.usage;
    finality = next.finality;
    await emit(defer);
    last = key;
  }
  return {
    async invoke(hook: Hook, payload?: Uint8Array, signal?: AbortSignal) {
      return operation(async () => {
        const result = await attempt.invoke(hook, payload, signal);
        await observe(result, hook === "response" && binding.delivery === "terminal-group");
        return result;
      });
    },
    async transformJSON(
      hook: "prepare" | "response" | "error" | "resource_response",
      metadata: Record<string, unknown>,
      body: Uint8Array,
      signal?: AbortSignal,
    ) {
      return operation(async () => {
        const transformed = await attempt.transformJSON(hook, metadata, body, signal);
        await observe(
          transformed.result,
          hook === "response" && binding.delivery === "terminal-group",
        );
        return transformed;
      });
    },
    async declare(declared: Record<string, string>) {
      return operation(() => update({ usage: { ...declared }, finality: "FINAL" }, false));
    },
    async finish(outcome: AttemptOutcome, releasePlugin = false) {
      return operation(() => terminal(outcome, releasePlugin));
    },
    async notExecuted(outcome: Pick<ObservedEvidence, "request_state" | "error_code">) {
      return operation(async () => {
        if (revision !== 0n) throw new Error("Attempt evidence already observed");
        finality = "NOT_EXECUTED";
        await terminal({ ...outcome, attempt_state: "NOT_EXECUTED" });
      });
    },
    flush: () => operation(flush),
    dispose() {
      if (disposed) return;
      disposed = true;
      pending = undefined;
      attempt.dispose();
    },
  };
}
