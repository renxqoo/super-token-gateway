import { UsageProducer } from "@gateway/runtime";
import type { Operation, UsageEvidence } from "@gateway/kernel";
import type { TokenSummary } from "@gateway/storage";
interface Scope {
  operation: Operation;
  attempts: Map<string, UsageEvidence | undefined>;
  finished?: boolean;
}
/** Per-request quota facts stay local; durable financial evidence is accepted only by the queue. */
export class Metering {
  constructor(
    readonly producer: UsageProducer,
    private readonly state?: Scope,
  ) {}
  scope(operation: Operation): Metering {
    return new Metering(this.producer, { operation, attempts: new Map() });
  }
  register(attemptId: string): void {
    this.state?.attempts.set(attemptId, undefined);
  }
  async publish(evidence: UsageEvidence): Promise<TokenSummary> {
    await this.producer.publish(evidence);
    this.state?.attempts.set(evidence.attemptId, evidence);
    if (this.state && evidence.requestState) this.state.finished = true;
    return this.summary();
  }
  async finish(): Promise<void> {
    if (!this.state || this.state.finished) return;
    const last = [...this.state.attempts.entries()].at(-1);
    if (!last) return;
    const [attemptId, observed] = last;
    await this.publish({
      ...observed,
      attemptId,
      source: "execution_outcome",
      state: observed?.state ?? "UNKNOWN",
      finality: observed?.finality ?? "UNKNOWN",
      usage: observed?.usage ?? {},
      requestState: "FAILED",
    });
  }
  summary(): TokenSummary {
    if (!this.state || !this.state.attempts.size) return { complete: false, tokens: 0 };
    let complete = true;
    let total = 0n;
    for (const evidence of this.state.attempts.values()) {
      if (!evidence) {
        complete = false;
        continue;
      }
      const usage = evidence.usage;
      if (
        evidence.state !== "NOT_EXECUTED" &&
        (evidence.finality !== "FINAL" ||
          usage.input_tokens === undefined ||
          (this.state.operation !== "embeddings" && usage.output_tokens === undefined))
      )
        complete = false;
      total +=
        BigInt(usage.input_tokens ?? "0") +
        BigInt(usage.cached_input_tokens ?? "0") +
        BigInt(usage.cache_write_tokens ?? "0") +
        BigInt(usage.output_tokens ?? "0");
    }
    const tokens = Number(total);
    return { complete: complete && Number.isSafeInteger(tokens) && tokens >= 0, tokens };
  }
  close(): void {
    this.producer.close();
  }
}
