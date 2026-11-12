import { GatewayError } from "@gateway/contracts";

export class Lifecycle {
  private active = new Set<Promise<unknown>>();
  private controllers = new Set<AbortController>();
  accepting = true;
  constructor(readonly maxInFlight: number) {}
  enter(): { controller: AbortController; release: () => void } {
    if (!this.accepting || this.controllers.size >= this.maxInFlight)
      throw new GatewayError(503, "overloaded", "Gateway admission capacity reached");
    const controller = new AbortController();
    this.controllers.add(controller);
    return {
      controller,
      release: () => {
        this.controllers.delete(controller);
      },
    };
  }
  track<T>(promise: Promise<T>): Promise<T> {
    this.active.add(promise);
    void promise.then(
      () => this.active.delete(promise),
      () => this.active.delete(promise),
    );
    return promise;
  }
  async drain(graceMs: number, cleanupMs = 30000): Promise<boolean> {
    this.accepting = false;
    const until = Date.now() + graceMs;
    while ((this.active.size || this.controllers.size) && Date.now() < until) await Bun.sleep(20);
    for (const controller of this.controllers) controller.abort(new Error("Gateway shutting down"));
    const cleanupUntil = Date.now() + cleanupMs;
    while ((this.active.size || this.controllers.size) && Date.now() < cleanupUntil)
      await Bun.sleep(20);
    return this.active.size === 0 && this.controllers.size === 0;
  }
  get size(): number {
    return this.controllers.size;
  }
}

export class Metrics {
  private counters = new Map<string, number>();
  private readonly buckets = [
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1,
    2.5,
    5,
    10,
    30,
    60,
    180,
    Infinity,
  ];
  private readonly durations = Array.from({ length: 16 }, () => 0);
  private count = 0;
  private total = 0;
  private readonly usageLag = Array.from({ length: 16 }, () => 0);
  private usageLagCount = 0;
  private usageLagTotal = 0;
  observeUsageLag(seconds: number, events: number): void {
    this.usageLagCount += events;
    this.usageLagTotal += seconds * events;
    for (let i = 0; i < this.buckets.length; i++)
      if (seconds <= this.buckets[i]!) this.usageLag[i] = (this.usageLag[i] ?? 0) + events;
  }
  observeDuration(seconds: number): void {
    this.count++;
    this.total += seconds;
    for (let i = 0; i < this.buckets.length; i++)
      if (seconds <= (this.buckets[i] ?? Infinity))
        this.durations[i] = (this.durations[i] ?? 0) + 1;
  }

  increment(name: string, amount = 1): void {
    this.counters.set(name, (this.counters.get(name) ?? 0) + amount);
  }
  render(inFlight: number, bufferedBytes = 0): string {
    return (
      [...this.counters.entries()].map(([key, value]) => `gateway_${key} ${value}`).join("\n") +
      `\ngateway_inflight ${inFlight}\ngateway_buffered_bytes ${bufferedBytes}\ngateway_rss_bytes ${process.memoryUsage().rss}\ngateway_cpu_seconds_total ${(process.cpuUsage().user + process.cpuUsage().system) / 1000000}\n` +
      this.buckets
        .map(
          (bucket, i) =>
            `gateway_request_duration_seconds_bucket{le="${bucket === Infinity ? "+Inf" : bucket}"} ${this.durations[i]}`,
        )
        .join("\n") +
      `\ngateway_request_duration_seconds_count ${this.count}\ngateway_request_duration_seconds_sum ${this.total}\n` +
      this.buckets
        .map(
          (bucket, i) =>
            `gateway_usage_ingest_lag_seconds_bucket{le="${bucket === Infinity ? "+Inf" : bucket}"} ${this.usageLag[i]}`,
        )
        .join("\n") +
      `\ngateway_usage_ingest_lag_seconds_count ${this.usageLagCount}\ngateway_usage_ingest_lag_seconds_sum ${this.usageLagTotal}\n`
    );
  }
}
