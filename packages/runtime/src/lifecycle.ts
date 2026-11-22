import { GatewayError } from "@gateway/contracts";

export class Lifecycle {
  private active = new Set<Promise<unknown>>();
  private handlers = new Set<() => void>();
  accepting = true;
  constructor(readonly maxInFlight: number) {}
  enterCallback(onDrain: () => void): { release: () => void } {
    if (!this.accepting || this.size >= this.maxInFlight)
      throw new GatewayError(503, "overloaded", "Gateway admission capacity reached");
    this.handlers.add(onDrain);
    return {
      release: () => {
        this.handlers.delete(onDrain);
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
    while ((this.active.size || this.size) && Date.now() < until) await Bun.sleep(20);
    for (const handler of this.handlers) handler();
    const cleanupUntil = Date.now() + cleanupMs;
    while ((this.active.size || this.size) && Date.now() < cleanupUntil) await Bun.sleep(20);
    return this.active.size === 0 && this.size === 0;
  }
  get size(): number {
    return this.handlers.size;
  }
}
