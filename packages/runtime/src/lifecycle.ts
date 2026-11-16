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
