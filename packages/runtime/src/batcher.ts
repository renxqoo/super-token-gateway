import { GatewayError } from "@gateway/contracts";
export class Batcher<Input, Output> {
  private pending: {
    input: Input;
    weight: number;
    resolve: (output: Output) => void;
    reject: (error: unknown) => void;
  }[] = [];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private immediateScheduled = false;
  private active = 0;
  private pendingWeight = 0;
  private closed: Error | undefined;
  private readonly minIdleBatch: number;
  constructor(
    private readonly run: (inputs: Input[]) => Promise<Output[]>,
    private readonly capacity = 64,
    private readonly delayMs = 1,
    private readonly maxActive = 4,
    private readonly maxQueued = 4096,
    private readonly options: {
      weight?: (input: Input) => number;
      collectWhenIdle?: boolean | number;
    } = {},
  ) {
    const collectWhenIdle = options.collectWhenIdle;
    this.minIdleBatch = collectWhenIdle === true ? 1 : collectWhenIdle || Infinity;
  }
  add(input: Input): Promise<Output> {
    if (this.closed) return Promise.reject(this.closed);
    const weight = this.options.weight?.(input) ?? 1;
    if (!Number.isSafeInteger(weight) || weight < 1 || weight > this.capacity)
      return Promise.reject(new RangeError("Batch item weight exceeds capacity"));
    if (this.pendingWeight + weight > this.maxQueued)
      return Promise.reject(
        new GatewayError(503, "overloaded", "Database batching capacity reached"),
      );
    return new Promise((resolve, reject) => {
      this.pending.push({ input, weight, resolve, reject });
      this.pendingWeight += weight;
      this.schedule();
    });
  }
  close(error: Error): void {
    if (this.closed) return;
    this.closed = error;
    clearTimeout(this.timer);
    this.timer = undefined;
    for (const item of this.pending) item.reject(error);
    this.pending = [];
    this.pendingWeight = 0;
  }
  private schedule(): void {
    if (this.active >= this.maxActive || !this.pending.length) return;
    if (this.pendingWeight >= this.capacity) {
      clearTimeout(this.timer);
      this.timer = undefined;
      this.flush();
    } else if (this.active === 0 && this.pending.length < this.minIdleBatch) {
      clearTimeout(this.timer);
      this.timer = undefined;
      if (!this.immediateScheduled) {
        this.immediateScheduled = true;
        queueMicrotask(() => {
          this.immediateScheduled = false;
          if (this.active === 0 && this.pending.length < this.minIdleBatch) this.flush();
          else this.schedule();
        });
      }
    } else if (!this.timer)
      this.timer = setTimeout(() => {
        this.timer = undefined;
        this.flush();
      }, this.delayMs);
  }
  private flush(): void {
    if (this.active >= this.maxActive || !this.pending.length) return;
    let count = 0,
      weight = 0;
    for (const item of this.pending) {
      if (weight + item.weight > this.capacity) break;
      weight += item.weight;
      count++;
    }
    const items = this.pending.splice(0, count);
    this.pendingWeight -= weight;
    this.active++;
    void this.run(items.map((item) => item.input))
      .then((outputs) => {
        if (outputs.length !== items.length)
          throw new Error("Database batch result cardinality mismatch");
        items.forEach((item, index) => item.resolve(outputs[index]!));
      })
      .catch((error) => {
        for (const item of items) item.reject(error);
      })
      .finally(() => {
        this.active--;
        this.schedule();
      });
    this.schedule();
  }
}
