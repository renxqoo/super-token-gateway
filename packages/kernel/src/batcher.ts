import { GatewayError } from "./errors.ts";
/** A bounded microbatch is acknowledged only after the shared database transaction commits. */
export class Batcher<Input, Output> {
  private pending: {
    input: Input;
    weight: number;
    resolve: (output: Output) => void;
    reject: (error: unknown) => void;
  }[] = [];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private active = 0;
  private pendingWeight = 0;
  constructor(
    private readonly run: (inputs: Input[]) => Promise<Output[]>,
    private readonly capacity = 64,
    private readonly delayMs = 1,
    private readonly maxActive = 4,
    private readonly maxQueued = 4096,
    private readonly weight: (input: Input) => number = () => 1,
  ) {}
  add(input: Input): Promise<Output> {
    const weight = this.weight(input);
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
  private schedule(): void {
    if (this.active >= this.maxActive || !this.pending.length) return;
    if (this.pendingWeight >= this.capacity) {
      clearTimeout(this.timer);
      this.timer = undefined;
      this.flush();
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
