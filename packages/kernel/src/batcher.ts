/** A bounded microbatch is acknowledged only after the shared database transaction commits. */
export class Batcher<Input, Output> {
  private pending: {
    input: Input;
    resolve: (output: Output) => void;
    reject: (error: unknown) => void;
  }[] = [];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private active = 0;
  constructor(
    private readonly run: (inputs: Input[]) => Promise<Output[]>,
    private readonly capacity = 64,
    private readonly delayMs = 1,
    private readonly maxActive = 4,
    private readonly maxQueued = 4096,
  ) {}
  add(input: Input): Promise<Output> {
    if (this.pending.length >= this.maxQueued)
      return Promise.reject(new Error("Database batching capacity reached"));
    return new Promise((resolve, reject) => {
      this.pending.push({ input, resolve, reject });
      this.schedule();
    });
  }
  private schedule(): void {
    if (this.active >= this.maxActive || !this.pending.length) return;
    if (this.pending.length >= this.capacity) {
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
    const items = this.pending.splice(0, this.capacity);
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
