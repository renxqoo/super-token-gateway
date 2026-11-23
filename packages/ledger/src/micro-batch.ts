export class MicroBatchQueue<I, O> {
  private pending: {
    input: I;
    resolve: (value: O | PromiseLike<O>) => void;
    reject: (err: unknown) => void;
  }[] = [];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private microtaskScheduled = false;
  private inflight = 0;
  private drainWaiters: (() => void)[] = [];

  constructor(
    private readonly executeBatch: (items: I[]) => Promise<(O | PromiseLike<O>)[]>,
    private readonly maxBatch: number,
    private readonly waitMs: number,
    private readonly maxConcurrency: number,
  ) {}

  enqueue(input: I): Promise<O> {
    return new Promise<O>((resolve, reject) => {
      this.pending.push({ input, resolve, reject });
      if (this.pending.length >= this.maxBatch) {
        if (this.timer) {
          clearTimeout(this.timer);
          this.timer = undefined;
        }
        this.pump();
      } else if (this.inflight === 0) {
        if (!this.microtaskScheduled) {
          this.microtaskScheduled = true;
          queueMicrotask(() => {
            this.microtaskScheduled = false;
            if (this.pending.length > 0 && this.inflight < this.maxConcurrency) {
              if (this.timer) {
                clearTimeout(this.timer);
                this.timer = undefined;
              }
              this.pump();
            }
          });
        }
      } else if (this.inflight < this.maxConcurrency && !this.timer) {
        if (this.waitMs <= 0) {
          this.pump();
        } else {
          this.timer = setTimeout(() => {
            this.timer = undefined;
            this.pump();
          }, this.waitMs);
        }
      }
    });
  }

  private checkDrained(): void {
    if (this.pending.length === 0 && this.inflight === 0 && this.drainWaiters.length > 0) {
      const waiters = this.drainWaiters.splice(0, this.drainWaiters.length);
      for (const resolve of waiters) resolve();
    }
  }

  private pump(): void {
    while (this.pending.length > 0 && this.inflight < this.maxConcurrency) {
      const slice = this.pending.splice(0, this.maxBatch);
      const inputs = Array.from<I>({ length: slice.length });
      for (let i = 0; i < slice.length; i++) inputs[i] = slice[i]!.input;
      this.inflight++;
      void this.executeBatch(inputs)
        .then(
          (results) => {
            for (let i = 0; i < slice.length; i++) slice[i]!.resolve(results[i]!);
          },
          (error) => {
            for (let i = 0; i < slice.length; i++) slice[i]!.reject(error);
          },
        )
        .finally(() => {
          this.inflight--;
          if (this.pending.length > 0) this.pump();
          else this.checkDrained();
        });
    }
  }

  async drain(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    this.pump();
    if (this.pending.length === 0 && this.inflight === 0) return;
    await new Promise<void>((resolve) => {
      this.drainWaiters.push(resolve);
    });
  }
}
