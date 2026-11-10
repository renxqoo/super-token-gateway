import { GatewayError } from "@gateway/kernel";
export class MemoryBudget {
  private used = 0;
  constructor(readonly limit: number) {}
  reserve(bytes: number): void {
    if (bytes < 0 || !Number.isSafeInteger(bytes)) throw new Error("Invalid memory reservation");
    if (this.used + bytes > this.limit)
      throw new GatewayError(503, "memory_limited", "Gateway buffered-byte capacity reached");
    this.used += bytes;
  }
  release(bytes: number): void {
    this.used = Math.max(0, this.used - bytes);
  }
  get bufferedBytes(): number {
    return this.used;
  }
  lease(): { update: (bytes: number) => void; close: () => void } {
    let bytes = 0;
    let closed = false;
    return {
      update: (delta) => {
        if (closed) return;
        if (delta >= 0) {
          this.reserve(delta);
          bytes += delta;
        } else {
          const released = Math.min(bytes, -delta);
          this.release(released);
          bytes -= released;
        }
      },
      close: () => {
        if (!closed) {
          closed = true;
          this.release(bytes);
          bytes = 0;
        }
      },
    };
  }
}
