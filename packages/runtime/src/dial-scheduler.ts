import { UpstreamPreDispatchError } from "./http-pool.ts";

export type DialLimits = {
  concurrency?: number;
  startsPerSecond?: number;
  maxQueueWaitMs?: number;
  maxQueueBytes?: number;
};

export type DialPermit = {
  start(): (() => void) | undefined;
  release(): void;
};

type Waiter = { grant: () => void; reject: (error: UpstreamPreDispatchError) => void };
type Destination = {
  tokens: number;
  updatedAt: number;
  reserved: number;
  active: number;
  waiting: Set<Waiter>;
  timer?: ReturnType<typeof setTimeout> | undefined;
};

const capacity = () =>
  new UpstreamPreDispatchError("upstream_capacity", "Upstream dial queue capacity exceeded");
const cancelled = () =>
  new UpstreamPreDispatchError(
    "upstream_not_dispatched",
    "Upstream dial cancelled before dispatch",
  );

export function createDialScheduler(maxQueued: number, limits: DialLimits = {}) {
  const {
    concurrency = 16,
    startsPerSecond = 400,
    maxQueueWaitMs = 10_000,
    maxQueueBytes = 64 * 1024 * 1024,
  } = limits;
  if (
    ![concurrency, startsPerSecond, maxQueueWaitMs, maxQueueBytes].every(
      (value) => Number.isSafeInteger(value) && value > 0,
    )
  ) {
    throw new RangeError("Invalid upstream dial limits");
  }
  const rate = startsPerSecond / 1000;
  const destinations = new Map<string, Destination>();
  let queued = 0;
  let queuedBytes = 0;
  let closed = false;

  function refill(state: Destination) {
    const now = performance.now();
    state.tokens = Math.min(concurrency, state.tokens + (now - state.updatedAt) * rate);
    state.updatedAt = now;
  }

  function canGrant(state: Destination) {
    return state.active < concurrency && state.tokens - state.reserved >= 1;
  }

  function pump(target: string, state: Destination) {
    clearTimeout(state.timer);
    state.timer = undefined;
    if (closed) return;
    refill(state);
    while (canGrant(state) && state.waiting.size) {
      state.waiting.values().next().value!.grant();
    }
    let delay: number;
    if (state.waiting.size) {
      if (state.active === concurrency) return;
      delay = (state.reserved + 1 - state.tokens) / rate;
    } else {
      if (state.active) return;
      if (state.tokens >= concurrency) {
        destinations.delete(target);
        return;
      }
      delay = (concurrency - state.tokens) / rate;
    }
    state.timer = setTimeout(() => pump(target, state), Math.max(1, Math.ceil(delay)));
    state.timer.unref();
  }

  function reserve(target: string, state: Destination): DialPermit {
    state.reserved++;
    state.active++;
    let reserved = true;
    const releaseActive = () => {
      state.active--;
      pump(target, state);
    };
    return {
      start() {
        if (!reserved || closed) return undefined;
        reserved = false;
        refill(state);
        state.reserved--;
        state.tokens--;
        let completed = false;
        return () => {
          if (completed) return;
          completed = true;
          releaseActive();
        };
      },
      release() {
        if (!reserved) return;
        reserved = false;
        state.reserved--;
        releaseActive();
      },
    };
  }

  return {
    acquire(
      target: string,
      bytes: number,
      signal?: AbortSignal | null,
      deadline?: number,
    ): DialPermit | Promise<DialPermit> {
      const now = performance.now();
      if (closed || signal?.aborted || (deadline !== undefined && deadline <= now)) {
        throw cancelled();
      }
      let state = destinations.get(target);
      if (!state) {
        state = { tokens: concurrency, updatedAt: now, reserved: 0, active: 0, waiting: new Set() };
        destinations.set(target, state);
      }
      refill(state);
      if (!state.waiting.size && canGrant(state)) return reserve(target, state);
      if (queued >= maxQueued || bytes > maxQueueBytes - queuedBytes) throw capacity();
      const destination = state;
      return new Promise<DialPermit>((resolve, reject) => {
        const budget = now + maxQueueWaitMs;
        const due = Math.min(budget, deadline ?? Infinity);
        const timedOut = () =>
          deadline !== undefined && deadline <= budget ? cancelled() : capacity();
        const cleanup = () => {
          if (!destination.waiting.delete(waiter)) return false;
          queued--;
          queuedBytes -= bytes;
          clearTimeout(timer);
          signal?.removeEventListener("abort", abort);
          return true;
        };
        const waiter: Waiter = {
          grant() {
            if (!cleanup()) return;
            if (performance.now() >= due) reject(timedOut());
            else resolve(reserve(target, destination));
          },
          reject(error) {
            if (!cleanup()) return;
            reject(error);
            pump(target, destination);
          },
        };
        const abort = () => waiter.reject(cancelled());
        const timer = setTimeout(() => waiter.reject(timedOut()), Math.max(0, due - now));
        timer.unref();
        queued++;
        queuedBytes += bytes;
        destination.waiting.add(waiter);
        signal?.addEventListener("abort", abort, { once: true });
        pump(target, destination);
      });
    },
    close() {
      if (closed) return;
      closed = true;
      for (const state of destinations.values()) {
        clearTimeout(state.timer);
        for (const waiter of state.waiting) waiter.reject(cancelled());
      }
      destinations.clear();
    },
  };
}
