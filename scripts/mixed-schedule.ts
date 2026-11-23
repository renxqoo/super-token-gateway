export function createMixedSchedule(seconds: number, rate: number, concurrency: number) {
  const total = seconds * rate;
  const intervalMs = 1000 / rate;
  const deadlineMs = seconds * 1000 + 50;
  const catchUpPerTick = Math.max(1, Math.min(256, Math.ceil(rate * 0.015)));
  let started = 0;
  let maxDebt = 0;
  return {
    take(elapsedMs: number, inFlight: number) {
      const first = started;
      if (elapsedMs <= deadlineMs) {
        const due = Math.min(total, Math.floor(elapsedMs / intervalMs) + 1);
        const available = Math.max(0, due - started);
        maxDebt = Math.max(maxDebt, available);
        started += Math.min(available, Math.max(0, concurrency - inFlight), catchUpPerTick);
      }
      return { first, end: started };
    },
    done(elapsedMs: number) {
      return started === total || elapsedMs > deadlineMs;
    },
    plannedAt(index: number) {
      return index * intervalMs;
    },
    status() {
      return { total, started, maxDebt };
    },
  };
}
