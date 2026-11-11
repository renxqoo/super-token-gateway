import { SSEParser, optionalObject, parseProviderJson } from "@gateway/kernel";

export class LatencyHistogram {
  private readonly bins = new Float64Array(4096);
  count = 0;
  add(milliseconds: number): void {
    if (!Number.isFinite(milliseconds) || milliseconds < 0) return;
    const i = Math.min(this.bins.length - 1, Math.ceil(Math.log2(milliseconds * 1000 + 1) * 128));
    this.bins[i] = (this.bins[i] ?? 0) + 1;
    this.count++;
  }
  quantile(p: number): number {
    const rank = Math.max(1, Math.ceil(this.count * p));
    let seen = 0;
    for (let i = 0; i < this.bins.length; i++) {
      seen += this.bins[i]!;
      if (seen >= rank) return Math.ceil((2 ** (i / 128) - 1) / 10) / 100;
    }
    return 0;
  }
}
export function validChat(body: string, stream: boolean, expectedText?: string): boolean {
  try {
    if (!stream) {
      const value = optionalObject(parseProviderJson(body));
      if (value.error || !Array.isArray(value.choices) || !value.choices.length) return false;
      return value.choices.every((raw) => {
        const choice = optionalObject(raw);
        const message = optionalObject(choice.message);
        return (
          typeof choice.finish_reason === "string" &&
          !!choice.finish_reason &&
          message.role === "assistant" &&
          ((typeof message.content === "string" &&
            !!message.content &&
            (!expectedText || message.content.includes(expectedText))) ||
            (Array.isArray(message.tool_calls) &&
              message.tool_calls.length > 0 &&
              message.tool_calls.every((raw) => {
                const call = optionalObject(raw);
                const fn = optionalObject(call.function);
                return (
                  typeof call.id === "string" &&
                  !!call.id &&
                  typeof fn.name === "string" &&
                  !!fn.name &&
                  typeof fn.arguments === "string"
                );
              })))
        );
      });
    }
    const parser = new SSEParser(1048576);
    const events = parser.feed(new TextEncoder().encode(body), true);
    if (parser.incomplete) return false;
    let done = 0;
    let finished = false;
    let content = "";
    let tool = false;
    for (const event of events) {
      if (!event.data) continue;
      if (event.data === "[DONE]") {
        done++;
        continue;
      }
      if (done) return false;
      const value = optionalObject(parseProviderJson(event.data));
      if (value.error || event.event === "error" || value.type === "error") return false;
      if (!Array.isArray(value.choices)) return false;
      for (const raw of value.choices) {
        const c = optionalObject(raw);
        const delta = optionalObject(c.delta);
        if (typeof delta.content === "string") content += delta.content;
        if (Array.isArray(delta.tool_calls) && delta.tool_calls.length) tool = true;
        if (typeof c.finish_reason === "string" && c.finish_reason) finished = true;
      }
    }
    return (
      done === 1 &&
      finished &&
      (tool || (!!content && (!expectedText || content.includes(expectedText))))
    );
  } catch {
    return false;
  }
}

export function passesCapacityTarget(
  dataValid: boolean,
  result: { succeeded: number; duration_seconds: number },
  targetRps: number,
): boolean {
  return (
    dataValid &&
    Number.isSafeInteger(result.succeeded) &&
    result.succeeded >= 0 &&
    Number.isFinite(result.duration_seconds) &&
    result.duration_seconds > 0 &&
    Number.isFinite(targetRps) &&
    targetRps > 0 &&
    result.succeeded / result.duration_seconds >= targetRps
  );
}

export const CONTRACT_MAX_P99_MS = 1500;

export function passesOnlineCapacity(
  valid: boolean,
  http: {
    succeeded: number;
    duration_seconds: number;
    latency_ms: { p99: number };
    latency_windows: Record<string, { p99: number }>;
  },
  targetRps: number,
  seconds: number,
  minuteCompletions: number[],
  maxP99Ms: number,
  drainAfterWindowMs: number,
): boolean {
  return (
    passesCapacityTarget(valid, http, targetRps) &&
    Number.isFinite(drainAfterWindowMs) &&
    drainAfterWindowMs >= 0 &&
    drainAfterWindowMs <= 120000 &&
    Number.isSafeInteger(seconds) &&
    seconds >= 60 &&
    seconds <= 3600 &&
    seconds % 60 === 0 &&
    Number.isFinite(maxP99Ms) &&
    maxP99Ms > 0 &&
    maxP99Ms <= CONTRACT_MAX_P99_MS &&
    Number.isFinite(http.latency_ms.p99) &&
    http.latency_ms.p99 >= 0 &&
    http.latency_ms.p99 <= maxP99Ms &&
    minuteCompletions.length === seconds / 60 &&
    minuteCompletions.every((count, index) => {
      const p99 = http.latency_windows[String(index)]?.p99;
      return (
        Number.isSafeInteger(count) &&
        count >= targetRps * 60 &&
        p99 !== undefined &&
        Number.isFinite(p99) &&
        p99 >= 0 &&
        p99 <= maxP99Ms
      );
    })
  );
}
