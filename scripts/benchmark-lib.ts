import { SSEParser, parseProviderJson } from "@gateway/adapters/protocol";
import { optionalObject } from "@gateway/contracts";

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
type ChatStreamObservation = "firstFrame" | "firstContent" | "nextContent" | "done" | "errorFrame";

export function createChatStreamReader(
  expected?: string | { exact: string },
  observe?: (event: ChatStreamObservation) => void,
) {
  const parser = new SSEParser(1048576);
  let valid = true;
  let sawFrame = false;
  let sawContent = false;
  let sawDone = false;
  let finished = false;
  let tool = false;
  const expectedText = typeof expected === "string" ? expected : undefined;
  const exact = typeof expected === "object" ? expected.exact : undefined;
  let offset = 0;
  let matched = !expectedText;
  let tail = "";
  function feed(chunk: Uint8Array, flush = false) {
    try {
      for (const event of parser.feed(chunk, flush)) {
        if (event.event === "error") {
          observe?.("errorFrame");
          valid = false;
          continue;
        }
        if (!event.data) continue;
        if (event.data === "[DONE]") {
          if (sawDone) valid = false;
          else observe?.("done");
          sawDone = true;
          continue;
        }
        let value: Record<string, unknown>;
        try {
          value = optionalObject(parseProviderJson(event.data));
        } catch {
          valid = false;
          continue;
        }
        if (value.error || value.type === "error") {
          observe?.("errorFrame");
          valid = false;
          continue;
        }
        if (sawDone || !Array.isArray(value.choices)) {
          valid = false;
          continue;
        }
        if (exact !== undefined && value.choices.length > 1) valid = false;
        for (const raw of value.choices) {
          const choice = optionalObject(raw);
          const hasFinish = typeof choice.finish_reason === "string" && !!choice.finish_reason;
          if (
            exact !== undefined &&
            (finished ||
              (choice.index !== undefined && choice.index !== 0) ||
              (choice.finish_reason !== undefined &&
                choice.finish_reason !== null &&
                choice.finish_reason !== "stop"))
          )
            valid = false;
          if (
            !hasFinish &&
            (!choice.delta || typeof choice.delta !== "object" || Array.isArray(choice.delta))
          ) {
            valid = false;
            continue;
          }
          if (!sawFrame) {
            sawFrame = true;
            observe?.("firstFrame");
          }
          finished ||= hasFinish;
          const delta = optionalObject(choice.delta);
          tool ||= Array.isArray(delta.tool_calls) && delta.tool_calls.length > 0;
          if (typeof delta.content !== "string" || !delta.content) continue;
          if (!sawContent) {
            sawContent = true;
            observe?.("firstContent");
          } else observe?.("nextContent");
          if (exact !== undefined) {
            if (!exact.startsWith(delta.content, offset)) valid = false;
            offset += delta.content.length;
          } else if (!matched && expectedText) {
            const text = tail + delta.content;
            matched = text.includes(expectedText);
            tail = matched ? "" : text.slice(Math.max(0, text.length - expectedText.length + 1));
          }
        }
      }
    } catch {
      valid = false;
    }
  }
  return {
    feed,
    finish() {
      feed(new Uint8Array(), true);
      const outputValid =
        exact === undefined
          ? tool || (sawContent && matched)
          : sawContent && !tool && offset === exact.length;
      return valid && !parser.incomplete && sawDone && finished && outputValid;
    },
  };
}

export function validChat(body: string, stream: boolean, expectedText?: string): boolean {
  if (stream) {
    const reader = createChatStreamReader(expectedText);
    reader.feed(new TextEncoder().encode(body));
    return reader.finish();
  }
  try {
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
export const CONTRACT_MAX_DRAIN_MS = 120000;

export function passesLongStreamCapacity(
  valid: boolean,
  targetRps: number,
  seconds: number,
  streamDurationSeconds: number,
  audit: {
    minuteCompletions: number[];
    longStream?: { minuteStarts: number[]; inflightAtWindowStart: number; shortStreams: number };
  },
  drainAfterWindowMs: number,
): boolean {
  const stream = audit.longStream;
  return (
    valid &&
    Number.isSafeInteger(targetRps) &&
    targetRps > 0 &&
    Number.isSafeInteger(seconds) &&
    seconds >= 60 &&
    seconds <= 3600 &&
    seconds % 60 === 0 &&
    [5, 30, 120].includes(streamDurationSeconds) &&
    drainAfterWindowMs >= 0 &&
    drainAfterWindowMs <= CONTRACT_MAX_DRAIN_MS &&
    !!stream &&
    stream.shortStreams === 0 &&
    stream.inflightAtWindowStart >= targetRps * streamDurationSeconds &&
    stream.minuteStarts.length === seconds / 60 &&
    audit.minuteCompletions.length === seconds / 60 &&
    stream.minuteStarts.every((starts) => starts >= targetRps * 60) &&
    audit.minuteCompletions.every((completions) => completions >= targetRps * 60)
  );
}

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
  completionLatency?: {
    samples: number;
    maxP99Ms: number;
    issuedMinutes: { samples: number; aboveBudget: number }[];
    dispatchesPastDrain: number;
  },
): boolean {
  return (
    passesCapacityTarget(valid, http, targetRps) &&
    Number.isFinite(drainAfterWindowMs) &&
    drainAfterWindowMs >= 0 &&
    drainAfterWindowMs <= CONTRACT_MAX_DRAIN_MS &&
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
    (!completionLatency ||
      (completionLatency.samples === http.succeeded &&
        completionLatency.maxP99Ms === maxP99Ms &&
        completionLatency.dispatchesPastDrain === 0 &&
        completionLatency.issuedMinutes.length >= minuteCompletions.length &&
        completionLatency.issuedMinutes.every(
          ({ samples, aboveBudget }, index) =>
            (samples > 0 || index >= minuteCompletions.length) && aboveBudget * 100 <= samples,
        ))) &&
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
