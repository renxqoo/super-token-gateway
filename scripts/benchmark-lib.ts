import { SSEParser, optionalObject, parseProviderJson } from "@gateway/kernel";

/** Fixed 32 KiB logarithmic histogram, independent of run duration or request count. */
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
