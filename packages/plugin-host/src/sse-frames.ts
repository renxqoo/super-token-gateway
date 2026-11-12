import { PluginHostError } from "./supervisor.ts";
export type SSEFrame = {
  handle: string;
  dataHandle: string;
  ordinal: number;
  lines: string[];
  event: string;
  id: string;
  data: Uint8Array;
  wire: Uint8Array;
  rawBytes: number;
  charge: number;
};
export function createSSEFrames(
  charge: (bytes: number) => void,
  limits: { maxEventBytes?: number; maxReadyFrames?: number; maxReadyBytes?: number } = {},
) {
  const maxEvent = limits.maxEventBytes ?? 1048576,
    maxFrames = limits.maxReadyFrames ?? 256,
    maxReady = limits.maxReadyBytes ?? 8 << 20;
  if (
    ![maxEvent, maxFrames, maxReady].every((n) => Number.isSafeInteger(n) && n > 0) ||
    maxEvent < 1024 ||
    maxEvent > 16 << 20 ||
    maxFrames > 1024 ||
    maxReady > 64 << 20
  )
    throw new PluginHostError("CONFIG_INVALID");
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }),
    queue: SSEFrame[] = [];
  let blocks: Buffer[] = [],
    used = 0,
    lineStart = 0,
    lineBytes = 0,
    eventBytes = 0,
    pendingCR = false,
    lines: string[] = [],
    reserved = 0,
    ordinal = 0,
    queuedBytes = 0,
    firstLine = true,
    seen = 0,
    bom: number[] = [];
  function adjust(n: number) {
    charge(n);
    reserved += n;
  }
  function range(start: number, end: number) {
    const out = Buffer.allocUnsafe(end - start);
    let offset = 0;
    while (start < end) {
      const index = Math.floor(start / 16384),
        within = start % 16384,
        n = Math.min(end - start, 16384 - within);
      blocks[index]!.copy(out, offset, within, within + n);
      offset += n;
      start += n;
    }
    return out;
  }
  function raw(value: Uint8Array) {
    if (used + value.length > 2 * maxEvent) throw new PluginHostError("SSE_WIRE_BUDGET");
    let offset = 0;
    while (offset < value.length) {
      const block = Math.floor(used / 16384),
        within = used % 16384;
      if (!blocks[block]) {
        adjust(16384 * 12);
        blocks.push(Buffer.allocUnsafe(16384));
      }
      const n = Math.min(value.length - offset, 16384 - within);
      blocks[block]!.set(value.subarray(offset, offset + n), within);
      used += n;
      offset += n;
    }
  }
  function reset() {
    blocks = [];
    used = 0;
    lineStart = 0;
    lineBytes = 0;
    eventBytes = 0;
    pendingCR = false;
    lines = [];
    reserved = 0;
  }
  function finishLine(end: number) {
    let line = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
      range(lineStart, end),
    );
    if (firstLine) {
      line = line.replace(/^\uFEFF/, "");
      firstLine = false;
    }
    eventBytes += Buffer.byteLength(line) + 1;
    if (eventBytes > maxEvent) throw new PluginHostError("SSE_EVENT_BUDGET");
    lineBytes = 0;
    lineStart = used;
    if (line) {
      adjust(256);
      lines.push(line);
      return;
    }
    if (!lines.length) {
      charge(-reserved);
      reset();
      return;
    }
    let event = "message",
      id = "";
    const data: string[] = [];
    for (const current of lines) {
      const colon = current.indexOf(":"),
        name = colon < 0 ? current : current.slice(0, colon),
        rawValue = colon < 0 ? "" : current.slice(colon + 1),
        value = rawValue.startsWith(" ") ? rawValue.slice(1) : rawValue;
      if (name === "data") data.push(value);
      if (name === "event") event = value;
      if (name === "id" && !value.includes("\0")) id = value;
    }
    const wire = new TextEncoder().encode(lines.join("\n") + "\n\n"),
      body = new TextEncoder().encode(data.join("\n"));
    if (queuedBytes + used > maxReady) throw new PluginHostError("SSE_QUEUE_BUDGET");
    queue.push({
      handle: crypto.randomUUID(),
      dataHandle: crypto.randomUUID(),
      ordinal: ++ordinal,
      lines,
      event,
      id,
      data: body,
      wire,
      rawBytes: used,
      charge: reserved,
    });
    queuedBytes += used;
    reset();
  }
  function lineGrowth(n: number) {
    const omitted =
      firstLine && bom.length === 3 && bom[0] === 239 && bom[1] === 187 && bom[2] === 191 ? 3 : 0;
    if (eventBytes + lineBytes + n - omitted > maxEvent)
      throw new PluginHostError("SSE_EVENT_BUDGET");
    lineBytes += n;
  }
  return {
    feed(input: Uint8Array) {
      let offset = 0;
      while (offset < input.length && queue.length < maxFrames) {
        if (pendingCR) {
          if (input[offset] === 10) {
            raw(input.subarray(offset, offset + 1));
            offset++;
            finishLine(used - 2);
          } else finishLine(used - 1);
          pendingCR = false;
          if (queue.length >= maxFrames) break;
          if (offset >= input.length) break;
        }
        let end = offset;
        while (end < input.length && input[end] !== 10 && input[end] !== 13) end++;
        if (seen < 3) {
          const n = Math.min(end - offset, 3 - seen);
          for (let i = 0; i < n; i++) bom.push(input[offset + i]!);
          seen += n;
        }
        if (end > offset) {
          lineGrowth(end - offset);
          raw(input.subarray(offset, end));
          offset = end;
        }
        if (offset === input.length) break;
        const ch = input[offset]!;
        if (
          eventBytes + lineBytes + 1 >
          maxEvent + (firstLine && bom.join(",") === "239,187,191" ? 3 : 0)
        )
          throw new PluginHostError("SSE_EVENT_BUDGET");
        raw(input.subarray(offset, offset + 1));
        offset++;
        if (ch === 13) pendingCR = true;
        else finishLine(used - 1);
      }
      decoder.decode(input.subarray(0, offset), { stream: true });
      return offset;
    },
    finish() {
      decoder.decode();
      if (pendingCR) {
        finishLine(used - 1);
        pendingCR = false;
      }
      if (used || lines.length) throw new PluginHostError("SSE_TRUNCATED");
    },
    peek: () => queue[0],
    consume(frame: SSEFrame) {
      if (queue[0] !== frame) throw new PluginHostError("SSE_FRAME");
      queue.shift();
      queuedBytes -= frame.rawBytes;
      charge(-frame.charge);
    },
    snapshot: () => ({
      ready: queue.length,
      readyBytes: queuedBytes,
      partialBytes: used,
      incomplete: used > 0 || lines.length > 0,
    }),
    dispose() {
      charge(-reserved);
      for (const frame of queue) charge(-frame.charge);
      queue.length = 0;
      queuedBytes = 0;
      reset();
    },
  };
}
