import { GatewayError, optionalObject } from "@gateway/contracts";
import type { Protocol } from "@gateway/kernel";
import { encodeSSE, type SSEEvent } from "@gateway/protocol";
const decoder = new TextDecoder();

export class BedrockEventStreamDecoder {
  private buffer = new Uint8Array(0);

  get incomplete(): boolean {
    return this.buffer.length > 0;
  }

  feed(chunk: Uint8Array): SSEEvent[] {
    const merged = new Uint8Array(this.buffer.length + chunk.length);
    merged.set(this.buffer, 0);
    merged.set(chunk, this.buffer.length);
    let offset = 0;
    const events: SSEEvent[] = [];

    while (merged.length - offset >= 16) {
      const view = new DataView(merged.buffer, merged.byteOffset + offset, merged.length - offset);
      const totalLen = view.getUint32(0, false);
      const headersLen = view.getUint32(4, false);
      if (totalLen < 16 || totalLen > 4 * 1024 * 1024) {
        throw new GatewayError(502, "invalid_upstream", "Invalid Bedrock event stream frame");
      }
      if (merged.length - offset < totalLen) break;
      const payloadStart = offset + 12 + headersLen;
      const payloadEnd = offset + totalLen - 4;
      const payloadBytes = merged.subarray(payloadStart, payloadEnd);
      offset += totalLen;
      try {
        const wrapper = JSON.parse(decoder.decode(payloadBytes)) as { bytes?: string };
        if (typeof wrapper.bytes === "string") {
          const decoded = Buffer.from(wrapper.bytes, "base64").toString("utf8");
          const parsed = JSON.parse(decoded) as { type?: string };
          const eventType = typeof parsed.type === "string" ? parsed.type : "message";
          events.push({
            event: eventType,
            data: decoded,
            id: "",
            wire: `event: ${eventType}\ndata: ${decoded}\n\n`,
          });
        }
      } catch {
        throw new GatewayError(502, "invalid_upstream", "Malformed Bedrock event payload");
      }
    }

    this.buffer = merged.slice(offset);
    return events;
  }
}

type ParsedSseChunk =
  | { kind: "ok"; obj: Record<string, unknown>; event: string }
  | { kind: "empty_or_invalid" }
  | { kind: "multi_event" };

function parseSseDataObject(chunkBytes: Uint8Array): ParsedSseChunk {
  const raw = decoder.decode(chunkBytes);
  const len = raw.length;
  const hasCr = raw.indexOf("\r") !== -1;
  let payload: string | undefined;
  let parts: string[] | undefined;
  let event = "message";
  let inEvent = false;
  let eventEnded = false;
  let start = 0;
  while (start < len) {
    let end: number;
    let nextStart: number;
    if (!hasCr) {
      const nl = raw.indexOf("\n", start);
      if (nl === -1) {
        end = len;
        nextStart = len;
      } else {
        end = nl;
        nextStart = nl + 1;
      }
    } else {
      const nl = raw.indexOf("\n", start);
      const cr = raw.indexOf("\r", start);
      if (nl === -1 && cr === -1) {
        end = len;
        nextStart = len;
      } else {
        end = nl === -1 ? cr : cr === -1 ? nl : nl < cr ? nl : cr;
        nextStart =
          raw.charCodeAt(end) === 13 && raw.charCodeAt(end + 1) === 10 ? end + 2 : end + 1;
      }
    }
    const lineLen = end - start;
    if (lineLen === 0) {
      if (inEvent) {
        eventEnded = true;
        inEvent = false;
      }
      start = nextStart;
      continue;
    }
    if (eventEnded) {
      if (raw.slice(start, end).trim().length > 0) return { kind: "multi_event" };
      start = nextStart;
      continue;
    }
    inEvent = true;
    if (raw.startsWith("data:", start)) {
      const valStart = start + 5 < end && raw.charCodeAt(start + 5) === 32 ? start + 6 : start + 5;
      const line = raw.slice(valStart, end);
      if (payload === undefined) {
        payload = line;
      } else {
        if (!parts) parts = [payload];
        parts.push(line);
      }
    } else if (lineLen === 4 && raw.startsWith("data", start)) {
      if (payload === undefined) {
        payload = "";
      } else {
        if (!parts) parts = [payload];
        parts.push("");
      }
    } else if (raw.startsWith("event:", start)) {
      const valStart = start + 6 < end && raw.charCodeAt(start + 6) === 32 ? start + 7 : start + 6;
      event = raw.slice(valStart, end);
    } else if (lineLen === 5 && raw.startsWith("event", start)) {
      event = "";
    }
    start = nextStart;
  }
  if (payload === undefined) return { kind: "empty_or_invalid" };
  const text = (parts ? parts.join("\n") : payload).trim();
  if (!text || text === "[DONE]") return { kind: "empty_or_invalid" };
  try {
    const parsed: unknown = JSON.parse(text);
    return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
      ? { kind: "ok", obj: parsed as Record<string, unknown>, event }
      : { kind: "empty_or_invalid" };
  } catch {
    return { kind: "empty_or_invalid" };
  }
}

function extractTextFromObject(obj: Record<string, unknown>, protocol: Protocol): string | null {
  if (protocol === "chat") {
    const choices = Array.isArray(obj.choices) ? obj.choices : [];
    const delta = optionalObject(optionalObject(choices[0]).delta);
    return typeof delta.content === "string" ? delta.content : null;
  }
  if (protocol === "messages") {
    const delta = optionalObject(obj.delta);
    return delta.type === "text_delta" && typeof delta.text === "string" ? delta.text : null;
  }
  if (protocol === "responses") {
    return obj.type === "response.output_text.delta" && typeof obj.delta === "string"
      ? obj.delta
      : null;
  }
  const cands = Array.isArray(obj.candidates) ? obj.candidates : [];
  const parts = Array.isArray(optionalObject(optionalObject(cands[0]).content).parts)
    ? (optionalObject(optionalObject(cands[0]).content).parts as unknown[])
    : [];
  let texts = "";
  for (let i = 0; i < parts.length; i++) {
    const part = optionalObject(parts[i]);
    if (typeof part.text === "string" && part.thought !== true) texts += part.text;
  }
  return texts.length > 0 ? texts : null;
}

function inspectAnthropicBlockFromObject(
  obj: Record<string, unknown>,
): { kind: "text_start" | "block_stop"; index: number } | null {
  const index = typeof obj.index === "number" && Number.isInteger(obj.index) ? obj.index : 0;
  if (obj.type === "content_block_start" && optionalObject(obj.content_block).type === "text") {
    return { kind: "text_start", index };
  }
  if (obj.type === "content_block_stop") {
    return { kind: "block_stop", index };
  }
  return null;
}

function stripTextFromObject(obj: Record<string, unknown>, protocol: Protocol): Uint8Array | null {
  if (protocol === "messages" || protocol === "responses") return null;
  if (protocol === "chat") {
    const choices = Array.isArray(obj.choices) ? obj.choices : [];
    if (choices.length === 0) return null;
    const firstChoice = optionalObject(choices[0]);
    const rawDelta = optionalObject(firstChoice.delta);
    let hasOtherDeltaKey = false;
    for (const k in rawDelta) {
      if (k !== "content") {
        hasOtherDeltaKey = true;
        break;
      }
    }
    const hasMeta =
      hasOtherDeltaKey ||
      choices.length > 1 ||
      (firstChoice.finish_reason !== null && firstChoice.finish_reason !== undefined) ||
      obj.usage !== undefined;
    if (!hasMeta) return null;
    const first = { ...firstChoice };
    const delta = { ...rawDelta };
    delete delta.content;
    first.delta = delta;
    return encodeSSE({ ...obj, choices: [first, ...choices.slice(1)] });
  }
  const cands = Array.isArray(obj.candidates) ? obj.candidates : [];
  if (cands.length === 0) return null;
  const firstCand = optionalObject(cands[0]);
  const content = optionalObject(firstCand.content);
  const parts = Array.isArray(content.parts) ? (content.parts as unknown[]) : [];
  const remainingParts = parts.filter(
    (p) => typeof optionalObject(p).text !== "string" || optionalObject(p).thought === true,
  );
  const hasMeta =
    remainingParts.length > 0 ||
    cands.length > 1 ||
    (firstCand.finishReason !== null && firstCand.finishReason !== undefined) ||
    obj.usageMetadata !== undefined;
  if (!hasMeta) return null;
  const first = { ...firstCand };
  if (remainingParts.length > 0) {
    first.content = { ...content, parts: remainingParts };
  } else {
    delete first.content;
  }
  return encodeSSE({ ...obj, candidates: [first, ...cands.slice(1)] });
}

function hasOtherTextInObject(
  obj: Record<string, unknown>,
  protocol: Protocol,
  eventName: string,
): boolean {
  if (
    eventName === "error" ||
    obj.error ||
    obj.type === "error" ||
    obj.type === "response.failed"
  ) {
    return false;
  }
  if (protocol === "chat") {
    const choices = Array.isArray(obj.choices) ? obj.choices : [];
    for (let i = 0; i < choices.length; i++) {
      const delta = optionalObject(optionalObject(choices[i]).delta);
      if (typeof delta.refusal === "string" && delta.refusal.length > 0) return true;
      if (i >= 1 && typeof delta.content === "string" && delta.content.length > 0) return true;
    }
    return false;
  }
  if (protocol === "messages") {
    const type = obj.type ?? eventName;
    if (type !== "content_block_start") return false;
    const block = optionalObject(obj.content_block);
    return block.type === "text" && typeof block.text === "string" && block.text.length > 0;
  }
  if (protocol === "responses") {
    const type = obj.type ?? eventName;
    return (
      (type === "response.refusal.delta" ||
        (obj.type === undefined && type === "response.output_text.delta")) &&
      typeof obj.delta === "string" &&
      obj.delta.length > 0
    );
  }
  const cands = Array.isArray(obj.candidates) ? obj.candidates : [];
  for (let i = 1; i < cands.length; i++) {
    const parts = optionalObject(optionalObject(cands[i]).content).parts;
    if (!Array.isArray(parts)) continue;
    for (let j = 0; j < parts.length; j++) {
      const part = optionalObject(parts[j]);
      if (part.thought !== true && typeof part.text === "string" && part.text.length > 0) {
        return true;
      }
    }
  }
  return false;
}

export type SplitSseChunkResult = {
  text: string | null;
  remainder: Uint8Array | null;
  hasOtherText: boolean | undefined;
  remainderHasText?: boolean;
  anthropicBlock?: { kind: "text_start" | "block_stop"; index: number };
};

export function splitSseChunkText(
  chunkBytes: Uint8Array,
  protocol: Protocol,
  includeRemainder = true,
): SplitSseChunkResult {
  const parsed = parseSseDataObject(chunkBytes);
  if (parsed.kind === "multi_event") {
    return { text: null, remainder: null, hasOtherText: undefined };
  }
  if (parsed.kind === "empty_or_invalid") {
    return { text: null, remainder: null, hasOtherText: false };
  }
  const { obj, event } = parsed;
  const anthropicBlock =
    protocol === "messages" ? (inspectAnthropicBlockFromObject(obj) ?? undefined) : undefined;
  const text = extractTextFromObject(obj, protocol);
  if (text === null) {
    const hasOtherText = hasOtherTextInObject(obj, protocol, event);
    return anthropicBlock
      ? { text: null, remainder: null, hasOtherText, anthropicBlock }
      : { text: null, remainder: null, hasOtherText };
  }
  const remainder = includeRemainder ? stripTextFromObject(obj, protocol) : null;
  const result: SplitSseChunkResult = {
    text,
    remainder,
    hasOtherText: false,
  };
  if (remainder !== null) {
    result.remainderHasText = hasOtherTextInObject(obj, protocol, event);
  }
  if (anthropicBlock) {
    result.anthropicBlock = anthropicBlock;
  }
  return result;
}

export function buildTextDeltaSse(
  protocol: Protocol,
  model: string,
  text: string,
  streamId: string,
  blockIndex = 0,
): Uint8Array {
  if (protocol === "chat") {
    return encodeSSE({
      id: streamId,
      object: "chat.completion.chunk",
      model,
      choices: [{ index: 0, delta: { content: text }, finish_reason: null }],
    });
  }
  if (protocol === "messages") {
    return encodeSSE(
      {
        type: "content_block_delta",
        index: blockIndex,
        delta: { type: "text_delta", text },
      },
      "content_block_delta",
    );
  }
  if (protocol === "responses") {
    return encodeSSE(
      {
        type: "response.output_text.delta",
        output_index: 0,
        item_id: "msg_0",
        content_index: 0,
        delta: text,
      },
      "response.output_text.delta",
    );
  }
  return encodeSSE({
    candidates: [{ index: 0, content: { role: "model", parts: [{ text }] } }],
  });
}
