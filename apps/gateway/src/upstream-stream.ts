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

export function extractChunkTextFromEncodedSse(
  chunkBytes: Uint8Array,
  protocol: Protocol,
): string | null {
  const raw = decoder.decode(chunkBytes);
  const dataLines = raw
    .split("\n")
    .filter((l) => l.startsWith("data: "))
    .map((l) => l.slice(6));
  if (dataLines.length === 0) return null;
  const payload = dataLines.join("\n").trim();
  if (!payload || payload === "[DONE]") return null;
  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(payload) as Record<string, unknown>;
  } catch {
    return null;
  }
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
  const texts = parts
    .map((p) => {
      const part = optionalObject(p);
      return typeof part.text === "string" && part.thought !== true ? part.text : "";
    })
    .join("");
  return texts.length > 0 ? texts : null;
}

export function inspectAnthropicBlockEvent(
  chunkBytes: Uint8Array,
): { kind: "text_start" | "block_stop"; index: number } | null {
  const raw = decoder.decode(chunkBytes);
  const dataLines = raw
    .split("\n")
    .filter((l) => l.startsWith("data: "))
    .map((l) => l.slice(6));
  if (dataLines.length === 0) return null;
  const payload = dataLines.join("\n").trim();
  if (!payload || payload === "[DONE]") return null;
  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(payload) as Record<string, unknown>;
  } catch {
    return null;
  }
  const index = typeof obj.index === "number" && Number.isInteger(obj.index) ? obj.index : 0;
  if (obj.type === "content_block_start" && optionalObject(obj.content_block).type === "text") {
    return { kind: "text_start", index };
  }
  if (obj.type === "content_block_stop") {
    return { kind: "block_stop", index };
  }
  return null;
}

export function stripTextFromSseChunk(
  chunkBytes: Uint8Array,
  protocol: Protocol,
): Uint8Array | null {
  if (protocol === "messages" || protocol === "responses") return null;
  const raw = decoder.decode(chunkBytes);
  const dataLines = raw
    .split("\n")
    .filter((l) => l.startsWith("data: "))
    .map((l) => l.slice(6));
  if (dataLines.length === 0) return null;
  const payload = dataLines.join("\n").trim();
  if (!payload || payload === "[DONE]") return null;
  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(payload) as Record<string, unknown>;
  } catch {
    return null;
  }
  if (protocol === "chat") {
    const choices = Array.isArray(obj.choices) ? obj.choices : [];
    if (choices.length === 0) return null;
    const first = { ...optionalObject(choices[0]) };
    const delta = { ...optionalObject(first.delta) };
    delete delta.content;
    first.delta = delta;
    const hasMeta =
      Object.keys(delta).length > 0 ||
      (first.finish_reason !== null && first.finish_reason !== undefined) ||
      obj.usage !== undefined;
    if (!hasMeta) return null;
    return encodeSSE({ ...obj, choices: [first, ...choices.slice(1)] });
  }
  const cands = Array.isArray(obj.candidates) ? obj.candidates : [];
  if (cands.length === 0) return null;
  const first = { ...optionalObject(cands[0]) };
  const content = optionalObject(first.content);
  const parts = Array.isArray(content.parts) ? (content.parts as unknown[]) : [];
  const remainingParts = parts.filter(
    (p) => typeof optionalObject(p).text !== "string" || optionalObject(p).thought === true,
  );
  const hasMeta =
    remainingParts.length > 0 ||
    (first.finishReason !== null && first.finishReason !== undefined) ||
    obj.usageMetadata !== undefined;
  if (!hasMeta) return null;
  if (remainingParts.length > 0) {
    first.content = { ...content, parts: remainingParts };
  } else {
    delete first.content;
  }
  return encodeSSE({ ...obj, candidates: [first, ...cands.slice(1)] });
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
