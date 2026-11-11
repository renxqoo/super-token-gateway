import {
  GatewayError,
  encodeSSE,
  extractUsage,
  providerObject as object,
  parseProviderJson,
  text,
} from "@gateway/kernel";
import type { Operation, SSEEvent, Usage } from "@gateway/kernel";
import type { StreamMeter } from "./stream.ts";

export class MediaStreamMeter implements StreamMeter {
  usage: Usage | undefined;
  terminal = false;
  failed = false;
  holdTerminal = false;
  remoteId: string | undefined;
  publicId: string | undefined;
  readonly source: string;
  readonly target: string;
  constructor(readonly operation: Operation) {
    this.source = operation;
    this.target = operation;
  }
  consume(event: SSEEvent): { chunks: Uint8Array[]; terminal: boolean } {
    if (!event.data)
      return { chunks: [new TextEncoder().encode(event.wire)], terminal: this.terminal };
    if (this.terminal)
      throw new GatewayError(
        502,
        "unexpected_trailing_event",
        "Media stream continued after completion",
      );
    const body = object(parseProviderJson(event.data));
    this.usage = extractUsage(body, "openai", this.usage) ?? this.usage;
    if (body.error || body.type === "error") {
      this.failed = true;
      throw new GatewayError(502, "provider_stream_error", "Provider media stream failed");
    }
    const type = text(body.type, event.event);
    const completed =
      this.operation === "images"
        ? type === "image_generation.completed"
        : this.operation === "image_edits"
          ? type === "image_edit.completed"
          : this.operation === "transcription"
            ? type === "transcript.text.done"
            : ["speech.audio.done", "audio.done"].includes(type);
    if (completed) {
      this.terminal = true;
      this.holdTerminal = true;
      if (
        (this.operation === "images" || this.operation === "image_edits") &&
        typeof body.b64_json === "string" &&
        body.b64_json.length
      )
        this.usage = { ...this.usage, images: "1" };
    }
    return {
      chunks: [encodeSSE(body, event.event === "message" ? undefined : event.event)],
      terminal: this.terminal,
    };
  }
  finalize(): Uint8Array[] {
    if (!this.terminal || this.failed)
      throw new GatewayError(502, "stream_truncated", "Media stream is incomplete");
    return [];
  }
}
