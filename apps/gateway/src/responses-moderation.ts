import { GatewayError } from "@gateway/contracts";
import type { PluginPipelineEngine } from "@gateway/plugin-engine";
import {
  copyExact,
  encodeSSE,
  parseProviderJson,
  SSEParser,
  type SSEEvent,
} from "@gateway/protocol";

type ObjectValue = Record<string, unknown>;
type Session = ReturnType<PluginPipelineEngine["createStreamSession"]>;
type FinalResult = Awaited<ReturnType<Session["finalize"]>>;

function object(value: unknown): ObjectValue | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as ObjectValue)
    : undefined;
}

function parseFrame(
  chunk: Uint8Array,
): { frame: SSEEvent; body: ObjectValue; kind: string } | undefined {
  const frame = new SSEParser().feed(chunk, true)[0];
  if (!frame?.data || frame.data === "[DONE]") return undefined;
  const body = object(parseProviderJson(frame.data));
  return body ? { frame, body, kind: String(body.type ?? frame.event) } : undefined;
}

function address(output: number, content: number, kind: string) {
  return `${output}/${content}/${kind}`;
}

export function createResponsesModeration(openSession: () => Session) {
  const lanes = new Map<
    string,
    { session: Session; item: NonNullable<ReturnType<typeof parseFrame>>; text: string }
  >();
  let approvedChars = 0;
  const approved = (output: number, content: number, kind: string, complete: boolean) =>
    complete ? (lanes.get(address(output, content, kind))?.text ?? "") : "";

  function emit(lane: NonNullable<ReturnType<typeof lanes.get>>, text: string): Uint8Array {
    approvedChars += text.length;
    if (approvedChars > 1024 * 1024) {
      throw new GatewayError(502, "stream_interrupted", "Moderated response exceeds text limit");
    }
    lane.text += text;
    const { frame, body, kind } = lane.item;
    return encodeSSE(
      copyExact(body, { ...body, type: body.type ?? kind, delta: text }),
      frame.event === "message" ? undefined : frame.event,
    );
  }

  function replacePart(part: unknown, output: number, content: number, complete: boolean): unknown {
    const value = object(part);
    if (!value) return part;
    if (typeof value.text === "string") {
      const text = approved(output, content, "response.output_text.delta", complete);
      return copyExact(value, { ...value, text });
    }
    if (typeof value.refusal === "string") {
      const refusal = approved(output, content, "response.refusal.delta", complete);
      return copyExact(value, { ...value, refusal });
    }
    return value;
  }

  function replaceItem(item: unknown, output: number, complete: boolean): unknown {
    const value = object(item);
    if (!value) return item;
    const result = copyExact(value, { ...value });
    if (Array.isArray(value.content)) {
      result.content = value.content.map((part, i) => replacePart(part, output, i, complete));
    }
    if (Array.isArray(value.summary)) {
      result.summary = value.summary.map((part) => replacePart(part, output, 0, false));
    }
    return result;
  }

  return {
    async push(
      chunk: Uint8Array,
    ): Promise<{ chars: number; blocked: boolean; rendered?: Uint8Array } | null> {
      const parsed = parseFrame(chunk);
      if (
        !parsed ||
        !["response.output_text.delta", "response.refusal.delta"].includes(parsed.kind) ||
        typeof parsed.body.delta !== "string"
      ) {
        return null;
      }
      const { body, kind } = parsed;
      const delta = parsed.body.delta as string;
      const key = address(Number(body.output_index ?? 0), Number(body.content_index ?? 0), kind);
      let lane = lanes.get(key);
      if (!lane) {
        if (lanes.size >= 64) {
          throw new GatewayError(
            502,
            "stream_interrupted",
            "Moderated response exceeds part limit",
          );
        }
        lane = { session: openSession(), item: parsed, text: "" };
        lanes.set(key, lane);
      }
      lane.item = parsed;
      const decision = await lane.session.push(delta);
      return {
        chars: delta.length,
        blocked: decision.blocked,
        ...(!decision.blocked && decision.emit ? { rendered: emit(lane, decision.emit) } : {}),
      };
    },
    isBlocked(): boolean {
      return [...lanes.values()].some((lane) => lane.session.isBlocked());
    },
    async finalize(): Promise<FinalResult & { frames: Uint8Array[] }> {
      if (!lanes.size) return { ...(await openSession().finalize()), frames: [] };
      const frames: Uint8Array[] = [];
      const audit: FinalResult["audit"] = [];
      let decision: FinalResult | undefined;
      for (const lane of lanes.values()) {
        const result = await lane.session.finalize();
        audit.push(...result.audit);
        if (!decision || (!decision.blocked && result.blocked)) decision = result;
        if (!result.blocked && result.emit) frames.push(emit(lane, result.emit));
      }
      return { ...decision!, audit, emit: "", frames };
    },
    defer(chunk: Uint8Array): boolean {
      const parsed = parseFrame(chunk);
      if (!parsed) return false;
      return [
        "response.output_text.done",
        "response.refusal.done",
        "response.content_part.done",
        "response.output_item.done",
        "response.completed",
        "response.incomplete",
      ].includes(parsed.kind);
    },
    rewrite(chunk: Uint8Array, complete = false): Uint8Array {
      const parsed = parseFrame(chunk);
      if (!parsed) return chunk;
      const { frame, body, kind } = parsed;
      const safe = copyExact(body, { ...body });
      const output = Number(body.output_index ?? 0);
      const content = Number(body.content_index ?? 0);
      if (typeof body.text === "string") {
        safe.text =
          kind === "response.output_text.done"
            ? approved(output, content, "response.output_text.delta", complete)
            : "";
      }
      if (typeof body.delta === "string" && kind !== "response.function_call_arguments.delta")
        safe.delta = "";
      if (typeof body.refusal === "string") {
        safe.refusal = approved(output, content, "response.refusal.delta", complete);
      }
      if (body.part) safe.part = replacePart(body.part, output, content, complete);
      if (body.item) safe.item = replaceItem(body.item, output, complete);
      const response = object(body.response);
      if (response) {
        const safeResponse = copyExact(response, { ...response });
        if (typeof response.output_text === "string") {
          safeResponse.output_text = complete
            ? [...lanes]
                .filter(([key]) => key.endsWith("/response.output_text.delta"))
                .map(([, lane]) => lane.text)
                .join("")
            : "";
        }
        if (Array.isArray(response.output)) {
          safeResponse.output = response.output.map((item, i) => replaceItem(item, i, complete));
        }
        safe.response = safeResponse;
      }
      return encodeSSE(safe, frame.event === "message" ? undefined : frame.event);
    },
  };
}
