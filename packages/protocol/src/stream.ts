import { GatewayError, providerObject as object, optionalObject, text } from "@gateway/contracts";
import { encodeSSE, hasChatToolDelta } from "./sse.ts";
import { extractUsage } from "./usage.ts";
import { parseProviderJson, copyExact, stringifyExact } from "./json.ts";
import type { Protocol, ProviderKind, Usage } from "@gateway/kernel";
import type { SSEEvent } from "./sse.ts";
import { finishReason, blockedPrompt } from "./finish.ts";
import type { Completion } from "./response.ts";
import { encodeStreamComplete, encodeStreamStart } from "./stream-encode.ts";

const streamEncoder = new TextEncoder();

export interface StreamDelta {
  type: "text" | "reasoning" | "tool";
  index: number;
  text: string;
  id?: string;
  name?: string;
}

export class StreamTranslator {
  usage: Usage | undefined;
  terminal = false;
  failed = false;
  private finish: Completion["finish"] = "stop";
  private started = false;
  private sequence = 0;
  private tools: Map<number, { id: string; name: string; args: string }> | undefined;
  private textBlocks: Map<number, string> | undefined;
  private reasoningBlocks: Map<number, string> | undefined;
  private blockTypes: Map<number, string> | undefined;
  private openBlocks: Set<number> | undefined;
  private accumulatedBytes = 0;
  holdTerminal = false;
  remoteId: string | undefined;
  publicId: string | undefined;
  private targetIndices: Map<string, number> | undefined;
  private nextTargetIndex = 0;
  private messageOpen: number | undefined;
  private messageClosed: Set<number> | undefined;
  private geminiCompleted: Set<number> | undefined;
  private chatCompletedMask = 0;
  private chatCompletedCount = 0;
  private chatCompleted: Set<number> | undefined;
  private geminiCalls: Map<string, { index: number; args: string; name: string }> | undefined;
  private cachedId: string | undefined;
  private get id(): string {
    return (this.cachedId ??= `gen_${crypto.randomUUID()}`);
  }
  constructor(
    readonly source: Protocol,
    readonly target: Protocol,
    readonly kind: ProviderKind | "custom",
    readonly model: string,
    private readonly maxAggregate = 1_048_576,
    private readonly expectedCandidates = 1,
  ) {}

  private markChatCompleted(index: number): void {
    if (index < 30) {
      const bit = 1 << index;
      if ((this.chatCompletedMask & bit) === 0) {
        this.chatCompletedMask |= bit;
        this.chatCompletedCount++;
      }
      return;
    }
    const set = (this.chatCompleted ??= new Set());
    if (!set.has(index)) {
      set.add(index);
      this.chatCompletedCount++;
    }
  }

  consume(
    event: SSEEvent,
    trackSourceContent = false,
  ): {
    chunks: Uint8Array[];
    terminal: boolean;
    hasSourceText?: boolean;
    hasSourceContent?: boolean;
    hasTargetText?: boolean;
    outputUsageBoundary?: "replace" | "ignore";
  } {
    if (this.terminal) {
      if (!event.data) return { chunks: [], terminal: true };
      const late = object(parseProviderJson(event.data));
      if (
        late.error ||
        this.source !== "gemini" ||
        !late.usageMetadata ||
        (Array.isArray(late.candidates) &&
          late.candidates.some((entry) => optionalObject(entry).content))
      ) {
        this.failed = true;
        throw new GatewayError(
          502,
          "unexpected_trailing_event",
          "Provider sent data after terminal completion",
        );
      }
      this.usage = extractUsage(late, this.kind, this.usage) ?? this.usage;
      return { chunks: this.source === this.target ? [encodeSSE(late)] : [], terminal: true };
    }
    const chunks: Uint8Array[] = [];
    if (!event.data)
      return {
        chunks: this.source === this.target ? [streamEncoder.encode(event.wire)] : [],
        terminal: false,
      };
    if (event.data === "[DONE]") {
      if (this.source !== "chat")
        throw new GatewayError(502, "invalid_upstream", "Unexpected terminal sentinel");
      if (this.chatCompletedCount !== this.expectedCandidates) {
        this.failed = true;
        throw new GatewayError(
          502,
          "stream_truncated",
          "Provider did not finish every requested choice",
        );
      }
      this.terminal = true;
      return {
        chunks: this.source === this.target ? [encodeSSE("[DONE]")] : [],
        terminal: true,
      };
    }
    const body = object(parseProviderJson(event.data));
    if (this.source === "responses") {
      const response = optionalObject(body.response);
      if (typeof response.id === "string") this.remoteId = response.id;
      if (this.publicId && typeof response.id === "string")
        body.response = copyExact(response, { ...response, id: this.publicId });
      if (this.publicId && typeof body.response_id === "string") body.response_id = this.publicId;
    }
    if (
      this.source === "chat" &&
      Array.isArray(body.choices) &&
      body.choices.some((raw) => {
        const reason = optionalObject(raw).finish_reason;
        return reason !== null && reason !== undefined;
      })
    )
      this.holdTerminal = true;
    if (
      this.source === "messages" &&
      optionalObject(body.delta).stop_reason !== null &&
      optionalObject(body.delta).stop_reason !== undefined
    )
      this.holdTerminal = true;
    const rawType = text(body.type, event.event);
    const responsesTerminal =
      this.source === "responses" &&
      (rawType === "response.completed" || rawType === "response.incomplete");
    const hasNestedMessage =
      body.message !== null && typeof body.message === "object" && !Array.isArray(body.message);
    const nested = hasNestedMessage ? (body.message as Record<string, unknown>) : undefined;
    if (
      body.error ||
      event.event === "error" ||
      body.type === "error" ||
      body.type === "response.failed"
    ) {
      this.failed = true;
      throw new GatewayError(502, "provider_stream_error", "Provider reported a stream error");
    }
    if (this.source === "gemini" && Array.isArray(body.candidates))
      for (const raw of body.candidates) {
        const candidate = optionalObject(raw);
        const index = Number(candidate.index ?? 0);
        if (!Number.isSafeInteger(index) || index < 0 || index >= this.expectedCandidates)
          throw new GatewayError(
            502,
            "invalid_upstream",
            "Candidate index exceeds requested count",
          );
        if (candidate.finishReason) (this.geminiCompleted ??= new Set()).add(index);
      }
    let terminalNow =
      this.source === "responses"
        ? responsesTerminal
        : this.source === "messages"
          ? rawType === "message_stop"
          : this.source === "gemini" &&
            (this.geminiCompleted?.size ?? 0) === this.expectedCandidates;
    if (this.source === "gemini" && blockedPrompt(body)) {
      terminalNow = true;
      this.finish = "content_filter";
    }
    this.terminal = terminalNow;
    const sameProtocol = this.source === this.target;
    let hasSourceText = false;
    let hasChatTool = false;
    let addsOutput = false;
    let addsGeminiThought = false;
    let deltas: StreamDelta[] | undefined;
    const pushDelta = (d: StreamDelta) => {
      (deltas ??= []).push(d);
    };
    if (this.source === "chat") {
      const choices = Array.isArray(body.choices) ? body.choices : [];
      if (!sameProtocol && choices.length > 1)
        throw new GatewayError(
          502,
          "conversion_loss",
          "Multiple stream choices cannot be converted",
        );
      for (const raw of choices) {
        const choice = object(raw);
        const index = Number(choice.index ?? 0);
        if (!Number.isSafeInteger(index) || index < 0 || index >= this.expectedCandidates)
          throw new GatewayError(502, "invalid_upstream", "Choice index exceeds requested count");
        if (choice.finish_reason !== undefined && choice.finish_reason !== null) {
          if (typeof choice.finish_reason !== "string" || !choice.finish_reason)
            throw new GatewayError(502, "invalid_upstream", "Invalid choice completion reason");
          this.markChatCompleted(index);
        }
        const rawDelta = choice.delta;
        if (rawDelta !== null && typeof rawDelta === "object" && !Array.isArray(rawDelta)) {
          const d = rawDelta as Record<string, unknown>;
          if (trackSourceContent && !hasChatTool) hasChatTool = hasChatToolDelta(d);
          if (
            (typeof d.content === "string" && d.content.length > 0) ||
            (typeof d.refusal === "string" && d.refusal.length > 0)
          ) {
            hasSourceText = true;
          }
          if (
            hasSourceText ||
            d.reasoning_content ||
            d.reasoning ||
            d.audio ||
            d.function_call ||
            (Array.isArray(d.tool_calls) && d.tool_calls.length > 0)
          ) {
            addsOutput = true;
          }
        }
        if (!sameProtocol) {
          const delta = optionalObject(choice.delta);
          if (delta.refusal || delta.audio)
            throw new GatewayError(502, "conversion_loss", "Unsupported stream delta");
          const reasoningDelta = delta.reasoning_content ?? delta.reasoning;
          if (typeof reasoningDelta === "string" && reasoningDelta.length > 0)
            pushDelta({ type: "reasoning", index: 0, text: reasoningDelta });
          if (typeof delta.content === "string")
            pushDelta({ type: "text", index: 0, text: delta.content });
          if (Array.isArray(delta.tool_calls))
            for (const rawCall of delta.tool_calls) {
              const call = object(rawCall);
              const fn = optionalObject(call.function);
              pushDelta({
                type: "tool",
                index: Number(call.index ?? 0) + 1,
                text: text(fn.arguments),
                ...(call.id ? { id: text(call.id) } : {}),
                ...(fn.name ? { name: text(fn.name) } : {}),
              });
            }
          this.finish = finishReason("chat", choice.finish_reason) ?? this.finish;
        }
        if (choice.finish_reason === "length") this.finish = "length";
        if (choice.finish_reason === "tool_calls") this.finish = "tool_calls";
        if (choice.finish_reason === "content_filter") this.finish = "content_filter";
      }
    } else if (this.source === "responses") {
      const type = text(body.type, event.event);
      const index = Number(body.output_index ?? 0);
      if (type === "response.output_text.delta") {
        const deltaStr = text(body.delta);
        if (deltaStr.length > 0) hasSourceText = true;
        pushDelta({ type: "text", index, text: deltaStr });
      } else if (
        type === "response.refusal.delta" &&
        typeof body.delta === "string" &&
        body.delta.length > 0
      ) {
        hasSourceText = true;
      }
      if (
        type === "response.reasoning_summary_text.delta" ||
        type === "response.reasoning_text.delta"
      )
        pushDelta({ type: "reasoning", index, text: text(body.delta) });
      if (type === "response.output_item.added") {
        const item = optionalObject(body.item);
        if (item.type === "function_call")
          pushDelta({
            type: "tool",
            index,
            text: "",
            id: text(item.call_id),
            name: text(item.name),
          });
        else if (
          this.target !== this.source &&
          item.type !== "message" &&
          item.type !== "reasoning"
        )
          throw new GatewayError(502, "conversion_loss", "Unsupported response output item");
      }
      if (type === "response.function_call_arguments.delta")
        pushDelta({ type: "tool", index, text: text(body.delta) });
      if (type === "response.completed" || type === "response.incomplete") {
        terminalNow = true;
        if (type === "response.incomplete") this.finish = "length";
      }
    } else if (this.source === "messages") {
      const type = text(body.type, event.event);
      const index = Number(body.index ?? 0);
      if (type === "content_block_start") {
        const block = object(body.content_block);
        (this.blockTypes ??= new Map()).set(index, text(block.type));
        if (block.type === "tool_use")
          pushDelta({
            type: "tool",
            index,
            text: "",
            id: text(block.id),
            name: text(block.name),
          });
        else if (block.type === "text" && typeof block.text === "string" && block.text.length > 0) {
          hasSourceText = true;
          pushDelta({ type: "text", index, text: block.text });
        } else if (
          block.type === "thinking" &&
          typeof block.thinking === "string" &&
          block.thinking.length > 0
        )
          pushDelta({ type: "reasoning", index, text: block.thinking });
        else if (this.target !== this.source && block.type !== "text" && block.type !== "thinking")
          throw new GatewayError(502, "conversion_loss", "Unsupported Anthropic stream block");
      }
      if (type === "content_block_delta") {
        const delta = object(body.delta);
        if (delta.type === "text_delta") {
          const dt = text(delta.text);
          if (dt.length > 0) hasSourceText = true;
          pushDelta({ type: "text", index, text: dt });
        } else if (delta.type === "thinking_delta")
          pushDelta({ type: "reasoning", index, text: text(delta.thinking) });
        else if (delta.type === "signature_delta") {
          void 0;
        } else if (delta.type === "input_json_delta")
          pushDelta({ type: "tool", index, text: text(delta.partial_json) });
        else if (this.target !== this.source)
          throw new GatewayError(502, "conversion_loss", "Unsupported Anthropic delta");
      }
      if (type === "message_delta") {
        const reason = optionalObject(body.delta).stop_reason;
        if (this.target !== this.source)
          this.finish = finishReason("messages", reason) ?? this.finish;
        if (reason === "max_tokens") this.finish = "length";
        if (reason === "tool_use") this.finish = "tool_calls";
      }
      if (type === "message_stop") terminalNow = true;
    } else {
      const candidates = Array.isArray(body.candidates) ? body.candidates : [];
      if (this.target !== this.source && candidates.length > 1)
        throw new GatewayError(502, "conversion_loss", "Multiple candidates cannot be converted");
      for (const raw of candidates) {
        const candidate = object(raw);
        const content = optionalObject(candidate.content);
        if (Array.isArray(content.parts))
          for (let index = 0; index < content.parts.length; index++) {
            const part = object(content.parts[index]);
            if (typeof part.text !== "string" || part.text.length > 0) addsOutput = true;
            if (part.thought === true && typeof part.text === "string") {
              if (part.text.length > 0) addsGeminiThought = true;
              pushDelta({ type: "reasoning", index: 0, text: part.text });
              continue;
            }
            if (typeof part.text === "string") {
              if (part.text.length > 0) hasSourceText = true;
              pushDelta({ type: "text", index: 0, text: part.text });
            } else if (part.functionCall) {
              const call = object(part.functionCall);
              const args = stringifyExact(call.args ?? {});
              const calls = (this.geminiCalls ??= new Map());
              const id = text(call.id, `call_${calls.size}`);
              const identity = `${Number(candidate.index ?? 0)}:${id}`;
              const prior = calls.get(identity);
              if (prior) {
                if (prior.args !== args || prior.name !== text(call.name))
                  throw new GatewayError(
                    502,
                    "conversion_loss",
                    "Cannot preserve changing Gemini tool snapshots",
                  );
                continue;
              }
              const callIndex = calls.size + 1;
              calls.set(identity, { index: callIndex, args, name: text(call.name) });
              pushDelta({
                type: "tool",
                index: callIndex,
                text: args,
                id,
                name: text(call.name),
              });
            } else if (this.target !== this.source)
              throw new GatewayError(502, "conversion_loss", "Unsupported Gemini stream part");
          }
        if (candidate.finishReason) {
          if (this.target !== this.source)
            this.finish = finishReason("gemini", candidate.finishReason) ?? this.finish;
          if (candidate.finishReason === "MAX_TOKENS") this.finish = "length";
        }
      }
    }
    const ignoreOutputUsage = this.source === "messages" && rawType === "message_start";
    const replaceOutputUsage = addsOutput || responsesTerminal;
    if (replaceOutputUsage && this.usage) {
      delete this.usage.output_tokens;
      if (this.source !== "gemini" || addsGeminiThought) delete this.usage.reasoning_tokens;
    }
    this.usage =
      (nested ? extractUsage(nested, this.kind, this.usage) : undefined) ??
      extractUsage(body, this.kind, this.usage) ??
      this.usage;
    if (ignoreOutputUsage && this.usage) {
      delete this.usage.output_tokens;
      delete this.usage.reasoning_tokens;
    }
    if (sameProtocol) {
      let modified = Boolean(this.publicId && this.source === "responses");
      if (typeof body.model === "string" && body.model !== this.model) {
        body.model = this.model;
        modified = true;
      }
      if (nested && typeof nested.model === "string" && nested.model !== this.model) {
        nested.model = this.model;
        modified = true;
      }
      if (body.response && typeof optionalObject(body.response).model === "string") {
        const respObj = body.response as Record<string, unknown>;
        if (respObj.model !== this.model) {
          respObj.model = this.model;
          modified = true;
        }
      }
      chunks.push(
        !modified &&
          event.event === "message" &&
          event.wire.startsWith("data: ") &&
          event.wire.indexOf("\n") === event.wire.length - 2
          ? (event.wireBytes ?? streamEncoder.encode(event.wire))
          : encodeSSE(body, event.event === "message" ? undefined : event.event),
      );
    } else {
      if (!this.started) chunks.push(...this.start());
      if (deltas) for (const delta of deltas) chunks.push(...this.delta(delta));
    }
    this.terminal = terminalNow;
    const hasTargetText = sameProtocol
      ? hasSourceText
      : Boolean(deltas?.some((d) => d.type === "text" && d.text.length > 0));
    return {
      chunks,
      terminal: terminalNow,
      hasSourceText,
      hasTargetText,
      ...(trackSourceContent &&
      (hasSourceText ||
        hasChatTool ||
        deltas?.some(
          (delta) => delta.type === "tool" && Boolean(delta.text || delta.id || delta.name),
        ))
        ? { hasSourceContent: true }
        : {}),
      ...(replaceOutputUsage ? { outputUsageBoundary: "replace" } : {}),
      ...(ignoreOutputUsage ? { outputUsageBoundary: "ignore" } : {}),
    };
  }

  assertComplete(): void {
    if (!this.terminal || this.failed)
      throw new GatewayError(502, "stream_truncated", "Cannot finalize incomplete stream");
  }

  finalize(): Uint8Array[] {
    this.assertComplete();
    return this.source === this.target ? [] : this.complete();
  }

  private event(type: string, body: Record<string, unknown>): Uint8Array {
    return encodeSSE({ type, sequence_number: this.sequence++, ...body }, type);
  }

  private start(): Uint8Array[] {
    this.started = true;
    return encodeStreamStart({
      id: this.id,
      target: this.target,
      model: this.model,
      finish: this.finish,
      usage: this.usage,
      nextEvent: (type, body) => this.event(type, body),
    });
  }

  private delta(delta: StreamDelta): Uint8Array[] {
    const sourceIndex = `${delta.type}:${delta.index}`;
    const targetIndices = (this.targetIndices ??= new Map());
    let targetIndex = targetIndices.get(sourceIndex);
    if (
      targetIndex === undefined ||
      (this.target === "messages" && this.messageClosed?.has(targetIndex))
    ) {
      targetIndex = this.nextTargetIndex++;
      targetIndices.set(sourceIndex, targetIndex);
    }
    delta = { ...delta, index: targetIndex };
    const chunks: Uint8Array[] = [];
    if (!Number.isSafeInteger(delta.index) || delta.index < 0 || delta.index > 127)
      throw new GatewayError(502, "invalid_upstream", "Stream block index is out of range");
    const openBlocks = (this.openBlocks ??= new Set());
    const first = !openBlocks.has(delta.index);
    if ((delta.type === "text" || delta.type === "reasoning") && this.tools?.has(delta.index))
      throw new GatewayError(502, "invalid_upstream", "Stream block type changed");
    if (delta.type === "tool") {
      const tools = (this.tools ??= new Map());
      const previous = tools.get(delta.index);
      const tool = {
        id: delta.id ?? previous?.id ?? `call_${delta.index}`,
        name: delta.name ?? previous?.name ?? "",
        args: (previous?.args ?? "") + delta.text,
      };
      tools.set(delta.index, tool);
      this.finish = "tool_calls";
    }
    if (this.target === "responses" || delta.type === "tool") {
      this.accumulatedBytes += streamEncoder.encode(delta.text).byteLength;
      if (this.accumulatedBytes > this.maxAggregate)
        throw new GatewayError(
          502,
          "conversion_limit",
          "Converted response aggregate exceeds limit",
        );
      if (delta.type === "text") {
        const textBlocks = (this.textBlocks ??= new Map());
        textBlocks.set(delta.index, (textBlocks.get(delta.index) ?? "") + delta.text);
      }
      if (delta.type === "reasoning") {
        const reasoningBlocks = (this.reasoningBlocks ??= new Map());
        reasoningBlocks.set(delta.index, (reasoningBlocks.get(delta.index) ?? "") + delta.text);
      }
    }
    if (this.target === "messages" && delta.type === "tool") return [];
    if (
      this.target === "messages" &&
      (delta.type === "text" || delta.type === "reasoning") &&
      this.tools?.size
    )
      throw new GatewayError(
        502,
        "conversion_loss",
        "Text after a streamed tool block cannot be preserved",
      );
    if (this.target === "chat") {
      const change =
        delta.type === "text"
          ? { content: delta.text }
          : delta.type === "reasoning"
            ? { reasoning_content: delta.text }
            : {
                tool_calls: [
                  {
                    index: delta.index,
                    ...(first ? { id: this.tools?.get(delta.index)?.id, type: "function" } : {}),
                    function: {
                      ...(delta.name ? { name: delta.name } : {}),
                      arguments: delta.text,
                    },
                  },
                ],
              };
      chunks.push(
        encodeSSE({
          id: this.id,
          object: "chat.completion.chunk",
          model: this.model,
          choices: [{ index: 0, delta: change, finish_reason: null }],
        }),
      );
    } else if (this.target === "messages") {
      if (first && this.messageOpen !== undefined) {
        chunks.push(
          encodeSSE({ type: "content_block_stop", index: this.messageOpen }, "content_block_stop"),
        );
        (this.messageClosed ??= new Set()).add(this.messageOpen);
      }
      if (first) this.messageOpen = delta.index;
      if (first)
        chunks.push(
          encodeSSE(
            {
              type: "content_block_start",
              index: delta.index,
              content_block:
                delta.type === "text"
                  ? { type: "text", text: "" }
                  : delta.type === "reasoning"
                    ? { type: "thinking", thinking: "" }
                    : {
                        type: "tool_use",
                        id: this.tools?.get(delta.index)?.id,
                        name: this.tools?.get(delta.index)?.name,
                        input: {},
                      },
            },
            "content_block_start",
          ),
        );
      chunks.push(
        encodeSSE(
          {
            type: "content_block_delta",
            index: delta.index,
            delta:
              delta.type === "text"
                ? { type: "text_delta", text: delta.text }
                : delta.type === "reasoning"
                  ? { type: "thinking_delta", thinking: delta.text }
                  : { type: "input_json_delta", partial_json: delta.text },
          },
          "content_block_delta",
        ),
      );
    } else if (this.target === "responses") {
      if (first) {
        chunks.push(
          this.event("response.output_item.added", {
            output_index: delta.index,
            item:
              delta.type === "text"
                ? {
                    id: `msg_${delta.index}`,
                    type: "message",
                    role: "assistant",
                    content: [],
                    status: "in_progress",
                  }
                : delta.type === "reasoning"
                  ? {
                      id: `rs_${delta.index}`,
                      type: "reasoning",
                      summary: [],
                      status: "in_progress",
                    }
                  : {
                      id: `fc_${delta.index}`,
                      type: "function_call",
                      call_id: this.tools?.get(delta.index)?.id,
                      name: this.tools?.get(delta.index)?.name,
                      arguments: "",
                      status: "in_progress",
                    },
          }),
        );
        if (delta.type === "text")
          chunks.push(
            this.event("response.content_part.added", {
              output_index: delta.index,
              item_id: `msg_${delta.index}`,
              content_index: 0,
              part: { type: "output_text", text: "", annotations: [] },
            }),
          );
      }
      chunks.push(
        this.event(
          delta.type === "text"
            ? "response.output_text.delta"
            : delta.type === "reasoning"
              ? "response.reasoning_summary_text.delta"
              : "response.function_call_arguments.delta",
          {
            output_index: delta.index,
            item_id: `${delta.type === "text" ? "msg" : delta.type === "reasoning" ? "rs" : "fc"}_${delta.index}`,
            ...(delta.type === "text"
              ? { content_index: 0 }
              : delta.type === "reasoning"
                ? { summary_index: 0 }
                : {}),
            delta: delta.text,
          },
        ),
      );
    } else if (delta.type === "text")
      chunks.push(
        encodeSSE({
          candidates: [{ index: 0, content: { role: "model", parts: [{ text: delta.text }] } }],
        }),
      );
    else if (delta.type === "reasoning")
      chunks.push(
        encodeSSE({
          candidates: [
            { index: 0, content: { role: "model", parts: [{ text: delta.text, thought: true }] } },
          ],
        }),
      );
    openBlocks.add(delta.index);
    return chunks;
  }

  private complete(): Uint8Array[] {
    const chunks: Uint8Array[] = [];
    if (!this.started) chunks.push(...this.start());
    chunks.push(
      ...encodeStreamComplete({
        id: this.id,
        target: this.target,
        model: this.model,
        finish: this.finish,
        usage: this.usage,
        nextEvent: (type, body) => this.event(type, body),
        openBlocks: this.openBlocks,
        messageClosed: this.messageClosed,
        tools: this.tools,
        textBlocks: this.textBlocks,
        reasoningBlocks: this.reasoningBlocks,
      }),
    );
    return chunks;
  }
}
