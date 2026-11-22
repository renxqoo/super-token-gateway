import { GatewayError, providerObject as object, optionalObject, text } from "@gateway/contracts";
import { encodeSSE } from "./sse.ts";
import { extractUsage } from "./usage.ts";
import { parseProviderJson, copyExact, stringifyExact } from "./json.ts";
import type { Protocol, ProviderKind, Usage } from "@gateway/kernel";
import type { SSEEvent } from "./sse.ts";
import { finishReason, blockedPrompt } from "./finish.ts";
import { encodeResponse, publicUsage } from "./response.ts";
import type { Completion } from "./response.ts";
import type { Part } from "./request.ts";

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
  private tools = new Map<number, { id: string; name: string; args: string }>();
  private textBlocks = new Map<number, string>();
  private reasoningBlocks = new Map<number, string>();
  private blockTypes = new Map<number, string>();
  private openBlocks = new Set<number>();
  private accumulatedBytes = 0;
  holdTerminal = false;
  remoteId: string | undefined;
  publicId: string | undefined;
  private targetIndices = new Map<string, number>();
  private nextTargetIndex = 0;
  private messageOpen: number | undefined;
  private messageClosed = new Set<number>();
  private geminiCompleted = new Set<number>();
  private chatCompleted = new Set<number>();
  private geminiCalls = new Map<string, { index: number; args: string; name: string }>();
  private readonly id = `gen_${crypto.randomUUID()}`;
  constructor(
    readonly source: Protocol,
    readonly target: Protocol,
    readonly kind: ProviderKind | "custom",
    readonly model: string,
    private readonly maxAggregate = 1_048_576,
    private readonly expectedCandidates = 1,
  ) {}

  consume(event: SSEEvent): { chunks: Uint8Array[]; terminal: boolean } {
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
        chunks: this.source === this.target ? [new TextEncoder().encode(event.wire)] : [],
        terminal: false,
      };
    if (event.data === "[DONE]") {
      if (this.source !== "chat")
        throw new GatewayError(502, "invalid_upstream", "Unexpected terminal sentinel");
      if (this.chatCompleted.size !== this.expectedCandidates) {
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
    const nested = optionalObject(body.message);
    this.usage =
      extractUsage(Object.keys(nested).length ? nested : body, this.kind, this.usage) ?? this.usage;
    if (
      body.error ||
      event.event === "error" ||
      body.type === "error" ||
      body.type === "response.failed"
    ) {
      this.failed = true;
      throw new GatewayError(502, "provider_stream_error", "Provider reported a stream error");
    }
    const rawType = text(body.type, event.event);
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
        if (candidate.finishReason) this.geminiCompleted.add(index);
      }
    let terminalNow =
      this.source === "responses"
        ? ["response.completed", "response.incomplete"].includes(rawType)
        : this.source === "messages"
          ? rawType === "message_stop"
          : this.source === "gemini" && this.geminiCompleted.size === this.expectedCandidates;
    if (this.source === "gemini" && blockedPrompt(body)) {
      terminalNow = true;
      this.finish = "content_filter";
    }
    this.terminal = terminalNow;
    const deltas: StreamDelta[] = [];
    if (this.source === "chat") {
      const choices = Array.isArray(body.choices) ? body.choices : [];
      if (this.target !== "chat" && choices.length > 1)
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
          this.chatCompleted.add(index);
        }
        const delta = optionalObject(choice.delta);
        if (this.target !== this.source && (delta.refusal || delta.audio))
          throw new GatewayError(502, "conversion_loss", "Unsupported stream delta");
        const reasoningDelta = delta.reasoning_content ?? delta.reasoning;
        if (typeof reasoningDelta === "string" && reasoningDelta.length > 0)
          deltas.push({ type: "reasoning", index: 0, text: reasoningDelta });
        if (typeof delta.content === "string")
          deltas.push({ type: "text", index: 0, text: delta.content });
        if (Array.isArray(delta.tool_calls))
          for (const rawCall of delta.tool_calls) {
            const call = object(rawCall);
            const fn = optionalObject(call.function);
            deltas.push({
              type: "tool",
              index: Number(call.index ?? 0) + 1,
              text: text(fn.arguments),
              ...(call.id ? { id: text(call.id) } : {}),
              ...(fn.name ? { name: text(fn.name) } : {}),
            });
          }
        if (this.target !== this.source)
          this.finish = finishReason("chat", choice.finish_reason) ?? this.finish;
        if (choice.finish_reason === "length") this.finish = "length";
        if (choice.finish_reason === "tool_calls") this.finish = "tool_calls";
        if (choice.finish_reason === "content_filter") this.finish = "content_filter";
      }
    } else if (this.source === "responses") {
      const type = text(body.type, event.event);
      const index = Number(body.output_index ?? 0);
      if (type === "response.output_text.delta")
        deltas.push({ type: "text", index, text: text(body.delta) });
      if (
        type === "response.reasoning_summary_text.delta" ||
        type === "response.reasoning_text.delta"
      )
        deltas.push({ type: "reasoning", index, text: text(body.delta) });
      if (type === "response.output_item.added") {
        const item = optionalObject(body.item);
        if (item.type === "function_call")
          deltas.push({
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
        deltas.push({ type: "tool", index, text: text(body.delta) });
      if (type === "response.completed" || type === "response.incomplete") {
        terminalNow = true;
        if (type === "response.incomplete") this.finish = "length";
      }
    } else if (this.source === "messages") {
      const type = text(body.type, event.event);
      const index = Number(body.index ?? 0);
      if (type === "content_block_start") {
        const block = object(body.content_block);
        this.blockTypes.set(index, text(block.type));
        if (block.type === "tool_use")
          deltas.push({
            type: "tool",
            index,
            text: "",
            id: text(block.id),
            name: text(block.name),
          });
        else if (block.type === "text" && typeof block.text === "string" && block.text.length > 0)
          deltas.push({ type: "text", index, text: block.text });
        else if (
          block.type === "thinking" &&
          typeof block.thinking === "string" &&
          block.thinking.length > 0
        )
          deltas.push({ type: "reasoning", index, text: block.thinking });
        else if (this.target !== this.source && block.type !== "text" && block.type !== "thinking")
          throw new GatewayError(502, "conversion_loss", "Unsupported Anthropic stream block");
      }
      if (type === "content_block_delta") {
        const delta = object(body.delta);
        if (delta.type === "text_delta")
          deltas.push({ type: "text", index, text: text(delta.text) });
        else if (delta.type === "thinking_delta")
          deltas.push({ type: "reasoning", index, text: text(delta.thinking) });
        else if (delta.type === "signature_delta") {
          void 0;
        } else if (delta.type === "input_json_delta")
          deltas.push({ type: "tool", index, text: text(delta.partial_json) });
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
            if (part.thought === true && typeof part.text === "string") {
              deltas.push({ type: "reasoning", index: 0, text: part.text });
              continue;
            }
            if (typeof part.text === "string")
              deltas.push({ type: "text", index: 0, text: part.text });
            else if (part.functionCall) {
              const call = object(part.functionCall);
              const args = stringifyExact(call.args ?? {});
              const id = text(call.id, `call_${this.geminiCalls.size}`);
              const identity = `${Number(candidate.index ?? 0)}:${id}`;
              const prior = this.geminiCalls.get(identity);
              if (prior) {
                if (prior.args !== args || prior.name !== text(call.name))
                  throw new GatewayError(
                    502,
                    "conversion_loss",
                    "Cannot preserve changing Gemini tool snapshots",
                  );
                continue;
              }
              const callIndex = this.geminiCalls.size + 1;
              this.geminiCalls.set(identity, { index: callIndex, args, name: text(call.name) });
              deltas.push({
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
    if (this.source === this.target) {
      const native = copyExact(body, { ...body });
      if (typeof native.model === "string") native.model = this.model;
      if (body.message && typeof nested.model === "string")
        native.message = { ...nested, model: this.model };
      if (native.response && typeof optionalObject(native.response).model === "string")
        native.response = { ...optionalObject(native.response), model: this.model };
      chunks.push(encodeSSE(native, event.event === "message" ? undefined : event.event));
    } else {
      if (!this.started) chunks.push(...this.start());
      for (const delta of deltas) chunks.push(...this.delta(delta));
    }
    this.terminal = terminalNow;
    return { chunks, terminal: terminalNow };
  }

  finalize(): Uint8Array[] {
    if (!this.terminal || this.failed)
      throw new GatewayError(502, "stream_truncated", "Cannot finalize incomplete stream");
    return this.source === this.target ? [] : this.complete();
  }

  private event(type: string, body: Record<string, unknown>): Uint8Array {
    return encodeSSE({ type, sequence_number: this.sequence++, ...body }, type);
  }

  private start(): Uint8Array[] {
    this.started = true;
    if (this.target === "chat")
      return [
        encodeSSE({
          id: this.id,
          object: "chat.completion.chunk",
          model: this.model,
          choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }],
        }),
      ];
    if (this.target === "messages")
      return [
        encodeSSE(
          {
            type: "message_start",
            message: {
              id: this.id,
              type: "message",
              role: "assistant",
              model: this.model,
              content: [],
              stop_reason: null,
              usage: publicUsage(this.usage, "messages"),
            },
          },
          "message_start",
        ),
      ];
    if (this.target === "responses")
      return [
        this.event("response.created", {
          response: {
            id: this.id,
            object: "response",
            model: this.model,
            status: "in_progress",
            output: [],
          },
        }),
      ];
    return [];
  }

  private delta(delta: StreamDelta): Uint8Array[] {
    const sourceIndex = `${delta.type}:${delta.index}`;
    let targetIndex = this.targetIndices.get(sourceIndex);
    if (
      targetIndex === undefined ||
      (this.target === "messages" && this.messageClosed.has(targetIndex))
    ) {
      targetIndex = this.nextTargetIndex++;
      this.targetIndices.set(sourceIndex, targetIndex);
    }
    delta = { ...delta, index: targetIndex };
    const chunks: Uint8Array[] = [];
    if (!Number.isSafeInteger(delta.index) || delta.index < 0 || delta.index > 127)
      throw new GatewayError(502, "invalid_upstream", "Stream block index is out of range");
    const first = !this.openBlocks.has(delta.index);
    if ((delta.type === "text" || delta.type === "reasoning") && this.tools.has(delta.index))
      throw new GatewayError(502, "invalid_upstream", "Stream block type changed");
    if (delta.type === "tool") {
      const previous = this.tools.get(delta.index);
      const tool = {
        id: delta.id ?? previous?.id ?? `call_${delta.index}`,
        name: delta.name ?? previous?.name ?? "",
        args: (previous?.args ?? "") + delta.text,
      };
      this.tools.set(delta.index, tool);
      this.finish = "tool_calls";
    }
    if (this.target === "responses" || delta.type === "tool") {
      this.accumulatedBytes += new TextEncoder().encode(delta.text).byteLength;
      if (this.accumulatedBytes > this.maxAggregate)
        throw new GatewayError(
          502,
          "conversion_limit",
          "Converted response aggregate exceeds limit",
        );
      if (delta.type === "text")
        this.textBlocks.set(delta.index, (this.textBlocks.get(delta.index) ?? "") + delta.text);
      if (delta.type === "reasoning")
        this.reasoningBlocks.set(
          delta.index,
          (this.reasoningBlocks.get(delta.index) ?? "") + delta.text,
        );
    }
    if (this.target === "messages" && delta.type === "tool") return [];
    if (
      this.target === "messages" &&
      (delta.type === "text" || delta.type === "reasoning") &&
      this.tools.size
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
                    ...(first ? { id: this.tools.get(delta.index)?.id, type: "function" } : {}),
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
        this.messageClosed.add(this.messageOpen);
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
                        id: this.tools.get(delta.index)?.id,
                        name: this.tools.get(delta.index)?.name,
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
                      call_id: this.tools.get(delta.index)?.id,
                      name: this.tools.get(delta.index)?.name,
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
    this.openBlocks.add(delta.index);
    return chunks;
  }

  private complete(): Uint8Array[] {
    const chunks: Uint8Array[] = [];
    if (!this.started) chunks.push(...this.start());
    if (this.target === "chat") {
      chunks.push(
        encodeSSE({
          id: this.id,
          object: "chat.completion.chunk",
          model: this.model,
          choices: [{ index: 0, delta: {}, finish_reason: this.finish }],
        }),
      );
      if (this.usage)
        chunks.push(
          encodeSSE({
            id: this.id,
            object: "chat.completion.chunk",
            model: this.model,
            choices: [],
            usage: publicUsage(this.usage, "chat"),
          }),
        );
      chunks.push(encodeSSE("[DONE]"));
    } else if (this.target === "messages") {
      for (const index of this.openBlocks)
        if (!this.messageClosed.has(index))
          chunks.push(encodeSSE({ type: "content_block_stop", index }, "content_block_stop"));
      for (const [index, tool] of this.tools) {
        parseProviderJson(tool.args || "{}");
        chunks.push(
          encodeSSE(
            {
              type: "content_block_start",
              index,
              content_block: { type: "tool_use", id: tool.id, name: tool.name, input: {} },
            },
            "content_block_start",
          ),
        );
        chunks.push(
          encodeSSE(
            {
              type: "content_block_delta",
              index,
              delta: { type: "input_json_delta", partial_json: tool.args || "{}" },
            },
            "content_block_delta",
          ),
        );
        chunks.push(encodeSSE({ type: "content_block_stop", index }, "content_block_stop"));
      }
      chunks.push(
        encodeSSE(
          {
            type: "message_delta",
            delta: {
              stop_reason:
                this.finish === "content_filter"
                  ? "refusal"
                  : this.finish === "length"
                    ? "max_tokens"
                    : this.tools.size
                      ? "tool_use"
                      : "end_turn",
              stop_sequence: null,
            },
            usage: publicUsage(this.usage, "messages"),
          },
          "message_delta",
        ),
      );
      chunks.push(encodeSSE({ type: "message_stop" }, "message_stop"));
    } else if (this.target === "responses") {
      const parts: Part[] = [];
      const sortedBlocks = [...this.openBlocks].sort((a, b) => a - b);
      for (const index of sortedBlocks) {
        const tool = this.tools.get(index);
        if (tool) {
          parts.push({ type: "call", id: tool.id, name: tool.name, arguments: tool.args });
          chunks.push(
            this.event("response.function_call_arguments.done", {
              output_index: index,
              item_id: `fc_${index}`,
              arguments: tool.args,
            }),
          );
        } else if (this.reasoningBlocks.has(index)) {
          const value = this.reasoningBlocks.get(index) ?? "";
          parts.push({ type: "reasoning", text: value });
          chunks.push(
            this.event("response.reasoning_summary_text.done", {
              output_index: index,
              item_id: `rs_${index}`,
              summary_index: 0,
              text: value,
            }),
          );
        } else {
          const value = this.textBlocks.get(index) ?? "";
          parts.push({ type: "text", text: value });
          chunks.push(
            this.event("response.output_text.done", {
              output_index: index,
              item_id: `msg_${index}`,
              content_index: 0,
              text: value,
            }),
          );
          chunks.push(
            this.event("response.content_part.done", {
              output_index: index,
              item_id: `msg_${index}`,
              content_index: 0,
              part: { type: "output_text", text: value, annotations: [] },
            }),
          );
        }
      }
      const response = encodeResponse(
        { id: this.id, parts, finish: this.finish, usage: this.usage },
        "responses",
        this.model,
      );
      const output = Array.isArray(response.output) ? response.output : [];
      for (let i = 0; i < output.length; i++)
        chunks.push(
          this.event("response.output_item.done", {
            output_index: sortedBlocks[i],
            item: output[i],
          }),
        );
      chunks.push(
        this.event(
          ["length", "content_filter"].includes(this.finish)
            ? "response.incomplete"
            : "response.completed",
          {
            response,
          },
        ),
      );
    } else {
      for (const tool of this.tools.values())
        chunks.push(
          encodeSSE({
            candidates: [
              {
                index: 0,
                content: {
                  role: "model",
                  parts: [
                    {
                      functionCall: {
                        id: tool.id,
                        name: tool.name,
                        args: parseProviderJson(tool.args || "{}"),
                      },
                    },
                  ],
                },
              },
            ],
          }),
        );
      chunks.push(
        encodeSSE({
          candidates: [
            {
              index: 0,
              finishReason:
                this.finish === "content_filter"
                  ? "SAFETY"
                  : this.finish === "length"
                    ? "MAX_TOKENS"
                    : "STOP",
            },
          ],
          ...(this.usage ? { usageMetadata: publicUsage(this.usage, "gemini") } : {}),
        }),
      );
    }
    return chunks;
  }
}
