import { encodeSSE } from "./sse.ts";
import { parseProviderJson } from "./json.ts";
import { encodeResponse, publicUsage, type Completion } from "./response.ts";
import type { Protocol, Usage } from "@gateway/kernel";
import type { Part } from "./request.ts";

export interface StreamEncodeState {
  id: string;
  target: Protocol;
  model: string;
  finish: Completion["finish"];
  usage: Usage | undefined;
  nextEvent: (type: string, body: Record<string, unknown>) => Uint8Array;
  openBlocks?: Set<number> | undefined;
  messageClosed?: Set<number> | undefined;
  tools?: Map<number, { id: string; name: string; args: string }> | undefined;
  textBlocks?: Map<number, string> | undefined;
  reasoningBlocks?: Map<number, string> | undefined;
}

export function encodeStreamStart(state: StreamEncodeState): Uint8Array[] {
  if (state.target === "chat")
    return [
      encodeSSE({
        id: state.id,
        object: "chat.completion.chunk",
        model: state.model,
        choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }],
      }),
    ];
  if (state.target === "messages")
    return [
      encodeSSE(
        {
          type: "message_start",
          message: {
            id: state.id,
            type: "message",
            role: "assistant",
            model: state.model,
            content: [],
            stop_reason: null,
            usage: publicUsage(state.usage, "messages"),
          },
        },
        "message_start",
      ),
    ];
  if (state.target === "responses")
    return [
      state.nextEvent("response.created", {
        response: {
          id: state.id,
          object: "response",
          model: state.model,
          status: "in_progress",
          output: [],
        },
      }),
    ];
  return [];
}

export function encodeStreamComplete(state: StreamEncodeState): Uint8Array[] {
  const chunks: Uint8Array[] = [];
  if (state.target === "chat") {
    chunks.push(
      encodeSSE({
        id: state.id,
        object: "chat.completion.chunk",
        model: state.model,
        choices: [{ index: 0, delta: {}, finish_reason: state.finish }],
      }),
    );
    if (state.usage)
      chunks.push(
        encodeSSE({
          id: state.id,
          object: "chat.completion.chunk",
          model: state.model,
          choices: [],
          usage: publicUsage(state.usage, "chat"),
        }),
      );
    chunks.push(encodeSSE("[DONE]"));
  } else if (state.target === "messages") {
    if (state.openBlocks)
      for (const index of state.openBlocks)
        if (!state.messageClosed?.has(index))
          chunks.push(encodeSSE({ type: "content_block_stop", index }, "content_block_stop"));
    if (state.tools)
      for (const [index, tool] of state.tools) {
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
              state.finish === "content_filter"
                ? "refusal"
                : state.finish === "length"
                  ? "max_tokens"
                  : state.tools?.size
                    ? "tool_use"
                    : "end_turn",
            stop_sequence: null,
          },
          usage: publicUsage(state.usage, "messages"),
        },
        "message_delta",
      ),
    );
    chunks.push(encodeSSE({ type: "message_stop" }, "message_stop"));
  } else if (state.target === "responses") {
    const parts: Part[] = [];
    const sortedBlocks = state.openBlocks ? [...state.openBlocks].sort((a, b) => a - b) : [];
    for (const index of sortedBlocks) {
      const tool = state.tools?.get(index);
      if (tool) {
        parts.push({ type: "call", id: tool.id, name: tool.name, arguments: tool.args });
        chunks.push(
          state.nextEvent("response.function_call_arguments.done", {
            output_index: index,
            item_id: `fc_${index}`,
            arguments: tool.args,
          }),
        );
      } else if (state.reasoningBlocks?.has(index)) {
        const value = state.reasoningBlocks.get(index) ?? "";
        parts.push({ type: "reasoning", text: value });
        chunks.push(
          state.nextEvent("response.reasoning_summary_text.done", {
            output_index: index,
            item_id: `rs_${index}`,
            summary_index: 0,
            text: value,
          }),
        );
      } else {
        const value = state.textBlocks?.get(index) ?? "";
        parts.push({ type: "text", text: value });
        chunks.push(
          state.nextEvent("response.output_text.done", {
            output_index: index,
            item_id: `msg_${index}`,
            content_index: 0,
            text: value,
          }),
        );
        chunks.push(
          state.nextEvent("response.content_part.done", {
            output_index: index,
            item_id: `msg_${index}`,
            content_index: 0,
            part: { type: "output_text", text: value, annotations: [] },
          }),
        );
      }
    }
    const response = encodeResponse(
      { id: state.id, parts, finish: state.finish, usage: state.usage },
      "responses",
      state.model,
    );
    const output = Array.isArray(response.output) ? response.output : [];
    for (let i = 0; i < output.length; i++)
      chunks.push(
        state.nextEvent("response.output_item.done", {
          output_index: sortedBlocks[i],
          item: output[i],
        }),
      );
    chunks.push(
      state.nextEvent(
        ["length", "content_filter"].includes(state.finish)
          ? "response.incomplete"
          : "response.completed",
        { response },
      ),
    );
  } else {
    if (state.tools)
      for (const tool of state.tools.values())
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
              state.finish === "content_filter"
                ? "SAFETY"
                : state.finish === "length"
                  ? "MAX_TOKENS"
                  : "STOP",
          },
        ],
        ...(state.usage ? { usageMetadata: publicUsage(state.usage, "gemini") } : {}),
      }),
    );
  }
  return chunks;
}
