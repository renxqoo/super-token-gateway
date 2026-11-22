import { setTimeout as sleep } from "node:timers/promises";
import type { CompatibleVariant } from "./compatible.ts";
import { obj, sse } from "./http.ts";

export interface Scenario {
  status?: number;
  errorUsage?: boolean;
  errorBodyBytes?: number;
  failFirst?: number;
  headerDelayMs?: number;
  firstEventDelayMs?: number;
  chunkDelayMs?: number;
  chunkBytes?: number;
  streamDurationMs?: number;
  jsonChunkDelayMs?: number;
  jsonChunkBytes?: number;
  truncate?: boolean;
  streamError?: boolean;
  malformed?: boolean;
  tools?: boolean;
  output?: string;
  echoRequestChallenge?: boolean;
  repeat?: number;
  omitUsage?: boolean;
  pollUsage?: boolean;
  batchRowUsage?: Record<string, unknown>[];
  batchUsage?: Record<string, unknown>;
  batchRowIds?: string[];
  batchReportedTotal?: number;
  emptyChoices?: boolean;
  invalidChoice?: "missing_message" | "empty_message" | "unfinished";
  terminalOnly?: boolean;
  whitespace?: boolean;
  thoughtSignature?: boolean;
  binaryBytes?: number;
  finishReason?: string;
  promptBlocked?: boolean;
  imageData?: unknown;
}
export type MockStats = { requests: number; active: number; cancellations: number; bytes: number };

export interface FixtureResource {
  id: string;
  kind: string;
  polls: number;
  output: string;
  cancelled: boolean;
  payload?: Record<string, unknown>;
  model?: string;
  batchIds?: string[];
}

export function createConversationFixture(
  scenarios: ReadonlyMap<string, Scenario>,
  attempts: Map<string, number>,
  resources: Map<string, FixtureResource>,
  stats: MockStats,
) {
  return async (
    request: Request,
    dialect: string,
    modelFromPath?: string,
    variant?: CompatibleVariant,
  ): Promise<Response> => {
    const body = obj(await request.json());
    const model = modelFromPath ?? String(body.model ?? "default");
    const scenario = scenarios.get(model) ?? {};
    const attempt = (attempts.get(model) ?? 0) + 1;
    attempts.set(model, attempt);
    stats.requests++;
    if (scenario.headerDelayMs) await Bun.sleep(scenario.headerDelayMs);
    if (scenario.status && (!scenario.failFirst || attempt <= scenario.failFirst))
      return Response.json(
        {
          error: {
            message:
              "mock-secret-error-must-not-persist" + "x".repeat(scenario.errorBodyBytes ?? 0),
            type: "mock_failure",
          },
          ...(scenario.errorUsage
            ? {
                usage: {
                  prompt_tokens: 10,
                  prompt_tokens_details: { cached_tokens: 3 },
                  completion_tokens: 5,
                },
              }
            : {}),
        },
        { status: scenario.status, headers: { "retry-after": "1" } },
      );
    if (scenario.malformed)
      return new Response("{ malformed mock payload", {
        headers: { "content-type": "application/json" },
      });
    const output = scenario.output ?? "Hello, 世界 🌍";
    const id = `${dialect}_${crypto.randomUUID().replaceAll("-", "")}`;
    const tool = scenario.tools === true;
    const usage =
      dialect === "anthropic"
        ? {
            input_tokens: 7,
            output_tokens: 5,
            cache_read_input_tokens: 3,
            cache_creation_input_tokens: 2,
          }
        : dialect === "gemini"
          ? {
              promptTokenCount: 10,
              candidatesTokenCount: 3,
              cachedContentTokenCount: 3,
              thoughtsTokenCount: 2,
              totalTokenCount: 15,
            }
          : variant === "deepseek"
            ? {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
                prompt_cache_hit_tokens: 3,
                prompt_cache_miss_tokens: 7,
              }
            : {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
                prompt_tokens_details: { cached_tokens: 3 },
                completion_tokens_details: { reasoning_tokens: 2 },
              };
    const rawUsage = scenario.omitUsage
      ? {}
      : dialect === "gemini"
        ? { usageMetadata: usage }
        : { usage };
    const fn = { name: "weather", arguments: '{"city":"北京"}' };
    let value: Record<string, unknown>;
    if (dialect === "anthropic")
      value = {
        id,
        type: "message",
        role: "assistant",
        model,
        content: tool
          ? [{ type: "tool_use", id: "call_1", name: fn.name, input: { city: "北京" } }]
          : [{ type: "text", text: output }],
        stop_reason: tool ? "tool_use" : "end_turn",
        ...rawUsage,
      };
    else if (dialect === "gemini")
      value = {
        candidates: [
          {
            index: 0,
            content: {
              role: "model",
              parts: tool
                ? [{ functionCall: { id: "call_1", name: fn.name, args: { city: "北京" } } }]
                : [{ text: output }],
            },
            finishReason: scenario.finishReason ?? "STOP",
          },
        ],
        ...rawUsage,
      };
    else if (dialect === "responses")
      value = {
        id,
        object: "response",
        model,
        status: "completed",
        output: tool
          ? [
              {
                id: "fc_1",
                type: "function_call",
                call_id: "call_1",
                name: fn.name,
                arguments: fn.arguments,
              },
            ]
          : [
              {
                id: "msg_1",
                type: "message",
                role: "assistant",
                content: [{ type: "output_text", text: output, annotations: [] }],
              },
            ],
        ...(scenario.omitUsage
          ? {}
          : {
              usage: {
                input_tokens: 10,
                output_tokens: 5,
                total_tokens: 15,
                input_tokens_details: { cached_tokens: 3 },
                output_tokens_details: { reasoning_tokens: 2 },
              },
            }),
      };
    else
      value = {
        id,
        object: "chat.completion",
        model,
        choices: [
          {
            index: 0,
            message: {
              role: "assistant",
              content: tool ? null : output,
              ...(tool ? { tool_calls: [{ id: "call_1", type: "function", function: fn }] } : {}),
            },
            finish_reason: tool ? "tool_calls" : "stop",
          },
        ],
        ...rawUsage,
        ...(variant === "minimax" ? { base_resp: { status_code: 0, status_msg: "" } } : {}),
      };
    if (dialect === "gemini" && scenario.promptBlocked) {
      const blocked = {
        promptFeedback: { blockReason: "SAFETY" },
        usageMetadata: {
          promptTokenCount: 10,
          candidatesTokenCount: 0,
          cachedContentTokenCount: 3,
        },
      };
      return body.stream === true ||
        new URL(request.url).pathname.endsWith(":streamGenerateContent")
        ? new Response(sse(blocked), { headers: { "content-type": "text/event-stream" } })
        : Response.json(blocked);
    }
    const streaming =
      body.stream === true || new URL(request.url).pathname.endsWith(":streamGenerateContent");
    if (scenario.emptyChoices) value = { ...value, choices: [] };
    if (scenario.invalidChoice)
      value = {
        ...value,
        choices: [
          scenario.invalidChoice === "missing_message"
            ? {}
            : scenario.invalidChoice === "empty_message"
              ? { index: 0, message: {}, finish_reason: "stop" }
              : {
                  index: 0,
                  message: { role: "assistant", content: "unfinished" },
                  finish_reason: null,
                },
        ],
      };
    if (streaming && scenario.terminalOnly)
      return new Response(sse("[DONE]"), { headers: { "content-type": "text/event-stream" } });
    if (scenario.thoughtSignature && dialect === "gemini")
      value = {
        ...value,
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: output, thoughtSignature: "provider-signed-history" }],
            },
            finishReason: "STOP",
          },
        ],
      };
    if (dialect === "responses" && body.store !== false)
      resources.set(id, {
        id,
        kind: "response",
        polls: 0,
        output,
        cancelled: false,
        payload: value,
      });
    if (!streaming) {
      const json =
        body.background === true && dialect === "responses"
          ? { ...value, status: "queued", output: [], usage: null }
          : value;
      if (scenario.jsonChunkDelayMs) {
        const bytes = new TextEncoder().encode(JSON.stringify(json));
        let offset = 0;
        return new Response(
          new ReadableStream<Uint8Array>({
            async pull(controller) {
              if (offset === bytes.length) {
                controller.close();
                return;
              }
              await Bun.sleep(scenario.jsonChunkDelayMs!);
              const chunk = bytes.slice(offset, offset + (scenario.jsonChunkBytes ?? 32));
              offset += chunk.length;
              controller.enqueue(chunk);
            },
          }),
          { headers: { "content-type": "application/json", "x-request-id": id } },
        );
      }
      return Response.json(json, { headers: { "x-request-id": id } });
    }
    const events: string[] = [];
    if (dialect === "anthropic") {
      events.push(
        sse(
          {
            type: "message_start",
            message: {
              id,
              type: "message",
              model,
              role: "assistant",
              content: [],
              usage: {
                input_tokens: 7,
                output_tokens: 0,
                cache_read_input_tokens: 3,
                cache_creation_input_tokens: 2,
              },
            },
          },
          "message_start",
        ),
      );
      events.push(
        sse(
          {
            type: "content_block_start",
            index: 0,
            content_block: tool
              ? { type: "tool_use", id: "call_1", name: fn.name, input: {} }
              : { type: "text", text: "" },
          },
          "content_block_start",
        ),
      );
      const pieces = tool ? ['{"city":', '"北京"}'] : Array(scenario.repeat ?? 1).fill(output);
      for (const piece of pieces)
        events.push(
          sse(
            {
              type: "content_block_delta",
              index: 0,
              delta: tool
                ? { type: "input_json_delta", partial_json: piece }
                : { type: "text_delta", text: piece },
            },
            "content_block_delta",
          ),
        );
      if (!scenario.truncate) {
        events.push(sse({ type: "content_block_stop", index: 0 }, "content_block_stop"));
        events.push(
          sse(
            {
              type: "message_delta",
              delta: { stop_reason: tool ? "tool_use" : "end_turn" },
              ...(scenario.omitUsage ? {} : { usage: { output_tokens: 5 } }),
            },
            "message_delta",
          ),
        );
        events.push(sse({ type: "message_stop" }, "message_stop"));
      }
    } else if (dialect === "responses") {
      events.push(
        sse(
          { type: "response.created", response: { id, status: "in_progress", output: [] } },
          "response.created",
        ),
      );
      events.push(
        sse(
          {
            type: "response.output_item.added",
            output_index: 0,
            item: tool
              ? {
                  type: "function_call",
                  id: "fc_1",
                  call_id: "call_1",
                  name: fn.name,
                  arguments: "",
                }
              : { type: "message", id: "msg_1", role: "assistant", content: [] },
          },
          "response.output_item.added",
        ),
      );
      for (const delta of tool ? ['{"city":', '"北京"}'] : Array(scenario.repeat ?? 1).fill(output))
        events.push(
          sse(
            {
              type: tool ? "response.function_call_arguments.delta" : "response.output_text.delta",
              output_index: 0,
              delta,
            },
            tool ? "response.function_call_arguments.delta" : "response.output_text.delta",
          ),
        );
      if (!scenario.truncate)
        events.push(sse({ type: "response.completed", response: value }, "response.completed"));
    } else if (dialect === "gemini") {
      for (let index = 0; index < (scenario.repeat ?? 1); index++)
        events.push(
          sse({
            candidates: [
              {
                index: 0,
                content: {
                  role: "model",
                  parts: tool
                    ? [{ functionCall: { id: "call_1", name: fn.name, args: { city: "北京" } } }]
                    : [{ text: output }],
                },
              },
            ],
            ...(scenario.truncate ? {} : { usageMetadata: usage }),
          }),
        );
      if (!scenario.truncate)
        events.push(
          sse({
            candidates: [{ index: 0, finishReason: scenario.finishReason ?? "STOP" }],
            ...rawUsage,
          }),
        );
    } else {
      events.push(
        sse({
          id,
          object: "chat.completion.chunk",
          model,
          choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }],
          ...(variant === "deepseek" && obj(body.stream_options).include_usage === true
            ? { usage: null }
            : {}),
        }),
      );
      const challenge = scenario.echoRequestChallenge && !tool ? body.user : undefined;
      if (
        scenario.echoRequestChallenge &&
        !tool &&
        (typeof challenge !== "string" || !/^[0-9a-f-]{36}[0-9a-f]{8}$/.test(challenge))
      )
        return Response.json({ error: "missing request challenge" }, { status: 400 });
      const textDeltas = tool ? [] : Array(scenario.repeat ?? 1).fill({ content: output });
      if (typeof challenge === "string" && textDeltas.length)
        textDeltas[textDeltas.length - 1] = { content: output + " " + challenge };
      for (const delta of tool
        ? [
            {
              tool_calls: [
                {
                  index: 0,
                  id: "call_1",
                  type: "function",
                  function: { name: fn.name, arguments: '{"city":' },
                },
              ],
            },
            { tool_calls: [{ index: 0, function: { arguments: '"北京"}' } }] },
          ]
        : textDeltas)
        events.push(
          sse({
            id,
            model,
            choices: [{ index: 0, delta, finish_reason: null }],
            ...(variant === "deepseek" && obj(body.stream_options).include_usage === true
              ? { usage: null }
              : {}),
          }),
        );
      if (!scenario.truncate) {
        events.push(
          sse({
            id,
            model,
            choices: [{ index: 0, delta: {}, finish_reason: tool ? "tool_calls" : "stop" }],
            ...(variant === "deepseek" && !scenario.omitUsage ? { usage } : {}),
          }),
        );
        if (variant !== "deepseek" && !scenario.omitUsage)
          events.push(sse({ id, model, choices: [], usage }));
        events.push(sse("[DONE]"));
      }
    }
    if (scenario.streamError)
      events.splice(
        1,
        events.length,
        sse({ error: { type: "mock_error", message: output } }, "error"),
      );
    const encoder = new TextEncoder();
    const encode = (text: string) =>
      encoder.encode(
        scenario.whitespace
          ? text.replaceAll('"finish_reason":null', '"finish_reason" :  null')
          : text,
      );
    const duration = scenario.streamDurationMs;
    if (
      duration !== undefined &&
      (!Number.isSafeInteger(duration) || duration < 1 || duration > 120000)
    )
      return Response.json({ error: "invalid stream duration" }, { status: 400 });
    const paced = duration === undefined ? undefined : events.map(encode);
    const all = paced ? undefined : encode(events.join(""));
    const started = performance.now();
    const delay = new AbortController();
    let frame = 0;
    let position = 0;
    let closed = false;
    stats.active++;
    const close = (cancelled = false) => {
      if (!closed) {
        closed = true;
        stats.active--;
        if (cancelled) stats.cancellations++;
        request.signal.removeEventListener("abort", abort);
        delay.abort();
      }
    };
    const abort = () => close(true);
    if (request.signal.aborted) abort();
    else request.signal.addEventListener("abort", abort, { once: true });
    return new Response(
      new ReadableStream<Uint8Array>(
        {
          async pull(writer) {
            if (closed) {
              writer.close();
              return;
            }
            try {
              if (paced) {
                const sendAt =
                  started + duration! * (Math.max(0, frame - 1) / Math.max(1, paced.length - 2));
                for (
                  let wait = sendAt - performance.now();
                  wait > 0;
                  wait = sendAt - performance.now()
                )
                  await sleep(Math.ceil(wait), undefined, { signal: delay.signal });
                const chunk = paced[frame++]!;
                stats.bytes += chunk.byteLength;
                writer.enqueue(chunk);
                if (frame < paced.length) return;
              } else {
                if (position === 0 && scenario.firstEventDelayMs)
                  await sleep(scenario.firstEventDelayMs, undefined, { signal: delay.signal });
                if (scenario.chunkDelayMs)
                  await sleep(scenario.chunkDelayMs, undefined, { signal: delay.signal });
                if (position < all!.length) {
                  const size = scenario.chunkBytes ?? 16384;
                  const chunk = all!.slice(position, position + size);
                  position += chunk.byteLength;
                  stats.bytes += chunk.byteLength;
                  writer.enqueue(chunk);
                  return;
                }
              }
            } catch (error) {
              if (closed) return;
              close();
              throw error;
            }
            close();
            writer.close();
          },
          cancel() {
            abort();
          },
        },
        { highWaterMark: 16384, size: (chunk) => chunk?.byteLength ?? 0 },
      ),
      { headers: { "content-type": "text/event-stream", "x-request-id": id } },
    );
  };
}
