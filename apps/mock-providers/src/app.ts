import { Hono } from "hono";

export interface Scenario {
  status?: number;
  errorUsage?: boolean;
  errorBodyBytes?: number;
  failFirst?: number;
  headerDelayMs?: number;
  firstEventDelayMs?: number;
  chunkDelayMs?: number;
  chunkBytes?: number;
  jsonChunkDelayMs?: number;
  jsonChunkBytes?: number;
  truncate?: boolean;
  streamError?: boolean;
  malformed?: boolean;
  tools?: boolean;
  output?: string;
  repeat?: number;
  omitUsage?: boolean;
  pollUsage?: boolean;
  batchRowUsage?: Record<string, unknown>[];
  emptyChoices?: boolean;
  invalidChoice?: "missing_message" | "empty_message" | "unfinished";
  terminalOnly?: boolean;
  whitespace?: boolean;
  thoughtSignature?: boolean;
  binaryBytes?: number;
  finishReason?: string;
  promptBlocked?: boolean;
}
interface FixtureResource {
  id: string;
  kind: string;
  polls: number;
  output: string;
  cancelled: boolean;
  payload?: Record<string, unknown>;
  model?: string;
}
const obj = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
const sse = (data: unknown, event?: string) =>
  `${event ? `event: ${event}\n` : ""}data: ${typeof data === "string" ? data : JSON.stringify(data)}\n\n`;

export function createMockApp(controlToken: string): {
  app: Hono;
  stats: { requests: number; active: number; cancellations: number; bytes: number };
  scenarios: Map<string, Scenario>;
} {
  const app = new Hono();
  app.all("/v1/*", (c) => {
    const url = new URL(c.req.url);
    url.pathname = `/openai${url.pathname}`;
    return app.fetch(new Request(url, c.req.raw));
  });
  const scenarios = new Map<string, Scenario>();
  const attempts = new Map<string, number>();
  const resources = new Map<string, FixtureResource>();
  const stats = { requests: 0, active: 0, cancellations: 0, bytes: 0 };
  app.get("/healthz", (c) => c.json({ status: "ready" }));
  app.use("/__control/*", async (c, next) => {
    if (c.req.header("authorization") !== `Bearer ${controlToken}`)
      return c.json({ error: "unauthorized" }, 401);
    await next();
  });
  app.post("/__control/scenarios", async (c) => {
    const body = obj(await c.req.json());
    if (typeof body.model !== "string") return c.json({ error: "model required" }, 400);
    scenarios.set(body.model, obj(body.scenario) as Scenario);
    attempts.delete(body.model);
    return c.json({ configured: true });
  });
  app.post("/__control/reset", (c) => {
    scenarios.clear();
    attempts.clear();
    resources.clear();
    stats.requests = 0;
    stats.cancellations = 0;
    stats.bytes = 0;
    return c.json({ reset: true });
  });
  app.get("/__control/stats", (c) =>
    c.json({ ...stats, attempts: Object.fromEntries(attempts), resources: resources.size }),
  );

  const generate = async (
    request: Request,
    dialect: string,
    modelFromPath?: string,
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
        }),
      );
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
        : Array(scenario.repeat ?? 1).fill({ content: output }))
        events.push(sse({ id, model, choices: [{ index: 0, delta, finish_reason: null }] }));
      if (!scenario.truncate) {
        events.push(
          sse({
            id,
            model,
            choices: [{ index: 0, delta: {}, finish_reason: tool ? "tool_calls" : "stop" }],
          }),
        );
        if (!scenario.omitUsage) events.push(sse({ id, model, choices: [], usage }));
        events.push(sse("[DONE]"));
      }
    }
    if (scenario.streamError)
      events.splice(
        1,
        events.length,
        sse({ error: { type: "mock_error", message: output } }, "error"),
      );
    const all = new TextEncoder().encode(
      scenario.whitespace
        ? events.join("").replaceAll('"finish_reason":null', '"finish_reason" :  null')
        : events.join(""),
    );
    let position = 0;
    let closed = false;
    stats.active++;
    const close = () => {
      if (!closed) {
        closed = true;
        stats.active--;
      }
    };
    request.signal.addEventListener(
      "abort",
      () => {
        if (!closed) stats.cancellations++;
        close();
      },
      { once: true },
    );
    return new Response(
      new ReadableStream<Uint8Array>(
        {
          async pull(writer) {
            if (closed) {
              writer.close();
              return;
            }
            if (position === 0 && scenario.firstEventDelayMs)
              await Bun.sleep(scenario.firstEventDelayMs);
            if (scenario.chunkDelayMs) await Bun.sleep(scenario.chunkDelayMs);
            if (position >= all.length) {
              close();
              writer.close();
              return;
            }
            const size = scenario.chunkBytes ?? 16384;
            const chunk = all.slice(position, position + size);
            position += chunk.length;
            stats.bytes += chunk.length;
            writer.enqueue(chunk);
          },
          cancel() {
            if (!closed) stats.cancellations++;
            close();
          },
        },
        { highWaterMark: 16384, size: (chunk) => chunk?.byteLength ?? 0 },
      ),
      { headers: { "content-type": "text/event-stream", "x-request-id": id } },
    );
  };

  for (const kind of ["deepseek", "ollama"])
    app.post(`/${kind}/v1/chat/completions`, async (c) => {
      const request = (await c.req.raw.clone().json()) as Record<string, unknown>;
      if (request.max_completion_tokens !== undefined || request.max_tokens === undefined)
        return c.json({ error: { message: "Expected max_tokens" } }, 400);
      return generate(c.req.raw, "openai");
    });
  app.post("/ollama/v1/responses", (c) => generate(c.req.raw, "responses"));
  app.post("/openai/v1/chat/completions", (c) => generate(c.req.raw, "openai"));
  app.post("/openai/v1/responses", (c) => generate(c.req.raw, "responses"));
  app.post("/anthropic/v1/messages", (c) => generate(c.req.raw, "anthropic"));
  app.post("/gemini/v1beta/models/:action", async (c) => {
    const action = c.req.param("action");
    const colon = action.lastIndexOf(":");
    const model = action.slice(0, colon);
    if (action.endsWith(":predictLongRunning")) {
      if (!c.req.header("content-type")?.includes("application/json"))
        return c.json({ error: "JSON required" }, 400);
      const payload = obj(await c.req.json());
      const instance = Array.isArray(payload.instances) ? obj(payload.instances[0]) : {};
      if (
        typeof instance.prompt !== "string" ||
        !instance.prompt ||
        typeof obj(payload.parameters).durationSeconds !== "number"
      )
        return c.json({ error: "Invalid video payload" }, 400);
      const remote = `operations/${crypto.randomUUID()}`;
      resources.set(remote, {
        id: remote,
        kind: "video",
        polls: 0,
        output: "mock-video-output-content",
        cancelled: false,
      });
      return c.json({ name: remote, done: false });
    }
    return generate(c.req.raw, "gemini", model);
  });
  for (const path of ["/openai/v1/embeddings", "/ollama/v1/embeddings"])
    app.post(path, async (c) => {
      await c.req.json();
      return c.json({
        object: "list",
        data: [{ object: "embedding", index: 0, embedding: [0.25, -0.5, 0.75] }],
        usage: { prompt_tokens: 10, total_tokens: 10 },
      });
    });
  const mediaEvents = (model: string, events: string[]) => {
    const scenario = scenarios.get(model) ?? {};
    if (scenario.truncate) events.pop();
    const bytes = new TextEncoder().encode(events.join(""));
    let offset = 0;
    return new Response(
      new ReadableStream<Uint8Array>({
        async pull(controller) {
          if (scenario.chunkDelayMs) await Bun.sleep(scenario.chunkDelayMs);
          if (offset === bytes.length) {
            controller.close();
            return;
          }
          const next = bytes.slice(offset, offset + (scenario.chunkBytes ?? 16384));
          offset += next.length;
          controller.enqueue(next);
        },
      }),
      { headers: { "content-type": "text/event-stream" } },
    );
  };
  for (const endpoint of ["generations", "edits"])
    app.post(`/openai/v1/images/${endpoint}`, async (c) => {
      const body = c.req.header("content-type")?.includes("multipart")
        ? Object.fromEntries(await c.req.formData())
        : obj(await c.req.json());
      const prefix = endpoint === "edits" ? "image_edit" : "image_generation";
      if (body.stream === true || body.stream === "true")
        return mediaEvents(String(body.model), [
          sse(
            {
              type: `${prefix}.partial_image`,
              partial_image_index: 0,
              b64_json: btoa("mock-partial-image-content"),
            },
            `${prefix}.partial_image`,
          ),
          sse(
            {
              type: `${prefix}.completed`,
              b64_json: btoa("mock-generated-image-content"),
              usage: { input_tokens: 10, output_tokens: 5 },
            },
            `${prefix}.completed`,
          ),
        ]);
      return c.json({
        created: 1,
        data: [{ b64_json: btoa("mock-generated-image-content") }],
        usage: { input_tokens: 10, output_tokens: 5 },
      });
    });
  const binary = (request: Request, bytes: number, mime: string) => {
    let remaining = bytes;
    let closed = false;
    stats.active++;
    const close = () => {
      if (!closed) {
        closed = true;
        stats.active--;
      }
    };
    request.signal.addEventListener(
      "abort",
      () => {
        if (!closed) stats.cancellations++;
        close();
      },
      { once: true },
    );
    return new Response(
      new ReadableStream<Uint8Array>(
        {
          pull(writer) {
            if (closed || remaining <= 0) {
              close();
              writer.close();
              return;
            }
            const size = Math.min(65536, remaining);
            remaining -= size;
            stats.bytes += size;
            writer.enqueue(new Uint8Array(size));
          },
          cancel() {
            if (!closed) stats.cancellations++;
            close();
          },
        },
        { highWaterMark: 65536, size: (chunk) => chunk?.byteLength ?? 0 },
      ),
      { headers: { "content-type": mime } },
    );
  };
  app.post("/openai/v1/audio/speech", async (c) => {
    const body = obj(await c.req.json());
    const scenario = scenarios.get(String(body.model)) ?? {};
    if (body.stream_format === "sse")
      return mediaEvents(String(body.model), [
        sse({ type: "speech.audio.delta", audio: btoa("mock-audio-chunk") }, "speech.audio.delta"),
        sse(
          { type: "speech.audio.done", usage: { input_tokens: 10, output_tokens: 5 } },
          "speech.audio.done",
        ),
      ]);
    if (scenario.binaryBytes) return binary(c.req.raw, scenario.binaryBytes, "audio/mpeg");
    return new Response("mock-generated-audio-content", {
      headers: { "content-type": "audio/mpeg" },
    });
  });
  for (const endpoint of ["transcriptions", "translations"])
    app.post(`/openai/v1/audio/${endpoint}`, async (c) => {
      const form = await c.req.formData();
      const format = form.get("response_format");
      if (form.get("stream") === "true")
        return mediaEvents(String(form.get("model")), [
          sse(
            { type: "transcript.text.delta", delta: "mock-transcribed-" },
            "transcript.text.delta",
          ),
          sse(
            {
              type: "transcript.text.done",
              text: "mock-transcribed-output-content",
              usage: { type: "duration", seconds: 0.125 },
            },
            "transcript.text.done",
          ),
        ]);
      if (["text", "srt", "vtt"].includes(String(format)))
        return new Response("mock-transcribed-output-content", {
          headers: { "content-type": "text/plain" },
        });
      return c.json({
        text: "mock-transcribed-output-content",
        usage: { input_tokens: 10, output_tokens: 5 },
      });
    });
  app.get("/openai/v1/responses/:id", (c) => {
    const resource = resources.get(c.req.param("id"));
    return resource?.payload
      ? c.json({
          ...resource.payload,
          ...(scenarios.get(String(resource.payload.model))?.pollUsage
            ? {
                usage: {
                  input_tokens: 10,
                  output_tokens: 5,
                  input_tokens_details: { cached_tokens: 3 },
                },
              }
            : {}),
          status: resource.cancelled ? "cancelled" : "completed",
        })
      : c.json({ error: "missing" }, 404);
  });
  app.get("/openai/v1/responses/:id/input_items", (c) =>
    resources.has(c.req.param("id"))
      ? c.json({ object: "list", data: [] })
      : c.json({ error: "missing" }, 404),
  );
  app.post("/openai/v1/responses/:id/cancel", (c) => {
    const resource = resources.get(c.req.param("id"));
    if (!resource) return c.json({ error: "missing" }, 404);
    resource.cancelled = true;
    return c.json({ id: resource.id, status: "cancelled" });
  });
  app.delete("/openai/v1/responses/:id", (c) =>
    c.json({ id: c.req.param("id"), deleted: resources.delete(c.req.param("id")) }),
  );
  app.post("/openai/v1/moderations", async (c) => {
    await c.req.json();
    return c.json({ id: "modr_1", results: [{ flagged: false }], usage: { input_tokens: 10 } });
  });
  app.post("/openai/v1/completions", async (c) => {
    await c.req.json();
    return c.json({
      id: "cmpl_1",
      choices: [{ text: "mock-completion-output-content", index: 0, finish_reason: "stop" }],
      usage: { prompt_tokens: 10, completion_tokens: 5 },
    });
  });
  for (const type of ["files", "batches", "videos"] as const) {
    const kind = type === "files" ? "file" : type === "batches" ? "batch" : "video";
    app.post(`/openai/v1/${type}`, async (c) => {
      if (resources.size >= 10000) return c.json({ error: "mock capacity" }, 503);
      let model: string | undefined;
      if (c.req.header("content-type")?.includes("multipart")) {
        const form = await c.req.formData();
        const file = form.get("file");
        if (file instanceof File && form.get("purpose") === "batch") {
          try {
            model = String(obj(obj(JSON.parse((await file.text()).split("\n")[0]!)).body).model);
          } catch {}
        }
      } else {
        const body = obj(await c.req.json());
        if (kind === "batch" && body.model !== undefined)
          return c.json({ error: "Batch has no top-level model parameter" }, 400);
        if (kind === "batch") model = resources.get(String(body.input_file_id))?.model;
      }
      const id = `${kind}-${crypto.randomUUID()}`;
      resources.set(id, {
        id,
        kind,
        polls: 0,
        output: `mock-${kind}-output-content`,
        cancelled: false,
        ...(model ? { model } : {}),
      });
      return c.json({
        id,
        object: kind,
        status: type === "files" ? "processed" : "queued",
        ...(kind === "video" ? { seconds: "8" } : {}),
      });
    });
    app.get(`/openai/v1/${type}/:id`, (c) => {
      const resource = resources.get(c.req.param("id"));
      if (!resource) return c.json({ error: "missing" }, 404);
      resource.polls++;
      if (kind === "batch") {
        const usages = scenarios.get(resource.model ?? "")?.batchRowUsage ?? [
          { prompt_tokens: 10, completion_tokens: 5, prompt_tokens_details: { cached_tokens: 0 } },
        ];
        const file = `file-result-${resource.id}`;
        resources.set(file, {
          id: file,
          kind: "file",
          polls: 0,
          output:
            usages
              .map((usage, index) =>
                JSON.stringify({
                  custom_id: `item-${index + 1}`,
                  response: {
                    status_code: 200,
                    body: {
                      choices: [{ message: { content: resource.output } }],
                      usage,
                    },
                  },
                }),
              )
              .join("\n") + "\n",
          cancelled: false,
        });
        return c.json({
          id: resource.id,
          object: kind,
          status: resource.cancelled ? "cancelled" : "completed",
          output_file_id: file,
          request_counts: { total: usages.length, completed: usages.length, failed: 0 },
        });
      }
      return c.json({
        id: resource.id,
        object: kind,
        status: resource.cancelled ? "cancelled" : "completed",
        ...(kind === "video" ? { seconds: "8", usage: { input_tokens: 0, output_tokens: 0 } } : {}),
      });
    });
    app.get(`/openai/v1/${type}/:id/content`, (c) => {
      const resource = resources.get(c.req.param("id"));
      return resource
        ? new Response(resource.output, {
            headers: {
              "content-type": kind === "video" ? "video/mp4" : "application/octet-stream",
            },
          })
        : c.json({ error: "missing" }, 404);
    });
    app.post(`/openai/v1/${type}/:id/cancel`, (c) => {
      const resource = resources.get(c.req.param("id"));
      if (!resource) return c.json({ error: "missing" }, 404);
      resource.cancelled = true;
      return c.json({ id: resource.id, status: "cancelled" });
    });
    app.delete(`/openai/v1/${type}/:id`, (c) => {
      const found = resources.delete(c.req.param("id"));
      return c.json({ id: c.req.param("id"), deleted: found });
    });
  }
  app.get("/gemini/v1beta/operations/:id", (c) => {
    const remote = `operations/${c.req.param("id")}`;
    const resource = resources.get(remote);
    if (!resource) return c.json({ error: "missing" }, 404);
    return c.json({
      name: remote,
      done: true,
      response: {
        generatedVideos: [{ video: { uri: `${new URL(c.req.url).origin}/mock-video-content` } }],
      },
      usageMetadata: { promptTokenCount: 0, candidatesTokenCount: 0 },
    });
  });
  app.get(
    "/mock-video-content",
    () => new Response("mock-video-output-content", { headers: { "content-type": "video/mp4" } }),
  );
  return { app, scenarios, stats };
}
