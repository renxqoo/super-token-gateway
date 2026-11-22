import { Hono } from "hono";
import { registerAzure } from "./providers/azure.ts";
import { registerVertex } from "./providers/vertex.ts";
import { registerBedrock } from "./providers/bedrock.ts";
import { registerCompatibleChat } from "./providers/compatible.ts";
import {
  createConversationFixture,
  type FixtureResource,
  type MockStats,
  type Scenario,
} from "./providers/conversation.ts";

export type { Scenario } from "./providers/conversation.ts";
import { obj, sse } from "./providers/http.ts";

export function createMockApp(controlToken: string): {
  app: Hono;
  stats: MockStats;
  scenarios: Map<string, Scenario>;
} {
  const app = new Hono();
  app.all("/v1/*", (c) => {
    const url = new URL(c.req.url);
    url.pathname = `/openai${url.pathname}`;
    return app.fetch(new Request(url, c.req.raw));
  });
  registerAzure(app);
  registerVertex(app);
  registerBedrock(app);
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
  app.post("/__control/mixed-policy", async (c) => {
    const body = obj(await c.req.json());
    if (body.hook !== "on_stream_end" || typeof body.text !== "string")
      return c.json({ error: "Expected final stream text" }, 400);
    return body.text.includes("alice@example.com")
      ? c.json({
          action: "TRANSFORM",
          text: body.text.replaceAll("alice@example.com", "[REDACTED_BY_WEBHOOK]"),
        })
      : c.json({ action: "PASS" });
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

  const generate = createConversationFixture(scenarios, attempts, resources, stats);

  registerCompatibleChat(app, (request, variant) =>
    generate(request, "openai", undefined, variant),
  );
  app.post("/ollama/v1/responses", (c) => generate(c.req.raw, "responses"));
  app.post("/openai/v1/chat/completions", (c) => generate(c.req.raw, "openai"));
  app.post("/openai/v1/responses", (c) => generate(c.req.raw, "responses"));
  app.post("/anthropic/v1/messages", async (c) => {
    if (!c.req.header("x-api-key") || c.req.header("anthropic-version") !== "2023-06-01") {
      return c.json(
        { error: { type: "authentication_error", message: "Expected Claude API key and version" } },
        401,
      );
    }
    return generate(c.req.raw, "anthropic");
  });
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
    if (
      !c.req.header("x-goog-api-key") &&
      c.req.header("authorization") !== "Bearer fixture-vertex"
    ) {
      return c.json({ error: { message: "Expected Google API key or access token" } }, 401);
    }
    if (!action.endsWith(":generateContent") && !action.endsWith(":streamGenerateContent")) {
      return c.json({ error: { message: "Unknown generateContent operation" } }, 404);
    }
    if (action.endsWith(":streamGenerateContent") && c.req.query("alt") !== "sse") {
      return c.json({ error: { message: "Expected alt=sse for streaming" } }, 400);
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
        data: scenarios.get(String(body.model))?.imageData ?? [
          { b64_json: btoa("mock-generated-image-content") },
        ],
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
      let batchIds: string[] | undefined;
      let batchContent: string | undefined;
      if (c.req.header("content-type")?.includes("multipart")) {
        const form = await c.req.formData();
        const file = form.get("file");
        if (file instanceof File && form.get("purpose") === "batch") {
          try {
            batchContent = await file.text();
            const items = batchContent
              .split("\n")
              .filter((line) => line.trim())
              .map((line) => obj(JSON.parse(line)));
            model = String(obj(items[0]?.body).model);
            batchIds = items.map((item) => String(item.custom_id));
          } catch {}
        }
      } else {
        const body = obj(await c.req.json());
        if (kind === "batch" && body.model !== undefined)
          return c.json({ error: "Batch has no top-level model parameter" }, 400);
        if (kind === "batch") {
          const input = resources.get(String(body.input_file_id));
          model = input?.model;
          batchIds = input?.batchIds;
        }
      }
      const id = `${kind}-${crypto.randomUUID()}`;
      resources.set(id, {
        id,
        kind,
        polls: 0,
        output: batchContent ?? `mock-${kind}-output-content`,
        cancelled: false,
        ...(model ? { model } : {}),
        ...(batchIds ? { batchIds } : {}),
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
        const scenario = scenarios.get(resource.model ?? "");
        const usages =
          scenario?.batchRowUsage ??
          Array.from({ length: resource.batchIds?.length ?? 1 }, () => ({
            prompt_tokens: 10,
            completion_tokens: 5,
            prompt_tokens_details: { cached_tokens: 0 },
          }));
        const file = `file-result-${resource.id}`;
        resources.set(file, {
          id: file,
          kind: "file",
          polls: 0,
          output:
            usages
              .map((usage, index) =>
                JSON.stringify({
                  custom_id:
                    scenario?.batchRowIds?.[index] ??
                    resource.batchIds?.[index] ??
                    `item-${index + 1}`,
                  response: {
                    status_code: 200,
                    body: {
                      ...(scenario?.imageData === undefined
                        ? { choices: [{ message: { content: resource.output } }] }
                        : { data: scenario.imageData }),
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
          ...(scenario?.batchUsage ? { usage: scenario.batchUsage } : {}),
          request_counts: {
            total: scenario?.batchReportedTotal ?? resource.batchIds?.length ?? usages.length,
            completed: usages.length,
            failed: 0,
          },
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
