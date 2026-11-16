import { Hono, type Context } from "hono";
import type { Operation } from "@gateway/kernel";
import { GatewayError } from "@gateway/contracts";
import { PluginHostError } from "@gateway/plugin-host";
import { clientAddress, readBounded, type Lifecycle } from "@gateway/runtime";
import { executeChat, type ChatDependencies } from "./chat.ts";
import { executeResource, listResources, type ResourceRequest } from "./resources.ts";

export type GatewayEnv = { Bindings: { peerAddress?: string } };

const pluginCapacity = new Set([
  "ATTEMPT_BUDGET",
  "PENDING_BUDGET",
  "QUEUE_TIMEOUT",
  "STATE_BUDGET",
]);

function errorResponse(error: unknown, requestId: string) {
  const known = error instanceof GatewayError;
  return Response.json(
    {
      error: {
        type: known ? error.code : "internal_error",
        code: known ? error.code : "internal_error",
        message: known ? error.message : "The gateway could not complete this request",
      },
      request_id: requestId,
    },
    { status: known ? error.status : 500, headers: { "x-request-id": requestId } },
  );
}

function released(response: Response, release: () => void) {
  if (!response.body) {
    release();
    return response;
  }
  const reader = response.body.getReader();
  let done = false;
  const finish = () => {
    if (!done) {
      done = true;
      release();
    }
  };
  return new Response(
    new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          const next = await reader.read();
          if (next.done) {
            finish();
            controller.close();
          } else controller.enqueue(next.value);
        } catch (error) {
          finish();
          controller.error(error);
        }
      },
      async cancel(reason) {
        try {
          await reader.cancel(reason);
        } finally {
          finish();
        }
      },
    }),
    { status: response.status, headers: response.headers },
  );
}

export function createGatewayApp(
  deps: ChatDependencies & { lifecycle: Lifecycle; ready: () => Promise<void> },
) {
  const app = new Hono<GatewayEnv>();
  app.get("/livez", (c) => c.json({ status: "alive" }));
  app.get("/readyz", async (c) => {
    if (!deps.lifecycle.accepting) return c.json({ status: "draining" }, 503);
    try {
      await deps.ready();
      return c.json({ status: "ready" });
    } catch {
      return c.json({ status: "unavailable" }, 503);
    }
  });
  type Inbound = ResourceRequest & {
    body: Uint8Array;
    contentType: string | undefined;
    idempotencyKey: string | undefined;
    drain: AbortSignal;
  };
  function serve(
    c: Context<GatewayEnv>,
    operation: Operation,
    path?: { model: string; stream: boolean },
  ) {
    return handle(c, (inbound) =>
      executeChat(deps, { ...inbound, operation, ...(path ? { path } : {}) }),
    );
  }
  async function handle(c: Context<GatewayEnv>, run: (inbound: Inbound) => Promise<Response>) {
    const requestId = crypto.randomUUID();
    let release: (() => void) | undefined;
    try {
      const authorization = c.req.header("authorization");
      const token = authorization?.startsWith("Bearer ")
        ? authorization.slice(7)
        : (c.req.header("x-api-key") ?? c.req.header("x-goog-api-key") ?? "");
      if (!/^gw_[A-Za-z0-9_-]{43}$/.test(token))
        throw new GatewayError(401, "unauthenticated", "A valid project API key is required");
      const declared = Number(c.req.header("content-length") ?? 0);
      if (declared > deps.config.max_body_bytes)
        throw new GatewayError(413, "body_too_large", "Request body exceeds limit");
      const ip = clientAddress(
        c.env.peerAddress,
        c.req.header("x-forwarded-for") ?? null,
        deps.config.trusted_proxy_cidrs,
      );
      if (!ip) throw new GatewayError(400, "invalid_client_address", "Client address unavailable");
      const entered = deps.lifecycle.enter();
      release = entered.release;
      const signal = AbortSignal.any([c.req.raw.signal, entered.controller.signal]);
      const body = await readBounded(c.req.raw.body, deps.config.max_body_bytes, signal);
      const response = await deps.lifecycle.track(
        run({
          requestId,
          body,
          contentType: c.req.header("content-type"),
          token,
          ip,
          idempotencyKey: c.req.header("idempotency-key"),
          signal: c.req.raw.signal,
          drain: entered.controller.signal,
        }),
      );
      const settled = release;
      release = undefined;
      return released(response, settled);
    } catch (error) {
      release?.();
      return errorResponse(
        (error instanceof GatewayError && error.code === "overloaded") ||
          (error instanceof PluginHostError && pluginCapacity.has(error.code))
          ? new GatewayError(
              503,
              deps.lifecycle.accepting ? "overloaded" : "draining",
              "Gateway admission capacity reached",
            )
          : error,
        requestId,
      );
    }
  }
  app.post("/v1/chat/completions", (c) => serve(c, "chat"));
  app.post("/v1/messages", (c) => serve(c, "messages"));
  app.post("/v1/responses", (c) => serve(c, "responses"));
  app.post("/v1/files", (c) => serve(c, "files"));
  app.post("/v1/batches", (c) => serve(c, "batches"));
  app.post("/v1/videos", (c) => serve(c, "videos"));
  app.get("/v1/videos", (c) =>
    handle(c, (inbound) =>
      listResources(deps, inbound, "video", {
        limit: c.req.query("limit"),
        after: c.req.query("after"),
      }),
    ),
  );
  app.get("/v1/videos/:id", (c) =>
    handle(c, (inbound) => executeResource(deps, inbound, c.req.param("id"), "get")),
  );
  app.delete("/v1/videos/:id", (c) =>
    handle(c, (inbound) => executeResource(deps, inbound, c.req.param("id"), "delete")),
  );
  app.get("/v1/videos/:id/content", (c) =>
    handle(c, (inbound) => executeResource(deps, inbound, c.req.param("id"), "content")),
  );
  app.get("/v1/batches", (c) =>
    handle(c, (inbound) =>
      listResources(deps, inbound, "batch", {
        limit: c.req.query("limit"),
        after: c.req.query("after"),
      }),
    ),
  );
  app.get("/v1/batches/:id", (c) =>
    handle(c, (inbound) => executeResource(deps, inbound, c.req.param("id"), "get")),
  );
  app.post("/v1/batches/:id/cancel", (c) =>
    handle(c, (inbound) => executeResource(deps, inbound, c.req.param("id"), "cancel")),
  );
  app.get("/v1/files", (c) =>
    handle(c, (inbound) =>
      listResources(deps, inbound, "file", {
        limit: c.req.query("limit"),
        after: c.req.query("after"),
      }),
    ),
  );
  app.get("/v1/files/:id", (c) =>
    handle(c, (inbound) => executeResource(deps, inbound, c.req.param("id"), "get")),
  );
  app.delete("/v1/files/:id", (c) =>
    handle(c, (inbound) => executeResource(deps, inbound, c.req.param("id"), "delete")),
  );
  app.get("/v1/files/:id/content", (c) =>
    handle(c, (inbound) => executeResource(deps, inbound, c.req.param("id"), "content")),
  );
  app.get("/v1/responses", (c) =>
    handle(c, (inbound) =>
      listResources(deps, inbound, "response", {
        limit: c.req.query("limit"),
        after: c.req.query("after"),
      }),
    ),
  );
  app.get("/v1/responses/:id", (c) =>
    handle(c, (inbound) => executeResource(deps, inbound, c.req.param("id"), "get")),
  );
  app.delete("/v1/responses/:id", (c) =>
    handle(c, (inbound) => executeResource(deps, inbound, c.req.param("id"), "delete")),
  );
  app.post("/v1/responses/:id/cancel", (c) =>
    handle(c, (inbound) => executeResource(deps, inbound, c.req.param("id"), "cancel")),
  );
  app.get("/v1/responses/:id/input_items", (c) =>
    handle(c, (inbound) => executeResource(deps, inbound, c.req.param("id"), "input_items")),
  );
  app.post("/v1/embeddings", (c) => serve(c, "embeddings"));
  app.post("/v1/moderations", (c) => serve(c, "moderations"));
  app.post("/v1/completions", (c) => serve(c, "completions"));
  app.post("/v1/images/generations", (c) => serve(c, "images"));
  app.post("/v1/images/edits", (c) => serve(c, "image_edits"));
  app.post("/v1/audio/speech", (c) => serve(c, "speech"));
  app.post("/v1/audio/transcriptions", (c) => serve(c, "transcription"));
  app.post("/v1/audio/translations", (c) => serve(c, "translation"));
  app.post("/v1/music/generations", (c) => serve(c, "music"));
  app.post("/v1beta/models/:action", (c) => {
    const match = /^([A-Za-z0-9._-]{1,200}):(generateContent|streamGenerateContent)$/.exec(
      c.req.param("action"),
    );
    if (!match)
      return errorResponse(
        new GatewayError(404, "not_found", "Unknown Gemini operation"),
        crypto.randomUUID(),
      );
    return serve(c, "gemini", {
      model: match[1]!,
      stream: match[2] === "streamGenerateContent",
    });
  });
  app.notFound(() =>
    errorResponse(new GatewayError(404, "not_found", "Unknown endpoint"), crypto.randomUUID()),
  );
  return app;
}
