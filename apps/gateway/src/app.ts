import { Hono } from "hono";
import { GatewayError } from "@gateway/contracts";
import { clientAddress, readBounded, type Lifecycle } from "@gateway/runtime";
import { executeChat, type ChatDependencies } from "./chat.ts";

export type GatewayEnv = { Bindings: { peerAddress?: string } };

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
  app.post("/v1/chat/completions", async (c) => {
    const requestId = crypto.randomUUID();
    let release: (() => void) | undefined;
    try {
      const authorization = c.req.header("authorization");
      const token = authorization?.startsWith("Bearer ")
        ? authorization.slice(7)
        : (c.req.header("x-api-key") ?? "");
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
        executeChat(deps, {
          requestId,
          body,
          token,
          ip,
          idempotencyKey: c.req.header("idempotency-key"),
          signal,
        }),
      );
      const settled = release;
      release = undefined;
      return released(response, settled);
    } catch (error) {
      release?.();
      return errorResponse(
        error instanceof GatewayError && error.code === "overloaded"
          ? new GatewayError(
              503,
              deps.lifecycle.accepting ? "overloaded" : "draining",
              error.message,
            )
          : error,
        requestId,
      );
    }
  });
  app.notFound(() =>
    errorResponse(new GatewayError(404, "not_found", "Unknown endpoint"), crypto.randomUUID()),
  );
  return app;
}
