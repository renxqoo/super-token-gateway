import { Hono } from "hono";
import type { Context, MiddlewareHandler } from "hono";
import { z, ZodError } from "zod";
import {
  GatewayError,
  errorResponse,
  object,
  sha256,
  text,
  verifier,
  createKey,
  keySchema,
  parseClientJson,
  copyExact,
} from "@gateway/kernel";
import type { Operation } from "@gateway/kernel";
import { boundedRelay, readBounded, clientAddress, validateCidr, inCidrs } from "@gateway/runtime";
import type { Dependencies, Env } from "./context.ts";
import { openapi, resourceActions, inferencePaths } from "./openapi.ts";
import { adminRoutes } from "./admin.ts";
import { execute } from "./execution.ts";
import { parseBatchInput } from "./batch-input.ts";
import type { BatchInput } from "./batch-input.ts";
import {
  queryResource,
  rememberResource,
  resolveFileReferences,
  resourceKind,
  videoContent,
} from "./resources.ts";

const routes = inferencePaths as Record<string, Operation>;

export function createApp(deps: Dependencies): Hono<Env> {
  const app = new Hono<Env>();
  let httpInFlight = 0;
  app.use("*", async (c, next) => {
    c.set("requestId", crypto.randomUUID());
    c.header("x-request-id", c.get("requestId"));
    c.header("x-content-type-options", "nosniff");
    c.header("cache-control", "no-store");
    deps.metrics.increment("http_requests_total");
    const length = Number(c.req.header("content-length") ?? 0);
    if (length > deps.config.MAX_BODY_BYTES)
      throw new GatewayError(413, "body_too_large", "Request body exceeds limit");
    if (!deps.lifecycle.accepting && !c.req.path.startsWith("/livez"))
      throw new GatewayError(503, "draining", "Gateway is draining");
    if (httpInFlight >= deps.config.MAX_INFLIGHT)
      throw new GatewayError(503, "overloaded", "HTTP admission capacity reached");
    httpInFlight++;
    const started = performance.now();
    const memory = deps.memory.lease();
    c.set("reserveMemory", memory.update);
    let released = false;
    const release = () => {
      if (!released) {
        released = true;
        httpInFlight--;
        memory.close();
        deps.metrics.observeDuration((performance.now() - started) / 1000);
      }
    };
    try {
      await next();
      const response = c.res;
      if (response.body)
        c.res = new Response(
          boundedRelay(
            response.body,
            c.req.raw.signal,
            deps.config.DOWNSTREAM_STALL_MS,
            deps.config.TOTAL_TIMEOUT_MS + 30000,
            release,
          ),
          { status: response.status, statusText: response.statusText, headers: response.headers },
        );
      else release();
    } catch (error) {
      release();
      throw error;
    }
  });
  app.onError((error, c) => {
    deps.metrics.increment("http_errors_total");
    if (error instanceof ZodError || error instanceof SyntaxError)
      return errorResponse(
        new GatewayError(400, "invalid_request", "Request failed schema validation"),
        c.get("requestId"),
      );
    const pgError = error as Error & { code?: string; errno?: string };
    const code = pgError.errno ?? pgError.code;
    if (code && /^P04(01|02|03|04|09|22)$/.test(code))
      return errorResponse(
        new GatewayError(
          Number(code.slice(1)),
          "policy_rejected",
          "Command rejected by authorization or funds policy",
        ),
        c.get("requestId"),
      );
    if (code === "23505")
      return errorResponse(
        new GatewayError(409, "conflict", "Configuration identity already exists"),
        c.get("requestId"),
      );
    if (code === "23503" || code === "22P02")
      return errorResponse(
        new GatewayError(400, "invalid_reference", "Request contains an invalid identity"),
        c.get("requestId"),
      );
    if (code?.startsWith("08") || ["57P01", "57014", "53300"].includes(code ?? ""))
      return errorResponse(
        new GatewayError(503, "storage_unavailable", "Durable storage is unavailable"),
        c.get("requestId"),
      );
    return errorResponse(error, c.get("requestId"));
  });
  app.get("/livez", (c) => c.json({ status: "alive" }));
  app.get("/readyz", async (c) => {
    try {
      await deps.database.ping();
      await deps.usage.producer.ready();
      await deps.limits.command("PING");
      return c.json({ status: "ready" });
    } catch {
      throw new GatewayError(503, "not_ready", "Gateway dependencies are unavailable");
    }
  });
  app.route("/admin", adminRoutes(deps));
  app.get("/openapi.json", (c) => c.json(openapi()));

  const publicApi = new Hono<Env>();
  const authenticate: MiddlewareHandler<Env> = async (c, next) => {
    const auth = c.req.header("authorization");
    const token = auth?.startsWith("Bearer ")
      ? auth.slice(7)
      : (c.req.header("x-api-key") ?? c.req.header("x-goog-api-key") ?? "");
    if (!/^gw_[A-Za-z0-9_-]{43}$/.test(token))
      throw new GatewayError(401, "unauthenticated", "A valid project API key is required");
    const resolved = await deps.store.authenticate(verifier(token, deps.config.KEY_PEPPER));
    if (!resolved)
      throw new GatewayError(
        401,
        "unauthenticated",
        "Project API key is invalid, revoked or expired",
      );
    const address = clientAddress(
      c.env?.peerAddress,
      c.req.raw.headers.get("x-forwarded-for"),
      deps.config.TRUSTED_PROXY_CIDRS,
    );
    if (
      resolved.credential.allowed_cidrs?.length &&
      (!address || !inCidrs(address, resolved.credential.allowed_cidrs))
    )
      throw new GatewayError(403, "ip_denied", "Credential does not permit this client address");
    if (
      !["GET", "HEAD"].includes(c.req.method) &&
      !["ADMIN", "INVOKER"].includes(resolved.credential.role ?? "INVOKER")
    )
      throw new GatewayError(
        403,
        "role_denied",
        "Read-only credential cannot modify resources or invoke models",
      );
    if (
      resolved.credential.role === "BILLING" &&
      !/^\/v1\/(wallets|usage|requests|models)(\/|$)/.test(c.req.path)
    )
      throw new GatewayError(
        403,
        "role_denied",
        "Billing credentials may access accounting metadata only",
      );
    c.set("auth", resolved);
    await next();
  };
  publicApi.use("*", authenticate);
  const requireRole = (c: Context<Env>, roles: string[]) => {
    if (!roles.includes(c.get("auth").credential.role ?? "INVOKER"))
      throw new GatewayError(403, "role_denied", "Credential role does not permit this operation");
  };
  publicApi.get("/project/keys", async (c) => {
    requireRole(c, ["ADMIN"]);
    return c.json({ data: await deps.store.keys(c.get("auth").project.id) });
  });
  publicApi.post("/project/keys", async (c) => {
    requireRole(c, ["ADMIN"]);
    const payload: unknown = JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(
        await readBounded(
          c.req.raw.body,
          65536,
          AbortSignal.any([c.req.raw.signal, AbortSignal.timeout(deps.config.HEADERS_TIMEOUT_MS)]),
          c.get("reserveMemory"),
        ),
      ),
    );
    const input = keySchema.parse(payload);
    input.allowed_cidrs = input.allowed_cidrs.map(validateCidr);
    const key = createKey();
    const delegated = await deps.store.delegateCredential(
      c.get("auth").credential.id,
      input,
      Object.hasOwn(payload as object, "operations"),
      verifier(key, deps.config.KEY_PEPPER),
      key.slice(0, 12),
    );
    return c.json({ id: delegated.id, key, ...delegated.input }, 201);
  });
  publicApi.delete("/project/keys/:id", async (c) => {
    requireRole(c, ["ADMIN"]);
    await deps.store.revokeProjectKey(z.uuid().parse(c.req.param("id")), c.get("auth").project.id);
    return c.json({ revoked: true });
  });
  publicApi.get("/wallets", async (c) => {
    requireRole(c, ["ADMIN", "BILLING"]);
    return c.json({ data: await deps.funds.wallets(c.get("auth").project.id) });
  });
  publicApi.get("/requests/:id/billing", async (c) => {
    requireRole(c, ["ADMIN", "BILLING"]);
    const auth = c.get("auth");
    const requestId = z.uuid().parse(c.req.param("id"));
    await deps.store.request(
      requestId,
      auth.project.id,
      auth.credential.models,
      auth.credential.operations,
    );
    return c.json(await deps.funds.receipt(requestId, auth.project.id));
  });
  publicApi.get("/models", async (c) =>
    c.json({
      object: "list",
      data: (await deps.store.models(c.get("auth").credential.models)).map((model) => ({
        id: model.name,
        object: "model",
        created: 0,
        owned_by: "gateway",
        capabilities: model.operations,
      })),
    }),
  );
  publicApi.get("/usage", async (c) =>
    c.json({
      data: await deps.store.usage(
        c.get("auth").project.id,
        100,
        c.req.query("before"),
        c.get("auth").credential.models,
        c.get("auth").credential.operations,
      ),
    }),
  );
  publicApi.get("/requests/:id", async (c) =>
    c.json(
      await deps.store.request(
        c.req.param("id"),
        c.get("auth").project.id,
        c.get("auth").credential.models,
        c.get("auth").credential.operations,
      ),
    ),
  );

  for (const kind of ["files", "batches", "videos", "responses"] as const) {
    const singular =
      kind === "files"
        ? "file"
        : kind === "batches"
          ? "batch"
          : kind === "responses"
            ? "response"
            : "video";
    publicApi.get(`/${kind}`, async (c) => {
      const auth = c.get("auth");
      if (!auth.credential.operations.includes(kind))
        throw new GatewayError(403, "scope_denied", "Operation not permitted");
      const limit = z.coerce
        .number()
        .int()
        .min(1)
        .max(100)
        .parse(c.req.query("limit") ?? 100);
      const page = await deps.store.resources(
        auth.project.id,
        singular,
        auth.credential.models,
        limit,
        c.req.query("after"),
      );
      const rows = page.slice(0, limit);
      return c.json({
        object: "list",
        has_more: page.length > limit,
        first_id: rows[0]?.id ?? null,
        last_id: rows.at(-1)?.id ?? null,
        data: rows.map((row) => ({
          id: row.id,
          object: singular,
          status: row.state,
          created_at: Math.floor(row.created_at.getTime() / 1000),
        })),
      });
    });
    for (const action of resourceActions[kind]) {
      if (
        (action === "input_items" && kind !== "responses") ||
        (action === "content" && kind === "responses")
      )
        continue;
      const path = `/${kind}/:id${action === "cancel" ? "/cancel" : action === "content" ? "/content" : action === "input_items" ? "/input_items" : ""}`;
      const handler = async (c: Context<Env>) => {
        const auth = c.get("auth");
        if (!auth.credential.operations.includes(kind))
          throw new GatewayError(403, "scope_denied", "Operation not permitted");
        const resource = await deps.store.resource(
          c.req.param("id") ?? "",
          auth.project.id,
          singular,
        );
        if (
          auth.credential.models.length &&
          (!resource.model || !auth.credential.models.includes(resource.model))
        )
          throw new GatewayError(404, "not_found", "Resource not found");
        const lifecycle = deps.lifecycle.enter();
        const releaseProject = await deps.limits
          .enter(
            `project:${auth.project.id}`,
            c.get("requestId"),
            auth.project.rpm,
            auth.project.concurrency,
            deps.config.TOTAL_TIMEOUT_MS + 10000,
          )
          .catch((error: unknown) => {
            lifecycle.release();
            throw error;
          });
        let releaseProvider: (() => Promise<void>) | undefined;
        const cleanup = async () => {
          await releaseProvider?.();
          await releaseProject();
          lifecycle.release();
        };
        const signal = AbortSignal.any([
          c.req.raw.signal,
          lifecycle.controller.signal,
          AbortSignal.timeout(deps.config.TOTAL_TIMEOUT_MS),
        ]);
        try {
          const provider = await deps.store.provider(resource.provider_id);
          releaseProvider = await deps.limits.enter(
            `provider:${provider.provider_id}`,
            c.get("requestId"),
            provider.rpm,
            provider.concurrency,
            deps.config.TOTAL_TIMEOUT_MS + 10000,
          );
          const response = await (action === "content" && singular === "video"
            ? videoContent(deps, resource, signal, c.get("reserveMemory"))
            : queryResource(deps, resource, action, signal, c.get("reserveMemory")));
          if (response.body)
            return new Response(
              boundedRelay(
                response.body,
                signal,
                deps.config.DOWNSTREAM_STALL_MS,
                deps.config.TOTAL_TIMEOUT_MS,
                cleanup,
              ),
              { status: response.status, headers: response.headers },
            );
          await cleanup();
          return response;
        } catch (error) {
          await cleanup();
          throw error;
        }
      };
      if (action === "delete") publicApi.delete(path, handler);
      else if (action === "cancel") publicApi.post(path, handler);
      else publicApi.get(path, handler);
    }
  }
  for (const [path, operation] of Object.entries(routes))
    publicApi.post(path.slice(3), async (c) => {
      const bytes = await readBounded(
        c.req.raw.body,
        deps.config.MAX_BODY_BYTES,
        AbortSignal.any([c.req.raw.signal, AbortSignal.timeout(deps.config.HEADERS_TIMEOUT_MS)]),
        c.get("reserveMemory"),
      );
      let body: Record<string, unknown>;
      let form: FormData | undefined;
      if (c.req.header("content-type")?.includes("multipart/form-data")) {
        form = await new Request("http://serialization.invalid", {
          method: "POST",
          headers: { "content-type": c.req.header("content-type") ?? "" },
          body: Buffer.from(bytes),
        }).formData();
        body = {};
        for (const key of new Set(form.keys())) {
          const value = form.get(key);
          body[key] =
            typeof value === "string"
              ? value
              : value
                ? {
                    digest: sha256(new Uint8Array(await value.arrayBuffer())),
                    size: value.size,
                    type: value.type,
                  }
                : null;
        }
      } else
        body = object(parseClientJson(new TextDecoder("utf-8", { fatal: true }).decode(bytes)));
      if (form && (form.get("stream") === "true" || form.get("stream") === "false"))
        body.stream = form.get("stream") === "true";
      const auth = c.get("auth");
      let model = text(body.model);
      let batchInput: BatchInput | undefined;
      let pinnedUpstreamModel: string | undefined;
      if (operation === "files" && form?.get("purpose") === "batch") {
        const file = form.get("file");
        if (!file || typeof file === "string")
          throw new GatewayError(400, "invalid_batch", "Batch file is required");
        batchInput = await parseBatchInput(file, auth);
        if (model && model !== batchInput.model)
          throw new GatewayError(400, "invalid_batch", "Upload model must match batch input model");
        model = batchInput.model;
      }
      if (operation === "batches") {
        const file = await deps.store.resource(text(body.input_file_id), auth.project.id, "file");
        if (!file.batch_endpoint || body.endpoint !== file.batch_endpoint || !file.model)
          throw new GatewayError(
            400,
            "invalid_batch",
            "Batch requires a validated input file with a matching endpoint",
          );
        if (model && model !== file.model)
          throw new GatewayError(400, "invalid_batch", "Batch model differs from input file");
        model = file.model;
        pinnedUpstreamModel = file.batch_upstream_model ?? undefined;
      }
      if (operation === "responses" && !model && typeof body.previous_response_id === "string")
        model =
          (await deps.store.resource(body.previous_response_id, auth.project.id, "response"))
            .model ?? "";
      if (!model && ["files", "batches"].includes(operation))
        model =
          (await deps.store.models(auth.credential.models)).find((m) =>
            m.operations.includes(operation),
          )?.name ?? "";
      if (!model) throw new GatewayError(400, "model_required", "A configured model is required");
      if (body.stream !== undefined && typeof body.stream !== "boolean")
        throw new GatewayError(400, "invalid_request", "stream must be boolean");
      if (
        body.stream === true &&
        ![
          "chat",
          "responses",
          "messages",
          "gemini",
          "images",
          "image_edits",
          "transcription",
          "speech",
        ].includes(operation)
      )
        throw new GatewayError(
          422,
          "unsupported_stream",
          "This operation currently requires non-streaming provider output",
        );
      const resolved = await resolveFileReferences(
        deps,
        auth.project.id,
        body,
        auth.credential.models,
      );
      const kind = resourceKind(operation);
      return execute(deps, {
        auth,
        request: c.req.raw,
        requestId: c.get("requestId"),
        operation,
        model,
        body: resolved.body,
        reserveMemory: c.get("reserveMemory"),
        ...(form ? { form } : {}),
        ...(batchInput ? { batchInput } : {}),
        ...(pinnedUpstreamModel ? { pinnedUpstreamModel } : {}),
        ...(resolved.providerId ? { pinnedProviderId: resolved.providerId } : {}),
        ...(kind
          ? {
              transformResponse: (value, route, attemptId, batch) =>
                kind === "response" && route.kind !== "openai"
                  ? Promise.resolve(value)
                  : rememberResource(deps, auth, kind, model, value, route, attemptId, batch),
            }
          : {}),
      });
    });
  app.route("/v1", publicApi);

  const gemini = new Hono<Env>();
  gemini.use("*", authenticate);
  gemini.get("/models", async (c) =>
    c.json({
      models: (await deps.store.models(c.get("auth").credential.models))
        .filter((m) => m.operations.includes("gemini"))
        .map((m) => ({
          name: `models/${m.name}`,
          supportedGenerationMethods: ["generateContent", "streamGenerateContent"],
        })),
    }),
  );
  gemini.post("/models/:action", async (c) => {
    const match = /^(.*):(generateContent|streamGenerateContent)$/.exec(c.req.param("action"));
    if (!match?.[1]) throw new GatewayError(404, "not_found", "Unknown Gemini operation");
    const bytes = await readBounded(
      c.req.raw.body,
      deps.config.MAX_BODY_BYTES,
      AbortSignal.any([c.req.raw.signal, AbortSignal.timeout(deps.config.HEADERS_TIMEOUT_MS)]),
      c.get("reserveMemory"),
    );
    const parsed = object(parseClientJson(new TextDecoder("utf-8", { fatal: true }).decode(bytes)));
    const body = copyExact(parsed, {
      ...parsed,
      model: match[1],
      stream: match[2] === "streamGenerateContent",
    });
    const resolved = await resolveFileReferences(
      deps,
      c.get("auth").project.id,
      body,
      c.get("auth").credential.models,
    );
    return execute(deps, {
      auth: c.get("auth"),
      request: c.req.raw,
      requestId: c.get("requestId"),
      reserveMemory: c.get("reserveMemory"),
      operation: "gemini",
      model: match[1],
      body: resolved.body,
      ...(resolved.providerId ? { pinnedProviderId: resolved.providerId } : {}),
    });
  });
  app.route("/v1beta", gemini);
  app.notFound((c) =>
    errorResponse(new GatewayError(404, "not_found", "Endpoint not found"), c.get("requestId")),
  );
  return app;
}
