import { recoverStale } from "./recovery.ts";
import { Hono } from "hono";
import type { Context } from "hono";
import { z } from "zod";
import {
  GatewayError,
  keyPatchSchema,
  keySchema,
  modelPatchSchema,
  modelSchema,
  organizationPatchSchema,
  organizationSchema,
  principalPatchSchema,
  principalSchema,
  projectPatchSchema,
  projectSchema,
  routePatchSchema,
  providerSchema,
  routeSchema,
} from "@gateway/contracts";
import { createKey, encrypt, secureEqual, verifier } from "@gateway/runtime";
import {
  readBounded,
  validateBaseUrl,
  validateCidr,
  validateCloudCredentials,
} from "@gateway/runtime";
import type { Dependencies, Env } from "./context.ts";

const id = (value: string) => z.uuid().parse(value);
export function adminRoutes(deps: Dependencies): Hono<Env> {
  const app = new Hono<Env>();
  const body = async (c: Context<Env>): Promise<unknown> =>
    JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(
        await readBounded(
          c.req.raw.body,
          Math.min(65536, deps.config.MAX_BODY_BYTES),
          AbortSignal.any([c.req.raw.signal, AbortSignal.timeout(deps.config.HEADERS_TIMEOUT_MS)]),
          c.get("reserveMemory"),
        ),
      ),
    );
  app.use("*", async (c, next) => {
    const token = c.req.header("authorization")?.replace(/^Bearer /, "") ?? "";
    if (!secureEqual(token, deps.config.ADMIN_KEY))
      throw new GatewayError(401, "unauthenticated", "Admin credential required");
    await next();
  });
  app.post("/organizations", async (c) => {
    const key = await deps.store.createOrganization(organizationSchema.parse(await body(c)));
    return c.json({ id: key }, 201);
  });
  app.patch("/organizations/:id", async (c) => {
    await deps.store.updateOrganization(
      id(c.req.param("id")),
      organizationPatchSchema.parse(await body(c)),
    );
    return c.json({ updated: true });
  });
  app.post("/organizations/:id/principals", async (c) => {
    const key = await deps.store.createPrincipal(
      id(c.req.param("id")),
      principalSchema.parse(await body(c)),
    );
    return c.json({ id: key }, 201);
  });
  app.patch("/principals/:id", async (c) => {
    await deps.store.updatePrincipal(
      id(c.req.param("id")),
      principalPatchSchema.parse(await body(c)),
    );
    return c.json({ updated: true });
  });
  app.get("/projects", async (c) => c.json({ data: await deps.store.projects() }));
  app.post("/projects", async (c) => {
    const input = projectSchema.parse(await body(c));
    const projectId = await deps.store.createProject(input);
    return c.json({ id: projectId, ...input }, 201);
  });
  app.patch("/projects/:id", async (c) => {
    await deps.store.updateProject(id(c.req.param("id")), projectPatchSchema.parse(await body(c)));
    return c.json({ updated: true });
  });
  app.get("/projects/:id/keys", async (c) =>
    c.json({ data: await deps.store.keys(id(c.req.param("id"))) }),
  );
  app.post("/projects/:id/keys", async (c) => {
    const input = keySchema.parse(await body(c));
    input.allowed_cidrs = input.allowed_cidrs.map(validateCidr);
    const key = createKey();
    const keyId = await deps.store.createCredential(
      id(c.req.param("id")),
      input,
      verifier(key, deps.config.KEY_PEPPER),
      key.slice(0, 12),
    );
    return c.json({ id: keyId, key, ...input }, 201);
  });
  app.delete("/keys/:id", async (c) => {
    await deps.store.revokeKey(id(c.req.param("id")));
    return c.json({ revoked: true });
  });
  app.get("/providers", async (c) => c.json({ data: await deps.store.providers() }));
  app.post("/providers", async (c) => {
    const input = providerSchema.parse(await body(c));
    validateBaseUrl(
      input.base_url,
      deps.config.ALLOW_PRIVATE_UPSTREAM,
      deps.config.PRIVATE_UPSTREAM_CIDRS,
    );
    validateCloudCredentials(input.kind, input.api_key);
    const providerId = crypto.randomUUID();
    const provider = await deps.store.createProvider(
      providerId,
      input,
      encrypt(input.api_key, deps.config.ENCRYPTION_KEY, providerId),
    );
    return c.json(provider, 201);
  });
  app.patch("/providers/:id", async (c) => {
    const input = z
      .object({
        enabled: z.boolean().optional(),
        rpm: z.number().int().min(1).max(10_000_000).optional(),
        concurrency: z.number().int().min(1).max(100_000).optional(),
        api_key: z.string().min(1).max(16384).optional(),
      })
      .strict()
      .parse(await body(c));
    const providerId = id(c.req.param("id"));
    if (input.api_key)
      validateCloudCredentials((await deps.store.provider(providerId)).kind, input.api_key);
    await deps.store.updateProvider(
      providerId,
      input,
      input.api_key ? encrypt(input.api_key, deps.config.ENCRYPTION_KEY, providerId) : undefined,
    );
    return c.json({ updated: true });
  });
  app.get("/models", async (c) => c.json({ data: await deps.store.adminModels() }));
  app.post("/models", async (c) => {
    const input = modelSchema.parse(await body(c));
    await deps.store.createModel(input);
    return c.json(input, 201);
  });
  app.post("/models/:model/routes", async (c) => {
    const input = routeSchema.parse(await body(c));
    const routeId = await deps.store.createRoute(c.req.param("model"), input);
    return c.json({ id: routeId }, 201);
  });
  app.patch("/models/:model", async (c) => {
    await deps.store.updateModel(c.req.param("model"), modelPatchSchema.parse(await body(c)));
    return c.json({ updated: true });
  });
  app.get("/models/:model/routes", async (c) =>
    c.json({ data: await deps.store.routeDefinitions(c.req.param("model")) }),
  );
  app.patch("/routes/:id", async (c) => {
    const input = routePatchSchema.parse(await body(c));
    await deps.store.updateRoute(id(c.req.param("id")), input);
    return c.json({ updated: true });
  });
  app.patch("/keys/:id", async (c) => {
    const input = keyPatchSchema.parse(await body(c));
    if (input.allowed_cidrs) input.allowed_cidrs = input.allowed_cidrs.map(validateCidr);
    await deps.store.updateCredential(id(c.req.param("id")), input);
    return c.json({ updated: true });
  });
  app.get("/usage", async (c) =>
    c.json({
      data: await deps.store.usage(
        c.req.query("project_id") ? id(c.req.query("project_id") ?? "") : null,
        100,
        c.req.query("before"),
      ),
    }),
  );
  app.post("/consumers", async (c) => {
    const input = z
      .object({ name: z.string().min(1).max(100), replay: z.boolean().default(true) })
      .strict()
      .parse(await body(c));
    return c.json({ id: await deps.store.createConsumer(input.name, input.replay) }, 201);
  });
  app.post("/consumers/:id/claim", async (c) => {
    const input = z
      .object({ limit: z.number().int().min(1).max(500).default(100) })
      .strict()
      .parse(await body(c));
    return c.json(await deps.store.claimEvents(id(c.req.param("id")), input.limit));
  });
  app.post("/consumers/:id/ack", async (c) => {
    const input = z
      .object({ lease_token: z.uuid(), event_ids: z.array(z.uuid()).min(1).max(500) })
      .strict()
      .parse(await body(c));
    return c.json({
      acknowledged: await deps.store.ackEvents(
        id(c.req.param("id")),
        input.lease_token,
        input.event_ids,
      ),
    });
  });
  app.post("/recover", async (c) =>
    c.json({
      recovered: await recoverStale(deps),
    }),
  );
  app.get("/queue", async (c) => c.json(await deps.usage.producer.stats()));
  app.post("/queue/jobs/:id/retry", async (c) => {
    await deps.usage.producer.retry(id(c.req.param("id")));
    await deps.store.audit("usage_job_retry", c.req.param("id"));
    return c.json({ scheduled: true });
  });
  app.get("/metrics", (c) =>
    c.text(deps.metrics.render(deps.lifecycle.size, deps.memory.bufferedBytes)),
  );
  return app;
}
