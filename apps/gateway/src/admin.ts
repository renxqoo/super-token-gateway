import { recoverStale } from "./recovery.ts";
import { Hono } from "hono";
import type { Context } from "hono";
import { z } from "zod";
import {
  creditSchema,
  creditPolicySchema,
  tariffSchema,
  refundSchema,
  adjustmentSchema,
  resolutionSchema,
  currencySchema,
  GatewayError,
  createKey,
  encrypt,
  keySchema,
  modelSchema,
  projectSchema,
  pricingSchema,
  operations,
  providerSchema,
  routeSchema,
  secureEqual,
  verifier,
} from "@gateway/kernel";
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
  const quotaFields = {
    rpm: z.number().int().min(1).max(10000000).optional(),
    tpm: z.number().int().min(1).max(1000000000).optional(),
    concurrency: z.number().int().min(1).max(1000000).optional(),
  };
  app.post("/organizations", async (c) => {
    const v = z
      .object({
        external_ref: z.string().min(1).max(200),
        name: z.string().min(1).max(100),
        ...quotaFields,
      })
      .strict()
      .parse(await body(c));
    const key = await deps.store.createOrganization(v);
    return c.json({ id: key }, 201);
  });
  app.patch("/organizations/:id", async (c) => {
    const v = z
      .object({ enabled: z.boolean().optional(), ...quotaFields })
      .strict()
      .parse(await body(c));
    await deps.store.updateOrganization(id(c.req.param("id")), v);
    return c.json({ updated: true });
  });
  app.post("/organizations/:id/principals", async (c) => {
    const v = z
      .object({
        external_ref: z.string().min(1).max(200),
        role: z.enum(["OWNER", "ADMIN", "MEMBER", "BILLING", "VIEWER"]),
        ...quotaFields,
      })
      .strict()
      .parse(await body(c));
    const key = await deps.store.createPrincipal(id(c.req.param("id")), v);
    return c.json({ id: key }, 201);
  });
  app.patch("/principals/:id", async (c) => {
    const v = z
      .object({
        enabled: z.boolean().optional(),
        role: z.enum(["OWNER", "ADMIN", "MEMBER", "BILLING", "VIEWER"]).optional(),
        ...quotaFields,
      })
      .strict()
      .parse(await body(c));
    await deps.store.updatePrincipal(id(c.req.param("id")), v);
    return c.json({ updated: true });
  });
  app.get("/projects", async (c) => c.json({ data: await deps.store.projects() }));
  app.post("/projects", async (c) => {
    const input = projectSchema.parse(await body(c));
    const projectId = await deps.store.createProject(input);
    return c.json({ id: projectId, ...input }, 201);
  });
  app.patch("/projects/:id", async (c) => {
    const input = z
      .object({
        enabled: z.boolean().optional(),
        name: z.string().min(1).max(100).optional(),
        billing_currency: currencySchema.optional(),
        rpm: z.number().int().min(1).max(10_000_000).optional(),
        tpm: z.number().int().min(1).max(1_000_000_000).optional(),
        concurrency: z.number().int().min(1).max(1_000_000).optional(),
      })
      .strict()
      .parse(await body(c));
    await deps.store.updateProject(id(c.req.param("id")), input);
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
    await deps.store.createProvider(
      providerId,
      input,
      encrypt(input.api_key, deps.config.ENCRYPTION_KEY, providerId),
    );
    return c.json({ id: providerId }, 201);
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
    const input = z
      .object({
        enabled: z.boolean().optional(),
        operations: z.array(z.enum(operations)).min(1).optional(),
        max_output_tokens: z.number().int().min(1).max(10_000_000).optional(),
        routing_strategy: z.enum(["WEIGHTED", "ROUND_ROBIN", "LEAST_LATENCY"]).optional(),
      })
      .strict()
      .parse(await body(c));
    await deps.store.updateModel(c.req.param("model"), input);
    return c.json({ updated: true });
  });
  app.get("/models/:model/routes", async (c) =>
    c.json({ data: await deps.store.routeDefinitions(c.req.param("model")) }),
  );
  app.patch("/routes/:id", async (c) => {
    const input = z
      .object({
        enabled: z.boolean().optional(),
        priority: z.number().int().min(0).max(1000).optional(),
        weight: z.number().min(0.001).max(1000).optional(),
        pricing: pricingSchema.optional(),
      })
      .strict()
      .parse(await body(c));
    await deps.store.updateRoute(id(c.req.param("id")), input);
    return c.json({ updated: true });
  });
  app.patch("/keys/:id", async (c) => {
    const input = z
      .object({
        models: z.array(z.string().min(1).max(150)).max(1000).optional(),
        operations: z.array(z.enum(operations)).min(1).optional(),
        allowed_cidrs: z
          .array(z.string().min(2).max(60))
          .max(100)
          .transform((v) => v.map(validateCidr))
          .optional(),
        role: z.enum(["ADMIN", "INVOKER", "BILLING", "VIEWER"]).optional(),
        ...quotaFields,
        expires_at: z.iso.datetime().optional(),
      })
      .strict()
      .parse(await body(c));
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
  app.post("/projects/:id/credits", async (c) =>
    c.json(await deps.funds.credit(id(c.req.param("id")), creditSchema.parse(await body(c))), 201),
  );
  app.get("/projects/:id/wallets", async (c) =>
    c.json({ data: await deps.funds.wallets(id(c.req.param("id"))) }),
  );
  app.post("/projects/:id/wallets/:currency/credit-policy", async (c) =>
    c.json(
      await deps.funds.configureCredit(
        id(c.req.param("id")),
        currencySchema.parse(c.req.param("currency")),
        creditPolicySchema.parse(await body(c)),
      ),
    ),
  );
  app.get("/projects/:id/wallets/:currency/lots", async (c) =>
    c.json({
      data: await deps.funds.lots(
        id(c.req.param("id")),
        currencySchema.parse(c.req.param("currency")),
      ),
    }),
  );
  app.get("/projects/:id/wallets/:currency/journal", async (c) =>
    c.json({
      data: await deps.funds.journal(
        id(c.req.param("id")),
        currencySchema.parse(c.req.param("currency")),
        c.req.query("before") ? id(c.req.query("before")!) : undefined,
      ),
    }),
  );
  app.patch("/projects/:id/wallets/:currency", async (c) => {
    const input = z
      .object({
        policy: z.enum(["GIFT_FIRST", "CASH_FIRST", "EARLIEST_EXPIRY"]).optional(),
        frozen: z.boolean().optional(),
      })
      .strict()
      .parse(await body(c));
    await deps.funds.configure(
      id(c.req.param("id")),
      currencySchema.parse(c.req.param("currency")),
      input,
    );
    return c.json({ updated: true });
  });
  app.get("/tariffs", async (c) =>
    c.json({ data: await deps.funds.tariffs(c.req.query("model")) }),
  );
  app.post("/tariffs", async (c) =>
    c.json(await deps.funds.setTariff(tariffSchema.parse(await body(c))), 201),
  );
  app.post("/charges/:id/refunds", async (c) =>
    c.json(await deps.funds.refund(id(c.req.param("id")), refundSchema.parse(await body(c))), 201),
  );
  app.post("/charges/:id/adjustments", async (c) =>
    c.json(
      await deps.funds.adjust(id(c.req.param("id")), adjustmentSchema.parse(await body(c))),
      201,
    ),
  );
  app.post("/requests/:id/resolutions", async (c) =>
    c.json(
      await deps.funds.resolve(id(c.req.param("id")), resolutionSchema.parse(await body(c))),
      201,
    ),
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
