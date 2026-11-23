import { Hono } from "hono";
import { and, desc, eq, isNull, sql } from "drizzle-orm";
import { z } from "zod";
import { GatewayError, pricingSchema } from "@gateway/contracts";
import type { Pricing } from "@gateway/kernel";
import { maskSecret, recordAuditLog } from "../auth.ts";
import {
  channels,
  consoleAuditLogs,
  modelCatalog,
  pricingOverrides,
  pricingTemplates,
  projects,
  routes,
  sessions,
  upstreamAccounts,
  users,
} from "../db/schema.ts";
import {
  currencySchema,
  nonNegativeAtomsSchema,
  operationEnum,
  parseJsonBody,
  parseUuid,
  positiveAtomsSchema,
  requirePlatformAdmin,
  signedAtomsSchema,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";

const createUpstreamAccountSchema = z
  .object({
    name: z.string().trim().min(1).max(120),
    state: z.enum(["ACTIVE", "DISABLED", "RECONCILE_REQUIRED"]).default("ACTIVE"),
  })
  .strict();

const updateUpstreamAccountSchema = z
  .object({
    name: z.string().trim().min(1).max(120).optional(),
    state: z.enum(["ACTIVE", "DISABLED", "RECONCILE_REQUIRED"]).optional(),
  })
  .strict();

const rechargeSupplierSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    reason: z.string().max(500).optional(),
  })
  .strict();

const createChannelSchema = z
  .object({
    account_id: z.uuid(),
    name: z.string().trim().min(1).max(120),
    kind: z.string().regex(/^[a-z0-9][a-z0-9_-]{1,63}$/),
    provider_plugin_id: z.uuid().nullable().optional(),
    base_url: z.url().max(1000),
    api_key: z.string().max(16384).default(""),
    rpm: z.number().int().min(1).max(10_000_000).default(1_000_000),
    concurrency: z.number().int().min(1).max(100_000).default(1000),
    enabled: z.boolean().default(true),
    routing_policy: z.record(z.string(), z.unknown()).default({}),
    adapter_config: z.record(z.string(), z.unknown()).default({}),
  })
  .strict();

const updateChannelSchema = z
  .object({
    account_id: z.uuid().optional(),
    name: z.string().trim().min(1).max(120).optional(),
    kind: z
      .string()
      .regex(/^[a-z0-9][a-z0-9_-]{1,63}$/)
      .optional(),
    provider_plugin_id: z.uuid().nullable().optional(),
    base_url: z.url().max(1000).optional(),
    api_key: z.string().max(16384).optional(),
    rpm: z.number().int().min(1).max(10_000_000).optional(),
    concurrency: z.number().int().min(1).max(100_000).optional(),
    enabled: z.boolean().optional(),
    routing_policy: z.record(z.string(), z.unknown()).optional(),
    adapter_config: z.record(z.string(), z.unknown()).optional(),
  })
  .strict();

const createPricingTemplateSchema = z
  .object({
    name: z.string().trim().min(1).max(120),
    side: z.enum(["CUSTOMER", "SUPPLIER"]),
    currency: currencySchema.default("USD"),
    basis: z
      .enum(["PROVIDER_USAGE", "REQUEST_COMPLETION", "INPUT_CHARACTERS"])
      .default("PROVIDER_USAGE"),
    rates: pricingSchema.shape.rates,
    ceilings: z.record(z.string(), nonNegativeAtomsSchema).default({}),
  })
  .strict();

const updatePricingTemplateSchema = z
  .object({
    name: z.string().trim().min(1).max(120).optional(),
    basis: z.enum(["PROVIDER_USAGE", "REQUEST_COMPLETION", "INPUT_CHARACTERS"]).optional(),
    rates: pricingSchema.shape.rates.optional(),
    ceilings: z.record(z.string(), nonNegativeAtomsSchema).optional(),
  })
  .strict();

const upsertModelSchema = z
  .object({
    name: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,149}$/),
    display_name: z.string().trim().min(1).max(150),
    provider_brand: z.string().trim().min(1).max(80).default("gateway"),
    description: z.string().max(2000).default(""),
    category: z.string().trim().min(1).max(64).default("chat"),
    context_length: z.number().int().min(1).max(100_000_000).default(128_000),
    max_output_tokens: z.number().int().min(1).max(10_000_000).default(32_768),
    operations: z.array(operationEnum).min(1),
    routing_strategy: z.enum(["WEIGHTED", "ROUND_ROBIN", "LEAST_LATENCY"]).default("WEIGHTED"),
    tags: z.array(z.string().min(1).max(40)).max(32).default([]),
    list_price_template_id: z.uuid().nullable().optional(),
    list_rates: pricingSchema.shape.rates.optional(),
    currency: currencySchema.default("USD"),
    is_public: z.boolean().default(true),
    enabled: z.boolean().default(true),
  })
  .strict();

const createRouteSchema = z
  .object({
    model_name: z.string().min(1).max(150),
    channel_id: z.uuid(),
    upstream_model: z.string().min(1).max(200),
    operations: z.array(operationEnum).min(1),
    priority: z.number().int().min(0).max(1000).default(0),
    weight: z.number().positive().max(1000).default(1),
    billing_price_id: z.uuid().optional(),
    cost_version_id: z.uuid().optional(),
    billing_template_id: z.uuid().optional(),
    cost_template_id: z.uuid().optional(),
    request_policy: z.record(z.string(), z.unknown()).default({}),
    adapter_overrides: z.record(z.string(), z.unknown()).default({}),
    retry_policy: z.record(z.string(), z.unknown()).default({}),
    enabled: z.boolean().default(true),
  })
  .strict();

const updateRouteSchema = createRouteSchema.partial().strict();

const multiplierIntSchema = z.string().regex(/^[1-9][0-9]{0,12}$/);

const upsertPricingOverrideSchema = z
  .object({
    project_id: z.uuid(),
    model_name: z.union([z.literal("*"), z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,149}$/)]),
    operation: z.union([z.literal("*"), operationEnum]).default("*"),
    override_kind: z.enum(["EXACT_PRICE", "MULTIPLIER"]),
    billing_price_id: z.uuid().nullable().optional(),
    billing_template_id: z.uuid().nullable().optional(),
    multiplier_numerator: multiplierIntSchema.nullable().optional(),
    multiplier_denominator: multiplierIntSchema.nullable().optional(),
    enabled: z.boolean().default(true),
  })
  .strict()
  .superRefine((v, ctx) => {
    if (v.override_kind === "EXACT_PRICE") {
      if (v.model_name === "*") {
        ctx.addIssue({
          code: "custom",
          path: ["model_name"],
          message: "EXACT_PRICE override requires a concrete model_name",
        });
      }
      if (!v.billing_price_id && !v.billing_template_id) {
        ctx.addIssue({
          code: "custom",
          path: ["billing_price_id"],
          message: "billing_price_id or billing_template_id is required for EXACT_PRICE",
        });
      }
    } else if (!v.multiplier_numerator || !v.multiplier_denominator) {
      ctx.addIssue({
        code: "custom",
        path: ["multiplier_numerator"],
        message: "multiplier_numerator and multiplier_denominator are required for MULTIPLIER",
      });
    }
  });

const adminUpdateProjectSchema = z
  .object({
    name: z.string().trim().min(1).max(120).optional(),
    rpm: z.number().int().min(1).max(10_000_000).optional(),
    tpm: z.number().int().min(1).max(9_007_199_254_740_991).optional(),
    concurrency: z.number().int().min(1).max(1_000_000).optional(),
    enabled: z.boolean().optional(),
  })
  .strict();

const topupSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    reason: z.string().max(500).optional(),
  })
  .strict();

const grantSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    priority: z.number().int().min(0).max(10_000).optional(),
    expires_at: z.iso.datetime().nullable().optional(),
    models: z.array(z.string().min(1).max(150)).max(1000).optional(),
    operations: z.array(operationEnum).max(32).optional(),
    reason: z.string().max(500).optional(),
  })
  .strict();

const creditPolicySchema = z
  .object({
    currency: currencySchema.default("USD"),
    expected_version: nonNegativeAtomsSchema,
    credit_limit_atoms: nonNegativeAtomsSchema,
    policy: z.enum(["GIFT_FIRST", "CASH_FIRST", "EARLIEST_EXPIRY"]).optional(),
  })
  .strict();

const refundSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    reason: z.string().min(1).max(500),
  })
  .strict();

const adjustmentSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    delta_atoms: signedAtomsSchema,
    reason: z.string().min(1).max(500),
  })
  .strict();

const updateUserSchema = z
  .object({
    status: z.enum(["ACTIVE", "SUSPENDED"]).optional(),
    is_platform_admin: z.boolean().optional(),
  })
  .strict();

export function createAdminRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();
  const adminOnly = requirePlatformAdmin(deps);
  app.use("*", async (c, next) => {
    if (
      c.req.path === "/admin/v1/auth/bootstrap" ||
      c.req.path === "/admin/v1/auth/login" ||
      c.req.path === "/admin/v1/auth/refresh"
    ) {
      return next();
    }
    return adminOnly(c, next);
  });

  async function resolveTemplatePriceId(templateId: string, expectedSide: "CUSTOMER" | "SUPPLIER") {
    const [tpl] = await deps.orm
      .select()
      .from(pricingTemplates)
      .where(eq(pricingTemplates.id, templateId))
      .limit(1);
    if (!tpl || tpl.side !== expectedSide) {
      throw new GatewayError(
        400,
        "invalid_template",
        `Pricing template not found for ${expectedSide}`,
      );
    }
    const synced = await deps.sync.syncPricingTemplate(tpl.id);
    if (!synced.currentPriceVersionId) {
      throw new GatewayError(500, "template_sync_error", "Template price version missing");
    }
    return synced;
  }

  app.get("/upstream-accounts", async (c) => {
    const items = await deps.orm
      .select()
      .from(upstreamAccounts)
      .orderBy(desc(upstreamAccounts.createdAt));
    return c.json({ items });
  });

  app.post("/upstream-accounts", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, createUpstreamAccountSchema);
    const id = crypto.randomUUID();
    await deps.orm.insert(upstreamAccounts).values({
      id,
      name: body.name,
      state: body.state,
      syncStatus: "PENDING",
    });
    const synced = await deps.sync.syncUpstreamAccount(id);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.upstream_account.create",
      targetType: "upstream_account",
      targetId: id,
    });
    return c.json(synced, 201);
  });

  app.patch("/upstream-accounts/:id", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, updateUpstreamAccountSchema);
    const updated = await deps.orm
      .update(upstreamAccounts)
      .set({
        ...(body.name !== undefined ? { name: body.name } : {}),
        ...(body.state !== undefined ? { state: body.state } : {}),
        desiredRevision: sql`${upstreamAccounts.desiredRevision} + 1`,
        syncStatus: "PENDING",
        updatedAt: sql`now()`,
      })
      .where(eq(upstreamAccounts.id, id))
      .returning({ id: upstreamAccounts.id });
    if (updated.length === 0)
      throw new GatewayError(404, "not_found", "Upstream account not found");
    const synced = await deps.sync.syncUpstreamAccount(id);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.upstream_account.update",
      targetType: "upstream_account",
      targetId: id,
    });
    return c.json(synced);
  });

  app.post("/upstream-accounts/:id/recharges", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, rechargeSupplierSchema);
    await deps.sync.syncUpstreamAccount(id);
    const res = await deps.gateway.rechargeSupplier(id, {
      currency: body.currency,
      external_ref: body.external_ref,
      amount_atoms: body.amount_atoms,
      ...(body.reason ? { reason: body.reason } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.upstream_account.recharge",
      targetType: "upstream_account",
      targetId: id,
      metadata: { amount_atoms: body.amount_atoms, external_ref: body.external_ref },
    });
    return c.json(res);
  });

  app.get("/upstream-accounts/:id/balance", async (c) => {
    const id = parseUuid(c.req.param("id"), "id");
    const currency = c.req.query("currency") ?? "USD";
    return c.json(await deps.gateway.getSupplierBalance(id, currency));
  });

  app.get("/channels", async (c) => {
    const items = await deps.orm.select().from(channels).orderBy(desc(channels.createdAt));
    return c.json({ items });
  });

  app.get("/channels/health", async (c) => {
    return c.json(await deps.gateway.getChannelHealth());
  });

  app.post("/channels", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, createChannelSchema);
    const id = crypto.randomUUID();
    const keyHint = maskSecret(body.api_key);
    await deps.orm.insert(channels).values({
      id,
      accountId: body.account_id,
      name: body.name,
      kind: body.kind,
      providerPluginId: body.provider_plugin_id ?? null,
      baseUrl: body.base_url,
      keyHint,
      rpm: body.rpm,
      concurrency: body.concurrency,
      enabled: body.enabled,
      routingPolicy: body.routing_policy,
      adapterConfig: body.adapter_config,
      syncStatus: "PENDING",
    });
    try {
      const synced = await deps.sync.syncChannel(id, body.api_key);
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.channel.create",
        targetType: "channel",
        targetId: id,
        metadata: { name: body.name, kind: body.kind, key_hint: keyHint },
      });
      return c.json(synced, 201);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm.delete(channels).where(eq(channels.id, id));
      }
      throw err;
    }
  });

  app.patch("/channels/:id", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, updateChannelSchema);
    const [existing] = await deps.orm.select().from(channels).where(eq(channels.id, id)).limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "Channel not found");

    if (body.api_key !== undefined) {
      await deps.sync.syncUpstreamAccount(body.account_id ?? existing.accountId, false);
      const gwCh = await deps.gateway.upsertChannel({
        id: existing.id,
        account_id: body.account_id ?? existing.accountId,
        name: body.name ?? existing.name,
        kind: body.kind ?? existing.kind,
        ...(body.provider_plugin_id !== undefined
          ? { provider_plugin_id: body.provider_plugin_id }
          : existing.providerPluginId
            ? { provider_plugin_id: existing.providerPluginId }
            : {}),
        base_url: body.base_url ?? existing.baseUrl,
        api_key: body.api_key,
        rpm: body.rpm ?? existing.rpm,
        concurrency: body.concurrency ?? existing.concurrency,
        enabled: body.enabled ?? existing.enabled,
        routing_policy: body.routing_policy ?? existing.routingPolicy,
        adapter_config: body.adapter_config ?? existing.adapterConfig,
      });
      const [updated] = await deps.orm
        .update(channels)
        .set({
          ...(body.account_id !== undefined ? { accountId: body.account_id } : {}),
          ...(body.name !== undefined ? { name: body.name } : {}),
          ...(body.kind !== undefined ? { kind: body.kind } : {}),
          providerPluginId: gwCh.provider_plugin_id ?? existing.providerPluginId,
          ...(body.base_url !== undefined ? { baseUrl: body.base_url } : {}),
          keyHint: maskSecret(body.api_key),
          ...(body.rpm !== undefined ? { rpm: body.rpm } : {}),
          ...(body.concurrency !== undefined ? { concurrency: body.concurrency } : {}),
          ...(body.enabled !== undefined ? { enabled: body.enabled } : {}),
          ...(body.routing_policy !== undefined ? { routingPolicy: body.routing_policy } : {}),
          ...(body.adapter_config !== undefined ? { adapterConfig: body.adapter_config } : {}),
          desiredRevision: sql`${channels.desiredRevision} + 1`,
          syncedRevision: sql`${channels.desiredRevision} + 1`,
          syncStatus: "SYNCED",
          lastSyncError: null,
          updatedAt: sql`now()`,
        })
        .where(eq(channels.id, id))
        .returning();
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.channel.update",
        targetType: "channel",
        targetId: id,
        metadata: { rotated_secret: true },
      });
      return c.json(updated);
    }

    await deps.orm
      .update(channels)
      .set({
        ...(body.account_id !== undefined ? { accountId: body.account_id } : {}),
        ...(body.name !== undefined ? { name: body.name } : {}),
        ...(body.kind !== undefined ? { kind: body.kind } : {}),
        ...(body.provider_plugin_id !== undefined
          ? { providerPluginId: body.provider_plugin_id }
          : {}),
        ...(body.base_url !== undefined ? { baseUrl: body.base_url } : {}),
        ...(body.rpm !== undefined ? { rpm: body.rpm } : {}),
        ...(body.concurrency !== undefined ? { concurrency: body.concurrency } : {}),
        ...(body.enabled !== undefined ? { enabled: body.enabled } : {}),
        ...(body.routing_policy !== undefined ? { routingPolicy: body.routing_policy } : {}),
        ...(body.adapter_config !== undefined ? { adapterConfig: body.adapter_config } : {}),
        desiredRevision: sql`${channels.desiredRevision} + 1`,
        syncStatus: "PENDING",
        updatedAt: sql`now()`,
      })
      .where(eq(channels.id, id));
    const synced = await deps.sync.syncChannel(id);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.channel.update",
      targetType: "channel",
      targetId: id,
      metadata: { rotated_secret: false },
    });
    return c.json(synced);
  });

  app.get("/pricing-templates", async (c) => {
    const items = await deps.orm
      .select()
      .from(pricingTemplates)
      .orderBy(desc(pricingTemplates.createdAt));
    return c.json({ items });
  });

  app.post("/pricing-templates", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, createPricingTemplateSchema);
    const id = crypto.randomUUID();
    await deps.orm.insert(pricingTemplates).values({
      id,
      name: body.name,
      side: body.side,
      currency: body.currency,
      basis: body.basis,
      rates: body.rates as Pricing["rates"],
      ceilings: body.ceilings,
      syncStatus: "PENDING",
    });
    const synced = await deps.sync.syncPricingTemplate(id);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.pricing_template.create",
      targetType: "pricing_template",
      targetId: id,
      metadata: { price_version_id: synced.currentPriceVersionId },
    });
    return c.json(synced, 201);
  });

  app.patch("/pricing-templates/:id", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, updatePricingTemplateSchema);
    const updated = await deps.orm
      .update(pricingTemplates)
      .set({
        ...(body.name !== undefined ? { name: body.name } : {}),
        ...(body.basis !== undefined ? { basis: body.basis } : {}),
        ...(body.rates !== undefined ? { rates: body.rates as Pricing["rates"] } : {}),
        ...(body.ceilings !== undefined ? { ceilings: body.ceilings } : {}),
        desiredRevision: sql`${pricingTemplates.desiredRevision} + 1`,
        syncStatus: "PENDING",
        updatedAt: sql`now()`,
      })
      .where(eq(pricingTemplates.id, id))
      .returning({ id: pricingTemplates.id });
    if (updated.length === 0)
      throw new GatewayError(404, "not_found", "Pricing template not found");
    const synced = await deps.sync.syncPricingTemplate(id);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.pricing_template.update",
      targetType: "pricing_template",
      targetId: id,
      metadata: { new_price_version_id: synced.currentPriceVersionId },
    });
    return c.json(synced);
  });

  app.get("/models", async (c) => {
    const items = await deps.orm.select().from(modelCatalog).orderBy(desc(modelCatalog.createdAt));
    return c.json({ items });
  });

  app.post("/models", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, upsertModelSchema);
    let listPriceVersionId: string | null = null;
    let listRates: Pricing["rates"] = (body.list_rates as Pricing["rates"]) ?? {};
    let currency = body.currency;
    if (body.list_price_template_id) {
      const tpl = await resolveTemplatePriceId(body.list_price_template_id, "CUSTOMER");
      listPriceVersionId = tpl.currentPriceVersionId;
      listRates = tpl.rates;
      currency = tpl.currency;
    }
    await deps.orm
      .insert(modelCatalog)
      .values({
        name: body.name,
        displayName: body.display_name,
        providerBrand: body.provider_brand,
        description: body.description,
        category: body.category,
        contextLength: body.context_length,
        maxOutputTokens: body.max_output_tokens,
        operations: body.operations,
        routingStrategy: body.routing_strategy,
        tags: body.tags,
        listPriceVersionId,
        listRates,
        currency,
        isPublic: body.is_public,
        enabled: body.enabled,
        syncStatus: "PENDING",
      })
      .onConflictDoUpdate({
        target: modelCatalog.name,
        set: {
          displayName: body.display_name,
          providerBrand: body.provider_brand,
          description: body.description,
          category: body.category,
          contextLength: body.context_length,
          maxOutputTokens: body.max_output_tokens,
          operations: body.operations,
          routingStrategy: body.routing_strategy,
          tags: body.tags,
          listPriceVersionId,
          listRates,
          currency,
          isPublic: body.is_public,
          enabled: body.enabled,
          desiredRevision: sql`${modelCatalog.desiredRevision} + 1`,
          syncStatus: "PENDING",
          updatedAt: sql`now()`,
        },
      });
    const synced = await deps.sync.syncModel(body.name);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.model.upsert",
      targetType: "model",
      targetId: body.name,
    });
    return c.json(synced, 201);
  });

  app.get("/routes", async (c) => {
    const items = await deps.orm.select().from(routes).orderBy(desc(routes.createdAt));
    return c.json({ items });
  });

  app.post("/routes", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, createRouteSchema);
    let billingPriceId = body.billing_price_id;
    let costVersionId = body.cost_version_id;
    if (!billingPriceId && body.billing_template_id) {
      const tpl = await resolveTemplatePriceId(body.billing_template_id, "CUSTOMER");
      billingPriceId = tpl.currentPriceVersionId!;
    }
    if (!costVersionId && body.cost_template_id) {
      const tpl = await resolveTemplatePriceId(body.cost_template_id, "SUPPLIER");
      costVersionId = tpl.currentPriceVersionId!;
    }
    if (!billingPriceId || !costVersionId) {
      throw new GatewayError(
        400,
        "validation_error",
        "billing_price_id/billing_template_id and cost_version_id/cost_template_id are required",
      );
    }
    const id = crypto.randomUUID();
    await deps.orm.insert(routes).values({
      id,
      modelName: body.model_name,
      channelId: body.channel_id,
      upstreamModel: body.upstream_model,
      operations: body.operations,
      priority: body.priority,
      weight: body.weight,
      billingPriceId,
      costVersionId,
      requestPolicy: body.request_policy,
      adapterOverrides: body.adapter_overrides,
      retryPolicy: body.retry_policy,
      enabled: body.enabled,
      syncStatus: "PENDING",
    });
    const synced = await deps.sync.syncRoute(id);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.route.create",
      targetType: "route",
      targetId: id,
    });
    return c.json(synced, 201);
  });

  app.patch("/routes/:id", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, updateRouteSchema);
    let billingPriceId = body.billing_price_id;
    let costVersionId = body.cost_version_id;
    if (!billingPriceId && body.billing_template_id) {
      const tpl = await resolveTemplatePriceId(body.billing_template_id, "CUSTOMER");
      billingPriceId = tpl.currentPriceVersionId!;
    }
    if (!costVersionId && body.cost_template_id) {
      const tpl = await resolveTemplatePriceId(body.cost_template_id, "SUPPLIER");
      costVersionId = tpl.currentPriceVersionId!;
    }
    const updated = await deps.orm
      .update(routes)
      .set({
        ...(body.model_name !== undefined ? { modelName: body.model_name } : {}),
        ...(body.channel_id !== undefined ? { channelId: body.channel_id } : {}),
        ...(body.upstream_model !== undefined ? { upstreamModel: body.upstream_model } : {}),
        ...(body.operations !== undefined ? { operations: body.operations } : {}),
        ...(body.priority !== undefined ? { priority: body.priority } : {}),
        ...(body.weight !== undefined ? { weight: body.weight } : {}),
        ...(billingPriceId !== undefined ? { billingPriceId } : {}),
        ...(costVersionId !== undefined ? { costVersionId } : {}),
        ...(body.request_policy !== undefined ? { requestPolicy: body.request_policy } : {}),
        ...(body.adapter_overrides !== undefined
          ? { adapterOverrides: body.adapter_overrides }
          : {}),
        ...(body.retry_policy !== undefined ? { retryPolicy: body.retry_policy } : {}),
        ...(body.enabled !== undefined ? { enabled: body.enabled } : {}),
        desiredRevision: sql`${routes.desiredRevision} + 1`,
        syncStatus: "PENDING",
        updatedAt: sql`now()`,
      })
      .where(eq(routes.id, id))
      .returning({ id: routes.id });
    if (updated.length === 0) throw new GatewayError(404, "not_found", "Route not found");
    const synced = await deps.sync.syncRoute(id);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.route.update",
      targetType: "route",
      targetId: id,
    });
    return c.json(synced);
  });

  app.get("/pricing-overrides", async (c) => {
    const projectId = c.req.query("project_id");
    const items = projectId
      ? await deps.orm
          .select()
          .from(pricingOverrides)
          .where(eq(pricingOverrides.projectId, parseUuid(projectId, "project_id")))
      : await deps.orm.select().from(pricingOverrides).orderBy(desc(pricingOverrides.createdAt));
    return c.json({ items });
  });

  app.put("/pricing-overrides", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, upsertPricingOverrideSchema);
    let billingPriceId = body.billing_price_id ?? null;
    if (body.override_kind === "EXACT_PRICE" && !billingPriceId && body.billing_template_id) {
      const tpl = await resolveTemplatePriceId(body.billing_template_id, "CUSTOMER");
      billingPriceId = tpl.currentPriceVersionId;
    }
    const id = crypto.randomUUID();
    const [row] = await deps.orm
      .insert(pricingOverrides)
      .values({
        id,
        projectId: body.project_id,
        modelName: body.model_name,
        operation: body.operation,
        overrideKind: body.override_kind,
        billingPriceId: body.override_kind === "EXACT_PRICE" ? billingPriceId : null,
        multiplierNumerator:
          body.override_kind === "MULTIPLIER" ? (body.multiplier_numerator ?? null) : null,
        multiplierDenominator:
          body.override_kind === "MULTIPLIER" ? (body.multiplier_denominator ?? null) : null,
        enabled: body.enabled,
        syncStatus: "PENDING",
      })
      .onConflictDoUpdate({
        target: [
          pricingOverrides.projectId,
          pricingOverrides.modelName,
          pricingOverrides.operation,
        ],
        set: {
          overrideKind: body.override_kind,
          billingPriceId: body.override_kind === "EXACT_PRICE" ? billingPriceId : null,
          multiplierNumerator:
            body.override_kind === "MULTIPLIER" ? (body.multiplier_numerator ?? null) : null,
          multiplierDenominator:
            body.override_kind === "MULTIPLIER" ? (body.multiplier_denominator ?? null) : null,
          enabled: body.enabled,
          desiredRevision: sql`${pricingOverrides.desiredRevision} + 1`,
          syncStatus: "PENDING",
          updatedAt: sql`now()`,
        },
      })
      .returning();
    const synced = await deps.sync.syncPricingOverride(row!.id);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.pricing_override.upsert",
      targetType: "pricing_override",
      targetId: synced.id,
      metadata: {
        project_id: body.project_id,
        model_name: body.model_name,
        override_kind: body.override_kind,
      },
    });
    return c.json(synced);
  });

  app.post("/sync/reconcile", async (c) => {
    const actor = c.get("user");
    const result = await deps.sync.reconcilePending();
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.sync.reconcile",
      targetType: "system",
      targetId: "reconciler",
      metadata: result,
    });
    return c.json(result);
  });

  app.get("/plugins", async (c) => {
    return c.json(await deps.gateway.listPlugins(c.req.query("capability")));
  });

  app.get("/plugins/builtins", async (c) => {
    return c.json(await deps.gateway.listBuiltinPlugins(c.req.query("capability")));
  });

  app.post("/plugins/builtins/:name/install", async (c) => {
    const actor = c.get("user");
    const name = c.req.param("name");
    const raw = await c.req.text();
    const body = raw.trim() ? (JSON.parse(raw) as Record<string, unknown>) : {};
    const res = await deps.gateway.installBuiltinPlugin(name, body);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.plugin.install_builtin",
      targetType: "plugin",
      targetId: name,
    });
    return c.json(res);
  });

  app.put("/plugins", async (c) => {
    const actor = c.get("user");
    const body = (await c.req.json()) as Record<string, unknown>;
    const res = await deps.gateway.upsertPlugin(body);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.plugin.upsert",
      targetType: "plugin",
      targetId: String(res.id ?? "unknown"),
    });
    return c.json(res);
  });

  app.post("/plugin-releases/validate", async (c) => {
    const body = (await c.req.json()) as Record<string, unknown>;
    return c.json(await deps.gateway.validatePluginRelease(body));
  });

  app.post("/plugin-releases", async (c) => {
    const actor = c.get("user");
    const body = (await c.req.json()) as Record<string, unknown>;
    const res = await deps.gateway.publishPluginRelease(body);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.plugin_release.publish",
      targetType: "plugin_release",
      targetId: String(body.id ?? "unknown"),
    });
    return c.json(res);
  });

  app.post("/plugin-releases/:releaseId/rollback", async (c) => {
    const actor = c.get("user");
    const releaseId = parseUuid(c.req.param("releaseId"), "releaseId");
    const body = (await c.req.json()) as Record<string, unknown>;
    const res = await deps.gateway.rollbackPluginRelease(releaseId, body);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.plugin_release.rollback",
      targetType: "plugin_release",
      targetId: releaseId,
    });
    return c.json(res);
  });

  app.post("/plugins/:pluginId/revoke", async (c) => {
    const actor = c.get("user");
    const pluginId = parseUuid(c.req.param("pluginId"), "pluginId");
    const body = await parseJsonBody(
      c,
      z.object({ reason: z.string().trim().min(1).max(1024) }).strict(),
    );
    const res = await deps.gateway.revokePlugin(pluginId, body.reason);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.plugin.revoke",
      targetType: "plugin",
      targetId: pluginId,
      metadata: { reason: body.reason },
    });
    return c.json(res);
  });

  app.post("/providers/preview", async (c) => {
    const body = (await c.req.json()) as Record<string, unknown>;
    return c.json(await deps.gateway.previewProvider(body));
  });

  app.post("/finance/projects/:projectId/topups", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, topupSchema);
    const proj = await deps.sync.ensureProjectSynced(projectId);
    const res = await deps.gateway.creditWallet(proj.gatewayProjectId, {
      currency: body.currency,
      external_ref: body.external_ref,
      amount_atoms: body.amount_atoms,
      kind: "CASH",
      ...(body.reason ? { reason: body.reason } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: proj.orgId,
      action: "admin.finance.topup",
      targetType: "project",
      targetId: projectId,
      metadata: { amount_atoms: body.amount_atoms, external_ref: body.external_ref },
    });
    return c.json(res);
  });

  app.post("/finance/projects/:projectId/grants", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, grantSchema);
    const proj = await deps.sync.ensureProjectSynced(projectId);
    const res = await deps.gateway.creditWallet(proj.gatewayProjectId, {
      currency: body.currency,
      external_ref: body.external_ref,
      amount_atoms: body.amount_atoms,
      kind: "GRANT",
      ...(body.priority !== undefined ? { priority: body.priority } : {}),
      ...(body.expires_at !== undefined ? { expires_at: body.expires_at } : {}),
      ...(body.models ? { models: body.models } : {}),
      ...(body.operations ? { operations: body.operations } : {}),
      ...(body.reason ? { reason: body.reason } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: proj.orgId,
      action: "admin.finance.grant",
      targetType: "project",
      targetId: projectId,
      metadata: { amount_atoms: body.amount_atoms, external_ref: body.external_ref },
    });
    return c.json(res);
  });

  app.post("/finance/projects/:projectId/credit-policy", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, creditPolicySchema);
    const proj = await deps.sync.ensureProjectSynced(projectId);
    const res = await deps.gateway.configureCreditPolicy(proj.gatewayProjectId, {
      currency: body.currency,
      expected_version: body.expected_version,
      credit_limit_atoms: body.credit_limit_atoms,
      ...(body.policy ? { policy: body.policy } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: proj.orgId,
      action: "admin.finance.credit_policy",
      targetType: "project",
      targetId: projectId,
      metadata: { credit_limit_atoms: body.credit_limit_atoms },
    });
    return c.json(res);
  });

  app.post("/finance/charges/:chargeId/refunds", async (c) => {
    const actor = c.get("user");
    const chargeId = parseUuid(c.req.param("chargeId"), "chargeId");
    const body = await parseJsonBody(c, refundSchema);
    const res = await deps.gateway.refundCharge(chargeId, body);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.refund",
      targetType: "charge",
      targetId: chargeId,
      metadata: { amount_atoms: body.amount_atoms, external_ref: body.external_ref },
    });
    return c.json(res);
  });

  app.post("/finance/charges/:chargeId/adjustments", async (c) => {
    const actor = c.get("user");
    const chargeId = parseUuid(c.req.param("chargeId"), "chargeId");
    const body = await parseJsonBody(c, adjustmentSchema);
    const res = await deps.gateway.adjustCharge(chargeId, body);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.adjustment",
      targetType: "charge",
      targetId: chargeId,
      metadata: { delta_atoms: body.delta_atoms, external_ref: body.external_ref },
    });
    return c.json(res);
  });

  app.get("/finance/holds", async (c) => {
    return c.json(
      await deps.gateway.listHolds({
        state: c.req.query("state"),
        project_id: c.req.query("project_id"),
        limit: c.req.query("limit"),
        offset: c.req.query("offset"),
      }),
    );
  });

  app.post("/finance/requests/:requestId/resolve", async (c) => {
    const actor = c.get("user");
    const requestId = parseUuid(c.req.param("requestId"), "requestId");
    const body = (await c.req.json()) as Record<string, unknown>;
    const res = await deps.gateway.resolveRequest(requestId, {
      ...body,
      actor: actor.email,
    });
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.resolve_request",
      targetType: "request",
      targetId: requestId,
    });
    return c.json(res);
  });

  app.post("/finance/requests/:requestId/attempts/:attemptId/resolve", async (c) => {
    const actor = c.get("user");
    const requestId = parseUuid(c.req.param("requestId"), "requestId");
    const attemptId = parseUuid(c.req.param("attemptId"), "attemptId");
    const body = (await c.req.json()) as Record<string, unknown>;
    const res = await deps.gateway.resolveSupplierAttempt(requestId, attemptId, {
      ...body,
      actor: actor.email,
    });
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.resolve_supplier_attempt",
      targetType: "supplier_attempt",
      targetId: `${requestId}:${attemptId}`,
    });
    return c.json(res);
  });

  app.get("/finance/async-tasks", async (c) => {
    return c.json(
      await deps.gateway.listAsyncTasks({
        project_id: c.req.query("project_id"),
        status: c.req.query("status"),
        refund_eligibility: c.req.query("refund_eligibility"),
        limit: c.req.query("limit"),
        offset: c.req.query("offset"),
      }),
    );
  });

  app.get("/finance/conservation", async (c) => {
    return c.json(await deps.gateway.getConservationAudit());
  });

  app.get("/audit-logs", async (c) => {
    const limit = Math.min(200, Math.max(1, Number(c.req.query("limit") ?? 50)));
    const items = await deps.orm
      .select()
      .from(consoleAuditLogs)
      .orderBy(desc(consoleAuditLogs.createdAt))
      .limit(limit);
    return c.json({ items });
  });

  app.get("/users", async (c) => {
    const items = await deps.orm
      .select({
        id: users.id,
        email: users.email,
        display_name: users.displayName,
        is_platform_admin: users.isPlatformAdmin,
        status: users.status,
        created_at: users.createdAt,
      })
      .from(users)
      .orderBy(desc(users.createdAt))
      .limit(200);
    return c.json({ items });
  });

  app.patch("/users/:userId", async (c) => {
    const actor = c.get("user");
    const userId = parseUuid(c.req.param("userId"), "userId");
    const body = await parseJsonBody(c, updateUserSchema);
    const updated = await deps.orm.transaction(async (tx) => {
      const [target] = await tx.select().from(users).where(eq(users.id, userId)).for("update");
      if (!target) throw new GatewayError(404, "not_found", "User not found");
      const removingAdminPower =
        target.isPlatformAdmin &&
        target.status === "ACTIVE" &&
        (body.is_platform_admin === false || body.status === "SUSPENDED");
      if (removingAdminPower) {
        const activeAdmins = await tx
          .select({ id: users.id })
          .from(users)
          .where(and(eq(users.isPlatformAdmin, true), eq(users.status, "ACTIVE")))
          .for("update");
        if (activeAdmins.length <= 1) {
          throw new GatewayError(
            400,
            "last_platform_admin",
            "Platform must retain at least one active administrator",
          );
        }
      }
      const [row] = await tx
        .update(users)
        .set({
          ...(body.status !== undefined ? { status: body.status } : {}),
          ...(body.is_platform_admin !== undefined
            ? { isPlatformAdmin: body.is_platform_admin }
            : {}),
          updatedAt: sql`now()`,
        })
        .where(eq(users.id, userId))
        .returning({
          id: users.id,
          email: users.email,
          display_name: users.displayName,
          is_platform_admin: users.isPlatformAdmin,
          status: users.status,
        });
      if (body.status === "SUSPENDED" || body.is_platform_admin === false) {
        await tx
          .update(sessions)
          .set({ revokedAt: sql`now()` })
          .where(and(eq(sessions.userId, userId), isNull(sessions.revokedAt)));
      }
      return row!;
    });
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.user.update",
      targetType: "user",
      targetId: userId,
      metadata: body,
    });
    return c.json(updated);
  });

  app.get("/projects", async (c) => {
    const items = await deps.orm
      .select()
      .from(projects)
      .orderBy(desc(projects.createdAt))
      .limit(200);
    return c.json({ items });
  });

  app.patch("/projects/:projectId", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, adminUpdateProjectSchema);
    const [existing] = await deps.orm
      .select()
      .from(projects)
      .where(eq(projects.id, projectId))
      .limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "Project not found");
    const [updated] = await deps.orm
      .update(projects)
      .set({
        ...(body.name !== undefined ? { name: body.name } : {}),
        ...(body.rpm !== undefined ? { rpm: body.rpm } : {}),
        ...(body.tpm !== undefined ? { tpm: body.tpm } : {}),
        ...(body.concurrency !== undefined ? { concurrency: body.concurrency } : {}),
        ...(body.enabled !== undefined ? { enabled: body.enabled } : {}),
        desiredRevision: sql`${projects.desiredRevision} + 1`,
        syncStatus: "PENDING",
        updatedAt: sql`now()`,
      })
      .where(eq(projects.id, projectId))
      .returning();
    const synced = existing.gatewaySynced
      ? await deps.sync.ensureProjectSynced(projectId)
      : updated!;
    await recordAuditLog(deps.orm, {
      actor,
      orgId: existing.orgId,
      action: "admin.project.update",
      targetType: "project",
      targetId: projectId,
      metadata: body,
    });
    return c.json(synced);
  });

  return app;
}
