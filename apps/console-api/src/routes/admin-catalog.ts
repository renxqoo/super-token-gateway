import { Hono } from "hono";
import { desc, eq, sql } from "drizzle-orm";
import { z } from "zod";
import { GatewayError, pricingSchema } from "@gateway/contracts";
import type { Pricing } from "@gateway/kernel";
import { maskSecret, recordAuditLog } from "../auth.ts";
import {
  channels,
  modelCatalog,
  priceVersionSnapshots,
  pricingOverrides,
  pricingTemplates,
  routes,
  upstreamAccounts,
} from "../db/schema.ts";
import {
  currencySchema,
  nonNegativeAtomsSchema,
  operationEnum,
  parseJsonBody,
  parseLimitOffset,
  parseUuid,
  positiveAtomsSchema,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";

function validatePricingRates(currency: string, rates: unknown, ctx: z.RefinementCtx) {
  const res = pricingSchema.safeParse({ currency, rates });
  if (!res.success) {
    for (const issue of res.error.issues) {
      ctx.addIssue({
        code: "custom",
        path: issue.path.length > 0 ? issue.path : ["rates"],
        message: issue.message,
      });
    }
  }
}

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
    expected_state: z.enum(["ACTIVE", "DISABLED", "RECONCILE_REQUIRED"]).optional(),
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
  .strict()
  .superRefine((v, ctx) => validatePricingRates(v.currency, v.rates, ctx));

const updatePricingTemplateSchema = z
  .object({
    name: z.string().trim().min(1).max(120).optional(),
    basis: z.enum(["PROVIDER_USAGE", "REQUEST_COMPLETION", "INPUT_CHARACTERS"]).optional(),
    rates: pricingSchema.shape.rates.optional(),
    ceilings: z.record(z.string(), nonNegativeAtomsSchema).optional(),
  })
  .strict()
  .superRefine((v, ctx) => {
    if (v.rates !== undefined) validatePricingRates("USD", v.rates, ctx);
  });

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
  .strict()
  .superRefine((v, ctx) => {
    if (v.list_rates !== undefined) validatePricingRates(v.currency, v.list_rates, ctx);
  });

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

const updateRouteSchema = z
  .object({
    channel_id: z.uuid().optional(),
    upstream_model: z.string().min(1).max(200).optional(),
    operations: z.array(operationEnum).min(1).optional(),
    priority: z.number().int().min(0).max(1000).optional(),
    weight: z.number().positive().max(1000).optional(),
    billing_price_id: z.uuid().optional(),
    cost_version_id: z.uuid().optional(),
    billing_template_id: z.uuid().optional(),
    cost_template_id: z.uuid().optional(),
    request_policy: z.record(z.string(), z.unknown()).optional(),
    adapter_overrides: z.record(z.string(), z.unknown()).optional(),
    retry_policy: z.record(z.string(), z.unknown()).optional(),
    enabled: z.boolean().optional(),
  })
  .strict();

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

export function createAdminCatalogRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();

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

  async function ensureCustomerPriceSnapshot(priceId: string): Promise<void> {
    const [local] = await deps.orm
      .select({ id: priceVersionSnapshots.id, side: priceVersionSnapshots.side })
      .from(priceVersionSnapshots)
      .where(eq(priceVersionSnapshots.id, priceId))
      .limit(1);
    if (local) {
      if (local.side !== "CUSTOMER") {
        throw new GatewayError(
          400,
          "invalid_price_override",
          "EXACT_PRICE override requires a CUSTOMER price version",
        );
      }
      return;
    }
    const remote = await deps.gateway.getPriceVersion(priceId);
    if (remote.side !== "CUSTOMER") {
      throw new GatewayError(
        400,
        "invalid_price_override",
        "EXACT_PRICE override requires a CUSTOMER price version",
      );
    }
    await deps.orm
      .insert(priceVersionSnapshots)
      .values({
        id: remote.id,
        templateId: null,
        side: remote.side,
        currency: remote.currency,
        basis: remote.basis,
        rates: remote.rates,
        ceilings: remote.ceilings,
      })
      .onConflictDoNothing({ target: priceVersionSnapshots.id });
  }

  app.get("/upstream-accounts", async (c) => {
    const { limit, offset } = parseLimitOffset(c, 100, 200);
    const localItems = await deps.orm
      .select()
      .from(upstreamAccounts)
      .orderBy(desc(upstreamAccounts.createdAt))
      .limit(limit)
      .offset(offset);
    const gwRes = await deps.gateway.listUpstreamAccounts().catch(() => null);
    const gwMap = new Map((gwRes?.items ?? []).map((a) => [a.id, a]));
    const items = localItems.map((item) => {
      const gw = gwMap.get(item.id);
      return gw ? { ...item, state: gw.state, gateway_state: gw.state } : item;
    });
    return c.json({ items, limit, offset, gateway_state_available: gwRes !== null });
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
    try {
      const synced = await deps.sync.syncUpstreamAccount(id, { includeState: true });
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.upstream_account.create",
        targetType: "upstream_account",
        targetId: id,
      });
      return c.json(synced, 201);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm.delete(upstreamAccounts).where(eq(upstreamAccounts.id, id));
      }
      throw err;
    }
  });

  app.patch("/upstream-accounts/:id", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, updateUpstreamAccountSchema);
    const [existing] = await deps.orm
      .select()
      .from(upstreamAccounts)
      .where(eq(upstreamAccounts.id, id))
      .limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "Upstream account not found");

    const explicitStateChange = body.state !== undefined;
    await deps.orm
      .update(upstreamAccounts)
      .set({
        ...(body.name !== undefined ? { name: body.name } : {}),
        ...(explicitStateChange ? { state: body.state } : {}),
        desiredRevision: sql`${upstreamAccounts.desiredRevision} + 1`,
        syncStatus: "PENDING",
        updatedAt: sql`now()`,
      })
      .where(eq(upstreamAccounts.id, id));

    try {
      const synced = await deps.sync.syncUpstreamAccount(id, {
        force: true,
        includeState: explicitStateChange,
        ...(body.expected_state ? { expectedState: body.expected_state } : {}),
      });
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.upstream_account.update",
        targetType: "upstream_account",
        targetId: id,
        metadata: {
          ...(body.state ? { state: body.state } : {}),
          ...(body.expected_state ? { expected_state: body.expected_state } : {}),
        },
      });
      return c.json(synced);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm
          .update(upstreamAccounts)
          .set({
            name: existing.name,
            state: existing.state,
            desiredRevision: existing.desiredRevision,
            syncStatus: existing.syncStatus,
            lastSyncError: existing.lastSyncError,
            updatedAt: sql`now()`,
          })
          .where(eq(upstreamAccounts.id, id));
      }
      throw err;
    }
  });

  app.post("/upstream-accounts/:id/recharges", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, rechargeSupplierSchema);
    await deps.sync.syncUpstreamAccount(id, { force: false, includeState: false });
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
    const { limit, offset } = parseLimitOffset(c, 100, 200);
    const localItems = await deps.orm
      .select()
      .from(channels)
      .orderBy(desc(channels.createdAt))
      .limit(limit)
      .offset(offset);
    const gwRes = await deps.gateway.listChannels().catch(() => null);
    const gwMap = new Map((gwRes?.items ?? []).map((ch) => [ch.id, ch]));
    const items = localItems.map((item) => {
      const gw = gwMap.get(item.id);
      return gw ? { ...item, providerPluginId: gw.provider_plugin_id } : item;
    });
    return c.json({ items, limit, offset });
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

    const nextKeyHint = body.api_key !== undefined ? maskSecret(body.api_key) : undefined;
    await deps.orm
      .update(channels)
      .set({
        ...(body.account_id !== undefined ? { accountId: body.account_id } : {}),
        ...(body.name !== undefined ? { name: body.name } : {}),
        ...(body.kind !== undefined ? { kind: body.kind } : {}),
        ...(body.base_url !== undefined ? { baseUrl: body.base_url } : {}),
        ...(nextKeyHint !== undefined ? { keyHint: nextKeyHint } : {}),
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

    try {
      const synced = await deps.sync.syncChannel(id, body.api_key);
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.channel.update",
        targetType: "channel",
        targetId: id,
        metadata: { rotated_secret: body.api_key !== undefined },
      });
      return c.json(synced);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm
          .update(channels)
          .set({
            accountId: existing.accountId,
            name: existing.name,
            kind: existing.kind,
            baseUrl: existing.baseUrl,
            keyHint: existing.keyHint,
            rpm: existing.rpm,
            concurrency: existing.concurrency,
            enabled: existing.enabled,
            routingPolicy: existing.routingPolicy,
            adapterConfig: existing.adapterConfig,
            desiredRevision: existing.desiredRevision,
            syncStatus: existing.syncStatus,
            lastSyncError: existing.lastSyncError,
            updatedAt: sql`now()`,
          })
          .where(eq(channels.id, id));
      }
      throw err;
    }
  });

  app.get("/pricing-templates", async (c) => {
    const { limit, offset } = parseLimitOffset(c, 100, 200);
    const items = await deps.orm
      .select()
      .from(pricingTemplates)
      .orderBy(desc(pricingTemplates.createdAt))
      .limit(limit)
      .offset(offset);
    return c.json({ items, limit, offset });
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
    try {
      const synced = await deps.sync.syncPricingTemplate(id);
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.pricing_template.create",
        targetType: "pricing_template",
        targetId: id,
        metadata: { price_version_id: synced.currentPriceVersionId },
      });
      return c.json(synced, 201);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm.delete(pricingTemplates).where(eq(pricingTemplates.id, id));
      }
      throw err;
    }
  });

  app.patch("/pricing-templates/:id", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, updatePricingTemplateSchema);
    const [existing] = await deps.orm
      .select()
      .from(pricingTemplates)
      .where(eq(pricingTemplates.id, id))
      .limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "Pricing template not found");

    const pricingChanged =
      body.basis !== undefined || body.rates !== undefined || body.ceilings !== undefined;
    const [updated] = await deps.orm
      .update(pricingTemplates)
      .set({
        ...(body.name !== undefined ? { name: body.name } : {}),
        ...(body.basis !== undefined ? { basis: body.basis } : {}),
        ...(body.rates !== undefined ? { rates: body.rates as Pricing["rates"] } : {}),
        ...(body.ceilings !== undefined ? { ceilings: body.ceilings } : {}),
        ...(pricingChanged
          ? {
              pendingPriceVersionId: null,
              pendingPriceRevision: null,
              desiredRevision: sql`${pricingTemplates.desiredRevision} + 1`,
              syncStatus: "PENDING",
            }
          : {}),
        updatedAt: sql`now()`,
      })
      .where(eq(pricingTemplates.id, id))
      .returning();

    const synced = pricingChanged ? await deps.sync.syncPricingTemplate(id) : updated!;
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
    const { limit, offset } = parseLimitOffset(c, 100, 200);
    const items = await deps.orm
      .select()
      .from(modelCatalog)
      .orderBy(desc(modelCatalog.createdAt))
      .limit(limit)
      .offset(offset);
    return c.json({ items, limit, offset });
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
    const [existing] = await deps.orm
      .select()
      .from(modelCatalog)
      .where(eq(modelCatalog.name, body.name))
      .limit(1);
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
    try {
      const synced = await deps.sync.syncModel(body.name);
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.model.upsert",
        targetType: "model",
        targetId: body.name,
      });
      return c.json(synced, existing ? 200 : 201);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500 && !existing) {
        await deps.orm.delete(modelCatalog).where(eq(modelCatalog.name, body.name));
      }
      throw err;
    }
  });

  app.get("/routes", async (c) => {
    const { limit, offset } = parseLimitOffset(c, 100, 200);
    const items = await deps.orm
      .select()
      .from(routes)
      .orderBy(desc(routes.createdAt))
      .limit(limit)
      .offset(offset);
    return c.json({ items, limit, offset });
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
    try {
      const synced = await deps.sync.syncRoute(id);
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.route.create",
        targetType: "route",
        targetId: id,
      });
      return c.json(synced, 201);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm.delete(routes).where(eq(routes.id, id));
      }
      throw err;
    }
  });

  app.patch("/routes/:id", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, updateRouteSchema);
    const [existing] = await deps.orm.select().from(routes).where(eq(routes.id, id)).limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "Route not found");

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
    await deps.orm
      .update(routes)
      .set({
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
      .where(eq(routes.id, id));

    try {
      const synced = await deps.sync.syncRoute(id);
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.route.update",
        targetType: "route",
        targetId: id,
      });
      return c.json(synced);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm
          .update(routes)
          .set({
            channelId: existing.channelId,
            upstreamModel: existing.upstreamModel,
            operations: existing.operations,
            priority: existing.priority,
            weight: existing.weight,
            billingPriceId: existing.billingPriceId,
            costVersionId: existing.costVersionId,
            requestPolicy: existing.requestPolicy,
            adapterOverrides: existing.adapterOverrides,
            retryPolicy: existing.retryPolicy,
            enabled: existing.enabled,
            desiredRevision: existing.desiredRevision,
            syncStatus: existing.syncStatus,
            lastSyncError: existing.lastSyncError,
            updatedAt: sql`now()`,
          })
          .where(eq(routes.id, id));
      }
      throw err;
    }
  });

  app.get("/pricing-overrides", async (c) => {
    const projectId = c.req.query("project_id");
    const { limit, offset } = parseLimitOffset(c, 100, 200);
    const items = projectId
      ? await deps.orm
          .select()
          .from(pricingOverrides)
          .where(eq(pricingOverrides.projectId, parseUuid(projectId, "project_id")))
          .limit(limit)
          .offset(offset)
      : await deps.orm
          .select()
          .from(pricingOverrides)
          .orderBy(desc(pricingOverrides.createdAt))
          .limit(limit)
          .offset(offset);
    return c.json({ items, limit, offset });
  });

  app.put("/pricing-overrides", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, upsertPricingOverrideSchema);
    let billingPriceId = body.billing_price_id ?? null;
    if (body.override_kind === "EXACT_PRICE") {
      if (!billingPriceId && body.billing_template_id) {
        const tpl = await resolveTemplatePriceId(body.billing_template_id, "CUSTOMER");
        billingPriceId = tpl.currentPriceVersionId;
      }
      if (!billingPriceId) {
        throw new GatewayError(400, "validation_error", "billing_price_id is required");
      }
      await ensureCustomerPriceSnapshot(billingPriceId);
    }

    const [existing] = await deps.orm
      .select()
      .from(pricingOverrides)
      .where(
        sql`${pricingOverrides.projectId} = ${body.project_id}::uuid AND ${pricingOverrides.modelName} = ${body.model_name} AND ${pricingOverrides.operation} = ${body.operation}`,
      )
      .limit(1);

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

    try {
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
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        if (existing) {
          await deps.orm
            .update(pricingOverrides)
            .set({
              overrideKind: existing.overrideKind,
              billingPriceId: existing.billingPriceId,
              multiplierNumerator: existing.multiplierNumerator,
              multiplierDenominator: existing.multiplierDenominator,
              enabled: existing.enabled,
              desiredRevision: existing.desiredRevision,
              syncStatus: existing.syncStatus,
              lastSyncError: existing.lastSyncError,
              updatedAt: sql`now()`,
            })
            .where(eq(pricingOverrides.id, existing.id));
        } else {
          await deps.orm.delete(pricingOverrides).where(eq(pricingOverrides.id, row!.id));
        }
      }
      throw err;
    }
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

  return app;
}
