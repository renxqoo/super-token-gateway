import { Hono } from "hono";
import {
  and,
  asc,
  desc,
  eq,
  gt,
  gte,
  inArray,
  lte,
  sql,
  type SQL as DrizzleSQL,
} from "drizzle-orm";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { AMOUNT_DECIMALS } from "@gateway/kernel";
import {
  authenticateSession,
  isSafePublicWebhookUrl,
  recordAuditLog,
  requireProjectAccess,
} from "../auth.ts";
import {
  alertDeliveries,
  alertRules,
  apiKeyRecords,
  modelCatalog,
  priceVersionSnapshots,
  pricingOverrides,
  usageDailyRollups,
  usageHourlyRollups,
} from "../db/schema.ts";
import {
  buildSdkSnippets,
  resolveMarketplacePricing,
  type MarketplaceOverrideRow,
  type MarketplacePriceSnapshot,
} from "../marketplace.ts";
import {
  enrichWalletDecimals,
  nonNegativeAtomsSchema,
  parseIsoDateParam,
  parseIsoDatetimeParam,
  parseJsonBody,
  parseLimitOffset,
  parseUuid,
  requireClientAuth,
  safeFormatAtoms,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";

function sanitizeCustomerCharge(raw: Record<string, unknown>) {
  const customerAtoms = String(raw.customerAtoms ?? raw.customer_atoms ?? "0");
  const fundedAtoms = String(raw.fundedAtoms ?? raw.funded_atoms ?? "0");
  const debtAtoms = String(raw.debtAtoms ?? raw.debt_atoms ?? "0");
  const refundedAtoms = String(raw.refundedAtoms ?? raw.refunded_atoms ?? "0");
  return {
    id: raw.id,
    request_id: raw.requestId ?? raw.request_id,
    project_id: raw.projectId ?? raw.project_id,
    api_key_id: raw.apiKeyId ?? raw.api_key_id ?? null,
    model: raw.model,
    operation: raw.operation,
    currency: raw.currency,
    billing_price_id: raw.billingPriceId ?? raw.billing_price_id,
    pricing_override_id: raw.pricingOverrideId ?? raw.pricing_override_id ?? null,
    discount_numerator: raw.discountNumerator ?? raw.discount_numerator ?? null,
    discount_denominator: raw.discountDenominator ?? raw.discount_denominator ?? null,
    usage: raw.usage,
    scale: AMOUNT_DECIMALS,
    customer_atoms: customerAtoms,
    customer_decimal: safeFormatAtoms(customerAtoms),
    funded_atoms: fundedAtoms,
    funded_decimal: safeFormatAtoms(fundedAtoms),
    debt_atoms: debtAtoms,
    debt_decimal: safeFormatAtoms(debtAtoms),
    refunded_atoms: refundedAtoms,
    refunded_decimal: safeFormatAtoms(refundedAtoms),
    outcome: raw.outcome,
    created_at: raw.createdAt ?? raw.created_at,
  };
}

function sanitizeCustomerTransaction(raw: Record<string, unknown>) {
  const amountAtoms = String(raw.amount_atoms ?? "0");
  const payload =
    raw.payload && typeof raw.payload === "object" ? (raw.payload as Record<string, unknown>) : {};
  const receipt =
    raw.receipt && typeof raw.receipt === "object" ? (raw.receipt as Record<string, unknown>) : {};
  return {
    id: raw.id,
    project_id: raw.project_id ?? null,
    charge_id: raw.charge_id ?? null,
    kind: raw.kind,
    lot_kind: payload.kind ?? payload.lot_kind ?? receipt.kind ?? null,
    external_ref: raw.external_ref,
    currency: raw.currency,
    scale: AMOUNT_DECIMALS,
    amount_atoms: amountAtoms,
    amount_decimal: safeFormatAtoms(amountAtoms),
    reason: raw.reason ?? "",
    created_at: raw.created_at,
  };
}

function sanitizeCustomerAsyncTask(raw: Record<string, unknown>) {
  return {
    id: raw.id,
    request_id: raw.request_id ?? raw.requestId,
    charge_id: raw.charge_id ?? raw.chargeId ?? null,
    project_id: raw.project_id ?? raw.projectId,
    api_key_id: raw.api_key_id ?? raw.apiKeyId ?? null,
    model: raw.model,
    operation: raw.operation,
    status: raw.status,
    user_cancelled: raw.user_cancelled ?? raw.userCancelled ?? false,
    failure_category: raw.failure_category ?? raw.failureCategory ?? null,
    refund_eligibility: raw.refund_eligibility ?? raw.refundEligibility,
    refund_id: raw.refund_id ?? raw.refundId ?? null,
    result_json: raw.result_json ?? raw.resultJson ?? {},
    created_at: raw.created_at ?? raw.createdAt,
    updated_at: raw.updated_at ?? raw.updatedAt,
    completed_at: raw.completed_at ?? raw.completedAt ?? null,
  };
}

export function createMarketplaceAndBillingRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();
  const auth = requireClientAuth(deps);

  const createAlertSchema = z
    .object({
      kind: z.enum(["LOW_BALANCE", "DAILY_SPEND"]),
      threshold_atoms: nonNegativeAtomsSchema,
      webhook_url: z
        .url()
        .max(1000)
        .refine((v) => deps.config.allowPrivateAlertWebhooks || isSafePublicWebhookUrl(v), {
          message: "webhook_url must be a public HTTPS URL",
        })
        .nullable()
        .optional(),
      enabled: z.boolean().default(true),
    })
    .strict();

  async function loadProjectOverridesIfAuthorized(
    authHeader: string | undefined,
    rawProjectId: string | undefined,
  ): Promise<{
    overrides: MarketplaceOverrideRow[];
    exactPrices: Map<string, MarketplacePriceSnapshot>;
  }> {
    if (!rawProjectId) return { overrides: [], exactPrices: new Map() };
    const projectId = parseUuid(rawProjectId, "project_id");
    const user = await authenticateSession(deps.orm, authHeader, "CLIENT");
    await requireProjectAccess(deps.orm, user.id, projectId);
    const rows = await deps.orm
      .select()
      .from(pricingOverrides)
      .where(
        and(
          eq(pricingOverrides.projectId, projectId),
          eq(pricingOverrides.enabled, true),
          gt(pricingOverrides.syncedRevision, 0),
        ),
      );
    const exactPriceIds = rows
      .filter((r) => r.overrideKind === "EXACT_PRICE" && r.billingPriceId)
      .map((r) => r.billingPriceId!);
    const exactPrices = new Map<string, MarketplacePriceSnapshot>();
    if (exactPriceIds.length > 0) {
      const snaps = await deps.orm
        .select({
          id: priceVersionSnapshots.id,
          side: priceVersionSnapshots.side,
          currency: priceVersionSnapshots.currency,
          rates: priceVersionSnapshots.rates,
        })
        .from(priceVersionSnapshots)
        .where(inArray(priceVersionSnapshots.id, exactPriceIds));
      for (const s of snaps) {
        exactPrices.set(s.id, { side: s.side, currency: s.currency, rates: s.rates });
      }
    }
    return { overrides: rows, exactPrices };
  }

  app.get("/models", async (c) => {
    const category = c.req.query("category")?.trim();
    const operation = c.req.query("operation")?.trim();
    const search = c.req.query("search")?.trim().toLowerCase();
    const projectIdParam = c.req.query("project_id")?.trim();

    const { overrides, exactPrices } = await loadProjectOverridesIfAuthorized(
      c.req.header("authorization"),
      projectIdParam,
    );

    const rows = await deps.orm
      .select()
      .from(modelCatalog)
      .where(
        and(
          eq(modelCatalog.enabled, true),
          eq(modelCatalog.isPublic, true),
          gt(modelCatalog.syncedRevision, 0),
        ),
      )
      .orderBy(asc(modelCatalog.providerBrand), asc(modelCatalog.name));

    const filtered = rows.filter((m) => {
      if (category && m.category !== category) return false;
      if (operation && !m.operations.includes(operation)) return false;
      if (
        search &&
        !m.name.toLowerCase().includes(search) &&
        !m.displayName.toLowerCase().includes(search) &&
        !m.providerBrand.toLowerCase().includes(search)
      ) {
        return false;
      }
      return true;
    });

    const items = filtered.map((m) => {
      const primaryOp = operation ?? m.operations[0] ?? "chat";
      const pricing = resolveMarketplacePricing({
        modelName: m.name,
        operation: primaryOp,
        currency: m.currency,
        listRates: m.listRates,
        overrides,
        exactPricesById: exactPrices,
      });
      return {
        name: m.name,
        display_name: m.displayName,
        provider_brand: m.providerBrand,
        description: m.description,
        category: m.category,
        context_length: m.contextLength,
        max_output_tokens: m.maxOutputTokens,
        operations: m.operations,
        tags: m.tags,
        pricing,
      };
    });

    return c.json({ items, total: items.length });
  });

  app.get("/models/:name", async (c) => {
    let modelName: string;
    try {
      modelName = decodeURIComponent(c.req.param("name"));
    } catch {
      throw new GatewayError(400, "validation_error", "Malformed model name encoding");
    }
    const projectIdParam = c.req.query("project_id")?.trim();
    const [m] = await deps.orm
      .select()
      .from(modelCatalog)
      .where(
        and(
          eq(modelCatalog.name, modelName),
          eq(modelCatalog.enabled, true),
          gt(modelCatalog.syncedRevision, 0),
        ),
      )
      .limit(1);
    if (!m || !m.isPublic) throw new GatewayError(404, "not_found", "Model not found");

    const { overrides, exactPrices } = await loadProjectOverridesIfAuthorized(
      c.req.header("authorization"),
      projectIdParam,
    );
    const pricingByOperation: Record<string, ReturnType<typeof resolveMarketplacePricing>> = {};
    for (const op of m.operations) {
      pricingByOperation[op] = resolveMarketplacePricing({
        modelName: m.name,
        operation: op,
        currency: m.currency,
        listRates: m.listRates,
        overrides,
        exactPricesById: exactPrices,
      });
    }

    return c.json({
      name: m.name,
      display_name: m.displayName,
      provider_brand: m.providerBrand,
      description: m.description,
      category: m.category,
      context_length: m.contextLength,
      max_output_tokens: m.maxOutputTokens,
      operations: m.operations,
      tags: m.tags,
      pricing: pricingByOperation[m.operations[0] ?? "chat"] ?? null,
      pricing_by_operation: pricingByOperation,
      sdk_snippets: buildSdkSnippets(deps.config.gatewayPublicUrl, m.name, m.operations),
    });
  });

  app.get("/projects/:projectId/wallet", auth, async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId);
    if (!project.gatewaySynced) {
      return c.json(
        enrichWalletDecimals({
          project_id: project.id,
          currency: project.currency,
          synced: false,
          policy: "GIFT_FIRST",
          available_atoms: "0",
          reserved_atoms: "0",
          spent_atoms: "0",
          expired_atoms: "0",
          debt_atoms: "0",
          credit_limit_atoms: "0",
          credit_reserved_atoms: "0",
          manual_frozen: false,
          freeze_reason: null,
          lots: [],
        }),
      );
    }
    const wallet = await deps.gateway.getWallet(project.gatewayProjectId, project.currency, true);
    return c.json(enrichWalletDecimals({ ...wallet, synced: true }));
  });

  app.get("/projects/:projectId/transactions", auth, async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId);
    const { limit, offset } = parseLimitOffset(c, 50, 200);
    if (!project.gatewaySynced) {
      return c.json({ scale: AMOUNT_DECIMALS, items: [], total: 0, limit, offset });
    }
    const rawKind = c.req.query("kind");
    if (rawKind && !["CREDIT", "REFUND", "ADJUST"].includes(rawKind)) {
      throw new GatewayError(400, "validation_error", "kind must be CREDIT, REFUND, or ADJUST");
    }
    const raw = (await deps.gateway.listFundingOperations({
      side: "CUSTOMER",
      project_id: project.gatewayProjectId,
      kind: rawKind,
      from: c.req.query("from"),
      to: c.req.query("to"),
      limit: String(limit),
      offset: String(offset),
    })) as { items?: Record<string, unknown>[]; total?: number; limit?: number; offset?: number };
    return c.json({
      scale: AMOUNT_DECIMALS,
      items: (raw.items ?? []).map(sanitizeCustomerTransaction),
      total: raw.total ?? 0,
      limit: raw.limit ?? limit,
      offset: raw.offset ?? offset,
    });
  });

  app.get("/projects/:projectId/charges", auth, async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId);
    const { limit, offset } = parseLimitOffset(c, 50, 200);
    if (!project.gatewaySynced) {
      return c.json({ items: [], total: 0, limit, offset });
    }
    const raw = (await deps.gateway.listCharges({
      project_id: project.gatewayProjectId,
      api_key_id: c.req.query("api_key_id"),
      outcome: c.req.query("outcome"),
      from: c.req.query("from"),
      to: c.req.query("to"),
      limit: String(limit),
      offset: String(offset),
    })) as { items?: Record<string, unknown>[]; total?: number; limit?: number; offset?: number };
    return c.json({
      items: (raw.items ?? []).map(sanitizeCustomerCharge),
      total: raw.total ?? 0,
      limit: raw.limit ?? limit,
      offset: raw.offset ?? offset,
    });
  });

  app.get("/projects/:projectId/tasks", auth, async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId);
    const { limit, offset } = parseLimitOffset(c, 50, 200);
    if (!project.gatewaySynced) {
      return c.json({ items: [], total: 0, limit, offset });
    }
    const raw = (await deps.gateway.listAsyncTasks({
      project_id: project.gatewayProjectId,
      status: c.req.query("status"),
      refund_eligibility: c.req.query("refund_eligibility"),
      limit: String(limit),
      offset: String(offset),
    })) as { items?: Record<string, unknown>[]; total?: number; limit?: number; offset?: number };
    return c.json({
      items: (raw.items ?? []).map(sanitizeCustomerAsyncTask),
      total: raw.total ?? 0,
      limit: raw.limit ?? limit,
      offset: raw.offset ?? offset,
    });
  });

  app.get("/projects/:projectId/usage/summary", auth, async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId);
    const from = parseIsoDateParam(c.req.query("from"), "from");
    const to = parseIsoDateParam(c.req.query("to"), "to");
    const conds: DrizzleSQL[] = [
      eq(usageDailyRollups.projectId, projectId),
      eq(usageDailyRollups.currency, project.currency),
    ];
    if (from) conds.push(gte(usageDailyRollups.bucketDay, from));
    if (to) conds.push(lte(usageDailyRollups.bucketDay, to));

    const [row] = await deps.orm
      .select({
        request_count: sql<string>`COALESCE(sum(${usageDailyRollups.requestCount}), 0)::bigint::text`,
        customer_atoms: sql<string>`COALESCE(sum(${usageDailyRollups.customerAtoms}), 0)::text`,
        input_tokens: sql<string>`COALESCE(sum(${usageDailyRollups.inputTokens}), 0)::bigint::text`,
        output_tokens: sql<string>`COALESCE(sum(${usageDailyRollups.outputTokens}), 0)::bigint::text`,
        cached_input_tokens: sql<string>`COALESCE(sum(${usageDailyRollups.cachedInputTokens}), 0)::bigint::text`,
        reasoning_tokens: sql<string>`COALESCE(sum(${usageDailyRollups.reasoningTokens}), 0)::bigint::text`,
      })
      .from(usageDailyRollups)
      .where(and(...conds));

    return c.json({
      currency: project.currency,
      request_count: Number(row?.request_count ?? 0),
      customer_atoms: row?.customer_atoms ?? "0",
      input_tokens: Number(row?.input_tokens ?? 0),
      output_tokens: Number(row?.output_tokens ?? 0),
      cached_input_tokens: Number(row?.cached_input_tokens ?? 0),
      reasoning_tokens: Number(row?.reasoning_tokens ?? 0),
    });
  });

  app.get("/projects/:projectId/usage/timeseries", auth, async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId);
    const granularity = c.req.query("granularity") === "hour" ? "hour" : "day";

    if (granularity === "hour") {
      const from = parseIsoDatetimeParam(c.req.query("from"), "from");
      const to = parseIsoDatetimeParam(c.req.query("to"), "to");
      const conds: DrizzleSQL[] = [
        eq(usageHourlyRollups.projectId, projectId),
        eq(usageHourlyRollups.currency, project.currency),
      ];
      if (from) conds.push(gte(usageHourlyRollups.bucketHour, from));
      if (to) conds.push(lte(usageHourlyRollups.bucketHour, to));
      const rows = await deps.orm
        .select({
          bucket: usageHourlyRollups.bucketHour,
          request_count: sql<string>`sum(${usageHourlyRollups.requestCount})::bigint::text`,
          customer_atoms: sql<string>`sum(${usageHourlyRollups.customerAtoms})::text`,
          input_tokens: sql<string>`sum(${usageHourlyRollups.inputTokens})::bigint::text`,
          output_tokens: sql<string>`sum(${usageHourlyRollups.outputTokens})::bigint::text`,
        })
        .from(usageHourlyRollups)
        .where(and(...conds))
        .groupBy(usageHourlyRollups.bucketHour)
        .orderBy(asc(usageHourlyRollups.bucketHour))
        .limit(744);
      return c.json({
        granularity,
        currency: project.currency,
        items: rows.map((r) => ({
          bucket: r.bucket.toISOString(),
          request_count: Number(r.request_count),
          customer_atoms: r.customer_atoms,
          input_tokens: Number(r.input_tokens),
          output_tokens: Number(r.output_tokens),
        })),
      });
    }

    const from = parseIsoDateParam(c.req.query("from"), "from");
    const to = parseIsoDateParam(c.req.query("to"), "to");
    const conds: DrizzleSQL[] = [
      eq(usageDailyRollups.projectId, projectId),
      eq(usageDailyRollups.currency, project.currency),
    ];
    if (from) conds.push(gte(usageDailyRollups.bucketDay, from));
    if (to) conds.push(lte(usageDailyRollups.bucketDay, to));
    const rows = await deps.orm
      .select({
        bucket: usageDailyRollups.bucketDay,
        request_count: sql<string>`sum(${usageDailyRollups.requestCount})::bigint::text`,
        customer_atoms: sql<string>`sum(${usageDailyRollups.customerAtoms})::text`,
        input_tokens: sql<string>`sum(${usageDailyRollups.inputTokens})::bigint::text`,
        output_tokens: sql<string>`sum(${usageDailyRollups.outputTokens})::bigint::text`,
      })
      .from(usageDailyRollups)
      .where(and(...conds))
      .groupBy(usageDailyRollups.bucketDay)
      .orderBy(asc(usageDailyRollups.bucketDay))
      .limit(366);
    return c.json({
      granularity,
      currency: project.currency,
      items: rows.map((r) => ({
        bucket: r.bucket,
        request_count: Number(r.request_count),
        customer_atoms: r.customer_atoms,
        input_tokens: Number(r.input_tokens),
        output_tokens: Number(r.output_tokens),
      })),
    });
  });

  app.get("/projects/:projectId/usage/by-key", auth, async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId);
    const from = parseIsoDateParam(c.req.query("from"), "from");
    const to = parseIsoDateParam(c.req.query("to"), "to");
    const { limit, offset } = parseLimitOffset(c, 100, 500);
    const conds: DrizzleSQL[] = [
      eq(usageDailyRollups.projectId, projectId),
      eq(usageDailyRollups.currency, project.currency),
    ];
    if (from) conds.push(gte(usageDailyRollups.bucketDay, from));
    if (to) conds.push(lte(usageDailyRollups.bucketDay, to));

    const rows = await deps.orm
      .select({
        api_key_id: usageDailyRollups.apiKeyId,
        request_count: sql<string>`sum(${usageDailyRollups.requestCount})::bigint::text`,
        customer_atoms: sql<string>`sum(${usageDailyRollups.customerAtoms})::text`,
        input_tokens: sql<string>`sum(${usageDailyRollups.inputTokens})::bigint::text`,
        output_tokens: sql<string>`sum(${usageDailyRollups.outputTokens})::bigint::text`,
      })
      .from(usageDailyRollups)
      .where(and(...conds))
      .groupBy(usageDailyRollups.apiKeyId)
      .orderBy(desc(sql`sum(${usageDailyRollups.customerAtoms})`))
      .limit(limit)
      .offset(offset);
    const keyIds = rows.map((r) => r.api_key_id).filter(Boolean);
    const keys =
      keyIds.length > 0
        ? await deps.orm
            .select({
              id: apiKeyRecords.id,
              name: apiKeyRecords.name,
              keyPrefix: apiKeyRecords.keyPrefix,
            })
            .from(apiKeyRecords)
            .where(and(eq(apiKeyRecords.projectId, projectId), inArray(apiKeyRecords.id, keyIds)))
        : [];
    const keyMap = new Map(keys.map((k) => [k.id, k]));
    return c.json({
      currency: project.currency,
      items: rows.map((r) => ({
        api_key_id: r.api_key_id || null,
        request_count: Number(r.request_count),
        customer_atoms: r.customer_atoms,
        input_tokens: Number(r.input_tokens),
        output_tokens: Number(r.output_tokens),
        key_name: r.api_key_id ? (keyMap.get(r.api_key_id)?.name ?? null) : null,
        key_prefix: r.api_key_id ? (keyMap.get(r.api_key_id)?.keyPrefix ?? null) : null,
      })),
      limit,
      offset,
    });
  });

  app.get("/projects/:projectId/usage/by-model", auth, async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId);
    const from = parseIsoDateParam(c.req.query("from"), "from");
    const to = parseIsoDateParam(c.req.query("to"), "to");
    const { limit, offset } = parseLimitOffset(c, 100, 500);
    const conds: DrizzleSQL[] = [
      eq(usageDailyRollups.projectId, projectId),
      eq(usageDailyRollups.currency, project.currency),
    ];
    if (from) conds.push(gte(usageDailyRollups.bucketDay, from));
    if (to) conds.push(lte(usageDailyRollups.bucketDay, to));

    const rows = await deps.orm
      .select({
        model: usageDailyRollups.model,
        operation: usageDailyRollups.operation,
        request_count: sql<string>`sum(${usageDailyRollups.requestCount})::bigint::text`,
        customer_atoms: sql<string>`sum(${usageDailyRollups.customerAtoms})::text`,
        input_tokens: sql<string>`sum(${usageDailyRollups.inputTokens})::bigint::text`,
        output_tokens: sql<string>`sum(${usageDailyRollups.outputTokens})::bigint::text`,
      })
      .from(usageDailyRollups)
      .where(and(...conds))
      .groupBy(usageDailyRollups.model, usageDailyRollups.operation)
      .orderBy(desc(sql`sum(${usageDailyRollups.customerAtoms})`))
      .limit(limit)
      .offset(offset);
    return c.json({
      currency: project.currency,
      items: rows.map((r) => ({
        model: r.model,
        operation: r.operation,
        request_count: Number(r.request_count),
        customer_atoms: r.customer_atoms,
        input_tokens: Number(r.input_tokens),
        output_tokens: Number(r.output_tokens),
      })),
      limit,
      offset,
    });
  });

  app.get("/projects/:projectId/alerts", auth, async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    await requireProjectAccess(deps.orm, user.id, projectId);
    const [rules, deliveries] = await Promise.all([
      deps.orm
        .select()
        .from(alertRules)
        .where(eq(alertRules.projectId, projectId))
        .orderBy(desc(alertRules.createdAt)),
      deps.orm
        .select()
        .from(alertDeliveries)
        .where(eq(alertDeliveries.projectId, projectId))
        .orderBy(desc(alertDeliveries.createdAt))
        .limit(50),
    ]);
    return c.json({ rules, recent_deliveries: deliveries });
  });

  app.post("/projects/:projectId/alerts", auth, async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId, [
      "OWNER",
      "ADMIN",
      "FINANCE",
    ]);
    const body = await parseJsonBody(c, createAlertSchema);
    const ruleId = crypto.randomUUID();
    const [created] = await deps.orm
      .insert(alertRules)
      .values({
        id: ruleId,
        projectId,
        kind: body.kind,
        thresholdAtoms: body.threshold_atoms,
        webhookUrl: body.webhook_url ?? null,
        enabled: body.enabled,
      })
      .returning();
    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: project.orgId,
      action: "alert.create",
      targetType: "alert_rule",
      targetId: ruleId,
    });
    return c.json(created, 201);
  });

  app.delete("/projects/:projectId/alerts/:ruleId", auth, async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const ruleId = parseUuid(c.req.param("ruleId"), "ruleId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId, [
      "OWNER",
      "ADMIN",
      "FINANCE",
    ]);
    const deleted = await deps.orm
      .delete(alertRules)
      .where(and(eq(alertRules.id, ruleId), eq(alertRules.projectId, projectId)))
      .returning({ id: alertRules.id });
    if (deleted.length === 0) throw new GatewayError(404, "not_found", "Alert rule not found");
    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: project.orgId,
      action: "alert.delete",
      targetType: "alert_rule",
      targetId: ruleId,
    });
    return c.json({ ok: true, id: ruleId });
  });

  return app;
}
