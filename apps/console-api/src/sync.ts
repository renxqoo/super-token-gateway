import { eq, inArray, lt, or, sql } from "drizzle-orm";
import { GatewayError } from "@gateway/contracts";
import {
  apiKeyRecords,
  channels,
  modelCatalog,
  priceVersionSnapshots,
  pricingOverrides,
  pricingTemplates,
  projects,
  routes,
  upstreamAccounts,
  type ConsoleDrizzle,
} from "./db/schema.ts";
import type { GatewayClient } from "./gateway-client.ts";

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message.slice(0, 500) : String(err).slice(0, 500);
}

export class ConsoleSyncEngine {
  constructor(
    private readonly orm: ConsoleDrizzle,
    private readonly gateway: GatewayClient,
  ) {}

  async ensureProjectSynced(projectId: string) {
    const [proj] = await this.orm
      .select()
      .from(projects)
      .where(eq(projects.id, projectId))
      .limit(1);
    if (!proj) throw new GatewayError(404, "not_found", "Project not found");
    if (
      proj.gatewaySynced &&
      proj.syncStatus === "SYNCED" &&
      proj.syncedRevision >= proj.desiredRevision
    ) {
      return proj;
    }
    const targetRev = proj.desiredRevision;
    try {
      await this.gateway.upsertProject({
        id: proj.gatewayProjectId,
        external_ref: `console:org:${proj.orgId}:project:${proj.id}`,
        name: proj.name,
        currency: proj.currency,
        rpm: proj.rpm,
        tpm: proj.tpm,
        concurrency: proj.concurrency,
        enabled: proj.enabled,
      });
      const [updated] = await this.orm
        .update(projects)
        .set({
          gatewaySynced: true,
          syncedRevision: sql`GREATEST(${projects.syncedRevision}, ${targetRev})`,
          syncStatus: sql`CASE WHEN ${projects.desiredRevision} <= ${targetRev} THEN 'SYNCED' ELSE 'PENDING' END`,
          lastSyncError: sql`CASE WHEN ${projects.desiredRevision} <= ${targetRev} THEN NULL ELSE ${projects.lastSyncError} END`,
          updatedAt: sql`now()`,
        })
        .where(eq(projects.id, proj.id))
        .returning();
      return updated ?? proj;
    } catch (err) {
      await this.orm
        .update(projects)
        .set({
          syncStatus: sql`CASE WHEN ${projects.desiredRevision} <= ${targetRev} THEN 'FAILED' ELSE 'PENDING' END`,
          lastSyncError: errMsg(err),
          updatedAt: sql`now()`,
        })
        .where(eq(projects.id, proj.id));
      throw err;
    }
  }

  async syncApiKey(keyId: string, rawKey?: string) {
    const [rec] = await this.orm
      .select()
      .from(apiKeyRecords)
      .where(eq(apiKeyRecords.id, keyId))
      .limit(1);
    if (!rec) throw new GatewayError(404, "not_found", "API key not found");
    const proj = await this.ensureProjectSynced(rec.projectId);
    const targetRev = rec.desiredRevision;
    const isRevoked = Boolean(rec.revokedAt);
    try {
      const gwRes = await this.gateway.upsertApiKey({
        id: rec.id,
        project_id: proj.gatewayProjectId,
        name: rec.name,
        ...(rawKey ? { raw_key: rawKey } : {}),
        role: rec.role,
        allowed_cidrs: isRevoked ? [] : rec.allowedCidrs,
        models: rec.models,
        operations: rec.operations,
        enabled: rec.enabled && !isRevoked,
        expires_at: rec.expiresAt,
        spend_limit_atoms: rec.spendLimitAtoms,
        spend_limit_window: rec.spendLimitWindow,
      });
      const [updated] = await this.orm
        .update(apiKeyRecords)
        .set({
          keyPrefix: gwRes.prefix || rec.keyPrefix,
          syncedRevision: sql`GREATEST(${apiKeyRecords.syncedRevision}, ${targetRev})`,
          syncStatus: sql`CASE WHEN ${apiKeyRecords.desiredRevision} <= ${targetRev} THEN 'SYNCED' ELSE 'PENDING' END`,
          lastSyncError: sql`CASE WHEN ${apiKeyRecords.desiredRevision} <= ${targetRev} THEN NULL ELSE ${apiKeyRecords.lastSyncError} END`,
          updatedAt: sql`now()`,
        })
        .where(eq(apiKeyRecords.id, rec.id))
        .returning();
      return { record: updated ?? rec, key: gwRes.key };
    } catch (err) {
      await this.orm
        .update(apiKeyRecords)
        .set({
          syncStatus: sql`CASE WHEN ${apiKeyRecords.desiredRevision} <= ${targetRev} THEN 'FAILED' ELSE 'PENDING' END`,
          lastSyncError: errMsg(err),
          updatedAt: sql`now()`,
        })
        .where(eq(apiKeyRecords.id, rec.id));
      throw err;
    }
  }

  async syncUpstreamAccount(accountId: string, force = false) {
    const [acc] = await this.orm
      .select()
      .from(upstreamAccounts)
      .where(eq(upstreamAccounts.id, accountId))
      .limit(1);
    if (!acc) throw new GatewayError(404, "not_found", "Upstream account not found");
    if (!force && acc.syncStatus === "SYNCED" && acc.syncedRevision >= acc.desiredRevision) {
      return acc;
    }
    const targetRev = acc.desiredRevision;
    try {
      await this.gateway.upsertUpstreamAccount({
        id: acc.id,
        name: acc.name,
        state: acc.state,
      });
      const [updated] = await this.orm
        .update(upstreamAccounts)
        .set({
          syncedRevision: sql`GREATEST(${upstreamAccounts.syncedRevision}, ${targetRev})`,
          syncStatus: sql`CASE WHEN ${upstreamAccounts.desiredRevision} <= ${targetRev} THEN 'SYNCED' ELSE 'PENDING' END`,
          lastSyncError: sql`CASE WHEN ${upstreamAccounts.desiredRevision} <= ${targetRev} THEN NULL ELSE ${upstreamAccounts.lastSyncError} END`,
          updatedAt: sql`now()`,
        })
        .where(eq(upstreamAccounts.id, acc.id))
        .returning();
      return updated ?? acc;
    } catch (err) {
      await this.orm
        .update(upstreamAccounts)
        .set({
          syncStatus: sql`CASE WHEN ${upstreamAccounts.desiredRevision} <= ${targetRev} THEN 'FAILED' ELSE 'PENDING' END`,
          lastSyncError: errMsg(err),
          updatedAt: sql`now()`,
        })
        .where(eq(upstreamAccounts.id, acc.id));
      throw err;
    }
  }

  async syncChannel(channelId: string, plainApiKey?: string) {
    const [ch] = await this.orm.select().from(channels).where(eq(channels.id, channelId)).limit(1);
    if (!ch) throw new GatewayError(404, "not_found", "Channel not found");
    await this.syncUpstreamAccount(ch.accountId, false);
    const targetRev = ch.desiredRevision;
    try {
      const gwCh = await this.gateway.upsertChannel({
        id: ch.id,
        account_id: ch.accountId,
        name: ch.name,
        kind: ch.kind,
        ...(ch.providerPluginId ? { provider_plugin_id: ch.providerPluginId } : {}),
        base_url: ch.baseUrl,
        ...(plainApiKey !== undefined ? { api_key: plainApiKey } : {}),
        rpm: ch.rpm,
        concurrency: ch.concurrency,
        enabled: ch.enabled,
        routing_policy: ch.routingPolicy,
        adapter_config: ch.adapterConfig,
      });
      const [updated] = await this.orm
        .update(channels)
        .set({
          providerPluginId: gwCh.provider_plugin_id ?? ch.providerPluginId,
          syncedRevision: sql`GREATEST(${channels.syncedRevision}, ${targetRev})`,
          syncStatus: sql`CASE WHEN ${channels.desiredRevision} <= ${targetRev} THEN 'SYNCED' ELSE 'PENDING' END`,
          lastSyncError: sql`CASE WHEN ${channels.desiredRevision} <= ${targetRev} THEN NULL ELSE ${channels.lastSyncError} END`,
          updatedAt: sql`now()`,
        })
        .where(eq(channels.id, ch.id))
        .returning();
      return updated ?? ch;
    } catch (err) {
      await this.orm
        .update(channels)
        .set({
          syncStatus: sql`CASE WHEN ${channels.desiredRevision} <= ${targetRev} THEN 'FAILED' ELSE 'PENDING' END`,
          lastSyncError: errMsg(err),
          updatedAt: sql`now()`,
        })
        .where(eq(channels.id, ch.id));
      throw err;
    }
  }

  async syncPricingTemplate(templateId: string) {
    const [tpl] = await this.orm
      .select()
      .from(pricingTemplates)
      .where(eq(pricingTemplates.id, templateId))
      .limit(1);
    if (!tpl) throw new GatewayError(404, "not_found", "Pricing template not found");
    if (
      tpl.currentPriceVersionId &&
      tpl.syncStatus === "SYNCED" &&
      tpl.syncedRevision >= tpl.desiredRevision
    ) {
      return tpl;
    }
    const targetRev = tpl.desiredRevision;
    let priceId = tpl.pendingPriceVersionId;
    if (!priceId) {
      priceId = crypto.randomUUID();
      await this.orm
        .update(pricingTemplates)
        .set({ pendingPriceVersionId: priceId })
        .where(eq(pricingTemplates.id, tpl.id));
    }
    try {
      let resolvedId = priceId;
      try {
        const created = await this.gateway.createPrice({
          id: priceId,
          side: tpl.side,
          currency: tpl.currency,
          basis: tpl.basis,
          rates: tpl.rates,
          ceilings: tpl.ceilings,
        });
        resolvedId = created.id;
      } catch (err) {
        if (!(err instanceof GatewayError && err.status === 409)) throw err;
      }
      await this.orm
        .insert(priceVersionSnapshots)
        .values({
          id: resolvedId,
          templateId: tpl.id,
          side: tpl.side,
          currency: tpl.currency,
          basis: tpl.basis,
          rates: tpl.rates,
          ceilings: tpl.ceilings,
        })
        .onConflictDoNothing({ target: priceVersionSnapshots.id });
      const [updated] = await this.orm
        .update(pricingTemplates)
        .set({
          currentPriceVersionId: resolvedId,
          pendingPriceVersionId: null,
          syncedRevision: sql`GREATEST(${pricingTemplates.syncedRevision}, ${targetRev})`,
          syncStatus: sql`CASE WHEN ${pricingTemplates.desiredRevision} <= ${targetRev} THEN 'SYNCED' ELSE 'PENDING' END`,
          lastSyncError: sql`CASE WHEN ${pricingTemplates.desiredRevision} <= ${targetRev} THEN NULL ELSE ${pricingTemplates.lastSyncError} END`,
          updatedAt: sql`now()`,
        })
        .where(eq(pricingTemplates.id, tpl.id))
        .returning();
      return updated ?? tpl;
    } catch (err) {
      await this.orm
        .update(pricingTemplates)
        .set({
          syncStatus: sql`CASE WHEN ${pricingTemplates.desiredRevision} <= ${targetRev} THEN 'FAILED' ELSE 'PENDING' END`,
          lastSyncError: errMsg(err),
          updatedAt: sql`now()`,
        })
        .where(eq(pricingTemplates.id, tpl.id));
      throw err;
    }
  }

  async syncModel(modelName: string) {
    const [m] = await this.orm
      .select()
      .from(modelCatalog)
      .where(eq(modelCatalog.name, modelName))
      .limit(1);
    if (!m) throw new GatewayError(404, "not_found", "Model not found");
    const targetRev = m.desiredRevision;
    try {
      await this.gateway.upsertModel({
        name: m.name,
        operations: m.operations,
        routing_strategy: m.routingStrategy,
        max_output_tokens: m.maxOutputTokens,
        enabled: m.enabled,
      });
      const [updated] = await this.orm
        .update(modelCatalog)
        .set({
          syncedRevision: sql`GREATEST(${modelCatalog.syncedRevision}, ${targetRev})`,
          syncStatus: sql`CASE WHEN ${modelCatalog.desiredRevision} <= ${targetRev} THEN 'SYNCED' ELSE 'PENDING' END`,
          lastSyncError: sql`CASE WHEN ${modelCatalog.desiredRevision} <= ${targetRev} THEN NULL ELSE ${modelCatalog.lastSyncError} END`,
          updatedAt: sql`now()`,
        })
        .where(eq(modelCatalog.name, m.name))
        .returning();
      return updated ?? m;
    } catch (err) {
      await this.orm
        .update(modelCatalog)
        .set({
          syncStatus: sql`CASE WHEN ${modelCatalog.desiredRevision} <= ${targetRev} THEN 'FAILED' ELSE 'PENDING' END`,
          lastSyncError: errMsg(err),
          updatedAt: sql`now()`,
        })
        .where(eq(modelCatalog.name, m.name));
      throw err;
    }
  }

  async syncRoute(routeId: string) {
    const [r] = await this.orm.select().from(routes).where(eq(routes.id, routeId)).limit(1);
    if (!r) throw new GatewayError(404, "not_found", "Route not found");
    const targetRev = r.desiredRevision;
    try {
      await this.gateway.upsertRoute({
        id: r.id,
        model_name: r.modelName,
        channel_id: r.channelId,
        upstream_model: r.upstreamModel,
        operations: r.operations,
        priority: r.priority,
        weight: r.weight,
        billing_price_id: r.billingPriceId,
        cost_version_id: r.costVersionId,
        request_policy: r.requestPolicy,
        adapter_overrides: r.adapterOverrides,
        retry_policy: r.retryPolicy,
        enabled: r.enabled,
      });
      const [updated] = await this.orm
        .update(routes)
        .set({
          syncedRevision: sql`GREATEST(${routes.syncedRevision}, ${targetRev})`,
          syncStatus: sql`CASE WHEN ${routes.desiredRevision} <= ${targetRev} THEN 'SYNCED' ELSE 'PENDING' END`,
          lastSyncError: sql`CASE WHEN ${routes.desiredRevision} <= ${targetRev} THEN NULL ELSE ${routes.lastSyncError} END`,
          updatedAt: sql`now()`,
        })
        .where(eq(routes.id, r.id))
        .returning();
      return updated ?? r;
    } catch (err) {
      await this.orm
        .update(routes)
        .set({
          syncStatus: sql`CASE WHEN ${routes.desiredRevision} <= ${targetRev} THEN 'FAILED' ELSE 'PENDING' END`,
          lastSyncError: errMsg(err),
          updatedAt: sql`now()`,
        })
        .where(eq(routes.id, r.id));
      throw err;
    }
  }

  async syncPricingOverride(overrideId: string) {
    const [o] = await this.orm
      .select()
      .from(pricingOverrides)
      .where(eq(pricingOverrides.id, overrideId))
      .limit(1);
    if (!o) throw new GatewayError(404, "not_found", "Pricing override not found");
    const proj = await this.ensureProjectSynced(o.projectId);
    const targetRev = o.desiredRevision;
    try {
      await this.gateway.upsertPricingOverride({
        id: o.id,
        project_id: proj.gatewayProjectId,
        model_name: o.modelName,
        operation: o.operation,
        override_kind: o.overrideKind,
        billing_price_id: o.billingPriceId,
        multiplier_numerator: o.multiplierNumerator,
        multiplier_denominator: o.multiplierDenominator,
        enabled: o.enabled,
      });
      const [updated] = await this.orm
        .update(pricingOverrides)
        .set({
          syncedRevision: sql`GREATEST(${pricingOverrides.syncedRevision}, ${targetRev})`,
          syncStatus: sql`CASE WHEN ${pricingOverrides.desiredRevision} <= ${targetRev} THEN 'SYNCED' ELSE 'PENDING' END`,
          lastSyncError: sql`CASE WHEN ${pricingOverrides.desiredRevision} <= ${targetRev} THEN NULL ELSE ${pricingOverrides.lastSyncError} END`,
          updatedAt: sql`now()`,
        })
        .where(eq(pricingOverrides.id, o.id))
        .returning();
      return updated ?? o;
    } catch (err) {
      await this.orm
        .update(pricingOverrides)
        .set({
          syncStatus: sql`CASE WHEN ${pricingOverrides.desiredRevision} <= ${targetRev} THEN 'FAILED' ELSE 'PENDING' END`,
          lastSyncError: errMsg(err),
          updatedAt: sql`now()`,
        })
        .where(eq(pricingOverrides.id, o.id));
      throw err;
    }
  }

  async reconcilePending(): Promise<{ synced: number; failed: number }> {
    let synced = 0;
    let failed = 0;
    const states: ("PENDING" | "FAILED")[] = ["PENDING", "FAILED"];

    const accs = await this.orm
      .select({ id: upstreamAccounts.id })
      .from(upstreamAccounts)
      .where(
        or(
          inArray(upstreamAccounts.syncStatus, states),
          lt(upstreamAccounts.syncedRevision, upstreamAccounts.desiredRevision),
        ),
      );
    for (const a of accs) {
      try {
        await this.syncUpstreamAccount(a.id, false);
        synced++;
      } catch {
        failed++;
      }
    }

    const chs = await this.orm
      .select({ id: channels.id, syncedRevision: channels.syncedRevision })
      .from(channels)
      .where(
        or(
          inArray(channels.syncStatus, states),
          lt(channels.syncedRevision, channels.desiredRevision),
        ),
      );
    for (const c of chs) {
      if (c.syncedRevision === 0) continue;
      try {
        await this.syncChannel(c.id);
        synced++;
      } catch {
        failed++;
      }
    }

    const tpls = await this.orm
      .select({ id: pricingTemplates.id })
      .from(pricingTemplates)
      .where(
        or(
          inArray(pricingTemplates.syncStatus, states),
          lt(pricingTemplates.syncedRevision, pricingTemplates.desiredRevision),
        ),
      );
    for (const t of tpls) {
      try {
        await this.syncPricingTemplate(t.id);
        synced++;
      } catch {
        failed++;
      }
    }

    const mods = await this.orm
      .select({ name: modelCatalog.name })
      .from(modelCatalog)
      .where(
        or(
          inArray(modelCatalog.syncStatus, states),
          lt(modelCatalog.syncedRevision, modelCatalog.desiredRevision),
        ),
      );
    for (const m of mods) {
      try {
        await this.syncModel(m.name);
        synced++;
      } catch {
        failed++;
      }
    }

    const rts = await this.orm
      .select({ id: routes.id })
      .from(routes)
      .where(
        or(inArray(routes.syncStatus, states), lt(routes.syncedRevision, routes.desiredRevision)),
      );
    for (const r of rts) {
      try {
        await this.syncRoute(r.id);
        synced++;
      } catch {
        failed++;
      }
    }

    const projs = await this.orm
      .select({ id: projects.id, gatewaySynced: projects.gatewaySynced })
      .from(projects)
      .where(
        or(
          inArray(projects.syncStatus, states),
          lt(projects.syncedRevision, projects.desiredRevision),
        ),
      );
    for (const p of projs) {
      if (!p.gatewaySynced) continue;
      try {
        await this.ensureProjectSynced(p.id);
        synced++;
      } catch {
        failed++;
      }
    }

    const ovrs = await this.orm
      .select({ id: pricingOverrides.id })
      .from(pricingOverrides)
      .where(
        or(
          inArray(pricingOverrides.syncStatus, states),
          lt(pricingOverrides.syncedRevision, pricingOverrides.desiredRevision),
        ),
      );
    for (const o of ovrs) {
      try {
        await this.syncPricingOverride(o.id);
        synced++;
      } catch {
        failed++;
      }
    }

    const keys = await this.orm
      .select({ id: apiKeyRecords.id, syncedRevision: apiKeyRecords.syncedRevision })
      .from(apiKeyRecords)
      .where(
        or(
          inArray(apiKeyRecords.syncStatus, states),
          lt(apiKeyRecords.syncedRevision, apiKeyRecords.desiredRevision),
        ),
      );
    for (const k of keys) {
      if (k.syncedRevision === 0) continue;
      try {
        await this.syncApiKey(k.id);
        synced++;
      } catch {
        failed++;
      }
    }

    return { synced, failed };
  }
}
