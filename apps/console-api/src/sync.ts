import type { SQL } from "bun";
import { and, eq, gt, inArray, lt, or, sql } from "drizzle-orm";
import { GatewayError } from "@gateway/contracts";
import {
  apiKeyRecords,
  channels,
  modelCatalog,
  platformCreditDefaults,
  priceVersionSnapshots,
  pricingOverrides,
  pricingTemplates,
  projects,
  routes,
  upstreamAccounts,
  type ConsoleDrizzle,
} from "./db/schema.ts";
import type { GatewayClient } from "./gateway-client.ts";

const RECONCILE_LEADER_LOCK = 88421002;
const LOCK_NS = {
  project: 88421101,
  apiKey: 88421102,
  account: 88421103,
  channel: 88421104,
  template: 88421105,
  model: 88421106,
  route: 88421107,
  override: 88421108,
} as const;

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message.slice(0, 500) : String(err).slice(0, 500);
}

function isStaleRevisionError(err: unknown): boolean {
  return err instanceof GatewayError && err.status === 409 && err.code === "stale_revision";
}

export class ConsoleSyncEngine {
  constructor(
    private readonly db: SQL,
    private readonly orm: ConsoleDrizzle,
    private readonly gateway: GatewayClient,
  ) {}

  private async withEntityLock<T>(
    namespace: number,
    entityKey: string,
    fn: () => Promise<T>,
  ): Promise<T> {
    const conn = await this.db.reserve();
    let locked = false;
    try {
      await conn`SELECT pg_advisory_lock(${namespace}::int, hashtext(${entityKey}))`;
      locked = true;
      return await fn();
    } finally {
      if (locked) {
        await conn`SELECT pg_advisory_unlock(${namespace}::int, hashtext(${entityKey}))`.catch(
          () => undefined,
        );
      }
      conn.release();
    }
  }

  async ensureProjectSynced(projectId: string) {
    return this.withEntityLock(LOCK_NS.project, projectId, async () => {
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
        const res = await this.gateway.upsertProject({
          id: proj.gatewayProjectId,
          external_ref: `console:org:${proj.orgId}:project:${proj.id}`,
          name: proj.name,
          currency: proj.currency,
          rpm: proj.rpm,
          tpm: proj.tpm,
          concurrency: proj.concurrency,
          enabled: proj.enabled,
          source_revision: targetRev,
        });
        const wasSynced = proj.gatewaySynced;
        const acceptedRev = Math.max(targetRev, res.source_revision ?? targetRev);
        const [updated] = await this.orm
          .update(projects)
          .set({
            gatewaySynced: true,
            syncedRevision: sql`GREATEST(${projects.syncedRevision}, ${acceptedRev})`,
            syncStatus: sql`CASE WHEN ${projects.desiredRevision} <= ${acceptedRev} THEN 'SYNCED' ELSE 'PENDING' END`,
            lastSyncError: sql`CASE WHEN ${projects.desiredRevision} <= ${acceptedRev} THEN NULL ELSE ${projects.lastSyncError} END`,
            updatedAt: sql`now()`,
          })
          .where(eq(projects.id, proj.id))
          .returning();
        if (!wasSynced) {
          const [defaults] = await this.orm
            .select()
            .from(platformCreditDefaults)
            .where(eq(platformCreditDefaults.id, 1))
            .limit(1);
          if (
            defaults &&
            (defaults.defaultCreditLimitAtoms !== "0" ||
              defaults.defaultWalletPolicy !== "GIFT_FIRST")
          ) {
            await this.gateway
              .configureCreditPolicy(proj.gatewayProjectId, {
                currency: proj.currency,
                expected_version: "0",
                credit_limit_atoms: defaults.defaultCreditLimitAtoms,
                policy: defaults.defaultWalletPolicy,
              })
              .catch(() => undefined);
          }
        }
        return updated ?? proj;
      } catch (err) {
        if (isStaleRevisionError(err)) {
          const [latest] = await this.orm
            .select()
            .from(projects)
            .where(eq(projects.id, proj.id))
            .limit(1);
          return latest ?? proj;
        }
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
    });
  }

  async syncApiKey(keyId: string, rawKey?: string) {
    const [initial] = await this.orm
      .select({ projectId: apiKeyRecords.projectId })
      .from(apiKeyRecords)
      .where(eq(apiKeyRecords.id, keyId))
      .limit(1);
    if (!initial) throw new GatewayError(404, "not_found", "API key not found");
    const proj = await this.ensureProjectSynced(initial.projectId);

    return this.withEntityLock(LOCK_NS.apiKey, keyId, async () => {
      const [rec] = await this.orm
        .select()
        .from(apiKeyRecords)
        .where(eq(apiKeyRecords.id, keyId))
        .limit(1);
      if (!rec) throw new GatewayError(404, "not_found", "API key not found");
      const targetRev = rec.desiredRevision;
      const isRevoked = Boolean(rec.revokedAt);
      const expiresAtIso = isRevoked ? null : rec.expiresAt ? rec.expiresAt.toISOString() : null;
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
          expires_at: expiresAtIso,
          spend_limit_atoms: rec.spendLimitAtoms,
          spend_limit_window: rec.spendLimitWindow,
          source_revision: targetRev,
        });
        const acceptedRev = Math.max(targetRev, gwRes.source_revision ?? targetRev);
        const [updated] = await this.orm
          .update(apiKeyRecords)
          .set({
            keyPrefix: gwRes.prefix || rec.keyPrefix,
            syncedRevision: sql`GREATEST(${apiKeyRecords.syncedRevision}, ${acceptedRev})`,
            syncStatus: sql`CASE WHEN ${apiKeyRecords.desiredRevision} <= ${acceptedRev} THEN 'SYNCED' ELSE 'PENDING' END`,
            lastSyncError: sql`CASE WHEN ${apiKeyRecords.desiredRevision} <= ${acceptedRev} THEN NULL ELSE ${apiKeyRecords.lastSyncError} END`,
            updatedAt: sql`now()`,
          })
          .where(eq(apiKeyRecords.id, rec.id))
          .returning();
        return { record: updated ?? rec, key: gwRes.key };
      } catch (err) {
        if (isStaleRevisionError(err)) {
          const [latest] = await this.orm
            .select()
            .from(apiKeyRecords)
            .where(eq(apiKeyRecords.id, rec.id))
            .limit(1);
          return { record: latest ?? rec, key: undefined };
        }
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
    });
  }

  async syncUpstreamAccount(
    accountId: string,
    options?: {
      force?: boolean;
      includeState?: boolean;
      expectedState?: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED";
    },
  ) {
    return this.withEntityLock(LOCK_NS.account, accountId, async () => {
      const [acc] = await this.orm
        .select()
        .from(upstreamAccounts)
        .where(eq(upstreamAccounts.id, accountId))
        .limit(1);
      if (!acc) throw new GatewayError(404, "not_found", "Upstream account not found");
      if (
        !options?.force &&
        acc.syncStatus === "SYNCED" &&
        acc.syncedRevision >= acc.desiredRevision
      ) {
        return acc;
      }
      const targetRev = acc.desiredRevision;
      const shouldSendState = options?.includeState ?? acc.syncedRevision === 0;
      try {
        const res = await this.gateway.upsertUpstreamAccount({
          id: acc.id,
          name: acc.name,
          ...(shouldSendState ? { state: acc.state } : {}),
          ...(options?.expectedState ? { expected_state: options.expectedState } : {}),
          source_revision: targetRev,
        });
        const acceptedRev = Math.max(targetRev, res.source_revision ?? targetRev);
        const [updated] = await this.orm
          .update(upstreamAccounts)
          .set({
            state: res.state,
            syncedRevision: sql`GREATEST(${upstreamAccounts.syncedRevision}, ${acceptedRev})`,
            syncStatus: sql`CASE WHEN ${upstreamAccounts.desiredRevision} <= ${acceptedRev} THEN 'SYNCED' ELSE 'PENDING' END`,
            lastSyncError: sql`CASE WHEN ${upstreamAccounts.desiredRevision} <= ${acceptedRev} THEN NULL ELSE ${upstreamAccounts.lastSyncError} END`,
            updatedAt: sql`now()`,
          })
          .where(eq(upstreamAccounts.id, acc.id))
          .returning();
        return updated ?? acc;
      } catch (err) {
        if (isStaleRevisionError(err)) {
          const [latest] = await this.orm
            .select()
            .from(upstreamAccounts)
            .where(eq(upstreamAccounts.id, acc.id))
            .limit(1);
          return latest ?? acc;
        }
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
    });
  }

  async syncChannel(channelId: string, plainApiKey?: string) {
    const [initial] = await this.orm
      .select({ accountId: channels.accountId })
      .from(channels)
      .where(eq(channels.id, channelId))
      .limit(1);
    if (!initial) throw new GatewayError(404, "not_found", "Channel not found");
    await this.syncUpstreamAccount(initial.accountId, { force: false, includeState: false });

    return this.withEntityLock(LOCK_NS.channel, channelId, async () => {
      const [ch] = await this.orm
        .select()
        .from(channels)
        .where(eq(channels.id, channelId))
        .limit(1);
      if (!ch) throw new GatewayError(404, "not_found", "Channel not found");
      const targetRev = ch.desiredRevision;
      const isCreate = ch.syncedRevision === 0;
      try {
        const gwCh = await this.gateway.upsertChannel({
          id: ch.id,
          account_id: ch.accountId,
          name: ch.name,
          kind: ch.kind,
          ...(isCreate && ch.providerPluginId ? { provider_plugin_id: ch.providerPluginId } : {}),
          base_url: ch.baseUrl,
          ...(plainApiKey !== undefined ? { api_key: plainApiKey } : {}),
          rpm: ch.rpm,
          concurrency: ch.concurrency,
          enabled: ch.enabled,
          routing_policy: ch.routingPolicy,
          adapter_config: ch.adapterConfig,
          source_revision: targetRev,
        });
        const acceptedRev = Math.max(targetRev, gwCh.source_revision ?? targetRev);
        const [updated] = await this.orm
          .update(channels)
          .set({
            providerPluginId: gwCh.provider_plugin_id ?? ch.providerPluginId,
            syncedRevision: sql`GREATEST(${channels.syncedRevision}, ${acceptedRev})`,
            syncStatus: sql`CASE WHEN ${channels.desiredRevision} <= ${acceptedRev} THEN 'SYNCED' ELSE 'PENDING' END`,
            lastSyncError: sql`CASE WHEN ${channels.desiredRevision} <= ${acceptedRev} THEN NULL ELSE ${channels.lastSyncError} END`,
            updatedAt: sql`now()`,
          })
          .where(eq(channels.id, ch.id))
          .returning();
        return updated ?? ch;
      } catch (err) {
        if (isStaleRevisionError(err)) {
          const [latest] = await this.orm
            .select()
            .from(channels)
            .where(eq(channels.id, ch.id))
            .limit(1);
          return latest ?? ch;
        }
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
    });
  }

  async syncPricingTemplate(templateId: string) {
    return this.withEntityLock(LOCK_NS.template, templateId, async () => {
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
      let priceId =
        tpl.pendingPriceVersionId && tpl.pendingPriceRevision === targetRev
          ? tpl.pendingPriceVersionId
          : null;
      if (!priceId) {
        priceId = crypto.randomUUID();
        await this.orm
          .update(pricingTemplates)
          .set({ pendingPriceVersionId: priceId, pendingPriceRevision: targetRev })
          .where(eq(pricingTemplates.id, tpl.id));
      }
      try {
        const created = await this.gateway.createPrice({
          id: priceId,
          side: tpl.side,
          currency: tpl.currency,
          basis: tpl.basis,
          rates: tpl.rates,
          ceilings: tpl.ceilings,
        });
        const resolvedId = created.id;
        const updated = await this.orm.transaction(async (tx) => {
          await tx
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
          const [row] = await tx
            .update(pricingTemplates)
            .set({
              currentPriceVersionId: resolvedId,
              pendingPriceVersionId: sql`CASE WHEN ${pricingTemplates.desiredRevision} <= ${targetRev} THEN NULL ELSE ${pricingTemplates.pendingPriceVersionId} END`,
              pendingPriceRevision: sql`CASE WHEN ${pricingTemplates.desiredRevision} <= ${targetRev} THEN NULL ELSE ${pricingTemplates.pendingPriceRevision} END`,
              syncedRevision: sql`GREATEST(${pricingTemplates.syncedRevision}, ${targetRev})`,
              syncStatus: sql`CASE WHEN ${pricingTemplates.desiredRevision} <= ${targetRev} THEN 'SYNCED' ELSE 'PENDING' END`,
              lastSyncError: sql`CASE WHEN ${pricingTemplates.desiredRevision} <= ${targetRev} THEN NULL ELSE ${pricingTemplates.lastSyncError} END`,
              updatedAt: sql`now()`,
            })
            .where(eq(pricingTemplates.id, tpl.id))
            .returning();
          return row;
        });
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
    });
  }

  async syncModel(modelName: string) {
    return this.withEntityLock(LOCK_NS.model, modelName, async () => {
      const [m] = await this.orm
        .select()
        .from(modelCatalog)
        .where(eq(modelCatalog.name, modelName))
        .limit(1);
      if (!m) throw new GatewayError(404, "not_found", "Model not found");
      const targetRev = m.desiredRevision;
      try {
        const res = await this.gateway.upsertModel({
          name: m.name,
          operations: m.operations,
          routing_strategy: m.routingStrategy,
          max_output_tokens: m.maxOutputTokens,
          enabled: m.enabled,
          source_revision: targetRev,
        });
        const acceptedRev = Math.max(targetRev, res.source_revision ?? targetRev);
        const [updated] = await this.orm
          .update(modelCatalog)
          .set({
            syncedRevision: sql`GREATEST(${modelCatalog.syncedRevision}, ${acceptedRev})`,
            syncStatus: sql`CASE WHEN ${modelCatalog.desiredRevision} <= ${acceptedRev} THEN 'SYNCED' ELSE 'PENDING' END`,
            lastSyncError: sql`CASE WHEN ${modelCatalog.desiredRevision} <= ${acceptedRev} THEN NULL ELSE ${modelCatalog.lastSyncError} END`,
            updatedAt: sql`now()`,
          })
          .where(eq(modelCatalog.name, m.name))
          .returning();
        return updated ?? m;
      } catch (err) {
        if (isStaleRevisionError(err)) {
          const [latest] = await this.orm
            .select()
            .from(modelCatalog)
            .where(eq(modelCatalog.name, m.name))
            .limit(1);
          return latest ?? m;
        }
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
    });
  }

  async syncRoute(routeId: string) {
    return this.withEntityLock(LOCK_NS.route, routeId, async () => {
      const [r] = await this.orm.select().from(routes).where(eq(routes.id, routeId)).limit(1);
      if (!r) throw new GatewayError(404, "not_found", "Route not found");
      const targetRev = r.desiredRevision;
      try {
        const res = await this.gateway.upsertRoute({
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
          source_revision: targetRev,
        });
        const acceptedRev = Math.max(targetRev, res.source_revision ?? targetRev);
        const [updated] = await this.orm
          .update(routes)
          .set({
            syncedRevision: sql`GREATEST(${routes.syncedRevision}, ${acceptedRev})`,
            syncStatus: sql`CASE WHEN ${routes.desiredRevision} <= ${acceptedRev} THEN 'SYNCED' ELSE 'PENDING' END`,
            lastSyncError: sql`CASE WHEN ${routes.desiredRevision} <= ${acceptedRev} THEN NULL ELSE ${routes.lastSyncError} END`,
            updatedAt: sql`now()`,
          })
          .where(eq(routes.id, r.id))
          .returning();
        return updated ?? r;
      } catch (err) {
        if (isStaleRevisionError(err)) {
          const [latest] = await this.orm.select().from(routes).where(eq(routes.id, r.id)).limit(1);
          return latest ?? r;
        }
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
    });
  }

  async syncPricingOverride(overrideId: string) {
    const [initial] = await this.orm
      .select({ projectId: pricingOverrides.projectId })
      .from(pricingOverrides)
      .where(eq(pricingOverrides.id, overrideId))
      .limit(1);
    if (!initial) throw new GatewayError(404, "not_found", "Pricing override not found");
    const proj = await this.ensureProjectSynced(initial.projectId);

    return this.withEntityLock(LOCK_NS.override, overrideId, async () => {
      const [o] = await this.orm
        .select()
        .from(pricingOverrides)
        .where(eq(pricingOverrides.id, overrideId))
        .limit(1);
      if (!o) throw new GatewayError(404, "not_found", "Pricing override not found");
      const targetRev = o.desiredRevision;
      try {
        const res = await this.gateway.upsertPricingOverride({
          id: o.id,
          project_id: proj.gatewayProjectId,
          model_name: o.modelName,
          operation: o.operation,
          override_kind: o.overrideKind,
          billing_price_id: o.billingPriceId,
          multiplier_numerator: o.multiplierNumerator,
          multiplier_denominator: o.multiplierDenominator,
          enabled: o.enabled,
          source_revision: targetRev,
        });
        const acceptedRev = Math.max(targetRev, res.source_revision ?? targetRev);
        const [updated] = await this.orm
          .update(pricingOverrides)
          .set({
            syncedRevision: sql`GREATEST(${pricingOverrides.syncedRevision}, ${acceptedRev})`,
            syncStatus: sql`CASE WHEN ${pricingOverrides.desiredRevision} <= ${acceptedRev} THEN 'SYNCED' ELSE 'PENDING' END`,
            lastSyncError: sql`CASE WHEN ${pricingOverrides.desiredRevision} <= ${acceptedRev} THEN NULL ELSE ${pricingOverrides.lastSyncError} END`,
            updatedAt: sql`now()`,
          })
          .where(eq(pricingOverrides.id, o.id))
          .returning();
        return updated ?? o;
      } catch (err) {
        if (isStaleRevisionError(err)) {
          const [latest] = await this.orm
            .select()
            .from(pricingOverrides)
            .where(eq(pricingOverrides.id, o.id))
            .limit(1);
          return latest ?? o;
        }
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
    });
  }

  async reconcilePending(signal?: AbortSignal): Promise<{ synced: number; failed: number }> {
    const conn = await this.db.reserve();
    let leader = false;
    try {
      const [row] = await conn`SELECT pg_try_advisory_lock(${RECONCILE_LEADER_LOCK}) AS locked`;
      leader = Boolean(row?.locked);
      if (!leader) return { synced: 0, failed: 0 };

      let synced = 0;
      let failed = 0;
      const states: ("PENDING" | "FAILED")[] = ["PENDING", "FAILED"];
      const batchLimit = 100;

      const accs = await this.orm
        .select({ id: upstreamAccounts.id })
        .from(upstreamAccounts)
        .where(
          or(
            inArray(upstreamAccounts.syncStatus, states),
            lt(upstreamAccounts.syncedRevision, upstreamAccounts.desiredRevision),
          ),
        )
        .limit(batchLimit);
      for (const a of accs) {
        if (signal?.aborted) return { synced, failed };
        try {
          await this.syncUpstreamAccount(a.id, { force: false, includeState: false });
          synced++;
        } catch {
          failed++;
        }
      }

      const chs = await this.orm
        .select({ id: channels.id })
        .from(channels)
        .where(
          and(
            gt(channels.syncedRevision, 0),
            or(
              inArray(channels.syncStatus, states),
              lt(channels.syncedRevision, channels.desiredRevision),
            ),
          ),
        )
        .limit(batchLimit);
      for (const c of chs) {
        if (signal?.aborted) return { synced, failed };
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
        )
        .limit(batchLimit);
      for (const t of tpls) {
        if (signal?.aborted) return { synced, failed };
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
        )
        .limit(batchLimit);
      for (const m of mods) {
        if (signal?.aborted) return { synced, failed };
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
        )
        .limit(batchLimit);
      for (const r of rts) {
        if (signal?.aborted) return { synced, failed };
        try {
          await this.syncRoute(r.id);
          synced++;
        } catch {
          failed++;
        }
      }

      const projs = await this.orm
        .select({ id: projects.id })
        .from(projects)
        .where(
          or(
            eq(projects.syncStatus, "FAILED"),
            and(
              eq(projects.gatewaySynced, true),
              or(
                inArray(projects.syncStatus, states),
                lt(projects.syncedRevision, projects.desiredRevision),
              ),
            ),
          ),
        )
        .limit(batchLimit);
      for (const p of projs) {
        if (signal?.aborted) return { synced, failed };
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
        )
        .limit(batchLimit);
      for (const o of ovrs) {
        if (signal?.aborted) return { synced, failed };
        try {
          await this.syncPricingOverride(o.id);
          synced++;
        } catch {
          failed++;
        }
      }

      const keys = await this.orm
        .select({ id: apiKeyRecords.id })
        .from(apiKeyRecords)
        .where(
          and(
            gt(apiKeyRecords.syncedRevision, 0),
            or(
              inArray(apiKeyRecords.syncStatus, states),
              lt(apiKeyRecords.syncedRevision, apiKeyRecords.desiredRevision),
            ),
          ),
        )
        .limit(batchLimit);
      for (const k of keys) {
        if (signal?.aborted) return { synced, failed };
        try {
          await this.syncApiKey(k.id);
          synced++;
        } catch {
          failed++;
        }
      }

      if (!signal?.aborted) {
        await this.db`
          DELETE FROM public.sessions
          WHERE (revoked_at IS NOT NULL AND revoked_at < now() - interval '30 days')
             OR refresh_expires_at < now() - interval '7 days'
        `.catch(() => undefined);
      }

      return { synced, failed };
    } finally {
      if (leader) {
        await conn`SELECT pg_advisory_unlock(${RECONCILE_LEADER_LOCK})`.catch(() => undefined);
      }
      conn.release();
    }
  }
}
