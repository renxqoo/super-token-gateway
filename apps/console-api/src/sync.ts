import type { SQL } from "bun";
import { and, asc, eq, gt, inArray, isNotNull, lt, or, sql } from "drizzle-orm";
import { GatewayError } from "@gateway/contracts";
import { maskSecret } from "./auth.ts";
import {
  apiKeyRecords,
  channels,
  modelCatalog,
  organizations,
  platformCreditDefaults,
  priceVersionSnapshots,
  pricingOverrides,
  pricingTemplates,
  billingAccounts,
  routes,
  upstreamAccounts,
  createConsoleDrizzle,
  type ConsoleDrizzle,
} from "./db/schema.ts";
import type { GatewayClient } from "./gateway-client.ts";

const RECONCILE_LEADER_LOCK = 88421002;
const LOCK_NS = {
  billingAccount: 88421101,
  apiKey: 88421102,
  account: 88421103,
  channel: 88421104,
  template: 88421105,
  model: 88421106,
  route: 88421107,
  override: 88421108,
  organization: 88421109,
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
    fn: (orm: ConsoleDrizzle) => Promise<T>,
  ): Promise<T> {
    const conn = await this.db.reserve();
    let locked = false;
    try {
      await conn`SELECT pg_advisory_lock(${namespace}::int, hashtext(${entityKey}))`;
      locked = true;
      return await fn(createConsoleDrizzle(conn));
    } finally {
      if (locked) {
        await conn`SELECT pg_advisory_unlock(${namespace}::int, hashtext(${entityKey}))`.catch(
          () => undefined,
        );
      }
      conn.release();
    }
  }

  async ensureBillingAccountSynced(billingAccountId: string) {
    return this.withEntityLock(LOCK_NS.billingAccount, billingAccountId, async (orm) => {
      const [billingAccount] = await orm
        .select()
        .from(billingAccounts)
        .where(eq(billingAccounts.id, billingAccountId))
        .limit(1);
      if (!billingAccount) throw new GatewayError(404, "not_found", "Billing account not found");
      if (
        billingAccount.gatewaySynced &&
        billingAccount.syncStatus === "SYNCED" &&
        billingAccount.syncedRevision >= billingAccount.desiredRevision
      ) {
        return billingAccount;
      }
      const targetRev = billingAccount.desiredRevision;
      try {
        const res = await this.gateway.upsertBillingAccount({
          id: billingAccount.gatewayBillingAccountId,
          external_ref: `console:org:${billingAccount.orgId}:billing-account:${billingAccount.id}`,
          name: billingAccount.name,
          currency: billingAccount.currency,
          rpm: billingAccount.rpm,
          tpm: billingAccount.tpm,
          concurrency: billingAccount.concurrency,
          enabled: billingAccount.enabled,
          source_revision: targetRev,
        });
        if (!billingAccount.gatewaySynced) {
          const [defaults] = await orm
            .select()
            .from(platformCreditDefaults)
            .where(eq(platformCreditDefaults.id, 1))
            .limit(1);
          if (
            defaults &&
            (defaults.defaultCreditLimitAtoms !== "0" ||
              defaults.defaultWalletPolicy !== "GIFT_FIRST")
          ) {
            const wallet = await this.gateway.getWallet(
              billingAccount.gatewayBillingAccountId,
              billingAccount.currency,
            );
            if (wallet.credit_policy_version === "0") {
              await this.gateway.configureCreditPolicy(billingAccount.gatewayBillingAccountId, {
                currency: billingAccount.currency,
                expected_version: "0",
                credit_limit_atoms: defaults.defaultCreditLimitAtoms,
                policy: defaults.defaultWalletPolicy,
              });
            }
          }
        }
        const acceptedRev = Math.max(targetRev, res.source_revision ?? targetRev);
        const [updated] = await orm
          .update(billingAccounts)
          .set({
            gatewaySynced: true,
            syncedRevision: sql`GREATEST(${billingAccounts.syncedRevision}, ${acceptedRev})`,
            syncStatus: sql`CASE WHEN ${billingAccounts.desiredRevision} <= ${acceptedRev} THEN 'SYNCED' ELSE 'PENDING' END`,
            lastSyncError: sql`CASE WHEN ${billingAccounts.desiredRevision} <= ${acceptedRev} THEN NULL ELSE ${billingAccounts.lastSyncError} END`,
            updatedAt: sql`now()`,
          })
          .where(eq(billingAccounts.id, billingAccount.id))
          .returning();
        return updated ?? billingAccount;
      } catch (err) {
        if (isStaleRevisionError(err)) {
          const [latest] = await orm
            .select()
            .from(billingAccounts)
            .where(eq(billingAccounts.id, billingAccount.id))
            .limit(1);
          return latest ?? billingAccount;
        }
        await orm
          .update(billingAccounts)
          .set({
            syncStatus: sql`CASE WHEN ${billingAccounts.desiredRevision} <= ${targetRev} THEN 'FAILED' ELSE 'PENDING' END`,
            lastSyncError: errMsg(err),
            updatedAt: sql`now()`,
          })
          .where(eq(billingAccounts.id, billingAccount.id));
        throw err;
      }
    });
  }

  async changeOrganizationStatus(organizationId: string, status: "ACTIVE" | "SUSPENDED") {
    return this.withEntityLock(LOCK_NS.organization, organizationId.toLowerCase(), async (orm) => {
      const [organization] = await orm
        .update(organizations)
        .set({
          inferenceTargetStatus: status,
          inferenceRevision: sql`CASE WHEN ${organizations.inferenceTargetStatus} <> ${status}
          THEN ${organizations.inferenceRevision} + 1 ELSE ${organizations.inferenceRevision} END`,
          updatedAt: sql`now()`,
        })
        .where(eq(organizations.id, organizationId))
        .returning();
      if (!organization) throw new GatewayError(404, "not_found", "Organization not found");
      return this.applyOrganizationAdmission(orm, organization).catch(() => organization);
    });
  }

  private async applyOrganizationAdmission(
    orm: ConsoleDrizzle,
    organization: typeof organizations.$inferSelect,
  ) {
    const revision = organization.inferenceRevision;
    if (revision === organization.inferenceSyncedRevision) return organization;
    try {
      const applied = await this.gateway.setOrganizationAdmission(
        organization.id,
        organization.inferenceTargetStatus === "SUSPENDED",
        revision,
      );
      if (applied?.source_revision !== revision) {
        throw new GatewayError(
          502,
          "gateway_invalid_response",
          "Gateway did not confirm organization admission",
        );
      }
    } catch (err) {
      await orm
        .update(organizations)
        .set({ updatedAt: sql`now()` })
        .where(eq(organizations.id, organization.id));
      throw err;
    }
    const [confirmed] = await orm
      .update(organizations)
      .set({ status: organization.inferenceTargetStatus, inferenceSyncedRevision: revision })
      .where(
        and(eq(organizations.id, organization.id), eq(organizations.inferenceRevision, revision)),
      )
      .returning();
    return confirmed ?? organization;
  }

  async syncApiKey(keyId: string) {
    const [initial] = await this.orm
      .select({ billingAccountId: apiKeyRecords.billingAccountId })
      .from(apiKeyRecords)
      .where(eq(apiKeyRecords.id, keyId))
      .limit(1);
    if (!initial) throw new GatewayError(404, "not_found", "API key not found");
    const billingAccount = await this.ensureBillingAccountSynced(initial.billingAccountId);

    return this.withEntityLock(LOCK_NS.apiKey, keyId, async (orm) => {
      const [rec] = await orm
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
          billing_account_id: billingAccount.gatewayBillingAccountId,
          ...(rec.rotatedFromKeyId && !isRevoked
            ? { rotated_from_key_id: rec.rotatedFromKeyId }
            : {}),
          name: rec.name,
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
        const [updated] = await orm
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
          const [latest] = await orm
            .select()
            .from(apiKeyRecords)
            .where(eq(apiKeyRecords.id, rec.id))
            .limit(1);
          return { record: latest ?? rec, key: undefined };
        }
        await orm
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

  async updateUpstreamAccount(
    accountId: string,
    body: {
      name?: string | undefined;
      state?: (typeof upstreamAccounts.$inferSelect)["state"] | undefined;
      expected_state?: (typeof upstreamAccounts.$inferSelect)["state"] | undefined;
    },
  ) {
    return this.withEntityLock(LOCK_NS.account, accountId, async (orm) => {
      const [existing] = await orm
        .select()
        .from(upstreamAccounts)
        .where(eq(upstreamAccounts.id, accountId));
      if (!existing) throw new GatewayError(404, "not_found", "Upstream account not found");
      const [changed] = await orm
        .update(upstreamAccounts)
        .set({
          ...(body.name !== undefined ? { name: body.name } : {}),
          ...(body.state !== undefined ? { pendingState: body.state } : {}),
          ...(body.expected_state !== undefined || body.state !== undefined
            ? { expectedState: body.expected_state ?? null }
            : {}),
          desiredRevision: existing.desiredRevision + 1,
          syncStatus: "PENDING",
          updatedAt: sql`now()`,
        })
        .where(eq(upstreamAccounts.id, accountId))
        .returning();
      try {
        return await this.applyUpstreamAccount(orm, changed!);
      } catch (err) {
        if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
          await orm
            .update(upstreamAccounts)
            .set({
              name: existing.name,
              pendingState: existing.pendingState,
              expectedState: existing.expectedState,
              desiredRevision: existing.desiredRevision,
              syncStatus: existing.syncStatus,
              lastSyncError: existing.lastSyncError,
              updatedAt: sql`now()`,
            })
            .where(eq(upstreamAccounts.id, accountId));
        }
        throw err;
      }
    });
  }

  async syncUpstreamAccount(accountId: string) {
    return this.withEntityLock(LOCK_NS.account, accountId, async (orm) => {
      const [account] = await orm
        .select()
        .from(upstreamAccounts)
        .where(eq(upstreamAccounts.id, accountId));
      if (!account) throw new GatewayError(404, "not_found", "Upstream account not found");
      if (account.syncStatus === "SYNCED" && account.syncedRevision >= account.desiredRevision) {
        return account;
      }
      return this.applyUpstreamAccount(orm, account);
    });
  }

  private async applyUpstreamAccount(
    orm: ConsoleDrizzle,
    account: typeof upstreamAccounts.$inferSelect,
  ) {
    const targetRev = account.desiredRevision;
    const state =
      account.pendingState ?? (account.syncedRevision === 0 ? account.state : undefined);
    try {
      const res = await this.gateway
        .upsertUpstreamAccount({
          id: account.id,
          name: account.name,
          ...(state !== undefined ? { state } : {}),
          ...(account.expectedState ? { expected_state: account.expectedState } : {}),
          source_revision: targetRev,
        })
        .catch(async (err: unknown) => {
          if (
            err instanceof GatewayError &&
            err.code === "reconcile_required_conflict" &&
            state !== undefined
          ) {
            const remote = await this.gateway.listUpstreamAccounts();
            const applied = remote.items.find((item) => item.id === account.id);
            if (
              applied?.source_revision === targetRev &&
              applied.state === state &&
              applied.name === account.name
            ) {
              return applied;
            }
          }
          throw err;
        });
      const [updated] = await orm
        .update(upstreamAccounts)
        .set({
          state: res.state,
          pendingState: null,
          expectedState: null,
          syncedRevision: targetRev,
          syncStatus: "SYNCED",
          lastSyncError: null,
          updatedAt: sql`now()`,
        })
        .where(eq(upstreamAccounts.id, account.id))
        .returning();
      return updated ?? account;
    } catch (err) {
      await orm
        .update(upstreamAccounts)
        .set({
          syncStatus: "FAILED",
          lastSyncError: errMsg(err),
          updatedAt: sql`now()`,
        })
        .where(eq(upstreamAccounts.id, account.id));
      throw err;
    }
  }

  async syncChannel(channelId: string, plainApiKey?: string) {
    const [initial] = await this.orm
      .select({ accountId: channels.accountId })
      .from(channels)
      .where(eq(channels.id, channelId))
      .limit(1);
    if (!initial) throw new GatewayError(404, "not_found", "Channel not found");
    await this.syncUpstreamAccount(initial.accountId);

    return this.withEntityLock(LOCK_NS.channel, channelId, async (orm) => {
      const [ch] = await orm.select().from(channels).where(eq(channels.id, channelId)).limit(1);
      if (!ch) throw new GatewayError(404, "not_found", "Channel not found");
      return this.applyChannel(orm, ch, plainApiKey);
    });
  }

  async updateChannel(
    channelId: string,
    changes: Partial<
      Pick<
        typeof channels.$inferSelect,
        | "accountId"
        | "name"
        | "kind"
        | "baseUrl"
        | "rpm"
        | "concurrency"
        | "enabled"
        | "routingPolicy"
        | "adapterConfig"
      >
    >,
    plainApiKey?: string,
  ) {
    const [initial] = await this.orm
      .select({ accountId: channels.accountId })
      .from(channels)
      .where(eq(channels.id, channelId));
    if (!initial) throw new GatewayError(404, "not_found", "Channel not found");
    await this.syncUpstreamAccount(changes.accountId ?? initial.accountId);
    return this.withEntityLock(LOCK_NS.channel, channelId, async (orm) => {
      const [existing] = await orm.select().from(channels).where(eq(channels.id, channelId));
      if (!existing) throw new GatewayError(404, "not_found", "Channel not found");
      if (
        (existing.secretSyncPending || existing.syncedRevision === 0) &&
        plainApiKey === undefined
      ) {
        throw new GatewayError(
          409,
          "channel_secret_pending",
          "Retry this channel update with the API key",
        );
      }
      const [changed] = await orm
        .update(channels)
        .set({
          ...changes,
          secretSyncPending: plainApiKey !== undefined,
          desiredRevision: existing.desiredRevision + 1,
          syncStatus: "PENDING",
          updatedAt: sql`now()`,
        })
        .where(eq(channels.id, channelId))
        .returning();
      try {
        return await this.applyChannel(orm, changed!, plainApiKey);
      } catch (err) {
        if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
          const {
            id: _id,
            createdAt: _createdAt,
            providerPluginId: _providerPluginId,
            syncedRevision: _syncedRevision,
            ...previous
          } = existing;
          await orm
            .update(channels)
            .set({ ...previous, updatedAt: sql`now()` })
            .where(eq(channels.id, channelId));
        }
        throw err;
      }
    });
  }

  private async applyChannel(
    orm: ConsoleDrizzle,
    ch: typeof channels.$inferSelect,
    plainApiKey?: string,
  ) {
    if (ch.secretSyncPending && plainApiKey === undefined) {
      throw new GatewayError(
        409,
        "channel_secret_pending",
        "Retry this channel update with the API key",
      );
    }
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
      const [updated] = await orm
        .update(channels)
        .set({
          ...(plainApiKey !== undefined
            ? { keyHint: maskSecret(plainApiKey), secretSyncPending: false }
            : {}),
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
        const [latest] = await orm.select().from(channels).where(eq(channels.id, ch.id)).limit(1);
        return latest ?? ch;
      }
      await orm
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
    return this.withEntityLock(LOCK_NS.template, templateId, async (orm) => {
      const [tpl] = await orm
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
        await orm
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
        const updated = await orm.transaction(async (tx) => {
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
        await orm
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
    return this.withEntityLock(LOCK_NS.model, modelName, async (orm) => {
      const [m] = await orm
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
        const [updated] = await orm
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
          const [latest] = await orm
            .select()
            .from(modelCatalog)
            .where(eq(modelCatalog.name, m.name))
            .limit(1);
          return latest ?? m;
        }
        await orm
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
    return this.withEntityLock(LOCK_NS.route, routeId, async (orm) => {
      const [route] = await orm.select().from(routes).where(eq(routes.id, routeId));
      if (!route) throw new GatewayError(404, "not_found", "Route not found");
      return this.applyRoute(orm, route);
    });
  }

  async updateRoute(
    routeId: string,
    changes: Partial<
      Pick<
        typeof routes.$inferSelect,
        | "channelId"
        | "upstreamModel"
        | "operations"
        | "priority"
        | "weight"
        | "billingPriceId"
        | "costVersionId"
        | "requestPolicy"
        | "adapterOverrides"
        | "retryPolicy"
        | "enabled"
      >
    >,
  ) {
    return this.withEntityLock(LOCK_NS.route, routeId, async (orm) => {
      const [existing] = await orm.select().from(routes).where(eq(routes.id, routeId));
      if (!existing) throw new GatewayError(404, "not_found", "Route not found");
      const [changed] = await orm
        .update(routes)
        .set({
          ...changes,
          desiredRevision: existing.desiredRevision + 1,
          syncStatus: "PENDING",
          updatedAt: sql`now()`,
        })
        .where(eq(routes.id, routeId))
        .returning();
      try {
        return await this.applyRoute(orm, changed!);
      } catch (err) {
        if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
          const {
            id: _id,
            createdAt: _createdAt,
            syncedRevision: _syncedRevision,
            ...previous
          } = existing;
          await orm
            .update(routes)
            .set({ ...previous, updatedAt: sql`now()` })
            .where(eq(routes.id, routeId));
        }
        throw err;
      }
    });
  }

  private async applyRoute(orm: ConsoleDrizzle, r: typeof routes.$inferSelect) {
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
      const [updated] = await orm
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
        const [latest] = await orm.select().from(routes).where(eq(routes.id, r.id)).limit(1);
        return latest ?? r;
      }
      await orm
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
    const [initial] = await this.orm
      .select({
        billingAccountId: pricingOverrides.billingAccountId,
        modelName: pricingOverrides.modelName,
        operation: pricingOverrides.operation,
      })
      .from(pricingOverrides)
      .where(eq(pricingOverrides.id, overrideId));
    if (!initial) throw new GatewayError(404, "not_found", "Pricing override not found");
    const billingAccount = await this.ensureBillingAccountSynced(initial.billingAccountId);
    const lockKey = JSON.stringify([
      initial.billingAccountId,
      initial.modelName,
      initial.operation,
    ]);
    return this.withEntityLock(LOCK_NS.override, lockKey, async (orm) => {
      const [override] = await orm
        .select()
        .from(pricingOverrides)
        .where(eq(pricingOverrides.id, overrideId));
      if (!override) throw new GatewayError(404, "not_found", "Pricing override not found");
      return this.applyPricingOverride(orm, billingAccount.gatewayBillingAccountId, override);
    });
  }

  async upsertPricingOverride(
    input: Pick<
      typeof pricingOverrides.$inferSelect,
      | "billingAccountId"
      | "modelName"
      | "operation"
      | "overrideKind"
      | "billingPriceId"
      | "multiplierNumerator"
      | "multiplierDenominator"
      | "enabled"
    >,
  ) {
    const billingAccount = await this.ensureBillingAccountSynced(input.billingAccountId);
    const lockKey = JSON.stringify([input.billingAccountId, input.modelName, input.operation]);
    return this.withEntityLock(LOCK_NS.override, lockKey, async (orm) => {
      const [previous] = await orm
        .select()
        .from(pricingOverrides)
        .where(
          and(
            eq(pricingOverrides.billingAccountId, input.billingAccountId),
            eq(pricingOverrides.modelName, input.modelName),
            eq(pricingOverrides.operation, input.operation),
          ),
        );
      const [changed] = previous
        ? await orm
            .update(pricingOverrides)
            .set({
              ...input,
              desiredRevision: previous.desiredRevision + 1,
              syncStatus: "PENDING",
              updatedAt: sql`now()`,
            })
            .where(eq(pricingOverrides.id, previous.id))
            .returning()
        : await orm
            .insert(pricingOverrides)
            .values({ ...input, id: crypto.randomUUID() })
            .returning();
      try {
        return await this.applyPricingOverride(
          orm,
          billingAccount.gatewayBillingAccountId,
          changed!,
        );
      } catch (err) {
        if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
          if (previous) {
            const {
              id: _id,
              createdAt: _createdAt,
              syncedRevision: _syncedRevision,
              ...snapshot
            } = previous;
            await orm
              .update(pricingOverrides)
              .set({ ...snapshot, updatedAt: sql`now()` })
              .where(eq(pricingOverrides.id, previous.id));
          } else {
            await orm.delete(pricingOverrides).where(eq(pricingOverrides.id, changed!.id));
          }
        }
        throw err;
      }
    });
  }

  private async applyPricingOverride(
    orm: ConsoleDrizzle,
    gatewayBillingAccountId: string,
    o: typeof pricingOverrides.$inferSelect,
  ) {
    const targetRev = o.desiredRevision;
    try {
      const res = await this.gateway.upsertPricingOverride({
        id: o.id,
        billing_account_id: gatewayBillingAccountId,
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
      const [updated] = await orm
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
        const [latest] = await orm
          .select()
          .from(pricingOverrides)
          .where(eq(pricingOverrides.id, o.id))
          .limit(1);
        return latest ?? o;
      }
      await orm
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

      const pendingOrganizations = await this.orm
        .select({ id: organizations.id })
        .from(organizations)
        .where(lt(organizations.inferenceSyncedRevision, organizations.inferenceRevision))
        .orderBy(asc(organizations.updatedAt), asc(organizations.id))
        .limit(batchLimit);
      for (const { id } of pendingOrganizations) {
        if (signal?.aborted) return { synced, failed };
        try {
          await this.withEntityLock(LOCK_NS.organization, id, async (orm) => {
            const [organization] = await orm
              .select()
              .from(organizations)
              .where(eq(organizations.id, id));
            if (organization) await this.applyOrganizationAdmission(orm, organization);
          });
          synced++;
        } catch {
          failed++;
        }
      }

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
          await this.syncUpstreamAccount(a.id);
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
            eq(channels.secretSyncPending, false),
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

      const pendingBillingAccounts = await this.orm
        .select({ id: billingAccounts.id })
        .from(billingAccounts)
        .where(
          or(
            eq(billingAccounts.syncStatus, "FAILED"),
            and(
              eq(billingAccounts.gatewaySynced, true),
              or(
                inArray(billingAccounts.syncStatus, states),
                lt(billingAccounts.syncedRevision, billingAccounts.desiredRevision),
              ),
            ),
          ),
        )
        .limit(batchLimit);
      for (const p of pendingBillingAccounts) {
        if (signal?.aborted) return { synced, failed };
        try {
          await this.ensureBillingAccountSynced(p.id);
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
            or(gt(apiKeyRecords.syncedRevision, 0), isNotNull(apiKeyRecords.revokedAt)),
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
