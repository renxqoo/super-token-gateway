import { and, isNull, eq, sql } from "drizzle-orm";
import { GatewayError } from "@gateway/contracts";
import { maskSecret } from "./auth.ts";
import {
  channels,
  modelCatalog,
  priceVersionSnapshots,
  pricingOverrides,
  pricingTemplates,
  routes,
  upstreamAccounts,
  type ConsoleDrizzle,
} from "./db/schema.ts";
import type { GatewayClient } from "./gateway-client.ts";
import { errMsg, isStaleRevisionError, LOCK_NS, type EntityLocker } from "./sync-state.ts";
import type { ConsoleAdmissionSync } from "./sync-admission.ts";

type ModelSettings = Omit<
  typeof modelCatalog.$inferInsert,
  | "pendingReleaseId"
  | "desiredRevision"
  | "syncedRevision"
  | "syncStatus"
  | "lastSyncError"
  | "createdAt"
  | "updatedAt"
>;

export function createConsoleCatalogSync(
  orm: ConsoleDrizzle,
  gateway: GatewayClient,
  withEntityLock: EntityLocker,
  ensureBillingAccountSynced: ConsoleAdmissionSync["ensureBillingAccountSynced"],
) {
  async function updateUpstreamAccount(
    accountId: string,
    body: {
      name?: string | undefined;
      state?: (typeof upstreamAccounts.$inferSelect)["state"] | undefined;
      expected_state?: (typeof upstreamAccounts.$inferSelect)["state"] | undefined;
    },
  ) {
    return withEntityLock(LOCK_NS.account, accountId, async (orm) => {
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
        return await applyUpstreamAccount(orm, changed!);
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

  async function syncUpstreamAccount(accountId: string) {
    return withEntityLock(LOCK_NS.account, accountId, async (orm) => {
      const [account] = await orm
        .select()
        .from(upstreamAccounts)
        .where(eq(upstreamAccounts.id, accountId));
      if (!account) throw new GatewayError(404, "not_found", "Upstream account not found");
      if (account.syncStatus === "SYNCED" && account.syncedRevision >= account.desiredRevision) {
        return account;
      }
      return applyUpstreamAccount(orm, account);
    });
  }

  async function applyUpstreamAccount(
    orm: ConsoleDrizzle,
    account: typeof upstreamAccounts.$inferSelect,
  ) {
    const targetRev = account.desiredRevision;
    const state =
      account.pendingState ?? (account.syncedRevision === 0 ? account.state : undefined);
    try {
      const res = await gateway
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
            const remote = await gateway.listUpstreamAccounts();
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

  async function syncChannel(channelId: string, plainApiKey?: string) {
    const [initial] = await orm
      .select({ accountId: channels.accountId })
      .from(channels)
      .where(eq(channels.id, channelId))
      .limit(1);
    if (!initial) throw new GatewayError(404, "not_found", "Channel not found");
    await syncUpstreamAccount(initial.accountId);

    return withEntityLock(LOCK_NS.channel, channelId, async (orm) => {
      const [ch] = await orm.select().from(channels).where(eq(channels.id, channelId)).limit(1);
      if (!ch) throw new GatewayError(404, "not_found", "Channel not found");
      return applyChannel(orm, ch, plainApiKey);
    });
  }

  async function updateChannel(
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
    const [initial] = await orm
      .select({ accountId: channels.accountId })
      .from(channels)
      .where(eq(channels.id, channelId));
    if (!initial) throw new GatewayError(404, "not_found", "Channel not found");
    await syncUpstreamAccount(changes.accountId ?? initial.accountId);
    return withEntityLock(LOCK_NS.channel, channelId, async (orm) => {
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
        return await applyChannel(orm, changed!, plainApiKey);
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

  async function applyChannel(
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
      const gwCh = await gateway.upsertChannel({
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

  async function syncPricingTemplate(templateId: string) {
    return withEntityLock(LOCK_NS.template, templateId, async (orm) => {
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
        const created = await gateway.createPrice({
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

  async function upsertModel(input: ModelSettings) {
    const { name, adaptiveRouting, ...settings } = input;
    return withEntityLock(LOCK_NS.model, name, async (orm) => {
      const where = eq(modelCatalog.name, name);
      let [existing] = await orm.select().from(modelCatalog).where(where);
      if (existing?.pendingReleaseId)
        throw new GatewayError(409, "model_onboarding_conflict", "Model has a pending release");
      if (existing && existing.syncedRevision < existing.desiredRevision) {
        existing = await applyModel(orm, existing);
        if (existing.syncedRevision < existing.desiredRevision) {
          throw new GatewayError(409, "stale_revision", "Previous model edit is not synced");
        }
      }

      const changes = {
        ...settings,
        ...(adaptiveRouting !== undefined
          ? {
              adaptiveRouting:
                adaptiveRouting === null
                  ? null
                  : sql`${JSON.stringify(adaptiveRouting)}::text::jsonb`,
            }
          : {}),
        syncStatus: "PENDING" as const,
      };
      const [saved] = await orm
        .insert(modelCatalog)
        .values({ name, ...changes })
        .onConflictDoUpdate({
          target: modelCatalog.name,
          set: {
            ...changes,
            desiredRevision: sql`${modelCatalog.desiredRevision} + 1`,
            updatedAt: sql`now()`,
          },
        })
        .returning();
      try {
        return { created: !existing, model: await applyModel(orm, saved!) };
      } catch (err) {
        if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
          if (existing) {
            const { name: _name, createdAt: _createdAt, ...previous } = existing;
            await orm
              .update(modelCatalog)
              .set({
                ...previous,
                adaptiveRouting:
                  previous.adaptiveRouting === false
                    ? sql`'false'::jsonb`
                    : previous.adaptiveRouting,
                updatedAt: sql`now()`,
              })
              .where(where);
          } else {
            await orm.delete(modelCatalog).where(where);
          }
        }
        throw err;
      }
    });
  }

  async function syncModel(modelName: string) {
    return withEntityLock(LOCK_NS.model, modelName, async (orm) => {
      const [model] = await orm.select().from(modelCatalog).where(eq(modelCatalog.name, modelName));
      if (!model) throw new GatewayError(404, "not_found", "Model not found");
      return model.pendingReleaseId ? model : applyModel(orm, model);
    });
  }

  async function applyModel(orm: ConsoleDrizzle, model: typeof modelCatalog.$inferSelect) {
    const where = eq(modelCatalog.name, model.name);
    try {
      const res = await gateway.upsertModel({
        name: model.name,
        operations: model.operations,
        routing_strategy: model.routingStrategy,
        adaptive_routing: model.adaptiveRouting,
        max_output_tokens: model.maxOutputTokens,
        enabled: model.enabled,
        source_revision: model.desiredRevision,
        expected_source_revision: model.syncedRevision,
      });
      const [updated] = await orm
        .update(modelCatalog)
        .set({
          syncedRevision: Math.max(
            model.desiredRevision,
            res.source_revision ?? model.desiredRevision,
          ),
          syncStatus: "SYNCED",
          lastSyncError: null,
          updatedAt: sql`now()`,
        })
        .where(where)
        .returning();
      return updated!;
    } catch (err) {
      if (isStaleRevisionError(err)) return model;
      await orm
        .update(modelCatalog)
        .set({ syncStatus: "FAILED", lastSyncError: errMsg(err), updatedAt: sql`now()` })
        .where(where);
      throw err;
    }
  }

  async function syncRoute(routeId: string) {
    return withEntityLock(LOCK_NS.route, routeId, async (orm) => {
      const [route] = await orm.select().from(routes).where(eq(routes.id, routeId));
      if (!route) throw new GatewayError(404, "not_found", "Route not found");
      return route.pendingReleaseId ? route : applyRoute(orm, route);
    });
  }

  async function updateRoute(
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
    return withEntityLock(LOCK_NS.route, routeId, async (orm) => {
      let [existing] = await orm.select().from(routes).where(eq(routes.id, routeId));
      if (!existing) throw new GatewayError(404, "not_found", "Route not found");
      if (existing.pendingReleaseId)
        throw new GatewayError(409, "route_release_conflict", "Route has a pending release");
      if (existing.syncedRevision < existing.desiredRevision) {
        existing = await applyRoute(orm, existing);
        if (existing.syncedRevision < existing.desiredRevision) {
          throw new GatewayError(409, "stale_revision", "Previous route edit is not synced");
        }
      }
      const [changed] = await orm
        .update(routes)
        .set({
          ...changes,
          desiredRevision: existing.desiredRevision + 1,
          syncStatus: "PENDING",
          updatedAt: sql`now()`,
        })
        .where(
          and(
            eq(routes.id, routeId),
            eq(routes.desiredRevision, existing.desiredRevision),
            isNull(routes.pendingReleaseId),
          ),
        )
        .returning();
      if (!changed)
        throw new GatewayError(
          409,
          "route_release_conflict",
          "Route changed or has a pending release",
        );
      try {
        return await applyRoute(orm, changed);
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
            .where(
              and(
                eq(routes.id, routeId),
                eq(routes.desiredRevision, changed.desiredRevision),
                isNull(routes.pendingReleaseId),
              ),
            );
        }
        throw err;
      }
    });
  }

  async function applyRoute(orm: ConsoleDrizzle, r: typeof routes.$inferSelect) {
    const targetRev = r.desiredRevision;
    try {
      const res = await gateway.upsertRoute({
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
        expected_source_revision: r.syncedRevision,
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

  async function syncPricingOverride(overrideId: string) {
    const [initial] = await orm
      .select({
        billingAccountId: pricingOverrides.billingAccountId,
        modelName: pricingOverrides.modelName,
        operation: pricingOverrides.operation,
      })
      .from(pricingOverrides)
      .where(eq(pricingOverrides.id, overrideId));
    if (!initial) throw new GatewayError(404, "not_found", "Pricing override not found");
    const billingAccount = await ensureBillingAccountSynced(initial.billingAccountId);
    const lockKey = JSON.stringify([
      initial.billingAccountId,
      initial.modelName,
      initial.operation,
    ]);
    return withEntityLock(LOCK_NS.override, lockKey, async (orm) => {
      const [override] = await orm
        .select()
        .from(pricingOverrides)
        .where(eq(pricingOverrides.id, overrideId));
      if (!override) throw new GatewayError(404, "not_found", "Pricing override not found");
      return applyPricingOverride(orm, billingAccount.gatewayBillingAccountId, override);
    });
  }

  async function upsertPricingOverride(
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
    const billingAccount = await ensureBillingAccountSynced(input.billingAccountId);
    const lockKey = JSON.stringify([input.billingAccountId, input.modelName, input.operation]);
    return withEntityLock(LOCK_NS.override, lockKey, async (orm) => {
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
        return await applyPricingOverride(orm, billingAccount.gatewayBillingAccountId, changed!);
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

  async function applyPricingOverride(
    orm: ConsoleDrizzle,
    gatewayBillingAccountId: string,
    o: typeof pricingOverrides.$inferSelect,
  ) {
    const targetRev = o.desiredRevision;
    try {
      const res = await gateway.upsertPricingOverride({
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

  return {
    updateUpstreamAccount,
    syncUpstreamAccount,
    syncChannel,
    updateChannel,
    syncPricingTemplate,
    upsertModel,
    syncModel,
    syncRoute,
    updateRoute,
    syncPricingOverride,
    upsertPricingOverride,
  };
}

export type ConsoleCatalogSync = ReturnType<typeof createConsoleCatalogSync>;
