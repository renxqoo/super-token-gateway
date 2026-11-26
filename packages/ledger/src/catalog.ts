import type { SQL } from "bun";
import { and, asc, desc, eq, inArray, sql } from "drizzle-orm";
import {
  canonical,
  DomainError,
  validatePricing,
  type Operation,
  type Pricing,
} from "@gateway/kernel";
import {
  channels,
  configEpoch,
  createDrizzle,
  models,
  plugins,
  priceVersions,
  billingAccountPricingOverrides,
  routes,
  upstreamAccounts,
} from "./schema.ts";
import type {
  AdapterOverrides,
  ChannelKind,
  ChannelRecord,
  EpochStamp,
  ModelRecord,
  PluginPipelineStep,
  PriceBasis,
  PriceSide,
  PriceVersionRecord,
  BillingAccountPricingOverrideRecord,
  RouteRecord,
  RouteRetryPolicy,
  RoutingStrategy,
  UpstreamAccountRecord,
} from "./types.ts";

import { createPluginCatalogOperations } from "./catalog-snapshot.ts";

import { createCatalogAdmissionOperations } from "./catalog-admission.ts";

import { bumpEpoch, catalogDigest } from "./catalog-epoch.ts";

export function createCatalogRepository(db: SQL) {
  const orm = createDrizzle(db);

  return {
    async getEpoch(): Promise<number> {
      const rows = await orm
        .select({ version: configEpoch.version })
        .from(configEpoch)
        .where(eq(configEpoch.id, 1));
      return rows[0]?.version ?? 0;
    },

    async getEpochStamp(): Promise<EpochStamp> {
      const rows = await orm
        .select({ version: configEpoch.version, lineageToken: configEpoch.lineageToken })
        .from(configEpoch)
        .where(eq(configEpoch.id, 1));
      return {
        version: rows[0]?.version ?? 0,
        lineageToken: rows[0]?.lineageToken ?? "",
      };
    },

    ...createCatalogAdmissionOperations({ db, orm }),

    async upsertUpstreamAccount(input: {
      id?: string;
      name: string;
      state?: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED";
      expected_state?: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED";
      source_revision?: number;
    }): Promise<UpstreamAccountRecord & { config_epoch: number; source_revision: number }> {
      const id = input.id ?? crypto.randomUUID();
      const sourceRev = input.source_revision;
      return orm.transaction(async (tx) => {
        const [existing] = await tx
          .select()
          .from(upstreamAccounts)
          .where(eq(upstreamAccounts.id, id))
          .for("update");
        if (existing) {
          if (sourceRev !== undefined && existing.sourceRevision > sourceRev) {
            throw new DomainError(
              "stale_revision",
              "Incoming upstream account revision is older than current Gateway revision",
            );
          }
          if (input.expected_state !== undefined && existing.state !== input.expected_state) {
            throw new DomainError(
              "reconcile_required_conflict",
              `Upstream account state is ${existing.state}, expected ${input.expected_state}`,
            );
          }
          const nextState = input.state ?? existing.state;
          if (
            existing.state === "RECONCILE_REQUIRED" &&
            nextState !== "RECONCILE_REQUIRED" &&
            input.expected_state !== "RECONCILE_REQUIRED"
          ) {
            throw new DomainError(
              "reconcile_required_conflict",
              "Upstream account in RECONCILE_REQUIRED state requires explicit reconciliation",
            );
          }
          const [r] = await tx
            .update(upstreamAccounts)
            .set({
              name: input.name,
              state: nextState,
              revision: sql`${upstreamAccounts.revision} + 1`,
              ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
              updatedAt: sql`now()`,
            })
            .where(eq(upstreamAccounts.id, id))
            .returning();
          const epoch = await bumpEpoch(tx);
          return {
            id: r!.id,
            name: r!.name,
            state: r!.state,
            revision: r!.revision,
            source_revision: r!.sourceRevision,
            config_epoch: epoch,
          };
        }
        const [r] = await tx
          .insert(upstreamAccounts)
          .values({
            id,
            name: input.name,
            state: input.state ?? "ACTIVE",
            ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
          })
          .returning();
        const epoch = await bumpEpoch(tx);
        return {
          id: r!.id,
          name: r!.name,
          state: r!.state,
          revision: r!.revision,
          source_revision: r!.sourceRevision,
          config_epoch: epoch,
        };
      });
    },

    async listUpstreamAccounts(): Promise<(UpstreamAccountRecord & { source_revision: number })[]> {
      const rows = await orm.select().from(upstreamAccounts).orderBy(asc(upstreamAccounts.name));
      return rows.map((r) => ({
        id: r.id,
        name: r.name,
        state: r.state,
        revision: r.revision,
        source_revision: r.sourceRevision,
      }));
    },

    async listChannels(): Promise<
      (Omit<ChannelRecord, "secret_ciphertext"> & { source_revision: number })[]
    > {
      const rows = await orm.select().from(channels).orderBy(asc(channels.name));
      return rows.map((r) => ({
        id: r.id,
        account_id: r.accountId,
        name: r.name,
        kind: r.kind,
        provider_plugin_id: r.providerPluginId,
        base_url: r.baseUrl,
        rpm: r.rpm,
        concurrency: r.concurrency,
        enabled: r.enabled,
        routing_policy: r.routingPolicy,
        adapter_config: r.adapterConfig,
        config_revision: r.configRevision,
        source_revision: r.sourceRevision,
      }));
    },

    async getPriceVersion(id: string): Promise<PriceVersionRecord | null> {
      const [r] = await orm.select().from(priceVersions).where(eq(priceVersions.id, id)).limit(1);
      if (!r) return null;
      return {
        id: r.id,
        side: r.side,
        currency: r.currency,
        basis: r.basis,
        rates: r.rates,
        ceilings: r.ceilings,
        source_digest: r.sourceDigest,
      };
    },

    async upsertChannel(input: {
      id?: string;
      account_id: string;
      name: string;
      kind: ChannelKind;
      provider_plugin_id?: string | null;
      base_url: string;
      secret_ciphertext?: string;
      rpm: number;
      concurrency: number;
      enabled?: boolean;
      routing_policy?: Record<string, unknown>;
      adapter_config?: AdapterOverrides;
      source_revision?: number;
    }): Promise<ChannelRecord & { config_epoch: number; source_revision: number }> {
      const id = input.id ?? crypto.randomUUID();
      const enabled = input.enabled ?? true;
      const routingPolicy = input.routing_policy ?? {};
      const adapterConfig = input.adapter_config ?? {};
      const sourceRev = input.source_revision;
      return orm.transaction(async (tx) => {
        const [existing] = await tx
          .select({
            providerId: channels.providerPluginId,
            secretCiphertext: channels.secretCiphertext,
            sourceRevision: channels.sourceRevision,
          })
          .from(channels)
          .where(eq(channels.id, id))
          .for("update");
        if (existing && sourceRev !== undefined && existing.sourceRevision > sourceRev) {
          throw new DomainError(
            "stale_revision",
            "Incoming channel revision is older than current Gateway revision",
          );
        }
        const effectiveSecret = input.secret_ciphertext ?? existing?.secretCiphertext;
        if (effectiveSecret === undefined) {
          throw new DomainError(
            "invalid_channel_secret",
            "api_key is required when creating a channel",
          );
        }
        const requestedProvider = input.provider_plugin_id ?? existing?.providerId;
        const providerWhere = requestedProvider
          ? eq(plugins.id, requestedProvider)
          : eq(plugins.name, input.kind);
        const [provider] = await tx
          .select({ id: plugins.id })
          .from(plugins)
          .where(and(providerWhere, eq(plugins.capability, "PROVIDER"), eq(plugins.enabled, true)))
          .orderBy(asc(plugins.builtin), desc(plugins.createdAt), asc(plugins.id))
          .limit(1)
          .for("share");
        if ((enabled || input.provider_plugin_id) && !provider) {
          throw new DomainError(
            "invalid_plugin_binding",
            "Channel requires an enabled provider revision",
          );
        }
        const providerPluginId = provider?.id ?? null;
        const setWhere =
          sourceRev !== undefined
            ? sql`(${channels.providerPluginId} IS NULL OR ${channels.providerPluginId} IS NOT DISTINCT FROM ${providerPluginId}::uuid) AND ${channels.sourceRevision} <= ${sourceRev}`
            : sql`${channels.providerPluginId} IS NULL OR ${channels.providerPluginId} IS NOT DISTINCT FROM ${providerPluginId}::uuid`;
        const rows = await tx
          .insert(channels)
          .values({
            id,
            accountId: input.account_id,
            name: input.name,
            kind: input.kind,
            providerPluginId,
            baseUrl: input.base_url,
            secretCiphertext: effectiveSecret,
            rpm: input.rpm,
            concurrency: input.concurrency,
            enabled,
            routingPolicy,
            adapterConfig,
            ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
          })
          .onConflictDoUpdate({
            target: channels.id,
            setWhere,
            set: {
              accountId: input.account_id,
              name: input.name,
              kind: input.kind,
              providerPluginId,
              baseUrl: input.base_url,
              ...(input.secret_ciphertext !== undefined
                ? { secretCiphertext: input.secret_ciphertext }
                : {}),
              rpm: input.rpm,
              concurrency: input.concurrency,
              enabled,
              routingPolicy,
              adapterConfig,
              configRevision: sql`${channels.configRevision} + 1`,
              ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
              updatedAt: sql`now()`,
            },
          })
          .returning();
        if (!rows[0])
          throw new DomainError(
            "plugin_release_conflict",
            "Use a plugin release to change an existing channel binding",
          );
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: r.id,
          account_id: r.accountId,
          name: r.name,
          kind: r.kind,
          provider_plugin_id: r.providerPluginId,
          base_url: r.baseUrl,
          secret_ciphertext: r.secretCiphertext,
          rpm: r.rpm,
          concurrency: r.concurrency,
          enabled: r.enabled,
          routing_policy: r.routingPolicy,
          adapter_config: r.adapterConfig,
          config_revision: r.configRevision,
          source_revision: r.sourceRevision,
          config_epoch: epoch,
        };
      });
    },

    async upsertModel(input: {
      name: string;
      operations: Operation[];
      routing_strategy?: RoutingStrategy;
      max_output_tokens: number;
      enabled?: boolean;
      source_revision?: number;
    }): Promise<ModelRecord & { config_epoch: number; source_revision: number }> {
      const routingStrategy = input.routing_strategy ?? "WEIGHTED";
      const enabled = input.enabled ?? true;
      const sourceRev = input.source_revision;
      return orm.transaction(async (tx) => {
        const rows = await tx
          .insert(models)
          .values({
            name: input.name,
            operations: input.operations,
            routingStrategy,
            maxOutputTokens: input.max_output_tokens,
            enabled,
            ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
          })
          .onConflictDoUpdate({
            target: models.name,
            ...(sourceRev !== undefined
              ? { setWhere: sql`${models.sourceRevision} <= ${sourceRev}` }
              : {}),
            set: {
              operations: input.operations,
              routingStrategy,
              maxOutputTokens: input.max_output_tokens,
              enabled,
              ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
            },
          })
          .returning();
        if (!rows[0]) {
          throw new DomainError(
            "stale_revision",
            "Incoming model revision is older than current Gateway revision",
          );
        }
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          name: r.name,
          operations: r.operations,
          routing_strategy: r.routingStrategy,
          max_output_tokens: r.maxOutputTokens,
          enabled: r.enabled,
          source_revision: r.sourceRevision,
          config_epoch: epoch,
        };
      });
    },

    async createPriceVersion(input: {
      id?: string;
      side: PriceSide;
      currency: string;
      basis?: PriceBasis;
      rates: Pricing["rates"];
      ceilings?: Record<string, string>;
    }): Promise<PriceVersionRecord & { config_epoch: number; replayed?: boolean }> {
      validatePricing({ currency: input.currency, rates: input.rates });
      const id = input.id ?? crypto.randomUUID();
      const basis = input.basis ?? "PROVIDER_USAGE";
      const ceilings = input.ceilings ?? {};
      const sourceDigest = catalogDigest(
        canonical({
          side: input.side,
          currency: input.currency,
          basis,
          rates: input.rates,
          ceilings,
        }),
      );
      return orm.transaction(async (tx) => {
        const rows = await tx
          .insert(priceVersions)
          .values({
            id,
            side: input.side,
            currency: input.currency,
            basis,
            rates: input.rates,
            ceilings,
            sourceDigest,
          })
          .onConflictDoNothing({ target: priceVersions.id })
          .returning();
        if (rows[0]) {
          const epoch = await bumpEpoch(tx);
          const r = rows[0]!;
          return {
            id: r.id,
            side: r.side,
            currency: r.currency,
            basis: r.basis,
            rates: r.rates,
            ceilings: r.ceilings,
            source_digest: r.sourceDigest,
            config_epoch: epoch,
            replayed: false,
          };
        }
        const [existing] = await tx
          .select()
          .from(priceVersions)
          .where(eq(priceVersions.id, id))
          .limit(1);
        if (!existing || existing.sourceDigest !== sourceDigest) {
          throw new DomainError(
            "price_version_conflict",
            "Price version identifier already exists with different rates",
          );
        }
        const [epochRow] = await tx
          .select({ version: configEpoch.version })
          .from(configEpoch)
          .where(eq(configEpoch.id, 1));
        return {
          id: existing.id,
          side: existing.side,
          currency: existing.currency,
          basis: existing.basis,
          rates: existing.rates,
          ceilings: existing.ceilings,
          source_digest: existing.sourceDigest,
          config_epoch: epochRow?.version ?? 0,
          replayed: true,
        };
      });
    },

    async upsertRoute(input: {
      id?: string;
      model_name: string;
      channel_id: string;
      upstream_model: string;
      operations: Operation[];
      priority?: number;
      weight?: number;
      billing_price_id: string;
      cost_version_id: string;
      request_policy?: Record<string, unknown>;
      adapter_overrides?: AdapterOverrides;
      plugin_pipeline?: PluginPipelineStep[];
      retry_policy?: RouteRetryPolicy;
      enabled?: boolean;
      source_revision?: number;
    }): Promise<RouteRecord & { config_epoch: number; source_revision: number }> {
      const id = input.id ?? crypto.randomUUID();
      const priority = input.priority ?? 0;
      const weight = input.weight ?? 1;
      const requestPolicy = input.request_policy ?? {};
      const adapterOverrides = input.adapter_overrides ?? {};
      const retryPolicy = input.retry_policy ?? {};
      const enabled = input.enabled ?? true;
      const sourceRev = input.source_revision;
      return orm.transaction(async (tx) => {
        const [existing] = await tx
          .select({ pipeline: routes.pluginPipeline, sourceRevision: routes.sourceRevision })
          .from(routes)
          .where(eq(routes.id, id))
          .for("update");
        if (existing && sourceRev !== undefined && existing.sourceRevision > sourceRev) {
          throw new DomainError(
            "stale_revision",
            "Incoming route revision is older than current Gateway revision",
          );
        }
        const pluginPipeline = input.plugin_pipeline ?? existing?.pipeline ?? [];
        const requiredPlugins = new Set(
          pluginPipeline.filter((step) => step.enabled !== false).map((step) => step.plugin_id),
        );
        if (enabled && requiredPlugins.size) {
          const available = await tx
            .select({ id: plugins.id })
            .from(plugins)
            .where(
              and(
                inArray(plugins.id, [...requiredPlugins]),
                eq(plugins.capability, "CONTENT"),
                eq(plugins.enabled, true),
              ),
            )
            .for("share");
          if (available.length !== requiredPlugins.size) {
            throw new DomainError(
              "invalid_plugin_binding",
              "Route requires an enabled content plugin",
            );
          }
        }
        const setWhere =
          input.plugin_pipeline !== undefined && sourceRev !== undefined
            ? sql`${routes.pluginPipeline} = ${JSON.stringify(pluginPipeline)}::text::jsonb AND ${routes.sourceRevision} <= ${sourceRev}`
            : input.plugin_pipeline !== undefined
              ? sql`${routes.pluginPipeline} = ${JSON.stringify(pluginPipeline)}::text::jsonb`
              : sourceRev !== undefined
                ? sql`${routes.sourceRevision} <= ${sourceRev}`
                : undefined;
        const rows = await tx
          .insert(routes)
          .values({
            id,
            modelName: input.model_name,
            channelId: input.channel_id,
            upstreamModel: input.upstream_model,
            operations: input.operations,
            priority,
            weight,
            billingPriceId: input.billing_price_id,
            costVersionId: input.cost_version_id,
            requestPolicy,
            adapterOverrides,
            pluginPipeline,
            retryPolicy,
            enabled,
            ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
          })
          .onConflictDoUpdate({
            target: routes.id,
            ...(setWhere ? { setWhere } : {}),
            set: {
              channelId: input.channel_id,
              upstreamModel: input.upstream_model,
              operations: input.operations,
              priority,
              weight,
              billingPriceId: input.billing_price_id,
              costVersionId: input.cost_version_id,
              requestPolicy,
              adapterOverrides,
              retryPolicy,
              enabled,
              ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
            },
          })
          .returning();
        if (!rows[0])
          throw new DomainError(
            "plugin_release_conflict",
            "Use a plugin release to change an existing route pipeline",
          );
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
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
          plugin_pipeline: r.pluginPipeline,
          retry_policy: r.retryPolicy,
          enabled: r.enabled,
          source_revision: r.sourceRevision,
          config_epoch: epoch,
        };
      });
    },

    ...createPluginCatalogOperations({ orm }),

    async upsertPricingOverride(input: {
      id?: string;
      billing_account_id: string;
      model_name: string;
      operation?: string;
      override_kind: "EXACT_PRICE" | "MULTIPLIER";
      billing_price_id?: string | null;
      multiplier_numerator?: string | null;
      multiplier_denominator?: string | null;
      enabled?: boolean;
      source_revision?: number;
    }): Promise<
      BillingAccountPricingOverrideRecord & { config_epoch: number; source_revision: number }
    > {
      const id = input.id ?? crypto.randomUUID();
      const operation = input.operation ?? "*";
      const enabled = input.enabled ?? true;
      const sourceRev = input.source_revision;
      const billingPriceId =
        input.override_kind === "EXACT_PRICE" ? (input.billing_price_id ?? null) : null;
      const num =
        input.override_kind === "MULTIPLIER" ? (input.multiplier_numerator ?? null) : null;
      const den =
        input.override_kind === "MULTIPLIER" ? (input.multiplier_denominator ?? null) : null;
      return orm.transaction(async (tx) => {
        if (billingPriceId) {
          const [pr] = await tx
            .select({ id: priceVersions.id, side: priceVersions.side })
            .from(priceVersions)
            .where(eq(priceVersions.id, billingPriceId));
          if (!pr || pr.side !== "CUSTOMER") {
            throw new DomainError(
              "invalid_price_override",
              "Exact price override requires an existing CUSTOMER price version",
            );
          }
        }
        const rows = await tx
          .insert(billingAccountPricingOverrides)
          .values({
            id,
            billingAccountId: input.billing_account_id,
            modelName: input.model_name,
            operation,
            overrideKind: input.override_kind,
            billingPriceId,
            multiplierNumerator: num,
            multiplierDenominator: den,
            enabled,
            ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
          })
          .onConflictDoUpdate({
            target: [
              billingAccountPricingOverrides.billingAccountId,
              billingAccountPricingOverrides.modelName,
              billingAccountPricingOverrides.operation,
            ],
            ...(sourceRev !== undefined
              ? { setWhere: sql`${billingAccountPricingOverrides.sourceRevision} <= ${sourceRev}` }
              : {}),
            set: {
              overrideKind: input.override_kind,
              billingPriceId,
              multiplierNumerator: num,
              multiplierDenominator: den,
              enabled,
              ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
              updatedAt: sql`now()`,
            },
          })
          .returning();
        if (!rows[0]) {
          throw new DomainError(
            "stale_revision",
            "Incoming pricing override revision is older than current Gateway revision",
          );
        }
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: r.id,
          billing_account_id: r.billingAccountId,
          model_name: r.modelName,
          operation: r.operation,
          override_kind: r.overrideKind,
          billing_price_id: r.billingPriceId,
          multiplier_numerator: r.multiplierNumerator,
          multiplier_denominator: r.multiplierDenominator,
          enabled: r.enabled,
          source_revision: r.sourceRevision,
          config_epoch: epoch,
        };
      });
    },

    async listPricingOverrides(
      billingAccountId?: string,
    ): Promise<BillingAccountPricingOverrideRecord[]> {
      const rows = billingAccountId
        ? await orm
            .select()
            .from(billingAccountPricingOverrides)
            .where(eq(billingAccountPricingOverrides.billingAccountId, billingAccountId))
            .orderBy(
              asc(billingAccountPricingOverrides.modelName),
              asc(billingAccountPricingOverrides.operation),
            )
        : await orm
            .select()
            .from(billingAccountPricingOverrides)
            .orderBy(
              asc(billingAccountPricingOverrides.billingAccountId),
              asc(billingAccountPricingOverrides.modelName),
              asc(billingAccountPricingOverrides.operation),
            );
      return rows.map((r) => ({
        id: r.id,
        billing_account_id: r.billingAccountId,
        model_name: r.modelName,
        operation: r.operation,
        override_kind: r.overrideKind,
        billing_price_id: r.billingPriceId,
        multiplier_numerator: r.multiplierNumerator,
        multiplier_denominator: r.multiplierDenominator,
        enabled: r.enabled,
      }));
    },
  };
}
