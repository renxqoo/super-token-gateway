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
  apiKeys,
  channels,
  configEpoch,
  createDrizzle,
  models,
  plugins,
  pluginRevocations,
  priceVersions,
  projects,
  routes,
  upstreamAccounts,
  wallets,
  type LedgerDrizzle,
} from "./schema.ts";
import type {
  AdapterOverrides,
  ApiKeyRecord,
  ChannelKind,
  ChannelRecord,
  ConfigSnapshot,
  EpochStamp,
  ModelRecord,
  PluginCapability,
  PluginKind,
  PluginPipelineStep,
  PluginRecord,
  PriceBasis,
  PriceSide,
  PriceVersionRecord,
  ProjectRecord,
  ProviderPluginConfig,
  RouteRecord,
  RouteRetryPolicy,
  RoutingStrategy,
  UpstreamAccountRecord,
  WasmPluginManifest,
  WebhookPluginConfig,
} from "./types.ts";

function sha256Hex(value: string | Uint8Array): string {
  return new Bun.CryptoHasher("sha256").update(value).digest("hex");
}

type DrizzleTx = Parameters<Parameters<LedgerDrizzle["transaction"]>[0]>[0];

function pluginRecord(row: typeof plugins.$inferSelect): PluginRecord {
  return {
    id: row.id,
    name: row.name,
    version: row.version,
    capability: row.capability,
    kind: row.kind,
    digest: row.digest,
    wasm_bytes: row.wasmBytes ? new Uint8Array(row.wasmBytes) : null,
    webhook_config: row.webhookConfig,
    provider_config: row.providerConfig,
    signature: row.signature,
    signing_key_id: row.signingKeyId,
    manifest: row.manifest,
    enabled: row.enabled,
    builtin: row.builtin,
    created_at: row.createdAt,
  };
}

async function bumpEpoch(tx: DrizzleTx): Promise<number> {
  const rows = await tx
    .update(configEpoch)
    .set({
      version: sql`${configEpoch.version} + 1`,
      updatedAt: sql`now()`,
    })
    .where(eq(configEpoch.id, 1))
    .returning({ version: configEpoch.version });
  return rows[0]!.version;
}

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

    async upsertProject(input: {
      id?: string;
      external_ref?: string | null;
      name: string;
      currency?: string;
      rpm: number;
      tpm: number;
      concurrency: number;
      enabled?: boolean;
    }): Promise<ProjectRecord & { config_epoch: number }> {
      const id = input.id ?? crypto.randomUUID();
      const currency = input.currency ?? "USD";
      const externalRef = input.external_ref ?? null;
      const enabled = input.enabled ?? true;
      return orm.transaction(async (tx) => {
        const rows = await tx
          .insert(projects)
          .values({
            id,
            externalRef,
            name: input.name,
            currency,
            rpm: input.rpm,
            tpm: input.tpm,
            concurrency: input.concurrency,
            enabled,
          })
          .onConflictDoUpdate({
            target: projects.id,
            set: {
              externalRef,
              name: input.name,
              currency,
              rpm: input.rpm,
              tpm: input.tpm,
              concurrency: input.concurrency,
              enabled,
              updatedAt: sql`now()`,
            },
          })
          .returning();
        await tx
          .insert(wallets)
          .values({
            id: crypto.randomUUID(),
            projectId: id,
            currency,
          })
          .onConflictDoNothing({ target: [wallets.projectId, wallets.currency] });
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: r.id,
          external_ref: r.externalRef,
          name: r.name,
          currency: r.currency,
          rpm: r.rpm,
          tpm: r.tpm,
          concurrency: r.concurrency,
          enabled: r.enabled,
          config_epoch: epoch,
        };
      });
    },

    async upsertApiKey(input: {
      id?: string;
      project_id: string;
      name: string;
      verifier: string;
      prefix: string;
      role?: "ADMIN" | "BILLING" | "INVOKER" | "VIEWER";
      allowed_cidrs?: string[];
      models?: string[];
      operations?: string[];
      enabled?: boolean;
      expires_at?: string | null;
    }): Promise<ApiKeyRecord & { config_epoch: number }> {
      const id = input.id ?? crypto.randomUUID();
      const role = input.role ?? "INVOKER";
      const allowedCidrs = input.allowed_cidrs ?? [];
      const keyModels = input.models ?? [];
      const keyOperations = input.operations ?? [];
      const enabled = input.enabled ?? true;
      const expiresAt = input.expires_at ?? null;
      return orm.transaction(async (tx) => {
        const rows = await tx
          .insert(apiKeys)
          .values({
            id,
            projectId: input.project_id,
            name: input.name,
            verifier: input.verifier,
            prefix: input.prefix,
            role,
            allowedCidrs,
            models: keyModels,
            operations: keyOperations,
            enabled,
            expiresAt,
          })
          .onConflictDoUpdate({
            target: apiKeys.id,
            set: {
              name: input.name,
              role,
              allowedCidrs,
              models: keyModels,
              operations: keyOperations,
              enabled,
              expiresAt,
            },
          })
          .returning();
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: r.id,
          project_id: r.projectId,
          name: r.name,
          verifier: r.verifier,
          prefix: r.prefix,
          role: r.role,
          allowed_cidrs: r.allowedCidrs,
          models: r.models,
          operations: r.operations,
          enabled: r.enabled,
          expires_at: r.expiresAt ? new Date(r.expiresAt).toISOString() : null,
          config_epoch: epoch,
        };
      });
    },

    async upsertUpstreamAccount(input: {
      id?: string;
      name: string;
      state?: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED";
    }): Promise<UpstreamAccountRecord & { config_epoch: number }> {
      const id = input.id ?? crypto.randomUUID();
      const state = input.state ?? "ACTIVE";
      return orm.transaction(async (tx) => {
        const rows = await tx
          .insert(upstreamAccounts)
          .values({ id, name: input.name, state })
          .onConflictDoUpdate({
            target: upstreamAccounts.id,
            set: {
              name: input.name,
              state,
              revision: sql`${upstreamAccounts.revision} + 1`,
              updatedAt: sql`now()`,
            },
          })
          .returning();
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: r.id,
          name: r.name,
          state: r.state,
          revision: r.revision,
          config_epoch: epoch,
        };
      });
    },

    async upsertChannel(input: {
      id?: string;
      account_id: string;
      name: string;
      kind: ChannelKind;
      provider_plugin_id?: string | null;
      base_url: string;
      secret_ciphertext: string;
      rpm: number;
      concurrency: number;
      enabled?: boolean;
      routing_policy?: Record<string, unknown>;
      adapter_config?: AdapterOverrides;
    }): Promise<ChannelRecord & { config_epoch: number }> {
      const id = input.id ?? crypto.randomUUID();
      const enabled = input.enabled ?? true;
      const routingPolicy = input.routing_policy ?? {};
      const adapterConfig = input.adapter_config ?? {};
      return orm.transaction(async (tx) => {
        const [existing] = await tx
          .select({ providerId: channels.providerPluginId })
          .from(channels)
          .where(eq(channels.id, id));
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
        const rows = await tx
          .insert(channels)
          .values({
            id,
            accountId: input.account_id,
            name: input.name,
            kind: input.kind,
            providerPluginId,
            baseUrl: input.base_url,
            secretCiphertext: input.secret_ciphertext,
            rpm: input.rpm,
            concurrency: input.concurrency,
            enabled,
            routingPolicy,
            adapterConfig,
          })
          .onConflictDoUpdate({
            target: channels.id,
            setWhere: sql`${channels.providerPluginId} IS NULL OR ${channels.providerPluginId} IS NOT DISTINCT FROM ${providerPluginId}::uuid`,
            set: {
              accountId: input.account_id,
              name: input.name,
              kind: input.kind,
              providerPluginId,
              baseUrl: input.base_url,
              secretCiphertext: input.secret_ciphertext,
              rpm: input.rpm,
              concurrency: input.concurrency,
              enabled,
              routingPolicy,
              adapterConfig,
              configRevision: sql`${channels.configRevision} + 1`,
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
    }): Promise<ModelRecord & { config_epoch: number }> {
      const routingStrategy = input.routing_strategy ?? "WEIGHTED";
      const enabled = input.enabled ?? true;
      return orm.transaction(async (tx) => {
        const rows = await tx
          .insert(models)
          .values({
            name: input.name,
            operations: input.operations,
            routingStrategy,
            maxOutputTokens: input.max_output_tokens,
            enabled,
          })
          .onConflictDoUpdate({
            target: models.name,
            set: {
              operations: input.operations,
              routingStrategy,
              maxOutputTokens: input.max_output_tokens,
              enabled,
            },
          })
          .returning();
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          name: r.name,
          operations: r.operations,
          routing_strategy: r.routingStrategy,
          max_output_tokens: r.maxOutputTokens,
          enabled: r.enabled,
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
    }): Promise<PriceVersionRecord & { config_epoch: number }> {
      validatePricing({ currency: input.currency, rates: input.rates });
      const id = input.id ?? crypto.randomUUID();
      const basis = input.basis ?? "PROVIDER_USAGE";
      const ceilings = input.ceilings ?? {};
      const sourceDigest = sha256Hex(
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
          .returning();
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
    }): Promise<RouteRecord & { config_epoch: number }> {
      const id = input.id ?? crypto.randomUUID();
      const priority = input.priority ?? 0;
      const weight = input.weight ?? 1;
      const requestPolicy = input.request_policy ?? {};
      const adapterOverrides = input.adapter_overrides ?? {};
      const retryPolicy = input.retry_policy ?? {};
      const enabled = input.enabled ?? true;
      return orm.transaction(async (tx) => {
        const [existing] = await tx
          .select({ pipeline: routes.pluginPipeline })
          .from(routes)
          .where(eq(routes.id, id));
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
          })
          .onConflictDoUpdate({
            target: routes.id,
            ...(input.plugin_pipeline !== undefined
              ? {
                  setWhere: sql`${routes.pluginPipeline} = ${JSON.stringify(pluginPipeline)}::text::jsonb`,
                }
              : {}),
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
          config_epoch: epoch,
        };
      });
    },

    async upsertPlugin(input: {
      id?: string;
      name: string;
      version: string;
      capability?: PluginCapability;
      kind: PluginKind;
      wasm_bytes?: Uint8Array | null;
      webhook_config?: WebhookPluginConfig | null;
      provider_config?: ProviderPluginConfig | null;
      signature?: string | null;
      signing_key_id?: string | null;
      manifest?: WasmPluginManifest | Record<string, unknown>;
    }): Promise<PluginRecord & { config_epoch: number }> {
      const id = input.id ?? crypto.randomUUID();
      const capability: PluginCapability = input.capability ?? "CONTENT";
      const wasmBytes = input.wasm_bytes ?? null;
      const webhookConfig = input.webhook_config ?? null;
      const providerConfig = input.provider_config ?? null;
      const manifest = (input.manifest ?? {}) as Record<string, unknown>;
      const signature = input.signature ?? null;
      const signingKeyId = input.signing_key_id ?? null;
      const digest = wasmBytes
        ? sha256Hex(wasmBytes)
        : sha256Hex(
            canonical({
              name: input.name,
              version: input.version,
              capability,
              kind: input.kind,
              webhook_config: webhookConfig,
              provider_config: providerConfig,
              manifest,
            }),
          );
      return orm.transaction(async (tx) => {
        const rows = await tx
          .insert(plugins)
          .values({
            id,
            name: input.name,
            version: input.version,
            capability,
            kind: input.kind,
            digest,
            wasmBytes,
            webhookConfig,
            providerConfig,
            signature,
            signingKeyId,
            manifest,
            enabled: false,
          })
          .onConflictDoNothing({ target: [plugins.name, plugins.version] })
          .returning();
        const [r] =
          rows.length > 0
            ? rows
            : await tx
                .select()
                .from(plugins)
                .where(and(eq(plugins.name, input.name), eq(plugins.version, input.version)));
        if (
          !r ||
          (input.id !== undefined && r.id !== input.id) ||
          digest !== r.digest ||
          capability !== r.capability ||
          input.kind !== r.kind ||
          signature !== r.signature ||
          signingKeyId !== r.signingKeyId ||
          canonical(manifest) !== canonical(r.manifest) ||
          canonical(providerConfig) !== canonical(r.providerConfig) ||
          canonical(webhookConfig) !== canonical(r.webhookConfig)
        ) {
          throw new DomainError(
            "plugin_revision_conflict",
            "Plugin revisions are immutable. Choose a new version.",
          );
        }
        const epoch =
          rows.length > 0
            ? await bumpEpoch(tx)
            : (
                await tx
                  .select({ version: configEpoch.version })
                  .from(configEpoch)
                  .where(eq(configEpoch.id, 1))
              )[0]!.version;
        return { ...pluginRecord(r), config_epoch: epoch };
      });
    },

    async getPlugin(id: string): Promise<PluginRecord | null> {
      const [row] = await orm.select().from(plugins).where(eq(plugins.id, id));
      return row ? pluginRecord(row) : null;
    },

    async listPlugins(capability?: PluginCapability): Promise<Omit<PluginRecord, "wasm_bytes">[]> {
      const base = orm
        .select({
          id: plugins.id,
          name: plugins.name,
          version: plugins.version,
          capability: plugins.capability,
          kind: plugins.kind,
          digest: plugins.digest,
          webhookConfig: plugins.webhookConfig,
          providerConfig: plugins.providerConfig,
          signature: plugins.signature,
          signingKeyId: plugins.signingKeyId,
          manifest: plugins.manifest,
          enabled: plugins.enabled,
          builtin: plugins.builtin,
          createdAt: plugins.createdAt,
        })
        .from(plugins);
      const rows = await (
        capability ? base.where(eq(plugins.capability, capability)) : base
      ).orderBy(asc(plugins.name), asc(plugins.version));
      return rows.map((r) => ({
        id: r.id,
        name: r.name,
        version: r.version,
        capability: r.capability,
        kind: r.kind,
        digest: r.digest,
        webhook_config: r.webhookConfig,
        provider_config: r.providerConfig,
        signature: r.signature,
        signing_key_id: r.signingKeyId,
        manifest: r.manifest,
        enabled: r.enabled,
        builtin: r.builtin,
        created_at: r.createdAt,
      }));
    },

    async loadSnapshot(previous?: ConfigSnapshot): Promise<ConfigSnapshot> {
      const {
        epochRows,
        projectRows,
        keyRows,
        accountRows,
        channelRows,
        modelRows,
        priceRows,
        routeRows,
        pluginMetaRows,
        wasmBytesById,
        revokedRows,
      } = await orm.transaction(
        async (tx) => {
          const [
            epochRows,
            projectRows,
            keyRows,
            accountRows,
            channelRows,
            modelRows,
            priceRows,
            routeRows,
            pluginMetaRows,
            revokedRows,
          ] = await Promise.all([
            tx
              .select({ version: configEpoch.version, lineageToken: configEpoch.lineageToken })
              .from(configEpoch)
              .where(eq(configEpoch.id, 1)),
            tx.select().from(projects),
            tx.select().from(apiKeys),
            tx.select().from(upstreamAccounts),
            tx.select().from(channels),
            tx.select().from(models),
            tx.select().from(priceVersions),
            tx
              .select()
              .from(routes)
              .where(eq(routes.enabled, true))
              .orderBy(asc(routes.modelName), desc(routes.priority), asc(routes.id)),
            tx
              .select({
                id: plugins.id,
                name: plugins.name,
                version: plugins.version,
                capability: plugins.capability,
                kind: plugins.kind,
                digest: plugins.digest,
                webhookConfig: plugins.webhookConfig,
                providerConfig: plugins.providerConfig,
                signature: plugins.signature,
                signingKeyId: plugins.signingKeyId,
                manifest: plugins.manifest,
                enabled: plugins.enabled,
                builtin: plugins.builtin,
                createdAt: plugins.createdAt,
              })
              .from(plugins)
              .where(eq(plugins.enabled, true))
              .orderBy(desc(plugins.createdAt), asc(plugins.id)),
            tx.select({ id: pluginRevocations.pluginId }).from(pluginRevocations),
          ]);
          const missingWasmIds: string[] = [];
          for (const row of pluginMetaRows) {
            if (row.kind !== "WASM") continue;
            const prev = previous?.plugins.get(row.id);
            if (!prev || prev.digest !== row.digest || !prev.wasm_bytes) {
              missingWasmIds.push(row.id);
            }
          }
          const wasmBytesById = new Map<string, Uint8Array | null>();
          if (missingWasmIds.length > 0) {
            const binaryRows = await tx
              .select({ id: plugins.id, wasmBytes: plugins.wasmBytes })
              .from(plugins)
              .where(inArray(plugins.id, missingWasmIds));
            for (const row of binaryRows) {
              wasmBytesById.set(row.id, row.wasmBytes ? new Uint8Array(row.wasmBytes) : null);
            }
          }
          return {
            epochRows,
            projectRows,
            keyRows,
            accountRows,
            channelRows,
            modelRows,
            priceRows,
            routeRows,
            pluginMetaRows,
            wasmBytesById,
            revokedRows,
          };
        },
        { isolationLevel: "repeatable read", accessMode: "read only" },
      );

      const projectMap = new Map<string, ProjectRecord>();
      for (const r of projectRows) {
        projectMap.set(r.id, {
          id: r.id,
          external_ref: r.externalRef,
          name: r.name,
          currency: r.currency,
          rpm: r.rpm,
          tpm: r.tpm,
          concurrency: r.concurrency,
          enabled: r.enabled,
        });
      }

      const keysByVerifier = new Map<string, ApiKeyRecord>();
      for (const r of keyRows) {
        keysByVerifier.set(r.verifier, {
          id: r.id,
          project_id: r.projectId,
          name: r.name,
          verifier: r.verifier,
          prefix: r.prefix,
          role: r.role,
          allowed_cidrs: r.allowedCidrs,
          models: r.models,
          operations: r.operations,
          enabled: r.enabled,
          expires_at: r.expiresAt ? new Date(r.expiresAt).toISOString() : null,
        });
      }

      const accountMap = new Map<string, UpstreamAccountRecord>();
      for (const r of accountRows) {
        accountMap.set(r.id, {
          id: r.id,
          name: r.name,
          state: r.state,
          revision: r.revision,
        });
      }

      const channelMap = new Map<string, ChannelRecord>();
      for (const r of channelRows) {
        channelMap.set(r.id, {
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
        });
      }

      const modelMap = new Map<string, ModelRecord>();
      for (const r of modelRows) {
        modelMap.set(r.name, {
          name: r.name,
          operations: r.operations,
          routing_strategy: r.routingStrategy,
          max_output_tokens: r.maxOutputTokens,
          enabled: r.enabled,
        });
      }

      const priceMap = new Map<string, PriceVersionRecord>();
      for (const r of priceRows) {
        priceMap.set(r.id, {
          id: r.id,
          side: r.side,
          currency: r.currency,
          basis: r.basis,
          rates: r.rates,
          ceilings: r.ceilings,
          source_digest: r.sourceDigest,
        });
      }

      const routesByModel = new Map<string, RouteRecord[]>();
      for (const r of routeRows) {
        const rec: RouteRecord = {
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
        };
        const list = routesByModel.get(rec.model_name);
        if (list) list.push(rec);
        else routesByModel.set(rec.model_name, [rec]);
      }

      const pluginMap = new Map<string, PluginRecord>();
      const providerPluginsByName = new Map<string, PluginRecord>();
      for (const r of pluginMetaRows) {
        const cachedBytes =
          r.kind === "WASM" && !wasmBytesById.has(r.id)
            ? (previous?.plugins.get(r.id)?.wasm_bytes ?? null)
            : (wasmBytesById.get(r.id) ?? null);
        const rec: PluginRecord = {
          id: r.id,
          name: r.name,
          version: r.version,
          capability: r.capability,
          kind: r.kind,
          digest: r.digest,
          wasm_bytes: cachedBytes,
          webhook_config: r.webhookConfig,
          provider_config: r.providerConfig,
          signature: r.signature,
          signing_key_id: r.signingKeyId,
          manifest: r.manifest,
          enabled: r.enabled,
          builtin: r.builtin,
          created_at: r.createdAt,
        };
        pluginMap.set(rec.id, rec);
        if (rec.capability === "PROVIDER" && !r.builtin && !providerPluginsByName.has(rec.name)) {
          providerPluginsByName.set(rec.name, rec);
        }
      }

      return {
        epoch: epochRows[0]?.version ?? 0,
        lineageToken: epochRows[0]?.lineageToken ?? "",
        projects: projectMap,
        keysByVerifier,
        accounts: accountMap,
        channels: channelMap,
        models: modelMap,
        prices: priceMap,
        routesByModel,
        plugins: pluginMap,
        revokedPlugins: new Set(revokedRows.map(({ id }) => id)),
        providerPluginsByName,
      };
    },
  };
}
