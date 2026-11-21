import type { SQL } from "bun";
import { asc, desc, eq, sql } from "drizzle-orm";
import { canonical, validatePricing, type Operation, type Pricing } from "@gateway/kernel";
import {
  apiKeys,
  channels,
  configEpoch,
  createDrizzle,
  models,
  plugins,
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
      const enabled = input.enabled ?? true;
      const externalRef = input.external_ref ?? null;
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
      const providerPluginId = input.provider_plugin_id ?? null;
      return orm.transaction(async (tx) => {
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
      const pluginPipeline = input.plugin_pipeline ?? [];
      const retryPolicy = input.retry_policy ?? {};
      const enabled = input.enabled ?? true;
      return orm.transaction(async (tx) => {
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
              pluginPipeline,
              retryPolicy,
              enabled,
            },
          })
          .returning();
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
      enabled?: boolean;
    }): Promise<PluginRecord & { config_epoch: number }> {
      const id = input.id ?? crypto.randomUUID();
      const capability: PluginCapability = input.capability ?? "CONTENT";
      const wasmBytes = input.wasm_bytes ?? null;
      const webhookConfig = input.webhook_config ?? null;
      const providerConfig = input.provider_config ?? null;
      const manifest = (input.manifest ?? {}) as Record<string, unknown>;
      const enabled = input.enabled ?? true;
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
            enabled,
          })
          .onConflictDoUpdate({
            target: [plugins.name, plugins.version],
            set: {
              capability,
              kind: input.kind,
              digest,
              wasmBytes,
              webhookConfig,
              providerConfig,
              signature,
              signingKeyId,
              manifest,
              enabled,
            },
          })
          .returning();
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: r.id,
          name: r.name,
          version: r.version,
          capability: r.capability,
          kind: r.kind,
          digest: r.digest,
          wasm_bytes: r.wasmBytes ? new Uint8Array(r.wasmBytes) : null,
          webhook_config: r.webhookConfig,
          provider_config: r.providerConfig,
          signature: r.signature,
          signing_key_id: r.signingKeyId,
          manifest: r.manifest,
          enabled: r.enabled,
          created_at: r.createdAt,
          config_epoch: epoch,
        };
      });
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
        created_at: r.createdAt,
      }));
    },

    async loadSnapshot(): Promise<ConfigSnapshot> {
      const [
        epochRows,
        projectRows,
        keyRows,
        accountRows,
        channelRows,
        modelRows,
        priceRows,
        routeRows,
        pluginRows,
      ] = await orm.transaction(
        (tx) =>
          Promise.all([
            tx
              .select({ version: configEpoch.version })
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
              .select()
              .from(plugins)
              .where(eq(plugins.enabled, true))
              .orderBy(desc(plugins.createdAt), asc(plugins.id)),
          ]),
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
      for (const r of pluginRows) {
        const rec: PluginRecord = {
          id: r.id,
          name: r.name,
          version: r.version,
          capability: r.capability,
          kind: r.kind,
          digest: r.digest,
          wasm_bytes: r.wasmBytes ? new Uint8Array(r.wasmBytes) : null,
          webhook_config: r.webhookConfig,
          provider_config: r.providerConfig,
          signature: r.signature,
          signing_key_id: r.signingKeyId,
          manifest: r.manifest,
          enabled: r.enabled,
          created_at: r.createdAt,
        };
        pluginMap.set(rec.id, rec);
        if (rec.capability === "PROVIDER" && !providerPluginsByName.has(rec.name)) {
          providerPluginsByName.set(rec.name, rec);
        }
      }

      return {
        epoch: epochRows[0]?.version ?? 0,
        projects: projectMap,
        keysByVerifier,
        accounts: accountMap,
        channels: channelMap,
        models: modelMap,
        prices: priceMap,
        routesByModel,
        plugins: pluginMap,
        providerPluginsByName,
      };
    },
  };
}
