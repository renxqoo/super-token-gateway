import type {
  PluginCapability,
  PluginKind,
  WebhookPluginConfig,
  ProviderPluginConfig,
  WasmPluginManifest,
  PluginRecord,
  ConfigSnapshot,
  BillingAccountRecord,
  ApiKeyRecord,
  UpstreamAccountRecord,
  ChannelRecord,
  ModelRecord,
  PriceVersionRecord,
  RouteRecord,
  BillingAccountPricingOverrideRecord,
} from "./types.ts";
import { catalogDigest, bumpEpoch } from "./catalog-epoch.ts";
import { canonical, DomainError } from "@gateway/kernel";
import {
  plugins,
  configEpoch,
  billingAccounts,
  apiKeys,
  upstreamAccounts,
  channels,
  models,
  priceVersions,
  billingAccountPricingOverrides,
  routes,
  pluginRevocations,
  type LedgerDrizzle,
} from "./schema.ts";
import { and, eq, asc, desc, inArray } from "drizzle-orm";

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

export function createPluginCatalogOperations({ orm }: { orm: LedgerDrizzle }) {
  return {
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
        ? catalogDigest(wasmBytes)
        : catalogDigest(
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
        billingAccountRows,
        keyRows,
        accountRows,
        channelRows,
        modelRows,
        priceRows,
        overrideRows,
        routeRows,
        pluginMetaRows,
        wasmBytesById,
        revokedRows,
      } = await orm.transaction(
        async (tx) => {
          const [
            epochRows,
            billingAccountRows,
            keyRows,
            accountRows,
            channelRows,
            modelRows,
            priceRows,
            overrideRows,
            routeRows,
            pluginMetaRows,
            revokedRows,
          ] = await Promise.all([
            tx
              .select({ version: configEpoch.version, lineageToken: configEpoch.lineageToken })
              .from(configEpoch)
              .where(eq(configEpoch.id, 1)),
            tx.select().from(billingAccounts),
            tx.select().from(apiKeys),
            tx.select().from(upstreamAccounts),
            tx.select().from(channels),
            tx.select().from(models),
            tx.select().from(priceVersions),
            tx
              .select()
              .from(billingAccountPricingOverrides)
              .where(eq(billingAccountPricingOverrides.enabled, true)),
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
            billingAccountRows,
            keyRows,
            accountRows,
            channelRows,
            modelRows,
            priceRows,
            overrideRows,
            routeRows,
            pluginMetaRows,
            wasmBytesById,
            revokedRows,
          };
        },
        { isolationLevel: "repeatable read", accessMode: "read only" },
      );

      const billingAccountMap = new Map<string, BillingAccountRecord>();
      for (const r of billingAccountRows) {
        billingAccountMap.set(r.id, {
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
          billing_account_id: r.billingAccountId,
          name: r.name,
          verifier: r.verifier,
          prefix: r.prefix,
          role: r.role,
          allowed_cidrs: r.allowedCidrs,
          models: r.models,
          operations: r.operations,
          enabled: r.enabled,
          expires_at: r.expiresAt?.toISOString() ?? null,
          spend_limit_atoms: r.spendLimitAtoms,
          spend_limit_window: r.spendLimitWindow,
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

      const pricingOverridesByBillingAccount = new Map<
        string,
        BillingAccountPricingOverrideRecord[]
      >();
      for (const r of overrideRows) {
        const rec: BillingAccountPricingOverrideRecord = {
          id: r.id,
          billing_account_id: r.billingAccountId,
          model_name: r.modelName,
          operation: r.operation,
          override_kind: r.overrideKind,
          billing_price_id: r.billingPriceId,
          multiplier_numerator: r.multiplierNumerator,
          multiplier_denominator: r.multiplierDenominator,
          enabled: r.enabled,
        };
        const list = pricingOverridesByBillingAccount.get(rec.billing_account_id);
        if (list) list.push(rec);
        else pricingOverridesByBillingAccount.set(rec.billing_account_id, [rec]);
      }

      return {
        epoch: epochRows[0]?.version ?? 0,
        lineageToken: epochRows[0]?.lineageToken ?? "",
        billingAccounts: billingAccountMap,
        keysByVerifier,
        accounts: accountMap,
        channels: channelMap,
        models: modelMap,
        prices: priceMap,
        pricingOverridesByBillingAccount,
        routesByModel,
        plugins: pluginMap,
        revokedPlugins: new Set(revokedRows.map(({ id }) => id)),
        providerPluginsByName,
      };
    },
  };
}
