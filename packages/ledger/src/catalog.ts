import type { SQL } from "bun";
import { canonical, validatePricing, type Operation, type Pricing } from "@gateway/kernel";
import type {
  ApiKeyRecord,
  ChannelKind,
  ChannelRecord,
  ConfigSnapshot,
  ModelRecord,
  PluginKind,
  PluginPipelineStep,
  PluginRecord,
  PriceBasis,
  PriceSide,
  PriceVersionRecord,
  ProjectRecord,
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

async function bumpEpoch(tx: SQL): Promise<number> {
  const rows =
    await tx`UPDATE public.config_epoch SET version = version + 1, updated_at = now() WHERE id = 1 RETURNING version::text`;
  return Number(rows[0]!.version);
}

export function createCatalogRepository(db: SQL) {
  return {
    async getEpoch(): Promise<number> {
      const rows = await db`SELECT version::text FROM public.config_epoch WHERE id = 1`;
      return Number(rows[0]?.version ?? 0);
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
      const ext = input.external_ref ?? null;
      return db.begin(async (tx) => {
        const rows = await tx`
          INSERT INTO public.projects (id, external_ref, name, currency, rpm, tpm, concurrency, enabled)
          VALUES (${id}, ${ext}, ${input.name}, ${currency}, ${input.rpm}, ${input.tpm}, ${input.concurrency}, ${enabled})
          ON CONFLICT (id) DO UPDATE SET
            external_ref = excluded.external_ref,
            name = excluded.name,
            currency = excluded.currency,
            rpm = excluded.rpm,
            tpm = excluded.tpm,
            concurrency = excluded.concurrency,
            enabled = excluded.enabled,
            updated_at = now()
          RETURNING id, external_ref, name, currency, rpm, tpm::text, concurrency, enabled
        `;
        await tx`
          INSERT INTO public.wallets (id, project_id, currency)
          VALUES (${crypto.randomUUID()}, ${id}, ${currency})
          ON CONFLICT (project_id, currency) DO NOTHING
        `;
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: String(r.id),
          external_ref: r.external_ref ? String(r.external_ref) : null,
          name: String(r.name),
          currency: String(r.currency),
          rpm: Number(r.rpm),
          tpm: Number(r.tpm),
          concurrency: Number(r.concurrency),
          enabled: Boolean(r.enabled),
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
      const models = input.models ?? [];
      const operations = input.operations ?? [];
      const enabled = input.enabled ?? true;
      const expiresAt = input.expires_at ?? null;
      return db.begin(async (tx) => {
        const rows = await tx`
          INSERT INTO public.api_keys (id, project_id, name, verifier, prefix, role, allowed_cidrs, models, operations, enabled, expires_at)
          VALUES (${id}, ${input.project_id}, ${input.name}, ${input.verifier}, ${input.prefix}, ${role}, ${allowedCidrs}, ${models}, ${operations}, ${enabled}, ${expiresAt}::timestamptz)
          ON CONFLICT (id) DO UPDATE SET
            name = excluded.name,
            role = excluded.role,
            allowed_cidrs = excluded.allowed_cidrs,
            models = excluded.models,
            operations = excluded.operations,
            enabled = excluded.enabled,
            expires_at = excluded.expires_at
          RETURNING id, project_id, name, verifier, prefix, role, allowed_cidrs, models, operations, enabled, expires_at
        `;
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: String(r.id),
          project_id: String(r.project_id),
          name: String(r.name),
          verifier: String(r.verifier),
          prefix: String(r.prefix),
          role: r.role as ApiKeyRecord["role"],
          allowed_cidrs: (r.allowed_cidrs ?? []) as string[],
          models: (r.models ?? []) as string[],
          operations: (r.operations ?? []) as string[],
          enabled: Boolean(r.enabled),
          expires_at: r.expires_at ? new Date(r.expires_at).toISOString() : null,
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
      return db.begin(async (tx) => {
        const rows = await tx`
          INSERT INTO public.upstream_accounts (id, name, state)
          VALUES (${id}, ${input.name}, ${state})
          ON CONFLICT (id) DO UPDATE SET
            name = excluded.name,
            state = excluded.state,
            revision = public.upstream_accounts.revision + 1,
            updated_at = now()
          RETURNING id, name, state, revision::text
        `;
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: String(r.id),
          name: String(r.name),
          state: r.state as UpstreamAccountRecord["state"],
          revision: String(r.revision),
          config_epoch: epoch,
        };
      });
    },

    async upsertChannel(input: {
      id?: string;
      account_id: string;
      name: string;
      kind: ChannelKind;
      base_url: string;
      secret_ciphertext: string;
      rpm: number;
      concurrency: number;
      enabled?: boolean;
      routing_policy?: Record<string, unknown>;
    }): Promise<ChannelRecord & { config_epoch: number }> {
      const id = input.id ?? crypto.randomUUID();
      const enabled = input.enabled ?? true;
      const routingPolicy = input.routing_policy ?? {};
      return db.begin(async (tx) => {
        const rows = await tx`
          INSERT INTO public.channels (id, account_id, name, kind, base_url, secret_ciphertext, rpm, concurrency, enabled, routing_policy)
          VALUES (${id}, ${input.account_id}, ${input.name}, ${input.kind}, ${input.base_url}, ${input.secret_ciphertext}, ${input.rpm}, ${input.concurrency}, ${enabled}, ${routingPolicy}::jsonb)
          ON CONFLICT (id) DO UPDATE SET
            account_id = excluded.account_id,
            name = excluded.name,
            kind = excluded.kind,
            base_url = excluded.base_url,
            secret_ciphertext = excluded.secret_ciphertext,
            rpm = excluded.rpm,
            concurrency = excluded.concurrency,
            enabled = excluded.enabled,
            routing_policy = excluded.routing_policy,
            config_revision = public.channels.config_revision + 1,
            updated_at = now()
          RETURNING id, account_id, name, kind, base_url, secret_ciphertext, rpm, concurrency, enabled, routing_policy, config_revision::text
        `;
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: String(r.id),
          account_id: String(r.account_id),
          name: String(r.name),
          kind: r.kind as ChannelKind,
          base_url: String(r.base_url),
          secret_ciphertext: String(r.secret_ciphertext),
          rpm: Number(r.rpm),
          concurrency: Number(r.concurrency),
          enabled: Boolean(r.enabled),
          routing_policy: (r.routing_policy ?? {}) as Record<string, unknown>,
          config_revision: Number(r.config_revision),
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
      const strategy = input.routing_strategy ?? "WEIGHTED";
      const enabled = input.enabled ?? true;
      return db.begin(async (tx) => {
        const rows = await tx`
          INSERT INTO public.models (name, operations, routing_strategy, max_output_tokens, enabled)
          VALUES (${input.name}, ${input.operations}, ${strategy}, ${input.max_output_tokens}, ${enabled})
          ON CONFLICT (name) DO UPDATE SET
            operations = excluded.operations,
            routing_strategy = excluded.routing_strategy,
            max_output_tokens = excluded.max_output_tokens,
            enabled = excluded.enabled
          RETURNING name, operations, routing_strategy, max_output_tokens, enabled
        `;
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          name: String(r.name),
          operations: (r.operations ?? []) as Operation[],
          routing_strategy: r.routing_strategy as RoutingStrategy,
          max_output_tokens: Number(r.max_output_tokens),
          enabled: Boolean(r.enabled),
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
      const digest = sha256Hex(
        canonical({
          side: input.side,
          currency: input.currency,
          basis,
          rates: input.rates,
          ceilings,
        }),
      );
      return db.begin(async (tx) => {
        const rows = await tx`
          INSERT INTO public.price_versions (id, side, currency, basis, rates, ceilings, source_digest)
          VALUES (${id}, ${input.side}, ${input.currency}, ${basis}, ${input.rates}::jsonb, ${ceilings}::jsonb, ${digest})
          RETURNING id, side, currency, basis, rates, ceilings, source_digest
        `;
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: String(r.id),
          side: r.side as PriceSide,
          currency: String(r.currency),
          basis: r.basis as PriceBasis,
          rates: r.rates as Pricing["rates"],
          ceilings: (r.ceilings ?? {}) as Record<string, string>,
          source_digest: String(r.source_digest),
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
      plugin_pipeline?: PluginPipelineStep[];
      retry_policy?: RouteRetryPolicy;
      enabled?: boolean;
    }): Promise<RouteRecord & { config_epoch: number }> {
      const id = input.id ?? crypto.randomUUID();
      const priority = input.priority ?? 0;
      const weight = input.weight ?? 1;
      const requestPolicy = input.request_policy ?? {};
      const pluginPipeline = input.plugin_pipeline ?? [];
      const retryPolicy = input.retry_policy ?? {};
      const enabled = input.enabled ?? true;
      return db.begin(async (tx) => {
        const rows = await tx`
          INSERT INTO public.routes (id, model_name, channel_id, upstream_model, operations, priority, weight, billing_price_id, cost_version_id, request_policy, plugin_pipeline, retry_policy, enabled)
          VALUES (${id}, ${input.model_name}, ${input.channel_id}, ${input.upstream_model}, ${input.operations}, ${priority}, ${weight}, ${input.billing_price_id}, ${input.cost_version_id}, ${requestPolicy}::jsonb, ${pluginPipeline}::jsonb, ${retryPolicy}::jsonb, ${enabled})
          ON CONFLICT (id) DO UPDATE SET
            channel_id = excluded.channel_id,
            upstream_model = excluded.upstream_model,
            operations = excluded.operations,
            priority = excluded.priority,
            weight = excluded.weight,
            billing_price_id = excluded.billing_price_id,
            cost_version_id = excluded.cost_version_id,
            request_policy = excluded.request_policy,
            plugin_pipeline = excluded.plugin_pipeline,
            retry_policy = excluded.retry_policy,
            enabled = excluded.enabled
          RETURNING id, model_name, channel_id, upstream_model, operations, priority, weight, billing_price_id, cost_version_id, request_policy, plugin_pipeline, retry_policy, enabled
        `;
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: String(r.id),
          model_name: String(r.model_name),
          channel_id: String(r.channel_id),
          upstream_model: String(r.upstream_model),
          operations: (r.operations ?? []) as Operation[],
          priority: Number(r.priority),
          weight: Number(r.weight),
          billing_price_id: String(r.billing_price_id),
          cost_version_id: String(r.cost_version_id),
          request_policy: (r.request_policy ?? {}) as Record<string, unknown>,
          plugin_pipeline: (r.plugin_pipeline ?? []) as PluginPipelineStep[],
          retry_policy: (r.retry_policy ?? {}) as RouteRetryPolicy,
          enabled: Boolean(r.enabled),
          config_epoch: epoch,
        };
      });
    },

    async upsertPlugin(input: {
      id?: string;
      name: string;
      version: string;
      kind: PluginKind;
      wasm_bytes?: Uint8Array | null;
      webhook_config?: WebhookPluginConfig | null;
      signature?: string | null;
      signing_key_id?: string | null;
      manifest?: WasmPluginManifest | Record<string, unknown>;
      enabled?: boolean;
    }): Promise<PluginRecord & { config_epoch: number }> {
      const id = input.id ?? crypto.randomUUID();
      const wasmBytes = input.wasm_bytes ?? null;
      const webhookConfig = input.webhook_config ?? null;
      const manifest = input.manifest ?? {};
      const enabled = input.enabled ?? true;
      const digest = wasmBytes
        ? sha256Hex(wasmBytes)
        : sha256Hex(
            canonical({
              name: input.name,
              version: input.version,
              kind: input.kind,
              webhook_config: webhookConfig,
              manifest,
            }),
          );
      return db.begin(async (tx) => {
        const rows = await tx`
          INSERT INTO public.plugins (id, name, version, kind, digest, wasm_bytes, webhook_config, signature, signing_key_id, manifest, enabled)
          VALUES (${id}, ${input.name}, ${input.version}, ${input.kind}, ${digest}, ${wasmBytes}, ${webhookConfig}::jsonb, ${input.signature ?? null}, ${input.signing_key_id ?? null}, ${manifest}::jsonb, ${enabled})
          ON CONFLICT (name, version) DO UPDATE SET
            digest = excluded.digest,
            wasm_bytes = excluded.wasm_bytes,
            webhook_config = excluded.webhook_config,
            signature = excluded.signature,
            signing_key_id = excluded.signing_key_id,
            manifest = excluded.manifest,
            enabled = excluded.enabled
          RETURNING id, name, version, kind, digest, wasm_bytes, webhook_config, signature, signing_key_id, manifest, enabled
        `;
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: String(r.id),
          name: String(r.name),
          version: String(r.version),
          kind: r.kind as PluginKind,
          digest: String(r.digest),
          wasm_bytes: r.wasm_bytes ? new Uint8Array(r.wasm_bytes) : null,
          webhook_config: (r.webhook_config ?? null) as WebhookPluginConfig | null,
          signature: r.signature ? String(r.signature) : null,
          signing_key_id: r.signing_key_id ? String(r.signing_key_id) : null,
          manifest: (r.manifest ?? {}) as Record<string, unknown>,
          enabled: Boolean(r.enabled),
          config_epoch: epoch,
        };
      });
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
      ] = await Promise.all([
        db`SELECT version::text FROM public.config_epoch WHERE id = 1`,
        db`SELECT id, external_ref, name, currency, rpm, tpm::text, concurrency, enabled FROM public.projects`,
        db`SELECT id, project_id, name, verifier, prefix, role, allowed_cidrs, models, operations, enabled, expires_at FROM public.api_keys`,
        db`SELECT id, name, state, revision::text FROM public.upstream_accounts`,
        db`SELECT id, account_id, name, kind, base_url, secret_ciphertext, rpm, concurrency, enabled, routing_policy, config_revision::text FROM public.channels`,
        db`SELECT name, operations, routing_strategy, max_output_tokens, enabled FROM public.models`,
        db`SELECT id, side, currency, basis, rates, ceilings, source_digest FROM public.price_versions`,
        db`SELECT id, model_name, channel_id, upstream_model, operations, priority, weight, billing_price_id, cost_version_id, request_policy, plugin_pipeline, retry_policy, enabled FROM public.routes WHERE enabled ORDER BY model_name, priority DESC, id`,
        db`SELECT id, name, version, kind, digest, wasm_bytes, webhook_config, signature, signing_key_id, manifest, enabled FROM public.plugins WHERE enabled`,
      ]);

      const projects = new Map<string, ProjectRecord>();
      for (const r of projectRows) {
        projects.set(String(r.id), {
          id: String(r.id),
          external_ref: r.external_ref ? String(r.external_ref) : null,
          name: String(r.name),
          currency: String(r.currency),
          rpm: Number(r.rpm),
          tpm: Number(r.tpm),
          concurrency: Number(r.concurrency),
          enabled: Boolean(r.enabled),
        });
      }

      const keysByVerifier = new Map<string, ApiKeyRecord>();
      for (const r of keyRows) {
        keysByVerifier.set(String(r.verifier), {
          id: String(r.id),
          project_id: String(r.project_id),
          name: String(r.name),
          verifier: String(r.verifier),
          prefix: String(r.prefix),
          role: r.role as ApiKeyRecord["role"],
          allowed_cidrs: (r.allowed_cidrs ?? []) as string[],
          models: (r.models ?? []) as string[],
          operations: (r.operations ?? []) as string[],
          enabled: Boolean(r.enabled),
          expires_at: r.expires_at ? new Date(r.expires_at).toISOString() : null,
        });
      }

      const accounts = new Map<string, UpstreamAccountRecord>();
      for (const r of accountRows) {
        accounts.set(String(r.id), {
          id: String(r.id),
          name: String(r.name),
          state: r.state as UpstreamAccountRecord["state"],
          revision: String(r.revision),
        });
      }

      const channels = new Map<string, ChannelRecord>();
      for (const r of channelRows) {
        channels.set(String(r.id), {
          id: String(r.id),
          account_id: String(r.account_id),
          name: String(r.name),
          kind: r.kind as ChannelKind,
          base_url: String(r.base_url),
          secret_ciphertext: String(r.secret_ciphertext),
          rpm: Number(r.rpm),
          concurrency: Number(r.concurrency),
          enabled: Boolean(r.enabled),
          routing_policy: (r.routing_policy ?? {}) as Record<string, unknown>,
          config_revision: Number(r.config_revision),
        });
      }

      const models = new Map<string, ModelRecord>();
      for (const r of modelRows) {
        models.set(String(r.name), {
          name: String(r.name),
          operations: (r.operations ?? []) as Operation[],
          routing_strategy: r.routing_strategy as RoutingStrategy,
          max_output_tokens: Number(r.max_output_tokens),
          enabled: Boolean(r.enabled),
        });
      }

      const prices = new Map<string, PriceVersionRecord>();
      for (const r of priceRows) {
        prices.set(String(r.id), {
          id: String(r.id),
          side: r.side as PriceSide,
          currency: String(r.currency),
          basis: r.basis as PriceBasis,
          rates: r.rates as Pricing["rates"],
          ceilings: (r.ceilings ?? {}) as Record<string, string>,
          source_digest: String(r.source_digest),
        });
      }

      const routesByModel = new Map<string, RouteRecord[]>();
      for (const r of routeRows) {
        const rec: RouteRecord = {
          id: String(r.id),
          model_name: String(r.model_name),
          channel_id: String(r.channel_id),
          upstream_model: String(r.upstream_model),
          operations: (r.operations ?? []) as Operation[],
          priority: Number(r.priority),
          weight: Number(r.weight),
          billing_price_id: String(r.billing_price_id),
          cost_version_id: String(r.cost_version_id),
          request_policy: (r.request_policy ?? {}) as Record<string, unknown>,
          plugin_pipeline: (r.plugin_pipeline ?? []) as PluginPipelineStep[],
          retry_policy: (r.retry_policy ?? {}) as RouteRetryPolicy,
          enabled: Boolean(r.enabled),
        };
        const list = routesByModel.get(rec.model_name);
        if (list) list.push(rec);
        else routesByModel.set(rec.model_name, [rec]);
      }

      const plugins = new Map<string, PluginRecord>();
      for (const r of pluginRows) {
        plugins.set(String(r.id), {
          id: String(r.id),
          name: String(r.name),
          version: String(r.version),
          kind: r.kind as PluginKind,
          digest: String(r.digest),
          wasm_bytes: r.wasm_bytes ? new Uint8Array(r.wasm_bytes) : null,
          webhook_config: (r.webhook_config ?? null) as WebhookPluginConfig | null,
          signature: r.signature ? String(r.signature) : null,
          signing_key_id: r.signing_key_id ? String(r.signing_key_id) : null,
          manifest: (r.manifest ?? {}) as Record<string, unknown>,
          enabled: Boolean(r.enabled),
        });
      }

      return {
        epoch: Number(epochRows[0]?.version ?? 0),
        projects,
        keysByVerifier,
        accounts,
        channels,
        models,
        prices,
        routesByModel,
        plugins,
      };
    },
  };
}
