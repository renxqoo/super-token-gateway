import {
  upsertPluginSchema,
  pluginReleaseSchema,
  rollbackPluginReleaseSchema,
  installBuiltinPluginSchema,
  previewProviderSchema,
  testPluginSchema,
} from "./internal-plugin-schemas.ts";
import { parseBody, parseUuid, type AdminEnv } from "./internal-request.ts";
import {
  declarativeProviderConfigSchema,
  wasmProviderConfigSchema,
} from "./internal-provider-schemas.ts";
import {
  type PluginCapability,
  type ProviderPluginConfig,
  type DeclarativeProviderConfig,
  type WasmProviderConfig,
  type PluginKind,
  type WebhookPluginConfig,
  type PluginReleaseTarget,
  type AdapterOverrides,
  createPluginReleaseRepository,
} from "@gateway/ledger";
import { GatewayError } from "@gateway/contracts";
import { validateBaseUrl } from "@gateway/runtime";
import { z } from "zod";
import { canonical, type Operation } from "@gateway/kernel";
import {
  type WasmRuntimeExecutor,
  resolveWasmtimeLibraryPath,
  createWasmtimeExecutor,
  listBuiltinPlugins,
  getBuiltinPlugin,
  type PluginHook,
} from "@gateway/plugin-engine";
import { validatePluginRelease } from "./plugin-release-validation.ts";
import { previewProviderRequest } from "./provider-request-preview.ts";
import { Hono, type MiddlewareHandler } from "hono";
import type {
  InternalApiDeps,
  InternalJsonRoute,
  InternalRoute,
  InternalAuthorize,
} from "./internal-route-types.ts";

export function registerInternalPluginRoutes({
  jsonRoute,
  deps,
  onConfigUpdated,
  route,
  pluginReleases,
  requestBodies,
  app,
  authorize,
  checkBodySize,
}: {
  jsonRoute: InternalJsonRoute;
  deps: InternalApiDeps;
  onConfigUpdated: (epoch: number) => Promise<void>;
  route: InternalRoute;
  pluginReleases: ReturnType<typeof createPluginReleaseRepository>;
  requestBodies: Map<string, { schema: z.ZodType; required: boolean }>;
  app: Hono<AdminEnv>;
  authorize: InternalAuthorize;
  checkBodySize: MiddlewareHandler;
}) {
  jsonRoute("PUT", "/internal/v1/plugins", "security", upsertPluginSchema, async (c, body) => {
    const capability = body.capability as PluginCapability;
    let validatedProviderConfig: ProviderPluginConfig | null = null;

    if (capability === "CONTENT") {
      if (body.kind === "DECLARATIVE") {
        throw new GatewayError(
          400,
          "validation_error",
          "CONTENT plugins must use kind WASM or WEBHOOK",
        );
      }
      if (body.provider_config) {
        throw new GatewayError(
          400,
          "validation_error",
          "CONTENT plugins cannot define provider_config",
        );
      }
    } else {
      if (body.kind === "WEBHOOK") {
        throw new GatewayError(
          400,
          "validation_error",
          "PROVIDER plugins must use kind DECLARATIVE or WASM",
        );
      }
      if (!body.provider_config) {
        throw new GatewayError(
          400,
          "validation_error",
          "provider_config is required for PROVIDER plugins",
        );
      }
      validatedProviderConfig =
        body.kind === "DECLARATIVE"
          ? (parseBody(
              declarativeProviderConfigSchema,
              body.provider_config,
            ) as DeclarativeProviderConfig)
          : (parseBody(wasmProviderConfigSchema, body.provider_config) as WasmProviderConfig);
    }

    if (body.kind === "WEBHOOK") {
      if (!body.webhook_config) {
        throw new GatewayError(400, "validation_error", "webhook_config is required for WEBHOOK");
      }
      validateBaseUrl(
        body.webhook_config.url,
        deps.config.allowPrivateUpstreams,
        deps.config.privateUpstreamCidrs,
      );
    }

    const wasmBytes = body.wasm_bytes_base64
      ? new Uint8Array(Buffer.from(body.wasm_bytes_base64, "base64"))
      : null;
    if (body.kind === "WASM") {
      if (!wasmBytes || wasmBytes.byteLength === 0) {
        throw new GatewayError(400, "validation_error", "wasm_bytes_base64 is required for WASM");
      }
      let mod: WebAssembly.Module;
      try {
        mod = new WebAssembly.Module(wasmBytes);
      } catch (err) {
        throw new GatewayError(
          400,
          "validation_error",
          `Invalid WebAssembly binary: ${err instanceof Error ? err.message : "compile failed"}`,
        );
      }
      if (WebAssembly.Module.imports(mod).length > 0) {
        throw new GatewayError(
          400,
          "validation_error",
          "WASM plugin must not declare host imports",
        );
      }
      if (
        capability === "PROVIDER" &&
        validatedProviderConfig &&
        "hooks" in validatedProviderConfig
      ) {
        const exportMap = new Map(WebAssembly.Module.exports(mod).map((e) => [e.name, e.kind]));
        if (exportMap.get("memory") !== "memory" || exportMap.get("stg_alloc") !== "function") {
          throw new GatewayError(
            400,
            "validation_error",
            "PROVIDER WASM plugin must export memory and stg_alloc",
          );
        }
        for (const hook of validatedProviderConfig.hooks) {
          if (exportMap.get(hook) !== "function") {
            throw new GatewayError(
              400,
              "validation_error",
              `PROVIDER WASM plugin missing exported hook function: ${hook}`,
            );
          }
        }
      }
    }

    const rec = await deps.catalog.upsertPlugin({
      ...(body.id ? { id: body.id } : {}),
      name: body.name,
      version: body.version,
      capability,
      kind: body.kind as PluginKind,
      wasm_bytes: body.kind === "WASM" ? wasmBytes : null,
      webhook_config:
        body.kind === "WEBHOOK"
          ? ((body.webhook_config ?? null) as WebhookPluginConfig | null)
          : null,
      provider_config: validatedProviderConfig,
      signature: body.signature ?? null,
      signing_key_id: body.signing_key_id ?? null,
      manifest: body.manifest,
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json({ ...rec, wasm_bytes: undefined });
  });

  route("GET", "/internal/v1/plugins", "security", async (c) => {
    const rawCap = c.req.query("capability");
    const cap = rawCap
      ? (parseBody(z.enum(["CONTENT", "PROVIDER"]), rawCap) as PluginCapability)
      : undefined;
    const items = await deps.catalog.listPlugins(cap);
    return c.json({ items });
  });

  function releaseDigest(value: unknown): string {
    return new Bun.CryptoHasher("sha256").update(canonical(value)).digest("hex");
  }

  async function replayRelease(id: string, actor: string, digest: string) {
    const existing = await pluginReleases.get(id);
    if (!existing) return null;
    if (existing.actor !== actor || existing.request_digest !== digest) {
      throw new GatewayError(
        409,
        "plugin_release_conflict",
        "Release identifier already belongs to another request",
      );
    }
    return existing;
  }

  async function reportRelease(
    release: NonNullable<Awaited<ReturnType<typeof pluginReleases.get>>>,
  ) {
    const { request_digest: _, ...record } = release;
    return {
      ...record,
      local_applied_epoch: deps.snapshot.get().epoch,
      nodes: await pluginReleases.confirmations(release),
    };
  }

  function releaseTargets(
    targets: z.output<typeof pluginReleaseSchema.shape.targets>,
  ): PluginReleaseTarget[] {
    return targets.map((target) =>
      "plugin_id" in target ? { ...target, available: true } : (target as PluginReleaseTarget),
    );
  }

  function requireReleaseEpoch(expected: number) {
    const snapshot = deps.snapshot.get();
    if (snapshot.epoch !== expected || !deps.snapshot.isFresh()) {
      throw new GatewayError(
        409,
        "plugin_release_conflict",
        "Validate against the current configuration epoch",
      );
    }
    return snapshot;
  }

  let activePreflights = 0;
  async function preflight(
    snapshot: ReturnType<typeof requireReleaseEpoch>,
    targets: PluginReleaseTarget[],
    samples: z.output<typeof pluginReleaseSchema.shape.samples>,
  ) {
    if (activePreflights >= 2) {
      throw new GatewayError(503, "plugin_preflight_busy", "Plugin validation capacity is full");
    }
    activePreflights++;
    let executor: WasmRuntimeExecutor | null = null;
    try {
      const library = resolveWasmtimeLibraryPath(deps.config.wasmLibPath);
      executor = createWasmtimeExecutor({
        poolSize: 1,
        coldCompileTimeoutMs: 5000,
        ...(library ? { wasmtimeLibraryPath: library } : {}),
      });
      return await validatePluginRelease({
        snapshot,
        targets,
        getPlugin: deps.catalog.getPlugin,
        decrypt: (channel) => deps.snapshot.decryptChannelSecret(channel),
        executor,
        signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem,
        samples,
      });
    } catch (error) {
      if (error instanceof GatewayError) throw error;
      throw new GatewayError(422, "plugin_preflight_failed", "Plugin execution preflight failed");
    } finally {
      executor?.close();
      activePreflights--;
    }
  }

  jsonRoute(
    "POST",
    "/internal/v1/plugin-releases/validate",
    "security",
    pluginReleaseSchema,
    async (c, body) => {
      const snapshot = requireReleaseEpoch(body.expected_epoch);
      const changes = await preflight(snapshot, releaseTargets(body.targets), body.samples);
      return c.json({ config_epoch: snapshot.epoch, changes });
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/plugin-releases",
    "security",
    pluginReleaseSchema,
    async (c, body) => {
      const actor = c.get("principal").id;
      const digest = releaseDigest(body);
      const existing = await replayRelease(body.id, actor, digest);
      if (existing) return c.json(await reportRelease(existing));
      let changes;
      try {
        const snapshot = requireReleaseEpoch(body.expected_epoch);
        changes = await preflight(snapshot, releaseTargets(body.targets), body.samples);
      } catch (error) {
        const replay = await replayRelease(body.id, actor, digest);
        if (replay) return c.json(await reportRelease(replay));
        throw error;
      }
      const result = await pluginReleases.publish({
        id: body.id,
        actor,
        request_digest: digest,
        expected_epoch: body.expected_epoch,
        changes,
        rollback_of: null,
      });
      await onConfigUpdated(result.config_epoch);
      return c.json(await reportRelease(result));
    },
  );

  route("GET", "/internal/v1/plugin-releases/:releaseId", "security", async (c) => {
    const release = await pluginReleases.get(parseUuid(c.req.param("releaseId"), "releaseId"));
    if (!release) throw new GatewayError(404, "not_found", "Plugin release not found");
    return c.json(await reportRelease(release));
  });

  jsonRoute(
    "POST",
    "/internal/v1/plugin-releases/:releaseId/rollback",
    "security",
    rollbackPluginReleaseSchema,
    async (c, body) => {
      const actor = c.get("principal").id;
      const releaseId = parseUuid(c.req.param("releaseId"), "releaseId");
      const digest = releaseDigest({ ...body, rollback_of: releaseId });
      const existing = await replayRelease(body.id, actor, digest);
      if (existing) return c.json(await reportRelease(existing));
      const snapshot = requireReleaseEpoch(body.expected_epoch);
      const release = await pluginReleases.get(releaseId);
      if (!release) throw new GatewayError(404, "not_found", "Plugin release not found");
      await preflight(
        snapshot,
        release.changes.map(({ before }) => before),
        body.samples,
      );
      const result = await pluginReleases.publish({
        id: body.id,
        actor,
        request_digest: digest,
        expected_epoch: body.expected_epoch,
        changes: release.changes.map(({ before, after }) => ({ before: after, after: before })),
        rollback_of: releaseId,
      });
      await onConfigUpdated(result.config_epoch);
      return c.json(await reportRelease(result));
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/plugins/:pluginId/revoke",
    "security",
    z.object({ reason: z.string().trim().min(1).max(1024) }).strict(),
    async (c, body) => {
      const pluginId = parseUuid(c.req.param("pluginId"), "pluginId");
      const revoked = await pluginReleases.revoke(pluginId, c.get("principal").id, body.reason);
      if (!revoked) throw new GatewayError(404, "not_found", "Plugin revision not found");
      await onConfigUpdated(revoked.config_epoch);
      return c.json(revoked);
    },
  );

  route("GET", "/internal/v1/plugins/builtins", "security", (c) => {
    const rawCap = c.req.query("capability");
    const cap = rawCap
      ? (parseBody(z.enum(["CONTENT", "PROVIDER"]), rawCap) as PluginCapability)
      : undefined;
    return c.json({ items: listBuiltinPlugins(cap) });
  });

  requestBodies.set("POST /internal/v1/plugins/builtins/:name/install", {
    schema: installBuiltinPluginSchema,
    required: false,
  });
  app.on(
    "POST",
    "/internal/v1/plugins/builtins/:name/install",
    authorize("POST", "/internal/v1/plugins/builtins/:name/install", "security"),
    checkBodySize,
    async (c) => {
      const builtin = getBuiltinPlugin(c.req.param("name"));
      if (!builtin) {
        return c.json({ error: { code: "not_found", message: "Built-in plugin not found" } }, 404);
      }
      const rawBody = await c.req.text();
      const parsedBody = rawBody.trim()
        ? parseBody(installBuiltinPluginSchema, JSON.parse(rawBody))
        : parseBody(installBuiltinPluginSchema, {});
      const manifest = parsedBody.manifest
        ? { ...builtin.manifest, ...parsedBody.manifest }
        : { ...builtin.manifest };
      let providerConfig: ProviderPluginConfig | null = null;
      if (builtin.capability === "PROVIDER") {
        const mergedCfg = parsedBody.provider_config
          ? { ...builtin.provider_config, ...parsedBody.provider_config }
          : builtin.provider_config;
        providerConfig = parseBody(
          declarativeProviderConfigSchema,
          mergedCfg,
        ) as DeclarativeProviderConfig;
      }
      const rec = await deps.catalog.upsertPlugin({
        name: builtin.name,
        version: builtin.version,
        capability: builtin.capability,
        kind: builtin.kind,
        wasm_bytes: builtin.capability === "CONTENT" ? builtin.wasm_bytes : null,
        webhook_config: null,
        provider_config: providerConfig,
        signature: parsedBody.signature ?? null,
        signing_key_id: parsedBody.signing_key_id ?? null,
        manifest,
      });
      await onConfigUpdated(rec.config_epoch);
      return c.json({ ...rec, wasm_bytes: undefined });
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/providers/preview",
    "catalog",
    previewProviderSchema,
    async (c, body) => {
      const snap = deps.snapshot.isLoaded()
        ? deps.snapshot.get()
        : { plugins: new Map(), providerPluginsByName: new Map() };
      const preview = await previewProviderRequest({
        provider: body.provider,
        ...(body.provider_plugin_id !== undefined
          ? { providerPluginId: body.provider_plugin_id }
          : {}),
        baseUrl: body.base_url,
        operation: body.operation as Operation,
        upstreamModel: body.upstream_model,
        stream: body.stream,
        clientBody: body.client_body,
        ...(body.channel_adapter_config
          ? { channelAdapterConfig: body.channel_adapter_config as AdapterOverrides }
          : {}),
        ...(body.adapter_overrides
          ? { routeAdapterOverrides: body.adapter_overrides as AdapterOverrides }
          : {}),
        ...(body.mock_upstream_response
          ? { mockUpstreamResponse: body.mock_upstream_response }
          : {}),
        snapshot: snap,
        wasmExecutor: deps.wasmExecutor,
        ...(deps.config.pluginSigningPublicKeyPem
          ? { signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem }
          : {}),
      });
      return c.json(preview);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/plugins/:pluginId/test",
    "security",
    testPluginSchema,
    async (c, body) => {
      const pluginId = parseUuid(c.req.param("pluginId"), "pluginId");
      const snap = deps.snapshot.get();
      const plugin = snap.plugins.get(pluginId);
      if (!plugin) {
        return c.json(
          { error: { code: "not_found", message: "Enabled plugin not found in active snapshot" } },
          404,
        );
      }
      const res = await deps.pluginEngine.runUnary(
        body.hook as PluginHook,
        [{ plugin_id: plugin.id, priority: 1 }],
        snap.plugins,
        {
          request_id: crypto.randomUUID(),
          billing_account_id: "dry-run",
          model: body.model,
          operation: body.operation,
        },
        body.text,
        c.req.raw.signal,
      );
      return c.json(res);
    },
  );
}
