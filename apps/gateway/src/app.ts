import { Hono } from "hono";
import type { SQL } from "bun";
import {
  assertSchemaVersion,
  assertServiceRole,
  createCatalogRepository,
  createDatabase,
  createGatewayConfigNodeRepository,
  createLedgerEngine,
  createLedgerQueries,
  type ConfigSnapshot,
} from "@gateway/ledger";
import {
  createWasmtimeExecutor,
  PluginPipelineEngine,
  resolveWasmtimeLibraryPath,
  WebhookPluginExecutor,
  type WasmRuntimeExecutor,
} from "@gateway/plugin-engine";
import { formatProtocolError } from "@gateway/protocol";
import {
  ClusterCoordinator,
  createTransport,
  Lifecycle,
  SmartRouter,
  VertexTokens,
} from "@gateway/runtime";
import { createWorkerRunner } from "@gateway/worker";
import type { GatewayConfig } from "./config.ts";
import { createInternalApi } from "./internal-api.ts";
import { GatewayMetrics } from "./observability.ts";
import { authenticateRequest, handleInferenceRequest, type ProxyDependencies } from "./proxy.ts";
import { SnapshotManager } from "./snapshot.ts";

export type GatewayRuntimeOptions = {
  config: GatewayConfig;
  db?: SQL;
  wasmExecutor?: WasmRuntimeExecutor | null;
  contentWasmExecutor?: WasmRuntimeExecutor | null;
  providerWasmExecutor?: WasmRuntimeExecutor | null;
  webhookExecutor?: WebhookPluginExecutor;
  routerOptions?: ConstructorParameters<typeof SmartRouter>[0];
};

export function createGatewayRuntime(options: GatewayRuntimeOptions) {
  const { config } = options;
  if (config.mode === "all-in-one" && config.dbMaxConnections < 2) {
    throw new Error("All-in-one mode requires at least two database connections");
  }
  const ownsDb = !options.db;
  const db = options.db ?? createDatabase(config.databaseUrl, config.dbMaxConnections);
  const snapshotDb = ownsDb
    ? createDatabase(config.databaseUrl, config.snapshotDbMaxConnections ?? 2)
    : db;
  const controlDb = config.internalDatabaseUrl
    ? createDatabase(config.internalDatabaseUrl, config.internalDbMaxConnections ?? 4)
    : db;
  const catalog = createCatalogRepository(snapshotDb);
  const queries = createLedgerQueries(db);
  const nodeId = crypto.randomUUID();
  const metrics = new GatewayMetrics(config.accessLog ?? false);

  const transport = createTransport(Math.max(1024, Math.min(16384, config.maxInFlight)));
  const vertexTokens = new VertexTokens((url, init) =>
    transport.fetch(url, init, config.allowPrivateUpstreams, config.privateUpstreamCidrs),
  );

  const resolvedLib = resolveWasmtimeLibraryPath(config.wasmLibPath);
  let contentWasmExecutor: WasmRuntimeExecutor | null;
  let providerWasmExecutor: WasmRuntimeExecutor | null;
  if (options.contentWasmExecutor !== undefined || options.providerWasmExecutor !== undefined) {
    contentWasmExecutor =
      options.contentWasmExecutor !== undefined
        ? options.contentWasmExecutor
        : (options.wasmExecutor ?? null);
    providerWasmExecutor =
      options.providerWasmExecutor !== undefined
        ? options.providerWasmExecutor
        : (options.wasmExecutor ?? null);
  } else if (options.wasmExecutor !== undefined) {
    contentWasmExecutor = options.wasmExecutor;
    providerWasmExecutor = options.wasmExecutor;
  } else {
    contentWasmExecutor = createWasmtimeExecutor({
      poolSize: config.contentWasmPoolSize ?? 4,
      ...(resolvedLib ? { wasmtimeLibraryPath: resolvedLib } : {}),
    });
    providerWasmExecutor = createWasmtimeExecutor({
      poolSize: config.providerWasmPoolSize ?? 4,
      ...(resolvedLib ? { wasmtimeLibraryPath: resolvedLib } : {}),
    });
  }

  function warmupSnapshotPlugins(snap: ConfigSnapshot): void {
    for (const plugin of snap.plugins.values()) {
      if (plugin.kind !== "WASM" || !plugin.wasm_bytes) continue;
      if (!snapshot.isPluginIntegrityVerified(plugin.id)) continue;
      const target = plugin.capability === "PROVIDER" ? providerWasmExecutor : contentWasmExecutor;
      if (!target) continue;
      try {
        target.compile(plugin.id, plugin.digest, plugin.wasm_bytes);
      } catch {
        void 0;
      }
    }
  }

  const snapshot = new SnapshotManager(
    catalog,
    config.encryptionKey,
    config.snapshotPollMs,
    {
      id: nodeId,
      repository: createGatewayConfigNodeRepository(snapshotDb),
    },
    {
      ...(config.pluginSigningPublicKeyPem
        ? { signingPublicKeyPem: config.pluginSigningPublicKeyPem }
        : {}),
      onDivergence() {
        metrics.recordSnapshotDiverged();
      },
      onIntegrityFailure() {
        metrics.recordPluginIntegrityFailure();
      },
      onSnapshotApplied(snap) {
        warmupSnapshotPlugins(snap);
      },
    },
  );

  let clusterRef: ClusterCoordinator | null = null;
  const externalOnStateChange = options.routerOptions?.onStateChange;
  const router = new SmartRouter({
    ...options.routerOptions,
    onStateChange(event) {
      externalOnStateChange?.(event);
      clusterRef?.publishCircuitEvent({
        channel_id: event.channel_id,
        state: event.state,
        open_duration_ms: event.open_duration_ms,
        cooldown_duration_ms: event.cooldown_duration_ms,
      });
    },
  });

  const cluster = new ClusterCoordinator({
    nodeId,
    ...(config.redisUrl ? { redisUrl: config.redisUrl } : {}),
    ...(config.redisNamespace ? { namespace: config.redisNamespace } : {}),
    ...(config.redisSentinel ? { sentinel: config.redisSentinel } : {}),
    onCircuitEvent(event) {
      router.applyRemoteEvent({
        channel_id: event.channel_id,
        state: event.state,
        open_duration_ms: event.open_duration_ms,
        cooldown_duration_ms: event.cooldown_duration_ms,
      });
    },
    onConfigEpoch(event) {
      void snapshot.refreshTo(event.epoch).catch(() => {
        console.error(
          JSON.stringify({
            service: "gateway",
            event: "snapshot_refresh_failed",
            epoch: event.epoch,
          }),
        );
      });
    },
  });
  clusterRef = cluster;

  const ledger = createLedgerEngine(db, {
    batchWindowMs: config.microBatchWindowMs,
    maxBatchSize: config.microBatchMaxSize,
    maxBatchConcurrency: 4,
    nodeId: cluster.nodeId,
    onLeaseRenewalFailure() {
      metrics.leaseRenewalFailures++;
    },
  });

  const webhookExecutor =
    options.webhookExecutor ??
    new WebhookPluginExecutor({
      fetchImpl: (rawUrl, init) =>
        transport.fetch(
          new URL(rawUrl),
          init,
          config.allowPrivateUpstreams,
          config.privateUpstreamCidrs,
        ),
    });

  const pluginEngine = new PluginPipelineEngine({
    wasmExecutor: contentWasmExecutor,
    webhookExecutor,
    onObservation(obs) {
      metrics.observePlugin(obs);
    },
    ...(config.pluginSigningPublicKeyPem
      ? { signingPublicKeyPem: config.pluginSigningPublicKeyPem }
      : {}),
  });
  const lifecycle = new Lifecycle(config.maxInFlight);

  const proxyDeps: ProxyDependencies = {
    config,
    snapshot,
    ledger,
    router,
    cluster,
    transport,
    vertexTokens,
    pluginEngine,
    wasmExecutor: providerWasmExecutor,
    lifecycle,
    metrics,
  };

  const publicApp = new Hono();

  publicApp.get("/livez", (c) => c.json({ status: "ok" }));

  async function readiness(): Promise<string> {
    if (snapshot.isDiverged()) {
      return "config_diverged";
    }
    if (!lifecycle.accepting || !snapshot.isFresh()) {
      return "not_ready";
    }
    if (config.redisUrl && !cluster.isRedisConnected()) {
      return "rate_limit_unavailable";
    }
    try {
      const [database] = await db`SELECT NOT pg_is_in_recovery()
        AND current_setting('transaction_read_only') = 'off' AS writable`;
      return database?.writable ? "ready" : "db_read_only";
    } catch {
      return "db_unavailable";
    }
  }

  publicApp.get("/readyz", async (c) => {
    const status = await readiness();
    if (status !== "ready") return c.json({ status }, 503);
    return c.json({
      status,
      epoch: snapshot.get().epoch,
      redis_connected: cluster.isRedisConnected(),
      wasm_runtime: contentWasmExecutor?.runtimeKind ?? providerWasmExecutor?.runtimeKind ?? "none",
    });
  });

  async function metricsResponse() {
    const channels = router.snapshot();
    const openCircuits = channels.filter((ch) => ch.state === "OPEN").length;
    const text = metrics.renderPrometheus({
      inFlight: lifecycle.size,
      configEpoch: snapshot.isLoaded() ? snapshot.get().epoch : 0,
      openCircuits,
      redisConnected: cluster.isRedisConnected(),
      redisRequired: Boolean(config.redisUrl),
      ready: (await readiness()) === "ready",
      snapshotVerifiedAt: snapshot.verifiedAtSeconds(),
      snapshotDiverged: snapshot.isDiverged(),
      wasmRuntime: contentWasmExecutor?.runtimeKind ?? providerWasmExecutor?.runtimeKind ?? "none",
      contentWasmStats: contentWasmExecutor?.stats?.() ?? null,
      providerWasmStats:
        providerWasmExecutor && providerWasmExecutor !== contentWasmExecutor
          ? (providerWasmExecutor.stats?.() ?? null)
          : null,
      webhookStats: webhookExecutor.stats?.() ?? null,
    });
    return new Response(text, {
      headers: { "content-type": "text/plain; version=0.0.4; charset=utf-8" },
    });
  }
  publicApp.get("/metrics", (c) => (config.privateMetrics ? c.notFound() : metricsResponse()));

  const peerOf = (c: { env?: unknown }): string | undefined => {
    const env = c.env as { peerAddress?: string } | undefined;
    return env?.peerAddress;
  };

  publicApp.get("/v1/models", (c) => {
    try {
      const snap = snapshot.get();
      const { key } = authenticateRequest(c, snap, config, undefined, peerOf(c));
      const data = [...snap.models.values()]
        .filter((m) => m.enabled && (key.models.length === 0 || key.models.includes(m.name)))
        .map((m) => ({ id: m.name, object: "model", owned_by: "gateway" }));
      return c.json({ object: "list", data });
    } catch (err) {
      const status =
        err instanceof Error && "status" in err ? Number((err as { status: number }).status) : 401;
      const code =
        err instanceof Error && "code" in err
          ? String((err as { code: string }).code)
          : "unauthorized";
      return c.json(
        {
          error: {
            code,
            message: err instanceof Error ? err.message : "Unauthorized",
          },
        },
        status as 401,
      );
    }
  });

  publicApp.post("/v1/chat/completions", (c) =>
    handleInferenceRequest(c, proxyDeps, "chat", undefined, peerOf(c)),
  );
  publicApp.post("/v1/responses", (c) =>
    handleInferenceRequest(c, proxyDeps, "responses", undefined, peerOf(c)),
  );
  publicApp.post("/v1/messages", (c) =>
    handleInferenceRequest(c, proxyDeps, "messages", undefined, peerOf(c)),
  );
  publicApp.post("/v1/embeddings", (c) =>
    handleInferenceRequest(c, proxyDeps, "embeddings", undefined, peerOf(c)),
  );
  publicApp.post("/v1/moderations", (c) =>
    handleInferenceRequest(c, proxyDeps, "moderations", undefined, peerOf(c)),
  );
  publicApp.post("/v1/completions", (c) =>
    handleInferenceRequest(c, proxyDeps, "completions", undefined, peerOf(c)),
  );
  publicApp.post("/v1/images/generations", (c) =>
    handleInferenceRequest(c, proxyDeps, "images", undefined, peerOf(c)),
  );
  publicApp.post("/v1/images/edits", (c) =>
    handleInferenceRequest(c, proxyDeps, "image_edits", undefined, peerOf(c)),
  );
  publicApp.post("/v1/audio/speech", (c) =>
    handleInferenceRequest(c, proxyDeps, "speech", undefined, peerOf(c)),
  );
  publicApp.post("/v1/audio/transcriptions", (c) =>
    handleInferenceRequest(c, proxyDeps, "transcription", undefined, peerOf(c)),
  );
  publicApp.post("/v1/audio/translations", (c) =>
    handleInferenceRequest(c, proxyDeps, "translation", undefined, peerOf(c)),
  );
  publicApp.post("/v1beta/models/:action", async (c) => {
    const action = c.req.param("action");
    const colon = action.lastIndexOf(":");
    if (colon <= 0) {
      return c.json(
        formatProtocolError(
          "gemini",
          400,
          "invalid_request",
          "Invalid Gemini action path",
          crypto.randomUUID(),
        ),
        400,
      );
    }
    const model = decodeURIComponent(action.slice(0, colon));
    const method = action.slice(colon + 1);
    const stream = method === "streamGenerateContent";
    return handleInferenceRequest(c, proxyDeps, "gemini", { model, stream }, peerOf(c));
  });

  const controlLedger =
    controlDb === db
      ? ledger
      : createLedgerEngine(controlDb, {
          enableLeases: false,
          batchWindowMs: config.microBatchWindowMs,
          maxBatchSize: config.microBatchMaxSize,
        });
  const {
    app: internalApp,
    requestBodies: internalRequestBodies,
    routeRoles: internalRouteRoles,
  } = createInternalApi({
    db: controlDb,
    onAuditFailure() {
      metrics.adminAuditFailures++;
    },
    config,
    catalog: controlDb === snapshotDb ? catalog : createCatalogRepository(controlDb),
    ledger: controlLedger,
    queries: controlDb === db ? queries : createLedgerQueries(controlDb),
    snapshot,
    router,
    cluster,
    pluginEngine,
    wasmExecutor: providerWasmExecutor,
  });

  const worker =
    config.mode === "all-in-one"
      ? createWorkerRunner({
          db,
          ...(config.businessWebhookUrl ? { businessWebhookUrl: config.businessWebhookUrl } : {}),
          ...(config.businessWebhookToken
            ? { businessWebhookToken: config.businessWebhookToken }
            : {}),
        })
      : null;

  return {
    db,
    catalog,
    queries,
    ledger,
    snapshot,
    router,
    cluster,
    lifecycle,
    metrics,
    metricsResponse,
    publicApp,
    internalApp,
    internalRequestBodies,
    internalRouteRoles,
    worker,

    async start(): Promise<void> {
      const checks: Promise<void>[] = [];
      if (contentWasmExecutor?.selfCheck) checks.push(contentWasmExecutor.selfCheck());
      if (
        providerWasmExecutor &&
        providerWasmExecutor !== contentWasmExecutor &&
        providerWasmExecutor.selfCheck
      ) {
        checks.push(providerWasmExecutor.selfCheck());
      }
      if (checks.length > 0) await Promise.all(checks);
      await assertSchemaVersion(db);
      if (controlDb !== db) await assertSchemaVersion(controlDb);
      if (config.requireDbRoles) {
        await Promise.all([
          assertServiceRole(db, "stg_data"),
          assertServiceRole(controlDb, "stg_control"),
        ]);
      }
      await snapshot.load();
      await cluster.start();
      await snapshot.start();
      if (worker) await worker.start();
    },

    async close(drainMs = 5000): Promise<void> {
      const draining = lifecycle.drain(drainMs);
      await draining;
      await snapshot.stop();
      await ledger.drain();
      if (controlDb !== db) {
        await controlLedger.drain();
        await controlDb.close();
      }
      if (worker) await worker.stop();
      await cluster.close();
      await transport.close();
      contentWasmExecutor?.close();
      if (providerWasmExecutor && providerWasmExecutor !== contentWasmExecutor) {
        providerWasmExecutor.close();
      }
      if (snapshotDb !== db) await snapshotDb.close();
      if (ownsDb) await db.close();
    },
  };
}
