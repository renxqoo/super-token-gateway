import { Hono } from "hono";
import type { SQL } from "bun";
import {
  assertSchemaVersion,
  createCatalogRepository,
  createDatabase,
  createLedgerEngine,
  createLedgerQueries,
} from "@gateway/ledger";
import {
  createBunWasmExecutor,
  createFfiWasmtimeExecutor,
  PluginPipelineEngine,
  resolveWasmtimeLibraryPath,
  WebhookPluginExecutor,
  type WasmRuntimeExecutor,
} from "@gateway/plugin-engine";
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
  webhookExecutor?: WebhookPluginExecutor;
  routerOptions?: ConstructorParameters<typeof SmartRouter>[0];
};

export function createGatewayRuntime(options: GatewayRuntimeOptions) {
  const { config } = options;
  const ownsDb = !options.db;
  const db = options.db ?? createDatabase(config.databaseUrl, config.dbMaxConnections);
  const catalog = createCatalogRepository(db);
  const queries = createLedgerQueries(db);
  const snapshot = new SnapshotManager(catalog, config.encryptionKey, config.snapshotPollMs);

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
    ...(config.redisUrl ? { redisUrl: config.redisUrl } : {}),
    ...(config.redisNamespace ? { namespace: config.redisNamespace } : {}),
    onCircuitEvent(event) {
      router.applyRemoteEvent({
        channel_id: event.channel_id,
        state: event.state,
        open_duration_ms: event.open_duration_ms,
        cooldown_duration_ms: event.cooldown_duration_ms,
      });
    },
    onConfigEpoch(event) {
      if (!snapshot.isLoaded() || event.epoch > snapshot.get().epoch) {
        void snapshot.load();
      }
    },
  });
  clusterRef = cluster;

  const ledger = createLedgerEngine(db, {
    batchWindowMs: config.microBatchWindowMs,
    maxBatchSize: config.microBatchMaxSize,
    maxBatchConcurrency: 4,
    nodeId: cluster.nodeId,
  });

  const transport = createTransport(Math.max(1024, Math.min(16384, config.maxInFlight)));
  const vertexTokens = new VertexTokens((url, init) =>
    transport.fetch(url, init, config.allowPrivateUpstreams, config.privateUpstreamCidrs),
  );
  let wasmExecutor: WasmRuntimeExecutor | null = null;
  if (options.wasmExecutor !== undefined) {
    wasmExecutor = options.wasmExecutor;
  } else {
    const resolvedLib = resolveWasmtimeLibraryPath(config.wasmLibPath);
    wasmExecutor = resolvedLib
      ? createFfiWasmtimeExecutor({ libraryPath: resolvedLib })
      : createBunWasmExecutor();
  }
  const pluginEngine = new PluginPipelineEngine({
    wasmExecutor,
    webhookExecutor:
      options.webhookExecutor ??
      new WebhookPluginExecutor({
        fetchImpl: (rawUrl, init) =>
          transport.fetch(
            new URL(rawUrl),
            init,
            config.allowPrivateUpstreams,
            config.privateUpstreamCidrs,
          ),
      }),
    ...(config.pluginSigningPublicKeyPem
      ? { signingPublicKeyPem: config.pluginSigningPublicKeyPem }
      : {}),
  });
  const lifecycle = new Lifecycle(config.maxInFlight);
  const metrics = new GatewayMetrics(config.accessLog ?? false);

  const proxyDeps: ProxyDependencies = {
    config,
    snapshot,
    ledger,
    router,
    cluster,
    transport,
    vertexTokens,
    pluginEngine,
    lifecycle,
    metrics,
  };

  const publicApp = new Hono();

  publicApp.get("/livez", (c) => c.json({ status: "ok" }));

  publicApp.get("/readyz", async (c) => {
    if (!lifecycle.accepting || !snapshot.isLoaded()) {
      return c.json({ status: "not_ready" }, 503);
    }
    try {
      await db`SELECT 1`;
      return c.json({
        status: "ready",
        epoch: snapshot.get().epoch,
        redis_connected: cluster.isRedisConnected(),
      });
    } catch {
      return c.json({ status: "db_unavailable" }, 503);
    }
  });

  publicApp.get("/metrics", () => {
    const channels = router.snapshot();
    const openCircuits = channels.filter((ch) => ch.state === "OPEN").length;
    const text = metrics.renderPrometheus({
      inFlight: lifecycle.size,
      configEpoch: snapshot.isLoaded() ? snapshot.get().epoch : 0,
      openCircuits,
      redisConnected: cluster.isRedisConnected(),
    });
    return new Response(text, {
      headers: { "content-type": "text/plain; version=0.0.4; charset=utf-8" },
    });
  });

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
        { error: { code: "invalid_request", message: "Invalid Gemini action path" } },
        400,
      );
    }
    const model = decodeURIComponent(action.slice(0, colon));
    const method = action.slice(colon + 1);
    const stream = method === "streamGenerateContent";
    return handleInferenceRequest(c, proxyDeps, "gemini", { model, stream }, peerOf(c));
  });

  const internalApp = createInternalApi({
    config,
    catalog,
    ledger,
    queries,
    snapshot,
    router,
    cluster,
  });

  const worker =
    config.mode === "all-in-one"
      ? createWorkerRunner({
          db,
          ...(config.businessWebhookUrl ? { businessWebhookUrl: config.businessWebhookUrl } : {}),
          ...(config.businessWebhookToken
            ? { businessWebhookToken: config.businessWebhookToken }
            : {}),
          leaseKey: 0,
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
    publicApp,
    internalApp,
    worker,

    async start(): Promise<void> {
      await assertSchemaVersion(db);
      await snapshot.load();
      await cluster.start();
      snapshot.start();
      if (worker) await worker.start();
    },

    async close(drainMs = 5000): Promise<void> {
      snapshot.stop();
      await lifecycle.drain(drainMs);
      await ledger.drain();
      if (worker) await worker.stop();
      await cluster.close();
      await transport.close();
      wasmExecutor?.close();
      if (ownsDb) await db.close();
    },
  };
}
