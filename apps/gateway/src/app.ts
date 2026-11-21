import { Hono } from "hono";
import type { SQL } from "bun";
import {
  createCatalogRepository,
  createDatabase,
  createLedgerEngine,
  SCHEMA_VERSION,
} from "@gateway/ledger";
import {
  createFfiWasmtimeExecutor,
  PluginPipelineEngine,
  resolveWasmtimeLibraryPath,
  WebhookPluginExecutor,
  type WasmRuntimeExecutor,
} from "@gateway/plugin-engine";
import {
  createTransport,
  Lifecycle,
  LocalRateLimiter,
  SmartRouter,
  VertexTokens,
} from "@gateway/runtime";
import { createWorkerRunner } from "@gateway/worker";
import type { GatewayConfig } from "./config.ts";
import { createInternalApi } from "./internal-api.ts";
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
  const ledger = createLedgerEngine(db, {
    batchWindowMs: config.microBatchWindowMs,
    maxBatchSize: config.microBatchMaxSize,
  });
  const snapshot = new SnapshotManager(catalog, config.encryptionKey, config.snapshotPollMs);
  const router = new SmartRouter(options.routerOptions);
  const rateLimiter = new LocalRateLimiter();
  const transport = createTransport(1024);
  const vertexTokens = new VertexTokens((url, init) =>
    transport.fetch(url, init, config.allowPrivateUpstreams, config.privateUpstreamCidrs),
  );
  let wasmExecutor: WasmRuntimeExecutor | null = null;
  if (options.wasmExecutor !== undefined) {
    wasmExecutor = options.wasmExecutor;
  } else {
    const resolvedLib = resolveWasmtimeLibraryPath(config.wasmLibPath);
    if (resolvedLib) {
      wasmExecutor = createFfiWasmtimeExecutor({ libraryPath: resolvedLib });
    }
  }
  const pluginEngine = new PluginPipelineEngine({
    wasmExecutor,
    webhookExecutor: options.webhookExecutor ?? new WebhookPluginExecutor(),
    ...(config.pluginSigningPublicKeyPem
      ? { signingPublicKeyPem: config.pluginSigningPublicKeyPem }
      : {}),
  });
  const lifecycle = new Lifecycle(config.maxInFlight);
  const metrics = {
    requestsTotal: 0,
    failoversTotal: 0,
    moderationBlocksTotal: 0,
  };

  const proxyDeps: ProxyDependencies = {
    config,
    snapshot,
    ledger,
    router,
    rateLimiter,
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
      return c.json({ status: "ready", epoch: snapshot.get().epoch });
    } catch {
      return c.json({ status: "db_unavailable" }, 503);
    }
  });

  publicApp.get("/metrics", () => {
    const channels = router.snapshot();
    const openCircuits = channels.filter((ch) => ch.state === "OPEN").length;
    const lines = [
      "# TYPE gateway_requests_total counter",
      `gateway_requests_total ${metrics.requestsTotal}`,
      "# TYPE gateway_failovers_total counter",
      `gateway_failovers_total ${metrics.failoversTotal}`,
      "# TYPE gateway_moderation_blocks_total counter",
      `gateway_moderation_blocks_total ${metrics.moderationBlocksTotal}`,
      "# TYPE gateway_in_flight gauge",
      `gateway_in_flight ${lifecycle.size}`,
      "# TYPE gateway_config_epoch gauge",
      `gateway_config_epoch ${snapshot.isLoaded() ? snapshot.get().epoch : 0}`,
      "# TYPE gateway_open_circuits gauge",
      `gateway_open_circuits ${openCircuits}`,
    ];
    return new Response(`${lines.join("\n")}\n`, {
      headers: { "content-type": "text/plain; version=0.0.4; charset=utf-8" },
    });
  });

  publicApp.get("/v1/models", (c) => {
    try {
      const snap = snapshot.get();
      const { key } = authenticateRequest(c, snap, config, "chat");
      const data = [...snap.models.values()]
        .filter((m) => m.enabled && (key.models.length === 0 || key.models.includes(m.name)))
        .map((m) => ({ id: m.name, object: "model", owned_by: "gateway" }));
      return c.json({ object: "list", data });
    } catch (err) {
      const status =
        err instanceof Error && "status" in err ? Number((err as { status: number }).status) : 401;
      return c.json(
        {
          error: {
            code: "unauthorized",
            message: err instanceof Error ? err.message : "Unauthorized",
          },
        },
        status as 401,
      );
    }
  });

  const peerOf = (c: { env?: unknown }): string | undefined => {
    const env = c.env as { peerAddress?: string } | undefined;
    return env?.peerAddress;
  };

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
  publicApp.post("/v1/completions", (c) =>
    handleInferenceRequest(c, proxyDeps, "completions", undefined, peerOf(c)),
  );
  publicApp.post("/v1/images/generations", (c) =>
    handleInferenceRequest(c, proxyDeps, "images", undefined, peerOf(c)),
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

  const internalApp = createInternalApi({ config, catalog, ledger, snapshot, router });

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
    ledger,
    snapshot,
    router,
    lifecycle,
    publicApp,
    internalApp,
    worker,

    async start(): Promise<void> {
      const vRows = await db`SELECT max(version) AS version FROM public.schema_migrations`;
      const currentVersion = Number(vRows[0]?.version ?? 0);
      if (currentVersion !== SCHEMA_VERSION) {
        throw new Error(
          `Schema version mismatch: expected ${SCHEMA_VERSION}, got ${currentVersion}`,
        );
      }
      await snapshot.load();
      snapshot.start();
      if (worker) await worker.start();
    },

    async close(drainMs = 5000): Promise<void> {
      snapshot.stop();
      await lifecycle.drain(drainMs);
      if (worker) await worker.stop();
      await transport.close();
      wasmExecutor?.close();
      if (ownsDb) await db.close();
    },
  };
}
