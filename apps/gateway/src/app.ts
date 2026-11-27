import { Hono } from "hono";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import type { SQL } from "bun";
import type { Operation } from "@gateway/kernel";
import {
  assertSchemaVersion,
  assertServiceRole,
  createAsyncTasksRepository,
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
  isValidHttpHeaderName,
  isValidHttpHeaderValue,
  Lifecycle,
  SmartRouter,
  VertexTokens,
} from "@gateway/runtime";
import { createWorkerRunner } from "@gateway/worker";
import type { GatewayConfig } from "./config.ts";
import { createConsoleAdmissionCheck } from "./console-admission.ts";
import {
  handleCancelAsyncTask,
  handleGetAsyncTask,
  type AsyncTaskHandlerDeps,
} from "./async-tasks.ts";
import { createInternalApi } from "./internal-api.ts";
import { GatewayMetrics } from "./observability.ts";
import { createTraceQueue } from "./trace-queue.ts";
import { authenticateRequest } from "./inference/auth.ts";
import type { InferenceContext, ProxyDependencies } from "./inference/types.ts";
import { handleInferenceRequest } from "./proxy.ts";
import { ProxyRuntimeState } from "./proxy-state.ts";
import { createBodyBufferBudget, createPublicIngress } from "./public-ingress.ts";
import { createPressureMonitor } from "./resource-pressure.ts";
import { SnapshotManager } from "./snapshot.ts";

class FastInferenceContext implements InferenceContext {
  readonly req = this;
  constructor(readonly raw: Request) {}
  query(name: string) {
    return new URL(this.raw.url).searchParams.get(name) ?? undefined;
  }
  json(data: unknown, status?: ContentfulStatusCode, headers?: Record<string, string>) {
    return new Response(JSON.stringify(data), {
      status: status ?? 200,
      headers: {
        "content-type": "application/json; charset=UTF-8",
        ...headers,
      },
    });
  }
}

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
  const checkConsoleAdmission = createConsoleAdmissionCheck(
    (snapshotDb === db ? catalog : createCatalogRepository(db)).getConsoleAdmission,
  );
  const queries = createLedgerQueries(db);
  const nodeId = crypto.randomUUID();
  const metrics = new GatewayMetrics(config.accessLog ?? false);
  const traceQueue = config.traceExport
    ? createTraceQueue(config.traceExport, config.keyPepper)
    : undefined;

  const transport = createTransport(
    Math.max(1024, Math.min(65535, config.maxInFlight)),
    config.upstreamDial ? { dial: config.upstreamDial } : undefined,
  );
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
      clusterRef?.publishCircuitEvent(event);
    },
  });

  const cluster = new ClusterCoordinator({
    nodeId,
    ...(config.redisUrl ? { redisUrl: config.redisUrl } : {}),
    ...(config.redisNamespace ? { namespace: config.redisNamespace } : {}),
    ...(config.redisSentinel ? { sentinel: config.redisSentinel } : {}),
    onCircuitEvent(event) {
      router.applyRemoteEvent(event);
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
    ...(config.walletLeaseIdleReleaseMs === undefined
      ? {}
      : { walletLeaseIdleReleaseMs: config.walletLeaseIdleReleaseMs }),
    nodeId: cluster.nodeId,
    onLeaseRenewalFailure() {
      metrics.leaseRenewalFailures++;
    },
  });

  const webhookUrls = new Map<string, URL>();
  const webhookHeaderCache = new WeakMap<
    object,
    { entries: readonly [string, string][]; valid: boolean }
  >();
  const getWebhookUrl = (rawUrl: string): URL => {
    let parsed = webhookUrls.get(rawUrl);
    if (!parsed) {
      if (webhookUrls.size >= 256) webhookUrls.clear();
      parsed = new URL(rawUrl);
      webhookUrls.set(rawUrl, parsed);
    }
    return parsed;
  };
  const prepareWebhookHeaders = (
    rawHeaders: RequestInit["headers"],
  ): { headers: RequestInit["headers"]; headersValidated: boolean } => {
    if (
      !rawHeaders ||
      rawHeaders instanceof Headers ||
      Array.isArray(rawHeaders) ||
      typeof rawHeaders !== "object"
    ) {
      return { headers: rawHeaders, headersValidated: false };
    }
    let cached = webhookHeaderCache.get(rawHeaders);
    if (!cached) {
      const entries: [string, string][] = [];
      let valid = true;
      for (const k in rawHeaders) {
        const v = rawHeaders[k];
        if (v === undefined) continue;
        if (!isValidHttpHeaderName(k) || !isValidHttpHeaderValue(v)) {
          valid = false;
          break;
        }
        entries.push([k.toLowerCase(), v]);
      }
      cached = { entries, valid };
      webhookHeaderCache.set(rawHeaders, cached);
    }
    if (!cached.valid) return { headers: rawHeaders, headersValidated: false };
    const headers: Record<string, string> = {};
    for (let i = 0; i < cached.entries.length; i++) {
      const pair = cached.entries[i]!;
      headers[pair[0]] = pair[1];
    }
    return { headers, headersValidated: true };
  };
  const webhookExecutor =
    options.webhookExecutor ??
    new WebhookPluginExecutor({
      fetchImpl: (rawUrl, init) => {
        const parsedUrl = getWebhookUrl(rawUrl);
        const { headers, headersValidated } = prepareWebhookHeaders(init.headers);
        const fastInit = {
          ...init,
          ...(headers !== undefined ? { headers } : {}),
          headersValidated,
        };
        if (!init.signal?.aborted) {
          const prepared = transport.tryPrepareDispatchSync(
            parsedUrl,
            config.allowPrivateUpstreams,
            config.privateUpstreamCidrs,
          );
          if (prepared) return prepared.fetchFast(fastInit);
        }
        return transport.fetchFast(
          parsedUrl,
          fastInit,
          config.allowPrivateUpstreams,
          config.privateUpstreamCidrs,
        );
      },
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
  const proxyState = new ProxyRuntimeState();
  const pressure = config.ingressPressure
    ? createPressureMonitor(config.ingressPressure)
    : undefined;
  const ingress = createPublicIngress(config, cluster, pressure);
  const bodyBufferBudget =
    config.ingressBodyBufferBytes === undefined
      ? undefined
      : createBodyBufferBudget(config.ingressBodyBufferBytes);
  const asyncTasks = createAsyncTasksRepository(db);
  const controlLedger =
    controlDb === db
      ? ledger
      : createLedgerEngine(controlDb, {
          enableLeases: false,
          batchWindowMs: config.microBatchWindowMs,
          maxBatchSize: config.microBatchMaxSize,
        });

  const proxyDeps: ProxyDependencies = {
    config,
    checkConsoleAdmission,
    snapshot,
    ledger,
    asyncTasks,
    router,
    cluster,
    transport,
    vertexTokens,
    pluginEngine,
    wasmExecutor: providerWasmExecutor,
    lifecycle,
    metrics,
    state: proxyState,
    ...(traceQueue ? { traceQueue } : {}),
    ...(bodyBufferBudget ? { bodyBufferBudget } : {}),
  };

  const asyncTaskDeps: AsyncTaskHandlerDeps = {
    config,
    checkConsoleAdmission,
    snapshot,
    asyncTasks,
    controlLedger,
    queries,
    cluster,
    transport,
    vertexTokens,
    state: proxyState,
  };

  const publicApp = new Hono();

  publicApp.get("/livez", (c) => c.json({ status: "ok" }));

  let databaseProbe: Promise<string> | undefined;
  let databaseReadiness: { status: string; expiresAt: number } | undefined;

  function localReadinessFailure(): string | undefined {
    if (snapshot.isDiverged()) {
      return "config_diverged";
    }
    if (!lifecycle.accepting || !snapshot.isFresh()) {
      return "not_ready";
    }
    if (config.redisUrl && !cluster.isRedisConnected()) {
      return "rate_limit_unavailable";
    }
  }

  async function probeDatabase(): Promise<string> {
    try {
      const [database] = await db`SELECT NOT pg_is_in_recovery()
        AND current_setting('transaction_read_only') = 'off' AS writable`;
      return database?.writable ? "ready" : "db_read_only";
    } catch {
      return "db_unavailable";
    }
  }

  async function readiness(): Promise<string> {
    const localFailure = localReadinessFailure();
    if (localFailure) return localFailure;
    if (databaseReadiness && databaseReadiness.expiresAt > performance.now()) {
      return databaseReadiness.status;
    }
    databaseProbe ??= probeDatabase()
      .then((status) => {
        databaseReadiness = { status, expiresAt: performance.now() + 250 };
        return status;
      })
      .finally(() => {
        databaseProbe = undefined;
      });
    const status = await databaseProbe;
    return localReadinessFailure() ?? status;
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
    const ingressMetrics = [
      ...ingress.metrics(),
      ...(pressure?.metrics() ?? []),
      ...(bodyBufferBudget?.metrics() ?? []),
      ...(traceQueue?.metrics() ?? []),
    ];
    return new Response(`${text.trimEnd()}\n${ingressMetrics.join("\n")}\n`, {
      headers: { "content-type": "text/plain; version=0.0.4; charset=utf-8" },
    });
  }
  publicApp.get("/metrics", (c) => (config.privateMetrics ? c.notFound() : metricsResponse()));

  const peerOf = (c: { env?: unknown }): string | (() => string | undefined) | undefined => {
    const env = c.env as
      { peerAddress?: string; getPeerAddress?: () => string | undefined } | undefined;
    return env?.peerAddress ?? env?.getPeerAddress;
  };

  publicApp.get("/v1/models", async (c) => {
    try {
      const snap = snapshot.get();
      const { key, billingAccount } = authenticateRequest(
        c,
        snap,
        config,
        undefined,
        peerOf(c),
        proxyState,
      );
      const check = checkConsoleAdmission(billingAccount, key);
      if (check) await check;
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
  publicApp.post("/v1/music/generations", (c) =>
    handleInferenceRequest(c, proxyDeps, "music", undefined, peerOf(c)),
  );
  publicApp.post("/v1/videos", (c) =>
    handleInferenceRequest(c, proxyDeps, "videos", undefined, peerOf(c)),
  );
  publicApp.get("/v1/videos/:taskId", (c) =>
    handleGetAsyncTask(c, asyncTaskDeps, c.req.param("taskId"), peerOf(c)),
  );
  publicApp.post("/v1/videos/:taskId/cancel", (c) =>
    handleCancelAsyncTask(c, asyncTaskDeps, c.req.param("taskId"), peerOf(c)),
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
    asyncTasks: controlDb === db ? asyncTasks : createAsyncTasksRepository(controlDb),
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

  const staticInferenceRoutes = new Map<string, Operation>([
    ["/v1/chat/completions", "chat"],
    ["/v1/responses", "responses"],
    ["/v1/messages", "messages"],
    ["/v1/embeddings", "embeddings"],
    ["/v1/moderations", "moderations"],
    ["/v1/completions", "completions"],
    ["/v1/images/generations", "images"],
    ["/v1/images/edits", "image_edits"],
    ["/v1/audio/speech", "speech"],
    ["/v1/audio/transcriptions", "transcription"],
    ["/v1/audio/translations", "translation"],
    ["/v1/music/generations", "music"],
    ["/v1/videos", "videos"],
  ]);

  function fetchPublic(
    req: Request,
    env?: string | { peerAddress?: string; getPeerAddress?: () => string | undefined },
    nativeHttp = false,
  ): Response | Promise<Response> {
    const url = req.url;
    const scheme = url.indexOf("://");
    const pathStart = scheme === -1 ? 0 : url.indexOf("/", scheme + 3);
    if (pathStart === -1)
      return publicApp.fetch(req, typeof env === "string" ? { peerAddress: env } : env);
    const queryStart = url.indexOf("?", pathStart);
    const pathname = queryStart === -1 ? url.slice(pathStart) : url.slice(pathStart, queryStart);
    if (pathname === "/livez" || pathname === "/readyz" || pathname === "/metrics") {
      return publicApp.fetch(req, typeof env === "string" ? { peerAddress: env } : env);
    }
    const operation = req.method === "POST" ? staticInferenceRoutes.get(pathname) : undefined;
    const peer = typeof env === "string" ? env : (env?.peerAddress ?? env?.getPeerAddress);
    const requestPeer = config.ingressIp && typeof peer === "function" ? peer() : peer;
    if (operation) {
      const prep = ingress.tryPrepareDeferred(req, operation, requestPeer);
      if (prep.kind === "deny") return prep.response;
      if (prep.kind === "allow") {
        return handleInferenceRequest(
          new FastInferenceContext(req),
          proxyDeps,
          operation,
          undefined,
          requestPeer,
          nativeHttp,
          { declaredSize: prep.declaredSize },
        );
      }
      if (prep.kind === "defer" && !snapshot.isDiverged() && snapshot.isFresh()) {
        const ctx = new FastInferenceContext(req);
        try {
          const snap = snapshot.get();
          const auth = authenticateRequest(ctx, snap, config, operation, requestPeer, proxyState);
          const consoleCheck = checkConsoleAdmission(auth.billingAccount, auth.key);
          if (!consoleCheck) {
            return handleInferenceRequest(
              ctx,
              proxyDeps,
              operation,
              undefined,
              requestPeer,
              nativeHttp,
              {
                preAuth: { snap, key: auth.key, billingAccount: auth.billingAccount },
                declaredSize: prep.declaredSize,
                deferredIngress: ingress.activateDeferred(prep.ip),
              },
            );
          }
        } catch {
          void 0;
        }
      }
    }
    const denial = ingress.admit(
      req,
      operation ?? (pathname.startsWith("/v1beta/") ? "gemini" : "chat"),
      requestPeer,
    );
    const dispatch = () =>
      operation
        ? handleInferenceRequest(
            new FastInferenceContext(req),
            proxyDeps,
            operation,
            undefined,
            requestPeer,
            nativeHttp,
          )
        : publicApp.fetch(
            req,
            typeof requestPeer === "string"
              ? { peerAddress: requestPeer }
              : typeof env === "string"
                ? { peerAddress: env }
                : env,
          );
    return denial instanceof Promise
      ? denial.then((response) => response ?? dispatch())
      : (denial ?? dispatch());
  }

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
    fetchPublic,
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
      pressure?.start();
    },

    async close(drainMs = 5000): Promise<void> {
      pressure?.stop();
      const draining = lifecycle.drain(drainMs);
      await draining;
      await traceQueue?.close();
      proxyState.close();
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
