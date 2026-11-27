import { Hono } from "hono";
import { bodyLimit } from "hono/body-limit";
import { cors } from "hono/cors";
import type { SQL } from "bun";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { constantTimeEquals, extractBearerToken, LoginRateLimiter } from "./auth.ts";
import type { ConsoleConfig } from "./config.ts";
import { assertConsoleSchemaVersion, createConsoleDatabase } from "./db/migrate.ts";
import { createConsoleDrizzle } from "./db/schema.ts";
import { GatewayClient } from "./gateway-client.ts";
import {
  ingestOutboxEvents,
  processPendingAlertEvaluations,
  processPendingWebhookDeliveries,
  pullOutboxBatch,
  type IncomingOutboxEvent,
} from "./outbox-consumer.ts";
import { createAdminRoutes } from "./routes/admin.ts";
import { createApiKeyRoutes } from "./routes/api-keys.ts";
import { createAdminAuthRoutes, createClientAuthRoutes } from "./routes/auth.ts";
import { createMarketplaceAndBillingRoutes } from "./routes/marketplace-billing.ts";
import { createOrgAndBillingAccountRoutes } from "./routes/orgs.ts";
import {
  extractPgErrno,
  parseJsonBody,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./routes/shared.ts";
import { createConsoleSync } from "./sync.ts";
import { createUsageMonitor } from "./usage-monitor/monitor.ts";

export type ConsoleRuntimeOptions = {
  config: ConsoleConfig;
  db?: SQL;
  gatewayFetch?: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
  alertFetch?: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
};

const outboxBatchSchema = z
  .object({
    consumer: z.string().min(1).max(120).optional(),
    events: z
      .array(
        z
          .object({
            seq: z.union([z.string(), z.number()]).nullable().optional(),
            event_id: z.uuid(),
            kind: z.string().min(1).max(120),
            request_id: z.string().nullable().optional(),
            billing_account_id: z.string().nullable().optional(),
            payload: z.record(z.string(), z.unknown()).optional(),
            created_at: z.string().optional(),
          })
          .strict(),
      )
      .max(2000),
  })
  .strict();

function logStructuredError(event: string, err: unknown): void {
  const sqlstate = extractPgErrno(err);
  console.error(
    JSON.stringify({
      service: "console-api",
      level: "error",
      event,
      ...(sqlstate ? { sqlstate } : {}),
      ...(err instanceof GatewayError ? { code: err.code, status: err.status } : {}),
    }),
  );
}

function attachCommonHandlers(
  app: Hono<ConsoleEnv>,
  db: SQL,
  peerIpMap: WeakMap<Request, string>,
): void {
  app.use("*", async (c, next) => {
    const peerIp = peerIpMap.get(c.req.raw);
    if (peerIp) c.set("peerIp", peerIp);
    await next();
  });

  app.use(
    "*",
    bodyLimit({
      maxSize: 4 * 1024 * 1024,
      onError: (c) =>
        c.json({ error: { code: "body_too_large", message: "Request body exceeds limit" } }, 413),
    }),
  );

  app.onError((err, c) => {
    if (err instanceof GatewayError) {
      return c.json({ error: { code: err.code, message: err.message } }, err.status as 400);
    }
    const errno = extractPgErrno(err);
    if (errno === "23505") {
      return c.json(
        { error: { code: "conflict", message: "A conflicting record already exists" } },
        409,
      );
    }
    if (errno === "23503") {
      return c.json(
        { error: { code: "reference_error", message: "Referenced entity does not exist" } },
        400,
      );
    }
    if (errno === "22P02" || errno === "22007") {
      return c.json(
        { error: { code: "validation_error", message: "Invalid parameter syntax" } },
        400,
      );
    }
    logStructuredError("http_unhandled_error", err);
    return c.json(
      { error: { code: "internal_error", message: "Internal console API error" } },
      500,
    );
  });

  app.get("/livez", (c) => c.json({ status: "ok" }));

  app.get("/readyz", async (c) => {
    try {
      const [row] = await db`SELECT current_setting('transaction_read_only') = 'off' AS writable`;
      if (!row?.writable) return c.json({ status: "db_unavailable" }, 503);
      return c.json({ status: "ready" });
    } catch {
      return c.json({ status: "db_unavailable" }, 503);
    }
  });
}

export function createConsoleRuntime(options: ConsoleRuntimeOptions) {
  const { config } = options;
  const ownsDb = !options.db;
  const db = options.db ?? createConsoleDatabase(config.databaseUrl, config.dbMaxConnections);
  const monitorDb = config.usageMonitor ? createConsoleDatabase(config.databaseUrl, 1) : undefined;
  const usageMonitor =
    monitorDb && config.usageMonitor
      ? createUsageMonitor(monitorDb, config.usageMonitor)
      : undefined;
  const orm = createConsoleDrizzle(db);
  const gateway = new GatewayClient({
    baseUrl: config.gatewayInternalUrl,
    tokens: config.gatewayTokens,
    ...(options.gatewayFetch ? { fetchImpl: options.gatewayFetch } : {}),
  });
  const sync = createConsoleSync(db, orm, gateway);
  const loginLimiter = new LoginRateLimiter();
  const peerIpMap = new WeakMap<Request, string>();

  const deps: ConsoleDeps = {
    config,
    db,
    orm,
    gateway,
    sync,
    loginLimiter,
  };

  const clientApp = new Hono<ConsoleEnv>();
  if (config.clientCorsOrigins.length > 0) {
    clientApp.use(
      "/api/*",
      cors({
        origin: config.clientCorsOrigins,
        allowHeaders: ["Authorization", "Content-Type"],
        allowMethods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
      }),
    );
  }
  attachCommonHandlers(clientApp, db, peerIpMap);
  clientApp.route("/api/v1/auth", createClientAuthRoutes(deps));
  clientApp.route("/api/v1", createOrgAndBillingAccountRoutes(deps));
  clientApp.route("/api/v1", createApiKeyRoutes(deps));
  clientApp.route("/api/v1", createMarketplaceAndBillingRoutes(deps));

  const adminApp = new Hono<ConsoleEnv>();
  if (config.adminCorsOrigins.length > 0) {
    adminApp.use(
      "/admin/*",
      cors({
        origin: config.adminCorsOrigins,
        allowHeaders: ["Authorization", "Content-Type"],
        allowMethods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
      }),
    );
  }
  attachCommonHandlers(adminApp, db, peerIpMap);

  adminApp.get("/metrics", (c) =>
    c.text(usageMonitor?.metrics() ?? "stg_console_usage_monitor_enabled 0\n", 200, {
      "content-type": "text/plain; version=0.0.4; charset=utf-8",
      "cache-control": "no-store",
    }),
  );

  adminApp.post("/internal/outbox/events", async (c) => {
    const token = extractBearerToken(c.req.header("authorization"));
    if (!token || !constantTimeEquals(token, config.outboxWebhookToken)) {
      return c.json({ error: { code: "unauthorized", message: "Invalid webhook token" } }, 401);
    }
    const body = await parseJsonBody(c, outboxBatchSchema);
    const result = await ingestOutboxEvents(db, orm, body.events as IncomingOutboxEvent[]);
    return c.json({ ok: true, ...result });
  });

  adminApp.route("/admin/v1/auth", createAdminAuthRoutes(deps));
  adminApp.route("/admin/v1", createAdminRoutes(deps));

  const timers: ReturnType<typeof setInterval>[] = [];
  let running = false;
  let stopController = new AbortController();
  const activeJobs = new Set<Promise<unknown>>();

  const webhookDeliveryOpts = {
    ...(options.alertFetch ? { fetchImpl: options.alertFetch } : {}),
    ...(config.allowPrivateAlertWebhooks !== undefined
      ? { allowPrivateWebhooks: config.allowPrivateAlertWebhooks }
      : {}),
  };

  function schedule(intervalMs: number, event: string, work: () => Promise<unknown>): void {
    if (intervalMs <= 0) return;
    let active = false;
    const timer = setInterval(() => {
      if (!running || active) return;
      active = true;
      const task = work()
        .catch((err) => logStructuredError(event, err))
        .finally(() => {
          active = false;
          activeJobs.delete(task);
        });
      activeJobs.add(task);
    }, intervalMs);
    timer.unref();
    timers.push(timer);
  }

  return {
    clientApp,
    adminApp,
    db,
    orm,
    gateway,
    sync,

    fetchClient(req: Request, peerIp?: string): Promise<Response> | Response {
      if (peerIp) peerIpMap.set(req, peerIp);
      return clientApp.fetch(req);
    },

    fetchAdmin(req: Request, peerIp?: string): Promise<Response> | Response {
      if (peerIp) peerIpMap.set(req, peerIp);
      return adminApp.fetch(req);
    },

    async pullOutboxOnce() {
      return pullOutboxBatch(db, orm, gateway, config.outboxConsumerName);
    },

    async flushAlertsOnce() {
      return processPendingAlertEvaluations(db, orm, gateway);
    },

    async deliverAlertWebhooksOnce() {
      return processPendingWebhookDeliveries(db, webhookDeliveryOpts);
    },

    async flushUsageMonitorOnce(now?: number) {
      return usageMonitor?.runOnce(now);
    },

    async start(): Promise<void> {
      if (running) return;
      await assertConsoleSchemaVersion(db);
      stopController = new AbortController();
      running = true;
      schedule(config.reconcileIntervalMs, "reconcile_failed", () =>
        sync.reconcilePending(stopController.signal),
      );
      schedule(config.outboxPullIntervalMs, "outbox_pull_failed", () =>
        pullOutboxBatch(db, orm, gateway, config.outboxConsumerName),
      );
      schedule(
        config.alertEvaluationIntervalMs ?? config.alertDeliveryIntervalMs,
        "alert_evaluation_failed",
        () => processPendingAlertEvaluations(db, orm, gateway),
      );
      schedule(config.alertDeliveryIntervalMs, "alert_delivery_failed", () =>
        processPendingWebhookDeliveries(db, {
          ...webhookDeliveryOpts,
          signal: stopController.signal,
        }),
      );
      if (usageMonitor && config.usageMonitor) {
        schedule(config.usageMonitor.intervalMs, "usage_monitor_failed", () =>
          usageMonitor.runOnce(),
        );
      }
    },

    async close(timeoutMs = 5_000): Promise<void> {
      const deadline = performance.now() + timeoutMs;
      running = false;
      stopController.abort();
      for (const t of timers) clearInterval(t);
      timers.length = 0;
      const pending = [...activeJobs];
      if (pending.length > 0) {
        const timeout = Promise.withResolvers<void>();
        const timer = setTimeout(timeout.resolve, timeoutMs);
        try {
          await Promise.race([Promise.allSettled(pending), timeout.promise]);
        } finally {
          clearTimeout(timer);
        }
      }
      if (ownsDb) await db.close({ timeout: Math.max(0, (deadline - performance.now()) / 1_000) });
      await monitorDb?.close({ timeout: Math.max(0, (deadline - performance.now()) / 1_000) });
    },
  };
}
