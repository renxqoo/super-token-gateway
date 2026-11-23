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
  pullOutboxBatch,
  type IncomingOutboxEvent,
} from "./outbox-consumer.ts";
import { createAdminRoutes } from "./routes/admin.ts";
import { createApiKeyRoutes } from "./routes/api-keys.ts";
import { createAdminAuthRoutes, createClientAuthRoutes } from "./routes/auth.ts";
import { createMarketplaceAndBillingRoutes } from "./routes/marketplace-billing.ts";
import { createOrgAndProjectRoutes } from "./routes/orgs.ts";
import { parseJsonBody, type ConsoleDeps, type ConsoleEnv } from "./routes/shared.ts";
import { ConsoleSyncEngine } from "./sync.ts";

export type ConsoleRuntimeOptions = {
  config: ConsoleConfig;
  db?: SQL;
  gatewayFetch?: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
};

const outboxBatchSchema = z
  .object({
    consumer: z.string().min(1).max(120).optional(),
    events: z
      .array(
        z.object({
          seq: z.union([z.string(), z.number()]).nullable().optional(),
          event_id: z.uuid(),
          kind: z.string().min(1).max(120),
          request_id: z.string().nullable().optional(),
          project_id: z.string().nullable().optional(),
          payload: z.record(z.string(), z.unknown()).optional(),
          created_at: z.string().optional(),
        }),
      )
      .max(2000),
  })
  .strict();

function attachCommonHandlers(app: Hono<ConsoleEnv>, db: SQL): void {
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
    if ("errno" in err && typeof err.errno === "string") {
      if (err.errno === "23505") {
        return c.json(
          { error: { code: "conflict", message: "A conflicting record already exists" } },
          409,
        );
      }
      if (err.errno === "23503") {
        return c.json(
          { error: { code: "reference_error", message: "Referenced entity does not exist" } },
          400,
        );
      }
    }
    return c.json(
      { error: { code: "internal_error", message: "Internal console API error" } },
      500,
    );
  });

  app.get("/livez", (c) => c.json({ status: "ok" }));

  app.get("/readyz", async (c) => {
    try {
      await db`SELECT 1`;
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
  const orm = createConsoleDrizzle(db);
  const gateway = new GatewayClient({
    baseUrl: config.gatewayInternalUrl,
    tokens: config.gatewayTokens,
    ...(options.gatewayFetch ? { fetchImpl: options.gatewayFetch } : {}),
  });
  const sync = new ConsoleSyncEngine(orm, gateway);
  const loginLimiter = new LoginRateLimiter();

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
  attachCommonHandlers(clientApp, db);
  clientApp.route("/api/v1/auth", createClientAuthRoutes(deps));
  clientApp.route("/api/v1", createOrgAndProjectRoutes(deps));
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
  attachCommonHandlers(adminApp, db);

  adminApp.post("/internal/outbox/events", async (c) => {
    const token = extractBearerToken(c.req.header("authorization"));
    if (!token || !constantTimeEquals(token, config.outboxWebhookToken)) {
      return c.json({ error: { code: "unauthorized", message: "Invalid webhook token" } }, 401);
    }
    const body = await parseJsonBody(c, outboxBatchSchema);
    const result = await ingestOutboxEvents(db, orm, gateway, body.events as IncomingOutboxEvent[]);
    return c.json({ ok: true, ...result });
  });

  adminApp.route("/admin/v1/auth", createAdminAuthRoutes(deps));
  adminApp.route("/admin/v1", createAdminRoutes(deps));

  const timers: ReturnType<typeof setInterval>[] = [];
  let running = false;
  let activeReconcile: Promise<unknown> | null = null;
  let activePull: Promise<unknown> | null = null;

  return {
    clientApp,
    adminApp,
    db,
    orm,
    gateway,
    sync,

    async pullOutboxOnce() {
      return pullOutboxBatch(db, orm, gateway, config.outboxConsumerName);
    },

    async start(): Promise<void> {
      if (running) return;
      await assertConsoleSchemaVersion(db);
      running = true;
      if (config.reconcileIntervalMs > 0) {
        const t = setInterval(() => {
          if (!running || activeReconcile) return;
          activeReconcile = sync
            .reconcilePending()
            .catch(() => undefined)
            .finally(() => {
              activeReconcile = null;
            });
        }, config.reconcileIntervalMs);
        t.unref();
        timers.push(t);
      }
      if (config.outboxPullIntervalMs > 0) {
        const t = setInterval(() => {
          if (!running || activePull) return;
          activePull = pullOutboxBatch(db, orm, gateway, config.outboxConsumerName)
            .catch(() => undefined)
            .finally(() => {
              activePull = null;
            });
        }, config.outboxPullIntervalMs);
        t.unref();
        timers.push(t);
      }
    },

    async close(): Promise<void> {
      running = false;
      for (const t of timers) clearInterval(t);
      timers.length = 0;
      await Promise.allSettled([activeReconcile, activePull].filter(Boolean));
      if (ownsDb) await db.close();
    },
  };
}
