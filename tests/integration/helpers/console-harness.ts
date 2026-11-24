import type { SQL } from "bun";
import { createDatabase, migrate } from "@gateway/ledger";
import { createGatewayRuntime, type GatewayConfig } from "@gateway/gateway";
import { createWorkerRunner } from "@gateway/worker";
import {
  createConsoleDatabase,
  createConsoleRuntime,
  migrateConsoleDb,
  type ConsoleConfig,
} from "@gateway/console-api";

export type GatewayInterceptor = (
  url: string,
  init: RequestInit | undefined,
  forward: () => Promise<Response>,
) => Promise<Response>;

export type RecordedWebhook = {
  deliveryId: string | null;
  body: Record<string, unknown>;
};

export type ConsoleTestHarnessOptions = {
  consoleConfigOverrides?: Partial<ConsoleConfig>;
  gatewayConfigOverrides?: Partial<GatewayConfig>;
  enableWorker?: boolean;
};

export async function callJson<T = Record<string, unknown>>(
  baseUrl: string,
  method: string,
  path: string,
  token?: string,
  body?: unknown,
  extraHeaders?: Record<string, string>,
): Promise<{ status: number; data: T; headers: Headers }> {
  const headers: Record<string, string> = { ...extraHeaders };
  if (token) headers.authorization = `Bearer ${token}`;
  if (body !== undefined) headers["content-type"] = "application/json";
  const res = await fetch(`${baseUrl}${path}`, {
    method,
    headers,
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
  const text = await res.text();
  let data: unknown = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = { raw: text };
    }
  }
  return { status: res.status, data: data as T, headers: res.headers };
}

export async function createConsoleTestHarness(options: ConsoleTestHarnessOptions = {}) {
  const baseDbUrl = process.env.DATABASE_URL;
  if (!baseDbUrl) throw new Error("DATABASE_URL is required");

  const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 10);
  const gatewayDbName = `stg_e2e_gw_${suffix}`;
  const consoleDbName = `stg_e2e_con_${suffix}`;
  const adminUrl = new URL(baseDbUrl);
  adminUrl.pathname = "/postgres";
  const gwDbUrl = new URL(baseDbUrl);
  gwDbUrl.pathname = `/${gatewayDbName}`;
  const conDbUrl = new URL(baseDbUrl);
  conDbUrl.pathname = `/${consoleDbName}`;

  const keyPepper = `console-e2e-pepper-${suffix}-0123456789`;
  const encryptionKey = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
  const adminToken = `console-e2e-gw-admin-${suffix}-0123`;
  const outboxToken = `console-e2e-outbox-${suffix}-012345`;
  const bootstrapAdminEmail = "admin@platform.test";
  const bootstrapAdminToken = "bootstrap-secret-token-1234";

  let lastUpstreamAuth = "";
  const videoStates = new Map<string, Record<string, unknown>>();
  const webhookDeliveries: RecordedWebhook[] = [];
  let webhookStatus = 200;
  let gatewayInterceptor: GatewayInterceptor | null = null;

  const mockServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      if (req.method === "POST" && url.pathname === "/v1/chat/completions") {
        lastUpstreamAuth = req.headers.get("authorization") ?? "";
        const body = (await req.json()) as { model?: string };
        return Response.json({
          id: `chatcmpl-${crypto.randomUUID().slice(0, 8)}`,
          object: "chat.completion",
          created: Math.floor(Date.now() / 1000),
          model: body.model ?? "gpt-4o",
          choices: [
            {
              index: 0,
              message: { role: "assistant", content: "hello from upstream" },
              finish_reason: "stop",
            },
          ],
          usage: { prompt_tokens: 100, completion_tokens: 50, total_tokens: 150 },
        });
      }
      if (req.method === "POST" && url.pathname === "/v2/video_generation") {
        const tid = `up_con_${crypto.randomUUID().slice(0, 8)}`;
        videoStates.set(tid, { status: "Processing", task_id: tid });
        return Response.json({ task_id: tid });
      }
      if (req.method === "GET" && url.pathname === "/v2/query/video_generation") {
        const tid = url.searchParams.get("task_id") ?? "";
        return Response.json(videoStates.get(tid) ?? { status: "Fail", error_code: 9999 });
      }
      return new Response("Not Found", { status: 404 });
    },
  });
  const mockUrl = `http://127.0.0.1:${mockServer.port}`;

  const webhookServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const deliveryId = req.headers.get("x-stg-alert-delivery-id");
      const body = (await req.json().catch(() => ({}))) as Record<string, unknown>;
      webhookDeliveries.push({ deliveryId, body });
      if (webhookStatus >= 300 && webhookStatus < 400) {
        return new Response(null, {
          status: webhookStatus,
          headers: { location: "https://127.0.0.1/redirected" },
        });
      }
      return Response.json({ received: true }, { status: webhookStatus });
    },
  });
  const webhookUrl = `http://127.0.0.1:${webhookServer.port}/alerts`;

  const adminDb = createDatabase(adminUrl.toString(), 2);
  let gwDb: SQL | null = null;
  let conDb: SQL | null = null;
  let gateway: ReturnType<typeof createGatewayRuntime> | null = null;
  let gatewayPublicServer: ReturnType<typeof Bun.serve> | null = null;
  let gatewayInternalServer: ReturnType<typeof Bun.serve> | null = null;
  let consoleRuntime: ReturnType<typeof createConsoleRuntime> | null = null;
  let consoleClientServer: ReturnType<typeof Bun.serve> | null = null;
  let consoleAdminServer: ReturnType<typeof Bun.serve> | null = null;
  let worker: ReturnType<typeof createWorkerRunner> | null = null;

  try {
    await adminDb.unsafe(`CREATE DATABASE ${gatewayDbName}`);
    await adminDb.unsafe(`CREATE DATABASE ${consoleDbName}`);

    gwDb = createDatabase(gwDbUrl.toString(), 16);
    await migrate(gwDb);

    conDb = createConsoleDatabase(conDbUrl.toString(), 16);
    await migrateConsoleDb(conDb);

    const gwConfig: GatewayConfig = {
      mode: "gateway",
      host: "127.0.0.1",
      port: 0,
      internalHost: "127.0.0.1",
      internalPort: 0,
      databaseUrl: gwDbUrl.toString(),
      dbMaxConnections: 16,
      keyPepper,
      encryptionKey,
      adminToken,
      allowPrivateUpstreams: true,
      privateUpstreamCidrs: [],
      trustedProxyCidrs: [],
      maxInFlight: 200,
      maxBodyBytes: 2 * 1024 * 1024,
      holdTtlMs: 30_000,
      attemptTimeoutMs: 5_000,
      snapshotPollMs: 0,
      microBatchWindowMs: 2,
      microBatchMaxSize: 32,
      ...options.gatewayConfigOverrides,
    };
    gateway = createGatewayRuntime({ config: gwConfig, db: gwDb, wasmExecutor: null });
    await gateway.start();

    gatewayPublicServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: (req, server) =>
        gateway!.fetchPublic(req, {
          getPeerAddress: () => server.requestIP(req)?.address,
        }),
    });
    const gatewayPublicUrl = `http://127.0.0.1:${gatewayPublicServer.port}`;

    gatewayInternalServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: (req) => gateway!.internalApp.fetch(req),
    });
    const gatewayInternalUrl = `http://127.0.0.1:${gatewayInternalServer.port}`;

    const consoleConfig: ConsoleConfig = {
      host: "127.0.0.1",
      port: 0,
      adminHost: "127.0.0.1",
      adminPort: 0,
      databaseUrl: conDbUrl.toString(),
      dbMaxConnections: 16,
      gatewayInternalUrl,
      gatewayPublicUrl,
      gatewayTokens: { admin: adminToken },
      outboxWebhookToken: outboxToken,
      outboxConsumerName: `console_e2e_${suffix}`,
      bootstrapAdminEmail,
      bootstrapAdminToken,
      trustedProxyCidrs: [],
      sessionTtlMs: 3_600_000,
      refreshTtlMs: 86_400_000,
      reconcileIntervalMs: 0,
      outboxPullIntervalMs: 0,
      alertDeliveryIntervalMs: 0,
      allowPrivateAlertWebhooks: false,
      clientCorsOrigins: [],
      adminCorsOrigins: [],
      ...options.consoleConfigOverrides,
    };

    consoleRuntime = createConsoleRuntime({
      config: consoleConfig,
      db: conDb,
      gatewayFetch: async (input, init) => {
        const url =
          typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
        const forward = () => fetch(url, init);
        if (gatewayInterceptor) return gatewayInterceptor(url, init, forward);
        return forward();
      },
    });
    await consoleRuntime.start();

    consoleClientServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: (req, server) => consoleRuntime!.fetchClient(req, server.requestIP(req)?.address),
    });
    const clientUrl = `http://127.0.0.1:${consoleClientServer.port}`;

    consoleAdminServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: (req, server) => consoleRuntime!.fetchAdmin(req, server.requestIP(req)?.address),
    });
    const adminUrlHttp = `http://127.0.0.1:${consoleAdminServer.port}`;

    if (options.enableWorker !== false) {
      worker = createWorkerRunner({
        db: gwDb,
        businessWebhookUrl: `${adminUrlHttp}/internal/outbox/events`,
        businessWebhookToken: outboxToken,
        outboxConsumerName: `console_e2e_wh_${suffix}`,
      });
    }

    const clientApi = <T = Record<string, unknown>>(
      method: string,
      path: string,
      token?: string,
      body?: unknown,
      extraHeaders?: Record<string, string>,
    ) => callJson<T>(clientUrl, method, path, token, body, extraHeaders);

    const adminApi = <T = Record<string, unknown>>(
      method: string,
      path: string,
      token?: string,
      body?: unknown,
      extraHeaders?: Record<string, string>,
    ) => callJson<T>(adminUrlHttp, method, path, token, body, extraHeaders);

    const gatewayInternalApi = <T = Record<string, unknown>>(
      method: string,
      path: string,
      body?: unknown,
    ) => callJson<T>(gatewayInternalUrl, method, path, adminToken, body);

    const bootstrapAdmin = async () => {
      const res = await adminApi<{
        access_token: string;
        refresh_token: string;
        user: { id: string; email: string };
      }>("POST", "/admin/v1/auth/bootstrap", undefined, {
        email: bootstrapAdminEmail,
        password: "AdminPassword123!",
        display_name: "Platform Root",
        bootstrap_token: bootstrapAdminToken,
      });
      if (res.status !== 201) {
        throw new Error(`Bootstrap failed: ${res.status} ${JSON.stringify(res.data)}`);
      }
      return {
        accessToken: res.data.access_token,
        refreshToken: res.data.refresh_token,
        user: res.data.user,
      };
    };

    const registerUser = async (
      email: string,
      password = "UserPassword123!",
      displayName = "User",
    ) => {
      const res = await clientApi<{
        access_token: string;
        refresh_token: string;
        user: { id: string; email: string };
        default_org_id: string;
        default_project_id: string;
      }>("POST", "/api/v1/auth/register", undefined, {
        email,
        password,
        display_name: displayName,
      });
      if (res.status !== 201) {
        throw new Error(`Register failed: ${res.status} ${JSON.stringify(res.data)}`);
      }
      return {
        accessToken: res.data.access_token,
        refreshToken: res.data.refresh_token,
        user: res.data.user,
        defaultOrgId: res.data.default_org_id,
        defaultProjectId: res.data.default_project_id,
      };
    };

    return {
      gatewayDbName,
      consoleDbName,
      gwDbUrl: gwDbUrl.toString(),
      conDbUrl: conDbUrl.toString(),
      adminToken,
      outboxToken,
      bootstrapAdminEmail,
      bootstrapAdminToken,
      gwDb,
      conDb,
      gateway,
      gatewayConfig: gwConfig,
      worker,
      consoleRuntime,
      consoleConfig,
      mockUrl,
      webhookUrl,
      clientUrl,
      adminUrlHttp,
      gatewayPublicUrl,
      gatewayInternalUrl,
      videoStates,
      webhookDeliveries,
      getLastUpstreamAuth: () => lastUpstreamAuth,
      setWebhookStatus: (status: number) => {
        webhookStatus = status;
      },
      setGatewayInterceptor: (fn: GatewayInterceptor | null) => {
        gatewayInterceptor = fn;
      },
      clientApi,
      adminApi,
      gatewayInternalApi,
      bootstrapAdmin,
      registerUser,
      async close() {
        const steps: (() => Promise<unknown> | unknown)[] = [
          () => worker?.stop(),
          () => consoleClientServer?.stop(true),
          () => consoleAdminServer?.stop(true),
          () => gatewayPublicServer?.stop(true),
          () => gatewayInternalServer?.stop(true),
          () => consoleRuntime?.close(2000),
          () => gateway?.close(2000),
          () => mockServer.stop(true),
          () => webhookServer.stop(true),
          () => conDb?.close(),
          () => gwDb?.close(),
          async () => {
            for (const dbName of [gatewayDbName, consoleDbName]) {
              try {
                await adminDb.unsafe(
                  `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${dbName}' AND pid <> pg_backend_pid()`,
                );
                await adminDb.unsafe(`DROP DATABASE IF EXISTS ${dbName}`);
              } catch {
                void 0;
              }
            }
          },
          () => adminDb.close(),
        ];
        for (const step of steps) {
          try {
            await step();
          } catch {
            void 0;
          }
        }
      },
    };
  } catch (err) {
    try {
      await worker?.stop();
    } catch {
      void 0;
    }
    try {
      consoleClientServer?.stop(true);
      consoleAdminServer?.stop(true);
      gatewayPublicServer?.stop(true);
      gatewayInternalServer?.stop(true);
      mockServer.stop(true);
      webhookServer.stop(true);
    } catch {
      void 0;
    }
    await consoleRuntime?.close(1000).catch(() => undefined);
    await gateway?.close(1000).catch(() => undefined);
    await conDb?.close().catch(() => undefined);
    await gwDb?.close().catch(() => undefined);
    for (const dbName of [gatewayDbName, consoleDbName]) {
      try {
        await adminDb.unsafe(
          `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${dbName}' AND pid <> pg_backend_pid()`,
        );
        await adminDb.unsafe(`DROP DATABASE IF EXISTS ${dbName}`);
      } catch {
        void 0;
      }
    }
    await adminDb.close().catch(() => undefined);
    throw err;
  }
}
