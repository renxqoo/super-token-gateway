import { afterAll, beforeAll, expect, test } from "bun:test";
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

const KEY_PEPPER = "console-e2e-pepper-0123456789abcdef";
const ENCRYPTION_KEY = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
const ADMIN_TOKEN = "console-e2e-gw-admin-token-0123456";
const OUTBOX_TOKEN = "console-e2e-outbox-webhook-secret-1";

let adminDb: SQL;
let gwDb: SQL;
let conDb: SQL;
let gateway: ReturnType<typeof createGatewayRuntime>;
let worker: ReturnType<typeof createWorkerRunner>;
let consoleRuntime: ReturnType<typeof createConsoleRuntime>;
let mockServer: ReturnType<typeof Bun.serve>;
let consoleServer: ReturnType<typeof Bun.serve>;
let gatewayPublicServer: ReturnType<typeof Bun.serve>;
let mockUrl = "";
let consoleUrl = "";
let gatewayPublicUrl = "";

let lastUpstreamAuth = "";
const videoStates = new Map<string, Record<string, unknown>>();

async function api<T = Record<string, unknown>>(
  method: string,
  path: string,
  token?: string,
  body?: unknown,
): Promise<{ status: number; data: T }> {
  const headers: Record<string, string> = {};
  if (token) headers.authorization = `Bearer ${token}`;
  if (body !== undefined) headers["content-type"] = "application/json";
  const res = await fetch(`${consoleUrl}${path}`, {
    method,
    headers,
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
  return { status: res.status, data: (await res.json()) as T };
}

beforeAll(async () => {
  mockServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      if (req.method === "POST" && url.pathname === "/v1/chat/completions") {
        lastUpstreamAuth = req.headers.get("authorization") ?? "";
        const body = (await req.json()) as { model?: string };
        return Response.json({
          id: "chatcmpl-console-e2e",
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
  mockUrl = `http://127.0.0.1:${mockServer.port}`;

  adminDb = createDatabase(adminUrl.toString(), 2);
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
    keyPepper: KEY_PEPPER,
    encryptionKey: ENCRYPTION_KEY,
    adminToken: ADMIN_TOKEN,
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
  };
  gateway = createGatewayRuntime({ config: gwConfig, db: gwDb, wasmExecutor: null });
  await gateway.start();

  gatewayPublicServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: (req) => gateway.fetchPublic(req),
  });
  gatewayPublicUrl = `http://127.0.0.1:${gatewayPublicServer.port}`;

  const consoleConfig: ConsoleConfig = {
    host: "127.0.0.1",
    port: 0,
    databaseUrl: conDbUrl.toString(),
    dbMaxConnections: 16,
    gatewayInternalUrl: "http://gateway.internal",
    gatewayPublicUrl,
    gatewayTokens: { admin: ADMIN_TOKEN },
    outboxWebhookToken: OUTBOX_TOKEN,
    outboxConsumerName: "console_e2e",
    bootstrapAdminEmail: "admin@platform.test",
    sessionTtlMs: 3_600_000,
    refreshTtlMs: 86_400_000,
    reconcileIntervalMs: 0,
    outboxPullIntervalMs: 0,
    corsOrigins: [],
  };
  consoleRuntime = createConsoleRuntime({
    config: consoleConfig,
    db: conDb,
    gatewayFetch: async (input, init) => {
      const url =
        typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
      const parsed = new URL(url);
      return gateway.internalApp.request(`${parsed.pathname}${parsed.search}`, init);
    },
  });
  await consoleRuntime.start();

  consoleServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: consoleRuntime.app.fetch,
  });
  consoleUrl = `http://127.0.0.1:${consoleServer.port}`;

  worker = createWorkerRunner({
    db: gwDb,
    businessWebhookUrl: `${consoleUrl}/internal/outbox/events`,
    businessWebhookToken: OUTBOX_TOKEN,
    outboxConsumerName: "console_e2e_webhook",
  });
});

afterAll(async () => {
  await worker?.stop();
  consoleServer?.stop(true);
  gatewayPublicServer?.stop(true);
  await consoleRuntime?.close();
  await gateway?.close(2000);
  mockServer?.stop(true);
  await conDb?.close();
  await gwDb?.close();
  if (adminDb) {
    await adminDb.unsafe(
      `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname IN ('${gatewayDbName}', '${consoleDbName}') AND pid <> pg_backend_pid()`,
    );
    await adminDb.unsafe(`DROP DATABASE IF EXISTS ${consoleDbName}`);
    await adminDb.unsafe(`DROP DATABASE IF EXISTS ${gatewayDbName}`);
    await adminDb.close();
  }
});

test("Console API full E2E: auth, enterprise RBAC, marketplace discount pricing, secretless catalog sync, self-service keys, real Worker Outbox rollups, alerts, and async video tasks", async () => {
  const adminReg = await api<{
    user: { id: string; is_platform_admin: boolean };
    access_token: string;
    refresh_token: string;
  }>("POST", "/api/v1/auth/register", undefined, {
    email: "admin@platform.test",
    password: "AdminPassword123!",
    display_name: "Platform Admin",
  });
  expect(adminReg.status).toBe(201);
  expect(adminReg.data.user.is_platform_admin).toBe(true);
  const adminToken = adminReg.data.access_token;

  const ownerReg = await api<{
    user: { id: string; is_platform_admin: boolean };
    default_org_id: string;
    default_project_id: string;
    access_token: string;
    refresh_token: string;
  }>("POST", "/api/v1/auth/register", undefined, {
    email: "owner@acme.test",
    password: "OwnerPassword123!",
    display_name: "Acme Owner",
  });
  expect(ownerReg.status).toBe(201);
  expect(ownerReg.data.user.is_platform_admin).toBe(false);

  const devReg = await api<{ user: { id: string }; access_token: string }>(
    "POST",
    "/api/v1/auth/register",
    undefined,
    {
      email: "dev@acme.test",
      password: "DevPassword123!",
      display_name: "Acme Dev",
    },
  );
  const financeReg = await api<{ user: { id: string }; access_token: string }>(
    "POST",
    "/api/v1/auth/register",
    undefined,
    {
      email: "fin@acme.test",
      password: "FinPassword123!",
      display_name: "Acme Finance",
    },
  );

  const nonAdminCheck = await api("GET", "/admin/v1/users", ownerReg.data.access_token);
  expect(nonAdminCheck.status).toBe(403);

  const refreshed = await api<{ access_token: string; refresh_token: string }>(
    "POST",
    "/api/v1/auth/refresh",
    undefined,
    { refresh_token: ownerReg.data.refresh_token },
  );
  expect(refreshed.status).toBe(200);
  const ownerToken = refreshed.data.access_token;
  const oldTokenRejected = await api("GET", "/api/v1/auth/me", ownerReg.data.access_token);
  expect(oldTokenRejected.status).toBe(401);

  const entOrg = await api<{ id: string; default_project_id: string }>(
    "POST",
    "/api/v1/orgs",
    ownerToken,
    { name: "Acme Corp", slug: "acme-corp" },
  );
  expect(entOrg.status).toBe(201);
  const orgId = entOrg.data.id;
  const projId = entOrg.data.default_project_id;

  const devInvite = await api<{ token: string }>(
    "POST",
    `/api/v1/orgs/${orgId}/invitations`,
    ownerToken,
    { email: "dev@acme.test", role: "DEVELOPER" },
  );
  expect(devInvite.status).toBe(201);
  const finInvite = await api<{ token: string }>(
    "POST",
    `/api/v1/orgs/${orgId}/invitations`,
    ownerToken,
    { email: "fin@acme.test", role: "FINANCE" },
  );
  expect(finInvite.status).toBe(201);

  expect(
    (
      await api(
        "POST",
        `/api/v1/invitations/${devInvite.data.token}/accept`,
        devReg.data.access_token,
      )
    ).status,
  ).toBe(200);
  expect(
    (
      await api(
        "POST",
        `/api/v1/invitations/${finInvite.data.token}/accept`,
        financeReg.data.access_token,
      )
    ).status,
  ).toBe(200);

  const finKeyDenied = await api(
    "POST",
    `/api/v1/projects/${projId}/api-keys`,
    financeReg.data.access_token,
    { name: "should-fail" },
  );
  expect(finKeyDenied.status).toBe(403);

  const removeLastOwner = await api(
    "DELETE",
    `/api/v1/orgs/${orgId}/members/${ownerReg.data.user.id}`,
    ownerToken,
  );
  expect(removeLastOwner.status).toBe(400);

  const upAcc = await api<{ id: string }>("POST", "/admin/v1/upstream-accounts", adminToken, {
    name: "e2e-upstream",
  });
  expect(upAcc.status).toBe(201);
  await api("POST", `/admin/v1/upstream-accounts/${upAcc.data.id}/recharges`, adminToken, {
    currency: "USD",
    external_ref: "e2e-sup-1",
    amount_atoms: "100000000",
  });

  const videoPlugin = await api<{ id: string; config_epoch: number }>(
    "PUT",
    "/admin/v1/plugins",
    adminToken,
    {
      name: "e2e_multi",
      version: "1.0.0",
      capability: "PROVIDER",
      kind: "DECLARATIVE",
      provider_config: {
        base_dialect: "openai_chat",
        usage_dialect: "openai",
        auth: { scheme: "bearer" },
        endpoints: {
          chat: { path: "/v1/chat/completions" },
          videos: { path: "/v2/video_generation" },
        },
        async_task_config: {
          query_path: "/v2/query/video_generation",
          query_param: "task_id",
          task_id_path: "task_id",
          status_path: "status",
          succeeded_values: ["Success"],
          failed_values: ["Fail"],
          error_code_path: "error_code",
          refundable_system_error_codes: [5001],
          non_refundable_error_codes: [1026],
        },
      },
    },
  );
  expect(videoPlugin.status).toBe(200);
  await api("POST", "/admin/v1/plugin-releases", adminToken, {
    id: crypto.randomUUID(),
    expected_epoch: videoPlugin.data.config_epoch,
    targets: [{ plugin_id: videoPlugin.data.id }],
  });

  const channelSecret = "sk-super-secret-upstream-token-9999";
  const ch = await api<{ id: string; keyHint: string }>("POST", "/admin/v1/channels", adminToken, {
    account_id: upAcc.data.id,
    name: "e2e-ch",
    kind: "e2e_multi",
    provider_plugin_id: videoPlugin.data.id,
    base_url: mockUrl,
    api_key: channelSecret,
  });
  expect(ch.status).toBe(201);
  expect(ch.data.keyHint).toBe("sk-sup***9999");

  const chDbDump = await conDb`SELECT * FROM public.channels WHERE id = ${ch.data.id}`;
  expect(JSON.stringify(chDbDump)).not.toContain(channelSecret);

  const chPatch = await api("PATCH", `/admin/v1/channels/${ch.data.id}`, adminToken, {
    name: "e2e-ch-updated",
    rpm: 200000,
  });
  expect(chPatch.status).toBe(200);

  const custTpl = await api<{ id: string; currentPriceVersionId: string }>(
    "POST",
    "/admin/v1/pricing-templates",
    adminToken,
    {
      name: "gpt4o-list",
      side: "CUSTOMER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: {
        input_tokens: { numerator: "10", denominator: "1" },
        output_tokens: { numerator: "20", denominator: "1" },
      },
      ceilings: { input_tokens: "1000", output_tokens: "1000" },
    },
  );
  const supTpl = await api<{ id: string; currentPriceVersionId: string }>(
    "POST",
    "/admin/v1/pricing-templates",
    adminToken,
    {
      name: "gpt4o-cost",
      side: "SUPPLIER",
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: {
        input_tokens: { numerator: "4", denominator: "1" },
        output_tokens: { numerator: "8", denominator: "1" },
      },
      ceilings: { input_tokens: "1000", output_tokens: "1000" },
    },
  );
  const vidCustTpl = await api<{ id: string; currentPriceVersionId: string }>(
    "POST",
    "/admin/v1/pricing-templates",
    adminToken,
    {
      name: "vid-list",
      side: "CUSTOMER",
      currency: "USD",
      basis: "REQUEST_COMPLETION",
      rates: { requests: { numerator: "3000", denominator: "1" } },
      ceilings: { requests: "1" },
    },
  );
  const vidSupTpl = await api<{ id: string; currentPriceVersionId: string }>(
    "POST",
    "/admin/v1/pricing-templates",
    adminToken,
    {
      name: "vid-cost",
      side: "SUPPLIER",
      currency: "USD",
      basis: "REQUEST_COMPLETION",
      rates: { requests: { numerator: "1000", denominator: "1" } },
      ceilings: { requests: "1" },
    },
  );

  await api("POST", "/admin/v1/models", adminToken, {
    name: "acme/gpt-4o",
    display_name: "GPT-4o Enterprise",
    provider_brand: "openai",
    category: "chat",
    operations: ["chat"],
    list_price_template_id: custTpl.data.id,
  });
  await api("POST", "/admin/v1/models", adminToken, {
    name: "acme/video-1",
    display_name: "Video Gen 1",
    provider_brand: "minimax",
    category: "video",
    operations: ["videos"],
    list_price_template_id: vidCustTpl.data.id,
  });

  await api("POST", "/admin/v1/routes", adminToken, {
    model_name: "acme/gpt-4o",
    channel_id: ch.data.id,
    upstream_model: "gpt-4o",
    operations: ["chat"],
    billing_template_id: custTpl.data.id,
    cost_template_id: supTpl.data.id,
  });
  await api("POST", "/admin/v1/routes", adminToken, {
    model_name: "acme/video-1",
    channel_id: ch.data.id,
    upstream_model: "video-1",
    operations: ["videos"],
    billing_template_id: vidCustTpl.data.id,
    cost_template_id: vidSupTpl.data.id,
  });

  const unsyncedWallet = await api<{ synced: boolean; available_atoms: string }>(
    "GET",
    `/api/v1/projects/${projId}/wallet`,
    financeReg.data.access_token,
  );
  expect(unsyncedWallet.status).toBe(200);
  expect(unsyncedWallet.data.synced).toBe(false);

  const topupRes = await api("POST", `/admin/v1/finance/projects/${projId}/topups`, adminToken, {
    currency: "USD",
    external_ref: "acme-wire-001",
    amount_atoms: "500000",
    reason: "Enterprise wire transfer",
  });
  expect(topupRes.status).toBe(200);

  await api("PUT", "/admin/v1/pricing-overrides", adminToken, {
    project_id: projId,
    model_name: "acme/gpt-4o",
    operation: "*",
    override_kind: "MULTIPLIER",
    multiplier_numerator: "8",
    multiplier_denominator: "10",
  });

  const anonMarket = await api<{
    items: {
      name: string;
      pricing: { effective_rates: { input_tokens?: { numerator: string } } };
    }[];
  }>("GET", "/api/v1/models?category=chat");
  expect(anonMarket.status).toBe(200);
  expect(anonMarket.data.items[0]!.pricing.effective_rates.input_tokens?.numerator).toBe("10");

  const entMarket = await api<{
    pricing: {
      list_rates: { input_tokens?: { numerator: string } };
      effective_rates: { input_tokens?: { numerator: string } };
      override_applied: { override_kind: string } | null;
    };
    sdk_snippets: { curl?: string; openai_ts?: string };
  }>("GET", `/api/v1/models/${encodeURIComponent("acme/gpt-4o")}?project_id=${projId}`, ownerToken);
  expect(entMarket.status).toBe(200);
  expect(entMarket.data.pricing.list_rates.input_tokens?.numerator).toBe("10");
  expect(entMarket.data.pricing.effective_rates.input_tokens?.numerator).toBe("8");
  expect(entMarket.data.pricing.override_applied?.override_kind).toBe("MULTIPLIER");
  expect(entMarket.data.sdk_snippets.curl).toContain("/v1/chat/completions");

  await api("POST", `/api/v1/projects/${projId}/alerts`, financeReg.data.access_token, {
    kind: "DAILY_SPEND",
    threshold_atoms: "1000",
  });

  const createdKey = await api<{ id: string; key: string; keyPrefix: string }>(
    "POST",
    `/api/v1/projects/${projId}/api-keys`,
    devReg.data.access_token,
    {
      name: "prod-service-key",
      spend_limit_atoms: "100000",
      spend_limit_window: "DAILY",
    },
  );
  expect(createdKey.status).toBe(201);
  expect(createdKey.data.key).toBeTruthy();

  const keyDbDump =
    await conDb`SELECT * FROM public.api_key_records WHERE id = ${createdKey.data.id}`;
  expect(JSON.stringify(keyDbDump)).not.toContain(createdKey.data.key);

  const chatRes = await fetch(`${gatewayPublicUrl}/v1/chat/completions`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${createdKey.data.key}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "acme/gpt-4o",
      max_tokens: 100,
      messages: [{ role: "user", content: "ping" }],
    }),
  });
  expect(chatRes.status).toBe(200);
  expect(lastUpstreamAuth).toBe(`Bearer ${channelSecret}`);

  const vidRes = await fetch(`${gatewayPublicUrl}/v1/videos`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${createdKey.data.key}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ model: "acme/video-1", prompt: "ocean" }),
  });
  expect(vidRes.status).toBe(200);
  const vidBody = (await vidRes.json()) as { gateway_task_id: string; task_id: string };
  videoStates.set(vidBody.task_id, { status: "Success", video_url: "https://cdn.test/v.mp4" });
  const vidPoll = await fetch(`${gatewayPublicUrl}/v1/videos/${vidBody.gateway_task_id}`, {
    headers: { authorization: `Bearer ${createdKey.data.key}` },
  });
  expect(vidPoll.status).toBe(200);

  const tick = await worker.runOnce(false);
  expect(tick.delivered_outbox_events).toBeGreaterThanOrEqual(2);

  const summary = await api<{
    request_count: number;
    customer_atoms: string;
    input_tokens: number;
    output_tokens: number;
  }>("GET", `/api/v1/projects/${projId}/usage/summary`, financeReg.data.access_token);
  expect(summary.status).toBe(200);
  expect(summary.data.request_count).toBe(2);
  expect(summary.data.customer_atoms).toBe("4600");
  expect(summary.data.input_tokens).toBe(100);
  expect(summary.data.output_tokens).toBe(50);

  const byKey = await api<{
    items: { api_key_id: string; key_name: string; customer_atoms: string }[];
  }>("GET", `/api/v1/projects/${projId}/usage/by-key`, financeReg.data.access_token);
  expect(byKey.data.items.length).toBe(1);
  expect(byKey.data.items[0]!.api_key_id).toBe(createdKey.data.id);
  expect(byKey.data.items[0]!.key_name).toBe("prod-service-key");
  expect(byKey.data.items[0]!.customer_atoms).toBe("4600");

  const byModel = await api<{ items: { model: string; customer_atoms: string }[] }>(
    "GET",
    `/api/v1/projects/${projId}/usage/by-model`,
    financeReg.data.access_token,
  );
  expect(byModel.data.items.length).toBe(2);

  const alertsState = await api<{ recent_deliveries: { status: string }[] }>(
    "GET",
    `/api/v1/projects/${projId}/alerts`,
    financeReg.data.access_token,
  );
  expect(alertsState.data.recent_deliveries.length).toBe(1);

  const replayEventId = (await conDb`SELECT event_id FROM public.usage_events LIMIT 1`)[0]!
    .event_id as string;
  const replayRes = await api<{ ingested: number }>(
    "POST",
    "/internal/outbox/events",
    OUTBOX_TOKEN,
    {
      events: [
        { event_id: replayEventId, kind: "CHARGE_SETTLED", project_id: projId, payload: {} },
      ],
    },
  );
  expect(replayRes.status).toBe(200);
  expect(replayRes.data.ingested).toBe(0);

  const projTasks = await api<{ items: { id: string; status: string }[] }>(
    "GET",
    `/api/v1/projects/${projId}/tasks`,
    devReg.data.access_token,
  );
  expect(projTasks.status).toBe(200);
  expect(projTasks.data.items[0]!.id).toBe(vidBody.gateway_task_id);
  expect(projTasks.data.items[0]!.status).toBe("SUCCEEDED");

  const keySpend = await api<{ spent_atoms: string }>(
    "GET",
    `/api/v1/projects/${projId}/api-keys/${createdKey.data.id}/spend`,
    devReg.data.access_token,
  );
  expect(keySpend.data.spent_atoms).toBe("4600");

  const tenantQuotaEscalation = await api("PATCH", `/api/v1/projects/${projId}`, ownerToken, {
    rpm: 9999999,
  });
  expect(tenantQuotaEscalation.status).toBe(400);

  const adminQuotaUpdate = await api("PATCH", `/admin/v1/projects/${projId}`, adminToken, {
    rpm: 8000,
  });
  expect(adminQuotaUpdate.status).toBe(200);

  const badCidrRes = await api(
    "POST",
    `/api/v1/projects/${projId}/api-keys`,
    devReg.data.access_token,
    { name: "bad-cidr", allowed_cidrs: ["not-an-ip/99"] },
  );
  expect(badCidrRes.status).toBe(400);

  const chargesRes = await api<{ items: Record<string, unknown>[] }>(
    "GET",
    `/api/v1/projects/${projId}/charges`,
    financeReg.data.access_token,
  );
  expect(chargesRes.status).toBe(200);
  expect(chargesRes.data.items.length).toBeGreaterThanOrEqual(2);
  expect(chargesRes.data.items[0]!.supplierCostAtoms).toBeUndefined();
  expect(chargesRes.data.items[0]!.supplier_cost_atoms).toBeUndefined();
  expect(chargesRes.data.items[0]!.costVersionId).toBeUndefined();
  expect(chargesRes.data.items[0]!.channelId).toBeUndefined();

  const devResetDenied = await api(
    "POST",
    `/api/v1/projects/${projId}/api-keys/${createdKey.data.id}/reset-spend`,
    devReg.data.access_token,
  );
  expect(devResetDenied.status).toBe(403);

  const devRotateLimitedDenied = await api(
    "POST",
    `/api/v1/projects/${projId}/api-keys/${createdKey.data.id}/rotate`,
    devReg.data.access_token,
  );
  expect(devRotateLimitedDenied.status).toBe(403);

  const firstChargeId = String(chargesRes.data.items[0]!.id);
  const refundRes = await api(
    "POST",
    `/admin/v1/finance/charges/${firstChargeId}/refunds`,
    adminToken,
    {
      external_ref: "console-e2e-refund-1",
      amount_atoms: "600",
      reason: "Goodwill partial refund",
    },
  );
  expect(refundRes.status).toBe(200);
  const refundTick = await worker.runOnce(false);
  expect(refundTick.delivered_outbox_events).toBeGreaterThanOrEqual(1);

  const summaryAfterRefund = await api<{ request_count: number; customer_atoms: string }>(
    "GET",
    `/api/v1/projects/${projId}/usage/summary`,
    financeReg.data.access_token,
  );
  expect(summaryAfterRefund.data.request_count).toBe(2);
  expect(summaryAfterRefund.data.customer_atoms).toBe("4000");

  const rotated = await api<{ id: string; key: string }>(
    "POST",
    `/api/v1/projects/${projId}/api-keys/${createdKey.data.id}/rotate`,
    ownerToken,
  );
  expect(rotated.status).toBe(201);
  expect(rotated.data.key).not.toBe(createdKey.data.key);

  const concurrentRefresh = await Promise.all([
    api("POST", "/api/v1/auth/refresh", undefined, { refresh_token: refreshed.data.refresh_token }),
    api("POST", "/api/v1/auth/refresh", undefined, { refresh_token: refreshed.data.refresh_token }),
  ]);
  expect(concurrentRefresh.map((r) => r.status).sort()).toEqual([200, 401]);

  const demoteLastAdmin = await api(
    "PATCH",
    `/admin/v1/users/${adminReg.data.user.id}`,
    adminToken,
    { is_platform_admin: false },
  );
  expect(demoteLastAdmin.status).toBe(400);

  const oldKeyResp = await fetch(`${gatewayPublicUrl}/v1/chat/completions`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${createdKey.data.key}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ model: "acme/gpt-4o", messages: [{ role: "user", content: "old" }] }),
  });
  expect(oldKeyResp.status).toBe(401);

  const newKeyResp = await fetch(`${gatewayPublicUrl}/v1/chat/completions`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${rotated.data.key}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "acme/gpt-4o",
      max_tokens: 100,
      messages: [{ role: "user", content: "new" }],
    }),
  });
  expect(newKeyResp.status).toBe(200);

  const conservation = await api<{ ok: boolean }>(
    "GET",
    "/admin/v1/finance/conservation",
    adminToken,
  );
  expect(conservation.status).toBe(200);
  expect(conservation.data.ok).toBe(true);

  const auditLogs = await api<{ items: unknown[] }>("GET", "/admin/v1/audit-logs", adminToken);
  expect(auditLogs.status).toBe(200);
  expect(auditLogs.data.items.length).toBeGreaterThanOrEqual(8);
});
