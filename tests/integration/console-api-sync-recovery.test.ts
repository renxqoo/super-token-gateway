import { afterAll, beforeAll, expect, test } from "bun:test";
import {
  createConsoleDatabase,
  createConsoleRuntime,
  migrateConsoleDb,
} from "@gateway/console-api";
import { createConsoleTestHarness } from "./helpers/console-harness.ts";

let h: Awaited<ReturnType<typeof createConsoleTestHarness>>;
let adminAccess = "";
let customerAccess = "";
let projectId = "";
let accountId = "";
let channelId = "";
let customerPriceId = "";
let supplierPriceId = "";

beforeAll(async () => {
  h = await createConsoleTestHarness({ enableWorker: false });
  const boot = await h.bootstrapAdmin();
  adminAccess = boot.accessToken;
  const user = await h.registerUser("sync-owner@example.com");
  customerAccess = user.accessToken;
  projectId = user.defaultProjectId;

  const topup = await h.adminApi(
    "POST",
    `/admin/v1/finance/projects/${projectId}/topups`,
    adminAccess,
    {
      external_ref: "sync-topup-1",
      amount_atoms: "500000",
    },
  );
  expect(topup.status).toBe(200);

  const acc = await h.adminApi<{ id: string }>("POST", "/admin/v1/upstream-accounts", adminAccess, {
    name: "sync-acc",
  });
  expect(acc.status).toBe(201);
  accountId = acc.data.id;
  await h.adminApi("POST", `/admin/v1/upstream-accounts/${accountId}/recharges`, adminAccess, {
    currency: "USD",
    external_ref: "sync-sup-1",
    amount_atoms: "500000",
  });

  const ch = await h.adminApi<{ id: string }>("POST", "/admin/v1/channels", adminAccess, {
    account_id: accountId,
    name: "sync-ch",
    kind: "openai",
    base_url: h.mockUrl,
    api_key: "sk-sync-initial",
  });
  expect(ch.status).toBe(201);
  channelId = ch.data.id;

  const cp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
    "POST",
    "/admin/v1/pricing-templates",
    adminAccess,
    {
      name: "sync-cust-price",
      side: "CUSTOMER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "10", denominator: "1" },
        output_tokens: { numerator: "20", denominator: "1" },
      },
      ceilings: { input_tokens: "10000", output_tokens: "1000" },
    },
  );
  expect(cp.status).toBe(201);
  customerPriceId = cp.data.currentPriceVersionId;

  const sp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
    "POST",
    "/admin/v1/pricing-templates",
    adminAccess,
    {
      name: "sync-sup-price",
      side: "SUPPLIER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "2", denominator: "1" },
      },
      ceilings: { input_tokens: "10000", output_tokens: "1000" },
    },
  );
  expect(sp.status).toBe(201);
  supplierPriceId = sp.data.currentPriceVersionId;

  const model = await h.adminApi("POST", "/admin/v1/models", adminAccess, {
    name: "sync-gpt",
    display_name: "Sync GPT",
    operations: ["chat"],
    max_output_tokens: 100,
    list_price_template_id: cp.data.id,
  });
  expect(model.status).toBe(201);
});

afterAll(async () => {
  await h?.close();
});

test("API key expires_at serializes UTC ISO cleanly across create, patch, and revoke", async () => {
  const created = await h.clientApi<{ id: string; expiresAt: string; syncStatus: string }>(
    "POST",
    `/api/v1/projects/${projectId}/api-keys`,
    customerAccess,
    {
      name: "expiring-key",
      expires_at: "2030-06-15T12:30:00+08:00",
    },
  );
  expect(created.status).toBe(201);
  expect(created.data.syncStatus).toBe("SYNCED");
  expect(created.data.expiresAt).toBe("2030-06-15T04:30:00.000Z");

  const updated = await h.clientApi<{ expiresAt: string; syncStatus: string }>(
    "PATCH",
    `/api/v1/projects/${projectId}/api-keys/${created.data.id}`,
    customerAccess,
    { expires_at: "2031-01-01T00:00:00.000Z" },
  );
  expect(updated.status).toBe(200);
  expect(updated.data.syncStatus).toBe("SYNCED");
  expect(updated.data.expiresAt).toBe("2031-01-01T00:00:00.000Z");

  const revoked = await h.clientApi(
    "DELETE",
    `/api/v1/projects/${projectId}/api-keys/${created.data.id}`,
    customerAccess,
  );
  expect(revoked.status).toBe(200);
});

test("route PATCH preserves unsubmitted fields in both Console and Gateway and rejects model_name mutation", async () => {
  const route = await h.adminApi<{
    id: string;
    priority: number;
    weight: number;
    enabled: boolean;
    retryPolicy: Record<string, unknown>;
  }>("POST", "/admin/v1/routes", adminAccess, {
    model_name: "sync-gpt",
    channel_id: channelId,
    upstream_model: "sync-gpt-v1",
    operations: ["chat"],
    priority: 17,
    weight: 7,
    billing_price_id: customerPriceId,
    cost_version_id: supplierPriceId,
    retry_policy: { max_attempts: 2 },
    enabled: false,
  });
  expect(route.status).toBe(201);

  const badPatch = await h.adminApi("PATCH", `/admin/v1/routes/${route.data.id}`, adminAccess, {
    model_name: "other-model",
  });
  expect(badPatch.status).toBe(400);

  const patched = await h.adminApi<{
    upstreamModel: string;
    priority: number;
    weight: number;
    enabled: boolean;
    retryPolicy: Record<string, unknown>;
    syncStatus: string;
  }>("PATCH", `/admin/v1/routes/${route.data.id}`, adminAccess, {
    upstream_model: "sync-gpt-v2",
  });
  expect(patched.status).toBe(200);
  expect(patched.data.syncStatus).toBe("SYNCED");
  expect(patched.data.upstreamModel).toBe("sync-gpt-v2");
  expect(patched.data.priority).toBe(17);
  expect(patched.data.weight).toBe(7);
  expect(patched.data.enabled).toBe(false);
  expect(patched.data.retryPolicy).toEqual({ max_attempts: 2 });

  const gwRoute = await h.gwDb<
    {
      upstream_model: string;
      priority: number;
      weight: number;
      enabled: boolean;
      retry_policy: Record<string, unknown>;
    }[]
  >`
    SELECT upstream_model, priority, weight, enabled, retry_policy
    FROM public.routes
    WHERE id = ${route.data.id}::uuid
  `;
  expect(gwRoute[0]).toEqual({
    upstream_model: "sync-gpt-v2",
    priority: 17,
    weight: 7,
    enabled: false,
    retry_policy: { max_attempts: 2 },
  });

  const enableRoute = await h.adminApi("PATCH", `/admin/v1/routes/${route.data.id}`, adminAccess, {
    enabled: true,
  });
  expect(enableRoute.status).toBe(200);
});

test("Gateway rejects stale source_revision writes without overwriting newer state", async () => {
  const upd = await h.clientApi("PATCH", `/api/v1/projects/${projectId}`, customerAccess, {
    name: "Fresh Project Name",
  });
  expect(upd.status).toBe(200);

  const gwRow = await h.gwDb<{ name: string; source_revision: number }[]>`
    SELECT name, source_revision FROM public.projects WHERE id = ${projectId}::uuid
  `;
  expect(gwRow[0]?.name).toBe("Fresh Project Name");
  const currentRev = gwRow[0]!.source_revision;
  expect(currentRev).toBeGreaterThanOrEqual(2);

  const staleReplay = await h.gatewayInternalApi("PUT", "/internal/v1/projects", {
    id: projectId,
    name: "Stale Overwrite Attempt",
    currency: "USD",
    rpm: 100,
    tpm: 100000,
    concurrency: 10,
    enabled: true,
    source_revision: currentRev - 1,
  });
  expect(staleReplay.status).toBe(409);

  const afterRow = await h.gwDb<{ name: string; source_revision: number }[]>`
    SELECT name, source_revision FROM public.projects WHERE id = ${projectId}::uuid
  `;
  expect(afterRow[0]?.name).toBe("Fresh Project Name");
  expect(afterRow[0]?.source_revision).toBe(currentRev);
});

test("pricing template recovers after lost Gateway commit response and isolates edited template with a new UUID", async () => {
  let dropOnce = true;
  h.setGatewayInterceptor(async (url, init, forward) => {
    const res = await forward();
    if (dropOnce && init?.method === "POST" && new URL(url).pathname === "/internal/v1/prices") {
      dropOnce = false;
      return new Response("gateway response dropped", { status: 503 });
    }
    return res;
  });

  const created = await h.adminApi("POST", "/admin/v1/pricing-templates", adminAccess, {
    name: "lost-response-tpl",
    side: "CUSTOMER",
    currency: "USD",
    rates: {
      input_tokens: { numerator: "15", denominator: "1" },
      output_tokens: { numerator: "30", denominator: "1" },
    },
    ceilings: { input_tokens: "10000", output_tokens: "1000" },
  });
  h.setGatewayInterceptor(null);

  expect(created.status).toBe(503);

  const beforeRecList = await h.adminApi<{
    items: {
      id: string;
      name: string;
      syncStatus: string;
      currentPriceVersionId: string | null;
      pendingPriceVersionId: string | null;
    }[];
  }>("GET", "/admin/v1/pricing-templates", adminAccess);
  const failedTpl = beforeRecList.data.items.find((t) => t.name === "lost-response-tpl");
  expect(failedTpl).toBeDefined();
  expect(failedTpl!.syncStatus).toBe("FAILED");
  expect(failedTpl!.currentPriceVersionId).toBeNull();
  const frozenUuid = failedTpl!.pendingPriceVersionId;
  expect(frozenUuid).toBeTruthy();

  const conflictDifferentRates = await h.gatewayInternalApi("POST", "/internal/v1/prices", {
    id: frozenUuid,
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    rates: {
      input_tokens: { numerator: "99", denominator: "1" },
      output_tokens: { numerator: "99", denominator: "1" },
    },
    ceilings: { input_tokens: "10000", output_tokens: "1000" },
  });
  expect(conflictDifferentRates.status).toBe(409);

  const rec = await h.adminApi("POST", "/admin/v1/sync/reconcile", adminAccess);
  expect(rec.status).toBe(200);

  const list = await h.adminApi<{
    items: { id: string; syncStatus: string; currentPriceVersionId: string | null }[];
  }>("GET", "/admin/v1/pricing-templates", adminAccess);
  const recovered = list.data.items.find((t) => t.id === failedTpl!.id);
  expect(recovered?.syncStatus).toBe("SYNCED");
  expect(recovered?.currentPriceVersionId).toBe(frozenUuid);

  const edited = await h.adminApi<{
    syncStatus: string;
    currentPriceVersionId: string;
  }>("PATCH", `/admin/v1/pricing-templates/${failedTpl!.id}`, adminAccess, {
    rates: {
      input_tokens: { numerator: "25", denominator: "1" },
      output_tokens: { numerator: "50", denominator: "1" },
    },
  });
  expect(edited.status).toBe(200);
  expect(edited.data.syncStatus).toBe("SYNCED");
  expect(edited.data.currentPriceVersionId).not.toBe(frozenUuid);
});

test("two-phase API key rotation keeps old key active on Gateway failure and revokes it only after new key succeeds", async () => {
  const k1 = await h.clientApi<{ id: string; key: string }>(
    "POST",
    `/api/v1/projects/${projectId}/api-keys`,
    customerAccess,
    { name: "rot-key-1" },
  );
  expect(k1.status).toBe(201);

  const callChat = (rawKey: string) =>
    fetch(`${h.gatewayPublicUrl}/v1/chat/completions`, {
      method: "POST",
      headers: { authorization: `Bearer ${rawKey}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "sync-gpt",
        messages: [{ role: "user", content: "rotate test" }],
        max_tokens: 50,
      }),
    });

  expect((await callChat(k1.data.key)).status).toBe(200);

  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method === "PUT" && new URL(url).pathname === "/internal/v1/api-keys") {
      return new Response("simulated gateway outage", { status: 503 });
    }
    return forward();
  });

  const failedRotate = await h.clientApi(
    "POST",
    `/api/v1/projects/${projectId}/api-keys/${k1.data.id}/rotate`,
    customerAccess,
  );
  expect(failedRotate.status).toBeGreaterThanOrEqual(500);
  h.setGatewayInterceptor(null);

  await h.consoleRuntime.sync.reconcilePending();
  expect((await callChat(k1.data.key)).status).toBe(200);

  const okRotate = await h.clientApi<{ id: string; key: string; rotatedFromKeyId: string }>(
    "POST",
    `/api/v1/projects/${projectId}/api-keys/${k1.data.id}/rotate`,
    customerAccess,
  );
  expect(okRotate.status).toBe(201);
  expect(okRotate.data.rotatedFromKeyId).toBe(k1.data.id);
  expect((await callChat(okRotate.data.key)).status).toBe(200);
  expect((await callChat(k1.data.key)).status).toBe(401);
});

test("upstream account RECONCILE_REQUIRED state survives ordinary rename and requires explicit expected_state to recover", async () => {
  await h.gwDb`
    UPDATE public.upstream_accounts
    SET state = 'RECONCILE_REQUIRED'
    WHERE id = ${accountId}::uuid
  `;

  const listed = await h.adminApi<{ items: { id: string; state: string }[] }>(
    "GET",
    "/admin/v1/upstream-accounts",
    adminAccess,
  );
  expect(listed.data.items.find((a) => a.id === accountId)?.state).toBe("RECONCILE_REQUIRED");

  const renamed = await h.adminApi<{ state: string }>(
    "PATCH",
    `/admin/v1/upstream-accounts/${accountId}`,
    adminAccess,
    { name: "sync-acc-renamed" },
  );
  expect(renamed.status).toBe(200);
  expect(renamed.data.state).toBe("RECONCILE_REQUIRED");

  const gwStateAfterRename = await h.gwDb<{ state: string; name: string }[]>`
    SELECT state, name FROM public.upstream_accounts WHERE id = ${accountId}::uuid
  `;
  expect(gwStateAfterRename[0]).toEqual({ state: "RECONCILE_REQUIRED", name: "sync-acc-renamed" });

  const blindReactivate = await h.adminApi(
    "PATCH",
    `/admin/v1/upstream-accounts/${accountId}`,
    adminAccess,
    { state: "ACTIVE" },
  );
  expect(blindReactivate.status).toBe(409);

  const explicitReactivate = await h.adminApi<{ state: string }>(
    "PATCH",
    `/admin/v1/upstream-accounts/${accountId}`,
    adminAccess,
    { state: "ACTIVE", expected_state: "RECONCILE_REQUIRED" },
  );
  expect(explicitReactivate.status).toBe(200);
  expect(explicitReactivate.data.state).toBe("ACTIVE");
});

test("plugin release channel binding stays consistent with Console channel listing and subsequent channel PATCH", async () => {
  const installed = await h.adminApi<{ id: string; config_epoch: number }>(
    "POST",
    "/admin/v1/plugins/builtins/minimax/install",
    adminAccess,
    {},
  );
  expect(installed.status).toBe(200);

  const videoCh = await h.adminApi<{ id: string }>("POST", "/admin/v1/channels", adminAccess, {
    account_id: accountId,
    name: "plugin-bound-ch",
    kind: "minimax",
    base_url: h.mockUrl,
    api_key: "sk-video-1",
  });
  expect(videoCh.status).toBe(201);

  const release = await h.adminApi("POST", "/admin/v1/plugin-releases", adminAccess, {
    id: crypto.randomUUID(),
    expected_epoch: h.gateway.snapshot.get().epoch,
    targets: [{ channel_id: videoCh.data.id, provider_plugin_id: installed.data.id }],
  });
  expect(release.status).toBe(200);

  const channels = await h.adminApi<{ items: { id: string; providerPluginId: string | null }[] }>(
    "GET",
    "/admin/v1/channels",
    adminAccess,
  );
  expect(channels.data.items.find((c) => c.id === videoCh.data.id)?.providerPluginId).toBe(
    installed.data.id,
  );

  const forbiddenBindingEdit = await h.adminApi(
    "PATCH",
    `/admin/v1/channels/${videoCh.data.id}`,
    adminAccess,
    { provider_plugin_id: null },
  );
  expect(forbiddenBindingEdit.status).toBe(400);

  const normalEdit = await h.adminApi<{ rpm: number; syncStatus: string }>(
    "PATCH",
    `/admin/v1/channels/${videoCh.data.id}`,
    adminAccess,
    { rpm: 777, api_key: "sk-video-rotated" },
  );
  expect(normalEdit.status).toBe(200);
  expect(normalEdit.data.rpm).toBe(777);
  expect(normalEdit.data.syncStatus).toBe("SYNCED");

  const gwChannel = await h.gwDb<{ provider_plugin_id: string | null; rpm: number }[]>`
    SELECT provider_plugin_id::text AS provider_plugin_id, rpm
    FROM public.channels
    WHERE id = ${videoCh.data.id}::uuid
  `;
  expect(gwChannel[0]).toEqual({ provider_plugin_id: installed.data.id, rpm: 777 });
});

test("background reconcile recovers failed syncs, runtime timers close cleanly, and migration refuses nonempty unversioned DB", async () => {
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method === "PUT" && new URL(url).pathname === "/internal/v1/projects") {
      return new Response("down", { status: 503 });
    }
    return forward();
  });

  const org = await h.clientApi<{ id: string }>("POST", "/api/v1/orgs", customerAccess, {
    name: "Recovery Org",
    slug: "recovery-org",
  });
  expect(org.status).toBe(201);
  const proj = await h.clientApi<{ id: string; syncStatus: string; gatewaySynced: boolean }>(
    "POST",
    `/api/v1/orgs/${org.data.id}/projects`,
    customerAccess,
    { name: "Delayed Sync Project", currency: "USD" },
  );
  expect(proj.status).toBe(201);
  expect(proj.data.syncStatus).toBe("PENDING");
  expect(proj.data.gatewaySynced).toBe(false);

  const failAttempt = await h.consoleRuntime.sync
    .ensureProjectSynced(proj.data.id)
    .catch(() => null);
  expect(failAttempt).toBeNull();
  h.setGatewayInterceptor(null);

  const failedRow = await h.conDb<{ sync_status: string; gateway_synced: boolean }[]>`
    SELECT sync_status, gateway_synced FROM public.projects WHERE id = ${proj.data.id}::uuid
  `;
  expect(failedRow[0]).toEqual({ sync_status: "FAILED", gateway_synced: false });

  const timerRuntime = createConsoleRuntime({
    config: {
      ...h.consoleConfig,
      reconcileIntervalMs: 50,
      alertDeliveryIntervalMs: 50,
    },
    db: h.conDb,
    gatewayFetch: (input, init) => fetch(input, init),
  });
  await timerRuntime.start();
  await Bun.sleep(150);
  await timerRuntime.close(2000);

  const gwProj = await h.gwDb<{ id: string }[]>`
    SELECT id::text AS id FROM public.projects WHERE id = ${proj.data.id}::uuid
  `;
  expect(gwProj.length).toBe(1);

  const rogueDb = createConsoleDatabase(h.gwDbUrl, 2);
  try {
    await expect(migrateConsoleDb(rogueDb)).rejects.toThrow(/nonempty/i);
  } finally {
    await rogueDb.close();
  }
});
