import { SQL } from "bun";
import { afterAll, beforeAll, expect, test } from "bun:test";
import {
  CONSOLE_SCHEMA_VERSION,
  createConsoleDatabase,
  createConsoleRuntime,
  migrateConsoleDb,
} from "@gateway/console-api";
import consoleMigrations from "../../apps/console-api/src/db/migrations/manifest.json";
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

test("reconcile completes with a two-connection pool instead of waiting for its own connection", async () => {
  const db = createConsoleDatabase(h.conDbUrl, 2);
  const runtime = createConsoleRuntime({ config: h.consoleConfig, db });
  const id = crypto.randomUUID();
  await h.conDb`
    INSERT INTO public.upstream_accounts (id, name, sync_status)
    VALUES (${id}, 'Small pool recovery', 'PENDING')
  `;
  const work = runtime.sync.reconcilePending();
  try {
    const result = await Promise.race([work, Bun.sleep(1500).then(() => null)]);
    expect(result).toEqual({ synced: 1, failed: 0 });
    const rows = await h.gwDb`SELECT name FROM public.upstream_accounts WHERE id = ${id}`;
    expect(rows[0]?.name).toBe("Small pool recovery");
  } finally {
    await db.close({ timeout: 0 });
    await work.catch(() => undefined);
    await h.conDb`DELETE FROM public.upstream_accounts WHERE id = ${id}`;
  }
});

test("a read-only Console database remains live but is not ready for traffic", async () => {
  const db = new SQL(h.conDbUrl, { connection: { default_transaction_read_only: "on" } });
  try {
    const runtime = createConsoleRuntime({ config: h.consoleConfig, db });
    expect((await runtime.fetchClient(new Request("http://console.test/livez"))).status).toBe(200);
    expect((await runtime.fetchClient(new Request("http://console.test/readyz"))).status).toBe(503);
  } finally {
    await db.close();
  }
});

test("upgrading an existing Console database preserves queued alerts and catalog data", async () => {
  const databaseName = `stg_console_upgrade_${crypto.randomUUID().replaceAll("-", "")}`;
  const url = new URL(h.conDbUrl);
  url.pathname = "/postgres";
  const manager = new SQL(url.toString(), { max: 1 });
  url.pathname = `/${databaseName}`;
  const db = createConsoleDatabase(url.toString(), 2);
  try {
    await manager.unsafe(`CREATE DATABASE ${databaseName}`);
    const base = new URL("../../apps/console-api/src/db/", import.meta.url);
    await db.begin(async (tx) => {
      for (const migration of consoleMigrations.migrations.filter(
        (entry) => entry.version <= 2026112303,
      )) {
        await tx.unsafe(await Bun.file(new URL(migration.path, base)).text());
        await tx`INSERT INTO public.console_schema_migrations (version, checksum) VALUES (${migration.version}, ${migration.checksum})`;
      }
      await tx`INSERT INTO public.users (id, email, password_hash, display_name) VALUES (${projectId}, 'upgrade@example.com', 'fixture', 'Upgrade')`;
      await tx`INSERT INTO public.organizations (id, slug, name, kind, owner_user_id, status) VALUES (${projectId}, 'upgrade-org', 'Upgrade', 'PERSONAL', ${projectId}, 'SUSPENDED')`;
      await tx`INSERT INTO public.projects (id, gateway_project_id, org_id, name) VALUES (${projectId}, ${projectId}, ${projectId}, 'Upgrade project')`;
      await tx`INSERT INTO public.alert_eval_queue (project_id, latest_wallet_available_atoms) VALUES (${projectId}, 15)`;
      await tx`INSERT INTO public.upstream_accounts (id, name, state) VALUES (${accountId}, 'Upgrade account', 'DISABLED')`;
      await tx`INSERT INTO public.channels (id, account_id, name, kind, base_url, desired_revision, synced_revision, sync_status) VALUES (${channelId}, ${accountId}, 'Upgrade channel', 'openai', ${h.mockUrl}, 2, 1, 'FAILED')`;
    });
    expect(await migrateConsoleDb(db)).toBe(CONSOLE_SCHEMA_VERSION);
    expect(await migrateConsoleDb(db)).toBe(CONSOLE_SCHEMA_VERSION);
    const [organization] =
      await db`SELECT status, inference_revision, inference_synced_revision FROM public.organizations WHERE id = ${projectId}`;
    expect(organization).toEqual({
      status: "SUSPENDED",
      inference_revision: 1,
      inference_synced_revision: 0,
    });
    const [alert] =
      await db`SELECT project_id, next_attempt_at <= now() AS due FROM public.alert_eval_queue`;
    expect(alert).toEqual({ project_id: projectId, due: true });
    const [account] =
      await db`SELECT state, pending_state FROM public.upstream_accounts WHERE id = ${accountId}`;
    expect(account).toEqual({ state: "DISABLED", pending_state: null });
    const [channel] =
      await db`SELECT account_id, secret_sync_pending FROM public.channels WHERE id = ${channelId}`;
    expect(channel).toEqual({ account_id: accountId, secret_sync_pending: true });
  } finally {
    await db.close({ timeout: 0 });
    try {
      await manager.unsafe(`DROP DATABASE IF EXISTS ${databaseName}`);
    } finally {
      await manager.close();
    }
  }
});

test("Console SIGTERM finishes even while a slow Gateway request is in flight", async () => {
  const started = Promise.withResolvers<void>();
  const releaseGateway = Promise.withResolvers<Response>();
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      started.resolve();
      return releaseGateway.promise;
    },
  });
  const child = Bun.spawn([process.execPath, "--no-env-file", "apps/console-api/src/main.ts"], {
    stdout: "pipe",
    stderr: "inherit",
    env: {
      CONSOLE_DATABASE_URL: h.conDbUrl,
      CONSOLE_HOST: "127.0.0.1",
      CONSOLE_PORT: "0",
      CONSOLE_ADMIN_PORT: "0",
      CONSOLE_RECONCILE_INTERVAL_MS: "0",
      CONSOLE_ALERT_DELIVERY_INTERVAL_MS: "0",
      GATEWAY_INTERNAL_URL: upstream.url.toString(),
      GATEWAY_ADMIN_TOKEN: h.adminToken,
      OUTBOX_WEBHOOK_TOKEN: h.outboxToken,
    },
  });
  const reader = child.stdout.getReader();
  let pending: Promise<unknown> | undefined;
  try {
    const { value } = await reader.read();
    const { client_port } = JSON.parse(new TextDecoder().decode(value)) as { client_port: number };
    pending = fetch(
      `http://127.0.0.1:${client_port}/api/v1/models/sync-gpt?project_id=${projectId}`,
      {
        headers: { authorization: `Bearer ${customerAccess}` },
      },
    )
      .then((response) => response.body?.cancel())
      .catch(() => undefined);
    expect(await Promise.race([started.promise.then(() => true), pending.then(() => false)])).toBe(
      true,
    );
    child.kill("SIGTERM");
    expect(await Promise.race([child.exited, Bun.sleep(6500).then(() => null)])).toBe(0);
  } finally {
    releaseGateway.resolve(Response.json({ items: [] }));
    if (child.exitCode === null) child.kill("SIGKILL");
    await child.exited;
    await pending;
    await reader.cancel();
    await upstream.stop(true);
  }
});

test("reconcile retains a disabled account and retries an explicit state update after Gateway recovers", async () => {
  const blockAccountWrites = () =>
    h.setGatewayInterceptor(async (url, init, forward) => {
      if (init?.method === "PUT" && new URL(url).pathname === "/internal/v1/upstream-accounts") {
        return new Response("unavailable", { status: 503 });
      }
      return forward();
    });
  blockAccountWrites();
  try {
    const create = await h.adminApi("POST", "/admin/v1/upstream-accounts", adminAccess, {
      name: "Recover Disabled Account",
      state: "DISABLED",
    });
    expect(create.status).toBe(503);
  } finally {
    h.setGatewayInterceptor(null);
  }
  await h.consoleRuntime.sync.reconcilePending();
  const [remote] =
    await h.gwDb`SELECT id, state FROM public.upstream_accounts WHERE name = 'Recover Disabled Account'`;
  expect(remote?.state).toBe("DISABLED");

  blockAccountWrites();
  try {
    const patch = await h.adminApi(
      "PATCH",
      `/admin/v1/upstream-accounts/${remote!.id}`,
      adminAccess,
      {
        state: "ACTIVE",
        expected_state: "DISABLED",
      },
    );
    expect(patch.status).toBe(503);
  } finally {
    h.setGatewayInterceptor(null);
  }
  await h.consoleRuntime.sync.reconcilePending();
  const [restored] =
    await h.gwDb`SELECT state FROM public.upstream_accounts WHERE id = ${remote!.id}`;
  expect(restored?.state).toBe("ACTIVE");
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

test("concurrent API key rotation issues one successor and keeps a disabled key disabled", async () => {
  const path = `/api/v1/projects/${projectId}/api-keys`;
  const created = await h.clientApi<{ id: string }>("POST", path, customerAccess, {
    name: "Rotation once",
  });
  expect(created.status).toBe(201);
  const original = `${path}/${created.data.id}`;
  expect((await h.clientApi("PATCH", original, customerAccess, { enabled: false })).status).toBe(
    200,
  );
  const results = await Promise.all([
    h.clientApi("POST", `${original}/rotate`, customerAccess),
    h.clientApi("POST", `${original}/rotate`, customerAccess),
  ]);
  expect(results.map((r) => r.status).sort()).toEqual([201, 409]);
  const issued = results.find((r) => r.status === 201)!;
  expect(issued.data).toMatchObject({ enabled: false, rotated_from: created.data.id });
  const [key] =
    await h.gwDb`SELECT enabled FROM public.api_keys WHERE id = ${String(issued.data.id)}`;
  expect(key?.enabled).toBe(false);
});

test("a rejected key update preserves a concurrent revocation for reconciliation", async () => {
  const keys = `/api/v1/projects/${projectId}/api-keys`;
  const key = await h.clientApi<{ id: string }>("POST", keys, customerAccess, {
    name: "Revocation recovery",
  });
  expect(key.status).toBe(201);
  const path = `${keys}/${key.data.id}`;
  const patchStarted = Promise.withResolvers<void>();
  const releasePatch = Promise.withResolvers<Response>();
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method === "PUT" && new URL(url).pathname === "/internal/v1/api-keys") {
      const body = JSON.parse(String(init.body)) as { id: string; enabled: boolean };
      if (body.id === key.data.id && body.enabled === false)
        return new Response("unavailable", { status: 503 });
      if (body.id === key.data.id) {
        patchStarted.resolve();
        return releasePatch.promise;
      }
    }
    return forward();
  });
  const update = h.clientApi("PATCH", path, customerAccess, { name: "Rejected rename" });
  let revoke: ReturnType<typeof h.clientApi> | undefined;
  try {
    await patchStarted.promise;
    revoke = h.clientApi("DELETE", path, customerAccess);
    let pendingRevision = 0;
    for (let check = 0; check < 100; check++) {
      const [row] =
        await h.conDb`SELECT desired_revision FROM public.api_key_records WHERE id = ${key.data.id} AND revoked_at IS NOT NULL`;
      if (row) {
        pendingRevision = row.desired_revision;
        break;
      }
      await Bun.sleep(10);
    }
    expect(pendingRevision).toBe(3);
    releasePatch.resolve(new Response("invalid", { status: 400 }));
    expect((await update).status).toBe(400);
    expect((await revoke).status).toBe(503);
    const [record] =
      await h.conDb`SELECT desired_revision, enabled FROM public.api_key_records WHERE id = ${key.data.id}`;
    expect(record).toEqual({ desired_revision: pendingRevision, enabled: false });
  } finally {
    releasePatch.resolve(new Response("invalid", { status: 400 }));
    await Promise.allSettled([update, revoke]);
    h.setGatewayInterceptor(null);
  }
  await h.consoleRuntime.sync.reconcilePending();
  const [remote] = await h.gwDb`SELECT enabled FROM public.api_keys WHERE id = ${key.data.id}`;
  expect(remote?.enabled).toBe(false);
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

  const rejecting = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method === "PUT" && new URL(url).pathname === "/internal/v1/routes") {
      const body = JSON.parse(String(init.body)) as { upstream_model: string };
      if (body.upstream_model === "rejected-upstream") {
        rejecting.resolve();
        await release.promise;
        return new Response("invalid", { status: 400 });
      }
    }
    return forward();
  });
  const path = `/admin/v1/routes/${route.data.id}`;
  const rejected = h.adminApi("PATCH", path, adminAccess, { upstream_model: "rejected-upstream" });
  let concurrent: ReturnType<typeof h.adminApi> | undefined;
  try {
    await rejecting.promise;
    concurrent = h.adminApi("PATCH", path, adminAccess, { weight: 19 });
    for (let check = 0; check < 15; check++) {
      const [current] = await h.conDb`SELECT weight FROM public.routes WHERE id = ${route.data.id}`;
      if (current?.weight === 19) break;
      await Bun.sleep(10);
    }
    release.resolve();
    expect((await rejected).status).toBe(400);
    expect((await concurrent).status).toBe(200);
    const [applied] =
      await h.gwDb`SELECT upstream_model, weight FROM public.routes WHERE id = ${route.data.id}`;
    expect(applied).toEqual({ upstream_model: "sync-gpt-v2", weight: 19 });
  } finally {
    release.resolve();
    await Promise.allSettled([rejected, concurrent]);
    h.setGatewayInterceptor(null);
  }
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

test("channel reconciliation does not report a failed secret update as synced without the credential", async () => {
  const path = `/admin/v1/channels/${channelId}`;
  const list = () =>
    h.adminApi<{ items: { id: string; keyHint: string; syncStatus: string }[] }>(
      "GET",
      "/admin/v1/channels",
      adminAccess,
    );
  const initial = (await list()).data.items.find((item) => item.id === channelId)!;
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method === "PUT" && new URL(url).pathname === "/internal/v1/channels") {
      return new Response("unavailable", { status: 503 });
    }
    return forward();
  });
  try {
    const failed = await h.adminApi("PATCH", path, adminAccess, {
      api_key: "sk-recovered-channel-credential-123",
    });
    expect(failed.status).toBe(503);
  } finally {
    h.setGatewayInterceptor(null);
  }
  await h.consoleRuntime.sync.reconcilePending();
  expect((await list()).data.items.find((item) => item.id === channelId)).toMatchObject({
    keyHint: initial.keyHint,
    syncStatus: "FAILED",
    secretSyncPending: true,
  });
  expect(
    (
      await h.adminApi("PATCH", path, adminAccess, {
        api_key: "sk-recovered-channel-credential-123",
      })
    ).data,
  ).toMatchObject({ syncStatus: "SYNCED", secretSyncPending: false });
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
