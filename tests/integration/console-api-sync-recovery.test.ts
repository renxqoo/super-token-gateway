import { localConsoleTokenEnvironment } from "../../scripts/local-internal-auth.ts";
import { SQL } from "bun";
import { expect, test } from "bun:test";
import {
  CONSOLE_SCHEMA_VERSION,
  assertConsoleSchemaVersion,
  createConsoleDatabase,
  createConsoleRuntime,
  migrateConsoleDb,
} from "@gateway/console-api";
import { h, billingAccountId, customerAccess, adminAccess } from "./console-sync-fixture.ts";
import { registerSyncFixture } from "./console-sync-fixture.ts";
import { registerCatalogRecoveryCases } from "./console-catalog-recovery-cases.ts";

registerSyncFixture();

test("adaptive model settings reconcile after failure and preserve an explicit disable until reset", async () => {
  const name = "adaptive-sync-gpt";
  const policy = {
    operations: ["chat"],
    mode: "observe",
    execution_policy: "protected",
    first_content_target_ms: 1200,
    supplier_reserve_limit: { currency: "USD", atoms: "300" },
  };
  const model = { name, display_name: "Adaptive sync", operations: ["chat"] };
  const read = async () => {
    const [result] = await h.gwDb`
      SELECT adaptive_routing AS policy, source_revision AS revision FROM public.models WHERE name = ${name}
    `;
    return result;
  };
  const put = (adaptive?: object | false | null) =>
    h.adminApi("POST", "/admin/v1/models", adminAccess, {
      ...model,
      ...(adaptive !== undefined ? { adaptive_routing: adaptive } : {}),
    });
  const { execution_policy: _, ...incomplete } = policy;
  const rejected = await put(incomplete);
  expect(rejected.status).toBe(400);
  expect(rejected.data.error).toMatchObject({ code: "validation_error" });
  expect(await read()).toBeUndefined();
  expect((await put(policy)).status).toBe(201);
  expect(await read()).toEqual({ policy, revision: 1 });
  expect((await put()).status).toBe(200);
  expect(await read()).toEqual({ policy, revision: 2 });
  const active = { ...policy, mode: "active", execution_policy: "route" };
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method === "PUT" && new URL(url).pathname === "/internal/v1/models")
      return new Response("unavailable", { status: 503 });
    return forward();
  });
  try {
    expect((await put(active)).status).toBe(503);
    expect(await read()).toEqual({ policy, revision: 2 });
  } finally {
    h.setGatewayInterceptor(null);
  }
  expect(await h.consoleRuntime.sync.reconcilePending()).toMatchObject({ failed: 0 });
  expect(await read()).toEqual({ policy: active, revision: 3 });
  expect((await put(false)).status).toBe(200);
  expect(await read()).toEqual({ policy: false, revision: 4 });
  expect(
    await h.conDb`SELECT adaptive_routing AS policy FROM model_catalog WHERE name = ${name}`,
  ).toEqual([{ policy: false }]);
  expect((await put()).status).toBe(200);
  expect(await read()).toEqual({ policy: false, revision: 5 });
  expect((await put(null)).status).toBe(200);
  expect(await read()).toEqual({ policy: null, revision: 6 });
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

test("Console initializes an empty database and rejects an incompatible target", async () => {
  const databaseName = "stg_console_fresh_" + crypto.randomUUID().replaceAll("-", "");
  const url = new URL(h.conDbUrl);
  url.pathname = "/postgres";
  const manager = new SQL(url.toString(), { max: 1 });
  url.pathname = "/" + databaseName;
  const db = createConsoleDatabase(url.toString(), 2);
  try {
    await manager.unsafe("CREATE DATABASE " + databaseName);
    await db.unsafe("CREATE SCHEMA existing_data");
    await expect(migrateConsoleDb(db)).rejects.toThrow(/nonempty database/);
    expect(
      await db.unsafe("SELECT nspname FROM pg_namespace WHERE nspname = 'existing_data'"),
    ).toEqual([{ nspname: "existing_data" }]);
    await db.unsafe("DROP SCHEMA existing_data");
    expect(await migrateConsoleDb(db)).toBe(CONSOLE_SCHEMA_VERSION);
    expect(await migrateConsoleDb(db)).toBe(CONSOLE_SCHEMA_VERSION);
    expect(await assertConsoleSchemaVersion(db)).toBe(CONSOLE_SCHEMA_VERSION);
    await db`UPDATE public.console_schema_migrations SET checksum = 'tampered'
      WHERE version = ${CONSOLE_SCHEMA_VERSION}`;
    await expect(migrateConsoleDb(db)).rejects.toThrow(/checksum mismatch/);
  } finally {
    await db.close({ timeout: 0 });
    await manager.unsafe("DROP DATABASE IF EXISTS " + databaseName);
    await manager.close();
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
      ...localConsoleTokenEnvironment(h.adminToken),
      OUTBOX_WEBHOOK_TOKEN: h.outboxToken,
    },
  });
  const reader = child.stdout.getReader();
  let pending: Promise<unknown> | undefined;
  try {
    const { value } = await reader.read();
    const { client_port } = JSON.parse(new TextDecoder().decode(value)) as { client_port: number };
    pending = fetch(
      `http://127.0.0.1:${client_port}/api/v1/models/sync-gpt?billing_account_id=${billingAccountId}`,
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
    `/api/v1/billing-accounts/${billingAccountId}/api-keys`,
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
    `/api/v1/billing-accounts/${billingAccountId}/api-keys/${created.data.id}`,
    customerAccess,
    { expires_at: "2031-01-01T00:00:00.000Z" },
  );
  expect(updated.status).toBe(200);
  expect(updated.data.syncStatus).toBe("SYNCED");
  expect(updated.data.expiresAt).toBe("2031-01-01T00:00:00.000Z");

  const revoked = await h.clientApi(
    "DELETE",
    `/api/v1/billing-accounts/${billingAccountId}/api-keys/${created.data.id}`,
    customerAccess,
  );
  expect(revoked.status).toBe(200);
});

test("concurrent API key rotation issues one successor and keeps a disabled key disabled", async () => {
  const path = `/api/v1/billing-accounts/${billingAccountId}/api-keys`;
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
  const keys = `/api/v1/billing-accounts/${billingAccountId}/api-keys`;
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

registerCatalogRecoveryCases();
