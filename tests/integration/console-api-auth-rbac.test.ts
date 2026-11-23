import { afterAll, beforeAll, expect, test } from "bun:test";
import { createConsoleTestHarness } from "./helpers/console-harness.ts";

let h: Awaited<ReturnType<typeof createConsoleTestHarness>>;
let adminAccess = "";
let adminRefresh = "";
let adminUserId = "";

beforeAll(async () => {
  h = await createConsoleTestHarness({
    consoleConfigOverrides: {
      clientCorsOrigins: ["https://client.test"],
      adminCorsOrigins: ["https://admin.internal"],
    },
    enableWorker: false,
  });
});

afterAll(async () => {
  await h?.close();
});

test("dual listeners isolate routes, CORS origins, bootstrap reservation, and session scopes", async () => {
  const clientAdminProbe = await h.clientApi("GET", "/admin/v1/users");
  expect(clientAdminProbe.status).toBe(404);
  const clientOutboxProbe = await h.clientApi("POST", "/internal/outbox/events", h.outboxToken, {
    events: [],
  });
  expect(clientOutboxProbe.status).toBe(404);
  const adminClientProbe = await h.adminApi("GET", "/api/v1/models");
  expect(adminClientProbe.status).toBe(404);

  const clientCorsOk = await h.clientApi("OPTIONS", "/api/v1/models", undefined, undefined, {
    origin: "https://client.test",
    "access-control-request-method": "GET",
  });
  expect(clientCorsOk.headers.get("access-control-allow-origin")).toBe("https://client.test");
  const clientCorsBlocked = await h.clientApi("OPTIONS", "/api/v1/models", undefined, undefined, {
    origin: "https://admin.internal",
    "access-control-request-method": "GET",
  });
  expect(clientCorsBlocked.headers.get("access-control-allow-origin")).toBeNull();

  const adminCorsOk = await h.adminApi("OPTIONS", "/admin/v1/users", undefined, undefined, {
    origin: "https://admin.internal",
    "access-control-request-method": "GET",
  });
  expect(adminCorsOk.headers.get("access-control-allow-origin")).toBe("https://admin.internal");
  const adminCorsBlocked = await h.adminApi("OPTIONS", "/admin/v1/users", undefined, undefined, {
    origin: "https://client.test",
    "access-control-request-method": "GET",
  });
  expect(adminCorsBlocked.headers.get("access-control-allow-origin")).toBeNull();

  const squatRes = await h.clientApi("POST", "/api/v1/auth/register", undefined, {
    email: h.bootstrapAdminEmail,
    password: "SquatterPassword123!",
    display_name: "Squatter",
  });
  expect(squatRes.status).toBe(403);

  const badBootstrap = await h.adminApi("POST", "/admin/v1/auth/bootstrap", undefined, {
    email: h.bootstrapAdminEmail,
    password: "AdminPassword123!",
    display_name: "Root",
    bootstrap_token: "wrong-bootstrap-token",
  });
  expect(badBootstrap.status).toBe(403);

  const [b1, b2] = await Promise.all([
    h.adminApi<{ access_token: string; refresh_token: string; user: { id: string } }>(
      "POST",
      "/admin/v1/auth/bootstrap",
      undefined,
      {
        email: h.bootstrapAdminEmail,
        password: "AdminPassword123!",
        display_name: "Root",
        bootstrap_token: h.bootstrapAdminToken,
      },
    ),
    h.adminApi<{ access_token: string; refresh_token: string; user: { id: string } }>(
      "POST",
      "/admin/v1/auth/bootstrap",
      undefined,
      {
        email: h.bootstrapAdminEmail,
        password: "AdminPassword123!",
        display_name: "Root",
        bootstrap_token: h.bootstrapAdminToken,
      },
    ),
  ]);
  expect([b1.status, b2.status].sort()).toEqual([201, 409]);
  const bootWin = b1.status === 201 ? b1.data : b2.data;
  adminAccess = bootWin.access_token;
  adminRefresh = bootWin.refresh_token;
  adminUserId = bootWin.user.id;

  const adminPublicLogin = await h.clientApi("POST", "/api/v1/auth/login", undefined, {
    email: h.bootstrapAdminEmail,
    password: "AdminPassword123!",
  });
  expect(adminPublicLogin.status).toBe(403);

  const adminOnClient = await h.clientApi("GET", "/api/v1/auth/me", adminAccess);
  expect(adminOnClient.status).toBe(401);

  const user = await h.registerUser("scope-check@example.com");
  const clientOnAdmin = await h.adminApi("GET", "/admin/v1/auth/me", user.accessToken);
  expect(clientOnAdmin.status).toBe(401);
  const clientOnAdminUsers = await h.adminApi("GET", "/admin/v1/users", user.accessToken);
  expect(clientOnAdminUsers.status).toBe(401);
});

test("session refresh concurrency, password change revocation, brute-force IP guard, and user/org suspension work end-to-end", async () => {
  const victim = await h.registerUser("session-victim@example.com", "OldPassword123!");
  const login2 = await h.clientApi<{ access_token: string; refresh_token: string }>(
    "POST",
    "/api/v1/auth/login",
    undefined,
    { email: "session-victim@example.com", password: "OldPassword123!" },
  );
  expect(login2.status).toBe(200);

  const [r1, r2] = await Promise.all([
    h.clientApi<{ access_token: string; refresh_token: string }>(
      "POST",
      "/api/v1/auth/refresh",
      undefined,
      { refresh_token: victim.refreshToken },
    ),
    h.clientApi<{ access_token: string; refresh_token: string }>(
      "POST",
      "/api/v1/auth/refresh",
      undefined,
      { refresh_token: victim.refreshToken },
    ),
  ]);
  expect([r1.status, r2.status].sort()).toEqual([200, 401]);
  const activeToken = r1.status === 200 ? r1.data.access_token : r2.data.access_token;

  const chg = await h.clientApi("POST", "/api/v1/auth/change-password", activeToken, {
    current_password: "OldPassword123!",
    new_password: "NewPassword456!",
  });
  expect(chg.status).toBe(200);
  expect((await h.clientApi("GET", "/api/v1/auth/me", login2.data.access_token)).status).toBe(401);
  expect((await h.clientApi("GET", "/api/v1/auth/me", activeToken)).status).toBe(200);

  expect((await h.clientApi("POST", "/api/v1/auth/logout", activeToken)).status).toBe(200);
  expect((await h.clientApi("GET", "/api/v1/auth/me", activeToken)).status).toBe(401);

  const demoteSelf = await h.adminApi("PATCH", `/admin/v1/users/${adminUserId}`, adminAccess, {
    is_platform_admin: false,
  });
  expect(demoteSelf.status).toBe(400);

  const secondAdmin = await h.registerUser("second-admin@example.com");
  const promote = await h.adminApi("PATCH", `/admin/v1/users/${secondAdmin.user.id}`, adminAccess, {
    is_platform_admin: true,
  });
  expect(promote.status).toBe(200);
  const secondAdminLogin = await h.adminApi<{ access_token: string }>(
    "POST",
    "/admin/v1/auth/login",
    undefined,
    { email: "second-admin@example.com", password: "UserPassword123!" },
  );
  expect(secondAdminLogin.status).toBe(200);
  const demoteSecond = await h.adminApi(
    "PATCH",
    `/admin/v1/users/${secondAdmin.user.id}`,
    adminAccess,
    { is_platform_admin: false },
  );
  expect(demoteSecond.status).toBe(200);
  expect(
    (await h.adminApi("GET", "/admin/v1/users", secondAdminLogin.data.access_token)).status,
  ).toBe(401);

  const suspendUser = await h.adminApi(
    "PATCH",
    `/admin/v1/users/${secondAdmin.user.id}`,
    adminAccess,
    { status: "SUSPENDED" },
  );
  expect(suspendUser.status).toBe(200);
  expect((await h.clientApi("GET", "/api/v1/auth/me", secondAdmin.accessToken)).status).toBe(401);

  const adminRefreshed = await h.adminApi<{ access_token: string; refresh_token: string }>(
    "POST",
    "/admin/v1/auth/refresh",
    undefined,
    { refresh_token: adminRefresh },
  );
  expect(adminRefreshed.status).toBe(200);
  adminAccess = adminRefreshed.data.access_token;
  adminRefresh = adminRefreshed.data.refresh_token;

  const orgOwner = await h.registerUser("org-suspend@example.com");
  const suspendOrg = await h.adminApi(
    "PATCH",
    `/admin/v1/orgs/${orgOwner.defaultOrgId}`,
    adminAccess,
    { status: "SUSPENDED" },
  );
  expect(suspendOrg.status).toBe(200);
  const deniedOrg = await h.clientApi(
    "GET",
    `/api/v1/orgs/${orgOwner.defaultOrgId}/projects`,
    orgOwner.accessToken,
  );
  expect(deniedOrg.status).toBe(403);

  for (let i = 0; i < 10; i++) {
    const fail = await h.clientApi(
      "POST",
      "/api/v1/auth/login",
      undefined,
      { email: "brute@example.com", password: "WrongPassword1!" },
      { "x-forwarded-for": `203.0.113.${i + 1}` },
    );
    expect(fail.status).toBe(401);
  }
  const blocked = await h.clientApi(
    "POST",
    "/api/v1/auth/login",
    undefined,
    { email: "brute-other@example.com", password: "WrongPassword1!" },
    { "x-forwarded-for": "198.51.100.99" },
  );
  expect(blocked.status).toBe(429);
});

test("5-role RBAC, invitations, cross-tenant IDOR, DEVELOPER key limits, and allowed_cidrs IP enforcement work end-to-end", async () => {
  const acc = await h.adminApi<{ id: string }>("POST", "/admin/v1/upstream-accounts", adminAccess, {
    name: "rbac-acc",
  });
  expect(acc.status).toBe(201);
  await h.adminApi("POST", `/admin/v1/upstream-accounts/${acc.data.id}/recharges`, adminAccess, {
    currency: "USD",
    external_ref: "rbac-sup-1",
    amount_atoms: "1000000",
  });
  const ch = await h.adminApi<{ id: string }>("POST", "/admin/v1/channels", adminAccess, {
    account_id: acc.data.id,
    name: "rbac-ch",
    kind: "openai",
    base_url: h.mockUrl,
    api_key: "sk-rbac-upstream",
  });
  expect(ch.status).toBe(201);
  const cp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
    "POST",
    "/admin/v1/pricing-templates",
    adminAccess,
    {
      name: "rbac-cust",
      side: "CUSTOMER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "10", denominator: "1" },
        output_tokens: { numerator: "20", denominator: "1" },
      },
      ceilings: { input_tokens: "150", output_tokens: "100" },
    },
  );
  expect(cp.status).toBe(201);
  const sp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
    "POST",
    "/admin/v1/pricing-templates",
    adminAccess,
    {
      name: "rbac-sup",
      side: "SUPPLIER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "2", denominator: "1" },
      },
      ceilings: { input_tokens: "150", output_tokens: "100" },
    },
  );
  expect(sp.status).toBe(201);
  const model = await h.adminApi("POST", "/admin/v1/models", adminAccess, {
    name: "rbac-gpt",
    display_name: "RBAC GPT",
    operations: ["chat"],
    max_output_tokens: 100,
    list_price_template_id: cp.data.id,
  });
  expect(model.status).toBe(201);
  const route = await h.adminApi("POST", "/admin/v1/routes", adminAccess, {
    model_name: "rbac-gpt",
    channel_id: ch.data.id,
    upstream_model: "rbac-gpt-up",
    operations: ["chat"],
    billing_price_id: cp.data.currentPriceVersionId,
    cost_version_id: sp.data.currentPriceVersionId,
  });
  expect(route.status).toBe(201);

  const owner = await h.registerUser("team-owner@example.com");
  const dev = await h.registerUser("team-dev@example.com");
  const finance = await h.registerUser("team-fin@example.com");
  const viewer = await h.registerUser("team-view@example.com");
  const outsider = await h.registerUser("outsider@example.com");

  const orgRes = await h.clientApi<{ id: string }>("POST", "/api/v1/orgs", owner.accessToken, {
    name: "Acme Enterprise",
    slug: "acme-enterprise",
  });
  expect(orgRes.status).toBe(201);
  const orgId = orgRes.data.id;

  const projRes = await h.clientApi<{ id: string }>(
    "POST",
    `/api/v1/orgs/${orgId}/projects`,
    owner.accessToken,
    {
      name: "Prod API",
      currency: "USD",
      developer_key_spend_limit_atoms: "5000",
      developer_max_active_keys: 2,
    },
  );
  expect(projRes.status).toBe(201);
  const projectId = projRes.data.id;

  await h.adminApi("POST", `/admin/v1/finance/projects/${projectId}/topups`, adminAccess, {
    external_ref: "rbac-topup-1",
    amount_atoms: "100000",
  });

  const invite = async (email: string, role: string) => {
    const r = await h.clientApi<{ id: string; token: string }>(
      "POST",
      `/api/v1/orgs/${orgId}/invitations`,
      owner.accessToken,
      { email, role },
    );
    expect(r.status).toBe(201);
    return r.data;
  };

  const devInv = await invite("team-dev@example.com", "DEVELOPER");
  const finInv = await invite("team-fin@example.com", "FINANCE");
  const viewInv = await invite("team-view@example.com", "VIEWER");
  const revokedInv = await invite("temp-revoke@example.com", "VIEWER");
  const expiredInv = await invite("temp-expired@example.com", "VIEWER");

  const listInv = await h.clientApi<{ items: { id: string }[] }>(
    "GET",
    `/api/v1/orgs/${orgId}/invitations`,
    owner.accessToken,
  );
  expect(listInv.status).toBe(200);
  expect(listInv.data.items.length).toBe(5);

  expect(
    (
      await h.clientApi(
        "DELETE",
        `/api/v1/orgs/${orgId}/invitations/${revokedInv.id}`,
        owner.accessToken,
      )
    ).status,
  ).toBe(200);

  await h.conDb`
    UPDATE public.org_invitations
    SET expires_at = now() - interval '1 minute'
    WHERE id = ${expiredInv.id}::uuid
  `;
  const expiredUser = await h.registerUser("temp-expired@example.com");
  const expAccept = await h.clientApi(
    "POST",
    `/api/v1/invitations/${expiredInv.token}/accept`,
    expiredUser.accessToken,
  );
  expect(expAccept.status).toBe(410);
  const expRow = await h.conDb<{ status: string }[]>`
    SELECT status FROM public.org_invitations WHERE id = ${expiredInv.id}::uuid
  `;
  expect(expRow[0]?.status).toBe("EXPIRED");

  const [devAccept1, devAccept2] = await Promise.all([
    h.clientApi("POST", `/api/v1/invitations/${devInv.token}/accept`, dev.accessToken),
    h.clientApi("POST", `/api/v1/invitations/${devInv.token}/accept`, dev.accessToken),
  ]);
  expect([devAccept1.status, devAccept2.status].sort()).toEqual([200, 404]);
  expect(
    (await h.clientApi("POST", `/api/v1/invitations/${finInv.token}/accept`, finance.accessToken))
      .status,
  ).toBe(200);
  expect(
    (await h.clientApi("POST", `/api/v1/invitations/${viewInv.token}/accept`, viewer.accessToken))
      .status,
  ).toBe(200);

  expect(
    (await h.clientApi("GET", `/api/v1/orgs/${orgId}/projects`, outsider.accessToken)).status,
  ).toBe(404);
  expect(
    (await h.clientApi("GET", `/api/v1/projects/${projectId}/wallet`, outsider.accessToken)).status,
  ).toBe(404);
  expect(
    (
      await h.clientApi("POST", `/api/v1/projects/${projectId}/api-keys`, viewer.accessToken, {
        name: "viewer-nope",
      })
    ).status,
  ).toBe(403);
  expect(
    (
      await h.clientApi("POST", `/api/v1/projects/${projectId}/api-keys`, finance.accessToken, {
        name: "finance-nope",
      })
    ).status,
  ).toBe(403);
  expect(
    (await h.clientApi("GET", `/api/v1/projects/${projectId}/wallet`, finance.accessToken)).status,
  ).toBe(200);

  const ownerKey = await h.clientApi<{ id: string }>(
    "POST",
    `/api/v1/projects/${projectId}/api-keys`,
    owner.accessToken,
    { name: "owner-master", role: "INVOKER" },
  );
  expect(ownerKey.status).toBe(201);

  expect(
    (
      await h.clientApi("POST", `/api/v1/projects/${projectId}/api-keys`, dev.accessToken, {
        name: "dev-admin-attempt",
        role: "ADMIN",
        spend_limit_atoms: "1000",
      })
    ).status,
  ).toBe(403);

  expect(
    (
      await h.clientApi("POST", `/api/v1/projects/${projectId}/api-keys`, dev.accessToken, {
        name: "dev-unbounded-attempt",
        role: "INVOKER",
        spend_limit_atoms: null,
      })
    ).status,
  ).toBe(403);

  expect(
    (
      await h.clientApi("POST", `/api/v1/projects/${projectId}/api-keys`, dev.accessToken, {
        name: "dev-overcap-attempt",
        role: "INVOKER",
        spend_limit_atoms: "6000",
      })
    ).status,
  ).toBe(403);

  const devKey1 = await h.clientApi<{ id: string; key: string; allowedCidrs: string[] }>(
    "POST",
    `/api/v1/projects/${projectId}/api-keys`,
    dev.accessToken,
    {
      name: "dev-ip-key-1",
      role: "INVOKER",
      spend_limit_atoms: "4000",
      allowed_cidrs: [" 198.51.100.0/24 "],
    },
  );
  expect(devKey1.status).toBe(201);
  expect(devKey1.data.allowedCidrs).toEqual(["198.51.100.0/24"]);

  const blockedInference = await fetch(`${h.gatewayPublicUrl}/v1/chat/completions`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${devKey1.data.key}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "rbac-gpt",
      messages: [{ role: "user", content: "ip block test" }],
      max_tokens: 50,
    }),
  });
  expect(blockedInference.status).toBe(403);

  const patchIp = await h.clientApi<{ allowedCidrs: string[] }>(
    "PATCH",
    `/api/v1/projects/${projectId}/api-keys/${devKey1.data.id}`,
    dev.accessToken,
    { allowed_cidrs: [" 127.0.0.1 "] },
  );
  expect(patchIp.status).toBe(200);
  expect(patchIp.data.allowedCidrs).toEqual(["127.0.0.1/32"]);

  const allowedInference = await fetch(`${h.gatewayPublicUrl}/v1/chat/completions`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${devKey1.data.key}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "rbac-gpt",
      messages: [{ role: "user", content: "ip allow test" }],
      max_tokens: 50,
    }),
  });
  expect(allowedInference.status).toBe(200);

  expect(
    (
      await h.clientApi(
        "PATCH",
        `/api/v1/projects/${projectId}/api-keys/${devKey1.data.id}`,
        dev.accessToken,
        { spend_limit_atoms: "5000" },
      )
    ).status,
  ).toBe(403);

  expect(
    (
      await h.clientApi(
        "DELETE",
        `/api/v1/projects/${projectId}/api-keys/${devKey1.data.id}`,
        dev.accessToken,
      )
    ).status,
  ).toBe(403);

  expect(
    (
      await h.clientApi(
        "POST",
        `/api/v1/projects/${projectId}/api-keys/${devKey1.data.id}/rotate`,
        dev.accessToken,
      )
    ).status,
  ).toBe(403);

  expect(
    (
      await h.clientApi(
        "PATCH",
        `/api/v1/projects/${projectId}/api-keys/${ownerKey.data.id}`,
        dev.accessToken,
        { enabled: false },
      )
    ).status,
  ).toBe(403);

  const devKey2 = await h.clientApi<{ spendLimitAtoms: string }>(
    "POST",
    `/api/v1/projects/${projectId}/api-keys`,
    dev.accessToken,
    {
      name: "dev-key-2-auto-capped",
      role: "VIEWER",
    },
  );
  expect(devKey2.status).toBe(201);
  expect(devKey2.data.spendLimitAtoms).toBe("5000");

  const devKey3 = await h.clientApi(
    "POST",
    `/api/v1/projects/${projectId}/api-keys`,
    dev.accessToken,
    {
      name: "dev-key-3-over-limit",
      role: "VIEWER",
      spend_limit_atoms: "1000",
    },
  );
  expect(devKey3.status).toBe(403);

  expect(
    (
      await h.clientApi(
        "PATCH",
        `/api/v1/projects/${projectId}/api-keys/${devKey1.data.id}`,
        dev.accessToken,
        { enabled: false },
      )
    ).status,
  ).toBe(200);
});
