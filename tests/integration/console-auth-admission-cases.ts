import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { verifier } from "@gateway/runtime";
import { h, adminSession } from "./console-auth-fixture.ts";

export function registerAdmissionCases() {
  test("an expired invitation no longer blocks the organization from issuing a replacement", async () => {
    const owner = await h.registerUser("invite-resend-owner@example.com");
    const org = await h.clientApi<{ id: string }>("POST", "/api/v1/orgs", owner.accessToken, {
      name: "Invite Renewal",
      slug: "invite-renewal",
    });
    expect(org.status).toBe(201);
    const invite = () =>
      h.clientApi<{ id: string; token: string }>(
        "POST",
        `/api/v1/orgs/${org.data.id}/invitations`,
        owner.accessToken,
        { email: "temp-resend@example.com", role: "VIEWER" },
      );
    const original = await invite();
    expect(original.status).toBe(201);
    await h.conDb`UPDATE public.org_invitations SET expires_at = now() - interval '1 minute' WHERE id = ${original.data.id}`;
    const replacement = await invite();
    expect(replacement.status).toBe(201);
    const invited = await h.registerUser("temp-resend@example.com");
    expect(
      (
        await h.clientApi(
          "POST",
          `/api/v1/invitations/${original.data.token}/accept`,
          invited.accessToken,
        )
      ).status,
    ).toBe(404);
    expect(
      (
        await h.clientApi(
          "POST",
          `/api/v1/invitations/${replacement.data.token}/accept`,
          invited.accessToken,
        )
      ).status,
    ).toBe(200);
  });

  test("only sk_ billingAccount keys are accepted by authentication, import, and issuance", async () => {
    const owner = await h.registerUser("key-format-owner@example.com");
    const keyPath = `/api/v1/billing-accounts/${owner.defaultBillingAccountId}/api-keys`;
    const modelsStatus = async (key: string) => {
      const response = await fetch(`${h.gatewayPublicUrl}/v1/models`, {
        headers: { authorization: `Bearer ${key}` },
      });
      await response.body?.cancel();
      return response.status;
    };
    type IssuedKey = { id: string; key: string; keyPrefix: string };
    const created = await h.clientApi<IssuedKey>("POST", keyPath, owner.accessToken, {
      name: "Key to rotate",
    });
    expect(created.status).toBe(201);
    const keyFormat = /^sk_[A-Za-z0-9_-]{43}$/;
    expect(created.data.key).toMatch(keyFormat);
    expect(created.data.keyPrefix).toBe(created.data.key.slice(0, 12));
    expect(await modelsStatus(created.data.key)).toBe(200);

    const legacyKey = `gw_${"A".repeat(43)}`;
    const legacyId = crypto.randomUUID();
    await h.gwDb`
    INSERT INTO public.api_keys (id, billing_account_id, name, verifier, prefix)
    VALUES (${legacyId}, ${owner.defaultBillingAccountId}, 'Retired key',
      ${verifier(legacyKey, h.gatewayConfig.keyPepper)}, ${legacyKey.slice(0, 12)})
  `;
    const importedKey = `sk_${"B".repeat(42)}A`;
    const importKey = (key: string) =>
      h.gatewayInternalApi<{ key: string }>("PUT", "/internal/v1/api-keys", {
        billing_account_id: owner.defaultBillingAccountId,
        name: "Imported key",
        raw_key: key,
      });
    const imported = await importKey(importedKey);
    expect(imported.status).toBe(200);
    expect(imported.data.key).toBe(importedKey);
    expect(await modelsStatus(importedKey)).toBe(200);
    expect(await modelsStatus(legacyKey)).toBe(401);
    expect((await importKey(legacyKey)).status).toBe(400);
    expect((await importKey("sk_short")).status).toBe(400);
    expect((await importKey(`${importedKey}\n`)).status).toBe(400);

    const rotated = await h.clientApi<IssuedKey & { old_key_revoke_pending: boolean }>(
      "POST",
      `${keyPath}/${created.data.id}/rotate`,
      owner.accessToken,
    );
    expect(rotated.status).toBe(201);
    expect(rotated.data.key).toMatch(keyFormat);
    expect(rotated.data.keyPrefix).toBe(rotated.data.key.slice(0, 12));
    expect(rotated.data.old_key_revoke_pending).toBe(false);
    expect(await modelsStatus(rotated.data.key)).toBe(200);
    expect(await modelsStatus(created.data.key)).toBe(401);
  });

  test("symptom: uppercase UUID spelling rejects valid Key updates and allows self-rotation", async () => {
    const accountId = "abcedf00-0000-4000-8000-000000000001";
    const rootId = "abcedf00-0000-4000-8000-000000000002";
    const keyId = "abcedf00-0000-4000-8000-000000000003";
    const account = await h.gatewayInternalApi("PUT", "/internal/v1/billing-accounts", {
      id: accountId,
      name: "Key scope normalization",
    });
    expect(account.status).toBe(200);
    const create = await h.gatewayInternalApi("PUT", "/internal/v1/api-keys", {
      id: rootId,
      billing_account_id: accountId,
    });
    expect(create.status).toBe(200);
    const update = await h.gatewayInternalApi("PUT", "/internal/v1/api-keys", {
      id: rootId.toUpperCase(),
      billing_account_id: accountId.toUpperCase(),
      name: "Updated Key",
    });
    const selfRotation = await h.gatewayInternalApi("PUT", "/internal/v1/api-keys", {
      id: rootId.toUpperCase(),
      billing_account_id: accountId.toUpperCase(),
      rotated_from_key_id: rootId,
    });
    expect([update, selfRotation]).toMatchObject([
      { status: 200, data: { id: rootId, billing_account_id: accountId, name: "Updated Key" } },
      { status: 409 },
    ]);
    const rotation = await h.gatewayInternalApi("PUT", "/internal/v1/api-keys", {
      id: keyId.toUpperCase(),
      billing_account_id: accountId.toUpperCase(),
      rotated_from_key_id: rootId.toUpperCase(),
    });
    expect(rotation).toMatchObject({ status: 200, data: { id: keyId } });
    const [stored] = await h.gwDb.unsafe(
      "SELECT resource_scope_id FROM public.api_keys WHERE id = $1",
      [keyId],
    );
    expect(stored?.resource_scope_id).toBe(rootId);
  });

  test("organization admission retries an unavailable Gateway and applies to a replica with a stale snapshot", async () => {
    const user = await h.registerUser("admission-retry@example.com");
    const { defaultOrgId: orgId, defaultBillingAccountId: billingAccountId } = user;
    const key = await h.clientApi<{ key: string }>(
      "POST",
      `/api/v1/billing-accounts/${billingAccountId}/api-keys`,
      user.accessToken,
      { name: "Replica admission" },
    );
    expect(key.status).toBe(201);
    const replica = createGatewayRuntime({
      config: { ...h.gatewayConfig, snapshotPollMs: 0 },
      db: h.gwDb,
      wasmExecutor: null,
    });
    const listModels = () =>
      replica.fetchPublic(
        new Request("http://test/v1/models", {
          headers: { authorization: `Bearer ${key.data.key}` },
        }),
      );
    const setStatus = (status: "SUSPENDED" | "ACTIVE") =>
      h.adminApi("PATCH", `/admin/v1/orgs/${orgId.toUpperCase()}`, adminSession.access, { status });
    const pending = () => h.adminApi("GET", "/admin/v1/sync/organizations", adminSession.access);
    const allowedAtConsole = () =>
      h.clientApi("GET", `/api/v1/billing-accounts/${billingAccountId}`, user.accessToken);
    try {
      await replica.start();
      const epoch = replica.snapshot.get().epoch;
      expect((await listModels()).status).toBe(200);
      const admissionPath = `/internal/v1/console/organizations/${orgId}/admission`;
      h.setGatewayInterceptor(async (url, init, forward) =>
        init?.method === "PUT" && new URL(url).pathname === admissionPath
          ? new Response(null, { status: 503 })
          : forward(),
      );
      expect(await setStatus("SUSPENDED")).toMatchObject({
        status: 202,
        data: {
          status: "ACTIVE",
          requested_status: "SUSPENDED",
          inference_sync_pending: true,
          inference_revision: 1,
        },
      });
      expect((await listModels()).status).toBe(200);
      expect((await allowedAtConsole()).status).toBe(403);
      expect((await pending()).data).toEqual({ pending: 1 });
      h.setGatewayInterceptor(null);
      expect(
        (await h.adminApi("POST", "/admin/v1/sync/reconcile", adminSession.access)).data,
      ).toMatchObject({
        failed: 0,
      });
      expect((await pending()).data).toEqual({ pending: 0 });
      const denied = await listModels();
      expect(denied.status).toBe(403);
      expect(await denied.json()).toMatchObject({ error: { code: "org_suspended" } });
      expect(
        (
          await h.gatewayInternalApi("PUT", "/internal/v1/billing-accounts", {
            id: billingAccountId,
            name: "Catalog update without organization reference",
          })
        ).status,
      ).toBe(200);
      expect(
        (
          await h.gatewayInternalApi("PUT", "/internal/v1/billing-accounts", {
            id: billingAccountId,
            name: "Catalog must preserve binding",
            external_ref: null,
          })
        ).status,
      ).toBe(409);
      const primary = await fetch(`${h.gatewayPublicUrl}/v1/models`, {
        headers: { authorization: `Bearer ${key.data.key}` },
      });
      expect(primary.status).toBe(403);
      expect(await primary.json()).toMatchObject({ error: { code: "org_suspended" } });

      h.setGatewayInterceptor(async (url, init, forward) => {
        const response = await forward();
        if (init?.method !== "PUT" || new URL(url).pathname !== admissionPath) return response;
        expect(response.status).toBe(200);
        await response.body?.cancel();
        return new Response(null, { status: 503 });
      });
      expect(await setStatus("ACTIVE")).toMatchObject({
        status: 202,
        data: {
          status: "SUSPENDED",
          requested_status: "ACTIVE",
          inference_sync_pending: true,
          inference_revision: 2,
        },
      });
      expect((await listModels()).status).toBe(200);
      expect((await allowedAtConsole()).status).toBe(403);
      h.setGatewayInterceptor(null);
      expect(
        (await h.adminApi("POST", "/admin/v1/sync/reconcile", adminSession.access)).data,
      ).toMatchObject({
        failed: 0,
      });
      expect((await allowedAtConsole()).status).toBe(200);
      expect(
        (await h.gatewayInternalApi("PUT", admissionPath, { suspended: true, source_revision: 1 }))
          .status,
      ).toBe(409);
      expect((await listModels()).status).toBe(200);
      expect(replica.snapshot.get().epoch).toBe(epoch);
      expect((await pending()).data).toEqual({ pending: 0 });
    } finally {
      h.setGatewayInterceptor(null);
      await replica.close();
    }
  });

  test("concurrent password changes only accept the current password once", async () => {
    const user = await h.registerUser("password-change-once@example.com", "CurrentPassword123!");
    const change = (new_password: string) =>
      h.clientApi("POST", "/api/v1/auth/change-password", user.accessToken, {
        current_password: "CurrentPassword123!",
        new_password,
      });
    const results = await Promise.all([
      change("DifferentPassword123!"),
      change("OtherPassword456!"),
    ]);
    expect(results.map((result) => result.status).sort()).toEqual([200, 400]);
    expect((await h.clientApi("GET", "/api/v1/auth/me", user.accessToken)).status).toBe(200);
  });
}
