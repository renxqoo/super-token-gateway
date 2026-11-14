import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { CoreRpcError } from "@gateway/core-client";
import { verifier } from "@gateway/runtime/crypto";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";
import { syntheticPepper } from "../support/catalog.ts";

async function administrator(db: SQL, permissions: string[]) {
  const id = crypto.randomUUID(),
    token = `synthetic-catalog-${crypto.randomUUID()}`;
  await db`INSERT INTO technical_administrators(id,name,permissions) VALUES(${id},'Synthetic catalog administrator',${permissions}::text[])`;
  await db`INSERT INTO administrator_credentials(id,administrator_id,verifier,prefix) VALUES(${crypto.randomUUID()},${id},${verifier(token, syntheticPepper)},'synthetic')`;
  return { actor: `technical-admin:${id}`, token };
}

async function audits(db: SQL, actor: string) {
  return (
    await db`SELECT action,resource_id FROM audit_events WHERE actor=${actor} ORDER BY created_at,id`
  ).map((row: { action: string; resource_id: string }) => [row.action, row.resource_id]);
}

function contextCode(pending: Promise<unknown>) {
  return pending.then(
    () => "ADMITTABLE",
    (error: unknown) =>
      error instanceof CoreRpcError ? `${error.status}:${error.detail.code}` : String(error),
  );
}

const denied = { status: 403, value: { error: { code: "platform_scope_denied" } } };

test("Core admin manages organizations, principals and key scope with immediate effect on the next context", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client(),
      manager = await administrator(db, ["catalog:manage"]),
      reader = await administrator(db, ["catalog:read"]);
    await client.startup();
    const organization = { external_ref: `org-${crypto.randomUUID()}`, name: "Synthetic org" };
    expect(await f.request("/organizations", organization, reader.token)).toMatchObject(denied);
    const org = await f.request("/organizations", organization, manager.token);
    expect(org.status).toBe(201);
    const orgId = String(org.value.id);
    const principal = await f.request(
      `/organizations/${orgId}/principals`,
      { external_ref: "alice", role: "MEMBER" },
      manager.token,
    );
    expect(principal.status).toBe(201);
    const principalId = String(principal.value.id);
    await db`UPDATE projects SET organization_id=${orgId} WHERE id=${setup.project.id}`;
    await db`UPDATE api_keys SET principal_id=${principalId} WHERE id=${setup.key.id}`;
    const context = () => contextCode(prepareFinancialAdmission(setup, client));
    expect(await context()).toBe("ADMITTABLE");
    const patch = (path: string, input: unknown, token = manager.token) =>
      f.request(path, input, token, "PATCH");
    expect(await patch(`/principals/${principalId}`, { role: "VIEWER" })).toEqual({
      status: 200,
      value: { updated: true },
    });
    expect(await context()).toBe("403:customer_forbidden");
    expect(await patch(`/principals/${principalId}`, { role: "MEMBER" })).toMatchObject({
      status: 200,
    });
    expect(await patch(`/organizations/${orgId}`, { enabled: false })).toMatchObject({
      status: 200,
    });
    expect(await context()).toBe("403:customer_forbidden");
    expect(await patch(`/organizations/${orgId}`, { enabled: true, rpm: 500 })).toMatchObject({
      status: 200,
    });
    expect(await context()).toBe("ADMITTABLE");
    expect(
      await patch(`/keys/${setup.key.id}`, { models: [`other-${crypto.randomUUID()}`] }),
    ).toEqual({ status: 200, value: { updated: true } });
    expect(await context()).toBe("403:customer_forbidden");
    expect(await patch(`/keys/${setup.key.id}`, { models: [setup.model] })).toMatchObject({
      status: 200,
    });
    expect(await patch(`/keys/${setup.key.id}`, { allowed_cidrs: ["10.0.0.0/8"] })).toMatchObject({
      status: 200,
    });
    expect(await context()).toBe("403:customer_forbidden");
    expect(await patch(`/keys/${setup.key.id}`, { allowed_cidrs: ["not-a-cidr"] })).toMatchObject({
      status: 400,
    });
    expect(await patch(`/keys/${setup.key.id}`, { allowed_cidrs: ["127.0.0.0/8"] })).toMatchObject({
      status: 200,
    });
    expect(await context()).toBe("ADMITTABLE");
    expect(await f.request(`/keys/${setup.key.id}`, undefined, manager.token, "DELETE")).toEqual({
      status: 200,
      value: { revoked: true },
    });
    expect(await context()).toBe("403:customer_forbidden");
    expect(await patch(`/keys/${setup.key.id}`, { role: "ADMIN" })).toMatchObject({
      status: 404,
      value: { error: { code: "not_found" } },
    });
    expect(await patch(`/organizations/${crypto.randomUUID()}`, { enabled: false })).toMatchObject({
      status: 404,
      value: { error: { code: "not_found" } },
    });
    expect(await patch("/principals/not-a-uuid", { enabled: false })).toMatchObject({
      status: 400,
      value: { error: { code: "invalid_management_input" } },
    });
    expect(await audits(db, manager.actor)).toEqual([
      ["organization.create", orgId],
      ["principal.create", principalId],
      ["principal.update", principalId],
      ["principal.update", principalId],
      ["organization.update", orgId],
      ["organization.update", orgId],
      ["key.update", setup.key.id],
      ["key.update", setup.key.id],
      ["key.update", setup.key.id],
      ["key.update", setup.key.id],
      ["key.revoke", setup.key.id],
    ]);
    expect((await db`SELECT rpm FROM organizations WHERE id=${orgId}`)[0].rpm).toBe(500);
  }));

test("Core admin lists and changes projects, keys, models and routes under catalog permissions without exposing key verifiers", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client(),
      manager = await administrator(db, ["catalog:manage"]),
      reader = await administrator(db, ["catalog:read"]),
      outsider = await administrator(db, ["funding:read"]);
    await client.startup();
    const context = () => contextCode(prepareFinancialAdmission(setup, client));
    expect(await f.request("/projects", undefined, outsider.token)).toMatchObject(denied);
    const projects = await f.request("/projects", undefined, reader.token);
    expect(projects.status).toBe(200);
    expect(
      (projects.value.data as { id: string; name: string }[]).find(
        (p) => p.id === setup.project.id,
      ),
    ).toMatchObject({ name: setup.model, enabled: true });
    const keys = await f.request(`/projects/${setup.project.id}/keys`, undefined, reader.token);
    expect(keys.value.data).toMatchObject([{ id: setup.key.id, name: "synthetic", enabled: true }]);
    expect(JSON.stringify(keys.value)).not.toContain("verifier");
    expect(JSON.stringify(keys.value)).not.toContain(setup.key.key);
    expect(
      await f.request(`/projects/${setup.project.id}`, { enabled: false }, reader.token, "PATCH"),
    ).toMatchObject(denied);
    expect(
      await f.request(
        `/projects/${setup.project.id}`,
        { enabled: false, name: "renamed" },
        manager.token,
        "PATCH",
      ),
    ).toEqual({ status: 200, value: { updated: true } });
    expect(await context()).toBe("403:customer_forbidden");
    expect(
      await f.request(`/projects/${setup.project.id}`, { enabled: true }, manager.token, "PATCH"),
    ).toMatchObject({ status: 200 });
    expect(await context()).toBe("ADMITTABLE");
    const models = await f.request("/models", undefined, reader.token);
    expect(
      (models.value.data as Record<string, unknown>[]).find((m) => m.name === setup.model),
    ).toEqual({
      name: setup.model,
      enabled: true,
      operations: ["chat"],
      max_output_tokens: 100,
      routing_strategy: "WEIGHTED",
    });
    expect(
      await f.request(
        `/models/${setup.model}`,
        { routing_strategy: "LEAST_LATENCY", max_output_tokens: 80 },
        manager.token,
        "PATCH",
      ),
    ).toEqual({ status: 200, value: { updated: true } });
    const routes = await f.request(`/models/${setup.model}/routes`, undefined, reader.token);
    expect(routes.value.data).toMatchObject([
      {
        id: setup.route.id,
        provider_id: setup.provider.id,
        upstream_model: "synthetic",
        pricing: { currency: "USD" },
        ceilings: { input_tokens: "100", output_tokens: "50" },
      },
    ]);
    expect(
      await f.request(`/models/${setup.model}`, { enabled: false }, manager.token, "PATCH"),
    ).toMatchObject({ status: 200 });
    expect(await context()).toBe("503:no_route");
    expect(
      await f.request(
        `/models/missing-${crypto.randomUUID()}`,
        { enabled: true },
        manager.token,
        "PATCH",
      ),
    ).toMatchObject({ status: 404, value: { error: { code: "not_found" } } });
    expect(
      (
        await db`SELECT max_output_tokens,routing_strategy FROM models WHERE name=${setup.model}`
      )[0],
    ).toEqual({ max_output_tokens: 80, routing_strategy: "LEAST_LATENCY" });
    expect((await db`SELECT name FROM projects WHERE id=${setup.project.id}`)[0].name).toBe(
      "renamed",
    );
  }));
