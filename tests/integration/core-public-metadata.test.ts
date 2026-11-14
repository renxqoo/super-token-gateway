import { expect, test } from "bun:test";
import { createDatabase } from "@gateway/storage";
import { withFinancial } from "../support/financial-execution.ts";
import { settledCharge, type Fixture } from "../support/core-settlement.ts";

async function key(f: Fixture, project: string, input: Record<string, unknown> & { role: string }) {
  const created = await f.request(`/projects/${project}/keys`, {
    name: `synthetic-${input.role.toLowerCase()}`,
    operations: ["chat"],
    ...input,
  });
  expect(created.status).toBe(201);
  return created.value as { id: string; key: string };
}

function publicApi(f: Fixture) {
  const port = f.identity.a.public_port;
  if (!port) throw new Error("Core public listener did not start");
  return async (
    path: string,
    token: string,
    init: { method?: string; body?: string; headers?: Record<string, string> } = {},
  ) => {
    const response = await fetch(`http://127.0.0.1:${port}${path}`, {
      method: init.method ?? "GET",
      body: init.body ?? null,
      headers: { authorization: `Bearer ${token}`, ...init.headers },
    });
    return { status: response.status, value: (await response.json()) as Payload };
  };
}

type Payload = Record<string, unknown> & {
  data?: Record<string, unknown>[];
  key?: string;
  id?: string;
  expires_at?: string;
};

const options = { account: { trustedProxyCidrs: ["127.0.0.1/32"] } };
const roleDenied = { status: 403, value: { error: { code: "role_denied" } } };

test("Core public listener serves models, usage and request metadata to every key role within key scope and never returns request bodies", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { request } = await settledCharge(db, setup, client),
      call = publicApi(f),
      project = setup.project.id;
    for (const role of ["ADMIN", "INVOKER", "BILLING", "VIEWER"]) {
      const credential = await key(f, project, { role });
      expect((await call("/v1/models", credential.key)).value).toEqual({
        object: "list",
        data: [
          {
            id: setup.model,
            object: "model",
            created: 0,
            owned_by: "gateway",
            capabilities: ["chat"],
          },
        ],
      });
      const usage = await call("/v1/usage", credential.key);
      expect(usage.status).toBe(200);
      expect(usage.value.data).toMatchObject([
        {
          project_id: project,
          request_id: request,
          finality: "FINAL",
          semantics: "attempt_snapshot",
          usage: { input_tokens: "31", output_tokens: "2" },
        },
      ]);
      const detail = await call(`/v1/requests/${request}`, credential.key);
      expect(detail.status).toBe(200);
      expect(Object.keys(detail.value).sort()).toEqual([
        "attempts",
        "created_at",
        "id",
        "model",
        "operation",
        "state",
        "updated_at",
        "usage",
      ]);
      expect(detail.value).toMatchObject({
        id: request,
        model: setup.model,
        operation: "chat",
        state: "SUCCEEDED",
        attempts: [{ ordinal: 1, state: "SUCCEEDED" }],
        usage: [{ finality: "FINAL", usage: { input_tokens: "31", output_tokens: "2" } }],
      });
    }
    const reader = await key(f, project, { role: "VIEWER" });
    const seq = String((await call("/v1/usage", reader.key)).value.data?.[0]?.seq);
    expect((await call(`/v1/usage?before=${seq}`, reader.key)).value.data).toEqual([]);
    expect(await call("/v1/usage?before=abc", reader.key)).toMatchObject({
      status: 400,
      value: { error: { code: "invalid_cursor" } },
    });
    expect(await call("/v1/requests/not-a-uuid", reader.key)).toMatchObject({
      status: 400,
      value: { error: { code: "invalid_request" } },
    });
    const scoped = await key(f, project, {
      role: "ADMIN",
      models: ["synthetic-unrelated-model"],
    });
    expect((await call("/v1/models", scoped.key)).value.data).toEqual([]);
    expect((await call("/v1/usage", scoped.key)).value.data).toEqual([]);
    expect(await call(`/v1/requests/${request}`, scoped.key)).toMatchObject({
      status: 404,
      value: { error: { code: "not_found" } },
    });
    const other = await f.request("/projects", {
      name: "synthetic-other",
      billing_currency: "USD",
      rpm: 10,
      tpm: 1000,
      concurrency: 1,
    });
    const foreign = await key(f, String(other.value.id), { role: "ADMIN" });
    expect((await call("/v1/usage", foreign.key)).value.data).toEqual([]);
    expect((await call(`/v1/requests/${request}`, foreign.key)).status).toBe(404);
    for (const role of ["INVOKER", "BILLING", "VIEWER"]) {
      const credential = await key(f, project, { role });
      expect(await call("/v1/project/keys", credential.key)).toMatchObject(roleDenied);
      expect(
        await call("/v1/project/keys", credential.key, {
          method: "POST",
          body: JSON.stringify({ name: "child" }),
        }),
      ).toMatchObject(roleDenied);
    }
  }, options));

test("Core public listener delegates project keys only within the issuing ADMIN key's authority and revokes only same-project keys", () =>
  withFinancial(async (f) => {
    const setup = await f.bootstrap(),
      call = publicApi(f),
      project = setup.project.id,
      expiresAt = new Date(Date.now() + 86_400_000).toISOString();
    const admin = await key(f, project, {
      role: "ADMIN",
      models: [setup.model],
      allowed_cidrs: ["127.0.0.0/8"],
      rpm: 50,
      expires_at: expiresAt,
    });
    const delegate = (input: unknown) =>
      call("/v1/project/keys", admin.key, { method: "POST", body: JSON.stringify(input) });
    const child = await delegate({ name: "child" });
    expect(child.status).toBe(201);
    expect(child.value).toMatchObject({
      name: "child",
      role: "INVOKER",
      models: [setup.model],
      operations: ["chat"],
      allowed_cidrs: ["127.0.0.0/8"],
      rpm: 50,
    });
    expect(Date.parse(String(child.value.expires_at))).toBe(Date.parse(expiresAt));
    expect(child.value.key).toMatch(/^gw_[A-Za-z0-9_-]{43}$/);
    expect((await call("/v1/models", String(child.value.key))).status).toBe(200);
    for (const escalation of [
      { models: [`other-${crypto.randomUUID()}`] },
      { operations: ["chat", "embeddings"] },
      { allowed_cidrs: ["0.0.0.0/0"] },
      { rpm: 51 },
      { expires_at: new Date(Date.parse(expiresAt) + 1000).toISOString() },
    ])
      expect(await delegate({ name: "escalated", ...escalation })).toMatchObject({
        status: 403,
        value: { error: { code: "delegation_denied" } },
      });
    expect(
      await call("/v1/project/keys", admin.key, { method: "POST", body: "{not json" }),
    ).toMatchObject({ status: 400, value: { error: { code: "invalid_request" } } });
    const listed = await call("/v1/project/keys", admin.key);
    expect(listed.status).toBe(200);
    expect((listed.value.data ?? []).map((row) => row.id).sort()).toEqual(
      [admin.id, String(child.value.id), setup.key.id].sort(),
    );
    expect(JSON.stringify(listed.value)).not.toContain("verifier");
    expect(JSON.stringify(listed.value)).not.toContain(String(child.value.key));
    const other = await f.request("/projects", {
      name: "synthetic-other",
      billing_currency: "USD",
      rpm: 10,
      tpm: 1000,
      concurrency: 1,
    });
    const foreign = await key(f, String(other.value.id), { role: "INVOKER" });
    expect(
      await call(`/v1/project/keys/${foreign.id}`, admin.key, { method: "DELETE" }),
    ).toMatchObject({ status: 404, value: { error: { code: "not_found" } } });
    expect(await call("/v1/models", foreign.key)).toMatchObject({ status: 200 });
    expect(
      await call(`/v1/project/keys/${String(child.value.id)}`, admin.key, { method: "DELETE" }),
    ).toEqual({ status: 200, value: { revoked: true } });
    expect(await call("/v1/models", String(child.value.key))).toMatchObject({
      status: 401,
      value: { error: { code: "unauthenticated" } },
    });
  }, options));

test("Core public account role can delegate and revoke keys but cannot read verifiers or rewrite key authority", () =>
  withFinancial(async (f) => {
    const connection = createDatabase(f.identity.accountUrl!, 1);
    try {
      for (const statement of [
        connection`SELECT verifier FROM api_keys LIMIT 1`,
        connection`UPDATE api_keys SET role='ADMIN'`,
        connection`UPDATE api_keys SET models='{}'::text[]`,
        connection`UPDATE projects SET enabled=true`,
        connection`DELETE FROM api_keys`,
        connection`SELECT resource_id FROM audit_events LIMIT 1`,
        connection`SELECT secret FROM channels LIMIT 1`,
      ])
        await expect(statement).rejects.toMatchObject({ errno: "42501" });
    } finally {
      await connection.close();
    }
  }, options));
