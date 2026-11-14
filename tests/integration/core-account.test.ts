import { expect, test } from "bun:test";
import { createDatabase } from "@gateway/storage";
import { withFinancial } from "../support/financial-execution.ts";
import { settledCharge, type Fixture } from "../support/core-settlement.ts";

async function key(
  f: Fixture,
  project: string,
  input: { role: string; models?: string[]; allowed_cidrs?: string[] },
) {
  const created = await f.request(`/projects/${project}/keys`, {
    name: `synthetic-${input.role.toLowerCase()}`,
    operations: ["chat"],
    ...input,
  });
  expect(created.status).toBe(201);
  return created.value as { id: string; key: string };
}

function account(f: Fixture) {
  const port = f.identity.a.public_port;
  if (!port) throw new Error("Core public listener did not start");
  return async (path: string, token: string, headers: Record<string, string> = {}) => {
    const response = await fetch(`http://127.0.0.1:${port}${path}`, {
      headers: { authorization: `Bearer ${token}`, ...headers },
    });
    return {
      status: response.status,
      headers: response.headers,
      value: (await response.json()) as Record<string, unknown>,
    };
  };
}

const options = { account: { trustedProxyCidrs: ["127.0.0.1/32"] } };

test("Core public listener serves wallets and request billing to ADMIN and BILLING keys with exact string atoms and denies other roles", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { request, charge } = await settledCharge(db, setup, client),
      project = setup.project.id,
      get = account(f);
    expect(
      (
        await f.request(`/projects/${project}/credits`, {
          currency: "EUR",
          external_ref: "eur-gift",
          amount_atoms: "7",
          kind: "GIFT",
        })
      ).status,
    ).toBe(201);
    for (const role of ["ADMIN", "BILLING"]) {
      const credential = await key(f, project, { role });
      const wallets = await get("/v1/wallets", credential.key);
      expect(wallets.status).toBe(200);
      expect(wallets.headers.get("cache-control")).toBe("no-store");
      expect(wallets.value.data).toMatchObject([
        { currency: "EUR", available_atoms: "7", spent_atoms: "0", debt_atoms: "0" },
        { currency: "USD", available_atoms: "900", spent_atoms: "100", debt_atoms: "0" },
      ]);
      const receipt = await get(`/v1/requests/${request}/billing`, credential.key);
      expect(receipt).toMatchObject({
        status: 200,
        value: {
          id: request,
          billing_currency: "USD",
          charge_id: charge,
          charged_atoms: "100",
          refunded_atoms: "0",
        },
      });
    }
    for (const role of ["INVOKER", "VIEWER"]) {
      const credential = await key(f, project, { role });
      for (const path of ["/v1/wallets", `/v1/requests/${request}/billing`])
        expect(await get(path, credential.key)).toMatchObject({
          status: 403,
          value: { error: { code: "role_denied" } },
        });
    }
    expect(await get("/v1/wallets", `gw_${"A".repeat(43)}`)).toMatchObject({
      status: 401,
      value: { error: { code: "unauthenticated" } },
    });
    expect(await get("/v1/wallets", "not-a-project-key")).toMatchObject({
      status: 401,
      value: { error: { code: "unauthenticated" } },
    });
  }, options));

test("Core public listener hides other projects' and out-of-scope requests and applies revocation, expiry and IP limits on the next call", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const { request } = await settledCharge(db, setup, client),
      get = account(f);
    const other = await f.request("/projects", {
      name: "synthetic-other",
      billing_currency: "USD",
      rpm: 10,
      tpm: 1000,
      concurrency: 1,
    });
    const foreign = await key(f, String(other.value.id), { role: "BILLING" });
    expect(await get(`/v1/requests/${request}/billing`, foreign.key)).toMatchObject({
      status: 404,
      value: { error: { code: "not_found" } },
    });
    expect((await get("/v1/wallets", foreign.key)).value.data).toEqual([]);
    const scoped = await key(f, setup.project.id, {
      role: "BILLING",
      models: ["synthetic-unrelated-model"],
    });
    expect((await get(`/v1/requests/${request}/billing`, scoped.key)).status).toBe(404);
    expect((await get(`/v1/requests/not-a-uuid/billing`, scoped.key)).status).toBe(400);
    const restricted = await key(f, setup.project.id, {
      role: "BILLING",
      allowed_cidrs: ["203.0.113.0/24"],
    });
    expect(
      (await get("/v1/wallets", restricted.key, { "x-forwarded-for": "203.0.113.9" })).status,
    ).toBe(200);
    expect(
      await get("/v1/wallets", restricted.key, { "x-forwarded-for": "198.51.100.1" }),
    ).toMatchObject({ status: 403, value: { error: { code: "ip_denied" } } });
    expect(await get("/v1/wallets", restricted.key)).toMatchObject({
      status: 403,
      value: { error: { code: "ip_denied" } },
    });
    const revoked = await key(f, setup.project.id, { role: "ADMIN" });
    expect((await get("/v1/wallets", revoked.key)).status).toBe(200);
    await db`UPDATE api_keys SET enabled=false WHERE id=${revoked.id}`;
    expect(await get("/v1/wallets", revoked.key)).toMatchObject({
      status: 401,
      value: { error: { code: "unauthenticated" } },
    });
    const expired = await key(f, setup.project.id, { role: "BILLING" });
    await db`UPDATE api_keys SET expires_at=now()-interval '1 second' WHERE id=${expired.id}`;
    expect((await get("/v1/wallets", expired.key)).status).toBe(401);
    const suspended = await key(f, setup.project.id, { role: "BILLING" });
    await db`UPDATE projects SET enabled=false WHERE id=${setup.project.id}`;
    expect((await get("/v1/wallets", suspended.key)).status).toBe(401);
  }, options));

test("Core public account role reads only the billing projection and cannot read credentials or write funds", () =>
  withFinancial(async (f) => {
    const connection = createDatabase(f.identity.accountUrl!, 1);
    try {
      for (const statement of [
        connection`SELECT verifier FROM api_keys LIMIT 1`,
        connection`SELECT secret FROM channels LIMIT 1`,
        connection`SELECT fingerprint FROM gateway_requests LIMIT 1`,
        connection`SELECT lines FROM money_journal LIMIT 1`,
        connection`UPDATE wallets SET available_atoms=available_atoms+1`,
        connection`SELECT credit_wallet(${crypto.randomUUID()}::uuid,'USD','forged',1::numeric,'CASH',1,NULL,'{}'::text[],'{}'::text[])`,
      ])
        await expect(statement).rejects.toMatchObject({ errno: "42501" });
      expect(
        (await connection`SELECT core_account_auth_v1('forged-verifier','127.0.0.1') AS r`)[0].r,
      ).toEqual({ status: "unauthenticated" });
    } finally {
      await connection.close();
    }
  }, options));
