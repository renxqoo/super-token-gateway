import { localInternalCredentials } from "../../scripts/local-internal-auth.ts";
import { afterAll, beforeAll, expect } from "bun:test";
import { SQL } from "bun";
import { createDatabase, migrate } from "@gateway/ledger";
import { createGatewayRuntime, type GatewayConfig } from "@gateway/gateway";

const baseDbUrl = process.env.DATABASE_URL;

if (!baseDbUrl) throw new Error("DATABASE_URL is required");

export const testDbName = `gateway_adv_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;

export const adminUrl = new URL(baseDbUrl);

adminUrl.pathname = "/postgres";

export const targetUrl = new URL(baseDbUrl);

targetUrl.pathname = `/${testDbName}`;

const OPERATOR_TOKEN = "adv-admin-token-0123456789abcdef00000000000000";

export let adminDb: SQL;

export let db: SQL;

export function makeConfig(overrides: Partial<GatewayConfig> = {}): GatewayConfig {
  return {
    mode: "gateway",
    host: "127.0.0.1",
    port: 0,
    internalHost: "127.0.0.1",
    internalPort: 0,
    databaseUrl: targetUrl.toString(),
    dbMaxConnections: 16,
    keyPepper: "adv-pepper-secret-0123456789abcdef",
    encryptionKey: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    internalCredentials: localInternalCredentials(OPERATOR_TOKEN),
    allowPrivateUpstreams: true,
    privateUpstreamCidrs: ["127.0.0.0/8"],
    trustedProxyCidrs: [],
    maxInFlight: 500,
    maxBodyBytes: 2 * 1024 * 1024,
    holdTtlMs: 30_000,
    attemptTimeoutMs: 5000,
    snapshotPollMs: 0,
    microBatchWindowMs: 2,
    microBatchMaxSize: 32,
    ...overrides,
  };
}

export async function internalCall(
  app: { fetch: (req: Request) => Response | Promise<Response> },
  method: string,
  path: string,
  body?: unknown,
  token = OPERATOR_TOKEN,
) {
  const res = await app.fetch(
    new Request(`http://internal.local${path}`, {
      method,
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    }),
  );
  const text = await res.text();
  let data: Record<string, unknown> = {};
  try {
    data = JSON.parse(text) as Record<string, unknown>;
  } catch {
    data = { raw: text };
  }
  return { status: res.status, data, headers: res.headers };
}

type Wallet = {
  available_atoms: string;
  reserved_atoms: string;
  spent_atoms: string;
};

export async function wallet(
  runtime: ReturnType<typeof createGatewayRuntime>,
  billingAccountId: string,
) {
  const r = await internalCall(
    runtime.internalApp,
    "GET",
    `/internal/v1/wallets/${billingAccountId}`,
  );
  expect(r.status).toBe(200);
  return r.data as unknown as Wallet;
}

export async function waitFor(condition: () => Promise<boolean>, ms = 4000) {
  const deadline = Date.now() + ms;
  while (!(await condition())) {
    if (Date.now() >= deadline) throw new Error("condition not met in time");
    await Bun.sleep(20);
  }
}

export async function seed(runtime: ReturnType<typeof createGatewayRuntime>, mockOrigin: string) {
  const call = async (method: string, path: string, body?: unknown) => {
    const r = await internalCall(runtime.internalApp, method, path, body);
    expect(r.status, JSON.stringify(r.data)).toBe(path === "/internal/v1/prices" ? 201 : 200);
    return r.data as Record<string, unknown>;
  };
  const billingAccount = await call("PUT", "/internal/v1/billing-accounts", {
    name: `adv-${crypto.randomUUID().slice(0, 8)}`,
    currency: "USD",
    rpm: 1_000_000,
    tpm: 100_000_000,
    concurrency: 1000,
  });
  const billingAccountId = String(billingAccount.id);
  await call("POST", `/internal/v1/wallets/${billingAccountId}/credits`, {
    currency: "USD",
    external_ref: `adv-credit-${crypto.randomUUID()}`,
    amount_atoms: "1000000000",
    kind: "CASH",
  });
  const key = await call("PUT", "/internal/v1/api-keys", {
    billing_account_id: billingAccountId,
    name: "adv",
  });
  const account = await call("PUT", "/internal/v1/upstream-accounts", {
    name: `adv-up-${crypto.randomUUID().slice(0, 8)}`,
  });
  await call("POST", `/internal/v1/upstream-accounts/${account.id}/recharges`, {
    currency: "USD",
    external_ref: `adv-sup-${crypto.randomUUID()}`,
    amount_atoms: "1000000000",
  });
  const channel = await call("PUT", "/internal/v1/channels", {
    account_id: account.id,
    kind: "openai",
    name: `adv-ch-${crypto.randomUUID().slice(0, 8)}`,
    base_url: `${mockOrigin}/openai`,
    api_key: "mock-adv",
  });
  const customer = await call("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    currency: "USD",
    rates: { input_tokens: { numerator: "1", denominator: "1" } },
    ceilings: { input_tokens: "100" },
  });
  const supplier = await call("POST", "/internal/v1/prices", {
    side: "SUPPLIER",
    currency: "USD",
    rates: { input_tokens: { numerator: "1", denominator: "1" } },
    ceilings: { input_tokens: "100" },
  });
  return { billingAccountId, key: String(key.key), channel, customer, supplier, call };
}

export function chatRequest(key: string, model: string, extra: Record<string, unknown> = {}) {
  return new Request("http://gateway.local/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
    body: JSON.stringify({
      model,
      stream: true,
      messages: [{ role: "user", content: "attack" }],
      max_tokens: 50,
      ...extra,
    }),
  });
}

export function realServer(runtime: ReturnType<typeof createGatewayRuntime>) {
  return Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: runtime.publicApp.fetch });
}

export function registerBillingFixture() {
  beforeAll(async () => {
    adminDb = createDatabase(adminUrl.toString(), 2);
    await adminDb.unsafe(`CREATE DATABASE ${testDbName}`);
    db = createDatabase(targetUrl.toString(), 16);
    await migrate(db);
  });

  afterAll(async () => {
    await db?.close();
    if (adminDb) {
      await adminDb.unsafe(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${testDbName}' AND pid <> pg_backend_pid()`,
      );
      await adminDb.unsafe(`DROP DATABASE IF EXISTS ${testDbName}`);
      await adminDb.close();
    }
  });
}
