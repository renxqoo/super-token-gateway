import { localInternalCredentials } from "../../scripts/local-internal-auth.ts";
import { afterAll, beforeAll, expect } from "bun:test";
import type { SQL } from "bun";
import { createDatabase, migrate } from "@gateway/ledger";
import { createGatewayRuntime, type GatewayConfig } from "@gateway/gateway";

const baseDbUrl = process.env.DATABASE_URL;

if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const testDbName = `gateway_v4_e2e_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;

const adminUrl = new URL(baseDbUrl);

adminUrl.pathname = "/postgres";

const targetUrl = new URL(baseDbUrl);

targetUrl.pathname = `/${testDbName}`;

const KEY_PEPPER = "e2e-pepper-secret-0123456789abcdef";

const ENCRYPTION_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

export const OPERATOR_TOKEN = "e2e-admin-token-0123456789abcdef00000000000000";

let adminDb: SQL;

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
    keyPepper: KEY_PEPPER,
    encryptionKey: ENCRYPTION_KEY,
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
): Promise<{ status: number; data: Record<string, unknown> }> {
  const res = await app.fetch(
    new Request(`http://internal.local${path}`, {
      method,
      headers: {
        authorization: `Bearer ${OPERATOR_TOKEN}`,
        "content-type": "application/json",
      },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    }),
  );
  const data = (await res.json()) as Record<string, unknown>;
  return { status: res.status, data };
}

export async function publishPlugins(
  runtime: ReturnType<typeof createGatewayRuntime>,
  plugins: [unknown, Record<string, unknown>?][],
) {
  const release = await internalCall(runtime.internalApp, "POST", "/internal/v1/plugin-releases", {
    id: crypto.randomUUID(),
    expected_epoch: runtime.snapshot.get().epoch,
    targets: plugins.map(([id]) => ({ plugin_id: id })),
    samples: Object.fromEntries(plugins.filter(([, sample]) => sample)),
  });
  expect(release.status, JSON.stringify(release.data)).toBe(200);
  return release.data;
}

export const textSample = { text: "Contact alice@example.com" };

export const webhookSample = { webhook_reply: { action: "PASS" } };

export function registerGatewayFixture() {
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
