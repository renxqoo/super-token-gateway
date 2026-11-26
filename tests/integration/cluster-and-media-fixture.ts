import { localInternalCredentials } from "../../scripts/local-internal-auth.ts";
import { afterAll, beforeAll } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SQL } from "bun";
import { ClusterCoordinator } from "@gateway/runtime";
import { createDatabase, migrate } from "@gateway/ledger";
import type { GatewayConfig } from "@gateway/gateway";

const baseDbUrl = process.env.DATABASE_URL;

if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const testDbName = `gateway_v4_cluster_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;

const adminUrl = new URL(baseDbUrl);

adminUrl.pathname = "/postgres";

const targetUrl = new URL(baseDbUrl);

targetUrl.pathname = `/${testDbName}`;

const KEY_PEPPER = "cluster-pepper-0123456789abcdef01";

const ENCRYPTION_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

const OPERATOR_TOKEN = "cluster-admin-token-0123456789ab00000000000000";

let adminDb: SQL;

export let db: SQL;

export let redisProc: ChildProcess | null = null;

let redisDir: string | null = null;

export let redisUrl = "";

async function findFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

export async function startEphemeralRedis(port?: number): Promise<string> {
  const redisBin =
    process.env.DURABILITY_REDIS_SERVER ??
    new URL("../../.local/toolchain/redis74/bin/redis-server", import.meta.url).pathname;
  port ??= await findFreePort();
  redisDir ??= await mkdtemp(join(tmpdir(), "stg-redis-test-"));
  redisProc = spawn(
    redisBin,
    [
      "--bind",
      "127.0.0.1",
      "--port",
      String(port),
      "--dir",
      redisDir,
      "--save",
      "",
      "--appendonly",
      "no",
    ],
    { stdio: "ignore" },
  );
  const url = `redis://127.0.0.1:${port}`;
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    const client = new Bun.RedisClient(url, { connectionTimeout: 300, autoReconnect: false });
    try {
      const pong = await client.send("PING", []);
      if (pong === "PONG") return url;
    } catch {
      await Bun.sleep(50);
    } finally {
      client.close();
    }
  }
  throw new Error("Ephemeral Redis did not start in time");
}

export async function waitForRedisState(
  expected: boolean,
  ...coordinators: ClusterCoordinator[]
): Promise<void> {
  const deadline = performance.now() + 5000;
  while (coordinators.some((node) => node.isRedisConnected() !== expected)) {
    if (performance.now() >= deadline) throw new Error("Nodes did not reach expected Redis state");
    await Bun.sleep(25);
  }
}

export function makeConfig(overrides: Partial<GatewayConfig> = {}): GatewayConfig {
  return {
    mode: "gateway",
    host: "127.0.0.1",
    port: 0,
    internalHost: "127.0.0.1",
    internalPort: 0,
    databaseUrl: targetUrl.toString(),
    dbMaxConnections: 16,
    redisUrl,
    redisNamespace: "stg_cluster_test",
    accessLog: false,
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

export function registerClusterFixture() {
  beforeAll(async () => {
    adminDb = createDatabase(adminUrl.toString(), 2);
    await adminDb.unsafe(`CREATE DATABASE ${testDbName}`);
    db = createDatabase(targetUrl.toString(), 16);
    await migrate(db);
    redisUrl = await startEphemeralRedis();
  });

  afterAll(async () => {
    redisProc?.kill("SIGTERM");
    if (redisDir) await rm(redisDir, { recursive: true, force: true });
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
