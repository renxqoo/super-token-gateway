import { afterAll, beforeAll, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SQL } from "bun";
import { ClusterCoordinator } from "@gateway/runtime";
import { createAsyncTasksRepository, createDatabase, migrate } from "@gateway/ledger";
import { getBuiltinPlugin, type WasmRuntimeExecutor } from "@gateway/plugin-engine";
import { createGatewayRuntime, type GatewayConfig } from "@gateway/gateway";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const testDbName = `gateway_v4_cluster_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const adminUrl = new URL(baseDbUrl);
adminUrl.pathname = "/postgres";
const targetUrl = new URL(baseDbUrl);
targetUrl.pathname = `/${testDbName}`;

const KEY_PEPPER = "cluster-pepper-0123456789abcdef01";
const ENCRYPTION_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const ADMIN_TOKEN = "cluster-admin-token-0123456789ab";

let adminDb: SQL;
let db: SQL;
let redisProc: ChildProcess | null = null;
let redisDir: string | null = null;
let redisUrl = "";

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

async function startEphemeralRedis(port?: number): Promise<string> {
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

async function waitForRedisState(
  expected: boolean,
  ...coordinators: ClusterCoordinator[]
): Promise<void> {
  const deadline = performance.now() + 5000;
  while (coordinators.some((node) => node.isRedisConnected() !== expected)) {
    if (performance.now() >= deadline) throw new Error("Nodes did not reach expected Redis state");
    await Bun.sleep(25);
  }
}

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

function makeConfig(overrides: Partial<GatewayConfig> = {}): GatewayConfig {
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
    adminToken: ADMIN_TOKEN,
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

async function internalCall(
  app: { fetch: (req: Request) => Response | Promise<Response> },
  method: string,
  path: string,
  body?: unknown,
): Promise<{ status: number; data: Record<string, unknown> }> {
  const res = await app.fetch(
    new Request(`http://internal.local${path}`, {
      method,
      headers: {
        authorization: `Bearer ${ADMIN_TOKEN}`,
        "content-type": "application/json",
      },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    }),
  );
  const data = (await res.json()) as Record<string, unknown>;
  return { status: res.status, data };
}

test("public IP quota is atomic across gateways and fails closed when the Redis connection is lost", async () => {
  const config = makeConfig({
    redisNamespace: `stg_ip_${crypto.randomUUID()}`,
    trustedProxyCidrs: ["127.0.0.1/32"],
    ingressIp: {
      mode: "enforce",
      windowMs: 10_000,
      maxRequests: 2,
      nodeMaxRequests: 32,
      maxPending: 16,
      timeoutMs: 200,
    },
  });
  const first = createGatewayRuntime({ config, db, wasmExecutor: null });
  const second = createGatewayRuntime({ config, db, wasmExecutor: null });
  const models = (forwarded: string) =>
    new Request("http://gateway.test/v1/models", {
      headers: { "x-forwarded-for": forwarded },
    });
  try {
    await Promise.all([first.start(), second.start()]);
    await waitForRedisState(true, first.cluster, second.cluster);
    const burst = await Promise.all(
      Array.from({ length: 8 }, (_, index) =>
        (index % 2 ? first : second).fetchPublic(models(`198.51.100.${index + 1}`), {
          peerAddress: index % 2 ? "192.0.2.1" : "::ffff:c000:201",
        }),
      ),
    );
    expect(burst.map((response) => response.status).sort()).toEqual([
      401, 401, 429, 429, 429, 429, 429, 429,
    ]);
    const limited = burst.find((response) => response.status === 429)!;
    expect(Number(limited.headers.get("retry-after"))).toBeGreaterThan(0);
    expect(await limited.json()).toMatchObject({ error: { code: "ip_rate_limited" } });
    expect(
      (await first.fetchPublic(models("192.0.2.2"), { peerAddress: "127.0.0.1" })).status,
    ).toBe(401);
    expect((await first.fetchPublic(new Request("http://gateway.test/livez"))).status).toBe(200);

    const control = new Bun.RedisClient(redisUrl);
    try {
      expect(await control.send("CLIENT", ["PAUSE", "600", "ALL"])).toBe("OK");
      const paused = await first.fetchPublic(models("192.0.2.4"), { peerAddress: "127.0.0.1" });
      expect(paused.status).toBe(503);
      expect(await paused.json()).toMatchObject({ error: { code: "ingress_quota_unavailable" } });
      expect(first.cluster.isRedisConnected()).toBe(true);
      expect(await control.send("PING", [])).toBe("PONG");
      expect(
        (await first.fetchPublic(models("192.0.2.5"), { peerAddress: "127.0.0.1" })).status,
      ).toBe(401);
    } finally {
      control.close();
    }

    await first.cluster.close();
    const unavailable = await first.fetchPublic(models("192.0.2.3"), { peerAddress: "127.0.0.1" });
    expect(unavailable.status).toBe(503);
    expect(await unavailable.json()).toMatchObject({
      error: { code: "ingress_quota_unavailable" },
    });
  } finally {
    await Promise.all([first.close(), second.close()]);
  }
});

test("HTTP request size and the node body budget are enforced for JSON, multipart and streamed uploads", async () => {
  const boundary = "gateway-ingress-fixture";
  const contentType = `multipart/form-data; boundary=${boundary}`;
  const multipart = new TextEncoder().encode(
    `--${boundary}\r\nContent-Disposition: form-data; name="model"\r\n\r\nfixture-missing\r\n` +
      `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="clip.wav"\r\n` +
      `Content-Type: audio/wav\r\n\r\nfixture\r\n--${boundary}--\r\n`,
  );
  const maxBodyBytes = multipart.byteLength;
  const config = makeConfig({
    maxBodyBytes,
    ingressBodyBufferBytes: maxBodyBytes,
    bodyReadTimeoutMs: 1000,
  });
  const runtime = createGatewayRuntime({ config, db, wasmExecutor: null });
  const abort = new AbortController();
  let server: ReturnType<typeof Bun.serve> | undefined;
  try {
    await runtime.start();
    const project = await internalCall(runtime.internalApp, "PUT", "/internal/v1/projects", {
      name: "body-budget",
    });
    expect(project.status).toBe(200);
    const key = await internalCall(runtime.internalApp, "PUT", "/internal/v1/api-keys", {
      name: "body-budget",
      project_id: project.data.id,
    });
    expect(key.status).toBe(200);
    const auth = { authorization: `Bearer ${key.data.key}` };
    const request = JSON.stringify({ model: "fixture-missing" });
    const json = request + " ".repeat(maxBodyBytes - request.length);
    server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req, listener) {
        return runtime.fetchPublic(req, { getPeerAddress: () => listener.requestIP(req)?.address });
      },
    });
    const chat = new URL("/v1/chat/completions", server.url);
    const transcription = new URL("/v1/audio/transcriptions", server.url);
    const post = (url: URL, body: BodyInit, headers: Record<string, string> = {}) =>
      fetch(url, { method: "POST", headers: { ...auth, ...headers }, body });
    const plain = await post(chat, json, { "content-encoding": "identity" });
    expect(plain.status).toBe(404);
    expect(await plain.json()).toMatchObject({ error: { code: "model_not_found" } });
    expect((await post(chat, json + " ")).status).toBe(413);
    const transcribed = await post(transcription, multipart, { "content-type": contentType });
    expect(transcribed.status).toBe(404);
    expect(await transcribed.json()).toMatchObject({ error: { code: "model_not_found" } });
    const large = new Uint8Array(maxBodyBytes + 1);
    large.set(multipart);
    expect((await post(transcription, large, { "content-type": contentType })).status).toBe(413);
    const chunked = new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(json + " "));
        controller.close();
      },
    });
    expect((await post(chat, chunked)).status).toBe(413);

    const reading = Promise.withResolvers<void>();
    const pending = runtime.fetchPublic(
      new Request(chat, {
        method: "POST",
        headers: auth,
        signal: abort.signal,
        body: new ReadableStream(
          {
            pull() {
              reading.resolve();
            },
          },
          { highWaterMark: 0 },
        ),
      }),
    );
    await reading.promise;
    const full = await post(chat, request);
    expect(full.status).toBe(503);
    expect(await full.json()).toMatchObject({
      error: { code: "request_body_capacity_unavailable" },
    });
    abort.abort();
    expect((await pending).status).toBe(499);
    expect((await post(chat, json)).status).toBe(404);
    let cancelled = false;
    const timedOut = await runtime.fetchPublic(
      new Request(chat, {
        method: "POST",
        headers: { ...auth, "content-length": String(maxBodyBytes) },
        body: new ReadableStream({
          cancel() {
            cancelled = true;
          },
        }),
      }),
    );
    expect(timedOut.status).toBe(408);
    expect(cancelled).toBe(true);
    expect((await post(chat, json)).status).toBe(404);
    expect(
      (await (await runtime.metricsResponse()).text()).match(
        /^gateway_ingress_body_buffer_reserved_bytes 0$/m,
      ),
    ).not.toBeNull();
  } finally {
    abort.abort();
    await server?.stop(true);
    await runtime.close();
  }
});

test("symptom: bursts of readiness and metrics checks each open a PostgreSQL probe", async () => {
  const config = makeConfig({ redisNamespace: `stg_readiness_${crypto.randomUUID()}` });
  let observe = false;
  let probes = 0;
  let databaseUnavailable = false;
  const observedDatabase = new Proxy(db, {
    apply(target, receiver, args) {
      if (observe) {
        probes++;
        if (databaseUnavailable) return Promise.reject(new Error("Database unavailable"));
      }
      return Reflect.apply(target, receiver, args);
    },
  });
  const runtime = createGatewayRuntime({ config, db: observedDatabase, wasmExecutor: null });
  try {
    await runtime.start();
    await waitForRedisState(true, runtime.cluster);
    observe = true;
    const [metrics, ...responses] = await Promise.all([
      runtime.metricsResponse(),
      ...Array.from({ length: 12 }, () => runtime.publicApp.request("/readyz")),
    ]);
    expect(responses.every((response) => response.status === 200)).toBe(true);
    expect(await metrics.text()).toContain("gateway_ready 1");
    expect(probes).toBe(1);
    expect((await runtime.publicApp.request("/readyz")).status).toBe(200);
    expect(probes).toBe(1);

    databaseUnavailable = true;
    await Bun.sleep(300);
    const unavailable = await runtime.publicApp.request("/readyz");
    expect(unavailable.status).toBe(503);
    expect(await unavailable.json()).toEqual({ status: "db_unavailable" });
    expect((await runtime.publicApp.request("/readyz")).status).toBe(503);
    expect(probes).toBe(2);
    runtime.lifecycle.accepting = false;
    const draining = await runtime.publicApp.request("/readyz");
    expect(await draining.json()).toEqual({ status: "not_ready" });
    expect(probes).toBe(2);
  } finally {
    observe = false;
    await runtime.close();
  }
});

test("symptom: missing and completed video queries and cancellations bypass the shared project rate limit", async () => {
  const config = makeConfig({ redisNamespace: `stg_video_quota_${crypto.randomUUID()}` });
  const first = createGatewayRuntime({ config, db, wasmExecutor: null });
  const second = createGatewayRuntime({ config, db, wasmExecutor: null });
  try {
    await first.start();
    async function configure(path: string, body: Record<string, unknown>) {
      const { status, data } = await internalCall(first.internalApp, "PUT", path, body);
      expect(status).toBe(200);
      return data;
    }
    const project = await configure("/internal/v1/projects", { name: "video-quota", rpm: 3 });
    const other = await configure("/internal/v1/projects", { name: "other-video-quota", rpm: 1 });
    const primaryKey = await configure("/internal/v1/api-keys", {
      name: "video-quota",
      project_id: project.id,
      operations: ["videos"],
    });
    const otherKey = await configure("/internal/v1/api-keys", {
      name: "other-video-quota",
      project_id: other.id,
      operations: ["videos"],
    });
    const account = await configure("/internal/v1/upstream-accounts", { name: "video-quota" });
    const channel = await configure("/internal/v1/channels", {
      name: "video-quota",
      account_id: account.id,
      kind: "openai",
      base_url: "http://127.0.0.1:1",
      api_key: "local-fixture",
    });
    const repository = createAsyncTasksRepository(db);
    const task = (projectId: unknown, status: "QUEUED" | "SUCCEEDED") =>
      repository.createTask({
        id: `vid_${crypto.randomUUID().replaceAll("-", "")}`,
        request_id: crypto.randomUUID(),
        project_id: String(projectId),
        charge_id: null,
        api_key_id: null,
        channel_id: String(channel.id),
        model: "fixture-video",
        operation: "videos",
        upstream_task_id: "fixture-task",
        status,
      });
    const [completed, queued, otherQueued] = await Promise.all([
      task(project.id, "SUCCEEDED"),
      task(project.id, "QUEUED"),
      task(other.id, "QUEUED"),
    ]);
    await second.start();
    await waitForRedisState(true, first.cluster, second.cluster);
    const call = (node: typeof first, key: unknown, id: string, method: "GET" | "POST" = "GET") =>
      node.publicApp.request(`/v1/videos/${id}${method === "POST" ? "/cancel" : ""}`, {
        method,
        headers: { authorization: `Bearer ${key}` },
      });

    const missing = `vid_${crypto.randomUUID().replaceAll("-", "")}`;
    expect((await call(first, primaryKey.key, missing)).status).toBe(404);
    expect((await call(first, primaryKey.key, otherQueued.id)).status).toBe(404);
    const own = await call(second, primaryKey.key, completed.id);
    expect(own.status).toBe(200);
    expect(await own.json()).toMatchObject({ status: "SUCCEEDED" });

    const limited = await Promise.all([
      call(first, primaryKey.key, completed.id),
      call(second, primaryKey.key, queued.id, "POST"),
    ]);
    expect(limited.map((response) => response.status)).toEqual([429, 429]);
    for (const response of limited) {
      expect(await response.json()).toMatchObject({ error: { code: "rate_limit_rpm" } });
    }
    expect((await repository.getTask(queued.id))?.status).toBe("QUEUED");

    const cancelled = await call(second, otherKey.key, otherQueued.id, "POST");
    expect(cancelled.status).toBe(200);
    expect(await cancelled.json()).toMatchObject({ status: "CANCELLED" });
    expect((await call(first, otherKey.key, otherQueued.id)).status).toBe(429);
  } finally {
    await Promise.all([first.close(), second.close()]);
  }
});

test("Redis cluster syncs rate limits, circuit breaker OPEN state, and config epochs across Node A and Node B in real time", async () => {
  let primaryCalls = 0;
  let backupCalls = 0;

  const primaryServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      primaryCalls++;
      return Response.json({ error: { message: "primary down" } }, { status: 503 });
    },
  });
  const backupServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      backupCalls++;
      return Response.json({
        id: "cmpl_backup",
        object: "chat.completion",
        model: "backup-gpt",
        choices: [
          { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
        ],
        usage: { prompt_tokens: 5, completion_tokens: 5, total_tokens: 10 },
      });
    },
  });

  const nodeA = createGatewayRuntime({
    config: makeConfig(),
    db,
    routerOptions: { failureThreshold: 3, baseOpenMs: 30_000 },
  });
  const nodeB = createGatewayRuntime({
    config: makeConfig({ snapshotPollMs: 250 }),
    db,
    routerOptions: { failureThreshold: 3, baseOpenMs: 30_000 },
  });

  try {
    await nodeA.start();
    await nodeB.start();
    expect(nodeA.cluster.isRedisConnected()).toBe(true);
    expect(nodeB.cluster.isRedisConnected()).toBe(true);

    const { data: project } = await internalCall(
      nodeA.internalApp,
      "PUT",
      "/internal/v1/projects",
      {
        name: "cluster-proj",
        currency: "USD",
        rpm: 5,
        tpm: 1_000_000,
        concurrency: 10,
      },
    );
    const projectId = String(project.id);
    const { data: keyRes } = await internalCall(nodeA.internalApp, "PUT", "/internal/v1/api-keys", {
      project_id: projectId,
      name: "cluster-key",
    });
    const apiKey = String(keyRes.key);

    await internalCall(nodeA.internalApp, "POST", `/internal/v1/wallets/${projectId}/credits`, {
      currency: "USD",
      external_ref: "cluster-credit-1",
      amount_atoms: "1000000",
      kind: "CASH",
    });
    const { data: acc } = await internalCall(
      nodeA.internalApp,
      "PUT",
      "/internal/v1/upstream-accounts",
      {
        name: "cluster-acc",
      },
    );
    await internalCall(
      nodeA.internalApp,
      "POST",
      `/internal/v1/upstream-accounts/${acc.id}/recharges`,
      {
        currency: "USD",
        external_ref: "cluster-sup-1",
        amount_atoms: "1000000",
      },
    );

    const { data: chP } = await internalCall(nodeA.internalApp, "PUT", "/internal/v1/channels", {
      account_id: acc.id,
      name: "cluster-primary",
      kind: "openai",
      base_url: `http://127.0.0.1:${primaryServer.port}`,
      api_key: "sk-p",
    });
    const { data: chB } = await internalCall(nodeA.internalApp, "PUT", "/internal/v1/channels", {
      account_id: acc.id,
      name: "cluster-backup",
      kind: "openai",
      base_url: `http://127.0.0.1:${backupServer.port}`,
      api_key: "sk-b",
    });
    await internalCall(nodeA.internalApp, "PUT", "/internal/v1/models", {
      name: "cluster-gpt",
      operations: ["chat"],
      max_output_tokens: 64,
    });
    const { data: custPrice } = await internalCall(
      nodeA.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "10", denominator: "1" },
          output_tokens: { numerator: "20", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", output_tokens: "64" },
      },
    );
    const { data: supPrice } = await internalCall(
      nodeA.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "2", denominator: "1" },
          output_tokens: { numerator: "4", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", output_tokens: "64" },
      },
    );
    await internalCall(nodeA.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "cluster-gpt",
      channel_id: chP.id,
      upstream_model: "p-up",
      operations: ["chat"],
      priority: 10,
      billing_price_id: custPrice.id,
      cost_version_id: supPrice.id,
    });
    await internalCall(nodeA.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "cluster-gpt",
      channel_id: chB.id,
      upstream_model: "b-up",
      operations: ["chat"],
      priority: 5,
      billing_price_id: custPrice.id,
      cost_version_id: supPrice.id,
    });

    const { data: syncedRevision } = await internalCall(
      nodeA.internalApp,
      "PUT",
      "/internal/v1/plugins",
      {
        name: "redis-sync-vendor",
        version: "1.0.0",
        capability: "PROVIDER",
        kind: "DECLARATIVE",
        provider_config: {
          base_dialect: "openai_chat",
          usage_dialect: "openai",
          auth: { scheme: "bearer" },
          endpoints: { default: { path: "/v1/chat/completions" } },
        },
      },
    );

    const activated = await internalCall(
      nodeA.internalApp,
      "POST",
      "/internal/v1/plugin-releases",
      {
        id: crypto.randomUUID(),
        expected_epoch: syncedRevision.config_epoch,
        targets: [{ plugin_id: syncedRevision.id }],
      },
    );
    expect(activated.status, JSON.stringify(activated.data)).toBe(200);

    await Bun.sleep(100);
    expect(nodeB.snapshot.get().models.has("cluster-gpt")).toBe(true);
    expect(nodeB.snapshot.get().providerPluginsByName.has("redis-sync-vendor")).toBe(true);

    const originalPublish = nodeA.cluster.publishConfigEpoch.bind(nodeA.cluster);
    nodeA.cluster.publishConfigEpoch = () => undefined;
    try {
      const { status, data } = await internalCall(
        nodeA.internalApp,
        "POST",
        "/internal/v1/plugin-releases",
        {
          id: crypto.randomUUID(),
          expected_epoch: nodeA.snapshot.get().epoch,
          targets: [{ channel_id: chB.id, provider_plugin_id: syncedRevision.id }],
        },
      );
      expect(status).toBe(200);
      const deadline = performance.now() + 3000;
      while (
        nodeB.snapshot.get().epoch < Number(data.config_epoch) &&
        performance.now() < deadline
      ) {
        await Bun.sleep(20);
      }
      expect(nodeB.snapshot.get().channels.get(String(chB.id))?.provider_plugin_id).toBe(
        String(syncedRevision.id),
      );
    } finally {
      nodeA.cluster.publishConfigEpoch = originalPublish;
    }

    const callNode = (node: typeof nodeA) =>
      node.publicApp.fetch(
        new Request("http://gw.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "cluster-gpt",
            messages: [{ role: "user", content: "ping" }],
            max_tokens: 32,
          }),
        }),
      );

    for (let i = 0; i < 3; i++) {
      const r = await callNode(nodeA);
      expect(r.status).toBe(200);
      expect(r.headers.get("x-gateway-channel")).toBe("cluster-backup");
    }
    expect(primaryCalls).toBe(3);

    await Bun.sleep(80);
    const r4 = await callNode(nodeB);
    expect(r4.status).toBe(200);
    expect(r4.headers.get("x-gateway-channel")).toBe("cluster-backup");
    expect(primaryCalls).toBe(3);

    const r5 = await callNode(nodeA);
    expect(r5.status).toBe(200);
    expect(backupCalls).toBe(5);

    const r6 = await callNode(nodeB);
    expect(r6.status).toBe(429);
    const r6Json = (await r6.json()) as { error: { code: string } };
    expect(r6Json.error.code).toBe("rate_limit_rpm");

    const active = await nodeA.cluster.acquireRateLimit(
      "outage",
      {
        rpm: 100,
        tpm: 100,
        concurrency: 1,
      },
      1,
    );
    if (!active.ok) throw new Error("Could not acquire the permit before testing the outage");
    const redisExited = once(redisProc!, "exit");
    redisProc!.kill("SIGTERM");
    await redisExited;
    await waitForRedisState(false, nodeA.cluster, nodeB.cluster);
    expect(active.signal?.aborted).toBe(false);
    expect((await nodeA.publicApp.request("/readyz")).status).toBe(503);
    const unavailable = await callNode(nodeB);
    expect(unavailable.status).toBe(503);
    expect(await unavailable.json()).toMatchObject({ error: { code: "rate_limit_unavailable" } });

    await startEphemeralRedis(Number(new URL(redisUrl).port));
    await waitForRedisState(true, nodeA.cluster, nodeB.cluster);
    expect(active.signal?.aborted).toBe(true);
    expect((await nodeB.publicApp.request("/readyz")).status).toBe(200);
    expect((await callNode(nodeA)).status).toBe(200);
  } finally {
    await nodeA.close();
    await nodeB.close();
    await primaryServer.stop(true);
    await backupServer.stop(true);
  }
});

test("on_request TRANSFORM, multimodal endpoints (moderations, speech, transcriptions, images/edits), traceparent, paginated internal queries, and bounded multipart fingerprints", async () => {
  let lastUpstreamTraceparent: string | null = null;
  let lastUpstreamPrompt = "";
  const mock = createMockApp("media-ctrl");
  const mockServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      lastUpstreamTraceparent = req.headers.get("traceparent");
      if (req.url.endsWith("/openai/v1/chat/completions")) {
        const cloned = req.clone();
        const body = (await cloned.json()) as { messages?: { content?: string }[] };
        lastUpstreamPrompt = String(body.messages?.[0]?.content ?? "");
      }
      return mock.app.fetch(req);
    },
  });

  const mockWasm: WasmRuntimeExecutor = {
    runtimeKind: "mock",
    compile() {},
    async invoke(_id, hook, inputBytes) {
      const payload = JSON.parse(new TextDecoder().decode(inputBytes)) as { text: string };
      if (hook === "on_request" && payload.text.includes("alice@secret.io")) {
        return new TextEncoder().encode(
          JSON.stringify({
            action: "TRANSFORM",
            text: payload.text.replaceAll("alice@secret.io", "[REDACTED_EMAIL]"),
          }),
        );
      }
      return new TextEncoder().encode(JSON.stringify({ action: "PASS" }));
    },
    close() {},
  };

  const singleNodeCfg = makeConfig();
  delete singleNodeCfg.redisUrl;
  const runtime = createGatewayRuntime({
    config: singleNodeCfg,
    db,
    wasmExecutor: mockWasm,
  });

  try {
    await runtime.start();

    const badProject = await internalCall(runtime.internalApp, "PUT", "/internal/v1/projects", {
      name: "",
      currency: "invalid",
    });
    expect(badProject.status).toBe(400);
    expect((badProject.data.error as { code: string }).code).toBe("validation_error");

    const { data: project } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/projects",
      {
        name: "media-proj",
        currency: "USD",
      },
    );
    const projectId = String(project.id);
    const { data: keyRes } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/api-keys",
      {
        project_id: projectId,
        name: "media-key",
      },
    );
    const apiKey = String(keyRes.key);
    await internalCall(runtime.internalApp, "POST", `/internal/v1/wallets/${projectId}/credits`, {
      currency: "USD",
      external_ref: "media-credit-1",
      amount_atoms: "1000000",
      kind: "CASH",
    });
    const { data: acc } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/upstream-accounts",
      {
        name: "media-acc",
      },
    );
    await internalCall(
      runtime.internalApp,
      "POST",
      `/internal/v1/upstream-accounts/${acc.id}/recharges`,
      {
        currency: "USD",
        external_ref: "media-sup-1",
        amount_atoms: "1000000",
      },
    );
    const { data: ch } = await internalCall(runtime.internalApp, "PUT", "/internal/v1/channels", {
      account_id: acc.id,
      name: "media-openai",
      kind: "openai",
      base_url: `http://127.0.0.1:${mockServer.port}/openai`,
      api_key: "sk-media",
    });

    const builtin = getBuiltinPlugin("pii-guard");
    if (builtin?.capability !== "CONTENT") throw new Error("Missing content plugin fixture");
    const { data: plugin } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/plugins",
      {
        name: "req-redactor",
        version: "1.0.0",
        kind: "WASM",
        wasm_bytes_base64: Buffer.from(builtin.wasm_bytes).toString("base64"),
        manifest: { hooks: ["on_request"] },
      },
    );

    const activated = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/plugin-releases",
      {
        id: crypto.randomUUID(),
        expected_epoch: plugin.config_epoch,
        targets: [{ plugin_id: plugin.id }],
        samples: { [String(plugin.id)]: { text: "Email me at alice@secret.io please" } },
      },
    );
    expect(activated.status, JSON.stringify(activated.data)).toBe(200);

    const { data: tokenCustPrice } = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "10", denominator: "1" },
          cached_input_tokens: { numerator: "5", denominator: "1" },
          output_tokens: { numerator: "20", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "100" },
      },
    );
    const { data: tokenSupPrice } = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "2", denominator: "1" },
          cached_input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: "4", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "100" },
      },
    );
    const { data: charCustPrice } = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "CUSTOMER",
        currency: "USD",
        basis: "INPUT_CHARACTERS",
        rates: { characters: { numerator: "3", denominator: "1" } },
        ceilings: { characters: "10000" },
      },
    );
    const { data: charSupPrice } = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "SUPPLIER",
        currency: "USD",
        basis: "INPUT_CHARACTERS",
        rates: { characters: { numerator: "1", denominator: "1" } },
        ceilings: { characters: "10000" },
      },
    );
    const { data: audioSecCustPrice } = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "CUSTOMER",
        currency: "USD",
        rates: { audio_seconds: { numerator: "80", denominator: "1" } },
        ceilings: { audio_seconds: "600" },
      },
    );
    const { data: audioSecSupPrice } = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "SUPPLIER",
        currency: "USD",
        rates: { audio_seconds: { numerator: "16", denominator: "1" } },
        ceilings: { audio_seconds: "600" },
      },
    );

    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "omni-model",
      operations: ["chat", "moderations", "image_edits"],
      max_output_tokens: 100,
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "tts-1",
      operations: ["speech"],
      max_output_tokens: 100,
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "whisper-1",
      operations: ["transcription"],
      max_output_tokens: 100,
    });

    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "omni-model",
      channel_id: ch.id,
      upstream_model: "omni-up",
      operations: ["chat", "moderations", "image_edits"],
      billing_price_id: tokenCustPrice.id,
      cost_version_id: tokenSupPrice.id,
      plugin_pipeline: [{ plugin_id: String(plugin.id), priority: 1 }],
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "tts-1",
      channel_id: ch.id,
      upstream_model: "tts-up",
      operations: ["speech"],
      billing_price_id: charCustPrice.id,
      cost_version_id: charSupPrice.id,
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "whisper-1",
      channel_id: ch.id,
      upstream_model: "whisper-up",
      operations: ["transcription"],
      billing_price_id: audioSecCustPrice.id,
      cost_version_id: audioSecSupPrice.id,
    });

    const incomingTrace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const chatResp = await runtime.publicApp.fetch(
      new Request("http://gw.local/v1/chat/completions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${apiKey}`,
          "content-type": "application/json",
          traceparent: incomingTrace,
        },
        body: JSON.stringify({
          model: "omni-model",
          messages: [{ role: "user", content: "Email me at alice@secret.io please" }],
          max_tokens: 50,
        }),
      }),
    );
    expect(chatResp.status).toBe(200);
    expect(lastUpstreamPrompt).toBe("Email me at [REDACTED_EMAIL] please");
    expect(chatResp.headers.get("x-trace-id")).toBe("4bf92f3577b34da6a3ce929d0e0e4736");
    expect(lastUpstreamTraceparent).toStartWith("00-4bf92f3577b34da6a3ce929d0e0e4736-");
    const chatReqId = chatResp.headers.get("x-request-id")!;

    const modResp = await runtime.fetchPublic(
      new Request("http://gw.local/v1/moderations", {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
        body: JSON.stringify({ model: "omni-model", input: "safe text" }),
      }),
      { getPeerAddress: () => "127.0.0.1" },
    );
    expect(modResp.status).toBe(200);
    const livezResp = await runtime.fetchPublic(new Request("http://gw.local/livez"));
    expect(livezResp.status).toBe(200);

    const speechBin = await runtime.publicApp.fetch(
      new Request("http://gw.local/v1/audio/speech", {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
        body: JSON.stringify({ model: "tts-1", input: "Hello audio!" }),
      }),
    );
    expect(speechBin.status).toBe(200);
    expect(speechBin.headers.get("content-type")).toContain("audio/mpeg");
    expect(await speechBin.text()).toBe("mock-generated-audio-content");

    const form = new FormData();
    form.append("model", "whisper-1");
    form.append("stream", "true");
    form.append("file", new File(["fake-wav-bytes"], "sample.wav", { type: "audio/wav" }));
    const transcribeSse = await runtime.publicApp.fetch(
      new Request("http://gw.local/v1/audio/transcriptions", {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}` },
        body: form,
      }),
    );
    expect(transcribeSse.status).toBe(200);
    const sseBody = await transcribeSse.text();
    expect(sseBody).toContain("transcript.text.done");

    const editForm = new FormData();
    editForm.append("model", "omni-model");
    editForm.append("prompt", "make it brighter");
    editForm.append("image", new File(["png-bytes"], "in.png", { type: "image/png" }));
    const editResp = await runtime.publicApp.fetch(
      new Request("http://gw.local/v1/images/edits", {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}` },
        body: editForm,
      }),
    );
    expect(editResp.status).toBe(200);
    const large = new FormData();
    large.append("model", "whisper-1");
    large.append("file", new File(["short"], "voice.wav", { type: "audio/wav" }));
    large.append("prompt", "x".repeat(2 * 1024 * 1024));
    const tooLarge = await runtime.publicApp.fetch(
      new Request("http://gw.local/v1/audio/transcriptions", {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}` },
        body: large,
      }),
    );
    expect(tooLarge.status).toBe(413);

    const metricsRes = await runtime.publicApp.fetch(new Request("http://gw.local/metrics"));
    const metricsTxt = await metricsRes.text();
    expect(metricsTxt).toContain("gateway_request_duration_ms_bucket");
    expect(metricsTxt).toContain("gateway_ttft_ms_bucket");
    expect(metricsTxt).toContain('gateway_http_requests_total{operation="chat"');
    expect(metricsTxt).toContain('gateway_http_requests_total{operation="speech"');

    const { data: chargesList } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/charges?project_id=${projectId}&limit=10`,
    );
    expect(chargesList.total).toBe(5);
    expect((chargesList.items as unknown[]).length).toBe(5);
    const transcribeWithKey = (bytes: string) => {
      const fileForm = new FormData();
      fileForm.append("model", "whisper-1");
      fileForm.append("file", new File([bytes], "voice.wav", { type: "audio/wav" }));
      return runtime.publicApp.fetch(
        new Request("http://gw.local/v1/audio/transcriptions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "idempotency-key": "same-file-name" },
          body: fileForm,
        }),
      );
    };
    expect((await transcribeWithKey("voice A")).status).toBe(200);
    const differentFile = await transcribeWithKey("voice B");
    expect(differentFile.status).toBe(409);
    expect(await differentFile.json()).toMatchObject({ error: { code: "idempotency_conflict" } });

    const { data: reqDetail } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/requests/${chatReqId}`,
    );
    expect(reqDetail.hold).toBeTruthy();
    expect(reqDetail.charge).toBeTruthy();
    expect((reqDetail.entries as unknown[]).length).toBeGreaterThanOrEqual(4);

    const { data: entriesList } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/ledger-entries?request_id=${chatReqId}`,
    );
    expect(entriesList.total).toBeGreaterThanOrEqual(4);

    const { data: audit } = await internalCall(
      runtime.internalApp,
      "GET",
      "/internal/v1/audit/conservation",
    );
    expect(audit.ok).toBe(true);
    expect(audit.open_holds).toBe(0);
    expect(audit.unknown_holds).toBe(0);
  } finally {
    await runtime.close();
    await mockServer.stop(true);
  }
});

test("Redis script cache eviction keeps renewals and shared admission limits intact", async () => {
  const redis = new Bun.RedisClient(redisUrl, { autoReconnect: false });
  const namespace = `script-cache-${crypto.randomUUID()}`;
  const first = new ClusterCoordinator({ redisUrl, namespace });
  const second = new ClusterCoordinator({ redisUrl, namespace });
  const limits = { rpm: 2, tpm: 6, concurrency: 1 };
  try {
    await Promise.all([first.start(), second.start(), redis.connect()]);
    const permit = await first.acquireRateLimit("test", limits, 3, 150);
    if (!permit.ok) throw new Error(`Initial Redis permit failed: ${permit.reason}`);
    try {
      await redis.send("SCRIPT", ["FLUSH", "SYNC"]);
      await Bun.sleep(260);
      expect(permit.signal?.aborted).toBe(false);
      expect(await second.acquireRateLimit("test", limits, 3, 150)).toEqual({
        ok: false,
        reason: "concurrency",
      });
    } finally {
      permit.release();
    }
    let admitted;
    for (let attempt = 0; attempt < 10; attempt++) {
      admitted = await second.acquireRateLimit("test", limits, 3, 150);
      if (admitted.ok) break;
      await Bun.sleep(10);
    }
    if (!admitted?.ok) throw new Error("Released Redis permit continued to block admission");
    admitted.release();
    expect(await redis.send("GET", [`${namespace}:rl:rpm:test`])).toBe("2");
    expect(await redis.send("GET", [`${namespace}:rl:tpm:test`])).toBe("6");
  } finally {
    redis.close();
    await Promise.all([first.close(), second.close()]);
  }
});

test.each([60_000, 90])(
  "a live Redis concurrency permit with %i ms TTL remains visible to another Gateway",
  async (leaseMs) => {
    const namespace = `concurrency-${crypto.randomUUID()}`;
    const nodeA = new ClusterCoordinator({ redisUrl, namespace });
    const nodeB = new ClusterCoordinator({ redisUrl, namespace });
    const limits = { rpm: 100, tpm: 100, concurrency: 1 };
    try {
      await nodeA.start();
      await nodeB.start();
      const first = await nodeA.acquireRateLimit("active-project", limits, 1, leaseMs);
      if (!first.ok) throw new Error("First concurrency permit rejected");
      try {
        await Bun.sleep(150);
        expect(await nodeB.acquireRateLimit("active-project", limits, 1, leaseMs)).toEqual({
          ok: false,
          reason: "concurrency",
        });
      } finally {
        first.release();
      }
    } finally {
      await nodeB.close();
      await nodeA.close();
    }
  },
);

test("batched Redis permits flush pending releases before dispatching new acquires", async () => {
  const redis = new Bun.RedisClient(redisUrl, { autoReconnect: false });
  const namespace = `batch-rel-acq-${crypto.randomUUID()}`;
  const coord = new ClusterCoordinator({ redisUrl, namespace });
  const holdPermits: { release: () => void }[] = [];
  try {
    await Promise.all([coord.start(), redis.connect()]);
    for (let i = 0; i < 6; i++) {
      const p = await coord.acquireRateLimit(
        `bg-${i}`,
        { rpm: 1000, tpm: 1000, concurrency: 10 },
        1,
        60_000,
      );
      if (!p.ok) throw new Error(`Background permit failed: ${p.reason}`);
      holdPermits.push(p);
    }
    const tight = { rpm: 1000, tpm: 1000, concurrency: 1 };
    for (let round = 0; round < 15; round++) {
      if (round === 5) await redis.send("SCRIPT", ["FLUSH", "SYNC"]);
      const [pA, pB] = await Promise.all([
        coord.acquireRateLimit("tight-slot-a", tight, 1, 60_000),
        coord.acquireRateLimit("tight-slot-b", tight, 1, 60_000),
      ]);
      expect(pA.ok).toBe(true);
      expect(pB.ok).toBe(true);
      if (pA.ok) pA.release();
      if (pB.ok) pB.release();
    }
  } finally {
    for (const p of holdPermits) p.release();
    redis.close();
    await coord.close();
  }
});

test("transient Redis socket drop preserves live permits, flushes offline releases on reconnect, and revokes permits before expiry if Redis stays down", async () => {
  const admin = new Bun.RedisClient(redisUrl, { autoReconnect: false });
  const namespace = `resilience-${crypto.randomUUID()}`;
  const coordA = new ClusterCoordinator({ redisUrl, namespace });
  const coordB = new ClusterCoordinator({ redisUrl, namespace });
  const limits = { rpm: 10_000, tpm: 10_000, concurrency: 2 };
  try {
    await Promise.all([coordA.start(), coordB.start(), admin.connect()]);
    const live = await coordA.acquireRateLimit("stream-key", limits, 1, 10_000);
    const releasedOffline = await coordA.acquireRateLimit("stream-key", limits, 1, 10_000);
    if (!live.ok || !releasedOffline.ok) throw new Error("Initial permits failed");
    expect(await coordB.acquireRateLimit("stream-key", limits, 1, 10_000)).toEqual({
      ok: false,
      reason: "concurrency",
    });

    await admin.send("CLIENT", ["KILL", "TYPE", "normal", "SKIPME", "yes"]);
    await waitForRedisState(false, coordA);
    expect(live.signal?.aborted).toBe(false);
    releasedOffline.release();

    await waitForRedisState(true, coordA, coordB);
    expect(live.signal?.aborted).toBe(false);
    const concKey = `${namespace}:rl:conc:stream-key`;
    expect(Number(await admin.send("ZCARD", [concKey]))).toBe(1);
    const reopened = await coordB.acquireRateLimit("stream-key", limits, 1, 10_000);
    expect(reopened.ok).toBe(true);
    if (reopened.ok) reopened.release();
    live.release();

    const batchPermits = await Promise.all(
      Array.from({ length: 300 }, (_, i) =>
        coordA.acquireRateLimit(
          `batch-${i % 10}`,
          { rpm: 10_000, tpm: 10_000, concurrency: 100 },
          1,
          1200,
        ),
      ),
    );
    expect(batchPermits.every((p) => p.ok)).toBe(true);
    await Bun.sleep(1500);
    for (const p of batchPermits) {
      if (p.ok) expect(p.signal?.aborted).toBe(false);
    }
    for (const p of batchPermits) {
      if (p.ok) p.release();
    }

    const shortPermit = await coordA.acquireRateLimit(
      "expiry-cutoff",
      { rpm: 100, tpm: 100, concurrency: 1 },
      1,
      450,
    );
    if (!shortPermit.ok) throw new Error("Short permit failed");
    const redisExited = once(redisProc!, "exit");
    redisProc!.kill("SIGTERM");
    await redisExited;
    await waitForRedisState(false, coordA);
    const abortDeadline = Date.now() + 900;
    while (!shortPermit.signal?.aborted && Date.now() < abortDeadline) {
      await Bun.sleep(15);
    }
    expect(shortPermit.signal?.aborted).toBe(true);

    await startEphemeralRedis(Number(new URL(redisUrl).port));
    await waitForRedisState(true, coordA, coordB);
  } finally {
    admin.close();
    await Promise.all([coordA.close(), coordB.close()]);
  }
});

test("declarative MiniMax-style routes cover Anthropic, OpenAI, speech, transcription, images, music, videos, and HTTP-200 base_resp error hold release", async () => {
  const seen: { path: string; auth: string | null; xApiKey: string | null; model?: unknown }[] = [];
  const mockUpstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      const auth = req.headers.get("authorization");
      const xApiKey = req.headers.get("x-api-key");
      const contentType = req.headers.get("content-type") ?? "";
      if (url.pathname === "/anthropic/v1/messages") {
        const body = (await req.json()) as Record<string, unknown>;
        seen.push({ path: url.pathname, auth, xApiKey, model: body.model });
        if (body.stream === true) {
          const sse = [
            `event: message_start\ndata: ${JSON.stringify({ type: "message_start", message: { id: "msg_mm_1", type: "message", role: "assistant", model: String(body.model), content: [], usage: { input_tokens: 12, output_tokens: 0 } } })}\n\n`,
            `event: content_block_start\ndata: ${JSON.stringify({ type: "content_block_start", index: 0, content_block: { type: "text", text: "" } })}\n\n`,
            `event: content_block_delta\ndata: ${JSON.stringify({ type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "ok-anth" } })}\n\n`,
            `event: content_block_stop\ndata: ${JSON.stringify({ type: "content_block_stop", index: 0 })}\n\n`,
            `event: message_delta\ndata: ${JSON.stringify({ type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 6 } })}\n\n`,
            `event: message_stop\ndata: ${JSON.stringify({ type: "message_stop" })}\n\n`,
          ].join("");
          return new Response(sse, { headers: { "content-type": "text/event-stream" } });
        }
        return Response.json({
          id: "msg_mm_1",
          type: "message",
          role: "assistant",
          model: String(body.model),
          content: [{ type: "text", text: "hello-from-minimax-anthropic" }],
          stop_reason: "end_turn",
          usage: { input_tokens: 12, output_tokens: 8 },
        });
      }
      if (url.pathname === "/v1/chat/completions") {
        const body = (await req.json()) as Record<string, unknown>;
        seen.push({ path: url.pathname, auth, xApiKey, model: body.model });
        if (
          Array.isArray(body.messages) &&
          String((body.messages[0] as { content?: string })?.content ?? "").includes("TRIGGER_2049")
        ) {
          return Response.json({
            base_resp: { status_code: 2049, status_msg: "invalid api key" },
          });
        }
        if (body.stream === true) {
          const sse = [
            `data: ${JSON.stringify({ id: "cmpl_mm_1", object: "chat.completion.chunk", model: String(body.model), choices: [{ index: 0, delta: { role: "assistant", content: "ok-oai" }, finish_reason: null }] })}\n\n`,
            `data: ${JSON.stringify({ id: "cmpl_mm_1", object: "chat.completion.chunk", model: String(body.model), choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 10, completion_tokens: 4, total_tokens: 14 } })}\n\n`,
            "data: [DONE]\n\n",
          ].join("");
          return new Response(sse, { headers: { "content-type": "text/event-stream" } });
        }
        return Response.json({
          id: "cmpl_mm_1",
          object: "chat.completion",
          model: String(body.model),
          choices: [
            {
              index: 0,
              message: { role: "assistant", content: "hello-from-minimax-openai" },
              finish_reason: "stop",
            },
          ],
          usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
          base_resp: { status_code: 0, status_msg: "success" },
        });
      }
      if (url.pathname === "/v1/t2a_v2") {
        const body = (await req.json()) as Record<string, unknown>;
        seen.push({ path: url.pathname, auth, xApiKey, model: body.model });
        return Response.json({
          data: { audio: "494433030000", status: 2 },
          extra_info: { audio_length: 2500, usage_characters: 11 },
          base_resp: { status_code: 0, status_msg: "success" },
        });
      }
      if (url.pathname === "/v1/speech_to_text") {
        const model = contentType.includes("multipart")
          ? String((await req.formData()).get("model"))
          : String(((await req.json()) as Record<string, unknown>).model);
        seen.push({ path: url.pathname, auth, xApiKey, model });
        return Response.json({
          text: "transcribed-by-minimax",
          extra_info: { audio_length: 3000 },
          base_resp: { status_code: 0, status_msg: "success" },
        });
      }
      if (url.pathname === "/v1/image_generation") {
        const body = (await req.json()) as Record<string, unknown>;
        seen.push({ path: url.pathname, auth, xApiKey, model: body.model });
        return Response.json({
          id: "img_mm_1",
          data: { image_urls: ["https://cdn.example/img1.png"] },
          metadata: { success_count: 2, failed_count: 0 },
          base_resp: { status_code: 0, status_msg: "success" },
        });
      }
      if (url.pathname === "/v1/music_generation") {
        const body = (await req.json()) as Record<string, unknown>;
        seen.push({ path: url.pathname, auth, xApiKey, model: body.model });
        return Response.json({
          data: { audio: "https://cdn.example/song.mp3", status: 2 },
          extra_info: { music_duration: 12500 },
          base_resp: { status_code: 0, status_msg: "success" },
        });
      }
      if (url.pathname === "/v2/video_generation") {
        const body = (await req.json()) as Record<string, unknown>;
        seen.push({ path: url.pathname, auth, xApiKey, model: body.model });
        return Response.json({
          task_id: "vid_task_12345",
          base_resp: { status_code: 0, status_msg: "success" },
        });
      }
      return Response.json({ error: `unexpected path ${url.pathname}` }, { status: 404 });
    },
  });

  const cfg = makeConfig();
  delete cfg.redisUrl;
  const runtime = createGatewayRuntime({ config: cfg, db, wasmExecutor: null });

  try {
    await runtime.start();
    const { data: project } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/projects",
      {
        name: "mm-mock-proj",
        currency: "USD",
      },
    );
    const projectId = String(project.id);
    const { data: keyRes } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/api-keys",
      {
        project_id: projectId,
        name: "mm-mock-key",
      },
    );
    const apiKey = String(keyRes.key);
    await internalCall(runtime.internalApp, "POST", `/internal/v1/wallets/${projectId}/credits`, {
      currency: "USD",
      external_ref: "mm-mock-credit",
      amount_atoms: "10000000",
      kind: "CASH",
    });
    const { data: acc } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/upstream-accounts",
      { name: "mm-mock-acc" },
    );
    await internalCall(
      runtime.internalApp,
      "POST",
      `/internal/v1/upstream-accounts/${acc.id}/recharges`,
      { currency: "USD", external_ref: "mm-mock-sup", amount_atoms: "10000000" },
    );

    const mmErrorRules = [
      {
        when_http_status: [200],
        path: "base_resp.status_code",
        equals: 1002,
        retry_kind: "rate_limit",
        message_path: "base_resp.status_msg",
      },
      {
        when_http_status: [200],
        path: "base_resp.status_code",
        not_equals: 0,
        retry_kind: "client_error",
        http_status: 400,
        message_path: "base_resp.status_msg",
      },
    ];

    const { data: mmPlugin } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/plugins",
      {
        name: "minimax_suite",
        version: "1.0.0",
        capability: "PROVIDER",
        kind: "DECLARATIVE",
        provider_config: {
          base_dialect: "openai_chat",
          usage_dialect: "openai",
          stream_framing: "sse",
          auth: { scheme: "bearer" },
          include_stream_usage_option: true,
          error_rules: mmErrorRules,
          usage_aliases: [
            { target: "images", source_path: "metadata.success_count" },
            { target: "music_ms", source_path: "extra_info.music_duration" },
            { target: "characters", source_path: "extra_info.usage_characters" },
            { target: "audio_ms", source_path: "extra_info.audio_length" },
          ],
          endpoints: {
            chat: { path: "/v1/chat/completions" },
            speech: { path: "/v1/t2a_v2" },
            transcription: { path: "/v1/speech_to_text" },
            images: { path: "/v1/image_generation" },
            music: { path: "/v1/music_generation" },
            videos: { path: "/v2/video_generation" },
            default: { path: "/v1/chat/completions" },
          },
        },
      },
    );
    const rel = await internalCall(runtime.internalApp, "POST", "/internal/v1/plugin-releases", {
      id: crypto.randomUUID(),
      expected_epoch: mmPlugin.config_epoch,
      targets: [{ plugin_id: mmPlugin.id }],
    });
    expect(rel.status, JSON.stringify(rel.data)).toBe(200);

    const { data: chAnth } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/channels",
      {
        account_id: acc.id,
        name: "mm-anthropic",
        kind: "anthropic",
        base_url: `http://127.0.0.1:${mockUpstream.port}/anthropic`,
        api_key: "sk-mm-test",
      },
    );
    const { data: chSuite } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/channels",
      {
        account_id: acc.id,
        name: "mm-suite",
        kind: "minimax_suite",
        provider_plugin_id: mmPlugin.id,
        base_url: `http://127.0.0.1:${mockUpstream.port}`,
        api_key: "sk-mm-test",
      },
    );

    const mkPrice = async (
      side: "CUSTOMER" | "SUPPLIER",
      basis: "PROVIDER_USAGE" | "REQUEST_COMPLETION" | "INPUT_CHARACTERS",
      rates: Record<string, { numerator: string; denominator: string }>,
      ceilings: Record<string, string>,
    ) => {
      const { data } = await internalCall(runtime.internalApp, "POST", "/internal/v1/prices", {
        side,
        currency: "USD",
        basis,
        rates,
        ceilings,
      });
      return String(data.id);
    };

    const tokCust = await mkPrice(
      "CUSTOMER",
      "PROVIDER_USAGE",
      {
        input_tokens: { numerator: "10", denominator: "1" },
        output_tokens: { numerator: "20", denominator: "1" },
      },
      { input_tokens: "1000", output_tokens: "200" },
    );
    const tokSup = await mkPrice(
      "SUPPLIER",
      "PROVIDER_USAGE",
      {
        input_tokens: { numerator: "2", denominator: "1" },
        output_tokens: { numerator: "4", denominator: "1" },
      },
      { input_tokens: "1000", output_tokens: "200" },
    );
    const speechCust = await mkPrice(
      "CUSTOMER",
      "PROVIDER_USAGE",
      { characters: { numerator: "5", denominator: "1" } },
      { characters: "10000" },
    );
    const speechSup = await mkPrice(
      "SUPPLIER",
      "PROVIDER_USAGE",
      { characters: { numerator: "1", denominator: "1" } },
      { characters: "10000" },
    );
    const sttCust = await mkPrice(
      "CUSTOMER",
      "PROVIDER_USAGE",
      { audio_ms: { numerator: "1", denominator: "10" } },
      { audio_ms: "600000" },
    );
    const sttSup = await mkPrice(
      "SUPPLIER",
      "PROVIDER_USAGE",
      { audio_ms: { numerator: "1", denominator: "100" } },
      { audio_ms: "600000" },
    );
    const imgCust = await mkPrice(
      "CUSTOMER",
      "PROVIDER_USAGE",
      { images: { numerator: "500", denominator: "1" } },
      { images: "10" },
    );
    const imgSup = await mkPrice(
      "SUPPLIER",
      "PROVIDER_USAGE",
      { images: { numerator: "100", denominator: "1" } },
      { images: "10" },
    );
    const musCust = await mkPrice(
      "CUSTOMER",
      "PROVIDER_USAGE",
      { music_ms: { numerator: "8", denominator: "1000" } },
      { music_ms: "300000" },
    );
    const musSup = await mkPrice(
      "SUPPLIER",
      "PROVIDER_USAGE",
      { music_ms: { numerator: "2", denominator: "1000" } },
      { music_ms: "300000" },
    );
    const vidCust = await mkPrice(
      "CUSTOMER",
      "REQUEST_COMPLETION",
      { requests: { numerator: "1000", denominator: "1" } },
      { requests: "1" },
    );
    const vidSup = await mkPrice(
      "SUPPLIER",
      "REQUEST_COMPLETION",
      { requests: { numerator: "200", denominator: "1" } },
      { requests: "1" },
    );

    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "MiniMax-M3.1-Flash",
      operations: ["chat", "messages"],
      max_output_tokens: 200,
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "Speech-2.8-HD",
      operations: ["speech", "transcription"],
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "MiniMax-H3-Image",
      operations: ["images"],
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "music-3.0",
      operations: ["music"],
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "MiniMax-H3",
      operations: ["videos"],
    });

    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "MiniMax-M3.1-Flash",
      channel_id: chAnth.id,
      upstream_model: "MiniMax-M3.1-Flash",
      operations: ["messages"],
      billing_price_id: tokCust,
      cost_version_id: tokSup,
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "MiniMax-M3.1-Flash",
      channel_id: chSuite.id,
      upstream_model: "MiniMax-M3.1-Flash",
      operations: ["chat"],
      billing_price_id: tokCust,
      cost_version_id: tokSup,
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "Speech-2.8-HD",
      channel_id: chSuite.id,
      upstream_model: "speech-2.8-hd",
      operations: ["speech"],
      billing_price_id: speechCust,
      cost_version_id: speechSup,
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "Speech-2.8-HD",
      channel_id: chSuite.id,
      upstream_model: "speech-2.8-hd",
      operations: ["transcription"],
      billing_price_id: sttCust,
      cost_version_id: sttSup,
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "MiniMax-H3-Image",
      channel_id: chSuite.id,
      upstream_model: "MiniMax-H3-Image",
      operations: ["images"],
      billing_price_id: imgCust,
      cost_version_id: imgSup,
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "music-3.0",
      channel_id: chSuite.id,
      upstream_model: "music-3.0",
      operations: ["music"],
      billing_price_id: musCust,
      cost_version_id: musSup,
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "MiniMax-H3",
      channel_id: chSuite.id,
      upstream_model: "MiniMax-H3",
      operations: ["videos"],
      billing_price_id: vidCust,
      cost_version_id: vidSup,
    });

    const postJson = (path: string, body: unknown, headers: Record<string, string> = {}) =>
      runtime.fetchPublic(
        new Request(`http://gw.local${path}`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${apiKey}`,
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify(body),
        }),
      );

    const anthRes = await postJson(
      "/v1/messages",
      {
        model: "MiniMax-M3.1-Flash",
        max_tokens: 64,
        messages: [{ role: "user", content: "hi" }],
      },
      { "x-api-key": apiKey },
    );
    expect(anthRes.status).toBe(200);
    expect(await anthRes.json()).toMatchObject({
      content: [{ type: "text", text: "hello-from-minimax-anthropic" }],
    });

    const anthStream = await postJson("/v1/messages", {
      model: "MiniMax-M3.1-Flash",
      max_tokens: 64,
      stream: true,
      messages: [{ role: "user", content: "hi" }],
    });
    expect(anthStream.status).toBe(200);
    expect(await anthStream.text()).toContain("ok-anth");

    const oaiRes = await postJson("/v1/chat/completions", {
      model: "MiniMax-M3.1-Flash",
      messages: [{ role: "user", content: "hi" }],
      max_tokens: 64,
    });
    expect(oaiRes.status).toBe(200);
    expect(await oaiRes.json()).toMatchObject({
      choices: [{ message: { content: "hello-from-minimax-openai" } }],
    });

    const oaiStream = await postJson("/v1/chat/completions", {
      model: "MiniMax-M3.1-Flash",
      messages: [{ role: "user", content: "hi" }],
      max_tokens: 64,
      stream: true,
    });
    expect(oaiStream.status).toBe(200);
    expect(await oaiStream.text()).toContain("ok-oai");

    const speechRes = await postJson("/v1/audio/speech", {
      model: "Speech-2.8-HD",
      text: "Hello world",
      voice_setting: { voice_id: "English_expressive_narrator" },
    });
    expect(speechRes.status).toBe(200);
    expect(await speechRes.json()).toMatchObject({ data: { status: 2 } });

    const sttForm = new FormData();
    sttForm.append("model", "Speech-2.8-HD");
    sttForm.append("file", new File(["wav-bytes"], "clip.wav", { type: "audio/wav" }));
    const sttRes = await runtime.fetchPublic(
      new Request("http://gw.local/v1/audio/transcriptions", {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}` },
        body: sttForm,
      }),
    );
    expect(sttRes.status).toBe(200);
    expect(await sttRes.json()).toMatchObject({ text: "transcribed-by-minimax" });

    const imgRes = await postJson("/v1/images/generations", {
      model: "MiniMax-H3-Image",
      prompt: "orange cat",
      n: 2,
    });
    expect(imgRes.status).toBe(200);
    expect(await imgRes.json()).toMatchObject({ metadata: { success_count: 2 } });

    const musRes = await postJson("/v1/music/generations", {
      model: "music-3.0",
      prompt: "acoustic folk",
      lyrics: "[verse]\nhello",
    });
    expect(musRes.status).toBe(200);
    expect(await musRes.json()).toMatchObject({ extra_info: { music_duration: 12500 } });

    const vidRes = await postJson("/v1/videos", {
      model: "MiniMax-H3",
      prompt: "cinematic sunset",
    });
    expect(vidRes.status).toBe(200);
    expect(await vidRes.json()).toMatchObject({ task_id: "vid_task_12345" });

    const { data: walletBeforeErr } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/wallets/${projectId}?currency=USD`,
    );

    const errRes = await postJson("/v1/chat/completions", {
      model: "MiniMax-M3.1-Flash",
      messages: [{ role: "user", content: "TRIGGER_2049" }],
      max_tokens: 64,
    });
    expect(errRes.status).toBe(400);
    expect(await errRes.json()).toMatchObject({
      error: { code: "upstream_client_error", message: "invalid api key" },
    });

    const { data: walletAfterErr } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/wallets/${projectId}?currency=USD`,
    );
    expect(walletAfterErr.available_atoms).toBe(walletBeforeErr.available_atoms);
    expect(walletAfterErr.spent_atoms).toBe("3355");

    const { data: audit } = await internalCall(
      runtime.internalApp,
      "GET",
      "/internal/v1/audit/conservation",
    );
    expect(audit.ok).toBe(true);
    expect(audit.open_holds).toBe(0);
    expect(seen.map((s) => s.path)).toEqual([
      "/anthropic/v1/messages",
      "/anthropic/v1/messages",
      "/v1/chat/completions",
      "/v1/chat/completions",
      "/v1/t2a_v2",
      "/v1/speech_to_text",
      "/v1/image_generation",
      "/v1/music_generation",
      "/v2/video_generation",
      "/v1/chat/completions",
    ]);
  } finally {
    await runtime.close();
    await mockUpstream.stop(true);
  }
});
