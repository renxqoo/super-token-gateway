import { afterAll, beforeAll, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SQL } from "bun";
import { ClusterCoordinator } from "@gateway/runtime";
import { createDatabase, migrate } from "@gateway/ledger";
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
    expect(active.signal?.aborted).toBe(true);
    expect((await nodeA.publicApp.request("/readyz")).status).toBe(503);
    const unavailable = await callNode(nodeB);
    expect(unavailable.status).toBe(503);
    expect(await unavailable.json()).toMatchObject({ error: { code: "rate_limit_unavailable" } });

    await startEphemeralRedis(Number(new URL(redisUrl).port));
    await waitForRedisState(true, nodeA.cluster, nodeB.cluster);
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
