import { afterAll, beforeAll, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SQL } from "bun";
import { createDatabase, migrate } from "@gateway/ledger";
import type { WasmRuntimeExecutor } from "@gateway/plugin-engine";
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

async function startEphemeralRedis(): Promise<string> {
  const redisBin =
    process.env.DURABILITY_REDIS_SERVER ??
    new URL("../../.local/toolchain/redis74/bin/redis-server", import.meta.url).pathname;
  const port = await findFreePort();
  redisDir = await mkdtemp(join(tmpdir(), "stg-redis-test-"));
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
    try {
      const client = new Bun.RedisClient(url, { connectionTimeout: 300, autoReconnect: false });
      const pong = await client.send("PING", []);
      client.close();
      if (pong === "PONG") return url;
    } catch {
      await Bun.sleep(50);
    }
  }
  throw new Error("Ephemeral Redis did not start in time");
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
    config: makeConfig(),
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

    await Bun.sleep(100);
    expect(nodeB.snapshot.get().models.has("cluster-gpt")).toBe(true);

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
  } finally {
    await nodeA.close();
    await nodeB.close();
    await primaryServer.stop(true);
    await backupServer.stop(true);
  }
});

test("on_request TRANSFORM, multimodal endpoints (moderations, speech, transcriptions, images/edits), traceparent, metrics histograms, Zod validation, and paginated internal queries", async () => {
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

    const wasmBytes = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
    const { data: plugin } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/plugins",
      {
        name: "req-redactor",
        version: "1.0.0",
        kind: "WASM",
        wasm_bytes_base64: Buffer.from(wasmBytes).toString("base64"),
        manifest: { hooks: ["on_request"] },
      },
    );

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

    const modResp = await runtime.publicApp.fetch(
      new Request("http://gw.local/v1/moderations", {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
        body: JSON.stringify({ model: "omni-model", input: "safe text" }),
      }),
    );
    expect(modResp.status).toBe(200);

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
