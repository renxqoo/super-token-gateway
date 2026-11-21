import { afterAll, beforeAll, expect, test } from "bun:test";
import { generateKeyPairSync, sign } from "node:crypto";
import type { SQL } from "bun";
import { createDatabase, migrate } from "@gateway/ledger";
import type { WasmRuntimeExecutor } from "@gateway/plugin-engine";
import { createGatewayRuntime, type GatewayConfig } from "@gateway/gateway";
import { createWorkerRunner } from "@gateway/worker";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const testDbName = `gateway_v4_e2e_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const adminUrl = new URL(baseDbUrl);
adminUrl.pathname = "/postgres";
const targetUrl = new URL(baseDbUrl);
targetUrl.pathname = `/${testDbName}`;

const KEY_PEPPER = "e2e-pepper-secret-0123456789abcdef";
const ENCRYPTION_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const ADMIN_TOKEN = "e2e-admin-token-0123456789abcdef";

let adminDb: SQL;
let db: SQL;

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

function makeConfig(overrides: Partial<GatewayConfig> = {}): GatewayConfig {
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

test("G2.1 + G2.2 SmartRouter pre-first-byte dynamic failover (503, 429, 401, ECONNREFUSED), circuit breaker trip & HALF_OPEN recovery, single customer charge", async () => {
  let primaryCalls = 0;
  let primaryMode: "503" | "429" | "401" | "ok" = "503";
  let backupCalls = 0;

  const primaryServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch() {
      primaryCalls++;
      if (primaryMode === "503") {
        return Response.json({ error: { message: "primary overloaded" } }, { status: 503 });
      }
      if (primaryMode === "429") {
        return Response.json(
          { error: { message: "rate limited" } },
          { status: 429, headers: { "retry-after": "1" } },
        );
      }
      if (primaryMode === "401") {
        return Response.json({ error: { message: "bad upstream key" } }, { status: 401 });
      }
      return Response.json({
        id: "chatcmpl_primary",
        object: "chat.completion",
        model: "gpt-primary",
        choices: [
          {
            index: 0,
            message: { role: "assistant", content: "from-primary" },
            finish_reason: "stop",
          },
        ],
        usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
      });
    },
  });

  const backupServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch() {
      backupCalls++;
      return Response.json({
        id: "chatcmpl_backup",
        object: "chat.completion",
        model: "gpt-backup",
        choices: [
          {
            index: 0,
            message: { role: "assistant", content: "from-backup" },
            finish_reason: "stop",
          },
        ],
        usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
      });
    },
  });

  let nowMs = 1000;
  const runtime = createGatewayRuntime({
    config: makeConfig(),
    db,
    routerOptions: {
      failureThreshold: 3,
      baseOpenMs: 10_000,
      now: () => nowMs,
    },
  });

  try {
    await runtime.start();
    const { data: project } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/projects",
      {
        name: "failover-project",
        currency: "PTS",
      },
    );
    const projectId = String(project.id);
    const { data: keyRes } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/api-keys",
      {
        project_id: projectId,
        name: "invoker",
      },
    );
    const apiKey = String(keyRes.key);

    await internalCall(runtime.internalApp, "POST", `/internal/v1/wallets/${projectId}/credits`, {
      currency: "PTS",
      external_ref: "credit-failover-1",
      amount_atoms: "1000000",
      kind: "CASH",
    });

    const { data: accPrimary } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/upstream-accounts",
      { name: "acc-primary" },
    );
    const { data: accBackup } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/upstream-accounts",
      { name: "acc-backup" },
    );
    await internalCall(
      runtime.internalApp,
      "POST",
      `/internal/v1/upstream-accounts/${accPrimary.id}/recharges`,
      { currency: "USD", external_ref: "sup-p-1", amount_atoms: "1000000" },
    );
    await internalCall(
      runtime.internalApp,
      "POST",
      `/internal/v1/upstream-accounts/${accBackup.id}/recharges`,
      { currency: "USD", external_ref: "sup-b-1", amount_atoms: "1000000" },
    );

    const { data: chPrimary } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/channels",
      {
        account_id: accPrimary.id,
        name: "primary-ch",
        kind: "openai",
        base_url: `http://127.0.0.1:${primaryServer.port}`,
        api_key: "sk-primary",
      },
    );
    const { data: chBackup } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/channels",
      {
        account_id: accBackup.id,
        name: "backup-ch",
        kind: "openai",
        base_url: `http://127.0.0.1:${backupServer.port}`,
        api_key: "sk-backup",
      },
    );

    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "smart-gpt",
      operations: ["chat"],
      routing_strategy: "WEIGHTED",
      max_output_tokens: 100,
    });

    const { data: cheapCustPrice } = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "CUSTOMER",
        currency: "PTS",
        rates: {
          input_tokens: { numerator: "10", denominator: "1" },
          output_tokens: { numerator: "20", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", output_tokens: "100" },
      },
    );
    const { data: pricierBackupCustPrice } = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "CUSTOMER",
        currency: "PTS",
        rates: {
          input_tokens: { numerator: "15", denominator: "1" },
          output_tokens: { numerator: "30", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", output_tokens: "100" },
      },
    );
    const { data: supPrice } = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "2", denominator: "1" },
          output_tokens: { numerator: "4", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", output_tokens: "100" },
      },
    );

    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "smart-gpt",
      channel_id: chPrimary.id,
      upstream_model: "gpt-primary",
      operations: ["chat"],
      priority: 10,
      billing_price_id: cheapCustPrice.id,
      cost_version_id: supPrice.id,
      retry_policy: { max_attempts: 3 },
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "smart-gpt",
      channel_id: chBackup.id,
      upstream_model: "gpt-backup",
      operations: ["chat"],
      priority: 5,
      billing_price_id: pricierBackupCustPrice.id,
      cost_version_id: supPrice.id,
      retry_policy: { max_attempts: 3 },
    });

    const invokeChat = async () =>
      runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: {
            authorization: `Bearer ${apiKey}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            model: "smart-gpt",
            messages: [{ role: "user", content: "hello" }],
            max_tokens: 50,
          }),
        }),
      );

    for (let i = 0; i < 3; i++) {
      const resp = await invokeChat();
      expect(resp.status).toBe(200);
      expect(resp.headers.get("x-gateway-channel")).toBe("backup-ch");
      const json = (await resp.json()) as { choices: { message: { content: string } }[] };
      expect(json.choices[0]!.message.content).toBe("from-backup");
    }
    expect(primaryCalls).toBe(3);
    expect(backupCalls).toBe(3);

    const respWhileOpen = await invokeChat();
    expect(respWhileOpen.status).toBe(200);
    expect(respWhileOpen.headers.get("x-gateway-channel")).toBe("backup-ch");
    expect(primaryCalls).toBe(3);
    expect(backupCalls).toBe(4);

    primaryMode = "ok";
    nowMs += 11_000;
    const probeResp = await invokeChat();
    expect(probeResp.status).toBe(200);
    expect(probeResp.headers.get("x-gateway-channel")).toBe("primary-ch");
    const probeJson = (await probeResp.json()) as { choices: { message: { content: string } }[] };
    expect(probeJson.choices[0]!.message.content).toBe("from-primary");
    expect(primaryCalls).toBe(4);

    const { data: wallet } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/wallets/${projectId}?currency=PTS`,
    );
    expect(wallet.reserved_atoms).toBe("0");
    expect(wallet.spent_atoms).toBe(String(4 * (10 * 15 + 5 * 30) + 1 * (10 * 10 + 5 * 20)));

    const { data: primarySup } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/upstream-accounts/${accPrimary.id}/balance?currency=USD`,
    );
    expect(primarySup.reserved_atoms).toBe("0");
    expect(primarySup.spent_atoms).toBe(String(10 * 2 + 5 * 4));

    const { data: backupSup } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/upstream-accounts/${accBackup.id}/balance?currency=USD`,
    );
    expect(backupSup.reserved_atoms).toBe("0");
    expect(backupSup.spent_atoms).toBe(String(4 * (10 * 2 + 5 * 4)));

    primaryMode = "401";
    const authFailResp = await invokeChat();
    expect(authFailResp.status).toBe(200);
    expect(authFailResp.headers.get("x-gateway-channel")).toBe("backup-ch");

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
    await primaryServer.stop(true);
    await backupServer.stop(true);
  }
});

test("G2.3 + G3.1 + G3.2 SSE streaming atomic [DONE], HOLD_UNTIL_APPROVED zero-leak Webhook + WASM redaction + CHARGE_CUSTOMER vs PLATFORM_ABSORB + zero plaintext DB", async () => {
  const mockControl = "mock-secret-ctrl";
  const mock = createMockApp(mockControl);
  const mockServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: mock.app.fetch,
  });

  const webhookServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const body = (await req.json()) as { hook: string; text: string };
      if (body.text.includes("FORBIDDEN_SECRET")) {
        return Response.json({
          action: "BLOCK",
          rule_code: "DLP_SECRET_BLOCK",
          http_status: 400,
          safe_message: "Response blocked by DLP webhook",
        });
      }
      return Response.json({ action: "PASS" });
    },
  });

  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const pubPem = publicKey.export({ type: "spki", format: "pem" }).toString();
  const wasmBytes = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
  const digest = new Bun.CryptoHasher("sha256").update(wasmBytes).digest("hex");
  const signature = sign(null, Buffer.from(digest, "utf8"), privateKey).toString("base64");

  const mockWasm: WasmRuntimeExecutor = {
    compile() {},
    async invoke(_id, _hook, inputBytes) {
      const payload = JSON.parse(new TextDecoder().decode(inputBytes)) as { text: string };
      if (payload.text.includes("PHONE_999")) {
        return new TextEncoder().encode(
          JSON.stringify({
            action: "TRANSFORM",
            text: payload.text.replaceAll("PHONE_999", "[MASKED_PHONE]"),
          }),
        );
      }
      return new TextEncoder().encode(JSON.stringify({ action: "PASS" }));
    },
    close() {},
  };

  const runtime = createGatewayRuntime({
    config: makeConfig({ pluginSigningPublicKeyPem: pubPem }),
    db,
    wasmExecutor: mockWasm,
  });

  try {
    await runtime.start();
    const { data: project } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/projects",
      {
        name: "moderation-project",
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
        name: "mod-key",
      },
    );
    const apiKey = String(keyRes.key);

    await internalCall(runtime.internalApp, "POST", `/internal/v1/wallets/${projectId}/credits`, {
      currency: "USD",
      external_ref: "credit-mod-1",
      amount_atoms: "1000000",
      kind: "CASH",
    });

    const { data: acc } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/upstream-accounts",
      { name: "mock-acc" },
    );
    await internalCall(
      runtime.internalApp,
      "POST",
      `/internal/v1/upstream-accounts/${acc.id}/recharges`,
      { currency: "USD", external_ref: "sup-mod-1", amount_atoms: "1000000" },
    );

    const { data: ch } = await internalCall(runtime.internalApp, "PUT", "/internal/v1/channels", {
      account_id: acc.id,
      name: "mock-openai",
      kind: "openai",
      base_url: `http://127.0.0.1:${mockServer.port}/openai`,
      api_key: "sk-mock",
    });

    const { data: custPrice } = await internalCall(
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
    const { data: supPrice } = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "3", denominator: "1" },
          cached_input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: "6", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "100" },
      },
    );

    const { data: wasmPlugin } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/plugins",
      {
        name: "pii-mask",
        version: "1.0.0",
        kind: "WASM",
        wasm_bytes_base64: Buffer.from(wasmBytes).toString("base64"),
        signature,
        signing_key_id: "key-1",
        manifest: {
          hooks: ["on_response", "on_stream_chunk", "on_stream_end"],
          window_bytes: 16,
        },
      },
    );
    const { data: whAbsorb } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/plugins",
      {
        name: "dlp-webhook-absorb",
        version: "1.0.0",
        kind: "WEBHOOK",
        webhook_config: {
          url: `http://127.0.0.1:${webhookServer.port}/check`,
          stream_mode: "HOLD_UNTIL_APPROVED",
          window_chars: 128,
          charge_policy: "PLATFORM_ABSORB",
        },
      },
    );
    const { data: whCharge } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/plugins",
      {
        name: "dlp-webhook-charge",
        version: "1.0.0",
        kind: "WEBHOOK",
        webhook_config: {
          url: `http://127.0.0.1:${webhookServer.port}/check`,
          stream_mode: "HOLD_UNTIL_APPROVED",
          window_chars: 128,
          charge_policy: "CHARGE_CUSTOMER",
        },
      },
    );

    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "mod-absorb",
      operations: ["chat", "messages"],
      max_output_tokens: 100,
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "mod-charge",
      operations: ["chat"],
      max_output_tokens: 100,
    });

    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "mod-absorb",
      channel_id: ch.id,
      upstream_model: "mod-absorb-up",
      operations: ["chat", "messages"],
      billing_price_id: custPrice.id,
      cost_version_id: supPrice.id,
      plugin_pipeline: [
        { plugin_id: String(wasmPlugin.id), priority: 1 },
        { plugin_id: String(whAbsorb.id), priority: 2, charge_policy: "PLATFORM_ABSORB" },
      ],
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "mod-charge",
      channel_id: ch.id,
      upstream_model: "mod-charge-up",
      operations: ["chat"],
      billing_price_id: custPrice.id,
      cost_version_id: supPrice.id,
      plugin_pipeline: [{ plugin_id: String(whCharge.id), priority: 1 }],
    });

    mock.scenarios.set("mod-absorb-up", { output: "Call PHONE_999 now" });
    const redactedResp = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "mod-absorb",
          messages: [{ role: "user", content: "PROMPT_MUST_NOT_PERSIST_123" }],
          max_tokens: 50,
        }),
      }),
    );
    expect(redactedResp.status).toBe(200);
    const redactedJson = (await redactedResp.json()) as {
      choices: { message: { content: string } }[];
    };
    expect(redactedJson.choices[0]!.message.content).toBe("Call [MASKED_PHONE] now");

    const anthropicCross = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/messages", {
        method: "POST",
        headers: { "x-api-key": apiKey, "content-type": "application/json" },
        body: JSON.stringify({
          model: "mod-absorb",
          max_tokens: 50,
          messages: [{ role: "user", content: "cross protocol test" }],
        }),
      }),
    );
    expect(anthropicCross.status).toBe(200);
    const anthropicJson = (await anthropicCross.json()) as {
      type: string;
      content: { type: string; text: string }[];
    };
    expect(anthropicJson.type).toBe("message");
    expect(anthropicJson.content[0]!.text).toBe("Call [MASKED_PHONE] now");

    const { data: walletBeforeBlock } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/wallets/${projectId}?currency=USD`,
    );

    mock.scenarios.set("mod-absorb-up", { output: "Here is FORBIDDEN_SECRET data" });
    const absorbStream = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "mod-absorb",
          stream: true,
          messages: [{ role: "user", content: "leak test" }],
          max_tokens: 50,
        }),
      }),
    );
    expect(absorbStream.status).toBe(200);
    const sseText = await absorbStream.text();
    expect(sseText).not.toContain("FORBIDDEN_SECRET");
    expect(sseText).toContain("DLP_SECRET_BLOCK");
    expect(sseText).not.toContain("[DONE]");

    const { data: walletAfterAbsorb } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/wallets/${projectId}?currency=USD`,
    );
    expect(walletAfterAbsorb.spent_atoms).toBe(walletBeforeBlock.spent_atoms);
    expect(walletAfterAbsorb.reserved_atoms).toBe("0");

    mock.scenarios.set("mod-charge-up", { output: "Blocked FORBIDDEN_SECRET payload" });
    const chargeBlock = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "mod-charge",
          messages: [{ role: "user", content: "charge block test" }],
          max_tokens: 50,
        }),
      }),
    );
    expect(chargeBlock.status).toBe(400);
    const { data: walletAfterChargeBlock } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/wallets/${projectId}?currency=USD`,
    );
    const expectedSingleCost = 7 * 10 + 3 * 5 + 5 * 20;
    expect(BigInt(String(walletAfterChargeBlock.spent_atoms))).toBe(
      BigInt(String(walletAfterAbsorb.spent_atoms)) + BigInt(expectedSingleCost),
    );

    mock.scenarios.set("mod-charge-up", { output: "Safe streaming answer", truncate: true });
    const truncatedStream = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "mod-charge",
          stream: true,
          messages: [{ role: "user", content: "truncate test" }],
          max_tokens: 50,
        }),
      }),
    );
    const truncText = await truncatedStream.text();
    expect(truncText).toContain("event: error");
    expect(truncText).not.toContain("[DONE]");

    const reqId = truncatedStream.headers.get("x-request-id")!;
    const holdRows = await db`SELECT state FROM public.billing_holds WHERE request_id = ${reqId}`;
    expect(holdRows[0]!.state).toBe("UNKNOWN");

    await internalCall(runtime.internalApp, "POST", `/internal/v1/requests/${reqId}/resolve`, {
      action: "RELEASE",
      reason: "truncated_upstream_stream",
      actor: "ops",
    });

    const rawDump = await db`
      SELECT coalesce(string_agg(payload::text, ' '), '') AS outbox_txt FROM public.outbox_events
    `;
    const chargesDump = await db`
      SELECT coalesce(string_agg(plugin_audit::text, ' '), '') AS audit_txt FROM public.billing_charges
    `;
    expect(String(rawDump[0]!.outbox_txt)).not.toContain("PROMPT_MUST_NOT_PERSIST_123");
    expect(String(rawDump[0]!.outbox_txt)).not.toContain("FORBIDDEN_SECRET");
    expect(String(chargesDump[0]!.audit_txt)).not.toContain("FORBIDDEN_SECRET");

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
    await webhookServer.stop(true);
  }
});

test("Worker advisory lease leader election, timed-out hold sweep, and Outbox webhook push delivery", async () => {
  const deliveredBatches: { consumer: string; events: { seq: string; kind: string }[] }[] = [];
  const bizWebhook = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      if (req.headers.get("authorization") !== "Bearer biz-secret") {
        return new Response("unauthorized", { status: 401 });
      }
      const body = (await req.json()) as {
        consumer: string;
        events: { seq: string; kind: string }[];
      };
      deliveredBatches.push(body);
      return Response.json({ ok: true });
    },
  });

  const workerA = createWorkerRunner({
    db,
    leaseKey: 81442601,
    businessWebhookUrl: `http://127.0.0.1:${bizWebhook.port}/events`,
    businessWebhookToken: "biz-secret",
    outboxConsumerName: "biz_sync_test",
  });
  const workerB = createWorkerRunner({
    db,
    leaseKey: 81442601,
    businessWebhookUrl: `http://127.0.0.1:${bizWebhook.port}/events`,
    businessWebhookToken: "biz-secret",
    outboxConsumerName: "biz_sync_test",
  });

  try {
    const tickA = await workerA.runOnce(true);
    expect(tickA.is_leader).toBe(true);
    expect(tickA.prepared_months.length).toBe(2);
    expect(tickA.delivered_outbox_events).toBeGreaterThan(0);
    expect(deliveredBatches.length).toBe(1);

    const tickB = await workerB.runOnce(false);
    expect(tickB.is_leader).toBe(false);
    expect(tickB.delivered_outbox_events).toBe(0);

    await workerA.stop();
    const tickBAfterFailover = await workerB.runOnce(false);
    expect(tickBAfterFailover.is_leader).toBe(true);
    expect(tickBAfterFailover.delivered_outbox_events).toBe(0);
  } finally {
    await workerA.stop();
    await workerB.stop();
    await bizWebhook.stop(true);
  }
});

test("G2.4 Production hardening: webhook SSRF block, /v1/models CIDR check, concurrent Idempotency-Key HELD guard, retry_on_ambiguous UNKNOWN preservation, and client SSE abort", async () => {
  const strictRuntime = createGatewayRuntime({
    config: makeConfig({
      allowPrivateUpstreams: false,
      privateUpstreamCidrs: [],
    }),
    db,
  });
  try {
    await strictRuntime.start();
    const ssrfMeta = await internalCall(strictRuntime.internalApp, "PUT", "/internal/v1/plugins", {
      name: "ssrf-meta",
      version: "1.0.0",
      kind: "WEBHOOK",
      webhook_config: {
        url: "http://169.254.169.254/latest/meta-data/",
      },
    });
    expect(ssrfMeta.status).toBe(400);
    expect((ssrfMeta.data.error as { code: string }).code).toBe("unsafe_upstream");

    const ssrfLoopback = await internalCall(
      strictRuntime.internalApp,
      "PUT",
      "/internal/v1/plugins",
      {
        name: "ssrf-loopback",
        version: "1.0.0",
        kind: "WEBHOOK",
        webhook_config: {
          url: "http://127.0.0.1:9999/hook",
        },
      },
    );
    expect(ssrfLoopback.status).toBe(400);
    expect((ssrfLoopback.data.error as { code: string }).code).toBe("unsafe_upstream");
  } finally {
    await strictRuntime.close();
  }

  let primaryMode: "slow_idem" | "hang_timeout" | "sse_hang" = "slow_idem";
  let primaryCalls = 0;
  let backupCalls = 0;
  const openTimers = new Set<ReturnType<typeof setTimeout>>();

  const primaryServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      primaryCalls++;
      if (primaryMode === "slow_idem") {
        await Bun.sleep(150);
        return Response.json({
          id: "chatcmpl_idem_1",
          object: "chat.completion",
          model: "gpt-sec-primary",
          choices: [
            {
              index: 0,
              message: { role: "assistant", content: "idem-ok" },
              finish_reason: "stop",
            },
          ],
          usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
        });
      }
      if (primaryMode === "hang_timeout") {
        await new Promise<void>((resolve) => {
          const t = setTimeout(() => {
            openTimers.delete(t);
            resolve();
          }, 600);
          openTimers.add(t);
          req.signal.addEventListener(
            "abort",
            () => {
              clearTimeout(t);
              openTimers.delete(t);
              resolve();
            },
            { once: true },
          );
        });
        return Response.json({ error: { message: "too slow" } }, { status: 504 });
      }
      const enc = new TextEncoder();
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(
            enc.encode(
              `data: ${JSON.stringify({
                id: "chatcmpl_sse_abort",
                object: "chat.completion.chunk",
                model: "gpt-sec-primary",
                choices: [{ index: 0, delta: { role: "assistant", content: "partial " } }],
              })}\n\n`,
            ),
          );
          const t = setTimeout(() => {
            openTimers.delete(t);
            try {
              controller.close();
            } catch {
              void 0;
            }
          }, 5000);
          openTimers.add(t);
          req.signal.addEventListener(
            "abort",
            () => {
              clearTimeout(t);
              openTimers.delete(t);
              try {
                controller.close();
              } catch {
                void 0;
              }
            },
            { once: true },
          );
        },
      });
      return new Response(stream, {
        status: 200,
        headers: { "content-type": "text/event-stream; charset=utf-8" },
      });
    },
  });

  const backupServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch() {
      backupCalls++;
      return Response.json({
        id: "chatcmpl_sec_backup",
        object: "chat.completion",
        model: "gpt-sec-backup",
        choices: [
          {
            index: 0,
            message: { role: "assistant", content: "from-sec-backup" },
            finish_reason: "stop",
          },
        ],
        usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
      });
    },
  });

  const runtime = createGatewayRuntime({
    config: makeConfig({ attemptTimeoutMs: 200 }),
    db,
  });

  try {
    await runtime.start();
    const { data: project } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/projects",
      {
        name: "hardening-project",
        currency: "USD",
      },
    );
    const projectId = String(project.id);

    const { data: unrestrictedKey } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/api-keys",
      {
        project_id: projectId,
        name: "hardening-open",
      },
    );
    const apiKey = String(unrestrictedKey.key);

    const { data: cidrKey } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/api-keys",
      {
        project_id: projectId,
        name: "hardening-cidr",
        allowed_cidrs: ["10.20.30.0/24"],
      },
    );
    const restrictedApiKey = String(cidrKey.key);

    await internalCall(runtime.internalApp, "POST", `/internal/v1/wallets/${projectId}/credits`, {
      currency: "USD",
      external_ref: "credit-hardening-1",
      amount_atoms: "1000000",
      kind: "CASH",
    });

    const { data: accPrimary } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/upstream-accounts",
      { name: "hardening-acc-primary" },
    );
    const { data: accBackup } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/upstream-accounts",
      { name: "hardening-acc-backup" },
    );
    await internalCall(
      runtime.internalApp,
      "POST",
      `/internal/v1/upstream-accounts/${accPrimary.id}/recharges`,
      { currency: "USD", external_ref: "sup-hp-1", amount_atoms: "1000000" },
    );
    await internalCall(
      runtime.internalApp,
      "POST",
      `/internal/v1/upstream-accounts/${accBackup.id}/recharges`,
      { currency: "USD", external_ref: "sup-hb-1", amount_atoms: "1000000" },
    );

    const { data: chPrimary } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/channels",
      {
        account_id: accPrimary.id,
        name: "hardening-primary",
        kind: "openai",
        base_url: `http://127.0.0.1:${primaryServer.port}`,
        api_key: "sk-hp",
      },
    );
    const { data: chBackup } = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/channels",
      {
        account_id: accBackup.id,
        name: "hardening-backup",
        kind: "openai",
        base_url: `http://127.0.0.1:${backupServer.port}`,
        api_key: "sk-hb",
      },
    );

    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "hardening-gpt",
      operations: ["chat"],
      routing_strategy: "WEIGHTED",
      max_output_tokens: 100,
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
      name: "hardening-sse-only",
      operations: ["chat"],
      routing_strategy: "WEIGHTED",
      max_output_tokens: 100,
    });

    const { data: custPrice } = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "10", denominator: "1" },
          output_tokens: { numerator: "20", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", output_tokens: "100" },
      },
    );
    const { data: supPrice } = await internalCall(
      runtime.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "2", denominator: "1" },
          output_tokens: { numerator: "4", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", output_tokens: "100" },
      },
    );

    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "hardening-gpt",
      channel_id: chPrimary.id,
      upstream_model: "gpt-sec-primary",
      operations: ["chat"],
      priority: 10,
      billing_price_id: custPrice.id,
      cost_version_id: supPrice.id,
      retry_policy: { max_attempts: 2, retry_on_ambiguous: true },
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "hardening-gpt",
      channel_id: chBackup.id,
      upstream_model: "gpt-sec-backup",
      operations: ["chat"],
      priority: 5,
      billing_price_id: custPrice.id,
      cost_version_id: supPrice.id,
      retry_policy: { max_attempts: 2, retry_on_ambiguous: true },
    });
    await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "hardening-sse-only",
      channel_id: chPrimary.id,
      upstream_model: "gpt-sec-primary",
      operations: ["chat"],
      priority: 10,
      billing_price_id: custPrice.id,
      cost_version_id: supPrice.id,
      retry_policy: { max_attempts: 1 },
    });

    const deniedModels = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/models", {
        headers: { authorization: `Bearer ${restrictedApiKey}` },
      }),
      { peerAddress: "192.168.1.5" },
    );
    expect(deniedModels.status).toBe(403);
    const deniedJson = (await deniedModels.json()) as { error: { code: string } };
    expect(deniedJson.error.code).toBe("ip_not_allowed");

    const allowedModels = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/models", {
        headers: { authorization: `Bearer ${restrictedApiKey}` },
      }),
      { peerAddress: "10.20.30.42" },
    );
    expect(allowedModels.status).toBe(200);
    const allowedJson = (await allowedModels.json()) as { data: { id: string }[] };
    expect(allowedJson.data.some((m) => m.id === "hardening-gpt")).toBe(true);

    const idemKey = "idem-concurrent-held-guard-1";
    const idemResponses = await Promise.all(
      Array.from({ length: 5 }, () =>
        runtime.publicApp.fetch(
          new Request("http://gateway.local/v1/chat/completions", {
            method: "POST",
            headers: {
              authorization: `Bearer ${apiKey}`,
              "content-type": "application/json",
              "idempotency-key": idemKey,
            },
            body: JSON.stringify({
              model: "hardening-gpt",
              messages: [{ role: "user", content: "concurrent idempotency test" }],
              max_tokens: 50,
            }),
          }),
        ),
      ),
    );
    const idemStatuses = idemResponses.map((r) => r.status).sort((a, b) => a - b);
    expect(idemStatuses).toEqual([200, 409, 409, 409, 409]);
    const conflictCodes = await Promise.all(
      idemResponses
        .filter((r) => r.status === 409)
        .map(async (r) => ((await r.json()) as { error: { code: string } }).error.code),
    );
    expect(conflictCodes.every((c) => c === "idempotency_in_progress")).toBe(true);
    expect(primaryCalls).toBe(1);

    const { data: walletAfterIdem } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/wallets/${projectId}?currency=USD`,
    );
    const singleCustCost = 10 * 10 + 5 * 20;
    expect(walletAfterIdem.spent_atoms).toBe(String(singleCustCost));
    expect(walletAfterIdem.reserved_atoms).toBe("0");

    primaryMode = "hang_timeout";
    const ambigResp = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${apiKey}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          model: "hardening-gpt",
          messages: [{ role: "user", content: "ambiguous timeout failover" }],
          max_tokens: 50,
        }),
      }),
    );
    expect(ambigResp.status).toBe(200);
    expect(ambigResp.headers.get("x-gateway-channel")).toBe("hardening-backup");
    expect(backupCalls).toBe(1);

    const { data: walletAfterAmbig } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/wallets/${projectId}?currency=USD`,
    );
    expect(walletAfterAmbig.spent_atoms).toBe(String(2 * singleCustCost));
    expect(walletAfterAmbig.reserved_atoms).toBe("0");

    const { data: primarySupAfterAmbig } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/upstream-accounts/${accPrimary.id}/balance?currency=USD`,
    );
    expect(BigInt(String(primarySupAfterAmbig.reserved_atoms))).toBeGreaterThan(0n);

    const { data: backupSupAfterAmbig } = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/upstream-accounts/${accBackup.id}/balance?currency=USD`,
    );
    expect(backupSupAfterAmbig.reserved_atoms).toBe("0");
    expect(backupSupAfterAmbig.spent_atoms).toBe(String(10 * 2 + 5 * 4));

    const preAbortHealth = runtime.router
      .snapshot()
      .find((s) => s.channel_id === String(chPrimary.id));
    const preAbortFailures = preAbortHealth?.consecutive_failures ?? 0;

    primaryMode = "sse_hang";
    const abortController = new AbortController();
    const sseResp = await runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${apiKey}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          model: "hardening-sse-only",
          stream: true,
          messages: [{ role: "user", content: "cancel mid stream" }],
          max_tokens: 50,
        }),
        signal: abortController.signal,
      }),
    );
    expect(sseResp.status).toBe(200);
    const sseReqId = sseResp.headers.get("x-request-id")!;
    const reader = sseResp.body!.getReader();
    const firstChunk = await reader.read();
    expect(firstChunk.done).toBe(false);
    expect(new TextDecoder().decode(firstChunk.value)).toContain("partial");
    abortController.abort();
    await reader.cancel();

    for (let i = 0; i < 40; i++) {
      if (runtime.lifecycle.size === 0) {
        const rows = await db`
          SELECT state FROM public.billing_holds WHERE request_id = ${sseReqId}
        `;
        if (rows[0]?.state === "UNKNOWN") break;
      }
      await Bun.sleep(20);
    }

    const sseHoldRows = await db`
      SELECT state FROM public.billing_holds WHERE request_id = ${sseReqId}
    `;
    expect(sseHoldRows[0]!.state).toBe("UNKNOWN");
    expect(runtime.lifecycle.size).toBe(0);

    const postAbortHealth = runtime.router
      .snapshot()
      .find((s) => s.channel_id === String(chPrimary.id));
    expect(postAbortHealth?.state).toBe("CLOSED");
    expect(postAbortHealth?.consecutive_failures ?? 0).toBe(preAbortFailures);

    await internalCall(runtime.internalApp, "POST", `/internal/v1/requests/${sseReqId}/resolve`, {
      action: "RELEASE",
      reason: "client_aborted_sse",
      actor: "ops",
    });

    const { data: audit } = await internalCall(
      runtime.internalApp,
      "GET",
      "/internal/v1/audit/conservation",
    );
    expect(audit.ok).toBe(true);
    expect(audit.open_holds).toBe(0);
    expect(audit.unknown_holds).toBe(0);
  } finally {
    for (const t of openTimers) clearTimeout(t);
    openTimers.clear();
    await runtime.close();
    await primaryServer.stop(true);
    await backupServer.stop(true);
  }
});
