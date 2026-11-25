import { localInternalCredentials } from "../../scripts/local-internal-auth.ts";
import { afterAll, beforeAll, expect, test } from "bun:test";
import type { SQL } from "bun";
import { createAsyncTasksRepository, createDatabase, migrate } from "@gateway/ledger";
import { createGatewayRuntime, type GatewayConfig } from "@gateway/gateway";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const testDbName = `gateway_p1_foundation_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const adminUrl = new URL(baseDbUrl);
adminUrl.pathname = "/postgres";
const targetUrl = new URL(baseDbUrl);
targetUrl.pathname = `/${testDbName}`;

const KEY_PEPPER = "foundation-pepper-0123456789abcdef";
const ENCRYPTION_KEY = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
const OPERATOR_TOKEN = "foundation-admin-token-01234567890000000000000";

let adminDb: SQL;
let db: SQL;
let gateway: ReturnType<typeof createGatewayRuntime>;
let mockServer: ReturnType<typeof Bun.serve>;
let mockUrl = "";

const videoTaskStatuses = new Map<string, Record<string, unknown>>();
let receivedSecret = "";
let videoSubmitCount = 0;

function adminHeaders() {
  return {
    authorization: `Bearer ${OPERATOR_TOKEN}`,
    "content-type": "application/json",
  };
}

async function internalCall<T = Record<string, unknown>>(
  method: string,
  path: string,
  body?: unknown,
): Promise<{ status: number; data: T }> {
  const res = await gateway.internalApp.request(path, {
    method,
    headers: adminHeaders(),
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
  return { status: res.status, data: (await res.json()) as T };
}

beforeAll(async () => {
  mockServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      if (req.method === "POST" && url.pathname === "/v1/chat/completions") {
        receivedSecret = req.headers.get("authorization") ?? "";
        const body = (await req.json()) as { model?: string };
        return Response.json({
          id: "chatcmpl-p1",
          object: "chat.completion",
          created: Math.floor(Date.now() / 1000),
          model: body.model ?? "mock-chat",
          choices: [
            { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
          ],
          usage: { prompt_tokens: 100, completion_tokens: 50, total_tokens: 150 },
        });
      }
      if (req.method === "POST" && url.pathname === "/v2/video_generation") {
        videoSubmitCount++;
        const body = (await req.json()) as { prompt?: string };
        const tid = `up_vid_${videoSubmitCount}_${crypto.randomUUID().slice(0, 6)}`;
        videoTaskStatuses.set(tid, {
          status: "Processing",
          task_id: tid,
          prompt: body.prompt ?? "",
        });
        return Response.json({
          task_id: tid,
          base_resp: { status_code: 0, status_msg: "success" },
        });
      }
      if (req.method === "GET" && url.pathname === "/v2/query/video_generation") {
        const tid = url.searchParams.get("task_id") ?? "";
        const state = videoTaskStatuses.get(tid);
        if (!state) return Response.json({ status: "Fail", error_code: 9999 }, { status: 404 });
        return Response.json(state);
      }
      return new Response("Not Found", { status: 404 });
    },
  });
  mockUrl = `http://127.0.0.1:${mockServer.port}`;

  adminDb = createDatabase(adminUrl.toString(), 2);
  await adminDb.unsafe(`CREATE DATABASE ${testDbName}`);
  db = createDatabase(targetUrl.toString(), 16);
  await migrate(db);

  const config: GatewayConfig = {
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
    privateUpstreamCidrs: [],
    trustedProxyCidrs: [],
    maxInFlight: 200,
    maxBodyBytes: 2 * 1024 * 1024,
    holdTtlMs: 30_000,
    attemptTimeoutMs: 5_000,
    snapshotPollMs: 0,
    microBatchWindowMs: 2,
    microBatchMaxSize: 32,
    maxActiveAsyncTasksPerBillingAccount: 2,
    maxAutoRefundsPerBillingAccount24h: 2,
  };
  gateway = createGatewayRuntime({ config, db, wasmExecutor: null });
  await gateway.start();
});

afterAll(async () => {
  await gateway?.close(2000);
  mockServer?.stop(true);
  await db?.close();
  if (adminDb) {
    await adminDb.unsafe(
      `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${testDbName}' AND pid <> pg_backend_pid()`,
    );
    await adminDb.unsafe(`DROP DATABASE IF EXISTS ${testDbName}`);
    await adminDb.close();
  }
});

test("Phase 1: channel secret preservation, api_key attribution & LIFETIME/DAILY spend limits, enterprise pricing overrides, and anti-abuse async video lifecycle", async () => {
  const { data: account } = await internalCall<{ id: string }>(
    "PUT",
    "/internal/v1/upstream-accounts",
    { name: "p1-upstream" },
  );
  await internalCall("POST", `/internal/v1/upstream-accounts/${account.id}/recharges`, {
    currency: "USD",
    external_ref: "p1-sup-topup",
    amount_atoms: "100000000",
  });

  const { data: videoPlugin } = await internalCall<{ id: string }>("PUT", "/internal/v1/plugins", {
    name: "p1_video_provider",
    version: "1.0.0",
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    provider_config: {
      base_dialect: "openai_chat",
      usage_dialect: "openai",
      auth: { scheme: "bearer" },
      endpoints: {
        chat: { path: "/v1/chat/completions" },
        videos: { path: "/v2/video_generation" },
      },
      async_task_config: {
        query_path: "/v2/query/video_generation",
        query_param: "task_id",
        task_id_path: "task_id",
        status_path: "status",
        succeeded_values: ["Success"],
        failed_values: ["Fail"],
        error_code_path: "error_code",
        refundable_system_error_codes: [5001, "SYS_CRASH"],
        non_refundable_error_codes: [1026, "CONTENT_POLICY"],
        billed_indicator_path: "compute_billed",
      },
    },
  });
  const epochBeforeRelease = gateway.snapshot.get().epoch;
  await internalCall("POST", "/internal/v1/plugin-releases", {
    id: crypto.randomUUID(),
    expected_epoch: epochBeforeRelease,
    targets: [{ plugin_id: videoPlugin.id }],
  });

  const { data: channel } = await internalCall<{ id: string }>("PUT", "/internal/v1/channels", {
    account_id: account.id,
    name: "p1-ch",
    kind: "p1_video_provider",
    provider_plugin_id: videoPlugin.id,
    base_url: mockUrl,
    api_key: "sk-upstream-original-secret",
  });

  const { status: updChStatus } = await internalCall("PUT", "/internal/v1/channels", {
    id: channel.id,
    account_id: account.id,
    name: "p1-ch-renamed",
    kind: "p1_video_provider",
    provider_plugin_id: videoPlugin.id,
    base_url: mockUrl,
    rpm: 50000,
  });
  expect(updChStatus).toBe(200);

  await internalCall("PUT", "/internal/v1/models", {
    name: "p1-chat",
    operations: ["chat"],
    max_output_tokens: 1000,
  });
  await internalCall("PUT", "/internal/v1/models", {
    name: "p1-video",
    operations: ["videos"],
    max_output_tokens: 1000,
  });

  const { data: listPrice } = await internalCall<{ id: string }>("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    rates: {
      input_tokens: { numerator: "10", denominator: "1" },
      output_tokens: { numerator: "20", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", output_tokens: "1000" },
  });
  const { data: supPrice } = await internalCall<{ id: string }>("POST", "/internal/v1/prices", {
    side: "SUPPLIER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    rates: {
      input_tokens: { numerator: "5", denominator: "1" },
      output_tokens: { numerator: "10", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", output_tokens: "1000" },
  });
  const { data: videoCustPrice } = await internalCall<{ id: string }>(
    "POST",
    "/internal/v1/prices",
    {
      side: "CUSTOMER",
      currency: "USD",
      basis: "REQUEST_COMPLETION",
      rates: { requests: { numerator: "5000", denominator: "1" } },
      ceilings: { requests: "1" },
    },
  );
  const { data: videoSupPrice } = await internalCall<{ id: string }>(
    "POST",
    "/internal/v1/prices",
    {
      side: "SUPPLIER",
      currency: "USD",
      basis: "REQUEST_COMPLETION",
      rates: { requests: { numerator: "2000", denominator: "1" } },
      ceilings: { requests: "1" },
    },
  );

  await internalCall("PUT", "/internal/v1/routes", {
    model_name: "p1-chat",
    channel_id: channel.id,
    upstream_model: "p1-chat",
    operations: ["chat"],
    billing_price_id: listPrice.id,
    cost_version_id: supPrice.id,
  });
  await internalCall("PUT", "/internal/v1/routes", {
    model_name: "p1-video",
    channel_id: channel.id,
    upstream_model: "p1-video",
    operations: ["videos"],
    billing_price_id: videoCustPrice.id,
    cost_version_id: videoSupPrice.id,
  });

  const { data: projA } = await internalCall<{ id: string }>(
    "PUT",
    "/internal/v1/billing-accounts",
    {
      name: "enterprise-a",
      currency: "USD",
      rpm: 10000,
      tpm: 10000000,
      concurrency: 100,
    },
  );
  const { data: projB } = await internalCall<{ id: string }>(
    "PUT",
    "/internal/v1/billing-accounts",
    {
      name: "tenant-b",
      currency: "USD",
      rpm: 10000,
      tpm: 10000000,
      concurrency: 100,
    },
  );
  await internalCall("POST", `/internal/v1/wallets/${projA.id}/credits`, {
    currency: "USD",
    external_ref: "projA-fund",
    amount_atoms: "1000000",
    kind: "CASH",
  });
  await internalCall("POST", `/internal/v1/wallets/${projB.id}/credits`, {
    currency: "USD",
    external_ref: "projB-fund",
    amount_atoms: "1000000",
    kind: "CASH",
  });

  const { data: overrideRec } = await internalCall<{ id: string }>(
    "PUT",
    "/internal/v1/pricing-overrides",
    {
      billing_account_id: projA.id,
      model_name: "p1-chat",
      operation: "*",
      override_kind: "MULTIPLIER",
      multiplier_numerator: "8",
      multiplier_denominator: "10",
    },
  );
  expect(overrideRec.id).toBeTruthy();

  const { data: keyLimited } = await internalCall<{
    id: string;
    key?: string;
    verifier?: string;
  }>("PUT", "/internal/v1/api-keys", {
    billing_account_id: projA.id,
    name: "limited-key",
    spend_limit_atoms: "40000",
    spend_limit_window: "LIFETIME",
  });
  expect(keyLimited.key).toBeTruthy();
  expect(keyLimited.verifier).toBeUndefined();

  const { data: updatedKeyRes } = await internalCall<{ id: string; key?: string }>(
    "PUT",
    "/internal/v1/api-keys",
    {
      id: keyLimited.id,
      billing_account_id: projA.id,
      name: "limited-key-renamed",
      spend_limit_atoms: "40000",
      spend_limit_window: "LIFETIME",
    },
  );
  expect(updatedKeyRes.key).toBeUndefined();

  const { data: keyUnlimited } = await internalCall<{ id: string; key?: string }>(
    "PUT",
    "/internal/v1/api-keys",
    { billing_account_id: projA.id, name: "unlimited-key" },
  );
  const { data: keyB } = await internalCall<{ id: string; key?: string }>(
    "PUT",
    "/internal/v1/api-keys",
    { billing_account_id: projB.id, name: "projB-key" },
  );

  const chatResp1 = await gateway.fetchPublic(
    new Request("http://localhost/v1/chat/completions", {
      method: "POST",
      headers: {
        authorization: `Bearer ${keyLimited.key}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "p1-chat",
        max_tokens: 100,
        messages: [{ role: "user", content: "hello" }],
      }),
    }),
  );
  expect(chatResp1.status).toBe(200);
  expect(receivedSecret).toBe("Bearer sk-upstream-original-secret");

  const { data: keyCharges } = await internalCall<{
    items: {
      apiKeyId: string | null;
      customerAtoms: string;
      pricingOverrideId: string | null;
      discountNumerator: string | null;
      discountDenominator: string | null;
    }[];
  }>("GET", `/internal/v1/charges?billing_account_id=${projA.id}&api_key_id=${keyLimited.id}`);
  expect(keyCharges.items.length).toBe(1);
  expect(keyCharges.items[0]!.apiKeyId).toBe(keyLimited.id);
  expect(keyCharges.items[0]!.customerAtoms).toBe("1600");
  expect(keyCharges.items[0]!.pricingOverrideId).toBe(overrideRec.id);
  expect(keyCharges.items[0]!.discountNumerator).toBe("8");
  expect(keyCharges.items[0]!.discountDenominator).toBe("10");

  await internalCall("PUT", "/internal/v1/api-keys", {
    id: keyLimited.id,
    billing_account_id: projA.id,
    name: "limited-key-tight",
    spend_limit_atoms: "13000",
    spend_limit_window: "LIFETIME",
  });
  const concurrentLimited = await Promise.all(
    Array.from({ length: 5 }, () =>
      gateway.fetchPublic(
        new Request("http://localhost/v1/chat/completions", {
          method: "POST",
          headers: {
            authorization: `Bearer ${keyLimited.key}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            model: "p1-chat",
            max_tokens: 100,
            messages: [{ role: "user", content: "race" }],
          }),
        }),
      ),
    ),
  );
  const statuses = concurrentLimited.map((r) => r.status);
  expect(statuses).toContain(200);
  expect(statuses).toContain(402);

  const { data: spendSnap } = await internalCall<{
    spent_atoms: string;
    reserved_atoms: string;
    spend_limit_atoms: string;
  }>("GET", `/internal/v1/api-keys/${keyLimited.id}/spend`);
  expect(BigInt(spendSnap.spent_atoms)).toBeLessThanOrEqual(13000n);
  expect(spendSnap.reserved_atoms).toBe("0");

  const { data: resetSnap } = await internalCall<{ spent_atoms: string }>(
    "POST",
    `/internal/v1/api-keys/${keyLimited.id}/reset-spend`,
    { reason: "operator reset" },
  );
  expect(resetSnap.spent_atoms).toBe("0");

  const afterReset = await gateway.fetchPublic(
    new Request("http://localhost/v1/chat/completions", {
      method: "POST",
      headers: {
        authorization: `Bearer ${keyLimited.key}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "p1-chat",
        max_tokens: 100,
        messages: [{ role: "user", content: "after-reset" }],
      }),
    }),
  );
  expect(afterReset.status).toBe(200);

  const submitVideo = async (rawKey: string, prompt: string) => {
    const res = await gateway.fetchPublic(
      new Request("http://localhost/v1/videos", {
        method: "POST",
        headers: { authorization: `Bearer ${rawKey}`, "content-type": "application/json" },
        body: JSON.stringify({ model: "p1-video", prompt }),
      }),
    );
    return { status: res.status, body: (await res.json()) as Record<string, unknown> };
  };
  const pollVideo = async (rawKey: string, taskId: string) => {
    const res = await gateway.fetchPublic(
      new Request(`http://localhost/v1/videos/${taskId}`, {
        method: "GET",
        headers: { authorization: `Bearer ${rawKey}` },
      }),
    );
    return { status: res.status, body: (await res.json()) as Record<string, unknown> };
  };
  const cancelVideo = async (rawKey: string, taskId: string) => {
    const res = await gateway.fetchPublic(
      new Request(`http://localhost/v1/videos/${taskId}/cancel`, {
        method: "POST",
        headers: { authorization: `Bearer ${rawKey}` },
      }),
    );
    return { status: res.status, body: (await res.json()) as Record<string, unknown> };
  };

  const v1 = await submitVideo(keyUnlimited.key!, "success video");
  expect(v1.status).toBe(200);
  const v1Id = String(v1.body.gateway_task_id);
  const v1Up = String(v1.body.task_id);

  const crossTenantPoll = await pollVideo(keyB.key!, v1Id);
  expect(crossTenantPoll.status).toBe(404);

  const v2 = await submitVideo(keyUnlimited.key!, "concurrent video 2");
  expect(v2.status).toBe(200);
  const v2Id = String(v2.body.gateway_task_id);
  const v2Up = String(v2.body.task_id);

  const vCapBlocked = await submitVideo(keyUnlimited.key!, "exceeds concurrency cap 2");
  expect(vCapBlocked.status).toBe(429);

  videoTaskStatuses.set(v1Up, { status: "Success", file_id: "f_123" });
  const v1Polled = await pollVideo(keyUnlimited.key!, v1Id);
  expect(v1Polled.status).toBe(200);
  expect(v1Polled.body.status).toBe("SUCCEEDED");
  expect(v1Polled.body.refund_eligibility).toBe("NOT_APPLICABLE");

  const walletBeforeAutoRefund = await internalCall<{ available_atoms: string }>(
    "GET",
    `/internal/v1/wallets/${projA.id}?currency=USD`,
  );
  videoTaskStatuses.set(v2Up, { status: "Fail", error_code: 5001 });
  const [v2Poll1, v2Poll2] = await Promise.all([
    pollVideo(keyUnlimited.key!, v2Id),
    pollVideo(keyUnlimited.key!, v2Id),
  ]);
  expect(v2Poll1.status).toBe(200);
  expect(v2Poll2.status).toBe(200);
  const v2Final = await pollVideo(keyUnlimited.key!, v2Id);
  expect(v2Final.body.status).toBe("FAILED");
  expect(v2Final.body.failure_category).toBe("UPSTREAM_SYSTEM");
  expect(v2Final.body.refund_eligibility).toBe("AUTO_REFUNDED");
  expect(v2Final.body.refund_id).toBeTruthy();

  const walletAfterAutoRefund = await internalCall<{ available_atoms: string }>(
    "GET",
    `/internal/v1/wallets/${projA.id}?currency=USD`,
  );
  expect(
    BigInt(walletAfterAutoRefund.data.available_atoms) -
      BigInt(walletBeforeAutoRefund.data.available_atoms),
  ).toBe(5000n);

  const vCancel = await submitVideo(keyUnlimited.key!, "user cancels");
  const vCancelId = String(vCancel.body.gateway_task_id);
  const vCancelUp = String(vCancel.body.task_id);
  const walletAfterCancelSubmit = await internalCall<{ available_atoms: string }>(
    "GET",
    `/internal/v1/wallets/${projA.id}?currency=USD`,
  );
  const cancelled = await cancelVideo(keyUnlimited.key!, vCancelId);
  expect(cancelled.status).toBe(200);
  expect(cancelled.body.user_cancelled).toBe(true);
  expect(cancelled.body.refund_eligibility).toBe("DENIED_USER_CANCELLED");
  videoTaskStatuses.set(vCancelUp, { status: "Fail", error_code: 5001 });
  const pollCancelled = await pollVideo(keyUnlimited.key!, vCancelId);
  expect(pollCancelled.body.refund_eligibility).toBe("DENIED_USER_CANCELLED");
  const walletAfterCancelPoll = await internalCall<{ available_atoms: string }>(
    "GET",
    `/internal/v1/wallets/${projA.id}?currency=USD`,
  );
  expect(walletAfterCancelPoll.data.available_atoms).toBe(
    walletAfterCancelSubmit.data.available_atoms,
  );

  const vPolicy = await submitVideo(keyUnlimited.key!, "policy violation");
  videoTaskStatuses.set(String(vPolicy.body.task_id), { status: "Fail", error_code: 1026 });
  const polRes = await pollVideo(keyUnlimited.key!, String(vPolicy.body.gateway_task_id));
  expect(polRes.body.refund_eligibility).toBe("DENIED_POLICY_OR_CLIENT");
  expect(polRes.body.refund_id).toBeNull();

  const vBilled = await submitVideo(keyUnlimited.key!, "upstream billed fail");
  videoTaskStatuses.set(String(vBilled.body.task_id), {
    status: "Fail",
    error_code: 5001,
    compute_billed: true,
  });
  const billedRes = await pollVideo(keyUnlimited.key!, String(vBilled.body.gateway_task_id));
  expect(billedRes.body.upstream_billed).toBe(true);
  expect(billedRes.body.refund_eligibility).toBe("DENIED_UPSTREAM_BILLED");
  expect(billedRes.body.refund_id).toBeNull();

  const vUnknown = await submitVideo(keyUnlimited.key!, "unknown error");
  videoTaskStatuses.set(String(vUnknown.body.task_id), { status: "Fail", error_code: 7777 });
  const unknownRes = await pollVideo(keyUnlimited.key!, String(vUnknown.body.gateway_task_id));
  expect(unknownRes.body.refund_eligibility).toBe("MANUAL_REVIEW");
  expect(unknownRes.body.refund_id).toBeNull();

  const vRef2 = await submitVideo(keyUnlimited.key!, "second auto refund");
  const vRef3 = await submitVideo(keyUnlimited.key!, "third hits 24h cap");
  videoTaskStatuses.set(String(vRef2.body.task_id), { status: "Fail", error_code: "SYS_CRASH" });
  videoTaskStatuses.set(String(vRef3.body.task_id), { status: "Fail", error_code: "SYS_CRASH" });
  const ref2Res = await pollVideo(keyUnlimited.key!, String(vRef2.body.gateway_task_id));
  expect(ref2Res.body.refund_eligibility).toBe("AUTO_REFUNDED");
  const ref3Res = await pollVideo(keyUnlimited.key!, String(vRef3.body.gateway_task_id));
  expect(ref3Res.body.refund_eligibility).toBe("DENIED_ABUSE_LIMIT");
  expect(ref3Res.body.refund_id).toBeNull();

  const vStale = await submitVideo(keyUnlimited.key!, "stale task");
  const vStaleId = String(vStale.body.gateway_task_id);
  await db`UPDATE public.async_tasks SET updated_at = now() - interval '3 hours' WHERE id = ${vStaleId}`;
  const asyncRepo = createAsyncTasksRepository(db);
  const expiredCount = await asyncRepo.expireStaleTasks(2 * 3_600_000, 100);
  expect(expiredCount).toBe(1);
  const staleCheck = await pollVideo(keyUnlimited.key!, vStaleId);
  expect(staleCheck.body.status).toBe("FAILED");
  expect(staleCheck.body.failure_category).toBe("TIMEOUT");
  expect(staleCheck.body.refund_eligibility).toBe("MANUAL_REVIEW");

  const { data: listedTasks } = await internalCall<{ total: number }>(
    "GET",
    `/internal/v1/async-tasks?billing_account_id=${projA.id}`,
  );
  expect(listedTasks.total).toBe(9);

  const { data: conservation } = await internalCall<{ ok: boolean }>(
    "GET",
    "/internal/v1/audit/conservation",
  );
  expect(conservation.ok).toBe(true);
});
