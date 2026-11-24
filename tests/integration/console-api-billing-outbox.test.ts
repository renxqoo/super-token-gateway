import { afterAll, beforeAll, expect, test } from "bun:test";
import { createConsoleRuntime } from "@gateway/console-api";
import { createConsoleTestHarness } from "./helpers/console-harness.ts";

let h: Awaited<ReturnType<typeof createConsoleTestHarness>>;
let adminAccess = "";
let customerAccess = "";
let orgId = "";
let usdProjectId = "";
let eurProjectId = "";
let accountId = "";
let channelId = "";
let videoChannelId = "";
let customerPriceId = "";
let supplierPriceId = "";

beforeAll(async () => {
  h = await createConsoleTestHarness({
    consoleConfigOverrides: {
      allowPrivateAlertWebhooks: true,
    },
    enableWorker: true,
  });
  const boot = await h.bootstrapAdmin();
  adminAccess = boot.accessToken;

  const user = await h.registerUser("billing-owner@example.com");
  customerAccess = user.accessToken;
  orgId = user.defaultOrgId;
  usdProjectId = user.defaultProjectId;

  const eurProj = await h.clientApi<{ id: string }>(
    "POST",
    `/api/v1/orgs/${orgId}/projects`,
    customerAccess,
    { name: "EU Billing Project", currency: "EUR" },
  );
  expect(eurProj.status).toBe(201);
  eurProjectId = eurProj.data.id;

  const acc = await h.adminApi<{ id: string }>("POST", "/admin/v1/upstream-accounts", adminAccess, {
    name: "bill-acc",
  });
  expect(acc.status).toBe(201);
  accountId = acc.data.id;
  await h.adminApi("POST", `/admin/v1/upstream-accounts/${accountId}/recharges`, adminAccess, {
    currency: "USD",
    external_ref: "bill-sup-1",
    amount_atoms: "5000000",
  });

  const ch = await h.adminApi<{ id: string }>("POST", "/admin/v1/channels", adminAccess, {
    account_id: accountId,
    name: "bill-openai",
    kind: "openai",
    base_url: h.mockUrl,
    api_key: "sk-bill-openai",
  });
  expect(ch.status).toBe(201);
  channelId = ch.data.id;

  const videoPlugin = await h.adminApi<{ id: string }>("PUT", "/admin/v1/plugins", adminAccess, {
    name: "bill-video-provider",
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
        refundable_system_error_codes: [5001],
        non_refundable_error_codes: [1026],
      },
    },
  });
  expect(videoPlugin.status).toBe(200);
  const vch = await h.adminApi<{ id: string }>("POST", "/admin/v1/channels", adminAccess, {
    account_id: accountId,
    name: "bill-video",
    kind: "minimax",
    base_url: h.mockUrl,
    api_key: "sk-bill-video",
  });
  expect(vch.status).toBe(201);
  videoChannelId = vch.data.id;
  const videoRel = await h.adminApi("POST", "/admin/v1/plugin-releases", adminAccess, {
    id: crypto.randomUUID(),
    expected_epoch: h.gateway.snapshot.get().epoch,
    targets: [{ channel_id: videoChannelId, provider_plugin_id: videoPlugin.data.id }],
  });
  expect(videoRel.status).toBe(200);

  const cp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
    "POST",
    "/admin/v1/pricing-templates",
    adminAccess,
    {
      name: "bill-cust",
      side: "CUSTOMER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "10", denominator: "1" },
        output_tokens: { numerator: "20", denominator: "1" },
      },
      ceilings: { input_tokens: "100", output_tokens: "50" },
    },
  );
  expect(cp.status).toBe(201);
  customerPriceId = cp.data.currentPriceVersionId;

  const sp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
    "POST",
    "/admin/v1/pricing-templates",
    adminAccess,
    {
      name: "bill-sup",
      side: "SUPPLIER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "2", denominator: "1" },
        output_tokens: { numerator: "4", denominator: "1" },
      },
      ceilings: { input_tokens: "100", output_tokens: "50" },
    },
  );
  expect(sp.status).toBe(201);
  supplierPriceId = sp.data.currentPriceVersionId;

  const vcp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
    "POST",
    "/admin/v1/pricing-templates",
    adminAccess,
    {
      name: "bill-video-cust",
      side: "CUSTOMER",
      currency: "USD",
      basis: "REQUEST_COMPLETION",
      rates: { requests: { numerator: "5000", denominator: "1" } },
      ceilings: { requests: "1" },
    },
  );
  const vsp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
    "POST",
    "/admin/v1/pricing-templates",
    adminAccess,
    {
      name: "bill-video-sup",
      side: "SUPPLIER",
      currency: "USD",
      basis: "REQUEST_COMPLETION",
      rates: { requests: { numerator: "1000", denominator: "1" } },
      ceilings: { requests: "1" },
    },
  );

  expect(
    (
      await h.adminApi("POST", "/admin/v1/models", adminAccess, {
        name: "bill-gpt",
        display_name: "Bill GPT",
        operations: ["chat"],
        max_output_tokens: 100,
        list_price_template_id: cp.data.id,
      })
    ).status,
  ).toBe(201);
  expect(
    (
      await h.adminApi("POST", "/admin/v1/routes", adminAccess, {
        model_name: "bill-gpt",
        channel_id: channelId,
        upstream_model: "bill-gpt-up",
        operations: ["chat"],
        billing_price_id: customerPriceId,
        cost_version_id: supplierPriceId,
      })
    ).status,
  ).toBe(201);

  expect(
    (
      await h.adminApi("POST", "/admin/v1/models", adminAccess, {
        name: "bill-video",
        display_name: "Bill Video",
        operations: ["videos"],
        max_output_tokens: 1,
        list_price_template_id: vcp.data.id,
      })
    ).status,
  ).toBe(201);
  expect(
    (
      await h.adminApi("POST", "/admin/v1/routes", adminAccess, {
        model_name: "bill-video",
        channel_id: videoChannelId,
        upstream_model: "MiniMax-H3",
        operations: ["videos"],
        billing_price_id: vcp.data.currentPriceVersionId,
        cost_version_id: vsp.data.currentPriceVersionId,
      })
    ).status,
  ).toBe(201);
});

afterAll(async () => {
  await h?.close();
});

test("non-USD project finance defaults to project currency and rejects currency mismatch", async () => {
  const mismatch = await h.adminApi(
    "POST",
    `/admin/v1/finance/projects/${eurProjectId}/topups`,
    adminAccess,
    { currency: "USD", external_ref: "eur-mismatch-1", amount_atoms: "5000" },
  );
  expect(mismatch.status).toBe(400);

  const topup = await h.adminApi(
    "POST",
    `/admin/v1/finance/projects/${eurProjectId}/topups`,
    adminAccess,
    { external_ref: "eur-default-1", amount_atoms: "25000" },
  );
  expect(topup.status).toBe(200);

  const grant = await h.adminApi(
    "POST",
    `/admin/v1/finance/projects/${eurProjectId}/grants`,
    adminAccess,
    { external_ref: "eur-grant-1", amount_atoms: "5000" },
  );
  expect(grant.status).toBe(200);

  const wallet = await h.clientApi<{ currency: string; available_atoms: string }>(
    "GET",
    `/api/v1/projects/${eurProjectId}/wallet`,
    customerAccess,
  );
  expect(wallet.status).toBe(200);
  expect(wallet.data.currency).toBe("EUR");
  expect(wallet.data.available_atoms).toBe("30000");
});

test("marketplace pricing hides unsynced overrides, resolves raw Gateway exact prices, and matches real Gateway settlement", async () => {
  await h.adminApi("POST", `/admin/v1/finance/projects/${usdProjectId}/topups`, adminAccess, {
    external_ref: "usd-topup-1",
    amount_atoms: "50000",
  });

  const keyRes = await h.clientApi<{ id: string; key: string }>(
    "POST",
    `/api/v1/projects/${usdProjectId}/api-keys`,
    customerAccess,
    { name: "bill-key", spend_limit_atoms: "1600" },
  );
  expect(keyRes.status).toBe(201);

  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method === "PUT" && new URL(url).pathname === "/internal/v1/pricing-overrides") {
      return new Response("down", { status: 503 });
    }
    return forward();
  });
  const failedOvr = await h.adminApi("PUT", "/admin/v1/pricing-overrides", adminAccess, {
    project_id: usdProjectId,
    model_name: "bill-gpt",
    operation: "chat",
    override_kind: "MULTIPLIER",
    multiplier_numerator: "5",
    multiplier_denominator: "10",
  });
  h.setGatewayInterceptor(null);
  expect(failedOvr.status).toBe(503);

  const quoteBeforeSync = await h.clientApi<{
    pricing: { effective_rates: { input_tokens: { numerator: string; denominator: string } } };
  }>("GET", `/api/v1/models/bill-gpt?project_id=${usdProjectId}`, customerAccess);
  expect(quoteBeforeSync.status).toBe(200);
  expect(quoteBeforeSync.data.pricing.effective_rates.input_tokens).toEqual({
    numerator: "10",
    denominator: "1",
  });

  const multOvr = await h.adminApi("PUT", "/admin/v1/pricing-overrides", adminAccess, {
    project_id: usdProjectId,
    model_name: "bill-gpt",
    operation: "chat",
    override_kind: "MULTIPLIER",
    multiplier_numerator: "8",
    multiplier_denominator: "10",
  });
  expect(multOvr.status).toBe(200);

  const quoteMult = await h.clientApi<{
    pricing: {
      effective_rates: {
        input_tokens: { numerator: string; denominator: string };
        output_tokens: { numerator: string; denominator: string };
      };
    };
  }>("GET", `/api/v1/models/bill-gpt?project_id=${usdProjectId}`, customerAccess);
  expect(quoteMult.data.pricing.effective_rates.input_tokens).toEqual({
    numerator: "8",
    denominator: "1",
  });
  expect(quoteMult.data.pricing.effective_rates.output_tokens).toEqual({
    numerator: "16",
    denominator: "1",
  });

  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method === "PUT" && new URL(url).pathname === "/internal/v1/pricing-overrides") {
      return new Response("down", { status: 503 });
    }
    return forward();
  });
  try {
    const rejectedEdit = await h.adminApi("PUT", "/admin/v1/pricing-overrides", adminAccess, {
      project_id: usdProjectId,
      model_name: "bill-gpt",
      operation: "chat",
      override_kind: "MULTIPLIER",
      multiplier_numerator: "1",
      multiplier_denominator: "2",
    });
    expect(rejectedEdit.status).toBe(503);
  } finally {
    h.setGatewayInterceptor(null);
  }
  const effective = await h.clientApi<typeof quoteMult.data>(
    "GET",
    `/api/v1/models/bill-gpt?project_id=${usdProjectId}`,
    customerAccess,
  );
  expect(effective.data.pricing.effective_rates.input_tokens).toEqual({
    numerator: "8",
    denominator: "1",
  });

  const callChat = () =>
    fetch(`${h.gatewayPublicUrl}/v1/chat/completions`, {
      method: "POST",
      headers: { authorization: `Bearer ${keyRes.data.key}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "bill-gpt",
        messages: [{ role: "user", content: "billing match test" }],
        max_tokens: 50,
      }),
    });

  expect((await callChat()).status).toBe(200);
  const walletAfter1 = await h.clientApi<{ available_atoms: string }>(
    "GET",
    `/api/v1/projects/${usdProjectId}/wallet`,
    customerAccess,
  );
  expect(walletAfter1.data.available_atoms).toBe("48400");

  expect((await callChat()).status).toBe(402);

  expect(
    (
      await h.clientApi(
        "POST",
        `/api/v1/projects/${usdProjectId}/api-keys/${keyRes.data.id}/reset-spend`,
        customerAccess,
        { reason: "raise" },
      )
    ).status,
  ).toBe(200);

  const rawPrice = await h.gatewayInternalApi<{ id: string }>("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    currency: "USD",
    basis: "PROVIDER_USAGE",
    rates: {
      input_tokens: { numerator: "5", denominator: "1" },
      output_tokens: { numerator: "10", denominator: "1" },
    },
    ceilings: { input_tokens: "100", output_tokens: "50" },
  });
  expect(rawPrice.status).toBe(201);

  const exactOvr = await h.adminApi("PUT", "/admin/v1/pricing-overrides", adminAccess, {
    project_id: usdProjectId,
    model_name: "bill-gpt",
    operation: "chat",
    override_kind: "EXACT_PRICE",
    billing_price_id: rawPrice.data.id,
  });
  expect(exactOvr.status).toBe(200);

  const quoteExact = await h.clientApi<{
    pricing: {
      effective_rates: {
        input_tokens: { numerator: string; denominator: string };
        output_tokens: { numerator: string; denominator: string };
      };
    };
  }>("GET", `/api/v1/models/bill-gpt?project_id=${usdProjectId}`, customerAccess);
  expect(quoteExact.data.pricing.effective_rates.input_tokens).toEqual({
    numerator: "5",
    denominator: "1",
  });
  expect(quoteExact.data.pricing.effective_rates.output_tokens).toEqual({
    numerator: "10",
    denominator: "1",
  });

  expect((await callChat()).status).toBe(200);
  const walletAfter2 = await h.clientApi<{ available_atoms: string }>(
    "GET",
    `/api/v1/projects/${usdProjectId}/wallet`,
    customerAccess,
  );
  expect(walletAfter2.data.available_atoms).toBe("47400");

  const videoKeyRes = await h.clientApi<{ id: string; key: string }>(
    "POST",
    `/api/v1/projects/${usdProjectId}/api-keys`,
    customerAccess,
    { name: "bill-video-key" },
  );
  expect(videoKeyRes.status).toBe(201);

  const videoRes = await fetch(`${h.gatewayPublicUrl}/v1/videos`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${videoKeyRes.data.key}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ model: "bill-video", prompt: "sunrise" }),
  });
  expect(videoRes.status).toBe(200);

  const clientCharges = await h.clientApi<{ items: Record<string, unknown>[] }>(
    "GET",
    `/api/v1/projects/${usdProjectId}/charges`,
    customerAccess,
  );
  expect(clientCharges.status).toBe(200);
  expect(clientCharges.data.items.length).toBeGreaterThanOrEqual(2);
  for (const ch of clientCharges.data.items) {
    expect(ch.supplier_cost_atoms).toBeUndefined();
    expect(ch.cost_version_id).toBeUndefined();
    expect(ch.channel_id).toBeUndefined();
  }

  const clientTasks = await h.clientApi<{ items: Record<string, unknown>[] }>(
    "GET",
    `/api/v1/projects/${usdProjectId}/tasks`,
    customerAccess,
  );
  expect(clientTasks.status).toBe(200);
  expect(clientTasks.data.items.length).toBeGreaterThanOrEqual(1);
  for (const t of clientTasks.data.items) {
    expect(t.channel_id).toBeUndefined();
    expect(t.provider_task_id).toBeUndefined();
  }

  const conservationOk = await h.adminApi<{ ok: boolean }>(
    "GET",
    "/admin/v1/finance/conservation",
    adminAccess,
  );
  expect(conservationOk.status).toBe(200);
  expect(conservationOk.data.ok).toBe(true);

  h.setGatewayInterceptor(async (url, _init, forward) => {
    if (new URL(url).pathname === "/internal/v1/audit/conservation") {
      return Response.json(
        { ok: false, open_holds: 3, wallet_imbalance_atoms: "42" },
        { status: 500 },
      );
    }
    return forward();
  });
  const conservationFail = await h.adminApi<{ ok: boolean; open_holds: number }>(
    "GET",
    "/admin/v1/finance/conservation",
    adminAccess,
  );
  h.setGatewayInterceptor(null);
  expect(conservationFail.status).toBe(500);
  expect(conservationFail.data.ok).toBe(false);
  expect(conservationFail.data.open_holds).toBe(3);
});

test("outbox push/pull, concurrent replay idempotency, refunds/adjustments, usage analytics, and async alert webhook delivery work end-to-end", async () => {
  const lowBalanceRule = await h.clientApi<{ id: string }>(
    "POST",
    `/api/v1/projects/${usdProjectId}/alerts`,
    customerAccess,
    {
      kind: "LOW_BALANCE",
      threshold_atoms: "100000",
      webhook_url: h.webhookUrl,
    },
  );
  expect(lowBalanceRule.status).toBe(201);

  const dailySpendRule = await h.clientApi<{ id: string }>(
    "POST",
    `/api/v1/projects/${usdProjectId}/alerts`,
    customerAccess,
    {
      kind: "DAILY_SPEND",
      threshold_atoms: "2000",
      webhook_url: h.webhookUrl,
    },
  );
  expect(dailySpendRule.status).toBe(201);

  const badAuthPush = await h.adminApi("POST", "/internal/outbox/events", "wrong-token", {
    events: [],
  });
  expect(badAuthPush.status).toBe(401);

  const pulled = await h.consoleRuntime.pullOutboxOnce();
  expect(pulled.claimed).toBeGreaterThanOrEqual(2);
  expect(pulled.ingested).toBeGreaterThanOrEqual(2);
  expect(pulled.acked).toBe(true);

  const rawEvents = await h.gwDb<
    {
      seq: string;
      event_id: string;
      kind: string;
      request_id: string | null;
      project_id: string | null;
      payload: Record<string, unknown>;
      created_at: Date;
    }[]
  >`
    SELECT
      seq::text AS seq,
      event_id::text AS event_id,
      kind,
      request_id::text AS request_id,
      project_id::text AS project_id,
      payload,
      created_at
    FROM public.outbox_events
    WHERE project_id = ${usdProjectId}::uuid
    ORDER BY seq ASC
  `;
  expect(rawEvents.length).toBeGreaterThanOrEqual(2);

  const replayBatch = rawEvents.map((e) => ({
    seq: e.seq,
    event_id: e.event_id,
    kind: e.kind,
    request_id: e.request_id,
    project_id: e.project_id,
    payload: e.payload,
    created_at: e.created_at.toISOString(),
  }));

  const [dup1, dup2] = await Promise.all([
    h.adminApi<{ ingested: number }>("POST", "/internal/outbox/events", h.outboxToken, {
      events: replayBatch,
    }),
    h.adminApi<{ ingested: number }>("POST", "/internal/outbox/events", h.outboxToken, {
      events: replayBatch,
    }),
  ]);
  expect(dup1.status).toBe(200);
  expect(dup2.status).toBe(200);
  expect(dup1.data.ingested).toBe(0);
  expect(dup2.data.ingested).toBe(0);

  const settledSummary = await h.clientApi<{
    request_count: number;
    customer_atoms: string;
    input_tokens: number;
    output_tokens: number;
  }>("GET", `/api/v1/projects/${usdProjectId}/usage/summary`, customerAccess);
  expect(settledSummary.status).toBe(200);
  expect(settledSummary.data.request_count).toBe(3);
  expect(settledSummary.data.customer_atoms).toBe("7600");
  expect(settledSummary.data.input_tokens).toBe(200);
  expect(settledSummary.data.output_tokens).toBe(100);

  const refundEventId = crypto.randomUUID();
  const adjustEventId = crypto.randomUUID();
  const adjPush = await h.adminApi<{ ingested: number }>(
    "POST",
    "/internal/outbox/events",
    h.outboxToken,
    {
      events: [
        {
          event_id: refundEventId,
          kind: "billing.refunded",
          project_id: usdProjectId,
          payload: {
            project_id: usdProjectId,
            model: "bill-gpt",
            operation: "chat",
            currency: "USD",
            refunded_atoms: "300",
          },
          created_at: new Date().toISOString(),
        },
        {
          event_id: adjustEventId,
          kind: "billing.adjusted",
          project_id: usdProjectId,
          payload: {
            project_id: usdProjectId,
            model: "bill-gpt",
            operation: "chat",
            currency: "USD",
            delta_atoms: "100",
          },
          created_at: new Date().toISOString(),
        },
      ],
    },
  );
  expect(adjPush.status).toBe(200);
  expect(adjPush.data.ingested).toBe(2);

  const summaryAfterAdj = await h.clientApi<{ request_count: number; customer_atoms: string }>(
    "GET",
    `/api/v1/projects/${usdProjectId}/usage/summary`,
    customerAccess,
  );
  expect(summaryAfterAdj.data.request_count).toBe(3);
  expect(summaryAfterAdj.data.customer_atoms).toBe("7400");

  const byModel = await h.clientApi<{ items: { model: string; customer_atoms: string }[] }>(
    "GET",
    `/api/v1/projects/${usdProjectId}/usage/by-model`,
    customerAccess,
  );
  expect(byModel.data.items.find((m) => m.model === "bill-gpt")?.customer_atoms).toBe("2400");
  expect(byModel.data.items.find((m) => m.model === "bill-video")?.customer_atoms).toBe("5000");

  const timeseries = await h.clientApi<{ items: unknown[] }>(
    "GET",
    `/api/v1/projects/${usdProjectId}/usage/timeseries?granularity=hour`,
    customerAccess,
  );
  expect(timeseries.status).toBe(200);
  expect(timeseries.data.items.length).toBeGreaterThanOrEqual(1);

  expect(
    (
      await h.clientApi(
        "GET",
        `/api/v1/projects/${usdProjectId}/usage/summary?from=not-a-date`,
        customerAccess,
      )
    ).status,
  ).toBe(400);

  h.setWebhookStatus(500);
  expect(await h.consoleRuntime.flushAlertsOnce()).toBe(2);
  const firstAttempt = await h.consoleRuntime.deliverAlertWebhooksOnce();
  expect(firstAttempt.attempted).toBe(2);
  expect(firstAttempt.delivered).toBe(0);

  const pendingRows = await h.conDb<
    { status: string; attempts: number; last_http_status: number }[]
  >`
    SELECT status, attempts, last_http_status
    FROM public.alert_deliveries
    WHERE project_id = ${usdProjectId}::uuid
  `;
  expect(pendingRows.length).toBe(2);
  for (const row of pendingRows) {
    expect(row.status).toBe("PENDING");
    expect(row.attempts).toBe(1);
    expect(row.last_http_status).toBe(500);
  }

  await h.conDb`
    UPDATE public.alert_deliveries
    SET next_attempt_at = now() - interval '1 second'
    WHERE project_id = ${usdProjectId}::uuid
  `;
  h.setWebhookStatus(200);
  const secondAttempt = await h.consoleRuntime.deliverAlertWebhooksOnce();
  expect(secondAttempt.attempted).toBe(2);
  expect(secondAttempt.delivered).toBe(2);

  const deliveredRows = await h.conDb<
    { status: string; attempts: number; last_http_status: number }[]
  >`
    SELECT status, attempts, last_http_status
    FROM public.alert_deliveries
    WHERE project_id = ${usdProjectId}::uuid
  `;
  expect(deliveredRows.every((r) => r.status === "DELIVERED" && r.last_http_status === 200)).toBe(
    true,
  );
  expect(h.webhookDeliveries.length).toBe(4);

  await h.conDb`
    INSERT INTO public.alert_eval_queue (project_id)
    VALUES (${usdProjectId}::uuid)
    ON CONFLICT (project_id) DO UPDATE SET dirty_at = now()
  `;
  const cooldownRun = await h.consoleRuntime.flushAlertsOnce();
  expect(cooldownRun).toBe(0);
});

test("a temporary wallet read error keeps an ingested alert eligible for retry without another event", async () => {
  const rule = await h.clientApi<{ id: string }>(
    "POST",
    `/api/v1/projects/${eurProjectId}/alerts`,
    customerAccess,
    { kind: "LOW_BALANCE", threshold_atoms: "99999999999999999999999999999999999999" },
  );
  expect(rule.status).toBe(201);
  const event = {
    event_id: crypto.randomUUID(),
    kind: "billing.settled",
    project_id: eurProjectId,
    payload: { currency: "EUR" },
  };
  let walletUnavailable = false;
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (
      init?.method === "GET" &&
      new URL(url).pathname === `/internal/v1/wallets/${eurProjectId}`
    ) {
      walletUnavailable = true;
      return new Response("temporarily unavailable", { status: 503 });
    }
    return forward();
  });
  try {
    const push = await h.adminApi("POST", "/internal/outbox/events", h.outboxToken, {
      events: [event],
    });
    expect(push.data).toMatchObject({ ingested: 1 });
    expect(await h.consoleRuntime.flushAlertsOnce()).toBe(0);
    expect(walletUnavailable).toBe(true);
  } finally {
    h.setGatewayInterceptor(null);
  }
  await h.conDb`UPDATE public.alert_eval_queue SET next_attempt_at = now() - interval '1 second' WHERE project_id = ${eurProjectId}`;
  expect(await h.consoleRuntime.flushAlertsOnce()).toBe(1);
  expect(await h.consoleRuntime.flushAlertsOnce()).toBe(0);
});

test("a late failed webhook attempt cannot overwrite a newer successful delivery", async () => {
  const rule = await h.clientApi<{ id: string }>(
    "POST",
    `/api/v1/projects/${eurProjectId}/alerts`,
    customerAccess,
    { kind: "DAILY_SPEND", threshold_atoms: "1", webhook_url: h.webhookUrl },
  );
  expect(rule.status).toBe(201);
  const push = await h.adminApi("POST", "/internal/outbox/events", h.outboxToken, {
    events: [
      {
        event_id: crypto.randomUUID(),
        kind: "billing.settled",
        project_id: eurProjectId,
        payload: { currency: "EUR", customer_atoms: "1", model: "bill-gpt", operation: "chat" },
      },
    ],
  });
  expect(push.data).toMatchObject({ ingested: 1 });
  expect(await h.consoleRuntime.flushAlertsOnce()).toBe(1);

  const started = Promise.withResolvers<void>();
  const firstResponse = Promise.withResolvers<Response>();
  const runtime = createConsoleRuntime({
    config: h.consoleConfig,
    db: h.conDb,
    alertFetch: async () => {
      started.resolve();
      return firstResponse.promise;
    },
  });
  const first = runtime.deliverAlertWebhooksOnce();
  try {
    await Promise.race([started.promise, first]);
    await h.conDb`UPDATE public.alert_deliveries SET next_attempt_at = now() - interval '1 second' WHERE rule_id = ${rule.data.id}`;
    const second = await h.consoleRuntime.deliverAlertWebhooksOnce();
    expect(second.delivered).toBe(1);
  } finally {
    firstResponse.resolve(new Response(null, { status: 503 }));
    await first;
  }
  const [delivery] =
    await h.conDb`SELECT status, attempts, last_http_status FROM public.alert_deliveries WHERE rule_id = ${rule.data.id}`;
  expect(delivery).toEqual({ status: "DELIVERED", attempts: 2, last_http_status: 200 });
});

test("low-balance alerts use the current wallet instead of a delayed settlement snapshot", async () => {
  const project = await h.clientApi<{ id: string }>(
    "POST",
    `/api/v1/orgs/${orgId}/projects`,
    customerAccess,
    {
      name: "Wallet Snapshot Recovery",
      currency: "USD",
    },
  );
  const id = project.data.id;
  expect(project.status).toBe(201);
  expect(
    (
      await h.adminApi("POST", `/admin/v1/finance/projects/${id}/topups`, adminAccess, {
        external_ref: "alert-wallet-current",
        amount_atoms: "10000",
      })
    ).status,
  ).toBe(200);
  expect(
    (
      await h.clientApi("POST", `/api/v1/projects/${id}/alerts`, customerAccess, {
        kind: "LOW_BALANCE",
        threshold_atoms: "5",
      })
    ).status,
  ).toBe(201);
  const event = await h.adminApi("POST", "/internal/outbox/events", h.outboxToken, {
    events: [
      {
        event_id: crypto.randomUUID(),
        kind: "billing.settled",
        project_id: id,
        payload: { currency: "USD", wallet_available_atoms: "0" },
      },
    ],
  });
  expect(event.data).toMatchObject({ ingested: 1 });
  expect(await h.consoleRuntime.flushAlertsOnce()).toBe(0);
});

test("Outbox push acknowledges persisted billing before any slow alert wallet read", async () => {
  const project = await h.clientApi<{ id: string }>(
    "POST",
    `/api/v1/orgs/${orgId}/projects`,
    customerAccess,
    {
      name: "Deferred Alert Evaluation",
      currency: "USD",
    },
  );
  expect(project.status).toBe(201);
  const id = project.data.id;
  expect(
    (
      await h.clientApi("POST", `/api/v1/projects/${id}/alerts`, customerAccess, {
        kind: "LOW_BALANCE",
        threshold_atoms: "5",
      })
    ).status,
  ).toBe(201);
  const release = Promise.withResolvers<Response>();
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method === "GET" && new URL(url).pathname === `/internal/v1/wallets/${id}`)
      return release.promise;
    return forward();
  });
  const push = h.adminApi("POST", "/internal/outbox/events", h.outboxToken, {
    events: [
      {
        event_id: crypto.randomUUID(),
        kind: "billing.settled",
        project_id: id,
        payload: { currency: "USD" },
      },
    ],
  });
  try {
    const ack = await Promise.race([push, Bun.sleep(500).then(() => null)]);
    expect(ack).toMatchObject({ status: 200, data: { ingested: 1 } });
    const evaluation = h.consoleRuntime.flushAlertsOnce();
    release.resolve(Response.json({ available_atoms: "0" }));
    expect(await evaluation).toBe(1);
  } finally {
    release.resolve(Response.json({ available_atoms: "0" }));
    await push;
    h.setGatewayInterceptor(null);
  }
});

test("shutdown during the last available webhook attempt leaves the alert eligible for delivery", async () => {
  const rule = await h.clientApi<{ id: string }>(
    "POST",
    `/api/v1/projects/${eurProjectId}/alerts`,
    customerAccess,
    { kind: "DAILY_SPEND", threshold_atoms: "1", webhook_url: h.webhookUrl },
  );
  expect(rule.status).toBe(201);
  const deliveryId = crypto.randomUUID();
  await h.conDb`
    INSERT INTO public.alert_deliveries (id, rule_id, project_id, status, webhook_url, attempts)
    VALUES (${deliveryId}, ${rule.data.id}, ${eurProjectId}, 'PENDING', ${h.webhookUrl}, 4)
  `;
  const started = Promise.withResolvers<void>();
  const runtime = createConsoleRuntime({
    config: { ...h.consoleConfig, alertEvaluationIntervalMs: 0, alertDeliveryIntervalMs: 10 },
    db: h.conDb,
    alertFetch: async (_input, init) => {
      const request = Promise.withResolvers<Response>();
      const signal = init!.signal!;
      signal.addEventListener("abort", () => request.reject(signal.reason), { once: true });
      started.resolve();
      return request.promise;
    },
  });
  try {
    await runtime.start();
    expect(
      await Promise.race([started.promise.then(() => true), Bun.sleep(1000).then(() => false)]),
    ).toBe(true);
  } finally {
    await runtime.close();
  }
  expect((await h.consoleRuntime.deliverAlertWebhooksOnce()).delivered).toBe(1);
  const [delivery] =
    await h.conDb`SELECT status, attempts FROM public.alert_deliveries WHERE id = ${deliveryId}`;
  expect(delivery).toEqual({ status: "DELIVERED", attempts: 5 });
});
