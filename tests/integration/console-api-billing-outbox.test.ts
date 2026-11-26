import { expect, test } from "bun:test";
import {
  h,
  eurBillingAccountId,
  adminAccess,
  customerAccess,
  usdBillingAccountId,
} from "./console-billing-fixture.ts";
import { registerBillingFixture } from "./console-billing-fixture.ts";
import { registerAlertCases } from "./console-billing-alert-cases.ts";
import { registerVideoTaskCases } from "./console-video-task-cases.ts";

registerBillingFixture();

test("non-USD billingAccount finance defaults to billingAccount currency and rejects currency mismatch", async () => {
  const mismatch = await h.adminApi(
    "POST",
    `/admin/v1/finance/billing-accounts/${eurBillingAccountId}/topups`,
    adminAccess,
    { currency: "USD", external_ref: "eur-mismatch-1", amount_atoms: "5000" },
  );
  expect(mismatch.status).toBe(400);

  const topup = await h.adminApi(
    "POST",
    `/admin/v1/finance/billing-accounts/${eurBillingAccountId}/topups`,
    adminAccess,
    { external_ref: "eur-default-1", amount_atoms: "25000" },
  );
  expect(topup.status).toBe(200);

  const grant = await h.adminApi(
    "POST",
    `/admin/v1/finance/billing-accounts/${eurBillingAccountId}/grants`,
    adminAccess,
    { external_ref: "eur-grant-1", amount_atoms: "5000" },
  );
  expect(grant.status).toBe(200);

  const wallet = await h.clientApi<{ currency: string; available_atoms: string }>(
    "GET",
    `/api/v1/billing-accounts/${eurBillingAccountId}/wallet`,
    customerAccess,
  );
  expect(wallet.status).toBe(200);
  expect(wallet.data.currency).toBe("EUR");
  expect(wallet.data.available_atoms).toBe("30000");
});

test("marketplace pricing hides unsynced overrides, resolves raw Gateway exact prices, and matches real Gateway settlement", async () => {
  await h.adminApi(
    "POST",
    `/admin/v1/finance/billing-accounts/${usdBillingAccountId}/topups`,
    adminAccess,
    {
      external_ref: "usd-topup-1",
      amount_atoms: "50000",
    },
  );

  const keyRes = await h.clientApi<{ id: string; key: string }>(
    "POST",
    `/api/v1/billing-accounts/${usdBillingAccountId}/api-keys`,
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
    billing_account_id: usdBillingAccountId,
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
  }>("GET", `/api/v1/models/bill-gpt?billing_account_id=${usdBillingAccountId}`, customerAccess);
  expect(quoteBeforeSync.status).toBe(200);
  expect(quoteBeforeSync.data.pricing.effective_rates.input_tokens).toEqual({
    numerator: "10",
    denominator: "1",
  });

  const multOvr = await h.adminApi("PUT", "/admin/v1/pricing-overrides", adminAccess, {
    billing_account_id: usdBillingAccountId,
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
  }>("GET", `/api/v1/models/bill-gpt?billing_account_id=${usdBillingAccountId}`, customerAccess);
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
      billing_account_id: usdBillingAccountId,
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
    `/api/v1/models/bill-gpt?billing_account_id=${usdBillingAccountId}`,
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
    `/api/v1/billing-accounts/${usdBillingAccountId}/wallet`,
    customerAccess,
  );
  expect(walletAfter1.data.available_atoms).toBe("48400");

  expect((await callChat()).status).toBe(402);

  expect(
    (
      await h.clientApi(
        "POST",
        `/api/v1/billing-accounts/${usdBillingAccountId}/api-keys/${keyRes.data.id}/reset-spend`,
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
    billing_account_id: usdBillingAccountId,
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
  }>("GET", `/api/v1/models/bill-gpt?billing_account_id=${usdBillingAccountId}`, customerAccess);
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
    `/api/v1/billing-accounts/${usdBillingAccountId}/wallet`,
    customerAccess,
  );
  expect(walletAfter2.data.available_atoms).toBe("47400");

  const videoKeyRes = await h.clientApi<{ id: string; key: string }>(
    "POST",
    `/api/v1/billing-accounts/${usdBillingAccountId}/api-keys`,
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
    `/api/v1/billing-accounts/${usdBillingAccountId}/charges`,
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
    `/api/v1/billing-accounts/${usdBillingAccountId}/tasks`,
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
    `/api/v1/billing-accounts/${usdBillingAccountId}/alerts`,
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
    `/api/v1/billing-accounts/${usdBillingAccountId}/alerts`,
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
      billing_account_id: string | null;
      payload: Record<string, unknown>;
      created_at: Date;
    }[]
  >`
    SELECT
      seq::text AS seq,
      event_id::text AS event_id,
      kind,
      request_id::text AS request_id,
      billing_account_id::text AS billing_account_id,
      payload,
      created_at
    FROM public.outbox_events
    WHERE billing_account_id = ${usdBillingAccountId}::uuid
    ORDER BY seq ASC
  `;
  expect(rawEvents.length).toBeGreaterThanOrEqual(2);

  const replayBatch = rawEvents.map((e) => ({
    seq: e.seq,
    event_id: e.event_id,
    kind: e.kind,
    request_id: e.request_id,
    billing_account_id: e.billing_account_id,
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
  }>("GET", `/api/v1/billing-accounts/${usdBillingAccountId}/usage/summary`, customerAccess);
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
          billing_account_id: usdBillingAccountId,
          payload: {
            billing_account_id: usdBillingAccountId,
            model: "bill-gpt",
            operation: "chat",
            currency: "USD",
            refunded_atoms: "300",
            delta_atoms: "-300",
          },
          created_at: new Date().toISOString(),
        },
        {
          event_id: adjustEventId,
          kind: "billing.adjusted",
          billing_account_id: usdBillingAccountId,
          payload: {
            billing_account_id: usdBillingAccountId,
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
    `/api/v1/billing-accounts/${usdBillingAccountId}/usage/summary`,
    customerAccess,
  );
  expect(summaryAfterAdj.data.request_count).toBe(3);
  expect(summaryAfterAdj.data.customer_atoms).toBe("7400");

  const byModel = await h.clientApi<{ items: { model: string; customer_atoms: string }[] }>(
    "GET",
    `/api/v1/billing-accounts/${usdBillingAccountId}/usage/by-model`,
    customerAccess,
  );
  expect(byModel.data.items.find((m) => m.model === "bill-gpt")?.customer_atoms).toBe("2400");
  expect(byModel.data.items.find((m) => m.model === "bill-video")?.customer_atoms).toBe("5000");

  const timeseries = await h.clientApi<{ items: unknown[] }>(
    "GET",
    `/api/v1/billing-accounts/${usdBillingAccountId}/usage/timeseries?granularity=hour`,
    customerAccess,
  );
  expect(timeseries.status).toBe(200);
  expect(timeseries.data.items.length).toBeGreaterThanOrEqual(1);

  expect(
    (
      await h.clientApi(
        "GET",
        `/api/v1/billing-accounts/${usdBillingAccountId}/usage/summary?from=not-a-date`,
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
    WHERE billing_account_id = ${usdBillingAccountId}::uuid
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
    WHERE billing_account_id = ${usdBillingAccountId}::uuid
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
    WHERE billing_account_id = ${usdBillingAccountId}::uuid
  `;
  expect(deliveredRows.every((r) => r.status === "DELIVERED" && r.last_http_status === 200)).toBe(
    true,
  );
  expect(h.webhookDeliveries.length).toBe(4);

  await h.conDb`
    INSERT INTO public.alert_eval_queue (billing_account_id)
    VALUES (${usdBillingAccountId}::uuid)
    ON CONFLICT (billing_account_id) DO UPDATE SET dirty_at = now()
  `;
  const cooldownRun = await h.consoleRuntime.flushAlertsOnce();
  expect(cooldownRun).toBe(0);
});

registerAlertCases();
registerVideoTaskCases();
